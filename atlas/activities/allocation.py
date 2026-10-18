"""Resource allocation activities (RD / RT / VLAN / VRF / PE-CE subnet).

``ResourceAllocator`` (``atlas.services.allocator``) is idempotent: a retried activity gets the
same values back because every allocation is keyed on ``(service_id, resource_type, scope)``
and uniqueness across services is enforced by the database, not by this code.
"""

from __future__ import annotations

from temporalio import activity

from atlas.activities._support import get_logger, map_domain_error, non_retryable
from atlas.core.config import get_settings
from atlas.core.enums import AuditEventType
from atlas.db.repositories import AllocationRepository, AuditRepository, ServiceRepository
from atlas.db.session import transaction
from atlas.schemas import AllocatedResources, ProvisionRequest, ServiceRef
from atlas.services.allocator import ResourceAllocator


def _wf_id() -> str | None:
    return activity.info().workflow_id if activity.in_activity() else None


@activity.defn(name="allocate_resources")
async def allocate_resources(req: ProvisionRequest) -> AllocatedResources:
    log = get_logger(service_id=req.service_id, correlation_id=req.correlation_id)
    try:
        async with transaction() as session:
            service = await ServiceRepository(session).get(req.service_id)
            if service is None:
                raise non_retryable(f"service {req.service_id} not found", error_type="ValidationError")
            allocator = ResourceAllocator(session, get_settings())
            resources = await allocator.allocate_for_service(service, req.request)
            await AuditRepository(session).record(
                AuditEventType.RESOURCES_ALLOCATED,
                service_id=req.service_id,
                workflow_id=_wf_id(),
                correlation_id=req.correlation_id,
                message=f"allocated RT {resources.route_target} and {len(resources.endpoints)} endpoint resource sets",
                details={
                    "route_target": resources.route_target,
                    "vrf_name": resources.vrf_name,
                    "endpoints": [
                        {
                            "device": e.device_hostname,
                            "rd": e.route_distinguisher,
                            "vlan": e.vlan_id,
                            "pe_ip": e.pe_ip_address,
                        }
                        for e in resources.endpoints
                    ],
                },
            )
    except Exception as exc:
        log.error("allocation_failed", error=str(exc))
        raise map_domain_error(exc) from exc
    log.info("resources_allocated", route_target=resources.route_target, endpoints=len(resources.endpoints))
    return resources


@activity.defn(name="confirm_allocations")
async def confirm_allocations(ref: ServiceRef) -> int:
    """RESERVED -> ALLOCATED once the service is confirmed on the network."""
    try:
        async with transaction() as session:
            count = await AllocationRepository(session).confirm(ref.service_id)
    except Exception as exc:
        raise map_domain_error(exc) from exc
    get_logger(service_id=ref.service_id, correlation_id=ref.correlation_id).info(
        "allocations_confirmed", count=count
    )
    return count


@activity.defn(name="release_resources")
async def release_resources(ref: ServiceRef) -> int:
    """Free every allocation of the service (idempotent; returns released count)."""
    log = get_logger(service_id=ref.service_id, correlation_id=ref.correlation_id)
    try:
        async with transaction() as session:
            allocator = ResourceAllocator(session, get_settings())
            count = await allocator.release_for_service(ref.service_id)
            if count:
                await AuditRepository(session).record(
                    AuditEventType.RESOURCES_RELEASED,
                    service_id=ref.service_id,
                    workflow_id=_wf_id(),
                    correlation_id=ref.correlation_id,
                    message=f"released {count} resource allocations",
                    details={"count": count},
                )
    except Exception as exc:
        log.error("release_failed", error=str(exc))
        raise map_domain_error(exc) from exc
    log.info("resources_released", count=count)
    return count


__all__ = ["allocate_resources", "confirm_allocations", "release_resources"]
