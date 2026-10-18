"""Post-provisioning verification: per-device operational state, service connectivity, commit."""

from __future__ import annotations

from typing import Any

from temporalio import activity

from atlas.activities._support import (
    get_logger,
    map_adapter_error,
    map_domain_error,
    record_device_operation,
    retryable,
    with_heartbeat,
)
from atlas.adapters.base import AdapterError
from atlas.adapters.registry import get_adapter, handle_from_device
from atlas.core.config import get_settings
from atlas.core.enums import (
    AuditEventType,
    DeviceOperationStatus,
    DeviceOperationType,
    EndpointStatus,
    FailureKind,
    LifecycleStatus,
    ResourceType,
    Vendor,
)
from atlas.db.repositories import (
    AllocationRepository,
    AuditRepository,
    DeviceOperationRepository,
    DeviceRepository,
    ServiceRepository,
)
from atlas.db.session import transaction
from atlas.schemas import (
    DeviceOperationRequest,
    DeviceOperationResult,
    ServiceRef,
    ValidationCheck,
    ValidationReport,
)

_HEARTBEAT_INTERVAL = 10.0


def _attempt() -> int:
    return activity.info().attempt if activity.in_activity() else 1


def _wf_id() -> str | None:
    return activity.info().workflow_id if activity.in_activity() else None


@activity.defn(name="verify_device")
async def verify_device(req: DeviceOperationRequest) -> DeviceOperationResult:
    """Verify VRF/RD/RT, interface and BGP state on one device against the intent."""
    log = get_logger(
        service_id=req.service_id, correlation_id=req.correlation_id, device_id=req.config.device_id
    )
    hostname = req.config.device_hostname
    try:
        async with transaction() as session:
            ops = DeviceOperationRepository(session)
            operation, _ = await ops.get_or_create(
                service_id=req.service_id,
                workflow_id=req.workflow_id,
                device_id=req.config.device_id,
                endpoint_id=req.config.endpoint_id,
                operation_type=DeviceOperationType.VERIFY,
                idempotency_key=req.idempotency_key,
                request_payload={"intent": req.config.intent},
            )
            op_id = operation.id
            await ops.mark(op_id, DeviceOperationStatus.IN_PROGRESS, attempt=_attempt())
            device = await DeviceRepository(session).get(req.config.device_id)
            if device is None:
                raise LookupError(f"device {req.config.device_id} not found")
            handle = handle_from_device(device)
        adapter = get_adapter(Vendor(req.config.vendor), get_settings())
        result = await with_heartbeat(
            adapter.verify_configuration(handle, req.config.intent), interval=_HEARTBEAT_INTERVAL
        )
    except AdapterError as exc:
        record_device_operation(
            vendor=str(req.config.vendor), operation_type=DeviceOperationType.VERIFY, success=False
        )
        raise map_adapter_error(exc) from exc
    except Exception as exc:
        raise map_domain_error(exc) from exc

    try:
        async with transaction() as session:
            ops = DeviceOperationRepository(session)
            if result.passed:
                await ops.mark(
                    op_id,
                    DeviceOperationStatus.SUCCEEDED,
                    response_payload={"checks": result.checks, "observed": result.observed},
                    attempt=_attempt(),
                )
                await ServiceRepository(session).set_endpoint_status(
                    req.config.endpoint_id, EndpointStatus.VERIFIED
                )
            else:
                failed = [c for c in result.checks if not c.get("passed", False)]
                await ops.mark(
                    op_id,
                    DeviceOperationStatus.PENDING,  # left retryable; final FAILED is decided by the workflow
                    error=f"verification failed: {failed}",
                    failure_kind=FailureKind.UNKNOWN,
                    attempt=_attempt(),
                )
            await AuditRepository(session).record(
                AuditEventType.DEVICE_OPERATION,
                service_id=req.service_id,
                workflow_id=req.workflow_id,
                device_id=req.config.device_id,
                correlation_id=req.correlation_id,
                message=f"VERIFY on {hostname} " + ("passed" if result.passed else "failed"),
                details={"checks": result.checks},
            )
    except Exception as exc:
        raise map_domain_error(exc) from exc

    record_device_operation(
        vendor=str(req.config.vendor), operation_type=DeviceOperationType.VERIFY, success=result.passed
    )
    if not result.passed:
        failed = [c.get("name", "?") for c in result.checks if not c.get("passed", False)]
        log.warning("verify_device_failed", hostname=hostname, failed_checks=failed, attempt=_attempt())
        raise retryable(
            f"verification failed on {hostname}: {', '.join(map(str, failed))}",
            error_type="VerificationFailedError",
            details={"checks": result.checks},
        )
    log.info("verify_device_passed", hostname=hostname)
    return DeviceOperationResult(
        operation_id=op_id,
        device_id=req.config.device_id,
        device_hostname=hostname,
        operation_type=DeviceOperationType.VERIFY,
        status=DeviceOperationStatus.SUCCEEDED,
        details={"checks": result.checks, "observed": result.observed},
    )


@activity.defn(name="validate_service_connectivity")
async def validate_service_connectivity(ref: ServiceRef) -> ValidationReport:
    """Pairwise reachability matrix: two endpoints are reachable when both are VERIFIED and share
    VRF + route-target. Returned (not raised) so the workflow can decide on compensation."""
    log = get_logger(service_id=ref.service_id, correlation_id=ref.correlation_id)
    try:
        async with transaction() as session:
            service = await ServiceRepository(session).get(ref.service_id)
            if service is None:
                raise LookupError(f"service {ref.service_id} not found")
            rts = {
                a.resource_value
                for a in service.allocations
                if str(a.resource_type) == ResourceType.ROUTE_TARGET
            }
            rt = next(iter(rts), (service.desired_state or {}).get("route_target"))
            endpoints = sorted(service.endpoints, key=lambda e: e.sequence)
            checks: list[ValidationCheck] = []
            matrix: dict[str, dict[str, bool]] = {}
            for a in endpoints:
                row: dict[str, bool] = {}
                for b in endpoints:
                    if a.id == b.id:
                        continue
                    reachable = (
                        str(a.status) == EndpointStatus.VERIFIED
                        and str(b.status) == EndpointStatus.VERIFIED
                        and a.vrf_name == b.vrf_name
                        and rt is not None
                    )
                    row[b.device.hostname] = reachable
                    if a.sequence < b.sequence:
                        checks.append(
                            ValidationCheck(
                                name="pairwise_reachability",
                                device_hostname=f"{a.device.hostname}<->{b.device.hostname}",
                                passed=reachable,
                                detail=None
                                if reachable
                                else f"{a.device.hostname}={a.status}, {b.device.hostname}={b.status}, vrf {a.vrf_name}/{b.vrf_name}, rt {rt}",
                            )
                        )
                matrix[a.device.hostname] = row
            if len(endpoints) == 1:
                ok = str(endpoints[0].status) == EndpointStatus.VERIFIED
                checks.append(
                    ValidationCheck(
                        name="single_site_verified", device_hostname=endpoints[0].device.hostname, passed=ok
                    )
                )
            report = ValidationReport(
                service_id=ref.service_id,
                passed=bool(checks) and all(c.passed for c in checks),
                checks=checks,
            )
            await AuditRepository(session).record(
                AuditEventType.VALIDATION_RESULT,
                service_id=ref.service_id,
                workflow_id=_wf_id(),
                correlation_id=ref.correlation_id,
                message="service connectivity " + ("verified" if report.passed else "FAILED"),
                details={"matrix": matrix, "route_target": rt},
            )
    except Exception as exc:
        raise map_domain_error(exc) from exc
    log.info("service_connectivity", passed=report.passed, pairs=len(checks))
    return report


@activity.defn(name="commit_service_state")
async def commit_service_state(ref: ServiceRef) -> str:
    """Snapshot observed state from every device, confirm allocations and transition to ACTIVE."""
    log = get_logger(service_id=ref.service_id, correlation_id=ref.correlation_id)
    try:
        async with transaction() as session:
            service = await ServiceRepository(session).get(ref.service_id)
            if service is None:
                raise LookupError(f"service {ref.service_id} not found")
            targets = [
                (ep.device.hostname, Vendor(ep.device.vendor), handle_from_device(ep.device))
                for ep in service.endpoints
            ]
        settings = get_settings()
        observed: dict[str, Any] = {}
        for hostname, vendor, handle in targets:
            adapter = get_adapter(vendor, settings)
            try:
                observed[hostname] = await with_heartbeat(
                    adapter.get_device_state(handle), interval=_HEARTBEAT_INTERVAL
                )
            except AdapterError as exc:
                raise map_adapter_error(exc) from exc
        async with transaction() as session:
            services = ServiceRepository(session)
            await services.set_observed_state(ref.service_id, observed)
            await AllocationRepository(session).confirm(ref.service_id)
            service = await services.transition(
                ref.service_id,
                LifecycleStatus.ACTIVE,
                reason="service verified and committed",
                correlation_id=ref.correlation_id,
                workflow_id=_wf_id(),
                actor="workflow",
            )
            status = str(service.lifecycle_status)
    except Exception as exc:
        raise map_domain_error(exc) from exc
    log.info("service_committed", status=status, devices=list(observed))
    return status


__all__ = ["commit_service_state", "validate_service_connectivity", "verify_device"]
