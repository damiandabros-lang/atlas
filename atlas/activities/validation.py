"""Request validation activity — business/technical constraints checked against inventory."""

from __future__ import annotations

import re
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from temporalio import activity

from atlas.activities._support import get_logger, map_domain_error, non_retryable
from atlas.core.enums import AuditEventType, DeviceOperationalStatus, DeviceRole, EndpointStatus, Vendor
from atlas.db.models import ServiceEndpoint
from atlas.db.repositories import AuditRepository, CustomerRepository, DeviceRepository, ServiceRepository
from atlas.db.session import transaction
from atlas.schemas import ProvisionRequest, ValidationCheck, ValidationReport

_RT_RE = re.compile(r"^\d{1,10}:\d{1,10}$")
_SUPPORTED_VENDORS = {Vendor.MOCK, Vendor.CISCO_IOSXR, Vendor.JUNIPER_JUNOS}


async def _find_conflicting_endpoint(
    session: AsyncSession, device_id: UUID, interface: str, vlan: int | None, service_id: UUID
) -> ServiceEndpoint | None:
    """Another non-decommissioned service already attached on this device/interface/vlan."""
    stmt = select(ServiceEndpoint).where(
        ServiceEndpoint.device_id == device_id,
        ServiceEndpoint.interface == interface,
        ServiceEndpoint.vlan_id == vlan,
        ServiceEndpoint.service_id != service_id,
        ServiceEndpoint.status != EndpointStatus.DECOMMISSIONED,
    )
    return (await session.execute(stmt)).scalars().first()


@activity.defn(name="validate_request")
async def validate_request(req: ProvisionRequest) -> ValidationReport:
    """Validate customer, devices, attachment circuits and RT format.

    Any failed check raises a non-retryable ``ApplicationError(type="ValidationError")`` with the
    report as detail; the workflow moves the service to FAILED. Infra errors stay retryable.
    """
    log = get_logger(service_id=req.service_id, correlation_id=req.correlation_id)
    request = req.request
    checks: list[ValidationCheck] = []
    try:
        async with transaction() as session:
            customer = await CustomerRepository(session).get(request.customer_id)
            checks.append(
                ValidationCheck(
                    name="customer_exists",
                    passed=customer is not None and bool(customer.is_active),
                    detail=None
                    if customer is not None and customer.is_active
                    else f"customer {request.customer_id} missing or inactive",
                )
            )

            devices = await DeviceRepository(session).get_many_by_hostnames(
                [e.device for e in request.endpoints]
            )
            for ep in request.endpoints:
                device = devices.get(ep.device.upper())
                if device is None:
                    checks.append(
                        ValidationCheck(
                            name="device_exists",
                            device_hostname=ep.device,
                            passed=False,
                            detail="not in inventory",
                        )
                    )
                    continue
                checks.append(ValidationCheck(name="device_exists", device_hostname=ep.device, passed=True))
                checks.append(
                    ValidationCheck(
                        name="device_role_pe",
                        device_hostname=ep.device,
                        passed=str(device.role) == DeviceRole.PE,
                        detail=None if str(device.role) == DeviceRole.PE else f"role {device.role} is not PE",
                    )
                )
                checks.append(
                    ValidationCheck(
                        name="device_up",
                        device_hostname=ep.device,
                        passed=str(device.operational_status) == DeviceOperationalStatus.UP,
                        detail=None
                        if str(device.operational_status) == DeviceOperationalStatus.UP
                        else f"status {device.operational_status}",
                    )
                )
                vendor_ok = str(device.vendor) in {v.value for v in _SUPPORTED_VENDORS}
                checks.append(
                    ValidationCheck(
                        name="vendor_supported",
                        device_hostname=ep.device,
                        passed=vendor_ok,
                        detail=None if vendor_ok else f"vendor {device.vendor} unsupported",
                    )
                )
                # Attachment circuit must not be used by another live service.
                conflict = await _find_conflicting_endpoint(
                    session, device.id, ep.interface, ep.vlan, req.service_id
                )
                checks.append(
                    ValidationCheck(
                        name="attachment_free",
                        device_hostname=ep.device,
                        passed=conflict is None,
                        detail=None
                        if conflict is None
                        else f"{ep.interface} vlan {ep.vlan} used by service {conflict.service_id}",
                    )
                )

            rt_ok = request.route_target is None or bool(_RT_RE.match(request.route_target))
            checks.append(
                ValidationCheck(
                    name="route_target_format", passed=rt_ok, detail=None if rt_ok else "expected ASN:NN"
                )
            )

            report = ValidationReport(
                service_id=req.service_id, passed=all(c.passed for c in checks), checks=checks
            )
            await AuditRepository(session).record(
                AuditEventType.VALIDATION_RESULT,
                service_id=req.service_id,
                correlation_id=req.correlation_id,
                workflow_id=activity.info().workflow_id if activity.in_activity() else None,
                message="request validation " + ("passed" if report.passed else "failed"),
                details={"checks": [c.model_dump() for c in checks if not c.passed] or "all passed"},
            )
            if await ServiceRepository(session).get(req.service_id, with_relations=False) is None:
                raise LookupError(f"service {req.service_id} not found")
    except Exception as exc:
        raise map_domain_error(exc) from exc

    if not report.passed:
        failed = [f"{c.name}({c.device_hostname or '-'}): {c.detail}" for c in checks if not c.passed]
        log.warning("validation_failed", failures=failed)
        raise non_retryable(
            "request validation failed: " + "; ".join(failed),
            error_type="ValidationError",
            details=report.model_dump(mode="json"),
        )
    log.info("validation_passed", checks=len(checks))
    return report


__all__ = ["validate_request"]
