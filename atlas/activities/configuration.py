"""Configuration generation and pre-deployment validation activities."""

from __future__ import annotations

from temporalio import activity

from atlas.activities._support import get_logger, map_adapter_error, map_domain_error, non_retryable
from atlas.adapters.base import AdapterError, ConfigurationRejectedError
from atlas.adapters.registry import get_adapter, handle_from_device
from atlas.core.config import get_settings
from atlas.core.enums import (
    AuditEventType,
    DeviceOperationStatus,
    DeviceOperationType,
    ResourceType,
    RoutingProtocol,
    Vendor,
)
from atlas.db.models import NetworkService
from atlas.db.repositories import (
    AuditRepository,
    DeviceOperationRepository,
    DeviceRepository,
    ServiceRepository,
)
from atlas.db.session import transaction
from atlas.schemas import (
    AllocatedResources,
    DeviceConfig,
    EndpointResources,
    ServiceRef,
    ValidationCheck,
    ValidationReport,
)
from atlas.services.config_generator import checksum as compute_checksum
from atlas.services.config_generator import generate_device_configs


def _wf_id() -> str | None:
    return activity.info().workflow_id if activity.in_activity() else None


@activity.defn(name="generate_configs")
async def generate_configs(resources: AllocatedResources) -> list[DeviceConfig]:
    """Render vendor-specific config per endpoint and persist text + checksum on the endpoint row."""
    log = get_logger(service_id=resources.service_id)
    try:
        configs = generate_device_configs(resources)
    except Exception as exc:
        raise non_retryable(f"config generation failed: {exc}", error_type="ValidationError") from exc
    try:
        async with transaction() as session:
            services = ServiceRepository(session)
            for cfg in configs:
                await services.update_endpoint(
                    cfg.endpoint_id, rendered_config=cfg.config_text, config_checksum=cfg.checksum
                )
            await AuditRepository(session).record(
                AuditEventType.CONFIG_GENERATED,
                service_id=resources.service_id,
                workflow_id=_wf_id(),
                message=f"generated {len(configs)} device configurations",
                details={
                    "devices": [
                        {"hostname": c.device_hostname, "vendor": str(c.vendor), "checksum": c.checksum}
                        for c in configs
                    ]
                },
            )
    except Exception as exc:
        raise map_domain_error(exc) from exc
    log.info("configs_generated", count=len(configs))
    return configs


@activity.defn(name="pre_deployment_validation")
async def pre_deployment_validation(configs: list[DeviceConfig]) -> ValidationReport:
    """Checksum integrity + adapter dry-run validation for every device, before anything is pushed."""
    if not configs:
        raise non_retryable("no device configurations to validate", error_type="ValidationError")
    service_id = configs[0].service_id
    log = get_logger(service_id=service_id)
    checks: list[ValidationCheck] = []
    settings = get_settings()
    try:
        async with transaction() as session:
            devices = DeviceRepository(session)
            for cfg in sorted(configs, key=lambda c: c.sequence):
                checksum_ok = compute_checksum(cfg.config_text) == cfg.checksum
                checks.append(
                    ValidationCheck(
                        name="checksum",
                        device_hostname=cfg.device_hostname,
                        passed=checksum_ok,
                        detail=None if checksum_ok else "rendered config checksum mismatch",
                    )
                )
                device = await devices.get(cfg.device_id)
                if device is None:
                    checks.append(
                        ValidationCheck(
                            name="device_exists",
                            device_hostname=cfg.device_hostname,
                            passed=False,
                            detail="not in inventory",
                        )
                    )
                    continue
                adapter = get_adapter(Vendor(cfg.vendor), settings)
                handle = handle_from_device(device)
                try:
                    await adapter.validate_configuration(handle, cfg.config_text, cfg.intent)
                    checks.append(
                        ValidationCheck(
                            name="adapter_dry_run", device_hostname=cfg.device_hostname, passed=True
                        )
                    )
                except ConfigurationRejectedError as exc:
                    checks.append(
                        ValidationCheck(
                            name="adapter_dry_run",
                            device_hostname=cfg.device_hostname,
                            passed=False,
                            detail=str(exc),
                        )
                    )
                except AdapterError as exc:
                    # Unreachable/timeout during dry-run: let the retry policy handle it.
                    raise map_adapter_error(exc) from exc
            report = ValidationReport(
                service_id=service_id, passed=all(c.passed for c in checks), checks=checks
            )
            await AuditRepository(session).record(
                AuditEventType.VALIDATION_RESULT,
                service_id=service_id,
                workflow_id=_wf_id(),
                message="pre-deployment validation " + ("passed" if report.passed else "failed"),
                details={"failed": [c.model_dump() for c in checks if not c.passed]},
            )
    except Exception as exc:
        raise map_domain_error(exc) from exc
    if not report.passed:
        failed = [f"{c.name}({c.device_hostname}): {c.detail}" for c in checks if not c.passed]
        log.warning("pre_deployment_validation_failed", failures=failed)
        raise non_retryable(
            "pre-deployment validation failed: " + "; ".join(failed),
            error_type="ValidationError",
            details=report.model_dump(mode="json"),
        )
    log.info("pre_deployment_validation_passed", devices=len(configs))
    return report


def _resources_from_service(service: NetworkService, pe_asn_default: int) -> AllocatedResources:
    """Rebuild ``AllocatedResources`` from persisted inventory rows (used by decommission/reconcile)."""
    rt = next(
        (a.resource_value for a in service.allocations if str(a.resource_type) == ResourceType.ROUTE_TARGET),
        None,
    )
    rt = rt or str((service.desired_state or {}).get("route_target") or "")
    endpoints: list[EndpointResources] = []
    for ep in sorted(service.endpoints, key=lambda e: e.sequence):
        device = ep.device
        endpoints.append(
            EndpointResources(
                endpoint_id=ep.id,
                device_id=ep.device_id,
                device_hostname=device.hostname,
                vendor=Vendor(device.vendor),
                interface=ep.interface,
                vlan_id=ep.vlan_id,
                vrf_name=ep.vrf_name,
                route_distinguisher=ep.route_distinguisher or "",
                route_target_import=[rt] if rt else [],
                route_target_export=[rt] if rt else [],
                pe_ip_address=ep.pe_ip_address or "",
                ce_ip_address=ep.ce_ip_address or "",
                routing_protocol=RoutingProtocol(ep.routing_protocol),
                pe_asn=device.bgp_asn or pe_asn_default,
                ce_asn=ep.ce_asn,
            )
        )
    return AllocatedResources(
        service_id=service.id,
        vrf_name=service.endpoints[0].vrf_name if service.endpoints else "",
        route_target=rt,
        endpoints=endpoints,
    )


@activity.defn(name="load_decommission_plan")
async def load_decommission_plan(ref: ServiceRef) -> list[DeviceConfig]:
    """``DeviceConfig`` for every endpoint that has a SUCCEEDED (not yet compensated) APPLY in the
    ledger — i.e. the devices that really carry this service's configuration. Ordered by sequence;
    the decommission workflow walks it in reverse."""
    log = get_logger(service_id=ref.service_id, correlation_id=ref.correlation_id)
    try:
        async with transaction() as session:
            service = await ServiceRepository(session).get(ref.service_id)
            if service is None:
                raise LookupError(f"service {ref.service_id} not found")
            resources = _resources_from_service(service, get_settings().bgp_asn)
            configs = generate_device_configs(resources) if resources.endpoints else []
            applied_devices = {
                op.device_id
                for op in await DeviceOperationRepository(session).list_for_service(
                    ref.service_id, status=DeviceOperationStatus.SUCCEEDED
                )
                if str(op.operation_type) == DeviceOperationType.APPLY
            }
            plan = [c for c in configs if c.device_id in applied_devices]
    except Exception as exc:
        raise map_domain_error(exc) from exc
    log.info("decommission_plan_loaded", devices=[c.device_hostname for c in plan])
    return plan


__all__ = ["generate_configs", "load_decommission_plan", "pre_deployment_validation"]
