"""Reconciliation engine — compares desired service intent with observed device state.

``diff_intent`` is a pure function over the vendor-neutral intent shape documented in
``docs/INTERFACES.md``; ``reconcile_service`` drives it across every endpoint of a service,
persists the observed state and audit trail, and emits metrics.

MVP policy: ``DETECT_ONLY``. ``AUTO_REMEDIATE`` is refused with ``PermissionError`` until an
explicit authorisation mechanism exists — the engine never modifies device configuration.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any

from atlas.adapters.base import AdapterError, DeviceHandle, DeviceUnreachableError, NetworkAdapter
from atlas.core.config import Settings, get_settings
from atlas.core.enums import AuditEventType, DriftStatus, ReconciliationMode
from atlas.db.models import NetworkService, ServiceEndpoint
from atlas.db.repositories import AuditRepository, ServiceRepository
from atlas.observability.logging import get_logger
from atlas.observability.metrics import record_drift
from atlas.schemas import DeviceDrift, DriftDifference, ReconciliationReport

log = get_logger(component="reconciliation")

# Resolved lazily so tests (and the engine itself) do not import the adapter registry at module
# import time; tests may patch these names directly.
get_adapter: Callable[..., NetworkAdapter] | None = None
handle_from_device: Callable[[Any], DeviceHandle] | None = None

# Intent paths compared as unordered sets.
SET_FIELDS: frozenset[str] = frozenset({"vrf.rt_import", "vrf.rt_export"})

# Dotted intent path -> operator-facing field name used in drift reports.
FIELD_SUMMARY: dict[str, str] = {
    "vrf.rt_import": "route_target_import",
    "vrf.rt_export": "route_target_export",
    "vrf.rd": "route_distinguisher",
    "vrf.name": "vrf_name",
    "interface.ip": "pe_ip_address",
    "interface.name": "interface",
    "interface.vlan": "vlan_id",
    "bgp.neighbor": "ce_ip_address",
    "bgp.remote_asn": "ce_asn",
}


def summarize_field(path: str) -> str:
    """Map a dotted intent path to a stable, operator-facing field name (unknown paths pass through)."""
    return FIELD_SUMMARY.get(path, path)


# ---------------------------------------------------------------------------
# Pure diff
# ---------------------------------------------------------------------------


def _flatten(value: Any, prefix: str = "") -> dict[str, Any]:
    """Flatten nested mappings into ``{"a.b.c": leaf}``; lists are leaves."""
    out: dict[str, Any] = {}
    if isinstance(value, Mapping):
        for key, sub in value.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            if isinstance(sub, Mapping) and sub:
                out.update(_flatten(sub, path))
            else:
                out[path] = sub
    return out


def _lookup(observed: Mapping[str, Any], path: str) -> tuple[bool, Any]:
    cur: Any = observed
    for part in path.split("."):
        if isinstance(cur, Mapping) and part in cur:
            cur = cur[part]
        else:
            return False, None
    return True, cur


def _as_set(value: Any) -> set[str] | None:
    if value is None:
        return None
    if isinstance(value, str | int):
        return {str(value)}
    if isinstance(value, list | tuple | set | frozenset):
        return {str(v) for v in value}
    return None


def _normalise_list(value: Any) -> Any:
    """Canonical representation for report output: sorted list, or the scalar for single element."""
    s = _as_set(value)
    if s is None:
        return value
    items = sorted(s)
    return items[0] if len(items) == 1 else items


def _values_equal(path: str, desired: Any, observed: Any) -> bool:
    if path in SET_FIELDS or (isinstance(desired, list) and isinstance(observed, list)):
        ds, os_ = _as_set(desired), _as_set(observed)
        if ds is not None and os_ is not None:
            return ds == os_
    if isinstance(desired, bool) or isinstance(observed, bool):
        return desired == observed
    if isinstance(desired, int | str) and isinstance(observed, int | str):
        return str(desired) == str(observed)
    return desired == observed


def diff_intent(desired: Mapping[str, Any], observed: Mapping[str, Any] | None) -> list[DriftDifference]:
    """Return every leaf of ``desired`` whose value differs from ``observed``.

    * dotted paths (``vrf.rd``, ``interface.vlan``) summarised via :func:`summarize_field`
    * ``vrf.rt_import`` / ``vrf.rt_export`` compared as unordered sets
    * a path missing from ``observed`` yields ``observed=None``
    * identical drift on both import and export RTs collapses into a single ``route_target`` entry
    * keys present only in ``observed`` are ignored (operational extras are not drift)
    """
    observed = observed or {}
    raw: dict[str, tuple[Any, Any]] = {}
    for path, want in _flatten(desired).items():
        present, got = _lookup(observed, path)
        if not present:
            got = None
        if present and _values_equal(path, want, got):
            continue
        if not present and want is None:
            continue
        raw[path] = (want, got)

    diffs: list[DriftDifference] = []
    rt_imp, rt_exp = raw.get("vrf.rt_import"), raw.get("vrf.rt_export")
    if (
        rt_imp is not None
        and rt_exp is not None
        and (_as_set(rt_imp[0]) == _as_set(rt_exp[0]) and _as_set(rt_imp[1]) == _as_set(rt_exp[1]))
    ):
        diffs.append(
            DriftDifference(
                field="route_target",
                desired=_normalise_list(rt_imp[0]),
                observed=_normalise_list(rt_imp[1]),
            )
        )
        raw.pop("vrf.rt_import")
        raw.pop("vrf.rt_export")

    for path, (want, got) in raw.items():
        if path in SET_FIELDS:
            want, got = _normalise_list(want), _normalise_list(got)
        diffs.append(DriftDifference(field=summarize_field(path), desired=want, observed=got))
    return diffs


# ---------------------------------------------------------------------------
# Intent construction
# ---------------------------------------------------------------------------


def endpoint_intent(
    endpoint: ServiceEndpoint, service: NetworkService, settings: Settings | None = None
) -> dict:
    """Vendor-neutral desired intent for one endpoint built from persisted inventory fields.

    Used when ``atlas.services.config_generator.build_intent`` is unavailable or the endpoint
    lacks the allocated resources it requires.
    """
    settings = settings or get_settings()
    desired_state: dict[str, Any] = service.desired_state or {}
    rt = desired_state.get("route_target")
    rts = [rt] if rt else []
    vrf = endpoint.vrf_name or desired_state.get("vrf_name")
    device = getattr(endpoint, "device", None)
    pe_asn = (
        (getattr(device, "bgp_asn", None) or settings.bgp_asn) if device is not None else settings.bgp_asn
    )

    intent: dict[str, Any] = {
        "vrf": {"name": vrf, "rd": endpoint.route_distinguisher, "rt_import": rts, "rt_export": rts},
        "interface": {
            "name": endpoint.interface,
            "vlan": endpoint.vlan_id,
            "ip": endpoint.pe_ip_address,
            "prefix_length": 30,
            "vrf": vrf,
            "admin_up": True,
        },
    }
    proto = str(getattr(endpoint.routing_protocol, "value", endpoint.routing_protocol) or "STATIC")
    if proto == "BGP":
        intent["bgp"] = {
            "asn": pe_asn,
            "vrf": vrf,
            "neighbor": endpoint.ce_ip_address,
            "remote_asn": endpoint.ce_asn,
        }
    return intent


def _build_desired_intent(endpoint: ServiceEndpoint, service: NetworkService, settings: Settings) -> dict:
    try:
        from atlas.schemas import EndpointResources
        from atlas.services.config_generator import build_intent  # type: ignore[import-not-found]

        desired_state: dict[str, Any] = service.desired_state or {}
        rt = desired_state.get("route_target")
        device = endpoint.device
        if not (rt and endpoint.route_distinguisher and endpoint.pe_ip_address and endpoint.ce_ip_address):
            raise ValueError("endpoint lacks allocated resources")
        resources = EndpointResources(
            endpoint_id=endpoint.id,
            device_id=endpoint.device_id,
            device_hostname=device.hostname,
            vendor=device.vendor,
            interface=endpoint.interface,
            vlan_id=endpoint.vlan_id,
            vrf_name=endpoint.vrf_name,
            route_distinguisher=endpoint.route_distinguisher,
            route_target_import=[rt],
            route_target_export=[rt],
            pe_ip_address=endpoint.pe_ip_address,
            ce_ip_address=endpoint.ce_ip_address,
            routing_protocol=endpoint.routing_protocol,
            pe_asn=device.bgp_asn or settings.bgp_asn,
            ce_asn=endpoint.ce_asn,
        )
        return dict(build_intent(resources))
    except Exception:
        return endpoint_intent(endpoint, service, settings)


def _resolve_registry() -> tuple[Callable[..., NetworkAdapter], Callable[[Any], DeviceHandle]]:
    ga, hfd = get_adapter, handle_from_device
    if ga is None or hfd is None:
        from atlas.adapters import registry as _registry

        ga = ga or _registry.get_adapter
        hfd = hfd or _registry.handle_from_device
    return ga, hfd


def _is_unprovisioned(observed: Mapping[str, Any] | None) -> bool:
    if not observed:
        return True
    vrf = observed.get("vrf")
    if not isinstance(vrf, Mapping) or not vrf.get("name"):
        return True
    return False


# ---------------------------------------------------------------------------
# Service-level reconciliation
# ---------------------------------------------------------------------------


def build_report(
    service_id: Any, mode: ReconciliationMode, devices: list[DeviceDrift]
) -> ReconciliationReport:
    return ReconciliationReport(
        service_id=service_id,
        mode=mode,
        in_sync=all(d.status == DriftStatus.IN_SYNC for d in devices),
        devices=devices,
        checked_at=datetime.now(UTC),
    )


async def reconcile_endpoint(
    endpoint: ServiceEndpoint,
    service: NetworkService,
    *,
    settings: Settings,
) -> tuple[DeviceDrift, dict[str, Any]]:
    """Reconcile one endpoint. Returns ``(drift, observed_state)``; never raises adapter errors."""
    ga, hfd = _resolve_registry()
    device = endpoint.device
    hostname = device.hostname
    desired = _build_desired_intent(endpoint, service, settings)
    observed: dict[str, Any] = {}

    try:
        adapter = ga(device.vendor, settings)
        handle = hfd(device)
        result = await adapter.verify_configuration(handle, desired)
        observed = dict(result.observed or {})
    except DeviceUnreachableError as exc:
        log.warning("reconcile.device_unreachable", device=hostname, error=str(exc))
        return DeviceDrift(service_id=service.id, device=hostname, status=DriftStatus.DEVICE_UNREACHABLE), {}
    except AdapterError as exc:
        # Timeouts and other transport-level failures: we could not observe the device.
        log.warning("reconcile.adapter_error", device=hostname, error=str(exc), kind=exc.failure_kind)
        return DeviceDrift(service_id=service.id, device=hostname, status=DriftStatus.DEVICE_UNREACHABLE), {}

    if _is_unprovisioned(observed):
        return DeviceDrift(
            service_id=service.id, device=hostname, status=DriftStatus.NOT_PROVISIONED
        ), observed

    differences = diff_intent(desired, observed)
    status = DriftStatus.DRIFT_DETECTED if differences else DriftStatus.IN_SYNC
    return DeviceDrift(
        service_id=service.id, device=hostname, status=status, differences=differences
    ), observed


async def reconcile_service(
    session: Any,
    service: NetworkService,
    mode: ReconciliationMode | str = ReconciliationMode.DETECT_ONLY,
    *,
    settings: Settings | None = None,
    correlation_id: str | None = None,
    workflow_id: str | None = None,
) -> ReconciliationReport:
    """Compare desired vs observed state for every endpoint of ``service``.

    Side effects: ``network_services.observed_state`` is replaced with ``{hostname: observed}``,
    a ``RECONCILIATION_RUN`` audit event is recorded (plus one ``DRIFT_DETECTED`` per drifted
    device) and ``atlas_reconciliation_drift_total`` is incremented per device.
    """
    mode = ReconciliationMode(mode)
    settings = settings or get_settings()
    if mode == ReconciliationMode.AUTO_REMEDIATE:
        raise PermissionError(
            "AUTO_REMEDIATE is not authorised in this release; reconciliation runs in DETECT_ONLY mode "
            "and never modifies device configuration"
        )

    endpoints = sorted(list(service.endpoints or []), key=lambda e: (getattr(e, "sequence", 0), str(e.id)))
    devices: list[DeviceDrift] = []
    observed_by_device: dict[str, Any] = {}

    for endpoint in endpoints:
        drift, observed = await reconcile_endpoint(endpoint, service, settings=settings)
        devices.append(drift)
        observed_by_device[drift.device] = observed
        record_drift(drift.status)
        log.info(
            "reconcile.device",
            service_id=str(service.id),
            device=drift.device,
            status=drift.status,
            differences=len(drift.differences),
        )

    report = build_report(service.id, mode, devices)

    services = ServiceRepository(session)
    audit = AuditRepository(session)
    await services.set_observed_state(service.id, observed_by_device)
    await audit.record(
        AuditEventType.RECONCILIATION_RUN,
        service_id=service.id,
        workflow_id=workflow_id,
        correlation_id=correlation_id,
        message=f"reconciliation ({mode.value}) finished: {'in sync' if report.in_sync else 'discrepancies found'}",
        details={
            "mode": mode.value,
            "in_sync": report.in_sync,
            "devices": {d.device: d.status.value for d in devices},
        },
    )
    device_ids = {ep.device.hostname: ep.device_id for ep in endpoints}
    for drift in devices:
        if drift.status == DriftStatus.DRIFT_DETECTED:
            await audit.record(
                AuditEventType.DRIFT_DETECTED,
                service_id=service.id,
                workflow_id=workflow_id,
                correlation_id=correlation_id,
                device_id=device_ids.get(drift.device),
                message=f"configuration drift detected on {drift.device}",
                details={"differences": [d.model_dump(mode="json") for d in drift.differences]},
            )

    log.info(
        "reconcile.service",
        service_id=str(service.id),
        mode=mode.value,
        in_sync=report.in_sync,
        devices=len(devices),
    )
    return report


__all__ = [
    "FIELD_SUMMARY",
    "SET_FIELDS",
    "build_report",
    "diff_intent",
    "endpoint_intent",
    "reconcile_endpoint",
    "reconcile_service",
    "summarize_field",
]
