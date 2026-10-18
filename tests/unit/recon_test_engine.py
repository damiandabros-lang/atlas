"""Unit tests for ``reconcile_service`` using a fake adapter and mocked repositories."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest

from atlas.adapters.base import DeviceUnreachableError, VerifyResult
from atlas.core.config import Settings
from atlas.core.enums import AuditEventType, DriftStatus, ReconciliationMode, RoutingProtocol, Vendor
from atlas.observability import metrics
from atlas.reconciliation import engine


class FakeAdapter:
    vendor = Vendor.MOCK

    def __init__(self, observed_by_host: dict[str, Any], unreachable: set[str] | None = None) -> None:
        self.observed_by_host = observed_by_host
        self.unreachable = unreachable or set()
        self.calls: list[tuple[str, dict]] = []

    async def verify_configuration(self, device, intent):
        self.calls.append((device.hostname, intent))
        if device.hostname in self.unreachable:
            raise DeviceUnreachableError("no route to host", device=device.hostname)
        observed = self.observed_by_host.get(device.hostname, {})
        return VerifyResult(passed=bool(observed), observed=observed)


def _device(hostname: str) -> SimpleNamespace:
    return SimpleNamespace(
        id=uuid4(),
        hostname=hostname,
        management_ip="192.0.2.1",
        vendor=Vendor.MOCK,
        platform="mock",
        bgp_asn=65000,
        attributes={},
    )


def _endpoint(hostname: str, sequence: int, rd: str) -> SimpleNamespace:
    device = _device(hostname)
    return SimpleNamespace(
        id=uuid4(),
        device_id=device.id,
        device=device,
        sequence=sequence,
        interface=f"GigabitEthernet0/0/0/{sequence}",
        vlan_id=100,
        vrf_name="ACME-PROD",
        route_distinguisher=rd,
        ce_ip_address="10.255.0.2",
        pe_ip_address="10.255.0.1",
        routing_protocol=RoutingProtocol.STATIC,
        ce_asn=None,
    )


def _service(rt: str = "65000:100") -> SimpleNamespace:
    return SimpleNamespace(
        id=uuid4(),
        desired_state={"vrf_name": "ACME-PROD", "route_target": rt},
        endpoints=[_endpoint("PE1", 1, "65000:1001"), _endpoint("PE2", 2, "65000:1002")],
    )


def _handle(device):
    from atlas.adapters.base import DeviceHandle

    return DeviceHandle(
        device_id=device.id,
        hostname=device.hostname,
        management_ip=device.management_ip,
        vendor=device.vendor,
    )


@pytest.fixture
def repos():
    metrics.reset_registry_for_tests()
    svc_repo = SimpleNamespace(set_observed_state=AsyncMock())
    audit_repo = SimpleNamespace(record=AsyncMock())
    with (
        patch.object(engine, "ServiceRepository", return_value=svc_repo),
        patch.object(engine, "AuditRepository", return_value=audit_repo),
        patch.object(engine, "handle_from_device", _handle),
    ):
        yield svc_repo, audit_repo


def _desired_for(service, endpoint) -> dict:
    """Exactly what the engine will compute as desired intent (config_generator when available)."""
    return engine._build_desired_intent(endpoint, service, Settings())


async def _run(service, adapter, mode=ReconciliationMode.DETECT_ONLY):
    with patch.object(engine, "get_adapter", lambda vendor, settings=None: adapter):
        return await engine.reconcile_service(AsyncMock(), service, mode, settings=Settings())


async def test_in_sync(repos) -> None:
    svc_repo, audit_repo = repos
    service = _service()
    observed = {ep.device.hostname: _desired_for(service, ep) for ep in service.endpoints}
    adapter = FakeAdapter(observed)

    report = await _run(service, adapter)

    assert report.in_sync is True
    assert [d.status for d in report.devices] == [DriftStatus.IN_SYNC, DriftStatus.IN_SYNC]
    assert [h for h, _ in adapter.calls] == ["PE1", "PE2"]
    svc_repo.set_observed_state.assert_awaited_once()
    sid, persisted = svc_repo.set_observed_state.await_args.args
    assert sid == service.id and set(persisted) == {"PE1", "PE2"}
    assert audit_repo.record.await_count == 1
    assert audit_repo.record.await_args.args[0] == AuditEventType.RECONCILIATION_RUN
    assert metrics.RECONCILIATION_DRIFT_TOTAL.labels(status="IN_SYNC")._value.get() == 2


async def test_drift_detected(repos) -> None:
    _, audit_repo = repos
    service = _service()
    observed = {ep.device.hostname: _desired_for(service, ep) for ep in service.endpoints}
    observed["PE2"]["vrf"]["rt_import"] = ["65000:200"]
    observed["PE2"]["vrf"]["rt_export"] = ["65000:200"]

    report = await _run(service, FakeAdapter(observed))

    assert report.in_sync is False
    pe2 = next(d for d in report.devices if d.device == "PE2")
    assert pe2.status == DriftStatus.DRIFT_DETECTED
    assert pe2.differences[0].model_dump() == {
        "field": "route_target",
        "desired": "65000:100",
        "observed": "65000:200",
    }
    event_types = [c.args[0] for c in audit_repo.record.await_args_list]
    assert event_types == [AuditEventType.RECONCILIATION_RUN, AuditEventType.DRIFT_DETECTED]
    drift_call = audit_repo.record.await_args_list[1]
    assert drift_call.kwargs["device_id"] == service.endpoints[1].device_id
    assert metrics.RECONCILIATION_DRIFT_TOTAL.labels(status="DRIFT_DETECTED")._value.get() == 1


async def test_device_unreachable(repos) -> None:
    service = _service()
    observed = {ep.device.hostname: _desired_for(service, ep) for ep in service.endpoints}
    report = await _run(service, FakeAdapter(observed, unreachable={"PE1"}))

    statuses = {d.device: d.status for d in report.devices}
    assert statuses == {"PE1": DriftStatus.DEVICE_UNREACHABLE, "PE2": DriftStatus.IN_SYNC}
    assert report.in_sync is False
    assert metrics.RECONCILIATION_DRIFT_TOTAL.labels(status="DEVICE_UNREACHABLE")._value.get() == 1


async def test_not_provisioned_when_vrf_absent(repos) -> None:
    service = _service()
    observed = {"PE1": {}, "PE2": {"interface": {"name": "GigabitEthernet0/0/0/2"}}}
    report = await _run(service, FakeAdapter(observed))
    assert all(d.status == DriftStatus.NOT_PROVISIONED for d in report.devices)
    assert all(d.differences == [] for d in report.devices)


async def test_auto_remediate_is_refused(repos) -> None:
    svc_repo, _ = repos
    with pytest.raises(PermissionError):
        await _run(_service(), FakeAdapter({}), mode=ReconciliationMode.AUTO_REMEDIATE)
    svc_repo.set_observed_state.assert_not_awaited()


def test_endpoint_intent_shape() -> None:
    service = _service()
    intent = engine.endpoint_intent(service.endpoints[0], service, Settings())
    assert intent["vrf"] == {
        "name": "ACME-PROD",
        "rd": "65000:1001",
        "rt_import": ["65000:100"],
        "rt_export": ["65000:100"],
    }
    assert intent["interface"]["name"] == "GigabitEthernet0/0/0/1"
    assert intent["interface"]["ip"] == "10.255.0.1"
    assert "bgp" not in intent
