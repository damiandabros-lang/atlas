"""MockNetworkAdapter: state roundtrip, idempotency, rollback isolation, failure injection."""

from __future__ import annotations

from uuid import uuid4

import pytest

from atlas.adapters.base import (
    ConfigurationRejectedError,
    DeviceHandle,
    DeviceTimeoutError,
    DeviceUnreachableError,
    NetworkAdapter,
    PartialApplyError,
)
from atlas.adapters.mock import MockNetworkAdapter
from atlas.core.config import Settings
from atlas.core.enums import FailureMode, RoutingProtocol, Vendor
from atlas.schemas import DeviceConfig
from atlas.services.config_generator import build_intent, generate_device_configs
from tests.unit.configgen_test_render import make_endpoint, make_resources


@pytest.fixture
def settings() -> Settings:
    return Settings(mock_latency_ms=0, mock_failure_mode=FailureMode.NONE, _env_file=None)


@pytest.fixture
def adapter(settings) -> MockNetworkAdapter:
    MockNetworkAdapter.reset()
    yield MockNetworkAdapter(settings)
    MockNetworkAdapter.reset()


@pytest.fixture
def configs() -> list[DeviceConfig]:
    return generate_device_configs(make_resources())


def handle(cfg: DeviceConfig) -> DeviceHandle:
    return DeviceHandle(
        device_id=cfg.device_id, hostname=cfg.device_hostname, management_ip="192.0.2.1", vendor=cfg.vendor
    )


async def apply(adapter, cfg: DeviceConfig, key: str | None = None):
    return await adapter.apply_configuration(
        handle(cfg), cfg.config_text, cfg.intent, idempotency_key=key or f"k-{cfg.device_hostname}"
    )


def test_protocol_conformance(adapter):
    assert isinstance(adapter, NetworkAdapter)
    assert adapter.vendor == Vendor.MOCK


async def test_apply_verify_get_state_roundtrip(adapter, configs):
    cfg = configs[0]
    res = await apply(adapter, cfg)
    assert not res.already_applied and res.operation_id.startswith("mock-op-")

    verify = await adapter.verify_configuration(handle(cfg), cfg.intent)
    assert verify.passed, verify.checks
    assert verify.observed["vrf"] == cfg.intent["vrf"]
    assert verify.observed["interface"] == cfg.intent["interface"]
    assert verify.observed["bgp"]["neighbor"] == "10.255.0.2"
    assert verify.observed["bgp"]["state"] == "Established"

    state = await adapter.get_device_state(handle(cfg))
    assert state["vrfs"]["ACME-PROD"]["rd"] == "65000:1001"
    assert state["interfaces"]["GigabitEthernet0/0/0/1.100"]["ip"] == "10.255.0.1"
    assert state["bgp"]["vrf_neighbors"]["ACME-PROD"][0]["remote_asn"] == 65001
    assert res.operation_id in state["applied_operations"]
    # deep copy: mutating the snapshot must not alter device state
    state["vrfs"].clear()
    assert "ACME-PROD" in MockNetworkAdapter.state("PE1")["vrfs"]


async def test_verify_fails_when_not_provisioned(adapter, configs):
    cfg = configs[1]
    verify = await adapter.verify_configuration(handle(cfg), cfg.intent)
    assert not verify.passed
    assert verify.observed["vrf"] is None and verify.observed["interface"] is None
    names = {c["name"]: c["passed"] for c in verify.checks}
    assert names["vrf_present"] is False


async def test_idempotent_apply_same_key(adapter, configs):
    cfg = configs[0]
    first = await apply(adapter, cfg, "same-key")
    second = await apply(adapter, cfg, "same-key")
    assert second.already_applied and second.operation_id == first.operation_id
    assert len(MockNetworkAdapter.state("PE1")["applied_operations"]) == 1


async def test_rollback_removes_only_own_vrf(adapter, configs):
    acme = configs[0]
    other_ep = make_endpoint("PE1", 0)
    other_ep.vrf_name = "BETA-PROD"
    other_ep.vlan_id = 200
    other_ep.route_distinguisher = "65000:2001"
    other_ep.pe_ip_address, other_ep.ce_ip_address = "10.255.1.1", "10.255.1.2"
    other_intent = build_intent(other_ep)

    res_a = await apply(adapter, acme, "acme")
    await adapter.apply_configuration(handle(acme), "cfg", other_intent, idempotency_key="beta")

    rb = await adapter.rollback_configuration(handle(acme), res_a.operation_id, acme.intent)
    assert not rb.already_applied
    state = MockNetworkAdapter.state("PE1")
    assert "ACME-PROD" not in state["vrfs"]
    assert "GigabitEthernet0/0/0/1.100" not in state["interfaces"]
    assert "ACME-PROD" not in state["bgp"]["vrf_neighbors"]
    # the other service's objects survive untouched
    assert state["vrfs"]["BETA-PROD"]["rd"] == "65000:2001"
    assert "GigabitEthernet0/0/0/1.200" in state["interfaces"]
    assert state["bgp"]["vrf_neighbors"]["BETA-PROD"][0]["neighbor"] == "10.255.1.2"
    beta_verify = await adapter.verify_configuration(handle(acme), other_intent)
    assert beta_verify.passed

    # rollback is idempotent: unknown / already rolled back op id
    again = await adapter.rollback_configuration(handle(acme), res_a.operation_id, acme.intent)
    assert again.already_applied


async def test_failure_fail_on_pe3(adapter, configs):
    MockNetworkAdapter.set_failure_mode(FailureMode.FAIL_ON_PE3)
    await apply(adapter, configs[0])
    await apply(adapter, configs[1])
    with pytest.raises(ConfigurationRejectedError) as exc:
        await apply(adapter, configs[2])
    assert exc.value.failure_kind == "PERMANENT"
    assert "ACME-PROD" not in MockNetworkAdapter.state("PE3")["vrfs"]
    assert "ACME-PROD" in MockNetworkAdapter.state("PE2")["vrfs"]


async def test_failure_timeout_on_pe2_transient_then_success(adapter, configs):
    MockNetworkAdapter.set_failure_mode(FailureMode.TIMEOUT_ON_PE2)
    cfg = configs[1]
    for _ in range(2):
        with pytest.raises(DeviceTimeoutError) as exc:
            await apply(adapter, cfg, "retry-key")
        assert exc.value.failure_kind == "TRANSIENT"
    res = await apply(adapter, cfg, "retry-key")
    assert not res.already_applied
    assert "ACME-PROD" in MockNetworkAdapter.state("PE2")["vrfs"]
    # PE1 unaffected by the PE2-specific mode
    await apply(adapter, configs[0])


async def test_failure_bgp_session_down(adapter, configs):
    MockNetworkAdapter.set_failure_mode(FailureMode.BGP_SESSION_DOWN)
    cfg = configs[0]
    await apply(adapter, cfg)
    verify = await adapter.verify_configuration(handle(cfg), cfg.intent)
    assert not verify.passed
    assert verify.observed["bgp"]["state"] == "Idle"
    failed = [c["name"] for c in verify.checks if not c["passed"]]
    assert failed == ["bgp_session_established"]


async def test_failure_device_unreachable(adapter, configs):
    MockNetworkAdapter.set_failure_mode(FailureMode.DEVICE_UNREACHABLE)
    cfg = configs[0]
    with pytest.raises(DeviceUnreachableError):
        await apply(adapter, cfg)
    with pytest.raises(DeviceUnreachableError):
        await adapter.get_device_state(handle(cfg))
    with pytest.raises(DeviceUnreachableError):
        await adapter.verify_configuration(handle(cfg), cfg.intent)
    with pytest.raises(DeviceUnreachableError):
        await adapter.rollback_configuration(handle(cfg), "x", cfg.intent)


async def test_set_reachable_per_device(adapter, configs):
    MockNetworkAdapter.set_reachable("PE1", False)
    with pytest.raises(DeviceUnreachableError):
        await apply(adapter, configs[0])
    await apply(adapter, configs[1])  # PE2 still fine
    MockNetworkAdapter.set_reachable("PE1", True)
    await apply(adapter, configs[0])


async def test_failure_partial_configuration(adapter, configs):
    MockNetworkAdapter.set_failure_mode(FailureMode.PARTIAL_CONFIGURATION)
    cfg = configs[0]
    with pytest.raises(PartialApplyError) as exc:
        await apply(adapter, cfg, "partial")
    assert exc.value.failure_kind == "UNKNOWN"
    state = MockNetworkAdapter.state("PE1")
    assert "ACME-PROD" in state["vrfs"]
    assert "GigabitEthernet0/0/0/1.100" not in state["interfaces"]
    op_id = exc.value.details["operation_id"]
    assert op_id in state["applied_operations"]
    verify = await adapter.verify_configuration(handle(cfg), cfg.intent)
    assert not verify.passed and verify.observed["interface"] is None
    # the recorded operation can be compensated, leaving the device clean
    rb = await adapter.rollback_configuration(handle(cfg), op_id, cfg.intent)
    assert "vrf:ACME-PROD" in rb.details["removed"]
    assert "ACME-PROD" not in MockNetworkAdapter.state("PE1")["vrfs"]


async def test_failure_mode_from_settings(configs):
    MockNetworkAdapter.reset()
    adapter = MockNetworkAdapter(
        Settings(mock_latency_ms=0, mock_failure_mode=FailureMode.FAIL_ON_PE3, _env_file=None)
    )
    with pytest.raises(ConfigurationRejectedError):
        await apply(adapter, configs[2])
    MockNetworkAdapter.set_failure_mode(FailureMode.NONE)  # override wins
    await apply(adapter, configs[2], "k2")
    MockNetworkAdapter.reset()


async def test_inject_drift_visible_in_verify(adapter, configs):
    cfg = configs[0]
    await apply(adapter, cfg)
    MockNetworkAdapter.inject_drift("PE1", "vrfs.ACME-PROD.rt_import", ["65000:200"])
    verify = await adapter.verify_configuration(handle(cfg), cfg.intent)
    assert not verify.passed
    assert verify.observed["vrf"]["rt_import"] == ["65000:200"]
    assert cfg.intent["vrf"]["rt_import"] == ["65000:100"]


async def test_static_endpoint_apply_and_verify(adapter):
    cfg = generate_device_configs(make_resources(protocol=RoutingProtocol.STATIC))[0]
    await apply(adapter, cfg)
    verify = await adapter.verify_configuration(handle(cfg), cfg.intent)
    assert verify.passed
    assert verify.observed["static"] == cfg.intent["static"]
    assert "neighbor" not in verify.observed["bgp"]


async def test_validate_rejects_empty_config(adapter, configs):
    with pytest.raises(ConfigurationRejectedError):
        await adapter.validate_configuration(handle(configs[0]), "   ", configs[0].intent)
    await adapter.validate_configuration(handle(configs[0]), configs[0].config_text, configs[0].intent)


async def test_latency_setting_applied(configs):
    MockNetworkAdapter.reset()
    import time

    adapter = MockNetworkAdapter(Settings(mock_latency_ms=30, _env_file=None))
    t0 = time.perf_counter()
    await adapter.get_device_state(handle(configs[0]))
    assert time.perf_counter() - t0 >= 0.025
    MockNetworkAdapter.reset()


def test_handle_uuid_untouched(configs):
    h = handle(configs[0])
    assert h.device_id == configs[0].device_id and h.device_id != uuid4()
