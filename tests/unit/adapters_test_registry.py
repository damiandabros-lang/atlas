"""Registry, credentials, and NETCONF adapter import/parse tests (no ncclient required)."""

from __future__ import annotations

import json
from types import SimpleNamespace
from uuid import uuid4

import pytest
from pydantic import SecretStr

from atlas.adapters import credentials as creds_mod
from atlas.adapters.base import AdapterError, DeviceHandle
from atlas.adapters.credentials import Credentials, CredentialsError, resolve_credentials
from atlas.adapters.iosxr import CiscoIOSXRAdapter, parse_running_config, wrap_cli_config
from atlas.adapters.junos import JuniperJunosAdapter, parse_configuration_text
from atlas.adapters.mock import MockNetworkAdapter
from atlas.adapters.registry import get_adapter, handle_from_device, reset_adapters
from atlas.core.config import Settings
from atlas.core.enums import Vendor
from atlas.services.config_generator import build_intent, render_config
from tests.unit.configgen_test_render import make_endpoint


@pytest.fixture(autouse=True)
def _clean_registry():
    reset_adapters()
    yield
    reset_adapters()


def _handle(vendor: Vendor = Vendor.CISCO_IOSXR) -> DeviceHandle:
    return DeviceHandle(device_id=uuid4(), hostname="PE1", management_ip="192.0.2.1", vendor=vendor)


def test_registry_mock_backend_for_all_vendors():
    settings = Settings(adapter_backend="mock", _env_file=None)
    for vendor in Vendor:
        adapter = get_adapter(vendor, settings)
        assert isinstance(adapter, MockNetworkAdapter), vendor
    assert get_adapter("cisco_iosxr", settings) is get_adapter(Vendor.CISCO_IOSXR, settings)  # cached


def test_registry_real_backend_maps_vendors():
    settings = Settings(adapter_backend="netconf", _env_file=None)
    assert isinstance(get_adapter(Vendor.CISCO_IOSXR, settings), CiscoIOSXRAdapter)
    assert isinstance(get_adapter(Vendor.JUNIPER_JUNOS, settings), JuniperJunosAdapter)
    assert isinstance(get_adapter(Vendor.MOCK, settings), MockNetworkAdapter)
    reset_adapters()
    assert isinstance(
        get_adapter(Vendor.CISCO_IOSXR, Settings(adapter_backend="mock", _env_file=None)), MockNetworkAdapter
    )


def test_handle_from_device():
    device_id = uuid4()
    device = SimpleNamespace(
        id=device_id,
        hostname="PE1",
        management_ip="10.0.0.1",
        vendor="cisco_iosxr",
        platform="NCS-540",
        attributes={"netconf_port": 2830, "site": "WAW"},
    )
    h = handle_from_device(device)  # type: ignore[arg-type]
    assert h.device_id == device_id and h.vendor == Vendor.CISCO_IOSXR and h.port == 2830
    assert h.attributes["site"] == "WAW" and h.platform == "NCS-540"


def test_credentials_repr_redacts():
    c = Credentials(username="atlas", password=SecretStr("hunter2"))
    assert "hunter2" not in repr(c) and "hunter2" not in str(c)
    assert "atlas" in repr(c)
    assert c.reveal() == "hunter2"


def test_credentials_env_provider():
    settings = Settings(
        device_credentials_ref="env", device_username="u1", device_password=SecretStr("p1"), _env_file=None
    )
    c = resolve_credentials(_handle(), settings)
    assert c.username == "u1" and c.reveal() == "p1"


def test_credentials_k8s_secret_provider(tmp_path):
    (tmp_path / "PE1.json").write_text(json.dumps({"username": "k8s-user", "password": "k8s-pass"}))
    settings = Settings(device_credentials_ref="k8s-secret", device_username="env-user", _env_file=None)
    c = resolve_credentials(_handle(), settings, secret_dir=tmp_path)
    assert c.username == "k8s-user" and c.reveal() == "k8s-pass"
    # missing file -> env fallback
    other = DeviceHandle(device_id=uuid4(), hostname="PE9", management_ip="x", vendor=Vendor.MOCK)
    assert resolve_credentials(other, settings, secret_dir=tmp_path).username == "env-user"
    (tmp_path / "PE2.json").write_text("not-json")
    bad = DeviceHandle(device_id=uuid4(), hostname="PE2", management_ip="x", vendor=Vendor.MOCK)
    with pytest.raises(CredentialsError):
        resolve_credentials(bad, settings, secret_dir=tmp_path)
    assert creds_mod.K8S_SECRET_DIR.as_posix() == "/var/run/secrets/atlas/devices"


def test_credentials_vault_and_unknown():
    with pytest.raises(NotImplementedError):
        resolve_credentials(_handle(), Settings(device_credentials_ref="vault", _env_file=None))
    with pytest.raises(CredentialsError):
        resolve_credentials(_handle(), Settings(device_credentials_ref="pigeon", _env_file=None))


async def test_netconf_adapters_fail_cleanly_without_ncclient(monkeypatch):
    import atlas.adapters.iosxr as iosxr
    import atlas.adapters.junos as junos

    monkeypatch.setattr(iosxr, "manager", None)
    monkeypatch.setattr(junos, "manager", None)
    settings = Settings(_env_file=None)
    with pytest.raises(AdapterError, match="ncclient"):
        await CiscoIOSXRAdapter(settings).get_device_state(_handle())
    with pytest.raises(AdapterError, match="ncclient"):
        await JuniperJunosAdapter(settings).get_device_state(_handle(Vendor.JUNIPER_JUNOS))


def test_iosxr_wrap_and_parse():
    ep = make_endpoint("PE1", 0, vendor=Vendor.CISCO_IOSXR)
    intent = build_intent(ep)
    text = render_config(ep, "svc")
    wrapped = wrap_cli_config("a < b")
    assert "&lt;" in wrapped and "Cisco-IOS-XR-cli-cfg" in wrapped
    observed = parse_running_config(text, intent)
    assert observed["vrf"]["name"] == "ACME-PROD" and observed["vrf"]["rd"] == "65000:1001"
    assert "65000:100" in observed["vrf"]["rt_import"]
    assert observed["interface"]["ip"] == "10.255.0.1" and observed["interface"]["admin_up"] is True
    assert parse_running_config("hostname PE1\n", intent)["vrf"] is None


def test_junos_parse():
    ep = make_endpoint("PE1", 0, vendor=Vendor.JUNIPER_JUNOS)
    intent = build_intent(ep)
    observed = parse_configuration_text(render_config(ep, "svc"), intent)
    assert observed["vrf"] == {
        "name": "ACME-PROD",
        "rd": "65000:1001",
        "rt_import": ["65000:100"],
        "rt_export": ["65000:100"],
    }
    assert observed["interface"]["ip"] == "10.255.0.1" and observed["interface"]["prefix_length"] == 30
    assert observed["bgp"]["neighbor"] == "10.255.0.2" and observed["bgp"]["remote_asn"] == 65001
    assert parse_configuration_text("system { host-name PE1; }", intent)["interface"] is None
