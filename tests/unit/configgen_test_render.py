"""Config generation tests: per-vendor rendering, intent shape, checksum stability."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from atlas.core.enums import RoutingProtocol, Vendor
from atlas.schemas import AllocatedResources, EndpointResources
from atlas.services.config_generator import (
    STATIC_DEFAULT_PREFIX,
    build_intent,
    checksum,
    generate_device_configs,
    render_config,
    render_rollback,
    render_rollback_from_intent,
)

SERVICE_ID = UUID("11111111-1111-1111-1111-111111111111")


def make_endpoint(
    hostname: str,
    index: int,
    *,
    vendor: Vendor = Vendor.MOCK,
    protocol: RoutingProtocol = RoutingProtocol.BGP,
) -> EndpointResources:
    base = index * 4
    return EndpointResources(
        endpoint_id=uuid4(),
        device_id=uuid4(),
        device_hostname=hostname,
        vendor=vendor,
        interface=f"GigabitEthernet0/0/0/{index + 1}"
        if vendor != Vendor.JUNIPER_JUNOS
        else f"ge-0/0/{index + 1}",
        vlan_id=100,
        vrf_name="ACME-PROD",
        route_distinguisher=f"65000:{1001 + index}",
        route_target_import=["65000:100"],
        route_target_export=["65000:100"],
        pe_ip_address=f"10.255.0.{base + 1}",
        ce_ip_address=f"10.255.0.{base + 2}",
        prefix_length=30,
        routing_protocol=protocol,
        pe_asn=65000,
        ce_asn=65001 if protocol == RoutingProtocol.BGP else None,
    )


def make_resources(
    vendor: Vendor = Vendor.MOCK, protocol: RoutingProtocol = RoutingProtocol.BGP
) -> AllocatedResources:
    return AllocatedResources(
        service_id=SERVICE_ID,
        vrf_name="ACME-PROD",
        route_target="65000:100",
        endpoints=[make_endpoint(f"PE{i + 1}", i, vendor=vendor, protocol=protocol) for i in range(3)],
    )


@pytest.fixture
def resources() -> AllocatedResources:
    return make_resources()


def test_generate_one_config_per_endpoint_in_sequence(resources):
    configs = generate_device_configs(resources)
    assert [c.sequence for c in configs] == [0, 1, 2]
    assert [c.device_hostname for c in configs] == ["PE1", "PE2", "PE3"]
    for cfg, ep in zip(configs, resources.endpoints, strict=True):
        assert cfg.endpoint_id == ep.endpoint_id
        assert cfg.checksum == checksum(cfg.config_text)
        assert cfg.intent == build_intent(ep)
        assert cfg.vendor == Vendor.MOCK


def test_intent_shape_bgp():
    ep = make_endpoint("PE1", 0)
    intent = build_intent(ep)
    assert intent == {
        "vrf": {
            "name": "ACME-PROD",
            "rd": "65000:1001",
            "rt_import": ["65000:100"],
            "rt_export": ["65000:100"],
        },
        "interface": {
            "name": "GigabitEthernet0/0/0/1",
            "vlan": 100,
            "ip": "10.255.0.1",
            "prefix_length": 30,
            "vrf": "ACME-PROD",
            "admin_up": True,
        },
        "bgp": {"asn": 65000, "vrf": "ACME-PROD", "neighbor": "10.255.0.2", "remote_asn": 65001},
    }


def test_intent_shape_static():
    intent = build_intent(make_endpoint("PE1", 0, protocol=RoutingProtocol.STATIC))
    assert "neighbor" not in intent["bgp"]
    assert intent["static"] == {"prefix": STATIC_DEFAULT_PREFIX, "next_hop": "10.255.0.2"}


def test_checksum_is_stable_sha256(resources):
    a = generate_device_configs(resources)
    b = generate_device_configs(resources)
    assert [c.checksum for c in a] == [c.checksum for c in b]
    assert len(a[0].checksum) == 64
    assert checksum("x") == "2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881"
    assert a[0].checksum != a[1].checksum  # different RD/IP per device


def test_iosxr_render_bgp():
    ep = make_endpoint("PE1", 0, vendor=Vendor.CISCO_IOSXR)
    text = render_config(ep, SERVICE_ID)
    for line in (
        "vrf ACME-PROD",
        " address-family ipv4 unicast",
        "  import route-target",
        "   65000:100",
        "  export route-target",
        "interface GigabitEthernet0/0/0/1.100",
        " encapsulation dot1q 100",
        " vrf ACME-PROD",
        " ipv4 address 10.255.0.1 255.255.255.252",
        "router bgp 65000",
        "  rd 65000:1001",
        "   redistribute connected",
        "  neighbor 10.255.0.2",
        "   remote-as 65001",
    ):
        assert line in text, line
    assert "router static" not in text


def test_iosxr_render_static():
    text = render_config(
        make_endpoint("PE1", 0, vendor=Vendor.CISCO_IOSXR, protocol=RoutingProtocol.STATIC), SERVICE_ID
    )
    assert "router static\n vrf ACME-PROD" in text
    assert f"   {STATIC_DEFAULT_PREFIX} 10.255.0.2" in text
    assert "neighbor" not in text


def test_junos_render_bgp():
    text = render_config(make_endpoint("PE2", 1, vendor=Vendor.JUNIPER_JUNOS), SERVICE_ID)
    for line in (
        "routing-instances {",
        "    ACME-PROD {",
        "        instance-type vrf;",
        "        interface ge-0/0/2.100;",
        "        route-distinguisher 65000:1002;",
        "            import target:65000:100;",
        "            export target:65000:100;",
        "                    peer-as 65001;",
        "                    neighbor 10.255.0.6;",
        "    ge-0/0/2 {",
        "        vlan-tagging;",
        "        unit 100 {",
        "            vlan-id 100;",
        "                address 10.255.0.5/30;",
    ):
        assert line in text, line


def test_junos_render_static():
    text = render_config(
        make_endpoint("PE2", 1, vendor=Vendor.JUNIPER_JUNOS, protocol=RoutingProtocol.STATIC), SERVICE_ID
    )
    assert "routing-options {" in text
    assert f"route {STATIC_DEFAULT_PREFIX} next-hop 10.255.0.6;" in text
    assert "protocols {" not in text


def test_mock_vendor_renders_iosxr_template():
    mock = render_config(make_endpoint("PE1", 0, vendor=Vendor.MOCK), SERVICE_ID)
    xr = render_config(make_endpoint("PE1", 0, vendor=Vendor.CISCO_IOSXR), SERVICE_ID)
    # Same template; only the endpoint UUIDs differ.
    strip = lambda s: "\n".join(line for line in s.splitlines() if "endpoint" not in line)  # noqa: E731
    assert strip(mock) == strip(xr)


def test_rollback_removes_only_service_objects():
    ep = make_endpoint("PE1", 0, vendor=Vendor.CISCO_IOSXR)
    rb = render_rollback(ep, SERVICE_ID)
    assert "no interface GigabitEthernet0/0/0/1.100" in rb
    assert "no vrf ACME-PROD" in rb
    assert "router bgp 65000\n no vrf ACME-PROD" in rb
    assert "no router bgp" not in rb
    assert "no interface GigabitEthernet0/0/0/1\n" not in rb  # physical interface untouched

    jrb = render_rollback(make_endpoint("PE1", 0, vendor=Vendor.JUNIPER_JUNOS), SERVICE_ID)
    assert jrb.strip().splitlines() == [
        "delete routing-instances ACME-PROD",
        "delete interfaces ge-0/0/1 unit 100",
    ]

    from_intent = render_rollback_from_intent(build_intent(ep), Vendor.CISCO_IOSXR, SERVICE_ID)
    assert "no vrf ACME-PROD" in from_intent
