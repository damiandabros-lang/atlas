"""Vendor-specific configuration generation for MPLS L3VPN endpoints.

Turns ``AllocatedResources`` (the output of the allocator) into one ``DeviceConfig`` per endpoint.
Every ``DeviceConfig`` carries both the rendered CLI text (what the adapter pushes) and a
vendor-neutral ``intent`` dict (what verify + reconciliation compare against).

Templates live in ``atlas/adapters/templates/`` and are rendered with ``StrictUndefined`` so that a
missing variable fails loudly at generation time rather than producing a half-rendered config.
"""

from __future__ import annotations

import hashlib
import ipaddress
from typing import Any

from jinja2 import Environment, PackageLoader, StrictUndefined

from atlas.core.enums import RoutingProtocol, Vendor
from atlas.schemas import AllocatedResources, DeviceConfig, EndpointResources

# Mock devices emulate IOS XR, so they share the IOS XR template.
_TEMPLATE_BY_VENDOR: dict[Vendor, str] = {
    Vendor.CISCO_IOSXR: "iosxr_l3vpn.j2",
    Vendor.JUNIPER_JUNOS: "junos_l3vpn.j2",
    Vendor.MOCK: "mock_l3vpn.j2",
}
_ROLLBACK_TEMPLATE_BY_VENDOR: dict[Vendor, str] = {
    Vendor.CISCO_IOSXR: "iosxr_l3vpn_rollback.j2",
    Vendor.JUNIPER_JUNOS: "junos_l3vpn_rollback.j2",
    Vendor.MOCK: "iosxr_l3vpn_rollback.j2",
}

# Placeholder customer prefix for STATIC endpoints. The API request does not (yet) carry customer
# LAN prefixes, so the MVP installs a default route towards the CE inside the VRF.
# TODO: accept customer prefixes per endpoint in ``EndpointRequest`` and render one route each.
STATIC_DEFAULT_PREFIX = "0.0.0.0/0"


def _netmask(prefix_length: int) -> str:
    return str(ipaddress.IPv4Network(f"0.0.0.0/{int(prefix_length)}").netmask)


def _junos_ifd(interface: str) -> str:
    """Strip an existing ``.unit`` suffix so templates can append their own."""
    return interface.split(".", 1)[0]


def _build_env() -> Environment:
    env = Environment(
        loader=PackageLoader("atlas.adapters", "templates"),
        trim_blocks=True,
        lstrip_blocks=True,
        undefined=StrictUndefined,
        autoescape=False,
        keep_trailing_newline=True,
    )
    env.filters["netmask"] = _netmask
    env.filters["junos_ifd"] = _junos_ifd
    return env


_ENV = _build_env()


def checksum(text: str) -> str:
    """Stable sha256 hex digest of the rendered configuration text."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def build_intent(ep: EndpointResources) -> dict[str, Any]:
    """Vendor-neutral desired state for one endpoint (shape frozen in docs/INTERFACES.md)."""
    intent: dict[str, Any] = {
        "vrf": {
            "name": ep.vrf_name,
            "rd": ep.route_distinguisher,
            "rt_import": sorted(ep.route_target_import),
            "rt_export": sorted(ep.route_target_export),
        },
        "interface": {
            "name": ep.interface,
            "vlan": ep.vlan_id,
            "ip": ep.pe_ip_address,
            "prefix_length": ep.prefix_length,
            "vrf": ep.vrf_name,
            "admin_up": True,
        },
        "bgp": {"asn": ep.pe_asn, "vrf": ep.vrf_name},
    }
    if ep.routing_protocol == RoutingProtocol.BGP:
        intent["bgp"]["neighbor"] = ep.ce_ip_address
        intent["bgp"]["remote_asn"] = ep.ce_asn
    else:
        # STATIC (and OSPF, which falls back to a static placeholder in the MVP).
        intent["static"] = {"prefix": STATIC_DEFAULT_PREFIX, "next_hop": ep.ce_ip_address}
    return intent


def _context(ep: EndpointResources, service_id: Any) -> dict[str, Any]:
    intent = build_intent(ep)
    return {
        **intent,
        "intent": intent,
        "service_id": str(service_id),
        "hostname": ep.device_hostname,
        "endpoint_id": str(ep.endpoint_id),
        "routing_protocol": ep.routing_protocol.value,
    }


def render_config(ep: EndpointResources, service_id: Any) -> str:
    template = _ENV.get_template(_TEMPLATE_BY_VENDOR[Vendor(ep.vendor)])
    return template.render(**_context(ep, service_id))


def render_rollback(ep: EndpointResources, service_id: Any | None = None) -> str:
    """Render a configuration that removes ONLY the objects owned by this endpoint's service:
    the VRF, the service sub-interface and the ``router bgp vrf`` / static stanza. Nothing else."""
    template = _ENV.get_template(_ROLLBACK_TEMPLATE_BY_VENDOR[Vendor(ep.vendor)])
    return template.render(**_context(ep, service_id or ""))


def render_rollback_from_intent(intent: dict[str, Any], vendor: Vendor | str, service_id: Any = "") -> str:
    """Rollback rendered from a stored intent dict (what adapters have at compensation time)."""
    template = _ENV.get_template(_ROLLBACK_TEMPLATE_BY_VENDOR[Vendor(vendor)])
    return template.render(**intent, intent=intent, service_id=str(service_id), endpoint_id="")


def generate_device_configs(resources: AllocatedResources) -> list[DeviceConfig]:
    """One ``DeviceConfig`` per endpoint, ``sequence`` = endpoint index (provisioning order)."""
    configs: list[DeviceConfig] = []
    for index, ep in enumerate(resources.endpoints):
        text = render_config(ep, resources.service_id)
        configs.append(
            DeviceConfig(
                service_id=resources.service_id,
                endpoint_id=ep.endpoint_id,
                device_id=ep.device_id,
                device_hostname=ep.device_hostname,
                vendor=Vendor(ep.vendor),
                sequence=index,
                config_text=text,
                checksum=checksum(text),
                intent=build_intent(ep),
            )
        )
    return configs


__all__ = [
    "STATIC_DEFAULT_PREFIX",
    "build_intent",
    "checksum",
    "generate_device_configs",
    "render_config",
    "render_rollback",
    "render_rollback_from_intent",
]
