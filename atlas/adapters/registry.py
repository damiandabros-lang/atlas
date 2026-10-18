"""Adapter registry: one cached adapter instance per vendor.

When ``Settings.adapter_backend == "mock"`` (the default and the only backend used in CI),
every vendor resolves to ``MockNetworkAdapter`` so the full provisioning path can be exercised
against inventory that declares real vendors.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from atlas.adapters.base import DeviceHandle, NetworkAdapter
from atlas.adapters.iosxr import CiscoIOSXRAdapter
from atlas.adapters.junos import JuniperJunosAdapter
from atlas.adapters.mock import MockNetworkAdapter
from atlas.core.config import Settings, get_settings
from atlas.core.enums import Vendor

if TYPE_CHECKING:
    from atlas.db.models import NetworkDevice

_ADAPTERS: dict[Vendor, NetworkAdapter] = {}

_REAL_ADAPTERS = {
    Vendor.CISCO_IOSXR: CiscoIOSXRAdapter,
    Vendor.JUNIPER_JUNOS: JuniperJunosAdapter,
    Vendor.MOCK: MockNetworkAdapter,
}


def get_adapter(vendor: Vendor | str, settings: Settings | None = None) -> NetworkAdapter:
    vendor = Vendor(vendor)
    adapter = _ADAPTERS.get(vendor)
    if adapter is None:
        settings = settings or get_settings()
        cls = MockNetworkAdapter if settings.adapter_backend == "mock" else _REAL_ADAPTERS[vendor]
        adapter = cls(settings)
        _ADAPTERS[vendor] = adapter
    return adapter


def handle_from_device(device: NetworkDevice) -> DeviceHandle:
    attrs = dict(device.attributes or {})
    return DeviceHandle(
        device_id=device.id,
        hostname=device.hostname,
        management_ip=device.management_ip,
        vendor=Vendor(device.vendor),
        platform=device.platform or "",
        port=int(attrs.get("netconf_port", 830)),
        attributes=attrs,
    )


def reset_adapters() -> None:
    """Drop cached adapters (tests / settings reload)."""
    _ADAPTERS.clear()


__all__ = ["get_adapter", "handle_from_device", "reset_adapters"]
