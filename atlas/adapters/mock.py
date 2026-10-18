"""Mock network adapter with simulated device state and configurable failure injection.

The mock is the authoritative adapter for development and tests. It does **not** simply return
success: every ``apply_configuration`` mutates an in-memory per-hostname device model (VRFs,
sub-interfaces, BGP VRF neighbours), ``verify_configuration`` reads that model back and
``rollback_configuration`` removes exactly the objects recorded under the operation id — never
objects owned by another service.

State is class-level (shared per process) so that different adapter instances — e.g. one per
Temporal activity worker thread — observe the same "network". Tests call ``reset()``.
"""

from __future__ import annotations

import asyncio
import copy
import uuid
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any, ClassVar

from atlas.adapters.base import (
    ApplyResult,
    ConfigurationRejectedError,
    DeviceHandle,
    DeviceTimeoutError,
    DeviceUnreachableError,
    PartialApplyError,
    VerifyResult,
)
from atlas.core.config import Settings, get_settings
from atlas.core.enums import FailureMode, Vendor

_BGP_UP = "Established"
_BGP_IDLE = "Idle"


def _new_device(asn: int) -> dict[str, Any]:
    return {
        "vrfs": {},
        "interfaces": {},
        "bgp": {"asn": asn, "vrf_neighbors": {}},
        "applied_operations": {},
        "reachable": True,
    }


def _subif_name(interface: dict[str, Any]) -> str:
    vlan = interface.get("vlan")
    return f"{interface['name']}.{vlan}" if vlan is not None else interface["name"]


class MockNetworkAdapter:
    """In-memory simulated network. Thread/async safe per hostname via ``asyncio.Lock``."""

    vendor: Vendor = Vendor.MOCK

    # --- class-level shared state -------------------------------------------------------------
    _devices: ClassVar[dict[str, dict[str, Any]]] = {}
    _locks: ClassVar[dict[str, asyncio.Lock]] = defaultdict(asyncio.Lock)
    _transient_attempts: ClassVar[dict[str, int]] = defaultdict(int)
    _failure_mode: ClassVar[FailureMode | None] = None  # None -> use settings

    def __init__(self, settings: Settings | None = None, *, transient_failures_before_success: int = 2):
        self.settings = settings or get_settings()
        self.transient_failures_before_success = transient_failures_before_success

    # --- test / operator helpers --------------------------------------------------------------

    @classmethod
    def reset(cls) -> None:
        """Wipe all simulated devices, locks, failure overrides and transient-attempt counters."""
        cls._devices.clear()
        cls._locks.clear()
        cls._transient_attempts.clear()
        cls._failure_mode = None

    @classmethod
    def set_failure_mode(cls, mode: FailureMode | str | None) -> None:
        cls._failure_mode = None if mode is None else FailureMode(mode)

    @property
    def failure_mode(self) -> FailureMode:
        return (
            self._failure_mode
            if self._failure_mode is not None
            else FailureMode(self.settings.mock_failure_mode)
        )

    @classmethod
    def state(cls, hostname: str) -> dict[str, Any]:
        """Deep copy of a device's simulated state (empty device if never touched)."""
        dev = cls._devices.get(hostname)
        return copy.deepcopy(dev) if dev is not None else _new_device(get_settings().bgp_asn)

    @classmethod
    def set_reachable(cls, hostname: str, reachable: bool) -> None:
        cls._device(hostname)["reachable"] = reachable

    @classmethod
    def inject_drift(cls, hostname: str, dotted_path: str, value: Any) -> None:
        """Set ``value`` at ``dotted_path`` inside the device state, e.g.
        ``inject_drift("PE1", "vrfs.ACME-PROD.rt_import", ["65000:200"])``."""
        node: Any = cls._device(hostname)
        parts = dotted_path.split(".")
        for part in parts[:-1]:
            if not isinstance(node, dict):
                raise KeyError(f"{dotted_path}: '{part}' is not a mapping")
            node = node.setdefault(part, {})
        if not isinstance(node, dict):
            raise KeyError(f"{dotted_path}: parent is not a mapping")
        node[parts[-1]] = copy.deepcopy(value)

    @classmethod
    def _device(cls, hostname: str) -> dict[str, Any]:
        if hostname not in cls._devices:
            cls._devices[hostname] = _new_device(get_settings().bgp_asn)
        return cls._devices[hostname]

    # --- internals ------------------------------------------------------------------------------

    async def _latency(self) -> None:
        if self.settings.mock_latency_ms > 0:
            await asyncio.sleep(self.settings.mock_latency_ms / 1000)

    def _check_reachable(self, device: DeviceHandle) -> None:
        if (
            self.failure_mode == FailureMode.DEVICE_UNREACHABLE
            or not self._device(device.hostname)["reachable"]
        ):
            raise DeviceUnreachableError(f"{device.hostname} unreachable", device=device.hostname)

    @staticmethod
    def _project(dev: dict[str, Any], intent: dict[str, Any]) -> dict[str, Any]:
        """Project device state onto the intent shape (missing objects -> None values)."""
        vrf_name = intent["vrf"]["name"]
        vrf = dev["vrfs"].get(vrf_name)
        ifname = _subif_name(intent["interface"])
        iface = dev["interfaces"].get(ifname)
        observed: dict[str, Any] = {
            "vrf": None
            if vrf is None
            else {
                "name": vrf_name,
                "rd": vrf["rd"],
                "rt_import": vrf["rt_import"],
                "rt_export": vrf["rt_export"],
            },
            "interface": None
            if iface is None
            else {
                "name": intent["interface"]["name"],
                "vlan": iface["vlan"],
                "ip": iface["ip"],
                "prefix_length": iface["prefix_length"],
                "vrf": iface["vrf"],
                "admin_up": iface["admin_up"],
            },
            "bgp": {"asn": dev["bgp"]["asn"], "vrf": vrf_name},
        }
        if "neighbor" in intent.get("bgp", {}):
            nbrs = dev["bgp"]["vrf_neighbors"].get(vrf_name, [])
            match = next((n for n in nbrs if n["neighbor"] == intent["bgp"]["neighbor"]), None)
            if match is not None:
                observed["bgp"]["neighbor"] = match["neighbor"]
                observed["bgp"]["remote_asn"] = match["remote_asn"]
                observed["bgp"]["state"] = match["state"]
        if "static" in intent and vrf is not None:
            routes = vrf.get("static_routes", [])
            match = next((r for r in routes if r["prefix"] == intent["static"]["prefix"]), None)
            if match is not None:
                observed["static"] = dict(match)
        return copy.deepcopy(observed)

    def _apply_intent(
        self, dev: dict[str, Any], intent: dict[str, Any], *, partial: bool = False
    ) -> list[dict]:
        """Write the intent into device state. Returns the list of owned objects for rollback."""
        owned: list[dict[str, Any]] = []
        vrf = intent["vrf"]
        dev["vrfs"][vrf["name"]] = {
            "rd": vrf["rd"],
            "rt_import": list(vrf["rt_import"]),
            "rt_export": list(vrf["rt_export"]),
            "static_routes": [],
        }
        owned.append({"type": "vrf", "name": vrf["name"]})
        if partial:
            return owned
        iface = intent["interface"]
        ifname = _subif_name(iface)
        dev["interfaces"][ifname] = {
            "vlan": iface["vlan"],
            "ip": iface["ip"],
            "prefix_length": iface["prefix_length"],
            "vrf": iface["vrf"],
            "admin_up": iface["admin_up"],
            "oper_up": iface["admin_up"],
        }
        owned.append({"type": "interface", "name": ifname})
        bgp = intent.get("bgp", {})
        if "neighbor" in bgp:
            state = _BGP_IDLE if self.failure_mode == FailureMode.BGP_SESSION_DOWN else _BGP_UP
            nbrs = dev["bgp"]["vrf_neighbors"].setdefault(vrf["name"], [])
            nbrs[:] = [n for n in nbrs if n["neighbor"] != bgp["neighbor"]]
            nbrs.append({"neighbor": bgp["neighbor"], "remote_asn": bgp["remote_asn"], "state": state})
            owned.append({"type": "bgp_neighbor", "vrf": vrf["name"], "neighbor": bgp["neighbor"]})
        if "static" in intent:
            dev["vrfs"][vrf["name"]]["static_routes"].append(dict(intent["static"]))
        return owned

    # --- NetworkAdapter protocol -----------------------------------------------------------------

    async def get_device_state(self, device: DeviceHandle) -> dict[str, Any]:
        await self._latency()
        self._check_reachable(device)
        return self.state(device.hostname)

    async def validate_configuration(
        self, device: DeviceHandle, config_text: str, intent: dict[str, Any]
    ) -> None:
        await self._latency()
        self._check_reachable(device)
        if not config_text.strip():
            raise ConfigurationRejectedError("empty configuration", device=device.hostname)
        for key in ("vrf", "interface"):
            if key not in intent:
                raise ConfigurationRejectedError(f"intent missing '{key}'", device=device.hostname)

    async def apply_configuration(
        self, device: DeviceHandle, config_text: str, intent: dict[str, Any], *, idempotency_key: str
    ) -> ApplyResult:
        await self._latency()
        self._check_reachable(device)
        hostname = device.hostname
        async with self._locks[hostname]:
            dev = self._device(hostname)
            # Idempotency: same key -> same operation, no re-application.
            for op_id, op in dev["applied_operations"].items():
                if op["idempotency_key"] == idempotency_key:
                    return ApplyResult(
                        operation_id=op_id, already_applied=True, details={"hostname": hostname}
                    )

            mode = self.failure_mode
            if mode == FailureMode.FAIL_ON_PE3 and hostname.upper() == "PE3":
                raise ConfigurationRejectedError(
                    "simulated: device rejected configuration", device=hostname, details={"mode": mode}
                )
            if mode == FailureMode.TIMEOUT_ON_PE2 and hostname.upper() == "PE2":
                counter_key = f"{hostname}:{idempotency_key}"
                self._transient_attempts[counter_key] += 1
                if self._transient_attempts[counter_key] <= self.transient_failures_before_success:
                    raise DeviceTimeoutError(
                        "simulated: commit timed out",
                        device=hostname,
                        details={"attempt": self._transient_attempts[counter_key]},
                    )

            op_id = f"mock-op-{uuid.uuid4()}"
            partial = mode == FailureMode.PARTIAL_CONFIGURATION
            owned = self._apply_intent(dev, intent, partial=partial)
            dev["applied_operations"][op_id] = {
                "idempotency_key": idempotency_key,
                "intent": copy.deepcopy(intent),
                "owned": owned,
                "applied_at": datetime.now(UTC).isoformat(),
                "checksum": None,
                "partial": partial,
            }
            if partial:
                raise PartialApplyError(
                    "simulated: VRF created but interface configuration failed",
                    device=hostname,
                    details={"operation_id": op_id, "applied": [o["type"] for o in owned]},
                )
            return ApplyResult(operation_id=op_id, details={"hostname": hostname, "objects": len(owned)})

    async def verify_configuration(self, device: DeviceHandle, intent: dict[str, Any]) -> VerifyResult:
        await self._latency()
        self._check_reachable(device)
        dev = self._device(device.hostname)
        observed = self._project(dev, intent)
        checks: list[dict[str, Any]] = []

        def check(name: str, passed: bool, detail: str) -> None:
            checks.append({"name": name, "passed": passed, "detail": detail})

        vrf_obs = observed["vrf"]
        check("vrf_present", vrf_obs is not None, f"vrf {intent['vrf']['name']}")
        if vrf_obs is not None:
            check("vrf_rd", vrf_obs["rd"] == intent["vrf"]["rd"], f"rd {vrf_obs['rd']}")
            check(
                "vrf_route_targets",
                sorted(vrf_obs["rt_import"]) == sorted(intent["vrf"]["rt_import"])
                and sorted(vrf_obs["rt_export"]) == sorted(intent["vrf"]["rt_export"]),
                f"import {vrf_obs['rt_import']} export {vrf_obs['rt_export']}",
            )
        if_obs = observed["interface"]
        check("interface_present", if_obs is not None, _subif_name(intent["interface"]))
        if if_obs is not None:
            want = intent["interface"]
            check(
                "interface_config",
                if_obs["ip"] == want["ip"]
                and if_obs["prefix_length"] == want["prefix_length"]
                and if_obs["vrf"] == want["vrf"]
                and if_obs["vlan"] == want["vlan"],
                f"{if_obs['ip']}/{if_obs['prefix_length']} vrf {if_obs['vrf']}",
            )
            iface_raw = dev["interfaces"][_subif_name(intent["interface"])]
            check("interface_up", bool(iface_raw["admin_up"] and iface_raw["oper_up"]), "admin/oper state")
        if "neighbor" in intent.get("bgp", {}):
            state = observed["bgp"].get("state")
            check("bgp_neighbor_present", state is not None, intent["bgp"]["neighbor"])
            check("bgp_session_established", state == _BGP_UP, f"state {state}")
        if "static" in intent:
            check("static_route_present", "static" in observed, intent["static"]["prefix"])

        return VerifyResult(passed=all(c["passed"] for c in checks), checks=checks, observed=observed)

    async def rollback_configuration(
        self, device: DeviceHandle, operation_id: str, intent: dict[str, Any]
    ) -> ApplyResult:
        await self._latency()
        self._check_reachable(device)
        hostname = device.hostname
        async with self._locks[hostname]:
            dev = self._device(hostname)
            op = dev["applied_operations"].get(operation_id)
            if op is None:
                return ApplyResult(
                    operation_id=operation_id, already_applied=True, details={"reason": "unknown op"}
                )
            removed: list[str] = []
            for obj in op["owned"]:
                if obj["type"] == "vrf":
                    if dev["vrfs"].pop(obj["name"], None) is not None:
                        removed.append(f"vrf:{obj['name']}")
                    dev["bgp"]["vrf_neighbors"].pop(obj["name"], None)
                elif obj["type"] == "interface":
                    if dev["interfaces"].pop(obj["name"], None) is not None:
                        removed.append(f"interface:{obj['name']}")
                elif obj["type"] == "bgp_neighbor":
                    nbrs = dev["bgp"]["vrf_neighbors"].get(obj["vrf"])
                    if nbrs:
                        nbrs[:] = [n for n in nbrs if n["neighbor"] != obj["neighbor"]]
                        removed.append(f"bgp:{obj['vrf']}/{obj['neighbor']}")
            del dev["applied_operations"][operation_id]
            return ApplyResult(operation_id=f"rollback-{operation_id}", details={"removed": removed})

    async def close(self) -> None:
        return None


__all__ = ["MockNetworkAdapter"]
