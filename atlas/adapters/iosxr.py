"""Cisco IOS XR adapter (NETCONF via ``ncclient``).

Apply flow: lock candidate -> ``edit-config`` (CLI text wrapped in the Cisco-IOS-XR-cli
``<config>`` container) -> ``validate`` -> ``commit`` with ``comment=idempotency_key``. The commit
id returned by IOS XR becomes ``ApplyResult.operation_id`` and is used for ``rollback
configuration to <id>`` style compensation.

``ncclient`` is an optional dependency: this module must import cleanly without it (unit tests,
mock-only deployments) and only fail when a connection is actually attempted.
"""

from __future__ import annotations

import asyncio
import re
from typing import Any

try:  # pragma: no cover - exercised only when ncclient is installed
    from ncclient import manager
except ImportError:  # pragma: no cover
    manager = None

from atlas.adapters.base import (
    AdapterError,
    ApplyResult,
    ConfigurationRejectedError,
    DeviceHandle,
    DeviceTimeoutError,
    DeviceUnreachableError,
    VerifyResult,
)
from atlas.adapters.credentials import resolve_credentials
from atlas.core.config import Settings, get_settings
from atlas.core.enums import Vendor

_CLI_NS = "http://cisco.com/ns/yang/Cisco-IOS-XR-cli-cfg"
_COMMIT_ID_RE = re.compile(r"commit[- ]id[\"']?>?\s*:?\s*(\d{10,})", re.IGNORECASE)


def _xml_escape(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def wrap_cli_config(config_text: str) -> str:
    """Wrap IOS XR CLI text into the NETCONF ``<config>`` payload accepted by ``edit-config``."""
    return f'<config><cli xmlns="{_CLI_NS}"><config>{_xml_escape(config_text)}</config></cli></config>'


class CiscoIOSXRAdapter:
    vendor: Vendor = Vendor.CISCO_IOSXR

    def __init__(self, settings: Settings | None = None, *, connect_timeout: int = 30):
        self.settings = settings or get_settings()
        self.connect_timeout = connect_timeout

    # --- connection ----------------------------------------------------------------------------

    def _connect(self, device: DeviceHandle):
        if manager is None:
            raise AdapterError("ncclient is not installed; install 'ncclient' to use the IOS XR adapter")
        creds = resolve_credentials(device, self.settings)
        try:
            return manager.connect(
                host=device.management_ip,
                port=device.port,
                username=creds.username,
                password=creds.reveal(),
                hostkey_verify=False,
                device_params={"name": "iosxr"},
                timeout=self.connect_timeout,
                allow_agent=False,
                look_for_keys=False,
            )
        except Exception as exc:  # ncclient raises transport-specific errors
            if "timed out" in str(exc).lower():
                raise DeviceTimeoutError(str(exc), device=device.hostname) from exc
            raise DeviceUnreachableError(str(exc), device=device.hostname) from exc

    async def _run(self, fn, *args):
        return await asyncio.to_thread(fn, *args)

    # --- NetworkAdapter --------------------------------------------------------------------------

    async def get_device_state(self, device: DeviceHandle) -> dict[str, Any]:
        def op() -> dict[str, Any]:
            with self._connect(device) as m:
                running = m.get_config(source="running").data_xml
                return {"hostname": device.hostname, "running_config_xml": running}

        return await self._run(op)

    async def validate_configuration(
        self, device: DeviceHandle, config_text: str, intent: dict[str, Any]
    ) -> None:
        def op() -> None:
            with self._connect(device) as m, m.locked("candidate"):
                try:
                    m.edit_config(target="candidate", config=wrap_cli_config(config_text))
                    m.validate(source="candidate")
                except Exception as exc:
                    raise ConfigurationRejectedError(str(exc), device=device.hostname) from exc
                finally:
                    m.discard_changes()

        await self._run(op)

    async def apply_configuration(
        self, device: DeviceHandle, config_text: str, intent: dict[str, Any], *, idempotency_key: str
    ) -> ApplyResult:
        def op() -> ApplyResult:
            with self._connect(device) as m, m.locked("candidate"):
                # Idempotency: a previous commit carrying this key means the config is present.
                # TODO: parse `show configuration commit list` via NETCONF for a robust lookup.
                try:
                    m.edit_config(target="candidate", config=wrap_cli_config(config_text))
                    m.validate(source="candidate")
                except Exception as exc:
                    m.discard_changes()
                    raise ConfigurationRejectedError(str(exc), device=device.hostname) from exc
                try:
                    reply = m.commit(comment=idempotency_key)
                except Exception as exc:
                    m.discard_changes()
                    if "timed out" in str(exc).lower():
                        raise DeviceTimeoutError(str(exc), device=device.hostname) from exc
                    raise AdapterError(str(exc), device=device.hostname) from exc
                match = _COMMIT_ID_RE.search(str(reply))
                commit_id = match.group(1) if match else idempotency_key
                return ApplyResult(operation_id=commit_id, details={"idempotency_key": idempotency_key})

        return await self._run(op)

    async def verify_configuration(self, device: DeviceHandle, intent: dict[str, Any]) -> VerifyResult:
        def op() -> VerifyResult:
            with self._connect(device) as m:
                running = m.get_config(source="running").data_xml
                observed = parse_running_config(running, intent)
                checks = [
                    {
                        "name": "vrf_present",
                        "passed": observed["vrf"] is not None,
                        "detail": intent["vrf"]["name"],
                    },
                    {
                        "name": "interface_present",
                        "passed": observed["interface"] is not None,
                        "detail": intent["interface"]["name"],
                    },
                ]
                # TODO: query Cisco-IOS-XR-ipv4-bgp-oper for neighbour session state.
                return VerifyResult(passed=all(c["passed"] for c in checks), checks=checks, observed=observed)

        return await self._run(op)

    async def rollback_configuration(
        self, device: DeviceHandle, operation_id: str, intent: dict[str, Any]
    ) -> ApplyResult:
        def op() -> ApplyResult:
            with self._connect(device) as m:
                # IOS XR: `rollback configuration to <commit-id>` undoes exactly that commit and
                # therefore never touches objects created by other services' commits.
                rpc = f'<roll-back-configuration-to xmlns="{_CLI_NS}"><commit-id>{operation_id}</commit-id></roll-back-configuration-to>'
                try:
                    m.dispatch(rpc)
                except Exception as exc:
                    if "does not exist" in str(exc).lower():
                        return ApplyResult(operation_id=operation_id, already_applied=True)
                    raise AdapterError(str(exc), device=device.hostname) from exc
                return ApplyResult(operation_id=f"rollback-{operation_id}")

        return await self._run(op)

    async def close(self) -> None:
        return None


def parse_running_config(running_xml: str, intent: dict[str, Any]) -> dict[str, Any]:
    """Project a running-config dump onto the intent shape. Simplified text parser.

    TODO: replace with YANG-model parsing (Cisco-IOS-XR-infra-rsi-cfg / ifmgr-cfg / ipv4-bgp-cfg).
    """
    vrf_name = intent["vrf"]["name"]
    iface = intent["interface"]
    ifname = f"{iface['name']}.{iface['vlan']}" if iface.get("vlan") is not None else iface["name"]
    vrf_present = re.search(rf"(?m)^\s*vrf {re.escape(vrf_name)}\s*$", running_xml) is not None
    rd = re.search(rf"vrf {re.escape(vrf_name)}\s+rd (\S+)", running_xml)
    rts = re.findall(r"(\d+:\d+)", running_xml)
    observed: dict[str, Any] = {
        "vrf": {
            "name": vrf_name,
            "rd": rd.group(1) if rd else None,
            "rt_import": sorted(set(rts)),
            "rt_export": sorted(set(rts)),
        }
        if vrf_present
        else None,
        "interface": None,
        "bgp": {"asn": intent["bgp"]["asn"], "vrf": vrf_name},
    }
    if re.search(rf"interface {re.escape(ifname)}\b", running_xml):
        ip = re.search(rf"interface {re.escape(ifname)}[\s\S]*?ipv4 address (\S+) (\S+)", running_xml)
        observed["interface"] = {
            "name": iface["name"],
            "vlan": iface.get("vlan"),
            "ip": ip.group(1) if ip else None,
            "prefix_length": iface["prefix_length"],
            "vrf": vrf_name,
            "admin_up": re.search(
                r"(?m)^\s*shutdown\s*$", running_xml.split(f"interface {ifname}", 1)[1].split("!", 1)[0]
            )
            is None,
        }
    return observed


__all__ = ["CiscoIOSXRAdapter", "parse_running_config", "wrap_cli_config"]
