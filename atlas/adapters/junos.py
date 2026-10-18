"""Juniper Junos adapter (NETCONF via ``ncclient`` with the ``junos`` device handler).

Apply flow: lock -> ``load-configuration`` (text/curly-brace format, ``merge``) -> ``commit
confirmed`` (auto-rollback safety net) -> ``commit`` to confirm -> unlock. The commit is tagged
with the idempotency key as its log comment. Rollback uses the rendered rollback snippet (set
format: ``delete routing-instances <vrf>`` / ``delete interfaces <ifd> unit <n>``) so only
service-owned objects are removed; ``rollback <n>`` is deliberately avoided because it could
undo commits made by other services in the meantime.

``ncclient`` is optional; this module imports without it and fails only on connect.
"""

from __future__ import annotations

import asyncio
import re
from typing import Any

try:  # pragma: no cover
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


class JuniperJunosAdapter:
    vendor: Vendor = Vendor.JUNIPER_JUNOS

    def __init__(
        self, settings: Settings | None = None, *, connect_timeout: int = 30, confirm_timeout: int = 2
    ):
        self.settings = settings or get_settings()
        self.connect_timeout = connect_timeout
        self.confirm_timeout = confirm_timeout  # minutes, for `commit confirmed`

    def _connect(self, device: DeviceHandle):
        if manager is None:
            raise AdapterError("ncclient is not installed; install 'ncclient' to use the Junos adapter")
        creds = resolve_credentials(device, self.settings)
        try:
            return manager.connect(
                host=device.management_ip,
                port=device.port,
                username=creds.username,
                password=creds.reveal(),
                hostkey_verify=False,
                device_params={"name": "junos"},
                timeout=self.connect_timeout,
                allow_agent=False,
                look_for_keys=False,
            )
        except Exception as exc:
            if "timed out" in str(exc).lower():
                raise DeviceTimeoutError(str(exc), device=device.hostname) from exc
            raise DeviceUnreachableError(str(exc), device=device.hostname) from exc

    async def _run(self, fn):
        return await asyncio.to_thread(fn)

    # --- NetworkAdapter --------------------------------------------------------------------------

    async def get_device_state(self, device: DeviceHandle) -> dict[str, Any]:
        def op() -> dict[str, Any]:
            with self._connect(device) as m:
                cfg = m.get_configuration(format="text")
                return {"hostname": device.hostname, "configuration_text": cfg.tostring.decode()}

        return await self._run(op)

    async def validate_configuration(
        self, device: DeviceHandle, config_text: str, intent: dict[str, Any]
    ) -> None:
        def op() -> None:
            with self._connect(device) as m, m.locked("candidate"):
                try:
                    m.load_configuration(action="merge", format="text", config=config_text)
                    m.validate()
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
                try:
                    m.load_configuration(action="merge", format="text", config=config_text)
                    diff = m.compare_configuration()
                    if "<configuration-information>" in str(diff) and not re.search(r"[+-]\s", str(diff)):
                        # Nothing to change: configuration already present (idempotent re-run).
                        m.discard_changes()
                        return ApplyResult(operation_id=idempotency_key, already_applied=True)
                    m.validate()
                except Exception as exc:
                    m.discard_changes()
                    raise ConfigurationRejectedError(str(exc), device=device.hostname) from exc
                try:
                    m.commit(
                        confirmed=True, timeout=str(self.confirm_timeout), comment=f"ATLAS {idempotency_key}"
                    )
                    m.commit(comment=f"ATLAS confirm {idempotency_key}")
                except Exception as exc:
                    if "timed out" in str(exc).lower():
                        raise DeviceTimeoutError(str(exc), device=device.hostname) from exc
                    raise AdapterError(str(exc), device=device.hostname) from exc
                return ApplyResult(operation_id=idempotency_key, details={"idempotency_key": idempotency_key})

        return await self._run(op)

    async def verify_configuration(self, device: DeviceHandle, intent: dict[str, Any]) -> VerifyResult:
        def op() -> VerifyResult:
            with self._connect(device) as m:
                cfg = m.get_configuration(format="text").tostring.decode()
                observed = parse_configuration_text(cfg, intent)
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
                # TODO: `get-bgp-neighbor-information instance <vrf>` for session state.
                return VerifyResult(passed=all(c["passed"] for c in checks), checks=checks, observed=observed)

        return await self._run(op)

    async def rollback_configuration(
        self, device: DeviceHandle, operation_id: str, intent: dict[str, Any]
    ) -> ApplyResult:
        from atlas.services.config_generator import render_rollback_from_intent

        set_cmds = render_rollback_from_intent(intent, Vendor.JUNIPER_JUNOS)

        def op() -> ApplyResult:
            with self._connect(device) as m, m.locked("candidate"):
                try:
                    m.load_configuration(action="set", config=set_cmds)
                    diff = m.compare_configuration()
                    if not re.search(r"[+-]\s", str(diff)):
                        m.discard_changes()
                        return ApplyResult(operation_id=operation_id, already_applied=True)
                    m.commit(comment=f"ATLAS rollback {operation_id}")
                except Exception as exc:
                    m.discard_changes()
                    raise AdapterError(str(exc), device=device.hostname) from exc
                return ApplyResult(operation_id=f"rollback-{operation_id}")

        return await self._run(op)

    async def close(self) -> None:
        return None


def parse_configuration_text(cfg: str, intent: dict[str, Any]) -> dict[str, Any]:
    """Project a Junos text configuration onto the intent shape. Simplified parser.

    TODO: use ``get_configuration(format="xml")`` + lxml for exact routing-instance parsing.
    """
    vrf_name = intent["vrf"]["name"]
    iface = intent["interface"]
    ifd = iface["name"].split(".", 1)[0]
    unit = iface["vlan"] if iface.get("vlan") is not None else 0
    observed: dict[str, Any] = {
        "vrf": None,
        "interface": None,
        "bgp": {"asn": intent["bgp"]["asn"], "vrf": vrf_name},
    }
    ri = re.search(rf"routing-instances\s*\{{[\s\S]*?{re.escape(vrf_name)}\s*\{{([\s\S]*?)\n    \}}", cfg)
    if ri:
        body = ri.group(1)
        rd = re.search(r"route-distinguisher (\S+);", body)
        observed["vrf"] = {
            "name": vrf_name,
            "rd": rd.group(1) if rd else None,
            "rt_import": sorted(set(re.findall(r"import target:(\d+:\d+);", body))),
            "rt_export": sorted(set(re.findall(r"export target:(\d+:\d+);", body))),
        }
        nbr = re.search(r"peer-as (\d+);[\s\S]*?neighbor (\S+);", body)
        if nbr:
            observed["bgp"].update({"neighbor": nbr.group(2), "remote_asn": int(nbr.group(1))})
    unit_block = re.search(rf"{re.escape(ifd)}\s*\{{[\s\S]*?unit {unit}\s*\{{([\s\S]*?)\n        \}}", cfg)
    if unit_block:
        body = unit_block.group(1)
        addr = re.search(r"address (\S+)/(\d+);", body)
        observed["interface"] = {
            "name": iface["name"],
            "vlan": iface.get("vlan"),
            "ip": addr.group(1) if addr else None,
            "prefix_length": int(addr.group(2)) if addr else None,
            "vrf": vrf_name if observed["vrf"] else None,
            "admin_up": "disable;" not in body,
        }
    return observed


__all__ = ["JuniperJunosAdapter", "parse_configuration_text"]
