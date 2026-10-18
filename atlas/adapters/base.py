"""Network adapter contract.

Every vendor adapter — mock, Cisco IOS XR (NETCONF), Juniper Junos (NETCONF) — implements
``NetworkAdapter``. The orchestration worker never talks to a device directly; it resolves an
adapter via ``atlas.adapters.registry.get_adapter(device)`` and calls these coroutines inside
Temporal activities.

Error contract
--------------
Adapters raise ``AdapterError`` subclasses carrying a ``FailureKind`` so that the activity
layer and the Saga can decide: retry (TRANSIENT), compensate/escalate (PERMANENT), or inspect
device state first (UNKNOWN).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable
from uuid import UUID

from atlas.core.enums import FailureKind, Vendor


class AdapterError(Exception):
    failure_kind: FailureKind = FailureKind.UNKNOWN

    def __init__(self, message: str, *, device: str | None = None, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.device = device
        self.details = details or {}


class DeviceUnreachableError(AdapterError):
    failure_kind = FailureKind.TRANSIENT


class DeviceTimeoutError(AdapterError):
    failure_kind = FailureKind.TRANSIENT


class ConfigurationRejectedError(AdapterError):
    """Device parsed and rejected the config (syntax/semantic) — retrying will not help."""

    failure_kind = FailureKind.PERMANENT


class PartialApplyError(AdapterError):
    """Device accepted part of the configuration; actual state must be inspected."""

    failure_kind = FailureKind.UNKNOWN


class VerificationFailedError(AdapterError):
    """Config applied but operational state (BGP/VRF/interface) is not as expected."""

    failure_kind = FailureKind.UNKNOWN


@dataclass(slots=True)
class DeviceHandle:
    """Everything an adapter needs to reach a device. Credentials are resolved at call time
    from ``atlas.adapters.credentials`` and are never persisted."""

    device_id: UUID
    hostname: str
    management_ip: str
    vendor: Vendor
    platform: str = ""
    port: int = 830
    attributes: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ApplyResult:
    operation_id: str
    """Adapter-side handle (commit id / transaction id) used for rollback."""
    already_applied: bool = False
    """True when the config was found already present — idempotent re-execution."""
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class VerifyResult:
    passed: bool
    checks: list[dict[str, Any]] = field(default_factory=list)
    observed: dict[str, Any] = field(default_factory=dict)
    """Vendor-neutral observed state (same shape as ``DeviceConfig.intent``) for reconciliation."""


@runtime_checkable
class NetworkAdapter(Protocol):
    vendor: Vendor

    async def get_device_state(self, device: DeviceHandle) -> dict[str, Any]:
        """Return vendor-neutral operational+config state snapshot of the device."""
        ...

    async def validate_configuration(
        self, device: DeviceHandle, config_text: str, intent: dict[str, Any]
    ) -> None:
        """Dry-run/syntax validation. Raise ``ConfigurationRejectedError`` on failure."""
        ...

    async def apply_configuration(
        self, device: DeviceHandle, config_text: str, intent: dict[str, Any], *, idempotency_key: str
    ) -> ApplyResult:
        """Apply configuration. MUST be idempotent for the same ``idempotency_key``."""
        ...

    async def verify_configuration(self, device: DeviceHandle, intent: dict[str, Any]) -> VerifyResult:
        """Verify operational state matches intent (VRF present, RT/RD correct, interface up, BGP up)."""
        ...

    async def rollback_configuration(
        self, device: DeviceHandle, operation_id: str, intent: dict[str, Any]
    ) -> ApplyResult:
        """Undo a previous apply. MUST be idempotent and MUST NOT touch config owned by other services."""
        ...

    async def close(self) -> None: ...
