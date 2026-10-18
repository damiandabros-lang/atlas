"""Shared helpers for the activity layer: logging, metrics, error mapping, heartbeats.

The observability package (``atlas.observability``) is implemented by another team member in
parallel. We import it by the names frozen in ``docs/INTERFACES.md`` and fall back to plain
``structlog`` / no-op metrics if a symbol is unavailable, so the worker never fails to start
because of a missing dashboard counter.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from typing import Any

import structlog
from temporalio import activity
from temporalio.exceptions import ApplicationError

from atlas.adapters.base import AdapterError
from atlas.core.enums import FailureKind
from atlas.core.lifecycle import InvalidTransitionError
from atlas.db.repositories import ResourcePoolExhaustedError

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

try:  # pragma: no cover - exercised only when the observability package is present
    from atlas.observability.logging import get_logger as _get_logger
except Exception:  # noqa: BLE001 - module may not exist yet / may differ

    def _get_logger(**bind: Any) -> Any:
        return structlog.get_logger("atlas.activities").bind(**bind)


def get_logger(**bind: Any) -> Any:
    """Structured logger bound with the current activity context (workflow_id, activity ...)."""
    context: dict[str, Any] = {}
    if activity.in_activity():
        info = activity.info()
        context.update(workflow_id=info.workflow_id, activity=info.activity_type, attempt=info.attempt)
    context.update({k: (str(v) if v is not None else None) for k, v in bind.items()})
    return _get_logger(**context)


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

try:  # pragma: no cover
    from atlas.observability import metrics as _metrics
except Exception:  # noqa: BLE001
    _metrics = None  # type: ignore[assignment]


def record_device_operation(
    *, vendor: str, operation_type: str, success: bool, failure_kind: str | None = None
) -> None:
    """Increment ``atlas_device_operations_total`` (+ ``_failures_total`` when failed)."""
    if _metrics is None:
        return
    try:
        _metrics.record_device_operation(
            str(vendor),
            str(operation_type),
            "success" if success else "failure",
            failure_kind=None if success else str(failure_kind or "UNKNOWN"),
        )
    except Exception:  # noqa: BLE001 - metrics must never break an activity
        pass


def record_drift(status: str) -> None:
    if _metrics is None:
        return
    try:
        _metrics.record_drift(str(status))
    except Exception:  # noqa: BLE001
        pass


def record_workflow_result(
    workflow_type: str, status: str, duration_s: float | None, failure_stage: str | None = None
) -> None:
    if _metrics is None:
        return
    try:
        _metrics.record_workflow_result(str(workflow_type), str(status), duration_s, failure_stage)
    except Exception:  # noqa: BLE001
        pass


# ---------------------------------------------------------------------------
# Error mapping
# ---------------------------------------------------------------------------


def non_retryable(message: str, *, error_type: str, details: Any | None = None) -> ApplicationError:
    """``ApplicationError`` that Temporal will not retry; ``type`` is matched by the policies."""
    args = (details,) if details is not None else ()
    return ApplicationError(message, *args, type=error_type, non_retryable=True)


def retryable(message: str, *, error_type: str, details: Any | None = None) -> ApplicationError:
    args = (details,) if details is not None else ()
    return ApplicationError(message, *args, type=error_type, non_retryable=False)


def map_adapter_error(exc: AdapterError) -> ApplicationError:
    """TRANSIENT -> retryable; PERMANENT -> non-retryable with the adapter class name as type;
    UNKNOWN -> non-retryable ``PartialApplyError`` (the workflow inspects device state)."""
    kind = getattr(exc, "failure_kind", FailureKind.UNKNOWN)
    details = {"device": exc.device, "failure_kind": str(kind), **(exc.details or {})}
    if kind == FailureKind.TRANSIENT:
        return retryable(str(exc), error_type=exc.__class__.__name__, details=details)
    if kind == FailureKind.PERMANENT:
        return non_retryable(str(exc), error_type=exc.__class__.__name__, details=details)
    return non_retryable(str(exc), error_type="PartialApplyError", details=details)


def map_domain_error(exc: Exception) -> ApplicationError:
    """Map repository/domain exceptions to typed ``ApplicationError`` for the retry policies."""
    if isinstance(exc, ApplicationError):
        return exc
    if isinstance(exc, InvalidTransitionError):
        return non_retryable(str(exc), error_type="InvalidTransitionError")
    if isinstance(exc, ResourcePoolExhaustedError):
        return non_retryable(str(exc), error_type="ResourcePoolExhaustedError")
    if isinstance(exc, LookupError):
        return non_retryable(str(exc), error_type="LookupError")
    if isinstance(exc, AdapterError):
        return map_adapter_error(exc)
    return retryable(f"{exc.__class__.__name__}: {exc}", error_type=exc.__class__.__name__)


# ---------------------------------------------------------------------------
# Heartbeat helper
# ---------------------------------------------------------------------------


class CancelledAfterSettled(asyncio.CancelledError):
    """Raised by ``with_heartbeat`` when the activity was cancelled but the shielded device
    write ran to completion anyway. Carries the write's outcome so the caller can record the
    *true* device state in the ledger before propagating the cancellation."""

    def __init__(self, *, result: Any = None, error: BaseException | None = None) -> None:
        super().__init__()
        self.result = result
        self.error = error


async def with_heartbeat[T](aw: Awaitable[T], *, interval: float = 5.0, detail: Any = None) -> T:
    """Await ``aw`` while heartbeating every ``interval`` seconds.

    If the activity is cancelled while the device write is in flight we do **not** abandon the
    write: the task is shielded so the device ends in a known state (applied or failed), which
    is what makes later compensation decisions sound. The ``CancelledError`` is re-raised
    once the write has settled.
    """
    task = asyncio.ensure_future(aw)
    # Heartbeat immediately: cancellation requests reach an activity only in heartbeat
    # responses, so a short device write must still get at least one chance to see them.
    if activity.in_activity():
        activity.heartbeat(detail)
    try:
        while True:
            shielded = asyncio.shield(task)
            try:
                return await asyncio.wait_for(shielded, timeout=interval)
            except TimeoutError:
                if activity.in_activity():
                    activity.heartbeat(detail)
    except asyncio.CancelledError:
        if not task.done():
            try:
                await task
            except BaseException:  # noqa: BLE001 - outcome is recorded by the caller's ledger pass
                pass
        if task.cancelled():
            raise
        raise CancelledAfterSettled(
            result=None if task.exception() else task.result(), error=task.exception()
        ) from None


def info_or_none() -> activity.Info | None:
    return activity.info() if activity.in_activity() else None


__all__ = [
    "CancelledAfterSettled",
    "get_logger",
    "info_or_none",
    "map_adapter_error",
    "map_domain_error",
    "non_retryable",
    "record_device_operation",
    "record_drift",
    "record_workflow_result",
    "retryable",
    "with_heartbeat",
]
