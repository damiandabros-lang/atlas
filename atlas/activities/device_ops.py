"""Device-touching activities: apply, inspect, rollback, decommission.

Transaction discipline
----------------------
A device write is never performed while a database transaction is open. Each activity does:

1. ``transaction()`` — ledger ``get_or_create`` (idempotency), mark IN_PROGRESS, load device.
2. adapter I/O (with heartbeats) — outside any DB transaction.
3. ``transaction()`` — record the outcome (ledger, endpoint status, audit, metrics).

Idempotency: the ledger row is keyed on ``(workflow_id, device_id, operation_type,
idempotency_key)``. A retried attempt that finds the row already SUCCEEDED short-circuits
with ``already_applied=True`` and never talks to the device again.
"""

from __future__ import annotations

import asyncio
from typing import Any
from uuid import UUID

from temporalio import activity

from atlas.activities._support import (
    CancelledAfterSettled,
    get_logger,
    map_adapter_error,
    map_domain_error,
    non_retryable,
    record_device_operation,
    with_heartbeat,
)
from atlas.adapters.base import AdapterError, DeviceHandle, NetworkAdapter
from atlas.adapters.registry import get_adapter, handle_from_device
from atlas.core.config import get_settings
from atlas.core.enums import (
    AuditEventType,
    DeviceOperationStatus,
    DeviceOperationType,
    EndpointStatus,
    FailureKind,
    Vendor,
)
from atlas.db.models import DeviceOperation
from atlas.db.repositories import (
    AuditRepository,
    DeviceOperationRepository,
    DeviceRepository,
    ServiceRepository,
)
from atlas.db.session import transaction
from atlas.schemas import DeviceOperationRequest, DeviceOperationResult

_HEARTBEAT_INTERVAL = 10.0


def _attempt() -> int:
    return activity.info().attempt if activity.in_activity() else 1


def _request_payload(req: DeviceOperationRequest) -> dict[str, Any]:
    return {
        "checksum": req.config.checksum,
        "intent": req.config.intent,
        "sequence": req.config.sequence,
        "endpoint_id": str(req.config.endpoint_id),
    }


def _result(
    req: DeviceOperationRequest,
    status: DeviceOperationStatus,
    *,
    operation_id: UUID | None = None,
    adapter_operation_id: str | None = None,
    failure_kind: FailureKind | None = None,
    error: str | None = None,
    already_applied: bool = False,
    details: dict[str, Any] | None = None,
) -> DeviceOperationResult:
    return DeviceOperationResult(
        operation_id=operation_id,
        device_id=req.config.device_id,
        device_hostname=req.config.device_hostname,
        operation_type=req.operation_type,
        status=status,
        adapter_operation_id=adapter_operation_id,
        failure_kind=failure_kind,
        error=error,
        already_applied=already_applied,
        details=details or {},
    )


async def _resolve_adapter(req: DeviceOperationRequest) -> tuple[NetworkAdapter, DeviceHandle]:
    async with transaction() as session:
        device = await DeviceRepository(session).get(req.config.device_id)
        if device is None:
            raise LookupError(f"device {req.config.device_id} not found")
        handle = handle_from_device(device)
    adapter = get_adapter(Vendor(req.config.vendor), get_settings())
    return adapter, handle


# ---------------------------------------------------------------------------
# APPLY
# ---------------------------------------------------------------------------


@activity.defn(name="provision_device")
async def provision_device(req: DeviceOperationRequest) -> DeviceOperationResult:
    log = get_logger(
        service_id=req.service_id, correlation_id=req.correlation_id, device_id=req.config.device_id
    )
    hostname = req.config.device_hostname
    op_type = DeviceOperationType.APPLY

    # --- 1. ledger -----------------------------------------------------------------------------
    try:
        async with transaction() as session:
            ops = DeviceOperationRepository(session)
            operation, created = await ops.get_or_create(
                service_id=req.service_id,
                workflow_id=req.workflow_id,
                device_id=req.config.device_id,
                endpoint_id=req.config.endpoint_id,
                operation_type=op_type,
                idempotency_key=req.idempotency_key,
                request_payload=_request_payload(req),
            )
            op_id = operation.id
            if not created and str(operation.status) == DeviceOperationStatus.SUCCEEDED:
                log.info("provision_device_already_applied", hostname=hostname)
                return _result(
                    req,
                    DeviceOperationStatus.SUCCEEDED,
                    operation_id=op_id,
                    adapter_operation_id=operation.adapter_operation_id,
                    already_applied=True,
                    details={"source": "ledger"},
                )
            await ops.mark(op_id, DeviceOperationStatus.IN_PROGRESS, attempt=_attempt())
            await ServiceRepository(session).set_endpoint_status(
                req.config.endpoint_id, EndpointStatus.PROVISIONING
            )
        adapter, handle = await _resolve_adapter(req)
    except Exception as exc:
        raise map_domain_error(exc) from exc

    # --- 2. device write -----------------------------------------------------------------------
    outcome_error: AdapterError | None = None
    cancelled = False
    apply_result = None
    try:
        apply_result = await with_heartbeat(
            adapter.apply_configuration(
                handle, req.config.config_text, req.config.intent, idempotency_key=req.idempotency_key
            ),
            interval=_HEARTBEAT_INTERVAL,
            detail={"device": hostname, "phase": "apply"},
        )
    except AdapterError as exc:
        outcome_error = exc
    except CancelledAfterSettled as exc:
        # Cancelled, but the shielded write finished: fall through with its real outcome so the
        # ledger reflects the device (compensation must know PE1 carries our VRF).
        cancelled = True
        apply_result = exc.result
        if isinstance(exc.error, AdapterError):
            outcome_error = exc.error
        elif exc.error is not None:
            outcome_error = AdapterError(f"{exc.error.__class__.__name__}: {exc.error}", device=hostname)
    except asyncio.CancelledError:
        cancelled = True
    except Exception as exc:  # unexpected adapter bug -> UNKNOWN device state
        outcome_error = AdapterError(f"{exc.__class__.__name__}: {exc}", device=hostname)

    # --- 3. outcome ----------------------------------------------------------------------------
    if cancelled and apply_result is None and outcome_error is None:
        async with transaction() as session:
            await DeviceOperationRepository(session).mark(
                op_id,
                DeviceOperationStatus.FAILED,
                error="cancelled before completion",
                failure_kind=FailureKind.UNKNOWN,
            )
            await ServiceRepository(session).set_endpoint_status(
                req.config.endpoint_id, EndpointStatus.FAILED, "cancelled"
            )
        record_device_operation(vendor=str(req.config.vendor), operation_type=op_type, success=False)
        log.warning("provision_device_cancelled", hostname=hostname)
        raise asyncio.CancelledError()

    if outcome_error is None and apply_result is not None:
        try:
            async with transaction() as session:
                await DeviceOperationRepository(session).mark(
                    op_id,
                    DeviceOperationStatus.SUCCEEDED,
                    adapter_operation_id=apply_result.operation_id,
                    response_payload={
                        "already_applied": apply_result.already_applied,
                        **(apply_result.details or {}),
                    },
                    attempt=_attempt(),
                )
                await ServiceRepository(session).set_endpoint_status(
                    req.config.endpoint_id, EndpointStatus.PROVISIONED
                )
                await AuditRepository(session).record(
                    AuditEventType.DEVICE_OPERATION,
                    service_id=req.service_id,
                    workflow_id=req.workflow_id,
                    device_id=req.config.device_id,
                    correlation_id=req.correlation_id,
                    message=f"APPLY on {hostname} succeeded",
                    details={
                        "adapter_operation_id": apply_result.operation_id,
                        "already_applied": apply_result.already_applied,
                    },
                )
        except Exception as exc:
            raise map_domain_error(exc) from exc
        record_device_operation(vendor=str(req.config.vendor), operation_type=op_type, success=True)
        log.info(
            "provision_device_succeeded",
            hostname=hostname,
            adapter_operation_id=apply_result.operation_id,
            cancelled=cancelled,
        )
        if cancelled:
            # The device carries our config and the ledger now says so; honour the cancellation
            # so the workflow's compensation path rolls this device back.
            raise asyncio.CancelledError()
        return _result(
            req,
            DeviceOperationStatus.SUCCEEDED,
            operation_id=op_id,
            adapter_operation_id=apply_result.operation_id,
            already_applied=apply_result.already_applied,
            details=dict(apply_result.details or {}),
        )

    assert outcome_error is not None
    kind = getattr(outcome_error, "failure_kind", FailureKind.UNKNOWN)
    record_device_operation(vendor=str(req.config.vendor), operation_type=op_type, success=False)
    if kind == FailureKind.TRANSIENT and not cancelled:
        # Keep the ledger row IN_PROGRESS/PENDING-like but note the error; Temporal retries.
        try:
            async with transaction() as session:
                await DeviceOperationRepository(session).mark(
                    op_id,
                    DeviceOperationStatus.PENDING,
                    error=str(outcome_error),
                    failure_kind=kind,
                    attempt=_attempt(),
                )
        except Exception as exc:
            raise map_domain_error(exc) from exc
        log.warning(
            "provision_device_transient_failure",
            hostname=hostname,
            error=str(outcome_error),
            attempt=_attempt(),
        )
        raise map_adapter_error(outcome_error) from outcome_error

    # PERMANENT or UNKNOWN: final for this activity.
    try:
        async with transaction() as session:
            await DeviceOperationRepository(session).mark(
                op_id,
                DeviceOperationStatus.FAILED,
                error=str(outcome_error),
                failure_kind=kind,
                attempt=_attempt(),
            )
            await ServiceRepository(session).set_endpoint_status(
                req.config.endpoint_id, EndpointStatus.FAILED, str(outcome_error)
            )
            await AuditRepository(session).record(
                AuditEventType.DEVICE_OPERATION,
                service_id=req.service_id,
                workflow_id=req.workflow_id,
                device_id=req.config.device_id,
                correlation_id=req.correlation_id,
                message=f"APPLY on {hostname} failed ({kind})",
                details={"error": str(outcome_error), "failure_kind": str(kind)},
            )
    except Exception as exc:
        raise map_domain_error(exc) from exc
    log.error(
        "provision_device_failed",
        hostname=hostname,
        failure_kind=str(kind),
        error=str(outcome_error),
        cancelled=cancelled,
    )
    if cancelled:
        raise asyncio.CancelledError()
    raise map_adapter_error(outcome_error) from outcome_error


# ---------------------------------------------------------------------------
# INSPECT (after an UNKNOWN / partial outcome)
# ---------------------------------------------------------------------------


@activity.defn(name="inspect_device_state")
async def inspect_device_state(req: DeviceOperationRequest) -> DeviceOperationResult:
    """Read the device and decide whether the intent is fully present.

    ``already_applied=True`` means the earlier APPLY (marked FAILED/UNKNOWN) actually landed:
    the ledger row is promoted to SUCCEEDED so later compensation knows to undo it, and the
    workflow may continue. ``False`` means the device is not (fully) configured; the workflow
    compensates. Reads never change device state, so this activity is freely retryable.
    """
    log = get_logger(
        service_id=req.service_id, correlation_id=req.correlation_id, device_id=req.config.device_id
    )
    hostname = req.config.device_hostname
    try:
        adapter, handle = await _resolve_adapter(req)
        state = await with_heartbeat(adapter.get_device_state(handle), interval=_HEARTBEAT_INTERVAL)
        verify = await with_heartbeat(
            adapter.verify_configuration(handle, req.config.intent), interval=_HEARTBEAT_INTERVAL
        )
    except AdapterError as exc:
        raise map_adapter_error(exc) from exc
    except Exception as exc:
        raise map_domain_error(exc) from exc

    applied = bool(verify.passed)
    try:
        async with transaction() as session:
            ops = DeviceOperationRepository(session)
            apply_ops = [
                op
                for op in await ops.list_for_service(req.service_id, workflow_id=req.workflow_id)
                if op.device_id == req.config.device_id
                and str(op.operation_type) == DeviceOperationType.APPLY
            ]
            apply_op: DeviceOperation | None = apply_ops[-1] if apply_ops else None
            if applied and apply_op is not None and str(apply_op.status) != DeviceOperationStatus.SUCCEEDED:
                await ops.mark(
                    apply_op.id,
                    DeviceOperationStatus.SUCCEEDED,
                    adapter_operation_id=apply_op.adapter_operation_id or req.idempotency_key,
                    response_payload={"promoted_by_inspection": True, "checks": verify.checks},
                )
                await ServiceRepository(session).set_endpoint_status(
                    req.config.endpoint_id, EndpointStatus.PROVISIONED, "confirmed by inspection"
                )
            await AuditRepository(session).record(
                AuditEventType.DEVICE_OPERATION,
                service_id=req.service_id,
                workflow_id=req.workflow_id,
                device_id=req.config.device_id,
                correlation_id=req.correlation_id,
                message=f"GET_STATE on {hostname}: intent {'present' if applied else 'absent/partial'}",
                details={"checks": verify.checks, "observed": verify.observed},
            )
    except Exception as exc:
        raise map_domain_error(exc) from exc
    record_device_operation(
        vendor=str(req.config.vendor), operation_type=DeviceOperationType.GET_STATE, success=True
    )
    log.info("inspect_device_state", hostname=hostname, applied=applied)
    return DeviceOperationResult(
        operation_id=apply_op.id if apply_op else None,
        device_id=req.config.device_id,
        device_hostname=hostname,
        operation_type=DeviceOperationType.GET_STATE,
        status=DeviceOperationStatus.SUCCEEDED if applied else DeviceOperationStatus.FAILED,
        adapter_operation_id=apply_op.adapter_operation_id if apply_op else None,
        already_applied=applied,
        details={
            "checks": verify.checks,
            "observed": verify.observed,
            "state_keys": sorted(state.keys()) if isinstance(state, dict) else [],
        },
    )


# ---------------------------------------------------------------------------
# ROLLBACK / DECOMMISSION (compensation)
# ---------------------------------------------------------------------------


async def _undo(
    req: DeviceOperationRequest,
    *,
    endpoint_target: EndpointStatus,
    audit_event: AuditEventType,
    label: str,
) -> DeviceOperationResult:
    """Shared body of ``rollback_device`` / ``decommission_device``.

    Finds the SUCCEEDED APPLY for this device (any workflow of the service), records a ROLLBACK
    ledger row keyed on ``req.idempotency_key``, calls ``adapter.rollback_configuration`` with the
    original adapter operation id, then marks the APPLY COMPENSATED. Idempotent: a repeated
    call finds the ROLLBACK row SUCCEEDED (or no APPLY to undo) and returns ``already_applied``.
    """
    log = get_logger(
        service_id=req.service_id, correlation_id=req.correlation_id, device_id=req.config.device_id
    )
    hostname = req.config.device_hostname
    try:
        async with transaction() as session:
            ops = DeviceOperationRepository(session)
            applies = [
                op
                for op in await ops.list_for_service(req.service_id, status=DeviceOperationStatus.SUCCEEDED)
                if op.device_id == req.config.device_id
                and str(op.operation_type) == DeviceOperationType.APPLY
            ]
            rollback_op, created = await ops.get_or_create(
                service_id=req.service_id,
                workflow_id=req.workflow_id,
                device_id=req.config.device_id,
                endpoint_id=req.config.endpoint_id,
                operation_type=DeviceOperationType.ROLLBACK,
                idempotency_key=req.idempotency_key,
                request_payload={**_request_payload(req), "compensates": [str(a.id) for a in applies]},
            )
            rb_id = rollback_op.id
            if not created and str(rollback_op.status) in (
                DeviceOperationStatus.SUCCEEDED,
                DeviceOperationStatus.SKIPPED,
            ):
                log.info(f"{label}_already_done", hostname=hostname)
                return _result(
                    req,
                    DeviceOperationStatus(str(rollback_op.status)),
                    operation_id=rb_id,
                    already_applied=True,
                    details={"source": "ledger"},
                )
            if not applies:
                # Nothing of ours is on the device: compensation is a no-op (never touch foreign config).
                await ops.mark(
                    rb_id,
                    DeviceOperationStatus.SKIPPED,
                    response_payload={"reason": "no succeeded APPLY to undo"},
                )
                await ServiceRepository(session).set_endpoint_status(
                    req.config.endpoint_id, endpoint_target, "nothing to undo"
                )
                log.info(f"{label}_skipped", hostname=hostname)
                return _result(
                    req,
                    DeviceOperationStatus.SKIPPED,
                    operation_id=rb_id,
                    already_applied=True,
                    details={"reason": "no succeeded APPLY"},
                )
            await ops.mark(rb_id, DeviceOperationStatus.IN_PROGRESS, attempt=_attempt())
            adapter_op_ids = [a.adapter_operation_id or a.idempotency_key for a in applies]
            apply_ids = [a.id for a in applies]
        adapter, handle = await _resolve_adapter(req)
    except Exception as exc:
        raise map_domain_error(exc) from exc

    error: AdapterError | None = None
    results = []
    try:
        for adapter_op_id in adapter_op_ids:
            results.append(
                await with_heartbeat(
                    adapter.rollback_configuration(handle, adapter_op_id, req.config.intent),
                    interval=_HEARTBEAT_INTERVAL,
                    detail={"device": hostname, "phase": label},
                )
            )
    except AdapterError as exc:
        error = exc
    except Exception as exc:  # noqa: BLE001
        error = AdapterError(f"{exc.__class__.__name__}: {exc}", device=hostname)

    if error is not None:
        record_device_operation(
            vendor=str(req.config.vendor), operation_type=DeviceOperationType.ROLLBACK, success=False
        )
        kind = getattr(error, "failure_kind", FailureKind.UNKNOWN)
        try:
            async with transaction() as session:
                status = (
                    DeviceOperationStatus.PENDING
                    if kind == FailureKind.TRANSIENT
                    else DeviceOperationStatus.FAILED
                )
                await DeviceOperationRepository(session).mark(
                    rb_id, status, error=str(error), failure_kind=kind, attempt=_attempt()
                )
                if status == DeviceOperationStatus.FAILED:
                    await ServiceRepository(session).set_endpoint_status(
                        req.config.endpoint_id, EndpointStatus.FAILED, f"{label} failed: {error}"
                    )
        except Exception as exc:
            raise map_domain_error(exc) from exc
        log.error(f"{label}_failed", hostname=hostname, failure_kind=str(kind), error=str(error))
        if kind == FailureKind.TRANSIENT:
            raise map_adapter_error(error) from error
        raise non_retryable(
            f"{label} on {hostname} failed: {error}", error_type=error.__class__.__name__
        ) from error

    try:
        async with transaction() as session:
            ops = DeviceOperationRepository(session)
            await ops.mark(
                rb_id,
                DeviceOperationStatus.SUCCEEDED,
                adapter_operation_id=results[-1].operation_id if results else None,
                response_payload={
                    "rolled_back": adapter_op_ids,
                    "already_applied": all(r.already_applied for r in results),
                },
                attempt=_attempt(),
            )
            for apply_id in apply_ids:
                await ops.mark(apply_id, DeviceOperationStatus.COMPENSATED)
            await ServiceRepository(session).set_endpoint_status(req.config.endpoint_id, endpoint_target)
            await AuditRepository(session).record(
                audit_event,
                service_id=req.service_id,
                workflow_id=req.workflow_id,
                device_id=req.config.device_id,
                correlation_id=req.correlation_id,
                message=f"{label.upper()} on {hostname} executed",
                details={
                    "compensated_operations": [str(i) for i in apply_ids],
                    "adapter_operation_ids": adapter_op_ids,
                },
            )
    except Exception as exc:
        raise map_domain_error(exc) from exc
    record_device_operation(
        vendor=str(req.config.vendor), operation_type=DeviceOperationType.ROLLBACK, success=True
    )
    log.info(f"{label}_succeeded", hostname=hostname, compensated=len(apply_ids))
    return _result(
        req,
        DeviceOperationStatus.SUCCEEDED,
        operation_id=rb_id,
        adapter_operation_id=results[-1].operation_id if results else None,
        already_applied=all(r.already_applied for r in results),
        details={"compensated_operations": [str(i) for i in apply_ids]},
    )


@activity.defn(name="rollback_device")
async def rollback_device(req: DeviceOperationRequest) -> DeviceOperationResult:
    """Saga compensation for a successful APPLY (endpoint -> ROLLED_BACK)."""
    return await _undo(
        req,
        endpoint_target=EndpointStatus.ROLLED_BACK,
        audit_event=AuditEventType.COMPENSATION_EXECUTED,
        label="rollback",
    )


@activity.defn(name="decommission_device")
async def decommission_device(req: DeviceOperationRequest) -> DeviceOperationResult:
    """Planned removal of the service from a device (endpoint -> DECOMMISSIONED)."""
    return await _undo(
        req,
        endpoint_target=EndpointStatus.DECOMMISSIONED,
        audit_event=AuditEventType.DEVICE_OPERATION,
        label="decommission",
    )


__all__ = ["decommission_device", "inspect_device_state", "provision_device", "rollback_device"]
