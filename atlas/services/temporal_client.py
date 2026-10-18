"""Temporal client access for the API layer.

A single ``Client`` is created lazily per process and cached. The client is cheap to share
(it multiplexes a gRPC channel) and must not be created per request.

Workflow-id helpers are the *only* place that knows how workflow ids are derived from
service ids; the API, the workflows and the worker all import them from here so the id
scheme cannot drift.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any
from uuid import UUID

from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.contrib.pydantic import pydantic_data_converter

from atlas.core.config import Settings, get_settings
from atlas.core.enums import WorkflowStatus

_client: Client | None = None
_lock = asyncio.Lock()


async def get_temporal_client(settings: Settings | None = None) -> Client:
    """Return the cached Temporal client, connecting on first use."""
    global _client
    if _client is not None:
        return _client
    async with _lock:
        if _client is None:
            settings = settings or get_settings()
            _client = await Client.connect(
                settings.temporal_host,
                namespace=settings.temporal_namespace,
                data_converter=pydantic_data_converter,
                tls=settings.temporal_tls,
            )
    return _client


def set_temporal_client(client: Client | None) -> None:
    """Inject a client (tests use a fake); ``None`` resets to lazy connection."""
    global _client
    _client = client


def reset_temporal_client() -> None:
    """Forget the cached client (test teardown / reconnect after config change)."""
    set_temporal_client(None)


# ---------------------------------------------------------------------------
# Workflow id scheme
# ---------------------------------------------------------------------------


def provision_workflow_id(service_id: UUID) -> str:
    return f"provision-l3vpn-{service_id}"


def decommission_workflow_id(service_id: UUID) -> str:
    return f"decommission-l3vpn-{service_id}"


def reconcile_workflow_id(service_id: UUID) -> str:
    return f"reconcile-service-{service_id}"


# ---------------------------------------------------------------------------
# Live status
# ---------------------------------------------------------------------------

_STATUS_MAP: dict[WorkflowExecutionStatus, WorkflowStatus] = {
    WorkflowExecutionStatus.RUNNING: WorkflowStatus.RUNNING,
    WorkflowExecutionStatus.CONTINUED_AS_NEW: WorkflowStatus.RUNNING,
    WorkflowExecutionStatus.COMPLETED: WorkflowStatus.COMPLETED,
    WorkflowExecutionStatus.FAILED: WorkflowStatus.FAILED,
    WorkflowExecutionStatus.CANCELED: WorkflowStatus.CANCELLED,
    WorkflowExecutionStatus.TERMINATED: WorkflowStatus.TERMINATED,
    WorkflowExecutionStatus.TIMED_OUT: WorkflowStatus.TIMED_OUT,
}


def map_execution_status(status: Any) -> WorkflowStatus | None:
    if status is None:
        return None
    if isinstance(status, WorkflowStatus):
        return status
    return _STATUS_MAP.get(status)


async def describe_workflow(
    client: Client | None,
    workflow_id: str,
    *,
    timeout: float = 2.0,  # noqa: ASYNC109 - used as an rpc deadline
) -> tuple[WorkflowStatus | None, str | None]:
    """Return ``(workflow_status, current_stage)`` for ``workflow_id`` from Temporal.

    ``current_stage`` is obtained through the ``progress`` query only while the workflow is
    running (queries on closed workflows need a worker and are not worth the round-trip).
    Any failure (Temporal unreachable, unknown workflow) degrades to ``(None, None)`` — the API
    still answers from the database in that case. ``client=None`` uses the cached client.
    """
    try:
        if client is None:
            client = await asyncio.wait_for(get_temporal_client(), timeout=timeout)
        handle = client.get_workflow_handle(workflow_id)
        desc = await asyncio.wait_for(handle.describe(), timeout=timeout)
    except Exception:
        return None, None
    status = map_execution_status(getattr(desc, "status", None))
    stage: str | None = None
    if status == WorkflowStatus.RUNNING:
        try:
            progress = await asyncio.wait_for(
                handle.query("progress", rpc_timeout=timedelta(seconds=timeout)), timeout=timeout
            )
            stage = _extract_stage(progress)
        except Exception:
            stage = None
    return status, stage


def _extract_stage(progress: Any) -> str | None:
    if progress is None:
        return None
    if isinstance(progress, dict):
        value = progress.get("stage")
    else:
        value = getattr(progress, "stage", None)
    return str(value) if value is not None else None


__all__ = [
    "decommission_workflow_id",
    "describe_workflow",
    "get_temporal_client",
    "map_execution_status",
    "provision_workflow_id",
    "reconcile_workflow_id",
    "reset_temporal_client",
    "set_temporal_client",
]
