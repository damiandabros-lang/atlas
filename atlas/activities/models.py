"""Small Pydantic payloads used only between workflows and bookkeeping activities.

Domain payloads (ProvisionRequest, DeviceOperationRequest ...) live in ``atlas.schemas``;
these are the orchestration-internal ones that never cross the REST boundary.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from pydantic import Field

from atlas.core.enums import AuditEventType, WorkflowStatus, WorkflowType
from atlas.schemas import AtlasModel


class WorkflowStartInput(AtlasModel):
    service_id: UUID
    workflow_id: str
    run_id: str | None = None
    workflow_type: WorkflowType
    task_queue: str
    correlation_id: str
    actor: str = "system"


class StageInput(AtlasModel):
    workflow_id: str
    stage: str
    service_id: UUID | None = None
    correlation_id: str | None = None


class WorkflowFinishInput(AtlasModel):
    workflow_id: str
    status: WorkflowStatus
    service_id: UUID | None = None
    correlation_id: str | None = None
    result: dict[str, Any] | None = None
    error: str | None = None


class AuditInput(AtlasModel):
    service_id: UUID | None
    event_type: AuditEventType
    message: str
    workflow_id: str | None = None
    device_id: UUID | None = None
    actor: str = "system"
    correlation_id: str | None = None
    from_status: str | None = None
    to_status: str | None = None
    details: dict[str, Any] = Field(default_factory=dict)


__all__ = ["AuditInput", "StageInput", "WorkflowFinishInput", "WorkflowStartInput"]
