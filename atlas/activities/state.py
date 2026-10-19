"""Bookkeeping activities: lifecycle transitions, workflow ledger, audit trail.

All of these are idempotent so Temporal may retry them freely (``STATE_TRANSITION`` policy):
``ServiceRepository.transition`` is a no-op for same-state calls, ``record_start`` returns the
existing row, ``record_finish`` overwrites with identical values.
"""

from __future__ import annotations

from temporalio import activity

from atlas.activities._support import get_logger, map_domain_error, record_workflow_result
from atlas.activities.models import AuditInput, StageInput, WorkflowFinishInput, WorkflowStartInput
from atlas.core.enums import AuditEventType, WorkflowStatus
from atlas.db.repositories import AuditRepository, ServiceRepository, WorkflowRepository
from atlas.db.session import transaction
from atlas.schemas import StatusTransition

_FINISH_EVENT: dict[WorkflowStatus, AuditEventType] = {
    WorkflowStatus.COMPLETED: AuditEventType.WORKFLOW_COMPLETED,
    WorkflowStatus.FAILED: AuditEventType.WORKFLOW_FAILED,
    WorkflowStatus.CANCELLED: AuditEventType.WORKFLOW_CANCELLED,
    WorkflowStatus.TERMINATED: AuditEventType.WORKFLOW_FAILED,
    WorkflowStatus.TIMED_OUT: AuditEventType.WORKFLOW_FAILED,
}


@activity.defn(name="transition_status")
async def transition_status(req: StatusTransition) -> str:
    """Move the service to ``req.target`` (validated by ``atlas.core.lifecycle``) and return it."""
    log = get_logger(service_id=req.service_id, correlation_id=req.correlation_id)
    try:
        async with transaction() as session:
            service = await ServiceRepository(session).transition(
                req.service_id,
                req.target,
                reason=req.reason,
                correlation_id=req.correlation_id,
                workflow_id=req.workflow_id,
                actor="workflow",
            )
            status = str(service.lifecycle_status)
    except Exception as exc:
        log.error("status_transition_failed", target=str(req.target), error=str(exc))
        raise map_domain_error(exc) from exc
    log.info("status_transition", to_status=status, reason=req.reason)
    return status


@activity.defn(name="record_workflow_start")
async def record_workflow_start(req: WorkflowStartInput) -> str:
    log = get_logger(service_id=req.service_id, correlation_id=req.correlation_id)
    try:
        async with transaction() as session:
            execution = await WorkflowRepository(session).record_start(
                service_id=req.service_id,
                workflow_id=req.workflow_id,
                workflow_type=req.workflow_type,
                task_queue=req.task_queue,
                run_id=req.run_id,
            )
            await ServiceRepository(session).set_active_workflow(req.service_id, req.workflow_id)
            await AuditRepository(session).record(
                AuditEventType.WORKFLOW_STARTED,
                service_id=req.service_id,
                workflow_id=req.workflow_id,
                actor=req.actor,
                correlation_id=req.correlation_id,
                message=f"{req.workflow_type} started",
                details={"task_queue": req.task_queue, "run_id": req.run_id},
            )
            execution_id = str(execution.id)
    except Exception as exc:
        raise map_domain_error(exc) from exc
    log.info("workflow_recorded", workflow_type=str(req.workflow_type))
    return execution_id


@activity.defn(name="set_workflow_stage")
async def set_workflow_stage(req: StageInput) -> None:
    try:
        async with transaction() as session:
            await WorkflowRepository(session).set_stage(req.workflow_id, req.stage)
    except Exception as exc:
        raise map_domain_error(exc) from exc
    get_logger(service_id=req.service_id, correlation_id=req.correlation_id).info(
        "workflow_stage", stage=req.stage
    )


@activity.defn(name="record_workflow_finish")
async def record_workflow_finish(req: WorkflowFinishInput) -> None:
    log = get_logger(service_id=req.service_id, correlation_id=req.correlation_id)
    try:
        async with transaction() as session:
            execution = await WorkflowRepository(session).record_finish(
                req.workflow_id, req.status, result=req.result, error=req.error
            )
            await AuditRepository(session).record(
                _FINISH_EVENT.get(req.status, AuditEventType.WORKFLOW_COMPLETED),
                service_id=req.service_id or execution.service_id,
                workflow_id=req.workflow_id,
                correlation_id=req.correlation_id,
                message=f"{execution.workflow_type} finished with {req.status}",
                details={"result": req.result or {}, "error": req.error},
            )
    except Exception as exc:
        raise map_domain_error(exc) from exc
    duration = float((req.result or {}).get("duration_seconds") or 0.0)
    record_workflow_result(
        workflow_type=str(execution.workflow_type),
        status=str(req.status),
        duration_s=duration,
        failure_stage=(req.result or {}).get("final_status") if req.error else None,
    )
    log.info("workflow_finished", status=str(req.status), error=req.error)


@activity.defn(name="record_audit")
async def record_audit(req: AuditInput) -> None:
    try:
        async with transaction() as session:
            await AuditRepository(session).record(
                req.event_type,
                service_id=req.service_id,
                message=req.message,
                workflow_id=req.workflow_id,
                device_id=req.device_id,
                actor=req.actor,
                correlation_id=req.correlation_id,
                from_status=req.from_status,
                to_status=req.to_status,
                details=req.details,
            )
    except Exception as exc:
        raise map_domain_error(exc) from exc


__all__ = [
    "record_audit",
    "record_workflow_finish",
    "record_workflow_start",
    "set_workflow_stage",
    "transition_status",
]
