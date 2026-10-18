"""``ReconcileServiceWorkflow`` — desired vs observed state comparison for one service.

The MVP runs in ``DETECT_ONLY``: a request for ``AUTO_REMEDIATE`` is refused with a
non-retryable ``NotAuthorized`` error because the platform must never modify device
configuration without explicit authorisation (brief §12).
"""

from __future__ import annotations

from typing import Any

from temporalio import workflow
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from atlas.activities import reconcile_service_activity, record_workflow_finish, record_workflow_start
    from atlas.activities.models import WorkflowFinishInput, WorkflowStartInput
    from atlas.core.enums import ReconciliationMode, WorkflowStatus, WorkflowType
    from atlas.schemas import ReconcileRequest, ReconciliationReport
    from atlas.workflows import policies


@workflow.defn(name="ReconcileServiceWorkflow")
class ReconcileServiceWorkflow:
    async def _act(self, fn: Any, arg: Any, policy: policies.ActivityPolicy) -> Any:
        return await workflow.execute_activity(fn, arg, **policy.options())

    @workflow.run
    async def run(self, request: ReconcileRequest) -> ReconciliationReport:
        if request.mode == ReconciliationMode.AUTO_REMEDIATE:
            raise ApplicationError(
                "AUTO_REMEDIATE is not authorised in this deployment (DETECT_ONLY)",
                type="NotAuthorized",
                non_retryable=True,
            )
        info = workflow.info()
        await self._act(
            record_workflow_start,
            WorkflowStartInput(
                service_id=request.service_id,
                workflow_id=info.workflow_id,
                run_id=info.run_id,
                workflow_type=WorkflowType.RECONCILE_SERVICE,
                task_queue=info.task_queue,
                correlation_id=request.correlation_id,
            ),
            policies.STATE_TRANSITION,
        )
        status = WorkflowStatus.FAILED
        report: ReconciliationReport | None = None
        error: str | None = None
        try:
            report = await self._act(reconcile_service_activity, request, policies.VERIFICATION)
            status = WorkflowStatus.COMPLETED
            return report
        except Exception as exc:
            error = str(exc)
            raise
        finally:
            try:
                await self._act(
                    record_workflow_finish,
                    WorkflowFinishInput(
                        workflow_id=info.workflow_id,
                        status=status,
                        service_id=request.service_id,
                        correlation_id=request.correlation_id,
                        result=report.model_dump(mode="json") if report else None,
                        error=error,
                    ),
                    policies.STATE_TRANSITION,
                )
            except Exception as exc:  # noqa: BLE001
                workflow.logger.warning("record_workflow_finish failed: %s", exc)


__all__ = ["ReconcileServiceWorkflow"]
