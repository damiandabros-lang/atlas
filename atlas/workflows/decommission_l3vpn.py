"""``DecommissionL3VPNWorkflow`` — planned removal of a service from every device that carries it.

    -> DECOMMISSIONING -> decommission_device per endpoint with a SUCCEEDED APPLY (reverse
       provisioning order) -> release_resources -> DECOMMISSIONED

A device that cannot be cleaned leaves the service in ``MANUAL_INTERVENTION_REQUIRED`` unless
``force`` is set, in which case the inventory is closed anyway and the failures are recorded
in ``status_reason`` for the operator.
"""

from __future__ import annotations

import asyncio
from typing import Any

from temporalio import workflow
from temporalio.exceptions import ActivityError, ApplicationError

with workflow.unsafe.imports_passed_through():
    from atlas.activities import (
        decommission_device,
        load_decommission_plan,
        record_workflow_finish,
        record_workflow_start,
        release_resources,
        set_workflow_stage,
        transition_status,
    )
    from atlas.activities.models import StageInput, WorkflowFinishInput, WorkflowStartInput
    from atlas.core.enums import DeviceOperationType, LifecycleStatus, WorkflowStatus, WorkflowType
    from atlas.schemas import (
        DecommissionRequest,
        DeviceOperationRequest,
        ProvisionResult,
        ServiceRef,
        StatusTransition,
        WorkflowProgress,
    )
    from atlas.workflows import policies


def _describe(err: BaseException) -> str:
    parts: list[str] = []
    cur: BaseException | None = err
    for _ in range(5):
        if cur is None:
            break
        text = str(cur).strip()
        if text and text not in parts:
            parts.append(text)
        cur = cur.__cause__
    return " <- ".join(parts) or err.__class__.__name__


def _is_invalid_transition(err: BaseException) -> bool:
    cur: BaseException | None = err
    while cur is not None:
        if isinstance(cur, ApplicationError) and cur.type == "InvalidTransitionError":
            return True
        cur = cur.__cause__
    return False


@workflow.defn(name="DecommissionL3VPNWorkflow")
class DecommissionL3VPNWorkflow:
    def __init__(self) -> None:
        self._progress = WorkflowProgress(stage=LifecycleStatus.DECOMMISSIONING.value)

    @workflow.query
    def progress(self) -> WorkflowProgress:
        return self._progress

    async def _act(self, fn: Any, arg: Any, policy: policies.ActivityPolicy) -> Any:
        return await workflow.execute_activity(fn, arg, **policy.options())

    async def _transition(
        self, request: DecommissionRequest, status: LifecycleStatus, reason: str | None
    ) -> bool:
        try:
            await self._act(
                transition_status,
                StatusTransition(
                    service_id=request.service_id,
                    target=status,
                    reason=reason,
                    correlation_id=request.correlation_id,
                    workflow_id=workflow.info().workflow_id,
                ),
                policies.STATE_TRANSITION,
            )
            return True
        except ActivityError as exc:
            if _is_invalid_transition(exc):
                workflow.logger.info("transition to %s skipped: %s", status, _describe(exc))
                return False
            raise

    @workflow.run
    async def run(self, request: DecommissionRequest) -> ProvisionResult:
        info = workflow.info()
        ref = ServiceRef(service_id=request.service_id, correlation_id=request.correlation_id)
        await self._act(
            record_workflow_start,
            WorkflowStartInput(
                service_id=request.service_id,
                workflow_id=info.workflow_id,
                run_id=info.run_id,
                workflow_type=WorkflowType.DECOMMISSION_L3VPN,
                task_queue=info.task_queue,
                correlation_id=request.correlation_id,
                actor=request.requested_by,
            ),
            policies.STATE_TRANSITION,
        )
        finish = WorkflowStatus.FAILED
        result: ProvisionResult | None = None
        error: str | None = None
        try:
            result = await self._execute(request, ref)
            finish = WorkflowStatus.COMPLETED
            return result
        except asyncio.CancelledError:
            finish = WorkflowStatus.CANCELLED
            error = "workflow cancelled"
            raise
        except Exception as exc:
            error = _describe(exc)
            raise
        finally:
            try:
                await self._act(
                    record_workflow_finish,
                    WorkflowFinishInput(
                        workflow_id=info.workflow_id,
                        status=finish,
                        service_id=request.service_id,
                        correlation_id=request.correlation_id,
                        result=result.model_dump(mode="json") if result else None,
                        error=error or (result.error if result else None),
                    ),
                    policies.STATE_TRANSITION,
                )
            except Exception as exc:  # noqa: BLE001
                workflow.logger.warning("record_workflow_finish failed: %s", exc)

    async def _execute(self, request: DecommissionRequest, ref: ServiceRef) -> ProvisionResult:
        wf_id = workflow.info().workflow_id
        await self._act(
            set_workflow_stage,
            StageInput(
                workflow_id=wf_id,
                stage=LifecycleStatus.DECOMMISSIONING.value,
                service_id=request.service_id,
                correlation_id=request.correlation_id,
            ),
            policies.STATE_TRANSITION,
        )
        # Services already FAILED / ROLLED_BACK cannot enter DECOMMISSIONING (they go straight to
        # DECOMMISSIONED); the transition activity tells us and we carry on.
        await self._transition(request, LifecycleStatus.DECOMMISSIONING, "decommission requested")

        plan = await self._act(load_decommission_plan, ref, policies.CONFIG_GENERATION)
        plan = sorted(plan, key=lambda c: c.sequence, reverse=True)
        failures: list[str] = []
        for config in plan:
            op = DeviceOperationRequest(
                service_id=request.service_id,
                workflow_id=wf_id,
                correlation_id=request.correlation_id,
                config=config,
                operation_type=DeviceOperationType.ROLLBACK,
                idempotency_key=f"{wf_id}:{config.device_hostname}:decommission:v1",
            )
            try:
                await self._act(decommission_device, op, policies.COMPENSATION)
                self._progress.compensated_devices.append(config.device_hostname)
            except ActivityError as exc:
                failures.append(f"{config.device_hostname}: {_describe(exc)}")
                self._progress.failed_devices.append(config.device_hostname)
                workflow.logger.error("decommission of %s failed: %s", config.device_hostname, _describe(exc))

        if failures and not request.force:
            reason = "decommission incomplete: " + "; ".join(failures)
            await self._transition(request, LifecycleStatus.MANUAL_INTERVENTION_REQUIRED, reason)
            self._progress.stage = LifecycleStatus.MANUAL_INTERVENTION_REQUIRED.value
            return ProvisionResult(
                service_id=request.service_id,
                final_status=LifecycleStatus.MANUAL_INTERVENTION_REQUIRED,
                workflow_type=WorkflowType.DECOMMISSION_L3VPN,
                compensated_devices=list(self._progress.compensated_devices),
                error=reason,
            )

        await self._act(release_resources, ref, policies.ALLOCATION)
        reason = (
            "decommissioned"
            if not failures
            else "force-decommissioned with device failures: " + "; ".join(failures)
        )
        await self._transition(request, LifecycleStatus.DECOMMISSIONED, reason)
        self._progress.stage = LifecycleStatus.DECOMMISSIONED.value
        return ProvisionResult(
            service_id=request.service_id,
            final_status=LifecycleStatus.DECOMMISSIONED,
            workflow_type=WorkflowType.DECOMMISSION_L3VPN,
            compensated_devices=list(self._progress.compensated_devices),
            error=None if not failures else reason,
        )


__all__ = ["DecommissionL3VPNWorkflow"]
