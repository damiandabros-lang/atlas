"""``ProvisionL3VPNWorkflow`` — durable, compensating provisioning of an MPLS L3VPN.

Stages (each one is a lifecycle transition persisted by an activity + the ``progress`` query):

    REQUESTED -> VALIDATING -> ALLOCATING -> PROVISIONING (PE1, PE2, PE3 sequentially)
              -> VERIFYING (verify all, connectivity matrix, commit) -> ACTIVE

Failure handling
----------------
* Validation / allocation / config-generation failure (no device touched): ``FAILED`` and
  resources released.
* A device APPLY that fails with an UNKNOWN outcome (``PartialApplyError``) is *inspected*
  first; when the intent is fully present the workflow continues, otherwise it compensates.
* Any other failure after the first device was touched runs the Saga in reverse order
  (``rollback_device`` per provisioned device), releases resources and ends in
  ``ROLLED_BACK`` — or ``MANUAL_INTERVENTION_REQUIRED`` if any compensation failed.
* Operator cancellation (``cancel`` signal) is honoured between stages / devices; Temporal
  cancellation (``asyncio.CancelledError``) triggers the same compensation before re-raising.

Determinism: all I/O goes through activities; time/ids come from ``workflow.now()`` /
``workflow.uuid4()``; branch changes are guarded by ``workflow.patched``.
"""

from __future__ import annotations

import asyncio
from typing import Any

from temporalio import workflow
from temporalio.exceptions import ActivityError, ApplicationError, CancelledError

with workflow.unsafe.imports_passed_through():
    from atlas.activities import (
        allocate_resources,
        commit_service_state,
        confirm_allocations,
        generate_configs,
        inspect_device_state,
        pre_deployment_validation,
        provision_device,
        record_workflow_finish,
        record_workflow_start,
        release_resources,
        rollback_device,
        set_workflow_stage,
        transition_status,
        validate_request,
        validate_service_connectivity,
        verify_device,
    )
    from atlas.activities.models import StageInput, WorkflowFinishInput, WorkflowStartInput
    from atlas.core.enums import DeviceOperationType, LifecycleStatus, WorkflowStatus, WorkflowType
    from atlas.schemas import (
        DeviceConfig,
        DeviceOperationRequest,
        ProvisionRequest,
        ProvisionResult,
        ServiceRef,
        StatusTransition,
        WorkflowProgress,
    )
    from atlas.workflows import policies
    from atlas.workflows.saga import Saga

PATCH_CONFIRM_ALLOCATIONS_EARLY = "atlas-provision-v2-confirm-allocations-after-verify"
"""Versioning marker: v2 confirms RESERVED allocations explicitly after verification instead
of relying on ``commit_service_state`` alone. Histories recorded before the patch replay the
old branch; new executions take the new one (brief §7 "Workflow versioning")."""


_PERMANENT_REJECTIONS = frozenset({"ConfigurationRejectedError", "ValidationError"})
"""Error types that prove the device did NOT accept the configuration (no undo required)."""


class _CancelRequested(Exception):
    """Raised internally when the operator ``cancel`` signal was observed between steps."""


class _ServiceVerificationFailed(Exception):
    pass


def _app_error_type(err: BaseException) -> str | None:
    cur: BaseException | None = err
    while cur is not None:
        if isinstance(cur, ApplicationError):
            return cur.type
        cur = cur.__cause__
    return None


def _is_cancellation(err: BaseException) -> bool:
    """Activity failed because the *workflow* was cancelled (Temporal wraps it as ActivityError)."""
    cur: BaseException | None = err
    while cur is not None:
        if isinstance(cur, CancelledError | asyncio.CancelledError):
            return True
        cur = cur.__cause__
    return False


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


@workflow.defn(name="ProvisionL3VPNWorkflow")
class ProvisionL3VPNWorkflow:
    def __init__(self) -> None:
        self._progress = WorkflowProgress(stage=LifecycleStatus.REQUESTED.value)
        self._cancel_requested = False
        self._saga = Saga()
        self._request: ProvisionRequest | None = None
        self._configs: list[DeviceConfig] = []
        self._in_flight: DeviceConfig | None = None
        self._devices_touched = False
        self._resources_allocated = False
        self._temporal_cancelled = False

    # ------------------------------------------------------------------ handlers

    @workflow.signal
    async def cancel(self) -> None:
        """Operator cancel request: honoured at the next safe point (between devices/stages)."""
        self._cancel_requested = True
        self._progress.cancel_requested = True

    @workflow.query
    def progress(self) -> WorkflowProgress:
        return self._progress

    # ------------------------------------------------------------------ helpers

    @property
    def _wf_id(self) -> str:
        return workflow.info().workflow_id

    async def _act(self, fn: Any, arg: Any, policy: policies.ActivityPolicy) -> Any:
        return await workflow.execute_activity(fn, arg, **policy.options())

    def _ref(self) -> ServiceRef:
        assert self._request is not None
        return ServiceRef(service_id=self._request.service_id, correlation_id=self._request.correlation_id)

    async def _enter_stage(self, status: LifecycleStatus, reason: str | None = None) -> None:
        assert self._request is not None
        self._progress.stage = status.value
        await self._act(
            set_workflow_stage,
            StageInput(
                workflow_id=self._wf_id,
                stage=status.value,
                service_id=self._request.service_id,
                correlation_id=self._request.correlation_id,
            ),
            policies.STATE_TRANSITION,
        )
        await self._act(
            transition_status,
            StatusTransition(
                service_id=self._request.service_id,
                target=status,
                reason=reason,
                correlation_id=self._request.correlation_id,
                workflow_id=self._wf_id,
            ),
            policies.STATE_TRANSITION,
        )

    def _device_request(
        self, config: DeviceConfig, op_type: DeviceOperationType, key_suffix: str
    ) -> DeviceOperationRequest:
        assert self._request is not None
        return DeviceOperationRequest(
            service_id=self._request.service_id,
            workflow_id=self._wf_id,
            correlation_id=self._request.correlation_id,
            config=config,
            operation_type=op_type,
            idempotency_key=f"{self._wf_id}:{config.device_hostname}:{key_suffix}:v1",
        )

    def _check_cancel(self) -> None:
        if self._cancel_requested:
            raise _CancelRequested("cancelled by operator")

    def _result(self, status: LifecycleStatus, error: str | None = None) -> ProvisionResult:
        assert self._request is not None
        return ProvisionResult(
            service_id=self._request.service_id,
            final_status=status,
            workflow_type=WorkflowType.PROVISION_L3VPN,
            provisioned_devices=list(self._progress.completed_devices),
            compensated_devices=list(self._progress.compensated_devices),
            error=error,
        )

    # ------------------------------------------------------------------ run

    @workflow.run
    async def run(self, request: ProvisionRequest) -> ProvisionResult:
        self._request = request
        info = workflow.info()
        started = workflow.now()
        await self._act(
            record_workflow_start,
            WorkflowStartInput(
                service_id=request.service_id,
                workflow_id=info.workflow_id,
                run_id=info.run_id,
                workflow_type=WorkflowType.PROVISION_L3VPN,
                task_queue=info.task_queue,
                correlation_id=request.correlation_id,
                actor=request.requested_by,
            ),
            policies.STATE_TRANSITION,
        )

        finish_status = WorkflowStatus.FAILED
        result: ProvisionResult | None = None
        error: str | None = None
        try:
            result = await self._execute()
            if self._temporal_cancelled:
                # Compensation already ran inside _execute; surface the cancellation to Temporal.
                finish_status = WorkflowStatus.CANCELLED
                error = "workflow cancelled"
                raise asyncio.CancelledError()
            finish_status = WorkflowStatus.COMPLETED
            return result
        except asyncio.CancelledError:
            finish_status = WorkflowStatus.CANCELLED
            error = "workflow cancelled"
            self._progress.cancel_requested = True
            if not self._temporal_cancelled:
                # Cancelled outside an activity await (e.g. between stages): compensate now.
                self._temporal_cancelled = True
                result = await self._compensate_and_finish("workflow cancelled by Temporal", cancelled=True)
            raise
        except Exception as exc:
            error = _describe(exc)
            workflow.logger.error("provision workflow failed unexpectedly: %s", error)
            raise
        finally:
            duration = (workflow.now() - started).total_seconds()
            try:
                await self._act(
                    record_workflow_finish,
                    WorkflowFinishInput(
                        workflow_id=info.workflow_id,
                        status=finish_status,
                        service_id=request.service_id,
                        correlation_id=request.correlation_id,
                        result={
                            **(result.model_dump(mode="json") if result else {}),
                            "duration_seconds": duration,
                        },
                        error=error or (result.error if result else None),
                    ),
                    policies.STATE_TRANSITION,
                )
            except Exception as exc:  # noqa: BLE001 - bookkeeping must not mask the real outcome
                workflow.logger.warning("record_workflow_finish failed: %s", exc)

    async def _execute(self) -> ProvisionResult:
        request = self._request
        assert request is not None

        # ---- VALIDATING / ALLOCATING -----------------------------------------------------------
        try:
            await self._enter_stage(LifecycleStatus.VALIDATING)
            await self._act(validate_request, request, policies.VALIDATION)
            self._check_cancel()

            await self._enter_stage(LifecycleStatus.ALLOCATING)
            resources = await self._act(allocate_resources, request, policies.ALLOCATION)
            self._resources_allocated = True
            self._check_cancel()

            configs = await self._act(generate_configs, resources, policies.CONFIG_GENERATION)
            self._configs = sorted(configs, key=lambda c: c.sequence)
            await self._act(pre_deployment_validation, self._configs, policies.VALIDATION)
            self._check_cancel()
        except (ActivityError, _CancelRequested) as err:
            if isinstance(err, ActivityError) and _is_cancellation(err):
                self._temporal_cancelled = True
                self._progress.cancel_requested = True
                reason = "workflow cancelled by Temporal"
            elif isinstance(err, _CancelRequested):
                reason = "cancelled by operator before provisioning"
            else:
                reason = _describe(err)
            return await self._fail_before_devices(reason)

        # ---- PROVISIONING ----------------------------------------------------------------------
        try:
            await self._enter_stage(LifecycleStatus.PROVISIONING)
            for config in self._configs:
                self._check_cancel()
                await self._provision_one(config)
            self._in_flight = None

            # ---- VERIFYING ---------------------------------------------------------------------
            await self._enter_stage(LifecycleStatus.VERIFYING)
            verify_requests = [
                self._device_request(c, DeviceOperationType.VERIFY, "verify") for c in self._configs
            ]
            await asyncio.gather(
                *(self._act(verify_device, vr, policies.VERIFICATION) for vr in verify_requests)
            )

            connectivity = await self._act(validate_service_connectivity, self._ref(), policies.VERIFICATION)
            if not connectivity.passed:
                failed = [f"{c.device_hostname}: {c.detail}" for c in connectivity.checks if not c.passed]
                raise _ServiceVerificationFailed(
                    "service connectivity validation failed: " + "; ".join(failed)
                )

            if workflow.patched(PATCH_CONFIRM_ALLOCATIONS_EARLY):
                await self._act(confirm_allocations, self._ref(), policies.STATE_TRANSITION)

            await self._act(commit_service_state, self._ref(), policies.STATE_TRANSITION)
            self._progress.stage = LifecycleStatus.ACTIVE.value
            workflow.logger.info(
                "service %s ACTIVE on %s", request.service_id, self._progress.completed_devices
            )
            return self._result(LifecycleStatus.ACTIVE)
        except (ActivityError, _CancelRequested, _ServiceVerificationFailed) as err:
            cancelled = isinstance(err, ActivityError) and _is_cancellation(err)
            if cancelled:
                self._temporal_cancelled = True
                self._progress.cancel_requested = True
            reason = "workflow cancelled by Temporal" if cancelled else _describe(err)
            if self._devices_touched or self._in_flight is not None:
                return await self._compensate_and_finish(reason, cancelled=cancelled)
            return await self._fail_before_devices(reason)

    async def _provision_one(self, config: DeviceConfig) -> None:
        hostname = config.device_hostname
        self._in_flight = config
        self._devices_touched = True
        apply_req = self._device_request(config, DeviceOperationType.APPLY, "apply")
        try:
            await self._act(provision_device, apply_req, policies.DEVICE_APPLY)
        except ActivityError as err:
            err_type = _app_error_type(err)
            if err_type == "PartialApplyError":
                workflow.logger.warning("partial apply on %s — inspecting device state", hostname)
                inspect_req = self._device_request(config, DeviceOperationType.GET_STATE, "inspect")
                inspection = await self._act(inspect_device_state, inspect_req, policies.VERIFICATION)
                if not inspection.already_applied:
                    # Device state is uncertain: keep it in-flight so compensation cleans it defensively.
                    self._progress.failed_devices.append(hostname)
                    raise
                workflow.logger.info("inspection confirmed %s fully applied — continuing", hostname)
            else:
                self._progress.failed_devices.append(hostname)
                if err_type in _PERMANENT_REJECTIONS:
                    # The device rejected the change: nothing of ours landed, no compensation needed.
                    self._in_flight = None
                raise
        # The device now carries our configuration: register its undo step.
        rollback_req = self._device_request(config, DeviceOperationType.ROLLBACK, "rollback")
        self._saga.add_compensation(hostname, rollback_device, rollback_req, policies.COMPENSATION)
        self._progress.completed_devices.append(hostname)
        self._in_flight = None

    # ------------------------------------------------------------------ failure paths

    async def _fail_before_devices(self, reason: str) -> ProvisionResult:
        """Nothing was pushed to any device: FAILED + release reservations."""
        workflow.logger.warning("provisioning failed before touching devices: %s", reason)
        await self._safe_transition(LifecycleStatus.FAILED, reason)
        if self._resources_allocated:
            await self._safe_release()
        self._progress.stage = LifecycleStatus.FAILED.value
        return self._result(LifecycleStatus.FAILED, error=reason)

    async def _compensate_and_finish(self, reason: str, *, cancelled: bool = False) -> ProvisionResult:
        """Saga compensation in reverse order, then ROLLED_BACK or MANUAL_INTERVENTION_REQUIRED."""
        if not self._devices_touched and self._in_flight is None:
            return await self._fail_before_devices(reason)
        workflow.logger.warning(
            "compensating service %s: %s", self._request.service_id if self._request else "?", reason
        )
        await self._safe_transition(LifecycleStatus.ROLLING_BACK, reason)
        self._progress.stage = LifecycleStatus.ROLLING_BACK.value

        if self._in_flight is not None:
            # The interrupted device may or may not hold our config; rollback_device consults the
            # ledger and is a no-op when nothing SUCCEEDED there.
            cfg = self._in_flight
            self._saga.add_compensation(
                cfg.device_hostname,
                rollback_device,
                self._device_request(cfg, DeviceOperationType.ROLLBACK, "rollback"),
                policies.COMPENSATION,
            )
            self._in_flight = None

        outcomes = await self._saga.compensate()
        for label, ok, err in outcomes:
            if ok:
                self._progress.compensated_devices.append(label)
            else:
                workflow.logger.error("compensation for %s failed: %s", label, err)
                if label not in self._progress.failed_devices:
                    self._progress.failed_devices.append(label)
        all_ok = all(ok for _, ok, _ in outcomes)

        if all_ok:
            released = await self._safe_release() if self._resources_allocated else True
            final = LifecycleStatus.ROLLED_BACK if released else LifecycleStatus.MANUAL_INTERVENTION_REQUIRED
        else:
            final = LifecycleStatus.MANUAL_INTERVENTION_REQUIRED
        suffix = " (cancelled)" if cancelled else ""
        status_reason = f"{'rolled back' if final == LifecycleStatus.ROLLED_BACK else 'compensation incomplete'}{suffix}: {reason}"
        await self._safe_transition(final, status_reason)
        self._progress.stage = final.value
        return self._result(final, error=reason)

    async def _safe_transition(self, status: LifecycleStatus, reason: str | None) -> bool:
        assert self._request is not None
        try:
            await self._act(
                transition_status,
                StatusTransition(
                    service_id=self._request.service_id,
                    target=status,
                    reason=(reason or "")[:2000] or None,
                    correlation_id=self._request.correlation_id,
                    workflow_id=self._wf_id,
                ),
                policies.STATE_TRANSITION,
            )
            return True
        except ActivityError as exc:
            workflow.logger.error("lifecycle transition to %s failed: %s", status, _describe(exc))
            return False

    async def _safe_release(self) -> bool:
        try:
            await self._act(release_resources, self._ref(), policies.ALLOCATION)
            return True
        except ActivityError as exc:
            workflow.logger.error("release_resources failed: %s", _describe(exc))
            return False


__all__ = ["PATCH_CONFIRM_ALLOCATIONS_EARLY", "ProvisionL3VPNWorkflow"]
