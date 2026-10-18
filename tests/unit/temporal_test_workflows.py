"""Workflow tests against Temporal's time-skipping test server with STUB activities.

The stubs carry the same activity names as the real ones (``atlas.activities``) so the
workflows are exercised exactly as deployed; what they do is record calls into ``Harness``
and fail/succeed on demand. Nothing here touches a database or an adapter.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import pytest
from temporalio import activity
from temporalio.client import Client, WorkflowFailureError, WorkflowHandle
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.exceptions import ApplicationError, CancelledError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from atlas.activities.models import StageInput, WorkflowFinishInput, WorkflowStartInput
from atlas.core.enums import (
    DeviceOperationStatus,
    DeviceOperationType,
    DriftStatus,
    LifecycleStatus,
    ReconciliationMode,
    Vendor,
    WorkflowType,
)
from atlas.schemas import (
    AllocatedResources,
    DecommissionRequest,
    DeviceConfig,
    DeviceOperationRequest,
    DeviceOperationResult,
    EndpointRequest,
    EndpointResources,
    L3VPNServiceRequest,
    ProvisionRequest,
    ReconcileRequest,
    ReconciliationReport,
    ServiceRef,
    StatusTransition,
    ValidationCheck,
    ValidationReport,
)
from atlas.workflows import ALL_WORKFLOWS
from atlas.workflows.decommission_l3vpn import DecommissionL3VPNWorkflow
from atlas.workflows.provision_l3vpn import ProvisionL3VPNWorkflow
from atlas.workflows.reconcile_service import ReconcileServiceWorkflow

HOSTS = ["PE1", "PE2", "PE3"]
DEVICE_IDS = {h: uuid.uuid5(uuid.NAMESPACE_DNS, h) for h in HOSTS}
ENDPOINT_IDS = {h: uuid.uuid5(uuid.NAMESPACE_URL, h) for h in HOSTS}


# ---------------------------------------------------------------------------
# Harness + stub activities
# ---------------------------------------------------------------------------


@dataclass
class Harness:
    calls: list[tuple[str, str | None]] = field(default_factory=list)
    transitions: list[str] = field(default_factory=list)
    apply_outcomes: dict[str, list[Exception | None]] = field(default_factory=dict)
    """Per hostname: queue of outcomes per attempt (None = success)."""
    apply_delay: dict[str, float] = field(default_factory=dict)
    inspect_applied: dict[str, bool] = field(default_factory=dict)
    rollback_fail: set[str] = field(default_factory=set)
    connectivity_pass: bool = True
    decommission_fail: set[str] = field(default_factory=set)
    drift: bool = False

    def hosts(self, name: str) -> list[str]:
        return [h for n, h in self.calls if n == name]

    def names(self) -> list[str]:
        return [n for n, _ in self.calls]


def make_resources(service_id: uuid.UUID) -> AllocatedResources:
    eps = [
        EndpointResources(
            endpoint_id=ENDPOINT_IDS[h],
            device_id=DEVICE_IDS[h],
            device_hostname=h,
            vendor=Vendor.MOCK,
            interface=f"GigabitEthernet0/0/0/{i + 1}",
            vlan_id=100,
            vrf_name="ACME-PROD",
            route_distinguisher=f"65000:100{i + 1}",
            route_target_import=["65000:100"],
            route_target_export=["65000:100"],
            pe_ip_address=f"10.255.{i}.1",
            ce_ip_address=f"10.255.{i}.2",
            routing_protocol="STATIC",
            pe_asn=65000,
        )
        for i, h in enumerate(HOSTS)
    ]
    return AllocatedResources(
        service_id=service_id, vrf_name="ACME-PROD", route_target="65000:100", endpoints=eps
    )


def make_configs(service_id: uuid.UUID) -> list[DeviceConfig]:
    return [
        DeviceConfig(
            service_id=service_id,
            endpoint_id=ENDPOINT_IDS[h],
            device_id=DEVICE_IDS[h],
            device_hostname=h,
            vendor=Vendor.MOCK,
            sequence=i,
            config_text=f"vrf ACME-PROD on {h}",
            checksum=f"sum-{h}",
            intent={"vrf": {"name": "ACME-PROD", "rt_import": ["65000:100"]}},
        )
        for i, h in enumerate(HOSTS)
    ]


def make_request() -> ProvisionRequest:
    service_id = uuid.uuid4()
    return ProvisionRequest(
        service_id=service_id,
        correlation_id=f"corr-{service_id}",
        request=L3VPNServiceRequest(
            customer_id=uuid.uuid4(),
            service_name="ACME-L3VPN-001",
            vrf_name="ACME-PROD",
            route_target="65000:100",
            endpoints=[
                EndpointRequest(device=h, interface=f"GigabitEthernet0/0/0/{i + 1}", vlan=100)
                for i, h in enumerate(HOSTS)
            ],
        ),
    )


def _ok_result(req: DeviceOperationRequest, op: DeviceOperationType, **kw) -> DeviceOperationResult:
    return DeviceOperationResult(
        operation_id=uuid.uuid4(),
        device_id=req.config.device_id,
        device_hostname=req.config.device_hostname,
        operation_type=op,
        status=DeviceOperationStatus.SUCCEEDED,
        adapter_operation_id=f"commit-{req.config.device_hostname}",
        **kw,
    )


def build_stubs(h: Harness) -> list:
    @activity.defn(name="record_workflow_start")
    async def record_workflow_start(req: WorkflowStartInput) -> str:
        h.calls.append(("record_workflow_start", None))
        return "exec-1"

    @activity.defn(name="set_workflow_stage")
    async def set_workflow_stage(req: StageInput) -> None:
        h.calls.append(("set_workflow_stage", req.stage))

    @activity.defn(name="transition_status")
    async def transition_status(req: StatusTransition) -> str:
        h.calls.append(("transition_status", req.target.value))
        h.transitions.append(req.target.value)
        return req.target.value

    @activity.defn(name="record_workflow_finish")
    async def record_workflow_finish(req: WorkflowFinishInput) -> None:
        h.calls.append(("record_workflow_finish", req.status.value))

    @activity.defn(name="validate_request")
    async def validate_request(req: ProvisionRequest) -> ValidationReport:
        h.calls.append(("validate_request", None))
        return ValidationReport(service_id=req.service_id, passed=True, checks=[])

    @activity.defn(name="allocate_resources")
    async def allocate_resources(req: ProvisionRequest) -> AllocatedResources:
        h.calls.append(("allocate_resources", None))
        return make_resources(req.service_id)

    @activity.defn(name="confirm_allocations")
    async def confirm_allocations(ref: ServiceRef) -> int:
        h.calls.append(("confirm_allocations", None))
        return 7

    @activity.defn(name="release_resources")
    async def release_resources(ref: ServiceRef) -> int:
        h.calls.append(("release_resources", None))
        return 7

    @activity.defn(name="generate_configs")
    async def generate_configs(res: AllocatedResources) -> list[DeviceConfig]:
        h.calls.append(("generate_configs", None))
        return make_configs(res.service_id)

    @activity.defn(name="pre_deployment_validation")
    async def pre_deployment_validation(configs: list[DeviceConfig]) -> ValidationReport:
        h.calls.append(("pre_deployment_validation", None))
        return ValidationReport(service_id=configs[0].service_id, passed=True, checks=[])

    @activity.defn(name="load_decommission_plan")
    async def load_decommission_plan(ref: ServiceRef) -> list[DeviceConfig]:
        h.calls.append(("load_decommission_plan", None))
        return make_configs(ref.service_id)

    @activity.defn(name="provision_device")
    async def provision_device(req: DeviceOperationRequest) -> DeviceOperationResult:
        host = req.config.device_hostname
        h.calls.append(("provision_device", host))
        delay = h.apply_delay.get(host, 0.0)
        if delay:
            # Heartbeat while "pushing" so Temporal can deliver a cancel.
            loops = max(1, int(delay / 0.1))
            try:
                for _ in range(loops):
                    activity.heartbeat(host)
                    await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                h.calls.append(("provision_device_cancelled", host))
                raise
        queue = h.apply_outcomes.get(host)
        if queue:
            outcome = queue.pop(0)
            if outcome is not None:
                raise outcome
        return _ok_result(req, DeviceOperationType.APPLY)

    @activity.defn(name="inspect_device_state")
    async def inspect_device_state(req: DeviceOperationRequest) -> DeviceOperationResult:
        host = req.config.device_hostname
        h.calls.append(("inspect_device_state", host))
        applied = h.inspect_applied.get(host, False)
        return DeviceOperationResult(
            device_id=req.config.device_id,
            device_hostname=host,
            operation_type=DeviceOperationType.GET_STATE,
            status=DeviceOperationStatus.SUCCEEDED if applied else DeviceOperationStatus.FAILED,
            already_applied=applied,
        )

    @activity.defn(name="rollback_device")
    async def rollback_device(req: DeviceOperationRequest) -> DeviceOperationResult:
        host = req.config.device_hostname
        h.calls.append(("rollback_device", host))
        if host in h.rollback_fail:
            raise ApplicationError(
                f"rollback rejected on {host}", type="ConfigurationRejectedError", non_retryable=True
            )
        return _ok_result(req, DeviceOperationType.ROLLBACK)

    @activity.defn(name="decommission_device")
    async def decommission_device(req: DeviceOperationRequest) -> DeviceOperationResult:
        host = req.config.device_hostname
        h.calls.append(("decommission_device", host))
        if host in h.decommission_fail:
            raise ApplicationError(
                f"decommission rejected on {host}", type="ConfigurationRejectedError", non_retryable=True
            )
        return _ok_result(req, DeviceOperationType.ROLLBACK)

    @activity.defn(name="verify_device")
    async def verify_device(req: DeviceOperationRequest) -> DeviceOperationResult:
        h.calls.append(("verify_device", req.config.device_hostname))
        return _ok_result(req, DeviceOperationType.VERIFY)

    @activity.defn(name="validate_service_connectivity")
    async def validate_service_connectivity(ref: ServiceRef) -> ValidationReport:
        h.calls.append(("validate_service_connectivity", None))
        return ValidationReport(
            service_id=ref.service_id,
            passed=h.connectivity_pass,
            checks=[
                ValidationCheck(
                    name="pairwise_reachability",
                    device_hostname="PE1<->PE2",
                    passed=h.connectivity_pass,
                    detail=None if h.connectivity_pass else "PE2 not verified",
                )
            ],
        )

    @activity.defn(name="commit_service_state")
    async def commit_service_state(ref: ServiceRef) -> str:
        h.calls.append(("commit_service_state", None))
        h.transitions.append(LifecycleStatus.ACTIVE.value)
        return LifecycleStatus.ACTIVE.value

    @activity.defn(name="reconcile_service_activity")
    async def reconcile_service_activity(req: ReconcileRequest) -> ReconciliationReport:
        h.calls.append(("reconcile_service_activity", None))
        from atlas.schemas import DeviceDrift, DriftDifference

        devices = [
            DeviceDrift(
                service_id=req.service_id,
                device="PE1",
                status=DriftStatus.DRIFT_DETECTED if h.drift else DriftStatus.IN_SYNC,
                differences=[
                    DriftDifference(field="vrf.rt_import", desired=["65000:100"], observed=["65000:200"])
                ]
                if h.drift
                else [],
            )
        ]
        return ReconciliationReport(
            service_id=req.service_id,
            mode=req.mode,
            in_sync=not h.drift,
            devices=devices,
            checked_at=datetime.now(UTC),
        )

    return [
        record_workflow_start,
        set_workflow_stage,
        transition_status,
        record_workflow_finish,
        validate_request,
        allocate_resources,
        confirm_allocations,
        release_resources,
        generate_configs,
        pre_deployment_validation,
        load_decommission_plan,
        provision_device,
        inspect_device_state,
        rollback_device,
        decommission_device,
        verify_device,
        validate_service_connectivity,
        commit_service_state,
        reconcile_service_activity,
    ]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def env() -> AsyncIterator[WorkflowEnvironment]:
    environment = await WorkflowEnvironment.start_time_skipping(data_converter=pydantic_data_converter)
    try:
        yield environment
    finally:
        await environment.shutdown()


@pytest.fixture
def harness() -> Harness:
    return Harness()


@pytest.fixture
async def worker(env: WorkflowEnvironment, harness: Harness) -> AsyncIterator[tuple[Client, str]]:
    task_queue = f"atlas-test-{uuid.uuid4()}"
    async with Worker(
        env.client,
        task_queue=task_queue,
        workflows=ALL_WORKFLOWS,
        activities=build_stubs(harness),
        # Cancellation reaches an activity through heartbeat responses; the SDK throttles
        # heartbeats to 80% of the heartbeat timeout (24s here), far too slow for a test.
        default_heartbeat_throttle_interval=timedelta(milliseconds=50),
        max_heartbeat_throttle_interval=timedelta(milliseconds=50),
    ):
        yield env.client, task_queue


async def _wait_for_call(harness: Harness, call: tuple[str, str | None], *, deadline_s: float = 20.0) -> None:
    deadline = asyncio.get_running_loop().time() + deadline_s
    while asyncio.get_running_loop().time() < deadline:
        if call in harness.calls:
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"activity call {call} never happened")


async def _wait_for_stage(handle: WorkflowHandle, stage: str, *, deadline_s: float = 20.0) -> None:
    deadline = asyncio.get_running_loop().time() + deadline_s
    while asyncio.get_running_loop().time() < deadline:
        progress = await handle.query(ProvisionL3VPNWorkflow.progress)
        if progress.stage == stage:
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"workflow never reached stage {stage}")


# ---------------------------------------------------------------------------
# Provision workflow
# ---------------------------------------------------------------------------


async def test_provision_happy_path(worker, harness: Harness):
    client, tq = worker
    request = make_request()
    result = await client.execute_workflow(
        ProvisionL3VPNWorkflow.run, request, id=f"provision-{request.service_id}", task_queue=tq
    )
    assert result.final_status == LifecycleStatus.ACTIVE
    assert result.provisioned_devices == HOSTS
    assert result.compensated_devices == []
    assert result.error is None
    assert harness.hosts("provision_device") == HOSTS, (
        "devices are provisioned sequentially in sequence order"
    )
    assert sorted(harness.hosts("verify_device")) == HOSTS
    assert harness.transitions == ["VALIDATING", "ALLOCATING", "PROVISIONING", "VERIFYING", "ACTIVE"]
    names = harness.names()
    assert names[0] == "record_workflow_start" and names[-1] == "record_workflow_finish"
    assert ("record_workflow_finish", "COMPLETED") in harness.calls
    assert "confirm_allocations" in names, "patched branch (v2) confirms allocations before commit"
    assert "rollback_device" not in names and "release_resources" not in names


async def test_provision_fail_on_pe3_compensates_in_reverse(worker, harness: Harness):
    client, tq = worker
    harness.apply_outcomes["PE3"] = [
        ApplicationError("config rejected by PE3", type="ConfigurationRejectedError", non_retryable=True)
    ]
    request = make_request()
    result = await client.execute_workflow(
        ProvisionL3VPNWorkflow.run, request, id=f"provision-{request.service_id}", task_queue=tq
    )
    assert result.final_status == LifecycleStatus.ROLLED_BACK
    assert harness.hosts("provision_device") == HOSTS
    assert harness.hosts("rollback_device") == ["PE2", "PE1"], "Saga compensates in reverse order"
    assert result.compensated_devices == ["PE2", "PE1"]
    assert result.provisioned_devices == ["PE1", "PE2"]
    assert "release_resources" in harness.names()
    assert harness.transitions[-2:] == ["ROLLING_BACK", "ROLLED_BACK"]
    assert "config rejected by PE3" in (result.error or "")
    assert "inspect_device_state" not in harness.names()


async def test_provision_transient_pe2_retries_then_succeeds(worker, harness: Harness):
    client, tq = worker
    harness.apply_outcomes["PE2"] = [
        ApplicationError("PE2 unreachable", type="DeviceUnreachableError"),
        ApplicationError("PE2 timeout", type="DeviceTimeoutError"),
        None,
    ]
    request = make_request()
    result = await client.execute_workflow(
        ProvisionL3VPNWorkflow.run, request, id=f"provision-{request.service_id}", task_queue=tq
    )
    assert result.final_status == LifecycleStatus.ACTIVE
    assert harness.hosts("provision_device") == ["PE1", "PE2", "PE2", "PE2", "PE3"]
    assert "rollback_device" not in harness.names()


async def test_provision_partial_pe3_inspection_confirms_applied(worker, harness: Harness):
    client, tq = worker
    harness.apply_outcomes["PE3"] = [
        ApplicationError("partial commit on PE3", type="PartialApplyError", non_retryable=True)
    ]
    harness.inspect_applied["PE3"] = True
    request = make_request()
    result = await client.execute_workflow(
        ProvisionL3VPNWorkflow.run, request, id=f"provision-{request.service_id}", task_queue=tq
    )
    assert result.final_status == LifecycleStatus.ACTIVE
    assert harness.hosts("provision_device") == HOSTS, "non-retryable partial outcome is not retried"
    assert harness.hosts("inspect_device_state") == ["PE3"]
    assert result.provisioned_devices == HOSTS
    assert "rollback_device" not in harness.names()


async def test_provision_partial_pe3_inspection_not_applied_compensates(worker, harness: Harness):
    client, tq = worker
    harness.apply_outcomes["PE3"] = [
        ApplicationError("partial commit on PE3", type="PartialApplyError", non_retryable=True)
    ]
    harness.inspect_applied["PE3"] = False
    request = make_request()
    result = await client.execute_workflow(
        ProvisionL3VPNWorkflow.run, request, id=f"provision-{request.service_id}", task_queue=tq
    )
    assert result.final_status == LifecycleStatus.ROLLED_BACK
    assert harness.hosts("inspect_device_state") == ["PE3"]
    # PE3 (in-flight, nothing confirmed) is compensated defensively first, then PE2, PE1.
    assert harness.hosts("rollback_device") == ["PE3", "PE2", "PE1"]


async def test_compensation_failure_requires_manual_intervention(worker, harness: Harness):
    client, tq = worker
    harness.apply_outcomes["PE3"] = [
        ApplicationError("rejected", type="ConfigurationRejectedError", non_retryable=True)
    ]
    harness.rollback_fail.add("PE1")
    request = make_request()
    result = await client.execute_workflow(
        ProvisionL3VPNWorkflow.run, request, id=f"provision-{request.service_id}", task_queue=tq
    )
    assert result.final_status == LifecycleStatus.MANUAL_INTERVENTION_REQUIRED
    assert harness.hosts("rollback_device") == ["PE2", "PE1"], "PE1 failure must not prevent PE2 compensation"
    assert result.compensated_devices == ["PE2"]
    assert "release_resources" not in harness.names(), (
        "resources are kept for the operator when config may remain"
    )
    assert harness.transitions[-1] == "MANUAL_INTERVENTION_REQUIRED"


async def test_validation_failure_ends_failed_without_touching_devices(worker, harness: Harness):
    client, tq = worker
    request = make_request()
    # Make validate_request fail via a stub override: use a dedicated worker with a failing stub.
    stubs = build_stubs(harness)

    @activity.defn(name="validate_request")
    async def failing_validate(req: ProvisionRequest) -> ValidationReport:
        harness.calls.append(("validate_request", None))
        raise ApplicationError("customer inactive", type="ValidationError", non_retryable=True)

    stubs = [s for s in stubs if getattr(s, "__temporal_activity_definition").name != "validate_request"] + [
        failing_validate
    ]
    tq2 = f"{tq}-validation"
    async with Worker(client, task_queue=tq2, workflows=[ProvisionL3VPNWorkflow], activities=stubs):
        result = await client.execute_workflow(
            ProvisionL3VPNWorkflow.run, request, id=f"provision-{request.service_id}", task_queue=tq2
        )
    assert result.final_status == LifecycleStatus.FAILED
    assert harness.hosts("validate_request") == [None], "non-retryable ValidationError is attempted once"
    assert "provision_device" not in harness.names()
    assert "allocate_resources" not in harness.names()
    assert "release_resources" not in harness.names()
    assert harness.transitions == ["VALIDATING", "FAILED"]
    assert "customer inactive" in (result.error or "")


async def test_cancel_signal_before_provisioning(worker, harness: Harness):
    client, tq = worker
    request = make_request()
    handle = await client.start_workflow(
        ProvisionL3VPNWorkflow.run,
        request,
        id=f"provision-{request.service_id}",
        task_queue=tq,
        start_signal="cancel",
    )
    result = await handle.result()
    assert result.final_status == LifecycleStatus.FAILED
    assert "provision_device" not in harness.names()
    assert "rollback_device" not in harness.names()
    assert "cancelled" in (result.error or "")
    assert harness.transitions[-1] == "FAILED"


async def test_cancel_signal_during_provisioning_compensates(worker, harness: Harness):
    client, tq = worker
    harness.apply_delay["PE1"] = 1.5
    request = make_request()
    handle = await client.start_workflow(
        ProvisionL3VPNWorkflow.run, request, id=f"provision-{request.service_id}", task_queue=tq
    )
    await _wait_for_stage(handle, "PROVISIONING")
    progress = await handle.query(ProvisionL3VPNWorkflow.progress)
    assert (
        progress.stage == "PROVISIONING"
        and progress.completed_devices == []
        and progress.cancel_requested is False
    )
    await handle.signal(ProvisionL3VPNWorkflow.cancel)
    result = await handle.result()
    assert result.final_status == LifecycleStatus.ROLLED_BACK
    assert harness.hosts("provision_device") == ["PE1"], "cancel is honoured before the next device"
    assert harness.hosts("rollback_device") == ["PE1"]
    assert "release_resources" in harness.names()
    final = await handle.query(ProvisionL3VPNWorkflow.progress)
    assert final.cancel_requested is True
    assert final.compensated_devices == ["PE1"]
    assert final.stage == "ROLLED_BACK"


async def test_temporal_cancellation_runs_compensation_then_cancels(worker, harness: Harness):
    client, tq = worker
    harness.apply_delay["PE1"] = 3.0
    request = make_request()
    handle = await client.start_workflow(
        ProvisionL3VPNWorkflow.run, request, id=f"provision-{request.service_id}", task_queue=tq
    )
    # Cancel while the PE1 push is in flight (the activity heartbeats, so it sees the cancel).
    await _wait_for_call(harness, ("provision_device", "PE1"))
    await asyncio.sleep(0.3)
    await handle.cancel()
    with pytest.raises(WorkflowFailureError) as excinfo:
        await handle.result()
    assert isinstance(excinfo.value.cause, CancelledError)
    assert ("provision_device_cancelled", "PE1") in harness.calls
    assert harness.hosts("rollback_device") == ["PE1"], "in-flight device is compensated (ledger-idempotent)"
    assert "ROLLING_BACK" in harness.transitions
    assert harness.transitions[-1] == "ROLLED_BACK"
    assert ("record_workflow_finish", "CANCELLED") in harness.calls


async def test_connectivity_failure_compensates(worker, harness: Harness):
    client, tq = worker
    harness.connectivity_pass = False
    request = make_request()
    result = await client.execute_workflow(
        ProvisionL3VPNWorkflow.run, request, id=f"provision-{request.service_id}", task_queue=tq
    )
    assert result.final_status == LifecycleStatus.ROLLED_BACK
    assert harness.hosts("rollback_device") == ["PE3", "PE2", "PE1"]
    assert "commit_service_state" not in harness.names()


async def test_progress_query_reflects_stages(worker, harness: Harness):
    client, tq = worker
    harness.apply_delay["PE2"] = 1.0
    request = make_request()
    handle = await client.start_workflow(
        ProvisionL3VPNWorkflow.run, request, id=f"provision-{request.service_id}", task_queue=tq
    )
    deadline = asyncio.get_running_loop().time() + 20
    seen = None
    while asyncio.get_running_loop().time() < deadline:
        p = await handle.query(ProvisionL3VPNWorkflow.progress)
        if p.stage == "PROVISIONING" and p.completed_devices == ["PE1"]:
            seen = p
            break
        await asyncio.sleep(0.05)
    assert seen is not None, "progress should expose PE1 completed while PE2 is being provisioned"
    result = await handle.result()
    assert result.final_status == LifecycleStatus.ACTIVE
    assert (await handle.query(ProvisionL3VPNWorkflow.progress)).stage == "ACTIVE"


# ---------------------------------------------------------------------------
# Decommission / reconcile workflows
# ---------------------------------------------------------------------------


async def test_decommission_happy_path(worker, harness: Harness):
    client, tq = worker
    service_id = uuid.uuid4()
    req = DecommissionRequest(service_id=service_id, correlation_id="corr-decom")
    result = await client.execute_workflow(
        DecommissionL3VPNWorkflow.run, req, id=f"decommission-{service_id}", task_queue=tq
    )
    assert result.final_status == LifecycleStatus.DECOMMISSIONED
    assert result.workflow_type == WorkflowType.DECOMMISSION_L3VPN
    assert harness.hosts("decommission_device") == ["PE3", "PE2", "PE1"], (
        "devices are cleaned in reverse order"
    )
    assert result.compensated_devices == ["PE3", "PE2", "PE1"]
    assert "release_resources" in harness.names()
    assert harness.transitions == ["DECOMMISSIONING", "DECOMMISSIONED"]
    assert ("record_workflow_finish", "COMPLETED") in harness.calls


async def test_decommission_partial_failure_requires_intervention_unless_forced(worker, harness: Harness):
    client, tq = worker
    harness.decommission_fail.add("PE2")
    service_id = uuid.uuid4()
    req = DecommissionRequest(service_id=service_id, correlation_id="corr-decom")
    result = await client.execute_workflow(
        DecommissionL3VPNWorkflow.run, req, id=f"decommission-{service_id}", task_queue=tq
    )
    assert result.final_status == LifecycleStatus.MANUAL_INTERVENTION_REQUIRED
    assert harness.hosts("decommission_device") == ["PE3", "PE2", "PE1"], (
        "one failure does not stop the others"
    )
    assert "release_resources" not in harness.names()

    harness.calls.clear()
    harness.transitions.clear()
    forced = DecommissionRequest(service_id=service_id, correlation_id="corr-decom-force", force=True)
    result = await client.execute_workflow(
        DecommissionL3VPNWorkflow.run, forced, id=f"decommission-{service_id}-force", task_queue=tq
    )
    assert result.final_status == LifecycleStatus.DECOMMISSIONED
    assert "release_resources" in harness.names()
    assert "PE2" in (result.error or "")


async def test_reconcile_detect_only_returns_report(worker, harness: Harness):
    client, tq = worker
    harness.drift = True
    service_id = uuid.uuid4()
    req = ReconcileRequest(
        service_id=service_id, correlation_id="corr-recon", mode=ReconciliationMode.DETECT_ONLY
    )
    report = await client.execute_workflow(
        ReconcileServiceWorkflow.run, req, id=f"reconcile-{service_id}", task_queue=tq
    )
    assert report.in_sync is False
    assert report.devices[0].status == DriftStatus.DRIFT_DETECTED
    assert report.devices[0].differences[0].field == "vrf.rt_import"
    assert ("record_workflow_finish", "COMPLETED") in harness.calls


async def test_reconcile_auto_remediate_is_not_authorised(worker, harness: Harness):
    client, tq = worker
    service_id = uuid.uuid4()
    req = ReconcileRequest(
        service_id=service_id, correlation_id="corr-recon", mode=ReconciliationMode.AUTO_REMEDIATE
    )
    with pytest.raises(WorkflowFailureError) as excinfo:
        await client.execute_workflow(
            ReconcileServiceWorkflow.run, req, id=f"reconcile-{service_id}", task_queue=tq
        )
    cause = excinfo.value.cause
    assert isinstance(cause, ApplicationError) and cause.type == "NotAuthorized" and cause.non_retryable
    assert "reconcile_service_activity" not in harness.names()
