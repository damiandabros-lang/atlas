"""``provision_device`` / ``rollback_device`` under ``ActivityEnvironment`` with the DB layer and
adapter registry patched out — verifies ledger idempotency and AdapterError -> ApplicationError
mapping (TRANSIENT retryable, PERMANENT final, UNKNOWN -> PartialApplyError)."""

from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment

from atlas.activities import device_ops
from atlas.adapters.base import (
    ApplyResult,
    ConfigurationRejectedError,
    DeviceTimeoutError,
    DeviceUnreachableError,
    PartialApplyError,
)
from atlas.core.enums import (
    DeviceOperationStatus,
    DeviceOperationType,
    EndpointStatus,
    FailureKind,
    Vendor,
)
from atlas.schemas import DeviceConfig, DeviceOperationRequest


@pytest.fixture
def req() -> DeviceOperationRequest:
    service_id = uuid.uuid4()
    cfg = DeviceConfig(
        service_id=service_id,
        endpoint_id=uuid.uuid4(),
        device_id=uuid.uuid4(),
        device_hostname="PE3",
        vendor=Vendor.MOCK,
        sequence=2,
        config_text="vrf ACME-PROD",
        checksum="abc",
        intent={"vrf": {"name": "ACME-PROD"}},
    )
    return DeviceOperationRequest(
        service_id=service_id,
        workflow_id="provision-l3vpn-test",
        correlation_id="corr-1",
        config=cfg,
        operation_type=DeviceOperationType.APPLY,
        idempotency_key="provision-l3vpn-test:PE3:apply:v1",
    )


class FakeRepos:
    """One set of AsyncMock repositories shared by every ``transaction()`` the activity opens."""

    def __init__(
        self, *, existing_status: DeviceOperationStatus | None = None, existing_adapter_op: str | None = None
    ):
        self.op_id = uuid.uuid4()
        self.operation = SimpleNamespace(
            id=self.op_id,
            status=existing_status or DeviceOperationStatus.PENDING,
            adapter_operation_id=existing_adapter_op,
            idempotency_key="k",
            device_id=None,
            operation_type=DeviceOperationType.APPLY,
        )
        created = existing_status is None
        self.ops = MagicMock()
        self.ops.get_or_create = AsyncMock(return_value=(self.operation, created))
        self.ops.mark = AsyncMock(return_value=self.operation)
        self.ops.list_for_service = AsyncMock(return_value=[])
        self.services = MagicMock()
        self.services.set_endpoint_status = AsyncMock()
        self.audit = MagicMock()
        self.audit.record = AsyncMock()
        self.devices = MagicMock()
        self.devices.get = AsyncMock(
            return_value=SimpleNamespace(
                id=uuid.uuid4(),
                hostname="PE3",
                management_ip="10.0.0.3",
                vendor=Vendor.MOCK,
                platform="mock",
                attributes={},
            )
        )

    def marks(self) -> list[DeviceOperationStatus]:
        return [call.args[1] for call in self.ops.mark.await_args_list]


@pytest.fixture
def adapter():
    a = MagicMock()
    a.apply_configuration = AsyncMock(
        return_value=ApplyResult(operation_id="commit-42", details={"lines": 12})
    )
    a.rollback_configuration = AsyncMock(return_value=ApplyResult(operation_id="rollback-42"))
    a.get_device_state = AsyncMock(return_value={})
    return a


def _install(monkeypatch: pytest.MonkeyPatch, repos: FakeRepos, adapter) -> None:
    @asynccontextmanager
    async def fake_transaction():
        yield MagicMock(name="session")

    monkeypatch.setattr(device_ops, "transaction", fake_transaction)
    monkeypatch.setattr(device_ops, "DeviceOperationRepository", lambda session: repos.ops)
    monkeypatch.setattr(device_ops, "ServiceRepository", lambda session: repos.services)
    monkeypatch.setattr(device_ops, "AuditRepository", lambda session: repos.audit)
    monkeypatch.setattr(device_ops, "DeviceRepository", lambda session: repos.devices)
    monkeypatch.setattr(device_ops, "get_adapter", lambda vendor, settings=None: adapter)
    monkeypatch.setattr(
        device_ops, "handle_from_device", lambda device: SimpleNamespace(hostname=device.hostname)
    )


async def test_provision_device_success_records_ledger_and_endpoint(monkeypatch, req, adapter):
    repos = FakeRepos()
    _install(monkeypatch, repos, adapter)
    result = await ActivityEnvironment().run(device_ops.provision_device, req)
    assert result.status == DeviceOperationStatus.SUCCEEDED
    assert result.adapter_operation_id == "commit-42"
    assert result.already_applied is False
    assert result.operation_id == repos.op_id
    adapter.apply_configuration.assert_awaited_once()
    assert adapter.apply_configuration.await_args.kwargs["idempotency_key"] == req.idempotency_key
    assert repos.marks() == [DeviceOperationStatus.IN_PROGRESS, DeviceOperationStatus.SUCCEEDED]
    endpoint_statuses = [c.args[1] for c in repos.services.set_endpoint_status.await_args_list]
    assert endpoint_statuses == [EndpointStatus.PROVISIONING, EndpointStatus.PROVISIONED]
    repos.audit.record.assert_awaited_once()


async def test_provision_device_short_circuits_on_succeeded_ledger_row(monkeypatch, req, adapter):
    repos = FakeRepos(existing_status=DeviceOperationStatus.SUCCEEDED, existing_adapter_op="commit-old")
    _install(monkeypatch, repos, adapter)
    result = await ActivityEnvironment().run(device_ops.provision_device, req)
    assert result.already_applied is True
    assert result.status == DeviceOperationStatus.SUCCEEDED
    assert result.adapter_operation_id == "commit-old"
    adapter.apply_configuration.assert_not_awaited()
    repos.ops.mark.assert_not_awaited()


@pytest.mark.parametrize("exc_cls", [DeviceUnreachableError, DeviceTimeoutError])
async def test_provision_device_transient_is_retryable(monkeypatch, req, adapter, exc_cls):
    repos = FakeRepos()
    adapter.apply_configuration = AsyncMock(side_effect=exc_cls("PE3 down", device="PE3"))
    _install(monkeypatch, repos, adapter)
    with pytest.raises(ApplicationError) as excinfo:
        await ActivityEnvironment().run(device_ops.provision_device, req)
    err = excinfo.value
    assert err.non_retryable is False
    assert err.type == exc_cls.__name__
    assert err.details[0]["failure_kind"] == FailureKind.TRANSIENT
    # Not marked FAILED: the ledger row stays open for the next Temporal attempt.
    assert DeviceOperationStatus.FAILED not in repos.marks()
    assert repos.marks()[-1] == DeviceOperationStatus.PENDING


async def test_provision_device_permanent_is_final(monkeypatch, req, adapter):
    repos = FakeRepos()
    adapter.apply_configuration = AsyncMock(side_effect=ConfigurationRejectedError("bad vrf", device="PE3"))
    _install(monkeypatch, repos, adapter)
    with pytest.raises(ApplicationError) as excinfo:
        await ActivityEnvironment().run(device_ops.provision_device, req)
    err = excinfo.value
    assert err.non_retryable is True
    assert err.type == "ConfigurationRejectedError"
    assert repos.marks()[-1] == DeviceOperationStatus.FAILED
    failed_call = repos.ops.mark.await_args_list[-1]
    assert failed_call.kwargs["failure_kind"] == FailureKind.PERMANENT
    assert repos.services.set_endpoint_status.await_args_list[-1].args[1] == EndpointStatus.FAILED


async def test_provision_device_unknown_maps_to_partial_apply(monkeypatch, req, adapter):
    repos = FakeRepos()
    adapter.apply_configuration = AsyncMock(side_effect=PartialApplyError("commit interrupted", device="PE3"))
    _install(monkeypatch, repos, adapter)
    with pytest.raises(ApplicationError) as excinfo:
        await ActivityEnvironment().run(device_ops.provision_device, req)
    err = excinfo.value
    assert err.non_retryable is True
    assert err.type == "PartialApplyError"
    assert repos.marks()[-1] == DeviceOperationStatus.FAILED
    assert repos.ops.mark.await_args_list[-1].kwargs["failure_kind"] == FailureKind.UNKNOWN


async def test_provision_device_unexpected_exception_is_unknown(monkeypatch, req, adapter):
    repos = FakeRepos()
    adapter.apply_configuration = AsyncMock(side_effect=RuntimeError("adapter bug"))
    _install(monkeypatch, repos, adapter)
    with pytest.raises(ApplicationError) as excinfo:
        await ActivityEnvironment().run(device_ops.provision_device, req)
    assert excinfo.value.type == "PartialApplyError"
    assert excinfo.value.non_retryable is True


async def test_rollback_device_is_noop_without_succeeded_apply(monkeypatch, req, adapter):
    repos = FakeRepos()
    _install(monkeypatch, repos, adapter)
    rb = req.model_copy(
        update={"operation_type": DeviceOperationType.ROLLBACK, "idempotency_key": "wf:PE3:rollback:v1"}
    )
    result = await ActivityEnvironment().run(device_ops.rollback_device, rb)
    assert result.status == DeviceOperationStatus.SKIPPED
    assert result.already_applied is True
    adapter.rollback_configuration.assert_not_awaited()
    assert repos.services.set_endpoint_status.await_args_list[-1].args[1] == EndpointStatus.ROLLED_BACK


async def test_rollback_device_compensates_succeeded_apply(monkeypatch, req, adapter):
    repos = FakeRepos()
    apply_op = SimpleNamespace(
        id=uuid.uuid4(),
        device_id=req.config.device_id,
        operation_type=DeviceOperationType.APPLY,
        status=DeviceOperationStatus.SUCCEEDED,
        adapter_operation_id="commit-42",
        idempotency_key=req.idempotency_key,
    )
    repos.ops.list_for_service = AsyncMock(return_value=[apply_op])
    _install(monkeypatch, repos, adapter)
    rb = req.model_copy(
        update={"operation_type": DeviceOperationType.ROLLBACK, "idempotency_key": "wf:PE3:rollback:v1"}
    )
    result = await ActivityEnvironment().run(device_ops.rollback_device, rb)
    assert result.status == DeviceOperationStatus.SUCCEEDED
    adapter.rollback_configuration.assert_awaited_once()
    assert adapter.rollback_configuration.await_args.args[1] == "commit-42"
    marked = {c.args[0]: c.args[1] for c in repos.ops.mark.await_args_list}
    assert marked[apply_op.id] == DeviceOperationStatus.COMPENSATED
    assert marked[repos.op_id] == DeviceOperationStatus.SUCCEEDED
    assert repos.services.set_endpoint_status.await_args_list[-1].args[1] == EndpointStatus.ROLLED_BACK
    repos.audit.record.assert_awaited_once()
