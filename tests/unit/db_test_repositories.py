"""Repository-layer tests: lifecycle transitions, optimistic locking, allocation pools, ledger, audit."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.orm.exc import StaleDataError

from atlas.core.enums import (
    AuditEventType,
    DeviceOperationStatus,
    DeviceOperationType,
    DeviceRole,
    LifecycleStatus,
    ResourceType,
    WorkflowStatus,
    WorkflowType,
)
from atlas.core.lifecycle import InvalidTransitionError
from atlas.db.models import NetworkService, ResourceAllocation
from atlas.db.repositories import (
    AllocationRepository,
    AuditRepository,
    DeviceOperationRepository,
    DeviceRepository,
    IdempotencyRepository,
    ResourcePoolExhaustedError,
    ServiceRepository,
    WorkflowRepository,
)
from atlas.db.session import get_sessionmaker, init_db_for_tests, reset_engine, transaction
from tests.unit.db_helpers import acme_request, create_service, seeded


@pytest.fixture
async def db():
    maker = await init_db_for_tests()
    try:
        yield maker
    finally:
        await reset_engine()


@pytest.fixture
async def service_id(db):
    async with transaction() as session:
        devices = await seeded(session)
        service = await create_service(session, acme_request(), devices)
        return service.id


# --------------------------------------------------------------------------- session


async def test_init_db_overrides_global_sessionmaker(db):
    assert get_sessionmaker() is db


async def test_sqlite_enforces_foreign_keys(db):
    from sqlalchemy.exc import IntegrityError

    with pytest.raises(IntegrityError):
        async with transaction() as session:
            await ServiceRepository(session).create(
                customer_id=uuid4(), service_name="orphan", service_type="MPLS_L3VPN", desired_state={}
            )


async def test_transaction_rolls_back_on_error(db):
    async with transaction() as session:
        await seeded(session)
    with pytest.raises(RuntimeError):
        async with transaction() as session:
            await DeviceRepository(session).upsert(
                hostname="PE9",
                management_ip="192.0.2.99",
                vendor="mock",
                platform="x",
                software_version="1",
                role=DeviceRole.PE,
            )
            raise RuntimeError("boom")
    async with transaction() as session:
        assert await DeviceRepository(session).get_by_hostname("pe9") is None
        assert await DeviceRepository(session).get_by_hostname("pe1") is not None


# --------------------------------------------------------------------------- lifecycle


async def test_valid_transition_bumps_version_and_audits(service_id):
    async with transaction() as session:
        repo = ServiceRepository(session)
        before = await repo.get(service_id, with_relations=False)
        assert before.lifecycle_status == LifecycleStatus.REQUESTED
        assert before.version == 1
        svc = await repo.transition(service_id, LifecycleStatus.VALIDATING, reason="ok", workflow_id="wf-1")
        assert svc.lifecycle_status == LifecycleStatus.VALIDATING
        assert svc.version == 2
        assert svc.active_workflow_id == "wf-1"
    async with transaction() as session:
        rows, total = await AuditRepository(session).history(service_id)
        assert total == 1
        assert rows[0].event_type == AuditEventType.STATUS_CHANGED
        assert (rows[0].from_status, rows[0].to_status) == ("REQUESTED", "VALIDATING")
        assert rows[0].workflow_id == "wf-1"


async def test_invalid_transition_raises(service_id):
    async with transaction() as session:
        with pytest.raises(InvalidTransitionError):
            await ServiceRepository(session).transition(service_id, LifecycleStatus.ACTIVE)
    async with transaction() as session:
        svc = await ServiceRepository(session).get(service_id)
        assert svc.lifecycle_status == LifecycleStatus.REQUESTED
        assert svc.version == 1


async def test_same_state_transition_is_noop(service_id):
    async with transaction() as session:
        repo = ServiceRepository(session)
        await repo.transition(service_id, LifecycleStatus.VALIDATING)
        svc = await repo.transition(service_id, LifecycleStatus.VALIDATING)  # retried activity
        assert svc.version == 2
        _, total = await AuditRepository(session).history(service_id)
        assert total == 1
        with pytest.raises(InvalidTransitionError):
            await repo.transition(service_id, LifecycleStatus.VALIDATING, allow_same=False)


async def test_full_happy_path_lifecycle(service_id):
    path = [
        LifecycleStatus.VALIDATING,
        LifecycleStatus.ALLOCATING,
        LifecycleStatus.PROVISIONING,
        LifecycleStatus.VERIFYING,
        LifecycleStatus.ACTIVE,
    ]
    async with transaction() as session:
        repo = ServiceRepository(session)
        for target in path:
            svc = await repo.transition(service_id, target)
        assert svc.version == 1 + len(path)
        rows, total = await AuditRepository(session).history(service_id)
        assert total == len(path)
        assert [r.to_status for r in rows] == [s.value for s in path]


async def test_optimistic_lock_detects_concurrent_update(service_id):
    """Two sessions load version 1; the second writer must fail with StaleDataError."""
    maker = get_sessionmaker()
    async with maker() as s1, maker() as s2:
        a = await ServiceRepository(s1).get(service_id, with_relations=False)
        b = await ServiceRepository(s2).get(service_id, with_relations=False)
        assert a.version == b.version == 1
        a.status_reason = "first writer"
        await s1.commit()
        b.status_reason = "second writer"
        with pytest.raises(StaleDataError):
            await s2.flush()
        await s2.rollback()


# --------------------------------------------------------------------------- allocation pools


async def test_try_allocate_conflict_returns_none_and_keeps_transaction(service_id, db):
    async with transaction() as session:
        devices = await DeviceRepository(session).get_many_by_hostnames(["PE1"])
        other = await create_service(
            session, acme_request("ACME-L3VPN-002", vlan=200, devices=("PE1",)), devices
        )
        other_id = other.id
    async with transaction() as session:
        repo = AllocationRepository(session)
        first = await repo.try_allocate(service_id, ResourceType.ROUTE_TARGET, "65000:100")
        assert first is not None
        # Work done before the losing INSERT must survive.
        await ServiceRepository(session).transition(service_id, LifecycleStatus.VALIDATING)
        lost = await repo.try_allocate(other_id, ResourceType.ROUTE_TARGET, "65000:100")
        assert lost is None
        # The enclosing transaction is still usable and its earlier writes intact.
        again = await repo.try_allocate(other_id, ResourceType.ROUTE_TARGET, "65000:101")
        assert again is not None
    async with transaction() as session:
        svc = await ServiceRepository(session).get(service_id)
        assert svc.lifecycle_status == LifecycleStatus.VALIDATING
        assert [a.resource_value for a in svc.allocations] == ["65000:100"]
        rows = await AllocationRepository(session).get_for_service(other_id)
        assert [a.resource_value for a in rows] == ["65000:101"]


async def test_allocate_next_skips_taken_and_is_idempotent(service_id, db):
    async with transaction() as session:
        devices = await DeviceRepository(session).get_many_by_hostnames(["PE1"])
        other = await create_service(
            session, acme_request("ACME-L3VPN-002", vlan=200, devices=("PE1",)), devices
        )
        repo = AllocationRepository(session)
        assert await repo.try_allocate(other.id, ResourceType.ROUTE_DISTINGUISHER, "65000:1000") is not None
        assert await repo.try_allocate(other.id, ResourceType.ROUTE_DISTINGUISHER, "65000:1001") is not None
        candidates = [f"65000:{n}" for n in range(1000, 1010)]
        mine = await repo.allocate_next(service_id, ResourceType.ROUTE_DISTINGUISHER, candidates)
        assert mine.resource_value == "65000:1002"
        rerun = await repo.allocate_next(service_id, ResourceType.ROUTE_DISTINGUISHER, candidates)
        assert rerun.id == mine.id  # same row, nothing new claimed
        total = (await session.execute(select(ResourceAllocation))).scalars().all()
        assert len(total) == 3


async def test_allocate_next_pool_exhausted(service_id, db):
    async with transaction() as session:
        repo = AllocationRepository(session)
        with pytest.raises(ResourcePoolExhaustedError):
            await repo.allocate_next(service_id, ResourceType.VLAN_ID, [], scope="device:PE1")


async def test_release_frees_slot_for_reuse(service_id, db):
    async with transaction() as session:
        devices = await DeviceRepository(session).get_many_by_hostnames(["PE1"])
        other = await create_service(
            session, acme_request("ACME-L3VPN-002", vlan=200, devices=("PE1",)), devices
        )
        other_id = other.id
        repo = AllocationRepository(session)
        assert await repo.try_allocate(service_id, ResourceType.VLAN_ID, "100", scope="device:PE1")
        assert await repo.try_allocate(service_id, ResourceType.ROUTE_TARGET, "65000:100")
        assert await repo.confirm(service_id) == 2
        assert await repo.try_allocate(other_id, ResourceType.VLAN_ID, "100", scope="device:PE1") is None
    async with transaction() as session:
        repo = AllocationRepository(session)
        assert await repo.release(service_id) == 2
        assert await repo.release(service_id) == 0  # idempotent
        assert await repo.get_for_service(service_id) == []
        reused = await repo.try_allocate(other_id, ResourceType.VLAN_ID, "100", scope="device:PE1")
        assert reused is not None


# --------------------------------------------------------------------------- workflow + device-op ledger


async def test_workflow_record_start_is_idempotent(service_id):
    async with transaction() as session:
        repo = WorkflowRepository(session)
        a = await repo.record_start(
            service_id=service_id,
            workflow_id="wf-1",
            workflow_type=WorkflowType.PROVISION_L3VPN,
            task_queue="q",
        )
        b = await repo.record_start(
            service_id=service_id,
            workflow_id="wf-1",
            workflow_type=WorkflowType.PROVISION_L3VPN,
            task_queue="q",
            run_id="run-2",
        )
        assert a.id == b.id and b.run_id == "run-2"
        await repo.set_stage("wf-1", "PROVISIONING")
        done = await repo.record_finish("wf-1", WorkflowStatus.COMPLETED, result={"ok": True})
        assert done.current_stage == "PROVISIONING"
        assert done.finished_at is not None and done.result == {"ok": True}


async def test_device_operation_get_or_create_idempotent_and_saga_list(service_id):
    async with transaction() as session:
        await WorkflowRepository(session).record_start(
            service_id=service_id,
            workflow_id="wf-1",
            workflow_type=WorkflowType.PROVISION_L3VPN,
            task_queue="q",
        )
        svc = await ServiceRepository(session).get(service_id)
        pe1, pe2, pe3 = (ep.device_id for ep in svc.endpoints)
        ops = DeviceOperationRepository(session)

        op1, created = await ops.get_or_create(
            service_id=service_id,
            workflow_id="wf-1",
            device_id=pe1,
            endpoint_id=svc.endpoints[0].id,
            operation_type=DeviceOperationType.APPLY,
            idempotency_key="k1",
            request_payload={"c": 1},
        )
        assert created
        op1_again, created_again = await ops.get_or_create(
            service_id=service_id,
            workflow_id="wf-1",
            device_id=pe1,
            endpoint_id=svc.endpoints[0].id,
            operation_type=DeviceOperationType.APPLY,
            idempotency_key="k1",
            request_payload={"c": 1},
        )
        assert not created_again and op1_again.id == op1.id
        await ops.mark(op1.id, DeviceOperationStatus.SUCCEEDED, adapter_operation_id="commit-1")

        op2, _ = await ops.get_or_create(
            service_id=service_id,
            workflow_id="wf-1",
            device_id=pe2,
            endpoint_id=None,
            operation_type=DeviceOperationType.APPLY,
            idempotency_key="k1",
            request_payload={},
        )
        await ops.mark(op2.id, DeviceOperationStatus.SUCCEEDED)
        await ops.mark(op2.id, DeviceOperationStatus.COMPENSATED)  # already undone

        op3, _ = await ops.get_or_create(
            service_id=service_id,
            workflow_id="wf-1",
            device_id=pe3,
            endpoint_id=None,
            operation_type=DeviceOperationType.APPLY,
            idempotency_key="k1",
            request_payload={},
        )
        failed = await ops.mark(
            op3.id, DeviceOperationStatus.FAILED, error="PE3 down", failure_kind="TRANSIENT"
        )
        assert failed.finished_at is not None

        verify, _ = await ops.get_or_create(
            service_id=service_id,
            workflow_id="wf-1",
            device_id=pe1,
            endpoint_id=None,
            operation_type=DeviceOperationType.VERIFY,
            idempotency_key="k1",
            request_payload={},
        )
        await ops.mark(verify.id, DeviceOperationStatus.SUCCEEDED)

        undo = await ops.succeeded_applies(service_id, "wf-1")
        assert [o.id for o in undo] == [op1.id]
        assert len(await ops.list_for_service(service_id, workflow_id="wf-1")) == 4
        assert len(await ops.list_for_service(service_id, status=DeviceOperationStatus.FAILED)) == 1


# --------------------------------------------------------------------------- audit + idempotency


async def test_audit_history_is_chronological(service_id):
    async with transaction() as session:
        audit = AuditRepository(session)
        base = datetime(2026, 1, 1, tzinfo=UTC)
        # Insert out of order on purpose; history must sort by occurred_at.
        for offset in (3, 1, 2):
            ev = await audit.record(
                AuditEventType.DEVICE_OPERATION, service_id=service_id, message=f"ev{offset}"
            )
            ev.occurred_at = base + timedelta(minutes=offset)
        await session.flush()
    async with transaction() as session:
        rows, total = await AuditRepository(session).history(service_id)
        assert total == 3
        assert [r.message for r in rows] == ["ev1", "ev2", "ev3"]
        page, total = await AuditRepository(session).history(service_id, limit=1, offset=1)
        assert total == 3 and [r.message for r in page] == ["ev2"]


async def test_idempotency_repository_roundtrip(db):
    now = datetime.now(UTC)
    async with transaction() as session:
        repo = IdempotencyRepository(session)
        await repo.put("k-1", "h1", 202, {"service_id": "x"}, now + timedelta(hours=1))
        await repo.put("k-1", "h1", 202, {"service_id": "y"}, now + timedelta(hours=1))  # upsert
        await repo.put("k-old", "h2", 200, {}, now - timedelta(seconds=1))
    async with transaction() as session:
        repo = IdempotencyRepository(session)
        rec = await repo.get("k-1")
        assert rec is not None and rec.response_body == {"service_id": "y"} and rec.status_code == 202
        assert await repo.purge_expired(now) == 1
        assert await repo.get("k-old") is None
        assert await repo.get("k-1") is not None


async def test_service_list_filters_and_counts(service_id, db):
    async with transaction() as session:
        devices = await DeviceRepository(session).get_many_by_hostnames(["PE1"])
        other = await create_service(
            session, acme_request("ACME-L3VPN-002", vlan=200, devices=("PE1",)), devices
        )
        await ServiceRepository(session).transition(other.id, LifecycleStatus.VALIDATING)
    async with transaction() as session:
        repo = ServiceRepository(session)
        rows, total = await repo.list()
        assert total == 2 and len(rows) == 2
        rows, total = await repo.list(status=LifecycleStatus.VALIDATING)
        assert total == 1 and rows[0].service_name == "ACME-L3VPN-002"
        rows, total = await repo.list(limit=1, offset=1)
        assert total == 2 and len(rows) == 1
        assert isinstance(rows[0], NetworkService)
        pe_rows, pe_total = await DeviceRepository(session).list(role=DeviceRole.PE)
        assert pe_total == 3 and [d.hostname for d in pe_rows] == ["PE1", "PE2", "PE3"]
