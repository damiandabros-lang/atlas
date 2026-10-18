"""Repository layer — the ONLY place that issues SQL.

INTERFACE CONTRACT (implemented by the DB agent, consumed by activities/API):
all methods take an ``AsyncSession`` and never commit; the caller (unit-of-work in
``atlas.db.session.transaction()``) owns the transaction boundary.

Concurrency notes
-----------------
* Optimistic locking: ``NetworkService.version`` is the mapper's ``version_id_col``. Every
  flush of a dirty service row emits ``UPDATE ... WHERE id=? AND version=?`` and raises
  ``StaleDataError`` if another transaction bumped it first. ``transition()`` flushes so
  the bump is visible immediately to the caller.
* Pessimistic locking: ``get_for_update()`` adds ``FOR UPDATE`` only on PostgreSQL. SQLite
  serialises writers at the file level, so the clause is both unsupported and unnecessary.
* Resource pools: uniqueness of ``(resource_type, resource_value, scope)`` is enforced by the
  database, not by application checks. ``try_allocate`` wraps the INSERT in a SAVEPOINT so a
  losing race (IntegrityError) rolls back only the savepoint and the enclosing transaction
  — with all the work done so far — survives intact.
* Device-operation ledger: ``get_or_create`` is keyed on the UNIQUE constraint so a retried
  Temporal activity (same idempotency key) sees the earlier attempt instead of creating a
  second one.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import Select, delete, func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from atlas.core.enums import (
    AllocationStatus,
    AuditEventType,
    DeviceOperationStatus,
    DeviceOperationType,
    DeviceRole,
    EndpointStatus,
    LifecycleStatus,
    ResourceType,
    WorkflowStatus,
    WorkflowType,
)
from atlas.core.lifecycle import assert_transition
from atlas.db.models import (
    AuditEvent,
    Customer,
    DeviceOperation,
    IdempotencyRecord,
    NetworkDevice,
    NetworkService,
    ResourceAllocation,
    ServiceEndpoint,
    WorkflowExecution,
)


def _utcnow() -> datetime:
    return datetime.now(UTC)


async def _count(session: AsyncSession, stmt: Select[Any]) -> int:
    """Total row count for a (possibly filtered) select, ignoring limit/offset."""
    count_stmt = select(func.count()).select_from(stmt.order_by(None).limit(None).offset(None).subquery())
    return int((await session.execute(count_stmt)).scalar_one())


class CustomerRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get(self, customer_id: UUID) -> Customer | None:
        return await self.session.get(Customer, customer_id)

    async def get_by_name(self, name: str) -> Customer | None:
        stmt = select(Customer).where(Customer.name == name)
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def create(self, name: str, external_ref: str | None = None) -> Customer:
        customer = Customer(name=name, external_ref=external_ref)
        self.session.add(customer)
        await self.session.flush()
        return customer


class DeviceRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get(self, device_id: UUID) -> NetworkDevice | None:
        return await self.session.get(NetworkDevice, device_id)

    async def get_by_hostname(self, hostname: str) -> NetworkDevice | None:
        """Case-insensitive hostname lookup."""
        stmt = select(NetworkDevice).where(func.upper(NetworkDevice.hostname) == hostname.upper())
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def get_many_by_hostnames(self, hostnames: Sequence[str]) -> dict[str, NetworkDevice]:
        """Returns {UPPERCASE hostname: device}."""
        wanted = {h.upper() for h in hostnames}
        if not wanted:
            return {}
        stmt = select(NetworkDevice).where(func.upper(NetworkDevice.hostname).in_(wanted))
        rows = (await self.session.execute(stmt)).scalars().all()
        return {d.hostname.upper(): d for d in rows}

    async def list(
        self, *, role: DeviceRole | None = None, limit: int = 100, offset: int = 0
    ) -> tuple[list[NetworkDevice], int]:
        stmt = select(NetworkDevice)
        if role is not None:
            stmt = stmt.where(NetworkDevice.role == role)
        total = await _count(self.session, stmt)
        stmt = stmt.order_by(NetworkDevice.hostname).limit(limit).offset(offset)
        rows = list((await self.session.execute(stmt)).scalars().all())
        return rows, total

    async def upsert(self, **fields: Any) -> NetworkDevice:
        """Insert or update by hostname (used by seed)."""
        hostname = fields.get("hostname")
        if not hostname:
            raise ValueError("upsert requires 'hostname'")
        device = await self.get_by_hostname(hostname)
        if device is None:
            device = NetworkDevice(**fields)
            self.session.add(device)
        else:
            for key, value in fields.items():
                setattr(device, key, value)
        await self.session.flush()
        return device


class ServiceRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    @staticmethod
    def _with_relations(stmt: Select[tuple[NetworkService]]) -> Select[tuple[NetworkService]]:
        return stmt.options(
            selectinload(NetworkService.endpoints).selectinload(ServiceEndpoint.device),
            selectinload(NetworkService.allocations),
        )

    async def get(self, service_id: UUID, *, with_relations: bool = True) -> NetworkService | None:
        """Eager-loads endpoints (+device), allocations when ``with_relations``."""
        stmt = select(NetworkService).where(NetworkService.id == service_id)
        if with_relations:
            stmt = self._with_relations(stmt)
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def get_for_update(self, service_id: UUID) -> NetworkService | None:
        """SELECT ... FOR UPDATE on PostgreSQL (no-op lock on SQLite)."""
        stmt = select(NetworkService).where(NetworkService.id == service_id)
        if self.session.get_bind().dialect.name == "postgresql":
            stmt = stmt.with_for_update()
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def get_by_idempotency_key(self, key: str) -> NetworkService | None:
        stmt = self._with_relations(select(NetworkService).where(NetworkService.idempotency_key == key))
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def list(
        self,
        *,
        customer_id: UUID | None = None,
        status: LifecycleStatus | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[NetworkService], int]:
        stmt = select(NetworkService)
        if customer_id is not None:
            stmt = stmt.where(NetworkService.customer_id == customer_id)
        if status is not None:
            stmt = stmt.where(NetworkService.lifecycle_status == status)
        total = await _count(self.session, stmt)
        stmt = stmt.order_by(NetworkService.created_at.desc(), NetworkService.id).limit(limit).offset(offset)
        rows = list((await self.session.execute(stmt)).scalars().all())
        return rows, total

    async def create(
        self,
        *,
        customer_id: UUID,
        service_name: str,
        service_type: str,
        desired_state: dict[str, Any],
        idempotency_key: str | None = None,
    ) -> NetworkService:
        service = NetworkService(
            customer_id=customer_id,
            service_name=service_name,
            service_type=service_type,
            desired_state=desired_state,
            lifecycle_status=LifecycleStatus.REQUESTED,
            idempotency_key=idempotency_key,
        )
        self.session.add(service)
        await self.session.flush()
        return service

    async def add_endpoint(self, service: NetworkService, **fields: Any) -> ServiceEndpoint:
        if "sequence" not in fields:
            fields["sequence"] = len(service.endpoints)
        endpoint = ServiceEndpoint(service=service, **fields)
        self.session.add(endpoint)
        await self.session.flush()
        return endpoint

    async def _require(self, service_id: UUID) -> NetworkService:
        service = await self.get(service_id, with_relations=False)
        if service is None:
            raise LookupError(f"service {service_id} not found")
        return service

    async def transition(
        self,
        service_id: UUID,
        target: LifecycleStatus,
        *,
        reason: str | None = None,
        correlation_id: str | None = None,
        workflow_id: str | None = None,
        actor: str = "system",
        allow_same: bool = True,
    ) -> NetworkService:
        """Validates the transition via ``atlas.core.lifecycle``, bumps ``version`` (optimistic lock),
        writes a STATUS_CHANGED audit event. ``allow_same`` makes repeated activity execution idempotent."""
        service = await self._require(service_id)
        current = LifecycleStatus(service.lifecycle_status)
        target = LifecycleStatus(target)
        if current == target and allow_same:
            # Idempotent re-execution (e.g. a retried Temporal activity): nothing to do, no
            # version bump, no duplicate audit row.
            return service
        assert_transition(current, target)
        service.lifecycle_status = target
        service.status_reason = reason
        if workflow_id is not None:
            service.active_workflow_id = workflow_id
        self.session.add(
            AuditEvent(
                service_id=service.id,
                workflow_id=workflow_id or service.active_workflow_id,
                event_type=AuditEventType.STATUS_CHANGED,
                actor=actor,
                correlation_id=correlation_id,
                from_status=current.value,
                to_status=target.value,
                message=reason or f"{current.value} -> {target.value}",
                details={"reason": reason} if reason else {},
            )
        )
        # Flush now: the version_id_col bump happens on UPDATE, and a concurrent writer that
        # already moved the row raises StaleDataError here rather than at commit time.
        await self.session.flush()
        return service

    async def set_observed_state(self, service_id: UUID, observed: dict[str, Any]) -> None:
        service = await self._require(service_id)
        service.observed_state = observed
        await self.session.flush()

    async def set_active_workflow(self, service_id: UUID, workflow_id: str | None) -> None:
        service = await self._require(service_id)
        service.active_workflow_id = workflow_id
        await self.session.flush()

    async def update_endpoint(self, endpoint_id: UUID, **fields: Any) -> ServiceEndpoint:
        endpoint = await self.session.get(ServiceEndpoint, endpoint_id)
        if endpoint is None:
            raise LookupError(f"endpoint {endpoint_id} not found")
        for key, value in fields.items():
            setattr(endpoint, key, value)
        await self.session.flush()
        return endpoint

    async def set_endpoint_status(
        self, endpoint_id: UUID, status: EndpointStatus, reason: str | None = None
    ) -> ServiceEndpoint:
        return await self.update_endpoint(endpoint_id, status=status, status_reason=reason)


class AllocationRepository:
    """Concurrency-safe resource pools backed by the UNIQUE constraint on resource_allocations."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get_for_service(self, service_id: UUID) -> list[ResourceAllocation]:
        stmt = (
            select(ResourceAllocation)
            .where(ResourceAllocation.service_id == service_id)
            .order_by(
                ResourceAllocation.resource_type, ResourceAllocation.scope, ResourceAllocation.resource_value
            )
        )
        return list((await self.session.execute(stmt)).scalars().all())

    async def find(
        self, service_id: UUID, resource_type: ResourceType, scope: str = "global"
    ) -> ResourceAllocation | None:
        """Existing allocation of this type for this service (makes allocation idempotent)."""
        stmt = (
            select(ResourceAllocation)
            .where(
                ResourceAllocation.service_id == service_id,
                ResourceAllocation.resource_type == resource_type,
                ResourceAllocation.scope == scope,
            )
            .order_by(ResourceAllocation.created_at)
            .limit(1)
        )
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def try_allocate(
        self, service_id: UUID, resource_type: ResourceType, value: str, scope: str = "global"
    ) -> ResourceAllocation | None:
        """Attempt to claim ``value``. Returns None if already taken by another service (uses a
        SAVEPOINT so the enclosing transaction survives the IntegrityError)."""
        allocation = ResourceAllocation(
            service_id=service_id,
            resource_type=resource_type,
            resource_value=value,
            scope=scope,
            allocation_status=AllocationStatus.RESERVED,
        )
        try:
            async with self.session.begin_nested():
                self.session.add(allocation)
                await self.session.flush()
        except IntegrityError:
            # The SAVEPOINT was rolled back and the pending object expunged; the outer
            # transaction is still usable. Somebody else owns this (type, value, scope).
            return None
        return allocation

    async def allocate_next(
        self,
        service_id: UUID,
        resource_type: ResourceType,
        candidates: Any,  # Iterable[str]
        scope: str = "global",
    ) -> ResourceAllocation:
        """Walk ``candidates`` and claim the first free one. Raises ``ResourcePoolExhaustedError``."""
        existing = await self.find(service_id, resource_type, scope)
        if existing is not None:
            return existing
        for candidate in candidates:
            allocation = await self.try_allocate(service_id, resource_type, str(candidate), scope)
            if allocation is not None:
                return allocation
        raise ResourcePoolExhaustedError(f"no free {resource_type} in scope {scope!r}")

    async def confirm(self, service_id: UUID) -> int:
        """RESERVED -> ALLOCATED for all of a service's allocations. Returns count."""
        stmt = (
            update(ResourceAllocation)
            .where(
                ResourceAllocation.service_id == service_id,
                ResourceAllocation.allocation_status == AllocationStatus.RESERVED,
            )
            .values(allocation_status=AllocationStatus.ALLOCATED)
            .execution_options(synchronize_session="fetch")
        )
        result = await self.session.execute(stmt)
        return int(result.rowcount or 0)

    async def release(self, service_id: UUID) -> int:
        """Mark all allocations RELEASED and free the (type,value,scope) slot by deleting rows.
        Idempotent. Returns count released."""
        rows = await self.get_for_service(service_id)
        now = _utcnow()
        for row in rows:
            # Mark then delete: the in-memory object reflects RELEASED for any caller still
            # holding a reference, while the row removal frees the unique slot for reuse.
            row.allocation_status = AllocationStatus.RELEASED
            row.released_at = now
            await self.session.delete(row)
        await self.session.flush()
        return len(rows)


class ResourcePoolExhaustedError(RuntimeError):
    pass


class WorkflowRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get_by_workflow_id(self, workflow_id: str) -> WorkflowExecution | None:
        stmt = select(WorkflowExecution).where(WorkflowExecution.workflow_id == workflow_id)
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def record_start(
        self,
        *,
        service_id: UUID,
        workflow_id: str,
        workflow_type: WorkflowType,
        task_queue: str,
        run_id: str | None = None,
    ) -> WorkflowExecution:
        """Idempotent: returns the existing row when workflow_id already recorded."""
        existing = await self.get_by_workflow_id(workflow_id)
        if existing is not None:
            if run_id and existing.run_id != run_id:
                existing.run_id = run_id
                await self.session.flush()
            return existing
        execution = WorkflowExecution(
            service_id=service_id,
            workflow_id=workflow_id,
            run_id=run_id,
            workflow_type=workflow_type,
            task_queue=task_queue,
            status=WorkflowStatus.RUNNING,
            started_at=_utcnow(),
        )
        try:
            async with self.session.begin_nested():
                self.session.add(execution)
                await self.session.flush()
        except IntegrityError:
            # Lost a race with another worker recording the same workflow_id.
            existing = await self.get_by_workflow_id(workflow_id)
            if existing is None:  # pragma: no cover - defensive
                raise
            return existing
        return execution

    async def _require(self, workflow_id: str) -> WorkflowExecution:
        execution = await self.get_by_workflow_id(workflow_id)
        if execution is None:
            raise LookupError(f"workflow {workflow_id} not recorded")
        return execution

    async def set_stage(self, workflow_id: str, stage: str) -> None:
        execution = await self._require(workflow_id)
        execution.current_stage = stage
        await self.session.flush()

    async def record_finish(
        self,
        workflow_id: str,
        status: WorkflowStatus,
        *,
        result: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> WorkflowExecution:
        execution = await self._require(workflow_id)
        execution.status = status
        execution.finished_at = _utcnow()
        if result is not None:
            execution.result = result
        if error is not None:
            execution.error = error
        await self.session.flush()
        return execution


class DeviceOperationRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def _find(
        self, workflow_id: str, device_id: UUID, operation_type: DeviceOperationType, idempotency_key: str
    ) -> DeviceOperation | None:
        stmt = select(DeviceOperation).where(
            DeviceOperation.workflow_id == workflow_id,
            DeviceOperation.device_id == device_id,
            DeviceOperation.operation_type == operation_type,
            DeviceOperation.idempotency_key == idempotency_key,
        )
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def get_or_create(
        self,
        *,
        service_id: UUID,
        workflow_id: str,
        device_id: UUID,
        endpoint_id: UUID | None,
        operation_type: DeviceOperationType,
        idempotency_key: str,
        request_payload: dict[str, Any],
    ) -> tuple[DeviceOperation, bool]:
        """Returns (operation, created). Existing SUCCEEDED op => activity may short-circuit."""
        existing = await self._find(workflow_id, device_id, operation_type, idempotency_key)
        if existing is not None:
            return existing, False
        operation = DeviceOperation(
            service_id=service_id,
            workflow_id=workflow_id,
            device_id=device_id,
            endpoint_id=endpoint_id,
            operation_type=operation_type,
            idempotency_key=idempotency_key,
            request_payload=request_payload,
            status=DeviceOperationStatus.PENDING,
            attempt=1,
        )
        try:
            async with self.session.begin_nested():
                self.session.add(operation)
                await self.session.flush()
        except IntegrityError:
            # Two activity attempts raced on the UNIQUE(workflow, device, op, key) ledger key.
            existing = await self._find(workflow_id, device_id, operation_type, idempotency_key)
            if existing is None:  # pragma: no cover - defensive
                raise
            return existing, False
        return operation, True

    async def mark(
        self,
        operation_id: UUID,
        status: DeviceOperationStatus,
        *,
        adapter_operation_id: str | None = None,
        response_payload: dict[str, Any] | None = None,
        error: str | None = None,
        failure_kind: str | None = None,
        attempt: int | None = None,
    ) -> DeviceOperation:
        operation = await self.session.get(DeviceOperation, operation_id)
        if operation is None:
            raise LookupError(f"device operation {operation_id} not found")
        now = _utcnow()
        operation.status = status
        if status == DeviceOperationStatus.IN_PROGRESS:
            operation.started_at = operation.started_at or now
        elif status in (
            DeviceOperationStatus.SUCCEEDED,
            DeviceOperationStatus.FAILED,
            DeviceOperationStatus.COMPENSATED,
            DeviceOperationStatus.SKIPPED,
        ):
            operation.started_at = operation.started_at or now
            operation.finished_at = now
        if adapter_operation_id is not None:
            operation.adapter_operation_id = adapter_operation_id
        if response_payload is not None:
            operation.response_payload = response_payload
        if error is not None:
            operation.error = error
        if failure_kind is not None:
            operation.failure_kind = failure_kind
        if attempt is not None:
            operation.attempt = attempt
        await self.session.flush()
        return operation

    async def list_for_service(
        self, service_id: UUID, *, workflow_id: str | None = None, status: DeviceOperationStatus | None = None
    ) -> list[DeviceOperation]:
        stmt = select(DeviceOperation).where(DeviceOperation.service_id == service_id)
        if workflow_id is not None:
            stmt = stmt.where(DeviceOperation.workflow_id == workflow_id)
        if status is not None:
            stmt = stmt.where(DeviceOperation.status == status)
        stmt = stmt.order_by(DeviceOperation.created_at, DeviceOperation.attempt)
        return list((await self.session.execute(stmt)).scalars().all())

    async def succeeded_applies(self, service_id: UUID, workflow_id: str) -> list[DeviceOperation]:
        """APPLY operations that SUCCEEDED and were not yet COMPENSATED — the Saga's undo list."""
        stmt = (
            select(DeviceOperation)
            .where(
                DeviceOperation.service_id == service_id,
                DeviceOperation.workflow_id == workflow_id,
                DeviceOperation.operation_type == DeviceOperationType.APPLY,
                DeviceOperation.status == DeviceOperationStatus.SUCCEEDED,
            )
            .order_by(DeviceOperation.created_at)
        )
        return list((await self.session.execute(stmt)).scalars().all())


class AuditRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def record(
        self,
        event_type: AuditEventType,
        *,
        service_id: UUID | None,
        message: str,
        workflow_id: str | None = None,
        device_id: UUID | None = None,
        actor: str = "system",
        correlation_id: str | None = None,
        from_status: str | None = None,
        to_status: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> AuditEvent:
        event = AuditEvent(
            service_id=service_id,
            workflow_id=workflow_id,
            device_id=device_id,
            event_type=event_type,
            actor=actor,
            correlation_id=correlation_id,
            from_status=from_status,
            to_status=to_status,
            message=message,
            details=details or {},
            occurred_at=_utcnow(),
        )
        self.session.add(event)
        await self.session.flush()
        return event

    async def history(
        self, service_id: UUID, *, limit: int = 200, offset: int = 0
    ) -> tuple[list[AuditEvent], int]:
        stmt = select(AuditEvent).where(AuditEvent.service_id == service_id)
        total = await _count(self.session, stmt)
        stmt = stmt.order_by(AuditEvent.occurred_at.asc()).limit(limit).offset(offset)
        rows = list((await self.session.execute(stmt)).scalars().all())
        return rows, total


class IdempotencyRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get(self, key: str) -> IdempotencyRecord | None:
        return await self.session.get(IdempotencyRecord, key)

    async def put(
        self,
        key: str,
        request_hash: str,
        status_code: int,
        response_body: dict[str, Any],
        expires_at: datetime,
    ) -> IdempotencyRecord:
        record = await self.session.merge(
            IdempotencyRecord(
                key=key,
                request_hash=request_hash,
                status_code=status_code,
                response_body=response_body,
                expires_at=expires_at,
            )
        )
        await self.session.flush()
        return record

    async def purge_expired(self, now: datetime) -> int:
        # "fetch" instead of the default "evaluate": SQLite hands back naive datetimes, which the
        # in-Python evaluator cannot compare with the tz-aware ``now``.
        stmt = (
            delete(IdempotencyRecord)
            .where(IdempotencyRecord.expires_at < now)
            .execution_options(synchronize_session="fetch")
        )
        result = await self.session.execute(stmt)
        return int(result.rowcount or 0)


__all__ = [
    "AllocationRepository",
    "AllocationStatus",
    "AuditRepository",
    "CustomerRepository",
    "DeviceOperationRepository",
    "DeviceRepository",
    "IdempotencyRepository",
    "ResourcePoolExhaustedError",
    "ServiceRepository",
    "WorkflowRepository",
]
