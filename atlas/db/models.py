"""SQLAlchemy 2.x declarative models — the persistent service inventory.

Design notes
------------
* Every table has a UUID primary key generated application-side (works identically on
  PostgreSQL and the SQLite used in unit tests).
* ``network_services.version`` implements optimistic locking.
* ``resource_allocations`` has a UNIQUE(resource_type, resource_value, scope) constraint —
  this is what makes allocation concurrency-safe: two workflows cannot hold the same RD,
  RT, VLAN (per device) or subnet. The repository layer catches ``IntegrityError`` and
  retries with the next candidate value.
* ``device_operations`` is the per-device audit/ledger used by Saga compensation to know
  exactly which operations succeeded and what to undo.
* No credentials are stored anywhere in this schema (see ``atlas.adapters.credentials``).
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from atlas.core.enums import (
    AllocationStatus,
    AuditEventType,
    DeviceOperationalStatus,
    DeviceOperationStatus,
    DeviceOperationType,
    DeviceRole,
    EndpointStatus,
    LifecycleStatus,
    ResourceType,
    RoutingProtocol,
    ServiceType,
    Vendor,
    WorkflowStatus,
    WorkflowType,
)

# JSONB on PostgreSQL, plain JSON elsewhere (SQLite in tests).
JSONType = JSON().with_variant(JSONB(), "postgresql")


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _uuid() -> uuid.UUID:
    return uuid.uuid4()


class Base(DeclarativeBase):
    type_annotation_map = {dict[str, Any]: JSONType, list[Any]: JSONType}


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, onupdate=_utcnow, server_default=func.now()
    )


class Customer(Base, TimestampMixin):
    __tablename__ = "customers"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=_uuid)
    name: Mapped[str] = mapped_column(String(255), unique=True)
    external_ref: Mapped[str | None] = mapped_column(String(255))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)

    services: Mapped[list[NetworkService]] = relationship(back_populates="customer")


class Site(Base, TimestampMixin):
    __tablename__ = "sites"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=_uuid)
    name: Mapped[str] = mapped_column(String(255), unique=True)
    location: Mapped[str | None] = mapped_column(String(255))

    devices: Mapped[list[NetworkDevice]] = relationship(back_populates="site")


class NetworkDevice(Base, TimestampMixin):
    __tablename__ = "network_devices"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=_uuid)
    hostname: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    management_ip: Mapped[str] = mapped_column(String(64))
    vendor: Mapped[Vendor] = mapped_column(String(32))
    platform: Mapped[str] = mapped_column(String(64))
    software_version: Mapped[str] = mapped_column(String(64))
    role: Mapped[DeviceRole] = mapped_column(String(8), index=True)
    site_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("sites.id", ondelete="SET NULL"))
    operational_status: Mapped[DeviceOperationalStatus] = mapped_column(
        String(16), default=DeviceOperationalStatus.UP
    )
    loopback_ip: Mapped[str | None] = mapped_column(String(64))
    bgp_asn: Mapped[int | None] = mapped_column(Integer)
    attributes: Mapped[dict[str, Any]] = mapped_column(default=dict)

    site: Mapped[Site | None] = relationship(back_populates="devices")
    endpoints: Mapped[list[ServiceEndpoint]] = relationship(back_populates="device")


class NetworkLink(Base, TimestampMixin):
    """Physical/logical adjacency between two devices (core topology for the topology view)."""

    __tablename__ = "network_links"
    __table_args__ = (
        UniqueConstraint("a_device_id", "a_interface", name="uq_link_a_interface"),
        UniqueConstraint("b_device_id", "b_interface", name="uq_link_b_interface"),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=_uuid)
    a_device_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("network_devices.id", ondelete="CASCADE"), index=True
    )
    a_interface: Mapped[str] = mapped_column(String(128))
    b_device_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("network_devices.id", ondelete="CASCADE"), index=True
    )
    b_interface: Mapped[str] = mapped_column(String(128))
    link_type: Mapped[str] = mapped_column(String(16), default="core")  # core | edge | rr
    bandwidth_gbps: Mapped[int | None] = mapped_column(Integer)
    igp_metric: Mapped[int | None] = mapped_column(Integer)
    operational_status: Mapped[DeviceOperationalStatus] = mapped_column(
        String(16), default=DeviceOperationalStatus.UP
    )

    a_device: Mapped[NetworkDevice] = relationship(foreign_keys=[a_device_id])
    b_device: Mapped[NetworkDevice] = relationship(foreign_keys=[b_device_id])


class NetworkService(Base, TimestampMixin):
    __tablename__ = "network_services"
    __table_args__ = (
        UniqueConstraint("customer_id", "service_name", name="uq_service_customer_name"),
        Index("ix_services_customer_status", "customer_id", "lifecycle_status"),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=_uuid)
    customer_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("customers.id", ondelete="RESTRICT"), index=True
    )
    service_type: Mapped[ServiceType] = mapped_column(String(32), index=True)
    service_name: Mapped[str] = mapped_column(String(255))
    desired_state: Mapped[dict[str, Any]] = mapped_column(default=dict)
    observed_state: Mapped[dict[str, Any]] = mapped_column(default=dict)
    lifecycle_status: Mapped[LifecycleStatus] = mapped_column(
        String(40), default=LifecycleStatus.REQUESTED, index=True
    )
    status_reason: Mapped[str | None] = mapped_column(Text)
    version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    idempotency_key: Mapped[str | None] = mapped_column(String(128), unique=True)
    active_workflow_id: Mapped[str | None] = mapped_column(String(255), index=True)

    __mapper_args__ = {"version_id_col": version}

    customer: Mapped[Customer] = relationship(back_populates="services")
    endpoints: Mapped[list[ServiceEndpoint]] = relationship(
        back_populates="service", cascade="all, delete-orphan", order_by="ServiceEndpoint.sequence"
    )
    allocations: Mapped[list[ResourceAllocation]] = relationship(
        back_populates="service", cascade="all, delete-orphan"
    )
    workflow_executions: Mapped[list[WorkflowExecution]] = relationship(back_populates="service")
    audit_events: Mapped[list[AuditEvent]] = relationship(back_populates="service")


class ServiceEndpoint(Base, TimestampMixin):
    __tablename__ = "service_endpoints"
    __table_args__ = (
        UniqueConstraint("device_id", "interface", "vlan_id", name="uq_endpoint_device_interface_vlan"),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=_uuid)
    service_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("network_services.id", ondelete="CASCADE"), index=True
    )
    device_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("network_devices.id", ondelete="RESTRICT"), index=True
    )
    sequence: Mapped[int] = mapped_column(Integer, default=0)
    interface: Mapped[str] = mapped_column(String(128))
    vlan_id: Mapped[int | None] = mapped_column(Integer)
    vrf_name: Mapped[str] = mapped_column(String(64))
    route_distinguisher: Mapped[str | None] = mapped_column(String(32))
    ce_ip_address: Mapped[str | None] = mapped_column(String(64))
    pe_ip_address: Mapped[str | None] = mapped_column(String(64))
    routing_protocol: Mapped[RoutingProtocol] = mapped_column(String(16), default=RoutingProtocol.STATIC)
    ce_asn: Mapped[int | None] = mapped_column(Integer)
    status: Mapped[EndpointStatus] = mapped_column(String(32), default=EndpointStatus.PENDING, index=True)
    status_reason: Mapped[str | None] = mapped_column(Text)
    rendered_config: Mapped[str | None] = mapped_column(Text)
    config_checksum: Mapped[str | None] = mapped_column(String(64))

    service: Mapped[NetworkService] = relationship(back_populates="endpoints")
    device: Mapped[NetworkDevice] = relationship(back_populates="endpoints")
    operations: Mapped[list[DeviceOperation]] = relationship(back_populates="endpoint")


class ResourceAllocation(Base):
    __tablename__ = "resource_allocations"
    __table_args__ = (
        # The concurrency guarantee: the DB rejects a duplicate (type, value, scope).
        UniqueConstraint("resource_type", "resource_value", "scope", name="uq_allocation_type_value_scope"),
        Index("ix_allocations_service_type", "service_id", "resource_type"),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=_uuid)
    service_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("network_services.id", ondelete="CASCADE"), index=True
    )
    resource_type: Mapped[ResourceType] = mapped_column(String(32))
    resource_value: Mapped[str] = mapped_column(String(128))
    scope: Mapped[str] = mapped_column(String(128), default="global")
    """Scope narrows uniqueness: 'global' for RD/RT/VRF/subnet, 'device:<hostname>' for VLANs."""
    allocation_status: Mapped[AllocationStatus] = mapped_column(String(16), default=AllocationStatus.RESERVED)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, server_default=func.now()
    )
    released_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    service: Mapped[NetworkService] = relationship(back_populates="allocations")


class WorkflowExecution(Base, TimestampMixin):
    __tablename__ = "workflow_executions"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=_uuid)
    service_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("network_services.id", ondelete="CASCADE"), index=True
    )
    workflow_id: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    run_id: Mapped[str | None] = mapped_column(String(255))
    workflow_type: Mapped[WorkflowType] = mapped_column(String(64))
    task_queue: Mapped[str] = mapped_column(String(128))
    status: Mapped[WorkflowStatus] = mapped_column(String(16), default=WorkflowStatus.RUNNING, index=True)
    current_stage: Mapped[str | None] = mapped_column(String(64))
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    result: Mapped[dict[str, Any]] = mapped_column(default=dict)
    error: Mapped[str | None] = mapped_column(Text)

    service: Mapped[NetworkService] = relationship(back_populates="workflow_executions")
    operations: Mapped[list[DeviceOperation]] = relationship(back_populates="workflow_execution")


class DeviceOperation(Base, TimestampMixin):
    """Ledger of every device-touching operation; the Saga reads this to compensate."""

    __tablename__ = "device_operations"
    __table_args__ = (
        # Idempotency: the same (workflow, device, op, attempt-key) is recorded once.
        UniqueConstraint(
            "workflow_id", "device_id", "operation_type", "idempotency_key", name="uq_device_op_idem"
        ),
        Index("ix_device_ops_service_device", "service_id", "device_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=_uuid)
    service_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("network_services.id", ondelete="CASCADE"), index=True
    )
    endpoint_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("service_endpoints.id", ondelete="SET NULL")
    )
    device_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("network_devices.id", ondelete="RESTRICT"), index=True
    )
    workflow_id: Mapped[str] = mapped_column(
        ForeignKey("workflow_executions.workflow_id", ondelete="CASCADE"), index=True
    )
    operation_type: Mapped[DeviceOperationType] = mapped_column(String(16))
    idempotency_key: Mapped[str] = mapped_column(String(128))
    status: Mapped[DeviceOperationStatus] = mapped_column(
        String(16), default=DeviceOperationStatus.PENDING, index=True
    )
    attempt: Mapped[int] = mapped_column(Integer, default=1)
    adapter_operation_id: Mapped[str | None] = mapped_column(String(255))
    """Opaque handle returned by the adapter (e.g. IOS XR commit id) used for rollback."""
    request_payload: Mapped[dict[str, Any]] = mapped_column(default=dict)
    response_payload: Mapped[dict[str, Any]] = mapped_column(default=dict)
    error: Mapped[str | None] = mapped_column(Text)
    failure_kind: Mapped[str | None] = mapped_column(String(16))
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    workflow_execution: Mapped[WorkflowExecution] = relationship(back_populates="operations")
    endpoint: Mapped[ServiceEndpoint | None] = relationship(back_populates="operations")


class AuditEvent(Base):
    __tablename__ = "audit_events"
    __table_args__ = (Index("ix_audit_service_time", "service_id", "occurred_at"),)

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=_uuid)
    service_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("network_services.id", ondelete="CASCADE"), index=True
    )
    workflow_id: Mapped[str | None] = mapped_column(String(255), index=True)
    device_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("network_devices.id", ondelete="SET NULL"))
    event_type: Mapped[AuditEventType] = mapped_column(String(48), index=True)
    actor: Mapped[str] = mapped_column(String(128), default="system")
    correlation_id: Mapped[str | None] = mapped_column(String(128), index=True)
    from_status: Mapped[str | None] = mapped_column(String(40))
    to_status: Mapped[str | None] = mapped_column(String(40))
    message: Mapped[str] = mapped_column(Text, default="")
    details: Mapped[dict[str, Any]] = mapped_column(default=dict)
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, server_default=func.now()
    )

    service: Mapped[NetworkService | None] = relationship(back_populates="audit_events")


class IdempotencyRecord(Base):
    """Stores the response of a mutating API call keyed by ``Idempotency-Key`` header."""

    __tablename__ = "idempotency_records"

    key: Mapped[str] = mapped_column(String(128), primary_key=True)
    request_hash: Mapped[str] = mapped_column(String(64))
    status_code: Mapped[int] = mapped_column(Integer)
    response_body: Mapped[dict[str, Any]] = mapped_column(default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, server_default=func.now()
    )
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
