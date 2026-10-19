"""initial schema — customers, sites, devices, services, endpoints, allocations, workflows, device ops, audit, idempotency

Revision ID: 0001
Revises:
Create Date: 2026-10-18 00:00:00 UTC
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _json() -> sa.types.TypeEngine:
    # JSONB on PostgreSQL, plain JSON elsewhere — mirrors atlas.db.models.JSONType.
    return sa.JSON().with_variant(postgresql.JSONB(), "postgresql")


def upgrade() -> None:
    # ------------------------------------------------------------------ customers
    op.create_table(
        "customers",
        sa.Column("id", sa.Uuid(), primary_key=True, nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("external_ref", sa.String(255), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("name"),
    )

    # ------------------------------------------------------------------ sites
    op.create_table(
        "sites",
        sa.Column("id", sa.Uuid(), primary_key=True, nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("location", sa.String(255), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("name"),
    )

    # ------------------------------------------------------------------ network_devices
    op.create_table(
        "network_devices",
        sa.Column("id", sa.Uuid(), primary_key=True, nullable=False),
        sa.Column("hostname", sa.String(255), nullable=False),
        sa.Column("management_ip", sa.String(64), nullable=False),
        sa.Column("vendor", sa.String(32), nullable=False),
        sa.Column("platform", sa.String(64), nullable=False),
        sa.Column("software_version", sa.String(64), nullable=False),
        sa.Column("role", sa.String(8), nullable=False),
        sa.Column("site_id", sa.Uuid(), sa.ForeignKey("sites.id", ondelete="SET NULL"), nullable=True),
        sa.Column("operational_status", sa.String(16), nullable=False),
        sa.Column("loopback_ip", sa.String(64), nullable=True),
        sa.Column("bgp_asn", sa.Integer(), nullable=True),
        sa.Column("attributes", _json(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_network_devices_hostname", "network_devices", ["hostname"], unique=True)
    op.create_index("ix_network_devices_role", "network_devices", ["role"])

    # ------------------------------------------------------------------ network_services
    op.create_table(
        "network_services",
        sa.Column("id", sa.Uuid(), primary_key=True, nullable=False),
        sa.Column(
            "customer_id", sa.Uuid(), sa.ForeignKey("customers.id", ondelete="RESTRICT"), nullable=False
        ),
        sa.Column("service_type", sa.String(32), nullable=False),
        sa.Column("service_name", sa.String(255), nullable=False),
        sa.Column("desired_state", _json(), nullable=False),
        sa.Column("observed_state", _json(), nullable=False),
        sa.Column("lifecycle_status", sa.String(40), nullable=False),
        sa.Column("status_reason", sa.Text(), nullable=True),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("idempotency_key", sa.String(128), nullable=True),
        sa.Column("active_workflow_id", sa.String(255), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("customer_id", "service_name", name="uq_service_customer_name"),
        sa.UniqueConstraint("idempotency_key"),
    )
    op.create_index("ix_network_services_customer_id", "network_services", ["customer_id"])
    op.create_index("ix_network_services_service_type", "network_services", ["service_type"])
    op.create_index("ix_network_services_lifecycle_status", "network_services", ["lifecycle_status"])
    op.create_index("ix_network_services_active_workflow_id", "network_services", ["active_workflow_id"])
    op.create_index("ix_services_customer_status", "network_services", ["customer_id", "lifecycle_status"])

    # ------------------------------------------------------------------ service_endpoints
    op.create_table(
        "service_endpoints",
        sa.Column("id", sa.Uuid(), primary_key=True, nullable=False),
        sa.Column(
            "service_id", sa.Uuid(), sa.ForeignKey("network_services.id", ondelete="CASCADE"), nullable=False
        ),
        sa.Column(
            "device_id", sa.Uuid(), sa.ForeignKey("network_devices.id", ondelete="RESTRICT"), nullable=False
        ),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("interface", sa.String(128), nullable=False),
        sa.Column("vlan_id", sa.Integer(), nullable=True),
        sa.Column("vrf_name", sa.String(64), nullable=False),
        sa.Column("route_distinguisher", sa.String(32), nullable=True),
        sa.Column("ce_ip_address", sa.String(64), nullable=True),
        sa.Column("pe_ip_address", sa.String(64), nullable=True),
        sa.Column("routing_protocol", sa.String(16), nullable=False),
        sa.Column("ce_asn", sa.Integer(), nullable=True),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("status_reason", sa.Text(), nullable=True),
        sa.Column("rendered_config", sa.Text(), nullable=True),
        sa.Column("config_checksum", sa.String(64), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("device_id", "interface", "vlan_id", name="uq_endpoint_device_interface_vlan"),
    )
    op.create_index("ix_service_endpoints_service_id", "service_endpoints", ["service_id"])
    op.create_index("ix_service_endpoints_device_id", "service_endpoints", ["device_id"])
    op.create_index("ix_service_endpoints_status", "service_endpoints", ["status"])

    # ------------------------------------------------------------------ resource_allocations
    op.create_table(
        "resource_allocations",
        sa.Column("id", sa.Uuid(), primary_key=True, nullable=False),
        sa.Column(
            "service_id", sa.Uuid(), sa.ForeignKey("network_services.id", ondelete="CASCADE"), nullable=False
        ),
        sa.Column("resource_type", sa.String(32), nullable=False),
        sa.Column("resource_value", sa.String(128), nullable=False),
        sa.Column("scope", sa.String(128), nullable=False),
        sa.Column("allocation_status", sa.String(16), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("released_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint(
            "resource_type", "resource_value", "scope", name="uq_allocation_type_value_scope"
        ),
    )
    op.create_index("ix_resource_allocations_service_id", "resource_allocations", ["service_id"])
    op.create_index("ix_allocations_service_type", "resource_allocations", ["service_id", "resource_type"])

    # ------------------------------------------------------------------ workflow_executions
    op.create_table(
        "workflow_executions",
        sa.Column("id", sa.Uuid(), primary_key=True, nullable=False),
        sa.Column(
            "service_id", sa.Uuid(), sa.ForeignKey("network_services.id", ondelete="CASCADE"), nullable=False
        ),
        sa.Column("workflow_id", sa.String(255), nullable=False),
        sa.Column("run_id", sa.String(255), nullable=True),
        sa.Column("workflow_type", sa.String(64), nullable=False),
        sa.Column("task_queue", sa.String(128), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("current_stage", sa.String(64), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("result", _json(), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_workflow_executions_service_id", "workflow_executions", ["service_id"])
    op.create_index("ix_workflow_executions_workflow_id", "workflow_executions", ["workflow_id"], unique=True)
    op.create_index("ix_workflow_executions_status", "workflow_executions", ["status"])

    # ------------------------------------------------------------------ device_operations
    op.create_table(
        "device_operations",
        sa.Column("id", sa.Uuid(), primary_key=True, nullable=False),
        sa.Column(
            "service_id", sa.Uuid(), sa.ForeignKey("network_services.id", ondelete="CASCADE"), nullable=False
        ),
        sa.Column(
            "endpoint_id",
            sa.Uuid(),
            sa.ForeignKey("service_endpoints.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "device_id", sa.Uuid(), sa.ForeignKey("network_devices.id", ondelete="RESTRICT"), nullable=False
        ),
        sa.Column(
            "workflow_id",
            sa.String(255),
            sa.ForeignKey("workflow_executions.workflow_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("operation_type", sa.String(16), nullable=False),
        sa.Column("idempotency_key", sa.String(128), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("adapter_operation_id", sa.String(255), nullable=True),
        sa.Column("request_payload", _json(), nullable=False),
        sa.Column("response_payload", _json(), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("failure_kind", sa.String(16), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint(
            "workflow_id", "device_id", "operation_type", "idempotency_key", name="uq_device_op_idem"
        ),
    )
    op.create_index("ix_device_operations_service_id", "device_operations", ["service_id"])
    op.create_index("ix_device_operations_device_id", "device_operations", ["device_id"])
    op.create_index("ix_device_operations_workflow_id", "device_operations", ["workflow_id"])
    op.create_index("ix_device_operations_status", "device_operations", ["status"])
    op.create_index("ix_device_ops_service_device", "device_operations", ["service_id", "device_id"])

    # ------------------------------------------------------------------ audit_events
    op.create_table(
        "audit_events",
        sa.Column("id", sa.Uuid(), primary_key=True, nullable=False),
        sa.Column(
            "service_id", sa.Uuid(), sa.ForeignKey("network_services.id", ondelete="CASCADE"), nullable=True
        ),
        sa.Column("workflow_id", sa.String(255), nullable=True),
        sa.Column(
            "device_id", sa.Uuid(), sa.ForeignKey("network_devices.id", ondelete="SET NULL"), nullable=True
        ),
        sa.Column("event_type", sa.String(48), nullable=False),
        sa.Column("actor", sa.String(128), nullable=False),
        sa.Column("correlation_id", sa.String(128), nullable=True),
        sa.Column("from_status", sa.String(40), nullable=True),
        sa.Column("to_status", sa.String(40), nullable=True),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("details", _json(), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_audit_events_service_id", "audit_events", ["service_id"])
    op.create_index("ix_audit_events_workflow_id", "audit_events", ["workflow_id"])
    op.create_index("ix_audit_events_event_type", "audit_events", ["event_type"])
    op.create_index("ix_audit_events_correlation_id", "audit_events", ["correlation_id"])
    op.create_index("ix_audit_service_time", "audit_events", ["service_id", "occurred_at"])

    # ------------------------------------------------------------------ idempotency_records
    op.create_table(
        "idempotency_records",
        sa.Column("key", sa.String(128), primary_key=True, nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("status_code", sa.Integer(), nullable=False),
        sa.Column("response_body", _json(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
    )


def downgrade() -> None:
    # Reverse dependency order.
    op.drop_table("idempotency_records")
    op.drop_index("ix_audit_service_time", table_name="audit_events")
    op.drop_index("ix_audit_events_correlation_id", table_name="audit_events")
    op.drop_index("ix_audit_events_event_type", table_name="audit_events")
    op.drop_index("ix_audit_events_workflow_id", table_name="audit_events")
    op.drop_index("ix_audit_events_service_id", table_name="audit_events")
    op.drop_table("audit_events")
    op.drop_index("ix_device_ops_service_device", table_name="device_operations")
    op.drop_index("ix_device_operations_status", table_name="device_operations")
    op.drop_index("ix_device_operations_workflow_id", table_name="device_operations")
    op.drop_index("ix_device_operations_device_id", table_name="device_operations")
    op.drop_index("ix_device_operations_service_id", table_name="device_operations")
    op.drop_table("device_operations")
    op.drop_index("ix_workflow_executions_status", table_name="workflow_executions")
    op.drop_index("ix_workflow_executions_workflow_id", table_name="workflow_executions")
    op.drop_index("ix_workflow_executions_service_id", table_name="workflow_executions")
    op.drop_table("workflow_executions")
    op.drop_index("ix_allocations_service_type", table_name="resource_allocations")
    op.drop_index("ix_resource_allocations_service_id", table_name="resource_allocations")
    op.drop_table("resource_allocations")
    op.drop_index("ix_service_endpoints_status", table_name="service_endpoints")
    op.drop_index("ix_service_endpoints_device_id", table_name="service_endpoints")
    op.drop_index("ix_service_endpoints_service_id", table_name="service_endpoints")
    op.drop_table("service_endpoints")
    op.drop_index("ix_services_customer_status", table_name="network_services")
    op.drop_index("ix_network_services_active_workflow_id", table_name="network_services")
    op.drop_index("ix_network_services_lifecycle_status", table_name="network_services")
    op.drop_index("ix_network_services_service_type", table_name="network_services")
    op.drop_index("ix_network_services_customer_id", table_name="network_services")
    op.drop_table("network_services")
    op.drop_index("ix_network_devices_role", table_name="network_devices")
    op.drop_index("ix_network_devices_hostname", table_name="network_devices")
    op.drop_table("network_devices")
    op.drop_table("sites")
    op.drop_table("customers")
