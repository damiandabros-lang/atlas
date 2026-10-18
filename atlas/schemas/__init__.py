"""Pydantic v2 schemas — API contracts and Temporal payloads.

Everything that crosses a process boundary (HTTP, Temporal task queue) is defined here so
that the API, the workflow, the activities and the adapters share one vocabulary.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from atlas.core.enums import (
    AllocationStatus,
    DeviceOperationalStatus,
    DeviceOperationStatus,
    DeviceOperationType,
    DeviceRole,
    DriftStatus,
    EndpointStatus,
    FailureKind,
    LifecycleStatus,
    ReconciliationMode,
    ResourceType,
    RoutingProtocol,
    ServiceType,
    Vendor,
    WorkflowStatus,
    WorkflowType,
)

_RT_RE = re.compile(r"^\d{1,10}:\d{1,10}$")
_IFACE_RE = re.compile(r"^[A-Za-z][A-Za-z0-9\-]*[0-9/\.:]+[0-9/\.:]*$")
_VRF_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_\-]{0,31}$")


class AtlasModel(BaseModel):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True, use_enum_values=False)


# ---------------------------------------------------------------------------
# API — requests
# ---------------------------------------------------------------------------


class EndpointRequest(AtlasModel):
    device: str = Field(..., description="PE hostname from inventory, e.g. PE1")
    interface: str = Field(..., examples=["GigabitEthernet0/0/0/1"])
    vlan: int | None = Field(None, ge=1, le=4094)
    routing_protocol: RoutingProtocol = RoutingProtocol.STATIC
    ce_asn: int | None = Field(None, ge=1, le=4294967295, description="Required when routing_protocol=BGP")
    ce_ip_address: str | None = Field(None, description="Optional; allocated from PE-CE pool when omitted")
    pe_ip_address: str | None = None

    @field_validator("interface")
    @classmethod
    def _iface(cls, v: str) -> str:
        if not _IFACE_RE.match(v):
            raise ValueError("interface name is not a valid router interface identifier")
        return v

    @model_validator(mode="after")
    def _bgp_requires_asn(self) -> EndpointRequest:
        if self.routing_protocol == RoutingProtocol.BGP and self.ce_asn is None:
            raise ValueError("ce_asn is required when routing_protocol is BGP")
        return self


class L3VPNServiceRequest(AtlasModel):
    customer_id: UUID
    service_name: str = Field(..., min_length=3, max_length=64, pattern=r"^[A-Za-z0-9][A-Za-z0-9\-_]*$")
    vrf_name: str = Field(..., examples=["ACME-PROD"])
    route_target: str | None = Field(
        None, examples=["65000:100"], description="Allocated from pool when omitted"
    )
    endpoints: list[EndpointRequest] = Field(..., min_length=1, max_length=64)
    description: str | None = Field(None, max_length=512)

    @field_validator("vrf_name")
    @classmethod
    def _vrf(cls, v: str) -> str:
        if not _VRF_RE.match(v):
            raise ValueError("vrf_name must start with a letter and contain only [A-Za-z0-9_-], max 32 chars")
        return v

    @field_validator("route_target")
    @classmethod
    def _rt(cls, v: str | None) -> str | None:
        if v is not None and not _RT_RE.match(v):
            raise ValueError("route_target must be in ASN:NN format")
        return v

    @model_validator(mode="after")
    def _unique_attachments(self) -> L3VPNServiceRequest:
        seen: set[tuple[str, str, int | None]] = set()
        for ep in self.endpoints:
            key = (ep.device.upper(), ep.interface, ep.vlan)
            if key in seen:
                raise ValueError(f"duplicate endpoint {ep.device}/{ep.interface} vlan {ep.vlan}")
            seen.add(key)
        return self


# ---------------------------------------------------------------------------
# API — responses
# ---------------------------------------------------------------------------


class ServiceAccepted(AtlasModel):
    service_id: UUID
    workflow_id: str
    status: LifecycleStatus


class EndpointOut(AtlasModel):
    id: UUID
    device_id: UUID
    device_hostname: str | None = None
    interface: str
    vlan_id: int | None
    vrf_name: str
    route_distinguisher: str | None
    ce_ip_address: str | None
    pe_ip_address: str | None
    routing_protocol: RoutingProtocol
    status: EndpointStatus
    status_reason: str | None = None


class AllocationOut(AtlasModel):
    resource_type: ResourceType
    resource_value: str
    scope: str
    allocation_status: AllocationStatus


class ServiceOut(AtlasModel):
    id: UUID
    customer_id: UUID
    service_type: ServiceType
    service_name: str
    lifecycle_status: LifecycleStatus
    status_reason: str | None
    version: int
    active_workflow_id: str | None
    desired_state: dict[str, Any]
    observed_state: dict[str, Any]
    endpoints: list[EndpointOut] = Field(default_factory=list)
    allocations: list[AllocationOut] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime


class ServiceSummary(AtlasModel):
    id: UUID
    customer_id: UUID
    service_type: ServiceType
    service_name: str
    lifecycle_status: LifecycleStatus
    active_workflow_id: str | None
    updated_at: datetime


class ServiceStatusOut(AtlasModel):
    service_id: UUID
    lifecycle_status: LifecycleStatus
    status_reason: str | None
    workflow_id: str | None
    workflow_status: WorkflowStatus | None
    current_stage: str | None
    endpoints: list[EndpointOut]


class AuditEventOut(AtlasModel):
    id: UUID
    event_type: str
    actor: str
    workflow_id: str | None
    device_id: UUID | None
    from_status: str | None
    to_status: str | None
    message: str
    details: dict[str, Any]
    occurred_at: datetime


class DeviceOut(AtlasModel):
    id: UUID
    hostname: str
    management_ip: str
    vendor: Vendor
    platform: str
    software_version: str
    role: DeviceRole
    site_id: UUID | None
    operational_status: DeviceOperationalStatus
    loopback_ip: str | None
    bgp_asn: int | None
    created_at: datetime
    updated_at: datetime


class PageMeta(AtlasModel):
    total: int
    limit: int
    offset: int


class ServiceList(AtlasModel):
    items: list[ServiceSummary]
    meta: PageMeta


class DeviceList(AtlasModel):
    items: list[DeviceOut]
    meta: PageMeta


class HealthOut(AtlasModel):
    status: str
    version: str
    database: str
    temporal: str


class ErrorOut(AtlasModel):
    error: str
    detail: str | None = None
    correlation_id: str | None = None


# ---------------------------------------------------------------------------
# Temporal — workflow / activity payloads
# ---------------------------------------------------------------------------


class ProvisionRequest(AtlasModel):
    """Input to ``ProvisionL3VPNWorkflow``. Carries only identifiers + the validated request so the
    workflow history stays small and deterministic; activities load full state from PostgreSQL."""

    service_id: UUID
    correlation_id: str
    request: L3VPNServiceRequest
    requested_by: str = "operator"


class DecommissionRequest(AtlasModel):
    service_id: UUID
    correlation_id: str
    requested_by: str = "operator"
    force: bool = False


class ReconcileRequest(AtlasModel):
    service_id: UUID
    correlation_id: str
    mode: ReconciliationMode = ReconciliationMode.DETECT_ONLY


class ServiceRef(AtlasModel):
    service_id: UUID
    correlation_id: str


class StatusTransition(AtlasModel):
    service_id: UUID
    target: LifecycleStatus
    reason: str | None = None
    correlation_id: str
    workflow_id: str | None = None


class EndpointResources(AtlasModel):
    endpoint_id: UUID
    device_id: UUID
    device_hostname: str
    vendor: Vendor
    interface: str
    vlan_id: int | None
    vrf_name: str
    route_distinguisher: str
    route_target_import: list[str]
    route_target_export: list[str]
    pe_ip_address: str
    ce_ip_address: str
    prefix_length: int = 30
    routing_protocol: RoutingProtocol
    pe_asn: int
    ce_asn: int | None = None


class AllocatedResources(AtlasModel):
    service_id: UUID
    vrf_name: str
    route_target: str
    endpoints: list[EndpointResources]


class DeviceConfig(AtlasModel):
    """One rendered, vendor-specific configuration unit for one endpoint on one device."""

    service_id: UUID
    endpoint_id: UUID
    device_id: UUID
    device_hostname: str
    vendor: Vendor
    sequence: int
    config_text: str
    checksum: str
    intent: dict[str, Any] = Field(
        default_factory=dict, description="Vendor-neutral desired state for verify/diff"
    )


class DeviceOperationRequest(AtlasModel):
    service_id: UUID
    workflow_id: str
    correlation_id: str
    config: DeviceConfig
    operation_type: DeviceOperationType
    idempotency_key: str
    attempt: int = 1


class DeviceOperationResult(AtlasModel):
    operation_id: UUID | None = None
    device_id: UUID
    device_hostname: str
    operation_type: DeviceOperationType
    status: DeviceOperationStatus
    adapter_operation_id: str | None = None
    failure_kind: FailureKind | None = None
    error: str | None = None
    already_applied: bool = False
    details: dict[str, Any] = Field(default_factory=dict)


class ValidationCheck(AtlasModel):
    name: str
    device_hostname: str | None = None
    passed: bool
    detail: str | None = None


class ValidationReport(AtlasModel):
    service_id: UUID
    passed: bool
    checks: list[ValidationCheck]


class DriftDifference(AtlasModel):
    field: str
    desired: Any
    observed: Any


class DeviceDrift(AtlasModel):
    service_id: UUID
    device: str
    status: DriftStatus
    differences: list[DriftDifference] = Field(default_factory=list)


class ReconciliationReport(AtlasModel):
    service_id: UUID
    mode: ReconciliationMode
    in_sync: bool
    devices: list[DeviceDrift]
    checked_at: datetime


class WorkflowProgress(AtlasModel):
    """Exposed through a Temporal query handler for live status."""

    stage: str
    completed_devices: list[str] = Field(default_factory=list)
    failed_devices: list[str] = Field(default_factory=list)
    compensated_devices: list[str] = Field(default_factory=list)
    cancel_requested: bool = False


class ProvisionResult(AtlasModel):
    service_id: UUID
    final_status: LifecycleStatus
    workflow_type: WorkflowType = WorkflowType.PROVISION_L3VPN
    provisioned_devices: list[str] = Field(default_factory=list)
    compensated_devices: list[str] = Field(default_factory=list)
    error: str | None = None
