"""Canonical enumerations shared by every ATLAS layer (API, DB, workflows, adapters).

These are plain ``str`` enums so they serialise cleanly through Pydantic, SQLAlchemy and
Temporal's JSON payload converter without custom converters.
"""

from __future__ import annotations

from enum import StrEnum


class ServiceType(StrEnum):
    MPLS_L3VPN = "MPLS_L3VPN"
    # Reserved for future releases — not provisionable yet.
    EVPN = "EVPN"
    L2VPN = "L2VPN"
    INTERNET_ACCESS = "INTERNET_ACCESS"
    BGP_PEERING = "BGP_PEERING"
    VRF = "VRF"


class LifecycleStatus(StrEnum):
    """Service lifecycle state machine (see ``atlas.core.lifecycle`` for allowed transitions)."""

    REQUESTED = "REQUESTED"
    VALIDATING = "VALIDATING"
    ALLOCATING = "ALLOCATING"
    PROVISIONING = "PROVISIONING"
    VERIFYING = "VERIFYING"
    ACTIVE = "ACTIVE"
    FAILED = "FAILED"
    ROLLING_BACK = "ROLLING_BACK"
    ROLLED_BACK = "ROLLED_BACK"
    MANUAL_INTERVENTION_REQUIRED = "MANUAL_INTERVENTION_REQUIRED"
    DECOMMISSIONING = "DECOMMISSIONING"
    DECOMMISSIONED = "DECOMMISSIONED"


class DeviceRole(StrEnum):
    PE = "PE"
    P = "P"
    RR = "RR"


class Vendor(StrEnum):
    CISCO_IOSXR = "cisco_iosxr"
    JUNIPER_JUNOS = "juniper_junos"
    MOCK = "mock"


class DeviceOperationalStatus(StrEnum):
    UP = "UP"
    DOWN = "DOWN"
    MAINTENANCE = "MAINTENANCE"
    UNKNOWN = "UNKNOWN"


class ResourceType(StrEnum):
    ROUTE_DISTINGUISHER = "ROUTE_DISTINGUISHER"
    ROUTE_TARGET = "ROUTE_TARGET"
    VLAN_ID = "VLAN_ID"
    VRF_NAME = "VRF_NAME"
    IP_SUBNET = "IP_SUBNET"


class AllocationStatus(StrEnum):
    RESERVED = "RESERVED"
    ALLOCATED = "ALLOCATED"
    RELEASED = "RELEASED"


class RoutingProtocol(StrEnum):
    STATIC = "STATIC"
    BGP = "BGP"
    OSPF = "OSPF"


class EndpointStatus(StrEnum):
    """Per-endpoint provisioning status — each endpoint is tracked independently."""

    PENDING = "PENDING"
    PROVISIONING = "PROVISIONING"
    PROVISIONED = "PROVISIONED"
    VERIFIED = "VERIFIED"
    FAILED = "FAILED"
    ROLLED_BACK = "ROLLED_BACK"
    DECOMMISSIONED = "DECOMMISSIONED"


class WorkflowStatus(StrEnum):
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    TERMINATED = "TERMINATED"
    TIMED_OUT = "TIMED_OUT"


class WorkflowType(StrEnum):
    PROVISION_L3VPN = "ProvisionL3VPNWorkflow"
    DECOMMISSION_L3VPN = "DecommissionL3VPNWorkflow"
    RECONCILE_SERVICE = "ReconcileServiceWorkflow"


class DeviceOperationType(StrEnum):
    APPLY = "APPLY"
    VERIFY = "VERIFY"
    ROLLBACK = "ROLLBACK"
    GET_STATE = "GET_STATE"
    VALIDATE = "VALIDATE"


class DeviceOperationStatus(StrEnum):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    COMPENSATED = "COMPENSATED"
    SKIPPED = "SKIPPED"


class FailureKind(StrEnum):
    """Classification used by recovery logic to decide retry vs compensate vs escalate."""

    TRANSIENT = "TRANSIENT"  # retry is safe (timeouts, unreachable)
    PERMANENT = "PERMANENT"  # retry is pointless (invalid config, rejected)
    UNKNOWN = "UNKNOWN"  # device state must be inspected before deciding


class FailureMode(StrEnum):
    """Failure injection modes understood by ``MockNetworkAdapter``."""

    NONE = "NONE"
    FAIL_ON_PE3 = "FAIL_ON_PE3"
    TIMEOUT_ON_PE2 = "TIMEOUT_ON_PE2"
    BGP_SESSION_DOWN = "BGP_SESSION_DOWN"
    DEVICE_UNREACHABLE = "DEVICE_UNREACHABLE"
    PARTIAL_CONFIGURATION = "PARTIAL_CONFIGURATION"


class ReconciliationMode(StrEnum):
    DETECT_ONLY = "DETECT_ONLY"
    AUTO_REMEDIATE = "AUTO_REMEDIATE"  # not enabled in MVP; requires explicit authorisation


class DriftStatus(StrEnum):
    IN_SYNC = "IN_SYNC"
    DRIFT_DETECTED = "DRIFT_DETECTED"
    DEVICE_UNREACHABLE = "DEVICE_UNREACHABLE"
    NOT_PROVISIONED = "NOT_PROVISIONED"


class AuditEventType(StrEnum):
    SERVICE_REQUESTED = "SERVICE_REQUESTED"
    STATUS_CHANGED = "STATUS_CHANGED"
    RESOURCES_ALLOCATED = "RESOURCES_ALLOCATED"
    RESOURCES_RELEASED = "RESOURCES_RELEASED"
    CONFIG_GENERATED = "CONFIG_GENERATED"
    DEVICE_OPERATION = "DEVICE_OPERATION"
    VALIDATION_RESULT = "VALIDATION_RESULT"
    COMPENSATION_EXECUTED = "COMPENSATION_EXECUTED"
    WORKFLOW_STARTED = "WORKFLOW_STARTED"
    WORKFLOW_COMPLETED = "WORKFLOW_COMPLETED"
    WORKFLOW_FAILED = "WORKFLOW_FAILED"
    WORKFLOW_CANCELLED = "WORKFLOW_CANCELLED"
    RECONCILIATION_RUN = "RECONCILIATION_RUN"
    DRIFT_DETECTED = "DRIFT_DETECTED"
    MANUAL_INTERVENTION = "MANUAL_INTERVENTION"
