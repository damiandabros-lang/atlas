"""Temporal activities — the only place where orchestration touches PostgreSQL or devices.

``ALL_ACTIVITIES`` is registered by the worker; workflows reference the functions by object so
the activity *names* (``@activity.defn(name=...)``) stay the contract between the two.
"""

from __future__ import annotations

from atlas.activities.allocation import allocate_resources, confirm_allocations, release_resources
from atlas.activities.configuration import generate_configs, load_decommission_plan, pre_deployment_validation
from atlas.activities.device_ops import (
    decommission_device,
    inspect_device_state,
    provision_device,
    rollback_device,
)
from atlas.activities.models import AuditInput, StageInput, WorkflowFinishInput, WorkflowStartInput
from atlas.activities.reconciliation import reconcile_service_activity
from atlas.activities.state import (
    record_audit,
    record_workflow_finish,
    record_workflow_start,
    set_workflow_stage,
    transition_status,
)
from atlas.activities.validation import validate_request
from atlas.activities.verification import commit_service_state, validate_service_connectivity, verify_device

ALL_ACTIVITIES = [
    # state / bookkeeping
    transition_status,
    record_workflow_start,
    set_workflow_stage,
    record_workflow_finish,
    record_audit,
    # validation / allocation / configuration
    validate_request,
    allocate_resources,
    confirm_allocations,
    release_resources,
    generate_configs,
    pre_deployment_validation,
    load_decommission_plan,
    # devices
    provision_device,
    inspect_device_state,
    rollback_device,
    decommission_device,
    # verification
    verify_device,
    validate_service_connectivity,
    commit_service_state,
    # reconciliation
    reconcile_service_activity,
]

__all__ = [
    "ALL_ACTIVITIES",
    "AuditInput",
    "StageInput",
    "WorkflowFinishInput",
    "WorkflowStartInput",
    "allocate_resources",
    "commit_service_state",
    "confirm_allocations",
    "decommission_device",
    "generate_configs",
    "inspect_device_state",
    "load_decommission_plan",
    "pre_deployment_validation",
    "provision_device",
    "reconcile_service_activity",
    "record_audit",
    "record_workflow_finish",
    "record_workflow_start",
    "release_resources",
    "rollback_device",
    "set_workflow_stage",
    "transition_status",
    "validate_request",
    "validate_service_connectivity",
    "verify_device",
]
