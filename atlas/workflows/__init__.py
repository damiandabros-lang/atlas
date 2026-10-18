"""Temporal workflow definitions registered by the ATLAS worker."""

from __future__ import annotations

from atlas.workflows.decommission_l3vpn import DecommissionL3VPNWorkflow
from atlas.workflows.provision_l3vpn import ProvisionL3VPNWorkflow
from atlas.workflows.reconcile_service import ReconcileServiceWorkflow

ALL_WORKFLOWS = [ProvisionL3VPNWorkflow, DecommissionL3VPNWorkflow, ReconcileServiceWorkflow]

__all__ = ["ALL_WORKFLOWS", "DecommissionL3VPNWorkflow", "ProvisionL3VPNWorkflow", "ReconcileServiceWorkflow"]
