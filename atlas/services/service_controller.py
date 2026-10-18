"""Service controller — business logic between the REST routers and the persistence/Temporal layers.

Responsibilities
----------------
* Translate API requests into inventory rows (``NetworkService`` + ``ServiceEndpoint``) and
  audit events inside ONE database transaction.
* Start / signal / cancel Temporal workflows. Temporal owns *execution* state; PostgreSQL owns
  *business* state — this class is the only place where the two meet on the API side.
* Enforce lifecycle preconditions for operator actions (cancel, decommission, reconcile) and
  surface violations as ``ApiError`` with the right HTTP status.

Transaction ordering for ``create_l3vpn``
-----------------------------------------
The service row is committed *before* ``start_workflow`` is called. If the workflow start then
fails, the row stays in REQUESTED and the operator can retry with the same ``Idempotency-Key``
(``WorkflowIDReusePolicy.REJECT_DUPLICATE`` + ``WorkflowAlreadyStartedError`` make the retry a
no-op when the workflow actually did start). Doing it the other way round (start workflow,
then commit) risks a running workflow whose service row does not exist.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from temporalio.client import Client
from temporalio.common import WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError

from atlas.api.errors import ApiError
from atlas.core.config import Settings
from atlas.core.enums import (
    AuditEventType,
    DeviceRole,
    EndpointStatus,
    LifecycleStatus,
    ReconciliationMode,
    ServiceType,
    WorkflowStatus,
    WorkflowType,
)
from atlas.core.lifecycle import IN_FLIGHT_STATES
from atlas.db.models import NetworkService, ServiceEndpoint
from atlas.db.repositories import (
    AuditRepository,
    CustomerRepository,
    DeviceRepository,
    ServiceRepository,
    WorkflowRepository,
)
from atlas.schemas import (
    AllocationOut,
    AuditEventOut,
    DecommissionRequest,
    DeviceList,
    DeviceOut,
    EndpointOut,
    L3VPNServiceRequest,
    PageMeta,
    ProvisionRequest,
    ReconcileRequest,
    ServiceAccepted,
    ServiceList,
    ServiceOut,
    ServiceStatusOut,
    ServiceSummary,
    ValidationCheck,
    ValidationReport,
)
from atlas.services.temporal_client import (
    decommission_workflow_id,
    describe_workflow,
    provision_workflow_id,
    reconcile_workflow_id,
)

DECOMMISSIONABLE_STATES: frozenset[LifecycleStatus] = frozenset(
    {
        LifecycleStatus.ACTIVE,
        LifecycleStatus.FAILED,
        LifecycleStatus.ROLLED_BACK,
        LifecycleStatus.MANUAL_INTERVENTION_REQUIRED,
    }
)
RECONCILABLE_STATES: frozenset[LifecycleStatus] = frozenset(
    {LifecycleStatus.ACTIVE, LifecycleStatus.MANUAL_INTERVENTION_REQUIRED}
)


def _endpoint_out(ep: ServiceEndpoint) -> EndpointOut:
    out = EndpointOut.model_validate(ep)
    device = getattr(ep, "device", None)
    out.device_hostname = device.hostname if device is not None else None
    return out


class ServiceController:
    def __init__(self, session: AsyncSession, temporal_client: Client, settings: Settings) -> None:
        self.session = session
        self.temporal = temporal_client
        self.settings = settings
        self.services = ServiceRepository(session)
        self.customers = CustomerRepository(session)
        self.devices = DeviceRepository(session)
        self.workflows = WorkflowRepository(session)
        self.audit = AuditRepository(session)

    # ------------------------------------------------------------------ helpers

    async def _require_service(self, service_id: UUID) -> NetworkService:
        service = await self.services.get(service_id)
        if service is None:
            raise ApiError(404, "service_not_found", f"service {service_id} not found")
        return service

    # ------------------------------------------------------------------ create

    async def create_l3vpn(
        self,
        req: L3VPNServiceRequest,
        *,
        actor: str,
        correlation_id: str,
        idempotency_key: str | None,
    ) -> ServiceAccepted:
        customer = await self.customers.get(req.customer_id)
        if customer is None:
            raise ApiError(404, "customer_not_found", f"customer {req.customer_id} not found")

        hostnames = [ep.device for ep in req.endpoints]
        found = await self.devices.get_many_by_hostnames(hostnames)
        unknown = sorted({h for h in hostnames if h.upper() not in found})
        if unknown:
            raise ApiError(422, "unknown_device", f"unknown device(s): {', '.join(unknown)}")
        non_pe = sorted({h for h in hostnames if DeviceRole(found[h.upper()].role) != DeviceRole.PE})
        if non_pe:
            raise ApiError(422, "device_not_pe", f"endpoint device(s) must have role PE: {', '.join(non_pe)}")

        service = await self.services.create(
            customer_id=req.customer_id,
            service_name=req.service_name,
            service_type=ServiceType.MPLS_L3VPN,
            desired_state=req.model_dump(mode="json"),
            idempotency_key=idempotency_key,
        )
        for seq, ep in enumerate(req.endpoints):
            try:
                await self.services.add_endpoint(
                    service,
                    sequence=seq,
                    device_id=found[ep.device.upper()].id,
                    interface=ep.interface,
                    vlan_id=ep.vlan,
                    vrf_name=req.vrf_name,
                    routing_protocol=ep.routing_protocol,
                    ce_asn=ep.ce_asn,
                    ce_ip_address=ep.ce_ip_address,
                    pe_ip_address=ep.pe_ip_address,
                    status=EndpointStatus.PENDING,
                )
            except IntegrityError:
                # UNIQUE(device_id, interface, vlan_id): the attachment circuit is owned by
                # another service. Nothing has been committed yet, so roll back cleanly.
                await self.session.rollback()
                raise ApiError(
                    409,
                    "endpoint_in_use",
                    f"{ep.device}/{ep.interface} vlan {ep.vlan} is already attached to another service",
                ) from None

        workflow_id = provision_workflow_id(service.id)
        await self.audit.record(
            AuditEventType.SERVICE_REQUESTED,
            service_id=service.id,
            workflow_id=workflow_id,
            actor=actor,
            correlation_id=correlation_id,
            to_status=LifecycleStatus.REQUESTED.value,
            message=f"L3VPN service {req.service_name} requested with {len(req.endpoints)} endpoint(s)",
            details={"endpoints": hostnames, "vrf_name": req.vrf_name},
        )
        await self.workflows.record_start(
            service_id=service.id,
            workflow_id=workflow_id,
            workflow_type=WorkflowType.PROVISION_L3VPN,
            task_queue=self.settings.temporal_task_queue,
        )
        await self.services.set_active_workflow(service.id, workflow_id)
        await self.session.commit()

        payload = ProvisionRequest(
            service_id=service.id, correlation_id=correlation_id, request=req, requested_by=actor
        )
        try:
            await self.temporal.start_workflow(
                WorkflowType.PROVISION_L3VPN.value,
                payload,
                id=workflow_id,
                task_queue=self.settings.temporal_task_queue,
                id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
            )
        except WorkflowAlreadyStartedError:
            # Idempotent success: the workflow for this service is already executing.
            pass

        return ServiceAccepted(
            service_id=service.id, workflow_id=workflow_id, status=LifecycleStatus.REQUESTED
        )

    # ------------------------------------------------------------------ read

    async def get_service(self, service_id: UUID) -> ServiceOut:
        service = await self._require_service(service_id)
        out = ServiceOut.model_validate(service)
        out.endpoints = [_endpoint_out(ep) for ep in service.endpoints]
        out.allocations = [AllocationOut.model_validate(a) for a in service.allocations]
        return out

    async def list_services(
        self,
        *,
        customer_id: UUID | None = None,
        status: LifecycleStatus | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> ServiceList:
        rows, total = await self.services.list(
            customer_id=customer_id, status=status, limit=limit, offset=offset
        )
        return ServiceList(
            items=[ServiceSummary.model_validate(r) for r in rows],
            meta=PageMeta(total=total, limit=limit, offset=offset),
        )

    async def get_status(self, service_id: UUID) -> ServiceStatusOut:
        service = await self._require_service(service_id)
        workflow_id = service.active_workflow_id
        workflow_status: WorkflowStatus | None = None
        current_stage: str | None = None
        if workflow_id:
            execution = await self.workflows.get_by_workflow_id(workflow_id)
            if execution is not None:
                workflow_status = WorkflowStatus(execution.status)
                current_stage = execution.current_stage
            live_status, live_stage = await describe_workflow(self.temporal, workflow_id)
            if live_status is not None:
                workflow_status = live_status
            if live_stage is not None:
                current_stage = live_stage
        return ServiceStatusOut(
            service_id=service.id,
            lifecycle_status=LifecycleStatus(service.lifecycle_status),
            status_reason=service.status_reason,
            workflow_id=workflow_id,
            workflow_status=workflow_status,
            current_stage=current_stage,
            endpoints=[_endpoint_out(ep) for ep in service.endpoints],
        )

    async def get_history(
        self, service_id: UUID, *, limit: int = 200, offset: int = 0
    ) -> tuple[list[AuditEventOut], int]:
        await self._require_service(service_id)
        rows, total = await self.audit.history(service_id, limit=limit, offset=offset)
        return [AuditEventOut.model_validate(r) for r in rows], total

    # ------------------------------------------------------------------ lifecycle actions

    async def cancel(self, service_id: UUID, *, actor: str, correlation_id: str) -> dict[str, Any]:
        service = await self._require_service(service_id)
        status = LifecycleStatus(service.lifecycle_status)
        if status not in IN_FLIGHT_STATES or not service.active_workflow_id:
            raise ApiError(
                409,
                "not_cancellable",
                f"service is {status.value} with no in-flight workflow; nothing to cancel",
            )
        workflow_id = service.active_workflow_id
        handle = self.temporal.get_workflow_handle(workflow_id)
        # Cooperative first: the workflow's cancel signal lets it finish the current device
        # operation and compensate cleanly. Then the Temporal-level cancel request as backstop.
        await handle.signal("cancel")
        await handle.cancel()
        await self.audit.record(
            AuditEventType.WORKFLOW_CANCELLED,
            service_id=service.id,
            workflow_id=workflow_id,
            actor=actor,
            correlation_id=correlation_id,
            from_status=status.value,
            message="cancellation requested by operator",
        )
        await self.session.commit()
        return {"service_id": str(service.id), "workflow_id": workflow_id, "status": "CANCEL_REQUESTED"}

    async def decommission(
        self, service_id: UUID, *, actor: str, correlation_id: str, force: bool = False
    ) -> ServiceAccepted:
        service = await self._require_service(service_id)
        status = LifecycleStatus(service.lifecycle_status)
        if status not in DECOMMISSIONABLE_STATES:
            raise ApiError(
                409,
                "not_decommissionable",
                f"service is {status.value}; decommission requires one of "
                f"{', '.join(sorted(s.value for s in DECOMMISSIONABLE_STATES))}",
            )
        workflow_id = decommission_workflow_id(service.id)
        await self.workflows.record_start(
            service_id=service.id,
            workflow_id=workflow_id,
            workflow_type=WorkflowType.DECOMMISSION_L3VPN,
            task_queue=self.settings.temporal_task_queue,
        )
        await self.services.set_active_workflow(service.id, workflow_id)
        await self.audit.record(
            AuditEventType.WORKFLOW_STARTED,
            service_id=service.id,
            workflow_id=workflow_id,
            actor=actor,
            correlation_id=correlation_id,
            from_status=status.value,
            message="decommission requested by operator" + (" (force)" if force else ""),
            details={"force": force, "workflow_type": WorkflowType.DECOMMISSION_L3VPN.value},
        )
        await self.session.commit()

        payload = DecommissionRequest(
            service_id=service.id, correlation_id=correlation_id, requested_by=actor, force=force
        )
        try:
            await self.temporal.start_workflow(
                WorkflowType.DECOMMISSION_L3VPN.value,
                payload,
                id=workflow_id,
                task_queue=self.settings.temporal_task_queue,
                id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
            )
        except WorkflowAlreadyStartedError:
            pass
        return ServiceAccepted(service_id=service.id, workflow_id=workflow_id, status=status)

    async def validate(self, service_id: UUID) -> ValidationReport:
        """Synchronous inventory-level validation (no device I/O — that is the workflow's job)."""
        service = await self._require_service(service_id)
        status = LifecycleStatus(service.lifecycle_status)
        checks = [
            ValidationCheck(name="service_exists", passed=True, detail=str(service.id)),
            ValidationCheck(
                name="lifecycle_active",
                passed=status == LifecycleStatus.ACTIVE,
                detail=f"lifecycle_status={status.value}",
            ),
        ]
        for ep in service.endpoints:
            ep_status = EndpointStatus(ep.status)
            hostname = ep.device.hostname if ep.device is not None else None
            checks.append(
                ValidationCheck(
                    name="endpoint_verified",
                    device_hostname=hostname,
                    passed=ep_status == EndpointStatus.VERIFIED,
                    detail=f"{ep.interface} vlan {ep.vlan_id}: {ep_status.value}",
                )
            )
        return ValidationReport(service_id=service.id, passed=all(c.passed for c in checks), checks=checks)

    async def reconcile(
        self, service_id: UUID, *, mode: ReconciliationMode, actor: str, correlation_id: str
    ) -> dict[str, Any]:
        service = await self._require_service(service_id)
        status = LifecycleStatus(service.lifecycle_status)
        if status not in RECONCILABLE_STATES:
            raise ApiError(
                409, "not_reconcilable", f"service is {status.value}; reconciliation requires ACTIVE"
            )
        if mode == ReconciliationMode.AUTO_REMEDIATE:
            # Brief §12: no automatic device modification without explicit authorisation.
            raise ApiError(403, "auto_remediate_forbidden", "AUTO_REMEDIATE is not enabled; use DETECT_ONLY")
        workflow_id = reconcile_workflow_id(service.id)
        payload = ReconcileRequest(service_id=service.id, correlation_id=correlation_id, mode=mode)
        await self.audit.record(
            AuditEventType.RECONCILIATION_RUN,
            service_id=service.id,
            workflow_id=workflow_id,
            actor=actor,
            correlation_id=correlation_id,
            message=f"reconciliation requested ({mode.value})",
            details={"mode": mode.value},
        )
        await self.session.commit()
        # Reconciliation is repeatable: a closed run must not block the next one.
        await self.temporal.start_workflow(
            WorkflowType.RECONCILE_SERVICE.value,
            payload,
            id=workflow_id,
            task_queue=self.settings.temporal_task_queue,
            id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        )
        return {
            "service_id": str(service.id),
            "workflow_id": workflow_id,
            "status": "STARTED",
            "mode": mode.value,
        }

    # ------------------------------------------------------------------ devices

    async def list_devices(
        self, *, role: DeviceRole | None = None, limit: int = 100, offset: int = 0
    ) -> DeviceList:
        rows, total = await self.devices.list(role=role, limit=limit, offset=offset)
        return DeviceList(
            items=[DeviceOut.model_validate(d) for d in rows],
            meta=PageMeta(total=total, limit=limit, offset=offset),
        )

    async def get_device(self, device_id: UUID) -> DeviceOut:
        device = await self.devices.get(device_id)
        if device is None:
            raise ApiError(404, "device_not_found", f"device {device_id} not found")
        return DeviceOut.model_validate(device)


__all__ = ["DECOMMISSIONABLE_STATES", "RECONCILABLE_STATES", "ServiceController"]
