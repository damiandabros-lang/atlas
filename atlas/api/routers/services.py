"""``/api/v1/services`` — service lifecycle endpoints."""

from __future__ import annotations

from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, Header, Query, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from atlas.api.auth import Principal, require_operator
from atlas.api.deps import get_controller, get_correlation_id, get_settings_dep
from atlas.api.idempotency import (
    IDEMPOTENCY_HEADER,
    check_idempotency,
    require_idempotency_key,
    store_idempotent_response,
)
from atlas.core.config import Settings
from atlas.core.enums import LifecycleStatus, ReconciliationMode
from atlas.schemas import (
    AuditEventOut,
    ErrorOut,
    L3VPNServiceRequest,
    PageMeta,
    ServiceAccepted,
    ServiceList,
    ServiceOut,
    ServiceStatusOut,
    ValidationReport,
)
from atlas.services.service_controller import ServiceController

router = APIRouter(prefix="/services", tags=["services"], dependencies=[Depends(require_operator)])

_ERRORS: dict[int | str, dict[str, Any]] = {
    401: {"model": ErrorOut},
    404: {"model": ErrorOut},
    409: {"model": ErrorOut},
    422: {"model": ErrorOut},
}


class ReconcileBody(BaseModel):
    mode: ReconciliationMode = ReconciliationMode.DETECT_ONLY


class HistoryOut(BaseModel):
    items: list[AuditEventOut]
    meta: PageMeta


class ActionAccepted(BaseModel):
    service_id: UUID
    workflow_id: str | None = None
    status: str
    mode: str | None = None


class ReconcileAccepted(ActionAccepted):
    mode: str = Field(default=ReconciliationMode.DETECT_ONLY.value)


Controller = Annotated[ServiceController, Depends(get_controller)]
Operator = Annotated[Principal, Depends(require_operator)]
CorrelationId = Annotated[str, Depends(get_correlation_id)]


@router.post(
    "/l3vpn",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=ServiceAccepted,
    responses={**_ERRORS, 400: {"model": ErrorOut}},
    summary="Request a new MPLS L3VPN service (asynchronous)",
)
async def create_l3vpn(
    request: Request,
    body: L3VPNServiceRequest,
    controller: Controller,
    operator: Operator,
    correlation_id: CorrelationId,
    settings: Annotated[Settings, Depends(get_settings_dep)],
    idempotency_key: Annotated[str | None, Header(alias=IDEMPOTENCY_HEADER)] = None,
) -> Any:
    key = require_idempotency_key(idempotency_key)
    ctx, replay = await check_idempotency(
        controller.session,
        key=key,
        method=request.method,
        path=request.url.path,
        payload=body.model_dump(mode="json"),
        correlation_id=correlation_id,
    )
    if replay is not None:
        return replay
    accepted = await controller.create_l3vpn(
        body, actor=operator.name, correlation_id=correlation_id, idempotency_key=key
    )
    content = accepted.model_dump(mode="json")
    await store_idempotent_response(
        controller.session,
        ctx,
        status_code=status.HTTP_202_ACCEPTED,
        body=content,
        ttl_seconds=settings.idempotency_ttl_seconds,
    )
    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED, content=content, headers={"X-Correlation-ID": correlation_id}
    )


@router.get("", response_model=ServiceList, summary="List services")
async def list_services(
    controller: Controller,
    customer_id: UUID | None = None,
    status_filter: Annotated[LifecycleStatus | None, Query(alias="status")] = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> ServiceList:
    return await controller.list_services(
        customer_id=customer_id, status=status_filter, limit=limit, offset=offset
    )


@router.get("/{service_id}", response_model=ServiceOut, responses=_ERRORS, summary="Get a service")
async def get_service(service_id: UUID, controller: Controller) -> ServiceOut:
    return await controller.get_service(service_id)


@router.get(
    "/{service_id}/status",
    response_model=ServiceStatusOut,
    responses=_ERRORS,
    summary="Lifecycle + live workflow status",
)
async def get_status(service_id: UUID, controller: Controller) -> ServiceStatusOut:
    return await controller.get_status(service_id)


@router.get("/{service_id}/history", response_model=HistoryOut, responses=_ERRORS, summary="Audit trail")
async def get_history(
    service_id: UUID,
    controller: Controller,
    limit: Annotated[int, Query(ge=1, le=500)] = 200,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> HistoryOut:
    items, total = await controller.get_history(service_id, limit=limit, offset=offset)
    return HistoryOut(items=items, meta=PageMeta(total=total, limit=limit, offset=offset))


@router.post(
    "/{service_id}/validate",
    response_model=ValidationReport,
    responses=_ERRORS,
    summary="Synchronous inventory-level validation",
)
async def validate_service(service_id: UUID, controller: Controller) -> ValidationReport:
    return await controller.validate(service_id)


@router.post(
    "/{service_id}/reconcile",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=ReconcileAccepted,
    responses={**_ERRORS, 403: {"model": ErrorOut}},
    summary="Start a desired-vs-observed reconciliation run",
)
async def reconcile_service(
    service_id: UUID,
    controller: Controller,
    operator: Operator,
    correlation_id: CorrelationId,
    body: ReconcileBody | None = None,
) -> Any:
    mode = (body or ReconcileBody()).mode
    return await controller.reconcile(
        service_id, mode=mode, actor=operator.name, correlation_id=correlation_id
    )


@router.post(
    "/{service_id}/cancel",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=ActionAccepted,
    responses=_ERRORS,
    summary="Cancel an in-flight orchestration",
)
async def cancel_service(
    service_id: UUID, controller: Controller, operator: Operator, correlation_id: CorrelationId
) -> Any:
    return await controller.cancel(service_id, actor=operator.name, correlation_id=correlation_id)


@router.delete(
    "/{service_id}",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=ServiceAccepted,
    responses=_ERRORS,
    summary="Decommission a service (asynchronous)",
)
async def decommission_service(
    service_id: UUID,
    controller: Controller,
    operator: Operator,
    correlation_id: CorrelationId,
    force: Annotated[bool, Query(description="Proceed even if some devices are unreachable")] = False,
) -> ServiceAccepted:
    return await controller.decommission(
        service_id, actor=operator.name, correlation_id=correlation_id, force=force
    )
