"""``/api/v1/devices`` — read-only inventory endpoints."""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query

from atlas.api.auth import require_operator
from atlas.api.deps import get_controller
from atlas.core.enums import DeviceRole
from atlas.schemas import DeviceList, DeviceOut, ErrorOut
from atlas.services.service_controller import ServiceController

router = APIRouter(prefix="/devices", tags=["devices"], dependencies=[Depends(require_operator)])

Controller = Annotated[ServiceController, Depends(get_controller)]


@router.get("", response_model=DeviceList, summary="List network devices")
async def list_devices(
    controller: Controller,
    role: DeviceRole | None = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> DeviceList:
    return await controller.list_devices(role=role, limit=limit, offset=offset)


@router.get(
    "/{device_id}",
    response_model=DeviceOut,
    responses={404: {"model": ErrorOut}},
    summary="Get a network device",
)
async def get_device(device_id: UUID, controller: Controller) -> DeviceOut:
    return await controller.get_device(device_id)
