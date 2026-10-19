"""``/api/v1/topology`` — network graph (devices, links, service overlays) for the topology view.

The JSON is intentionally UI-neutral: nodes + links + per-service overlays, so it can feed the
bundled ``/ui/topology`` page, a Grafana Node Graph panel (via a JSON datasource) or any
external NMS.
"""

from __future__ import annotations

from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from atlas.api.auth import require_operator
from atlas.api.deps import get_db
from atlas.core.enums import LifecycleStatus
from atlas.db.models import NetworkDevice, NetworkLink, NetworkService, ServiceEndpoint
from atlas.schemas import AtlasModel

router = APIRouter(prefix="/topology", tags=["topology"], dependencies=[Depends(require_operator)])

Session = Annotated[AsyncSession, Depends(get_db)]

_VISIBLE_STATES = {
    LifecycleStatus.PROVISIONING,
    LifecycleStatus.VERIFYING,
    LifecycleStatus.ACTIVE,
    LifecycleStatus.FAILED,
    LifecycleStatus.ROLLING_BACK,
    LifecycleStatus.MANUAL_INTERVENTION_REQUIRED,
    LifecycleStatus.DECOMMISSIONING,
}


class TopologyNode(AtlasModel):
    id: UUID
    hostname: str
    role: str
    vendor: str
    platform: str
    operational_status: str
    loopback_ip: str | None
    site: str | None
    services: list[str]


class TopologyLink(AtlasModel):
    id: UUID
    a: str
    a_interface: str
    b: str
    b_interface: str
    link_type: str
    bandwidth_gbps: int | None
    igp_metric: int | None
    operational_status: str


class TopologyAttachment(AtlasModel):
    device: str
    interface: str
    vlan_id: int | None
    vrf_name: str
    route_distinguisher: str | None
    pe_ip_address: str | None
    ce_ip_address: str | None
    status: str


class TopologyService(AtlasModel):
    id: UUID
    service_name: str
    customer_id: UUID
    service_type: str
    lifecycle_status: str
    vrf_name: str | None
    route_target: str | None
    attachments: list[TopologyAttachment]


class TopologyOut(AtlasModel):
    nodes: list[TopologyNode]
    links: list[TopologyLink]
    services: list[TopologyService]


@router.get("", response_model=TopologyOut, summary="Network topology with service overlays")
async def get_topology(session: Session, include_all: bool = False) -> TopologyOut:
    devices = (
        (await session.execute(select(NetworkDevice).options(selectinload(NetworkDevice.site))))
        .scalars()
        .all()
    )
    links = (await session.execute(select(NetworkLink))).scalars().all()
    stmt = select(NetworkService).options(
        selectinload(NetworkService.endpoints).selectinload(ServiceEndpoint.device)
    )
    if not include_all:
        stmt = stmt.where(NetworkService.lifecycle_status.in_([s.value for s in _VISIBLE_STATES]))
    services = (await session.execute(stmt)).scalars().all()

    by_id: dict[UUID, NetworkDevice] = {d.id: d for d in devices}
    services_per_device: dict[UUID, list[str]] = {d.id: [] for d in devices}
    out_services: list[TopologyService] = []
    for svc in services:
        desired: dict[str, Any] = svc.desired_state or {}
        attachments = []
        for ep in svc.endpoints:
            services_per_device.setdefault(ep.device_id, []).append(svc.service_name)
            attachments.append(
                TopologyAttachment(
                    device=ep.device.hostname if ep.device else str(ep.device_id),
                    interface=ep.interface,
                    vlan_id=ep.vlan_id,
                    vrf_name=ep.vrf_name,
                    route_distinguisher=ep.route_distinguisher,
                    pe_ip_address=ep.pe_ip_address,
                    ce_ip_address=ep.ce_ip_address,
                    status=str(ep.status),
                )
            )
        out_services.append(
            TopologyService(
                id=svc.id,
                service_name=svc.service_name,
                customer_id=svc.customer_id,
                service_type=str(svc.service_type),
                lifecycle_status=str(svc.lifecycle_status),
                vrf_name=desired.get("vrf_name"),
                route_target=desired.get("route_target"),
                attachments=attachments,
            )
        )

    nodes = [
        TopologyNode(
            id=d.id,
            hostname=d.hostname,
            role=str(d.role),
            vendor=str(d.vendor),
            platform=d.platform,
            operational_status=str(d.operational_status),
            loopback_ip=d.loopback_ip,
            site=d.site.name if d.site else None,
            services=sorted(set(services_per_device.get(d.id, []))),
        )
        for d in sorted(devices, key=lambda x: (str(x.role), x.hostname))
    ]
    out_links = [
        TopologyLink(
            id=link.id,
            a=by_id[link.a_device_id].hostname,
            a_interface=link.a_interface,
            b=by_id[link.b_device_id].hostname,
            b_interface=link.b_interface,
            link_type=link.link_type,
            bandwidth_gbps=link.bandwidth_gbps,
            igp_metric=link.igp_metric,
            operational_status=str(link.operational_status),
        )
        for link in links
        if link.a_device_id in by_id and link.b_device_id in by_id
    ]
    return TopologyOut(nodes=nodes, links=out_links, services=out_services)
