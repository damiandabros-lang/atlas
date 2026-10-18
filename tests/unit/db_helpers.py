"""Shared helpers for the DB-layer unit tests (imported explicitly by db_test_*.py)."""

from __future__ import annotations

from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from atlas.core.enums import RoutingProtocol, ServiceType
from atlas.db.models import NetworkDevice, NetworkService
from atlas.db.repositories import DeviceRepository, ServiceRepository
from atlas.db.seed import ACME_CUSTOMER_ID, seed
from atlas.schemas import EndpointRequest, L3VPNServiceRequest


async def seeded(session: AsyncSession) -> dict[str, NetworkDevice]:
    await seed(session)
    return await DeviceRepository(session).get_many_by_hostnames(["PE1", "PE2", "PE3"])


def acme_request(
    name: str = "ACME-L3VPN-001",
    *,
    vrf: str = "ACME-PROD",
    route_target: str | None = "65000:100",
    vlan: int | None = 100,
    devices: tuple[str, ...] = ("PE1", "PE2", "PE3"),
    customer_id: UUID = ACME_CUSTOMER_ID,
    interface_prefix: str = "GigabitEthernet0/0/0/",
) -> L3VPNServiceRequest:
    return L3VPNServiceRequest(
        customer_id=customer_id,
        service_name=name,
        vrf_name=vrf,
        route_target=route_target,
        endpoints=[
            EndpointRequest(device=d, interface=f"{interface_prefix}{i + 1}", vlan=vlan)
            for i, d in enumerate(devices)
        ],
    )


async def create_service(
    session: AsyncSession, request: L3VPNServiceRequest, devices: dict[str, NetworkDevice]
) -> NetworkService:
    """Persist a service + endpoints the way the API layer will (no allocation yet)."""
    repo = ServiceRepository(session)
    service = await repo.create(
        customer_id=request.customer_id,
        service_name=request.service_name,
        service_type=ServiceType.MPLS_L3VPN,
        desired_state=request.model_dump(mode="json"),
    )
    for i, ep in enumerate(request.endpoints):
        await repo.add_endpoint(
            service,
            device_id=devices[ep.device.upper()].id,
            sequence=i,
            interface=ep.interface,
            vlan_id=ep.vlan,
            vrf_name=request.vrf_name,
            routing_protocol=ep.routing_protocol or RoutingProtocol.STATIC,
            ce_asn=ep.ce_asn,
        )
    return service
