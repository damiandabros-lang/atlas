"""Resource allocation for MPLS L3VPN services.

The allocator turns a validated ``L3VPNServiceRequest`` into concrete, conflict-free network
resources and records them in ``resource_allocations`` through ``AllocationRepository``:

* Route Target      — one per service, scope ``global``; taken from the request or the RT pool.
* VRF name          — one per service, scope ``global``; must not be owned by another service.
* Route Distinguisher — one per endpoint, scope ``global``, ``"<asn>:<n>"`` from the RD pool.
* VLAN ID           — one per endpoint, scope ``device:<HOSTNAME>``; from the request or the pool.
* PE-CE /30 subnet  — one per endpoint, scope ``global``; carved from ``Settings.pe_ce_supernet``.
                      PE gets the first host, CE the second.

Idempotency
-----------
Temporal may re-run the allocation activity after a crash. Re-running ``allocate_for_service``
therefore has to yield *exactly* the same values without creating duplicate rows. Per-service
resources use ``AllocationRepository.find``; per-endpoint resources look at the values already
written on the ``ServiceEndpoint`` row and re-attach to the existing allocation. In every case a
candidate that is already held *by this service* counts as a success, not a conflict.

Concurrency
-----------
Uniqueness is enforced by the database (``uq_allocation_type_value_scope``). The allocator only
pre-filters obviously taken values to avoid pointless round-trips; the final word is always the
SAVEPOINT-protected INSERT in ``AllocationRepository.try_allocate``.
"""

from __future__ import annotations

import ipaddress
from collections.abc import Iterable, Iterator
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from atlas.core.config import Settings
from atlas.core.enums import AuditEventType, ResourceType
from atlas.db.models import NetworkService, ResourceAllocation, ServiceEndpoint
from atlas.db.repositories import AllocationRepository, AuditRepository, ResourcePoolExhaustedError
from atlas.schemas import AllocatedResources, EndpointRequest, EndpointResources, L3VPNServiceRequest

__all__ = ["ResourceAllocator", "ResourceConflictError", "ResourcePoolExhaustedError"]


class ResourceConflictError(RuntimeError):
    """An explicitly requested value (RT, VLAN, VRF name, address) is owned by another service."""


def _device_scope(hostname: str) -> str:
    return f"device:{hostname.upper()}"


class ResourceAllocator:
    def __init__(self, session: AsyncSession, settings: Settings) -> None:
        self.session = session
        self.settings = settings
        self.allocations = AllocationRepository(session)
        self.audit = AuditRepository(session)

    # ------------------------------------------------------------------ public API

    async def allocate_for_service(
        self, service: NetworkService, request: L3VPNServiceRequest
    ) -> AllocatedResources:
        """Allocate (or re-attach to) every resource the service needs. Idempotent."""
        endpoints = await self._load_endpoints(service.id)
        if not endpoints:
            raise ValueError(f"service {service.id} has no endpoints to allocate resources for")

        created_before = len(await self.allocations.get_for_service(service.id))

        route_target = await self._allocate_route_target(service.id, request)
        vrf_name = await self._allocate_vrf_name(service.id, request.vrf_name)

        endpoint_resources: list[EndpointResources] = []
        for endpoint in endpoints:
            ep_request = self._match_request(endpoint, request, endpoints)
            rd = await self._allocate_rd(service.id, endpoint)
            vlan = await self._allocate_vlan(service.id, endpoint, ep_request)
            network = await self._allocate_subnet(service.id, endpoint, ep_request)
            hosts = list(network.hosts())
            pe_ip, ce_ip = self._pick_addresses(hosts, endpoint, ep_request)

            endpoint.route_distinguisher = rd
            endpoint.vlan_id = vlan
            endpoint.pe_ip_address = pe_ip
            endpoint.ce_ip_address = ce_ip
            if endpoint.vrf_name != vrf_name:
                endpoint.vrf_name = vrf_name

            endpoint_resources.append(
                EndpointResources(
                    endpoint_id=endpoint.id,
                    device_id=endpoint.device_id,
                    device_hostname=endpoint.device.hostname,
                    vendor=endpoint.device.vendor,
                    interface=endpoint.interface,
                    vlan_id=vlan,
                    vrf_name=vrf_name,
                    route_distinguisher=rd,
                    route_target_import=[route_target],
                    route_target_export=[route_target],
                    pe_ip_address=pe_ip,
                    ce_ip_address=ce_ip,
                    prefix_length=network.prefixlen,
                    routing_protocol=endpoint.routing_protocol,
                    pe_asn=self.settings.bgp_asn,
                    ce_asn=endpoint.ce_asn,
                )
            )

        await self.session.flush()

        rows = await self.allocations.get_for_service(service.id)
        if len(rows) != created_before:
            # Only record when something new was actually claimed (keeps re-runs quiet).
            await self.audit.record(
                AuditEventType.RESOURCES_ALLOCATED,
                service_id=service.id,
                workflow_id=service.active_workflow_id,
                message=f"allocated {len(rows) - created_before} resource(s)",
                details={
                    "route_target": route_target,
                    "vrf_name": vrf_name,
                    "allocations": [
                        {"type": r.resource_type, "value": r.resource_value, "scope": r.scope} for r in rows
                    ],
                },
            )

        return AllocatedResources(
            service_id=service.id,
            vrf_name=vrf_name,
            route_target=route_target,
            endpoints=endpoint_resources,
        )

    async def release_for_service(self, service_id: UUID) -> int:
        """Free every resource held by the service. Returns the number released (0 on re-run)."""
        released = await self.allocations.release(service_id)
        if released:
            await self.audit.record(
                AuditEventType.RESOURCES_RELEASED,
                service_id=service_id,
                message=f"released {released} resource(s)",
                details={"count": released},
            )
        return released

    # ------------------------------------------------------------------ helpers: loading / matching

    async def _load_endpoints(self, service_id: UUID) -> list[ServiceEndpoint]:
        stmt = (
            select(ServiceEndpoint)
            .where(ServiceEndpoint.service_id == service_id)
            .options(selectinload(ServiceEndpoint.device))
            .order_by(ServiceEndpoint.sequence, ServiceEndpoint.created_at)
        )
        return list((await self.session.execute(stmt)).scalars().all())

    @staticmethod
    def _match_request(
        endpoint: ServiceEndpoint, request: L3VPNServiceRequest, all_endpoints: list[ServiceEndpoint]
    ) -> EndpointRequest | None:
        """Find the request entry for a persisted endpoint (device+interface, then VLAN, then position)."""
        host = endpoint.device.hostname.upper()
        same_port = [
            r for r in request.endpoints if r.device.upper() == host and r.interface == endpoint.interface
        ]
        if len(same_port) == 1:
            return same_port[0]
        if same_port:
            for r in same_port:
                if r.vlan is not None and r.vlan == endpoint.vlan_id:
                    return r
        index = all_endpoints.index(endpoint)
        if index < len(request.endpoints):
            return request.endpoints[index]
        return None

    # ------------------------------------------------------------------ helpers: claiming

    async def _owned(self, service_id: UUID, resource_type: ResourceType, value: str, scope: str) -> bool:
        stmt = select(ResourceAllocation.service_id).where(
            ResourceAllocation.resource_type == resource_type,
            ResourceAllocation.resource_value == value,
            ResourceAllocation.scope == scope,
        )
        owner = (await self.session.execute(stmt)).scalar_one_or_none()
        return owner == service_id

    async def _claim(self, service_id: UUID, resource_type: ResourceType, value: str, scope: str) -> bool:
        """Claim ``value``; True when this service holds it afterwards (newly or already)."""
        if await self._owned(service_id, resource_type, value, scope):
            return True
        allocation = await self.allocations.try_allocate(service_id, resource_type, value, scope)
        return allocation is not None

    async def _claim_or_conflict(
        self, service_id: UUID, resource_type: ResourceType, value: str, scope: str
    ) -> str:
        if not await self._claim(service_id, resource_type, value, scope):
            raise ResourceConflictError(
                f"{resource_type} {value!r} (scope {scope}) is held by another service"
            )
        return value

    async def _taken_values(self, resource_type: ResourceType, scope: str) -> set[str]:
        stmt = select(ResourceAllocation.resource_value).where(
            ResourceAllocation.resource_type == resource_type, ResourceAllocation.scope == scope
        )
        return set((await self.session.execute(stmt)).scalars().all())

    async def _claim_first_free(
        self, service_id: UUID, resource_type: ResourceType, candidates: Iterable[str], scope: str
    ) -> str:
        taken = await self._taken_values(resource_type, scope)
        for candidate in candidates:
            if candidate in taken:
                continue
            allocation = await self.allocations.try_allocate(service_id, resource_type, candidate, scope)
            if allocation is not None:
                return candidate
        raise ResourcePoolExhaustedError(f"no free {resource_type} in scope {scope!r}")

    # ------------------------------------------------------------------ per-resource logic

    def _asn_values(self, start: int, end: int) -> Iterator[str]:
        asn = self.settings.bgp_asn
        for n in range(start, end + 1):
            yield f"{asn}:{n}"

    async def _allocate_route_target(self, service_id: UUID, request: L3VPNServiceRequest) -> str:
        existing = await self.allocations.find(service_id, ResourceType.ROUTE_TARGET, "global")
        if existing is not None:
            return existing.resource_value
        if request.route_target:
            return await self._claim_or_conflict(
                service_id, ResourceType.ROUTE_TARGET, request.route_target, "global"
            )
        candidates = self._asn_values(self.settings.rt_pool_start, self.settings.rt_pool_end)
        return await self._claim_first_free(service_id, ResourceType.ROUTE_TARGET, candidates, "global")

    async def _allocate_vrf_name(self, service_id: UUID, vrf_name: str) -> str:
        existing = await self.allocations.find(service_id, ResourceType.VRF_NAME, "global")
        if existing is not None:
            return existing.resource_value
        return await self._claim_or_conflict(service_id, ResourceType.VRF_NAME, vrf_name, "global")

    async def _allocate_rd(self, service_id: UUID, endpoint: ServiceEndpoint) -> str:
        if endpoint.route_distinguisher and await self._owned(
            service_id, ResourceType.ROUTE_DISTINGUISHER, endpoint.route_distinguisher, "global"
        ):
            return endpoint.route_distinguisher
        candidates = self._asn_values(self.settings.rd_pool_start, self.settings.rd_pool_end)
        return await self._claim_first_free(
            service_id, ResourceType.ROUTE_DISTINGUISHER, candidates, "global"
        )

    async def _allocate_vlan(
        self, service_id: UUID, endpoint: ServiceEndpoint, ep_request: EndpointRequest | None
    ) -> int:
        scope = _device_scope(endpoint.device.hostname)
        if endpoint.vlan_id is not None and await self._owned(
            service_id, ResourceType.VLAN_ID, str(endpoint.vlan_id), scope
        ):
            return endpoint.vlan_id
        requested = ep_request.vlan if ep_request is not None else endpoint.vlan_id
        if requested is not None:
            await self._claim_or_conflict(service_id, ResourceType.VLAN_ID, str(requested), scope)
            return requested
        candidates = (str(v) for v in range(self.settings.vlan_pool_start, self.settings.vlan_pool_end + 1))
        return int(await self._claim_first_free(service_id, ResourceType.VLAN_ID, candidates, scope))

    async def _allocate_subnet(
        self, service_id: UUID, endpoint: ServiceEndpoint, ep_request: EndpointRequest | None
    ) -> ipaddress.IPv4Network:
        # Re-run: the PE address already on the endpoint pins the /30.
        if endpoint.pe_ip_address:
            network = ipaddress.ip_interface(f"{endpoint.pe_ip_address}/30").network
            if await self._owned(service_id, ResourceType.IP_SUBNET, str(network), "global"):
                return network  # type: ignore[return-value]

        # Operator-supplied addressing pins the /30 too (must not collide with another service).
        pinned = None
        if ep_request is not None:
            pinned = ep_request.pe_ip_address or ep_request.ce_ip_address
        if pinned:
            network = ipaddress.ip_interface(f"{pinned}/30").network
            await self._claim_or_conflict(service_id, ResourceType.IP_SUBNET, str(network), "global")
            return network  # type: ignore[return-value]

        supernet = ipaddress.ip_network(self.settings.pe_ce_supernet, strict=False)
        candidates = (str(n) for n in supernet.subnets(new_prefix=30))
        value = await self._claim_first_free(service_id, ResourceType.IP_SUBNET, candidates, "global")
        return ipaddress.ip_network(value)  # type: ignore[return-value]

    @staticmethod
    def _pick_addresses(
        hosts: list[ipaddress.IPv4Address], endpoint: ServiceEndpoint, ep_request: EndpointRequest | None
    ) -> tuple[str, str]:
        """PE = first host, CE = second host — unless the operator pinned either address."""
        pe_ip, ce_ip = str(hosts[0]), str(hosts[1])
        req_pe = ep_request.pe_ip_address if ep_request is not None else None
        req_ce = ep_request.ce_ip_address if ep_request is not None else None
        if req_pe and req_ce:
            return req_pe, req_ce
        if req_ce:
            # CE pinned: PE takes the other usable address of the /30.
            other = [str(h) for h in hosts if str(h) != req_ce]
            return (other[0] if other else pe_ip), req_ce
        if req_pe:
            other = [str(h) for h in hosts if str(h) != req_pe]
            return req_pe, (other[0] if other else ce_ip)
        return pe_ip, ce_ip
