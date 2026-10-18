"""ResourceAllocator tests: values, idempotency, conflicts, pool exhaustion, release."""

from __future__ import annotations

import ipaddress

import pytest

from atlas.core.config import Settings
from atlas.core.enums import AuditEventType, ResourceType
from atlas.db.repositories import AllocationRepository, AuditRepository, DeviceRepository, ServiceRepository
from atlas.db.session import init_db_for_tests, reset_engine, transaction
from atlas.schemas import AllocatedResources, EndpointRequest, L3VPNServiceRequest
from atlas.services.allocator import ResourceAllocator, ResourceConflictError, ResourcePoolExhaustedError
from tests.unit.db_helpers import acme_request, create_service, seeded


@pytest.fixture
async def db():
    maker = await init_db_for_tests()
    try:
        yield maker
    finally:
        await reset_engine()


@pytest.fixture
def settings() -> Settings:
    return Settings(database_url="sqlite+aiosqlite:///:memory:")


async def _provision(settings: Settings, request: L3VPNServiceRequest) -> AllocatedResources:
    async with transaction() as session:
        devices = await DeviceRepository(session).get_many_by_hostnames([e.device for e in request.endpoints])
        service = await create_service(session, request, devices)
        return await ResourceAllocator(session, settings).allocate_for_service(service, request)


async def test_allocates_expected_values(db, settings):
    async with transaction() as session:
        await seeded(session)
    request = acme_request()
    res = await _provision(settings, request)

    assert res.vrf_name == "ACME-PROD"
    assert res.route_target == "65000:100"
    assert [e.device_hostname for e in res.endpoints] == ["PE1", "PE2", "PE3"]
    assert [e.route_distinguisher for e in res.endpoints] == ["65000:1000", "65000:1001", "65000:1002"]
    assert [e.vlan_id for e in res.endpoints] == [100, 100, 100]
    assert [e.pe_ip_address for e in res.endpoints] == ["10.255.0.1", "10.255.0.5", "10.255.0.9"]
    assert [e.ce_ip_address for e in res.endpoints] == ["10.255.0.2", "10.255.0.6", "10.255.0.10"]
    for e in res.endpoints:
        assert e.prefix_length == 30
        assert e.pe_asn == 65000
        assert e.vendor == "mock"
        assert e.route_target_import == e.route_target_export == ["65000:100"]

    async with transaction() as session:
        svc = await ServiceRepository(session).get(res.service_id)
        eps = svc.endpoints
        assert [ep.route_distinguisher for ep in eps] == ["65000:1000", "65000:1001", "65000:1002"]
        assert [ep.pe_ip_address for ep in eps] == ["10.255.0.1", "10.255.0.5", "10.255.0.9"]
        rows = await AllocationRepository(session).get_for_service(res.service_id)
        by_type = {}
        for r in rows:
            by_type.setdefault(r.resource_type, []).append((r.resource_value, r.scope))
        assert by_type[ResourceType.ROUTE_TARGET] == [("65000:100", "global")]
        assert by_type[ResourceType.VRF_NAME] == [("ACME-PROD", "global")]
        assert len(by_type[ResourceType.ROUTE_DISTINGUISHER]) == 3
        assert sorted(s for _, s in by_type[ResourceType.VLAN_ID]) == [
            "device:PE1",
            "device:PE2",
            "device:PE3",
        ]
        assert [v for v, _ in by_type[ResourceType.IP_SUBNET]] == [
            "10.255.0.0/30",
            "10.255.0.4/30",
            "10.255.0.8/30",
        ]
        assert len(rows) == 2 + 3 * 3
        events, _ = await AuditRepository(session).history(res.service_id)
        assert [e.event_type for e in events] == [AuditEventType.RESOURCES_ALLOCATED]


async def test_allocation_is_idempotent(db, settings):
    async with transaction() as session:
        await seeded(session)
    request = acme_request()
    first = await _provision(settings, request)

    async with transaction() as session:
        service = await ServiceRepository(session).get(first.service_id)
        second = await ResourceAllocator(session, settings).allocate_for_service(service, request)
        third = await ResourceAllocator(session, settings).allocate_for_service(service, request)
        assert second == first == third
        rows = await AllocationRepository(session).get_for_service(first.service_id)
        assert len(rows) == 11  # no duplicates
        events, _ = await AuditRepository(session).history(first.service_id)
        assert len(events) == 1  # re-runs are silent


async def test_pool_allocation_skips_values_held_by_other_services(db, settings):
    async with transaction() as session:
        await seeded(session)
    a = await _provision(settings, acme_request("SVC-A", vrf="VRF-A", route_target=None, vlan=None))
    b = await _provision(settings, acme_request("SVC-B", vrf="VRF-B", route_target=None, vlan=None))

    assert a.route_target == "65000:100" and b.route_target == "65000:101"
    assert [e.route_distinguisher for e in b.endpoints] == ["65000:1003", "65000:1004", "65000:1005"]
    # VLAN pools are per device: both services get the first free VLAN on each PE.
    assert [e.vlan_id for e in a.endpoints] == [100, 100, 100]
    assert [e.vlan_id for e in b.endpoints] == [101, 101, 101]
    assert [e.pe_ip_address for e in b.endpoints] == ["10.255.0.13", "10.255.0.17", "10.255.0.21"]


async def test_explicit_route_target_conflict_raises(db, settings):
    async with transaction() as session:
        await seeded(session)
    await _provision(settings, acme_request("SVC-A", vrf="VRF-A"))
    with pytest.raises(ResourceConflictError):
        await _provision(settings, acme_request("SVC-B", vrf="VRF-B", vlan=200))  # same RT 65000:100


async def test_explicit_vlan_conflict_on_same_device_raises(db, settings):
    async with transaction() as session:
        await seeded(session)
    await _provision(settings, acme_request("SVC-A", vrf="VRF-A", route_target="65000:100", vlan=300))
    with pytest.raises(ResourceConflictError):
        # Different physical ports, same VLAN on the same PE -> per-device VLAN pool conflict.
        await _provision(
            settings,
            acme_request(
                "SVC-B", vrf="VRF-B", route_target="65000:200", vlan=300, interface_prefix="TenGigE0/0/0/"
            ),
        )


async def test_vrf_name_must_be_globally_unique(db, settings):
    async with transaction() as session:
        await seeded(session)
    await _provision(settings, acme_request("SVC-A", vrf="SHARED", route_target="65000:100", vlan=100))
    with pytest.raises(ResourceConflictError):
        await _provision(settings, acme_request("SVC-B", vrf="SHARED", route_target="65000:200", vlan=200))


async def test_pool_exhausted(db):
    tiny = Settings(database_url="sqlite+aiosqlite:///:memory:", rd_pool_start=1000, rd_pool_end=1001)
    async with transaction() as session:
        await seeded(session)
    with pytest.raises(ResourcePoolExhaustedError):
        await _provision(tiny, acme_request())  # three endpoints need three RDs, pool has two


async def test_operator_pinned_ce_address(db, settings):
    async with transaction() as session:
        await seeded(session)
    request = L3VPNServiceRequest(
        customer_id=acme_request().customer_id,
        service_name="PINNED",
        vrf_name="PINNED",
        route_target="65000:500",
        endpoints=[
            EndpointRequest(
                device="PE1", interface="GigabitEthernet0/0/0/1", vlan=500, ce_ip_address="10.9.9.2"
            ),
            EndpointRequest(
                device="PE2",
                interface="GigabitEthernet0/0/0/2",
                vlan=500,
                routing_protocol="BGP",
                ce_asn=65010,
            ),
        ],
    )
    res = await _provision(settings, request)
    assert (res.endpoints[0].pe_ip_address, res.endpoints[0].ce_ip_address) == ("10.9.9.1", "10.9.9.2")
    assert ipaddress.ip_network("10.9.9.0/30") == ipaddress.ip_interface("10.9.9.1/30").network
    assert res.endpoints[1].routing_protocol == "BGP" and res.endpoints[1].ce_asn == 65010
    assert res.endpoints[1].pe_ip_address == "10.255.0.1"


async def test_release_frees_everything_and_is_idempotent(db, settings):
    async with transaction() as session:
        await seeded(session)
    res = await _provision(settings, acme_request())
    async with transaction() as session:
        allocator = ResourceAllocator(session, settings)
        assert await allocator.release_for_service(res.service_id) == 11
        assert await allocator.release_for_service(res.service_id) == 0
        assert await AllocationRepository(session).get_for_service(res.service_id) == []
        events, _ = await AuditRepository(session).history(res.service_id)
        assert [e.event_type for e in events][-1] == AuditEventType.RESOURCES_RELEASED
    # Slots are free again: a new service can take the exact same values.
    again = await _provision(settings, acme_request("ACME-L3VPN-002", vlan=200))
    assert again.route_target == "65000:100"
    assert [e.route_distinguisher for e in again.endpoints] == ["65000:1000", "65000:1001", "65000:1002"]
