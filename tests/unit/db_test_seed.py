"""Seed tests: reference inventory content and idempotent re-runs."""

from __future__ import annotations

import pytest
from sqlalchemy import func, select

from atlas.core.enums import DeviceRole, Vendor
from atlas.db.models import Customer, NetworkDevice, Site
from atlas.db.repositories import CustomerRepository, DeviceRepository
from atlas.db.seed import ACME_CUSTOMER_ID, seed
from atlas.db.session import init_db_for_tests, reset_engine, transaction


@pytest.fixture
async def db():
    maker = await init_db_for_tests()
    try:
        yield maker
    finally:
        await reset_engine()


async def _counts(session) -> tuple[int, int, int]:
    c = (await session.execute(select(func.count()).select_from(Customer))).scalar_one()
    s = (await session.execute(select(func.count()).select_from(Site))).scalar_one()
    d = (await session.execute(select(func.count()).select_from(NetworkDevice))).scalar_one()
    return c, s, d


async def test_seed_creates_reference_inventory(db):
    async with transaction() as session:
        counts = await seed(session)
        assert counts == {"customers": 2, "sites": 4, "devices": 6}
    async with transaction() as session:
        acme = await CustomerRepository(session).get(ACME_CUSTOMER_ID)
        assert acme is not None and acme.name == "ACME Corporation"
        assert await CustomerRepository(session).get_by_name("Globex") is not None

        devices = DeviceRepository(session)
        pes, total = await devices.list(role=DeviceRole.PE)
        assert total == 3
        assert [d.hostname for d in pes] == ["PE1", "PE2", "PE3"]
        assert [d.loopback_ip for d in pes] == ["10.0.0.1", "10.0.0.2", "10.0.0.3"]
        assert [d.management_ip for d in pes] == ["192.0.2.11", "192.0.2.12", "192.0.2.13"]
        assert {d.vendor for d in pes} == {Vendor.MOCK}
        assert {d.platform for d in pes} == {"mock-iosxr"}
        assert {d.bgp_asn for d in pes} == {65000}
        assert (await devices.list(role=DeviceRole.P))[1] == 2
        assert (await devices.list(role=DeviceRole.RR))[1] == 1

        pe1 = await devices.get_by_hostname("PE1")
        site = await session.get(Site, pe1.site_id)
        assert site.name == "Warsaw"
        rr1 = await devices.get_by_hostname("rr1")
        assert (await session.get(Site, rr1.site_id)).name == "Core"


async def test_seed_is_idempotent(db):
    async with transaction() as session:
        await seed(session)
        pe1 = await DeviceRepository(session).get_by_hostname("PE1")
        pe1.management_ip = "10.99.99.99"  # drift that the re-seed should repair
        first_ids = {d.hostname: d.id for (d,) in (await session.execute(select(NetworkDevice))).all()}
    async with transaction() as session:
        await seed(session)
        await seed(session)
        assert await _counts(session) == (2, 4, 6)
        pe1 = await DeviceRepository(session).get_by_hostname("PE1")
        assert pe1.management_ip == "192.0.2.11"
        second_ids = {d.hostname: d.id for (d,) in (await session.execute(select(NetworkDevice))).all()}
        assert first_ids == second_ids  # rows updated in place, never recreated
