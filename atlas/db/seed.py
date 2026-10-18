"""Development / test inventory seed.

``seed()`` is an idempotent upsert: running it twice leaves the same rows in place (matched by
customer name, site name and device hostname). ``run()`` is the ``atlas-seed`` console entry
point and executes the seed inside one ``transaction()`` against ``Settings.database_url``.

Topology
--------
    PE1 (Warsaw) ─┐
    PE2 (Krakow) ─┼─ P1 / P2 / RR1 (Core)
    PE3 (Gdansk) ─┘

All devices are ``vendor=mock`` so the MVP runs end-to-end against ``MockNetworkAdapter``.
"""

from __future__ import annotations

import asyncio
import sys
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from atlas.core.enums import DeviceOperationalStatus, DeviceRole, Vendor
from atlas.db.models import Customer, Site
from atlas.db.repositories import CustomerRepository, DeviceRepository
from atlas.db.session import transaction

ACME_CUSTOMER_ID = UUID("550e8400-e29b-41d4-a716-446655440000")
GLOBEX_CUSTOMER_ID = UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")

CUSTOMERS: list[dict[str, Any]] = [
    {"id": ACME_CUSTOMER_ID, "name": "ACME Corporation", "external_ref": "CRM-ACME-0001"},
    {"id": GLOBEX_CUSTOMER_ID, "name": "Globex", "external_ref": "CRM-GLOBEX-0002"},
]

SITES: list[dict[str, Any]] = [
    {"name": "Warsaw", "location": "Warsaw, PL"},
    {"name": "Krakow", "location": "Krakow, PL"},
    {"name": "Gdansk", "location": "Gdansk, PL"},
    {"name": "Core", "location": "MPLS core (virtual)"},
]

_COMMON: dict[str, Any] = {
    "vendor": Vendor.MOCK,
    "platform": "mock-iosxr",
    "software_version": "7.9.2-mock",
    "operational_status": DeviceOperationalStatus.UP,
    "bgp_asn": 65000,
}

DEVICES: list[dict[str, Any]] = [
    {
        "hostname": "PE1",
        "role": DeviceRole.PE,
        "site": "Warsaw",
        "management_ip": "192.0.2.11",
        "loopback_ip": "10.0.0.1",
    },
    {
        "hostname": "PE2",
        "role": DeviceRole.PE,
        "site": "Krakow",
        "management_ip": "192.0.2.12",
        "loopback_ip": "10.0.0.2",
    },
    {
        "hostname": "PE3",
        "role": DeviceRole.PE,
        "site": "Gdansk",
        "management_ip": "192.0.2.13",
        "loopback_ip": "10.0.0.3",
    },
    {
        "hostname": "P1",
        "role": DeviceRole.P,
        "site": "Core",
        "management_ip": "192.0.2.21",
        "loopback_ip": "10.0.0.11",
    },
    {
        "hostname": "P2",
        "role": DeviceRole.P,
        "site": "Core",
        "management_ip": "192.0.2.22",
        "loopback_ip": "10.0.0.12",
    },
    {
        "hostname": "RR1",
        "role": DeviceRole.RR,
        "site": "Core",
        "management_ip": "192.0.2.31",
        "loopback_ip": "10.0.0.21",
    },
]


async def _upsert_customers(session: AsyncSession) -> dict[str, Customer]:
    repo = CustomerRepository(session)
    out: dict[str, Customer] = {}
    for spec in CUSTOMERS:
        customer = await repo.get_by_name(spec["name"])
        if customer is None:
            customer = Customer(**spec, is_active=True)
            session.add(customer)
        else:
            customer.external_ref = spec["external_ref"]
            customer.is_active = True
        out[spec["name"]] = customer
    await session.flush()
    return out


async def _upsert_sites(session: AsyncSession) -> dict[str, Site]:
    out: dict[str, Site] = {}
    for spec in SITES:
        site = (await session.execute(select(Site).where(Site.name == spec["name"]))).scalar_one_or_none()
        if site is None:
            site = Site(**spec)
            session.add(site)
        else:
            site.location = spec["location"]
        out[spec["name"]] = site
    await session.flush()
    return out


async def _upsert_devices(session: AsyncSession, sites: dict[str, Site]) -> int:
    repo = DeviceRepository(session)
    for spec in DEVICES:
        fields = {**_COMMON, **{k: v for k, v in spec.items() if k != "site"}}
        fields["site_id"] = sites[spec["site"]].id
        fields["attributes"] = {"seeded": True, "site": spec["site"]}
        await repo.upsert(**fields)
    return len(DEVICES)


async def seed(session: AsyncSession) -> dict[str, int]:
    """Upsert the reference inventory. Never commits — the caller owns the transaction."""
    customers = await _upsert_customers(session)
    sites = await _upsert_sites(session)
    devices = await _upsert_devices(session, sites)
    return {"customers": len(customers), "sites": len(sites), "devices": devices}


async def _main() -> dict[str, int]:
    async with transaction() as session:
        return await seed(session)


def run() -> None:
    """Console entry point (``atlas-seed``)."""
    counts = asyncio.run(_main())
    sys.stdout.write(
        f"seeded {counts['customers']} customers, {counts['sites']} sites, {counts['devices']} devices\n"
    )


if __name__ == "__main__":  # pragma: no cover
    run()
