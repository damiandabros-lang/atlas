"""``GET /api/v1/topology`` and the bundled topology UI."""

from __future__ import annotations

import uuid

from sqlalchemy import select

from atlas.core.enums import LifecycleStatus, ServiceType
from atlas.db.models import NetworkDevice, NetworkLink, NetworkService, ServiceEndpoint
from tests.unit.api_fixtures import AUTH, CUSTOMER_ID, ApiContext

pytest_plugins = ("tests.unit.api_fixtures",)


async def _add_service(api: ApiContext, name: str, status: LifecycleStatus, hosts: list[str]) -> uuid.UUID:
    async with api.sessionmaker() as s:
        devs = {d.hostname: d for d in (await s.execute(select(NetworkDevice))).scalars()}
        svc = NetworkService(
            customer_id=CUSTOMER_ID,
            service_type=ServiceType.MPLS_L3VPN,
            service_name=name,
            desired_state={"vrf_name": f"VRF-{name}", "route_target": "65000:100"},
            lifecycle_status=status,
        )
        s.add(svc)
        await s.flush()
        for i, h in enumerate(hosts):
            s.add(
                ServiceEndpoint(
                    service_id=svc.id,
                    device_id=devs[h].id,
                    sequence=i,
                    interface=f"GigabitEthernet0/0/0/{i + 1}",
                    vlan_id=100 + i + (900 if name == "OLD" else 0),
                    vrf_name=f"VRF-{name}",
                )
            )
        if not (await s.execute(select(NetworkLink))).scalars().first():
            s.add(
                NetworkLink(
                    a_device_id=devs["PE1"].id,
                    a_interface="Hu0/0/0/0",
                    b_device_id=devs["PE2"].id,
                    b_interface="Hu0/0/0/0",
                    bandwidth_gbps=100,
                )
            )
        await s.commit()
        return svc.id


async def test_topology_nodes_links_and_overlays(api: ApiContext) -> None:
    active = await _add_service(api, "ACME", LifecycleStatus.ACTIVE, ["PE1", "PE2", "PE3"])
    await _add_service(api, "OLD", LifecycleStatus.DECOMMISSIONED, ["PE1"])

    r = await api.client.get("/api/v1/topology", headers=AUTH)
    assert r.status_code == 200, r.text
    body = r.json()
    assert {"PE1", "PE2", "PE3"} <= {n["hostname"] for n in body["nodes"]}
    assert body["links"] and body["links"][0]["a"] == "PE1" and body["links"][0]["b"] == "PE2"
    # Terminal services are hidden by default, visible with include_all.
    assert [s["service_name"] for s in body["services"]] == ["ACME"]
    svc = body["services"][0]
    assert svc["id"] == str(active) and svc["vrf_name"] == "VRF-ACME" and len(svc["attachments"]) == 3
    pe1 = next(n for n in body["nodes"] if n["hostname"] == "PE1")
    assert pe1["services"] == ["ACME"]

    r = await api.client.get("/api/v1/topology?include_all=true", headers=AUTH)
    assert {s["service_name"] for s in r.json()["services"]} == {"ACME", "OLD"}


async def test_topology_requires_api_key_and_ui_is_public(api: ApiContext) -> None:
    assert (await api.client.get("/api/v1/topology")).status_code == 401
    r = await api.client.get("/ui/topology")
    assert r.status_code == 200 and "ATLAS" in r.text and "/api/v1/topology" in r.text
    r = await api.client.get("/", follow_redirects=False)
    assert r.status_code in (302, 307) and r.headers["location"] == "/ui/topology"
