"""End-to-end: REST API -> Temporal (time-skipping test server) -> real activities -> SQLite -> mock devices.

This is the integration proof that every layer fits together with *no stubs*:
the only substitutions are an in-memory SQLite database and the Temporal test server.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from datetime import timedelta

import pytest
from httpx import ASGITransport, AsyncClient
from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from atlas.activities import ALL_ACTIVITIES
from atlas.adapters.mock import MockNetworkAdapter
from atlas.adapters.registry import reset_adapters
from atlas.api.deps import get_db, get_temporal
from atlas.api.main import create_app
from atlas.core.config import get_settings
from atlas.core.enums import DriftStatus, FailureMode, LifecycleStatus
from atlas.db.seed import seed
from atlas.db.session import init_db_for_tests, reset_engine, transaction
from atlas.schemas import ReconciliationReport
from atlas.services.temporal_client import reset_temporal_client, set_temporal_client
from atlas.workflows import ALL_WORKFLOWS

API_KEY = "e2e-key"
AUTH = {"X-API-Key": API_KEY}
CUSTOMER_ID = "550e8400-e29b-41d4-a716-446655440000"
TASK_QUEUE = "atlas-e2e"


def l3vpn_payload(name: str = "ACME-L3VPN-001") -> dict:
    return {
        "customer_id": CUSTOMER_ID,
        "service_name": name,
        "vrf_name": "ACME-PROD",
        "route_target": "65000:100",
        "endpoints": [
            {"device": "PE1", "interface": "GigabitEthernet0/0/0/1", "vlan": 100},
            {"device": "PE2", "interface": "GigabitEthernet0/0/0/2", "vlan": 100},
            {"device": "PE3", "interface": "GigabitEthernet0/0/0/3", "vlan": 100},
        ],
    }


@pytest.fixture
async def env() -> AsyncIterator[WorkflowEnvironment]:
    async with await WorkflowEnvironment.start_time_skipping(data_converter=pydantic_data_converter) as e:
        yield e


@pytest.fixture
async def stack(env: WorkflowEnvironment, monkeypatch: pytest.MonkeyPatch, tmp_path) -> AsyncIterator[dict]:
    # Settings are read through the cached ``get_settings()`` everywhere, so drive them via env.
    monkeypatch.setenv("ATLAS_API_KEYS", f'["{API_KEY}"]')
    monkeypatch.setenv("ATLAS_TEMPORAL_TASK_QUEUE", TASK_QUEUE)
    monkeypatch.setenv("ATLAS_ADAPTER_BACKEND", "mock")
    monkeypatch.setenv("ATLAS_MOCK_LATENCY_MS", "0")
    monkeypatch.setenv("ATLAS_MOCK_FAILURE_MODE", "NONE")
    monkeypatch.setenv("ATLAS_METRICS_ENABLED", "false")
    monkeypatch.setenv("ATLAS_LOG_JSON", "false")
    get_settings.cache_clear()
    settings = get_settings()

    MockNetworkAdapter.reset()
    MockNetworkAdapter.set_failure_mode(FailureMode.NONE)
    reset_adapters()
    # File-based SQLite: the API and the worker must NOT share one connection, otherwise a
    # ROLLBACK from a read-only API session could undo an activity's in-flight transaction.
    db_file = tmp_path / "atlas-e2e.db"
    sessionmaker = await init_db_for_tests(f"sqlite+aiosqlite:///{db_file}")
    async with transaction() as s:
        await seed(s)

    client: Client = env.client
    set_temporal_client(client)

    app = create_app(settings)

    async def _db():
        async with sessionmaker() as s:
            yield s

    app.dependency_overrides[get_db] = _db
    app.dependency_overrides[get_temporal] = lambda: client

    async with Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=ALL_WORKFLOWS,
        activities=ALL_ACTIVITIES,
        default_heartbeat_throttle_interval=timedelta(milliseconds=50),
        max_heartbeat_throttle_interval=timedelta(milliseconds=50),
    ):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://atlas") as http:
            yield {"http": http, "client": client, "settings": settings}

    reset_temporal_client()
    reset_adapters()
    MockNetworkAdapter.reset()
    await reset_engine()
    get_settings.cache_clear()


async def _wait_terminal(
    http: AsyncClient, service_id: str, max_wait: float = 60.0, *, terminal: set[str] | None = None
) -> dict:
    terminal = terminal or {
        "ACTIVE",
        "FAILED",
        "ROLLED_BACK",
        "MANUAL_INTERVENTION_REQUIRED",
        "DECOMMISSIONED",
    }
    deadline = asyncio.get_event_loop().time() + max_wait
    while True:
        r = await http.get(f"/api/v1/services/{service_id}/status", headers=AUTH)
        assert r.status_code == 200, r.text
        body = r.json()
        if body["lifecycle_status"] in terminal:
            return body
        assert asyncio.get_event_loop().time() < deadline, f"timed out in {body['lifecycle_status']}"
        await asyncio.sleep(0.2)


async def test_happy_path_three_pe_l3vpn_becomes_active(stack: dict) -> None:
    http: AsyncClient = stack["http"]
    r = await http.post(
        "/api/v1/services/l3vpn", json=l3vpn_payload(), headers={**AUTH, "Idempotency-Key": str(uuid.uuid4())}
    )
    assert r.status_code == 202, r.text
    accepted = r.json()
    assert accepted["status"] == "REQUESTED"

    status = await _wait_terminal(http, accepted["service_id"])
    assert status["lifecycle_status"] == "ACTIVE", status
    assert {ep["status"] for ep in status["endpoints"]} == {"VERIFIED"}

    # Devices really carry the VRF now.
    for host in ("PE1", "PE2", "PE3"):
        st = MockNetworkAdapter.state(host)
        assert "ACME-PROD" in st["vrfs"], host
        assert st["vrfs"]["ACME-PROD"]["rt_import"] == ["65000:100"]

    svc = (await http.get(f"/api/v1/services/{accepted['service_id']}", headers=AUTH)).json()
    rds = {ep["route_distinguisher"] for ep in svc["endpoints"]}
    assert len(rds) == 3 and all(rd.startswith("65000:") for rd in rds)
    assert {a["resource_type"] for a in svc["allocations"]} >= {
        "ROUTE_TARGET",
        "ROUTE_DISTINGUISHER",
        "VLAN_ID",
        "IP_SUBNET",
        "VRF_NAME",
    }

    hist = (await http.get(f"/api/v1/services/{accepted['service_id']}/history", headers=AUTH)).json()
    events = [e["event_type"] for e in hist["items"]]
    assert "SERVICE_REQUESTED" in events and "STATUS_CHANGED" in events and "DEVICE_OPERATION" in events


async def test_fail_on_pe3_rolls_back_pe1_and_pe2(stack: dict) -> None:
    http: AsyncClient = stack["http"]
    MockNetworkAdapter.set_failure_mode(FailureMode.FAIL_ON_PE3)

    r = await http.post(
        "/api/v1/services/l3vpn",
        json=l3vpn_payload("ACME-L3VPN-FAIL"),
        headers={**AUTH, "Idempotency-Key": str(uuid.uuid4())},
    )
    assert r.status_code == 202, r.text
    status = await _wait_terminal(http, r.json()["service_id"])
    assert status["lifecycle_status"] == "ROLLED_BACK", status

    for host in ("PE1", "PE2", "PE3"):
        assert "ACME-PROD" not in MockNetworkAdapter.state(host)["vrfs"], host

    svc = (await http.get(f"/api/v1/services/{r.json()['service_id']}", headers=AUTH)).json()
    assert svc["allocations"] == [], "resources released after compensation"
    hist = (await http.get(f"/api/v1/services/{svc['id']}/history", headers=AUTH)).json()
    assert "COMPENSATION_EXECUTED" in {e["event_type"] for e in hist["items"]}


async def test_timeout_on_pe2_retries_then_active(stack: dict) -> None:
    http: AsyncClient = stack["http"]
    MockNetworkAdapter.set_failure_mode(FailureMode.TIMEOUT_ON_PE2)

    r = await http.post(
        "/api/v1/services/l3vpn",
        json=l3vpn_payload("ACME-L3VPN-RETRY"),
        headers={**AUTH, "Idempotency-Key": str(uuid.uuid4())},
    )
    assert r.status_code == 202, r.text
    status = await _wait_terminal(http, r.json()["service_id"], max_wait=120)
    assert status["lifecycle_status"] == "ACTIVE", status


async def test_reconcile_detects_drift_then_decommission(stack: dict) -> None:
    http: AsyncClient = stack["http"]
    client: Client = stack["client"]

    r = await http.post(
        "/api/v1/services/l3vpn",
        json=l3vpn_payload("ACME-L3VPN-DRIFT"),
        headers={**AUTH, "Idempotency-Key": str(uuid.uuid4())},
    )
    sid = r.json()["service_id"]
    assert (await _wait_terminal(http, sid))["lifecycle_status"] == "ACTIVE"

    MockNetworkAdapter.inject_drift("PE1", "vrfs.ACME-PROD.rt_import", ["65000:200"])
    MockNetworkAdapter.inject_drift("PE1", "vrfs.ACME-PROD.rt_export", ["65000:200"])

    rr = await http.post(f"/api/v1/services/{sid}/reconcile", json={"mode": "DETECT_ONLY"}, headers=AUTH)
    assert rr.status_code == 202, rr.text
    report = await client.get_workflow_handle(
        rr.json()["workflow_id"], result_type=ReconciliationReport
    ).result()
    by_dev = {d.device: d for d in report.devices}
    assert by_dev["PE1"].status == DriftStatus.DRIFT_DETECTED
    diff = by_dev["PE1"].differences[0]
    assert (diff.field, diff.desired, diff.observed) == ("route_target", "65000:100", "65000:200")
    assert by_dev["PE2"].status == DriftStatus.IN_SYNC

    forbidden = await http.post(
        f"/api/v1/services/{sid}/reconcile", json={"mode": "AUTO_REMEDIATE"}, headers=AUTH
    )
    assert forbidden.status_code == 403

    d = await http.delete(f"/api/v1/services/{sid}", headers=AUTH)
    assert d.status_code == 202, d.text
    final = await _wait_terminal(
        http, sid, terminal={"DECOMMISSIONED", "MANUAL_INTERVENTION_REQUIRED", "FAILED"}
    )
    assert final["lifecycle_status"] == "DECOMMISSIONED", final
    for host in ("PE1", "PE2", "PE3"):
        assert "ACME-PROD" not in MockNetworkAdapter.state(host)["vrfs"], host


async def test_cancel_in_flight_service(stack: dict) -> None:
    http: AsyncClient = stack["http"]
    client: Client = stack["client"]
    # Slow every device operation down so the cancel lands while a device apply is running
    # (cancellation reaches an activity through its heartbeat, i.e. only while it executes).
    from atlas.adapters.registry import get_adapter

    adapter = get_adapter("mock")
    adapter.settings = adapter.settings.model_copy(update={"mock_latency_ms": 1500})

    r = await http.post(
        "/api/v1/services/l3vpn",
        json=l3vpn_payload("ACME-L3VPN-CANCEL"),
        headers={**AUTH, "Idempotency-Key": str(uuid.uuid4())},
    )
    sid = r.json()["service_id"]
    handle = client.get_workflow_handle(r.json()["workflow_id"])
    # Wait until PE1 is being provisioned, then cancel through the API.
    for _ in range(200):
        progress = await handle.query("progress")
        if progress.get("stage") == "PROVISIONING":
            break
        await asyncio.sleep(0.05)
    c = await http.post(f"/api/v1/services/{sid}/cancel", headers=AUTH)
    assert c.status_code == 202, c.text
    status = await _wait_terminal(http, sid, max_wait=120)
    assert status["lifecycle_status"] in {"ROLLED_BACK", "FAILED"}, status
    for host in ("PE1", "PE2", "PE3"):
        assert "ACME-PROD" not in MockNetworkAdapter.state(host)["vrfs"], host
    assert LifecycleStatus(status["lifecycle_status"]) is not LifecycleStatus.ACTIVE
