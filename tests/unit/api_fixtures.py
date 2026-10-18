"""Shared fixtures for the API unit tests (imported explicitly by each ``api_test_*`` module).

* SQLite in-memory database via ``atlas.db.session.init_db_for_tests`` seeded with one
  customer and three mock PE routers.
* ``FakeTemporalClient`` standing in for ``temporalio.client.Client`` — records every
  ``start_workflow`` call and serves fake workflow handles.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from temporalio.client import WorkflowExecutionStatus
from temporalio.exceptions import WorkflowAlreadyStartedError

from atlas.api.deps import get_db, get_temporal
from atlas.api.main import create_app
from atlas.core.config import Settings
from atlas.core.enums import DeviceRole, Vendor
from atlas.db.models import Customer, NetworkDevice
from atlas.db.session import init_db_for_tests, reset_engine
from atlas.services.temporal_client import reset_temporal_client, set_temporal_client

API_KEY = "test-operator-key"
AUTH = {"X-API-Key": API_KEY}
CUSTOMER_ID = uuid.UUID("550e8400-e29b-41d4-a716-446655440000")
PE_HOSTNAMES = ("PE1", "PE2", "PE3")


@dataclass
class FakeDescription:
    status: WorkflowExecutionStatus = WorkflowExecutionStatus.RUNNING


@dataclass
class FakeWorkflowHandle:
    id: str
    client: FakeTemporalClient
    signals: list[tuple[str, tuple[Any, ...]]] = field(default_factory=list)
    cancelled: bool = False

    async def signal(self, name: str, *args: Any, **_: Any) -> None:
        self.signals.append((name, args))

    async def cancel(self) -> None:
        self.cancelled = True

    async def query(self, name: str, *args: Any, **_: Any) -> Any:
        if name == "progress":
            return self.client.progress
        raise RuntimeError(f"unknown query {name}")

    async def describe(self) -> FakeDescription:
        return FakeDescription(status=self.client.describe_status)


@dataclass
class FakeTemporalClient:
    started: list[dict[str, Any]] = field(default_factory=list)
    handles: dict[str, FakeWorkflowHandle] = field(default_factory=dict)
    raise_already_started: bool = False
    describe_status: WorkflowExecutionStatus = WorkflowExecutionStatus.RUNNING
    progress: Any = field(default_factory=lambda: {"stage": "PROVISIONING", "completed_devices": ["PE1"]})

    async def start_workflow(
        self, workflow: Any, arg: Any = None, *, id: str, task_queue: str, **kwargs: Any
    ) -> Any:
        if self.raise_already_started:
            raise WorkflowAlreadyStartedError(id, str(workflow))
        self.started.append({"workflow": workflow, "arg": arg, "id": id, "task_queue": task_queue, **kwargs})
        return self.get_workflow_handle(id)

    def get_workflow_handle(self, workflow_id: str, **_: Any) -> FakeWorkflowHandle:
        handle = self.handles.get(workflow_id)
        if handle is None:
            handle = FakeWorkflowHandle(id=workflow_id, client=self)
            self.handles[workflow_id] = handle
        return handle


@dataclass
class ApiContext:
    client: AsyncClient
    temporal: FakeTemporalClient
    sessionmaker: async_sessionmaker[AsyncSession]
    settings: Settings
    devices: dict[str, uuid.UUID]


async def _seed(sessionmaker: async_sessionmaker[AsyncSession]) -> dict[str, uuid.UUID]:
    devices: dict[str, uuid.UUID] = {}
    async with sessionmaker() as session:
        session.add(Customer(id=CUSTOMER_ID, name="ACME Corporation"))
        for n, hostname in enumerate(PE_HOSTNAMES, start=1):
            device = NetworkDevice(
                hostname=hostname,
                management_ip=f"192.0.2.{n}",
                vendor=Vendor.MOCK,
                platform="mock-xr",
                software_version="1.0",
                role=DeviceRole.PE,
                loopback_ip=f"10.0.0.{n}",
                bgp_asn=65000,
            )
            session.add(device)
            await session.flush()
            devices[hostname] = device.id
        session.add(
            NetworkDevice(
                hostname="P1",
                management_ip="192.0.2.100",
                vendor=Vendor.MOCK,
                platform="mock-xr",
                software_version="1.0",
                role=DeviceRole.P,
            )
        )
        await session.commit()
    return devices


@pytest.fixture
async def api(tmp_path: Any) -> AsyncIterator[ApiContext]:
    sessionmaker = await init_db_for_tests()
    devices = await _seed(sessionmaker)
    settings = Settings(api_keys=[API_KEY], database_url="sqlite+aiosqlite:///:memory:", metrics_enabled=True)
    fake = FakeTemporalClient()
    set_temporal_client(fake)  # type: ignore[arg-type]  -- health probe uses the cached client

    app = create_app(settings)

    async def _db() -> AsyncIterator[AsyncSession]:
        async with sessionmaker() as session:
            yield session

    async def _temporal() -> FakeTemporalClient:
        return fake

    app.dependency_overrides[get_db] = _db
    app.dependency_overrides[get_temporal] = _temporal

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield ApiContext(
            client=client, temporal=fake, sessionmaker=sessionmaker, settings=settings, devices=devices
        )

    reset_temporal_client()
    await reset_engine()


def l3vpn_payload(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "customer_id": str(CUSTOMER_ID),
        "service_name": "ACME-L3VPN-001",
        "vrf_name": "ACME-PROD",
        "route_target": "65000:100",
        "endpoints": [
            {"device": "PE1", "interface": "GigabitEthernet0/0/0/1", "vlan": 100},
            {"device": "PE2", "interface": "GigabitEthernet0/0/0/2", "vlan": 100},
            {"device": "PE3", "interface": "GigabitEthernet0/0/0/3", "vlan": 100},
        ],
    }
    body.update(overrides)
    return body


async def create_service(api: ApiContext, key: str = "idem-1", **overrides: Any) -> dict[str, Any]:
    resp = await api.client.post(
        "/api/v1/services/l3vpn", json=l3vpn_payload(**overrides), headers={**AUTH, "Idempotency-Key": key}
    )
    assert resp.status_code == 202, resp.text
    return resp.json()


async def set_status(api: ApiContext, service_id: str, status: str) -> None:
    """Force a lifecycle status directly in the DB (bypasses the state machine on purpose)."""
    from sqlalchemy import update

    from atlas.db.models import NetworkService

    async with api.sessionmaker() as session:
        await session.execute(
            update(NetworkService)
            .where(NetworkService.id == uuid.UUID(service_id))
            .values(lifecycle_status=status)
        )
        await session.commit()
