"""REST API tests — devices inventory, health, metrics, auth plumbing."""

from __future__ import annotations

import uuid

from atlas.api.auth import verify_api_key
from tests.unit.api_fixtures import AUTH, ApiContext

pytest_plugins = ("tests.unit.api_fixtures",)


async def test_list_devices(api: ApiContext) -> None:
    resp = await api.client.get("/api/v1/devices", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["meta"]["total"] == 4
    assert [d["hostname"] for d in body["items"]] == ["P1", "PE1", "PE2", "PE3"]

    resp = await api.client.get("/api/v1/devices", headers=AUTH, params={"role": "PE", "limit": 2})
    body = resp.json()
    assert body["meta"]["total"] == 3
    assert len(body["items"]) == 2
    assert all(d["role"] == "PE" and d["vendor"] == "mock" for d in body["items"])


async def test_get_device(api: ApiContext) -> None:
    device_id = api.devices["PE1"]
    resp = await api.client.get(f"/api/v1/devices/{device_id}", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["hostname"] == "PE1"
    assert body["loopback_ip"] == "10.0.0.1"
    assert body["bgp_asn"] == 65000


async def test_get_device_404(api: ApiContext) -> None:
    resp = await api.client.get(f"/api/v1/devices/{uuid.uuid4()}", headers=AUTH)
    assert resp.status_code == 404
    assert resp.json()["error"] == "device_not_found"


async def test_devices_require_auth(api: ApiContext) -> None:
    resp = await api.client.get("/api/v1/devices")
    assert resp.status_code == 401
    assert resp.headers["WWW-Authenticate"].startswith("ApiKey")


async def test_health_public_200(api: ApiContext) -> None:
    resp = await api.client.get("/api/v1/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["database"] == "ok"
    assert body["temporal"] == "ok"
    assert body["version"]


async def test_metrics_public(api: ApiContext) -> None:
    resp = await api.client.get("/api/v1/metrics")
    assert resp.status_code == 200
    assert "text/plain" in resp.headers["content-type"]
    assert "python_info" in resp.text or "# HELP" in resp.text


async def test_correlation_id_generated_when_absent(api: ApiContext) -> None:
    resp = await api.client.get("/api/v1/health")
    cid = resp.headers.get("X-Correlation-ID")
    assert cid
    uuid.UUID(cid)  # generated ids are UUID4


async def test_openapi_documents_api_key_scheme(api: ApiContext) -> None:
    resp = await api.client.get("/openapi.json")
    assert resp.status_code == 200
    schema = resp.json()
    assert schema["info"]["title"] == "ATLAS — Network Service Orchestration API"
    assert "APIKeyHeader" in schema["components"]["securitySchemes"]
    assert "/api/v1/metrics" in schema["paths"]
    assert "/api/v1/services/l3vpn" in schema["paths"]


def test_verify_api_key_constant_time_helper() -> None:
    assert verify_api_key("a", ["b", "a"]) is True
    assert verify_api_key("a", ["b"]) is False
    assert verify_api_key(None, ["a"]) is False
    assert verify_api_key("", ["a"]) is False
