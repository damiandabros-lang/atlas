"""REST API tests — service lifecycle endpoints."""

from __future__ import annotations

import uuid

from temporalio.common import WorkflowIDReusePolicy

from tests.unit.api_fixtures import (
    AUTH,
    ApiContext,
    create_service,
    l3vpn_payload,
    set_status,
)

pytest_plugins = ("tests.unit.api_fixtures",)

SERVICES = "/api/v1/services"


async def test_create_l3vpn_accepted_and_workflow_started(api: ApiContext) -> None:
    resp = await api.client.post(
        f"{SERVICES}/l3vpn",
        json=l3vpn_payload(),
        headers={**AUTH, "Idempotency-Key": "k1", "X-Correlation-ID": "c-1"},
    )
    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["status"] == "REQUESTED"
    assert body["workflow_id"] == f"provision-l3vpn-{body['service_id']}"
    assert resp.headers["X-Correlation-ID"] == "c-1"

    assert len(api.temporal.started) == 1
    started = api.temporal.started[0]
    assert started["workflow"] == "ProvisionL3VPNWorkflow"
    assert started["id"] == body["workflow_id"]
    assert started["task_queue"] == api.settings.temporal_task_queue
    assert started["id_reuse_policy"] == WorkflowIDReusePolicy.REJECT_DUPLICATE
    assert str(started["arg"].service_id) == body["service_id"]
    assert started["arg"].correlation_id == "c-1"
    assert started["arg"].request.vrf_name == "ACME-PROD"


async def test_create_requires_api_key(api: ApiContext) -> None:
    resp = await api.client.post(f"{SERVICES}/l3vpn", json=l3vpn_payload(), headers={"Idempotency-Key": "k"})
    assert resp.status_code == 401
    assert resp.json()["error"] == "unauthorized"


async def test_wrong_api_key_rejected(api: ApiContext) -> None:
    resp = await api.client.get(SERVICES, headers={"X-API-Key": "nope"})
    assert resp.status_code == 401


async def test_create_requires_idempotency_key(api: ApiContext) -> None:
    resp = await api.client.post(f"{SERVICES}/l3vpn", json=l3vpn_payload(), headers=AUTH)
    assert resp.status_code == 400
    assert resp.json()["error"] == "idempotency_key_required"
    assert api.temporal.started == []


async def test_idempotent_replay_same_key_same_body(api: ApiContext) -> None:
    first = await create_service(api, key="same")
    resp = await api.client.post(
        f"{SERVICES}/l3vpn", json=l3vpn_payload(), headers={**AUTH, "Idempotency-Key": "same"}
    )
    assert resp.status_code == 202
    assert resp.headers.get("Idempotent-Replay") == "true"
    assert resp.json() == first
    assert len(api.temporal.started) == 1  # no second workflow


async def test_same_key_different_body_rejected(api: ApiContext) -> None:
    await create_service(api, key="reuse")
    resp = await api.client.post(
        f"{SERVICES}/l3vpn",
        json=l3vpn_payload(service_name="ACME-L3VPN-002"),
        headers={**AUTH, "Idempotency-Key": "reuse"},
    )
    assert resp.status_code == 422
    assert resp.json()["error"] == "idempotency_key_reused"


async def test_workflow_already_started_is_idempotent_success(api: ApiContext) -> None:
    api.temporal.raise_already_started = True
    body = await create_service(api, key="dup")
    assert body["status"] == "REQUESTED"


async def test_unknown_device_422(api: ApiContext) -> None:
    payload = l3vpn_payload()
    payload["endpoints"][2]["device"] = "PE9"
    resp = await api.client.post(f"{SERVICES}/l3vpn", json=payload, headers={**AUTH, "Idempotency-Key": "d"})
    assert resp.status_code == 422
    assert resp.json()["error"] == "unknown_device"
    assert "PE9" in resp.json()["detail"]
    assert api.temporal.started == []


async def test_non_pe_device_422(api: ApiContext) -> None:
    payload = l3vpn_payload()
    payload["endpoints"][0]["device"] = "P1"
    resp = await api.client.post(f"{SERVICES}/l3vpn", json=payload, headers={**AUTH, "Idempotency-Key": "p"})
    assert resp.status_code == 422
    assert resp.json()["error"] == "device_not_pe"


async def test_unknown_customer_404(api: ApiContext) -> None:
    resp = await api.client.post(
        f"{SERVICES}/l3vpn",
        json=l3vpn_payload(customer_id=str(uuid.uuid4())),
        headers={**AUTH, "Idempotency-Key": "c"},
    )
    assert resp.status_code == 404
    assert resp.json()["error"] == "customer_not_found"


async def test_bad_payload_422_error_shape(api: ApiContext) -> None:
    payload = l3vpn_payload(vrf_name="9bad vrf", route_target="not-an-rt")
    resp = await api.client.post(f"{SERVICES}/l3vpn", json=payload, headers={**AUTH, "Idempotency-Key": "b"})
    assert resp.status_code == 422
    body = resp.json()
    assert body["error"] == "validation_error"
    assert "vrf_name" in body["detail"]
    assert "correlation_id" in body


async def test_bgp_endpoint_requires_ce_asn(api: ApiContext) -> None:
    payload = l3vpn_payload()
    payload["endpoints"][0]["routing_protocol"] = "BGP"
    resp = await api.client.post(
        f"{SERVICES}/l3vpn", json=payload, headers={**AUTH, "Idempotency-Key": "bgp"}
    )
    assert resp.status_code == 422
    assert "ce_asn" in resp.json()["detail"]


async def test_list_and_get_service(api: ApiContext) -> None:
    created = await create_service(api, key="l1")
    second = l3vpn_payload(service_name="ACME-L3VPN-002")
    for ep in second["endpoints"]:
        ep["vlan"] = 200
    await create_service(api, key="l2", **second)

    resp = await api.client.get(SERVICES, headers=AUTH)
    assert resp.status_code == 200
    data = resp.json()
    assert data["meta"]["total"] == 2
    assert {s["service_name"] for s in data["items"]} == {"ACME-L3VPN-001", "ACME-L3VPN-002"}

    resp = await api.client.get(SERVICES, headers=AUTH, params={"status": "REQUESTED", "limit": 1})
    assert resp.json()["meta"] == {"total": 2, "limit": 1, "offset": 0}
    assert len(resp.json()["items"]) == 1

    resp = await api.client.get(SERVICES, headers=AUTH, params={"customer_id": str(uuid.uuid4())})
    assert resp.json()["meta"]["total"] == 0

    resp = await api.client.get(f"{SERVICES}/{created['service_id']}", headers=AUTH)
    assert resp.status_code == 200
    svc = resp.json()
    assert svc["lifecycle_status"] == "REQUESTED"
    assert svc["active_workflow_id"] == created["workflow_id"]
    assert svc["desired_state"]["vrf_name"] == "ACME-PROD"
    assert [ep["device_hostname"] for ep in svc["endpoints"]] == ["PE1", "PE2", "PE3"]
    assert all(ep["status"] == "PENDING" and ep["vrf_name"] == "ACME-PROD" for ep in svc["endpoints"])
    assert svc["allocations"] == []


async def test_endpoint_already_attached_409(api: ApiContext) -> None:
    await create_service(api, key="e1")
    resp = await api.client.post(
        f"{SERVICES}/l3vpn",
        json=l3vpn_payload(service_name="ACME-L3VPN-002"),
        headers={**AUTH, "Idempotency-Key": "e2"},
    )
    assert resp.status_code == 409
    assert resp.json()["error"] == "endpoint_in_use"
    assert len(api.temporal.started) == 1


async def test_list_limit_bounded(api: ApiContext) -> None:
    resp = await api.client.get(SERVICES, headers=AUTH, params={"limit": 500})
    assert resp.status_code == 422


async def test_get_unknown_service_404(api: ApiContext) -> None:
    resp = await api.client.get(f"{SERVICES}/{uuid.uuid4()}", headers=AUTH)
    assert resp.status_code == 404
    assert resp.json()["error"] == "service_not_found"


async def test_status_merges_live_workflow_state(api: ApiContext) -> None:
    created = await create_service(api, key="s")
    resp = await api.client.get(f"{SERVICES}/{created['service_id']}/status", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["lifecycle_status"] == "REQUESTED"
    assert body["workflow_id"] == created["workflow_id"]
    assert body["workflow_status"] == "RUNNING"
    assert body["current_stage"] == "PROVISIONING"  # from the fake "progress" query
    assert len(body["endpoints"]) == 3


async def test_history_contains_service_requested(api: ApiContext) -> None:
    created = await create_service(api, key="h")
    resp = await api.client.get(f"{SERVICES}/{created['service_id']}/history", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["meta"]["total"] >= 1
    assert body["items"][0]["event_type"] == "SERVICE_REQUESTED"
    assert body["items"][0]["actor"].startswith("operator:")
    assert body["items"][0]["workflow_id"] == created["workflow_id"]


async def test_cancel_requested_service_202(api: ApiContext) -> None:
    created = await create_service(api, key="cx")
    resp = await api.client.post(f"{SERVICES}/{created['service_id']}/cancel", headers=AUTH)
    assert resp.status_code == 202, resp.text
    assert resp.json()["status"] == "CANCEL_REQUESTED"
    handle = api.temporal.handles[created["workflow_id"]]
    assert ("cancel", ()) in handle.signals
    assert handle.cancelled is True

    hist = (await api.client.get(f"{SERVICES}/{created['service_id']}/history", headers=AUTH)).json()
    assert "WORKFLOW_CANCELLED" in {e["event_type"] for e in hist["items"]}


async def test_cancel_active_service_409(api: ApiContext) -> None:
    created = await create_service(api, key="ca")
    await set_status(api, created["service_id"], "ACTIVE")
    resp = await api.client.post(f"{SERVICES}/{created['service_id']}/cancel", headers=AUTH)
    assert resp.status_code == 409
    assert resp.json()["error"] == "not_cancellable"


async def test_delete_active_starts_decommission(api: ApiContext) -> None:
    created = await create_service(api, key="del")
    await set_status(api, created["service_id"], "ACTIVE")
    resp = await api.client.delete(
        f"{SERVICES}/{created['service_id']}", headers=AUTH, params={"force": "true"}
    )
    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["workflow_id"] == f"decommission-l3vpn-{created['service_id']}"
    started = api.temporal.started[-1]
    assert started["workflow"] == "DecommissionL3VPNWorkflow"
    assert started["arg"].force is True

    svc = (await api.client.get(f"{SERVICES}/{created['service_id']}", headers=AUTH)).json()
    assert svc["active_workflow_id"] == body["workflow_id"]


async def test_delete_provisioning_409(api: ApiContext) -> None:
    created = await create_service(api, key="dp")
    await set_status(api, created["service_id"], "PROVISIONING")
    resp = await api.client.delete(f"{SERVICES}/{created['service_id']}", headers=AUTH)
    assert resp.status_code == 409
    assert resp.json()["error"] == "not_decommissionable"
    assert len(api.temporal.started) == 1


async def test_validate_reports_checks(api: ApiContext) -> None:
    created = await create_service(api, key="v")
    resp = await api.client.post(f"{SERVICES}/{created['service_id']}/validate", headers=AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["passed"] is False  # REQUESTED, endpoints PENDING
    names = [c["name"] for c in body["checks"]]
    assert names[:2] == ["service_exists", "lifecycle_active"]
    assert names.count("endpoint_verified") == 3


async def test_reconcile_auto_remediate_403(api: ApiContext) -> None:
    created = await create_service(api, key="r1")
    await set_status(api, created["service_id"], "ACTIVE")
    resp = await api.client.post(
        f"{SERVICES}/{created['service_id']}/reconcile", headers=AUTH, json={"mode": "AUTO_REMEDIATE"}
    )
    assert resp.status_code == 403
    assert resp.json()["error"] == "auto_remediate_forbidden"


async def test_reconcile_detect_only_starts_workflow(api: ApiContext) -> None:
    created = await create_service(api, key="r2")
    await set_status(api, created["service_id"], "ACTIVE")
    resp = await api.client.post(f"{SERVICES}/{created['service_id']}/reconcile", headers=AUTH, json={})
    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["status"] == "STARTED"
    assert body["mode"] == "DETECT_ONLY"
    started = api.temporal.started[-1]
    assert started["workflow"] == "ReconcileServiceWorkflow"
    assert started["id_reuse_policy"] == WorkflowIDReusePolicy.ALLOW_DUPLICATE


async def test_reconcile_requested_service_409(api: ApiContext) -> None:
    created = await create_service(api, key="r3")
    resp = await api.client.post(f"{SERVICES}/{created['service_id']}/reconcile", headers=AUTH)
    assert resp.status_code == 409
