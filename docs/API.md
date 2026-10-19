# ATLAS REST API

Base path: `/api/v1`. Interactive docs: `/docs` (Swagger UI), `/redoc`, schema at `/openapi.json`.

## Conventions

| Concern | Rule |
|---|---|
| Authentication | `X-API-Key: <key>` on every endpoint except `/health` and `/metrics`. Keys come from `ATLAS_API_KEYS`. Missing/invalid → `401`. |
| Idempotency | `POST` and `DELETE` accept `Idempotency-Key: <client uuid>`. Same key + same body within `ATLAS_IDEMPOTENCY_TTL_SECONDS` → the stored response is replayed (same status code). Same key + different body → `422 Unprocessable Entity` (`idempotency_key_reused`). Recommended for all mutating calls; required by clients that retry. |
| Correlation | Optional `X-Correlation-ID` request header; echoed in the response and bound to every log line, audit event and trace of the resulting workflow. Generated when absent. |
| Async operations | Provisioning, decommissioning and reconciliation are long-running → `202 Accepted` with a `workflow_id`; poll `/status`. |
| Errors | JSON `{"error": "<code>", "detail": "...", "correlation_id": "..."}`. Validation errors are `422` with Pydantic details. |
| Pagination | `?limit=50&offset=0` on list endpoints; response carries `meta: {total, limit, offset}`. |

## Endpoints

| Method | Path | Purpose | Success |
|---|---|---|---|
| `POST` | `/services/l3vpn` | Request a new MPLS L3VPN | `202` `ServiceAccepted` |
| `GET` | `/services` | List services (`?customer_id=&status=&limit=&offset=`) | `200` `ServiceList` |
| `GET` | `/services/{service_id}` | Full service view: desired/observed state, endpoints, allocations | `200` `ServiceOut` |
| `GET` | `/services/{service_id}/status` | Lifecycle status + live Temporal stage + per-endpoint status | `200` `ServiceStatusOut` |
| `GET` | `/services/{service_id}/history` | Audit trail, oldest first | `200` `list[AuditEventOut]` |
| `POST` | `/services/{service_id}/validate` | Re-run device/service verification (allowed from `ACTIVE`, `MANUAL_INTERVENTION_REQUIRED`) | `202` |
| `POST` | `/services/{service_id}/reconcile` | Desired vs observed drift detection (`DETECT_ONLY`) | `200` `ReconciliationReport` (sync) or `202` when run as a workflow |
| `POST` | `/services/{service_id}/cancel` | Signal cancellation of the in-flight provisioning workflow → saga compensation | `202` |
| `DELETE` | `/services/{service_id}` | Decommission (`?force=true` releases resources even if a device is unreachable) | `202` `ServiceAccepted` |
| `GET` | `/devices` | Inventory (`?role=PE&vendor=mock`) | `200` `DeviceList` |
| `GET` | `/devices/{device_id}` | One device | `200` `DeviceOut` |
| `GET` | `/health` | Liveness/readiness: DB + Temporal connectivity | `200` `HealthOut` / `503` |
| `GET` | `/metrics` | Prometheus exposition | `200` text |

Lifecycle guards: `cancel` is accepted only while the service is in an in-flight state
(`REQUESTED…VERIFYING`, `ROLLING_BACK`); `DELETE` from `ACTIVE`, `FAILED`,
`MANUAL_INTERVENTION_REQUIRED` or `ROLLED_BACK`; anything else → `409` with the current status.

## Payloads

### `POST /services/l3vpn`

```json
{
  "customer_id": "550e8400-e29b-41d4-a716-446655440000",
  "service_name": "ACME-L3VPN-001",
  "vrf_name": "ACME-PROD",
  "route_target": "65000:100",
  "description": "ACME Corporation production VPN",
  "endpoints": [
    {"device": "PE1", "interface": "GigabitEthernet0/0/0/1", "vlan": 100},
    {"device": "PE2", "interface": "GigabitEthernet0/0/0/2", "vlan": 100},
    {"device": "PE3", "interface": "GigabitEthernet0/0/0/3", "vlan": 100,
     "routing_protocol": "BGP", "ce_asn": 65001}
  ]
}
```

Field rules (Pydantic): `service_name` `^[A-Za-z0-9][A-Za-z0-9\-_]*$` 3–64 chars; `vrf_name`
letter-initial, `[A-Za-z0-9_-]`, ≤ 32; `route_target` `ASN:NN` (omit to allocate from the RT
pool); `vlan` 1–4094 (omit to allocate per device); `routing_protocol` `STATIC` (default) |
`BGP` (requires `ce_asn`) | `OSPF`; duplicate `(device, interface, vlan)` tuples are rejected;
1–64 endpoints.

Response `202`:

```json
{"service_id": "0d1f3b6e-…", "workflow_id": "provision-l3vpn-0d1f3b6e-…", "status": "REQUESTED"}
```

### `GET /services/{id}/status`

```json
{
  "service_id": "0d1f3b6e-…",
  "lifecycle_status": "PROVISIONING",
  "status_reason": null,
  "workflow_id": "provision-l3vpn-0d1f3b6e-…",
  "workflow_status": "RUNNING",
  "current_stage": "apply_device_config:PE2",
  "endpoints": [
    {"device_hostname": "PE1", "interface": "GigabitEthernet0/0/0/1", "vlan_id": 100, "vrf_name": "ACME-PROD",
     "route_distinguisher": "65000:1001", "pe_ip_address": "10.255.0.1", "ce_ip_address": "10.255.0.2",
     "routing_protocol": "STATIC", "status": "VERIFIED", "status_reason": null, "...": "..."},
    {"device_hostname": "PE2", "status": "PROVISIONING", "...": "..."},
    {"device_hostname": "PE3", "status": "PENDING", "...": "..."}
  ]
}
```

### `GET /services/{id}` (excerpt)

```json
{
  "id": "0d1f3b6e-…", "customer_id": "550e8400-…", "service_type": "MPLS_L3VPN",
  "service_name": "ACME-L3VPN-001", "lifecycle_status": "ACTIVE", "version": 7,
  "active_workflow_id": null,
  "desired_state": {
    "vrf_name": "ACME-PROD", "route_target": "65000:100",
    "endpoints": [{"device": "PE1", "rd": "65000:1001", "rt_import": ["65000:100"], "rt_export": ["65000:100"], "...": "..."}]
  },
  "observed_state": {"PE1": {"vrf": {"name": "ACME-PROD", "rd": "65000:1001", "rt_import": ["65000:100"], "rt_export": ["65000:100"]}, "...": "..."}},
  "allocations": [
    {"resource_type": "ROUTE_TARGET", "resource_value": "65000:100", "scope": "global", "allocation_status": "ALLOCATED"},
    {"resource_type": "ROUTE_DISTINGUISHER", "resource_value": "65000:1001", "scope": "global", "allocation_status": "ALLOCATED"},
    {"resource_type": "VLAN_ID", "resource_value": "100", "scope": "device:PE1", "allocation_status": "ALLOCATED"},
    {"resource_type": "IP_SUBNET", "resource_value": "10.255.0.0/30", "scope": "global", "allocation_status": "ALLOCATED"}
  ]
}
```

### `GET /services/{id}/history` (excerpt)

```json
[
  {"event_type": "SERVICE_REQUESTED", "actor": "api-key:operator", "from_status": null, "to_status": "REQUESTED", "message": "L3VPN requested", "occurred_at": "2026-10-18T10:00:00Z", "...": "..."},
  {"event_type": "RESOURCES_ALLOCATED", "message": "RT 65000:100, RD x3, VLAN x3, /30 x3", "...": "..."},
  {"event_type": "DEVICE_OPERATION", "device_id": "…PE1", "message": "APPLY SUCCEEDED (op 7f1…)", "...": "..."},
  {"event_type": "DEVICE_OPERATION", "device_id": "…PE3", "message": "APPLY FAILED: config rejected (PERMANENT)", "...": "..."},
  {"event_type": "STATUS_CHANGED", "from_status": "PROVISIONING", "to_status": "ROLLING_BACK", "...": "..."},
  {"event_type": "COMPENSATION_EXECUTED", "device_id": "…PE2", "message": "ROLLBACK COMPENSATED (op 9ac…)", "...": "..."},
  {"event_type": "COMPENSATION_EXECUTED", "device_id": "…PE1", "...": "..."},
  {"event_type": "RESOURCES_RELEASED", "...": "..."},
  {"event_type": "STATUS_CHANGED", "from_status": "ROLLING_BACK", "to_status": "ROLLED_BACK", "...": "..."}
]
```

### `POST /services/{id}/reconcile`

```json
{
  "service_id": "0d1f3b6e-…",
  "mode": "DETECT_ONLY",
  "in_sync": false,
  "checked_at": "2026-10-18T10:05:00Z",
  "devices": [
    {"device": "PE1", "status": "DRIFT_DETECTED",
     "differences": [{"field": "vrf.rt_import", "desired": ["65000:100"], "observed": ["65000:200"]}]},
    {"device": "PE2", "status": "IN_SYNC", "differences": []},
    {"device": "PE3", "status": "IN_SYNC", "differences": []}
  ]
}
```

### `GET /devices`

```json
{
  "items": [
    {"id": "…", "hostname": "PE1", "management_ip": "192.0.2.11", "vendor": "mock", "platform": "mock-iosxr",
     "software_version": "7.9.2", "role": "PE", "operational_status": "UP", "loopback_ip": "10.0.0.1", "bgp_asn": 65000, "...": "..."}
  ],
  "meta": {"total": 4, "limit": 50, "offset": 0}
}
```

### Errors

| Status | `error` | When |
|---|---|---|
| `401` | `unauthorized` | missing / unknown API key |
| `404` | `service_not_found` / `device_not_found` | unknown ID |
| `422` | `idempotency_key_reused` | same `Idempotency-Key`, different body |
| `409` | `invalid_lifecycle_transition` | e.g. `cancel` on an `ACTIVE` service |
| `409` | `resource_conflict` | VRF name / interface already in use by another service |
| `422` | `validation_error` | schema violation (details in `detail`) |
| `503` | `dependency_unavailable` | Temporal or database unreachable |


## Topology

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/v1/topology` | Nodes (devices with role/status/site/services), links (`network_links` with bandwidth, IGP metric, status) and service overlays (attachments per PE). `?include_all=true` also returns terminal (decommissioned/rolled-back) services. |
| `GET` | `/ui/topology` | Bundled topology viewer (no auth; it calls the API with the key you enter). `/` redirects here. |
