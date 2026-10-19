# ATLAS — Network Service Orchestration Platform

[![CI](https://github.com/damiandabros-lang/atlas/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/damiandabros-lang/atlas/actions/workflows/ci.yml)
[![Tests](https://img.shields.io/badge/tests-174_passing-success?logo=pytest&logoColor=white)](tests)
[![Python 3.12](https://img.shields.io/badge/python-3.12+-3776AB?logo=python&logoColor=white)](pyproject.toml)
[![Temporal](https://img.shields.io/badge/orchestration-Temporal-black?logo=temporal)](https://temporal.io)
[![FastAPI](https://img.shields.io/badge/API-FastAPI-009688?logo=fastapi)](https://fastapi.tiangolo.com)
[![PostgreSQL](https://img.shields.io/badge/db-PostgreSQL_16-4169E1?logo=postgresql&logoColor=white)](https://www.postgresql.org)
[![Helm](https://img.shields.io/badge/deploy-Helm_chart-0F1689?logo=helm)](deploy/helm/atlas)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

ATLAS is a production-oriented orchestration platform that provisions, verifies, reconciles and
decommissions **MPLS L3VPN** services across a service-provider core. An operator submits a
service intent through a REST API; ATLAS validates it, allocates RD/RT/VLAN/addressing from
concurrency-safe pools, renders vendor-specific configuration, pushes it to each PE router through
a pluggable adapter layer, verifies BGP/VRF/interface state, and records the result in a durable
inventory. Long-running provisioning is executed by **Temporal** workflows, so progress survives
worker crashes, failures are retried or compensated with Saga semantics, and every step is
auditable. The MVP runs against a stateful mock router adapter with configurable failure
injection; the adapter contract is designed for NETCONF/gNMI drivers for Cisco IOS XR and Junos.

> Built by **Damian Dąbroś — CCIE R&S #40482** as a reference implementation of network-as-code
> on a modern distributed-systems stack.

---


<p align="center">
  <img src="docs/img/topology-l3vpn-overlay.png" alt="Live topology with L3VPN overlay" width="900"><br>
  <em>Live topology view (<code>/ui/topology</code>): PE/P/RR inventory, core links, and the selected L3VPN's VRF attachments and virtual full-mesh.</em>
</p>
<p align="center">
  <img src="docs/img/temporal-ui-workflows.png" alt="Temporal UI" width="440">
  <img src="docs/img/api-swagger.png" alt="OpenAPI docs" width="440"><br>
  <em>Temporal UI with provisioning / reconciliation workflows · OpenAPI docs of the REST gateway.</em>
</p>

## Architecture

```
          Operator / NOC tooling
                  │  REST (JSON, X-API-Key, Idempotency-Key)
                  ▼
   ┌──────────────────────────────┐
   │  FastAPI gateway  (:8000)    │  auth · validation · idempotent POST · OpenAPI
   └──────────────┬───────────────┘
                  │ ServiceController
      ┌───────────┴────────────┐
      ▼                        ▼
┌───────────────┐      ┌─────────────────────┐
│ PostgreSQL    │      │ Temporal server     │   durable workflow state,
│ inventory,    │      │ task queue:         │   retries, timers, signals,
│ desired state,│      │ atlas-orchestration │   queries, cancellation
│ allocations,  │      └──────────┬──────────┘
│ audit         │                 │ activities
└───────────────┘                 ▼
                       ┌─────────────────────┐
                       │ Worker  (:9090)     │  ProvisionL3VPNWorkflow
                       │ validate → allocate │  DecommissionL3VPNWorkflow
                       │ → render → apply    │  ReconcileServiceWorkflow
                       │ → verify → commit   │
                       └──────────┬──────────┘
                                  │ NetworkAdapter protocol
                 ┌────────────────┼────────────────┐
                 ▼                ▼                ▼
             ┌──────┐         ┌──────┐         ┌──────┐
             │ PE1  │         │ PE2  │         │ PE3  │   mock · IOS XR · Junos
             └──┬───┘         └──┬───┘         └──┬───┘
                └────────── MPLS core ────────────┘
```

**Data ownership is deliberately split:** PostgreSQL is the system of record for business and
service state (what the service *should* be and what we last *observed*); Temporal owns execution
state (where a long-running workflow *is* and what it has already done). Neither is used for the
other's job. See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## Key engineering properties

| Property | How ATLAS achieves it |
|---|---|
| **Durable execution** | Workflows run on Temporal; a worker restart mid-provisioning resumes from the last completed activity, never from scratch. |
| **Idempotency end to end** | `Idempotency-Key` on mutating HTTP calls; deterministic workflow IDs (`provision-l3vpn-<service_id>`); per-device operation keys so an activity retry cannot double-apply configuration. |
| **Saga compensation** | PE1 ✔ PE2 ✔ PE3 ✖ → failures are classified (transient / permanent / unknown), device state is inspected, and only *our* configuration on PE1/PE2 is removed. Nothing owned by another service is ever touched. |
| **Concurrency-safe allocation** | RD, RT, VLAN (per device), /30 subnets and VRF names are claimed inside one transaction guarded by a `UNIQUE (resource_type, scope, resource_value)` constraint — two workflows can never win the same resource. |
| **Explicit lifecycle** | 12-state machine (`REQUESTED … ACTIVE … ROLLED_BACK … MANUAL_INTERVENTION_REQUIRED`) with a single source of truth for allowed transitions and optimistic locking on the service row. |
| **Reconciliation (DETECT_ONLY)** | Desired intent vs. observed device state diffed per field; drift is reported and counted, never auto-remediated without explicit authorisation. |
| **Observability** | Prometheus metrics with bounded labels, structured JSON logs with correlation IDs and credential redaction, OpenTelemetry traces across API → Temporal → adapter. |
| **Secure by default** | Non-root, read-only-rootfs containers; no credentials in the database; API keys and device secrets from Kubernetes Secrets / a secrets manager. |

## Quickstart

Prerequisites: Docker with Compose v2, `curl`, optionally `jq`.

```bash
git clone https://github.com/damiandabros-lang/atlas && cd atlas
cp .env.example .env
make up            # builds the image, starts postgres, temporal, api, worker, prometheus, grafana
make demo          # provisions ACME-L3VPN across PE1..PE3 and prints the audit trail
```

| UI | URL |
|---|---|
| API docs (OpenAPI) | http://localhost:8000/docs |
| Temporal Web UI | http://localhost:8080 |
| Grafana (admin / admin) | http://localhost:3000 |
| Prometheus | http://localhost:9091 |
| Jaeger (`docker compose --profile tracing up -d`) | http://localhost:16686 |

The API container runs `alembic upgrade head` and seeds a demo inventory (PE1–PE3 as mock
IOS XR devices, one route reflector, customer *ACME Corporation*) on first start.

## API walkthrough

All mutating calls require `X-API-Key` and accept an `Idempotency-Key` (replaying the same key
returns the original 202 instead of creating a second service).

```bash
export API=http://localhost:8000/api/v1 KEY=dev-operator-key

# 1. Inventory
curl -s -H "X-API-Key: $KEY" $API/devices | jq '.items[] | {hostname, vendor, role, operational_status}'

# 2. Request a three-site L3VPN  -> 202 Accepted
curl -s -X POST $API/services/l3vpn \
  -H "X-API-Key: $KEY" -H "Content-Type: application/json" -H "Idempotency-Key: $(uuidgen)" \
  -d '{
    "customer_id": "550e8400-e29b-41d4-a716-446655440000",
    "service_name": "ACME-L3VPN-001",
    "vrf_name": "ACME-PROD",
    "route_target": "65000:100",
    "endpoints": [
      {"device": "PE1", "interface": "GigabitEthernet0/0/0/1", "vlan": 100},
      {"device": "PE2", "interface": "GigabitEthernet0/0/0/2", "vlan": 100},
      {"device": "PE3", "interface": "GigabitEthernet0/0/0/3", "vlan": 100}
    ]}'
# {"service_id":"…","workflow_id":"provision-l3vpn-…","status":"REQUESTED"}

# 3. Live status (lifecycle + Temporal stage + per-endpoint state)
curl -s -H "X-API-Key: $KEY" $API/services/$SID/status | jq

# 4. Audit trail
curl -s -H "X-API-Key: $KEY" $API/services/$SID/history | jq '.[] | {occurred_at, event_type, message}'

# 5. Drift detection (DETECT_ONLY)
curl -s -X POST -H "X-API-Key: $KEY" $API/services/$SID/reconcile | jq

# 6. Cancel an in-flight request / decommission an active service
curl -s -X POST  -H "X-API-Key: $KEY" $API/services/$SID/cancel
curl -s -X DELETE -H "X-API-Key: $KEY" -H "Idempotency-Key: $(uuidgen)" $API/services/$SID
```

Full endpoint reference with payloads: [`docs/API.md`](docs/API.md).

## Failure-injection demo

The mock adapter keeps per-device simulated state and can inject faults. Set
`ATLAS_MOCK_FAILURE_MODE` on the worker (`make demo-fail`, `make demo-timeout`) and watch the
workflow in the Temporal UI.

| `ATLAS_MOCK_FAILURE_MODE` | What the mock does | Expected outcome |
|---|---|---|
| `NONE` | all operations succeed | `ACTIVE` |
| `FAIL_ON_PE3` | PE3 rejects the configuration (permanent) | PE1/PE2 compensated → `ROLLED_BACK`, resources released |
| `TIMEOUT_ON_PE2` | PE2 times out on the first attempts (transient) | Temporal retries with backoff → `ACTIVE` |
| `DEVICE_UNREACHABLE` | a PE never answers | retries exhausted → compensation → `ROLLED_BACK` |
| `BGP_SESSION_DOWN` | config applies, PE-CE BGP never establishes | verification fails → `MANUAL_INTERVENTION_REQUIRED` |
| `PARTIAL_CONFIGURATION` | device accepts only part of the config | verify detects mismatch → `MANUAL_INTERVENTION_REQUIRED` |

```bash
make demo-fail      # FAIL_ON_PE3    -> ROLLED_BACK
make demo-timeout   # TIMEOUT_ON_PE2 -> ACTIVE
```

## Service lifecycle

```mermaid
stateDiagram-v2
    [*] --> REQUESTED
    REQUESTED --> VALIDATING
    VALIDATING --> ALLOCATING
    ALLOCATING --> PROVISIONING
    PROVISIONING --> VERIFYING
    VERIFYING --> ACTIVE
    ACTIVE --> VERIFYING: reconcile / re-verify
    ACTIVE --> DECOMMISSIONING
    DECOMMISSIONING --> DECOMMISSIONED
    DECOMMISSIONED --> [*]

    REQUESTED --> FAILED
    VALIDATING --> FAILED
    ALLOCATING --> FAILED
    PROVISIONING --> ROLLING_BACK: permanent failure / cancel
    VERIFYING --> ROLLING_BACK
    ROLLING_BACK --> ROLLED_BACK
    ROLLED_BACK --> DECOMMISSIONED
    PROVISIONING --> MANUAL_INTERVENTION_REQUIRED: unknown device state
    VERIFYING --> MANUAL_INTERVENTION_REQUIRED
    ROLLING_BACK --> MANUAL_INTERVENTION_REQUIRED: compensation failed
    MANUAL_INTERVENTION_REQUIRED --> VERIFYING: operator fixed
    MANUAL_INTERVENTION_REQUIRED --> ROLLING_BACK: operator abort
    MANUAL_INTERVENTION_REQUIRED --> ACTIVE: operator accept
    FAILED --> DECOMMISSIONED
```

## Project layout

```
atlas/
  api/            FastAPI app, auth (X-API-Key), idempotency, routers (services, devices, health)
  core/           settings (ATLAS_* env), enums, lifecycle state machine
  db/             SQLAlchemy 2.x models, repositories, async session, seed
  schemas/        Pydantic v2 contracts shared by API, workflows, activities, adapters
  services/       service controller, resource allocator, config generator, Temporal client
  workflows/      ProvisionL3VPNWorkflow, DecommissionL3VPNWorkflow, ReconcileServiceWorkflow, saga
  activities/     validation, allocation, configuration, device_ops, verification, state, reconciliation
  adapters/       NetworkAdapter protocol, mock (stateful + failure injection), iosxr, junos, Jinja2 templates
  reconciliation/ desired-vs-observed diff engine
  observability/  structlog JSON logging, Prometheus metrics, OpenTelemetry tracing
  worker/         Temporal worker entrypoint
alembic/          migrations
deploy/           docker/ (Dockerfile, entrypoint), helm/atlas/, prometheus/, grafana/, otel/, temporal/
scripts/          demo.sh, demo_failure.sh, wait-for.sh
docs/             ARCHITECTURE, API, RUNBOOK, INTERFACES, adr/
tests/            unit/ (per-layer, no external services) and integration/ (end-to-end: API → Temporal → activities → mock PEs)
```

## Topology view

`http://localhost:8000/ui/topology` (also the API root `/`) renders the live network graph from
`GET /api/v1/topology`: PE / P / RR routers with their core links, bandwidth and IGP metrics, and
an **L3VPN overlay** per service — click a service to highlight its PEs, VRF/sub-interface badges
and the virtual full-mesh between attachment points. Devices and links come from the inventory
(`network_devices`, `network_links`), services from `network_services`/`service_endpoints`, so the
view reflects provisioning, rollback and decommission in real time (5 s refresh). The page is a
single self-contained HTML file with no external dependencies; it asks for the operator API key
once and keeps it in the browser.

## Running tests

```bash
make install            # python3.12 -m venv .venv && pip install -e ".[dev]"
make test-unit          # fast: SQLite in-memory + Temporal time-skipping test environment
make test-integration   # end-to-end, no Docker needed: real API, real workflows & activities,
                        # Temporal time-skipping test server, file-based SQLite, mock PE routers
make test               # both (≈170 tests, ~35 s)
make lint typecheck
```

The e2e suite (`tests/integration/test_e2e_provision.py`) drives the public REST API and asserts
on the simulated devices: happy path → `ACTIVE`; `FAIL_ON_PE3` → PE2/PE1 compensated,
`ROLLED_BACK`, resources released; `TIMEOUT_ON_PE2` → retried → `ACTIVE`; drift injected on PE1
→ `DRIFT_DETECTED` with the exact `route_target` diff; decommission → `DECOMMISSIONED`; cancel
mid-provisioning → device rolled back. It is what caught a real bug during development: a device
write that completed *while* the activity was being cancelled was not recorded in the ledger, so
compensation skipped it (fixed in `atlas/activities/_support.py::CancelledAfterSettled`).

CI (`.github/workflows/ci.yml`) runs ruff, mypy, the full pytest suite with coverage against
PostgreSQL 16 and `temporalio/auto-setup`, an Alembic upgrade/downgrade round-trip, a multi-arch
Docker build (pushed to GHCR on `main`), `helm lint` + `kubeconform`, and `docker compose config`.
Tags `v*` trigger [`release.yml`](.github/workflows/release.yml): image + OCI Helm chart + GitHub release.

## Kubernetes deployment (Helm)

The chart deploys the API, the worker and a pre-install/pre-upgrade migration Job. PostgreSQL and
Temporal are external dependencies (managed PostgreSQL + the official Temporal chart or Temporal Cloud).

```bash
# Secret keys expected: ATLAS_DATABASE_URL, ATLAS_API_KEYS, ATLAS_DEVICE_USERNAME, ATLAS_DEVICE_PASSWORD
helm upgrade --install atlas deploy/helm/atlas -n atlas --create-namespace \
  --set image.tag=0.1.0 \
  --set config.temporal.host=temporal-frontend.temporal.svc.cluster.local:7233 \
  --set secrets.existingSecret=atlas-secrets \
  --set serviceMonitor.enabled=true \
  --set api.ingress.enabled=true --set api.ingress.hosts[0].host=atlas.example.com
```

Pods run as UID 10001 with a read-only root filesystem and all capabilities dropped; the worker
gets a 120 s termination grace period so in-flight device activities drain cleanly. Optional:
HPA for the API, PodDisruptionBudgets, NetworkPolicy, ServiceMonitor. See `deploy/helm/atlas/values.yaml`.

## Configuration

Every setting is an `ATLAS_*` environment variable (see [`.env.example`](.env.example)). The most relevant:

| Variable | Default | Purpose |
|---|---|---|
| `ATLAS_DATABASE_URL` | `postgresql+asyncpg://atlas:atlas@localhost:5432/atlas` | business / inventory database |
| `ATLAS_TEMPORAL_HOST` | `localhost:7233` | Temporal frontend |
| `ATLAS_TEMPORAL_NAMESPACE` | `default` | Temporal namespace |
| `ATLAS_TEMPORAL_TASK_QUEUE` | `atlas-orchestration` | task queue shared by API (starter) and worker |
| `ATLAS_API_KEYS` | `["dev-operator-key"]` | JSON list of accepted `X-API-Key` values |
| `ATLAS_IDEMPOTENCY_TTL_SECONDS` | `86400` | how long `Idempotency-Key` replays are honoured |
| `ATLAS_ADAPTER_BACKEND` | `mock` | `mock` / `iosxr` / `junos` |
| `ATLAS_MOCK_FAILURE_MODE` | `NONE` | failure injection (table above) |
| `ATLAS_MOCK_LATENCY_MS` | `50` | simulated device latency |
| `ATLAS_DEVICE_CREDENTIALS_REF` | `env` | `env` / `k8s-secret` / `vault` |
| `ATLAS_BGP_ASN` | `65000` | provider ASN used for RD/RT |
| `ATLAS_RD_POOL_START/END`, `ATLAS_RT_POOL_START/END`, `ATLAS_VLAN_POOL_START/END` | `1000–1999`, `100–999`, `100–4000` | allocation pools |
| `ATLAS_PE_CE_SUPERNET` | `10.255.0.0/16` | /30 PE–CE links are carved from here |
| `ATLAS_RECONCILIATION_MODE` | `DETECT_ONLY` | `AUTO_REMEDIATE` reserved, disabled in MVP |
| `ATLAS_OTEL_ENABLED` / `ATLAS_OTEL_EXPORTER_OTLP_ENDPOINT` | `false` / `http://localhost:4317` | tracing |
| `ATLAS_LOG_JSON` / `ATLAS_LOG_LEVEL` | `true` / `INFO` | logging |

## Roadmap

- Cisco IOS XR and Junos adapters over NETCONF (ncclient) with candidate/confirmed-commit, then gNMI telemetry for verification.
- EVPN and L2VPN service models on the same workflow skeleton; BGP peering and Internet access.
- `AUTO_REMEDIATE` reconciliation behind an explicit approval signal.
- OIDC / JWT authentication with per-customer RBAC replacing static API keys.
- Scheduled reconciliation (Temporal schedules), change windows and maintenance-mode awareness.
- Multi-vendor pre-deployment validation (Batfish-style) before any device is touched.

## Licence

MIT — see [LICENSE](LICENSE). © 2026 Damian Dąbroś, CCIE R&S #40482.
