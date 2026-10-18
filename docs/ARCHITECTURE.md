# ATLAS Architecture

This document describes how ATLAS is put together and, more importantly, *why*. The
accompanying decisions are recorded as ADRs in [`docs/adr/`](adr/).

## 1. Components

| Component | Process | Responsibility |
|---|---|---|
| **API gateway** (`atlas.api`) | `atlas-api` (uvicorn) | Authentication (`X-API-Key`), request validation (Pydantic v2), idempotent POST/DELETE, service inventory reads, starting/signalling Temporal workflows. Stateless; scale horizontally. |
| **Service controller** (`atlas.services.service_controller`) | inside the API | Business logic for accepting a request: persist `NetworkService` + `ServiceEndpoint` rows in `REQUESTED`, write the first audit event, start `ProvisionL3VPNWorkflow` with a deterministic workflow ID. |
| **PostgreSQL** | external | System of record for customers, devices, services, endpoints, resource allocations, device operations, workflow execution index and audit events. |
| **Temporal server** | external | Durable execution engine: workflow histories, timers, retries, signals, queries, cancellation. |
| **Worker** (`atlas.worker`) | `atlas-worker` | Hosts the three workflows and all activities. Polls task queue `atlas-orchestration`. Exposes Prometheus metrics on `:9090`. Scale horizontally; work is partitioned by Temporal. |
| **Network adapter layer** (`atlas.adapters`) | inside activities | Vendor abstraction (`NetworkAdapter` protocol); `MockNetworkAdapter` (stateful, failure injection), `CiscoIOSXRAdapter`, `JuniperJunosAdapter`. |
| **Reconciliation engine** (`atlas.reconciliation`) | inside activities / API | Diffs vendor-neutral desired intent against observed device state; `DETECT_ONLY` in the MVP. |
| **Observability** (`atlas.observability`) | all processes | structlog JSON logs, Prometheus metrics, OpenTelemetry tracing with Temporal interceptors. |

### Request path (happy case)

```
POST /services/l3vpn ─► auth ─► Pydantic validation ─► idempotency lookup
   ─► TX { insert service(REQUESTED) + endpoints + audit(SERVICE_REQUESTED) } 
   ─► temporal.start_workflow(ProvisionL3VPNWorkflow, id="provision-l3vpn-<service_id>")
   ─► 202 {service_id, workflow_id, status: REQUESTED}
```

The database write happens **before** the workflow start. If the start fails, the API marks the
service `FAILED` with a reason; if the API crashes between the two steps, the service remains
`REQUESTED` with no `active_workflow_id` and a runbook procedure re-starts it (the workflow ID is
derived from the service ID, so re-starting is idempotent — Temporal rejects a duplicate ID with
`WorkflowAlreadyStartedError`, which the controller treats as success).

## 2. Data ownership: PostgreSQL vs Temporal

This boundary is the most important architectural rule in ATLAS.

| Concern | Owner | Rationale |
|---|---|---|
| What the service *should* look like (`desired_state`) | PostgreSQL | Business data; queried by UI/API; survives workflow retention. |
| What we last *observed* on devices (`observed_state`) | PostgreSQL | Needed for reconciliation and reporting outside any workflow. |
| Resource pools and allocations | PostgreSQL | Must be globally consistent across *all* workflows → DB constraints. |
| Lifecycle status, version | PostgreSQL | Read constantly by the API; optimistic locking. |
| Audit trail | PostgreSQL | Compliance; long retention; independent of Temporal history retention. |
| Which stage a provisioning run is in, which devices are done | Temporal workflow history | Execution state; replayable; drives retries and compensation. |
| Retry counters, backoff timers, heartbeats | Temporal | Built in; re-implementing them in SQL is the classic mistake. |
| Cancellation requests | Temporal signal → workflow → DB status | Signal is the durable intent; DB status reflects the outcome. |

Workflows therefore carry only identifiers plus the validated request in their input, and
activities load/store everything else from PostgreSQL. Workflow histories stay small, and the
business DB never contains half-finished execution bookkeeping that could diverge from Temporal.

The `workflow_executions` table is an **index**, not a copy: `(workflow_id, run_id, type,
status, started_at, closed_at)` so the API can answer "which workflow is driving this service?"
without calling Temporal, and so operators can correlate audit events with Temporal UI links.

## 3. Workflow stages

`ProvisionL3VPNWorkflow.run(ProvisionRequest) -> ProvisionResult`

| # | Stage | Activity | Timeout | Retry | Notes |
|---|---|---|---|---|---|
| 1 | VALIDATING | `validate_request` | 30 s | 3× | Devices exist, are PE role, UP; interfaces not already in use; VRF name not taken by another customer. |
| 2 | ALLOCATING | `allocate_resources` | 30 s | 3× | Single DB transaction; idempotent (re-run returns the same values). |
| 3 | — | `generate_configs` | 60 s | 3× | Pure function of allocated resources → one `DeviceConfig` per endpoint, with checksum + vendor-neutral intent. |
| 4 | PROVISIONING | `validate_device_config` per device | 60 s | 3× | Adapter-side syntax/semantic check before any change. |
| 5 | PROVISIONING | `apply_device_config` per device (sequential) | 5 min, heartbeat 30 s | policy below | Records a `device_operations` row keyed by idempotency key before and after. |
| 6 | VERIFYING | `verify_device_config` per device | 2 min | 3× | VRF present, RTs match, interface up, BGP/static as intended. |
| 7 | VERIFYING | `validate_service` | 2 min | 2× | End-to-end checks across all endpoints (route exchange, reachability in the mock). |
| 8 | ACTIVE | `commit_service_state` | 30 s | 3× | Writes `observed_state`, bumps `version`, emits `WORKFLOW_COMPLETED`. |

Every stage transition is itself an activity (`transition_status`) so that the lifecycle state
machine in `atlas.core.lifecycle` is enforced in exactly one place and every change is audited.

Retry policy for device operations: initial 2 s, backoff 2.0, max interval 60 s, max 5 attempts,
**non-retryable** error types `PermanentDeviceError`, `ConfigRejectedError`. Transient errors
(timeouts, unreachable) are retried by Temporal; permanent ones fall straight into the saga.

A `progress` query returns `WorkflowProgress{stage, completed_devices, failed_devices,
compensated_devices, cancel_requested}` and the API merges it into `GET /status`.

### Determinism rules observed

- No I/O, clock, random or environment access in workflow code — only `workflow.now()`, activities and child workflows.
- All payloads are Pydantic models through `temporalio.contrib.pydantic` data converter.
- Changes to the sequence of commands use `workflow.patched("…")` so in-flight histories replay.

## 4. Saga / compensation decision tree

Scenario: PE1 ✔, PE2 ✔, PE3 ✖.

```
apply(PE3) raised
 ├─ TRANSIENT (timeout, unreachable, 5xx)       → Temporal retries per policy
 │     └─ retries exhausted                      → treat as UNKNOWN (device may have applied)
 ├─ PERMANENT (config rejected, validation fail) → compensate
 └─ UNKNOWN                                      → inspect: get_device_state(PE3)
       ├─ our config present & verifies           → mark PE3 SUCCEEDED, continue to VERIFYING
       ├─ our config absent                       → compensate
       └─ partially present / inspection failed   → MANUAL_INTERVENTION_REQUIRED (no automatic changes)

compensate:
  status → ROLLING_BACK
  for device in reversed(completed_devices):          # PE2, then PE1
      rollback_device_config(device, our operation_id)    # idempotent; removes ONLY objects we created
      ├─ ok           → device_operations.status = COMPENSATED
      └─ failed       → status → MANUAL_INTERVENTION_REQUIRED, stop (never leave half-rolled-back silently)
  release_resources(service_id)                        # RD/RT/VLAN/subnet → RELEASED
  status → ROLLED_BACK
```

Rules that make compensation safe:

1. **Scope of ownership.** Each `DeviceOperation` records exactly which objects it created
   (VRF, sub-interface, BGP neighbour). Rollback removes those and nothing else; a VRF that
   already existed (another service, or pre-existing) is left untouched.
2. **Idempotent rollback.** Rollback of an operation that was never applied, or already rolled
   back, is a no-op that returns `already_applied=True`.
3. **Audit before and after.** `COMPENSATION_EXECUTED` events carry the operation IDs so an
   operator can reconstruct the sequence from the API alone.
4. **Cancellation reuses the saga.** A `cancel` signal flips `cancel_requested`; the workflow
   stops before the next device and runs the same compensation path.

## 5. Idempotency key scheme

| Layer | Key | Effect of replay |
|---|---|---|
| HTTP | `Idempotency-Key` header (client UUID), stored with request hash + response for `ATLAS_IDEMPOTENCY_TTL_SECONDS` | Same key + same body → original response; same key + different body → `422`. |
| Workflow | `provision-l3vpn-<service_id>`, `decommission-l3vpn-<service_id>`, `reconcile-<service_id>` | `WorkflowAlreadyStartedError` → treated as success; impossible to run two provisioners for one service. |
| Device operation | `sha256(service_id : endpoint_id : operation_type : config_checksum)` | `device_operations` has `UNIQUE(idempotency_key)`; an activity retry finds the row `SUCCEEDED` and returns without touching the device. |
| Adapter | `operation_id` returned by the device/mock | Used for `rollback_configuration(operation_id)` and for `verify`. |
| Resource allocation | `UNIQUE(resource_type, resource_value, scope)`; allocator first looks up the service's existing row for `(resource_type, scope)` | Re-running `allocate_resources` returns the existing rows. |

## 6. Resource allocation concurrency

Two provisioning workflows can run concurrently on different workers and both want the next free
RD. ATLAS does not use an in-memory lock, Redis or an advisory "pool cursor":

1. Open a transaction; if the service already holds a value for `(resource_type, scope)`, return
   it (idempotent replay of the activity).
2. Walk the configured pool range and try `INSERT` of each candidate inside a savepoint. The
   `UNIQUE (resource_type, resource_value, scope)` constraint makes exactly one transaction win;
   the loser gets `IntegrityError`, rolls back the savepoint and picks the next candidate (range
   exhausted → `ResourcePoolExhaustedError`).
3. Commit together with the `ServiceEndpoint` updates so allocation and endpoint state move
   atomically.

VLANs are scoped per device (`scope="device:PE1"`), RD/RT/VRF/subnets are global
(`scope="global"`). Release deletes the allocation rows (frees the value; history is kept in
`audit_events`). See
[ADR-0003](adr/0003-db-unique-constraint-for-allocation.md).

## 7. Adapter abstraction

```python
class NetworkAdapter(Protocol):
    async def get_device_state(self, handle: DeviceHandle) -> dict
    async def validate_configuration(self, handle, config: DeviceConfig) -> ValidationReport
    async def apply_configuration(self, handle, config: DeviceConfig, idempotency_key: str) -> AdapterResult
    async def verify_configuration(self, handle, config: DeviceConfig) -> ValidationReport
    async def rollback_configuration(self, handle, operation_id: str) -> AdapterResult
```

- The adapter is chosen per device from inventory (`vendor`), via `registry.get_adapter`.
- Configuration is rendered by `services.config_generator` from Jinja2 templates
  (`adapters/templates/{iosxr,junos,mock}_l3vpn*.j2`); the adapter receives both the vendor text
  and the vendor-neutral **intent** dict, so `verify` and reconciliation compare structured state,
  not CLI strings.
- `MockNetworkAdapter` keeps a per-process, per-hostname state store (VRFs, interfaces, BGP
  neighbours, applied operation IDs) so that verify/reconcile/rollback behave like a real device,
  and honours `ATLAS_MOCK_FAILURE_MODE`.
- Real adapters will use NETCONF (ncclient) with candidate datastore + `commit confirmed`, which
  maps naturally onto apply/verify/rollback; gNMI for `get_device_state`.
- Adapters raise typed errors: `TransientDeviceError`, `PermanentDeviceError`,
  `UnknownDeviceStateError`, mapped to `FailureKind` for the saga.

## 8. Security and secrets

- **AuthN/AuthZ (MVP):** static operator API keys from `ATLAS_API_KEYS` (JSON list); every
  mutating endpoint requires one. Roadmap: OIDC/JWT with per-customer scopes.
- **No credentials in the database.** `network_devices` stores a `credentials_ref`, never a
  password. Resolution strategy is `ATLAS_DEVICE_CREDENTIALS_REF`: `env` (dev), `k8s-secret`
  (mounted Secret), `vault` (future).
- **Containers:** non-root UID 10001, read-only root FS, all capabilities dropped,
  `seccompProfile: RuntimeDefault`, no service-account token mounted.
- **Logs/traces redact** passwords, API keys and anything matching `*password*|*secret*|*token*`;
  the OTel collector additionally deletes `device.password` and `x-api-key` attributes.
- **Idempotency store** holds a hash of the request body, not the body itself.
- **Network policy** (optional in the chart) restricts worker metrics to Prometheus.

## 9. Observability

Metrics (bounded labels only — `workflow_type`, `status`, `vendor`, `device_role`,
`operation_type`; never service or device IDs):

```
atlas_workflow_total{workflow_type,status}
atlas_workflow_failures_total{workflow_type}
atlas_workflow_duration_seconds_bucket{workflow_type}
atlas_device_operations_total{vendor,device_role,operation_type,status}
atlas_device_operation_failures_total{vendor,device_role,operation_type}
atlas_reconciliation_drift_total
```

Logs: JSON with `timestamp, level, service_id, workflow_id, activity, device_id,
correlation_id, message`. The `correlation_id` is minted at the API edge (or taken from
`X-Correlation-ID`), carried in the workflow input and bound into every activity's log context.

Tracing: FastAPI auto-instrumentation + Temporal client/worker interceptors propagate the trace
context through the task queue, so one trace spans `POST /services/l3vpn` → workflow →
`apply_device_config(PE2)`.
