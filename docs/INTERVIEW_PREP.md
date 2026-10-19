# ATLAS — technical interview prep

Private notes for walking through the project. Every answer points at the code so the claim can be
shown, not just said. Files worth re-reading before the call:
`atlas/workflows/provision_l3vpn.py`, `atlas/activities/device_ops.py`, `atlas/activities/_support.py`,
`atlas/services/allocator.py`, `atlas/db/repositories.py` (AllocationRepository, ServiceRepository.transition),
`deploy/helm/atlas/templates/{migration-job,deployment-worker,deployment-api}.yaml`, `docs/adr/*`.

## 10-minute demo script

1. `make up` (or `docker compose ps` if already running) — 7 containers, API healthy.
2. `./scripts/demo.sh` — POST → 202 → poll status → ACTIVE in ~1 s → allocations, endpoints VERIFIED, 20 audit events, reconcile in sync.
3. Temporal UI `localhost:8080` → open the provision workflow → event history: activities, timeouts, the `confirm_allocations` patch marker.
4. `localhost:8000/ui/topology` → click the service → VRF badges, L3VPN mesh.
5. `make demo-fail` → FAIL_ON_PE3 → watch topology: PROVISIONING → ROLLING_BACK → service gone; history shows `COMPENSATION_EXECUTED` ×2, `RESOURCES_RELEASED`.
6. `make demo-timeout` → TIMEOUT_ON_PE2 → Temporal UI shows 3 attempts with backoff → ACTIVE.
7. Grafana `localhost:3000` → workflow totals, p95, device-op failures.
8. If Kubernetes is up: `kubectl -n atlas get pods,jobs` → migration Job Completed, api/worker Running; `make k8s-forward` + `make k8s-demo`.

## Likely questions and answers

### Architecture

**Why Temporal, not Celery/RQ + DB state machine?**
Provisioning spans minutes, touches 3+ devices and must survive a worker crash between PE2 and PE3. Temporal gives durable execution (history replay), per-activity retry/timeout policies, heartbeats, cancellation and versioning out of the box. A DB state machine + queue would mean re-implementing all of that and still losing the "where exactly was I" guarantee. ADR-0001.

**What lives in PostgreSQL vs Temporal?**
PostgreSQL: business state — services, endpoints, allocations, device-operation ledger, audit. Temporal: execution state — which activity ran, attempts, timers. Never mixed: the workflow passes only IDs and the validated request; activities load everything else from the DB. `ProvisionRequest` is deliberately small so history stays small and deterministic.

**Determinism — what can't you do in workflow code?**
No I/O, no `datetime.now()`, `random`, `uuid4()` — use `workflow.now()`, `workflow.uuid4()`. All DB/device work is in activities. Imports of non-deterministic libs go through `workflow.unsafe.imports_passed_through()`. Replay tests would catch violations.

**How do you change a running workflow safely?**
`workflow.patched("atlas-provision-v2-confirm-allocations-after-verify")` guards the branch where `confirm_allocations` moved after verification. Old histories replay the old path, new executions take the new one; once no old runs exist you `deprecate_patch`.

### Failure handling

**PE1 ok, PE2 ok, PE3 fails — walk me through it.**
`provision_device` on PE3 gets `ConfigurationRejectedError` → `FailureKind.PERMANENT` → ledger row FAILED → non-retryable `ApplicationError`. Workflow catches `ActivityError`, sees devices were touched, transitions ROLLING_BACK, `Saga.compensate()` runs `rollback_device` for PE2 then PE1 (LIFO), each idempotent and scoped to objects recorded as owned by that operation id; releases RD/RT/VLAN/subnets; ROLLED_BACK with `status_reason` carrying the cause. If any compensation fails → MANUAL_INTERVENTION_REQUIRED, nothing hidden.

**How do you tell transient from permanent?**
Adapter error classes carry `FailureKind`: `DeviceUnreachableError`/`DeviceTimeoutError` → TRANSIENT (Temporal retries, backoff 2 s ×2 up to 60 s, 5 attempts); `ConfigurationRejectedError` → PERMANENT (non-retryable, no point retrying a syntax error); `PartialApplyError`/`VerificationFailedError` → UNKNOWN → workflow runs `inspect_device_state` and decides continue vs compensate. `_support.map_adapter_error`.

**Why not just "delete everything" on failure?**
Devices may hold other services' VRFs. Rollback removes only objects the ledger says this operation created (`applied_operations[op_id].owned` in the mock; rollback by commit id on IOS XR). ADR-0002.

**Idempotency — what happens if `provision_device` runs twice?**
Idempotency key `f"{workflow_id}:{hostname}:apply:v1"`. `DeviceOperationRepository.get_or_create` on (workflow_id, device_id, op_type, key): an existing SUCCEEDED row short-circuits; the adapter also treats the same key as already applied. Double execution cannot double-configure or double-allocate.

**The bug the e2e suite found.**
Cancel arrives while PE1's write is in flight. `with_heartbeat` shields the write so the device ends in a known state — but the original code then raised `CancelledError` without recording the outcome, so the ledger had no SUCCEEDED row and compensation skipped PE1, leaving a VRF on the router. Fix: `CancelledAfterSettled` carries the write result, `provision_device` writes SUCCEEDED to the ledger, *then* propagates the cancel. Test: `test_cancel_in_flight_service`.

**Worker dies between PE2 and PE3?**
Temporal re-dispatches the workflow task to another worker; history replays to the point after PE2; PE3's activity is scheduled fresh. No DB state is lost (each activity committed its own transaction). If the worker died *during* PE3's write, the activity times out (heartbeat 30 s) and retries with the same idempotency key → ledger/adapter short-circuit.

### Data

**Concurrency-safe allocation without locks?**
`resource_allocations` has `UNIQUE(resource_type, resource_value, scope)`. The allocator walks the pool, inserts inside a SAVEPOINT; the loser of a race gets `IntegrityError`, rolls back the savepoint and tries the next value. Correct across any number of workers, no Redis, invariant held by the DB. Scope `global` for RD/RT/VRF/subnet, `device:<PE>` for VLANs. Idempotent: a re-run finds the service's existing rows. ADR-0003.

**Optimistic locking?**
`network_services.version` is SQLAlchemy's `version_id_col`; concurrent writers get `StaleDataError`. `ServiceRepository.transition` validates the state machine (`core/lifecycle.py`) and writes a STATUS_CHANGED audit row in the same transaction.

**Why delete released allocations instead of flagging?**
Keeps the constraint a plain UNIQUE that behaves identically on PostgreSQL and SQLite; history lives in `audit_events`. A partial index (`WHERE status <> 'RELEASED'`) is the PostgreSQL-only follow-up if row history becomes a requirement.

### API

**Why 202 and idempotency keys?**
Provisioning is asynchronous; the client gets `service_id` + `workflow_id` and polls `/status` (which merges DB status with a live Temporal query). `Idempotency-Key` stores fingerprint + response: same key+body → replay (header `Idempotent-Replay: true`), same key different body → 422. Workflow IDs are deterministic (`provision-l3vpn-<service_id>`) with REJECT_DUPLICATE, so even a double start is safe.

**Auth?**
API-key header, constant-time compare, principal recorded as audit actor. Deliberately simple for the MVP; OIDC/JWT is the next step and the dependency is isolated in `api/auth.py`.

### Kubernetes

**What's in the chart and why?**
API + worker Deployments; migration as a `pre-install,pre-upgrade` hook Job (`alembic upgrade head`, optional seed) so schema is ready before pods roll; ConfigMap for `ATLAS_*`, Secret or `existingSecret` (ESO/Vault) for DB URL, API keys, device credentials; probes on `/api/v1/health` (DB + Temporal) and worker metrics port; worker `terminationGracePeriodSeconds: 120` + preStop so in-flight activities finish (Temporal retries the rest); HPA for API, PDB, NetworkPolicy, ServiceMonitor; non-root, read-only root FS, all caps dropped. `values-orbstack.yaml` + `deploy/k8s/dev-deps.yaml` for a laptop cluster.

**How do you scale?**
API is stateless — HPA. Workers scale horizontally; the Temporal task queue is the load balancer and `max_concurrent_activities` bounds device fan-out per worker. PostgreSQL and Temporal are external (managed DB / Temporal Cloud) in production values.

**Secrets?**
Never in the DB; `adapters/credentials.py` resolves from env, mounted Kubernetes Secret files, or a Vault provider stub; logs redact password/token/api-key fields (`observability/logging.py`).

### Network

**Why sequential PE provisioning?**
Matches the brief and keeps compensation ordering obvious. Parallel is a small change: child workflow or `asyncio.gather` over `provision_device` with per-device saga entries; verification already runs in parallel.

**What does verification actually check?**
VRF present, RD, RT import/export sets, sub-interface present with IP/VRF, admin/oper up, static route or BGP neighbor Established; then a pairwise connectivity matrix (same VRF + RT on both ends in the mock; real impl = ping/traceroute inside VRF or BGP RIB check).

**Reconciliation?**
`reconciliation/engine.py` diffs desired intent (rebuilt from inventory) against observed (adapter verify). RT 65000:100 → 65000:200 yields `{"field":"route_target","desired":"65000:100","observed":"65000:200"}`. DETECT_ONLY by default; AUTO_REMEDIATE is refused (403 / NotAuthorized) until an approval signal exists. ADR-0004.

**Real devices?**
`CiscoIOSXRAdapter`: NETCONF candidate → validate → commit with comment = idempotency key, rollback by commit id. `JuniperJunosAdapter`: load merge, commit-confirmed, rollback by delete snippet (not `rollback N`, which could undo other services). Parsers are simplified (marked TODO); next step is containerlab/XRd in the e2e suite.

### Process

**How much of this did AI write?**
Architecture, scope, failure semantics, test scenarios and review are mine; code generation was AI-assisted and I read and own every file (which is why I can walk through any of them now). The e2e suite was my insistence — it is what caught the cancellation race. Dependabot, CI and the Helm lint are there so the repo is maintained like a product, not a demo.

**What would you do differently with more time?**
Child workflows per device with per-site failure domains; gNMI telemetry subscriptions feeding reconciliation instead of polling; EVPN as a second service type to prove the model generalises; OIDC; Temporal Cloud values; a partial-unique-index allocation model with retained history.
