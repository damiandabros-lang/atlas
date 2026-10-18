# ATLAS Operations Runbook

Audience: on-call engineers operating ATLAS. Every procedure below is safe to run against the
compose stack (`make up`) as well as a Kubernetes deployment; substitute `kubectl exec` /
`port-forward` for `docker compose exec` as needed.

Conventions: `$API=http://localhost:8000/api/v1`, `$KEY` an operator API key, `$SID` a service ID.

---

## 0. Quick health check

```bash
curl -s $API/health | jq          # {"status":"ok","database":"ok","temporal":"ok",...}
curl -s $API/metrics | grep atlas_workflow_total
docker compose ps                  # all services healthy
```

Temporal worker liveness: Temporal UI → *Task Queues* → `atlas-orchestration` must list pollers
for both workflow and activity tasks. No pollers = no worker; services will sit in `REQUESTED`.

## 1. Service stuck in an in-flight state

Symptoms: `GET /services/$SID/status` shows `PROVISIONING`/`VERIFYING`/`ROLLING_BACK` for far
longer than the stage timeouts (5 min per device, 2 min verify).

1. Find the workflow: `workflow_id` from the status response (`provision-l3vpn-$SID`).
2. Open it in the Temporal UI (`http://localhost:8080/namespaces/default/workflows/<id>`).
   - **Pending activity with retries** → look at `lastFailure`. Transient device errors
     (timeouts, unreachable) are expected to retry up to 5× with backoff up to 60 s.
   - **No workers polling** → the worker is down; restart it. The workflow resumes automatically.
   - **Workflow task failure** (non-determinism / bug) → the history is preserved; fix and
     redeploy the worker, Temporal replays. Do not terminate unless instructed by engineering.
3. If the device is genuinely broken and you want ATLAS to stop trying:
   ```bash
   curl -s -X POST -H "X-API-Key: $KEY" $API/services/$SID/cancel
   ```
   This sends the `cancel` signal; the workflow finishes the current device operation, runs the
   saga compensation for completed devices and ends in `ROLLED_BACK` (or
   `MANUAL_INTERVENTION_REQUIRED` if compensation fails).
4. **Service `REQUESTED` with no `workflow_id`** — the API persisted the request but the workflow
   start failed (Temporal was down). Re-submit the same request with the same `Idempotency-Key`
   once Temporal is healthy: the controller finds the existing service and starts the workflow
   with its deterministic ID (`provision-l3vpn-$SID`), so no duplicate service is created.

Never `terminate` a workflow that is in `ROLLING_BACK`: you would leave compensations half done.
Use `cancel` or let it finish.

## 2. `MANUAL_INTERVENTION_REQUIRED`

ATLAS enters this state when it **cannot safely decide** by itself: device state is partial or
unreadable, a compensation step failed, or verification failed after configuration was accepted
(`BGP_SESSION_DOWN`, `PARTIAL_CONFIGURATION`).

1. Read the reason and the per-endpoint statuses:
   ```bash
   curl -s -H "X-API-Key: $KEY" $API/services/$SID | jq '{status_reason, endpoints: [.endpoints[] | {device_hostname, status, status_reason}]}'
   curl -s -H "X-API-Key: $KEY" $API/services/$SID/history | jq '.[] | select(.event_type|test("DEVICE_OPERATION|COMPENSATION|VALIDATION"))'
   ```
2. Inspect the device(s) named in the failing operations (or `state(hostname)` on the mock).
3. Decide:
   | Situation | Action |
   |---|---|
   | You fixed the device / the CE came up | `POST $API/services/$SID/validate` → re-runs verification; on success the service returns to `ACTIVE`. |
   | Configuration is present and correct, only ATLAS's view is stale | `POST $API/services/$SID/reconcile` → if `in_sync`, the service is marked `ACTIVE`. |
   | The service must be removed | `DELETE $API/services/$SID` (with `Idempotency-Key`) → decommission workflow with `force=false`; it removes only ATLAS-owned objects. If a device is unreachable use `?force=true` to release resources and mark `DECOMMISSIONED` while recording the orphaned objects in the audit trail. |
4. Record what you did: every operator action above (`validate`, `reconcile`, `DELETE`) writes a
   `MANUAL_INTERVENTION` / `STATUS_CHANGED` audit event with the API key label as actor. Put the
   human context (ticket number, what was fixed on the device) in your ticketing system and
   reference the `service_id`.

## 3. Reconciliation and drift

Reconciliation is `DETECT_ONLY`: ATLAS reports differences and increments
`atlas_reconciliation_drift_total`, but **never modifies devices**.

```bash
curl -s -X POST -H "X-API-Key: $KEY" $API/services/$SID/reconcile | jq
```

Result `DRIFT_DETECTED` with `differences[]` per device:

- `route_target` / `rd` drift → somebody edited the VRF on the box. Restore from the desired
  state (shown in `GET /services/$SID` → `desired_state`) and re-run reconcile.
- `interface.admin_up: desired true, observed false` → check the physical port before touching config.
- `NOT_PROVISIONED` → our VRF is missing entirely; treat as an outage. Re-provisioning is
  `DELETE` + new `POST` (the workflow is not designed to re-apply onto an `ACTIVE` service).
- `DEVICE_UNREACHABLE` → reconciliation could not read state; nothing is concluded. Fix
  management reachability and retry.

Re-running reconcile is always safe and idempotent. For a fleet-wide sweep:

```bash
for s in $(curl -s -H "X-API-Key: $KEY" "$API/services?status=ACTIVE&limit=500" | jq -r '.items[].id'); do
  curl -s -X POST -H "X-API-Key: $KEY" $API/services/$s/reconcile | jq -c '{service_id, in_sync}'
done
```

## 4. Rotating API keys

`ATLAS_API_KEYS` is a JSON list; the API accepts any key in the list, so rotation is
add-new → switch clients → remove-old with no downtime.

Compose:
```bash
# .env
ATLAS_API_KEYS=["old-key","new-key"]
docker compose up -d api            # rolling restart of the api container only
# …migrate clients…
ATLAS_API_KEYS=["new-key"]
docker compose up -d api
```

Kubernetes (chart-managed Secret):
```bash
helm upgrade atlas deploy/helm/atlas --reuse-values --set 'secrets.apiKeys=["old-key","new-key"]'
```
With `secrets.existingSecret`, patch the Secret key `ATLAS_API_KEYS` and restart the API
Deployment (`kubectl rollout restart deploy/atlas-api`); the chart's checksum annotation also
triggers a roll on `helm upgrade`.

Keys are never logged (redacted as `***`). Audit events record the actor as the key's label,
not its value.

## 5. Scaling workers

Workers are stateless; Temporal distributes workflow and activity tasks across all pollers on the
task queue.

```bash
docker compose up -d --scale worker=3 worker       # compose (remove container_name first)
kubectl scale deploy/atlas-worker --replicas=4     # kubernetes
```

Guidance:
- Device operations are sequential per service, so throughput scales with *concurrent services*,
  not with devices per service.
- Watch `atlas_workflow_duration_seconds` p95 and Temporal's *schedule-to-start* latency for
  the task queue. Rising schedule-to-start = add workers.
- Draining: `terminationGracePeriodSeconds=120` lets a worker finish or heartbeat its current
  activity; anything not finished is re-dispatched to another worker after the heartbeat timeout.
  Deploys therefore never lose work and never double-apply (device operations are idempotent).

## 6. Worker deploy with a workflow code change

1. Keep changes to the activity *sequence* behind `workflow.patched("<name>")`.
2. Deploy workers; in-flight workflows replay with the old branch, new ones take the new branch.
3. After all old histories have completed (Temporal UI filter `WorkflowType=ProvisionL3VPNWorkflow AND ExecutionStatus=Running`), remove the patch in a later release with `workflow.deprecate_patch`.
4. If the UI shows *Workflow task failed: Nondeterminism*, roll the worker image back; the
   histories are intact and will replay correctly on the previous version.

## 7. Database

Migrations run as a Helm pre-upgrade Job (or on API start with `ATLAS_RUN_MIGRATIONS=1` in
compose). Check state:

```bash
docker compose exec api alembic current
docker compose exec api alembic history --verbose | head
```

Backup (compose):
```bash
docker compose exec -T postgres pg_dump -U atlas -d atlas -Fc > atlas-$(date +%F).dump
docker compose exec -T postgres pg_dump -U atlas -d temporal -Fc > temporal-$(date +%F).dump
docker compose exec -T postgres pg_dump -U atlas -d temporal_visibility -Fc > temporal_visibility-$(date +%F).dump
```
Restore ATLAS **and** Temporal databases from the same point in time; restoring one without the
other leaves workflow executions referencing services that do not exist (or vice versa). In
production use managed PostgreSQL PITR for both.

Connection pool exhaustion (`TimeoutError: QueuePool limit`): raise `ATLAS_DB_POOL_SIZE` or
reduce worker concurrency; each activity holds a connection only for its transaction.

## 8. Temporal server issues

- `temporal` container unhealthy on first start: auto-setup is still creating schemas; wait for
  the health check (up to 5 min on slow disks). The API/worker wait for the port and retry.
- Namespace missing: `docker compose --profile tools run --rm temporal-admin-tools temporal operator namespace create default`.
- History shard errors after a Postgres restore: restart the `temporal` container.

## 9. Failure injection left on

If every new service ends in `ROLLED_BACK` or `MANUAL_INTERVENTION_REQUIRED`, check the worker's
`ATLAS_MOCK_FAILURE_MODE`:

```bash
docker compose exec worker env | grep ATLAS_MOCK
ATLAS_MOCK_FAILURE_MODE=NONE docker compose up -d --force-recreate worker
```

## 10. Escalation data to collect

- `GET /services/$SID`, `/status`, `/history` output.
- Temporal workflow history export (UI → *Download*).
- Worker logs filtered by `correlation_id` (`docker compose logs worker | grep <correlation_id>`).
- Grafana *ATLAS — Network Service Orchestration* dashboard screenshot for the time window.
