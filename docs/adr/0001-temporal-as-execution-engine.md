# ADR-0001: Temporal as the authoritative execution engine

- **Status:** Accepted
- **Date:** 2026-10-18
- **Deciders:** Damian Dąbroś (owner), Claude (technical direction)

## Context

Provisioning an L3VPN touches several routers sequentially, each step can take minutes, and any
step can fail transiently (device busy, management-plane blip) or permanently (config rejected).
The platform must survive worker crashes mid-run, retry safely, support operator cancellation, and
leave a replayable record of what happened. Options considered:

1. **Celery/RQ tasks + state machine in PostgreSQL** — familiar, but retries, timers, resumption
   after crash, cancellation and "which step am I on" all have to be hand-built and kept
   consistent with the business tables. Every real-world orchestrator that went this way grew a
   bespoke, fragile workflow engine.
2. **Kubernetes Jobs / Argo Workflows** — good for batch DAGs, poor for long-lived per-service
   workflows that need signals, queries and fine-grained compensation.
3. **Temporal** — durable execution: workflow code is ordinary Python, progress is persisted
   event by event, activities get declarative retry/timeout policies, signals/queries/cancellation
   are first-class, and histories replay deterministically after a restart or a redeploy.

## Decision

Temporal is the single execution engine for all long-running orchestration
(`ProvisionL3VPNWorkflow`, `DecommissionL3VPNWorkflow`, `ReconcileServiceWorkflow`).
PostgreSQL stores business and service state; Temporal stores execution state. The two are never
conflated: no "current step" columns in SQL, no business inventory in workflow history.

Constraints we accept with this decision:

- Workflow code must be deterministic; all I/O goes through activities.
- Payloads crossing the task queue are Pydantic models (Temporal Pydantic data converter) and are
  kept small (IDs + validated request); activities load state from PostgreSQL.
- Workflow IDs are deterministic (`provision-l3vpn-<service_id>`) to make starts idempotent.
- Code changes to the activity sequence must be guarded with `workflow.patched()`.

## Consequences

- (+) Resumption after a worker crash, retries with exponential backoff, heartbeats, timeouts,
  cancellation and a full execution timeline come for free and are visible in the Temporal UI.
- (+) Horizontal scaling of workers is trivial; work is partitioned by the server.
- (+) Compensation (ADR-0002) is expressed as ordinary control flow in the workflow.
- (−) One more stateful dependency to operate (Temporal server + its own PostgreSQL schemas).
  Mitigated in the MVP by `temporalio/auto-setup`; production can use Temporal Cloud or the
  official Helm chart.
- (−) Developers must learn determinism rules and the replay model. Enforced by tests using the
  Temporal time-skipping test environment and replay tests on recorded histories.
