# ADR-0004: Reconciliation is DETECT_ONLY in the MVP

- **Status:** Accepted
- **Date:** 2026-10-18

## Context

ATLAS stores the desired state of every service and can read the observed state from devices.
A reconciliation engine that compares the two is essential for operating a network-as-a-service:
manual changes, failed pushes and device replacements all create drift. The obvious next step —
automatically re-applying desired state — is also the most dangerous feature in any network
orchestrator: a wrong desired state, a stale inventory record or an adapter bug would then be
*enforced* onto production routers, repeatedly, at machine speed.

## Decision

The reconciliation engine ships with `ATLAS_RECONCILIATION_MODE=DETECT_ONLY`:

- `POST /services/{id}/reconcile` and `ReconcileServiceWorkflow` read device state through the
  adapter, diff it field-by-field against the vendor-neutral intent, and return a
  `ReconciliationReport` (`IN_SYNC` / `DRIFT_DETECTED` / `NOT_PROVISIONED` / `DEVICE_UNREACHABLE`
  per device with concrete `differences[]`).
- Drift increments `atlas_reconciliation_drift_total`, writes a `DRIFT_DETECTED` audit event and
  updates `observed_state`; it **never** calls `apply_configuration`.
- `AUTO_REMEDIATE` exists in the enum and the settings so the API contract is stable, but
  selecting it is rejected at startup in this release.

## Future: AUTO_REMEDIATE

When enabled it must require, per service or per run, an explicit authorisation (operator approval
signal on the workflow, or a scoped API key with `remediate` permission), be limited to
ATLAS-owned objects (same ownership rule as ADR-0002), respect change windows, and run as the
normal apply → verify → compensate saga so a bad remediation rolls itself back.

## Consequences

- (+) Safe to run reconciliation continuously (scheduled sweeps) from day one.
- (+) Drift reports are directly usable by NOC staff and as a data source for dashboards.
- (−) Humans close the loop in the MVP; remediation is a runbook step (see `docs/RUNBOOK.md §3`).
