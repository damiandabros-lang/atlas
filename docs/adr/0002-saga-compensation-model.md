# ADR-0002: Saga-style compensation with failure classification

- **Status:** Accepted
- **Date:** 2026-10-18

## Context

Configuring three PE routers is not one atomic transaction. If PE3 fails after PE1 and PE2 were
configured, the network is in a partially provisioned state that is invisible to the customer but
consumes resources and may leak routes. Blindly "deleting everything" is unsafe: the failure may
be a timeout after the device actually applied the config, or the VRF may be shared with another
service, or the rollback itself may fail.

## Decision

Use a **Saga**: every forward device operation has a compensating operation, and the workflow
runs compensations in reverse order for operations that *completed*. Before compensating, the
failure is **classified**:

| Classification | Source | Action |
|---|---|---|
| `TRANSIENT` | timeouts, unreachable, 5xx-like | Temporal retry policy (backoff 2 s → 60 s, max 5). |
| `PERMANENT` | config rejected, validation failed, non-retryable error type | Compensate completed devices, release resources → `ROLLED_BACK`. |
| `UNKNOWN` | retries exhausted, ambiguous response | Inspect device state through the adapter: if our config is present and verifies, continue; if absent, compensate; if partial/unreadable → `MANUAL_INTERVENTION_REQUIRED`. |

Invariants:

1. **Ownership.** A device operation records the objects it created; its compensation removes
   only those. Pre-existing or foreign configuration is never removed.
2. **Idempotent compensation.** Rolling back a never-applied or already-rolled-back operation is
   a no-op. Compensation can therefore be retried by Temporal like any activity.
3. **No silent half-rollback.** If any compensation fails after its own retries, the saga stops
   and the service goes to `MANUAL_INTERVENTION_REQUIRED` with the exact device and operation in
   `status_reason` and the audit trail.
4. **Resources are released last**, only after all device compensations succeeded, so a
   half-rolled-back service can never have its RD/VLAN reused by another service.
5. **Cancellation is a saga trigger**, not a termination: the `cancel` signal stops forward
   progress at the next device boundary and runs the same compensation path.

## Alternatives rejected

- *Two-phase commit across devices* (NETCONF confirmed-commit on all PEs, then confirm): attractive
  for NETCONF-capable devices and may be layered in later, but it does not exist for all vendors,
  does not cover verification failures discovered after commit, and still needs a saga for the
  "confirm" step failing on one device.
- *Always roll back everything on any error*: violates ownership and can delete customer-affecting
  configuration on a device whose only problem was a slow management plane.

## Consequences

- (+) Partial failures converge to one of two auditable end states: fully `ROLLED_BACK` or
  explicitly `MANUAL_INTERVENTION_REQUIRED`. Never "silently half-provisioned".
- (+) The mock adapter's failure modes (`FAIL_ON_PE3`, `TIMEOUT_ON_PE2`, `PARTIAL_CONFIGURATION`…)
  give deterministic tests for every branch of the decision tree.
- (−) Adapters must implement a reliable `rollback_configuration(operation_id)` and
  `get_device_state`, which is the hardest part of real-vendor drivers.
