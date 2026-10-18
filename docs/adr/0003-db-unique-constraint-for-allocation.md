# ADR-0003: Database unique constraints for concurrency-safe resource allocation

- **Status:** Accepted
- **Date:** 2026-10-18

## Context

Route distinguishers, route targets, VLAN IDs (per PE), VRF names and PE–CE /30 subnets are
finite pools. Two provisioning workflows run concurrently on different workers and may try to
allocate "the next free value" at the same time. Allocation must be atomic, concurrency-safe and
idempotent (an activity may be retried after it already committed).

Options:

1. **In-process lock** — useless across multiple workers.
2. **Redis / distributed lock** — new infrastructure, lease-expiry edge cases, and the lock
   protects the *code path*, not the *data*; a bug elsewhere can still create duplicates.
3. **`SELECT … FOR UPDATE` on a pool-cursor row** — serialises all allocations through one hot row
   and still needs a uniqueness guarantee for safety.
4. **Let PostgreSQL enforce uniqueness** — `UNIQUE (resource_type, scope, resource_value)` on
   `resource_allocations`; attempt the insert, catch `IntegrityError`, pick the next candidate.

## Decision

Option 4. The uniqueness of an allocated value is a *data invariant*, so it lives in the database:

```sql
ALTER TABLE resource_allocations
  ADD CONSTRAINT uq_allocation_type_value_scope UNIQUE (resource_type, resource_value, scope);
CREATE INDEX ix_allocations_service_type ON resource_allocations (service_id, resource_type);
```

Algorithm in `ResourceAllocator.allocate_for_service` (one transaction, savepoint per attempt):

1. If this service already holds a resource of `(resource_type, scope)` → return it (idempotency;
   `AllocationRepository.find`). Per-endpoint values already written to `service_endpoints`
   (RD, VLAN, PE/CE IPs) are reused the same way.
2. Walk the configured candidate range in order.
3. `INSERT` inside a savepoint (`AllocationRepository.try_allocate`). On `IntegrityError`
   (another transaction won the same value) roll back the savepoint and try the next candidate;
   when the range is exhausted raise `ResourcePoolExhaustedError`.
4. Commit allocations together with the `service_endpoints` updates.

Scopes: `global` for RD/RT/VRF/subnet, `device:<HOSTNAME>` for VLANs. Release
(`AllocationRepository.release`) **deletes** the rows, which frees the value immediately and
keeps the constraint a plain `UNIQUE` that behaves identically on PostgreSQL and SQLite; the
allocation history is preserved in `audit_events` (`RESOURCES_ALLOCATED` / `RESOURCES_RELEASED`
carry the values). An explicitly requested value (RT, VLAN, VRF name) that belongs to another
service raises `ResourceConflictError` — a permanent validation failure, never retried.

## Consequences

- (+) Correct under any number of workers without extra infrastructure; the invariant holds even
  if a future code path forgets the allocator.
- (+) Idempotent by construction — retried activities see their own rows.
- (+) Explicit per-service uniqueness prevents double-allocation on replay.
- (−) Under heavy contention several attempts may be needed; acceptable for provisioning rates
  (tens per minute, not thousands per second). Pool scans are indexed and cheap.
- (−) Released rows are deleted rather than retained; the audit log is the source of allocation
  history. A retained-row model (partial unique index `WHERE allocation_status <> 'RELEASED'`)
  is a straightforward PostgreSQL-only follow-up if row-level history becomes a requirement.
