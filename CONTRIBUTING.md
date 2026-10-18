# Contributing to ATLAS

Thanks for your interest. ATLAS is a reference implementation, but it is run like a product:
small PRs, green CI, documented decisions.

## Development setup

```bash
make install          # python3.12 venv + editable install with dev extras
cp .env.example .env
make up               # PostgreSQL, Temporal, API, worker, Prometheus, Grafana
make test-unit        # fast feedback; no external services
make test-integration # against the compose stack
make lint fmt typecheck
```

## Ground rules

- **Never touch production devices.** The adapter backend in development is `mock`. Real
  adapters are exercised only against lab/virtual routers.
- **Interfaces are contracts.** Signatures in `docs/INTERFACES.md` and the Pydantic models in
  `atlas/schemas` are shared by API, workflows, activities and adapters. Changing one is a
  cross-cutting change — say so in the PR and update the doc.
- **Workflow determinism.** No I/O, clocks, randomness or env access inside `atlas/workflows`.
  Sequence changes go behind `workflow.patched("...")`. Add a replay test when you change a workflow.
- **Lifecycle transitions** go through `atlas.core.lifecycle` only.
- **Every schema change ships with an Alembic migration** (`make migration MSG="..."`) that
  upgrades *and* downgrades; CI runs the round-trip.
- **No secrets in code, config or tests.** `.env` is git-ignored; use `.env.example` for documentation.
- **Observability is part of the feature**: new activities log with the bound context and
  update the relevant counters; new failure modes get a dashboard panel or an alert.

## Pull requests

1. Branch from `main`; one logical change per PR.
2. `make check` must pass locally (ruff, mypy, pytest).
3. Describe *why*; link the ADR if the change alters an architectural decision, or add a new ADR
   in `docs/adr/` (`NNNN-short-title.md`, same template as the existing ones).
4. Commit messages: imperative mood, ≤ 72 chars subject, body explains the reasoning.

## Reporting issues

Include the `service_id`, `workflow_id`, `correlation_id`, the `GET /history` output and the
worker log excerpt. See `docs/RUNBOOK.md §10`.
