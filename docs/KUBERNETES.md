# ATLAS on Kubernetes (local: OrbStack)

The Helm chart (`deploy/helm/atlas`) deploys the API, the Temporal worker and a pre-install/
pre-upgrade migration Job. PostgreSQL and Temporal are external dependencies; for a laptop
cluster `deploy/k8s/dev-deps.yaml` provides both in-cluster.

## OrbStack

1. OrbStack → Settings → Kubernetes → **Enable**. Verify: `kubectl config current-context` → `orbstack`.
2. `make k8s-up` — builds `atlas:dev` (OrbStack's cluster pulls straight from the local Docker
   image store, no registry needed), applies Postgres + Temporal + Temporal UI, installs the chart
   with `deploy/helm/values-orbstack.yaml` (migrations + seed enabled, 1 replica each).
3. `make k8s-forward` in one terminal, then in another: `make k8s-demo`
   (`./scripts/demo.sh` against `http://localhost:8000`).
4. Topology: <http://localhost:8000/ui/topology> · Temporal UI: <http://localhost:8080>.
5. `make k8s-down` removes everything.

What the chart shows beyond compose:

| Concern | Implementation |
|---|---|
| Hook ordering | ServiceAccount, ConfigMap and Secret are `pre-install,pre-upgrade` hooks (weight −10) so the migration Job (weight 0) can mount them; Helm runs hooks before regular resources. They survive upgrades (`before-hook-creation`) and are removed by `make k8s-down` |
| Service links | `enableServiceLinks: false` on every pod: Kubernetes would otherwise inject `TEMPORAL_UI_PORT=tcp://…` from the `temporal-ui` Service, which the Temporal images parse as a port number and crash |
| Schema migrations | Helm hook Job (`alembic upgrade head`, then `atlas-seed` when `migrations.seed`) runs before every install/upgrade |
| Config vs secrets | `ConfigMap` for `ATLAS_*` settings, `Secret` (or `existingSecret` from ESO/Vault) for DB URL, API keys, device credentials |
| Health | startup/readiness/liveness probes on `/api/v1/health`; worker probed on its metrics port |
| Graceful drain | worker `terminationGracePeriodSeconds: 120` + preStop so in-flight activities finish (Temporal retries the rest) |
| Security | non-root, read-only root FS, all capabilities dropped, optional NetworkPolicy |
| Scale | HPA for the API; worker replicas scale horizontally — Temporal task queue is the load balancer |
| Observability | ServiceMonitor for Prometheus Operator; metrics on API `/api/v1/metrics` and worker `:9090` |

## Any other cluster

```bash
helm upgrade --install atlas deploy/helm/atlas -n atlas --create-namespace \
  --set image.repository=ghcr.io/<owner>/atlas --set image.tag=<version> \
  --set config.temporal.host=<temporal-frontend>:7233 \
  --set secrets.existingSecret=atlas-secrets
```
