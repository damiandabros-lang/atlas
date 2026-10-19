#!/usr/bin/env bash
# One-shot, no-questions finalisation on the developer's Mac:
#   1. Dependabot: auto-merge safe action bumps, close major version bumps.
#   2. Kubernetes (OrbStack): clean a half-installed release, reinstall the chart, collect
#      diagnostics (pods, hook job logs, temporal-ui logs) into a report, run the demo via port-forward.
#   3. Commit + push everything; print a summary.
# Every step is best-effort: a failure is recorded in the report and the script carries on.
set -uo pipefail
cd "$(dirname "$0")/.."
REPORT="${REPORT:-_finalize-report.txt}"
: > "$REPORT"
log() { printf '\n==> %s\n' "$*" | tee -a "$REPORT"; }
run() { log "\$ $*"; "$@" 2>&1 | tee -a "$REPORT"; return "${PIPESTATUS[0]}"; }

# ---------------------------------------------------------------- 1. Dependabot PRs
if command -v gh >/dev/null && gh auth status >/dev/null 2>&1; then
  log "Dependabot: closing major bumps, auto-merging safe ones"
  gh pr list --author app/dependabot --json number,title --jq '.[] | "\(.number)\t\(.title)"' | while IFS=$'\t' read -r num title; do
    case "$title" in
      *"python from 3.12"*|*"postgres from 16"*|*"prometheus from v2"*|*"grafana from 11"*)
        run gh pr close "$num" --comment "Major version bump pinned on purpose for the MVP; will be upgraded deliberately." ;;
      *)
        run gh pr merge "$num" --squash --auto || run gh pr merge "$num" --squash ;;
    esac
  done
else
  log "gh not authenticated — skipping Dependabot step"
fi

# ---------------------------------------------------------------- 2. Kubernetes
if kubectl config use-context orbstack >/dev/null 2>&1 && kubectl get nodes >/dev/null 2>&1; then
  log "Kubernetes: $(kubectl config current-context) — $(kubectl get nodes --no-headers | wc -l | tr -d ' ') node(s)"
  run docker build -q -t atlas:dev -f deploy/docker/Dockerfile .
  run kubectl apply -f deploy/k8s/dev-deps.yaml
  run kubectl -n atlas rollout status deploy/postgres --timeout=180s
  run kubectl -n atlas rollout status deploy/temporal --timeout=300s

  # temporal-ui is optional; if it crash-loops, capture why and keep going.
  if ! kubectl -n atlas rollout status deploy/temporal-ui --timeout=60s >/dev/null 2>&1; then
    log "temporal-ui not ready — logs:"
    kubectl -n atlas logs deploy/temporal-ui --tail=40 --previous 2>&1 | tee -a "$REPORT" || true
    kubectl -n atlas describe pod -l app=temporal-ui 2>&1 | grep -A12 "Last State\|Events:" | tee -a "$REPORT" || true
  fi

  # A release left in pending-install/failed by an interrupted helm --wait blocks upgrades.
  state=$(helm status atlas -n atlas -o json 2>/dev/null | python3 -c 'import sys,json;print(json.load(sys.stdin)["info"]["status"])' 2>/dev/null || echo none)
  log "helm release state: $state"
  if [ "$state" != "deployed" ] && [ "$state" != "none" ]; then
    run helm uninstall atlas -n atlas --wait --timeout 2m
  fi
  # A hook Job left from an interrupted install blocks the next one.
  kubectl -n atlas delete job -l app.kubernetes.io/instance=atlas --ignore-not-found 2>&1 | tee -a "$REPORT"

  log "helm upgrade --install (background) + diagnostics while it runs"
  helm upgrade --install atlas deploy/helm/atlas -n atlas -f deploy/helm/values-orbstack.yaml --wait --timeout 6m >>"$REPORT" 2>&1 &
  helm_pid=$!
  for i in 1 2 3 4 5 6; do
    sleep 20
    { echo "--- t+$((i*20))s"; kubectl -n atlas get pods,jobs 2>&1; } | tee -a "$REPORT"
    kubectl -n atlas get pods -o name 2>/dev/null | grep -E "migrat|atlas-" | while read -r p; do
      echo "--- $p" >>"$REPORT"; kubectl -n atlas logs "$p" --all-containers --tail=30 >>"$REPORT" 2>&1 || true
      kubectl -n atlas describe "$p" 2>/dev/null | grep -A8 "^Events:" >>"$REPORT" || true
    done
    kill -0 "$helm_pid" 2>/dev/null || break
  done
  wait "$helm_pid"; helm_rc=$?
  log "helm exit code: $helm_rc"
  run kubectl -n atlas get pods,svc,jobs

  if [ "$helm_rc" -eq 0 ]; then
    log "k8s demo via port-forward (API :18000)"
    kubectl -n atlas port-forward svc/atlas-api 18000:80 >/dev/null 2>&1 &
    pf=$!; sleep 4
    API_URL=http://localhost:18000 ./scripts/demo.sh 2>&1 | tail -25 | tee -a "$REPORT" || true
    kill "$pf" 2>/dev/null || true
  fi
else
  log "OrbStack Kubernetes not available (enable it in OrbStack → Kubernetes) — skipping k8s step"
fi

# ---------------------------------------------------------------- 3. commit + push
log "git: commit + push"
git add -A
git commit -q -m "docs: CI badge, screenshots; scripts/finalize.sh" 2>&1 | tee -a "$REPORT" || true
run git pull --rebase --autostash
run git push
run git log --oneline -3

log "DONE — full report in $REPORT"
