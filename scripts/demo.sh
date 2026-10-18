#!/usr/bin/env bash
# demo.sh — happy-path walkthrough of the ATLAS API.
#
#   1. wait for the API
#   2. POST /api/v1/services/l3vpn  (ACME, three PE endpoints)   -> 202 Accepted
#   3. poll GET /status until terminal
#   4. GET /history (audit trail)
#   5. POST /reconcile  (DETECT_ONLY)  -> drift report
#
# Env: API_URL (default http://localhost:8000), API_KEY (default dev-operator-key)
set -euo pipefail
# shellcheck source=_lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_lib.sh"

SERVICE_NAME="${SERVICE_NAME:-ACME-L3VPN-$(date +%H%M%S)}"
VRF_NAME="${VRF_NAME:-ACME-PROD-$(date +%H%M%S)}"

bold "ATLAS demo — MPLS L3VPN provisioning (happy path)"
wait_for_api

info "inventory: GET /api/v1/devices"
api GET "/api/v1/devices" | pp

info "request: POST /api/v1/services/l3vpn  (${SERVICE_NAME}, VRF ${VRF_NAME})"
body=$(l3vpn_request "$SERVICE_NAME" "$VRF_NAME")
printf '%s\n' "$body" | pp
accepted=$(api POST "/api/v1/services/l3vpn" "$body")
printf '%s\n' "$accepted" | pp
service_id=$(printf '%s' "$accepted" | jfield service_id)
workflow_id=$(printf '%s' "$accepted" | jfield workflow_id)
[ -n "$service_id" ] || { fail "no service_id in response"; exit 1; }
ok "accepted service_id=${service_id} workflow_id=${workflow_id}"
echo "    Temporal UI: http://localhost:8080/namespaces/default/workflows/${workflow_id}"

info "polling GET /api/v1/services/${service_id}/status"
final=$(poll_until_terminal "$service_id" | tee /dev/stderr | tail -n1)

info "service: GET /api/v1/services/${service_id}"
api GET "/api/v1/services/${service_id}" | pp

info "audit trail: GET /api/v1/services/${service_id}/history"
api GET "/api/v1/services/${service_id}/history" | pp

if [ "$final" = "ACTIVE" ]; then
  info "reconciliation (DETECT_ONLY): POST /api/v1/services/${service_id}/reconcile"
  api POST "/api/v1/services/${service_id}/reconcile" '{}' | pp
  ok "service ${SERVICE_NAME} is ACTIVE and in sync"
else
  fail "service finished in ${final} (expected ACTIVE). Check ATLAS_MOCK_FAILURE_MODE on the worker."
  exit 1
fi
