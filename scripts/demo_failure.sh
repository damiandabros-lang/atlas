#!/usr/bin/env bash
# demo_failure.sh — failure-injection walkthrough.
#
# The worker must be running with ATLAS_MOCK_FAILURE_MODE set (see `make demo-fail` /
# `make demo-timeout`). This script provisions a service and asserts the expected outcome:
#
#   FAIL_ON_PE3     PE1, PE2 succeed; PE3 fails permanently -> saga compensation -> ROLLED_BACK
#   TIMEOUT_ON_PE2  PE2 times out on first attempts -> Temporal retries w/ backoff -> ACTIVE
#
# Env: FAILURE_MODE (label only), EXPECT (ACTIVE|ROLLED_BACK|FAILED|MANUAL_INTERVENTION_REQUIRED)
set -euo pipefail
# shellcheck source=_lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_lib.sh"

FAILURE_MODE="${FAILURE_MODE:-FAIL_ON_PE3}"
case "$FAILURE_MODE" in
  FAIL_ON_PE3)            DEFAULT_EXPECT=ROLLED_BACK ;;
  TIMEOUT_ON_PE2)         DEFAULT_EXPECT=ACTIVE ;;
  DEVICE_UNREACHABLE)     DEFAULT_EXPECT=ROLLED_BACK ;;
  BGP_SESSION_DOWN)       DEFAULT_EXPECT=MANUAL_INTERVENTION_REQUIRED ;;
  PARTIAL_CONFIGURATION)  DEFAULT_EXPECT=MANUAL_INTERVENTION_REQUIRED ;;
  *)                      DEFAULT_EXPECT=ACTIVE ;;
esac
EXPECT="${EXPECT:-$DEFAULT_EXPECT}"

SERVICE_NAME="${SERVICE_NAME:-ACME-${FAILURE_MODE//_/-}-$(date +%H%M%S)}"
VRF_NAME="${VRF_NAME:-ACME-FI-$(date +%H%M%S)}"

bold "ATLAS demo — failure injection: ${FAILURE_MODE}  (expected outcome: ${EXPECT})"
wait_for_api

info "request: POST /api/v1/services/l3vpn  (${SERVICE_NAME})"
accepted=$(api POST "/api/v1/services/l3vpn" "$(l3vpn_request "$SERVICE_NAME" "$VRF_NAME")")
printf '%s\n' "$accepted" | pp
service_id=$(printf '%s' "$accepted" | jfield service_id)
workflow_id=$(printf '%s' "$accepted" | jfield workflow_id)
[ -n "$service_id" ] || { fail "no service_id in response"; exit 1; }
echo "    Temporal UI: http://localhost:8080/namespaces/default/workflows/${workflow_id}"

info "polling status (watch the per-device progression and compensation)"
final=$(poll_until_terminal "$service_id" | tee /dev/stderr | tail -n1)

info "per-endpoint state"
api GET "/api/v1/services/${service_id}" | pp

info "audit trail (look for DEVICE_OPERATION / COMPENSATION_EXECUTED events)"
api GET "/api/v1/services/${service_id}/history" | pp

if [ "$final" = "$EXPECT" ]; then
  ok "${FAILURE_MODE}: service ended in ${final} as expected"
  case "$FAILURE_MODE" in
    FAIL_ON_PE3)
      echo "    -> PE1 and PE2 configuration was compensated (removed) because PE3 failed permanently;"
      echo "       resources (RD/RT/VLAN/subnets) were released. No partial service left on the network." ;;
    TIMEOUT_ON_PE2)
      echo "    -> PE2 transient timeouts were retried by Temporal with exponential backoff;"
      echo "       the apply activity is idempotent, so retries did not duplicate configuration." ;;
  esac
else
  fail "${FAILURE_MODE}: service ended in ${final}, expected ${EXPECT}"
  exit 1
fi
