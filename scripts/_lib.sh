#!/usr/bin/env bash
# Shared helpers for the demo scripts. Source, do not execute.

API_URL="${API_URL:-http://localhost:8000}"
API_KEY="${API_KEY:-dev-operator-key}"
POLL_INTERVAL="${POLL_INTERVAL:-2}"
POLL_TIMEOUT="${POLL_TIMEOUT:-600}"
CUSTOMER_ID="${CUSTOMER_ID:-550e8400-e29b-41d4-a716-446655440000}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

bold()  { printf '\033[1m%s\033[0m\n' "$*"; }
info()  { printf '\033[36m==>\033[0m %s\n' "$*"; }
ok()    { printf '\033[32m[OK]\033[0m %s\n' "$*"; }
fail()  { printf '\033[31m[FAIL]\033[0m %s\n' "$*" >&2; }

# Pretty-print JSON from stdin: jq if present, else python.
pp() {
  if command -v jq >/dev/null 2>&1; then jq .; else python3 -m json.tool; fi
}

# Extract a top-level JSON field from stdin.
jfield() {
  local field="$1"
  if command -v jq >/dev/null 2>&1; then
    jq -r --arg f "$field" '.[$f] // empty'
  else
    python3 -c 'import json,sys; d=json.load(sys.stdin); v=d.get(sys.argv[1]); print("" if v is None else v)' "$field"
  fi
}

uuidgen_portable() {
  if command -v uuidgen >/dev/null 2>&1; then uuidgen | tr 'A-Z' 'a-z'
  else python3 -c 'import uuid; print(uuid.uuid4())'; fi
}

api() {  # api <METHOD> <path> [json-body]
  local method="$1" path="$2" body="${3:-}"
  local -a args=(-sS -X "$method" "${API_URL}${path}" -H "X-API-Key: ${API_KEY}" -H "Accept: application/json")
  if [ -n "$body" ]; then
    args+=(-H "Content-Type: application/json" -H "Idempotency-Key: $(uuidgen_portable)" --data "$body")
  fi
  curl "${args[@]}"
}

wait_for_api() {
  info "waiting for API at ${API_URL}/api/v1/health"
  "${SCRIPT_DIR}/wait-for.sh" "${API_URL}/api/v1/health" "${1:-180}"
}

# Each demo run must not collide with services left over from earlier runs (the inventory
# persists in PostgreSQL): the VLAN is derived from the clock (unique per PE interface) and the
# route target is left to the allocator pool instead of being pinned.
demo_vlan() { echo $(( 100 + $(date +%s) % 3800 )); }

l3vpn_request() {  # l3vpn_request <service_name> <vrf_name> [vlan]
  local name="$1" vrf="$2" vlan="${3:-$(demo_vlan)}"
  cat <<JSON
{
  "customer_id": "${CUSTOMER_ID}",
  "service_name": "${name}",
  "vrf_name": "${vrf}",
  "description": "ACME Corporation production L3VPN (demo, vlan ${vlan})",
  "endpoints": [
    {"device": "PE1", "interface": "GigabitEthernet0/0/0/1", "vlan": ${vlan}},
    {"device": "PE2", "interface": "GigabitEthernet0/0/0/2", "vlan": ${vlan}},
    {"device": "PE3", "interface": "GigabitEthernet0/0/0/3", "vlan": ${vlan}}
  ]
}
JSON
}

# Poll GET /status until the lifecycle status is terminal for a provisioning run.
# Prints the final status; returns 1 on timeout.
poll_until_terminal() {  # poll_until_terminal <service_id>
  local sid="$1" start now status stage last=""
  start=$(date +%s)
  while :; do
    local resp
    resp=$(api GET "/api/v1/services/${sid}/status")
    status=$(printf '%s' "$resp" | jfield lifecycle_status)
    stage=$(printf '%s' "$resp" | jfield current_stage)
    if [ "${status}|${stage}" != "$last" ]; then
      printf '    %-8s status=%-30s stage=%s\n' "$(( $(date +%s) - start ))s" "$status" "${stage:-"-"}"
      last="${status}|${stage}"
    fi
    case "$status" in
      ACTIVE|FAILED|ROLLED_BACK|MANUAL_INTERVENTION_REQUIRED|DECOMMISSIONED)
        printf '%s\n' "$status"; return 0 ;;
    esac
    now=$(date +%s)
    if (( now - start >= POLL_TIMEOUT )); then
      fail "timed out after ${POLL_TIMEOUT}s (last status: $status)"; printf '%s\n' "$status"; return 1
    fi
    sleep "$POLL_INTERVAL"
  done
}
