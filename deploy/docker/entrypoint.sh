#!/usr/bin/env bash
# ATLAS container entrypoint.
#
#   ATLAS_RUN_MIGRATIONS=1   run `alembic upgrade head` before starting
#   ATLAS_SEED=1             run `atlas-seed` (idempotent demo inventory) before starting
#   ATLAS_WAIT_FOR_DB=1      wait for PostgreSQL TCP port before migrating (default: 1)
#   ATLAS_WAIT_FOR_TEMPORAL=1 wait for Temporal frontend TCP port (default: 1)
#
# Any arguments are exec'd afterwards (default CMD in the Dockerfile: atlas-api).
set -euo pipefail

log() { printf '{"timestamp":"%s","level":"INFO","logger":"entrypoint","message":"%s"}\n' \
        "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"; }

# --- tiny TCP wait helper (bash /dev/tcp — no extra tooling needed) -----------
wait_for_tcp() {
  local host="$1" port="$2" name="$3" timeout="${4:-90}" i=0
  while ! (exec 3<>"/dev/tcp/${host}/${port}") 2>/dev/null; do
    i=$((i + 1))
    if [ "$i" -ge "$timeout" ]; then
      log "timed out waiting for ${name} at ${host}:${port}"
      return 1
    fi
    [ $((i % 10)) -eq 0 ] && log "waiting for ${name} at ${host}:${port} (${i}s)"
    sleep 1
  done
  log "${name} reachable at ${host}:${port}"
}

# Extract host:port from an SQLAlchemy URL such as postgresql+asyncpg://u:p@host:5432/db
db_host_port() {
  local url="${ATLAS_DATABASE_URL:-}"
  [[ "$url" == postgresql* ]] || return 1
  local rest="${url#*://}"      # u:p@host:5432/db
  rest="${rest##*@}"            # host:5432/db
  rest="${rest%%/*}"            # host:5432
  rest="${rest%%\?*}"
  local host="${rest%%:*}" port="${rest##*:}"
  [ "$host" = "$port" ] && port=5432
  printf '%s %s' "$host" "$port"
}

if [ "${ATLAS_WAIT_FOR_DB:-1}" = "1" ]; then
  if hp="$(db_host_port)"; then
    # shellcheck disable=SC2086
    wait_for_tcp $hp "postgresql"
  fi
fi

if [ "${ATLAS_WAIT_FOR_TEMPORAL:-1}" = "1" ] && [ -n "${ATLAS_TEMPORAL_HOST:-}" ]; then
  wait_for_tcp "${ATLAS_TEMPORAL_HOST%%:*}" "${ATLAS_TEMPORAL_HOST##*:}" "temporal" 120 || true
fi

if [ "${ATLAS_RUN_MIGRATIONS:-0}" = "1" ]; then
  log "running database migrations (alembic upgrade head)"
  alembic upgrade head
fi

if [ "${ATLAS_SEED:-0}" = "1" ]; then
  log "seeding demo inventory (atlas-seed)"
  atlas-seed
fi

log "starting: $*"
exec "$@"
