#!/usr/bin/env bash
# wait-for.sh — block until an HTTP endpoint returns 2xx or a TCP port accepts connections.
#
#   scripts/wait-for.sh http://localhost:8000/api/v1/health [timeout_seconds]
#   scripts/wait-for.sh localhost:7233 [timeout_seconds]
set -euo pipefail

target="${1:?usage: wait-for.sh <url|host:port> [timeout]}"
timeout="${2:-120}"
start=$(date +%s)

while :; do
  if [[ "$target" == http* ]]; then
    if curl -fsS -o /dev/null --max-time 3 "$target"; then
      echo "ready: $target"; exit 0
    fi
  else
    host="${target%%:*}"; port="${target##*:}"
    if (exec 3<>"/dev/tcp/${host}/${port}") 2>/dev/null; then
      echo "ready: $target"; exit 0
    fi
  fi
  if (( $(date +%s) - start >= timeout )); then
    echo "timeout after ${timeout}s waiting for $target" >&2; exit 1
  fi
  sleep 2
done
