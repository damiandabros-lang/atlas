#!/bin/bash
# Double-click in Finder: starts the ATLAS stack with Docker Compose and runs the demo.
cd "$(dirname "$0")"
echo "== ATLAS: docker compose up -d --build =="
docker compose up -d --build || { echo "docker compose failed"; read -p "Enter to close"; exit 1; }
echo
echo "== Waiting for API health =="
for i in $(seq 1 90); do
  if curl -fsS http://localhost:8000/api/v1/health >/dev/null 2>&1; then echo "API is up"; break; fi
  sleep 2
done
echo
echo "== Demo: provision ACME L3VPN on PE1/PE2/PE3 =="
./scripts/demo.sh
echo
echo "Temporal UI: http://localhost:8080   API docs: http://localhost:8000/docs   Grafana: http://localhost:3000 (admin/admin)"
echo "Failure demo:  ATLAS_MOCK_FAILURE_MODE=FAIL_ON_PE3 make demo-fail"
read -p "Enter to close"
