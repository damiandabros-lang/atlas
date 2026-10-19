# ATLAS — developer Makefile
# Usage: make help

SHELL          := /bin/bash
.DEFAULT_GOAL  := help

PYTHON         ?= python3.12
VENV           ?= .venv
BIN            := $(VENV)/bin
PIP            := $(BIN)/pip
COMPOSE        ?= docker compose
HELM           ?= helm
IMAGE          ?= ghcr.io/damiandabros-lang/atlas
TAG            ?= dev
API_URL        ?= http://localhost:8000
API_KEY        ?= dev-operator-key

export ATLAS_MOCK_FAILURE_MODE ?= NONE

.PHONY: help venv install lint fmt typecheck test test-unit test-integration migrate migration seed \
        run-api run-worker up down restart logs ps demo demo-fail demo-timeout clean \
        docker-build helm-lint helm-template compose-config check

help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nATLAS targets:\n\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) }' $(MAKEFILE_LIST)
	@echo

##@ Environment
venv: ## Create the virtualenv
	test -d $(VENV) || $(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip wheel

install: venv ## Install the project with dev extras (editable)
	$(PIP) install -e ".[dev]"

##@ Quality
lint: ## Ruff lint + format check
	$(BIN)/ruff check .
	$(BIN)/ruff format --check .

fmt: ## Auto-format and fix imports
	$(BIN)/ruff format .
	$(BIN)/ruff check --fix .

typecheck: ## mypy (advisory)
	$(BIN)/mypy atlas

check: lint typecheck test ## lint + typecheck + test

##@ Tests
test: ## Full test suite with coverage
	$(BIN)/pytest -q --cov=atlas --cov-report=term-missing --cov-report=xml

test-unit: ## Unit tests only (no external services)
	$(BIN)/pytest -q tests/unit

test-integration: ## End-to-end tests (no Docker needed: Temporal test server + SQLite + mock PEs)
	$(BIN)/pytest -q tests/integration

##@ Database
migrate: ## alembic upgrade head
	$(BIN)/alembic upgrade head

migration: ## Autogenerate a migration: make migration MSG="add foo"
	@test -n "$(MSG)" || { echo "usage: make migration MSG=\"describe change\""; exit 1; }
	$(BIN)/alembic revision --autogenerate -m "$(MSG)"

seed: ## Seed demo inventory (PE1..PE3, ACME customer) — idempotent
	$(BIN)/atlas-seed

##@ Run locally (against `make up` infra or external services)
run-api: ## Start FastAPI gateway with auto-reload
	$(BIN)/uvicorn atlas.api.main:app --reload --host 0.0.0.0 --port 8000

run-worker: ## Start the Temporal worker
	$(BIN)/atlas-worker

##@ Docker Compose
up: ## Build & start the full stack (API 8000, Temporal UI 8080, Grafana 3000)
	$(COMPOSE) up -d --build
	@echo
	@echo "  API docs    http://localhost:8000/docs"
	@echo "  Temporal UI http://localhost:8080"
	@echo "  Grafana     http://localhost:3000  (admin/admin)"
	@echo "  Prometheus  http://localhost:9091"
	@echo

down: ## Stop the stack (keep volumes)
	$(COMPOSE) down --remove-orphans

down-clean: ## Stop the stack and delete volumes
	$(COMPOSE) down --remove-orphans --volumes

restart: ## Restart api + worker (e.g. after changing ATLAS_MOCK_FAILURE_MODE)
	$(COMPOSE) up -d --build --force-recreate api worker

logs: ## Tail api + worker logs
	$(COMPOSE) logs -f --tail=200 api worker

ps: ## Show stack status
	$(COMPOSE) ps

compose-config: ## Validate docker-compose.yml interpolation
	$(COMPOSE) config --quiet && echo "compose config OK"

docker-build: ## Build the image locally
	docker build -f deploy/docker/Dockerfile -t $(IMAGE):$(TAG) .

##@ Demos
demo: ## Happy path: provision ACME L3VPN across PE1..PE3 -> ACTIVE, then reconcile
	API_URL=$(API_URL) API_KEY=$(API_KEY) scripts/demo.sh

demo-fail: ## Failure injection: FAIL_ON_PE3 -> saga compensation -> ROLLED_BACK
	ATLAS_MOCK_FAILURE_MODE=FAIL_ON_PE3 $(COMPOSE) up -d --build --force-recreate worker
	API_URL=$(API_URL) API_KEY=$(API_KEY) FAILURE_MODE=FAIL_ON_PE3 EXPECT=ROLLED_BACK scripts/demo_failure.sh
	ATLAS_MOCK_FAILURE_MODE=NONE $(COMPOSE) up -d --force-recreate worker

demo-timeout: ## Failure injection: TIMEOUT_ON_PE2 -> retries with backoff -> ACTIVE
	ATLAS_MOCK_FAILURE_MODE=TIMEOUT_ON_PE2 $(COMPOSE) up -d --build --force-recreate worker
	API_URL=$(API_URL) API_KEY=$(API_KEY) FAILURE_MODE=TIMEOUT_ON_PE2 EXPECT=ACTIVE scripts/demo_failure.sh
	ATLAS_MOCK_FAILURE_MODE=NONE $(COMPOSE) up -d --force-recreate worker

##@ Helm
helm-lint: ## helm lint --strict
	$(HELM) lint deploy/helm/atlas --strict

helm-template: ## Render the chart with every optional resource enabled
	$(HELM) template atlas deploy/helm/atlas \
		--set api.hpa.enabled=true --set api.ingress.enabled=true \
		--set serviceMonitor.enabled=true --set networkPolicy.enabled=true

##@ Housekeeping
clean: ## Remove caches, build artefacts and coverage
	rm -rf .pytest_cache .ruff_cache .mypy_cache htmlcov coverage.xml .coverage dist build *.egg-info
	find . -type d -name __pycache__ -not -path "./$(VENV)/*" -exec rm -rf {} +

# ---------------------------------------------------------------------------
# Local Kubernetes (OrbStack / Docker Desktop / kind)
# ---------------------------------------------------------------------------
KUBECTL ?= kubectl
K8S_NS  ?= atlas

.PHONY: k8s-up k8s-down k8s-status k8s-forward k8s-demo

k8s-up: ## Build image, deploy in-cluster Postgres+Temporal, install the chart (namespace atlas)
	docker build -t atlas:dev -f deploy/docker/Dockerfile .
	$(KUBECTL) apply -f deploy/k8s/dev-deps.yaml
	$(KUBECTL) -n $(K8S_NS) rollout status deploy/postgres --timeout=180s
	$(KUBECTL) -n $(K8S_NS) rollout status deploy/temporal --timeout=300s
	$(HELM) upgrade --install atlas deploy/helm/atlas -n $(K8S_NS) -f deploy/helm/values-orbstack.yaml --wait --timeout 10m
	$(KUBECTL) -n $(K8S_NS) get pods

k8s-status: ## Pods, services and jobs in the atlas namespace
	$(KUBECTL) -n $(K8S_NS) get pods,svc,jobs

k8s-forward: ## Port-forward API :8000 and Temporal UI :8080 (Ctrl+C to stop)
	@echo "API http://localhost:8000/ui/topology  ·  Temporal UI http://localhost:8080"
	@$(KUBECTL) -n $(K8S_NS) port-forward svc/atlas-api 8000:80 & \
	 $(KUBECTL) -n $(K8S_NS) port-forward svc/temporal-ui 8080:8080 & wait

k8s-demo: ## Run the happy-path demo against the port-forwarded API
	./scripts/demo.sh

k8s-down: ## Uninstall the chart and the dev dependencies (keeps nothing)
	-$(HELM) uninstall atlas -n $(K8S_NS)
	-$(KUBECTL) delete -f deploy/k8s/dev-deps.yaml
