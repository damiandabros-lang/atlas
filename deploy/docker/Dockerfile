# syntax=docker/dockerfile:1.7
#
# ATLAS — multi-stage image. One image serves both the API (default CMD) and the
# Temporal worker (`command: atlas-worker`).
#
#   docker build -f deploy/docker/Dockerfile -t ghcr.io/ddabros/atlas:dev .
#
# ---------------------------------------------------------------------------
# Stage 1 — builder: build a self-contained virtualenv from pyproject.toml
# ---------------------------------------------------------------------------
FROM python:3.12-slim AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential \
 && rm -rf /var/lib/apt/lists/*

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /build

# Copy metadata first so dependency resolution is cached independently of source changes.
# (alembic.ini* is globbed together with other files so the build does not fail
#  before the migration config exists in the tree.)
COPY pyproject.toml README.md alembic.ini* ./
COPY atlas ./atlas
COPY alembic ./alembic

RUN pip install --upgrade pip wheel \
 && pip install .

# ---------------------------------------------------------------------------
# Stage 2 — runtime: slim, non-root, read-only friendly
# ---------------------------------------------------------------------------
FROM python:3.12-slim AS runtime

LABEL org.opencontainers.image.title="ATLAS" \
      org.opencontainers.image.description="Enterprise Network Service Orchestration Platform (MPLS L3VPN on Temporal)" \
      org.opencontainers.image.source="https://github.com/ddabros/atlas" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.authors="Damian Dąbroś"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH" \
    ATLAS_API_HOST=0.0.0.0 \
    ATLAS_API_PORT=8000 \
    ATLAS_LOG_JSON=true

# curl is used by the HEALTHCHECK and by the entrypoint's wait loop.
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates tini \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --system --gid 10001 atlas \
 && useradd  --system --uid 10001 --gid atlas --home-dir /app --shell /usr/sbin/nologin atlas

WORKDIR /app

COPY --from=builder /opt/venv /opt/venv
# Alembic migrations + config are run from the image (entrypoint: ATLAS_RUN_MIGRATIONS=1).
COPY --chown=atlas:atlas alembic ./alembic
COPY --chown=atlas:atlas pyproject.toml alembic.ini* ./
COPY --chown=atlas:atlas deploy/docker/entrypoint.sh /usr/local/bin/atlas-entrypoint
RUN chmod 0755 /usr/local/bin/atlas-entrypoint && chown -R atlas:atlas /app

USER atlas:atlas

# 8000 — REST API (+ /api/v1/metrics)     9090 — worker Prometheus exporter
EXPOSE 8000 9090

HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=5 \
  CMD curl -fsS "http://127.0.0.1:${ATLAS_API_PORT:-8000}/api/v1/health" || exit 1

ENTRYPOINT ["tini", "--", "atlas-entrypoint"]
CMD ["atlas-api"]
