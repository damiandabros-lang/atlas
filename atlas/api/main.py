"""FastAPI application factory for the ATLAS REST API.

``create_app()`` builds a fully wired app; the module-level ``app`` is what uvicorn / the
``atlas-api`` console script serve. Observability hooks (structured logging, tracing) are
imported defensively so the API runs even when ``atlas.observability`` is not installed.
"""

from __future__ import annotations

import logging
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from importlib import resources as _resources

from fastapi import FastAPI, Request, Response
from fastapi.openapi.utils import get_openapi
from fastapi.responses import HTMLResponse, RedirectResponse

import atlas
from atlas.api.deps import CORRELATION_HEADER, get_settings_dep
from atlas.api.errors import install_exception_handlers
from atlas.api.routers import devices, health, services, topology
from atlas.api.routers.health import metrics_app
from atlas.core.config import Settings, get_settings
from atlas.services.temporal_client import reset_temporal_client

API_PREFIX = "/api/v1"
TITLE = "ATLAS — Network Service Orchestration API"

log = logging.getLogger("atlas.api.access")


def _configure_observability(app: FastAPI, settings: Settings) -> None:
    try:
        from atlas.observability.logging import configure_logging

        configure_logging(settings)
    except Exception:  # module missing or misconfigured — fall back to stdlib logging
        logging.basicConfig(level=getattr(logging, settings.log_level.upper(), logging.INFO))
    if settings.otel_enabled:
        try:
            from atlas.observability.tracing import configure_tracing

            configure_tracing(settings, app)
        except Exception:
            log.warning("tracing requested but atlas.observability.tracing is unavailable")


def _make_lifespan(settings: Settings) -> Callable[[FastAPI], AsyncIterator[None]]:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        _configure_observability(app, settings)
        log.info(
            "atlas api starting", extra={"version": atlas.__version__, "environment": settings.environment}
        )
        try:
            yield
        finally:
            reset_temporal_client()
            log.info("atlas api stopped")

    return lifespan


async def _correlation_and_access_log(
    request: Request, call_next: Callable[[Request], Awaitable[Response]]
) -> Response:
    """Assign/propagate ``X-Correlation-ID`` and emit one structured access-log line per request."""
    correlation_id = request.headers.get(CORRELATION_HEADER) or str(uuid.uuid4())
    request.state.correlation_id = correlation_id
    started = time.perf_counter()
    status_code = 500
    try:
        response = await call_next(request)
        status_code = response.status_code
    finally:
        duration_ms = round((time.perf_counter() - started) * 1000, 2)
        log.info(
            "http request",
            extra={
                "correlation_id": correlation_id,
                "method": request.method,
                "path": request.url.path,
                "status_code": status_code,
                "duration_ms": duration_ms,
                "client": request.client.host if request.client else None,
            },
        )
    response.headers.setdefault(CORRELATION_HEADER, correlation_id)
    return response


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    app = FastAPI(
        title=TITLE,
        version=atlas.__version__,
        description="Provisioning, lifecycle management and reconciliation of MPLS L3VPN services.",
        lifespan=_make_lifespan(settings),
        docs_url="/docs",
        openapi_url="/openapi.json",
    )
    app.state.settings = settings
    app.dependency_overrides[get_settings_dep] = lambda: settings

    app.middleware("http")(_correlation_and_access_log)
    install_exception_handlers(app)

    app.include_router(health.router, prefix=API_PREFIX)
    app.include_router(services.router, prefix=API_PREFIX)
    app.include_router(devices.router, prefix=API_PREFIX)
    app.include_router(topology.router, prefix=API_PREFIX)

    _topology_html = _resources.files("atlas.api").joinpath("ui/topology.html").read_text(encoding="utf-8")

    @app.get("/ui/topology", include_in_schema=False, response_class=HTMLResponse)
    async def topology_ui() -> HTMLResponse:
        """Bundled single-file topology viewer (reads /api/v1/topology with the operator's API key)."""
        return HTMLResponse(_topology_html)

    @app.get("/", include_in_schema=False)
    async def root() -> RedirectResponse:
        return RedirectResponse("/ui/topology")

    if settings.metrics_enabled:
        app.add_route(f"{API_PREFIX}/metrics", metrics_app(), methods=["GET"], include_in_schema=False)

    def _openapi() -> dict:
        if app.openapi_schema:
            return app.openapi_schema
        schema = get_openapi(
            title=app.title, version=app.version, description=app.description, routes=app.routes
        )
        schema.setdefault("paths", {})[f"{API_PREFIX}/metrics"] = {
            "get": {
                "tags": ["ops"],
                "summary": "Prometheus metrics",
                "responses": {"200": {"description": "Prometheus text exposition format"}},
            }
        }
        app.openapi_schema = schema
        return schema

    app.openapi = _openapi  # type: ignore[method-assign]
    return app


app = create_app()


def run() -> None:
    """Console-script entry point (``atlas-api``)."""
    import uvicorn

    settings = get_settings()
    uvicorn.run(
        "atlas.api.main:app",
        host=settings.api_host,
        port=settings.api_port,
        log_level=settings.log_level.lower(),
        proxy_headers=True,
    )


__all__ = ["API_PREFIX", "app", "create_app", "run"]
