"""``/api/v1/health`` and ``/api/v1/metrics`` — public (no API key) operational endpoints."""

from __future__ import annotations

import asyncio
from typing import Annotated, Any

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

import atlas
from atlas.api.deps import get_db, get_settings_dep
from atlas.core.config import Settings
from atlas.schemas import HealthOut
from atlas.services.temporal_client import get_temporal_client

router = APIRouter(tags=["ops"])

_TEMPORAL_PROBE_TIMEOUT = 2.0


async def _probe_db(session: AsyncSession) -> str:
    try:
        await session.execute(text("SELECT 1"))
        return "ok"
    except Exception as exc:  # pragma: no cover - exercised only with a broken DB
        return f"error: {type(exc).__name__}"


async def _probe_temporal(settings: Settings) -> str:
    """Connect (cached) and issue a cheap server call. Degrades to ``unavailable`` on error."""
    try:
        client = await asyncio.wait_for(get_temporal_client(settings), timeout=_TEMPORAL_PROBE_TIMEOUT)
        probe = getattr(getattr(client, "service_client", None), "check_health", None)
        if probe is not None:
            await asyncio.wait_for(probe(), timeout=_TEMPORAL_PROBE_TIMEOUT)
        return "ok"
    except Exception as exc:
        return f"unavailable: {type(exc).__name__}"


@router.get(
    "/health", response_model=HealthOut, responses={503: {"model": HealthOut}}, summary="Liveness/readiness"
)
async def health(
    session: Annotated[AsyncSession, Depends(get_db)],
    settings: Annotated[Settings, Depends(get_settings_dep)],
) -> Any:
    db_status, temporal_status = await asyncio.gather(_probe_db(session), _probe_temporal(settings))
    db_ok = db_status == "ok"
    temporal_ok = temporal_status == "ok"
    overall = "ok" if db_ok and temporal_ok else ("degraded" if db_ok else "unhealthy")
    body = HealthOut(status=overall, version=atlas.__version__, database=db_status, temporal=temporal_status)
    # Temporal being down is "degraded": reads still work from the DB. DB down is fatal.
    status_code = 200 if db_ok else 503
    return JSONResponse(status_code=status_code, content=body.model_dump(mode="json"))


class MetricsEndpoint:
    """ASGI endpoint wrapping the Prometheus app.

    ``atlas.observability.metrics.metrics_asgi_app()`` is preferred (it serves the registry the
    workflow/activity metrics are registered in); when that module is absent we fall back to
    ``prometheus_client``'s default registry. Wrapped in a class so Starlette's ``Route`` treats
    it as a raw ASGI app rather than a request handler — this is how ``GET /api/v1/metrics`` is
    served without the trailing-slash redirect a ``Mount`` would introduce.
    """

    def __init__(self) -> None:
        try:
            from atlas.observability.metrics import metrics_asgi_app

            self.app = metrics_asgi_app()
        except Exception:
            from prometheus_client import make_asgi_app

            self.app = make_asgi_app()

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        await self.app(scope, receive, send)


def metrics_app() -> MetricsEndpoint:
    return MetricsEndpoint()
