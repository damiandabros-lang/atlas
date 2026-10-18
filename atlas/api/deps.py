"""FastAPI dependencies: settings, DB session, Temporal client, controller, correlation id.

Everything here is overridable through ``app.dependency_overrides`` — tests swap
``get_temporal`` for a fake client and ``get_db`` for an SQLite session.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator

from fastapi import Depends, Request, Response
from sqlalchemy.ext.asyncio import AsyncSession
from temporalio.client import Client

from atlas.core.config import Settings, get_settings
from atlas.db.session import get_session
from atlas.services.service_controller import ServiceController
from atlas.services.temporal_client import get_temporal_client

CORRELATION_HEADER = "X-Correlation-ID"


def get_settings_dep() -> Settings:
    """Thin wrapper so the cached ``get_settings`` can be overridden per app instance."""
    return get_settings()


async def get_db() -> AsyncIterator[AsyncSession]:
    async for session in get_session():
        yield session


async def get_temporal(settings: Settings = Depends(get_settings_dep)) -> Client:
    return await get_temporal_client(settings)


def get_correlation_id(request: Request, response: Response) -> str:
    """Correlation id for this request: taken from ``X-Correlation-ID`` or generated.

    The middleware in ``atlas.api.main`` already sets ``request.state.correlation_id``; this
    dependency is the typed accessor for handlers and guarantees the header is echoed back.
    """
    cid = getattr(request.state, "correlation_id", None)
    if not cid:
        cid = request.headers.get(CORRELATION_HEADER) or str(uuid.uuid4())
        request.state.correlation_id = cid
    response.headers[CORRELATION_HEADER] = cid
    return cid


async def get_controller(
    session: AsyncSession = Depends(get_db),
    temporal: Client = Depends(get_temporal),
    settings: Settings = Depends(get_settings_dep),
) -> ServiceController:
    return ServiceController(session, temporal, settings)


__all__ = [
    "CORRELATION_HEADER",
    "get_controller",
    "get_correlation_id",
    "get_db",
    "get_settings_dep",
    "get_temporal",
]
