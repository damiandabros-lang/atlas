"""Engine / session factory / unit-of-work for ATLAS.

Concurrency & transaction model
-------------------------------
* One process-wide ``AsyncEngine`` (lazily created from ``Settings.database_url``) backed by
  asyncpg on PostgreSQL. Pool size comes from ``Settings.db_pool_size``.
* ``transaction()`` is the *only* place that commits. Repositories never commit; they just
  issue statements against the session they were given. This keeps business-state
  transitions and resource allocation inside one atomic unit (brief §11 "Transactions").
* ``get_session()`` is the FastAPI dependency: it hands out a session without auto-commit so
  request handlers can compose repositories and explicitly ``await session.commit()`` (or
  wrap in ``transaction()``).
* SQLite (unit tests) does not enforce foreign keys unless ``PRAGMA foreign_keys=ON`` is
  issued on every new connection, so we install a connect-event listener for that dialect.
* Tests call ``init_db_for_tests()`` which builds a dedicated engine, runs ``create_all`` and
  *replaces* the process-wide sessionmaker so other layers (activities, API) transparently
  run against SQLite. ``reset_engine()`` restores the lazy defaults.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from atlas.core.config import get_settings
from atlas.db.models import Base

_engine: AsyncEngine | None = None
_sessionmaker: async_sessionmaker[AsyncSession] | None = None


def _is_sqlite(url: str) -> bool:
    return url.startswith("sqlite")


def _install_sqlite_pragmas(engine: AsyncEngine) -> None:
    """Turn on FK enforcement for every pooled SQLite connection (off by default in SQLite)."""

    @event.listens_for(engine.sync_engine, "connect")
    def _on_connect(dbapi_connection: Any, _record: Any) -> None:  # pragma: no cover - trivial
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()


def _build_engine(url: str, *, echo: bool = False, pool_size: int | None = None) -> AsyncEngine:
    kwargs: dict[str, Any] = {"echo": echo, "future": True}
    if _is_sqlite(url):
        # In-memory SQLite must share ONE connection or every session sees an empty DB.
        if ":memory:" in url or url.endswith("sqlite+aiosqlite://"):
            kwargs["poolclass"] = StaticPool
            kwargs["connect_args"] = {"check_same_thread": False}
        else:
            # File-based SQLite (e2e tests): separate connections per session, wait for the
            # write lock instead of failing fast when API and worker write concurrently.
            kwargs["connect_args"] = {"check_same_thread": False, "timeout": 30}
    else:
        if pool_size is not None:
            kwargs["pool_size"] = pool_size
        kwargs["pool_pre_ping"] = True
    engine = create_async_engine(url, **kwargs)
    if _is_sqlite(url):
        _install_sqlite_pragmas(engine)
    return engine


def get_engine() -> AsyncEngine:
    """Return the cached process-wide engine built from ``Settings``."""
    global _engine
    if _engine is None:
        settings = get_settings()
        _engine = _build_engine(settings.database_url, echo=settings.db_echo, pool_size=settings.db_pool_size)
    return _engine


def get_sessionmaker() -> async_sessionmaker[AsyncSession]:
    """Return the cached sessionmaker. ``expire_on_commit=False`` so ORM objects remain usable
    after ``transaction()`` commits (activities serialise them into Pydantic payloads)."""
    global _sessionmaker
    if _sessionmaker is None:
        _sessionmaker = async_sessionmaker(get_engine(), expire_on_commit=False, autoflush=True)
    return _sessionmaker


def set_sessionmaker(sessionmaker: async_sessionmaker[AsyncSession] | None) -> None:
    """Override the process-wide sessionmaker (used by tests to point every layer at SQLite)."""
    global _sessionmaker
    _sessionmaker = sessionmaker


@asynccontextmanager
async def transaction() -> AsyncIterator[AsyncSession]:
    """Unit of work: begin -> yield session -> commit; rollback and re-raise on any exception.

    Nested use inside repositories is forbidden by design — the caller owns the boundary.
    """
    async with get_sessionmaker()() as session:
        try:
            async with session.begin():
                yield session
        except BaseException:
            # ``session.begin()`` already rolled back; re-raise so Temporal/FastAPI see the failure.
            raise


async def get_session() -> AsyncIterator[AsyncSession]:
    """FastAPI dependency: a session with no implicit commit. Handlers commit explicitly."""
    async with get_sessionmaker()() as session:
        yield session


async def init_db_for_tests(
    url: str = "sqlite+aiosqlite:///:memory:", *, override_global: bool = True
) -> async_sessionmaker[AsyncSession]:
    """Create a fresh engine for ``url``, ``create_all`` the schema and return a sessionmaker.

    With ``override_global`` (default) the module-level engine/sessionmaker are replaced so
    code paths that call ``get_sessionmaker()``/``transaction()`` run against the test DB.
    """
    global _engine, _sessionmaker
    engine = _build_engine(url)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False, autoflush=True)
    if override_global:
        if _engine is not None and _engine is not engine:
            await _engine.dispose()
        _engine = engine
        _sessionmaker = maker
    return maker


async def reset_engine() -> None:
    """Dispose the cached engine and forget the sessionmaker (test teardown / reconfiguration)."""
    global _engine, _sessionmaker
    if _engine is not None:
        await _engine.dispose()
    _engine = None
    _sessionmaker = None


__all__ = [
    "get_engine",
    "get_session",
    "get_sessionmaker",
    "init_db_for_tests",
    "reset_engine",
    "set_sessionmaker",
    "transaction",
]
