"""``Idempotency-Key`` handling for mutating endpoints.

Why an explicit helper and not middleware
-----------------------------------------
A generic middleware would have to buffer and hash the raw request body *before* FastAPI has
parsed and validated it, re-inject the body stream for the downstream handler, and capture
the streamed response to store it. That is fragile (body replay, streaming responses, 422s
from validation that should *not* be cached) and it hides the business rule in plumbing.

Instead the create endpoint calls :func:`check_idempotency` after validation and
:func:`store_idempotent_response` after success. The flow is explicit in the handler, only
successful (2xx) responses are recorded, and the fingerprint is computed over the
*canonical* validated payload, so semantically identical JSON (key order, whitespace)
hashes the same.

Semantics
---------
* missing header            -> 400 ``idempotency_key_required``
* same key, same payload    -> replay stored status/body with ``Idempotent-Replay: true``
* same key, other payload   -> 422 ``idempotency_key_reused``
* expired record            -> treated as absent (and will be overwritten)
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from atlas.api.errors import ApiError
from atlas.db.repositories import IdempotencyRepository

IDEMPOTENCY_HEADER = "Idempotency-Key"
REPLAY_HEADER = "Idempotent-Replay"
_MAX_KEY_LEN = 128


@dataclass(frozen=True, slots=True)
class IdempotencyContext:
    key: str
    request_hash: str


def request_fingerprint(method: str, path: str, payload: Any) -> str:
    """sha256 over method + path + canonical JSON of the (validated) payload."""
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(f"{method.upper()} {path}\n{canonical}".encode()).hexdigest()


def require_idempotency_key(raw: str | None) -> str:
    if raw is None or not raw.strip():
        raise ApiError(400, "idempotency_key_required", f"{IDEMPOTENCY_HEADER} header is required")
    key = raw.strip()
    if len(key) > _MAX_KEY_LEN:
        raise ApiError(
            400, "idempotency_key_invalid", f"{IDEMPOTENCY_HEADER} must be <= {_MAX_KEY_LEN} chars"
        )
    return key


async def check_idempotency(
    session: AsyncSession,
    *,
    key: str,
    method: str,
    path: str,
    payload: Any,
    correlation_id: str | None = None,
) -> tuple[IdempotencyContext, JSONResponse | None]:
    """Return ``(ctx, replay)``. ``replay`` is a ready response when the key was already used
    with an identical payload; ``None`` means the caller must proceed and later call
    :func:`store_idempotent_response`."""
    ctx = IdempotencyContext(key=key, request_hash=request_fingerprint(method, path, payload))
    record = await IdempotencyRepository(session).get(key)
    if record is None:
        return ctx, None
    expires_at = record.expires_at
    if expires_at.tzinfo is None:  # SQLite returns naive datetimes
        expires_at = expires_at.replace(tzinfo=UTC)
    if expires_at <= datetime.now(UTC):
        return ctx, None
    if record.request_hash != ctx.request_hash:
        raise ApiError(
            422,
            "idempotency_key_reused",
            f"{IDEMPOTENCY_HEADER} {key!r} was already used with a different request payload",
        )
    headers = {REPLAY_HEADER: "true"}
    if correlation_id:
        headers["X-Correlation-ID"] = correlation_id
    return ctx, JSONResponse(status_code=record.status_code, content=record.response_body, headers=headers)


async def store_idempotent_response(
    session: AsyncSession,
    ctx: IdempotencyContext,
    *,
    status_code: int,
    body: dict[str, Any],
    ttl_seconds: int,
) -> None:
    """Persist and commit the successful response for later replay."""
    await IdempotencyRepository(session).put(
        ctx.key,
        ctx.request_hash,
        status_code,
        body,
        expires_at=datetime.now(UTC) + timedelta(seconds=ttl_seconds),
    )
    await session.commit()


__all__ = [
    "IDEMPOTENCY_HEADER",
    "REPLAY_HEADER",
    "IdempotencyContext",
    "check_idempotency",
    "request_fingerprint",
    "require_idempotency_key",
    "store_idempotent_response",
]
