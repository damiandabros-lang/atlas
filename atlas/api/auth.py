"""API-key authentication (MVP).

Operators present ``X-API-Key``; it is compared in constant time against every configured key
in ``Settings.api_keys``. ``/api/v1/health`` and ``/api/v1/metrics`` are public (they are not
wired through ``require_operator``) so Kubernetes probes and Prometheus need no credentials.

Production deployments should replace this with OIDC/JWT; the ``Principal`` type is the
seam — routers only ever depend on ``require_operator`` and never on the header itself.
"""

from __future__ import annotations

import hashlib
import hmac
from dataclasses import dataclass

from fastapi import Depends, HTTPException, Security, status
from fastapi.security import APIKeyHeader

from atlas.api.deps import get_settings_dep
from atlas.core.config import Settings

API_KEY_HEADER = "X-API-Key"

api_key_scheme = APIKeyHeader(name=API_KEY_HEADER, auto_error=False, description="Operator API key")


@dataclass(frozen=True, slots=True)
class Principal:
    """Authenticated caller. ``name`` is recorded as the audit ``actor``."""

    name: str
    role: str = "operator"


def _digest(value: str) -> bytes:
    return hashlib.sha256(value.encode("utf-8")).digest()


def verify_api_key(presented: str | None, configured: list[str]) -> bool:
    """Constant-time comparison against *every* configured key (no early exit on match so the
    timing does not leak which key matched)."""
    if not presented:
        return False
    presented_digest = _digest(presented)
    matched = False
    for key in configured:
        if hmac.compare_digest(presented_digest, _digest(key)):
            matched = True
    return matched


def _principal_name(key: str) -> str:
    # Never log or persist the key itself; a short fingerprint identifies the caller.
    return f"operator:{hashlib.sha256(key.encode()).hexdigest()[:12]}"


async def require_operator(
    api_key: str | None = Security(api_key_scheme),
    settings: Settings = Depends(get_settings_dep),
) -> Principal:
    if not verify_api_key(api_key, settings.api_keys):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing or invalid API key",
            headers={"WWW-Authenticate": f'ApiKey header="{API_KEY_HEADER}"'},
        )
    assert api_key is not None
    return Principal(name=_principal_name(api_key))


__all__ = ["API_KEY_HEADER", "Principal", "api_key_scheme", "require_operator", "verify_api_key"]
