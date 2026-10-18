"""Domain error type and FastAPI exception handlers.

Every error leaving the API is rendered as ``atlas.schemas.ErrorOut`` so clients get one
shape regardless of where the failure originated.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from atlas.core.lifecycle import InvalidTransitionError
from atlas.db.repositories import ResourcePoolExhaustedError
from atlas.schemas import ErrorOut

log = logging.getLogger("atlas.api")


class ApiError(Exception):
    """Domain-level error carrying an HTTP status, a short machine-readable ``error`` code and
    a human-readable ``detail``."""

    def __init__(self, status_code: int, error: str, detail: str | None = None, **extra: Any) -> None:
        super().__init__(detail or error)
        self.status_code = status_code
        self.error = error
        self.detail = detail
        self.extra = extra


def _correlation_id(request: Request) -> str | None:
    return getattr(request.state, "correlation_id", None)


def _render(request: Request, status_code: int, error: str, detail: str | None) -> JSONResponse:
    body = ErrorOut(error=error, detail=detail, correlation_id=_correlation_id(request))
    headers = {}
    if body.correlation_id:
        headers["X-Correlation-ID"] = body.correlation_id
    return JSONResponse(status_code=status_code, content=body.model_dump(mode="json"), headers=headers)


async def api_error_handler(request: Request, exc: ApiError) -> JSONResponse:
    return _render(request, exc.status_code, exc.error, exc.detail)


async def validation_error_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    parts = []
    for err in exc.errors():
        loc = ".".join(str(p) for p in err.get("loc", ()) if p != "body")
        parts.append(f"{loc}: {err.get('msg')}" if loc else str(err.get("msg")))
    return _render(request, 422, "validation_error", "; ".join(parts) or "invalid request")


async def invalid_transition_handler(request: Request, exc: InvalidTransitionError) -> JSONResponse:
    return _render(request, 409, "invalid_transition", str(exc))


async def pool_exhausted_handler(request: Request, exc: ResourcePoolExhaustedError) -> JSONResponse:
    return _render(request, 503, "resource_pool_exhausted", str(exc) or "resource pool exhausted")


async def http_exception_handler(request: Request, exc: StarletteHTTPException) -> JSONResponse:
    detail = exc.detail if isinstance(exc.detail, str) else None
    code = {401: "unauthorized", 403: "forbidden", 404: "not_found", 405: "method_not_allowed"}.get(
        exc.status_code, "http_error"
    )
    resp = _render(request, exc.status_code, code, detail)
    if exc.headers:
        resp.headers.update(exc.headers)
    return resp


async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    log.exception(
        "unhandled exception", extra={"correlation_id": _correlation_id(request), "path": request.url.path}
    )
    return _render(request, 500, "internal_error", "internal server error")


def install_exception_handlers(app: FastAPI) -> None:
    app.add_exception_handler(ApiError, api_error_handler)  # type: ignore[arg-type]
    app.add_exception_handler(RequestValidationError, validation_error_handler)  # type: ignore[arg-type]
    app.add_exception_handler(InvalidTransitionError, invalid_transition_handler)  # type: ignore[arg-type]
    app.add_exception_handler(ResourcePoolExhaustedError, pool_exhausted_handler)  # type: ignore[arg-type]
    app.add_exception_handler(StarletteHTTPException, http_exception_handler)  # type: ignore[arg-type]
    app.add_exception_handler(Exception, unhandled_exception_handler)


__all__ = ["ApiError", "install_exception_handlers"]
