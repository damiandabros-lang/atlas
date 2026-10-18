"""Structured logging (structlog) with secret redaction and a stdlib bridge.

* JSON output by default (``Settings.log_json``), human-readable console renderer otherwise.
* Context variables (``correlation_id``, ``service_id``, ``workflow_id``, ``device_id``, ``activity``)
  are merged into every event via ``structlog.contextvars``.
* Values of sensitive keys and inline secret patterns are masked before rendering.
* Stdlib loggers (uvicorn, temporalio, sqlalchemy, ...) are routed through
  ``structlog.stdlib.ProcessorFormatter`` so the whole process emits one log format.
"""

from __future__ import annotations

import logging
import re
import sys
from collections.abc import Iterator, MutableMapping
from contextlib import contextmanager
from typing import Any

import structlog
from structlog.types import EventDict, Processor, WrappedLogger

from atlas.core.config import Settings

REDACTED = "***"

_SENSITIVE_KEY_RE = re.compile(
    r"(password|secret|token|authorization|api[-_]?key|x-api-key|credential)", re.IGNORECASE
)

# Inline patterns inside free-text values, e.g. "password cisco123", "Authorization: Bearer abc.def".
_INLINE_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (
        re.compile(r"(?i)\b(authorization\s*:\s*(?:bearer|basic)\s+)\S+"),
        rf"\1{REDACTED}",
    ),
    (
        re.compile(r"(?i)\b(bearer\s+)[A-Za-z0-9\-._~+/]+=*"),
        rf"\1{REDACTED}",
    ),
    (
        re.compile(
            r"(?i)\b((?:password|passwd|pwd|secret|token|api[-_]?key|credential)s?\s*[:=]?\s+)(?!\*\*\*)\S+"
        ),
        rf"\1{REDACTED}",
    ),
)


def _redact_text(value: str) -> str:
    for pattern, repl in _INLINE_PATTERNS:
        value = pattern.sub(repl, value)
    return value


def _redact_value(key: str | None, value: Any, depth: int = 0) -> Any:
    if depth > 8:
        return value
    if key is not None and _SENSITIVE_KEY_RE.search(key):
        return REDACTED
    if isinstance(value, str):
        return _redact_text(value)
    if isinstance(value, MutableMapping):
        return {k: _redact_value(str(k), v, depth + 1) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return type(value)(_redact_value(None, v, depth + 1) for v in value)
    return value


def redact_processor(_logger: WrappedLogger, _method: str, event_dict: EventDict) -> EventDict:
    """structlog processor masking secrets by key name and by inline pattern."""
    for key in list(event_dict.keys()):
        event_dict[key] = _redact_value(key, event_dict[key])
    return event_dict


def rename_event_to_message(_logger: WrappedLogger, _method: str, event_dict: EventDict) -> EventDict:
    if "event" in event_dict and "message" not in event_dict:
        event_dict["message"] = event_dict.pop("event")
    return event_dict


def _shared_processors() -> list[Processor]:
    return [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True, key="timestamp"),
        structlog.processors.StackInfoRenderer(),
        redact_processor,
    ]


def _renderer(log_json: bool) -> Processor:
    if log_json:
        return structlog.processors.JSONRenderer(sort_keys=True)
    return structlog.dev.ConsoleRenderer(colors=sys.stderr.isatty())


class _LateBoundStdout:
    """Stream that resolves ``sys.stdout`` at write time (survives stdout replacement/capture)."""

    def write(self, data: str) -> int:
        return sys.stdout.write(data)

    def flush(self) -> None:
        sys.stdout.flush()


def configure_logging(settings: Settings) -> None:
    """Configure structlog + stdlib logging for the whole process. Safe to call more than once."""
    level = logging.getLevelName(settings.log_level.upper())
    if not isinstance(level, int):
        level = logging.INFO

    shared = _shared_processors()
    # When rendering JSON the stdlib "event" key is renamed to "message"; the console renderer
    # expects "event" so the rename is only applied for JSON output.
    tail: list[Processor] = [structlog.processors.format_exc_info]
    if settings.log_json:
        tail.append(rename_event_to_message)

    structlog.configure(
        processors=[
            *shared,
            structlog.processors.UnicodeDecoder(),
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=False,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            *tail,
            _renderer(settings.log_json),
        ],
    )

    handler = logging.StreamHandler(_LateBoundStdout())
    handler.setFormatter(formatter)

    root = logging.getLogger()
    for existing in list(root.handlers):
        root.removeHandler(existing)
    root.addHandler(handler)
    root.setLevel(level)

    # Route well-known third-party loggers through the root handler (single format, no duplicates).
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access", "temporalio", "sqlalchemy.engine", "alembic"):
        lg = logging.getLogger(name)
        lg.handlers.clear()
        lg.propagate = True
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)


def get_logger(**bind: Any) -> structlog.stdlib.BoundLogger:
    """Return a structlog logger with optional initial bindings (``get_logger(component="api")``)."""
    return structlog.get_logger().bind(**bind)  # type: ignore[no-any-return]


def bind_context(**kv: Any) -> None:
    """Bind correlation fields (correlation_id, service_id, workflow_id, device_id, activity, ...)
    to the current async context; they appear on every subsequent log line."""
    structlog.contextvars.bind_contextvars(**{k: v for k, v in kv.items() if v is not None})


def unbind_context(*keys: str) -> None:
    structlog.contextvars.unbind_contextvars(*keys)


def clear_context() -> None:
    structlog.contextvars.clear_contextvars()


@contextmanager
def log_context(**kv: Any) -> Iterator[None]:
    """Temporarily bind context fields; previous bindings are restored on exit."""
    tokens = structlog.contextvars.bind_contextvars(**{k: v for k, v in kv.items() if v is not None})
    try:
        yield
    finally:
        structlog.contextvars.reset_contextvars(**tokens)


__all__ = [
    "REDACTED",
    "bind_context",
    "clear_context",
    "configure_logging",
    "get_logger",
    "log_context",
    "redact_processor",
    "rename_event_to_message",
    "unbind_context",
]
