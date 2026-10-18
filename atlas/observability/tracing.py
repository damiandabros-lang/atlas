"""OpenTelemetry tracing: FastAPI auto-instrumentation + Temporal interceptor.

When ``Settings.otel_enabled`` is false everything degrades to no-ops (the global
``TracerProvider`` stays the SDK default proxy provider, which records nothing).
"""

from __future__ import annotations

import functools
import inspect
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from opentelemetry import trace
from opentelemetry.trace import Tracer

from atlas.core.config import Settings

F = TypeVar("F", bound=Callable[..., Awaitable[Any]])

_configured = False
_provider: Any = None


def is_configured() -> bool:
    return _configured


def configure_tracing(settings: Settings, app: Any | None = None) -> None:
    """Install a ``TracerProvider`` with an OTLP/gRPC exporter and instrument ``app`` (FastAPI).

    Idempotent: a second call only (re-)instruments the app when one is given.
    """
    global _configured, _provider

    if not settings.otel_enabled:
        return

    if not _configured:
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.resources import SERVICE_NAME, Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        resource = Resource.create(
            {
                SERVICE_NAME: settings.otel_service_name,
                "deployment.environment": settings.environment,
                "service.namespace": settings.app_name,
            }
        )
        provider = TracerProvider(resource=resource)
        exporter = OTLPSpanExporter(endpoint=settings.otel_exporter_otlp_endpoint, insecure=True)
        provider.add_span_processor(BatchSpanProcessor(exporter))
        trace.set_tracer_provider(provider)
        _provider = provider
        _configured = True

    if app is not None:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

        FastAPIInstrumentor.instrument_app(app, excluded_urls="health,metrics")


def shutdown_tracing() -> None:
    global _configured, _provider
    if _provider is not None:
        try:
            _provider.shutdown()
        finally:
            _provider = None
            _configured = False


def temporal_interceptors() -> list[Any]:
    """Temporal client/worker interceptors propagating trace context through workflows & activities."""
    try:
        from temporalio.contrib.opentelemetry import TracingInterceptor
    except Exception:  # pragma: no cover - optional dependency
        return []
    return [TracingInterceptor()]


def get_tracer(name: str = "atlas") -> Tracer:
    return trace.get_tracer(name)


def traced(span_name: str | None = None, **attributes: Any) -> Callable[[F], F]:
    """Decorator wrapping an ``async`` function in a span. Exceptions are recorded and re-raised."""

    def decorator(func: F) -> F:
        if not inspect.iscoroutinefunction(func):
            raise TypeError("@traced only supports async functions")
        name = span_name or f"{func.__module__}.{func.__qualname__}"

        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            tracer = get_tracer(func.__module__)
            with tracer.start_as_current_span(name) as span:
                for k, v in attributes.items():
                    span.set_attribute(k, v)
                try:
                    return await func(*args, **kwargs)
                except Exception as exc:
                    span.record_exception(exc)
                    span.set_status(trace.Status(trace.StatusCode.ERROR, str(exc)))
                    raise

        return wrapper  # type: ignore[return-value]

    return decorator


__all__ = [
    "configure_tracing",
    "get_tracer",
    "is_configured",
    "shutdown_tracing",
    "temporal_interceptors",
    "traced",
]
