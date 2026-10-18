"""Prometheus metrics for ATLAS.

All metrics live in a dedicated ``CollectorRegistry`` so tests can reset them and so the
default process collectors do not leak into the ``/api/v1/metrics`` scrape.

Label cardinality is bounded: every label value passes through ``safe_label`` which clamps
length and, where an allow-list is given, maps unknown values to ``"other"``.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
    make_asgi_app,
)

from atlas.core.enums import DriftStatus, Vendor, WorkflowType

OTHER = "other"
_LABEL_SANITISE_RE = re.compile(r"[^A-Za-z0-9_.:/\-]")

WORKFLOW_DURATION_BUCKETS: tuple[float, ...] = (1, 2, 5, 10, 30, 60, 120, 300, 600, 1200, 1800, 3600)
API_DURATION_BUCKETS: tuple[float, ...] = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10)

ALLOWED_WORKFLOW_TYPES: frozenset[str] = frozenset(w.value for w in WorkflowType)
ALLOWED_VENDORS: frozenset[str] = frozenset(v.value for v in Vendor)
ALLOWED_DRIFT_STATUSES: frozenset[str] = frozenset(d.value for d in DriftStatus)


def safe_label(value: Any, allowed: Iterable[str] | None = None, max_len: int = 40) -> str:
    """Return a bounded, sanitised label value.

    * ``None``/empty -> ``"unknown"``
    * enum members use their ``.value``
    * when ``allowed`` is given, values outside it collapse to ``"other"``
    * otherwise non-label-safe characters are replaced and the value is truncated to ``max_len``
    """
    if value is None:
        return "unknown"
    raw = getattr(value, "value", value)
    text = str(raw).strip()
    if not text:
        return "unknown"
    if allowed is not None:
        allowed_set = set(allowed)
        return text if text in allowed_set else OTHER
    text = _LABEL_SANITISE_RE.sub("_", text)
    return text[:max_len]


class _Metrics:
    """Holder so the registry (and every metric) can be re-created in tests."""

    def __init__(self) -> None:
        self.registry = CollectorRegistry(auto_describe=True)
        r = self.registry

        self.workflow_total = Counter(
            "atlas_workflow_total",
            "Total orchestration workflows finished, by type and terminal status.",
            ["workflow_type", "status"],
            registry=r,
        )
        self.workflow_failures_total = Counter(
            "atlas_workflow_failures_total",
            "Workflow failures, by type and the stage in which the failure occurred.",
            ["workflow_type", "failure_stage"],
            registry=r,
        )
        self.workflow_duration_seconds = Histogram(
            "atlas_workflow_duration_seconds",
            "End-to-end workflow duration in seconds.",
            ["workflow_type"],
            buckets=WORKFLOW_DURATION_BUCKETS,
            registry=r,
        )
        self.device_operations_total = Counter(
            "atlas_device_operations_total",
            "Device operations executed through network adapters.",
            ["vendor", "operation_type", "status"],
            registry=r,
        )
        self.device_operation_failures_total = Counter(
            "atlas_device_operation_failures_total",
            "Failed device operations, classified by failure kind (TRANSIENT/PERMANENT/UNKNOWN).",
            ["vendor", "operation_type", "failure_kind"],
            registry=r,
        )
        self.reconciliation_drift_total = Counter(
            "atlas_reconciliation_drift_total",
            "Per-device reconciliation outcomes (IN_SYNC, DRIFT_DETECTED, DEVICE_UNREACHABLE, NOT_PROVISIONED).",
            ["status"],
            registry=r,
        )
        self.api_requests_total = Counter(
            "atlas_api_requests_total",
            "HTTP requests served by the API gateway.",
            ["method", "route", "status"],
            registry=r,
        )
        self.api_request_duration_seconds = Histogram(
            "atlas_api_request_duration_seconds",
            "HTTP request latency in seconds.",
            ["method", "route"],
            buckets=API_DURATION_BUCKETS,
            registry=r,
        )
        self.build_info = Gauge(
            "atlas_build_info",
            "Build metadata (value is always 1).",
            ["version", "environment"],
            registry=r,
        )


_metrics = _Metrics()


def _m() -> _Metrics:
    return _metrics


# Public module-level aliases (kept in sync by ``reset_registry_for_tests``).
WORKFLOW_TOTAL = _metrics.workflow_total
WORKFLOW_FAILURES_TOTAL = _metrics.workflow_failures_total
WORKFLOW_DURATION_SECONDS = _metrics.workflow_duration_seconds
DEVICE_OPERATIONS_TOTAL = _metrics.device_operations_total
DEVICE_OPERATION_FAILURES_TOTAL = _metrics.device_operation_failures_total
RECONCILIATION_DRIFT_TOTAL = _metrics.reconciliation_drift_total
API_REQUESTS_TOTAL = _metrics.api_requests_total
API_REQUEST_DURATION_SECONDS = _metrics.api_request_duration_seconds
BUILD_INFO = _metrics.build_info


def _rebind_aliases() -> None:
    g = globals()
    g["WORKFLOW_TOTAL"] = _metrics.workflow_total
    g["WORKFLOW_FAILURES_TOTAL"] = _metrics.workflow_failures_total
    g["WORKFLOW_DURATION_SECONDS"] = _metrics.workflow_duration_seconds
    g["DEVICE_OPERATIONS_TOTAL"] = _metrics.device_operations_total
    g["DEVICE_OPERATION_FAILURES_TOTAL"] = _metrics.device_operation_failures_total
    g["RECONCILIATION_DRIFT_TOTAL"] = _metrics.reconciliation_drift_total
    g["API_REQUESTS_TOTAL"] = _metrics.api_requests_total
    g["API_REQUEST_DURATION_SECONDS"] = _metrics.api_request_duration_seconds
    g["BUILD_INFO"] = _metrics.build_info


def get_registry() -> CollectorRegistry:
    return _metrics.registry


def reset_registry_for_tests() -> CollectorRegistry:
    """Replace the registry and all metrics with fresh instances (unit tests only)."""
    global _metrics
    _metrics = _Metrics()
    _rebind_aliases()
    return _metrics.registry


def set_build_info(version: str, environment: str) -> None:
    _m().build_info.labels(version=safe_label(version), environment=safe_label(environment)).set(1)


def metrics_asgi_app() -> Any:
    """ASGI app serving the Prometheus exposition format; mount at ``/api/v1/metrics``."""
    return make_asgi_app(registry=get_registry())


def render_metrics() -> tuple[bytes, str]:
    """Return ``(payload, content_type)`` for a plain FastAPI route."""
    return generate_latest(get_registry()), CONTENT_TYPE_LATEST


# ---------------------------------------------------------------------------
# Recording helpers
# ---------------------------------------------------------------------------


def record_workflow_result(
    workflow_type: Any,
    status: Any,
    duration_s: float | None,
    failure_stage: str | None = None,
) -> None:
    m = _m()
    wt = safe_label(workflow_type, allowed=ALLOWED_WORKFLOW_TYPES)
    st = safe_label(status)
    m.workflow_total.labels(workflow_type=wt, status=st).inc()
    if duration_s is not None and duration_s >= 0:
        m.workflow_duration_seconds.labels(workflow_type=wt).observe(float(duration_s))
    if failure_stage is not None:
        m.workflow_failures_total.labels(workflow_type=wt, failure_stage=safe_label(failure_stage)).inc()


def record_device_operation(vendor: Any, op_type: Any, status: Any, failure_kind: Any | None = None) -> None:
    m = _m()
    v = safe_label(vendor, allowed=ALLOWED_VENDORS)
    op = safe_label(op_type)
    m.device_operations_total.labels(vendor=v, operation_type=op, status=safe_label(status)).inc()
    if failure_kind is not None:
        m.device_operation_failures_total.labels(
            vendor=v, operation_type=op, failure_kind=safe_label(failure_kind)
        ).inc()


def record_drift(status: Any) -> None:
    _m().reconciliation_drift_total.labels(status=safe_label(status, allowed=ALLOWED_DRIFT_STATUSES)).inc()


def record_api_request(method: str, route: str, status_code: int, duration_s: float) -> None:
    m = _m()
    meth = safe_label(method.upper(), max_len=10)
    rt = safe_label(route, max_len=80)
    m.api_requests_total.labels(method=meth, route=rt, status=str(int(status_code))).inc()
    m.api_request_duration_seconds.labels(method=meth, route=rt).observe(max(0.0, float(duration_s)))


__all__ = [
    "API_REQUESTS_TOTAL",
    "API_REQUEST_DURATION_SECONDS",
    "BUILD_INFO",
    "DEVICE_OPERATIONS_TOTAL",
    "DEVICE_OPERATION_FAILURES_TOTAL",
    "RECONCILIATION_DRIFT_TOTAL",
    "WORKFLOW_DURATION_SECONDS",
    "WORKFLOW_FAILURES_TOTAL",
    "WORKFLOW_TOTAL",
    "get_registry",
    "metrics_asgi_app",
    "record_api_request",
    "record_device_operation",
    "record_drift",
    "record_workflow_result",
    "render_metrics",
    "reset_registry_for_tests",
    "safe_label",
    "set_build_info",
]
