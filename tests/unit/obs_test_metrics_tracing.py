"""Prometheus metric names/labels/helpers and tracing no-op behaviour."""

from __future__ import annotations

import pytest
from prometheus_client import generate_latest

from atlas.core.config import Settings
from atlas.core.enums import DeviceOperationType, DriftStatus, FailureKind, Vendor, WorkflowType
from atlas.observability import metrics, tracing

EXPECTED_METRICS = {
    "atlas_workflow_total": {"workflow_type", "status"},
    "atlas_workflow_failures_total": {"workflow_type", "failure_stage"},
    "atlas_workflow_duration_seconds": {"workflow_type"},
    "atlas_device_operations_total": {"vendor", "operation_type", "status"},
    "atlas_device_operation_failures_total": {"vendor", "operation_type", "failure_kind"},
    "atlas_reconciliation_drift_total": {"status"},
    "atlas_api_requests_total": {"method", "route", "status"},
    "atlas_api_request_duration_seconds": {"method", "route"},
    "atlas_build_info": {"version", "environment"},
}


@pytest.fixture(autouse=True)
def fresh_registry():
    metrics.reset_registry_for_tests()
    yield
    metrics.reset_registry_for_tests()


def test_metric_names_and_labels() -> None:
    # Touch every metric once so label names show up in the exposition.
    metrics.record_workflow_result("ProvisionL3VPNWorkflow", "COMPLETED", 1.0, failure_stage="x")
    metrics.record_device_operation("mock", "APPLY", "FAILED", failure_kind="TRANSIENT")
    metrics.record_drift("IN_SYNC")
    metrics.record_api_request("GET", "/api/v1/services", 200, 0.01)
    metrics.set_build_info("0.0.0", "test")
    found: dict[str, set[str]] = {}
    for family in metrics.get_registry().collect():
        name = family.name
        if family.type == "counter":
            name = f"{name}_total"
        if name in EXPECTED_METRICS:
            labels: set[str] = set()
            for sample in family.samples:
                labels |= {k for k in sample.labels if k != "le"}
            found[name] = labels
    assert found == EXPECTED_METRICS


def test_workflow_duration_buckets_span_1_to_3600() -> None:
    assert metrics.WORKFLOW_DURATION_BUCKETS[0] == 1
    assert metrics.WORKFLOW_DURATION_BUCKETS[-1] == 3600


def test_record_workflow_result() -> None:
    metrics.record_workflow_result(
        WorkflowType.PROVISION_L3VPN, "FAILED", 12.5, failure_stage="provision_device"
    )
    metrics.record_workflow_result("SomethingUnknown", "COMPLETED", 1.0)
    text = generate_latest(metrics.get_registry()).decode()
    assert 'atlas_workflow_total{status="FAILED",workflow_type="ProvisionL3VPNWorkflow"} 1.0' in text
    assert (
        'atlas_workflow_failures_total{failure_stage="provision_device",workflow_type="ProvisionL3VPNWorkflow"} 1.0'
        in text
    )
    assert 'atlas_workflow_duration_seconds_count{workflow_type="ProvisionL3VPNWorkflow"} 1.0' in text
    assert 'atlas_workflow_total{status="COMPLETED",workflow_type="other"} 1.0' in text


def test_record_device_operation() -> None:
    metrics.record_device_operation(Vendor.MOCK, DeviceOperationType.APPLY, "SUCCEEDED")
    metrics.record_device_operation(
        Vendor.MOCK, DeviceOperationType.APPLY, "FAILED", failure_kind=FailureKind.TRANSIENT
    )
    text = generate_latest(metrics.get_registry()).decode()
    assert (
        'atlas_device_operations_total{operation_type="APPLY",status="SUCCEEDED",vendor="mock"} 1.0' in text
    )
    assert (
        'atlas_device_operation_failures_total{failure_kind="TRANSIENT",operation_type="APPLY",vendor="mock"} 1.0'
        in text
    )


def test_record_drift() -> None:
    metrics.record_drift(DriftStatus.DRIFT_DETECTED)
    metrics.record_drift("DRIFT_DETECTED")
    metrics.record_drift("garbage")
    assert metrics.RECONCILIATION_DRIFT_TOTAL.labels(status="DRIFT_DETECTED")._value.get() == 2
    assert metrics.RECONCILIATION_DRIFT_TOTAL.labels(status="other")._value.get() == 1


def test_safe_label_bounds_cardinality() -> None:
    assert metrics.safe_label(None) == "unknown"
    assert metrics.safe_label("") == "unknown"
    assert metrics.safe_label("x" * 100) == "x" * 40
    assert metrics.safe_label("a b;c") == "a_b_c"
    assert metrics.safe_label("mock", allowed={"mock"}) == "mock"
    assert metrics.safe_label("evil", allowed={"mock"}) == "other"
    assert metrics.safe_label(Vendor.CISCO_IOSXR) == "cisco_iosxr"


def test_metrics_asgi_app_and_build_info() -> None:
    metrics.set_build_info("1.2.3", "dev")
    app = metrics.metrics_asgi_app()
    assert callable(app)
    payload, content_type = metrics.render_metrics()
    assert b'atlas_build_info{environment="dev",version="1.2.3"} 1.0' in payload
    assert "text/plain" in content_type


def test_tracing_disabled_is_noop() -> None:
    tracing.configure_tracing(Settings(otel_enabled=False), app=None)
    assert tracing.is_configured() is False
    tracer = tracing.get_tracer("test")
    with tracer.start_as_current_span("noop") as span:
        assert span.is_recording() is False


def test_temporal_interceptors_list() -> None:
    interceptors = tracing.temporal_interceptors()
    assert isinstance(interceptors, list)
    from temporalio.contrib.opentelemetry import TracingInterceptor

    assert len(interceptors) == 1 and isinstance(interceptors[0], TracingInterceptor)


async def test_traced_decorator_runs_and_propagates_errors() -> None:
    @tracing.traced("unit.span", kind="test")
    async def ok(x: int) -> int:
        return x * 2

    @tracing.traced()
    async def boom() -> None:
        raise ValueError("bad")

    assert await ok(21) == 42
    with pytest.raises(ValueError):
        await boom()
    with pytest.raises(TypeError):
        tracing.traced()(lambda: None)  # type: ignore[arg-type]
