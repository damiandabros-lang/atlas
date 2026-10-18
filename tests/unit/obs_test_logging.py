"""Structured logging: JSON keys, context binding and secret redaction."""

from __future__ import annotations

import json
import logging

import pytest
import structlog

from atlas.core.config import Settings
from atlas.observability.logging import (
    REDACTED,
    bind_context,
    clear_context,
    configure_logging,
    get_logger,
    log_context,
    redact_processor,
)


@pytest.fixture
def json_logs(capsys):
    configure_logging(Settings(log_json=True, log_level="DEBUG"))
    clear_context()
    yield capsys
    clear_context()
    structlog.reset_defaults()
    logging.getLogger().handlers.clear()


def _last_record(capsys) -> dict:
    lines = [ln for ln in capsys.readouterr().out.strip().splitlines() if ln.strip()]
    return json.loads(lines[-1])


def test_json_output_contains_required_keys(json_logs) -> None:
    bind_context(correlation_id="corr-1", service_id="svc-1", workflow_id="wf-1")
    get_logger(component="test").info("provisioning started", device_id="PE1", activity="apply")
    rec = _last_record(json_logs)
    assert rec["message"] == "provisioning started"
    assert "event" not in rec
    assert rec["level"] == "info"
    assert rec["timestamp"].endswith("Z")
    assert rec["correlation_id"] == "corr-1"
    assert rec["service_id"] == "svc-1"
    assert rec["workflow_id"] == "wf-1"
    assert rec["device_id"] == "PE1"
    assert rec["activity"] == "apply"
    assert rec["component"] == "test"


def test_log_context_is_scoped(json_logs) -> None:
    log = get_logger()
    with log_context(device_id="PE2"):
        log.info("inside")
        assert _last_record(json_logs)["device_id"] == "PE2"
    log.info("outside")
    assert "device_id" not in _last_record(json_logs)


def test_sensitive_keys_are_masked(json_logs) -> None:
    get_logger().info(
        "connect",
        password="cisco123",
        API_KEY="abc",
        Authorization="Bearer xyz",
        device_credential={"user": "u"},
        nested={"token": "t0k", "ok": "keep"},
    )
    rec = _last_record(json_logs)
    assert rec["password"] == REDACTED
    assert rec["API_KEY"] == REDACTED
    assert rec["Authorization"] == REDACTED
    assert rec["device_credential"] == REDACTED
    assert rec["nested"] == {"token": REDACTED, "ok": "keep"}


def test_inline_patterns_are_masked(json_logs) -> None:
    get_logger().info("sending Authorization: Bearer eyJhbGciOi.payload with password hunter2 to device")
    msg = _last_record(json_logs)["message"]
    assert "eyJhbGciOi" not in msg
    assert "hunter2" not in msg
    assert msg.count(REDACTED) == 2


def test_stdlib_loggers_are_bridged(json_logs) -> None:
    logging.getLogger("uvicorn.error").warning("worker %s restarted", 3)
    rec = _last_record(json_logs)
    assert rec["message"] == "worker 3 restarted"
    assert rec["logger"] == "uvicorn.error"
    assert rec["level"] == "warning"


def test_redact_processor_is_pure() -> None:
    out = redact_processor(None, "info", {"event": "x", "secret_key": "s", "plain": "p"})
    assert out == {"event": "x", "secret_key": REDACTED, "plain": "p"}
