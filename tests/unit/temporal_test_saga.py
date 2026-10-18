"""Saga helper + activity policies (pure, no Temporal server needed)."""

from __future__ import annotations

from datetime import timedelta

import pytest
from temporalio.workflow import ActivityCancellationType

from atlas.workflows import policies
from atlas.workflows.saga import Saga

# ---------------------------------------------------------------------------
# Saga
# ---------------------------------------------------------------------------


def _make_executor(fail: set[str] | None = None):
    calls: list[str] = []

    async def executor(activity_fn, arg, policy):
        calls.append(arg)
        if fail and arg in fail:
            raise RuntimeError(f"compensation {arg} exploded")
        return arg

    return executor, calls


async def test_saga_runs_in_reverse_order():
    executor, calls = _make_executor()
    saga = Saga(executor=executor)
    for host in ("PE1", "PE2", "PE3"):
        saga.add_compensation(host, object(), host, policies.COMPENSATION)
    assert saga.labels == ["PE1", "PE2", "PE3"]
    outcomes = await saga.compensate()
    assert calls == ["PE3", "PE2", "PE1"]
    assert outcomes == [("PE3", True, None), ("PE2", True, None), ("PE1", True, None)]


async def test_saga_isolates_failures_and_continues():
    executor, calls = _make_executor(fail={"PE2"})
    saga = Saga(executor=executor)
    for host in ("PE1", "PE2", "PE3"):
        saga.add_compensation(host, object(), host, policies.COMPENSATION)
    outcomes = await saga.compensate()
    assert calls == ["PE3", "PE2", "PE1"], "a failing compensation must not stop the remaining ones"
    by_label = {label: (ok, err) for label, ok, err in outcomes}
    assert by_label["PE3"] == (True, None)
    assert by_label["PE1"] == (True, None)
    assert by_label["PE2"][0] is False
    assert "exploded" in by_label["PE2"][1]


async def test_saga_compensate_is_single_shot():
    executor, calls = _make_executor()
    saga = Saga(executor=executor)
    saga.add_compensation("PE1", object(), "PE1", policies.COMPENSATION)
    assert len(saga) == 1
    assert await saga.compensate() == [("PE1", True, None)]
    assert await saga.compensate() == []
    assert calls == ["PE1"]
    with pytest.raises(RuntimeError):
        saga.add_compensation("PE2", object(), "PE2", policies.COMPENSATION)


async def test_saga_empty_is_noop():
    executor, calls = _make_executor()
    assert await Saga(executor=executor).compensate() == []
    assert calls == []


# ---------------------------------------------------------------------------
# Policies
# ---------------------------------------------------------------------------


def test_validation_policy():
    p = policies.VALIDATION
    assert p.start_to_close_timeout == timedelta(seconds=30)
    assert p.max_attempts == 3
    assert "ValidationError" in p.retry_policy.non_retryable_error_types


def test_allocation_policy():
    p = policies.ALLOCATION
    assert p.start_to_close_timeout == timedelta(seconds=30)
    assert p.max_attempts == 3
    assert set(p.retry_policy.non_retryable_error_types) >= {"ResourcePoolExhaustedError", "ValidationError"}


def test_config_generation_policy():
    assert policies.CONFIG_GENERATION.start_to_close_timeout == timedelta(seconds=60)


def test_device_apply_policy():
    p = policies.DEVICE_APPLY
    assert p.start_to_close_timeout == timedelta(minutes=5)
    assert p.heartbeat_timeout == timedelta(seconds=30)
    rp = p.retry_policy
    assert rp.initial_interval == timedelta(seconds=2)
    assert rp.backoff_coefficient == 2.0
    assert rp.maximum_interval == timedelta(seconds=60)
    assert rp.maximum_attempts == 5
    assert set(rp.non_retryable_error_types) >= {"ConfigurationRejectedError", "PartialApplyError"}
    assert p.cancellation_type == ActivityCancellationType.WAIT_CANCELLATION_COMPLETED


def test_verification_state_and_compensation_policies():
    assert policies.VERIFICATION.start_to_close_timeout == timedelta(minutes=2)
    assert policies.VERIFICATION.max_attempts == 3
    assert policies.STATE_TRANSITION.start_to_close_timeout == timedelta(seconds=15)
    assert policies.STATE_TRANSITION.max_attempts == 5
    assert policies.COMPENSATION.start_to_close_timeout == timedelta(minutes=5)
    assert policies.COMPENSATION.max_attempts == 5


def test_options_are_complete_for_execute_activity():
    for p in policies.ALL_POLICIES:
        opts = p.options()
        assert opts["start_to_close_timeout"] == p.start_to_close_timeout
        assert opts["retry_policy"] is p.retry_policy
        assert "cancellation_type" in opts
        assert ("heartbeat_timeout" in opts) == (p.heartbeat_timeout is not None)
