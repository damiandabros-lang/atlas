"""Explicit Temporal retry policies and timeouts, one per activity class.

Every ``workflow.execute_activity`` call in ATLAS passes ``**<POLICY>.options()`` so that no
activity ever runs with implicit/unbounded limits (brief §7 "Activity timeouts" / "Retry
policies"). Non-retryable error *types* match the ``type`` of the ``ApplicationError`` raised
by the activity layer (``atlas.activities``), which in turn mirrors the adapter exception
class names (``ConfigurationRejectedError``, ``PartialApplyError`` ...).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from temporalio.common import RetryPolicy
from temporalio.workflow import ActivityCancellationType


@dataclass(frozen=True, slots=True)
class ActivityPolicy:
    """Bundle of the execution limits applied to one class of activities."""

    name: str
    start_to_close_timeout: timedelta
    retry_policy: RetryPolicy
    heartbeat_timeout: timedelta | None = None
    schedule_to_close_timeout: timedelta | None = None
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def max_attempts(self) -> int:
        return self.retry_policy.maximum_attempts

    def options(self) -> dict[str, Any]:
        """Keyword arguments for ``workflow.execute_activity``."""
        opts: dict[str, Any] = {
            "start_to_close_timeout": self.start_to_close_timeout,
            "retry_policy": self.retry_policy,
            "cancellation_type": self.cancellation_type,
        }
        if self.heartbeat_timeout is not None:
            opts["heartbeat_timeout"] = self.heartbeat_timeout
        if self.schedule_to_close_timeout is not None:
            opts["schedule_to_close_timeout"] = self.schedule_to_close_timeout
        opts.update(self.extra)
        return opts


def _retry(
    *,
    attempts: int,
    initial: float = 1.0,
    coefficient: float = 2.0,
    maximum: float = 30.0,
    non_retryable: tuple[str, ...] = (),
) -> RetryPolicy:
    return RetryPolicy(
        initial_interval=timedelta(seconds=initial),
        backoff_coefficient=coefficient,
        maximum_interval=timedelta(seconds=maximum),
        maximum_attempts=attempts,
        non_retryable_error_types=list(non_retryable),
    )


# ---------------------------------------------------------------------------
# Policies
# ---------------------------------------------------------------------------

VALIDATION = ActivityPolicy(
    name="validation",
    start_to_close_timeout=timedelta(seconds=30),
    retry_policy=_retry(attempts=3, non_retryable=("ValidationError",)),
)
"""Request / pre-deployment validation: a semantic failure is final, only infra errors retry."""

ALLOCATION = ActivityPolicy(
    name="allocation",
    start_to_close_timeout=timedelta(seconds=30),
    retry_policy=_retry(attempts=3, non_retryable=("ResourcePoolExhaustedError", "ValidationError")),
)
"""Resource allocation is idempotent (allocator returns existing rows), so retries are safe."""

CONFIG_GENERATION = ActivityPolicy(
    name="config_generation",
    start_to_close_timeout=timedelta(seconds=60),
    retry_policy=_retry(attempts=3, non_retryable=("ValidationError",)),
)
"""Template rendering + persistence of rendered config / checksum."""

DEVICE_APPLY = ActivityPolicy(
    name="device_apply",
    start_to_close_timeout=timedelta(minutes=5),
    heartbeat_timeout=timedelta(seconds=30),
    retry_policy=_retry(
        attempts=5,
        initial=2.0,
        coefficient=2.0,
        maximum=60.0,
        non_retryable=("ConfigurationRejectedError", "PartialApplyError"),
    ),
    # The activity finishes (or records) the in-flight device write before acknowledging a
    # cancel; the workflow must wait for that acknowledgement before compensating.
    cancellation_type=ActivityCancellationType.WAIT_CANCELLATION_COMPLETED,
)
"""Device configuration push. TRANSIENT adapter errors retry with exponential backoff;
PERMANENT (rejected) and UNKNOWN (partial) outcomes are final and handled by the workflow."""

VERIFICATION = ActivityPolicy(
    name="verification",
    start_to_close_timeout=timedelta(minutes=2),
    retry_policy=_retry(attempts=3, initial=5.0, maximum=30.0, non_retryable=("ValidationError",)),
)
"""Operational-state verification (VRF/BGP/interface) — BGP convergence may need a retry."""

STATE_TRANSITION = ActivityPolicy(
    name="state_transition",
    start_to_close_timeout=timedelta(seconds=15),
    retry_policy=_retry(
        attempts=5, initial=0.5, maximum=10.0, non_retryable=("InvalidTransitionError", "LookupError")
    ),
)
"""PostgreSQL bookkeeping (lifecycle transitions, workflow ledger, audit). Cheap, idempotent."""

COMPENSATION = ActivityPolicy(
    name="compensation",
    start_to_close_timeout=timedelta(minutes=5),
    heartbeat_timeout=timedelta(seconds=30),
    retry_policy=_retry(
        attempts=5, initial=2.0, coefficient=2.0, maximum=60.0, non_retryable=("ConfigurationRejectedError",)
    ),
    cancellation_type=ActivityCancellationType.WAIT_CANCELLATION_COMPLETED,
)
"""Saga compensation (rollback / decommission). Idempotent by ledger, so aggressive retries."""

ALL_POLICIES: tuple[ActivityPolicy, ...] = (
    VALIDATION,
    ALLOCATION,
    CONFIG_GENERATION,
    DEVICE_APPLY,
    VERIFICATION,
    STATE_TRANSITION,
    COMPENSATION,
)

__all__ = [
    "ALLOCATION",
    "ALL_POLICIES",
    "COMPENSATION",
    "CONFIG_GENERATION",
    "DEVICE_APPLY",
    "STATE_TRANSITION",
    "VALIDATION",
    "VERIFICATION",
    "ActivityPolicy",
]
