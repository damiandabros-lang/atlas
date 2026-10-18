"""Deterministic Saga helper for Temporal workflows.

A ``Saga`` records compensating actions as forward steps succeed and, on failure, runs them in
**reverse order**, isolating each failure so one broken compensation does not prevent the
others from running. The helper itself performs no I/O: every compensation is a Temporal
activity executed through ``workflow.execute_activity`` (or an injected executor in unit
tests), so it is safe inside workflow code and under replay.

Compensations must be idempotent and scoped to the resources the forward step created —
the activity layer (``atlas.activities.device_ops.rollback_device``) guarantees this via the
device-operation ledger.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from atlas.workflows.policies import ActivityPolicy

Executor = Callable[[Any, Any, ActivityPolicy], Awaitable[Any]]


async def _temporal_executor(activity_fn: Any, arg: Any, policy: ActivityPolicy) -> Any:
    return await workflow.execute_activity(activity_fn, arg, **policy.options())


@dataclass(slots=True)
class CompensationStep:
    label: str
    activity_fn: Any
    arg: Any
    policy: ActivityPolicy


@dataclass(slots=True)
class CompensationOutcome:
    label: str
    ok: bool
    error: str | None = None

    def as_tuple(self) -> tuple[str, bool, str | None]:
        return (self.label, self.ok, self.error)


@dataclass(slots=True)
class Saga:
    """Collects compensations; ``compensate()`` runs them LIFO and never raises."""

    executor: Executor = _temporal_executor
    _steps: list[CompensationStep] = field(default_factory=list)
    _compensated: bool = False

    def add_compensation(self, label: str, activity_fn: Any, arg: Any, policy: ActivityPolicy) -> None:
        if self._compensated:
            raise RuntimeError("cannot add a compensation after compensate() has run")
        self._steps.append(CompensationStep(label=label, activity_fn=activity_fn, arg=arg, policy=policy))

    @property
    def labels(self) -> list[str]:
        return [step.label for step in self._steps]

    def __len__(self) -> int:
        return len(self._steps)

    async def compensate(self) -> list[tuple[str, bool, str | None]]:
        """Run every registered compensation in reverse order.

        Returns ``[(label, ok, error), ...]`` in execution order (i.e. last-registered first).
        A failing compensation is recorded and the remaining ones still run. Calling this
        twice is a no-op (returns an empty list) so a workflow cannot double-compensate.
        """
        if self._compensated:
            return []
        self._compensated = True
        outcomes: list[CompensationOutcome] = []
        for step in reversed(self._steps):
            try:
                await self.executor(step.activity_fn, step.arg, step.policy)
                outcomes.append(CompensationOutcome(label=step.label, ok=True))
            except Exception as exc:  # noqa: BLE001 - isolate every compensation failure
                outcomes.append(CompensationOutcome(label=step.label, ok=False, error=_describe(exc)))
        return [o.as_tuple() for o in outcomes]


def _describe(exc: BaseException) -> str:
    """Flatten Temporal's wrapped error chain into one line (ActivityError -> ApplicationError)."""
    parts: list[str] = []
    cur: BaseException | None = exc
    seen = 0
    while cur is not None and seen < 5:
        text = str(cur).strip()
        if text and text not in parts:
            parts.append(f"{cur.__class__.__name__}: {text}")
        cur = cur.__cause__
        seen += 1
    return " <- ".join(parts) or exc.__class__.__name__


__all__ = ["CompensationOutcome", "CompensationStep", "Saga"]
