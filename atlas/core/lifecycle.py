"""Service lifecycle state machine — the single source of truth for allowed transitions."""

from __future__ import annotations

from atlas.core.enums import LifecycleStatus as S

ALLOWED_TRANSITIONS: dict[S, frozenset[S]] = {
    S.REQUESTED: frozenset({S.VALIDATING, S.FAILED}),
    S.VALIDATING: frozenset({S.ALLOCATING, S.FAILED}),
    S.ALLOCATING: frozenset({S.PROVISIONING, S.FAILED, S.ROLLING_BACK}),
    S.PROVISIONING: frozenset({S.VERIFYING, S.FAILED, S.ROLLING_BACK, S.MANUAL_INTERVENTION_REQUIRED}),
    S.VERIFYING: frozenset({S.ACTIVE, S.FAILED, S.ROLLING_BACK, S.MANUAL_INTERVENTION_REQUIRED}),
    S.ACTIVE: frozenset({S.DECOMMISSIONING, S.MANUAL_INTERVENTION_REQUIRED, S.VERIFYING}),
    S.ROLLING_BACK: frozenset({S.ROLLED_BACK, S.MANUAL_INTERVENTION_REQUIRED}),
    S.ROLLED_BACK: frozenset({S.DECOMMISSIONED}),
    S.FAILED: frozenset({S.ROLLING_BACK, S.MANUAL_INTERVENTION_REQUIRED, S.DECOMMISSIONED}),
    S.MANUAL_INTERVENTION_REQUIRED: frozenset({S.VERIFYING, S.ROLLING_BACK, S.DECOMMISSIONING, S.ACTIVE}),
    S.DECOMMISSIONING: frozenset({S.DECOMMISSIONED, S.MANUAL_INTERVENTION_REQUIRED}),
    S.DECOMMISSIONED: frozenset(),
}

TERMINAL_STATES: frozenset[S] = frozenset({S.DECOMMISSIONED})
IN_FLIGHT_STATES: frozenset[S] = frozenset(
    {S.REQUESTED, S.VALIDATING, S.ALLOCATING, S.PROVISIONING, S.VERIFYING, S.ROLLING_BACK, S.DECOMMISSIONING}
)


class InvalidTransitionError(ValueError):
    def __init__(self, current: S, target: S) -> None:
        super().__init__(f"Illegal lifecycle transition {current} -> {target}")
        self.current = current
        self.target = target


def can_transition(current: S, target: S) -> bool:
    return target in ALLOWED_TRANSITIONS.get(current, frozenset())


def assert_transition(current: S, target: S) -> None:
    if not can_transition(current, target):
        raise InvalidTransitionError(current, target)
