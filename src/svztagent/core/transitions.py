"""Run lifecycle transition rules."""

from __future__ import annotations

from svztagent.core.errors import InvalidStateTransitionError
from svztagent.core.state import RunLifecycleState, coerce_run_lifecycle_state


ALLOWED_TRANSITIONS: dict[RunLifecycleState, frozenset[RunLifecycleState]] = {
    RunLifecycleState.INITIALIZED: frozenset(
        {
            RunLifecycleState.PLANNED,
            RunLifecycleState.STAGED,
            RunLifecycleState.SUBMITTED,
            RunLifecycleState.FAILED,
            RunLifecycleState.CANCELLED,
            RunLifecycleState.UNKNOWN,
        }
    ),
    RunLifecycleState.PLANNED: frozenset(
        {
            RunLifecycleState.STAGED,
            RunLifecycleState.SUBMITTED,
            RunLifecycleState.FAILED,
            RunLifecycleState.CANCELLED,
            RunLifecycleState.UNKNOWN,
        }
    ),
    RunLifecycleState.STAGED: frozenset(
        {
            RunLifecycleState.SUBMITTED,
            RunLifecycleState.FAILED,
            RunLifecycleState.CANCELLED,
            RunLifecycleState.UNKNOWN,
        }
    ),
    RunLifecycleState.SUBMITTED: frozenset(
        {
            RunLifecycleState.PENDING,
            RunLifecycleState.RUNNING,
            RunLifecycleState.COMPLETED,
            RunLifecycleState.FAILED,
            RunLifecycleState.CANCELLED,
            RunLifecycleState.UNKNOWN,
        }
    ),
    RunLifecycleState.PENDING: frozenset(
        {
            RunLifecycleState.RUNNING,
            RunLifecycleState.COMPLETED,
            RunLifecycleState.FAILED,
            RunLifecycleState.CANCELLED,
            RunLifecycleState.UNKNOWN,
        }
    ),
    RunLifecycleState.RUNNING: frozenset(
        {
            RunLifecycleState.COMPLETED,
            RunLifecycleState.FAILED,
            RunLifecycleState.CANCELLED,
            RunLifecycleState.UNKNOWN,
        }
    ),
    RunLifecycleState.UNKNOWN: frozenset(
        {
            RunLifecycleState.PENDING,
            RunLifecycleState.RUNNING,
            RunLifecycleState.COMPLETED,
            RunLifecycleState.FAILED,
            RunLifecycleState.CANCELLED,
            RunLifecycleState.UNKNOWN,
        }
    ),
    RunLifecycleState.COMPLETED: frozenset(
        {
            RunLifecycleState.SUBMITTED,
            RunLifecycleState.FETCHED,
            RunLifecycleState.POSTPROCESSED,
        }
    ),
    RunLifecycleState.FAILED: frozenset(
        {
            RunLifecycleState.SUBMITTED,
            RunLifecycleState.FETCHED,
            RunLifecycleState.POSTPROCESSED,
        }
    ),
    RunLifecycleState.CANCELLED: frozenset(
        {
            RunLifecycleState.SUBMITTED,
            RunLifecycleState.FETCHED,
            RunLifecycleState.POSTPROCESSED,
        }
    ),
    RunLifecycleState.FETCHED: frozenset(
        {
            RunLifecycleState.SUBMITTED,
            RunLifecycleState.POSTPROCESSED,
        }
    ),
    RunLifecycleState.POSTPROCESSED: frozenset({RunLifecycleState.SUBMITTED}),
}


def can_transition(
    from_state: RunLifecycleState | str | None,
    to_state: RunLifecycleState | str | None,
) -> bool:
    source = coerce_run_lifecycle_state(from_state)
    target = coerce_run_lifecycle_state(to_state)
    if source == target:
        return True
    return target in ALLOWED_TRANSITIONS[source]


def validate_transition(
    from_state: RunLifecycleState | str | None,
    to_state: RunLifecycleState | str | None,
) -> None:
    source = coerce_run_lifecycle_state(from_state)
    target = coerce_run_lifecycle_state(to_state)
    if source == target:
        return
    if target in ALLOWED_TRANSITIONS[source]:
        return

    allowed = ", ".join(sorted(state.value for state in ALLOWED_TRANSITIONS[source])) or "<none>"
    raise InvalidStateTransitionError(
        f"invalid lifecycle transition '{source.value}' -> '{target.value}'; "
        f"allowed next states: {allowed}"
    )


def transition_or_noop(
    from_state: RunLifecycleState | str | None,
    to_state: RunLifecycleState | str | None,
) -> bool:
    source = coerce_run_lifecycle_state(from_state)
    target = coerce_run_lifecycle_state(to_state)
    if source == target:
        return False
    validate_transition(source, target)
    return True
