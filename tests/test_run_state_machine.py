from __future__ import annotations

import pytest

from svztagent.core.errors import InvalidStateTransitionError
from svztagent.core.state import RunLifecycleState
from svztagent.core.transitions import transition_or_noop, validate_transition


def test_run_state_machine_allows_valid_transitions():
    validate_transition(RunLifecycleState.INITIALIZED, RunLifecycleState.PLANNED)
    validate_transition(RunLifecycleState.PLANNED, RunLifecycleState.SUBMITTED)
    validate_transition(RunLifecycleState.SUBMITTED, RunLifecycleState.PENDING)
    validate_transition(RunLifecycleState.PENDING, RunLifecycleState.RUNNING)
    validate_transition(RunLifecycleState.RUNNING, RunLifecycleState.COMPLETED)
    validate_transition(RunLifecycleState.COMPLETED, RunLifecycleState.FETCHED)
    validate_transition(RunLifecycleState.COMPLETED, RunLifecycleState.SUBMITTED)
    validate_transition(RunLifecycleState.FETCHED, RunLifecycleState.SUBMITTED)


def test_run_state_machine_rejects_invalid_transition():
    with pytest.raises(InvalidStateTransitionError, match="invalid lifecycle transition"):
        validate_transition(RunLifecycleState.COMPLETED, RunLifecycleState.RUNNING)


def test_transition_or_noop_returns_false_for_same_state():
    changed = transition_or_noop(RunLifecycleState.RUNNING, RunLifecycleState.RUNNING)
    assert changed is False
