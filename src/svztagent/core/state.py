"""Canonical internal run lifecycle states."""

from __future__ import annotations

from enum import Enum


class RunLifecycleState(str, Enum):
    INITIALIZED = "initialized"
    PLANNED = "planned"
    STAGED = "staged"
    SUBMITTED = "submitted"
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    FETCHED = "fetched"
    POSTPROCESSED = "postprocessed"
    UNKNOWN = "unknown"


TERMINAL_STATES = frozenset(
    {
        RunLifecycleState.COMPLETED,
        RunLifecycleState.FAILED,
        RunLifecycleState.CANCELLED,
        RunLifecycleState.FETCHED,
        RunLifecycleState.POSTPROCESSED,
    }
)

ACTIVE_STATES = frozenset(
    {
        RunLifecycleState.INITIALIZED,
        RunLifecycleState.PLANNED,
        RunLifecycleState.STAGED,
        RunLifecycleState.SUBMITTED,
        RunLifecycleState.PENDING,
        RunLifecycleState.RUNNING,
        RunLifecycleState.UNKNOWN,
    }
)


def coerce_run_lifecycle_state(
    value: RunLifecycleState | str | None,
    *,
    default: RunLifecycleState = RunLifecycleState.UNKNOWN,
) -> RunLifecycleState:
    if value is None:
        return default
    if isinstance(value, RunLifecycleState):
        return value
    cleaned = value.strip().lower()
    if not cleaned:
        return default
    try:
        return RunLifecycleState(cleaned)
    except ValueError:
        return default


def is_terminal_state(value: RunLifecycleState | str | None) -> bool:
    return coerce_run_lifecycle_state(value) in TERMINAL_STATES
