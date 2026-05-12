"""Scheduler state normalization helpers."""

from __future__ import annotations

from dataclasses import dataclass

from svztagent.core.state import RunLifecycleState


_SLURM_STATE_MAP = {
    "PENDING": RunLifecycleState.PENDING,
    "CONFIGURING": RunLifecycleState.PENDING,
    "REQUEUED": RunLifecycleState.PENDING,
    "RUNNING": RunLifecycleState.RUNNING,
    "COMPLETING": RunLifecycleState.RUNNING,
    "COMPLETED": RunLifecycleState.COMPLETED,
    "FAILED": RunLifecycleState.FAILED,
    "TIMEOUT": RunLifecycleState.FAILED,
    "OUT_OF_MEMORY": RunLifecycleState.FAILED,
    "NODE_FAIL": RunLifecycleState.FAILED,
    "PREEMPTED": RunLifecycleState.FAILED,
    "BOOT_FAIL": RunLifecycleState.FAILED,
    "DEADLINE": RunLifecycleState.FAILED,
    "CANCELLED": RunLifecycleState.CANCELLED,
    "CANCELLED+": RunLifecycleState.CANCELLED,
}

_SLURM_TERMINAL_REASON_MAP = {
    "COMPLETED": "completed",
    "FAILED": "failed",
    "TIMEOUT": "timeout",
    "OUT_OF_MEMORY": "out_of_memory",
    "NODE_FAIL": "node_fail",
    "PREEMPTED": "preempted",
    "BOOT_FAIL": "boot_fail",
    "DEADLINE": "deadline",
    "CANCELLED": "cancelled",
    "CANCELLED+": "cancelled",
}


# Backward-compatible alias used by existing tests and status callers.
NormalizedRunState = RunLifecycleState


@dataclass(frozen=True)
class SchedulerStateSnapshot:
    raw_state: str | None
    normalized_state: RunLifecycleState
    terminal_reason: str | None


def _normalize_slurm_token(raw_state: str | None) -> str | None:
    if raw_state is None:
        return None

    cleaned = raw_state.strip()
    if not cleaned:
        return None

    # Slurm may report states with trailing reason chunks, e.g. "CANCELLED by 1234".
    return cleaned.split()[0].split("+")[0].upper()


def normalize_slurm_state(raw_state: str | None) -> RunLifecycleState:
    token = _normalize_slurm_token(raw_state)
    if token is None:
        return RunLifecycleState.UNKNOWN
    return _SLURM_STATE_MAP.get(token, RunLifecycleState.UNKNOWN)


def terminal_reason_for_slurm_state(raw_state: str | None) -> str | None:
    token = _normalize_slurm_token(raw_state)
    if token is None:
        return None
    return _SLURM_TERMINAL_REASON_MAP.get(token)


def snapshot_slurm_state(raw_state: str | None) -> SchedulerStateSnapshot:
    return SchedulerStateSnapshot(
        raw_state=raw_state,
        normalized_state=normalize_slurm_state(raw_state),
        terminal_reason=terminal_reason_for_slurm_state(raw_state),
    )
