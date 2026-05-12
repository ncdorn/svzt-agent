from __future__ import annotations

import pytest

from svztagent.core.state import RunLifecycleState
from svztagent.core.status import normalize_slurm_state, terminal_reason_for_slurm_state


@pytest.mark.parametrize(
    ("raw_state", "expected"),
    [
        ("PENDING", RunLifecycleState.PENDING),
        ("RUNNING", RunLifecycleState.RUNNING),
        ("COMPLETED", RunLifecycleState.COMPLETED),
        ("FAILED", RunLifecycleState.FAILED),
        ("CANCELLED by 123", RunLifecycleState.CANCELLED),
        ("TIMEOUT", RunLifecycleState.FAILED),
        ("PREEMPTED", RunLifecycleState.FAILED),
        ("NODE_FAIL", RunLifecycleState.FAILED),
        ("OUT_OF_MEMORY", RunLifecycleState.FAILED),
        (None, RunLifecycleState.UNKNOWN),
        ("", RunLifecycleState.UNKNOWN),
        ("mystery", RunLifecycleState.UNKNOWN),
    ],
)
def test_normalize_slurm_state(raw_state: str | None, expected: RunLifecycleState):
    assert normalize_slurm_state(raw_state) == expected


def test_terminal_reason_for_slurm_state():
    assert terminal_reason_for_slurm_state("OUT_OF_MEMORY") == "out_of_memory"
    assert terminal_reason_for_slurm_state("TIMEOUT") == "timeout"
    assert terminal_reason_for_slurm_state("CANCELLED by 123") == "cancelled"
    assert terminal_reason_for_slurm_state("PENDING") is None
