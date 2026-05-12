from __future__ import annotations

import pytest

from svztagent.core.errors import PathPolicyError
from svztagent.core.paths import (
    validate_remote_patient_read_path,
    validate_remote_write_path,
    validate_run_id,
)


def test_remote_write_must_stay_in_runs_root():
    with pytest.raises(PathPolicyError):
        validate_remote_write_path(
            "/scratch/users/ndorn/models/PPAS/tof-stent/TST-STAN-x",
            "/scratch/users/ndorn/svzt_runs",
        )


def test_patient_root_write_rejected():
    with pytest.raises(PathPolicyError):
        validate_remote_patient_read_path(
            "/scratch/users/ndorn/svzt_runs/run-001",
            "/scratch/users/ndorn/models/PPAS/tof-stent",
        )


def test_invalid_run_id_rejected():
    with pytest.raises(PathPolicyError):
        validate_run_id("../bad")

    with pytest.raises(PathPolicyError):
        validate_run_id("bad/name")
