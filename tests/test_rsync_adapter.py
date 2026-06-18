from __future__ import annotations

import pytest

from svztagent.core.errors import PathPolicyError
from svztagent.hpc.executor import CommandExecutor
from svztagent.hpc.interfaces import ExecutionMode, SyncDirection
from svztagent.hpc.rsync import RsyncFileTransferAdapter
from svztagent.hpc.ssh import SshRemoteExecAdapter


def _build_adapter() -> RsyncFileTransferAdapter:
    executor = CommandExecutor(mode=ExecutionMode.DRY_RUN)
    remote = SshRemoteExecAdapter(
        user="ndorn",
        host="sherlock.stanford.edu",
        executor=executor,
    )
    return RsyncFileTransferAdapter(
        user="ndorn",
        host="sherlock.stanford.edu",
        runs_root="/scratch/users/ndorn/svzt_runs",
        permanent_data_root="/oak/stanford/groups/amarsden/ndorn/PPAS-study/tof-stent",
        remote_exec=remote,
        executor=executor,
        default_include=["*.yaml"],
        default_exclude=["*.tmp"],
    )


def test_rsync_push_command_construction():
    adapter = _build_adapter()
    result = adapter.push(
        local_path="/tmp/staged_inputs",
        remote_path="/scratch/users/ndorn/svzt_runs/run-001/inputs",
    )
    assert result.argv[0] == "rsync"
    assert "--dry-run" in result.argv
    assert result.argv[-1] == "ndorn@sherlock.stanford.edu:/scratch/users/ndorn/svzt_runs/run-001/inputs"


def test_rsync_push_file_does_not_append_trailing_slash():
    adapter = _build_adapter()
    result = adapter.push(
        local_path="/tmp/run_tune_iter.sh",
        remote_path="/scratch/users/ndorn/svzt_runs/run-001/iterations/iter-01/run_tune_iter.sh",
    )
    assert result.argv[0] == "rsync"
    assert result.argv[-2] == "/tmp/run_tune_iter.sh"
    assert not result.argv[-2].endswith("/")


def test_rsync_sync_pull_command_construction():
    adapter = _build_adapter()
    result = adapter.sync(
        local_dir="/tmp/pulled",
        remote_dir="/scratch/users/ndorn/svzt_runs/run-001",
        include=["logs/**", "results/**"],
        exclude=["*"],
        direction=SyncDirection.PULL,
    )
    assert result.argv[0] == "rsync"
    assert "--include" in result.argv
    assert "--exclude" in result.argv
    assert result.argv[-1] == "/tmp/pulled/"


def test_rsync_pull_rejects_non_durable_patient_data_read_path():
    adapter = _build_adapter()
    with pytest.raises(PathPolicyError):
        adapter.pull(
            remote_path="/scratch/users/ndorn/models/PPAS/tof-stent/TST-STAN-x/simplified_nonlinear_zerod.json",
            local_path="/tmp/pulled",
        )


def test_rsync_pull_allows_permanent_data_read_path():
    adapter = _build_adapter()
    result = adapter.pull(
        remote_path="/oak/stanford/groups/amarsden/ndorn/PPAS-study/tof-stent/TST-STAN-x/clinical_targets.csv",
        local_path="/tmp/pulled",
    )
    assert result.argv[0] == "rsync"
    assert result.argv[-1] == "/tmp/pulled/"


def test_rsync_rejects_remote_write_outside_runs_root():
    adapter = _build_adapter()
    with pytest.raises(PathPolicyError):
        adapter.push(
            local_path="/tmp/staged_inputs",
            remote_path="/scratch/users/ndorn/models/PPAS/tof-stent/TST-STAN-x",
        )
