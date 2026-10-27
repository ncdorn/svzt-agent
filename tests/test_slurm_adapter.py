from __future__ import annotations

from pathlib import Path

import pytest

import svztagent
from svztagent.config.models import SchedulerDefaults
from svztagent.core.errors import SchedulerResponseError
from svztagent.hpc.fake import FakeRemoteExecAdapter
from svztagent.hpc.interfaces import CommandResult
from svztagent.hpc.slurm import (
    SlurmSchedulerAdapter,
    SlurmSubmitOptions,
    build_sbatch_command,
    parse_sbatch_job_id,
)


def _scheduler(remote: FakeRemoteExecAdapter) -> SlurmSchedulerAdapter:
    return SlurmSchedulerAdapter(
        remote_exec=remote,
        runs_root="/scratch/users/ndorn/svzt_runs",
        submit_options=SlurmSubmitOptions(
            job_name="run-001",
            account="acct",
            partition="normal",
            wall_time="01:00:00",
            mem="8G",
            cpus="4",
        ),
    )


def test_slurm_submit_command_and_parse():
    remote = FakeRemoteExecAdapter()
    remote.queue_response(
        CommandResult(
            argv=["sbatch"],
            returncode=0,
            stdout="12345",
            stderr="",
            dry_run=False,
        )
    )
    scheduler = _scheduler(remote)
    result = scheduler.submit("/scratch/users/ndorn/svzt_runs/run-001/run_tune_trees.sh")
    assert result.job_id == "12345"
    submit_call = remote.calls[0][0]
    assert submit_call[0] == "sbatch"
    assert "--parsable" in submit_call
    assert "--job-name" in submit_call


def test_slurm_status_and_accounting_command_construction():
    remote = FakeRemoteExecAdapter()
    remote.queue_response(
        CommandResult(
            argv=["squeue"],
            returncode=0,
            stdout="RUNNING",
            stderr="",
            dry_run=False,
        )
    )
    remote.queue_response(
        CommandResult(
            argv=["sacct"],
            returncode=0,
            stdout="COMPLETED|",
            stderr="",
            dry_run=False,
        )
    )
    scheduler = _scheduler(remote)
    status = scheduler.status("12345")
    accounting = scheduler.accounting("12345")
    assert status.raw_state == "RUNNING"
    assert accounting.raw_state == "COMPLETED"
    assert remote.calls[0][0][0] == "squeue"
    assert remote.calls[1][0][0] == "sacct"


def test_sbatch_command_passes_explicit_node_count():
    # TACC systems (Stampede3) reject submissions without -N.
    cmd = build_sbatch_command(
        "/scratch/run-001/run_tune_iter.sh",
        SlurmSubmitOptions(job_name="run-001", partition="skx", nodes="1", cpus="4"),
    )
    assert cmd[cmd.index("--nodes") + 1] == "1"
    assert cmd[-1] == "/scratch/run-001/run_tune_iter.sh"


def test_sbatch_command_omits_node_count_by_default():
    cmd = build_sbatch_command(
        "/scratch/run-001/run_tune_iter.sh",
        SlurmSubmitOptions(job_name="run-001", partition="normal"),
    )
    assert "--nodes" not in cmd


def test_scheduler_defaults_nodes_is_optional():
    assert SchedulerDefaults().nodes is None
    assert SchedulerDefaults(nodes="1").nodes == "1"


# Abridged stdout of `sbatch --parsable` on Stampede3: TACC's wrapper prints a
# banner and its filter checks before the job id.
TACC_SBATCH_STDOUT = """
-----------------------------------------------------------------
          Welcome to the Stampede3 Supercomputer
-----------------------------------------------------------------

--> Verifying valid submit host (login3)...OK
--> Checking available allocation (TG-PHY260317)...OK
3623254
"""


def test_parse_sbatch_job_id_skips_tacc_banner():
    assert parse_sbatch_job_id(TACC_SBATCH_STDOUT) == "3623254"
    assert parse_sbatch_job_id("12345;sherlock\n") == "12345"


def test_parse_sbatch_job_id_rejects_output_without_job_id():
    with pytest.raises(SchedulerResponseError):
        parse_sbatch_job_id("-----\nWelcome\n")

