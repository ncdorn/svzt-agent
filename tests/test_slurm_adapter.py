from __future__ import annotations

from svztagent.hpc.fake import FakeRemoteExecAdapter
from svztagent.hpc.interfaces import CommandResult
from svztagent.hpc.slurm import SlurmSchedulerAdapter, SlurmSubmitOptions


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
