from __future__ import annotations

from svztagent.core.manifest import read_manifest
from svztagent.hpc.fake import FakeFileTransferAdapter, FakeRemoteExecAdapter, FakeSchedulerAdapter
from svztagent.hpc.interfaces import CommandResult, ExecutionMode, SubmitResult
from svztagent.workflows.tune_trees import run_tune_trees


def test_run_tune_with_fake_adapters_executes_submission(sample_config_files):
    fake_transfer = FakeFileTransferAdapter()
    fake_scheduler = FakeSchedulerAdapter()
    fake_remote = FakeRemoteExecAdapter()
    fake_scheduler.set_submit_result(
        SubmitResult(
            job_id="555123",
            command=CommandResult(
                argv=[
                    "sbatch",
                    "--parsable",
                    "/scratch/users/ndorn/svzt_runs/run-fake-001/iterations/iter-01/run_tune_iter.sh",
                ],
                returncode=0,
                stdout="555123",
                stderr="",
                dry_run=False,
            ),
        )
    )

    result = run_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-fake-001",
        mode=ExecutionMode.EXECUTE,
        transfer_adapter=fake_transfer,
        scheduler_adapter=fake_scheduler,
        remote_exec_adapter=fake_remote,
    )

    assert result.submitted_job_id == "555123"
    assert result.iteration == 1
    assert len(fake_transfer.ensure_calls) == 5
    assert len(fake_transfer.sync_calls) == 1
    assert len(fake_transfer.push_calls) == 1

    manifest = read_manifest(sample_config_files / "runs" / "run-fake-001" / "manifest.yaml")
    assert manifest.status == "submitted"
    assert manifest.execution.submitted_job_id == "555123"
    assert manifest.jobs[0]["mode"] == "execute"
    assert manifest.tuning_iteration_tracker.current_iteration == 1
    assert manifest.tuning_iteration_tracker.iterations[0].tune_job_id == "555123"
