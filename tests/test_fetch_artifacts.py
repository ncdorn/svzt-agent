from __future__ import annotations

from svztagent.core.manifest import read_manifest
from svztagent.hpc.fake import FakeFileTransferAdapter, FakeRemoteExecAdapter, FakeSchedulerAdapter
from svztagent.hpc.interfaces import CommandResult, ExecutionMode, SubmitResult
from svztagent.workflows.tune_trees import fetch_run_artifacts, run_tune_trees


def test_fetch_uses_pull_rules_and_updates_manifest(sample_config_files):
    fake_transfer = FakeFileTransferAdapter()
    fake_scheduler = FakeSchedulerAdapter()
    fake_remote = FakeRemoteExecAdapter()
    fake_scheduler.set_submit_result(
        SubmitResult(
            job_id="889900",
            command=CommandResult(
                argv=["sbatch"],
                returncode=0,
                stdout="889900",
                stderr="",
                dry_run=False,
            ),
        )
    )

    run_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-fetch-001",
        mode=ExecutionMode.EXECUTE,
        transfer_adapter=fake_transfer,
        scheduler_adapter=fake_scheduler,
        remote_exec_adapter=fake_remote,
    )

    fetch_transfer = FakeFileTransferAdapter()
    result = fetch_run_artifacts(
        workspace_root=sample_config_files,
        run_id="run-fetch-001",
        transfer_adapter=fetch_transfer,
        mode=ExecutionMode.DRY_RUN,
    )

    assert result.run_id == "run-fetch-001"
    assert len(fetch_transfer.sync_calls) == 3
    sync_manifest, sync_logs, sync_results = fetch_transfer.sync_calls
    assert sync_manifest[4].value == "pull"
    assert sync_manifest[2] == ["manifest.yaml"]
    assert sync_logs[1].endswith("/iterations/iter-01/logs")
    assert sync_logs[4].value == "pull"
    assert sync_results[1].endswith("/iterations/iter-01/results")
    assert sync_results[4].value == "pull"

    manifest = read_manifest(sample_config_files / "runs" / "run-fetch-001" / "manifest.yaml")
    assert manifest.execution.fetch_timestamps
    assert "manifest.yaml" in manifest.execution.retrieved_artifacts
