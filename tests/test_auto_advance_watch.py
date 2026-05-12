from __future__ import annotations

from pathlib import Path
import re

import pytest

from svztagent.core.errors import ConfigError
from svztagent.core.manifest import read_manifest, write_manifest
from svztagent.hpc.fake import FakeFileTransferAdapter, FakeRemoteExecAdapter, FakeSchedulerAdapter
from svztagent.hpc.interfaces import CommandResult, ExecutionMode, SchedulerStatusResult, SubmitResult, SyncDirection
from svztagent.workflows.tune_trees import run_tune_trees, watch_and_auto_advance_tuning


def _status_result(job_id: str, raw_state: str, source: str = "squeue") -> SchedulerStatusResult:
    argv = ["squeue"] if source == "squeue" else ["sacct"]
    return SchedulerStatusResult(
        job_id=job_id,
        raw_state=raw_state,
        source=source,
        command=CommandResult(
            argv=argv,
            returncode=0,
            stdout=raw_state,
            stderr="",
            dry_run=False,
        ),
    )


def _create_submitted_run(sample_config_files, run_id: str, scheduler: FakeSchedulerAdapter) -> str:
    job_id = "991100"
    scheduler.set_submit_result(
        SubmitResult(
            job_id=job_id,
            command=CommandResult(
                argv=["sbatch"],
                returncode=0,
                stdout=job_id,
                stderr="",
                dry_run=False,
            ),
        )
    )
    run_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id=run_id,
        mode=ExecutionMode.EXECUTE,
        transfer_adapter=FakeFileTransferAdapter(),
        scheduler_adapter=scheduler,
        remote_exec_adapter=FakeRemoteExecAdapter(),
    )
    return job_id


class DecisionProducingTransfer(FakeFileTransferAdapter):
    def __init__(self, decisions_by_iteration: dict[int, str]):
        super().__init__()
        self.decisions_by_iteration = decisions_by_iteration

    def sync(
        self,
        local_dir: str,
        remote_dir: str,
        include: list[str] | None = None,
        exclude: list[str] | None = None,
        direction: SyncDirection = SyncDirection.PUSH,
    ) -> CommandResult:
        result = super().sync(
            local_dir=local_dir,
            remote_dir=remote_dir,
            include=include,
            exclude=exclude,
            direction=direction,
        )
        if direction == SyncDirection.PULL:
            match = re.search(r"iter-(\d+)", remote_dir)
            if match:
                iteration = int(match.group(1))
                decision = self.decisions_by_iteration.get(iteration)
                if decision is not None:
                    local_results = Path(local_dir)
                    local_results.mkdir(parents=True, exist_ok=True)
                    (local_results / "iteration_decision.json").write_text(
                        f"decision: {decision}\n",
                        encoding="utf-8",
                    )
                    (local_results / "iteration_metrics.json").write_text(
                        "mpa_mean: 25.0\n",
                        encoding="utf-8",
                    )
        return result


def test_auto_advance_watch_converges(sample_config_files):
    run_id = "run-auto-watch-001"
    scheduler = FakeSchedulerAdapter()
    job_id = _create_submitted_run(sample_config_files, run_id, scheduler)
    transfer = DecisionProducingTransfer({1: "not_close", 2: "converged"})
    scheduler.queue_status_results(
        [
            _status_result(job_id, "RUNNING"),
            _status_result(job_id, "COMPLETED"),
            _status_result(job_id, "RUNNING"),
            _status_result(job_id, "COMPLETED"),
        ]
    )

    result = watch_and_auto_advance_tuning(
        workspace_root=sample_config_files,
        run_id=run_id,
        scheduler_adapter=scheduler,
        transfer_adapter=transfer,
        remote_exec_adapter=FakeRemoteExecAdapter(),
        poll_interval_seconds=5,
        max_polls=10,
    )

    assert result.final_action == "converged"
    assert result.tracker_status == "converged"
    assert [record.iteration for record in result.iterations] == [1, 2]
    assert result.iterations[0].advance_action == "advanced_and_submitted"
    assert result.iterations[1].advance_action == "already_converged"
    assert len(scheduler.submit_calls) == 2
    pull_includes = [
        include
        for _, _, include, _, direction in transfer.sync_calls
        if direction == SyncDirection.PULL
    ]
    assert pull_includes
    assert all("full_pa_zerod.json" in include for include in pull_includes)
    assert all("simplified_zerod_tuned_RRI.json" in include for include in pull_includes)


def test_auto_advance_watch_stops_on_scheduler_failure(sample_config_files):
    run_id = "run-auto-watch-002"
    scheduler = FakeSchedulerAdapter()
    job_id = _create_submitted_run(sample_config_files, run_id, scheduler)
    scheduler.queue_status_results(
        [
            _status_result(job_id, "RUNNING"),
            _status_result(job_id, "FAILED"),
        ]
    )

    result = watch_and_auto_advance_tuning(
        workspace_root=sample_config_files,
        run_id=run_id,
        scheduler_adapter=scheduler,
        transfer_adapter=FakeFileTransferAdapter(),
        remote_exec_adapter=FakeRemoteExecAdapter(),
        poll_interval_seconds=5,
        max_polls=10,
    )

    assert result.final_action == "scheduler_terminal_failure"
    assert result.iterations[0].advance_action == "scheduler_terminal_failure"
    assert len(scheduler.submit_calls) == 1


def test_auto_advance_watch_stops_at_max_iter_failed(sample_config_files):
    run_id = "run-auto-watch-003"
    scheduler = FakeSchedulerAdapter()
    job_id = _create_submitted_run(sample_config_files, run_id, scheduler)
    manifest_path = sample_config_files / "runs" / run_id / "manifest.yaml"
    manifest = read_manifest(manifest_path)
    manifest.tuning_iteration_tracker.max_iterations = 1
    write_manifest(manifest, manifest_path)

    scheduler.queue_status_results(
        [
            _status_result(job_id, "RUNNING"),
            _status_result(job_id, "COMPLETED"),
        ]
    )

    result = watch_and_auto_advance_tuning(
        workspace_root=sample_config_files,
        run_id=run_id,
        scheduler_adapter=scheduler,
        transfer_adapter=DecisionProducingTransfer({1: "not_close"}),
        remote_exec_adapter=FakeRemoteExecAdapter(),
        poll_interval_seconds=5,
        max_polls=10,
    )

    assert result.final_action == "max_iter_failed"
    assert result.tracker_status == "failed_max_iter"
    assert result.iterations[0].advance_action == "max_iter_failed"
    assert len(scheduler.submit_calls) == 1


def test_auto_advance_watch_pauses_on_needs_review(sample_config_files):
    run_id = "run-auto-watch-needs-review"
    scheduler = FakeSchedulerAdapter()
    job_id = _create_submitted_run(sample_config_files, run_id, scheduler)
    scheduler.queue_status_results(
        [
            _status_result(job_id, "RUNNING"),
            _status_result(job_id, "COMPLETED"),
        ]
    )

    result = watch_and_auto_advance_tuning(
        workspace_root=sample_config_files,
        run_id=run_id,
        scheduler_adapter=scheduler,
        transfer_adapter=DecisionProducingTransfer({1: "needs_review"}),
        remote_exec_adapter=FakeRemoteExecAdapter(),
        poll_interval_seconds=5,
        max_polls=10,
    )

    assert result.final_action == "needs_review_pause"
    assert result.tracker_status == "paused_review"
    assert result.iterations[0].decision == "needs_review"
    assert result.iterations[0].advance_action == "paused_needs_review"
    assert len(scheduler.submit_calls) == 1


def test_auto_advance_watch_missing_decision_artifact_raises(sample_config_files):
    run_id = "run-auto-watch-004"
    scheduler = FakeSchedulerAdapter()
    job_id = _create_submitted_run(sample_config_files, run_id, scheduler)
    scheduler.queue_status_results(
        [
            _status_result(job_id, "RUNNING"),
            _status_result(job_id, "COMPLETED"),
        ]
    )

    with pytest.raises(ConfigError, match="decision artifact missing after pull"):
        watch_and_auto_advance_tuning(
            workspace_root=sample_config_files,
            run_id=run_id,
            scheduler_adapter=scheduler,
            transfer_adapter=FakeFileTransferAdapter(),
            remote_exec_adapter=FakeRemoteExecAdapter(),
            poll_interval_seconds=5,
            max_polls=10,
        )
