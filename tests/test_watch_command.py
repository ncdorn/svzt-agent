from __future__ import annotations

import pytest

from svztagent.cli.main import build_parser, main
from svztagent.core.errors import MissingJobIdError
from svztagent.core.state import RunLifecycleState
from svztagent.hpc.fake import FakeFileTransferAdapter, FakeRemoteExecAdapter, FakeSchedulerAdapter
from svztagent.hpc.interfaces import CommandResult, ExecutionMode, SchedulerStatusResult, SubmitResult
from svztagent.workflows.tune_trees import (
    AutoAdvanceIterationRecord,
    AutoAdvanceResult,
    StatusQueryResult,
    TuneExecutionResult,
    WatchResult,
    init_run_workspace,
    run_tune_trees,
    watch_run_lifecycle,
)


def _status_result(job_id: str, raw_state: str, source: str) -> SchedulerStatusResult:
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


def test_watch_run_lifecycle_missing_job_id_raises(sample_config_files):
    init_run_workspace(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-watch-missing-job",
    )
    scheduler = FakeSchedulerAdapter()

    with pytest.raises(MissingJobIdError, match="has no submitted job id"):
        watch_run_lifecycle(
            workspace_root=sample_config_files,
            run_id="run-watch-missing-job",
            scheduler_adapter=scheduler,
            poll_interval_seconds=5,
            max_polls=1,
        )


def test_watch_run_lifecycle_fetch_on_complete(sample_config_files):
    run_id = "run-watch-fetch"
    job_id = "991100"
    transfer = FakeFileTransferAdapter()
    remote = FakeRemoteExecAdapter()
    submit_scheduler = FakeSchedulerAdapter()
    submit_scheduler.set_submit_result(
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
        transfer_adapter=transfer,
        scheduler_adapter=submit_scheduler,
        remote_exec_adapter=remote,
    )

    watch_scheduler = FakeSchedulerAdapter()
    watch_scheduler.queue_status_results(
        [
            _status_result(job_id, "RUNNING", "squeue"),
            _status_result(job_id, "COMPLETED", "squeue"),
        ]
    )
    fetch_transfer = FakeFileTransferAdapter()

    result = watch_run_lifecycle(
        workspace_root=sample_config_files,
        run_id=run_id,
        scheduler_adapter=watch_scheduler,
        transfer_adapter=fetch_transfer,
        poll_interval_seconds=5,
        max_polls=5,
        fetch_on_complete=True,
    )

    assert result.fetch_attempted is True
    assert result.fetch_succeeded is True
    assert result.final_state == RunLifecycleState.FETCHED
    assert len(fetch_transfer.sync_calls) == 3


def test_cli_watch_terminal_failure_returns_1(sample_config_files, monkeypatch, capsys):
    def _fake_watch_run_lifecycle(**_kwargs):
        return WatchResult(
            run_id="run-fail",
            job_id="999001",
            initial_state=RunLifecycleState.RUNNING,
            final_state=RunLifecycleState.FAILED,
            terminal_state=RunLifecycleState.FAILED,
            raw_scheduler_state="OUT_OF_MEMORY",
            terminal_reason="out_of_memory",
            poll_count=3,
            remote_run_dir="/scratch/users/ndorn/svzt_runs/run-fail",
            local_run_dir="/tmp/svz/runs/run-fail",
            local_logs_dir="/tmp/svz/runs/run-fail/logs",
            remote_logs_dir="/scratch/users/ndorn/svzt_runs/run-fail/logs",
            job_script_path="/scratch/users/ndorn/svzt_runs/run-fail/run_tune_trees.sh",
            fetch_attempted=False,
            fetch_succeeded=None,
            fetch_error=None,
            observations=[],
        )

    monkeypatch.setattr("svztagent.cli.main.watch_run_lifecycle", _fake_watch_run_lifecycle)
    rc = main(
        [
            "--workspace-root",
            str(sample_config_files),
            "watch",
            "run-fail",
        ]
    )

    captured = capsys.readouterr()
    assert rc == 1
    assert "Terminal summary" in captured.out
    assert "normalized_state: failed" in captured.out


def test_watch_parser_accepts_auto_advance_flag():
    parser = build_parser()
    args = parser.parse_args(["watch", "run-watch-parser", "--auto-advance"])
    assert args.auto_advance is True


def test_run_parser_rejects_removed_iteration1_seed_flags():
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "run",
                "tune",
                "--cluster",
                "sherlock",
                "--patient",
                "TST-STAN-x",
                "--iter1-seed-source",
                "path",
            ]
        )


def test_cli_run_tune_prints_progress_updates(sample_config_files, monkeypatch, capsys):
    def _fake_run_tune_trees(**kwargs):
        progress_callback = kwargs.get("progress_callback")
        assert progress_callback is not None
        progress_callback("[svzt] Loading execution plan")
        progress_callback("[svzt] Staging inputs for iteration 1")
        return TuneExecutionResult(
            run_id="run-progress",
            iteration=1,
            mode=ExecutionMode.DRY_RUN,
            plan_path=sample_config_files / "runs" / "run-progress" / "execution_plan.yaml",
            remote_run_dir="/scratch/users/ndorn/svzt_runs/run-progress",
            remote_job_script_path="/scratch/users/ndorn/svzt_runs/run-progress/iterations/iter-01/run_tune_iter.sh",
            local_job_script_path=sample_config_files / "runs" / "run-progress" / "iterations" / "iter-01" / "run_tune_iter.sh",
            submitted_job_id="dryrun-run-progress",
            command_previews=[["sbatch", "--parsable", "run_tune_iter.sh"]],
        )

    monkeypatch.setattr("svztagent.cli.main.run_tune_trees", _fake_run_tune_trees)

    rc = main(
        [
            "--workspace-root",
            str(sample_config_files),
            "run",
            "tune",
            "--cluster",
            "sherlock",
            "--patient",
            "TST-STAN-x",
        ]
    )

    captured = capsys.readouterr()
    assert rc == 0
    assert "[svzt] Starting tune workflow for patient TST-STAN-x on sherlock" in captured.out
    assert "[svzt] Loading execution plan" in captured.out
    assert "[svzt] Staging inputs for iteration 1" in captured.out
    assert "Run ID: run-progress" in captured.out


def test_cli_watch_auto_advance_converged_returns_0(sample_config_files, monkeypatch, capsys):
    def _fake_watch_and_auto_advance_tuning(**_kwargs):
        return AutoAdvanceResult(
            run_id="run-auto",
            final_action="converged",
            tracker_status="converged",
            final_iteration=2,
            final_terminal_state=RunLifecycleState.COMPLETED,
            iterations=[
                AutoAdvanceIterationRecord(
                    iteration=1,
                    terminal_state=RunLifecycleState.COMPLETED,
                    decision="not_close",
                    advance_action="advanced_and_submitted",
                    submitted_job_id="12346",
                ),
                AutoAdvanceIterationRecord(
                    iteration=2,
                    terminal_state=RunLifecycleState.COMPLETED,
                    decision="converged",
                    advance_action="already_converged",
                    submitted_job_id=None,
                ),
            ],
        )

    monkeypatch.setattr(
        "svztagent.cli.main.watch_and_auto_advance_tuning",
        _fake_watch_and_auto_advance_tuning,
    )
    rc = main(
        [
            "--workspace-root",
            str(sample_config_files),
            "watch",
            "run-auto",
            "--auto-advance",
        ]
    )

    captured = capsys.readouterr()
    assert rc == 0
    assert "Auto-advance summary" in captured.out
    assert "Final action: converged" in captured.out


def test_cli_watch_auto_advance_max_iter_failed_returns_1(sample_config_files, monkeypatch):
    def _fake_watch_and_auto_advance_tuning(**_kwargs):
        return AutoAdvanceResult(
            run_id="run-auto-failed",
            final_action="max_iter_failed",
            tracker_status="failed_max_iter",
            final_iteration=5,
            final_terminal_state=RunLifecycleState.COMPLETED,
            iterations=[
                AutoAdvanceIterationRecord(
                    iteration=5,
                    terminal_state=RunLifecycleState.COMPLETED,
                    decision="not_close",
                    advance_action="max_iter_failed",
                    submitted_job_id=None,
                )
            ],
        )

    monkeypatch.setattr(
        "svztagent.cli.main.watch_and_auto_advance_tuning",
        _fake_watch_and_auto_advance_tuning,
    )
    rc = main(
        [
            "--workspace-root",
            str(sample_config_files),
            "watch",
            "run-auto-failed",
            "--auto-advance",
        ]
    )

    assert rc == 1


def test_cli_watch_auto_advance_needs_review_returns_1(sample_config_files, monkeypatch):
    def _fake_watch_and_auto_advance_tuning(**_kwargs):
        return AutoAdvanceResult(
            run_id="run-auto-needs-review",
            final_action="needs_review_pause",
            tracker_status="paused_review",
            final_iteration=3,
            final_terminal_state=RunLifecycleState.COMPLETED,
            iterations=[
                AutoAdvanceIterationRecord(
                    iteration=3,
                    terminal_state=RunLifecycleState.COMPLETED,
                    decision="needs_review",
                    advance_action="paused_needs_review",
                    submitted_job_id=None,
                )
            ],
        )

    monkeypatch.setattr(
        "svztagent.cli.main.watch_and_auto_advance_tuning",
        _fake_watch_and_auto_advance_tuning,
    )
    rc = main(
        [
            "--workspace-root",
            str(sample_config_files),
            "watch",
            "run-auto-needs-review",
            "--auto-advance",
        ]
    )

    assert rc == 1


def test_cli_status_prints_failure_error_log(sample_config_files, monkeypatch, capsys):
    def _fake_query_run_status(**_kwargs):
        return StatusQueryResult(
            run_id="run-fail",
            job_id="12345",
            raw_state="FAILED",
            normalized_state=RunLifecycleState.FAILED,
            source="squeue",
            current_iteration=2,
            max_iterations=5,
            tracker_status="active",
            stage_key="preop_3d",
            stage_label="3D preop simulation",
            stage_detail="Preop job 99887 is running.",
            decision=None,
            needs_review_reason=None,
            progress_source="remote_pull",
            progress_warnings=["iteration metrics are not available yet"],
            preop_job_id="99887",
            preop_job_state_raw="RUNNING",
            preop_job_state_normalized=RunLifecycleState.RUNNING,
            failure_error_log_path="/tmp/run-fail_12345.error",
            failure_error_log_tail="traceback line 1\ntraceback line 2",
        )

    monkeypatch.setattr("svztagent.cli.main.query_run_status", _fake_query_run_status)
    rc = main(
        [
            "--workspace-root",
            str(sample_config_files),
            "status",
            "run-fail",
        ]
    )

    captured = capsys.readouterr()
    assert rc == 0
    assert "Current iteration: 2 / 5" in captured.out
    assert "Current stage: 3D preop simulation" in captured.out
    assert "Decision: pending" in captured.out
    assert "Progress artifact source: remote_pull" in captured.out
    assert "Preop job: 99887 (running)" in captured.out
    assert "Warnings:" in captured.out
    assert "Failure error log: /tmp/run-fail_12345.error" in captured.out
    assert "Failure error tail:" in captured.out
    assert "traceback line 2" in captured.out
