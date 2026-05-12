from __future__ import annotations

import json
from pathlib import Path

from svztagent.core.manifest import read_manifest
from svztagent.core.status import NormalizedRunState, normalize_slurm_state
from svztagent.hpc.fake import FakeFileTransferAdapter
from svztagent.hpc.fake import FakeSchedulerAdapter
from svztagent.hpc.interfaces import (
    CommandResult,
    ExecutionMode,
    SchedulerStatusResult,
    SubmitResult,
    SyncDirection,
)
from svztagent.workflows.tune_trees import query_run_status, run_tune_trees


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


def test_normalize_slurm_states():
    assert normalize_slurm_state("PENDING") == NormalizedRunState.PENDING
    assert normalize_slurm_state("RUNNING") == NormalizedRunState.RUNNING
    assert normalize_slurm_state("COMPLETED") == NormalizedRunState.COMPLETED
    assert normalize_slurm_state("FAILED") == NormalizedRunState.FAILED
    assert normalize_slurm_state("CANCELLED by 123") == NormalizedRunState.CANCELLED
    assert normalize_slurm_state("mystery") == NormalizedRunState.UNKNOWN


def test_query_status_updates_manifest(sample_config_files, fake_hpc):
    fake_transfer, fake_scheduler, fake_remote = fake_hpc
    fake_scheduler.set_submit_result(
        SubmitResult(
            job_id="778899",
            command=CommandResult(
                argv=["sbatch"],
                returncode=0,
                stdout="778899",
                stderr="",
                dry_run=False,
            ),
        )
    )
    run_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-status-001",
        mode=ExecutionMode.EXECUTE,
        transfer_adapter=fake_transfer,
        scheduler_adapter=fake_scheduler,
        remote_exec_adapter=fake_remote,
    )

    status_scheduler = FakeSchedulerAdapter()
    status_scheduler.set_status_result(
        SchedulerStatusResult(
            job_id="778899",
            raw_state="RUNNING",
            source="squeue",
            command=CommandResult(
                argv=["squeue", "--job", "778899"],
                returncode=0,
                stdout="RUNNING",
                stderr="",
                dry_run=False,
            ),
        )
    )

    status = query_run_status(
        workspace_root=sample_config_files,
        run_id="run-status-001",
        scheduler_adapter=status_scheduler,
        transfer_adapter=FakeFileTransferAdapter(),
    )
    assert status.normalized_state == NormalizedRunState.RUNNING

    manifest = read_manifest(sample_config_files / "runs" / "run-status-001" / "manifest.yaml")
    assert manifest.execution.last_known_scheduler_state == "running"


def test_query_status_reports_local_zerod_tuning_stage(sample_config_files, fake_hpc):
    fake_transfer, fake_scheduler, fake_remote = fake_hpc
    run_id = "run-status-zerod"
    job_id = "778902"
    fake_scheduler.set_submit_result(
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
        transfer_adapter=fake_transfer,
        scheduler_adapter=fake_scheduler,
        remote_exec_adapter=fake_remote,
    )

    local_driver_log = (
        sample_config_files / "runs" / run_id / "iterations" / "iter-01" / "logs" / "iteration_driver_log.json"
    )
    local_driver_log.parent.mkdir(parents=True, exist_ok=True)
    local_driver_log.write_text(json.dumps({"steps": ["0d_tuning_started"], "errors": [], "warnings": []}), encoding="utf-8")

    status_scheduler = FakeSchedulerAdapter()
    status_scheduler.set_status_result(_status_result(job_id, "RUNNING"))

    status = query_run_status(
        workspace_root=sample_config_files,
        run_id=run_id,
        scheduler_adapter=status_scheduler,
        transfer_adapter=FakeFileTransferAdapter(),
    )

    assert status.current_iteration == 1
    assert status.stage_key == "zerod_tuning"
    assert status.stage_label == "0D tuning"
    assert status.progress_source == "local"


def test_query_status_reports_preop_child_job_progress(sample_config_files, fake_hpc):
    fake_transfer, fake_scheduler, fake_remote = fake_hpc
    run_id = "run-status-preop"
    job_id = "778903"
    fake_scheduler.set_submit_result(
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
        transfer_adapter=fake_transfer,
        scheduler_adapter=fake_scheduler,
        remote_exec_adapter=fake_remote,
    )

    driver_log = (
        sample_config_files / "runs" / run_id / "iterations" / "iter-01" / "logs" / "iteration_driver_log.json"
    )
    driver_log.parent.mkdir(parents=True, exist_ok=True)
    driver_log.write_text(
        json.dumps(
            {
                "steps": ["0d_tuning_started", "0d_tuning_completed", "preop_3d_setup_started", "preop_submitted"],
                "errors": [],
                "warnings": [],
                "preop_job_id": "991100",
            }
        ),
        encoding="utf-8",
    )

    status_scheduler = FakeSchedulerAdapter()
    status_scheduler.queue_status_results(
        [
            _status_result(job_id, "RUNNING"),
            _status_result("991100", "RUNNING"),
        ]
    )

    status = query_run_status(
        workspace_root=sample_config_files,
        run_id=run_id,
        scheduler_adapter=status_scheduler,
        transfer_adapter=FakeFileTransferAdapter(),
    )

    assert status.stage_key == "preop_3d"
    assert status.preop_job_id == "991100"
    assert status.preop_job_state_normalized == NormalizedRunState.RUNNING
    assert status.stage_detail == "Preop job 991100 is running."


def test_query_status_pulls_remote_progress_artifacts(sample_config_files, fake_hpc):
    fake_transfer, fake_scheduler, fake_remote = fake_hpc
    run_id = "run-status-remote-progress"
    job_id = "778904"
    fake_scheduler.set_submit_result(
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
        transfer_adapter=fake_transfer,
        scheduler_adapter=fake_scheduler,
        remote_exec_adapter=fake_remote,
    )

    status_scheduler = FakeSchedulerAdapter()
    status_scheduler.queue_status_results(
        [
            _status_result(job_id, "COMPLETED"),
            _status_result("991100", "COMPLETED"),
            _status_result("991101", "RUNNING"),
        ]
    )
    transfer = _ProgressTransfer(
        driver_log={
            "steps": [
                "0d_tuning_started",
                "0d_tuning_completed",
                "preop_3d_setup_started",
                "preop_submitted",
                "preop_completed",
                "clinical_gate_evaluated",
                "postop_submitted",
            ],
            "errors": [],
            "warnings": [],
            "preop_job_id": "991100",
            "postop_job_id": "991101",
        },
        decision={
            "decision": "converged",
            "postop_submission_requested": True,
            "postop_job_id": "991101",
        },
        metrics={"preop_job_id": "991100"},
    )

    status = query_run_status(
        workspace_root=sample_config_files,
        run_id=run_id,
        scheduler_adapter=status_scheduler,
        transfer_adapter=transfer,
    )

    assert status.stage_key == "postop_3d"
    assert status.progress_source == "remote_pull"
    assert status.postop_job_id == "991101"
    assert status.postop_job_state_normalized == NormalizedRunState.RUNNING
    assert any(call[2] == ["iteration_driver_log.json"] for call in transfer.sync_calls)
    assert any(call[2] == ["iteration_decision.json", "iteration_metrics.json"] for call in transfer.sync_calls)


def test_query_status_missing_progress_artifacts_falls_back(sample_config_files, fake_hpc):
    fake_transfer, fake_scheduler, fake_remote = fake_hpc
    run_id = "run-status-missing-progress"
    job_id = "778905"
    fake_scheduler.set_submit_result(
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
        transfer_adapter=fake_transfer,
        scheduler_adapter=fake_scheduler,
        remote_exec_adapter=fake_remote,
    )

    status_scheduler = FakeSchedulerAdapter()
    status_scheduler.set_status_result(_status_result(job_id, "COMPLETED"))

    status = query_run_status(
        workspace_root=sample_config_files,
        run_id=run_id,
        scheduler_adapter=status_scheduler,
        transfer_adapter=FakeFileTransferAdapter(),
    )

    assert status.stage_key == "terminal_unknown"
    assert status.progress_source == "unavailable"
    assert status.progress_warnings
    assert "missing progress artifacts" in status.progress_warnings[-1]


def test_query_status_child_job_lookup_failure_adds_warning(sample_config_files, fake_hpc):
    fake_transfer, fake_scheduler, fake_remote = fake_hpc
    run_id = "run-status-child-warning"
    job_id = "778906"
    fake_scheduler.set_submit_result(
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
        transfer_adapter=fake_transfer,
        scheduler_adapter=fake_scheduler,
        remote_exec_adapter=fake_remote,
    )

    driver_log = (
        sample_config_files / "runs" / run_id / "iterations" / "iter-01" / "logs" / "iteration_driver_log.json"
    )
    driver_log.parent.mkdir(parents=True, exist_ok=True)
    driver_log.write_text(
        json.dumps(
            {
                "steps": ["preop_submitted"],
                "errors": [],
                "warnings": [],
                "preop_job_id": "broken-job",
            }
        ),
        encoding="utf-8",
    )

    class _FailingChildScheduler(FakeSchedulerAdapter):
        def status(self, job_id: str) -> SchedulerStatusResult:
            if job_id == "broken-job":
                raise RuntimeError("scheduler unavailable")
            return super().status(job_id)

    status_scheduler = _FailingChildScheduler()
    status_scheduler.set_status_result(_status_result(job_id, "RUNNING"))

    status = query_run_status(
        workspace_root=sample_config_files,
        run_id=run_id,
        scheduler_adapter=status_scheduler,
        transfer_adapter=FakeFileTransferAdapter(),
    )

    assert status.preop_job_id == "broken-job"
    assert status.preop_job_state_normalized is None
    assert any("scheduler unavailable" in warning for warning in status.progress_warnings or [])


def test_query_status_reads_local_error_log_when_failed(sample_config_files, fake_hpc):
    fake_transfer, fake_scheduler, fake_remote = fake_hpc
    run_id = "run-status-failed-local"
    job_id = "778900"
    fake_scheduler.set_submit_result(
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
        transfer_adapter=fake_transfer,
        scheduler_adapter=fake_scheduler,
        remote_exec_adapter=fake_remote,
    )

    local_error_log = (
        sample_config_files
        / "runs"
        / run_id
        / "iterations"
        / "iter-01"
        / "logs"
        / f"{run_id}_{job_id}.error"
    )
    local_error_log.parent.mkdir(parents=True, exist_ok=True)
    local_error_log.write_text("line 1\nline 2\nfatal: tune failed\n", encoding="utf-8")

    status_scheduler = FakeSchedulerAdapter()
    status_scheduler.set_status_result(
        SchedulerStatusResult(
            job_id=job_id,
            raw_state="FAILED",
            source="squeue",
            command=CommandResult(
                argv=["squeue", "--job", job_id],
                returncode=0,
                stdout="FAILED",
                stderr="",
                dry_run=False,
            ),
        )
    )

    status = query_run_status(
        workspace_root=sample_config_files,
        run_id=run_id,
        scheduler_adapter=status_scheduler,
        transfer_adapter=FakeFileTransferAdapter(),
    )
    assert status.normalized_state == NormalizedRunState.FAILED
    assert status.failure_error_log_path == str(local_error_log)
    assert status.failure_error_log_tail is not None
    assert "fatal: tune failed" in status.failure_error_log_tail


class _PullingTransfer(FakeFileTransferAdapter):
    def __init__(self, *, run_id: str, job_id: str):
        super().__init__()
        self._run_id = run_id
        self._job_id = job_id

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
        if direction == SyncDirection.PULL and include == [f"*_{self._job_id}.error"]:
            target = Path(local_dir) / f"{self._run_id}_{self._job_id}.error"
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text("remote stderr\nsolver diverged\n", encoding="utf-8")
        return result


class _ProgressTransfer(FakeFileTransferAdapter):
    def __init__(
        self,
        *,
        driver_log: dict | None = None,
        decision: dict | None = None,
        metrics: dict | None = None,
    ):
        super().__init__()
        self._driver_log = driver_log
        self._decision = decision
        self._metrics = metrics

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
        if direction != SyncDirection.PULL:
            return result

        local_root = Path(local_dir)
        local_root.mkdir(parents=True, exist_ok=True)
        include_rules = include or []
        if include_rules == ["iteration_driver_log.json"] and self._driver_log is not None:
            (local_root / "iteration_driver_log.json").write_text(
                json.dumps(self._driver_log),
                encoding="utf-8",
            )
        if include_rules == ["iteration_decision.json", "iteration_metrics.json"]:
            if self._decision is not None:
                (local_root / "iteration_decision.json").write_text(
                    json.dumps(self._decision),
                    encoding="utf-8",
                )
            if self._metrics is not None:
                (local_root / "iteration_metrics.json").write_text(
                    json.dumps(self._metrics),
                    encoding="utf-8",
                )
        return result


def test_query_status_pulls_error_log_when_failed(sample_config_files, fake_hpc):
    fake_transfer, fake_scheduler, fake_remote = fake_hpc
    run_id = "run-status-failed-pull"
    job_id = "778901"
    fake_scheduler.set_submit_result(
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
        transfer_adapter=fake_transfer,
        scheduler_adapter=fake_scheduler,
        remote_exec_adapter=fake_remote,
    )

    status_scheduler = FakeSchedulerAdapter()
    status_scheduler.set_status_result(
        SchedulerStatusResult(
            job_id=job_id,
            raw_state="FAILED",
            source="squeue",
            command=CommandResult(
                argv=["squeue", "--job", job_id],
                returncode=0,
                stdout="FAILED",
                stderr="",
                dry_run=False,
            ),
        )
    )
    pulling_transfer = _PullingTransfer(run_id=run_id, job_id=job_id)

    status = query_run_status(
        workspace_root=sample_config_files,
        run_id=run_id,
        scheduler_adapter=status_scheduler,
        transfer_adapter=pulling_transfer,
    )
    assert status.normalized_state == NormalizedRunState.FAILED
    assert status.failure_error_log_path is not None
    assert status.failure_error_log_path.endswith(f"{run_id}_{job_id}.error")
    assert status.failure_error_log_tail is not None
    assert "solver diverged" in status.failure_error_log_tail
    assert any(
        call[-1] == SyncDirection.PULL and call[2] == [f"*_{job_id}.error"]
        for call in pulling_transfer.sync_calls
    )
