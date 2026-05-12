from __future__ import annotations

from dataclasses import dataclass

import pytest

from svztagent.core.errors import WatchMaxPollsExceededError, WatchTimeoutError
from svztagent.core.manifest import read_manifest
from svztagent.core.monitor import MonitorSettings, RunMonitorService
from svztagent.core.state import RunLifecycleState
from svztagent.hpc.fake import FakeFileTransferAdapter, FakeRemoteExecAdapter, FakeSchedulerAdapter
from svztagent.hpc.interfaces import CommandResult, ExecutionMode, SchedulerStatusResult, SubmitResult
from svztagent.workflows.tune_trees import run_tune_trees


@dataclass
class IsoClock:
    index: int = 0

    def __call__(self) -> str:
        timestamp = f"2026-03-10T00:00:{self.index:02d}+00:00"
        self.index += 1
        return timestamp


def _status_result(job_id: str, raw_state: str | None, source: str) -> SchedulerStatusResult:
    stdout = raw_state or ""
    argv = ["squeue"] if source == "squeue" else ["sacct"]
    return SchedulerStatusResult(
        job_id=job_id,
        raw_state=raw_state,
        source=source,
        command=CommandResult(
            argv=argv,
            returncode=0,
            stdout=stdout,
            stderr="",
            dry_run=False,
        ),
    )


def _create_submitted_run(sample_config_files, run_id: str, job_id: str) -> None:
    fake_transfer = FakeFileTransferAdapter()
    fake_remote = FakeRemoteExecAdapter()
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
        transfer_adapter=fake_transfer,
        scheduler_adapter=submit_scheduler,
        remote_exec_adapter=fake_remote,
    )


def test_monitor_service_reaches_terminal_state_and_uses_sacct_fallback(sample_config_files):
    run_id = "run-monitor-001"
    job_id = "123450"
    _create_submitted_run(sample_config_files, run_id=run_id, job_id=job_id)
    manifest_path = sample_config_files / "runs" / run_id / "manifest.yaml"

    watch_scheduler = FakeSchedulerAdapter()
    watch_scheduler.queue_status_results(
        [
            _status_result(job_id, "RUNNING", "squeue"),
            _status_result(job_id, None, "squeue"),
        ]
    )
    watch_scheduler.queue_accounting_results(
        [
            _status_result(job_id, "COMPLETED", "sacct"),
        ]
    )

    service = RunMonitorService(
        scheduler_adapter=watch_scheduler,
        sleep_fn=lambda _: None,
        monotonic_fn=lambda: 0.0,
        now_fn=IsoClock(),
    )
    summary = service.watch(
        manifest_path=manifest_path,
        settings=MonitorSettings(
            poll_interval_seconds=5,
            max_polls=10,
        ),
    )

    assert summary.final_state == RunLifecycleState.COMPLETED
    assert summary.poll_count == 2
    assert len(watch_scheduler.status_calls) == 2
    assert len(watch_scheduler.accounting_calls) == 1

    manifest = read_manifest(manifest_path)
    assert manifest.execution.poll_count == 2
    assert manifest.execution.lifecycle_timestamps.first_running_at is not None
    assert manifest.execution.lifecycle_timestamps.terminal_state_at is not None


def test_monitor_service_timeout(sample_config_files):
    run_id = "run-monitor-timeout"
    job_id = "123451"
    _create_submitted_run(sample_config_files, run_id=run_id, job_id=job_id)
    manifest_path = sample_config_files / "runs" / run_id / "manifest.yaml"

    watch_scheduler = FakeSchedulerAdapter()
    watch_scheduler.set_status_result(_status_result(job_id, "PENDING", "squeue"))

    ticks = iter([0.0, 2.0])
    service = RunMonitorService(
        scheduler_adapter=watch_scheduler,
        sleep_fn=lambda _: None,
        monotonic_fn=lambda: next(ticks),
        now_fn=IsoClock(),
    )

    with pytest.raises(WatchTimeoutError, match="watch timed out"):
        service.watch(
            manifest_path=manifest_path,
            settings=MonitorSettings(
                poll_interval_seconds=5,
                timeout_seconds=1,
            ),
        )


def test_monitor_service_max_polls(sample_config_files):
    run_id = "run-monitor-maxpolls"
    job_id = "123452"
    _create_submitted_run(sample_config_files, run_id=run_id, job_id=job_id)
    manifest_path = sample_config_files / "runs" / run_id / "manifest.yaml"

    watch_scheduler = FakeSchedulerAdapter()
    watch_scheduler.set_status_result(_status_result(job_id, "PENDING", "squeue"))

    service = RunMonitorService(
        scheduler_adapter=watch_scheduler,
        sleep_fn=lambda _: None,
        monotonic_fn=lambda: 0.0,
        now_fn=IsoClock(),
    )

    with pytest.raises(WatchMaxPollsExceededError, match="watch exceeded max polls"):
        service.watch(
            manifest_path=manifest_path,
            settings=MonitorSettings(
                poll_interval_seconds=5,
                max_polls=2,
            ),
        )

    manifest = read_manifest(manifest_path)
    assert manifest.execution.poll_count == 2
