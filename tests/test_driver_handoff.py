"""Split tune driver: the agent follows the pre-3D job's handoff to its post-3D job."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from svztagent.core.errors import ConfigError
from svztagent.core.manifest import read_manifest, record_driver_handoff
from svztagent.core.state import RunLifecycleState
from svztagent.hpc.fake import FakeFileTransferAdapter, FakeRemoteExecAdapter, FakeSchedulerAdapter
from svztagent.hpc.interfaces import CommandResult, ExecutionMode, SchedulerStatusResult, SubmitResult
from svztagent.workflows.tune_trees import run_tune_trees, watch_run_lifecycle

PRE3D, PREOP, POST3D = "700001", "700002", "700003"


def _status(job_id: str, raw_state: str) -> SchedulerStatusResult:
    return SchedulerStatusResult(
        job_id=job_id,
        raw_state=raw_state,
        source="sacct",
        command=CommandResult(argv=["sacct"], returncode=0, stdout=raw_state, stderr="", dry_run=False),
    )


class HandoffTransfer(FakeFileTransferAdapter):
    """Serves results/iteration_handoff.json when the agent pulls it."""

    def __init__(self, handoff: dict | None):
        super().__init__()
        self.handoff = handoff

    def sync(self, *, local_dir, remote_dir, include=None, exclude=None, direction=None):
        result = super().sync(
            local_dir=local_dir, remote_dir=remote_dir, include=include, exclude=exclude, direction=direction
        )
        if self.handoff is not None and include == ["iteration_handoff.json"]:
            Path(local_dir, "iteration_handoff.json").write_text(json.dumps(self.handoff), encoding="utf-8")
        return result


def _submit(sample_config_files, run_id: str) -> Path:
    scheduler = FakeSchedulerAdapter()
    scheduler.set_submit_result(
        SubmitResult(
            job_id=PRE3D,
            command=CommandResult(argv=["sbatch"], returncode=0, stdout=PRE3D, stderr="", dry_run=False),
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
    return sample_config_files / "runs" / run_id / "manifest.yaml"


def _handoff(iteration: int = 1, pre3d: str = PRE3D) -> dict:
    return {"iteration": iteration, "pre3d_job_id": pre3d, "preop_job_id": PREOP, "post3d_job_id": POST3D}


def test_watch_follows_pre3d_handoff_to_post3d_job(sample_config_files):
    manifest_path = _submit(sample_config_files, "run-handoff")
    scheduler = FakeSchedulerAdapter()
    scheduler.queue_status_results(
        [_status(PRE3D, "COMPLETED"), _status(POST3D, "PENDING"), _status(POST3D, "COMPLETED")]
    )

    result = watch_run_lifecycle(
        workspace_root=sample_config_files,
        run_id="run-handoff",
        scheduler_adapter=scheduler,
        transfer_adapter=HandoffTransfer(_handoff()),
        poll_interval_seconds=5,
        max_polls=5,
    )

    assert result.job_id == POST3D
    assert result.terminal_state == RunLifecycleState.COMPLETED
    manifest = read_manifest(manifest_path)
    record = manifest.tuning_iteration_tracker.iterations[0]
    assert record.tune_job_id == POST3D
    assert any(f"continues tune job {PRE3D}" in note for note in record.notes)
    assert [job["job_id"] for job in manifest.jobs] == [POST3D, PRE3D]


@pytest.mark.parametrize("handoff", [None, _handoff(pre3d="699999"), _handoff(iteration=2)])
def test_watch_ignores_missing_or_stale_handoff(sample_config_files, handoff):
    manifest_path = _submit(sample_config_files, "run-no-handoff")
    scheduler = FakeSchedulerAdapter()
    scheduler.queue_status_results([_status(PRE3D, "COMPLETED")])

    result = watch_run_lifecycle(
        workspace_root=sample_config_files,
        run_id="run-no-handoff",
        scheduler_adapter=scheduler,
        transfer_adapter=HandoffTransfer(handoff),
        poll_interval_seconds=5,
        max_polls=5,
    )

    assert result.job_id == PRE3D
    assert read_manifest(manifest_path).tuning_iteration_tracker.iterations[0].tune_job_id == PRE3D


def test_record_driver_handoff_requires_the_ended_tracked_job(sample_config_files):
    manifest = read_manifest(_submit(sample_config_files, "run-handoff-guard"))
    with pytest.raises(ConfigError, match="has not ended"):
        record_driver_handoff(manifest, iteration=1, from_job_id=PRE3D, to_job_id=POST3D)
    with pytest.raises(ConfigError, match="does not track tune job"):
        record_driver_handoff(manifest, iteration=1, from_job_id="123", to_job_id=POST3D)


def _driver_function(name: str) -> str:
    template = (
        Path(__file__).resolve().parents[1] / "src" / "svztagent" / "templates" / "slurm" / "job_template.sh"
    ).read_text()
    start = template.index(f"def {name}(")
    return template[start : template.index("\ndef ", start + 1)]


@pytest.mark.parametrize("state, reused", [("COMPLETED", True), ("FAILED", False), ("CANCELLED", False)])
def test_post3d_driver_reuses_3d_only_after_it_completed(tmp_path, state, reused):
    import shutil

    logs = tmp_path / "logs"
    preop = tmp_path / "preop" / "72-procs"
    logs.mkdir()
    preop.mkdir(parents=True)
    (preop / "result_06000.vtu").write_text("", encoding="utf-8")
    (logs / "iteration_driver_log.json").write_text(
        json.dumps({"preop_job_id": PREOP, "steps": ["preop_submitted", f"post3d_driver_submitted:{POST3D}"]}),
        encoding="utf-8",
    )
    log: dict = {}
    namespace = {
        "json": json,
        "shutil": shutil,
        "Path": Path,
        "remote_logs_dir": logs,
        "driver_phase": "post3d",
        "log": log,
        "_query_state": lambda job_id: (state, "sacct"),
        "_latest_result_vtu": lambda sim_dir: next(iter(sorted(sim_dir.rglob("result_*.vtu"))), None),
    }
    exec(_driver_function("_completed_preop_job_for_reuse"), namespace)

    job_id, error = namespace["_completed_preop_job_for_reuse"](tmp_path / "preop")

    assert (job_id == PREOP) is reused
    assert log["preop_terminal_state"] == state
    if reused:
        assert (logs / f"iteration_driver_log.pre3d_{PREOP}.json").exists()
    else:
        assert error == f"preop simulation did not complete successfully: {state}"


def test_post3d_driver_is_queued_afterany_on_the_3d_job(tmp_path, monkeypatch):
    import os
    import subprocess

    (tmp_path / "run_tune_iter.sh").write_text("#!/bin/bash\n", encoding="utf-8")
    calls = []

    def fake_run(argv, **kwargs):
        calls.append(argv)
        return subprocess.CompletedProcess(argv, 0, stdout=f"{POST3D}\n", stderr="")

    namespace = {
        "os": os,
        "subprocess": type("S", (), {"run": staticmethod(fake_run)}),
        "remote_iter_dir": tmp_path,
        "run_id": "run-x",
        "_nested_sbatch_env": lambda: {},
    }
    exec(_driver_function("_submit_post3d_driver"), namespace)

    assert namespace["_submit_post3d_driver"](PREOP) == POST3D
    argv = calls[0]
    assert f"--dependency=afterany:{PREOP}" in argv
    assert argv[argv.index("--export") + 1] == "ALL,SVZT_DRIVER_PHASE=post3d"
    assert argv[-1] == str(tmp_path / "run_tune_iter.sh")
