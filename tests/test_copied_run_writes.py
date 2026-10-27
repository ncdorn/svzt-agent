"""Steps run against a copied run dir must never write into the original.

Manifests record absolute local paths at init (``local_paths``) and at each
submission (record ``local_dir``).  A shadow workspace for golden dry runs is a
copy of a real workspace, so those recorded paths still point at the original
run.  Every write must land in the run dir actually being operated on.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
import shutil

from svztagent.core.manifest import read_manifest
from svztagent.core.paths import relocate_recorded_local_path
from svztagent.hpc.fake import FakeFileTransferAdapter, FakeRemoteExecAdapter, FakeSchedulerAdapter
from svztagent.hpc.interfaces import CommandResult, ExecutionMode, SubmitResult, SyncDirection
from svztagent.workflows.adapt import run_adapt
from svztagent.workflows.postop import run_postop
from svztagent.workflows.tune_trees import advance_tune_iteration, fetch_run_artifacts, run_tune_trees

from test_full_pa_calibration_cadence import (
    CalibrationPublishingTransfer,
    _completed_full_pa_run,
    _completed_status,
)
from test_adapt_workflow import _prepare_adaptable_run
from test_postop_workflow import _prepare_selected_run


def _snapshot(run_dir: Path) -> dict[str, tuple[str, int]]:
    return {
        str(path.relative_to(run_dir)): (
            hashlib.sha256(path.read_bytes()).hexdigest(),
            path.stat().st_mtime_ns,
        )
        for path in sorted(run_dir.rglob("*"))
        if path.is_file()
    } | {
        str(path.relative_to(run_dir)) + "/": ("dir", 0)
        for path in sorted(run_dir.rglob("*"))
        if path.is_dir()
    }


def _shadow_workspace(original_root: Path) -> Path:
    shadow_root = original_root.parent / "shadow" / original_root.name
    shutil.copytree(original_root, shadow_root)
    return shadow_root


class PullWritingTransfer(FakeFileTransferAdapter):
    """Drop a marker file into every local pull destination."""

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
            destination = Path(local_dir)
            destination.mkdir(parents=True, exist_ok=True)
            name = "manifest.yaml" if include == ["manifest.yaml"] else "pulled.txt"
            (destination / name).write_text("pulled\n", encoding="utf-8")
        return result


def test_relocate_recorded_local_path_reroots_only_paths_under_the_recorded_run(tmp_path):
    original = tmp_path / "svz" / "runs" / "run-a"
    active = tmp_path / "shadow" / "svz" / "runs" / "run-a"

    assert relocate_recorded_local_path(
        str(original / "iterations" / "iter-01" / "calibration"),
        recorded_run_dir=str(original),
        active_run_dir=active,
    ) == active / "iterations" / "iter-01" / "calibration"
    assert relocate_recorded_local_path(
        str(original), recorded_run_dir=str(original), active_run_dir=active
    ) == active
    outside = tmp_path / "elsewhere" / "file.json"
    assert relocate_recorded_local_path(
        str(outside), recorded_run_dir=str(original), active_run_dir=active
    ) == outside


def test_fetch_of_copied_run_pulls_into_the_copy(sample_config_files):
    run_id = "run-fetch-shadow"
    scheduler = FakeSchedulerAdapter()
    scheduler.set_submit_result(
        SubmitResult(
            job_id="889900",
            command=CommandResult(argv=["sbatch"], returncode=0, stdout="889900", stderr="", dry_run=False),
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
    original_run = sample_config_files / "runs" / run_id
    shadow_root = _shadow_workspace(sample_config_files)
    shadow_run = shadow_root / "runs" / run_id
    before = _snapshot(original_run)

    transfer = PullWritingTransfer()
    result = fetch_run_artifacts(
        workspace_root=shadow_root,
        run_id=run_id,
        transfer_adapter=transfer,
        mode=ExecutionMode.DRY_RUN,
    )

    assert _snapshot(original_run) == before
    assert transfer.sync_calls
    for call in transfer.sync_calls:
        assert Path(call[0]).is_relative_to(shadow_run / "pulled_outputs")
    assert Path(result.local_output_dir) == shadow_run / "pulled_outputs"
    assert (shadow_run / "pulled_outputs" / "iterations" / "iter-01" / "results" / "pulled.txt").is_file()
    assert read_manifest(shadow_run / "manifest.yaml").execution.fetch_timestamps


def test_calibration_promotion_of_copied_run_writes_into_the_copy(sample_config_files):
    run_id = "run-calibration-shadow"
    _completed_full_pa_run(sample_config_files, run_id, decision="not_close")
    transfer = CalibrationPublishingTransfer()
    scheduler = FakeSchedulerAdapter()
    remote = FakeRemoteExecAdapter()
    submitted = advance_tune_iteration(
        workspace_root=sample_config_files,
        run_id=run_id,
        execute=True,
        transfer_adapter=transfer,
        scheduler_adapter=scheduler,
        remote_exec_adapter=remote,
    )
    assert submitted.action == "calibration_submitted"

    original_run = sample_config_files / "runs" / run_id
    shadow_root = _shadow_workspace(sample_config_files)
    shadow_run = shadow_root / "runs" / run_id
    # The copy still records the original run's absolute calibration dir.
    copied = read_manifest(shadow_run / "manifest.yaml")
    assert Path(copied.calibration_runs[0].local_dir).is_relative_to(original_run)
    before = _snapshot(original_run)

    scheduler.queue_status_results([_completed_status(submitted.submitted_job_id or "calibration")])
    advanced = advance_tune_iteration(
        workspace_root=shadow_root,
        run_id=run_id,
        execute=True,
        transfer_adapter=transfer,
        scheduler_adapter=scheduler,
        remote_exec_adapter=remote,
    )

    assert advanced.action == "advanced_and_submitted"
    assert _snapshot(original_run) == before
    pulls = [call for call in transfer.sync_calls if call[4] == SyncDirection.PULL]
    for call in pulls[-2:]:
        assert Path(call[0]).is_relative_to(shadow_run)
    updated = read_manifest(shadow_run / "manifest.yaml")
    seed = Path(updated.tuning_iteration_tracker.iterations[0].calibrated_seed_path)
    assert seed.is_relative_to(shadow_run)
    assert seed.is_file()
    assert Path(updated.calibration_promotion.promoted_seed_path) == seed


def test_postop_of_copied_run_uses_the_copy_workspace(sample_config_files):
    run_id = "run-postop-shadow"
    _prepare_selected_run(sample_config_files, run_id=run_id)
    original_run = sample_config_files / "runs" / run_id
    shadow_root = _shadow_workspace(sample_config_files)
    shadow_run = shadow_root / "runs" / run_id
    clusters = shadow_root / "config" / "clusters.yaml"
    clusters.write_text(
        clusters.read_text(encoding="utf-8").replace(
            "/home/users/ndorn/bin/svslicer", "/home/users/ndorn/bin/svslicer-shadow"
        ),
        encoding="utf-8",
    )
    before = _snapshot(original_run)

    result = run_postop(
        workspace_root=shadow_root,
        run_id=run_id,
        mode=ExecutionMode.DRY_RUN,
        transfer_adapter=FakeFileTransferAdapter(),
        scheduler_adapter=FakeSchedulerAdapter(),
        remote_exec_adapter=FakeRemoteExecAdapter(),
    )

    assert _snapshot(original_run) == before
    assert result.plan_path.is_relative_to(shadow_run)
    assert result.local_job_script_path.is_relative_to(shadow_run)
    # The job script is rendered from the copy's workspace config, not the
    # original workspace that the recorded local_paths.run_dir points into.
    assert "svslicer-shadow" in result.local_job_script_path.read_text(encoding="utf-8")
    plan_text = result.plan_path.read_text(encoding="utf-8")
    assert str(shadow_run / "manifest.yaml") in plan_text
    assert str(original_run) not in plan_text


def test_adapt_of_copied_run_writes_into_the_copy(sample_config_files):
    run_id = "run-adapt-shadow"
    _prepare_adaptable_run(sample_config_files, run_id=run_id)
    original_run = sample_config_files / "runs" / run_id
    shadow_root = _shadow_workspace(sample_config_files)
    shadow_run = shadow_root / "runs" / run_id
    before = _snapshot(original_run)

    result = run_adapt(
        workspace_root=shadow_root,
        run_id=run_id,
        model="M2",
        mode=ExecutionMode.EXECUTE,
        transfer_adapter=FakeFileTransferAdapter(),
        scheduler_adapter=FakeSchedulerAdapter(),
        remote_exec_adapter=FakeRemoteExecAdapter(),
    )

    assert _snapshot(original_run) == before
    assert result.plan_path.is_relative_to(shadow_run)
    assert result.local_job_script_path.is_relative_to(shadow_run)
    assert read_manifest(shadow_run / "manifest.yaml").adaptation_runs
    plan_text = result.plan_path.read_text(encoding="utf-8")
    assert str(shadow_run / "manifest.yaml") in plan_text
    assert str(original_run) not in plan_text
