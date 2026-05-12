from __future__ import annotations

import json

import pytest

from svztagent.core.errors import ConfigError
from svztagent.core.manifest import (
    mark_iteration_decision,
    mark_iteration_submitted,
    read_manifest,
    write_manifest,
)
from svztagent.hpc.fake import FakeFileTransferAdapter, FakeRemoteExecAdapter, FakeSchedulerAdapter
from svztagent.hpc.interfaces import CommandResult, ExecutionMode, SubmitResult
from svztagent.workflows.postop import run_postop, select_converged_preop_iteration
from svztagent.workflows.tune_trees import init_run_workspace


def _enable_postop_mesh(sample_config_files):
    postop_mesh = (
        sample_config_files
        / "remote_data"
        / "permanent"
        / "TST-STAN-x"
        / "postop-mesh-complete"
    )
    (postop_mesh / "mesh-surfaces").mkdir(parents=True, exist_ok=True)
    defaults_path = sample_config_files / "config" / "defaults.yaml"
    defaults = defaults_path.read_text(encoding="utf-8")
    if "postop_mesh_complete_dir" not in defaults:
        defaults = defaults.replace(
            'preop_mesh_complete_dir: "preop-mesh-complete"\n',
            'preop_mesh_complete_dir: "preop-mesh-complete"\n'
            '    postop_mesh_complete_dir: "postop-mesh-complete"\n',
        )
        defaults_path.write_text(defaults, encoding="utf-8")


def _write_completed_iteration_artifacts(paths, *, iteration: int, decision: str = "not_close"):
    iter_dir = paths.run_dir / "iterations" / f"iter-{iteration:02d}"
    results = iter_dir / "results"
    logs = iter_dir / "logs"
    results.mkdir(parents=True, exist_ok=True)
    logs.mkdir(parents=True, exist_ok=True)
    tuned = (
        f"/scratch/users/ndorn/svzt_runs/{paths.run_dir.name}/iterations/"
        f"iter-{iteration:02d}/results/svzerod_3d_coupling_tuned.json"
    )
    (results / "iteration_decision.json").write_text(
        json.dumps(
            {
                "decision": decision,
                "tuning_artifacts": {
                    "tuned_zerod_config": tuned,
                    "optimized_params_csv": "optimized_params.csv",
                    "pa_config_snapshot": "pa_config_tuning_snapshot.json",
                },
            }
        ),
        encoding="utf-8",
    )
    (results / "iteration_metrics.json").write_text(
        json.dumps({"preop_job_id": "991100", "mpa_mean": 31.0}),
        encoding="utf-8",
    )
    (logs / "iteration_driver_log.json").write_text(
        json.dumps(
            {
                "steps": ["preop_submitted", "preop_completed"],
                "preop_job_id": "991100",
                "errors": [],
                "warnings": [],
            }
        ),
        encoding="utf-8",
    )


def _prepare_selected_run(sample_config_files, *, run_id: str = "run-postop"):
    _enable_postop_mesh(sample_config_files)
    paths, _ctx = init_run_workspace(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id=run_id,
    )
    manifest = read_manifest(paths.manifest)
    manifest = mark_iteration_submitted(
        manifest,
        iteration=3,
        tune_job_id="990003",
        local_dir=str(paths.run_dir / "iterations" / "iter-03"),
        remote_dir=f"/scratch/users/ndorn/svzt_runs/{run_id}/iterations/iter-03",
        job_script_path=f"/scratch/users/ndorn/svzt_runs/{run_id}/iterations/iter-03/run_tune_iter.sh",
    )
    manifest = mark_iteration_decision(
        manifest,
        iteration=3,
        decision="not_close",
        metrics={"mpa_mean": 31.0},
        deltas={"mpa_mean": 0.5},
    )
    write_manifest(manifest, paths.manifest)
    _write_completed_iteration_artifacts(paths, iteration=3, decision="not_close")
    transfer = FakeFileTransferAdapter()
    scheduler = FakeSchedulerAdapter()
    remote = FakeRemoteExecAdapter()
    select_converged_preop_iteration(
        workspace_root=sample_config_files,
        run_id=run_id,
        iteration=3,
        reason="best tuned preop",
        transfer_adapter=transfer,
        scheduler_adapter=scheduler,
        remote_exec_adapter=remote,
    )
    return paths


def test_preop_select_records_best_completed_nonfinal_iteration(sample_config_files):
    paths = _prepare_selected_run(sample_config_files, run_id="run-preop-select")

    manifest = read_manifest(paths.manifest)
    assert manifest.converged_preop_iteration is not None
    assert manifest.converged_preop_iteration.iteration == 3
    assert manifest.converged_preop_iteration.selection_kind == "operator_promoted_best_completed"
    assert manifest.converged_preop_iteration.preop_job_id == "991100"
    assert manifest.selected_preop_postprocess is not None
    assert manifest.selected_preop_postprocess.source_preop_iteration == 3


def test_preop_select_submits_selected_preop_postprocess(sample_config_files):
    _enable_postop_mesh(sample_config_files)
    paths, _ctx = init_run_workspace(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-preop-postprocess",
    )
    manifest = read_manifest(paths.manifest)
    manifest = mark_iteration_submitted(
        manifest,
        iteration=2,
        tune_job_id="990002",
        local_dir=str(paths.run_dir / "iterations" / "iter-02"),
        remote_dir="/scratch/users/ndorn/svzt_runs/run-preop-postprocess/iterations/iter-02",
        job_script_path="/scratch/users/ndorn/svzt_runs/run-preop-postprocess/iterations/iter-02/run_tune_iter.sh",
    )
    write_manifest(manifest, paths.manifest)
    _write_completed_iteration_artifacts(paths, iteration=2, decision="converged")
    transfer = FakeFileTransferAdapter()
    scheduler = FakeSchedulerAdapter()
    remote = FakeRemoteExecAdapter()

    result = select_converged_preop_iteration(
        workspace_root=sample_config_files,
        run_id="run-preop-postprocess",
        iteration=2,
        transfer_adapter=transfer,
        scheduler_adapter=scheduler,
        remote_exec_adapter=remote,
    )

    assert result.postprocess_job_id == "dryrun-fake"
    assert len(transfer.ensure_calls) == 4
    assert len(scheduler.submit_calls) == 1


def test_preop_select_replaces_existing_selection(sample_config_files):
    paths = _prepare_selected_run(sample_config_files, run_id="run-preop-replace")
    manifest = read_manifest(paths.manifest)
    manifest = mark_iteration_submitted(
        manifest,
        iteration=5,
        tune_job_id="990005",
        local_dir=str(paths.run_dir / "iterations" / "iter-05"),
        remote_dir="/scratch/users/ndorn/svzt_runs/run-preop-replace/iterations/iter-05",
        job_script_path="/scratch/users/ndorn/svzt_runs/run-preop-replace/iterations/iter-05/run_tune_iter.sh",
    )
    write_manifest(manifest, paths.manifest)
    _write_completed_iteration_artifacts(paths, iteration=5, decision="converged")

    transfer = FakeFileTransferAdapter()
    scheduler = FakeSchedulerAdapter()
    remote = FakeRemoteExecAdapter()
    select_converged_preop_iteration(
        workspace_root=sample_config_files,
        run_id="run-preop-replace",
        iteration=5,
        reason="new best",
        transfer_adapter=transfer,
        scheduler_adapter=scheduler,
        remote_exec_adapter=remote,
    )

    updated = read_manifest(paths.manifest)
    assert updated.converged_preop_iteration is not None
    assert updated.converged_preop_iteration.iteration == 5
    assert updated.converged_preop_iteration.selection_kind == "formal_converged"
    assert updated.converged_preop_iteration.reason == "new best"


def test_preop_select_fails_without_completed_preop_evidence(sample_config_files):
    paths, _ctx = init_run_workspace(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-preop-missing",
    )
    _write_completed_iteration_artifacts(paths, iteration=1)

    with pytest.raises(ConfigError, match="no iter-03 record"):
        select_converged_preop_iteration(
            workspace_root=sample_config_files,
            run_id="run-preop-missing",
            iteration=3,
            transfer_adapter=FakeFileTransferAdapter(),
            scheduler_adapter=FakeSchedulerAdapter(),
            remote_exec_adapter=FakeRemoteExecAdapter(),
        )


def test_run_postop_fails_without_converged_preop_iteration(sample_config_files):
    _enable_postop_mesh(sample_config_files)
    init_run_workspace(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-postop-no-selection",
    )

    with pytest.raises(ConfigError, match="no converged_preop_iteration"):
        run_postop(
            workspace_root=sample_config_files,
            run_id="run-postop-no-selection",
        )


def test_run_postop_dry_run_writes_plan_without_manifest_submission(sample_config_files):
    paths = _prepare_selected_run(sample_config_files, run_id="run-postop-dry")
    transfer = FakeFileTransferAdapter()
    scheduler = FakeSchedulerAdapter()
    remote = FakeRemoteExecAdapter()

    result = run_postop(
        workspace_root=sample_config_files,
        run_id="run-postop-dry",
        mode=ExecutionMode.DRY_RUN,
        transfer_adapter=transfer,
        scheduler_adapter=scheduler,
        remote_exec_adapter=remote,
    )

    assert result.mode == ExecutionMode.DRY_RUN
    assert result.source_preop_iteration == 3
    assert result.plan_path.exists()
    assert result.local_job_script_path.exists()
    assert len(scheduler.submit_calls) == 1

    manifest = read_manifest(paths.manifest)
    assert manifest.postop_run is None
    assert "postop_plan_files" in manifest.artifacts


def test_run_postop_execute_first_generates_plan_and_records_job(sample_config_files):
    paths = _prepare_selected_run(sample_config_files, run_id="run-postop-exec")
    transfer = FakeFileTransferAdapter()
    scheduler = FakeSchedulerAdapter()
    remote = FakeRemoteExecAdapter()
    scheduler.set_submit_result(
        SubmitResult(
            job_id="777888",
            command=CommandResult(
                argv=["sbatch", "--parsable", "run_postop.sh"],
                returncode=0,
                stdout="777888",
                stderr="",
                dry_run=False,
            ),
        )
    )

    result = run_postop(
        workspace_root=sample_config_files,
        run_id="run-postop-exec",
        mode=ExecutionMode.EXECUTE,
        transfer_adapter=transfer,
        scheduler_adapter=scheduler,
        remote_exec_adapter=remote,
    )

    assert result.submitted_job_id == "777888"
    assert result.plan_path.exists()
    assert len(transfer.ensure_calls) == 4
    assert len(transfer.sync_calls) == 1
    assert len(transfer.push_calls) == 1

    manifest = read_manifest(paths.manifest)
    assert manifest.postop_run is not None
    assert manifest.postop_run.source_preop_iteration == 3
    assert manifest.postop_run.postop_job_id == "777888"
    assert manifest.postop_postprocess is not None
    assert manifest.postop_postprocess.scheduler_job_id == "777888"
