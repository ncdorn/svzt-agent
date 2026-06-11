from __future__ import annotations

import json
from pathlib import Path
import shutil

from svztagent.core.errors import ConfigError
from svztagent.core.manifest import read_manifest
from svztagent.hpc.fake import FakeFileTransferAdapter, FakeRemoteExecAdapter, FakeSchedulerAdapter
from svztagent.hpc.interfaces import ExecutionMode
from svztagent.workflows.adapt import run_adapt
from svztagent.workflows.paraview_viz import submit_adaptation_paraview_viz
from svztagent.workflows.postop import run_postop, select_converged_preop_iteration
from svztagent.workflows.tune_trees import (
    fetch_run_artifacts,
    init_run_workspace,
)

from svztagent.core.manifest import mark_iteration_decision, mark_iteration_submitted, write_manifest


def _switch_to_sibling_repo_layout(workspace: Path) -> dict[str, Path]:
    shutil.rmtree(workspace / "repos")
    sibling_root = workspace.parent
    paths = {}
    for name in ("svzt-agent", "svZeroDTrees", "svZeroDSolver"):
        path = sibling_root / name
        path.mkdir(parents=True, exist_ok=True)
        paths[name] = path
    return paths


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


def _set_patient_paraview_camera(sample_config_files):
    patients_path = sample_config_files / "config" / "patients.yaml"
    payload = patients_path.read_text(encoding="utf-8")
    if "camera_offset_dir" in payload:
        return
    payload = payload.replace(
        '    data_policy: "read_only"\n',
        '    data_policy: "read_only"\n'
        '    postprocess:\n'
        '      paraview_viz:\n'
        '        camera_offset_dir: [0.25, -0.5, 0.75]\n'
        '        camera_view_up: [0.0, 0.0, 1.0]\n',
    )
    patients_path.write_text(payload, encoding="utf-8")


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
    regenerated = (
        f"/scratch/users/ndorn/svzt_runs/{paths.run_dir.name}/iterations/"
        f"iter-{iteration:02d}/results/simplified_zerod_tuned_RRI.json"
    )
    (results / "iteration_decision.json").write_text(
        json.dumps(
            {
                "decision": decision,
                "regenerated_config_path": regenerated,
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


def _prepare_adaptable_run(sample_config_files, *, run_id: str = "run-adapt"):
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
        decision="converged",
        metrics={"mpa_mean": 31.0},
        deltas={"mpa_mean": 0.5},
        regenerated_config_path=(
            f"/scratch/users/ndorn/svzt_runs/{run_id}/iterations/"
            "iter-03/results/simplified_zerod_tuned_RRI.json"
        ),
    )
    write_manifest(manifest, paths.manifest)
    _write_completed_iteration_artifacts(paths, iteration=3, decision="converged")

    transfer = FakeFileTransferAdapter()
    scheduler = FakeSchedulerAdapter()
    remote = FakeRemoteExecAdapter()
    select_converged_preop_iteration(
        workspace_root=sample_config_files,
        run_id=run_id,
        iteration=3,
        transfer_adapter=transfer,
        scheduler_adapter=scheduler,
        remote_exec_adapter=remote,
    )
    run_postop(
        workspace_root=sample_config_files,
        run_id=run_id,
        mode=ExecutionMode.EXECUTE,
        transfer_adapter=transfer,
        scheduler_adapter=scheduler,
        remote_exec_adapter=remote,
    )
    return paths


def test_run_adapt_records_submission_and_inflow_provenance(sample_config_files):
    paths = _prepare_adaptable_run(sample_config_files, run_id="run-adapt-record")
    transfer = FakeFileTransferAdapter()
    scheduler = FakeSchedulerAdapter()
    remote = FakeRemoteExecAdapter()

    result = run_adapt(
        workspace_root=sample_config_files,
        run_id="run-adapt-record",
        model="M2",
        mode=ExecutionMode.EXECUTE,
        transfer_adapter=transfer,
        scheduler_adapter=scheduler,
        remote_exec_adapter=remote,
    )

    assert result.model == "M2"
    manifest = read_manifest(paths.manifest)
    assert manifest.adaptation_runs
    record = manifest.adaptation_runs[-1]
    assert record.model == "M2"
    assert record.parameter_set == "default"
    assert record.inflow_provenance.source_path.endswith("/inflow.csv")
    assert record.comparison_path is not None
    assert manifest.execution.submitted_job_id == record.scheduler_job_id


def test_run_adapt_prefers_regenerated_reduced_seed_for_adaptation(sample_config_files):
    _prepare_adaptable_run(sample_config_files, run_id="run-adapt-reduced-seed")

    result = run_adapt(
        workspace_root=sample_config_files,
        run_id="run-adapt-reduced-seed",
        model="M1",
        mode=ExecutionMode.DRY_RUN,
        transfer_adapter=FakeFileTransferAdapter(),
        scheduler_adapter=FakeSchedulerAdapter(),
        remote_exec_adapter=FakeRemoteExecAdapter(),
    )

    script_text = result.local_job_script_path.read_text(encoding="utf-8")
    assert (
        "/iterations/iter-03/results/simplified_zerod_tuned_RRI.json" in script_text
    )


def test_run_adapt_requires_completed_postop(sample_config_files):
    _prepare_adaptable_run(sample_config_files, run_id="run-adapt-missing-postop")
    manifest = read_manifest(
        sample_config_files / "runs" / "run-adapt-missing-postop" / "manifest.yaml"
    )
    manifest.postop_run = None
    write_manifest(
        manifest,
        sample_config_files / "runs" / "run-adapt-missing-postop" / "manifest.yaml",
    )

    try:
        run_adapt(
            workspace_root=sample_config_files,
            run_id="run-adapt-missing-postop",
            model="M1",
            mode=ExecutionMode.DRY_RUN,
            transfer_adapter=FakeFileTransferAdapter(),
            scheduler_adapter=FakeSchedulerAdapter(),
            remote_exec_adapter=FakeRemoteExecAdapter(),
        )
    except ConfigError as exc:
        assert "postop_run" in str(exc)
    else:
        raise AssertionError("expected ConfigError for missing postop run")


def test_fetch_run_artifacts_includes_adaptation_directories(sample_config_files):
    paths = _prepare_adaptable_run(sample_config_files, run_id="run-adapt-fetch")
    transfer = FakeFileTransferAdapter()
    scheduler = FakeSchedulerAdapter()
    remote = FakeRemoteExecAdapter()
    run_adapt(
        workspace_root=sample_config_files,
        run_id="run-adapt-fetch",
        model="M1",
        mode=ExecutionMode.EXECUTE,
        transfer_adapter=transfer,
        scheduler_adapter=scheduler,
        remote_exec_adapter=remote,
    )

    fetch_transfer = FakeFileTransferAdapter()
    fetch_run_artifacts(
        workspace_root=sample_config_files,
        run_id="run-adapt-fetch",
        mode=ExecutionMode.DRY_RUN,
        transfer_adapter=fetch_transfer,
    )

    pulled_remote_dirs = [remote_dir for _local, remote_dir, *_rest in fetch_transfer.sync_calls]
    assert any("/adaptation/" in remote_dir and remote_dir.endswith("/logs") for remote_dir in pulled_remote_dirs)
    assert any("/adaptation/" in remote_dir and remote_dir.endswith("/results") for remote_dir in pulled_remote_dirs)


def test_run_adapt_supports_sibling_repo_layout(sample_config_files):
    sibling_paths = _switch_to_sibling_repo_layout(sample_config_files)
    paths = _prepare_adaptable_run(sample_config_files, run_id="run-adapt-sibling")
    transfer = FakeFileTransferAdapter()
    scheduler = FakeSchedulerAdapter()
    remote = FakeRemoteExecAdapter()

    result = run_adapt(
        workspace_root=sample_config_files,
        run_id="run-adapt-sibling",
        model="M1",
        mode=ExecutionMode.DRY_RUN,
        transfer_adapter=transfer,
        scheduler_adapter=scheduler,
        remote_exec_adapter=remote,
    )

    manifest = read_manifest(paths.manifest)
    assert result.run_id == "run-adapt-sibling"
    assert manifest.repos["svzt_agent"] == str(sibling_paths["svzt-agent"].resolve())
    assert manifest.repos["svZeroDTrees"] == str(sibling_paths["svZeroDTrees"].resolve())
    assert manifest.repos["svZeroDSolver"] == str(sibling_paths["svZeroDSolver"].resolve())


def test_run_adapt_renders_progress_logging(sample_config_files):
    _prepare_adaptable_run(sample_config_files, run_id="run-adapt-logging")

    result = run_adapt(
        workspace_root=sample_config_files,
        run_id="run-adapt-logging",
        model="M1",
        mode=ExecutionMode.DRY_RUN,
        transfer_adapter=FakeFileTransferAdapter(),
        scheduler_adapter=FakeSchedulerAdapter(),
        remote_exec_adapter=FakeRemoteExecAdapter(),
    )

    script_text = result.local_job_script_path.read_text(encoding="utf-8")
    assert "adaptation_manager_log.jsonl" in script_text
    assert 'status_path = remote_results_dir / "adaptation_status.json"' in script_text
    assert '"adaptation_started"' in script_text
    assert '"adaptation_completed"' in script_text
    assert '"adaptation_summary_recorded"' in script_text
    assert '"adapted_cmm_submitted"' in script_text
    assert '"paraview_viz_submitted"' in script_text
    assert '"paraview_viz_skipped"' in script_text
    assert '"manager_failed"' in script_text


def test_run_adapt_renders_adaptation_paraview_submission(sample_config_files):
    _set_patient_paraview_camera(sample_config_files)
    _prepare_adaptable_run(sample_config_files, run_id="run-adapt-paraview")

    result = run_adapt(
        workspace_root=sample_config_files,
        run_id="run-adapt-paraview",
        model="M1",
        mode=ExecutionMode.DRY_RUN,
        transfer_adapter=FakeFileTransferAdapter(),
        scheduler_adapter=FakeSchedulerAdapter(),
        remote_exec_adapter=FakeRemoteExecAdapter(),
    )

    script_text = result.local_job_script_path.read_text(encoding="utf-8")
    assert 'paraview_enabled = True' in script_text
    assert 'paraview_viz_submission_started' in script_text
    assert 'paraview_viz_skipped' in script_text
    assert 'paraview_viz_submission.json' in script_text
    assert '/adaptation/from-iter-03/m1/results/paraview_viz' in script_text
    assert script_text.count("camera_offset_dir=[0.25, -0.5, 0.75]") == 2
    assert script_text.count("camera_view_up=[0.0, 0.0, 1.0]") == 2


def test_submit_adaptation_paraview_viz_records_manifest(sample_config_files):
    paths = _prepare_adaptable_run(sample_config_files, run_id="run-adapt-pviz-submit")
    transfer = FakeFileTransferAdapter()
    scheduler = FakeSchedulerAdapter()
    remote = FakeRemoteExecAdapter()
    run_adapt(
        workspace_root=sample_config_files,
        run_id="run-adapt-pviz-submit",
        model="M1",
        mode=ExecutionMode.EXECUTE,
        transfer_adapter=transfer,
        scheduler_adapter=scheduler,
        remote_exec_adapter=remote,
    )

    pviz_transfer = FakeFileTransferAdapter()
    pviz_scheduler = FakeSchedulerAdapter()
    pviz_remote = FakeRemoteExecAdapter()
    result = submit_adaptation_paraview_viz(
        workspace_root=sample_config_files,
        run_id="run-adapt-pviz-submit",
        model="M1",
        transfer_adapter=pviz_transfer,
        scheduler_adapter=pviz_scheduler,
        remote_exec_adapter=pviz_remote,
    )

    assert result.stage == "adaptation-m1"
    manifest = read_manifest(paths.manifest)
    assert manifest.paraview_viz_runs
    record = manifest.paraview_viz_runs[-1]
    assert record.stage == "adaptation-m1"
    assert record.remote_dir.endswith("/adaptation/from-iter-03/m1/results/paraview_viz")
