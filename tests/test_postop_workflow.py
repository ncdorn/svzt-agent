from __future__ import annotations

import json
from pathlib import Path
import shutil

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
from svztagent.workflows.postprocess import _load_stage_target_payload
from svztagent.workflows.postop import run_postop, select_converged_preop_iteration
from svztagent.workflows.tune_trees import init_run_workspace


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
        regenerated_config_path=(
            f"/scratch/users/ndorn/svzt_runs/{run_id}/iterations/"
            "iter-03/results/simplified_zerod_tuned_RRI.json"
        ),
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
    assert manifest.converged_preop_iteration.remote_tuned_zerod_config.endswith(
        "/iterations/iter-03/results/simplified_zerod_tuned_RRI.json"
    )
    assert manifest.selected_preop_postprocess is not None
    assert manifest.selected_preop_postprocess.source_preop_iteration == 3


def test_load_stage_target_payload_uses_canonical_mpa_pressure_key(sample_config_files):
    config_dir = sample_config_files / "config"
    (config_dir / "clinical_targets.yaml").write_text(
        """
clinical_targets:
  preop:
    patients:
      TST-STAN-x:
        mpa_pressure: [38.0, 10.0, 23.0]
        rpa_split: 0.5
""".strip()
        + "\n",
        encoding="utf-8",
    )
    payload = _load_stage_target_payload(
        sample_config_files,
        patient_alias="TST-STAN-x",
        stage="preop",
    )

    assert payload is not None
    assert "mpa_pressure" in payload
    assert "mpa_p" not in payload
    assert payload["rpa_split"] == pytest.approx(0.5)


def test_preop_select_submits_selected_preop_postprocess(sample_config_files):
    _enable_postop_mesh(sample_config_files)
    _set_patient_paraview_camera(sample_config_files)
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
    assert len(transfer.ensure_calls) == 7
    assert len(scheduler.submit_calls) == 1
    manifest = read_manifest(paths.manifest)
    assert manifest.selected_preop_postprocess is not None
    assert manifest.paraview_viz_runs
    assert manifest.paraview_viz_runs[-1].stage == "preop"
    assert manifest.paraview_viz_runs[-1].scheduler_job_id is not None
    remote_job_script = Path(manifest.selected_preop_postprocess.remote_job_script_path)
    script_text = (
        paths.run_dir / "iterations" / "iter-02" / "postprocess" / "run_postprocess.sh"
    ).read_text(encoding="utf-8")
    assert f"#SBATCH --chdir={remote_job_script.parent}" in script_text
    assert f"#SBATCH --output={remote_job_script.parent}/logs/slurm-%j.out" in script_text
    assert "#SBATCH --cpus-per-task=4" in script_text
    assert "#SBATCH --mem=64G" in script_text
    assert '"resistance_map_workers": 4' in script_text
    assert "def _run_postprocess_suite_with_optional_camera(" in script_text
    assert '"simulation_dir":' in script_text
    assert 'if [0.25, -0.5, 0.75] is not None:' in script_text
    assert 'postprocess_kwargs["camera_offset_dir"] = [0.25, -0.5, 0.75]' in script_text
    assert 'if [0.0, 0.0, 1.0] is not None:' in script_text
    assert 'postprocess_kwargs["camera_view_up"] = [0.0, 0.0, 1.0]' in script_text
    assert "_run_postprocess_suite_with_optional_camera(" in script_text
    assert "_repair_failed_suite_result_if_outputs_exist(" in script_text
    assert "_write_stacked_centerline_timeseries(" in script_text
    assert "centerline_timeseries_last_cycle.vtp" in script_text
    assert "centerline_timeseries_last_cycle_metadata.json" in script_text
    assert "_preserve_intermediate_centerlines" in script_text
    assert "_cleanup_intermediate_centerlines" in script_text


def test_preop_select_skips_duplicate_paraview_submission(sample_config_files):
    _enable_postop_mesh(sample_config_files)
    _set_patient_paraview_camera(sample_config_files)
    paths, _ctx = init_run_workspace(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-preop-no-dup-pviz",
    )
    manifest = read_manifest(paths.manifest)
    manifest = mark_iteration_submitted(
        manifest,
        iteration=2,
        tune_job_id="990002",
        local_dir=str(paths.run_dir / "iterations" / "iter-02"),
        remote_dir="/scratch/users/ndorn/svzt_runs/run-preop-no-dup-pviz/iterations/iter-02",
        job_script_path="/scratch/users/ndorn/svzt_runs/run-preop-no-dup-pviz/iterations/iter-02/run_tune_iter.sh",
    )
    write_manifest(manifest, paths.manifest)
    _write_completed_iteration_artifacts(paths, iteration=2, decision="converged")

    first_transfer = FakeFileTransferAdapter()
    first_scheduler = FakeSchedulerAdapter()
    first_remote = FakeRemoteExecAdapter()
    select_converged_preop_iteration(
        workspace_root=sample_config_files,
        run_id="run-preop-no-dup-pviz",
        iteration=2,
        transfer_adapter=first_transfer,
        scheduler_adapter=first_scheduler,
        remote_exec_adapter=first_remote,
    )

    second_transfer = FakeFileTransferAdapter()
    second_scheduler = FakeSchedulerAdapter()
    second_remote = FakeRemoteExecAdapter()
    select_converged_preop_iteration(
        workspace_root=sample_config_files,
        run_id="run-preop-no-dup-pviz",
        iteration=2,
        transfer_adapter=second_transfer,
        scheduler_adapter=second_scheduler,
        remote_exec_adapter=second_remote,
    )

    manifest = read_manifest(paths.manifest)
    assert len(manifest.paraview_viz_runs) == 1
    assert len(second_scheduler.submit_calls) == 1


def test_preop_select_can_skip_selected_preop_postprocess(sample_config_files):
    _enable_postop_mesh(sample_config_files)
    paths, _ctx = init_run_workspace(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-preop-skip-postprocess",
    )
    manifest = read_manifest(paths.manifest)
    manifest = mark_iteration_submitted(
        manifest,
        iteration=2,
        tune_job_id="990002",
        local_dir=str(paths.run_dir / "iterations" / "iter-02"),
        remote_dir="/scratch/users/ndorn/svzt_runs/run-preop-skip-postprocess/iterations/iter-02",
        job_script_path="/scratch/users/ndorn/svzt_runs/run-preop-skip-postprocess/iterations/iter-02/run_tune_iter.sh",
    )
    write_manifest(manifest, paths.manifest)
    _write_completed_iteration_artifacts(paths, iteration=2, decision="converged")

    transfer = FakeFileTransferAdapter()
    scheduler = FakeSchedulerAdapter()
    remote = FakeRemoteExecAdapter()
    result = select_converged_preop_iteration(
        workspace_root=sample_config_files,
        run_id="run-preop-skip-postprocess",
        iteration=2,
        submit_postprocess=False,
        transfer_adapter=transfer,
        scheduler_adapter=scheduler,
        remote_exec_adapter=remote,
    )

    assert result.postprocess_job_id is None
    assert scheduler.submit_calls == []
    assert transfer.ensure_calls == []
    updated = read_manifest(paths.manifest)
    assert updated.selected_preop_postprocess is None


def test_preop_select_submit_command_uses_selected_postprocess_resource_overrides(sample_config_files):
    _enable_postop_mesh(sample_config_files)
    paths, _ctx = init_run_workspace(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-preop-postprocess-submit-overrides",
    )
    manifest = read_manifest(paths.manifest)
    manifest = mark_iteration_submitted(
        manifest,
        iteration=2,
        tune_job_id="990002",
        local_dir=str(paths.run_dir / "iterations" / "iter-02"),
        remote_dir="/scratch/users/ndorn/svzt_runs/run-preop-postprocess-submit-overrides/iterations/iter-02",
        job_script_path="/scratch/users/ndorn/svzt_runs/run-preop-postprocess-submit-overrides/iterations/iter-02/run_tune_iter.sh",
    )
    write_manifest(manifest, paths.manifest)
    _write_completed_iteration_artifacts(paths, iteration=2, decision="converged")
    transfer = FakeFileTransferAdapter()
    remote = FakeRemoteExecAdapter()

    result = select_converged_preop_iteration(
        workspace_root=sample_config_files,
        run_id="run-preop-postprocess-submit-overrides",
        iteration=2,
        transfer_adapter=transfer,
        remote_exec_adapter=remote,
    )

    assert result.postprocess_job_id == "dryrun-run-preop-postprocess-submit-overrides"
    submit_calls = [command for command, _cwd in remote.calls if command and command[0] == "sbatch"]
    assert len(submit_calls) == 2
    submit_call = submit_calls[0]
    assert "--cpus-per-task" in submit_call
    assert submit_call[submit_call.index("--cpus-per-task") + 1] == "4"
    assert "--mem" in submit_call
    assert submit_call[submit_call.index("--mem") + 1] == "64G"
    assert "pviz" in submit_calls[1][submit_calls[1].index("--job-name") + 1]


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
    manifest = read_manifest(paths.manifest)
    manifest.remote["threed_defaults"]["prestress_file"] = "/oak/example/preop-prestress/result_009.vtu"
    write_manifest(manifest, paths.manifest)
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
    script_text = result.local_job_script_path.read_text(encoding="utf-8")
    assert f"#SBATCH --chdir={Path(result.remote_job_script_path).parent}" in script_text
    assert f"#SBATCH --output={Path(result.remote_job_script_path).parent}/logs/slurm-%j.out" in script_text
    assert "#SBATCH --nodes=1" in script_text
    assert "#SBATCH --cpus-per-task=24" in script_text
    assert "#SBATCH --ntasks-per-node=24" not in script_text
    assert "_submit_job" in script_text
    assert "_wait_for_completion" in script_text
    assert "run_postop_postprocess.sh" in script_text
    assert 'solver_execution["mode"] = "slurm"' in script_text
    assert 'solver_execution["submit_command"] = "bash"' in script_text
    assert "strip_existing_launch_tail = False" in script_text
    assert 'stripped.startswith("cd ")' in script_text
    assert 'threed_config.pop("prestress_file", None)' in script_text
    assert 'threed_config.pop("prestress_file_path", None)' in script_text
    assert "resolved_inflow_path = " in script_text
    assert "def _sync_postop_inflow(sim_dir: SimulationDirectory) -> None:" in script_text
    assert 'inflow_path=resolved_inflow_path,' in script_text
    assert "full_inflow = getattr(inflow_helper, \"inflow_3d\", None)" in script_text
    assert "sim_dir.zerod_config.set_inflow(full_inflow)" in script_text
    assert "sim_dir.svzerod_3Dcoupling.set_inflow(full_inflow)" in script_text
    assert 'canonical_coupler = Path("/scratch/users/ndorn/svzt_runs/run-postop-dry/iterations/iter-03/results/svzerod_3Dcoupling.json")' in script_text
    assert 'threed_coupler=str(canonical_coupler)' in script_text
    assert "_sync_postop_inflow(sim)" in script_text
    assert "existing = _latest_result_vtu(prestress_root)" in script_text
    assert "if existing is not None:" in script_text
    assert 'if str(threed_config.get("wall_model", "rigid")).lower() == "deformable":' in script_text
    assert 'threed_config["prestress_file_path"] = str(_generate_postop_prestress_file())' in script_text
    assert '"wall_model": "deformable"' in script_text
    assert '"tissue_support"' in script_text
    assert 'nodes=1,' in script_text
    assert 'procs_per_node=1,' in script_text
    assert 'nodes=int(threed_config.get("nodes", 3))' in script_text
    assert 'procs_per_node=int(threed_config.get("procs_per_node", 24))' in script_text
    assert "srun -N {nodes} -n {total_tasks}" in script_text
    assert "paraview_viz_submission.json" in script_text
    assert "paraview_viz_job_id" in script_text
    assert "_write_stacked_centerline_timeseries(" in script_text
    assert "centerline_timeseries_last_cycle_vtp" in script_text

    manifest = read_manifest(paths.manifest)
    assert manifest.postop_run is None
    assert "postop_plan_files" in manifest.artifacts


def test_run_postop_execute_first_generates_plan_and_records_job(sample_config_files):
    _set_patient_paraview_camera(sample_config_files)
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
    assert manifest.paraview_viz_runs
    assert manifest.paraview_viz_runs[-1].stage == "postop"
    assert manifest.paraview_viz_runs[-1].scheduler_job_id is None
    script_text = result.local_job_script_path.read_text(encoding="utf-8")
    assert 'resistance_map_workers' in script_text
    assert "#SBATCH --cpus-per-task=24" in script_text
    assert "#SBATCH --nodes=1" in script_text
    assert "#SBATCH --ntasks-per-node=24" not in script_text
    assert "_submit_job" in script_text
    assert "_wait_for_completion" in script_text
    assert "run_postop_postprocess.sh" in script_text
    assert 'solver_execution["mode"] = "slurm"' in script_text
    assert 'solver_execution["submit_command"] = "bash"' in script_text
    assert "strip_existing_launch_tail = False" in script_text
    assert 'threed_config.pop("prestress_file", None)' in script_text
    assert "def _sync_postop_inflow(sim_dir: SimulationDirectory) -> None:" in script_text
    assert "sim_dir.zerod_config.set_inflow(full_inflow)" in script_text
    assert 'threed_coupler=str(canonical_coupler)' in script_text
    assert "_sync_postop_inflow(sim)" in script_text
    assert 'threed_config["prestress_file_path"] = str(_generate_postop_prestress_file())' in script_text
    assert "paraview_viz_submission.json" in script_text
    assert "ParaView visualization job submitted" in script_text
    assert "_write_stacked_centerline_timeseries(" in script_text
    assert "centerline_timeseries_last_cycle_metadata.json" in script_text


def test_run_postop_refreshes_stale_selected_tuned_config_from_local_decision(sample_config_files):
    paths = _prepare_selected_run(sample_config_files, run_id="run-postop-stale-selection")
    manifest = read_manifest(paths.manifest)
    assert manifest.converged_preop_iteration is not None
    manifest.converged_preop_iteration = manifest.converged_preop_iteration.model_copy(
        update={
            "remote_tuned_zerod_config": (
                "/scratch/users/ndorn/svzt_runs/run-postop-stale-selection/"
                "iterations/iter-03/results/svzerod_3d_coupling_tuned.json"
            )
        }
    )
    write_manifest(manifest, paths.manifest)

    result = run_postop(
        workspace_root=sample_config_files,
        run_id="run-postop-stale-selection",
        mode=ExecutionMode.DRY_RUN,
        transfer_adapter=FakeFileTransferAdapter(),
        scheduler_adapter=FakeSchedulerAdapter(),
        remote_exec_adapter=FakeRemoteExecAdapter(),
    )

    updated = read_manifest(paths.manifest)
    assert updated.converged_preop_iteration is not None
    assert updated.converged_preop_iteration.remote_tuned_zerod_config.endswith(
        "/iterations/iter-03/results/simplified_zerod_tuned_RRI.json"
    )
    script_text = result.local_job_script_path.read_text(encoding="utf-8")
    assert (
        'tuned_zerod = Path("/scratch/users/ndorn/svzt_runs/run-postop-stale-selection/'
        'iterations/iter-03/results/simplified_zerod_tuned_RRI.json")'
    ) in script_text


def test_run_postop_supports_sibling_repo_layout(sample_config_files):
    sibling_paths = _switch_to_sibling_repo_layout(sample_config_files)
    paths = _prepare_selected_run(sample_config_files, run_id="run-postop-sibling")
    transfer = FakeFileTransferAdapter()
    scheduler = FakeSchedulerAdapter()
    remote = FakeRemoteExecAdapter()

    result = run_postop(
        workspace_root=sample_config_files,
        run_id="run-postop-sibling",
        mode=ExecutionMode.DRY_RUN,
        transfer_adapter=transfer,
        scheduler_adapter=scheduler,
        remote_exec_adapter=remote,
    )

    manifest = read_manifest(paths.manifest)
    script_text = result.local_job_script_path.read_text(encoding="utf-8")
    assert result.run_id == "run-postop-sibling"
    assert manifest.repos["svzt_agent"] == str(sibling_paths["svzt-agent"].resolve())
    assert manifest.repos["svZeroDTrees"] == str(sibling_paths["svZeroDTrees"].resolve())
    assert manifest.repos["svZeroDSolver"] == str(sibling_paths["svZeroDSolver"].resolve())
    assert '"wall_model": "deformable"' in script_text
