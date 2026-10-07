from __future__ import annotations

from pathlib import Path
import re
import shlex
import shutil

import pytest

from svztagent.core.errors import ConfigError
from svztagent.core.manifest import read_manifest, record_lifecycle_transition, write_manifest
from svztagent.core.state import RunLifecycleState
from svztagent.hpc.fake import (
    FakeFileTransferAdapter,
    FakeRemoteExecAdapter,
    FakeSchedulerAdapter,
)
from svztagent.hpc.interfaces import CommandResult, ExecutionMode
from svztagent.workflows.tune_trees import _iteration_impedance_config, run_tune_trees


def _switch_to_sibling_repo_layout(workspace: Path) -> dict[str, Path]:
    shutil.rmtree(workspace / "repos")
    sibling_root = workspace.parent
    paths = {}
    for name in ("svzt-agent", "svZeroDTrees", "svZeroDSolver"):
        path = sibling_root / name
        path.mkdir(parents=True, exist_ok=True)
        paths[name] = path
    return paths


def test_iteration_impedance_config_nonzero_diameter_scale_disables_mean_assignment():
    rendered = _iteration_impedance_config(
        {
            "tuning_model": "rri",
            "diameter_scale": 0.1,
            "use_mean": True,
        },
        iteration=1,
    )

    assert rendered["diameter_scale"] == 0.1
    assert rendered["use_mean"] is False


def test_iteration_impedance_config_omits_unset_outlet_mapping_keys_for_rri():
    rendered = _iteration_impedance_config(
        {
            "tuning_model": "rri",
            "outlet_mapping_mode": None,
            "outlet_mapping": None,
            "outlet_mapping_centerline": None,
        },
        iteration=1,
    )

    assert "outlet_mapping_mode" not in rendered
    assert "outlet_mapping" not in rendered
    assert "outlet_mapping_centerline" not in rendered


def test_iteration_impedance_config_passes_full_pa_mapping_centerline():
    rendered = _iteration_impedance_config(
        {
            "tuning_model": "full_pa",
            "outlet_mapping_mode": "auto",
            "outlet_mapping": None,
            "outlet_mapping_centerline": "/oak/patient/centerlines.vtp",
        },
        iteration=2,
    )

    assert rendered["outlet_mapping_mode"] == "auto"
    assert rendered["outlet_mapping_centerline"] == "/oak/patient/centerlines.vtp"
    assert "outlet_mapping" not in rendered


def test_iteration_impedance_config_legacy_rri_drops_full_pa_mapping():
    rendered = _iteration_impedance_config(
        {
            "tuning_model": "full_pa",
            "outlet_mapping_mode": "centerline",
            "outlet_mapping_centerline": "/oak/patient/centerlines.vtp",
        },
        iteration=2,
        seed_policy="legacy_rri_after_first",
    )

    assert rendered["tuning_model"] == "rri"
    assert "outlet_mapping_mode" not in rendered
    assert "outlet_mapping_centerline" not in rendered


def test_iteration_impedance_config_forwards_objective_tree_policy_unchanged():
    policy = {"use_mean": True, "reference_diameter": "conductance_matched"}
    rendered = _iteration_impedance_config(
        {
            "tuning_model": "full_pa",
            "use_mean": False,
            "diameter_scale": 0.2,
            "objective_tree_policy": policy,
        },
        iteration=1,
    )

    assert rendered["objective_tree_policy"] == policy


def test_iteration_impedance_config_forwards_stopping_for_every_tuning_model():
    stopping = {"enabled": True, "target_tolerance": 0.025, "maxfev": 200}
    for iteration, seed_policy in ((1, "calibrated_full_pa"), (2, "legacy_rri_after_first")):
        rendered = _iteration_impedance_config(
            {"tuning_model": "full_pa", "stopping": stopping},
            iteration=iteration,
            seed_policy=seed_policy,
        )
        assert rendered["stopping"] == stopping


def test_iteration_impedance_config_omits_unset_objective_tree_policy():
    rendered = _iteration_impedance_config(
        {"tuning_model": "full_pa", "objective_tree_policy": None},
        iteration=1,
    )

    assert "objective_tree_policy" not in rendered


def test_iteration_impedance_config_legacy_rri_drops_objective_tree_policy():
    rendered = _iteration_impedance_config(
        {"tuning_model": "full_pa", "objective_tree_policy": {"use_mean": True}},
        iteration=2,
        seed_policy="legacy_rri_after_first",
    )

    assert rendered["tuning_model"] == "rri"
    assert "objective_tree_policy" not in rendered


def test_run_tune_dry_run_updates_manifest_and_previews(sample_config_files):
    result = run_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-dry-001",
        mode=ExecutionMode.DRY_RUN,
    )

    assert result.run_id == "run-dry-001"
    assert result.iteration == 1
    assert result.mode == ExecutionMode.DRY_RUN
    assert result.submitted_job_id == "dryrun-run-dry-001"
    assert result.local_job_script_path.exists()

    manifest = read_manifest(sample_config_files / "runs" / "run-dry-001" / "manifest.yaml")
    assert manifest.execution.submitted_job_id == "dryrun-run-dry-001"
    assert manifest.execution.job_script_path.endswith(
        "/run-dry-001/iterations/iter-01/run_tune_iter.sh"
    )
    assert manifest.execution.plan_path.endswith("/run-dry-001/execution_plan.yaml")
    assert manifest.jobs[0]["mode"] == "dry_run"


def test_run_tune_default_submission_matches_rendered_tune_cpus(sample_config_files):
    defaults_path = sample_config_files / "config" / "defaults.yaml"
    defaults_path.write_text(
        defaults_path.read_text(encoding="utf-8").replace(
            'cpus: "<count>"', 'cpus: "4"', 1
        ),
        encoding="utf-8",
    )

    result = run_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-dry-default-cpus",
        mode=ExecutionMode.DRY_RUN,
    )

    rendered_script = result.local_job_script_path.read_text(encoding="utf-8")
    submit_preview = next(
        command
        for command in result.command_previews
        if command[0] == "ssh" and "sbatch" in command[-1]
    )
    submit_command = shlex.split(submit_preview[-1])
    cpus_index = submit_command.index("--cpus-per-task")
    assert submit_command[cpus_index + 1] == "24"
    assert "#SBATCH --cpus-per-task=24" in rendered_script


def test_run_tune_dry_run_supports_sibling_repo_layout_without_changing_run_contract(
    sample_config_files,
):
    sibling_paths = _switch_to_sibling_repo_layout(sample_config_files)

    result = run_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-dry-sibling-layout",
        mode=ExecutionMode.DRY_RUN,
    )

    manifest = read_manifest(
        sample_config_files / "runs" / "run-dry-sibling-layout" / "manifest.yaml"
    )
    expected_runs_root = (sample_config_files / "remote_runs").as_posix()
    expected_remote_run_dir = f"{expected_runs_root}/run-dry-sibling-layout"

    assert manifest.repos["svzt_agent"] == str(sibling_paths["svzt-agent"].resolve())
    assert manifest.repos["svZeroDTrees"] == str(sibling_paths["svZeroDTrees"].resolve())
    assert manifest.repos["svZeroDSolver"] == str(sibling_paths["svZeroDSolver"].resolve())
    assert manifest.local_paths.run_dir.endswith("/runs/run-dry-sibling-layout")
    assert manifest.local_paths.iterations.endswith("/runs/run-dry-sibling-layout/iterations")
    assert manifest.remote["runs_root"] == expected_runs_root
    assert manifest.remote["remote_run_dir"] == expected_remote_run_dir
    assert manifest.execution.remote_run_dir == expected_remote_run_dir
    assert result.remote_run_dir == expected_remote_run_dir


def test_run_tune_iter_dry_run_can_skip_zerod_tuning(sample_config_files):
    result = run_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-dry-skip-0d",
        mode=ExecutionMode.DRY_RUN,
        skip_zerod_tuning=True,
    )

    rendered_script = result.local_job_script_path.read_text(encoding="utf-8")
    assert "skip_zerod_tuning = json.loads" in rendered_script
    assert 'skip_zerod_tuning = json.loads(r\'\'\'true\'\'\')' in rendered_script
    assert "svzerodsolver_build_dir" in rendered_script
    assert "/home/users/ndorn/svZeroDSolver-build" in rendered_script
    assert 'log["steps"].append("0d_tuning_skipped")' in rendered_script
    assert 'remote_results_dir / "svzerod_3d_coupling_tuned.json"' in rendered_script
    assert 'remote_results_dir / "svzerod_3Dcoupling.json"' in rendered_script
    assert "skip_zerod_tuning requested but required tuning artifacts are missing" in rendered_script
    assert "postop_ready_for_explicit_submission" in rendered_script
    assert "postop_submission_failed" not in rendered_script


def test_run_tune_iter_dry_run_restart_preserves_completed_lifecycle(sample_config_files):
    run_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-dry-completed-restart",
        mode=ExecutionMode.DRY_RUN,
    )
    manifest_path = sample_config_files / "runs" / "run-dry-completed-restart" / "manifest.yaml"
    manifest = read_manifest(manifest_path)
    manifest = record_lifecycle_transition(manifest, to_state=RunLifecycleState.SUBMITTED)
    manifest = record_lifecycle_transition(manifest, to_state=RunLifecycleState.COMPLETED)
    write_manifest(manifest, manifest_path)

    run_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-dry-completed-restart",
        mode=ExecutionMode.DRY_RUN,
        skip_zerod_tuning=True,
    )

    manifest = read_manifest(manifest_path)
    assert manifest.execution.lifecycle_state == RunLifecycleState.COMPLETED.value


def test_run_tune_dry_run_emits_progress_updates(sample_config_files):
    messages: list[str] = []

    run_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-dry-progress",
        mode=ExecutionMode.DRY_RUN,
        progress_callback=messages.append,
    )

    assert messages
    assert messages[0] == "[svzt] Initializing tune run run-dry-progress for patient TST-STAN-x"
    assert "[svzt] Loading execution plan" in messages
    assert "[svzt] Staging inputs for iteration 1" in messages
    assert "[svzt] Previewing scheduler submission" in messages


def test_run_tune_dry_run_iteration1_stages_seed_from_yaml_config(sample_config_files):
    run_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-dry-iter1-seed",
        mode=ExecutionMode.DRY_RUN,
    )

    staged_seed = (
        sample_config_files
        / "runs"
        / "run-dry-iter1-seed"
        / "iterations"
        / "iter-01"
        / "inputs"
        / "simplified_nonlinear_zerod.json"
    )
    assert staged_seed.exists()
    assert staged_seed.read_text(encoding="utf-8") == "{\"default_seed\": true}"

    staged_inflow = (
        sample_config_files
        / "runs"
        / "run-dry-iter1-seed"
        / "iterations"
        / "iter-01"
        / "inputs"
        / "inflow.csv"
    )
    assert staged_inflow.exists()
    assert staged_inflow.read_text(encoding="utf-8") == "t,q\n0,0\n"


def test_run_tune_dry_run_iteration1_missing_seed_path_leaves_seed_for_remote_driver(
    sample_config_files,
):
    (sample_config_files / "config" / "defaults.yaml").write_text(
        """
defaults:
  tuning:
    iteration1_seed:
      source: "path"
      path: "missing_seed.json"
""".strip()
        + "\n",
        encoding="utf-8",
    )
    result = run_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-dry-iter1-seed-generate",
        mode=ExecutionMode.DRY_RUN,
    )

    staged_seed = (
        sample_config_files
        / "runs"
        / "run-dry-iter1-seed-generate"
        / "iterations"
        / "iter-01"
        / "inputs"
        / "simplified_nonlinear_zerod.json"
    )
    assert not staged_seed.exists()
    rendered_script = result.local_job_script_path.read_text(encoding="utf-8")
    assert "if not staged_seed_path.exists():" in rendered_script
    assert "_generate_iteration_seed(staged_seed_path)" in rendered_script


def test_run_tune_dry_run_generate_seed_skips_local_generation_when_assets_are_remote_only(
    sample_config_files,
):
    (sample_config_files / "config" / "clusters.yaml").write_text(
        """
clusters:
  - name: "sherlock"
    host: "sherlock.stanford.edu"
    user: "ndorn"
    scheduler:
      type: "slurm"
    executables:
      svfsiplus_path: "/home/users/ndorn/svMP-build/svMultiPhysics-build/bin/svmultiphysics"
      svzerodsolver_build_dir: "/home/users/ndorn/svZeroDSolver-build"
    remote_roots:
      permanent_data_root: "/oak/stanford/groups/amarsden/ndorn/PPAS-study/tof-stent"
      runs_root: "/scratch/users/ndorn/svzt_runs"
""".strip()
        + "\n",
        encoding="utf-8",
    )
    (sample_config_files / "config" / "patients.yaml").write_text(
        """
patients:
  - alias: "TST-STAN-x"
    permanent_remote_path: "/oak/stanford/groups/amarsden/ndorn/PPAS-study/tof-stent/TST-STAN-x"
    data_policy: "read_only"
    tuning:
      iteration1_seed:
        source: "generate"
""".strip()
        + "\n",
        encoding="utf-8",
    )

    result = run_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-dry-remote-generate-seed",
        mode=ExecutionMode.DRY_RUN,
    )

    staged_seed = (
        sample_config_files
        / "runs"
        / "run-dry-remote-generate-seed"
        / "iterations"
        / "iter-01"
        / "inputs"
        / "simplified_nonlinear_zerod.json"
    )
    assert not staged_seed.exists()
    rendered_script = result.local_job_script_path.read_text(encoding="utf-8")
    assert "if not staged_seed_path.exists():" in rendered_script
    assert "_generate_iteration_seed(staged_seed_path)" in rendered_script


def test_run_tune_execute_generate_seed_defers_to_remote_driver(
    sample_config_files,
):
    (sample_config_files / "config" / "defaults.yaml").write_text(
        """
defaults:
  tuning:
    iteration1_seed:
      source: "generate"
""".strip()
        + "\n",
        encoding="utf-8",
    )

    result = run_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-exec-iter1-seed-remote",
        mode=ExecutionMode.EXECUTE,
        remote_exec_adapter=FakeRemoteExecAdapter(),
        transfer_adapter=FakeFileTransferAdapter(),
        scheduler_adapter=FakeSchedulerAdapter(),
    )

    staged_seed = (
        sample_config_files
        / "runs"
        / "run-exec-iter1-seed-remote"
        / "iterations"
        / "iter-01"
        / "inputs"
        / "simplified_nonlinear_zerod.json"
    )
    assert not staged_seed.exists()
    rendered_script = result.local_job_script_path.read_text(encoding="utf-8")
    assert "if not staged_seed_path.exists():" in rendered_script
    assert "_generate_iteration_seed(staged_seed_path)" in rendered_script


def test_run_tune_learned_zerod_seed_defers_full_pa_generation_to_remote_driver(
    sample_config_files,
):
    patient_root = (
        sample_config_files
        / "remote_data"
        / "permanent"
        / "TST-STAN-x"
    )
    (sample_config_files / "config" / "patients.yaml").write_text(
        f"""
patients:
  - alias: "TST-STAN-x"
    permanent_remote_path: "{patient_root.as_posix()}"
    data_policy: "read_only"
    tuning:
      iteration1_seed:
        source: "learned_zerod"
        learned_zerod_executable: "/opt/learned/bin/learned-zerod"
        svzerodsolver_executable: "/opt/svzerod/bin/svzerodsolver"
      impedance:
        tuning_model: "full_pa"
        outlet_mapping_mode: "centerline"
        diameter_scale: 0.2
        objective_tree_policy:
          use_mean: true
          reference_diameter: "conductance_matched"
""".strip()
        + "\n",
        encoding="utf-8",
    )

    result = run_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-dry-learned-seed",
        mode=ExecutionMode.DRY_RUN,
    )

    staged_seed = (
        sample_config_files
        / "runs"
        / "run-dry-learned-seed"
        / "iterations"
        / "iter-01"
        / "inputs"
        / "full_pa_zerod.json"
    )
    assert not staged_seed.exists()
    staged_source = (
        sample_config_files
        / "runs"
        / "run-dry-learned-seed"
        / "iterations"
        / "iter-01"
        / "inputs"
        / "source_0d_config.json"
    )
    assert staged_source.read_text(encoding="utf-8") == '{"default_seed": true}'
    rendered_script = result.local_job_script_path.read_text(encoding="utf-8")
    assert '"source": "learned_zerod"' in rendered_script
    assert '"path": "' + result.remote_run_dir + '/iterations/iter-01/inputs/source_0d_config.json"' in rendered_script
    assert (
        '"learned_zerod_executable": "/opt/learned/bin/learned-zerod"'
        in rendered_script
    )
    assert (
        '"svzerodsolver_executable": "/opt/svzerod/bin/svzerodsolver"'
        in rendered_script
    )
    assert '"outlet_mapping_mode": "centerline"' in rendered_script
    assert (
        '"objective_tree_policy": {"reference_diameter": "conductance_matched", '
        '"use_mean": true}'
    ) in rendered_script
    assert (
        '"outlet_mapping_centerline": "'
        + patient_root.as_posix()
        + '/centerlines.vtp"'
    ) in rendered_script
    assert "def _generate_learned_iteration_seed(seed_path: Path)" in rendered_script
    assert "_generate_learned_iteration_seed(staged_seed_path)" in rendered_script
    learned_helper = rendered_script[
        rendered_script.index("def _generate_learned_iteration_seed(seed_path: Path)") :
        rendered_script.index("_NESTED_SBATCH_STRIP_ENV_VARS")
    ]
    assert "_generate_iteration_seed(source_seed_path)" not in learned_helper
    assert "run_steady_sims()" not in learned_helper
    assert 'seed_workspace / "preop"' not in learned_helper
    assert 'seed_workspace / "postop"' not in learned_helper
    assert 'seed_workspace / "steady"' not in learned_helper
    assert "input 0D JSON is missing or unreadable" in learned_helper
    assert 'junction_type.lower() == "internal_junction"' in learned_helper
    assert 'junction["junction_type"] = "NORMAL_JUNCTION"' in learned_helper
    assert 'log["learned_seed_normalized_internal_junctions"]' in learned_helper
    assert 'output_filename="full_pa_zerod.json"' in rendered_script
    python_blocks = re.split(r"<<'PY'[^\n]*\n", rendered_script)[1:]
    assert len(python_blocks) == 2
    for index, block in enumerate(python_blocks):
        source, separator, _remainder = block.partition("\nPY\n")
        assert separator
        compile(source, f"<rendered-tune-job-{index}>", "exec")


def test_run_tune_learned_zerod_remote_only_source_is_referenced_without_local_staging(
    sample_config_files,
):
    remote_patient_root = "/oak/stanford/groups/amarsden/PPAS-study/tof-stent/TST-STAN-x"
    cluster_config = sample_config_files / "config" / "clusters.yaml"
    cluster_config.write_text(
        cluster_config.read_text(encoding="utf-8").replace(
            (sample_config_files / "remote_data" / "permanent").as_posix(),
            "/oak/stanford/groups/amarsden/PPAS-study/tof-stent",
        ),
        encoding="utf-8",
    )
    (sample_config_files / "config" / "patients.yaml").write_text(
        f"""
patients:
  - alias: "TST-STAN-x"
    permanent_remote_path: "{remote_patient_root}"
    data_policy: "read_only"
    tuning:
      iteration1_seed:
        source: "learned_zerod"
        path: "zerod-models/baseline_0d_learned.json"
      impedance:
        tuning_model: "full_pa"
""".strip()
        + "\n",
        encoding="utf-8",
    )

    result = run_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-dry-learned-remote-only",
        mode=ExecutionMode.DRY_RUN,
    )

    staged_source = (
        sample_config_files
        / "runs"
        / "run-dry-learned-remote-only"
        / "iterations"
        / "iter-01"
        / "inputs"
        / "source_0d_config.json"
    )
    assert not staged_source.exists()
    rendered_script = result.local_job_script_path.read_text(encoding="utf-8")
    assert (
        '"path": "/oak/stanford/groups/amarsden/PPAS-study/tof-stent/'
        'TST-STAN-x/zerod-models/baseline_0d_learned.json"'
    ) in rendered_script


def test_run_tune_learned_zerod_missing_remote_source_fails_before_submission(
    sample_config_files,
):
    cluster_config = sample_config_files / "config" / "clusters.yaml"
    cluster_config.write_text(
        cluster_config.read_text(encoding="utf-8").replace(
            (sample_config_files / "remote_data" / "permanent").as_posix(),
            "/oak/stanford/groups/amarsden/PPAS-study/tof-stent",
        ),
        encoding="utf-8",
    )
    (sample_config_files / "config" / "patients.yaml").write_text(
        """
patients:
  - alias: "TST-STAN-x"
    permanent_remote_path: "/oak/stanford/groups/amarsden/PPAS-study/tof-stent/TST-STAN-x"
    data_policy: "read_only"
    tuning:
      iteration1_seed:
        source: "learned_zerod"
        path: "zerod-models/missing-baseline.json"
      impedance:
        tuning_model: "full_pa"
""".strip()
        + "\n",
        encoding="utf-8",
    )
    remote = FakeRemoteExecAdapter()
    remote.queue_response(
        CommandResult(
            argv=["test", "-f", "/oak/stanford/groups/amarsden/PPAS-study/tof-stent/TST-STAN-x/zerod-models/missing-baseline.json"],
            returncode=1,
            stdout="",
            stderr="",
            dry_run=False,
        )
    )

    with pytest.raises(ConfigError, match="configured learned source is unavailable"):
        run_tune_trees(
            workspace_root=sample_config_files,
            cluster_name="sherlock",
            patient_alias="TST-STAN-x",
            run_id="run-exec-learned-missing-source",
            mode=ExecutionMode.EXECUTE,
            remote_exec_adapter=remote,
            transfer_adapter=FakeFileTransferAdapter(),
            scheduler_adapter=FakeSchedulerAdapter(),
        )


def test_run_tune_dry_run_renders_threed_defaults_and_stage_paths(sample_config_files):
    result = run_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-dry-3d-defaults",
        mode=ExecutionMode.DRY_RUN,
    )

    rendered_script = result.local_job_script_path.read_text(encoding="utf-8")
    assert "{{" not in rendered_script
    assert (
        'cluster_svfsiplus_path = "/home/users/ndorn/svMP-build/svMultiPhysics-build/bin/svmultiphysics"'
        in rendered_script
    )
    assert '"wall_model": "deformable"' in rendered_script
    assert '"inflow_boundary_condition": "neumann"' in rendered_script
    assert '"prestress_file": "auto"' in rendered_script
    assert '"tissue_support": {"apply_along_normal_direction": true, "damping": 10000.0, "enabled": true, "spatial_values_file_path": null, "stiffness": 1000.0, "type": "uniform"}' in rendered_script
    assert '"compliance_model": "olufsen"' in rendered_script
    assert '"convert_to_cm": false' in rendered_script
    assert '"lpa.xi"' in rendered_script
    assert '"ub": "inf"' in rendered_script
    assert '"solver": "Nelder-Mead"' in rendered_script
    assert '"nm_iter": 5' in rendered_script
    assert "preop-mesh-complete" in rendered_script
    assert 'remote_inflow_path = Path("' in rendered_script
    assert 'staged_inflow_path = remote_inputs_dir / "inflow.csv"' in rendered_script
    assert "def _resolve_tuning_inflow_path() -> Path | None:" in rendered_script
    assert 'inflow_path=resolved_inflow_path,' in rendered_script
    assert 'postop_mesh_complete_path = Path("") if "" else None' in rendered_script
    assert 'mesh_scale_factor = float("1.0")' in rendered_script
    assert "mesh_scale_factor=mesh_scale_factor" in rendered_script
    assert "_normalize_solver_runscript(" in rendered_script
    assert "_NESTED_SBATCH_STRIP_ENV_VARS = (" in rendered_script
    assert "env=_nested_sbatch_env()" in rendered_script
    assert 'f"#SBATCH --chdir={stage_dir}"' in rendered_script
    assert '["sbatch", "--parsable", "--chdir", str(script_path.parent), script_path.name]' in rendered_script
    assert "cwd=script_path.parent" in rendered_script
    assert 'if [ -n "${SLURM_CPUS_PER_TASK:-}" ] && [ -n "${SLURM_TRES_PER_TASK:-}" ]; then' in rendered_script
    assert 'unset SLURM_TRES_PER_TASK' in rendered_script
    assert "run_impedance_tuning_for_iteration" in rendered_script
    assert 'zerod_config_path.name == "svzerod_3d_coupling_tuned.json"' in rendered_script
    assert 'provenance_path = stage_dir / zerod_config_path.name' in rendered_script
    assert 'shutil.copy2(zerod_config_path, provenance_path)' in rendered_script
    assert 'shutil.copy2(zerod_config_path, canonical_coupling_path)' not in rendered_script
    assert "def _validate_canonical_coupler(stage_dir: Path) -> None:" in rendered_script
    assert "external_solver_coupling_blocks" in rendered_script
    assert "contains duplicate coupling block names" in rendered_script
    assert "svZeroD_interface.dat" not in rendered_script
    assert "_validate_canonical_coupler(stage_dir)" in rendered_script
    assert "#SBATCH --cpus-per-task=24" in rendered_script
    assert 'PYTHON_CANDIDATE="python3"' in rendered_script
    assert "svzerodtrees.tuning missing required symbols" in rendered_script
    assert "sim.run_steady_sims()" in rendered_script
    assert 'solver_paths=threed_config.get("solver_paths")' in rendered_script
    assert 'execution_config=threed_config.get("execution"),' in rendered_script


def test_run_tune_dry_run_renders_slurm_mail_settings_for_svzerodtrees(sample_config_files):
    (sample_config_files / "config" / "defaults.yaml").write_text(
        """
defaults:
  tuning:
    threed:
      execution:
        slurm:
          mail_user: "user@example.com"
          mail_types: ["fail", "end"]
""".strip()
        + "\n",
        encoding="utf-8",
    )

    result = run_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-dry-mail-user",
        mode=ExecutionMode.DRY_RUN,
    )

    rendered_script = result.local_job_script_path.read_text(encoding="utf-8")
    assert '"execution": {"slurm": {"mail_types": ["fail", "end"], "mail_user": "user@example.com"}}' in rendered_script
    assert "def _resolve_slurm_mail_user(sim_cfg: dict) -> str | None:" in rendered_script
    assert "def _resolve_slurm_mail_types(sim_cfg: dict) -> list[str]:" in rendered_script
    assert "mail_user=_resolve_slurm_mail_user(sim_cfg)" in rendered_script
    assert "mail_types=_resolve_slurm_mail_types(sim_cfg)" in rendered_script
    assert "sim.generate_simplified_nonlinear_zerod()" in rendered_script
    assert "sim.run_pipeline(run_steady=True, optimize_bcs=False" not in rendered_script
    assert "def _resolve_prestress_file_path(sim_cfg: dict) -> str | None:" in rendered_script
    assert 'prestress_mode == "generate"' in rendered_script
    assert "def _ensure_generated_prestress_file() -> Path:" in rendered_script
    assert (
        '"deformable run requested with prestress_file=auto, but auto prestress generation is not available in iteration script; continuing without Prestress_file_path"'
        in rendered_script
    )


def test_run_tune_dry_run_uses_configured_solver_path(sample_config_files):
    (sample_config_files / "config" / "clusters.yaml").write_text(
        f"""
clusters:
  - name: "sherlock"
    host: "sherlock.stanford.edu"
    user: "ndorn"
    scheduler:
      type: "slurm"
    executables:
      svfsiplus_path: "/opt/svfsiplus/bin/svmultiphysics"
      svzerodsolver_build_dir: "/opt/svZeroDSolver-build"
    remote_roots:
      permanent_data_root: "{(sample_config_files / 'remote_data' / 'permanent').as_posix()}"
      runs_root: "{(sample_config_files / 'remote_runs').as_posix()}"
""".strip()
        + "\n",
        encoding="utf-8",
    )

    result = run_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-dry-sherlock-path-override",
        mode=ExecutionMode.DRY_RUN,
    )

    rendered_script = result.local_job_script_path.read_text(encoding="utf-8")
    assert (
        'cluster_svfsiplus_path = "/opt/svfsiplus/bin/svmultiphysics"'
        in rendered_script
    )
    assert 'solver_execution["executable"] = cluster_svfsiplus_path' in rendered_script
    assert 'solver_execution["svfsiplus_path"] = cluster_svfsiplus_path' in rendered_script
    assert 'threed_config["execution"] = solver_execution' in rendered_script
    assert "/home/users/ndorn/svMP-build/svMultiPhysics-build/bin/svmultiphysics" not in rendered_script


def test_run_tune_dry_run_renders_patient_threed_override(sample_config_files):
    (sample_config_files / "config" / "patients.yaml").write_text(
        f"""
patients:
  - alias: "TST-STAN-x"
    permanent_remote_path: "{(sample_config_files / 'remote_data' / 'permanent' / 'TST-STAN-x').as_posix()}"
    data_policy: "read_only"
    tuning:
      threed:
        wall_model: "rigid"
        inflow_boundary_condition: "dirichlet"
        tissue_support:
          enabled: false
        n_tsteps: 1234
        wait_timeout_seconds: 999
""".strip()
        + "\n",
        encoding="utf-8",
    )

    result = run_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-dry-3d-override",
        mode=ExecutionMode.DRY_RUN,
    )

    rendered_script = result.local_job_script_path.read_text(encoding="utf-8")
    assert '"wall_model": "rigid"' in rendered_script
    assert '"inflow_boundary_condition": "dirichlet"' in rendered_script
    assert '"enabled": false' in rendered_script
    assert '"n_tsteps": 1234' in rendered_script
    assert '"wait_timeout_seconds": 999' in rendered_script


def test_run_tune_dry_run_renders_generate_prestress_from_seed_mean(sample_config_files):
    (sample_config_files / "config" / "patients.yaml").write_text(
        f"""
patients:
  - alias: "TST-STAN-x"
    permanent_remote_path: "{(sample_config_files / 'remote_data' / 'permanent' / 'TST-STAN-x').as_posix()}"
    data_policy: "read_only"
    tuning:
      iteration1_seed:
        source: "generate"
      threed:
        wall_model: "deformable"
        prestress_file: "generate"
""".strip()
        + "\n",
        encoding="utf-8",
    )
    result = run_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-dry-generate-prestress",
        mode=ExecutionMode.DRY_RUN,
    )

    rendered_script = result.local_job_script_path.read_text(encoding="utf-8")
    assert '"prestress_file": "generate"' in rendered_script
    assert 'remote_run_dir = Path("' in rendered_script
    assert 'remote_run_dir / "prestress"' in rendered_script
    assert (
        'remote_run_dir / "iterations" / "iter-01" / "seed_generation" / "steady" / "mean"'
        in rendered_script
    )
    assert '"prestress_file=generate requires seed-generation mean steady VTUs under "' in rendered_script
    assert 'Path.home() / "scripts" / "calc_mean_wall_traction.py"' not in rendered_script
    assert "import vtk" in rendered_script
    assert "def _write_mean_wall_traction_and_pressure(" in rendered_script
    assert 'pressure_file = prestress_dir / "rigid_wall_mean_pressure.vtp"' in rendered_script
    assert '"simulation_mode": "prestress"' in rendered_script
    assert '"n_tsteps": 20' in rendered_script
    assert '"dt": 0.001' in rendered_script
    assert '"vtk_save_increment": 1' in rendered_script
    assert "import xml.etree.ElementTree as ET" in rendered_script
    assert '_force_xml_text(prestress_dir / "svFSIplus.xml", "Increment_in_saving_VTK_files", "1")' in rendered_script
    assert "nodes=1" in rendered_script
    assert "procs_per_node=1" in rendered_script
    assert 'log["prestress_job_id"] = prestress_job_id' in rendered_script
    assert 'log["prestress_file_path"] = str(generated)' in rendered_script
    assert 'log["prestress_traction_source"] = str(mean_result_dir)' in rendered_script
    assert "prestress_reused" in rendered_script
    assert "sim_cfg[\"prestress_file_path\"] = prestress_file_path" in rendered_script


def test_run_tune_dry_run_preserves_explicit_prestress_path(sample_config_files):
    prestress_path = (
        "/oak/stanford/groups/amarsden/ndorn/PPAS-study/tof-stent/"
        "TST-STAN-x/prestress/1-procs/result_009.vtu"
    )
    (sample_config_files / "config" / "patients.yaml").write_text(
        f"""
patients:
  - alias: "TST-STAN-x"
    permanent_remote_path: "{(sample_config_files / 'remote_data' / 'permanent' / 'TST-STAN-x').as_posix()}"
    data_policy: "read_only"
    tuning:
      threed:
        wall_model: "deformable"
        prestress_file: "{prestress_path}"
""".strip()
        + "\n",
        encoding="utf-8",
    )

    result = run_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-dry-explicit-prestress",
        mode=ExecutionMode.DRY_RUN,
    )

    rendered_script = result.local_job_script_path.read_text(encoding="utf-8")
    assert f'"prestress_file": "{prestress_path}"' in rendered_script
    assert 'prestress_mode not in {"auto", "from_steady_mean", "generate"}' in rendered_script
    assert "return prestress_setting" in rendered_script
    assert "sim_cfg[\"prestress_file_path\"] = prestress_file_path" in rendered_script


def test_run_tune_dry_run_renders_patient_impedance_override(sample_config_files):
    (sample_config_files / "config" / "patients.yaml").write_text(
        f"""
patients:
  - alias: "TST-STAN-x"
    permanent_remote_path: "{(sample_config_files / 'remote_data' / 'permanent' / 'TST-STAN-x').as_posix()}"
    data_policy: "read_only"
    tuning:
      impedance:
        nm_iter: 12
        n_procs: 8
        tuning_model: "full_pa"
        diameter_std_cap: 1.25
""".strip()
        + "\n",
        encoding="utf-8",
    )

    result = run_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-dry-impedance-override",
        mode=ExecutionMode.DRY_RUN,
    )

    rendered_script = result.local_job_script_path.read_text(encoding="utf-8")
    assert '"nm_iter": 12' in rendered_script
    assert '"n_procs": 8' in rendered_script
    assert '"tuning_model": "full_pa"' in rendered_script
    assert '"diameter_std_cap": 1.25' in rendered_script
    assert 'seed_filename = "full_pa_zerod.json"' in rendered_script
    staged_seed = (
        sample_config_files
        / "runs"
        / "run-dry-impedance-override"
        / "iterations"
        / "iter-01"
        / "inputs"
        / "full_pa_zerod.json"
    )
    reduced_seed = (
        sample_config_files
        / "runs"
        / "run-dry-impedance-override"
        / "iterations"
        / "iter-01"
        / "inputs"
        / "simplified_nonlinear_zerod.json"
    )
    assert staged_seed.exists()
    assert not reduced_seed.exists()


def test_run_tune_dry_run_renders_rcr_tuning_path(sample_config_files):
    (sample_config_files / "config" / "defaults.yaml").write_text(
        """
defaults:
  tuning:
    bc_type: "rcr"
    rcr:
      n_procs: 8
      convert_to_cm: true
""".strip()
        + "\n",
        encoding="utf-8",
    )

    result = run_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-dry-rcr",
        mode=ExecutionMode.DRY_RUN,
    )

    rendered_script = result.local_job_script_path.read_text(encoding="utf-8")
    assert 'tuning_bc_type = "rcr".strip().lower() or "impedance"' in rendered_script
    assert "run_rcr_tuning_for_iteration" in rendered_script
    assert "tuning = run_rcr_tuning_for_iteration(" in rendered_script
    assert '"n_procs": 8' in rendered_script
    assert '"convert_to_cm": true' in rendered_script
    assert 'optimized_tuning_filename = (\n        "optimized_rcr_params.csv" if tuning_bc_type == "rcr" else "optimized_params.csv"\n    )' in rendered_script
    assert 'seed_filename = "simplified_nonlinear_zerod.json"' in rendered_script
    staged_seed = (
        sample_config_files
        / "runs"
        / "run-dry-rcr"
        / "iterations"
        / "iter-01"
        / "inputs"
        / "simplified_nonlinear_zerod.json"
    )
    assert staged_seed.exists()


def test_run_tune_dry_run_renders_rcr_skip_tuning_artifacts(sample_config_files):
    (sample_config_files / "config" / "defaults.yaml").write_text(
        """
defaults:
  tuning:
    bc_type: "rcr"
""".strip()
        + "\n",
        encoding="utf-8",
    )

    result = run_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-dry-rcr-skip",
        mode=ExecutionMode.DRY_RUN,
        skip_zerod_tuning=True,
    )

    rendered_script = result.local_job_script_path.read_text(encoding="utf-8")
    assert 'str(remote_results_dir / optimized_tuning_filename)' in rendered_script
    assert 'remote_results_dir / optimized_tuning_filename,' in rendered_script
    assert "optimized_rcr_params.csv" in rendered_script


def test_run_tune_dry_run_renders_patient_mesh_scale_override(sample_config_files):
    (sample_config_files / "config" / "patients.yaml").write_text(
        f"""
patients:
  - alias: "TST-STAN-x"
    permanent_remote_path: "{(sample_config_files / 'remote_data' / 'permanent' / 'TST-STAN-x').as_posix()}"
    data_policy: "read_only"
    mesh_scale_factor: 2.5
""".strip()
        + "\n",
        encoding="utf-8",
    )

    result = run_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-dry-mesh-scale-override",
        mode=ExecutionMode.DRY_RUN,
    )

    rendered_script = result.local_job_script_path.read_text(encoding="utf-8")
    assert 'mesh_scale_factor = float("2.5")' in rendered_script


def test_generate_prestress_runs_mean_steady_sim_when_seed_has_none(sample_config_files, tmp_path):
    """Learned/path seeds run no steady solves; prestress=generate runs the mean one."""
    import ast
    import os

    import numpy as np

    (sample_config_files / "config" / "patients.yaml").write_text(
        f"""
patients:
  - alias: "TST-STAN-x"
    permanent_remote_path: "{(sample_config_files / 'remote_data' / 'permanent' / 'TST-STAN-x').as_posix()}"
    data_policy: "read_only"
    tuning:
      threed:
        wall_model: "deformable"
        prestress_file: "generate"
""".strip()
        + "\n",
        encoding="utf-8",
    )
    result = run_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-dry-generate-prestress-mean-steady",
        mode=ExecutionMode.DRY_RUN,
    )
    rendered = result.local_job_script_path.read_text(encoding="utf-8")
    body = re.split(r"<<'PY'[^\n]*\n", rendered)[2]
    tree = ast.parse(body[: body.index("\nPY\n")])
    wanted = {
        "_extract_result_step",
        "_result_vtus",
        "_seed_generation_mean_result_dirs",
        "_run_seed_generation_mean_steady_sim",
        "_seed_generation_mean_result_dir",
    }
    module = ast.Module(
        body=[node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in wanted],
        type_ignores=[],
    )

    calls: dict[str, object] = {}

    class FakeSimulation:
        convert_to_cm = False
        inflow = type("Inflow", (), {"q": [10.0, 30.0, 50.0]})()
        preop_dir = type("Preop", (), {"mesh_complete": type("MC", (), {"path": "/mesh"})()})()

    class FakeSteadyDirectory:
        solver_paths = None

        def __init__(self, path):
            self.path = Path(path)

        @classmethod
        def from_directory(cls, path, mesh_complete, convert_to_cm, mesh_scale_factor):
            calls["from_directory"] = (path, mesh_complete, convert_to_cm, mesh_scale_factor)
            return cls(path)

        def generate_steady_sim(self, flow_rate=None, execution_config=None):
            calls["flow_rate"] = flow_rate
            (self.path / "run_solver.sh").write_text("#!/bin/bash\n", encoding="utf-8")

    def fake_simulation(workspace, **kwargs):
        calls["sim_kwargs"] = kwargs
        return FakeSimulation()

    def fake_normalize(**kwargs):
        calls["normalized"] = kwargs

    def fake_submit(path):
        calls["submitted"] = str(path)
        return "9001"

    def fake_wait(job_id, poll_seconds, timeout_seconds):
        procs = tmp_path / "iterations" / "iter-01" / "seed_generation" / "steady" / "mean" / "48-procs"
        procs.mkdir(parents=True)
        for step in (100, 200, 300):
            (procs / f"result_{step:03d}.vtu").write_text("", encoding="utf-8")
        return True, "COMPLETED"

    namespace = {
        "np": np,
        "re": re,
        "os": os,
        "Path": Path,
        "remote_run_dir": tmp_path,
        "mesh_scale_factor": 1.425,
        "threed_config": {"nodes": 3, "procs_per_node": 24, "memory": 16},
        "scheduler_defaults": {"partition": "amarsden"},
        "cluster_svfsiplus_path": "/bin/svmp",
        "log": {"steps": []},
        "SimulationDirectory": FakeSteadyDirectory,
        "_seed_generation_simulation": fake_simulation,
        "_normalize_solver_runscript": fake_normalize,
        "_resolve_slurm_mail_user": lambda cfg: None,
        "_resolve_slurm_mail_types": lambda cfg: [],
        "_submit_job": fake_submit,
        "_wait_for_completion": fake_wait,
    }
    exec(compile(module, "driver", "exec"), namespace)

    result_dir = namespace["_seed_generation_mean_result_dir"]()

    mean_dir = tmp_path / "iterations" / "iter-01" / "seed_generation" / "steady" / "mean"
    assert result_dir == mean_dir / "48-procs"
    assert calls["flow_rate"] == pytest.approx(30.0)
    assert calls["sim_kwargs"] == {"mesh_scale_factor": 1.425}
    assert calls["from_directory"][3] == 1.425
    assert calls["submitted"] == str(mean_dir / "run_solver.sh")
    assert calls["normalized"]["nodes"] == 3
    assert namespace["log"]["prestress_mean_steady_job_id"] == "9001"
    assert "prestress_mean_steady_sim_completed" in namespace["log"]["steps"]

    # Existing seed-generation VTUs are reused without another solve.
    calls.clear()
    assert namespace["_seed_generation_mean_result_dir"]() == mean_dir / "48-procs"
    assert "submitted" not in calls


def test_run_tune_iter_reuse_preop_3d_requires_skip_zerod_tuning(sample_config_files):
    with pytest.raises(ConfigError, match="reuse_preop_3d requires skip_zerod_tuning"):
        run_tune_trees(
            workspace_root=sample_config_files,
            cluster_name="sherlock",
            patient_alias="TST-STAN-x",
            run_id="run-dry-reuse-no-skip",
            mode=ExecutionMode.DRY_RUN,
            reuse_preop_3d=True,
        )


def test_run_tune_iter_reuse_preop_3d_uses_only_completed_evidence(sample_config_files, tmp_path):
    """Reuse needs a COMPLETED preop job in the previous driver log plus result VTUs."""
    import ast
    import json
    import shutil as _shutil

    result = run_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-dry-reuse-preop",
        mode=ExecutionMode.DRY_RUN,
        skip_zerod_tuning=True,
        reuse_preop_3d=True,
    )
    rendered = result.local_job_script_path.read_text(encoding="utf-8")
    assert "reuse_preop_3d = json.loads(r'''true''')" in rendered
    assert 'log["steps"].append(f"preop_reused:{reused_job_id}")' in rendered

    body = re.split(r"<<'PY'[^\n]*\n", rendered)[2]
    tree = ast.parse(body[: body.index("\nPY\n")])
    wanted = {"_extract_result_step", "_latest_result_vtu", "_completed_preop_job_for_reuse"}
    module = ast.Module(
        body=[node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in wanted],
        type_ignores=[],
    )
    logs_dir = tmp_path / "logs"
    logs_dir.mkdir()
    preop_dir = tmp_path / "preop"
    namespace = {"json": json, "re": re, "Path": Path, "shutil": _shutil, "remote_logs_dir": logs_dir}
    exec(compile(module, "driver", "exec"), namespace)
    check = namespace["_completed_preop_job_for_reuse"]

    job_id, error = check(preop_dir)
    assert job_id is None and "previous driver log unreadable" in error

    previous = {"preop_job_id": "4242", "preop_terminal_state": "FAILED", "steps": ["preop_submitted"]}
    (logs_dir / "iteration_driver_log.json").write_text(json.dumps(previous), encoding="utf-8")
    job_id, error = check(preop_dir)
    assert job_id is None and "does not record a completed preop job" in error

    previous.update(preop_terminal_state="COMPLETED", steps=["preop_submitted", "preop_completed"])
    (logs_dir / "iteration_driver_log.json").write_text(json.dumps(previous), encoding="utf-8")
    job_id, error = check(preop_dir)
    assert job_id is None and "no preop result VTUs" in error

    (preop_dir / "72-procs").mkdir(parents=True)
    (preop_dir / "72-procs" / "result_4000.vtu").write_text("", encoding="utf-8")
    job_id, error = check(preop_dir)
    assert (job_id, error) == ("4242", None)
    assert json.loads((logs_dir / "iteration_driver_log.preop_4242.json").read_text()) == previous


def test_run_tune_iter_resubmission_resets_previous_outcome(sample_config_files):
    """A re-submitted iteration is decided by the new attempt, not the stale one."""
    import json

    from svztagent.core.manifest import mark_iteration_decision
    from svztagent.hpc.interfaces import SubmitResult

    def _submit(job_id: str, **kwargs):
        scheduler = FakeSchedulerAdapter()
        scheduler.set_submit_result(
            SubmitResult(
                job_id=job_id,
                command=CommandResult(argv=["sbatch"], returncode=0, stdout=job_id, stderr="", dry_run=False),
            )
        )
        return run_tune_trees(
            workspace_root=sample_config_files,
            cluster_name="sherlock",
            patient_alias="TST-STAN-x",
            run_id="run-exec-resubmit",
            mode=ExecutionMode.EXECUTE,
            remote_exec_adapter=FakeRemoteExecAdapter(),
            transfer_adapter=FakeFileTransferAdapter(),
            scheduler_adapter=scheduler,
            **kwargs,
        )

    _submit("1001")
    run_dir = sample_config_files / "runs" / "run-exec-resubmit"
    manifest_path = run_dir / "manifest.yaml"
    manifest = mark_iteration_decision(
        read_manifest(manifest_path),
        iteration=1,
        decision="needs_review",
        metrics={"mpa_sys": 1.0},
        deltas={"mpa_sys": 1.0},
    )
    write_manifest(manifest, manifest_path)
    iter_dir = run_dir / "iterations" / "iter-01"
    (iter_dir / "logs").mkdir(parents=True, exist_ok=True)
    (iter_dir / "results").mkdir(parents=True, exist_ok=True)
    stale_log = {"errors": ["old failure"]}
    (iter_dir / "logs" / "iteration_driver_log.json").write_text(json.dumps(stale_log), encoding="utf-8")
    (iter_dir / "results" / "iteration_decision.json").write_text('{"decision": "needs_review"}', encoding="utf-8")

    _submit("1002", iteration=1, skip_zerod_tuning=True)

    record = read_manifest(manifest_path).tuning_iteration_tracker.iterations[0]
    assert record.tune_job_id == "1002"
    assert record.decision is None and record.metrics is None and record.deltas is None
    assert any("previous tune job 1001 ended with decision needs_review" in note for note in record.notes)
    assert not (iter_dir / "logs" / "iteration_driver_log.json").exists()
    assert json.loads((iter_dir / "logs" / "iteration_driver_log.tune_1001.json").read_text()) == stale_log
    assert (iter_dir / "results" / "iteration_decision.tune_1001.json").exists()
