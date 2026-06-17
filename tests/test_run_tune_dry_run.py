from __future__ import annotations

from pathlib import Path
import shutil

from svztagent.core.manifest import read_manifest, record_lifecycle_transition, write_manifest
from svztagent.core.state import RunLifecycleState
from svztagent.hpc.interfaces import ExecutionMode
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


def test_run_tune_dry_run_iteration1_missing_seed_path_falls_back_to_generate(
    sample_config_files, monkeypatch
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
    generated = sample_config_files / "generated_seed.json"
    generated.write_text("{\"generated\": true}", encoding="utf-8")

    monkeypatch.setattr(
        "svztagent.workflows.tune_trees._generate_iteration1_seed_via_svzerodtrees",
        lambda **_: generated,
    )

    run_tune_trees(
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
    assert staged_seed.exists()
    assert staged_seed.read_text(encoding="utf-8") == "{\"generated\": true}"


def test_run_tune_dry_run_generate_seed_skips_local_generation_when_assets_are_remote_only(
    sample_config_files, monkeypatch
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
    remote_roots:
      patient_data_root: "/scratch/users/ndorn/models/PPAS/tof-stent"
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
    remote_path: "/scratch/users/ndorn/models/PPAS/tof-stent/TST-STAN-x"
    permanent_remote_path: "/oak/stanford/groups/amarsden/ndorn/PPAS-study/tof-stent/TST-STAN-x"
    data_policy: "read_only"
    tuning:
      iteration1_seed:
        source: "generate"
""".strip()
        + "\n",
        encoding="utf-8",
    )

    called = {"generate": False}

    def _fake_generate(**_kwargs):
        called["generate"] = True
        return sample_config_files / "generated_seed.json"

    monkeypatch.setattr(
        "svztagent.workflows.tune_trees._generate_iteration1_seed_via_svzerodtrees",
        _fake_generate,
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
    assert called["generate"] is False
    rendered_script = result.local_job_script_path.read_text(encoding="utf-8")
    assert "if not staged_seed_path.exists():" in rendered_script
    assert "_generate_iteration_seed(staged_seed_path)" in rendered_script


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
    remote_roots:
      patient_data_root: "{(sample_config_files / 'remote_data' / 'active').as_posix()}"
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
    remote_path: "{(sample_config_files / 'remote_data' / 'active' / 'TST-STAN-x').as_posix()}"
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


def test_run_tune_dry_run_renders_generate_prestress_from_seed_mean(sample_config_files, monkeypatch):
    (sample_config_files / "config" / "patients.yaml").write_text(
        f"""
patients:
  - alias: "TST-STAN-x"
    remote_path: "{(sample_config_files / 'remote_data' / 'active' / 'TST-STAN-x').as_posix()}"
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
    generated = sample_config_files / "generated_seed.json"
    generated.write_text("{\"generated\": true}", encoding="utf-8")
    monkeypatch.setattr(
        "svztagent.workflows.tune_trees._generate_iteration1_seed_via_svzerodtrees",
        lambda **_: generated,
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
    assert 'Path.home() / "scripts" / "calc_mean_wall_traction.py"' in rendered_script
    assert '"--result-dir"' in rendered_script
    assert '"--wall"' in rendered_script
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
    remote_path: "{(sample_config_files / 'remote_data' / 'active' / 'TST-STAN-x').as_posix()}"
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
    remote_path: "{(sample_config_files / 'remote_data' / 'active' / 'TST-STAN-x').as_posix()}"
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


def test_run_tune_dry_run_renders_patient_mesh_scale_override(sample_config_files):
    (sample_config_files / "config" / "patients.yaml").write_text(
        f"""
patients:
  - alias: "TST-STAN-x"
    remote_path: "{(sample_config_files / 'remote_data' / 'active' / 'TST-STAN-x').as_posix()}"
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
