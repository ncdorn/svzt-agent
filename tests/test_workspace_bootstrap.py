from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from svztagent.cli.main import main
from svztagent.core.errors import ConfigError
from svztagent.core.manifest import read_manifest
from svztagent.workspace_bootstrap import init_workspace


def _configure_bootstrapped_workspace(workspace: Path) -> None:
    sibling_root = workspace.parent
    solver_root = sibling_root.parent / "svZeroDSolver"
    for path in (
        sibling_root / "svzt-agent",
        sibling_root / "svZeroDTrees",
        solver_root,
    ):
        path.mkdir(parents=True, exist_ok=True)

    patient_alias = "TST-STAN-x"
    active_patient_path = workspace / "remote_data" / "active" / patient_alias
    permanent_patient_path = workspace / "remote_data" / "permanent" / patient_alias
    (permanent_patient_path / "preop-mesh-complete" / "mesh-surfaces").mkdir(
        parents=True, exist_ok=True
    )
    active_patient_path.mkdir(parents=True, exist_ok=True)
    (permanent_patient_path / "clinical_targets.csv").write_text(
        "target,value\n",
        encoding="utf-8",
    )
    (permanent_patient_path / "centerlines.vtp").write_text("<vtk/>", encoding="utf-8")
    (permanent_patient_path / "inflow.csv").write_text("t,q\n0,0\n", encoding="utf-8")
    (permanent_patient_path / "simplified_nonlinear_zerod.json").write_text(
        "{\"default_seed\": true}",
        encoding="utf-8",
    )

    (workspace / "config" / "clusters.yaml").write_text(
        f"""
clusters:
  - name: "sherlock"
    host: "sherlock.stanford.edu"
    user: "ndorn"
    scheduler:
      type: "slurm"
    executables:
      svfsiplus_path: "/home/users/ndorn/svMP-build/svMultiPhysics-build/bin/svmultiphysics"
      svslicer_path: "/home/users/ndorn/bin/svslicer"
    remote_roots:
      patient_data_root: "{(workspace / 'remote_data' / 'active').as_posix()}"
      permanent_data_root: "{(workspace / 'remote_data' / 'permanent').as_posix()}"
      runs_root: "{(workspace / 'remote_runs').as_posix()}"
""".strip()
        + "\n",
        encoding="utf-8",
    )
    (workspace / "config" / "patients.yaml").write_text(
        f"""
patients:
  - alias: "{patient_alias}"
    remote_path: "{active_patient_path.as_posix()}"
    permanent_remote_path: "{permanent_patient_path.as_posix()}"
    data_policy: "read_only"
""".strip()
        + "\n",
        encoding="utf-8",
    )
    (workspace / "config" / "defaults.yaml").write_text(
        """
defaults:
  rsync:
    include_patterns: ["*.json", "*.yaml", "*.csv"]
    exclude_patterns: ["*.tmp"]
  artifacts:
    pull: ["manifest.yaml", "results/**"]
  scheduler:
    account: "<account>"
    partition: "<partition>"
    wall_time: "<HH:MM:SS>"
    mem: "<memory>"
    cpus: "<count>"
  execution:
    python_executable: "python3"
    env_activation_hooks:
      - "source ~/.bashrc"
  monitoring:
    poll_interval_seconds: 30
    fetch_on_failure: false
  mesh_scale_factor: 1.0
  tuning:
    iteration1_seed:
      source: "path"
      path: "simplified_nonlinear_zerod.json"
    threed:
      wall_model: "deformable"
      inflow_boundary_condition: "neumann"
    impedance:
      solver: "Nelder-Mead"
      nm_iter: 5
      n_procs: 24
      grid_search_init: true
      d_min: 0.01
      use_mean: true
      specify_diameter: true
      rescale_inflow: true
      convert_to_cm: false
      compliance_model: "olufsen"
      tune_space:
        free:
          - name: "lpa.xi"
            init: 2.3
            lb: 0.0
            ub: 6.0
        fixed:
          - name: "lrr"
            value: 10.0
        tied: []
  adaptation:
    default_model: "M2"
    models:
      m1:
        max_nodes: 200000
        wss_gain: 0.01
  patient_data_layout:
    clinical_targets_csv: "clinical_targets.csv"
    centerlines_vtp: "centerlines.vtp"
    inflow_csv: "inflow.csv"
    preop_mesh_complete_dir: "preop-mesh-complete"
    mesh_surfaces_subdir: "mesh-surfaces"
""".strip()
        + "\n",
        encoding="utf-8",
    )


def test_init_workspace_creates_example_files(tmp_path: Path):
    workspace = tmp_path / "workspace"

    result = init_workspace(workspace)

    assert result.workspace_root == workspace.resolve()
    assert (workspace / "config" / "clusters.yaml").exists()
    assert (workspace / "config" / "patients.yaml").exists()
    assert (workspace / "config" / "defaults.yaml").exists()
    assert (workspace / "config" / "clinical_targets.yaml").exists()
    assert (workspace / "config" / "repositories.yaml").exists()
    assert (workspace / "runs").is_dir()
    assert (workspace / "mirrors").is_dir()
    assert (workspace / "templates").is_dir()
    repositories = yaml.safe_load(
        (workspace / "config" / "repositories.yaml").read_text(encoding="utf-8")
    )
    assert repositories["repositories"]["svzt_agent"] == "../svzt-agent"
    assert repositories["repositories"]["svZeroDTrees"] == "../svZeroDTrees"
    assert repositories["repositories"]["svZeroDSolver"] == "../../svZeroDSolver"


def test_init_workspace_refuses_to_overwrite_without_force(tmp_path: Path):
    workspace = tmp_path / "workspace"
    init_workspace(workspace)

    with pytest.raises(ConfigError, match="Refusing to overwrite existing workspace files"):
        init_workspace(workspace)


def test_cli_config_validate_reports_pass(sample_config_files, capsys):
    rc = main(["--workspace-root", str(sample_config_files), "config", "validate"])

    captured = capsys.readouterr()
    assert rc == 0
    assert "Config validation: PASS" in captured.out
    assert "Workspace root:" in captured.out


def test_cli_doctor_reports_pass(sample_config_files, capsys):
    rc = main(["--workspace-root", str(sample_config_files), "doctor"])

    captured = capsys.readouterr()
    assert rc == 0
    assert "Doctor: PASS" in captured.out
    assert "Warnings:" in captured.out


def test_cli_init_workspace_bootstraps_target_directory(tmp_path: Path, capsys):
    workspace = tmp_path / "bootstrap-target"

    rc = main(["init-workspace", str(workspace)])

    captured = capsys.readouterr()
    assert rc == 0
    assert "Workspace root:" in captured.out
    assert (workspace / "config" / "clusters.yaml").exists()


def test_clean_workspace_bootstrap_supports_local_operator_smoke_flow(
    tmp_path: Path,
    capsys,
):
    workspace = tmp_path / "SimVascular" / "ppas-dev" / "svz"

    rc = main(["init-workspace", str(workspace)])
    assert rc == 0
    _configure_bootstrapped_workspace(workspace)

    rc = main(["--workspace-root", str(workspace), "config", "validate"])
    captured = capsys.readouterr()
    assert rc == 0
    assert "Config validation: PASS" in captured.out

    rc = main(["--workspace-root", str(workspace), "doctor"])
    captured = capsys.readouterr()
    assert rc == 0
    assert "Doctor: PASS" in captured.out

    rc = main(
        [
            "--workspace-root",
            str(workspace),
            "run",
            "tune",
            "--cluster",
            "sherlock",
            "--patient",
            "TST-STAN-x",
            "--run-id",
            "clean-smoke-run",
        ]
    )
    captured = capsys.readouterr()
    assert rc == 0
    assert "Run ID: clean-smoke-run" in captured.out
    assert "Mode: dry_run" in captured.out
    assert "Submitted job ID: dryrun-clean-smoke-run" in captured.out

    manifest = read_manifest(workspace / "runs" / "clean-smoke-run" / "manifest.yaml")
    assert manifest.run_id == "clean-smoke-run"
    assert manifest.repos["svzt_agent"] == str((workspace.parent / "svzt-agent").resolve())
    assert manifest.repos["svZeroDTrees"] == str((workspace.parent / "svZeroDTrees").resolve())
    assert manifest.repos["svZeroDSolver"] == str(
        (workspace.parent.parent / "svZeroDSolver").resolve()
    )
    assert (
        workspace / "runs" / "clean-smoke-run" / "execution_plan.yaml"
    ).exists()
