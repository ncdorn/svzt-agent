from __future__ import annotations

from pathlib import Path
import sys

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from svztagent.hpc.local_fake import LocalFakeRemoteExec, LocalFakeScheduler, LocalFakeTransfer


@pytest.fixture
def temp_workspace(tmp_path: Path) -> Path:
    workspace = tmp_path / "svz"
    (workspace / "config").mkdir(parents=True)
    (workspace / "runs").mkdir(parents=True)
    (workspace / "repos" / "svzt-agent").mkdir(parents=True)
    (workspace / "repos" / "svZeroDTrees").mkdir(parents=True)
    (workspace / "repos" / "svZeroDSolver").mkdir(parents=True)
    return workspace


@pytest.fixture
def sample_config_files(temp_workspace: Path) -> Path:
    patient_data_root = temp_workspace / "remote_data" / "active"
    permanent_data_root = temp_workspace / "remote_data" / "permanent"
    runs_root = temp_workspace / "remote_runs"
    patient_alias = "TST-STAN-x"
    active_patient_path = patient_data_root / patient_alias
    permanent_patient_path = permanent_data_root / patient_alias
    preop_mesh_complete = permanent_patient_path / "preop-mesh-complete"
    mesh_surfaces = preop_mesh_complete / "mesh-surfaces"

    active_patient_path.mkdir(parents=True, exist_ok=True)
    mesh_surfaces.mkdir(parents=True, exist_ok=True)
    runs_root.mkdir(parents=True, exist_ok=True)
    (permanent_patient_path / "clinical_targets.csv").write_text("target,value\n", encoding="utf-8")
    (permanent_patient_path / "centerlines.vtp").write_text("<vtk/>", encoding="utf-8")
    (permanent_patient_path / "inflow.csv").write_text("t,q\n0,0\n", encoding="utf-8")
    (
        permanent_patient_path / "simplified_nonlinear_zerod.json"
    ).write_text("{\"default_seed\": true}", encoding="utf-8")

    (temp_workspace / "config" / "clusters.yaml").write_text(
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
      patient_data_root: "{patient_data_root.as_posix()}"
      permanent_data_root: "{permanent_data_root.as_posix()}"
      runs_root: "{runs_root.as_posix()}"
""".strip()
        + "\n",
        encoding="utf-8",
    )

    (temp_workspace / "config" / "patients.yaml").write_text(
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

    (temp_workspace / "config" / "defaults.yaml").write_text(
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
      - "conda activate svz"
  mesh_scale_factor: 1.0
  validation:
    require_dry_run_before_execute: true
    enforce_remote_write_root: true
  tuning:
    iteration1_seed:
      source: "path"
      path: "simplified_nonlinear_zerod.json"
    threed:
      inflow_boundary_condition: "neumann"
      tissue_support:
        enabled: true
        type: "uniform"
        stiffness: 1000.0
        damping: 10000.0
        apply_along_normal_direction: true
        spatial_values_file_path: null
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
          - name: "lpa.eta_sym"
            init: 0.6
            lb: 0.3
            ub: 0.9
          - name: "rpa.xi"
            init: 2.3
            lb: 0.0
            ub: 6.0
          - name: "rpa.eta_sym"
            init: 0.7
            lb: 0.3
            ub: 0.9
          - name: "lpa.inductance"
            init: 1.0
            lb: 0.0
            ub: "inf"
          - name: "rpa.inductance"
            init: 1.0
            lb: 0.0
            ub: "inf"
          - name: "comp.lpa.k2"
            init: -75.0
            lb: -100.0
            ub: -1.0
        fixed:
          - name: "lrr"
            value: 10.0
          - name: "d_min"
            value: 0.01
        tied:
          - name: "comp.rpa.k2"
            other: "comp.lpa.k2"
            fn: "identity"
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

    return temp_workspace


@pytest.fixture
def fake_hpc() -> tuple[LocalFakeTransfer, LocalFakeScheduler, LocalFakeRemoteExec]:
    return LocalFakeTransfer(), LocalFakeScheduler(), LocalFakeRemoteExec()
