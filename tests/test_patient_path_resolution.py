from __future__ import annotations

from pathlib import Path

import pytest

from svztagent.config.load import load_workspace_config, resolve_patient_alias
from svztagent.core.errors import ConfigError


def test_resolve_patient_alias_success(sample_config_files):
    config = load_workspace_config(sample_config_files)
    patient = resolve_patient_alias(config, "sherlock", "TST-STAN-x")
    assert patient.remote_path.endswith("/TST-STAN-x")
    assert patient.permanent_remote_path is not None
    assert patient.permanent_remote_path.endswith("/TST-STAN-x")
    assert patient.patient_assets is not None
    assert patient.patient_assets.clinical_targets.endswith("/clinical_targets.csv")
    assert patient.patient_assets.centerlines.endswith("/centerlines.vtp")
    assert patient.patient_assets.inflow.endswith("/inflow.csv")
    assert patient.patient_assets.preop_mesh_complete_dir.endswith("/preop-mesh-complete")
    assert patient.patient_assets.mesh_surfaces_dir.endswith(
        "/preop-mesh-complete/mesh-surfaces"
    )
    assert patient.patient_assets.iteration1_seed_source == "path"
    assert patient.patient_assets.iteration1_seed_path.endswith(
        "/TST-STAN-x/simplified_nonlinear_zerod.json"
    )
    assert patient.patient_assets.postop_mesh_complete_dir is None
    assert patient.bc_type == "impedance"
    assert patient.threed.wall_model == "deformable"
    assert patient.threed.inflow_boundary_condition == "neumann"
    assert patient.threed.prestress_file == "auto"
    assert patient.impedance.solver == "Nelder-Mead"
    assert patient.impedance.compliance_model == "olufsen"
    assert patient.impedance.convert_to_cm is False
    assert patient.impedance.tune_space.free[0].name == "lpa.xi"
    assert patient.impedance.nm_iter == 5
    assert patient.impedance.n_procs == 24
    assert patient.mesh_scale_factor == pytest.approx(1.0)
    assert patient.data_policy == "read_only"


def test_alias_not_found_raises(sample_config_files):
    config = load_workspace_config(sample_config_files)

    with pytest.raises(ConfigError, match="Unknown patient alias"):
        resolve_patient_alias(config, "sherlock", "missing-alias")


def test_iteration1_seed_absolute_override_is_preserved(sample_config_files):
    absolute_seed = "/tmp/global_seed.json"
    (sample_config_files / "config" / "patients.yaml").write_text(
        f"""
patients:
  - alias: "TST-STAN-x"
    remote_path: "/tmp/active/TST-STAN-x"
    permanent_remote_path: "/tmp/permanent/TST-STAN-x"
    data_policy: "read_only"
    tuning:
      iteration1_seed:
        source: "path"
        path: "{absolute_seed}"
""".strip()
        + "\n",
        encoding="utf-8",
    )
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
      patient_data_root: "/tmp/active"
      permanent_data_root: "/tmp/permanent"
      runs_root: "/tmp/runs"
""".strip()
        + "\n",
        encoding="utf-8",
    )

    config = load_workspace_config(sample_config_files)
    patient = resolve_patient_alias(config, "sherlock", "TST-STAN-x")
    assert patient.patient_assets is not None
    assert patient.patient_assets.iteration1_seed_source == "path"
    assert patient.patient_assets.iteration1_seed_path == absolute_seed


def test_patient_threed_override_merges_with_defaults(sample_config_files):
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
        nodes: 4
""".strip()
        + "\n",
        encoding="utf-8",
    )

    config = load_workspace_config(sample_config_files)
    patient = resolve_patient_alias(config, "sherlock", "TST-STAN-x")
    assert patient.threed.wall_model == "rigid"
    assert patient.threed.inflow_boundary_condition == "dirichlet"
    assert patient.threed.tissue_support is not None
    assert patient.threed.tissue_support.enabled is False
    assert patient.threed.nodes == 4
    assert patient.threed.prestress_file == "auto"
    assert patient.threed.procs_per_node == 24


def test_patient_impedance_override_merges_with_defaults(sample_config_files):
    (sample_config_files / "config" / "patients.yaml").write_text(
        f"""
patients:
  - alias: "TST-STAN-x"
    remote_path: "{(sample_config_files / 'remote_data' / 'active' / 'TST-STAN-x').as_posix()}"
    permanent_remote_path: "{(sample_config_files / 'remote_data' / 'permanent' / 'TST-STAN-x').as_posix()}"
    data_policy: "read_only"
    tuning:
      impedance:
        nm_iter: 11
        use_mean: false
""".strip()
        + "\n",
        encoding="utf-8",
    )

    config = load_workspace_config(sample_config_files)
    patient = resolve_patient_alias(config, "sherlock", "TST-STAN-x")
    assert patient.impedance.nm_iter == 11
    assert patient.impedance.use_mean is False
    assert patient.impedance.n_procs == 24


def test_patient_impedance_tune_space_override_replaces_defaults(sample_config_files):
    (sample_config_files / "config" / "patients.yaml").write_text(
        f"""
patients:
  - alias: "TST-STAN-x"
    remote_path: "{(sample_config_files / 'remote_data' / 'active' / 'TST-STAN-x').as_posix()}"
    permanent_remote_path: "{(sample_config_files / 'remote_data' / 'permanent' / 'TST-STAN-x').as_posix()}"
    data_policy: "read_only"
    tuning:
      impedance:
        tune_space:
          free:
            - name: "comp.lpa.k2"
              init: -50.0
              lb: -100.0
              ub: -1.0
          fixed:
            - name: "lrr"
              value: 12.0
          tied:
            - name: "comp.rpa.k2"
              other: "comp.lpa.k2"
              fn: "identity"
""".strip()
        + "\n",
        encoding="utf-8",
    )

    config = load_workspace_config(sample_config_files)
    patient = resolve_patient_alias(config, "sherlock", "TST-STAN-x")
    assert len(patient.impedance.tune_space.free) == 1
    assert patient.impedance.tune_space.free[0].name == "comp.lpa.k2"
    assert patient.impedance.tune_space.fixed[0].name == "lrr"
    assert patient.impedance.tune_space.tied[0].name == "comp.rpa.k2"


def test_patient_rcr_override_merges_with_defaults(sample_config_files):
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
    (sample_config_files / "config" / "patients.yaml").write_text(
        f"""
patients:
  - alias: "TST-STAN-x"
    remote_path: "{(sample_config_files / 'remote_data' / 'active' / 'TST-STAN-x').as_posix()}"
    permanent_remote_path: "{(sample_config_files / 'remote_data' / 'permanent' / 'TST-STAN-x').as_posix()}"
    data_policy: "read_only"
    tuning:
      rcr:
        n_procs: 12
        rescale_inflow: false
""".strip()
        + "\n",
        encoding="utf-8",
    )

    config = load_workspace_config(sample_config_files)
    patient = resolve_patient_alias(config, "sherlock", "TST-STAN-x")
    assert patient.bc_type == "rcr"
    assert patient.rcr.n_procs == 12
    assert patient.rcr.rescale_inflow is False
    assert patient.rcr.convert_to_cm is True


def test_patient_mesh_scale_override_wins_over_default(sample_config_files):
    (sample_config_files / "config" / "patients.yaml").write_text(
        f"""
patients:
  - alias: "TST-STAN-x"
    remote_path: "{(sample_config_files / 'remote_data' / 'active' / 'TST-STAN-x').as_posix()}"
    permanent_remote_path: "{(sample_config_files / 'remote_data' / 'permanent' / 'TST-STAN-x').as_posix()}"
    data_policy: "read_only"
    mesh_scale_factor: 2.2
""".strip()
        + "\n",
        encoding="utf-8",
    )

    config = load_workspace_config(sample_config_files)
    patient = resolve_patient_alias(config, "sherlock", "TST-STAN-x")
    assert patient.mesh_scale_factor == pytest.approx(2.2)


def test_resolve_patient_alias_supports_optional_postop_mesh_layout(sample_config_files):
    postop_mesh_complete = (
        sample_config_files / "remote_data" / "permanent" / "TST-STAN-x" / "postop-mesh-complete"
    )
    (postop_mesh_complete / "mesh-surfaces").mkdir(parents=True, exist_ok=True)

    (sample_config_files / "config" / "defaults.yaml").write_text(
        """
defaults:
  rsync:
    include_patterns: ["*.json", "*.yaml"]
    exclude_patterns: ["*.tmp"]
  artifacts:
    pull: ["manifest.yaml", "results/**"]
  scheduler:
    account: "<account>"
    partition: "<partition>"
    wall_time: "<HH:MM:SS>"
    mem: "<memory>"
    cpus: "<count>"
  validation:
    require_dry_run_before_execute: true
    enforce_remote_write_root: true
  tuning:
    iteration1_seed:
      source: "path"
      path: "simplified_nonlinear_zerod.json"
  patient_data_layout:
    clinical_targets_csv: "clinical_targets.csv"
    centerlines_vtp: "centerlines.vtp"
    inflow_csv: "inflow.csv"
    preop_mesh_complete_dir: "preop-mesh-complete"
    postop_mesh_complete_dir: "postop-mesh-complete"
    mesh_surfaces_subdir: "mesh-surfaces"
""".strip()
        + "\n",
        encoding="utf-8",
    )

    config = load_workspace_config(sample_config_files)
    patient = resolve_patient_alias(config, "sherlock", "TST-STAN-x")
    assert patient.patient_assets is not None
    assert patient.patient_assets.postop_mesh_complete_dir is not None
    assert patient.patient_assets.postop_mesh_complete_dir.endswith("/postop-mesh-complete")
    assert patient.patient_assets.postop_mesh_surfaces_dir is not None
    assert patient.patient_assets.postop_mesh_surfaces_dir.endswith(
        "/postop-mesh-complete/mesh-surfaces"
    )


def test_permanent_remote_path_requires_cluster_permanent_root(temp_workspace: Path):
    (temp_workspace / "config" / "clusters.yaml").write_text(
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
      runs_root: "/scratch/users/ndorn/svzt_runs"
""".strip()
        + "\n",
        encoding="utf-8",
    )
    (temp_workspace / "config" / "patients.yaml").write_text(
        """
patients:
  - alias: "TST-STAN-x"
    remote_path: "/scratch/users/ndorn/models/PPAS/tof-stent/TST-STAN-x"
    permanent_remote_path: "/oak/stanford/groups/amarsden/ndorn/PPAS-study/tof-stent/TST-STAN-x"
    data_policy: "read_only"
""".strip()
        + "\n",
        encoding="utf-8",
    )
    (temp_workspace / "config" / "defaults.yaml").write_text(
        """
defaults:
  rsync:
    include_patterns: ["*.json", "*.yaml"]
    exclude_patterns: ["*.tmp"]
  artifacts:
    pull: ["manifest.yaml", "results/**"]
  scheduler:
    partition: "<partition>"
    wall_time: "<HH:MM:SS>"
    mem: "<memory>"
    cpus: "<count>"
  validation:
    require_dry_run_before_execute: true
    enforce_remote_write_root: true
""".strip()
        + "\n",
        encoding="utf-8",
    )

    config = load_workspace_config(temp_workspace)
    with pytest.raises(ConfigError, match="permanent_data_root is required"):
        resolve_patient_alias(config, "sherlock", "TST-STAN-x")


def test_patient_requires_permanent_remote_path(temp_workspace: Path):
    (temp_workspace / "config" / "clusters.yaml").write_text(
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
    (temp_workspace / "config" / "patients.yaml").write_text(
        """
patients:
  - alias: "TST-STAN-x"
    remote_path: "/scratch/users/ndorn/models/PPAS/tof-stent/TST-STAN-x"
    data_policy: "read_only"
""".strip()
        + "\n",
        encoding="utf-8",
    )
    (temp_workspace / "config" / "defaults.yaml").write_text(
        """
defaults:
  rsync:
    include_patterns: ["*.json", "*.yaml"]
    exclude_patterns: ["*.tmp"]
  artifacts:
    pull: ["manifest.yaml", "results/**"]
  scheduler:
    partition: "<partition>"
    wall_time: "<HH:MM:SS>"
    mem: "<memory>"
    cpus: "<count>"
  validation:
    require_dry_run_before_execute: true
    enforce_remote_write_root: true
""".strip()
        + "\n",
        encoding="utf-8",
    )

    config = load_workspace_config(temp_workspace)
    with pytest.raises(ConfigError, match="must define permanent_remote_path"):
        resolve_patient_alias(config, "sherlock", "TST-STAN-x")
