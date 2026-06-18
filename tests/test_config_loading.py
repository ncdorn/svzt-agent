from __future__ import annotations

import pytest

from svztagent.config.load import load_workspace_config, resolve_patient_alias
from svztagent.core.errors import ConfigError


def test_load_workspace_config_success(sample_config_files):
    config = load_workspace_config(sample_config_files)
    assert len(config.clusters) == 1
    assert config.clusters[0].name == "sherlock"
    assert (
        config.clusters[0].executables.svfsiplus_path
        == "/home/users/ndorn/svMP-build/svMultiPhysics-build/bin/svmultiphysics"
    )
    assert (
        config.clusters[0].executables.svzerodsolver_build_dir
        == "/home/users/ndorn/svZeroDSolver-build"
    )
    assert config.clusters[0].executables.svslicer_path == "/home/users/ndorn/bin/svslicer"
    assert config.defaults.validation.enforce_remote_write_root is True
    assert config.defaults.patient_data_layout.clinical_targets_csv == "clinical_targets.csv"
    assert config.defaults.tuning.iteration1_seed.source == "path"
    assert config.defaults.tuning.iteration1_seed.path == "simplified_nonlinear_zerod.json"
    assert config.defaults.tuning.bc_type == "impedance"
    assert config.defaults.tuning.threed.wall_model == "deformable"
    assert config.defaults.tuning.threed.inflow_boundary_condition == "neumann"
    assert config.defaults.tuning.threed.prestress_file == "auto"
    assert config.defaults.tuning.threed.tissue_support is not None
    assert config.defaults.tuning.threed.tissue_support.enabled is True
    assert config.defaults.tuning.threed.tissue_support.type == "uniform"
    assert config.defaults.tuning.threed.tissue_support.stiffness == pytest.approx(1000.0)
    assert config.defaults.tuning.threed.tissue_support.damping == pytest.approx(10000.0)
    assert config.defaults.tuning.threed.tissue_support.apply_along_normal_direction is True
    assert config.defaults.mesh_scale_factor == pytest.approx(1.0)
    assert config.defaults.tuning.impedance.solver == "Nelder-Mead"
    assert config.defaults.tuning.impedance.nm_iter == 5
    assert config.defaults.tuning.impedance.compliance_model == "olufsen"
    assert config.defaults.tuning.impedance.convert_to_cm is False
    assert config.defaults.tuning.impedance.tuning_model == "rri"
    assert config.defaults.tuning.impedance.diameter_std_cap is None
    assert config.defaults.tuning.impedance.tune_space.free[0].name == "lpa.xi"
    assert config.defaults.tuning.impedance.tune_space.free[-1].name == "comp.lpa.k2"
    assert config.defaults.tuning.impedance.tune_space.tied[0].name == "comp.rpa.k2"
    assert config.defaults.tuning.impedance.use_mean is True
    assert config.defaults.tuning.rcr.solver == "Nelder-Mead"
    assert config.defaults.tuning.rcr.n_procs == 24
    assert config.defaults.tuning.rcr.rescale_inflow is True
    assert config.defaults.execution.python_executable == "python3"
    assert config.defaults.postprocess.resistance_map.workers == "auto"
    assert config.defaults.postprocess.resistance_map.selected_preop_mem == "64G"


def test_load_workspace_config_supports_patient_seed_override(sample_config_files):
    (sample_config_files / "config" / "patients.yaml").write_text(
        """
patients:
  - alias: "TST-STAN-x"
    remote_path: "/tmp/active/TST-STAN-x"
    permanent_remote_path: "/tmp/permanent/TST-STAN-x"
    data_policy: "read_only"
    tuning:
      iteration1_seed:
        source: "generate"
        path: "/tmp/custom_seed.json"
""".strip()
        + "\n",
        encoding="utf-8",
    )

    config = load_workspace_config(sample_config_files)
    assert config.patients[0].tuning is not None
    assert config.patients[0].tuning.iteration1_seed is not None
    assert config.patients[0].tuning.iteration1_seed.source == "generate"
    assert config.patients[0].tuning.iteration1_seed.path == "/tmp/custom_seed.json"


def test_load_workspace_config_supports_patient_threed_override(sample_config_files):
    (sample_config_files / "config" / "patients.yaml").write_text(
        """
patients:
  - alias: "TST-STAN-x"
    remote_path: "/tmp/active/TST-STAN-x"
    permanent_remote_path: "/tmp/permanent/TST-STAN-x"
    data_policy: "read_only"
    tuning:
      threed:
        wall_model: "rigid"
        inflow_boundary_condition: "dirichlet"
        tissue_support:
          enabled: false
        n_tsteps: 3000
        wait_timeout_seconds: 3600
""".strip()
        + "\n",
        encoding="utf-8",
    )
    config = load_workspace_config(sample_config_files)
    assert config.patients[0].tuning is not None
    assert config.patients[0].tuning.threed is not None
    assert config.patients[0].tuning.threed.wall_model == "rigid"
    assert config.patients[0].tuning.threed.inflow_boundary_condition == "dirichlet"
    assert config.patients[0].tuning.threed.tissue_support is not None
    assert config.patients[0].tuning.threed.tissue_support.enabled is False
    assert config.patients[0].tuning.threed.n_tsteps == 3000
    assert config.patients[0].tuning.threed.wait_timeout_seconds == 3600


def test_load_workspace_config_supports_threed_slurm_mail_defaults(sample_config_files):
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

    config = load_workspace_config(sample_config_files)
    assert config.defaults.tuning.threed.execution.slurm.mail_user == "user@example.com"
    assert config.defaults.tuning.threed.execution.slurm.mail_types == ["fail", "end"]


def test_load_workspace_config_merges_patient_threed_slurm_mail_override(sample_config_files):
    active_patient_path = sample_config_files / "remote_data" / "active" / "TST-STAN-x"
    permanent_patient_path = sample_config_files / "remote_data" / "permanent" / "TST-STAN-x"
    (sample_config_files / "config" / "defaults.yaml").write_text(
        """
defaults:
  tuning:
    threed:
      execution:
        slurm:
          mail_user: "default@example.com"
          mail_types: ["begin", "end"]
""".strip()
        + "\n",
        encoding="utf-8",
    )
    (sample_config_files / "config" / "patients.yaml").write_text(
        f"""
patients:
  - alias: "TST-STAN-x"
    remote_path: "{active_patient_path.as_posix()}"
    permanent_remote_path: "{permanent_patient_path.as_posix()}"
    data_policy: "read_only"
    tuning:
      threed:
        execution:
          slurm:
            mail_user: "patient@example.com"
""".strip()
        + "\n",
        encoding="utf-8",
    )

    config = load_workspace_config(sample_config_files)
    patient = resolve_patient_alias(config, "sherlock", "TST-STAN-x")
    assert patient.threed.execution.slurm.mail_user == "patient@example.com"
    assert patient.threed.execution.slurm.mail_types == ["begin", "end"]


def test_load_workspace_config_preserves_generate_prestress_mode(sample_config_files):
    (sample_config_files / "config" / "patients.yaml").write_text(
        """
patients:
  - alias: "TST-STAN-x"
    remote_path: "/tmp/active/TST-STAN-x"
    permanent_remote_path: "/tmp/permanent/TST-STAN-x"
    data_policy: "read_only"
    tuning:
      threed:
        wall_model: "deformable"
        prestress_file: "generate"
""".strip()
        + "\n",
        encoding="utf-8",
    )

    config = load_workspace_config(sample_config_files)
    assert config.patients[0].tuning is not None
    assert config.patients[0].tuning.threed is not None
    assert config.patients[0].tuning.threed.prestress_file == "generate"


def test_load_workspace_config_supports_patient_tissue_support_override(sample_config_files):
    (sample_config_files / "config" / "patients.yaml").write_text(
        """
patients:
  - alias: "TST-STAN-x"
    remote_path: "/tmp/active/TST-STAN-x"
    permanent_remote_path: "/tmp/permanent/TST-STAN-x"
    data_policy: "read_only"
    tuning:
      threed:
        tissue_support:
          enabled: true
          type: "uniform"
          stiffness: 2500.0
          damping: 12000.0
          apply_along_normal_direction: false
""".strip()
        + "\n",
        encoding="utf-8",
    )
    config = load_workspace_config(sample_config_files)
    support = config.patients[0].tuning.threed.tissue_support
    assert support is not None
    assert support.stiffness == pytest.approx(2500.0)
    assert support.damping == pytest.approx(12000.0)
    assert support.apply_along_normal_direction is False


def test_invalid_threed_inflow_boundary_condition_fails(sample_config_files):
    (sample_config_files / "config" / "defaults.yaml").write_text(
        """
defaults:
  tuning:
    threed:
      inflow_boundary_condition: "bad-mode"
""".strip()
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match="inflow_boundary_condition"):
        load_workspace_config(sample_config_files)


def test_load_workspace_config_supports_patient_impedance_override(sample_config_files):
    (sample_config_files / "config" / "patients.yaml").write_text(
        """
patients:
  - alias: "TST-STAN-x"
    remote_path: "/tmp/active/TST-STAN-x"
    permanent_remote_path: "/tmp/permanent/TST-STAN-x"
    data_policy: "read_only"
    tuning:
      impedance:
        nm_iter: 9
        n_procs: 12
        use_mean: false
        tuning_model: "full_pa"
        diameter_std_cap: 1.5
""".strip()
        + "\n",
        encoding="utf-8",
    )

    config = load_workspace_config(sample_config_files)
    assert config.patients[0].tuning is not None
    assert config.patients[0].tuning.impedance is not None
    assert config.patients[0].tuning.impedance.nm_iter == 9
    assert config.patients[0].tuning.impedance.n_procs == 12
    assert config.patients[0].tuning.impedance.use_mean is False
    assert config.patients[0].tuning.impedance.tuning_model == "full_pa"
    assert config.patients[0].tuning.impedance.diameter_std_cap == pytest.approx(1.5)


def test_load_workspace_config_supports_rcr_tuning_defaults_and_override(sample_config_files):
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
        """
patients:
  - alias: "TST-STAN-x"
    remote_path: "/tmp/active/TST-STAN-x"
    permanent_remote_path: "/tmp/permanent/TST-STAN-x"
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
    assert config.defaults.tuning.bc_type == "rcr"
    assert config.defaults.tuning.rcr.n_procs == 8
    assert config.defaults.tuning.rcr.convert_to_cm is True
    assert config.patients[0].tuning is not None
    assert config.patients[0].tuning.rcr is not None
    assert config.patients[0].tuning.rcr.n_procs == 12
    assert config.patients[0].tuning.rcr.rescale_inflow is False


def test_invalid_impedance_tuning_model_fails(sample_config_files):
    (sample_config_files / "config" / "defaults.yaml").write_text(
        """
defaults:
  tuning:
    impedance:
      tuning_model: "whole_model"
""".strip()
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match="tuning_model"):
        load_workspace_config(sample_config_files)


def test_invalid_tuning_bc_type_fails(sample_config_files):
    (sample_config_files / "config" / "defaults.yaml").write_text(
        """
defaults:
  tuning:
    bc_type: "windkessel"
""".strip()
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match="bc_type"):
        load_workspace_config(sample_config_files)


def test_invalid_impedance_diameter_std_cap_fails(sample_config_files):
    (sample_config_files / "config" / "defaults.yaml").write_text(
        """
defaults:
  tuning:
    impedance:
      diameter_std_cap: -0.1
""".strip()
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match="diameter_std_cap"):
        load_workspace_config(sample_config_files)


def test_load_workspace_config_supports_patient_mesh_scale_override(sample_config_files):
    (sample_config_files / "config" / "patients.yaml").write_text(
        """
patients:
  - alias: "TST-STAN-x"
    remote_path: "/tmp/active/TST-STAN-x"
    permanent_remote_path: "/tmp/permanent/TST-STAN-x"
    data_policy: "read_only"
    mesh_scale_factor: 1.8
""".strip()
        + "\n",
        encoding="utf-8",
    )
    config = load_workspace_config(sample_config_files)
    assert config.patients[0].mesh_scale_factor == pytest.approx(1.8)


def test_load_workspace_config_requires_absolute_svfsiplus_path(sample_config_files):
    (sample_config_files / "config" / "clusters.yaml").write_text(
        """
clusters:
  - name: "sherlock"
    host: "sherlock.stanford.edu"
    user: "ndorn"
    scheduler:
      type: "slurm"
    executables:
      svfsiplus_path: "relative/svmultiphysics"
    remote_roots:
      patient_data_root: "/tmp/active"
      permanent_data_root: "/tmp/permanent"
      runs_root: "/tmp/runs"
""".strip()
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match="svfsiplus_path"):
        load_workspace_config(sample_config_files)


def test_load_workspace_config_requires_absolute_svzerodsolver_build_dir(sample_config_files):
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
      svzerodsolver_build_dir: "relative/svZeroDSolver-build"
    remote_roots:
      patient_data_root: "/tmp/active"
      permanent_data_root: "/tmp/permanent"
      runs_root: "/tmp/runs"
""".strip()
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match="svzerodsolver_build_dir"):
        load_workspace_config(sample_config_files)


def test_load_workspace_config_requires_absolute_svslicer_path(sample_config_files):
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
      svslicer_path: "relative/svslicer"
    remote_roots:
      patient_data_root: "/tmp/active"
      permanent_data_root: "/tmp/permanent"
      runs_root: "/tmp/runs"
""".strip()
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match="svslicer_path"):
        load_workspace_config(sample_config_files)


def test_missing_required_config_fails_fast(sample_config_files):
    (sample_config_files / "config" / "defaults.yaml").write_text("not_defaults: {}\n", encoding="utf-8")

    with pytest.raises(ConfigError, match="top-level key 'defaults'"):
        load_workspace_config(sample_config_files)


def test_invalid_iteration1_seed_relative_path_fails(sample_config_files):
    (sample_config_files / "config" / "defaults.yaml").write_text(
        """
defaults:
  tuning:
    iteration1_seed:
      source: "path"
      path: "../bad_seed.json"
""".strip()
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match="iteration-1 seed path cannot contain"):
        load_workspace_config(sample_config_files)


def test_invalid_impedance_defaults_fail_validation(sample_config_files):
    (sample_config_files / "config" / "defaults.yaml").write_text(
        """
defaults:
  tuning:
    impedance:
      nm_iter: 0
""".strip()
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match="nm_iter"):
        load_workspace_config(sample_config_files)


def test_invalid_impedance_tune_space_transform_fails(sample_config_files):
    (sample_config_files / "config" / "defaults.yaml").write_text(
        """
defaults:
  tuning:
    impedance:
      tune_space:
        free:
          - name: "lpa.xi"
            init: 2.3
            lb: 0.0
            ub: 6.0
            to_native: "bad_transform"
""".strip()
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match="to_native"):
        load_workspace_config(sample_config_files)


def test_invalid_impedance_inf_string_fails(sample_config_files):
    (sample_config_files / "config" / "defaults.yaml").write_text(
        """
defaults:
  tuning:
    impedance:
      tune_space:
        free:
          - name: "lpa.inductance"
            init: 1.0
            lb: 0.0
            ub: "infinity"
""".strip()
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match="bound strings"):
        load_workspace_config(sample_config_files)


def test_load_workspace_config_supports_postprocess_defaults(sample_config_files):
    (sample_config_files / "config" / "defaults.yaml").write_text(
        """
defaults:
  postprocess:
    resistance_map:
      workers: 3
      selected_preop_mem: "96G"
""".strip()
        + "\n",
        encoding="utf-8",
    )

    config = load_workspace_config(sample_config_files)
    assert config.defaults.postprocess.resistance_map.workers == 3
    assert config.defaults.postprocess.resistance_map.selected_preop_mem == "96G"


def test_invalid_postprocess_workers_fail_validation(sample_config_files):
    (sample_config_files / "config" / "defaults.yaml").write_text(
        """
defaults:
  postprocess:
    resistance_map:
      workers: 0
""".strip()
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match="workers"):
        load_workspace_config(sample_config_files)


def test_invalid_mesh_scale_factor_fails(sample_config_files):
    (sample_config_files / "config" / "defaults.yaml").write_text(
        """
defaults:
  mesh_scale_factor: 0.0
""".strip()
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match="mesh_scale_factor"):
        load_workspace_config(sample_config_files)
