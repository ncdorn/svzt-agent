from __future__ import annotations

import pytest
import yaml
from pydantic import ValidationError

from svztagent.config.load import load_workspace_config, resolve_patient_alias
from svztagent.config.models import ImpedanceTuningConfig
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


def test_resolve_patient_supports_learned_zerod_seed_source(sample_config_files):
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
""".strip()
        + "\n",
        encoding="utf-8",
    )

    config = load_workspace_config(sample_config_files)
    patient = resolve_patient_alias(config, "sherlock", "TST-STAN-x")

    assert patient.patient_assets is not None
    assert patient.patient_assets.iteration1_seed_source == "learned_zerod"
    assert patient.impedance.outlet_mapping_mode == "auto"
    assert patient.impedance.outlet_mapping_centerline == patient.patient_assets.centerlines
    assert (
        patient.patient_assets.iteration1_seed_learned_zerod_executable
        == "/opt/learned/bin/learned-zerod"
    )
    assert (
        patient.patient_assets.iteration1_seed_svzerodsolver_executable
        == "/opt/svzerod/bin/svzerodsolver"
    )


def test_resolve_patient_learned_seed_inherits_default_executables(
    sample_config_files,
):
    defaults_path = sample_config_files / "config" / "defaults.yaml"
    defaults_path.write_text(
        defaults_path.read_text(encoding="utf-8").replace(
            'source: "path"\n      path: "simplified_nonlinear_zerod.json"',
            'source: "path"\n      path: "simplified_nonlinear_zerod.json"\n'
            '      learned_zerod_executable: "/opt/learned/bin/learned-zerod"\n'
            '      svzerodsolver_executable: "/opt/svzerod/bin/svzerodsolver"',
        ),
        encoding="utf-8",
    )
    patient_root = sample_config_files / "remote_data" / "permanent" / "TST-STAN-x"
    (sample_config_files / "config" / "patients.yaml").write_text(
        f'''
patients:
  - alias: "TST-STAN-x"
    permanent_remote_path: "{patient_root.as_posix()}"
    data_policy: "read_only"
    tuning:
      iteration1_seed:
        source: "learned_zerod"
        path: "baseline_0d.json"
      impedance:
        tuning_model: "full_pa"
'''.strip()
        + "\n",
        encoding="utf-8",
    )

    patient = resolve_patient_alias(
        load_workspace_config(sample_config_files), "sherlock", "TST-STAN-x"
    )

    assert patient.patient_assets is not None
    assert patient.patient_assets.iteration1_seed_path.endswith("baseline_0d.json")
    assert (
        patient.patient_assets.iteration1_seed_learned_zerod_executable
        == "/opt/learned/bin/learned-zerod"
    )
    assert (
        patient.patient_assets.iteration1_seed_svzerodsolver_executable
        == "/opt/svzerod/bin/svzerodsolver"
    )


def test_resolve_patient_rejects_learned_zerod_for_reduced_tuning(
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
""".strip()
        + "\n",
        encoding="utf-8",
    )

    config = load_workspace_config(sample_config_files)
    with pytest.raises(
        ConfigError,
        match="source='learned_zerod'.*tuning_model='full_pa'",
    ):
        resolve_patient_alias(config, "sherlock", "TST-STAN-x")


def _write_full_pa_patient(sample_config_files, impedance_yaml: str) -> str:
    patient_root = (
        sample_config_files / "remote_data" / "permanent" / "TST-STAN-x"
    ).as_posix()
    (sample_config_files / "config" / "patients.yaml").write_text(
        f"""
patients:
  - alias: "TST-STAN-x"
    permanent_remote_path: "{patient_root}"
    data_policy: "read_only"
    tuning:
      impedance:
        tuning_model: "full_pa"
{impedance_yaml}
""".strip()
        + "\n",
        encoding="utf-8",
    )
    return patient_root


def test_resolve_patient_full_pa_centerline_mode_defaults_to_patient_centerline(
    sample_config_files,
):
    patient_root = _write_full_pa_patient(
        sample_config_files, '        outlet_mapping_mode: "centerline"'
    )

    patient = resolve_patient_alias(
        load_workspace_config(sample_config_files), "sherlock", "TST-STAN-x"
    )

    assert patient.impedance.outlet_mapping_mode == "centerline"
    assert patient.impedance.outlet_mapping_centerline == f"{patient_root}/centerlines.vtp"


def test_resolve_patient_full_pa_relative_mapping_centerline_uses_patient_root(
    sample_config_files,
):
    patient_root = _write_full_pa_patient(
        sample_config_files,
        '        outlet_mapping_mode: "centerline"\n'
        '        outlet_mapping_centerline: "seed-source/centerlines.vtp"',
    )

    patient = resolve_patient_alias(
        load_workspace_config(sample_config_files), "sherlock", "TST-STAN-x"
    )

    assert (
        patient.impedance.outlet_mapping_centerline
        == f"{patient_root}/seed-source/centerlines.vtp"
    )


def test_resolve_patient_full_pa_absolute_mapping_centerline_is_kept(
    sample_config_files,
):
    _write_full_pa_patient(
        sample_config_files,
        '        outlet_mapping_centerline: "/tmp/other/centerlines.vtp"',
    )

    patient = resolve_patient_alias(
        load_workspace_config(sample_config_files), "sherlock", "TST-STAN-x"
    )

    assert patient.impedance.outlet_mapping_mode == "auto"
    assert patient.impedance.outlet_mapping_centerline == "/tmp/other/centerlines.vtp"


@pytest.mark.parametrize("mode", ["metadata", "cap_name", "serialized_cap_order"])
def test_resolve_patient_full_pa_non_geometric_mode_has_no_mapping_centerline(
    sample_config_files, mode
):
    _write_full_pa_patient(sample_config_files, f'        outlet_mapping_mode: "{mode}"')

    patient = resolve_patient_alias(
        load_workspace_config(sample_config_files), "sherlock", "TST-STAN-x"
    )

    assert patient.impedance.outlet_mapping_mode == mode
    assert patient.impedance.outlet_mapping_centerline is None


def test_mapping_centerline_rejected_for_non_geometric_mode():
    with pytest.raises(ValueError, match="used only by outlet_mapping_mode 'auto' or 'centerline'"):
        ImpedanceTuningConfig.model_validate(
            {
                "tuning_model": "full_pa",
                "outlet_mapping_mode": "metadata",
                "outlet_mapping_centerline": "/tmp/centerlines.vtp",
            }
        )


def test_mapping_centerline_rejected_for_rri():
    with pytest.raises(ValueError, match="only for tuning_model='full_pa'"):
        ImpedanceTuningConfig.model_validate(
            {"tuning_model": "rri", "outlet_mapping_centerline": "/tmp/centerlines.vtp"}
        )


def test_resolve_patient_rri_has_no_mapping_centerline(sample_config_files):
    patient = resolve_patient_alias(
        load_workspace_config(sample_config_files), "sherlock", "TST-STAN-x"
    )

    assert patient.impedance.tuning_model == "rri"
    assert patient.impedance.outlet_mapping_mode is None
    assert patient.impedance.outlet_mapping_centerline is None


def test_wedge_pressure_policy_defaults_to_upstream_clamp():
    config = ImpedanceTuningConfig.model_validate({"tuning_model": "full_pa"})

    assert config.model_dump(mode="json")["wedge_pressure_policy"] == "clamp_to_diastolic"


def test_wedge_pressure_policy_rejects_unknown_value():
    with pytest.raises(ValidationError):
        ImpedanceTuningConfig.model_validate({"wedge_pressure_policy": "mean"})


def test_patient_wedge_pressure_policy_override(sample_config_files):
    patients_path = sample_config_files / "config" / "patients.yaml"
    payload = yaml.safe_load(patients_path.read_text())
    payload["patients"][0].setdefault("tuning", {}).setdefault("impedance", {})[
        "wedge_pressure_policy"
    ] = "measured"
    patients_path.write_text(yaml.safe_dump(payload))

    config = load_workspace_config(sample_config_files)
    alias = payload["patients"][0]["alias"]
    patient = resolve_patient_alias(config, "sherlock", alias)

    assert config.defaults.tuning.impedance.wedge_pressure_policy == "clamp_to_diastolic"
    assert patient.impedance.wedge_pressure_policy == "measured"
    assert patient.impedance.model_dump(mode="json")["wedge_pressure_policy"] == "measured"


def test_impedance_config_omits_unset_objective_tree_policy():
    config = ImpedanceTuningConfig.model_validate({"tuning_model": "full_pa"})

    assert config.objective_tree_policy is None
    assert config.model_dump(mode="json")["objective_tree_policy"] is None


def test_objective_tree_policy_serializes_only_set_fields():
    config = ImpedanceTuningConfig.model_validate(
        {
            "tuning_model": "full_pa",
            "diameter_scale": 0.2,
            "diameter_std_cap": 1.5,
            "objective_tree_policy": {
                "use_mean": True,
                "diameter_std_cap": None,
                "reference_diameter": "conductance_matched",
            },
        }
    )

    # Omitted diameter_scale must stay omitted so svZeroDTrees inherits the
    # final policy; explicit diameter_std_cap: null means "no cap".
    assert config.model_dump(mode="json")["objective_tree_policy"] == {
        "use_mean": True,
        "diameter_std_cap": None,
        "reference_diameter": "conductance_matched",
    }


@pytest.mark.parametrize(
    "policy",
    [
        {"unknown_key": 1},
        {"reference_diameter": "geometric_mean"},
        {"diameter_scale": -0.1},
        {"diameter_std_cap": -1.0},
    ],
)
def test_objective_tree_policy_rejects_invalid_fields(policy):
    with pytest.raises(ValueError):
        ImpedanceTuningConfig.model_validate(
            {"tuning_model": "full_pa", "objective_tree_policy": policy}
        )


def test_objective_tree_policy_rejected_for_rri():
    with pytest.raises(ValueError, match="objective_tree_policy is supported only"):
        ImpedanceTuningConfig.model_validate(
            {"tuning_model": "rri", "objective_tree_policy": {"use_mean": True}}
        )


def test_resolve_patient_full_pa_objective_tree_policy_override(sample_config_files):
    _write_full_pa_patient(
        sample_config_files,
        "        diameter_scale: 0.2\n"
        "        objective_tree_policy:\n"
        "          use_mean: true\n"
        "          diameter_std_cap: null\n"
        '          reference_diameter: "conductance_matched"',
    )

    patient = resolve_patient_alias(
        load_workspace_config(sample_config_files), "sherlock", "TST-STAN-x"
    )

    assert patient.impedance.model_dump(mode="json")["objective_tree_policy"] == {
        "use_mean": True,
        "diameter_std_cap": None,
        "reference_diameter": "conductance_matched",
    }


def test_resolve_patient_rejects_unknown_objective_tree_policy_key(sample_config_files):
    _write_full_pa_patient(
        sample_config_files,
        "        objective_tree_policy:\n          reference: \"conductance_matched\"",
    )

    with pytest.raises(ConfigError):
        load_workspace_config(sample_config_files)


def test_impedance_config_enables_nelder_mead_stopping_by_default():
    stopping = ImpedanceTuningConfig().model_dump(mode="json")["stopping"]

    assert stopping["enabled"] is True
    assert stopping["target_tolerance"] == 0.025
    assert stopping["maxfev"] == 200


@pytest.mark.parametrize(
    "stopping",
    [{"maxfev": 0}, {"xatol": -1.0}, {"initial_simplex_step": 0.9}, {"unknown": 1}],
)
def test_nelder_mead_stopping_rejects_invalid_fields(stopping):
    with pytest.raises(ValueError):
        ImpedanceTuningConfig.model_validate({"stopping": stopping})


def test_resolve_patient_stopping_override_patches_fields(sample_config_files):
    _write_full_pa_patient(
        sample_config_files,
        "        stopping:\n"
        "          maxfev: 120\n"
        "          target_tolerance: null",
    )

    patient = resolve_patient_alias(
        load_workspace_config(sample_config_files), "sherlock", "TST-STAN-x"
    )

    stopping = patient.impedance.model_dump(mode="json")["stopping"]
    assert stopping["maxfev"] == 120
    # An explicit null survives the patch and disables the target stop.
    assert stopping["target_tolerance"] is None
    assert stopping["stall_rel_improvement"] == 0.01


def test_load_workspace_config_supports_patient_threed_override(sample_config_files):
    (sample_config_files / "config" / "patients.yaml").write_text(
        """
patients:
  - alias: "TST-STAN-x"
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
