"""Physiological full-PA tuning controls (docs/TUNING_MODEL.md)."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from svztagent.config.load import _resolve_patient_impedance_config, load_workspace_config
from svztagent.config.models import (
    MATCH_PROXIMAL_COMPLIANCE,
    ImpedanceTuningConfig,
    PatientThreedOverrides,
    ThreedTuningConfig,
)
from svztagent.config.tuning_checks import tuning_model_warnings
from svztagent.workflows.tune_trees import _iteration_impedance_config

FULL_PA_IMPEDANCE = {
    "tuning_model": "full_pa",
    "diameter_scale": 1.0,
    "use_mean": False,
    "objective_tree_policy": {"use_mean": True, "reference_diameter": "conductance_matched"},
    "objective": {"type": "likelihood", "pressure_sigma_mmhg": 2.0, "split_sigma": 0.02},
    "keep_diastolic_target": True,
    "wedge_pressure_policy": "diastolic_offset",
    "proximal_compliance": {"wall_ehr": 5.0e4},
    "tree_max_nodes": 1_000_000,
    "polish": {"maxfev": 100},
}


def _set_impedance(workspace: Path, impedance: dict, patient_impedance: dict | None = None) -> None:
    defaults_path = workspace / "config" / "defaults.yaml"
    defaults = yaml.safe_load(defaults_path.read_text())
    defaults["defaults"]["tuning"]["impedance"].update(impedance)
    defaults_path.write_text(yaml.safe_dump(defaults))
    if patient_impedance is not None:
        patients_path = workspace / "config" / "patients.yaml"
        patients = yaml.safe_load(patients_path.read_text())
        patients["patients"][0].setdefault("tuning", {})["impedance"] = patient_impedance
        patients_path.write_text(yaml.safe_dump(patients))


def test_impedance_config_accepts_physiological_controls():
    config = ImpedanceTuningConfig.model_validate(FULL_PA_IMPEDANCE)
    assert config.objective.type == "likelihood"
    assert config.objective.target_sigma == 1.0
    assert config.proximal_compliance.wall_ehr == 5.0e4
    assert config.polish.maxfev == 100
    dumped = config.model_dump(mode="json")
    assert dumped["polish"] == {"maxfev": 100}
    assert dumped["wedge_pressure_policy"] == "diastolic_offset"


@pytest.mark.parametrize(
    "change, match",
    [
        ({"objective_tree_policy": None}, "polish requires objective_tree_policy"),
        ({"tuning_model": "rri", "objective_tree_policy": None, "polish": None}, "proximal_compliance"),
        ({"precapillary_fraction": 1.2}, "precapillary_fraction"),
        ({"proximal_compliance": {"wall_ehr": 0.0}}, "wall_ehr"),
        ({"wedge_pressure_policy": "lagged"}, "wedge_pressure_policy"),
    ],
)
def test_impedance_config_rejects_invalid_controls(change, match):
    with pytest.raises(ValidationError, match=match):
        ImpedanceTuningConfig.model_validate({**FULL_PA_IMPEDANCE, **change})


def test_patient_override_null_clears_workspace_default(sample_config_files):
    _set_impedance(
        sample_config_files,
        FULL_PA_IMPEDANCE,
        patient_impedance={"proximal_compliance": None, "wedge_pressure_policy": "precapillary_fraction"},
    )
    config = load_workspace_config(sample_config_files)
    resolved = _resolve_patient_impedance_config(config, config.patients[0])
    assert resolved.proximal_compliance is None
    assert resolved.wedge_pressure_policy == "precapillary_fraction"
    assert resolved.polish.maxfev == 100  # untouched defaults survive


def test_rri_iterations_drop_full_pa_only_controls():
    rendered = ImpedanceTuningConfig.model_validate(FULL_PA_IMPEDANCE).model_dump(mode="json")
    full_pa = _iteration_impedance_config(rendered, 1, "calibrated_full_pa")
    rri = _iteration_impedance_config(rendered, 2, "legacy_rri_after_first")
    assert full_pa["proximal_compliance"] == {"wall_ehr": 5.0e4}
    assert full_pa["polish"] == {"maxfev": 100}
    assert rri["tuning_model"] == "rri"
    for key in ("proximal_compliance", "polish", "objective_tree_policy"):
        assert key not in rri


def test_tuning_checks_flag_policy_and_wall_mismatches(sample_config_files):
    _set_impedance(sample_config_files, FULL_PA_IMPEDANCE)
    alias = yaml.safe_load((sample_config_files / "config" / "patients.yaml").read_text())["patients"][0]["alias"]
    (sample_config_files / "config" / "clinical_targets.yaml").write_text(
        yaml.safe_dump({"clinical_targets": {"preop": {"patients": {alias: {
            "mpa_pressure": [34.0, 3.0, 16.0], "wedge_pressure": 7.0, "regurgitation": True,
        }}}}})
    )
    config = load_workspace_config(sample_config_files)
    warnings = tuning_model_warnings(config, sample_config_files)
    assert any("regurgitation" in w and "precapillary_fraction" in w for w in warnings)
    assert any("3D wall" in w for w in warnings)


def test_tuning_checks_quiet_when_consistent(sample_config_files):
    _set_impedance(
        sample_config_files,
        {**FULL_PA_IMPEDANCE, "wedge_pressure_policy": "precapillary_fraction", "proximal_compliance": None},
    )
    alias = yaml.safe_load((sample_config_files / "config" / "patients.yaml").read_text())["patients"][0]["alias"]
    (sample_config_files / "config" / "clinical_targets.yaml").write_text(
        yaml.safe_dump({"clinical_targets": {"preop": {"patients": {alias: {
            "mpa_pressure": [34.0, 3.0, 16.0], "wedge_pressure": 7.0, "regurgitation": True,
        }}}}})
    )
    config = load_workspace_config(sample_config_files)
    assert tuning_model_warnings(config, sample_config_files) == []


def test_job_template_guards_unsupported_keys_and_uses_sigma_gate():
    template = (
        Path(__file__).resolve().parents[1] / "src" / "svztagent" / "templates" / "slurm" / "job_template.sh"
    ).read_text()
    assert "SUPPORTED_IMPEDANCE_KEYS" in template
    assert "sys.exit(8)" in template
    assert "**gate_kwargs" in template
    assert '"tuning_diagnostics": tuning.get("tuning_diagnostics")' in template


def test_patient_objective_override_patches_workspace_objective(sample_config_files):
    _set_impedance(
        sample_config_files,
        FULL_PA_IMPEDANCE,
        patient_impedance={"objective": {"target_sigma": 2.0}, "polish": {"initial_simplex_step": 0.05}},
    )
    config = load_workspace_config(sample_config_files)
    resolved = _resolve_patient_impedance_config(config, config.patients[0])
    assert resolved.objective.type == "likelihood"
    assert resolved.objective.target_sigma == 2.0
    assert resolved.objective.pressure_sigma_mmhg == 2.0
    assert resolved.polish.maxfev == 100
    assert resolved.polish.initial_simplex_step == 0.05


def test_patient_objective_override_can_disable_target_stop(sample_config_files):
    _set_impedance(sample_config_files, FULL_PA_IMPEDANCE, patient_impedance={"objective": {"target_sigma": None}})
    config = load_workspace_config(sample_config_files)
    resolved = _resolve_patient_impedance_config(config, config.patients[0])
    assert resolved.objective.type == "likelihood"
    assert resolved.objective.target_sigma is None


def test_patient_can_override_full_pa_workspace_to_rri(sample_config_files):
    _set_impedance(
        sample_config_files,
        {**FULL_PA_IMPEDANCE, "outlet_mapping_mode": "centerline"},
        patient_impedance={
            "tuning_model": "rri",
            "objective_tree_policy": None,
            "proximal_compliance": None,
            "polish": None,
            "outlet_mapping_mode": None,
            "outlet_mapping_centerline": None,
        },
    )
    config = load_workspace_config(sample_config_files)
    resolved = _resolve_patient_impedance_config(config, config.patients[0])
    assert resolved.tuning_model == "rri"
    assert resolved.objective_tree_policy is None
    assert resolved.outlet_mapping_mode is None


def _job_preflight_script(python_executable: Path) -> str:
    """The job template up to the first remote write, with placeholders filled."""
    import re

    template = (
        Path(__file__).resolve().parents[1] / "src" / "svztagent" / "templates" / "slurm" / "job_template.sh"
    ).read_text()
    marker = 'mkdir -p "{{REMOTE_INPUTS_DIR}}"'
    assert marker in template
    prefix = template.split(marker, 1)[0]
    prefix = prefix.replace("{{PYTHON_EXECUTABLE}}", str(python_executable)).replace("{{ENV_HOOKS}}", "")
    prefix = re.sub(r"\{\{[A-Z0-9_]+\}\}", "x", prefix)
    return prefix + "echo REACHED_MAIN\n"


@pytest.mark.parametrize("preflight_status", [0, 7, 8])
def test_job_preflight_failure_stops_the_job(tmp_path, preflight_status):
    import shutil
    import subprocess

    bash = shutil.which("bash")
    if bash is None:
        pytest.skip("bash not available")
    fake_python = tmp_path / "python"
    fake_python.write_text(f"#!/bin/sh\ncat >/dev/null\nexit {preflight_status}\n")
    fake_python.chmod(0o755)
    script = tmp_path / "job.sh"
    script.write_text(_job_preflight_script(fake_python))
    result = subprocess.run([bash, str(script)], capture_output=True, text=True, check=False)
    assert result.returncode == preflight_status
    assert ("REACHED_MAIN" in result.stdout) == (preflight_status == 0)


@pytest.mark.parametrize(
    "scheduler_cpus, tuning, expected",
    [
        ("4", {"n_procs": 1}, "1"),
        ("4", {"n_procs": 24}, "24"),
        ("4", {}, "4"),
        (None, {}, "<count>"),
    ],
)
def test_tune_job_requests_tuning_n_procs(scheduler_cpus, tuning, expected):
    from svztagent.workflows.tune_trees import _resolve_effective_tune_cpus

    assert _resolve_effective_tune_cpus(scheduler_cpus=scheduler_cpus, tuning_config=tuning) == expected


def _template_function(name: str) -> str:
    template = (
        Path(__file__).resolve().parents[1] / "src" / "svztagent" / "templates" / "slurm" / "job_template.sh"
    ).read_text()
    start = template.index(f"def {name}(")
    end = template.index("\ndef ", start + 1)
    return template[start:end]


@pytest.mark.parametrize("modulus, warns", [(1.375e5, False), (2.5e6, True)])
def test_job_records_threed_wall_vs_proximal_compliance(tmp_path, modulus, warns):
    import json

    diagnostics = tmp_path / "tuning_diagnostics.json"
    diagnostics.write_text(json.dumps({"proximal_compliance": {"matched_uniform_wall_eh": 2.751e4}}))
    namespace = {
        "json": json,
        "log": {"warnings": []},
        "threed_config": {"wall_model": "deformable", "elasticity_modulus": modulus, "shell_thickness": 0.2},
    }
    exec(_template_function("_record_threed_wall_match"), namespace)
    namespace["_record_threed_wall_match"](str(diagnostics))
    record = namespace["log"]["threed_wall_vs_proximal_compliance"]
    assert record["ratio"] == pytest.approx(modulus * 0.2 / 2.751e4)
    assert record["matched_elasticity_modulus"] == pytest.approx(2.751e4 / 0.2)
    assert bool(namespace["log"]["warnings"]) is warns


def test_threed_config_accepts_match_proximal_compliance_for_deformable_walls():
    assert ThreedTuningConfig(elasticity_modulus=MATCH_PROXIMAL_COMPLIANCE).elasticity_modulus == MATCH_PROXIMAL_COMPLIANCE
    assert PatientThreedOverrides(elasticity_modulus=MATCH_PROXIMAL_COMPLIANCE).elasticity_modulus == MATCH_PROXIMAL_COMPLIANCE
    with pytest.raises(ValidationError, match="only valid with wall_model=deformable"):
        ThreedTuningConfig(
            elasticity_modulus=MATCH_PROXIMAL_COMPLIANCE, wall_model="rigid", tissue_support=None
        )
    with pytest.raises(ValidationError):
        ThreedTuningConfig(elasticity_modulus="stiff")


def _driver_namespace(monkeypatch, modulus, matched):
    import sys
    import types

    def matched_wall_elasticity_modulus(path, shell_thickness):
        if matched is None:
            raise ValueError("no matched_uniform_wall_eh")
        return {"elasticity_modulus": matched / shell_thickness, "matched_uniform_wall_eh": matched}

    module = types.ModuleType("svzerodtrees.tune_bcs.tuning_diagnostics")
    module.matched_wall_elasticity_modulus = matched_wall_elasticity_modulus
    monkeypatch.setitem(sys.modules, "svzerodtrees.tune_bcs.tuning_diagnostics", module)
    reviews = []
    namespace = {
        "log": {"steps": [], "warnings": []},
        "threed_config": {"elasticity_modulus": modulus, "shell_thickness": 0.2},
        "_mark_needs_review": reviews.append,
        "MATCH_PROXIMAL_COMPLIANCE": MATCH_PROXIMAL_COMPLIANCE,
    }
    exec(_template_function("_resolve_threed_elasticity_modulus"), namespace)
    return namespace, reviews


def test_job_resolves_matched_elasticity_modulus_from_tuning_diagnostics(monkeypatch):
    namespace, reviews = _driver_namespace(monkeypatch, MATCH_PROXIMAL_COMPLIANCE, 4.91e4)
    namespace["_resolve_threed_elasticity_modulus"]("/runs/r/iterations/iter-01/results/tuning_diagnostics.json")
    assert namespace["threed_config"]["elasticity_modulus"] == pytest.approx(4.91e4 / 0.2)
    assert namespace["log"]["threed_elasticity_modulus"]["source"] == MATCH_PROXIMAL_COMPLIANCE
    assert not reviews


def test_job_keeps_explicit_elasticity_modulus(monkeypatch):
    namespace, reviews = _driver_namespace(monkeypatch, 1.375e5, 4.91e4)
    namespace["_resolve_threed_elasticity_modulus"]("/unused.json")
    assert namespace["threed_config"]["elasticity_modulus"] == 1.375e5
    assert "threed_elasticity_modulus" not in namespace["log"]


@pytest.mark.parametrize("diagnostics, matched", [(None, 4.91e4), ("/r/tuning_diagnostics.json", None)])
def test_job_needs_review_when_matched_modulus_is_unavailable(monkeypatch, diagnostics, matched):
    namespace, reviews = _driver_namespace(monkeypatch, MATCH_PROXIMAL_COMPLIANCE, matched)
    namespace["_resolve_threed_elasticity_modulus"](diagnostics)
    assert namespace["threed_config"]["elasticity_modulus"] == MATCH_PROXIMAL_COMPLIANCE
    assert reviews and reviews[0].startswith("threed_elasticity_modulus_unresolved")


LEAF_IMPEDANCE = {
    **FULL_PA_IMPEDANCE,
    "wedge_pressure_policy": "measured",
    "leaf_resistance": {"downstream_fraction": 0.668},
}


def test_leaf_resistance_config_requires_measured_outlet_and_full_pa():
    config = ImpedanceTuningConfig.model_validate(LEAF_IMPEDANCE)
    assert config.leaf_resistance.downstream_fraction == 0.668
    with pytest.raises(ValidationError, match="requires wedge_pressure_policy 'measured'"):
        ImpedanceTuningConfig.model_validate({**LEAF_IMPEDANCE, "wedge_pressure_policy": "diastolic_offset"})
    with pytest.raises(ValidationError, match=r"in \(0, 1\)"):
        ImpedanceTuningConfig.model_validate({**LEAF_IMPEDANCE, "leaf_resistance": {"downstream_fraction": 1.0}})
    with pytest.raises(ValidationError, match="leaf_resistance is supported only"):
        ImpedanceTuningConfig.model_validate(
            {**LEAF_IMPEDANCE, "tuning_model": "rri", "objective_tree_policy": None, "polish": None,
             "proximal_compliance": None}
        )


def test_leaf_resistance_rendering_and_rri_stripping():
    rendered = ImpedanceTuningConfig.model_validate(LEAF_IMPEDANCE).model_dump(mode="json")
    assert _iteration_impedance_config(rendered, 1, "calibrated_full_pa")["leaf_resistance"] == {
        "downstream_fraction": 0.668
    }
    assert "leaf_resistance" not in _iteration_impedance_config(rendered, 2, "legacy_rri_after_first")
    unset = ImpedanceTuningConfig.model_validate(FULL_PA_IMPEDANCE).model_dump(mode="json")
    # Unset optional controls are omitted so older cluster installs accept the payload.
    assert "leaf_resistance" not in _iteration_impedance_config(unset, 1, "calibrated_full_pa")


def test_tuning_checks_with_leaf_resistance_skip_policy_rules(sample_config_files):
    _set_impedance(sample_config_files, {**LEAF_IMPEDANCE, "proximal_compliance": None})
    alias = yaml.safe_load((sample_config_files / "config" / "patients.yaml").read_text())["patients"][0]["alias"]
    (sample_config_files / "config" / "clinical_targets.yaml").write_text(
        yaml.safe_dump({"clinical_targets": {"preop": {"patients": {alias: {
            "mpa_pressure": [34.0, 3.0, 16.0], "regurgitation": True,
        }}}}})
    )
    config = load_workspace_config(sample_config_files)
    warnings = tuning_model_warnings(config, sample_config_files)
    assert len(warnings) == 1 and "measured wedge pressure" in warnings[0]
