"""Tied stiffness multiplier and xi floor 2.1 (docs/TUNING_MODEL.md).

``proximal_compliance.tied_k3_ref`` plus a ``stiffness_scale`` tune-space
parameter s: svZeroDTrees sets Eh/r = s * wall_ehr on every seed vessel and
tree k3 = s * tied_k3_ref on both sides, every objective evaluation.
"""

from __future__ import annotations

import copy
import math
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from svztagent.config.load import _resolve_patient_impedance_config, load_workspace_config
from svztagent.config.models import STIFFNESS_SCALE_PARAM, ImpedanceTuningConfig
from svztagent.config.tuning_checks import tuning_model_warnings
from svztagent.workflows.tune_trees import _iteration_impedance_config

TIED_TUNE_SPACE = {
    "free": [
        {"name": "lpa.xi", "init": 2.46, "lb": 2.1, "ub": 3.0},
        {"name": "rpa.xi", "init": 2.17, "lb": 2.1, "ub": 3.0},
        {"name": "lpa.eta_sym", "init": 0.96, "lb": 0.3, "ub": 1.0},
        {"name": "rpa.eta_sym", "init": 0.39, "lb": 0.3, "ub": 1.0},
        {"name": "stiffness_scale", "init": 6.56, "lb": 0.5, "ub": 20.0,
         "to_native": "positive", "from_native": "log"},
    ],
    "fixed": [
        {"name": "lrr", "value": 10.0},
        {"name": "d_min", "value": 0.01},
        {"name": "comp.lpa.k1", "value": 2.6e5},
        {"name": "comp.lpa.k2", "value": -14.0},
    ],
    "tied": [
        {"name": "comp.rpa.k1", "other": "comp.lpa.k1", "fn": "identity"},
        {"name": "comp.rpa.k2", "other": "comp.lpa.k2", "fn": "identity"},
    ],
}
TIED_PROXIMAL = {"wall_ehr": 5.0e4, "tied_k3_ref": 1.0e5}
FULL_PA = {
    "tuning_model": "full_pa",
    "compliance_model": "olufsen",
    "objective_tree_policy": {"use_mean": True, "reference_diameter": "conductance_matched"},
    "proximal_compliance": {"wall_ehr": 5.0e4},
}


def _tied(**change):
    payload = {
        **copy.deepcopy(FULL_PA),
        "proximal_compliance": dict(TIED_PROXIMAL),
        "tune_space": copy.deepcopy(TIED_TUNE_SPACE),
    }
    payload.update(change)
    return payload


def _free(payload, name):
    return next(p for p in payload["tune_space"]["free"] if p["name"] == name)


def test_tied_stiffness_config_renders_free_scale_and_bounds():
    rendered = _iteration_impedance_config(
        ImpedanceTuningConfig.model_validate(_tied()).model_dump(mode="json"), 1, "calibrated_full_pa"
    )
    assert rendered["proximal_compliance"] == TIED_PROXIMAL
    scale = _free(rendered, STIFFNESS_SCALE_PARAM)
    # Fitted in log space within [0.5, 20] (below 1 for softer patients).
    assert (scale["lb"], scale["ub"]) == (0.5, 20.0)
    assert (scale["to_native"], scale["from_native"]) == ("positive", "log")
    for side in ("lpa", "rpa"):
        assert (_free(rendered, f"{side}.xi")["lb"], _free(rendered, f"{side}.xi")["ub"]) == (2.1, 3.0)
        eta = _free(rendered, f"{side}.eta_sym")
        assert (eta["lb"], eta["ub"]) == (0.3, 1.0)
    fixed = {p["name"]: p["value"] for p in rendered["tune_space"]["fixed"]}
    assert fixed == {"lrr": 10.0, "d_min": 0.01, "comp.lpa.k1": 2.6e5, "comp.lpa.k2": -14.0}
    names = {p["name"] for section in rendered["tune_space"].values() for p in section}
    assert not names & {"comp.lpa.k3", "comp.rpa.k3"}


def test_untied_proximal_compliance_renders_unchanged():
    # A cluster svZeroDTrees without tied_k3_ref must keep accepting other patients.
    dumped = ImpedanceTuningConfig.model_validate(FULL_PA).model_dump(mode="json")
    assert dumped["proximal_compliance"] == {"wall_ehr": 5.0e4}


@pytest.mark.parametrize(
    "change, match",
    [
        ({"proximal_compliance": {"wall_ehr": 5.0e4}}, "requires proximal_compliance.tied_k3_ref"),
        ({"proximal_compliance": {"wall_ehr": 5.0e4, "tied_k3_ref": 0.0}}, "tied_k3_ref must be > 0"),
        ({"compliance_model": "constant"}, "olufsen"),
    ],
)
def test_tied_stiffness_rejects_inconsistent_config(change, match):
    with pytest.raises(ValidationError, match=match):
        ImpedanceTuningConfig.model_validate(_tied(**change))


def test_tied_k3_ref_requires_stiffness_scale():
    space = copy.deepcopy(TIED_TUNE_SPACE)
    space["free"] = [p for p in space["free"] if p["name"] != STIFFNESS_SCALE_PARAM]
    with pytest.raises(ValidationError, match="requires a 'stiffness_scale'"):
        ImpedanceTuningConfig.model_validate(_tied(tune_space=space))


def test_tied_stiffness_rejects_explicit_k3_and_nonpositive_scale():
    space = copy.deepcopy(TIED_TUNE_SPACE)
    space["fixed"].append({"name": "comp.lpa.k3", "value": 1.0e5})
    with pytest.raises(ValidationError, match="must not set"):
        ImpedanceTuningConfig.model_validate(_tied(tune_space=space))
    space = copy.deepcopy(TIED_TUNE_SPACE)
    _free({"tune_space": space}, STIFFNESS_SCALE_PARAM).update(
        {"lb": 0.0, "to_native": "identity", "from_native": "identity"}
    )
    with pytest.raises(ValidationError, match="must be > 0"):
        ImpedanceTuningConfig.model_validate(_tied(tune_space=space))


def _write_workspace(workspace: Path, *, patient_impedance: dict | None, threed: dict | None = None) -> None:
    defaults_path = workspace / "config" / "defaults.yaml"
    defaults = yaml.safe_load(defaults_path.read_text())
    defaults["defaults"]["tuning"]["impedance"].update(copy.deepcopy(FULL_PA))
    if threed is not None:
        defaults["defaults"]["tuning"].setdefault("threed", {}).update(threed)
    defaults_path.write_text(yaml.safe_dump(defaults))
    if patient_impedance is not None:
        patients_path = workspace / "config" / "patients.yaml"
        patients = yaml.safe_load(patients_path.read_text())
        patients["patients"][0].setdefault("tuning", {})["impedance"] = patient_impedance
        patients_path.write_text(yaml.safe_dump(patients))


def test_patient_override_enables_tied_stiffness(sample_config_files):
    _write_workspace(
        sample_config_files,
        patient_impedance={"proximal_compliance": dict(TIED_PROXIMAL), "tune_space": copy.deepcopy(TIED_TUNE_SPACE)},
    )
    config = load_workspace_config(sample_config_files)
    resolved = _resolve_patient_impedance_config(config, config.patients[0])
    assert resolved.proximal_compliance.tied_k3_ref == 1.0e5
    assert resolved.objective_tree_policy is not None  # workspace defaults survive
    rendered = _iteration_impedance_config(resolved.model_dump(mode="json"), 2, "calibrated_full_pa")
    assert rendered["proximal_compliance"] == TIED_PROXIMAL
    scale = _free(rendered, STIFFNESS_SCALE_PARAM)
    assert math.log(scale["lb"]) == pytest.approx(math.log(0.5))
    assert scale["ub"] == 20.0


def test_tuning_checks_flag_fixed_3d_wall_with_tied_stiffness(sample_config_files):
    _write_workspace(
        sample_config_files,
        patient_impedance={"proximal_compliance": dict(TIED_PROXIMAL), "tune_space": copy.deepcopy(TIED_TUNE_SPACE)},
        threed={"wall_model": "deformable", "elasticity_modulus": 1.375e5, "shell_thickness": 0.2},
    )
    config = load_workspace_config(sample_config_files)
    warnings = tuning_model_warnings(config, sample_config_files)
    assert any("tied stiffness multiplier" in w for w in warnings)


def test_rri_iteration_rejects_tied_stiffness_before_submission():
    from svztagent.core.errors import ConfigError

    rendered = ImpedanceTuningConfig.model_validate(_tied()).model_dump(mode="json")
    assert _iteration_impedance_config(rendered, 1, "legacy_rri_after_first")["tuning_model"] == "full_pa"
    with pytest.raises(ConfigError, match="stiffness_scale"):
        _iteration_impedance_config(rendered, 2, "legacy_rri_after_first")
