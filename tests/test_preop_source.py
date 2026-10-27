from __future__ import annotations

from types import SimpleNamespace

import pytest

from svztagent.core.errors import ConfigError
from svztagent.core.manifest import ConvergedPreopIteration
from svztagent.core.preop_source import apply_threed_overrides, resolve_preop_source

ROOT = "/scratch/users/ndorn/svzt_runs/run-x"
THREED_DEFAULTS = {
    "elasticity_modulus": "match_proximal_compliance",
    "n_tsteps": 6000,
    "tissue_support": {"enabled": True, "stiffness": 1000.0, "damping": 10000.0},
}


def _manifest(handoff: ConvergedPreopIteration | None):
    return SimpleNamespace(run_id="run-x", converged_preop_iteration=handoff, remote={"threed_defaults": THREED_DEFAULTS})


def _handoff(**overrides) -> ConvergedPreopIteration:
    fields = dict(
        iteration=2,
        selection_kind="manual",
        selected_at="2026-10-26T00:00:00+00:00",
        remote_iteration_dir=f"{ROOT}/iterations/iter-02",
        remote_preop_dir=f"{ROOT}/iterations/iter-02/preop",
        remote_tuned_zerod_config=f"{ROOT}/iterations/iter-02/results/svzerod_3d_coupling_tuned.json",
        remote_canonical_coupler=f"{ROOT}/iterations/iter-02/preop/svzerod_3Dcoupling.json",
    )
    fields.update(overrides)
    return ConvergedPreopIteration(**fields)


def test_legacy_source_reads_the_iteration_directory_and_threed_defaults():
    source = resolve_preop_source(_manifest(_handoff()))
    assert source.iteration == 2 and not source.is_wall_correction
    assert source.remote_threed_dir == f"{ROOT}/iterations/iter-02/preop"
    assert source.remote_tuning_results_dir == f"{ROOT}/iterations/iter-02/results"
    assert source.remote_zerod_model.endswith("svzerod_3d_coupling_tuned.json")
    assert source.threed_config == THREED_DEFAULTS
    assert source.handoff_command == ("svzt", "preop", "select", "--run-id", "run-x", "--iteration", "2")


def test_wall_correction_source_carries_its_wall_and_tuning_results():
    checks = "/scratch/users/ndorn/svzt_runs/_checks/elaw/x/preop"
    source = resolve_preop_source(
        _manifest(
            _handoff(
                selection_kind="wall_correction",
                remote_preop_dir=checks,
                remote_canonical_coupler=f"{checks}/svzerod_3Dcoupling.json",
                remote_tuned_zerod_config=f"{ROOT}/wall_correction/iter-02/calibration/results/calibrated_full_pa_zerod.json",
                wall_correction_id="wallcorr-x",
                remote_tuning_results_dir=f"{ROOT}/iterations/iter-02/results",
                threed_overrides={"elasticity_modulus": 810717.3883845098, "damping": 7694.9},
            )
        )
    )
    assert source.is_wall_correction and source.wall_correction_id == "wallcorr-x"
    assert source.remote_threed_dir == checks and source.remote_coupler == f"{checks}/svzerod_3Dcoupling.json"
    # The tuned trees stay in the source iteration, not beside the adopted 3D run.
    assert source.remote_tuning_results_dir == f"{ROOT}/iterations/iter-02/results"
    assert source.threed_config["elasticity_modulus"] == 810717.3883845098
    assert source.threed_config["tissue_support"] == {"enabled": True, "stiffness": 1000.0, "damping": 7694.9}
    assert source.handoff_command[:3] == ("svzt", "preop", "finalize")
    # The workspace defaults themselves are not mutated.
    assert THREED_DEFAULTS["elasticity_modulus"] == "match_proximal_compliance"
    assert THREED_DEFAULTS["tissue_support"]["damping"] == 10000.0


def test_missing_or_incomplete_sources_fail_fast():
    with pytest.raises(ConfigError, match="no pre-op source; run svzt run wall-correction"):
        resolve_preop_source(_manifest(None))
    with pytest.raises(ConfigError, match="without its correction id"):
        resolve_preop_source(_manifest(_handoff(selection_kind="wall_correction")))


def test_overrides_keep_key_order():
    updated = apply_threed_overrides(THREED_DEFAULTS, {"damping": 1.0})
    assert list(updated) == list(THREED_DEFAULTS)
    assert apply_threed_overrides(THREED_DEFAULTS, None) == THREED_DEFAULTS
