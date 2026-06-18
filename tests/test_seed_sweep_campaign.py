from __future__ import annotations

from pathlib import Path
import json
import shutil

import yaml

from svztagent.campaigns import seed_sweep
from svztagent.hpc.interfaces import ExecutionMode


def _switch_to_sibling_repo_layout(workspace: Path) -> dict[str, Path]:
    shutil.rmtree(workspace / "repos")
    sibling_root = workspace.parent
    paths = {}
    for name in ("svzt-agent", "svZeroDTrees", "svZeroDSolver"):
        path = sibling_root / name
        path.mkdir(parents=True, exist_ok=True)
        paths[name] = path
    return paths


def _add_campaign_patients(workspace: Path) -> None:
    patients_path = workspace / "config" / "patients.yaml"
    payload = yaml.safe_load(patients_path.read_text(encoding="utf-8"))
    template = payload["patients"][0]
    patients = []
    for alias in ("TST-STAN-1", "TST-STAN-5"):
        entry = dict(template)
        entry["alias"] = alias
        entry["permanent_remote_path"] = str(
            Path(template["permanent_remote_path"]).parent / alias
        )
        patients.append(entry)

        permanent = Path(entry["permanent_remote_path"])
        (permanent / "preop-mesh-complete" / "mesh-surfaces").mkdir(
            parents=True, exist_ok=True
        )
        (permanent / "clinical_targets.csv").write_text("target,value\n", encoding="utf-8")
        (permanent / "centerlines.vtp").write_text("<vtk/>", encoding="utf-8")
        (permanent / "inflow.csv").write_text("t,q\n0,0\n", encoding="utf-8")
        (permanent / "simplified_nonlinear_zerod.json").write_text(
            "{\"seed\": true}", encoding="utf-8"
        )

    patients_path.write_text(
        yaml.safe_dump({"patients": patients}, sort_keys=False),
        encoding="utf-8",
    )


def _fake_prepare_rri_seed(**kwargs) -> None:
    output_path = Path(kwargs["output_path"])
    metrics_path = Path(kwargs["metrics_path"])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps({"prepared": "rri"}), encoding="utf-8")
    metrics_path.write_text(
        json.dumps({"method": "rri_from_learned_reference"}),
        encoding="utf-8",
    )


def test_seed_sweep_plan_creates_three_tst_stan_5_child_runs(
    sample_config_files, monkeypatch, tmp_path
):
    _add_campaign_patients(sample_config_files)
    learned_root = tmp_path / "learned"
    for alias in ("TST-STAN-1", "TST-STAN-5"):
        path = learned_root / alias / "baseline_0d_learned.json"
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps({"boundary_conditions": []}), encoding="utf-8")

    monkeypatch.setattr(
        seed_sweep,
        "learned_model_path",
        lambda patient: learned_root / patient / "baseline_0d_learned.json",
    )
    monkeypatch.setattr(seed_sweep, "_prepare_rri_seed", _fake_prepare_rri_seed)

    manifest = seed_sweep.plan_seed_sweep_campaign(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        campaign_id="seed-sweep-test",
    )

    assert manifest["patients"] == ["TST-STAN-5"]
    assert len(manifest["child_runs"]) == 3
    assert {
        child["case_id"] for child in manifest["child_runs"]
    } == {
        "learned-tree-dscale-0p0",
        "learned-tree-dscale-0p1",
        "learned-rri-reduced",
    }
    assert {child["patient"] for child in manifest["child_runs"]} == {"TST-STAN-5"}
    assert (
        sample_config_files
        / "runs"
        / "campaigns"
        / "seed-sweep-test"
        / "campaign_manifest.yaml"
    ).exists()

    children = {child["case_id"]: child for child in manifest["child_runs"]}
    dscale_0p0 = children["learned-tree-dscale-0p0"]
    dscale_0p1 = children["learned-tree-dscale-0p1"]
    rri = children["learned-rri-reduced"]
    assert dscale_0p0["seed_path"].endswith("baseline_0d_learned.json")
    assert dscale_0p0["tuning_model"] == "full_pa"
    assert dscale_0p0["diameter_scale"] == 0.0
    assert dscale_0p1["seed_path"].endswith("baseline_0d_learned.json")
    assert dscale_0p1["tuning_model"] == "full_pa"
    assert dscale_0p1["diameter_scale"] == 0.1
    assert rri["seed_path"].endswith("prepared_inputs/simplified_zerod_tuned_RRI.json")
    assert rri["tuning_model"] == "rri"

    for case_id, child in children.items():
        patients_yaml = yaml.safe_load(
            (Path(child["child_workspace"]) / "config" / "patients.yaml").read_text(
                encoding="utf-8"
            )
        )
        patient_entry = next(
            entry for entry in patients_yaml["patients"] if entry["alias"] == "TST-STAN-5"
        )
        tuning = patient_entry["tuning"]
        impedance = tuning["impedance"]
        assert impedance["diameter_scale"] == children[case_id]["diameter_scale"]
        assert impedance["tuning_model"] == children[case_id]["tuning_model"]
        assert impedance["allow_ordered_outlet_mapping"] is True
        if case_id == "learned-tree-dscale-0p1":
            assert impedance["use_mean"] is False
        if case_id == "learned-rri-reduced":
            assert tuning["iteration1_seed"]["path"].endswith(
                "prepared_inputs/simplified_zerod_tuned_RRI.json"
            )
            assert (
                Path(child["child_workspace"])
                / "prepared_inputs"
                / "simplified_zerod_tuned_RRI.json"
            ).exists()
        else:
            assert tuning["iteration1_seed"]["path"].endswith("baseline_0d_learned.json")


def test_seed_sweep_explicit_patients_preserves_multi_patient_override(
    sample_config_files, monkeypatch, tmp_path
):
    _add_campaign_patients(sample_config_files)
    learned_root = tmp_path / "learned"
    for alias in ("TST-STAN-1", "TST-STAN-5"):
        path = learned_root / alias / "baseline_0d_learned.json"
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps({"boundary_conditions": []}), encoding="utf-8")

    monkeypatch.setattr(
        seed_sweep,
        "learned_model_path",
        lambda patient: learned_root / patient / "baseline_0d_learned.json",
    )
    monkeypatch.setattr(seed_sweep, "_prepare_rri_seed", _fake_prepare_rri_seed)

    manifest = seed_sweep.plan_seed_sweep_campaign(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patients=["TST-STAN-1", "TST-STAN-5"],
        campaign_id="seed-sweep-two-patients",
    )

    assert manifest["patients"] == ["TST-STAN-1", "TST-STAN-5"]
    assert len(manifest["child_runs"]) == 6


def test_seed_sweep_rejects_missing_learned_model(sample_config_files, monkeypatch):
    _add_campaign_patients(sample_config_files)
    monkeypatch.setattr(
        seed_sweep,
        "learned_model_path",
        lambda patient: sample_config_files / "missing" / patient / "seed.json",
    )

    try:
        seed_sweep.plan_seed_sweep_campaign(
            workspace_root=sample_config_files,
            cluster_name="sherlock",
            campaign_id="seed-sweep-missing",
        )
    except Exception as exc:
        assert "learned 0D model does not exist" in str(exc)
    else:
        raise AssertionError("expected missing learned model to fail")


def test_seed_sweep_summary_and_slides(sample_config_files, monkeypatch, tmp_path):
    _add_campaign_patients(sample_config_files)
    learned_root = tmp_path / "learned"
    for alias in ("TST-STAN-1", "TST-STAN-5"):
        path = learned_root / alias / "baseline_0d_learned.json"
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps({"boundary_conditions": []}), encoding="utf-8")

    monkeypatch.setattr(
        seed_sweep,
        "learned_model_path",
        lambda patient: learned_root / patient / "baseline_0d_learned.json",
    )
    monkeypatch.setattr(seed_sweep, "_prepare_rri_seed", _fake_prepare_rri_seed)
    manifest = seed_sweep.plan_seed_sweep_campaign(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        campaign_id="seed-sweep-summary",
    )
    child = manifest["child_runs"][0]
    results_dir = (
        Path(child["child_workspace"])
        / "runs"
        / child["run_id"]
        / "iterations"
        / "iter-01"
        / "results"
    )
    results_dir.mkdir(parents=True, exist_ok=True)
    (results_dir / "iteration_metrics.json").write_text(
        json.dumps({"mpa_mean": 30.0, "rpa_split": 0.45}),
        encoding="utf-8",
    )
    (results_dir / "iteration_decision.json").write_text(
        json.dumps({"decision": "converged"}),
        encoding="utf-8",
    )

    rows = seed_sweep.summarize_seed_sweep_campaign(
        workspace_root=sample_config_files,
        campaign_id="seed-sweep-summary",
    )
    assert len(rows) == 3
    assert rows[0]["mpa_mean"] == 30.0
    assert rows[0]["tuning_model"] == "full_pa"

    slides = seed_sweep.write_seed_sweep_slides(
        workspace_root=sample_config_files,
        campaign_id="seed-sweep-summary",
    )
    assert slides.exists()
    assert slides.suffix == ".pptx"


def test_seed_sweep_campaign_run_supports_sibling_repo_layout(
    sample_config_files, monkeypatch, tmp_path
):
    sibling_paths = _switch_to_sibling_repo_layout(sample_config_files)
    _add_campaign_patients(sample_config_files)
    learned_root = tmp_path / "learned"
    for alias in ("TST-STAN-1", "TST-STAN-5"):
        path = learned_root / alias / "baseline_0d_learned.json"
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps({"boundary_conditions": []}), encoding="utf-8")

    monkeypatch.setattr(
        seed_sweep,
        "learned_model_path",
        lambda patient: learned_root / patient / "baseline_0d_learned.json",
    )
    monkeypatch.setattr(seed_sweep, "_prepare_rri_seed", _fake_prepare_rri_seed)

    manifest = seed_sweep.plan_seed_sweep_campaign(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        campaign_id="seed-sweep-sibling-layout",
    )
    child_workspace = Path(manifest["child_runs"][0]["child_workspace"])
    repositories_yaml = yaml.safe_load(
        (child_workspace / "config" / "repositories.yaml").read_text(encoding="utf-8")
    )
    assert repositories_yaml["repositories"]["svzt_agent"] == str(
        sibling_paths["svzt-agent"].resolve()
    )
    assert repositories_yaml["repositories"]["svZeroDTrees"] == str(
        sibling_paths["svZeroDTrees"].resolve()
    )
    assert repositories_yaml["repositories"]["svZeroDSolver"] == str(
        sibling_paths["svZeroDSolver"].resolve()
    )

    result = seed_sweep.run_seed_sweep_campaign(
        workspace_root=sample_config_files,
        campaign_id="seed-sweep-sibling-layout",
        mode=ExecutionMode.DRY_RUN,
    )
    assert len(result["last_run_results"]) == 3
