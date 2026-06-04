from __future__ import annotations

from pathlib import Path
import csv
import json

from svztagent.cli.main import build_parser
from svztagent.core.manifest import (
    record_converged_preop_iteration,
    record_postop_submission,
    read_manifest,
    write_manifest,
)
from svztagent.postprocess.cfd_results import (
    CfdResultsWriteResult,
    default_cfd_results_output_path,
    default_cfd_results_template_path,
    write_run_cfd_results,
)
from svztagent.workflows.tune_trees import init_run_workspace


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _placeholder_png(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("png", encoding="utf-8")


def _template_payload() -> dict:
    blank_metric = {
        "mpa_systolic_mmhg": None,
        "mpa_diastolic_mmhg": None,
        "mpa_mean_mmhg": None,
        "rpa_flow_pct": None,
        "lpa_flow_pct": None,
        "stenosis_gradient_mmhg": None,
        "total_pvr": None,
        "total_pvr_units": None,
    }
    blank_hotspots = [
        {"rank": 1, "vessel": None, "resistance": None, "units": None, "contribution_pct": None, "interpretation": None},
        {"rank": 2, "vessel": None, "resistance": None, "units": None, "contribution_pct": None, "interpretation": None},
        {"rank": 3, "vessel": None, "resistance": None, "units": None, "contribution_pct": None, "interpretation": None},
    ]
    blank_error = {
        "baseline_error": None,
        "baseline_abs": None,
        "baseline_pct": None,
        "adapted_error": None,
        "adapted_abs": None,
        "adapted_pct": None,
        "improvement_abs": None,
        "improvement_pct": None,
        "within_tolerance": "pending",
        "closer_to_measured": "insufficient",
    }
    return {
        "_about": {"schema_version": "1.0"},
        "patient_id": "ALG-###",
        "patient_id_external": None,
        "study": "alagille-stent-cfd",
        "cohort": "PA cohort",
        "version": "v2026.05",
        "measured_preop": {
            "date": None,
            "source": "pre-op catheter / pre-op LPS / pre-op angio",
            "mpa_systolic_mmhg": None,
            "mpa_diastolic_mmhg": None,
            "mpa_mean_mmhg": None,
            "rpa_flow_pct": None,
            "lpa_flow_pct": None,
            "stenosis_gradient_mmhg": None,
            "stenosis_gradient_branch": None,
        },
        "measured_postop": {
            "date": None,
            "source": "post-op catheter / post-op LPS / post-op angio",
            "mpa_systolic_mmhg": None,
            "mpa_diastolic_mmhg": None,
            "mpa_mean_mmhg": None,
            "rpa_flow_pct": None,
            "lpa_flow_pct": None,
            "stenosis_gradient_mmhg": None,
            "stenosis_gradient_branch": None,
            "tolerance_band_mmhg": None,
        },
        "states": {
            "preop_tuned": {
                "run_id": None,
                "run_path": None,
                "run_status": "pending",
                "metrics": dict(blank_metric),
                "resistance_hotspots": list(blank_hotspots),
            },
            "postop_baseline": {
                "run_id": None,
                "run_path": None,
                "run_status": "pending",
                "metrics": dict(blank_metric),
                "resistance_hotspots": list(blank_hotspots),
            },
            "postop_adapted": {
                "run_id": None,
                "run_path": None,
                "run_status": "pending",
                "metrics": dict(blank_metric),
                "resistance_hotspots": list(blank_hotspots),
            },
        },
        "errors": {
            "by_metric": {
                "mpa_systolic_mmhg": dict(blank_error),
                "mpa_diastolic_mmhg": dict(blank_error),
                "mpa_mean_mmhg": dict(blank_error),
                "flow_split_pct": dict(blank_error),
                "stenosis_gradient_mmhg": dict(blank_error),
            }
        },
        "figures": {
            "pressure_waveforms": {
                "preop_tuned": None,
                "postop_baseline": None,
                "postop_adapted": None,
                "combined_baseline_vs_adapted": None,
                "x_axis": "time",
                "y_axis": "MPA pressure, mmHg",
                "target_lines": [],
                "tolerance_band": None,
            },
            "resistance_maps": {
                "preop_tuned": None,
                "postop_baseline": None,
                "postop_adapted": None,
                "baseline_vs_adapted": None,
                "anatomy_context_3d": None,
                "colorbar_units": None,
                "colorbar_min": None,
                "colorbar_max": None,
                "same_scale_across_states": None,
                "delta_preop_to_baseline": {"delta_pvr": None, "interpretation": "geometry edit only"},
                "delta_baseline_to_adapted": {"delta_pvr": None, "interpretation": "BC adaptation only"},
                "delta_preop_to_adapted": {"delta_pvr": None, "interpretation": "combined predicted effect"},
            },
        },
        "methods": {
            "solver": {
                "name": None,
                "version": None,
                "type": None,
                "steady_or_pulsatile": None,
                "cardiac_cycle_duration_s": None,
                "convergence_residual": None,
            },
            "boundary_conditions": {
                "inlet": None,
                "outlet_method": None,
                "preop_tuning_targets": None,
                "preop_tuning_final_error": None,
                "adapted_bc_assumptions": None,
            },
        },
    }


def _source_payload() -> dict:
    return {
        "patient_id": "legacy-patient-id",
        "measured_postop": {
            "date": None,
            "source": "legacy manual source",
            "mpa_systolic_mmhg": 25.0,
            "mpa_diastolic_mmhg": 8.0,
            "mpa_mean_mmhg": 15.0,
            "rpa_flow_pct": 55.5,
            "lpa_flow_pct": 44.5,
            "stenosis_gradient_mmhg": None,
            "stenosis_gradient_branch": None,
            "tolerance_band_mmhg": None,
        },
        "states": {
            "preop_tuned": {
                "metrics": {
                    "mpa_systolic_mmhg": 999.0,
                    "mpa_diastolic_mmhg": 999.0,
                    "mpa_mean_mmhg": 999.0,
                    "rpa_flow_pct": 999.0,
                    "lpa_flow_pct": -899.0,
                    "stenosis_gradient_mmhg": None,
                }
            }
        },
    }


def _configure_run(sample_config_files: Path, run_id: str) -> Path:
    run_paths, _ = init_run_workspace(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id=run_id,
    )
    manifest = read_manifest(run_paths.manifest)
    manifest.remote["threed_defaults"]["wall_model"] = "deformable"
    manifest.remote["threed_defaults"]["inflow_boundary_condition"] = "dirichlet"
    manifest = record_converged_preop_iteration(
        manifest,
        iteration=1,
        source_decision="best_completed",
        selection_kind="operator_selected",
        reason="test",
        metrics={"mpa_sys": 41.0},
        deltas={"mpa_sys": 3.0},
        remote_iteration_dir=f"/scratch/users/test/{run_id}/iterations/iter-01",
        remote_preop_dir=f"/scratch/users/test/{run_id}/iterations/iter-01/preop",
        remote_tuned_zerod_config=f"/scratch/users/test/{run_id}/iterations/iter-01/results/svzerod_3d_coupling_tuned.json",
        remote_canonical_coupler=f"/scratch/users/test/{run_id}/iterations/iter-01/preop/mesh-complete/mesh-surfaces/svzerod_3Dcoupling.json",
        preop_job_id="101",
    )
    manifest = record_postop_submission(
        manifest,
        source_preop_iteration=1,
        local_dir=str(run_paths.run_dir / "postop" / "from-iter-01"),
        remote_dir=f"/scratch/users/test/{run_id}/postop/from-iter-01",
        local_job_script_path=str(run_paths.run_dir / "postop" / "from-iter-01" / "run_postop.sh"),
        remote_job_script_path=f"/scratch/users/test/{run_id}/postop/from-iter-01/run_postop.sh",
        postop_job_id="202",
    )
    write_manifest(manifest, run_paths.manifest)
    return run_paths.run_dir


def _seed_run_artifacts(
    run_dir: Path,
    *,
    include_preop_systolic: bool,
    include_adapted_postprocess: bool = False,
) -> None:
    run_id = run_dir.name
    iter1_results = run_dir / "iterations" / "iter-01" / "results"
    iter2_results = run_dir / "iterations" / "iter-02" / "results"
    iter1_post = iter1_results / "postprocess"
    postop_post = run_dir / "postop" / "from-iter-01" / "results" / "postprocess"
    adapted_post = (
        run_dir / "adaptation" / "from-iter-01" / "m1" / "results" / "adapted_postprocess"
    )

    _write_json(
        iter1_results / "iteration_decision.json",
        {
            "clinical_targets": {
                "mpa_sys": 38.0,
                "mpa_dia": 10.0,
                "mpa_mean": 23.0,
                "rpa_split": 0.71,
            }
        },
    )
    _write_json(
        iter1_results / "iteration_metrics.json",
        {
            "mpa_sys": 41.0,
            "mpa_dia": -2.0,
            "mpa_mean": 15.5,
            "rpa_split": 0.675,
        },
    )
    _write_json(
        iter2_results / "iteration_metrics.json",
        {
            "mpa_sys": 88.0,
            "mpa_dia": 77.0,
            "mpa_mean": 66.0,
            "rpa_split": 0.22,
        },
    )
    _write_json(
        iter1_post / "postprocess_suite_metadata.json",
        {
            "status": "completed",
            "cycle_duration_s": 0.8,
            "simulation_dir": f"/scratch/users/test/{run_id}/iterations/iter-01/preop",
        },
    )
    _placeholder_png(iter1_post / "mpa_pressure_vs_time.png")
    _placeholder_png(iter1_post / "resistance_map_mean.png")
    _write_csv(
        iter1_post / "branch_resistance_summary.csv",
        [
            "branch_id",
            "resistance_mean",
            "rank",
        ],
        [
            {"branch_id": 9, "resistance_mean": 100.0, "rank": 1},
            {"branch_id": 12, "resistance_mean": 50.0, "rank": 2},
            {"branch_id": 15, "resistance_mean": 25.0, "rank": 3},
        ],
    )
    if include_preop_systolic:
        _placeholder_png(iter1_post / "resistance_map_systolic.png")
        _write_csv(
            iter1_post / "branch_resistance_summary_systolic.csv",
            [
                "branch_id",
                "resistance_systolic",
                "rank",
            ],
            [
                {"branch_id": 2, "resistance_systolic": 300.0, "rank": 1},
                {"branch_id": 4, "resistance_systolic": 150.0, "rank": 2},
                {"branch_id": 6, "resistance_systolic": 50.0, "rank": 3},
            ],
        )

    _write_json(
        postop_post / "postprocess_suite_metadata.json",
        {
            "status": "completed",
            "cycle_duration_s": 0.8,
            "simulation_dir": f"/scratch/users/test/{run_id}/postop/from-iter-01/simulation",
            "flow_split": {
                "rpa_split": 0.55,
            },
        },
    )
    _placeholder_png(postop_post / "mpa_pressure_vs_time.png")
    _placeholder_png(postop_post / "resistance_map_systolic.png")
    _write_csv(
        postop_post / "mpa_pressure_vs_time.csv",
        ["timestep_id", "time_s", "mpa_pressure_mmhg"],
        [
            {"timestep_id": 20, "time_s": 0.008, "mpa_pressure_mmhg": 10.0},
            {"timestep_id": 40, "time_s": 0.016, "mpa_pressure_mmhg": 20.0},
            {"timestep_id": 60, "time_s": 0.024, "mpa_pressure_mmhg": 30.0},
        ],
    )
    _write_csv(
        postop_post / "branch_resistance_summary_systolic.csv",
        ["branch_id", "resistance_systolic", "rank"],
        [
            {"branch_id": 7, "resistance_systolic": 90.0, "rank": 1},
            {"branch_id": 8, "resistance_systolic": 45.0, "rank": 2},
            {"branch_id": 9, "resistance_systolic": 15.0, "rank": 3},
        ],
    )
    if include_adapted_postprocess:
        _write_json(
            adapted_post / "postprocess_suite_metadata.json",
            {
                "status": "completed",
                "cycle_duration_s": 0.8,
                "simulation_dir": f"/scratch/users/test/{run_id}/adaptation/from-iter-01/m1/simulation",
                "flow_split": {
                    "rpa_split": 0.61,
                },
            },
        )
        _placeholder_png(adapted_post / "mpa_pressure_vs_time.png")
        _placeholder_png(adapted_post / "resistance_map_systolic.png")
        _write_csv(
            adapted_post / "mpa_pressure_vs_time.csv",
            ["timestep_id", "time_s", "mpa_pressure_mmhg"],
            [
                {"timestep_id": 20, "time_s": 0.008, "mpa_pressure_mmhg": 12.0},
                {"timestep_id": 40, "time_s": 0.016, "mpa_pressure_mmhg": 22.0},
                {"timestep_id": 60, "time_s": 0.024, "mpa_pressure_mmhg": 32.0},
            ],
        )
        _write_csv(
            adapted_post / "branch_resistance_summary_systolic.csv",
            ["branch_id", "resistance_systolic", "rank"],
            [
                {"branch_id": 17, "resistance_systolic": 70.0, "rank": 1},
                {"branch_id": 18, "resistance_systolic": 40.0, "rank": 2},
                {"branch_id": 19, "resistance_systolic": 20.0, "rank": 3},
            ],
        )


def test_write_run_cfd_results_prefers_selected_iteration_and_systolic_hotspots(sample_config_files: Path) -> None:
    template_path = sample_config_files / "data" / "cfd-results" / "cfd-results-template.json"
    source_path = sample_config_files / "legacy" / "TST-STAN-x.json"
    _write_json(template_path, _template_payload())
    _write_json(source_path, _source_payload())

    run_dir = _configure_run(sample_config_files, "run-cfd-001")
    _seed_run_artifacts(run_dir, include_preop_systolic=True)

    result = write_run_cfd_results(
        workspace_root=sample_config_files,
        run_id="run-cfd-001",
        source_path=source_path,
        overwrite=True,
    )

    payload = json.loads(result.output_path.read_text(encoding="utf-8"))
    assert payload["patient_id"] == "TST-STAN-x"
    assert payload["measured_preop"]["mpa_systolic_mmhg"] == 38.0
    assert payload["measured_preop"]["rpa_flow_pct"] == 71.0
    assert payload["measured_postop"]["mpa_systolic_mmhg"] == 25.0

    preop = payload["states"]["preop_tuned"]
    assert preop["run_id"] == "run-cfd-001"
    assert preop["run_path"] == "/scratch/users/test/run-cfd-001/iterations/iter-01"
    assert preop["metrics"]["mpa_systolic_mmhg"] == 41.0
    assert preop["metrics"]["mpa_diastolic_mmhg"] == -2.0
    assert preop["metrics"]["rpa_flow_pct"] == 67.5
    assert preop["metrics"]["total_pvr"] == 500.0
    assert preop["metrics"]["total_pvr_units"] == "mmHg / (L/min)"
    assert preop["resistance_hotspots"][0]["vessel"] == "branch_id:2"
    assert payload["figures"]["resistance_maps"]["preop_tuned"] == "runs/run-cfd-001/iterations/iter-01/results/postprocess/resistance_map_systolic.png"

    postop = payload["states"]["postop_baseline"]
    assert postop["run_path"] == "/scratch/users/test/run-cfd-001/postop/from-iter-01"
    assert postop["metrics"]["mpa_systolic_mmhg"] == 30.0
    assert postop["metrics"]["mpa_diastolic_mmhg"] == 10.0
    assert postop["metrics"]["mpa_mean_mmhg"] == 20.0
    assert postop["metrics"]["rpa_flow_pct"] == 55.00000000000001
    assert postop["metrics"]["total_pvr"] == 150.0
    assert postop["metrics"]["total_pvr_units"] == "mmHg / (L/min)"
    assert postop["resistance_hotspots"][0]["vessel"] == "branch_id:7"
    assert postop["run_status"] == "completed"

    assert payload["errors"]["by_metric"]["mpa_systolic_mmhg"]["baseline_error"] == 5.0
    assert payload["errors"]["by_metric"]["mpa_mean_mmhg"]["baseline_error"] == 5.0
    assert payload["errors"]["by_metric"]["flow_split_pct"]["baseline_error"] == -0.4999999999999929
    assert payload["figures"]["resistance_maps"]["colorbar_units"] == "mmHg / (L/min)"
    assert payload["figures"]["resistance_maps"]["delta_preop_to_baseline"]["delta_pvr"] == -350.0
    assert payload["methods"]["solver"]["name"] == "svMultiPhysics"
    assert payload["methods"]["solver"]["type"] == "deformable"
    assert payload["methods"]["solver"]["steady_or_pulsatile"] == "pulsatile"
    assert payload["methods"]["solver"]["cardiac_cycle_duration_s"] == 0.8
    assert payload["methods"]["boundary_conditions"]["inlet"] == "dirichlet"
    assert result.output_path == default_cfd_results_output_path(sample_config_files, "run-cfd-001")
    assert result.template_path == default_cfd_results_template_path(sample_config_files)


def test_write_run_cfd_results_falls_back_to_mean_hotspots_when_systolic_missing(sample_config_files: Path) -> None:
    template_path = sample_config_files / "data" / "cfd-results" / "cfd-results-template.json"
    source_path = sample_config_files / "legacy" / "TST-STAN-x.json"
    _write_json(template_path, _template_payload())
    _write_json(source_path, _source_payload())

    run_dir = _configure_run(sample_config_files, "run-cfd-002")
    _seed_run_artifacts(run_dir, include_preop_systolic=False)

    result = write_run_cfd_results(
        workspace_root=sample_config_files,
        run_id="run-cfd-002",
        source_path=source_path,
        overwrite=True,
    )

    payload = json.loads(result.output_path.read_text(encoding="utf-8"))
    assert payload["states"]["preop_tuned"]["resistance_hotspots"][0]["vessel"] == "branch_id:9"
    assert payload["figures"]["resistance_maps"]["preop_tuned"] == "runs/run-cfd-002/iterations/iter-01/results/postprocess/resistance_map_mean.png"
    assert payload["states"]["preop_tuned"]["metrics"]["total_pvr"] == 175.0
    assert payload["states"]["preop_tuned"]["run_status"] == "completed"


def test_write_run_cfd_results_populates_adapted_state_when_adaptation_postprocess_exists(
    sample_config_files: Path,
) -> None:
    template_path = sample_config_files / "data" / "cfd-results" / "cfd-results-template.json"
    source_path = sample_config_files / "legacy" / "TST-STAN-x.json"
    _write_json(template_path, _template_payload())
    _write_json(source_path, _source_payload())

    run_dir = _configure_run(sample_config_files, "run-cfd-004")
    _seed_run_artifacts(
        run_dir,
        include_preop_systolic=True,
        include_adapted_postprocess=True,
    )

    result = write_run_cfd_results(
        workspace_root=sample_config_files,
        run_id="run-cfd-004",
        source_path=source_path,
        overwrite=True,
    )

    payload = json.loads(result.output_path.read_text(encoding="utf-8"))
    adapted = payload["states"]["postop_adapted"]
    assert adapted["run_id"] == "run-cfd-004"
    assert adapted["run_path"] == "/scratch/users/test/run-cfd-004/adaptation/from-iter-01/m1"
    assert adapted["run_status"] == "completed"
    assert adapted["metrics"]["mpa_systolic_mmhg"] == 32.0
    assert adapted["metrics"]["mpa_diastolic_mmhg"] == 12.0
    assert adapted["metrics"]["mpa_mean_mmhg"] == 22.0
    assert adapted["metrics"]["rpa_flow_pct"] == 61.0
    assert adapted["metrics"]["total_pvr"] == 130.0
    assert adapted["metrics"]["total_pvr_units"] == "mmHg / (L/min)"
    assert adapted["resistance_hotspots"][0]["vessel"] == "branch_id:17"

    assert (
        payload["figures"]["pressure_waveforms"]["postop_adapted"]
        == "runs/run-cfd-004/adaptation/from-iter-01/m1/results/adapted_postprocess/mpa_pressure_vs_time.png"
    )
    assert (
        payload["figures"]["resistance_maps"]["postop_adapted"]
        == "runs/run-cfd-004/adaptation/from-iter-01/m1/results/adapted_postprocess/resistance_map_systolic.png"
    )
    assert payload["figures"]["resistance_maps"]["delta_baseline_to_adapted"]["delta_pvr"] == -20.0
    assert payload["figures"]["resistance_maps"]["delta_preop_to_adapted"]["delta_pvr"] == -370.0

    assert payload["errors"]["by_metric"]["mpa_systolic_mmhg"]["adapted_error"] == 7.0
    assert payload["errors"]["by_metric"]["mpa_systolic_mmhg"]["improvement_abs"] == -2.0
    assert payload["errors"]["by_metric"]["mpa_systolic_mmhg"]["closer_to_measured"] == "baseline"
    assert payload["errors"]["by_metric"]["flow_split_pct"]["adapted_error"] == 5.5


def test_postprocess_cli_uses_default_template_and_output_paths(sample_config_files: Path, monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_write_run_cfd_results(**kwargs):
        captured.update(kwargs)
        workspace_root = Path(kwargs["workspace_root"])
        run_id = str(kwargs["run_id"])
        return CfdResultsWriteResult(
            run_id=run_id,
            workspace_root=workspace_root,
            template_path=default_cfd_results_template_path(workspace_root),
            output_path=default_cfd_results_output_path(workspace_root, run_id),
            source_path=None,
        )

    monkeypatch.setattr("svztagent.cli.main.write_run_cfd_results", fake_write_run_cfd_results)
    parser = build_parser()
    args = parser.parse_args(
        [
            "--workspace-root",
            str(sample_config_files),
            "postprocess",
            "cfd-results",
            "--run-id",
            "run-cfd-003",
        ]
    )

    exit_code = args.handler(args)
    assert exit_code == 0
    assert captured["workspace_root"] == sample_config_files
    assert captured["run_id"] == "run-cfd-003"
    assert captured["template_path"] is None
    assert captured["output_path"] is None
    assert captured["source_path"] is None
    assert captured["overwrite"] is False
