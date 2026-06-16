from __future__ import annotations

from pathlib import Path
import json

from svztagent.cli.main import build_parser
from svztagent.core.manifest import (
    read_manifest,
    record_converged_preop_iteration,
    write_manifest,
)
from svztagent.postprocess.tuning_progress import (
    TuningProgressWriteResult,
    write_tuning_progress,
)
from svztagent.workflows.tune_trees import init_run_workspace


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _configure_run(sample_config_files: Path, run_id: str) -> Path:
    run_paths, _ = init_run_workspace(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id=run_id,
    )
    return run_paths.run_dir


def _iteration_dirs(run_dir: Path, iteration: int) -> tuple[Path, Path]:
    root = run_dir / "iterations" / f"iter-{iteration:02d}"
    results = root / "results"
    results.mkdir(parents=True, exist_ok=True)
    return root, results


def test_write_tuning_progress_reuses_existing_zerod_metrics(
    sample_config_files: Path,
    monkeypatch,
) -> None:
    run_dir = _configure_run(sample_config_files, "run-progress-001")
    _, results = _iteration_dirs(run_dir, 1)
    _write_json(
        results / "iteration_decision.json",
        {
            "clinical_targets": {"mpa_sys": 40.0, "mpa_dia": 10.0, "mpa_mean": 25.0, "rpa_split": 0.5},
            "thresholds": {"mpa_sys": 4.0, "mpa_dia": 1.0, "mpa_mean": 2.5, "rpa_split": 0.05},
            "deltas": {"mpa_sys": 1.0, "mpa_dia": 1.0, "mpa_mean": 1.0, "rpa_split": 0.02},
        },
    )
    _write_json(
        results / "iteration_metrics.json",
        {"mpa_sys": 39.0, "mpa_dia": 11.0, "mpa_mean": 24.0, "rpa_split": 0.48},
    )
    _write_json(
        results / "zerod_pre_mapping_metrics.json",
        {
            "run_id": "run-progress-001",
            "iteration": 1,
            "status": "ok",
            "source_kind": "pa_config_tuning_snapshot",
            "source_config_path": "/tmp/snapshot.json",
            "metrics": {"mpa_sys": 40.0, "mpa_dia": 10.0, "mpa_mean": 25.0, "rpa_split": 0.5},
            "comparison": {
                "targets": {"mpa_sys": 40.0, "mpa_dia": 10.0, "mpa_mean": 25.0, "rpa_split": 0.5},
                "signed_deltas": {"mpa_sys": 0.0, "mpa_dia": 0.0, "mpa_mean": 0.0, "rpa_split": 0.0},
                "absolute_deltas": {"mpa_sys": 0.0, "mpa_dia": 0.0, "mpa_mean": 0.0, "rpa_split": 0.0},
                "thresholds": {"mpa_sys": 4.0, "mpa_dia": 1.0, "mpa_mean": 2.5, "rpa_split": 0.05},
                "within_threshold": {"mpa_sys": True, "mpa_dia": True, "mpa_mean": True, "rpa_split": True},
            },
        },
    )

    def fail_if_called(*_args, **_kwargs):
        raise AssertionError("backfill helpers should not run when zerod_pre_mapping_metrics.json exists")

    monkeypatch.setattr("svztagent.postprocess.tuning_progress._summarize_pulmonary_config", fail_if_called)
    monkeypatch.setattr("svztagent.postprocess.tuning_progress._evaluate_against_targets", fail_if_called)
    monkeypatch.setattr(
        "svztagent.postprocess.tuning_progress._render_figure",
        lambda path, _rows, selected_iteration=None: path.write_text("png", encoding="utf-8"),
    )

    result = write_tuning_progress(workspace_root=sample_config_files, run_id="run-progress-001")

    assert result.csv_path.exists()
    assert result.json_path.exists()
    assert result.figure_path.exists()
    payload = json.loads(result.json_path.read_text(encoding="utf-8"))
    assert payload["iterations"][0]["zerod_pre_mapping"]["metrics"]["mpa_sys"] == 40.0
    assert payload["selected_preop_iteration"] is None


def test_write_tuning_progress_backfills_from_snapshot(
    sample_config_files: Path,
    monkeypatch,
) -> None:
    run_dir = _configure_run(sample_config_files, "run-progress-002")
    _, results = _iteration_dirs(run_dir, 1)
    _write_json(
        results / "iteration_decision.json",
        {
            "clinical_targets": {"mpa_sys": 42.0, "mpa_dia": 12.0, "mpa_mean": 26.0, "rpa_split": 0.45},
            "thresholds": {"mpa_sys": 4.2, "mpa_dia": 1.2, "mpa_mean": 2.6, "rpa_split": 0.045},
            "deltas": {"mpa_sys": 1.0, "mpa_dia": 1.0, "mpa_mean": 1.0, "rpa_split": 0.01},
        },
    )
    _write_json(
        results / "iteration_metrics.json",
        {"mpa_sys": 41.0, "mpa_dia": 11.0, "mpa_mean": 25.0, "rpa_split": 0.44},
    )
    _write_json(results / "pa_config_tuning_snapshot.json", {"snapshot": True})

    monkeypatch.setattr(
        "svztagent.postprocess.tuning_progress._summarize_pulmonary_config",
        lambda _root, path: {
            "config_path": str(path),
            "mpa_sys": 43.0,
            "mpa_dia": 13.0,
            "mpa_mean": 27.0,
            "rpa_split": 0.46,
            "mpa_flow": 12.0,
            "lpa_flow": 6.5,
            "rpa_flow": 5.5,
        },
    )
    monkeypatch.setattr(
        "svztagent.postprocess.tuning_progress._evaluate_against_targets",
        lambda _root, metrics, clinical_targets: {
            "decision": "converged",
            "close_to_targets": True,
            "clinical_targets": clinical_targets,
            "thresholds": {"mpa_sys": 4.2, "mpa_dia": 1.2, "mpa_mean": 2.6, "rpa_split": 0.045},
            "deltas": {"mpa_sys": 1.0, "mpa_dia": 1.0, "mpa_mean": 1.0, "rpa_split": 0.01},
            "metrics": metrics,
        },
    )
    monkeypatch.setattr(
        "svztagent.postprocess.tuning_progress._render_figure",
        lambda path, _rows, selected_iteration=None: path.write_text("png", encoding="utf-8"),
    )

    result = write_tuning_progress(workspace_root=sample_config_files, run_id="run-progress-002")

    payload = json.loads(result.json_path.read_text(encoding="utf-8"))
    zerod = payload["iterations"][0]["zerod_pre_mapping"]
    assert zerod["status"] == "ok"
    assert zerod["metrics"]["mpa_sys"] == 43.0
    assert zerod["comparison"]["decision"] == "converged"


def test_write_tuning_progress_uses_pulled_outputs_snapshot_when_local_missing(
    sample_config_files: Path,
    monkeypatch,
) -> None:
    run_dir = _configure_run(sample_config_files, "run-progress-003")
    iteration_root, results = _iteration_dirs(run_dir, 1)
    _write_json(
        results / "iteration_decision.json",
        {
            "clinical_targets": {"mpa_sys": 42.0, "mpa_dia": 12.0, "mpa_mean": 26.0, "rpa_split": 0.45},
            "thresholds": {"mpa_sys": 4.2, "mpa_dia": 1.2, "mpa_mean": 2.6, "rpa_split": 0.045},
            "deltas": {"mpa_sys": 1.0, "mpa_dia": 1.0, "mpa_mean": 1.0, "rpa_split": 0.01},
        },
    )
    _write_json(
        results / "iteration_metrics.json",
        {"mpa_sys": 41.0, "mpa_dia": 11.0, "mpa_mean": 25.0, "rpa_split": 0.44},
    )
    pulled_snapshot = run_dir / "pulled_outputs" / "iterations" / iteration_root.name / "results" / "pa_config_tuning_snapshot.json"
    _write_json(pulled_snapshot, {"snapshot": "pulled"})

    seen = {}

    def fake_summary(_root: Path, path: Path):
        seen["path"] = str(path)
        return {
            "config_path": str(path),
            "mpa_sys": 43.0,
            "mpa_dia": 13.0,
            "mpa_mean": 27.0,
            "rpa_split": 0.46,
            "mpa_flow": 12.0,
            "lpa_flow": 6.5,
            "rpa_flow": 5.5,
        }

    monkeypatch.setattr("svztagent.postprocess.tuning_progress._summarize_pulmonary_config", fake_summary)
    monkeypatch.setattr(
        "svztagent.postprocess.tuning_progress._evaluate_against_targets",
        lambda _root, metrics, clinical_targets: {
            "decision": "converged",
            "close_to_targets": True,
            "clinical_targets": clinical_targets,
            "thresholds": {"mpa_sys": 4.2, "mpa_dia": 1.2, "mpa_mean": 2.6, "rpa_split": 0.045},
            "deltas": {"mpa_sys": 1.0, "mpa_dia": 1.0, "mpa_mean": 1.0, "rpa_split": 0.01},
            "metrics": metrics,
        },
    )
    monkeypatch.setattr(
        "svztagent.postprocess.tuning_progress._render_figure",
        lambda path, _rows, selected_iteration=None: path.write_text("png", encoding="utf-8"),
    )

    write_tuning_progress(workspace_root=sample_config_files, run_id="run-progress-003")

    assert seen["path"] == str(pulled_snapshot)


def test_write_tuning_progress_carries_selected_iteration_from_manifest(
    sample_config_files: Path,
    monkeypatch,
) -> None:
    run_dir = _configure_run(sample_config_files, "run-progress-selected")
    manifest_path = run_dir / "manifest.yaml"
    manifest = read_manifest(manifest_path)
    manifest = record_converged_preop_iteration(
        manifest,
        iteration=3,
        source_decision="converged",
        selection_kind="formal_converged",
        reason=None,
        metrics={"mpa_sys": 40.0},
        deltas={"mpa_sys": 0.0},
        remote_iteration_dir="/remote/iter-03",
        remote_preop_dir="/remote/preop",
        remote_tuned_zerod_config="/remote/tuned.json",
        remote_canonical_coupler="/remote/coupler.json",
        preop_job_id="12345",
    )
    write_manifest(manifest, manifest_path)

    _, results = _iteration_dirs(run_dir, 3)
    _write_json(
        results / "iteration_decision.json",
        {
            "clinical_targets": {"mpa_sys": 40.0, "mpa_dia": 10.0, "mpa_mean": 25.0, "rpa_split": 0.5},
            "thresholds": {"mpa_sys": 4.0, "mpa_dia": 1.0, "mpa_mean": 2.5, "rpa_split": 0.05},
            "deltas": {"mpa_sys": 1.0, "mpa_dia": 1.0, "mpa_mean": 1.0, "rpa_split": 0.02},
        },
    )
    _write_json(
        results / "iteration_metrics.json",
        {"mpa_sys": 39.0, "mpa_dia": 11.0, "mpa_mean": 24.0, "rpa_split": 0.48},
    )
    _write_json(
        results / "zerod_pre_mapping_metrics.json",
        {
            "run_id": "run-progress-selected",
            "iteration": 3,
            "status": "ok",
            "source_kind": "pa_config_tuning_snapshot",
            "source_config_path": "/tmp/snapshot.json",
            "metrics": {"mpa_sys": 40.0, "mpa_dia": 10.0, "mpa_mean": 25.0, "rpa_split": 0.5},
            "comparison": {
                "targets": {"mpa_sys": 40.0, "mpa_dia": 10.0, "mpa_mean": 25.0, "rpa_split": 0.5},
                "signed_deltas": {"mpa_sys": 0.0, "mpa_dia": 0.0, "mpa_mean": 0.0, "rpa_split": 0.0},
                "absolute_deltas": {"mpa_sys": 0.0, "mpa_dia": 0.0, "mpa_mean": 0.0, "rpa_split": 0.0},
                "thresholds": {"mpa_sys": 4.0, "mpa_dia": 1.0, "mpa_mean": 2.5, "rpa_split": 0.05},
                "within_threshold": {"mpa_sys": True, "mpa_dia": True, "mpa_mean": True, "rpa_split": True},
            },
        },
    )

    seen = {}

    def fake_render(path: Path, _rows, selected_iteration=None):
        seen["selected_iteration"] = selected_iteration
        path.write_text("png", encoding="utf-8")

    monkeypatch.setattr("svztagent.postprocess.tuning_progress._render_figure", fake_render)

    result = write_tuning_progress(workspace_root=sample_config_files, run_id="run-progress-selected")

    payload = json.loads(result.json_path.read_text(encoding="utf-8"))
    assert payload["selected_preop_iteration"] == 3
    assert seen["selected_iteration"] == 3


def test_build_parser_routes_postprocess_tuning_progress(monkeypatch, tmp_path: Path) -> None:
    called = {}

    def fake_write_tuning_progress(**kwargs):
        called.update(kwargs)
        output_dir = tmp_path / "tuning-progress"
        output_dir.mkdir(parents=True, exist_ok=True)
        return TuningProgressWriteResult(
            run_id=kwargs["run_id"],
            output_dir=output_dir,
            csv_path=output_dir / "tuning_progress.csv",
            json_path=output_dir / "tuning_progress.json",
            figure_path=output_dir / "tuning_progress.png",
        )

    monkeypatch.setattr("svztagent.cli.main.detect_workspace_root", lambda value=None: Path("/tmp/workspace"))
    monkeypatch.setattr("svztagent.cli.main.write_tuning_progress", fake_write_tuning_progress)

    parser = build_parser()
    args = parser.parse_args(
        ["postprocess", "tuning-progress", "--run-id", "run-progress-004", "--overwrite"]
    )

    exit_code = args.handler(args)

    assert exit_code == 0
    assert called["workspace_root"] == Path("/tmp/workspace")
    assert called["run_id"] == "run-progress-004"
    assert called["overwrite"] is True
