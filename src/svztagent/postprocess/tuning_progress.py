"""Run-scoped tuning-progress diagnostics for 0D and 3D iteration metrics."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import csv
import importlib
import json
import sys
from typing import Any, Mapping

from svztagent.config.load import (
    detect_workspace_root,
    load_workspace_config,
    resolve_repository_locations,
)
from svztagent.core.errors import ConfigError
from svztagent.core.manifest import read_manifest
from svztagent.core.paths import build_local_run_paths, validate_run_id


_METRIC_ORDER = ("mpa_sys", "mpa_dia", "mpa_mean", "rpa_split")
_METRIC_LABELS = {
    "mpa_sys": "MPA systolic (mmHg)",
    "mpa_dia": "MPA diastolic (mmHg)",
    "mpa_mean": "MPA mean (mmHg)",
    "rpa_split": "RPA split",
}
_PANEL_TITLES = {
    "mpa_sys": "MPA systolic",
    "mpa_dia": "MPA diastolic",
    "mpa_mean": "MPA mean",
    "rpa_split": "RPA split",
}
_FIGURE_FILENAME = "tuning_progress.png"
_CSV_FILENAME = "tuning_progress.csv"
_JSON_FILENAME = "tuning_progress.json"
_ZEROD_METRICS_FILENAME = "zerod_pre_mapping_metrics.json"


@dataclass(frozen=True)
class TuningProgressWriteResult:
    run_id: str
    output_dir: Path
    csv_path: Path
    json_path: Path
    figure_path: Path


def default_tuning_progress_output_dir(workspace_root: str | Path, run_id: str) -> Path:
    root = detect_workspace_root(workspace_root)
    return build_local_run_paths(root, validate_run_id(run_id)).run_dir / "tuning-progress"


def _load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as stream:
        payload = json.load(stream)
    if not isinstance(payload, dict):
        raise ConfigError(f"{path} must contain a JSON object")
    return payload


def _float_or_none(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if result != result:
        return None
    return result


def _resolved_repository_paths(workspace_root: Path) -> dict[str, str | None]:
    config = load_workspace_config(workspace_root)
    return resolve_repository_locations(config, workspace_root)


def _load_svzerodtrees_tuning(workspace_root: Path):
    repos = _resolved_repository_paths(workspace_root)
    repo_root = repos.get("svZeroDTrees")
    if not repo_root:
        raise ConfigError("svZeroDTrees repository path could not be resolved")

    source_root = Path(repo_root) / "src"
    if not source_root.exists():
        raise ConfigError(f"svZeroDTrees source root not found: {source_root}")

    if str(source_root) not in sys.path:
        sys.path.insert(0, str(source_root))

    return importlib.import_module("svzerodtrees.tuning")


def _evaluate_against_targets(
    workspace_root: Path,
    *,
    metrics: Mapping[str, float],
    clinical_targets: Mapping[str, Any],
) -> dict[str, Any]:
    tuning = _load_svzerodtrees_tuning(workspace_root)
    return tuning.evaluate_iteration_gate(
        metrics=metrics,
        clinical_targets={
            "mpa_p": [
                float(clinical_targets["mpa_sys"]),
                float(clinical_targets["mpa_dia"]),
                float(clinical_targets["mpa_mean"]),
            ],
            "rpa_split": float(clinical_targets["rpa_split"]),
        },
    )


def _summarize_pulmonary_config(workspace_root: Path, config_path: Path) -> dict[str, Any]:
    tuning = _load_svzerodtrees_tuning(workspace_root)
    return tuning.summarize_pulmonary_zerod_config(config_path)


def _comparison_payload(
    *,
    metrics: Mapping[str, Any],
    gate_payload: Mapping[str, Any],
) -> dict[str, Any]:
    targets = gate_payload.get("clinical_targets") or {}
    deltas = gate_payload.get("deltas") or {}
    thresholds = gate_payload.get("thresholds") or {}
    signed_deltas = {
        key: (
            _float_or_none(metrics.get(key)) - _float_or_none(targets.get(key))
            if _float_or_none(metrics.get(key)) is not None and _float_or_none(targets.get(key)) is not None
            else None
        )
        for key in _METRIC_ORDER
    }
    within_threshold = {
        key: (
            float(deltas[key]) <= float(thresholds[key])
            if key in deltas and key in thresholds
            else None
        )
        for key in _METRIC_ORDER
    }
    return {
        "targets": {key: _float_or_none(targets.get(key)) for key in _METRIC_ORDER},
        "signed_deltas": signed_deltas,
        "absolute_deltas": {key: _float_or_none(deltas.get(key)) for key in _METRIC_ORDER},
        "thresholds": {key: _float_or_none(thresholds.get(key)) for key in _METRIC_ORDER},
        "within_threshold": within_threshold,
        "close_to_targets": bool(gate_payload.get("close_to_targets")),
        "decision": gate_payload.get("decision"),
    }


def _artifact_candidates(
    run_dir: Path,
    iteration_dir: Path,
    filename: str,
) -> list[Path]:
    return [
        iteration_dir / "results" / filename,
        run_dir / "pulled_outputs" / "iterations" / iteration_dir.name / "results" / filename,
    ]


def _first_existing(paths: list[Path]) -> Path | None:
    for path in paths:
        if path.exists():
            return path
    return None


def _zerod_stage_summary(
    workspace_root: Path,
    *,
    run_id: str,
    run_dir: Path,
    iteration_dir: Path,
    decision_payload: dict[str, Any] | None,
) -> dict[str, Any]:
    existing_path = _first_existing(_artifact_candidates(run_dir, iteration_dir, _ZEROD_METRICS_FILENAME))
    if existing_path is not None:
        payload = _load_json(existing_path)
        if payload is None:
            raise ConfigError(f"{existing_path} is missing or invalid")
        return payload

    clinical_targets = (
        decision_payload.get("clinical_targets")
        if isinstance(decision_payload, Mapping)
        and isinstance(decision_payload.get("clinical_targets"), Mapping)
        else None
    )
    if clinical_targets is None:
        return {
            "run_id": run_id,
            "iteration": int(iteration_dir.name.split("-")[-1]),
            "status": "missing",
            "reason": "clinical_targets_unavailable",
        }

    snapshot_path = _first_existing(
        _artifact_candidates(run_dir, iteration_dir, "pa_config_tuning_snapshot.json")
    )
    if snapshot_path is None:
        return {
            "run_id": run_id,
            "iteration": int(iteration_dir.name.split("-")[-1]),
            "status": "missing",
            "reason": "pa_config_tuning_snapshot_missing",
        }

    metrics = _summarize_pulmonary_config(workspace_root, snapshot_path)
    gate = _evaluate_against_targets(
        workspace_root,
        metrics={key: float(metrics[key]) for key in _METRIC_ORDER},
        clinical_targets=clinical_targets,
    )
    return {
        "run_id": run_id,
        "iteration": int(iteration_dir.name.split("-")[-1]),
        "status": "ok",
        "source_kind": "pa_config_tuning_snapshot",
        "source_config_path": str(snapshot_path),
        "metrics": {
            key: _float_or_none(metrics.get(key))
            for key in (
                "mpa_sys",
                "mpa_dia",
                "mpa_mean",
                "rpa_split",
                "mpa_flow",
                "lpa_flow",
                "rpa_flow",
            )
        },
        "clinical_targets": {
            key: _float_or_none(clinical_targets.get(key)) for key in _METRIC_ORDER
        },
        "comparison": _comparison_payload(metrics=metrics, gate_payload=gate),
    }


def _threed_stage_summary(
    *,
    run_id: str,
    iteration_dir: Path,
    metrics_payload: dict[str, Any] | None,
    decision_payload: dict[str, Any] | None,
) -> dict[str, Any]:
    if metrics_payload is None or decision_payload is None:
        return {
            "run_id": run_id,
            "iteration": int(iteration_dir.name.split("-")[-1]),
            "status": "missing",
            "reason": "iteration_metrics_or_decision_missing",
        }

    metrics = {key: _float_or_none(metrics_payload.get(key)) for key in _METRIC_ORDER}
    comparison = _comparison_payload(metrics=metrics, gate_payload=decision_payload)
    return {
        "run_id": run_id,
        "iteration": int(iteration_dir.name.split("-")[-1]),
        "status": "ok",
        "source_kind": "iteration_metrics",
        "source_config_path": str(iteration_dir / "results" / "iteration_metrics.json"),
        "metrics": metrics,
        "clinical_targets": comparison["targets"],
        "comparison": comparison,
    }


def _long_rows(iteration_payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for iteration_payload in iteration_payloads:
        iteration = int(iteration_payload["iteration"])
        for stage_key, stage_name in (
            ("zerod_pre_mapping", "zerod_pre_mapping"),
            ("threed_preop", "threed_preop"),
        ):
            stage_payload = iteration_payload[stage_key]
            metrics = stage_payload.get("metrics") if isinstance(stage_payload, Mapping) else None
            comparison = stage_payload.get("comparison") if isinstance(stage_payload, Mapping) else None
            targets = comparison.get("targets") if isinstance(comparison, Mapping) else {}
            signed = comparison.get("signed_deltas") if isinstance(comparison, Mapping) else {}
            absolute = comparison.get("absolute_deltas") if isinstance(comparison, Mapping) else {}
            thresholds = comparison.get("thresholds") if isinstance(comparison, Mapping) else {}
            within = comparison.get("within_threshold") if isinstance(comparison, Mapping) else {}
            for metric in _METRIC_ORDER:
                rows.append(
                    {
                        "run_id": iteration_payload["run_id"],
                        "iteration": iteration,
                        "stage": stage_name,
                        "metric": metric,
                        "value": None if metrics is None else _float_or_none(metrics.get(metric)),
                        "target": _float_or_none(targets.get(metric)) if isinstance(targets, Mapping) else None,
                        "signed_delta": _float_or_none(signed.get(metric)) if isinstance(signed, Mapping) else None,
                        "abs_delta": _float_or_none(absolute.get(metric)) if isinstance(absolute, Mapping) else None,
                        "threshold": _float_or_none(thresholds.get(metric)) if isinstance(thresholds, Mapping) else None,
                        "within_threshold": within.get(metric) if isinstance(within, Mapping) else None,
                        "status": stage_payload.get("status"),
                        "source_path": stage_payload.get("source_config_path"),
                    }
                )
    return rows


def _write_rows_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "run_id",
        "iteration",
        "stage",
        "metric",
        "value",
        "target",
        "signed_delta",
        "abs_delta",
        "threshold",
        "within_threshold",
        "status",
        "source_path",
    ]
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _render_figure(
    path: Path,
    rows: list[dict[str, Any]],
    *,
    selected_iteration: int | None = None,
) -> None:
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception as exc:  # pragma: no cover - environment-specific dependency availability
        raise ConfigError(f"matplotlib is required to render tuning progress figures: {exc}") from exc

    iterations = sorted({int(row["iteration"]) for row in rows})
    fig, axes = plt.subplots(2, 2, figsize=(12, 8), constrained_layout=True)
    stage_styles = {
        "zerod_pre_mapping": {"label": "0D pre-mapping", "color": "#1f4e79", "marker": "o"},
        "threed_preop": {"label": "3D preop", "color": "#bf5b17", "marker": "s"},
    }
    for axis, metric in zip(axes.flat, _METRIC_ORDER):
        metric_rows = [row for row in rows if row["metric"] == metric]
        target = next((row["target"] for row in metric_rows if row["target"] is not None), None)
        threshold = next((row["threshold"] for row in metric_rows if row["threshold"] is not None), None)
        if target is not None and threshold is not None:
            axis.axhspan(target - threshold, target + threshold, color="#d9ead3", alpha=0.45)
        if target is not None:
            axis.axhline(target, color="#274e13", linestyle="--", linewidth=1.2)
        if selected_iteration is not None:
            axis.axvline(
                selected_iteration,
                color="#7f6000",
                linestyle=":",
                linewidth=1.4,
                label="Selected iteration",
            )
        for stage, style in stage_styles.items():
            stage_rows = sorted(
                (row for row in metric_rows if row["stage"] == stage),
                key=lambda row: int(row["iteration"]),
            )
            x_values = [int(row["iteration"]) for row in stage_rows]
            y_values = [row["value"] for row in stage_rows]
            axis.plot(
                x_values,
                y_values,
                marker=style["marker"],
                color=style["color"],
                linewidth=1.6,
                label=style["label"],
            )
        axis.set_title(_PANEL_TITLES[metric])
        axis.set_xlabel("Iteration")
        axis.set_ylabel(_METRIC_LABELS[metric])
        axis.set_xticks(iterations)
        axis.grid(True, alpha=0.25)
        if selected_iteration is not None:
            y_min, y_max = axis.get_ylim()
            axis.annotate(
                "selected",
                xy=(selected_iteration, y_max),
                xytext=(4, -6),
                textcoords="offset points",
                ha="left",
                va="top",
                fontsize=8,
                color="#7f6000",
                bbox={"boxstyle": "round,pad=0.15", "facecolor": "#fff2cc", "edgecolor": "none", "alpha": 0.9},
            )
    handles, labels = axes.flat[0].get_legend_handles_labels()
    if handles:
        fig.legend(handles, labels, loc="upper center", ncol=2, frameon=False)
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=200)
    plt.close(fig)


def write_tuning_progress(
    *,
    workspace_root: str | Path,
    run_id: str,
    output_dir: str | Path | None = None,
    overwrite: bool = False,
) -> TuningProgressWriteResult:
    root = detect_workspace_root(workspace_root)
    validated_run_id = validate_run_id(run_id)
    local_paths = build_local_run_paths(root, validated_run_id)
    if not local_paths.run_dir.exists():
        raise ConfigError(f"run directory does not exist: {local_paths.run_dir}")
    manifest = read_manifest(local_paths.manifest)
    selected_iteration = (
        int(manifest.converged_preop_iteration.iteration)
        if manifest.converged_preop_iteration is not None
        else None
    )

    out_dir = Path(output_dir).expanduser().resolve() if output_dir is not None else default_tuning_progress_output_dir(root, validated_run_id)
    csv_path = out_dir / _CSV_FILENAME
    json_path = out_dir / _JSON_FILENAME
    figure_path = out_dir / _FIGURE_FILENAME
    if not overwrite and any(path.exists() for path in (csv_path, json_path, figure_path)):
        raise ConfigError(f"tuning progress outputs already exist under {out_dir}; pass overwrite=True to replace them")

    iteration_dirs = sorted(path for path in local_paths.iterations.glob("iter-*") if path.is_dir())
    if not iteration_dirs:
        raise ConfigError(f"no iteration directories found for run '{validated_run_id}'")

    iterations_payload: list[dict[str, Any]] = []
    for iteration_dir in iteration_dirs:
        decision_payload = _load_json(iteration_dir / "results" / "iteration_decision.json")
        metrics_payload = _load_json(iteration_dir / "results" / "iteration_metrics.json")
        iteration = int(iteration_dir.name.split("-")[-1])
        iterations_payload.append(
            {
                "run_id": validated_run_id,
                "iteration": iteration,
                "zerod_pre_mapping": _zerod_stage_summary(
                    root,
                    run_id=validated_run_id,
                    run_dir=local_paths.run_dir,
                    iteration_dir=iteration_dir,
                    decision_payload=decision_payload,
                ),
                "threed_preop": _threed_stage_summary(
                    run_id=validated_run_id,
                    iteration_dir=iteration_dir,
                    metrics_payload=metrics_payload,
                    decision_payload=decision_payload,
                ),
            }
        )

    rows = _long_rows(iterations_payload)
    payload = {
        "run_id": validated_run_id,
        "output_dir": str(out_dir),
        "selected_preop_iteration": selected_iteration,
        "artifacts": {
            "csv": str(csv_path),
            "json": str(json_path),
            "figure": str(figure_path),
        },
        "iterations": iterations_payload,
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    _write_rows_csv(csv_path, rows)
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    _render_figure(figure_path, rows, selected_iteration=selected_iteration)
    return TuningProgressWriteResult(
        run_id=validated_run_id,
        output_dir=out_dir,
        csv_path=csv_path,
        json_path=json_path,
        figure_path=figure_path,
    )
