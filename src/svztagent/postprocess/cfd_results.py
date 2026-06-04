"""Run-scoped CFD results normalization helpers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import copy
import csv
import json
from typing import Any

from svztagent.config.load import detect_workspace_root
from svztagent.core.errors import ConfigError
from svztagent.core.manifest import read_manifest
from svztagent.core.paths import build_local_run_paths, iteration_dir_name


@dataclass(frozen=True)
class CfdResultsWriteResult:
    run_id: str
    workspace_root: Path
    template_path: Path
    output_path: Path
    source_path: Path | None


def default_cfd_results_template_path(workspace_root: str | Path) -> Path:
    root = Path(workspace_root).expanduser().resolve()
    return root / "data" / "cfd-results" / "cfd-results-template.json"


def default_cfd_results_output_path(workspace_root: str | Path, run_id: str) -> Path:
    root = Path(workspace_root).expanduser().resolve()
    return build_local_run_paths(root, run_id).run_dir / "cfd-results.json"


def _load_json_object(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ConfigError(f"JSON file not found: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ConfigError(f"Invalid JSON in {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ConfigError(f"Expected JSON object at {path}")
    return payload


def _json_copy(value: Any) -> Any:
    return copy.deepcopy(value)


def _merge_template_overlay(template: Any, source: Any) -> Any:
    """Overlay source values onto the template without adding new keys."""

    if isinstance(template, dict):
        result: dict[str, Any] = {}
        source_dict = source if isinstance(source, dict) else {}
        for key, template_value in template.items():
            if key in source_dict:
                result[key] = _merge_template_overlay(template_value, source_dict[key])
            else:
                result[key] = _json_copy(template_value)
        return result

    if isinstance(template, list):
        if not isinstance(source, list):
            return _json_copy(template)
        if not template:
            return _json_copy(source)
        result: list[Any] = []
        for index, template_item in enumerate(template):
            if index < len(source):
                result.append(_merge_template_overlay(template_item, source[index]))
            else:
                result.append(_json_copy(template_item))
        return result

    return _json_copy(source)


def _set_nested(mapping: dict[str, Any], path: list[str], value: Any) -> None:
    cursor = mapping
    for key in path[:-1]:
        next_value = cursor.get(key)
        if not isinstance(next_value, dict):
            next_value = {}
            cursor[key] = next_value
        cursor = next_value
    cursor[path[-1]] = value


def _relative_to_workspace(workspace_root: Path, path: Path) -> str | None:
    if not path.exists():
        return None
    try:
        return path.resolve().relative_to(workspace_root.resolve()).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise ConfigError(f"CSV file not found: {path}")
    with path.open("r", encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        return list(reader)


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _normalize_run_status(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    if not text:
        return None
    aliases = {
        "complete": "completed",
        "completed": "completed",
        "success": "completed",
        "succeeded": "completed",
        "submitted": "submitted",
        "running": "running",
        "pending": "pending",
        "failed": "failed",
        "error": "failed",
        "cancelled": "cancelled",
        "canceled": "cancelled",
        "unknown": "unknown",
    }
    return aliases.get(text, text)


def _pct_from_fraction(value: float | None) -> float | None:
    if value is None:
        return None
    return round(value * 100.0, 10)


def _resolve_selected_iteration(manifest: Any) -> int:
    if manifest.converged_preop_iteration is not None:
        return int(manifest.converged_preop_iteration.iteration)
    if manifest.selected_preop_postprocess is not None:
        return int(manifest.selected_preop_postprocess.source_preop_iteration)
    if manifest.postop_run is not None:
        return int(manifest.postop_run.source_preop_iteration)
    if manifest.postop_postprocess is not None:
        return int(manifest.postop_postprocess.source_preop_iteration)
    if manifest.tuning_iteration_tracker.converged_iteration is not None:
        return int(manifest.tuning_iteration_tracker.converged_iteration)
    raise ConfigError(
        f"Run {manifest.run_id} does not record a selected preop iteration for CFD results migration"
    )


def _selected_preop_paths(run_dir: Path, iteration: int) -> dict[str, Path]:
    name = iteration_dir_name(iteration)
    root = run_dir / "iterations" / name
    results = root / "results"
    postprocess = results / "postprocess"
    return {
        "iteration_root": root,
        "results": results,
        "postprocess": postprocess,
        "metrics": results / "iteration_metrics.json",
        "decision": results / "iteration_decision.json",
        "suite_metadata": postprocess / "postprocess_suite_metadata.json",
    }


def _postop_paths(run_dir: Path, iteration: int) -> dict[str, Path]:
    root = run_dir / "postop" / f"from-{iteration_dir_name(iteration)}"
    postprocess = root / "results" / "postprocess"
    return {
        "root": root,
        "postprocess": postprocess,
        "suite_metadata": postprocess / "postprocess_suite_metadata.json",
    }


def _latest_adaptation_record(manifest: Any, iteration: int) -> Any | None:
    records = getattr(manifest, "adaptation_runs", None) or []
    matching = [
        record
        for record in records
        if int(getattr(record, "source_preop_iteration", -1)) == int(iteration)
    ]
    candidates = matching or records
    return candidates[-1] if candidates else None


def _adapted_paths(run_dir: Path, manifest: Any, iteration: int) -> dict[str, Path | Any | None]:
    record = _latest_adaptation_record(manifest, iteration)
    root: Path | None = None
    if record is not None and getattr(record, "local_dir", None):
        candidate = Path(str(record.local_dir)).expanduser().resolve()
        if candidate.exists():
            root = candidate

    if root is None:
        adapt_root = run_dir / "adaptation" / f"from-{iteration_dir_name(iteration)}"
        if adapt_root.exists():
            model_dirs = sorted(path for path in adapt_root.iterdir() if path.is_dir())
            if model_dirs:
                root = model_dirs[-1]

    if root is None:
        return {
            "record": record,
            "root": None,
            "results": None,
            "postprocess": None,
            "suite_metadata": None,
        }

    results = root / "results"
    postprocess = results / "adapted_postprocess"
    return {
        "record": record,
        "root": root,
        "results": results,
        "postprocess": postprocess,
        "suite_metadata": postprocess / "postprocess_suite_metadata.json",
    }


def _load_pressure_metrics(csv_path: Path) -> dict[str, float | None]:
    rows = _read_csv_rows(csv_path)
    values = [_float_or_none(row.get("mpa_pressure_mmhg")) for row in rows]
    series = [value for value in values if value is not None]
    if not series:
        return {
            "mpa_systolic_mmhg": None,
            "mpa_diastolic_mmhg": None,
            "mpa_mean_mmhg": None,
        }
    return {
        "mpa_systolic_mmhg": max(series),
        "mpa_diastolic_mmhg": min(series),
        "mpa_mean_mmhg": sum(series) / len(series),
    }


def _preferred_resistance_artifacts(postprocess_dir: Path, *, prefer_systolic: bool) -> dict[str, Any]:
    candidates: list[tuple[str, Path, Path]] = []
    if prefer_systolic:
        candidates.append(
            (
                "systolic",
                postprocess_dir / "branch_resistance_summary_systolic.csv",
                postprocess_dir / "resistance_map_systolic.png",
            )
        )
    candidates.append(
        (
            "mean",
            postprocess_dir / "branch_resistance_summary.csv",
            postprocess_dir / "resistance_map_mean.png",
        )
    )
    for metric_suffix, summary_csv, png_path in candidates:
        if summary_csv.exists():
            return {
                "metric_suffix": metric_suffix,
                "summary_csv": summary_csv,
                "png_path": png_path if png_path.exists() else None,
            }
    return {
        "metric_suffix": None,
        "summary_csv": None,
        "png_path": None,
    }


def _load_hotspots(summary_csv: Path, metric_suffix: str, template_entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = _read_csv_rows(summary_csv)
    resistance_key = f"resistance_{metric_suffix}"
    ranked = sorted(
        rows,
        key=lambda row: int(row.get("rank") or "999999"),
    )
    total_resistance = sum(
        abs(_float_or_none(row.get(resistance_key)) or 0.0) for row in ranked
    )

    results: list[dict[str, Any]] = []
    for index, template_entry in enumerate(template_entries):
        entry = _json_copy(template_entry)
        row = ranked[index] if index < len(ranked) else None
        if row is None:
            results.append(entry)
            continue
        resistance = _float_or_none(row.get(resistance_key))
        branch_id = row.get("branch_id")
        entry["rank"] = index + 1
        entry["vessel"] = f"branch_id:{branch_id}" if branch_id else None
        entry["resistance"] = resistance
        entry["units"] = "mmHg / (L/min)" if resistance is not None else None
        if resistance is None or total_resistance == 0.0:
            entry["contribution_pct"] = None
        else:
            entry["contribution_pct"] = abs(resistance) / total_resistance * 100.0
        entry["interpretation"] = None
        results.append(entry)
    return results


def _load_total_pvr(summary_csv: Path, metric_suffix: str) -> tuple[float | None, str | None]:
    rows = _read_csv_rows(summary_csv)
    resistance_key = f"resistance_{metric_suffix}"
    total_resistance = sum(
        abs(_float_or_none(row.get(resistance_key)) or 0.0) for row in rows
    )
    if total_resistance == 0.0:
        return None, None
    return total_resistance, "mmHg / (L/min)"


def _state_metrics_from_iteration_metrics(metrics_payload: dict[str, Any]) -> dict[str, float | None]:
    rpa_split = _float_or_none(metrics_payload.get("rpa_split"))
    rpa_flow_pct = rpa_split * 100.0 if rpa_split is not None else None
    return {
        "mpa_systolic_mmhg": _float_or_none(metrics_payload.get("mpa_sys")),
        "mpa_diastolic_mmhg": _float_or_none(metrics_payload.get("mpa_dia")),
        "mpa_mean_mmhg": _float_or_none(metrics_payload.get("mpa_mean")),
        "rpa_flow_pct": rpa_flow_pct,
        "lpa_flow_pct": (100.0 - rpa_flow_pct) if rpa_flow_pct is not None else None,
        "stenosis_gradient_mmhg": None,
        "total_pvr": None,
        "total_pvr_units": None,
    }


def _state_metrics_from_postop_suite(postprocess_dir: Path, suite_payload: dict[str, Any]) -> dict[str, float | None]:
    pressure_csv = postprocess_dir / "mpa_pressure_vs_time.csv"
    pressure_metrics = _load_pressure_metrics(pressure_csv)
    flow_split = suite_payload.get("flow_split") if isinstance(suite_payload.get("flow_split"), dict) else {}
    rpa_split = _float_or_none(flow_split.get("rpa_split"))
    rpa_flow_pct = rpa_split * 100.0 if rpa_split is not None else None
    return {
        **pressure_metrics,
        "rpa_flow_pct": rpa_flow_pct,
        "lpa_flow_pct": (100.0 - rpa_flow_pct) if rpa_flow_pct is not None else None,
        "stenosis_gradient_mmhg": None,
        "total_pvr": None,
        "total_pvr_units": None,
    }


def _run_path_from_suite(suite_payload: dict[str, Any], fallback: str | None) -> str | None:
    simulation_dir = suite_payload.get("simulation_dir")
    if isinstance(simulation_dir, str) and simulation_dir:
        return str(Path(simulation_dir).parent)
    return fallback


def _status_from_preop_artifacts(
    metrics_payload: dict[str, Any],
    suite_payload: dict[str, Any],
    fallback: str | None,
) -> str:
    return (
        _normalize_run_status(suite_payload.get("status"))
        or _normalize_run_status(metrics_payload.get("preop_terminal_state"))
        or fallback
        or "pending"
    )


def _status_from_postop_manifest(manifest: Any, fallback: str | None) -> str:
    return (
        _normalize_run_status(
            manifest.postop_postprocess.status if manifest.postop_postprocess is not None else None
        )
        or _normalize_run_status(manifest.postop_run.status if manifest.postop_run is not None else None)
        or _normalize_run_status(
            manifest.execution.get("normalized_scheduler_state")
            if isinstance(manifest.execution, dict)
            else None
        )
        or fallback
        or "pending"
    )


def _apply_state_payload(
    result: dict[str, Any],
    *,
    state_key: str,
    run_id: str | None,
    run_path: str | None,
    run_status: str | None,
    metrics: dict[str, float | None] | None,
    hotspots: list[dict[str, Any]] | None,
) -> None:
    prefix = ["states", state_key]
    _set_nested(result, prefix + ["run_id"], run_id)
    _set_nested(result, prefix + ["run_path"], run_path)
    if run_status is not None:
        _set_nested(result, prefix + ["run_status"], run_status)
    if metrics is not None:
        _set_nested(result, prefix + ["metrics"], metrics)
    if hotspots is not None:
        _set_nested(result, prefix + ["resistance_hotspots"], hotspots)


def _compute_error_entry(
    measured: float | None,
    baseline_simulated: float | None,
    adapted_simulated: float | None,
    entry: dict[str, Any],
) -> dict[str, Any]:
    updated = _json_copy(entry)
    if measured is None or baseline_simulated is None:
        updated["baseline_error"] = None
        updated["baseline_abs"] = None
        updated["baseline_pct"] = None
    else:
        baseline_error = baseline_simulated - measured
        updated["baseline_error"] = baseline_error
        updated["baseline_abs"] = abs(baseline_error)
        updated["baseline_pct"] = None if measured == 0 else baseline_error / measured * 100.0

    if measured is None or adapted_simulated is None:
        updated["adapted_error"] = None
        updated["adapted_abs"] = None
        updated["adapted_pct"] = None
        updated["improvement_abs"] = None
        updated["improvement_pct"] = None
        updated["closer_to_measured"] = "insufficient"
        return updated

    adapted_error = adapted_simulated - measured
    updated["adapted_error"] = adapted_error
    updated["adapted_abs"] = abs(adapted_error)
    updated["adapted_pct"] = None if measured == 0 else adapted_error / measured * 100.0

    baseline_abs = updated.get("baseline_abs")
    adapted_abs = updated.get("adapted_abs")
    if baseline_abs is None or adapted_abs is None:
        updated["improvement_abs"] = None
        updated["improvement_pct"] = None
        updated["closer_to_measured"] = "insufficient"
        return updated

    updated["improvement_abs"] = baseline_abs - adapted_abs
    updated["improvement_pct"] = (
        None if baseline_abs == 0 else (baseline_abs - adapted_abs) / baseline_abs * 100.0
    )
    if abs(baseline_abs - adapted_abs) < 0.05:
        updated["closer_to_measured"] = "equivalent"
    elif adapted_abs < baseline_abs:
        updated["closer_to_measured"] = "adapted"
    else:
        updated["closer_to_measured"] = "baseline"
    return updated


def build_run_cfd_results(
    *,
    workspace_root: str | Path,
    run_id: str,
    template_path: str | Path | None = None,
    source_path: str | Path | None = None,
    output_path: str | Path | None = None,
) -> tuple[dict[str, Any], Path, Path, Path | None]:
    root = detect_workspace_root(workspace_root)
    template_file = (
        Path(template_path).expanduser().resolve()
        if template_path is not None
        else default_cfd_results_template_path(root)
    )
    output_file = (
        Path(output_path).expanduser().resolve()
        if output_path is not None
        else default_cfd_results_output_path(root, run_id)
    )
    resolved_source = (
        Path(source_path).expanduser().resolve()
        if source_path is not None
        else output_file.resolve() if output_file.exists() else None
    )

    template = _load_json_object(template_file)
    source = _load_json_object(resolved_source) if resolved_source is not None and resolved_source.exists() else None
    result = _merge_template_overlay(template, source) if source is not None else _json_copy(template)

    run_paths = build_local_run_paths(root, run_id)
    manifest = read_manifest(run_paths.manifest)
    selected_iteration = _resolve_selected_iteration(manifest)
    preop_paths = _selected_preop_paths(run_paths.run_dir, selected_iteration)
    postop_paths = _postop_paths(run_paths.run_dir, selected_iteration)
    adapted_paths = _adapted_paths(run_paths.run_dir, manifest, selected_iteration)

    preop_decision = _load_json_object(preop_paths["decision"])
    preop_metrics_payload = _load_json_object(preop_paths["metrics"])
    preop_suite = _load_json_object(preop_paths["suite_metadata"]) if preop_paths["suite_metadata"].exists() else {}
    postop_suite = _load_json_object(postop_paths["suite_metadata"]) if postop_paths["suite_metadata"].exists() else {}
    adapted_suite = (
        _load_json_object(adapted_paths["suite_metadata"])
        if isinstance(adapted_paths.get("suite_metadata"), Path)
        and adapted_paths["suite_metadata"].exists()
        else {}
    )

    result["patient_id"] = manifest.patient["alias"]

    measured_preop = result.get("measured_preop")
    if isinstance(measured_preop, dict):
        clinical_targets = preop_decision.get("clinical_targets") if isinstance(preop_decision.get("clinical_targets"), dict) else {}
        rpa_split = _float_or_none(clinical_targets.get("rpa_split"))
        measured_preop["mpa_systolic_mmhg"] = _float_or_none(clinical_targets.get("mpa_sys"))
        measured_preop["mpa_diastolic_mmhg"] = _float_or_none(clinical_targets.get("mpa_dia"))
        measured_preop["mpa_mean_mmhg"] = _float_or_none(clinical_targets.get("mpa_mean"))
        measured_preop["rpa_flow_pct"] = _pct_from_fraction(rpa_split)
        measured_preop["lpa_flow_pct"] = (
            round(100.0 - measured_preop["rpa_flow_pct"], 10)
            if measured_preop["rpa_flow_pct"] is not None
            else None
        )

    preop_artifacts = _preferred_resistance_artifacts(preop_paths["postprocess"], prefer_systolic=True)
    postop_artifacts = _preferred_resistance_artifacts(postop_paths["postprocess"], prefer_systolic=True)

    preop_hotspot_template = result["states"]["preop_tuned"]["resistance_hotspots"]
    postop_hotspot_template = result["states"]["postop_baseline"]["resistance_hotspots"]

    preop_hotspots = None
    if preop_artifacts["summary_csv"] is not None:
        preop_hotspots = _load_hotspots(
            preop_artifacts["summary_csv"],
            str(preop_artifacts["metric_suffix"]),
            preop_hotspot_template,
        )
        preop_total_pvr, preop_total_pvr_units = _load_total_pvr(
            preop_artifacts["summary_csv"],
            str(preop_artifacts["metric_suffix"]),
        )
    else:
        preop_total_pvr, preop_total_pvr_units = (None, None)

    postop_hotspots = None
    if postop_artifacts["summary_csv"] is not None:
        postop_hotspots = _load_hotspots(
            postop_artifacts["summary_csv"],
            str(postop_artifacts["metric_suffix"]),
            postop_hotspot_template,
        )
        postop_total_pvr, postop_total_pvr_units = _load_total_pvr(
            postop_artifacts["summary_csv"],
            str(postop_artifacts["metric_suffix"]),
        )
    else:
        postop_total_pvr, postop_total_pvr_units = (None, None)

    preop_metrics = _state_metrics_from_iteration_metrics(preop_metrics_payload)
    preop_metrics["total_pvr"] = preop_total_pvr
    preop_metrics["total_pvr_units"] = preop_total_pvr_units

    _apply_state_payload(
        result,
        state_key="preop_tuned",
        run_id=manifest.run_id,
        run_path=_run_path_from_suite(
            preop_suite,
            manifest.converged_preop_iteration.remote_iteration_dir
            if manifest.converged_preop_iteration is not None
            else None,
        ),
        run_status=_status_from_preop_artifacts(
            preop_metrics_payload,
            preop_suite,
            _normalize_run_status(result["states"]["preop_tuned"].get("run_status")),
        ),
        metrics=preop_metrics,
        hotspots=preop_hotspots,
    )

    postop_metrics = (
        _state_metrics_from_postop_suite(postop_paths["postprocess"], postop_suite)
        if postop_suite
        else None
    )
    if postop_metrics is not None:
        postop_metrics["total_pvr"] = postop_total_pvr
        postop_metrics["total_pvr_units"] = postop_total_pvr_units
    _apply_state_payload(
        result,
        state_key="postop_baseline",
        run_id=manifest.run_id,
        run_path=_run_path_from_suite(
            postop_suite,
            manifest.postop_run.remote_dir if manifest.postop_run is not None else None,
        ),
        run_status=(
            _normalize_run_status(postop_suite.get("status"))
            or _status_from_postop_manifest(
                manifest,
                _normalize_run_status(result["states"]["postop_baseline"].get("run_status")),
            )
        ),
        metrics=postop_metrics,
        hotspots=postop_hotspots,
    )

    adapted_artifacts = (
        _preferred_resistance_artifacts(adapted_paths["postprocess"], prefer_systolic=True)
        if isinstance(adapted_paths.get("postprocess"), Path)
        else {"metric_suffix": None, "summary_csv": None, "png_path": None}
    )
    adapted_hotspot_template = result["states"]["postop_adapted"]["resistance_hotspots"]
    adapted_hotspots = None
    if adapted_artifacts["summary_csv"] is not None:
        adapted_hotspots = _load_hotspots(
            adapted_artifacts["summary_csv"],
            str(adapted_artifacts["metric_suffix"]),
            adapted_hotspot_template,
        )
        adapted_total_pvr, adapted_total_pvr_units = _load_total_pvr(
            adapted_artifacts["summary_csv"],
            str(adapted_artifacts["metric_suffix"]),
        )
    else:
        adapted_total_pvr, adapted_total_pvr_units = (None, None)

    adapted_metrics = (
        _state_metrics_from_postop_suite(adapted_paths["postprocess"], adapted_suite)
        if adapted_suite and isinstance(adapted_paths.get("postprocess"), Path)
        else None
    )
    if adapted_metrics is not None:
        adapted_metrics["total_pvr"] = adapted_total_pvr
        adapted_metrics["total_pvr_units"] = adapted_total_pvr_units
    adapted_record = adapted_paths.get("record")
    _apply_state_payload(
        result,
        state_key="postop_adapted",
        run_id=manifest.run_id,
        run_path=_run_path_from_suite(
            adapted_suite,
            str(adapted_record.remote_dir) if adapted_record is not None and getattr(adapted_record, "remote_dir", None) else None,
        ),
        run_status=(
            _normalize_run_status(adapted_suite.get("status"))
            or _normalize_run_status(getattr(adapted_record, "status", None))
            or _normalize_run_status(result["states"]["postop_adapted"].get("run_status"))
        ),
        metrics=adapted_metrics,
        hotspots=adapted_hotspots,
    )

    pressure_waveforms = result.get("figures", {}).get("pressure_waveforms")
    if isinstance(pressure_waveforms, dict):
        pressure_waveforms["preop_tuned"] = _relative_to_workspace(
            root, preop_paths["postprocess"] / "mpa_pressure_vs_time.png"
        )
        pressure_waveforms["postop_baseline"] = _relative_to_workspace(
            root, postop_paths["postprocess"] / "mpa_pressure_vs_time.png"
        )
        if isinstance(adapted_paths.get("postprocess"), Path):
            pressure_waveforms["postop_adapted"] = _relative_to_workspace(
                root, adapted_paths["postprocess"] / "mpa_pressure_vs_time.png"
            )

    resistance_maps = result.get("figures", {}).get("resistance_maps")
    if isinstance(resistance_maps, dict):
        preop_png = preop_artifacts["png_path"]
        postop_png = postop_artifacts["png_path"]
        adapted_png = adapted_artifacts["png_path"]
        resistance_maps["preop_tuned"] = (
            _relative_to_workspace(root, preop_png) if isinstance(preop_png, Path) else None
        )
        resistance_maps["postop_baseline"] = (
            _relative_to_workspace(root, postop_png) if isinstance(postop_png, Path) else None
        )
        resistance_maps["postop_adapted"] = (
            _relative_to_workspace(root, adapted_png) if isinstance(adapted_png, Path) else None
        )
        if preop_total_pvr_units is not None:
            resistance_maps["colorbar_units"] = preop_total_pvr_units
        if isinstance(resistance_maps.get("delta_preop_to_baseline"), dict):
            resistance_maps["delta_preop_to_baseline"]["delta_pvr"] = (
                postop_total_pvr - preop_total_pvr
                if preop_total_pvr is not None and postop_total_pvr is not None
                else None
            )
        adapted_metrics = result["states"]["postop_adapted"].get("metrics")
        adapted_total_pvr = (
            _float_or_none(adapted_metrics.get("total_pvr"))
            if isinstance(adapted_metrics, dict)
            else None
        )
        if isinstance(resistance_maps.get("delta_baseline_to_adapted"), dict):
            resistance_maps["delta_baseline_to_adapted"]["delta_pvr"] = (
                adapted_total_pvr - postop_total_pvr
                if adapted_total_pvr is not None and postop_total_pvr is not None
                else None
            )
        if isinstance(resistance_maps.get("delta_preop_to_adapted"), dict):
            resistance_maps["delta_preop_to_adapted"]["delta_pvr"] = (
                adapted_total_pvr - preop_total_pvr
                if adapted_total_pvr is not None and preop_total_pvr is not None
                else None
            )

    solver = result.get("methods", {}).get("solver")
    if isinstance(solver, dict):
        threed_defaults = manifest.remote.get("threed_defaults") if isinstance(manifest.remote, dict) else {}
        solver["name"] = "svMultiPhysics"
        solver["type"] = threed_defaults.get("wall_model")
        solver["steady_or_pulsatile"] = "pulsatile"
        if preop_suite:
            solver["cardiac_cycle_duration_s"] = _float_or_none(preop_suite.get("cycle_duration_s"))
        elif postop_suite:
            solver["cardiac_cycle_duration_s"] = _float_or_none(postop_suite.get("cycle_duration_s"))

    boundary_conditions = result.get("methods", {}).get("boundary_conditions")
    if isinstance(boundary_conditions, dict):
        threed_defaults = manifest.remote.get("threed_defaults") if isinstance(manifest.remote, dict) else {}
        boundary_conditions["inlet"] = threed_defaults.get("inflow_boundary_condition")

    measured_postop = result.get("measured_postop") if isinstance(result.get("measured_postop"), dict) else {}
    postop_baseline_metrics = result["states"]["postop_baseline"]["metrics"]
    postop_adapted_metrics = result["states"]["postop_adapted"]["metrics"]
    error_map = result.get("errors", {}).get("by_metric")
    if isinstance(error_map, dict) and isinstance(postop_baseline_metrics, dict):
        if "mpa_systolic_mmhg" in error_map:
            error_map["mpa_systolic_mmhg"] = _compute_error_entry(
                _float_or_none(measured_postop.get("mpa_systolic_mmhg")),
                _float_or_none(postop_baseline_metrics.get("mpa_systolic_mmhg")),
                _float_or_none(postop_adapted_metrics.get("mpa_systolic_mmhg"))
                if isinstance(postop_adapted_metrics, dict)
                else None,
                error_map["mpa_systolic_mmhg"],
            )
        if "mpa_diastolic_mmhg" in error_map:
            error_map["mpa_diastolic_mmhg"] = _compute_error_entry(
                _float_or_none(measured_postop.get("mpa_diastolic_mmhg")),
                _float_or_none(postop_baseline_metrics.get("mpa_diastolic_mmhg")),
                _float_or_none(postop_adapted_metrics.get("mpa_diastolic_mmhg"))
                if isinstance(postop_adapted_metrics, dict)
                else None,
                error_map["mpa_diastolic_mmhg"],
            )
        if "mpa_mean_mmhg" in error_map:
            error_map["mpa_mean_mmhg"] = _compute_error_entry(
                _float_or_none(measured_postop.get("mpa_mean_mmhg")),
                _float_or_none(postop_baseline_metrics.get("mpa_mean_mmhg")),
                _float_or_none(postop_adapted_metrics.get("mpa_mean_mmhg"))
                if isinstance(postop_adapted_metrics, dict)
                else None,
                error_map["mpa_mean_mmhg"],
            )
        if "flow_split_pct" in error_map:
            error_map["flow_split_pct"] = _compute_error_entry(
                _float_or_none(measured_postop.get("rpa_flow_pct")),
                _float_or_none(postop_baseline_metrics.get("rpa_flow_pct")),
                _float_or_none(postop_adapted_metrics.get("rpa_flow_pct"))
                if isinstance(postop_adapted_metrics, dict)
                else None,
                error_map["flow_split_pct"],
            )
        if "stenosis_gradient_mmhg" in error_map:
            error_map["stenosis_gradient_mmhg"] = _compute_error_entry(
                _float_or_none(measured_postop.get("stenosis_gradient_mmhg")),
                _float_or_none(postop_baseline_metrics.get("stenosis_gradient_mmhg")),
                _float_or_none(postop_adapted_metrics.get("stenosis_gradient_mmhg"))
                if isinstance(postop_adapted_metrics, dict)
                else None,
                error_map["stenosis_gradient_mmhg"],
            )

    return result, template_file, output_file, resolved_source


def write_run_cfd_results(
    *,
    workspace_root: str | Path,
    run_id: str,
    template_path: str | Path | None = None,
    source_path: str | Path | None = None,
    output_path: str | Path | None = None,
    overwrite: bool = False,
) -> CfdResultsWriteResult:
    result, template_file, output_file, resolved_source = build_run_cfd_results(
        workspace_root=workspace_root,
        run_id=run_id,
        template_path=template_path,
        source_path=source_path,
        output_path=output_path,
    )
    if output_file.exists() and not overwrite:
        raise ConfigError(
            f"Refusing to overwrite existing CFD results JSON without --overwrite: {output_file}"
        )
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    return CfdResultsWriteResult(
        run_id=run_id,
        workspace_root=detect_workspace_root(workspace_root),
        template_path=template_file,
        output_path=output_file,
        source_path=resolved_source if resolved_source is not None and resolved_source.exists() else None,
    )
