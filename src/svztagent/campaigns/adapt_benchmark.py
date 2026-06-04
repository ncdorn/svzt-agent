"""Campaign helpers for adaptation model benchmarking."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any
import csv
import json

import yaml

from svztagent.core.errors import ConfigError
from svztagent.hpc.interfaces import ExecutionMode
from svztagent.workflows.adapt import run_adapt


def generate_campaign_id(now: datetime | None = None) -> str:
    now = now or datetime.now(UTC)
    return f"adapt-benchmark-{now.strftime('%Y%m%d-%H%M%S')}"


def _campaign_dir(workspace_root: str | Path, campaign_id: str) -> Path:
    return (
        Path(workspace_root).expanduser().resolve()
        / "runs"
        / "campaigns"
        / campaign_id
    )


def _manifest_path(workspace_root: str | Path, campaign_id: str) -> Path:
    return _campaign_dir(workspace_root, campaign_id) / "campaign_manifest.yaml"


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ConfigError(f"missing campaign manifest: {path}")
    with path.open("r", encoding="utf-8") as stream:
        payload = yaml.safe_load(stream) or {}
    if not isinstance(payload, dict):
        raise ConfigError(f"{path} must contain a YAML mapping")
    return payload


def _write_yaml(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as stream:
        yaml.safe_dump(payload, stream, sort_keys=False)


def _iter_run_manifests(workspace_root: Path):
    runs_root = workspace_root / "runs"
    for run_dir in sorted(runs_root.iterdir()) if runs_root.exists() else []:
        if not run_dir.is_dir() or run_dir.name == "campaigns":
            continue
        manifest_path = run_dir / "manifest.yaml"
        if manifest_path.exists():
            yield run_dir.name, manifest_path


def _read_run_manifest(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as stream:
        payload = yaml.safe_load(stream) or {}
    if not isinstance(payload, dict):
        raise ConfigError(f"run manifest must contain a mapping: {path}")
    return payload


def _select_source_runs(workspace_root: Path, run_ids: list[str] | None) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    requested = set(run_ids or [])
    for run_id, manifest_path in _iter_run_manifests(workspace_root):
        if requested and run_id not in requested:
            continue
        manifest = _read_run_manifest(manifest_path)
        if not manifest.get("converged_preop_iteration") or not manifest.get("postop_run"):
            continue
        selected.append(
            {
                "run_id": run_id,
                "manifest_path": str(manifest_path),
                "patient": str((manifest.get("patient") or {}).get("alias") or ""),
            }
        )
    if requested:
        found = {entry["run_id"] for entry in selected}
        missing = sorted(requested.difference(found))
        if missing:
            raise ConfigError(
                "adapt benchmark source runs missing converged preop/postop prerequisites: "
                + ", ".join(missing)
            )
    if not selected:
        raise ConfigError("no eligible source runs found for adapt benchmark")
    return selected


def plan_adapt_benchmark_campaign(
    *,
    workspace_root: str | Path,
    run_ids: list[str] | None = None,
    campaign_id: str | None = None,
    models: list[str] | None = None,
    parameter_set: str | None = None,
    benchmark_mode: str = "predict",
) -> dict[str, Any]:
    workspace = Path(workspace_root).expanduser().resolve()
    selected_models = [item.upper() for item in (models or ["M1", "M2", "M3"])]
    if benchmark_mode not in {"predict", "retrospective_fit"}:
        raise ConfigError("benchmark_mode must be one of predict|retrospective_fit")

    runs = _select_source_runs(workspace, run_ids)
    campaign = campaign_id or generate_campaign_id()
    child_runs: list[dict[str, Any]] = []
    for source in runs:
        for model in selected_models:
            child_runs.append(
                {
                    "run_id": source["run_id"],
                    "patient": source["patient"],
                    "model": model,
                    "parameter_set": parameter_set,
                    "benchmark_mode": benchmark_mode,
                }
            )

    manifest = {
        "campaign_id": campaign,
        "workflow": "adapt-benchmark",
        "created_at": datetime.now(UTC).isoformat(),
        "source_run_ids": [entry["run_id"] for entry in runs],
        "models": selected_models,
        "parameter_set": parameter_set,
        "benchmark_mode": benchmark_mode,
        "child_runs": child_runs,
    }
    _write_yaml(_manifest_path(workspace, campaign), manifest)
    return manifest


def run_adapt_benchmark_campaign(
    *,
    workspace_root: str | Path,
    campaign_id: str,
    mode: ExecutionMode,
) -> dict[str, Any]:
    workspace = Path(workspace_root).expanduser().resolve()
    manifest = _read_yaml(_manifest_path(workspace, campaign_id))
    results: list[dict[str, Any]] = []
    for child in manifest.get("child_runs", []):
        result = run_adapt(
            workspace_root=workspace,
            run_id=str(child["run_id"]),
            model=str(child["model"]),
            parameter_set=child.get("parameter_set"),
            adaptation_mode=str(child.get("benchmark_mode", "predict")),
            mode=mode,
        )
        results.append(
            {
                "run_id": result.run_id,
                "model": result.model,
                "parameter_set": result.parameter_set,
                "mode": result.mode.value,
                "submitted_job_id": result.submitted_job_id,
            }
        )
    manifest["last_run_results"] = results
    manifest["updated_at"] = datetime.now(UTC).isoformat()
    _write_yaml(_manifest_path(workspace, campaign_id), manifest)
    return manifest


def _comparison_path_for_child(workspace: Path, run_id: str, model: str) -> Path | None:
    run_dir = workspace / "runs" / run_id
    manifest_path = run_dir / "manifest.yaml"
    if not manifest_path.exists():
        return None
    manifest = _read_run_manifest(manifest_path)
    selected = manifest.get("converged_preop_iteration") or {}
    iteration = int(selected.get("iteration") or 0)
    if iteration <= 0:
        return None
    candidate = (
        run_dir
        / "adaptation"
        / f"from-iter-{iteration:02d}"
        / model.lower()
        / "results"
        / "baseline_vs_adapted_comparison.json"
    )
    if candidate.exists():
        return candidate
    pulled = (
        run_dir
        / "pulled_outputs"
        / "adaptation"
        / f"from-iter-{iteration:02d}"
        / model.lower()
        / "results"
        / "baseline_vs_adapted_comparison.json"
    )
    return pulled if pulled.exists() else None


def summarize_adapt_benchmark_campaign(
    *,
    workspace_root: str | Path,
    campaign_id: str,
) -> list[dict[str, Any]]:
    workspace = Path(workspace_root).expanduser().resolve()
    manifest = _read_yaml(_manifest_path(workspace, campaign_id))
    rows: list[dict[str, Any]] = []
    for child in manifest.get("child_runs", []):
        comparison_path = _comparison_path_for_child(
            workspace,
            run_id=str(child["run_id"]),
            model=str(child["model"]),
        )
        comparison = {}
        if comparison_path is not None:
            comparison = json.loads(comparison_path.read_text(encoding="utf-8"))
        baseline = comparison.get("baseline") or {}
        adapted = comparison.get("adapted") or {}
        improvement = comparison.get("improvement") or {}
        rows.append(
            {
                "campaign_id": campaign_id,
                "run_id": child["run_id"],
                "patient": child.get("patient"),
                "model": child["model"],
                "benchmark_mode": child.get("benchmark_mode", manifest.get("benchmark_mode")),
                "parameter_set": child.get("parameter_set") or "default",
                "baseline_mae": baseline.get("mae"),
                "adapted_mae": adapted.get("mae"),
                "mae_delta": improvement.get("mae_delta"),
                "baseline_rpa_split_error": (baseline.get("errors") or {}).get("rpa_split"),
                "adapted_rpa_split_error": (adapted.get("errors") or {}).get("rpa_split"),
                "comparison_json": str(comparison_path) if comparison_path is not None else None,
            }
        )

    out_dir = _campaign_dir(workspace, campaign_id)
    summary_json = out_dir / "adapt_benchmark_summary.json"
    summary_csv = out_dir / "adapt_benchmark_summary.csv"
    summary_json.write_text(json.dumps(rows, indent=2, sort_keys=True), encoding="utf-8")
    with summary_csv.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0].keys()) if rows else [])
        if rows:
            writer.writeheader()
            writer.writerows(rows)
    return rows
