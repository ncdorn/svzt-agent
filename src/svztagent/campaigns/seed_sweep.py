"""Seed-method campaign orchestration for learned 0D initial guesses."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import importlib.util
from pathlib import Path
from typing import Any
import csv
import json
import shutil
import zipfile

import yaml

from svztagent.core.errors import ConfigError
from svztagent.hpc.interfaces import ExecutionMode
from svztagent.workflows.tune_trees import (
    plan_tune_trees,
    run_tune_trees,
)

DEFAULT_PATIENTS = ("TST-STAN-5",)


@dataclass(frozen=True)
class SeedSweepCase:
    case_id: str
    seed_mode: str
    diameter_scale: float
    tuning_model: str
    allow_ordered_outlet_mapping: bool = False


CASES = (
    SeedSweepCase("learned-tree-dscale-0p0", "learned_path", 0.0, "full_pa", True),
    SeedSweepCase("learned-tree-dscale-0p1", "learned_path", 0.1, "full_pa", True),
    SeedSweepCase("learned-rri-reduced", "rri_from_learned_reference", 0.0, "rri", True),
)


def generate_campaign_id(now: datetime | None = None) -> str:
    now = now or datetime.now(UTC)
    return f"seed-sweep-{now.strftime('%Y%m%d-%H%M%S')}"


def learned_model_path(patient: str) -> Path:
    return (
        Path.home()
        / "Documents/Stanford/PhD/Marsden_Lab/Projects/PPAS/tof-stent"
        / patient
        / "zerod-models/baseline_0d_learned.json"
    )


def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as stream:
        payload = yaml.safe_load(stream) or {}
    if not isinstance(payload, dict):
        raise ConfigError(f"{path} must contain a YAML mapping")
    return payload


def _write_yaml(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as stream:
        yaml.safe_dump(payload, stream, sort_keys=False)


def _copy_workspace_config(source_root: Path, target_root: Path) -> None:
    config_dir = target_root / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    for name in ("clusters.yaml", "patients.yaml", "defaults.yaml"):
        shutil.copy2(source_root / "config" / name, config_dir / name)
    optional = source_root / "config" / "clinical_targets.yaml"
    if optional.exists():
        shutil.copy2(optional, config_dir / optional.name)
    (target_root / "runs").mkdir(parents=True, exist_ok=True)


def _find_patient_entry(patients_yaml: dict[str, Any], patient: str) -> dict[str, Any]:
    for entry in patients_yaml.get("patients", []):
        if entry.get("alias") == patient:
            return entry
    raise ConfigError(f"patient '{patient}' not found in config/patients.yaml")


def _prepare_rri_seed(
    *,
    learned_path: Path,
    output_path: Path,
    metrics_path: Path,
    reduced_template: Path | None = None,
    workspace_root: Path,
) -> None:
    helper = None
    try:
        from svzerodtrees.tuning import prepare_reduced_rri_seed_from_learned
        helper = prepare_reduced_rri_seed_from_learned
    except Exception as exc:  # pragma: no cover - depends on installed sibling package
        helper_path = (
            workspace_root
            / "repos"
            / "svZeroDTrees"
            / "src"
            / "svzerodtrees"
            / "tuning"
            / "learned_seed.py"
        )
        if helper_path.exists():
            spec = importlib.util.spec_from_file_location(
                "svzerodtrees_learned_seed_helper",
                helper_path,
            )
            if spec is not None and spec.loader is not None:
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                helper = module.prepare_reduced_rri_seed_from_learned
        if helper is not None:
            helper(
                learned_config=learned_path,
                reduced_template=reduced_template,
                output_config=output_path,
                metrics_path=metrics_path,
            )
            return

        source = reduced_template or learned_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, output_path)
        metrics_path.parent.mkdir(parents=True, exist_ok=True)
        metrics_path.write_text(
            json.dumps(
                {
                    "learned_config": str(learned_path),
                    "output_config": str(output_path),
                    "source_template": str(source),
                    "method": "rri_from_learned_reference",
                    "warning": f"svZeroDTrees reduced-seed helper unavailable: {exc}",
                },
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        return

    helper(
        learned_config=learned_path,
        reduced_template=reduced_template,
        output_config=output_path,
        metrics_path=metrics_path,
    )


def _local_reduced_template(patient_entry: dict[str, Any]) -> Path | None:
    tuning = patient_entry.get("tuning")
    if not isinstance(tuning, dict):
        return None
    seed = tuning.get("iteration1_seed")
    if not isinstance(seed, dict) or seed.get("source") != "path":
        return None
    seed_path = str(seed.get("path") or "").strip()
    if not seed_path:
        return None
    candidate = Path(seed_path).expanduser()
    return candidate if candidate.exists() else None


def _configure_child_workspace(
    *,
    workspace_root: Path,
    child_root: Path,
    patient: str,
    case: SeedSweepCase,
    learned_path: Path,
) -> Path:
    _copy_workspace_config(workspace_root, child_root)

    patients_yaml_path = child_root / "config" / "patients.yaml"
    defaults_yaml_path = child_root / "config" / "defaults.yaml"
    patients_yaml = _load_yaml(patients_yaml_path)
    defaults_yaml = _load_yaml(defaults_yaml_path)

    patient_entry = _find_patient_entry(patients_yaml, patient)
    tuning = patient_entry.setdefault("tuning", {})
    impedance = tuning.setdefault("impedance", {})
    impedance["diameter_scale"] = case.diameter_scale
    impedance["tuning_model"] = case.tuning_model
    impedance["allow_ordered_outlet_mapping"] = case.allow_ordered_outlet_mapping
    if case.tuning_model == "full_pa" and case.diameter_scale > 0.0:
        impedance["use_mean"] = False

    seed_path = learned_path
    if case.seed_mode == "rri_from_learned_reference":
        seed_path = child_root / "prepared_inputs" / "simplified_zerod_tuned_RRI.json"
        _prepare_rri_seed(
            learned_path=learned_path,
            output_path=seed_path,
            metrics_path=child_root / "prepared_inputs" / "rri_seed_metrics.json",
            reduced_template=_local_reduced_template(patient_entry),
            workspace_root=workspace_root,
        )

    tuning["iteration1_seed"] = {
        "source": "path",
        "path": str(seed_path),
    }

    default_impedance = defaults_yaml.setdefault("defaults", {}).setdefault(
        "tuning", {}
    ).setdefault("impedance", {})
    default_impedance.setdefault("allow_ordered_outlet_mapping", False)
    default_impedance.setdefault("tuning_model", "rri")

    _write_yaml(patients_yaml_path, patients_yaml)
    _write_yaml(defaults_yaml_path, defaults_yaml)
    return seed_path


def _manifest_path(campaign_dir: Path) -> Path:
    return campaign_dir / "campaign_manifest.yaml"


def _read_manifest(campaign_dir: Path) -> dict[str, Any]:
    path = _manifest_path(campaign_dir)
    if not path.exists():
        raise ConfigError(f"campaign manifest not found: {path}")
    return _load_yaml(path)


def plan_seed_sweep_campaign(
    *,
    workspace_root: str | Path,
    cluster_name: str,
    patients: list[str] | None = None,
    campaign_id: str | None = None,
) -> dict[str, Any]:
    workspace = Path(workspace_root).expanduser().resolve()
    campaign = campaign_id or generate_campaign_id()
    selected_patients = patients or list(DEFAULT_PATIENTS)
    campaign_dir = workspace / "runs" / "campaigns" / campaign
    campaign_dir.mkdir(parents=True, exist_ok=True)

    child_runs: list[dict[str, Any]] = []
    for patient in selected_patients:
        learned_path = learned_model_path(patient)
        if not learned_path.exists():
            raise ConfigError(f"learned 0D model does not exist: {learned_path}")
        for case in CASES:
            run_id = f"{campaign}-{patient}-{case.case_id}"
            child_root = campaign_dir / "child_workspaces" / run_id
            seed_path = _configure_child_workspace(
                workspace_root=workspace,
                child_root=child_root,
                patient=patient,
                case=case,
                learned_path=learned_path,
            )
            plan = plan_tune_trees(
                workspace_root=child_root,
                cluster_name=cluster_name,
                patient_alias=patient,
                run_id=run_id,
            )
            child_runs.append(
                {
                    "run_id": run_id,
                    "patient": patient,
                    "case_id": case.case_id,
                    "seed_mode": case.seed_mode,
                    "diameter_scale": case.diameter_scale,
                    "tuning_model": case.tuning_model,
                    "allow_ordered_outlet_mapping": case.allow_ordered_outlet_mapping,
                    "learned_model": str(learned_path),
                    "seed_path": str(seed_path),
                    "child_workspace": str(child_root),
                    "plan_path": str(Path(plan.local_run_dir) / "execution_plan.yaml"),
                }
            )

    manifest = {
        "campaign_id": campaign,
        "workflow": "seed-sweep",
        "created_at": datetime.now(UTC).isoformat(),
        "cluster": cluster_name,
        "patients": selected_patients,
        "child_runs": child_runs,
    }
    _write_yaml(_manifest_path(campaign_dir), manifest)
    return manifest


def run_seed_sweep_campaign(
    *,
    workspace_root: str | Path,
    campaign_id: str,
    mode: ExecutionMode,
) -> dict[str, Any]:
    campaign_dir = Path(workspace_root).expanduser().resolve() / "runs" / "campaigns" / campaign_id
    manifest = _read_manifest(campaign_dir)
    results = []
    for child in manifest["child_runs"]:
        result = run_tune_trees(
            workspace_root=Path(child["child_workspace"]),
            cluster_name=str(manifest["cluster"]),
            patient_alias=str(child["patient"]),
            run_id=str(child["run_id"]),
            mode=mode,
        )
        results.append(
            {
                "run_id": result.run_id,
                "case_id": child["case_id"],
                "patient": child["patient"],
                "mode": result.mode.value,
                "submitted_job_id": result.submitted_job_id,
            }
        )
    manifest["last_run_results"] = results
    manifest["updated_at"] = datetime.now(UTC).isoformat()
    _write_yaml(_manifest_path(campaign_dir), manifest)
    return manifest


def _load_json_if_exists(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as stream:
        payload = json.load(stream)
    return payload if isinstance(payload, dict) else {}


def _iteration_result_path(run_dir: Path, name: str) -> Path:
    local_path = run_dir / "iterations" / "iter-01" / "results" / name
    if local_path.exists():
        return local_path
    return run_dir / "pulled_outputs" / "iterations" / "iter-01" / "results" / name


def summarize_seed_sweep_campaign(
    *,
    workspace_root: str | Path,
    campaign_id: str,
) -> list[dict[str, Any]]:
    campaign_dir = Path(workspace_root).expanduser().resolve() / "runs" / "campaigns" / campaign_id
    manifest = _read_manifest(campaign_dir)
    rows: list[dict[str, Any]] = []
    for child in manifest["child_runs"]:
        run_dir = Path(child["child_workspace"]) / "runs" / child["run_id"]
        metrics = _load_json_if_exists(_iteration_result_path(run_dir, "iteration_metrics.json"))
        decision = _load_json_if_exists(_iteration_result_path(run_dir, "iteration_decision.json"))
        rri_metrics = _load_json_if_exists(
            Path(child["child_workspace"]) / "prepared_inputs" / "rri_seed_metrics.json"
        )
        rows.append(
            {
                "campaign_id": campaign_id,
                "run_id": child["run_id"],
                "patient": child["patient"],
                "case_id": child["case_id"],
                "seed_mode": child["seed_mode"],
                "diameter_scale": child["diameter_scale"],
                "tuning_model": child.get("tuning_model"),
                "decision": decision.get("decision"),
                "mpa_sys": metrics.get("mpa_sys"),
                "mpa_dia": metrics.get("mpa_dia"),
                "mpa_mean": metrics.get("mpa_mean"),
                "rpa_split": metrics.get("rpa_split"),
                "preop_terminal_state": metrics.get("preop_terminal_state"),
                "learned_reference_method": rri_metrics.get("method"),
            }
        )

    summary_json = campaign_dir / "seed_sweep_summary.json"
    summary_csv = campaign_dir / "seed_sweep_summary.csv"
    summary_json.write_text(json.dumps(rows, indent=2, sort_keys=True), encoding="utf-8")
    with summary_csv.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0].keys()) if rows else [])
        if rows:
            writer.writeheader()
            writer.writerows(rows)
    return rows


def write_seed_sweep_slides(
    *,
    workspace_root: str | Path,
    campaign_id: str,
) -> Path:
    campaign_dir = Path(workspace_root).expanduser().resolve() / "runs" / "campaigns" / campaign_id
    rows = summarize_seed_sweep_campaign(workspace_root=workspace_root, campaign_id=campaign_id)
    out = campaign_dir / "seed_sweep_comparison.pptx"
    slide_text = [
        "Learned 0D Seed Sweep",
        f"Campaign: {campaign_id}",
        "",
        "patient | case | decision | MPA mean | RPA split",
    ]
    for row in rows:
        slide_text.append(
            f"{row['patient']} | {row['case_id']} | {row.get('decision') or 'pending'} | "
            f"{row.get('mpa_mean') or ''} | {row.get('rpa_split') or ''}"
        )
    _write_minimal_pptx(out, "\n".join(slide_text))
    return out


def _write_minimal_pptx(path: Path, text: str) -> None:
    escaped = (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("\n", "</a:t></a:r><a:br/><a:r><a:t>")
    )
    files = {
        "[Content_Types].xml": """<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/ppt/presentation.xml" ContentType="application/vnd.openxmlformats-officedocument.presentationml.presentation.main+xml"/><Override PartName="/ppt/slides/slide1.xml" ContentType="application/vnd.openxmlformats-officedocument.presentationml.slide+xml"/></Types>""",
        "_rels/.rels": """<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="ppt/presentation.xml"/></Relationships>""",
        "ppt/_rels/presentation.xml.rels": """<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/slide" Target="slides/slide1.xml"/></Relationships>""",
        "ppt/presentation.xml": """<?xml version="1.0" encoding="UTF-8" standalone="yes"?><p:presentation xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships" xmlns:p="http://schemas.openxmlformats.org/presentationml/2006/main"><p:sldIdLst><p:sldId id="256" r:id="rId1"/></p:sldIdLst><p:sldSz cx="12192000" cy="7620000" type="screen16x10"/></p:presentation>""",
        "ppt/slides/slide1.xml": f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?><p:sld xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main" xmlns:p="http://schemas.openxmlformats.org/presentationml/2006/main"><p:cSld><p:spTree><p:nvGrpSpPr><p:cNvPr id="1" name=""/><p:cNvGrpSpPr/><p:nvPr/></p:nvGrpSpPr><p:grpSpPr/><p:sp><p:nvSpPr><p:cNvPr id="2" name="Summary"/><p:cNvSpPr/><p:nvPr/></p:nvSpPr><p:spPr><a:xfrm><a:off x="685800" y="685800"/><a:ext cx="10668000" cy="6096000"/></a:xfrm></p:spPr><p:txBody><a:bodyPr/><a:lstStyle/><a:p><a:r><a:rPr lang="en-US" sz="2200"/><a:t>{escaped}</a:t></a:r></a:p></p:txBody></p:sp></p:spTree></p:cSld></p:sld>""",
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, content in files.items():
            archive.writestr(name, content)
