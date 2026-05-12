"""Remote postprocess orchestration for selected preop and postop stages."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path, PurePosixPath
import json

import yaml

from svztagent.config.load import detect_workspace_root, load_workspace_config, resolve_cluster
from svztagent.core.errors import ConfigError
from svztagent.core.manifest import read_manifest, record_postprocess_submission, write_manifest
from svztagent.core.paths import build_iteration_local_paths, build_local_run_paths, iteration_dir_name, validate_run_id
from svztagent.hpc.interfaces import (
    ExecutionMode,
    FileTransferAdapter,
    RemoteExecAdapter,
    SchedulerAdapter,
    SyncDirection,
)
from svztagent.workflows.tune_trees import _build_default_adapters


@dataclass(frozen=True)
class PostprocessSubmissionResult:
    run_id: str
    stage: str
    source_preop_iteration: int
    remote_results_dir: str
    remote_job_script_path: str
    local_job_script_path: Path
    submitted_job_id: str
    command_previews: list[list[str]]


def _load_stage_target_payload(
    workspace_root: Path,
    *,
    patient_alias: str,
    stage: str,
) -> dict[str, float] | None:
    path = workspace_root / "config" / "clinical_targets.yaml"
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as stream:
        payload = yaml.safe_load(stream) or {}
    if not isinstance(payload, dict):
        return None
    stage_payload = payload.get("clinical_targets", {}).get(stage, {}).get("patients", {})
    patient_payload = stage_payload.get(patient_alias)
    if not isinstance(patient_payload, dict):
        return None
    mpa_pressure = patient_payload.get("mpa_pressure")
    rpa_split = patient_payload.get("rpa_split")
    if not isinstance(mpa_pressure, list) or len(mpa_pressure) < 3 or rpa_split is None:
        return None
    return {
        "mpa_p": [float(mpa_pressure[0]), float(mpa_pressure[1]), float(mpa_pressure[2])],
        "rpa_split": float(rpa_split),
    }


def _python_bootstrap(config) -> str:
    hooks = "\n".join(config.defaults.execution.env_activation_hooks)
    python_executable = config.defaults.execution.python_executable or "python3"
    return f"""PYTHON_CANDIDATE="{python_executable}"
{hooks}
if [ -z "${{PYTHON_CANDIDATE}}" ]; then
  PYTHON_CANDIDATE="python3"
fi
if [ "${{PYTHON_CANDIDATE#*/}}" != "${{PYTHON_CANDIDATE}}" ]; then
  PYTHON_BIN="${{PYTHON_CANDIDATE}}"
else
  PYTHON_BIN="$(command -v "${{PYTHON_CANDIDATE}}" || command -v python3 || command -v python)"
fi
if [ -z "${{PYTHON_BIN}}" ]; then
  echo "[svzt] error: no Python interpreter found on PATH" >&2
  exit 1
fi
"""


def _render_postprocess_script(
    *,
    config,
    cluster,
    simulation_dir: str,
    output_dir: str,
    centerline: str,
    svslicer_path: str,
    stage: str,
    clinical_targets_payload: dict[str, float] | None,
    fallback_clinical_targets_csv: str | None = None,
    inflow_csv: str | None = None,
) -> str:
    clinical_targets_expr = (
        json.dumps(clinical_targets_payload, sort_keys=True)
        if clinical_targets_payload is not None
        else ("None" if fallback_clinical_targets_csv is None else json.dumps(fallback_clinical_targets_csv))
    )
    inflow_expr = "None" if inflow_csv is None else json.dumps(inflow_csv)
    return f"""#!/usr/bin/env bash
set -euo pipefail

{_python_bootstrap(config)}

mkdir -p {json.dumps(output_dir)}

"${{PYTHON_BIN}}" - <<'PY'
from pathlib import Path
import json

from svzerodtrees.post_processing import run_pulmonary_threed_postprocess_suite

result = run_pulmonary_threed_postprocess_suite(
    simulation_dir={json.dumps(simulation_dir)},
    output_dir={json.dumps(output_dir)},
    centerline={json.dumps(centerline)},
    stage={json.dumps(stage)},
    svslicer_path={json.dumps(svslicer_path)},
    clinical_targets={clinical_targets_expr},
    inflow_csv={inflow_expr},
)
Path({json.dumps(str(PurePosixPath(output_dir) / "postprocess_submission.json"))}).write_text(
    json.dumps(result, indent=2, sort_keys=True),
    encoding="utf-8",
)
PY
"""


def _selected_preop_local_paths(workspace_root: Path, run_id: str, iteration: int) -> dict[str, Path]:
    local_paths = build_local_run_paths(workspace_root, run_id)
    iter_paths = build_iteration_local_paths(local_paths, iteration)
    root = iter_paths["root"] / "postprocess"
    return {
        "root": root,
        "inputs": root / "inputs",
        "logs": root / "logs",
        "job_script": root / "run_postprocess.sh",
    }


def _selected_preop_remote_layout(remote_run_dir: str, iteration: int) -> dict[str, str]:
    remote_root = str(PurePosixPath(remote_run_dir) / "iterations" / iteration_dir_name(iteration) / "postprocess")
    return {
        "remote_root": remote_root,
        "remote_inputs_dir": str(PurePosixPath(remote_root) / "inputs"),
        "remote_logs_dir": str(PurePosixPath(remote_root) / "logs"),
        "remote_job_script_path": str(PurePosixPath(remote_root) / "run_postprocess.sh"),
        "remote_results_dir": str(
            PurePosixPath(remote_run_dir) / "iterations" / iteration_dir_name(iteration) / "results" / "postprocess"
        ),
    }


def submit_selected_preop_postprocess(
    *,
    workspace_root: str | Path,
    run_id: str,
    iteration: int,
    transfer_adapter: FileTransferAdapter | None = None,
    scheduler_adapter: SchedulerAdapter | None = None,
    remote_exec_adapter: RemoteExecAdapter | None = None,
) -> PostprocessSubmissionResult:
    root = detect_workspace_root(workspace_root)
    validated_run_id = validate_run_id(run_id)
    local_paths = build_local_run_paths(root, validated_run_id)
    manifest = read_manifest(local_paths.manifest)
    config = load_workspace_config(root)
    cluster = resolve_cluster(config, str(manifest.cluster.get("name")))
    if cluster.executables.svslicer_path is None:
        raise ConfigError("cluster executables.svslicer_path is required for postprocessing")

    remote_run_dir = manifest.execution.remote_run_dir or manifest.remote.get("remote_run_dir")
    if not remote_run_dir:
        raise ConfigError(f"run '{validated_run_id}' is missing remote_run_dir")
    remote_layout = _selected_preop_remote_layout(str(remote_run_dir), iteration)
    local_layout = _selected_preop_local_paths(root, validated_run_id, iteration)
    for key in ("root", "inputs", "logs"):
        local_layout[key].mkdir(parents=True, exist_ok=True)

    patient_alias = str(manifest.patient.get("alias"))
    clinical_targets_payload = _load_stage_target_payload(root, patient_alias=patient_alias, stage="preop")
    svzerodtrees_paths = manifest.remote.get("svzerodtrees_paths", {})
    fallback_csv = svzerodtrees_paths.get("clinical_targets")
    centerline = svzerodtrees_paths.get("centerlines")
    inflow_csv = svzerodtrees_paths.get("inflow")
    if not centerline:
        raise ConfigError("manifest remote.svzerodtrees_paths.centerlines is required for postprocessing")

    simulation_dir = str(PurePosixPath(remote_layout["remote_results_dir"]).parent.parent / "preop")
    script_body = _render_postprocess_script(
        config=config,
        cluster=cluster,
        simulation_dir=simulation_dir,
        output_dir=remote_layout["remote_results_dir"],
        centerline=str(centerline),
        svslicer_path=cluster.executables.svslicer_path,
        stage="preop",
        clinical_targets_payload=clinical_targets_payload,
        fallback_clinical_targets_csv=str(fallback_csv) if fallback_csv else None,
        inflow_csv=str(inflow_csv) if inflow_csv else None,
    )
    local_layout["job_script"].write_text(script_body, encoding="utf-8")

    if transfer_adapter is None or scheduler_adapter is None or remote_exec_adapter is None:
        default_transfer, default_scheduler, default_remote = _build_default_adapters(
            cluster=cluster,
            config=config,
            run_id=validated_run_id,
            mode=ExecutionMode.EXECUTE,
        )
        transfer_adapter = transfer_adapter or default_transfer
        scheduler_adapter = scheduler_adapter or default_scheduler
        remote_exec_adapter = remote_exec_adapter or default_remote

    command_results = [
        transfer_adapter.ensure_remote_dir(remote_layout["remote_root"]),
        transfer_adapter.ensure_remote_dir(remote_layout["remote_inputs_dir"]),
        transfer_adapter.ensure_remote_dir(remote_layout["remote_logs_dir"]),
        transfer_adapter.ensure_remote_dir(remote_layout["remote_results_dir"]),
        transfer_adapter.sync(
            local_dir=str(local_layout["inputs"]),
            remote_dir=remote_layout["remote_inputs_dir"],
            include=config.defaults.rsync.include_patterns,
            exclude=config.defaults.rsync.exclude_patterns,
            direction=SyncDirection.PUSH,
        ),
        transfer_adapter.push(str(local_layout["job_script"]), remote_layout["remote_job_script_path"]),
    ]
    submit_result = scheduler_adapter.submit(remote_layout["remote_job_script_path"])
    command_results.append(submit_result.command)

    manifest = read_manifest(local_paths.manifest)
    manifest = record_postprocess_submission(
        manifest,
        field_name="selected_preop_postprocess",
        stage="selected_preop",
        source_preop_iteration=iteration,
        local_dir=str(local_layout["root"]),
        remote_dir=remote_layout["remote_results_dir"],
        local_job_script_path=str(local_layout["job_script"]),
        remote_job_script_path=remote_layout["remote_job_script_path"],
        scheduler_job_id=submit_result.job_id,
        note="Selected preop postprocess submitted",
    )
    write_manifest(manifest, local_paths.manifest)

    return PostprocessSubmissionResult(
        run_id=validated_run_id,
        stage="selected_preop",
        source_preop_iteration=iteration,
        remote_results_dir=remote_layout["remote_results_dir"],
        remote_job_script_path=remote_layout["remote_job_script_path"],
        local_job_script_path=local_layout["job_script"],
        submitted_job_id=submit_result.job_id,
        command_previews=[result.argv for result in command_results],
    )
