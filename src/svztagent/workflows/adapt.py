"""Explicit adaptation workflow orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path, PurePosixPath
import json

import yaml

from svztagent.config.load import detect_workspace_root, load_workspace_config, resolve_cluster
from svztagent.core.errors import ConfigError
from svztagent.core.manifest import (
    read_manifest,
    record_adaptation_submission,
    record_paraview_viz_submission,
    record_submission,
    write_manifest,
)
from svztagent.core.paths import (
    LocalRunPaths,
    build_local_run_paths,
    iteration_dir_name,
    validate_remote_write_path,
    validate_run_id,
)
from svztagent.core.plan import (
    ExecutionPlan,
    PlanStep,
    StepCategory,
    utc_now_iso,
    write_plan_json,
    write_plan_yaml,
)
from svztagent.core.plan_validate import assert_valid_execution_plan
from svztagent.hpc.interfaces import (
    ExecutionMode,
    FileTransferAdapter,
    RemoteExecAdapter,
    SchedulerAdapter,
    SyncDirection,
)
from svztagent.hpc.slurm import SlurmSchedulerAdapter, SlurmSubmitOptions
from svztagent.workflows.paraview_viz import (
    _paraview_skip_reason,
    _prepare_adaptation_paraview_viz_job,
)
from svztagent.workflows.postprocess import (
    _python_bootstrap,
    _resolve_resistance_map_camera,
    _stacked_centerline_timeseries_python_source,
)
from svztagent.workflows.tune_trees import (
    _apply_cluster_svzerodsolver_build_dir,
    _build_default_adapters,
    _resolve_cluster_svfsiplus_path,
)


@dataclass(frozen=True)
class AdaptRunResult:
    run_id: str
    model: str
    parameter_set: str
    mode: ExecutionMode
    source_preop_iteration: int
    plan_path: Path
    remote_adaptation_dir: str
    remote_job_script_path: str
    local_job_script_path: Path
    submitted_job_id: str
    inflow_source_path: str
    command_previews: list[list[str]]


def _resolve_cluster_for_manifest(workspace_root: Path, manifest):
    cluster_name = manifest.cluster.get("name")
    if not cluster_name:
        raise ConfigError("manifest cluster.name is required")
    config = load_workspace_config(workspace_root)
    return config, resolve_cluster(config, cluster_name)


def _adaptation_local_paths(
    local_paths: LocalRunPaths,
    *,
    iteration: int,
    model: str,
) -> dict[str, Path]:
    root = (
        local_paths.run_dir
        / "adaptation"
        / f"from-{iteration_dir_name(iteration)}"
        / model.lower()
    )
    return {
        "root": root,
        "inputs": root / "inputs",
        "logs": root / "logs",
        "results": root / "results",
        "job_script": root / "run_adapt.sh",
        "plan_json": root / "adapt_execution_plan.json",
        "plan_yaml": root / "adapt_execution_plan.yaml",
    }


def _adaptation_remote_layout(
    *,
    remote_run_dir: str,
    iteration: int,
    model: str,
) -> dict[str, str]:
    remote_adaptation_dir = str(
        PurePosixPath(remote_run_dir)
        / "adaptation"
        / f"from-{iteration_dir_name(iteration)}"
        / model.lower()
    )
    return {
        "remote_adaptation_dir": remote_adaptation_dir,
        "remote_inputs_dir": str(PurePosixPath(remote_adaptation_dir) / "inputs"),
        "remote_logs_dir": str(PurePosixPath(remote_adaptation_dir) / "logs"),
        "remote_results_dir": str(PurePosixPath(remote_adaptation_dir) / "results"),
        "remote_job_script_path": str(PurePosixPath(remote_adaptation_dir) / "run_adapt.sh"),
        "remote_simulation_dir": str(PurePosixPath(remote_adaptation_dir) / "simulation"),
    }


def _fingerprint_inflow(path: str) -> tuple[str | None, dict[str, str | int | float | bool | None]]:
    candidate = Path(path)
    metadata: dict[str, str | int | float | bool | None] = {
        "exists_locally": candidate.exists(),
    }
    if not candidate.exists() or not candidate.is_file():
        return None, metadata

    digest = sha256()
    size = 0
    with candidate.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
            size += len(chunk)
    metadata["size_bytes"] = size
    return digest.hexdigest(), metadata


def _resolve_adaptation_defaults(manifest) -> dict:
    payload = manifest.remote.get("adaptation_defaults")
    if not isinstance(payload, dict):
        raise ConfigError("manifest remote.adaptation_defaults is required for adaptation workflow")
    return payload


def _resolve_parameter_payload(
    manifest,
    *,
    model: str,
    parameter_set: str | None,
) -> tuple[str, dict]:
    adaptation_defaults = _resolve_adaptation_defaults(manifest)
    default_model = str(adaptation_defaults.get("default_model") or "M2").upper()
    resolved_model = model.upper()
    if resolved_model not in {"M1", "M2", "M3"}:
        raise ConfigError(f"unsupported adaptation model '{model}'")

    models_payload = adaptation_defaults.get("models") or {}
    base_model_payload = dict(models_payload.get(resolved_model.lower()) or {})
    resolved_parameter_set = parameter_set or "default"
    if resolved_parameter_set != "default":
        parameter_sets = adaptation_defaults.get("parameter_sets") or {}
        if resolved_parameter_set not in parameter_sets:
            raise ConfigError(
                f"adaptation parameter set '{resolved_parameter_set}' is not defined"
            )
        override_payload = dict(
            (parameter_sets.get(resolved_parameter_set) or {}).get(resolved_model.lower()) or {}
        )
        base_model_payload.update(override_payload)
    elif not base_model_payload and resolved_model == default_model:
        base_model_payload = dict((models_payload.get(default_model.lower()) or {}))

    if not base_model_payload:
        raise ConfigError(f"adaptation model '{resolved_model}' has no configuration payload")
    return resolved_parameter_set, base_model_payload


def _build_adapt_plan(
    *,
    manifest,
    cluster,
    local_paths: LocalRunPaths,
    adaptation_paths: dict[str, Path],
    remote_layout: dict[str, str],
    model: str,
    parameter_set: str,
    inflow_source_path: str,
    inflow_fingerprint: str | None,
) -> ExecutionPlan:
    selected = manifest.converged_preop_iteration
    postop_run = manifest.postop_run
    if selected is None:
        raise ConfigError("converged_preop_iteration is required for adaptation workflow")
    if postop_run is None or not postop_run.remote_dir:
        raise ConfigError("completed postop_run is required for adaptation workflow")

    steps = [
        PlanStep(
            step_id="a01_validate_preop_postop_prereqs",
            name="validate_preop_postop_prereqs",
            category=StepCategory.RESOLVE_PATHS,
            description="Validate selected converged preop iteration and explicit postop submission.",
            outputs={
                "source_preop_iteration": str(selected.iteration),
                "source_postop_dir": postop_run.remote_dir,
            },
            remote_paths={
                "read": [
                    selected.remote_preop_dir,
                    selected.remote_tuned_zerod_config,
                    selected.remote_canonical_coupler,
                    postop_run.remote_dir,
                ],
                "write": [],
            },
            command_preview=["svzt", "run", "adapt", "--run-id", manifest.run_id, "--model", model],
        ),
        PlanStep(
            step_id="a02_validate_postop_target_payload",
            name="validate_postop_target_payload",
            category=StepCategory.RESOLVE_PATHS,
            description="Validate stage='postop' clinical target payload and adaptation defaults.",
            outputs={"target_stage": "postop", "parameter_set": parameter_set},
            dependencies=["a01_validate_preop_postop_prereqs"],
            remote_paths={"read": [], "write": []},
            command_preview=["validate_postop_targets", "--run-id", manifest.run_id],
        ),
        PlanStep(
            step_id="a03_resolve_source_inflow",
            name="resolve_source_inflow",
            category=StepCategory.RESOLVE_PATHS,
            description="Resolve the preop/postop 3D source-of-truth patient inflow waveform.",
            outputs={
                "inflow_source_path": inflow_source_path,
                "inflow_fingerprint": inflow_fingerprint or "<resolved-on-cluster>",
            },
            dependencies=["a02_validate_postop_target_payload"],
            remote_paths={"read": [inflow_source_path], "write": []},
            safety_notes=["Adapted 3D inflow must match the selected preop/postop 3D inflow contract exactly."],
            command_preview=["resolve_inflow", inflow_source_path],
        ),
        PlanStep(
            step_id="a04_stage_adaptation_inputs",
            name="stage_adaptation_inputs",
            category=StepCategory.STAGE_INPUTS,
            description="Stage adaptation submission metadata locally.",
            outputs={"local_inputs_dir": str(adaptation_paths["inputs"])},
            dependencies=["a03_resolve_source_inflow"],
            local_paths={"inputs": str(adaptation_paths["inputs"])},
            command_preview=["mkdir", "-p", str(adaptation_paths["inputs"])],
        ),
        PlanStep(
            step_id="a05_define_remote_adaptation_destination",
            name="define_remote_adaptation_destination",
            category=StepCategory.PUSH_TO_CLUSTER,
            description="Define run-scoped remote adaptation directory under runs_root.",
            outputs={"remote_adaptation_dir": remote_layout["remote_adaptation_dir"]},
            dependencies=["a04_stage_adaptation_inputs"],
            remote_paths={
                "read": [],
                "write": [
                    remote_layout["remote_adaptation_dir"],
                    remote_layout["remote_inputs_dir"],
                    remote_layout["remote_logs_dir"],
                    remote_layout["remote_results_dir"],
                    remote_layout["remote_simulation_dir"],
                ],
            },
            safety_notes=["All remote writes stay under configured runs_root."],
            command_preview=["rsync", "-az", str(adaptation_paths["inputs"]) + "/", remote_layout["remote_inputs_dir"]],
        ),
        PlanStep(
            step_id="a06_render_adaptation_job_script",
            name="render_adaptation_job_script",
            category=StepCategory.GENERATE_JOB_SCRIPT,
            description="Render explicit adaptation manager script.",
            outputs={"remote_script_path": remote_layout["remote_job_script_path"]},
            dependencies=["a05_define_remote_adaptation_destination"],
            remote_paths={
                "read": [
                    selected.remote_preop_dir,
                    selected.remote_tuned_zerod_config,
                    postop_run.remote_dir,
                    inflow_source_path,
                ],
                "write": [remote_layout["remote_job_script_path"]],
            },
            command_preview=["generate_adapt_job_script", "--output", remote_layout["remote_job_script_path"]],
        ),
        PlanStep(
            step_id="a07_submit_adaptation_job",
            name="submit_adaptation_job",
            category=StepCategory.SUBMIT_JOB,
            description="Submit the explicit adaptation job script to the scheduler.",
            outputs={"adaptation_job_id": "<job_id>"},
            dependencies=["a06_render_adaptation_job_script"],
            remote_paths={
                "read": [remote_layout["remote_job_script_path"]],
                "write": [remote_layout["remote_logs_dir"], remote_layout["remote_results_dir"]],
            },
            command_preview=["sbatch", "--parsable", remote_layout["remote_job_script_path"]],
        ),
        PlanStep(
            step_id="a08_record_adaptation_submission",
            name="record_adaptation_submission",
            category=StepCategory.FINALIZE_MANIFEST,
            description="Record adaptation plan/submission metadata in the run manifest.",
            outputs={"manifest_path": manifest.local_paths.manifest},
            dependencies=["a07_submit_adaptation_job"],
            local_paths={"manifest": manifest.local_paths.manifest},
            command_preview=["svzt", "run", "adapt", "--run-id", manifest.run_id, "--model", model],
        ),
    ]

    plan = ExecutionPlan(
        plan_id=f"plan-{manifest.run_id}-adapt-{model.lower()}-{iteration_dir_name(selected.iteration)}",
        workflow_name="adapt",
        run_id=manifest.run_id,
        cluster=cluster.name,
        patient=str(manifest.patient.get("alias")),
        created_at=utc_now_iso(),
        manifest_path=str(local_paths.manifest),
        local_run_dir=str(local_paths.run_dir),
        remote_run_dir=str(manifest.execution.remote_run_dir or manifest.remote.get("remote_run_dir")),
        steps=steps,
        summary={
            "source_preop_iteration": selected.iteration,
            "source_postop_job_id": postop_run.postop_job_id,
            "model": model,
            "parameter_set": parameter_set,
            "inflow_source_path": inflow_source_path,
            "remote_adaptation_dir": remote_layout["remote_adaptation_dir"],
        },
    )
    validation = assert_valid_execution_plan(
        plan=plan,
        runs_root=cluster.remote_roots.runs_root,
    )
    return plan.model_copy(update={"validation_results": validation})


def _render_adapt_job_script(
    *,
    config,
    manifest,
    cluster,
    remote_layout: dict[str, str],
    model: str,
    parameter_set: str,
    adaptation_mode: str,
    parameter_payload: dict,
    paraview_job=None,
    paraview_skip_reason: str | None = None,
) -> str:
    selected = manifest.converged_preop_iteration
    postop_run = manifest.postop_run
    if selected is None or postop_run is None:
        raise ConfigError("adaptation job script requires selected preop and postop run")

    svzerodtrees_paths = manifest.remote.get("svzerodtrees_paths", {})
    threed_config = dict(manifest.remote.get("threed_defaults", {}))
    adaptation_defaults = _resolve_adaptation_defaults(manifest)
    territory_scheme = str(adaptation_defaults.get("territory_scheme") or "lpa_rpa")
    target_stage = str(adaptation_defaults.get("target_stage") or "postop")
    mesh_scale_factor = float(manifest.patient.get("mesh_scale_factor") or 1.0)
    inflow_source_path = str(svzerodtrees_paths.get("inflow") or "").strip()
    if not inflow_source_path:
        raise ConfigError("manifest remote.svzerodtrees_paths.inflow is required for adaptation")
    postop_mesh = svzerodtrees_paths.get("postop_mesh_complete")
    if not postop_mesh:
        raise ConfigError("postop mesh-complete path is required for adaptation")
    clinical_targets = svzerodtrees_paths.get("clinical_targets")
    centerline = svzerodtrees_paths.get("centerlines")
    if not clinical_targets:
        raise ConfigError("clinical target path is required for adaptation")
    if not centerline:
        raise ConfigError("centerline path is required for adaptation")
    patient_alias = str(manifest.patient.get("alias"))
    camera_offset_dir, camera_view_up = _resolve_resistance_map_camera(
        config,
        patient_alias=patient_alias,
    )
    camera_offset_expr = "None" if camera_offset_dir is None else repr(camera_offset_dir)
    camera_view_up_expr = "None" if camera_view_up is None else repr(camera_view_up)

    scheduler_defaults = manifest.remote.get("scheduler_defaults", {})
    account = str(scheduler_defaults.get("account") or "").strip() or None
    partition = str(scheduler_defaults.get("partition") or "amarsden")
    svslicer_path = cluster.executables.svslicer_path
    if svslicer_path is None:
        raise ConfigError("cluster executables.svslicer_path is required for adaptation postprocessing")
    bootstrap = _python_bootstrap(config)
    svfsiplus_path = _resolve_cluster_svfsiplus_path(
        cluster_name=str(cluster.name),
        configured_path=cluster.executables.svfsiplus_path,
    )
    threed_config = _apply_cluster_svzerodsolver_build_dir(
        cluster_name=str(cluster.name),
        configured_path=cluster.executables.svzerodsolver_build_dir,
        threed_config=threed_config,
    )
    postprocess_workers = max(int(threed_config.get("procs_per_node", 4) or 4), 1)
    adapted_results_dir = PurePosixPath(remote_layout["remote_results_dir"])
    baseline_postprocess_dir = adapted_results_dir / "baseline_postop_postprocess"
    adapted_postprocess_dir = adapted_results_dir / "adapted_postprocess"
    paraview_stage = paraview_job.stage if paraview_job is not None else f"adaptation-{model.lower()}"
    paraview_script_json = (
        json.dumps(paraview_job.pvpython_script_content) if paraview_job is not None else "null"
    )
    paraview_slurm_json = (
        json.dumps(paraview_job.slurm_script_content) if paraview_job is not None else "null"
    )
    paraview_root = paraview_job.remote_root if paraview_job is not None else ""
    paraview_logs_dir = paraview_job.remote_logs_dir if paraview_job is not None else ""
    paraview_script_path = paraview_job.remote_pvpython_script if paraview_job is not None else ""
    paraview_slurm_path = paraview_job.remote_slurm_script if paraview_job is not None else ""
    paraview_output_dir = paraview_job.remote_output_dir if paraview_job is not None else ""
    paraview_submission_metadata_path = (
        paraview_job.remote_submission_metadata_path if paraview_job is not None else ""
    )

    return f"""#!/usr/bin/env bash
#SBATCH --job-name=adapt-{manifest.run_id}
#SBATCH --partition={partition}
#SBATCH --chdir={remote_layout["remote_adaptation_dir"]}
#SBATCH --output={remote_layout["remote_logs_dir"]}/slurm-%j.out
#SBATCH --error={remote_layout["remote_logs_dir"]}/slurm-%j.err
#SBATCH --time={int(threed_config.get("hours", 20)) + 8}:00:00
#SBATCH --nodes=1
#SBATCH --cpus-per-task=4
#SBATCH --mem={int(threed_config.get("memory", 16))}G
{"#SBATCH --account=" + account if account else ""}
set -euo pipefail

mkdir -p "{remote_layout["remote_inputs_dir"]}" "{remote_layout["remote_results_dir"]}" "{remote_layout["remote_logs_dir"]}" "{remote_layout["remote_simulation_dir"]}"

{bootstrap}

"${{PYTHON_BIN}}" - <<'PY'
from pathlib import Path
import copy
import csv
import glob
import json
import os
import shutil
import subprocess
import time

import numpy as np
import pandas as pd

from svzerodtrees.io import ConfigHandler
from svzerodtrees.adaptation.workflow import run_structured_tree_adaptation
from svzerodtrees.post_processing.pulmonary_threed_suite import run_pulmonary_threed_postprocess_suite
from svzerodtrees.simulation import Simulation
from svzerodtrees.simulation.simulation_directory import SimulationDirectory
from svzerodtrees.tune_bcs.clinical_targets import ClinicalTargets

{_stacked_centerline_timeseries_python_source()}

remote_adaptation_dir = Path({json.dumps(remote_layout["remote_adaptation_dir"])})
remote_results_dir = Path({json.dumps(remote_layout["remote_results_dir"])})
simulation_dir = Path({json.dumps(remote_layout["remote_simulation_dir"])})
preop_dir = Path({json.dumps(selected.remote_preop_dir)})
postop_root = Path({json.dumps(postop_run.remote_dir)})
postop_simulation_dir = postop_root / "simulation"
postop_results_dir = postop_root / "results"
adapted_coupler_path = simulation_dir / "svzerod_3Dcoupling.json"
adapted_zerod_config_path = simulation_dir / "svzerod_config.json"
clinical_targets_csv = Path({json.dumps(str(clinical_targets))})
centerline = Path({json.dumps(str(centerline))})
svslicer_path = Path({json.dumps(str(svslicer_path))})
postop_mesh_complete = Path({json.dumps(str(postop_mesh))})
reduced_order_pa = Path({json.dumps(selected.remote_tuned_zerod_config)})
tree_params_csv = preop_dir.parent / "results" / "optimized_params.csv"
parameter_payload = json.loads({json.dumps(json.dumps(parameter_payload, sort_keys=True))})
model = {json.dumps(model)}
parameter_set = {json.dumps(parameter_set)}
adaptation_mode = {json.dumps(adaptation_mode)}
territory_scheme = {json.dumps(territory_scheme)}
target_stage = {json.dumps(target_stage)}
inflow_source_path = Path({json.dumps(inflow_source_path)})
mesh_scale_factor = float({json.dumps(str(mesh_scale_factor))})
threed_config = json.loads({json.dumps(json.dumps(threed_config, sort_keys=True))})
partition = {json.dumps(partition)}
account = {json.dumps(account) if account is not None else "None"}
cluster_svfsiplus_path = {json.dumps(svfsiplus_path)}
solver_execution = dict(threed_config.get("execution", {{}}))
solver_execution["mode"] = "slurm"
solver_execution["executable"] = cluster_svfsiplus_path
solver_execution["submit_command"] = "bash"
solver_execution["svfsiplus_path"] = cluster_svfsiplus_path
threed_config["execution"] = solver_execution
poll_seconds = int(threed_config.get("wait_poll_seconds", 30))
cmm_timeout_seconds = max(int(threed_config.get("hours", 20)) * 3600 + 7200, int(threed_config.get("wait_timeout_seconds", 14400)))
manager_log_path = remote_adaptation_dir / "logs" / "adaptation_manager_log.jsonl"
status_path = remote_results_dir / "adaptation_status.json"
paraview_stage = {json.dumps(paraview_stage)}
paraview_root = Path({json.dumps(paraview_root)})
paraview_logs_dir = Path({json.dumps(paraview_logs_dir)})
paraview_script_path = Path({json.dumps(paraview_script_path)})
paraview_slurm_path = Path({json.dumps(paraview_slurm_path)})
paraview_output_dir = Path({json.dumps(paraview_output_dir)})
paraview_submission_metadata_path = Path({json.dumps(paraview_submission_metadata_path)})
paraview_enabled = {repr(paraview_job is not None)}
paraview_skip_reason = {repr(paraview_skip_reason)}
paraview_job_id = None
paraview_script_content = {paraview_script_json}
paraview_slurm_content = {paraview_slurm_json}

if not inflow_source_path.exists():
    raise RuntimeError(f"adaptation requires source-of-truth inflow file, missing: {{inflow_source_path}}")
if not postop_simulation_dir.exists():
    raise RuntimeError(f"adaptation requires completed postop simulation directory: {{postop_simulation_dir}}")
if not tree_params_csv.exists():
    raise RuntimeError(f"adaptation requires optimized_params.csv from selected preop iteration: {{tree_params_csv}}")


def _safe_remove(path: Path) -> None:
    if path.is_symlink() or path.is_file():
        path.unlink()
    elif path.is_dir():
        shutil.rmtree(path)


def _latest_result_vtu(root: Path) -> Path | None:
    candidates = [Path(path) for path in glob.glob(str(root / "*-procs" / "result_*.vtu"))]
    if not candidates:
        candidates = [Path(path) for path in glob.glob(str(root / "result_*.vtu"))]
    if not candidates:
        return None
    return max(candidates, key=lambda path: (path.stat().st_mtime, path.name))


def _nested_sbatch_env() -> dict:
    env = os.environ.copy()
    for name in (
        "SBATCH_CPUS_PER_TASK",
        "SBATCH_MEM",
        "SBATCH_MEM_PER_CPU",
        "SBATCH_MEM_PER_NODE",
        "SBATCH_NTASKS",
        "SBATCH_NTASKS_PER_NODE",
        "SBATCH_NODES",
        "SLURM_CPUS_ON_NODE",
        "SLURM_CPUS_PER_TASK",
        "SLURM_JOB_CPUS_PER_NODE",
        "SLURM_JOB_NUM_NODES",
        "SLURM_MEM_PER_CPU",
        "SLURM_MEM_PER_NODE",
        "SLURM_NNODES",
        "SLURM_NPROCS",
        "SLURM_NTASKS",
        "SLURM_NTASKS_PER_NODE",
        "SLURM_TASKS_PER_NODE",
        "SLURM_TRES_PER_TASK",
    ):
        env.pop(name, None)
    return env


if manager_log_path.exists():
    manager_log_path.unlink()
if status_path.exists():
    status_path.unlink()


def _record_event(step: str, status: str = "info", **payload) -> None:
    entry = {{
        "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "step": step,
        "status": status,
    }}
    if payload:
        entry.update(payload)
    with manager_log_path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(entry, sort_keys=True) + "\\n")
    status_payload = dict(entry)
    status_payload["log_path"] = str(manager_log_path)
    status_path.write_text(json.dumps(status_payload, indent=2, sort_keys=True), encoding="utf-8")
    print(f"[svzt] {{step}}: {{status}}", flush=True)


def _submit_job(script_path: Path) -> str:
    proc = subprocess.run(
        ["sbatch", "--parsable", "--chdir", str(script_path.parent), script_path.name],
        cwd=script_path.parent,
        capture_output=True,
        text=True,
        env=_nested_sbatch_env(),
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"sbatch failed rc={{proc.returncode}}: {{proc.stderr.strip()}}")
    stdout = proc.stdout.strip()
    if not stdout:
        raise RuntimeError("sbatch returned empty stdout")
    return stdout.split(";")[0].strip()


def _query_state(job_id: str):
    squeue = subprocess.run(
        ["squeue", "--job", job_id, "--noheader", "--format", "%T"],
        capture_output=True,
        text=True,
        check=False,
    )
    if squeue.returncode == 0:
        raw = squeue.stdout.strip().splitlines()
        if raw:
            return raw[0].strip().split()[0].strip().upper(), "squeue"
    sacct = subprocess.run(
        ["sacct", "-j", job_id, "--noheader", "--format", "State"],
        capture_output=True,
        text=True,
        check=False,
    )
    if sacct.returncode == 0:
        for line in sacct.stdout.splitlines():
            cleaned = line.strip()
            if cleaned:
                return cleaned.split()[0].split("+")[0].strip().upper(), "sacct"
    return None, "unknown"


def _wait_for_completion(job_id: str, poll_seconds: int, timeout_seconds: int, *, event_prefix: str):
    success_states = {{"COMPLETED"}}
    failure_states = {{"FAILED", "CANCELLED", "TIMEOUT", "PREEMPTED", "OUT_OF_MEMORY", "NODE_FAIL", "BOOT_FAIL", "DEADLINE"}}
    active_states = {{"PENDING", "RUNNING", "CONFIGURING", "COMPLETING", "SUSPENDED", "RESIZING", "REQUEUED", "REQUEUE_HOLD", "SIGNALING", "SPECIAL_EXIT", "STAGE_OUT", "STOPPED"}}
    start = time.monotonic()
    last_state = None
    while True:
        elapsed = int(time.monotonic() - start)
        if elapsed > timeout_seconds:
            _record_event(
                f"{{event_prefix}}_wait_timeout",
                "error",
                scheduler_job_id=job_id,
                timeout_seconds=timeout_seconds,
                last_state=last_state or "unknown",
            )
            return False, f"timeout after {{timeout_seconds}}s (last_state={{last_state or 'unknown'}})"
        state, source = _query_state(job_id)
        if state:
            if state != last_state:
                _record_event(
                    f"{{event_prefix}}_state",
                    "info",
                    scheduler_job_id=job_id,
                    scheduler_state=state,
                    scheduler_source=source,
                )
            last_state = state
            print(f"[svzt] job {{job_id}} state: {{state}} ({{source}})", flush=True)
            if state in success_states:
                return True, state
            if state in failure_states:
                _record_event(
                    f"{{event_prefix}}_terminal_failure",
                    "error",
                    scheduler_job_id=job_id,
                    scheduler_state=state,
                    scheduler_source=source,
                )
                return False, state
            if state not in active_states:
                _record_event(
                    f"{{event_prefix}}_unexpected_terminal_state",
                    "error",
                    scheduler_job_id=job_id,
                    scheduler_state=state,
                    scheduler_source=source,
                )
                return False, f"unexpected terminal scheduler state: {{state}}"
        time.sleep(max(poll_seconds, 5))


def _normalize_solver_runscript(script_path: Path, *, nodes: int, procs_per_node: int, memory_gb: int, hours: int, partition: str, account: str | None, mail_user: str | None, mail_types: list[str] | None, svfsiplus_path: str) -> None:
    stage_dir = script_path.parent.resolve()
    output_path = stage_dir / "svFlowSolver.o%j"
    error_path = stage_dir / "svFlowSolver.e%j"
    lines = script_path.read_text(encoding="utf-8", errors="replace").splitlines()
    body: list[str] = []
    strip_existing_launch_tail = False
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("#!/") or stripped.startswith("#SBATCH"):
            continue
        if "--mail-user" in stripped or "--mail-type" in stripped:
            continue
        if stripped.startswith("cd ") or stripped.startswith('if [ -n "${{SLURM_CPUS_PER_TASK:-}}" ]') or stripped.startswith("srun "):
            strip_existing_launch_tail = True
        if strip_existing_launch_tail:
            continue
        body.append(line)
    header = [
        "#!/usr/bin/env bash",
        "",
        "#SBATCH --job-name=svFlowSolver",
        f"#SBATCH --partition={{partition}}",
        f"#SBATCH --chdir={{stage_dir}}",
        f"#SBATCH --output={{output_path}}",
        f"#SBATCH --error={{error_path}}",
        f"#SBATCH --time={{hours}}:00:00",
        f"#SBATCH --nodes={{nodes}}",
        f"#SBATCH --ntasks-per-node={{procs_per_node}}",
        f"#SBATCH --mem={{memory_gb}}G",
    ]
    if account:
        header.insert(4, f"#SBATCH --account={{account}}")
    if mail_user:
        header.append(f"#SBATCH --mail-user={{mail_user}}")
        for mail_type in mail_types or ["begin", "end"]:
            header.append(f"#SBATCH --mail-type={{mail_type}}")
    total_tasks = nodes * procs_per_node
    rendered = "\\n".join(header) + "\\n\\n"
    if body:
        rendered += "\\n".join(body) + "\\n"
    rendered += f"cd {{stage_dir}}\\n"
    rendered += 'if [ -n "${{SLURM_CPUS_PER_TASK:-}}" ] && [ -n "${{SLURM_TRES_PER_TASK:-}}" ]; then\\n'
    rendered += '  case "${{SLURM_TRES_PER_TASK}}" in\\n'
    rendered += '    cpu=*)\\n'
    rendered += '      _svzt_tres_cpus="${{SLURM_TRES_PER_TASK#cpu=}}"\\n'
    rendered += '      _svzt_tres_cpus="${{_svzt_tres_cpus%%,*}}"\\n'
    rendered += '      if [ "${{SLURM_CPUS_PER_TASK}}" != "${{_svzt_tres_cpus}}" ]; then\\n'
    rendered += '        unset SLURM_TRES_PER_TASK\\n'
    rendered += '      fi\\n'
    rendered += '      unset _svzt_tres_cpus\\n'
    rendered += '      ;;\\n'
    rendered += '  esac\\n'
    rendered += 'fi\\n'
    rendered += f"srun -N {{nodes}} -n {{total_tasks}} {{svfsiplus_path}} svFSIplus.xml\\n"
    script_path.write_text(rendered, encoding="utf-8")
    script_path.chmod(0o755)


def _resolve_slurm_mail_user(threed_config: dict) -> str | None:
    execution = threed_config.get("execution") or {{}}
    if not isinstance(execution, dict):
        return None
    slurm = execution.get("slurm") or {{}}
    if not isinstance(slurm, dict):
        return None
    value = slurm.get("mail_user")
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _resolve_slurm_mail_types(threed_config: dict) -> list[str]:
    execution = threed_config.get("execution") or {{}}
    if not isinstance(execution, dict):
        return ["begin", "end"]
    slurm = execution.get("slurm") or {{}}
    if not isinstance(slurm, dict):
        return ["begin", "end"]
    value = slurm.get("mail_types", ["begin", "end"])
    if not isinstance(value, list):
        return ["begin", "end"]
    normalized = [str(item).strip() for item in value if str(item).strip()]
    return normalized or ["begin", "end"]


def _sync_source_of_truth_inflow(sim_dir: SimulationDirectory) -> None:
    inflow_helper = Simulation(
        path=str(remote_adaptation_dir),
        clinical_targets=str(clinical_targets_csv),
        preop_dir=str(preop_dir),
        postop_dir=str(postop_simulation_dir),
        adapted_dir=str(simulation_dir),
        zerod_config=str(reduced_order_pa),
        convert_to_cm=False,
        mesh_scale_factor=mesh_scale_factor,
        wall_model=str(threed_config.get("wall_model", "deformable")),
        elasticity_modulus=float(threed_config.get("elasticity_modulus", 5062674.563165)),
        poisson_ratio=float(threed_config.get("poisson_ratio", 0.5)),
        shell_thickness=float(threed_config.get("shell_thickness", 0.12)),
        prestress_file=None,
        prestress_file_path=None,
        tissue_support=threed_config.get("tissue_support"),
        solver_paths=threed_config.get("solver_paths"),
        inflow_path=str(inflow_source_path),
        execution_config=solver_execution,
    )
    full_inflow = getattr(inflow_helper, "inflow_3d", None)
    if full_inflow is None:
        raise RuntimeError("adapted simulation could not resolve full-scale 3D inflow")

    if sim_dir.zerod_config is None:
        raise RuntimeError("adapted simulation requires zerod_config to generate dirichlet inflow")
    sim_dir.zerod_config.set_inflow(full_inflow)
    sim_dir.zerod_config.inflows[full_inflow.name] = full_inflow
    sim_dir.zerod_config.to_json(sim_dir.zerod_config.path)

    if sim_dir.svzerod_3Dcoupling is not None:
        sim_dir.svzerod_3Dcoupling.set_inflow(full_inflow)
        sim_dir.svzerod_3Dcoupling.inflows[full_inflow.name] = full_inflow
        sim_dir.svzerod_3Dcoupling.to_json(sim_dir.svzerod_3Dcoupling.path)


def _pressure_metrics_from_csv(csv_path: Path) -> dict[str, float]:
    df = pd.read_csv(csv_path)
    column = "mpa_pressure_mmhg" if "mpa_pressure_mmhg" in df.columns else "pressure"
    values = pd.to_numeric(df[column], errors="coerce").dropna().to_numpy(dtype=float)
    if values.size == 0:
        raise RuntimeError(f"pressure CSV contains no valid pressure samples: {{csv_path}}")
    return {{
        "mpa_sys": float(np.max(values)),
        "mpa_dia": float(np.min(values)),
        "mpa_mean": float(np.mean(values)),
    }}


def _comparison_payload(name: str, metadata_path: Path, targets: dict[str, float]) -> dict:
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    outputs = metadata.get("outputs") or {{}}
    pressure_metrics = _pressure_metrics_from_csv(Path(outputs["mpa_pressure_csv"]))
    flow_split = metadata.get("flow_split") or {{}}
    result = {{
        "name": name,
        "pressure": pressure_metrics,
        "rpa_split": float(flow_split.get("rpa_split")),
        "errors": {{
            "mpa_sys": pressure_metrics["mpa_sys"] - targets["mpa_sys"],
            "mpa_dia": pressure_metrics["mpa_dia"] - targets["mpa_dia"],
            "mpa_mean": pressure_metrics["mpa_mean"] - targets["mpa_mean"],
            "rpa_split": float(flow_split.get("rpa_split")) - targets["rpa_split"],
        }},
        "metadata_json": str(metadata_path),
    }}
    result["mae"] = float(np.mean([abs(value) for value in result["errors"].values()]))
    return result


try:
    _record_event(
        "manager_started",
        "info",
        model=model,
        parameter_set=parameter_set,
        adaptation_mode=adaptation_mode,
        source_preop_iteration={selected.iteration},
    )
    postop_submission_path = postop_results_dir / "postop_submission.json"
    prestress_source = None
    if postop_submission_path.exists():
        postop_submission = json.loads(postop_submission_path.read_text(encoding="utf-8"))
        prestress_source = postop_submission.get("prestress_file_path")
        _record_event(
            "postop_submission_loaded",
            "info",
            postop_submission_json=str(postop_submission_path),
            prestress_file_path=prestress_source,
        )
    else:
        _record_event(
            "postop_submission_missing",
            "warning",
            postop_submission_json=str(postop_submission_path),
        )
    if str(threed_config.get("wall_model", "rigid")).lower() == "deformable":
        if not prestress_source:
            _record_event(
                "prestress_file_path_missing",
                "error",
                postop_submission_json=str(postop_submission_path),
            )
            raise RuntimeError("adapted deformable-wall simulation requires postop prestress_file_path")
        threed_config["prestress_file_path"] = str(prestress_source)
        _record_event(
            "prestress_file_path_resolved",
            "info",
            prestress_file_path=str(prestress_source),
        )

    remote_results_dir.mkdir(parents=True, exist_ok=True)
    simulation_dir.mkdir(parents=True, exist_ok=True)
    mesh_target = simulation_dir / "mesh-complete"
    _safe_remove(mesh_target)
    try:
        mesh_target.symlink_to(postop_mesh_complete, target_is_directory=True)
        _record_event("postop_mesh_linked", "info", mesh_target=str(mesh_target))
    except OSError:
        shutil.copytree(postop_mesh_complete, mesh_target)
        _record_event("postop_mesh_copied", "info", mesh_target=str(mesh_target))

    _record_event(
        "adaptation_started",
        "info",
        model=model,
        territory_scheme=territory_scheme,
        target_stage=target_stage,
    )
    adaptation_result = run_structured_tree_adaptation(
        preop_dir=str(preop_dir),
        postop_dir=str(postop_simulation_dir),
        adapted_dir=str(simulation_dir),
        clinical_targets=str(clinical_targets_csv),
        reduced_order_pa=str(reduced_order_pa),
        tree_params=str(tree_params_csv),
        model=model,
        territory_scheme=territory_scheme,
        parameter_set=parameter_payload,
        mode=adaptation_mode,
        convert_to_cm=False,
        output_root=str(remote_results_dir),
    )
    _record_event(
        "adaptation_completed",
        "info",
        adapted_coupler_exists=adapted_coupler_path.exists(),
        adaptation_result_type=type(adaptation_result).__name__,
    )
    adaptation_summary_path = remote_results_dir / "adaptation_summary.json"
    if adaptation_summary_path.exists():
        adaptation_summary = json.loads(adaptation_summary_path.read_text(encoding="utf-8"))
        hemodynamics = adaptation_summary.get("hemodynamics") or {{}}
        threed_hemodynamics = hemodynamics.get("threed") or {{}}
        internal_hemodynamics = hemodynamics.get("internal_zerod") or {{}}
        solver_metrics = adaptation_summary.get("solver_metrics") or {{}}
        solver_diagnostics = solver_metrics.get("solver_diagnostics") or {{}}
        _record_event(
            "adaptation_summary_recorded",
            "info",
            summary_json=str(adaptation_summary_path),
            threed_preop_rpa_split=(threed_hemodynamics.get("preop") or {{}}).get("rpa_split"),
            threed_postop_rpa_split=(threed_hemodynamics.get("postop") or {{}}).get("rpa_split"),
            internal_preop_rpa_split=(internal_hemodynamics.get("preop") or {{}}).get("rpa_split"),
            internal_postop_rpa_split=(internal_hemodynamics.get("postop_initial") or {{}}).get("rpa_split"),
            internal_final_rpa_split=(internal_hemodynamics.get("adapted_final") or {{}}).get("rpa_split"),
            termination_reason=solver_diagnostics.get("termination_reason"),
            event_time=solver_diagnostics.get("event_time"),
            rhs_l2_initial=solver_diagnostics.get("rhs_l2_initial"),
            rhs_l2_final=solver_diagnostics.get("rhs_l2_final"),
            flow_log_points=solver_metrics.get("flow_log_points"),
        )

    if not adapted_coupler_path.exists():
        _record_event(
            "adapted_coupler_missing",
            "error",
            adapted_coupler_json=str(adapted_coupler_path),
        )
        raise RuntimeError(f"adaptation wrapper did not write adapted coupler: {{adapted_coupler_path}}")
    shutil.copy2(reduced_order_pa, adapted_zerod_config_path)
    _record_event(
        "adapted_inputs_staged",
        "info",
        adapted_coupler_json=str(adapted_coupler_path),
        adapted_zerod_config_json=str(adapted_zerod_config_path),
    )

    sim = SimulationDirectory.from_directory(
        path=str(simulation_dir),
        zerod_config=str(adapted_zerod_config_path),
        mesh_complete=str(mesh_target),
        threed_coupler=str(adapted_coupler_path),
        mesh_scale_factor=mesh_scale_factor,
    )
    _sync_source_of_truth_inflow(sim)
    sim.write_files(simname="Adapted Simulation", user_input=False, sim_config=threed_config)
    run_solver_path = simulation_dir / "run_solver.sh"
    _normalize_solver_runscript(
        run_solver_path,
        nodes=int(threed_config.get("nodes", 3)),
        procs_per_node=int(threed_config.get("procs_per_node", 24)),
        memory_gb=int(threed_config.get("memory", 16)),
        hours=int(threed_config.get("hours", 20)),
        partition=partition,
        account=account,
        mail_user=_resolve_slurm_mail_user(threed_config),
        mail_types=_resolve_slurm_mail_types(threed_config),
        svfsiplus_path=cluster_svfsiplus_path,
    )
    _record_event(
        "adapted_cmm_submit_started",
        "info",
        run_solver_script=str(run_solver_path),
    )
    print("[svzt] Submitting adapted CMM simulation...", flush=True)
    cmm_job_id = _submit_job(run_solver_path)
    print(f"[svzt] Adapted CMM job submitted: {{cmm_job_id}}", flush=True)
    _record_event(
        "adapted_cmm_submitted",
        "info",
        scheduler_job_id=cmm_job_id,
    )
    ok, terminal = _wait_for_completion(
        cmm_job_id,
        poll_seconds,
        cmm_timeout_seconds,
        event_prefix="adapted_cmm",
    )
    if not ok:
        raise RuntimeError(f"adapted CMM simulation did not complete successfully: {{terminal}}")
    _record_event(
        "adapted_cmm_completed",
        "info",
        scheduler_job_id=cmm_job_id,
        terminal_state=terminal,
    )

    if paraview_enabled:
        paraview_root.mkdir(parents=True, exist_ok=True)
        paraview_logs_dir.mkdir(parents=True, exist_ok=True)
        paraview_output_dir.mkdir(parents=True, exist_ok=True)
        paraview_script_path.write_text(paraview_script_content, encoding="utf-8")
        paraview_slurm_path.write_text(paraview_slurm_content, encoding="utf-8")
        paraview_slurm_path.chmod(0o755)
        _record_event(
            "paraview_viz_submission_started",
            "info",
            stage=paraview_stage,
            slurm_script=str(paraview_slurm_path),
        )
        paraview_job_id = _submit_job(paraview_slurm_path)
        paraview_submission_metadata_path.write_text(
            json.dumps(
                {{
                    "stage": paraview_stage,
                    "source_preop_iteration": {selected.iteration},
                    "owner_scheduler_job_id": os.environ.get("SLURM_JOB_ID"),
                    "scheduler_job_id": paraview_job_id,
                    "slurm_script": str(paraview_slurm_path),
                    "output_dir": str(paraview_output_dir),
                    "submitted_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                }},
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        _record_event(
            "paraview_viz_submitted",
            "info",
            stage=paraview_stage,
            scheduler_job_id=paraview_job_id,
            output_dir=str(paraview_output_dir),
        )
    else:
        _record_event(
            "paraview_viz_skipped",
            "info",
            stage=f"adaptation-{{model.lower()}}",
            reason=paraview_skip_reason,
        )

    _record_event("baseline_postprocess_started", "info")
    baseline_output_dir = Path({json.dumps(str(baseline_postprocess_dir))})
    baseline_original_rmtree = _preserve_intermediate_centerlines(
        baseline_output_dir / "resistance_map" / "intermediate_centerlines"
    )
    try:
        baseline_postprocess_kwargs = {{
            "simulation_dir": str(postop_simulation_dir),
            "centerline": str(centerline),
            "svslicer_path": str(svslicer_path),
            "output_dir": str(baseline_output_dir),
            "clinical_targets": str(clinical_targets_csv),
            "stage": target_stage,
            "inflow_csv": str(inflow_source_path),
            "resistance_map_workers": {postprocess_workers},
        }}
        if {camera_offset_expr} is not None:
            baseline_postprocess_kwargs["camera_offset_dir"] = {camera_offset_expr}
        if {camera_view_up_expr} is not None:
            baseline_postprocess_kwargs["camera_view_up"] = {camera_view_up_expr}
        baseline_meta = _run_postprocess_suite_with_optional_camera(
            run_pulmonary_threed_postprocess_suite,
            baseline_postprocess_kwargs,
        )
        _write_stacked_centerline_timeseries(
            output_dir=baseline_output_dir,
            suite_metadata_path=baseline_output_dir / "postprocess_suite_metadata.json",
            result=baseline_meta,
        )
    finally:
        _cleanup_intermediate_centerlines(
            baseline_output_dir / "resistance_map" / "intermediate_centerlines",
            baseline_original_rmtree,
        )
    _record_event(
        "baseline_postprocess_completed",
        "info",
        metadata_json=baseline_meta.get("metadata_json"),
    )
    _record_event("adapted_postprocess_started", "info")
    adapted_output_dir = Path({json.dumps(str(adapted_postprocess_dir))})
    adapted_original_rmtree = _preserve_intermediate_centerlines(
        adapted_output_dir / "resistance_map" / "intermediate_centerlines"
    )
    try:
        adapted_postprocess_kwargs = {{
            "simulation_dir": str(simulation_dir),
            "centerline": str(centerline),
            "svslicer_path": str(svslicer_path),
            "output_dir": str(adapted_output_dir),
            "clinical_targets": str(clinical_targets_csv),
            "stage": target_stage,
            "inflow_csv": str(inflow_source_path),
            "resistance_map_workers": {postprocess_workers},
        }}
        if {camera_offset_expr} is not None:
            adapted_postprocess_kwargs["camera_offset_dir"] = {camera_offset_expr}
        if {camera_view_up_expr} is not None:
            adapted_postprocess_kwargs["camera_view_up"] = {camera_view_up_expr}
        adapted_meta = _run_postprocess_suite_with_optional_camera(
            run_pulmonary_threed_postprocess_suite,
            adapted_postprocess_kwargs,
        )
        _write_stacked_centerline_timeseries(
            output_dir=adapted_output_dir,
            suite_metadata_path=adapted_output_dir / "postprocess_suite_metadata.json",
            result=adapted_meta,
        )
    finally:
        _cleanup_intermediate_centerlines(
            adapted_output_dir / "resistance_map" / "intermediate_centerlines",
            adapted_original_rmtree,
        )
    _record_event(
        "adapted_postprocess_completed",
        "info",
        metadata_json=adapted_meta.get("metadata_json"),
    )

    targets = ClinicalTargets.from_csv(str(clinical_targets_csv))
    target_payload = {{
        "mpa_sys": float(targets.mpa_p[0]),
        "mpa_dia": float(targets.mpa_p[1]),
        "mpa_mean": float(targets.mpa_p[2]),
        "rpa_split": float(targets.rpa_split),
    }}
    baseline_summary = _comparison_payload("postop_baseline", Path(baseline_meta["metadata_json"]), target_payload)
    adapted_summary = _comparison_payload("adapted_prediction", Path(adapted_meta["metadata_json"]), target_payload)
    comparison = {{
        "model": model,
        "parameter_set": parameter_set,
        "territory_scheme": territory_scheme,
        "target_stage": target_stage,
        "inflow_provenance": {{
            "source_path": str(inflow_source_path),
            "size_bytes": inflow_source_path.stat().st_size,
        }},
        "targets": target_payload,
        "baseline": baseline_summary,
        "adapted": adapted_summary,
        "improvement": {{
            "mae_delta": baseline_summary["mae"] - adapted_summary["mae"],
            "mpa_sys_abs_error_delta": abs(baseline_summary["errors"]["mpa_sys"]) - abs(adapted_summary["errors"]["mpa_sys"]),
            "mpa_dia_abs_error_delta": abs(baseline_summary["errors"]["mpa_dia"]) - abs(adapted_summary["errors"]["mpa_dia"]),
            "mpa_mean_abs_error_delta": abs(baseline_summary["errors"]["mpa_mean"]) - abs(adapted_summary["errors"]["mpa_mean"]),
            "rpa_split_abs_error_delta": abs(baseline_summary["errors"]["rpa_split"]) - abs(adapted_summary["errors"]["rpa_split"]),
        }},
        "artifacts": {{
            "adaptation_summary_json": str(remote_results_dir / "adaptation_summary.json"),
            "adaptation_metrics_json": str(remote_results_dir / "adaptation_metrics.json"),
            "adapted_coupler_json": str(adapted_coupler_path),
            "baseline_postprocess_metadata_json": baseline_meta["metadata_json"],
            "adapted_postprocess_metadata_json": adapted_meta["metadata_json"],
            "adapted_simulation_dir": str(simulation_dir),
            "paraview_viz_submission_json": str(paraview_submission_metadata_path) if paraview_enabled else None,
            "manager_log_jsonl": str(manager_log_path),
            "status_json": str(status_path),
        }},
        "cmm_job_id": cmm_job_id,
        "paraview_viz_job_id": paraview_job_id,
    }}
    comparison_path = remote_results_dir / "baseline_vs_adapted_comparison.json"
    comparison_path.write_text(
        json.dumps(comparison, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    _record_event(
        "comparison_written",
        "info",
        comparison_json=str(comparison_path),
    )
    _record_event("manager_completed", "info")
    print("[svzt] Adaptation manager job complete.", flush=True)
except Exception as exc:
    _record_event(
        "manager_failed",
        "error",
        error_type=type(exc).__name__,
        error=str(exc),
    )
    raise
PY
"""


def run_adapt(
    workspace_root: str | Path,
    run_id: str,
    *,
    model: str,
    parameter_set: str | None = None,
    adaptation_mode: str = "predict",
    mode: ExecutionMode = ExecutionMode.DRY_RUN,
    transfer_adapter: FileTransferAdapter | None = None,
    scheduler_adapter: SchedulerAdapter | None = None,
    remote_exec_adapter: RemoteExecAdapter | None = None,
) -> AdaptRunResult:
    root = detect_workspace_root(workspace_root)
    validated_run_id = validate_run_id(run_id)
    local_paths = build_local_run_paths(root, validated_run_id)
    manifest = read_manifest(local_paths.manifest)
    selected = manifest.converged_preop_iteration
    if selected is None:
        raise ConfigError(
            f"run '{validated_run_id}' has no converged_preop_iteration; run svzt preop select first"
        )
    if manifest.postop_run is None or not manifest.postop_run.postop_job_id:
        raise ConfigError(
            f"run '{validated_run_id}' has no completed postop_run; run svzt run postop first"
        )
    tuning_bc_type = str((manifest.remote or {}).get("tuning_bc_type") or "impedance").strip().lower()
    if tuning_bc_type != "impedance":
        raise ConfigError(
            "adaptation currently requires impedance-tuned structured-tree outputs; "
            f"selected run uses bc_type={tuning_bc_type!r}"
        )
    if adaptation_mode not in {"predict", "retrospective_fit"}:
        raise ConfigError("adaptation_mode must be one of predict|retrospective_fit")

    inflow_source_path = str(
        (manifest.remote.get("svzerodtrees_paths") or {}).get("inflow") or ""
    ).strip()
    if not inflow_source_path:
        raise ConfigError("adapted 3D requires source-of-truth inflow path in manifest")
    inflow_fingerprint, inflow_metadata = _fingerprint_inflow(inflow_source_path)

    config, cluster = _resolve_cluster_for_manifest(root, manifest)
    resolved_parameter_set, parameter_payload = _resolve_parameter_payload(
        manifest,
        model=model,
        parameter_set=parameter_set,
    )

    remote_run_dir = manifest.execution.remote_run_dir or manifest.remote.get("remote_run_dir")
    if not remote_run_dir:
        raise ConfigError(f"run '{validated_run_id}' is missing remote_run_dir")
    remote_layout = _adaptation_remote_layout(
        remote_run_dir=str(remote_run_dir),
        iteration=selected.iteration,
        model=model.upper(),
    )
    for path in remote_layout.values():
        validate_remote_write_path(
            path,
            cluster.remote_roots.runs_root,
        )

    adaptation_paths = _adaptation_local_paths(
        local_paths,
        iteration=selected.iteration,
        model=model.upper(),
    )
    for key in ("root", "inputs", "logs", "results"):
        adaptation_paths[key].mkdir(parents=True, exist_ok=True)

    plan = _build_adapt_plan(
        manifest=manifest,
        cluster=cluster,
        local_paths=local_paths,
        adaptation_paths=adaptation_paths,
        remote_layout=remote_layout,
        model=model.upper(),
        parameter_set=resolved_parameter_set,
        inflow_source_path=inflow_source_path,
        inflow_fingerprint=inflow_fingerprint,
    )
    write_plan_json(plan, adaptation_paths["plan_json"])
    write_plan_yaml(plan, adaptation_paths["plan_yaml"])

    payload = {
        "model": model.upper(),
        "parameter_set": resolved_parameter_set,
        "parameter_payload": parameter_payload,
        "source_preop_iteration": selected.iteration,
        "source_postop_job_id": manifest.postop_run.postop_job_id,
        "inflow_source_path": inflow_source_path,
        "inflow_fingerprint": inflow_fingerprint,
        "inflow_metadata": inflow_metadata,
    }
    (adaptation_paths["inputs"] / "adaptation_request.yaml").write_text(
        yaml.safe_dump(payload, sort_keys=True),
        encoding="utf-8",
    )
    prepared_paraview_job = None
    resolved_paraview_skip_reason = _paraview_skip_reason(
        config=config,
        cluster=cluster,
        manifest=manifest,
    )
    if resolved_paraview_skip_reason is None:
        prepared_paraview_job = _prepare_adaptation_paraview_viz_job(
            workspace_root=root,
            run_id=validated_run_id,
            manifest=manifest,
            cluster=cluster,
            model=model.upper(),
            remote_adaptation_dir=remote_layout["remote_adaptation_dir"],
            source_iteration=selected.iteration,
        )

    script_body = _render_adapt_job_script(
        config=config,
        manifest=manifest,
        cluster=cluster,
        remote_layout=remote_layout,
        model=model.upper(),
        parameter_set=resolved_parameter_set,
        adaptation_mode=adaptation_mode,
        parameter_payload=parameter_payload,
        paraview_job=prepared_paraview_job,
        paraview_skip_reason=resolved_paraview_skip_reason,
    )
    adaptation_paths["job_script"].write_text(script_body, encoding="utf-8")

    if transfer_adapter is None or scheduler_adapter is None or remote_exec_adapter is None:
        default_transfer, default_scheduler, default_remote = _build_default_adapters(
            cluster=cluster,
            config=config,
            run_id=validated_run_id,
            mode=mode,
        )
        transfer_adapter = transfer_adapter or default_transfer
        scheduler_adapter = scheduler_adapter or default_scheduler
        remote_exec_adapter = remote_exec_adapter or default_remote

    if isinstance(scheduler_adapter, SlurmSchedulerAdapter):
        scheduler_adapter = SlurmSchedulerAdapter(
            remote_exec=scheduler_adapter.remote_exec,
            runs_root=scheduler_adapter.runs_root,
            submit_options=SlurmSubmitOptions(job_name=f"{validated_run_id}-adapt"),
        )

    command_results = [
        transfer_adapter.ensure_remote_dir(remote_layout["remote_adaptation_dir"]),
        transfer_adapter.ensure_remote_dir(remote_layout["remote_inputs_dir"]),
        transfer_adapter.ensure_remote_dir(remote_layout["remote_logs_dir"]),
        transfer_adapter.ensure_remote_dir(remote_layout["remote_results_dir"]),
        transfer_adapter.ensure_remote_dir(remote_layout["remote_simulation_dir"]),
        transfer_adapter.sync(
            local_dir=str(adaptation_paths["inputs"]),
            remote_dir=remote_layout["remote_inputs_dir"],
            include=config.defaults.rsync.include_patterns,
            exclude=config.defaults.rsync.exclude_patterns,
            direction=SyncDirection.PUSH,
        ),
        transfer_adapter.push(
            str(adaptation_paths["job_script"]),
            remote_layout["remote_job_script_path"],
        ),
    ]
    submit_result = scheduler_adapter.submit(remote_layout["remote_job_script_path"])
    command_results.append(submit_result.command)

    manifest = read_manifest(local_paths.manifest)
    manifest.artifacts["adaptation_plan_files"] = manifest.artifacts.get("adaptation_plan_files", [])
    manifest.artifacts["adaptation_plan_files"].extend(
        [str(adaptation_paths["plan_json"]), str(adaptation_paths["plan_yaml"])]
    )
    if mode == ExecutionMode.EXECUTE:
        manifest = record_submission(
            manifest,
            remote_run_dir=str(remote_run_dir),
            job_script_path=remote_layout["remote_job_script_path"],
            scheduler_type=cluster.scheduler.type,
            submitted_job_id=submit_result.job_id,
            mode=mode.value,
        )
        manifest = record_adaptation_submission(
            manifest,
            model=model.upper(),
            mode=adaptation_mode,
            parameter_set=resolved_parameter_set,
            source_preop_iteration=selected.iteration,
            source_postop_job_id=manifest.postop_run.postop_job_id if manifest.postop_run else None,
            territory_scheme=str((_resolve_adaptation_defaults(manifest).get("territory_scheme") or "lpa_rpa")),
            target_stage=str((_resolve_adaptation_defaults(manifest).get("target_stage") or "postop")),
            local_dir=str(adaptation_paths["root"]),
            remote_dir=remote_layout["remote_adaptation_dir"],
            local_job_script_path=str(adaptation_paths["job_script"]),
            remote_job_script_path=remote_layout["remote_job_script_path"],
            scheduler_job_id=submit_result.job_id,
            inflow_source_path=inflow_source_path,
            inflow_fingerprint=inflow_fingerprint,
            inflow_metadata=inflow_metadata,
            artifact_roots={
                "results": remote_layout["remote_results_dir"],
                "simulation": remote_layout["remote_simulation_dir"],
            },
            summary_path=str(PurePosixPath(remote_layout["remote_results_dir"]) / "adaptation_summary.json"),
            comparison_path=str(PurePosixPath(remote_layout["remote_results_dir"]) / "baseline_vs_adapted_comparison.json"),
            note="Explicit adaptation workflow submitted",
        )
        if prepared_paraview_job is not None:
            manifest = record_paraview_viz_submission(
                manifest,
                stage=prepared_paraview_job.stage,
                source_iteration=prepared_paraview_job.source_iteration,
                local_dir=str(prepared_paraview_job.local_root),
                remote_dir=prepared_paraview_job.remote_output_dir,
                local_script_path=str(prepared_paraview_job.local_slurm_script),
                remote_script_path=prepared_paraview_job.remote_slurm_script,
                scheduler_job_id=None,
                note="ParaView visualization is submitted by the adaptation manager job after adapted CMM completion",
            )
    write_manifest(manifest, local_paths.manifest)

    return AdaptRunResult(
        run_id=validated_run_id,
        model=model.upper(),
        parameter_set=resolved_parameter_set,
        mode=mode,
        source_preop_iteration=selected.iteration,
        plan_path=adaptation_paths["plan_yaml"],
        remote_adaptation_dir=remote_layout["remote_adaptation_dir"],
        remote_job_script_path=remote_layout["remote_job_script_path"],
        local_job_script_path=adaptation_paths["job_script"],
        submitted_job_id=submit_result.job_id,
        inflow_source_path=inflow_source_path,
        command_previews=[item.argv for item in command_results],
    )
