"""Explicit postop workflow orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path, PurePosixPath
import json

import yaml

from svztagent.config.load import detect_workspace_root, load_workspace_config, resolve_cluster
from svztagent.core.errors import ConfigError
from svztagent.core.manifest import (
    read_manifest,
    record_paraview_viz_submission,
    record_submission,
    record_converged_preop_iteration,
    record_postprocess_submission,
    record_postop_submission,
    write_manifest,
)
from svztagent.core.paths import (
    LocalRunPaths,
    build_iteration_local_paths,
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
from svztagent.workflows.postprocess import (
    _load_stage_target_payload,
    _parse_positive_int,
    _python_bootstrap,
    _render_postprocess_script,
    _render_postprocess_slurm_header,
    _resolve_resistance_map_camera,
    _resolved_postprocess_worker_count,
    submit_selected_preop_postprocess,
)
from svztagent.workflows.paraview_viz import _paraview_skip_reason, _prepare_postop_paraview_viz_job
from svztagent.workflows.tune_trees import (
    _build_default_adapters,
    _resolve_cluster_svfsiplus_path,
)


@dataclass(frozen=True)
class PreopSelectionResult:
    run_id: str
    iteration: int
    selection_kind: str
    remote_preop_dir: str
    remote_tuned_zerod_config: str
    remote_canonical_coupler: str
    postprocess_job_id: str | None = None


@dataclass(frozen=True)
class PostopRunResult:
    run_id: str
    source_preop_iteration: int
    mode: ExecutionMode
    plan_path: Path
    remote_postop_dir: str
    remote_job_script_path: str
    local_job_script_path: Path
    submitted_job_id: str
    command_previews: list[list[str]]


def _load_mapping(path: Path, *, label: str) -> dict:
    if not path.exists():
        raise ConfigError(f"{label} is missing: {path}")
    with path.open("r", encoding="utf-8") as stream:
        payload = yaml.safe_load(stream) or {}
    if not isinstance(payload, dict):
        raise ConfigError(f"{label} must contain a mapping: {path}")
    return payload


def _resolve_cluster_for_manifest(workspace_root: Path, manifest):
    cluster_name = manifest.cluster.get("name")
    if not cluster_name:
        raise ConfigError("manifest cluster.name is required")
    config = load_workspace_config(workspace_root)
    return config, resolve_cluster(config, cluster_name)


def _postop_local_paths(local_paths: LocalRunPaths, iteration: int) -> dict[str, Path]:
    root = local_paths.run_dir / "postop" / f"from-{iteration_dir_name(iteration)}"
    return {
        "root": root,
        "inputs": root / "inputs",
        "logs": root / "logs",
        "results": root / "results",
        "job_script": root / "run_postop.sh",
        "plan_json": root / "postop_execution_plan.json",
        "plan_yaml": root / "postop_execution_plan.yaml",
    }


def _postop_remote_layout(*, remote_run_dir: str, iteration: int) -> dict[str, str]:
    remote_postop_dir = str(
        PurePosixPath(remote_run_dir) / "postop" / f"from-{iteration_dir_name(iteration)}"
    )
    return {
        "remote_postop_dir": remote_postop_dir,
        "remote_inputs_dir": str(PurePosixPath(remote_postop_dir) / "inputs"),
        "remote_logs_dir": str(PurePosixPath(remote_postop_dir) / "logs"),
        "remote_results_dir": str(PurePosixPath(remote_postop_dir) / "results"),
        "remote_job_script_path": str(PurePosixPath(remote_postop_dir) / "run_postop.sh"),
    }


def _numeric_mapping(payload: object) -> dict[str, float] | None:
    if not isinstance(payload, dict):
        return None
    values: dict[str, float] = {}
    for key, value in payload.items():
        if isinstance(value, bool):
            continue
        if isinstance(value, (int, float)):
            values[str(key)] = float(value)
    return values or None


def _resolve_selected_reduced_config_path(
    *,
    record,
    decision: dict,
    tuned_config: str,
) -> str:
    regenerated_config = str(
        record.regenerated_config_path
        or decision.get("regenerated_config_path")
        or ""
    ).strip()
    return regenerated_config or tuned_config


def _refresh_selected_reduced_config_from_local_iteration(
    *,
    manifest,
    local_paths: LocalRunPaths,
):
    selected = manifest.converged_preop_iteration
    if selected is None:
        return manifest, None

    iteration_paths = build_iteration_local_paths(local_paths, int(selected.iteration))
    if not iteration_paths["decision"].exists():
        return manifest, selected

    decision = _load_mapping(iteration_paths["decision"], label="iteration_decision.json")
    artifacts = decision.get("tuning_artifacts")
    if not isinstance(artifacts, dict):
        return manifest, selected

    tuned_config = str(artifacts.get("tuned_zerod_config") or "").strip()
    if not tuned_config:
        return manifest, selected

    regenerated_config = str(decision.get("regenerated_config_path") or "").strip()
    refreshed_config = regenerated_config or tuned_config
    if refreshed_config == selected.remote_tuned_zerod_config:
        return manifest, selected

    updated = manifest.model_copy(deep=True)
    updated.converged_preop_iteration = selected.model_copy(
        update={"remote_tuned_zerod_config": refreshed_config}
    )
    write_manifest(updated, local_paths.manifest)
    return updated, updated.converged_preop_iteration


def select_converged_preop_iteration(
    workspace_root: str | Path,
    run_id: str,
    *,
    iteration: int,
    reason: str | None = None,
    submit_postprocess: bool = True,
    transfer_adapter: FileTransferAdapter | None = None,
    scheduler_adapter: SchedulerAdapter | None = None,
    remote_exec_adapter: RemoteExecAdapter | None = None,
) -> PreopSelectionResult:
    root = detect_workspace_root(workspace_root)
    validated_run_id = validate_run_id(run_id)
    local_paths = build_local_run_paths(root, validated_run_id)
    manifest = read_manifest(local_paths.manifest)

    record = next(
        (item for item in manifest.tuning_iteration_tracker.iterations if item.iteration == iteration),
        None,
    )
    if record is None:
        raise ConfigError(f"run '{validated_run_id}' has no iter-{iteration:02d} record")

    iteration_paths = build_iteration_local_paths(local_paths, iteration)
    decision = _load_mapping(iteration_paths["decision"], label="iteration_decision.json")
    metrics_payload = _load_mapping(iteration_paths["metrics"], label="iteration_metrics.json")
    driver_log = _load_mapping(
        iteration_paths["logs"] / "iteration_driver_log.json",
        label="iteration_driver_log.json",
    )

    steps = {str(step) for step in driver_log.get("steps", []) if step}
    if "preop_completed" not in steps:
        raise ConfigError(
            f"iter-{iteration:02d} is missing completed preop evidence in iteration_driver_log.json"
        )

    artifacts = decision.get("tuning_artifacts")
    if not isinstance(artifacts, dict):
        raise ConfigError(f"iter-{iteration:02d} decision is missing tuning_artifacts")

    tuned_config = str(artifacts.get("tuned_zerod_config") or "").strip()
    if not tuned_config:
        raise ConfigError(f"iter-{iteration:02d} is missing tuned_zerod_config")
    selected_reduced_config = _resolve_selected_reduced_config_path(
        record=record,
        decision=decision,
        tuned_config=tuned_config,
    )

    remote_iter_dir = str(record.remote_dir or "").strip()
    if not remote_iter_dir:
        remote_run_dir = manifest.execution.remote_run_dir or manifest.remote.get("remote_run_dir")
        if not remote_run_dir:
            raise ConfigError(f"run '{validated_run_id}' is missing remote_run_dir")
        remote_iter_dir = str(PurePosixPath(str(remote_run_dir)) / "iterations" / iteration_dir_name(iteration))

    remote_preop_dir = str(PurePosixPath(remote_iter_dir) / "preop")
    remote_canonical_coupler = str(PurePosixPath(remote_iter_dir) / "results" / "svzerod_3Dcoupling.json")
    source_decision = str(decision.get("decision")) if decision.get("decision") else record.decision
    selection_kind = (
        "formal_converged"
        if source_decision == "converged"
        else "operator_promoted_best_completed"
    )
    preop_job_id = (
        str(driver_log.get("preop_job_id"))
        if driver_log.get("preop_job_id")
        else str(metrics_payload.get("preop_job_id"))
        if metrics_payload.get("preop_job_id")
        else None
    )

    metrics = record.metrics or _numeric_mapping(decision.get("metrics")) or _numeric_mapping(metrics_payload)
    deltas = record.deltas or _numeric_mapping(decision.get("deltas"))

    updated = record_converged_preop_iteration(
        manifest,
        iteration=iteration,
        source_decision=source_decision,
        selection_kind=selection_kind,
        reason=reason,
        metrics=metrics,
        deltas=deltas,
        remote_iteration_dir=remote_iter_dir,
        remote_preop_dir=remote_preop_dir,
        remote_tuned_zerod_config=selected_reduced_config,
        remote_canonical_coupler=remote_canonical_coupler,
        preop_job_id=preop_job_id,
    )
    write_manifest(updated, local_paths.manifest)

    postprocess_job_id: str | None = None
    if submit_postprocess:
        postprocess_result = submit_selected_preop_postprocess(
            workspace_root=root,
            run_id=validated_run_id,
            iteration=iteration,
            transfer_adapter=transfer_adapter,
            scheduler_adapter=scheduler_adapter,
            remote_exec_adapter=remote_exec_adapter,
        )
        postprocess_job_id = postprocess_result.submitted_job_id

    return PreopSelectionResult(
        run_id=validated_run_id,
        iteration=iteration,
        selection_kind=selection_kind,
        remote_preop_dir=remote_preop_dir,
        remote_tuned_zerod_config=selected_reduced_config,
        remote_canonical_coupler=remote_canonical_coupler,
        postprocess_job_id=postprocess_job_id,
    )


def _build_postop_plan(
    *,
    manifest,
    cluster,
    local_paths: LocalRunPaths,
    postop_paths: dict[str, Path],
    remote_layout: dict[str, str],
) -> ExecutionPlan:
    selected = manifest.converged_preop_iteration
    if selected is None:
        raise ConfigError(
            f"run '{manifest.run_id}' has no converged_preop_iteration; run svzt preop select first"
        )
    postop_mesh = manifest.remote.get("svzerodtrees_paths", {}).get("postop_mesh_complete")
    if not postop_mesh:
        raise ConfigError("postop mesh-complete path is not configured for this patient")

    remote_run_dir = manifest.execution.remote_run_dir or manifest.remote.get("remote_run_dir")
    steps = [
        PlanStep(
            step_id="p01_validate_converged_preop_iteration",
            name="validate_converged_preop_iteration",
            category=StepCategory.RESOLVE_PATHS,
            description="Validate the manifest-selected converged preop iteration.",
            inputs={"iteration": str(selected.iteration)},
            outputs={
                "tuned_zerod_config": selected.remote_tuned_zerod_config,
                "canonical_coupler": selected.remote_canonical_coupler,
            },
            remote_paths={
                "read": [
                    selected.remote_tuned_zerod_config,
                    selected.remote_canonical_coupler,
                    selected.remote_preop_dir,
                ],
                "write": [],
            },
            safety_notes=["Preop artifacts are read-only inputs to postop generation."],
            command_preview=["svzt", "preop", "select", "--run-id", manifest.run_id, "--iteration", str(selected.iteration)],
        ),
        PlanStep(
            step_id="p02_stage_postop_inputs",
            name="stage_postop_inputs",
            category=StepCategory.STAGE_INPUTS,
            description="Stage postop submission metadata locally.",
            outputs={"local_inputs_dir": str(postop_paths["inputs"])},
            dependencies=["p01_validate_converged_preop_iteration"],
            local_paths={"inputs": str(postop_paths["inputs"])},
            command_preview=["mkdir", "-p", str(postop_paths["inputs"])],
        ),
        PlanStep(
            step_id="p03_define_remote_postop_destination",
            name="define_remote_postop_destination",
            category=StepCategory.PUSH_TO_CLUSTER,
            description="Define run-scoped remote postop directory under runs_root.",
            outputs={"remote_postop_dir": remote_layout["remote_postop_dir"]},
            dependencies=["p02_stage_postop_inputs"],
            remote_paths={
                "read": [],
                "write": [
                    remote_layout["remote_postop_dir"],
                    remote_layout["remote_inputs_dir"],
                    remote_layout["remote_logs_dir"],
                    remote_layout["remote_results_dir"],
                ],
            },
            safety_notes=["All remote writes stay under configured runs_root."],
            command_preview=["rsync", "-az", str(postop_paths["inputs"]) + "/", remote_layout["remote_inputs_dir"]],
        ),
        PlanStep(
            step_id="p04_generate_postop_job_script",
            name="generate_postop_job_script",
            category=StepCategory.GENERATE_JOB_SCRIPT,
            description="Render explicit postop setup/submission script.",
            outputs={"remote_script_path": remote_layout["remote_job_script_path"]},
            dependencies=["p03_define_remote_postop_destination"],
            remote_paths={
                "read": [
                    selected.remote_tuned_zerod_config,
                    selected.remote_canonical_coupler,
                    postop_mesh,
                ],
                "write": [remote_layout["remote_job_script_path"]],
            },
            command_preview=["generate_postop_job_script", "--output", remote_layout["remote_job_script_path"]],
        ),
        PlanStep(
            step_id="p05_submit_postop_job",
            name="submit_postop_job",
            category=StepCategory.SUBMIT_JOB,
            description="Submit the explicit postop job script to the scheduler.",
            inputs={"script_path": remote_layout["remote_job_script_path"]},
            outputs={"postop_job_id": "<job_id>"},
            dependencies=["p04_generate_postop_job_script"],
            remote_paths={"read": [remote_layout["remote_job_script_path"]], "write": [remote_layout["remote_logs_dir"], remote_layout["remote_results_dir"]]},
            command_preview=["sbatch", "--parsable", remote_layout["remote_job_script_path"]],
        ),
        PlanStep(
            step_id="p06_record_postop_submission",
            name="record_postop_submission",
            category=StepCategory.FINALIZE_MANIFEST,
            description="Record postop plan/submission metadata in the run manifest.",
            outputs={"manifest_path": manifest.local_paths.manifest},
            dependencies=["p05_submit_postop_job"],
            local_paths={"manifest": manifest.local_paths.manifest},
            command_preview=["svzt", "record-postop", "--run-id", manifest.run_id],
        ),
    ]
    plan = ExecutionPlan(
        plan_id=f"plan-{manifest.run_id}-postop-from-{iteration_dir_name(selected.iteration)}",
        workflow_name="postop",
        run_id=manifest.run_id,
        cluster=cluster.name,
        patient=str(manifest.patient.get("alias")),
        created_at=utc_now_iso(),
        manifest_path=manifest.local_paths.manifest,
        local_run_dir=manifest.local_paths.run_dir,
        remote_run_dir=str(remote_run_dir),
        steps=steps,
        summary={
            "source_preop_iteration": selected.iteration,
            "selection_kind": selected.selection_kind,
            "remote_canonical_coupler": selected.remote_canonical_coupler,
            "postop_mesh_complete": postop_mesh,
            "remote_postop_dir": remote_layout["remote_postop_dir"],
        },
    )
    validation = assert_valid_execution_plan(
        plan=plan,
        runs_root=cluster.remote_roots.runs_root,
        patient_data_root=cluster.remote_roots.patient_data_root,
    )
    return plan.model_copy(update={"validation_results": validation})


def _render_postop_job_script(
    *,
    manifest,
    selected,
    remote_layout: dict[str, str],
    paraview_job=None,
    paraview_skip_reason: str | None = None,
) -> str:
    svzerodtrees_paths = manifest.remote.get("svzerodtrees_paths", {})
    postop_mesh = svzerodtrees_paths.get("postop_mesh_complete")
    clinical_targets = svzerodtrees_paths.get("clinical_targets")
    threed_config = manifest.remote.get("threed_defaults", {})
    mesh_scale_factor = manifest.patient.get("mesh_scale_factor") or 1.0
    patient_alias = str(manifest.patient.get("alias"))
    cluster_name = str(manifest.cluster.get("name"))
    workspace_root = detect_workspace_root(Path(manifest.local_paths.run_dir).parents[1])
    config = load_workspace_config(workspace_root)
    cluster = resolve_cluster(config, cluster_name)
    clinical_targets_payload = _load_stage_target_payload(workspace_root, patient_alias=patient_alias, stage="postop")
    svslicer_path = cluster.executables.svslicer_path
    postop_postprocess_cpus = _resolved_postprocess_worker_count(
        config,
        fallback_cpus=_parse_positive_int(threed_config.get("procs_per_node")) or 4,
    )
    bootstrap = _python_bootstrap(config)
    if svslicer_path is None:
        raise ConfigError("cluster executables.svslicer_path is required for postop postprocessing")
    centerline = svzerodtrees_paths.get("centerlines")
    if not centerline:
        raise ConfigError("manifest remote.svzerodtrees_paths.centerlines is required for postop postprocessing")
    if not clinical_targets:
        raise ConfigError("manifest remote.svzerodtrees_paths.clinical_targets is required for postop staging")
    inflow_csv = svzerodtrees_paths.get("inflow")
    scheduler_defaults = manifest.remote.get("scheduler_defaults", {})
    account = str(scheduler_defaults.get("account") or "").strip() or None
    partition = str(scheduler_defaults.get("partition") or "amarsden")
    svfsiplus_path = _resolve_cluster_svfsiplus_path(
        cluster_name=cluster_name,
        configured_path=cluster.executables.svfsiplus_path,
    )
    camera_offset_dir, camera_view_up = _resolve_resistance_map_camera(
        config,
        patient_alias=patient_alias,
    )

    # Pre-render the postprocessing script so it can be embedded in the manager job.
    # The manager writes it to disk after CMM completes and submits it as a separate job.
    postprocess_script_content = _render_postprocess_script(
        config=config,
        cluster=cluster,
        remote_root=remote_layout["remote_postop_dir"],
        remote_logs_dir=remote_layout["remote_logs_dir"],
        simulation_dir=str(PurePosixPath(remote_layout["remote_postop_dir"]) / "simulation"),
        output_dir=str(PurePosixPath(remote_layout["remote_results_dir"]) / "postprocess"),
        centerline=str(centerline),
        svslicer_path=svslicer_path,
        stage="postop",
        clinical_targets_payload=clinical_targets_payload,
        fallback_clinical_targets_csv=str(clinical_targets) if clinical_targets else None,
        inflow_csv=str(inflow_csv) if inflow_csv else None,
        resistance_map_workers=postop_postprocess_cpus,
        camera_offset_dir=camera_offset_dir,
        camera_view_up=camera_view_up,
        cpus_per_task=postop_postprocess_cpus,
        mem=f"{int(threed_config.get('memory', 16))}G",
        account=account,
        partition=partition,
        wall_time_hours=4,
    )
    postprocess_script_json = json.dumps(postprocess_script_content)
    paraview_stage = paraview_job.stage if paraview_job is not None else "postop"
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

    # Manager job: 1 node, few CPUs — submits and monitors child SLURM jobs.
    # Wall time must cover: mean steady (≤6h) + prestress (≤hours) + CMM (≤hours) + buffer.
    manager_wall_hours = min(int(threed_config.get("hours", 20)) * 2 + 8, 47)
    slurm_header = _render_postprocess_slurm_header(
        remote_root=remote_layout["remote_postop_dir"],
        remote_logs_dir=remote_layout["remote_logs_dir"],
        account=account,
        partition=partition,
        wall_time_hours=manager_wall_hours,
        nodes=1,
        ntasks_per_node=None,
        cpus_per_task=4,
        mem=f"{int(threed_config.get('memory', 16))}G",
    )
    return f"""#!/usr/bin/env bash
{slurm_header}
set -euo pipefail

mkdir -p "{remote_layout["remote_inputs_dir"]}" "{remote_layout["remote_results_dir"]}" "{remote_layout["remote_logs_dir"]}"

{bootstrap}

"${{PYTHON_BIN}}" - <<'PY'
from pathlib import Path
import glob
import json
import os
import subprocess
import shutil
import time
import xml.etree.ElementTree as ET

import numpy as np

from svzerodtrees.simulation import Simulation, SimulationDirectory

remote_postop_dir = Path({json.dumps(remote_layout["remote_postop_dir"])})
remote_results_dir = Path({json.dumps(remote_layout["remote_results_dir"])})
mesh_complete = Path({json.dumps(str(postop_mesh))})
tuned_clinical_targets = Path({json.dumps(str(clinical_targets))})
tuned_zerod = Path({json.dumps(selected.remote_tuned_zerod_config)})
canonical_coupler = Path({json.dumps(selected.remote_canonical_coupler)})
threed_config = json.loads({json.dumps(json.dumps(threed_config, sort_keys=True))})
mesh_scale_factor = float({json.dumps(str(mesh_scale_factor))})
cluster_svfsiplus_path = {json.dumps(svfsiplus_path)}
partition = {json.dumps(partition)}
account = {json.dumps(account) if account is not None else "None"}
poll_seconds = int(threed_config.get("wait_poll_seconds", 30))
cmm_timeout_seconds = max(int(threed_config.get("hours", 20)) * 3600 + 7200, int(threed_config.get("wait_timeout_seconds", 14400)))
paraview_stage = {json.dumps(paraview_stage)}
paraview_root = Path({json.dumps(paraview_root)})
paraview_logs_dir = Path({json.dumps(paraview_logs_dir)})
paraview_script_path = Path({json.dumps(paraview_script_path)})
paraview_slurm_path = Path({json.dumps(paraview_slurm_path)})
paraview_output_dir = Path({json.dumps(paraview_output_dir)})
paraview_submission_metadata_path = Path({json.dumps(paraview_submission_metadata_path)})
paraview_enabled = {repr(paraview_job is not None)}
paraview_skip_reason = {repr(paraview_skip_reason)}
paraview_script_content = {paraview_script_json}
paraview_slurm_content = {paraview_slurm_json}

simulation_dir = remote_postop_dir / "simulation"
simulation_dir.mkdir(parents=True, exist_ok=True)
mesh_target = simulation_dir / "mesh-complete"
if mesh_target.is_symlink() or mesh_target.is_file():
    mesh_target.unlink()
elif mesh_target.is_dir():
    shutil.rmtree(mesh_target)
try:
    mesh_target.symlink_to(mesh_complete, target_is_directory=True)
except OSError:
    shutil.copytree(mesh_complete, mesh_target)
threed_config.pop("prestress_file_path", None)
threed_config.pop("prestress_file", None)
solver_execution = dict(threed_config.get("execution", {{}}))
solver_execution["mode"] = "slurm"
solver_execution["submit_command"] = "bash"
resolved_inflow_path = {json.dumps(str(inflow_csv)) if inflow_csv else "None"}


def _safe_remove(path: Path) -> None:
    if path.is_symlink() or path.is_file():
        path.unlink()
    elif path.is_dir():
        shutil.rmtree(path)


def _sync_postop_inflow(sim_dir: SimulationDirectory) -> None:
    if resolved_inflow_path is None:
        raise RuntimeError("postop simulation requires a patient inflow waveform")
    inflow_helper = Simulation(
        path=str(remote_postop_dir),
        clinical_targets=str(tuned_clinical_targets),
        preop_dir=str(simulation_dir),
        postop_dir=str(simulation_dir),
        adapted_dir=None,
        zerod_config=str(tuned_zerod),
        convert_to_cm=False,
        mesh_scale_factor=mesh_scale_factor,
        wall_model=str(threed_config.get("wall_model", "deformable")),
        elasticity_modulus=float(threed_config.get("elasticity_modulus", 5062674.563165)),
        poisson_ratio=float(threed_config.get("poisson_ratio", 0.5)),
        shell_thickness=float(threed_config.get("shell_thickness", 0.12)),
        prestress_file=None,
        prestress_file_path=None,
        tissue_support=threed_config.get("tissue_support"),
        inflow_path=resolved_inflow_path,
        execution_config=solver_execution,
    )
    full_inflow = getattr(inflow_helper, "inflow_3d", None)
    if full_inflow is None:
        raise RuntimeError("postop simulation could not resolve full-scale 3D inflow")

    if sim_dir.zerod_config is None:
        raise RuntimeError("postop simulation requires zerod_config to generate dirichlet inflow")
    sim_dir.zerod_config.set_inflow(full_inflow)
    sim_dir.zerod_config.inflows[full_inflow.name] = full_inflow
    sim_dir.zerod_config.to_json(sim_dir.zerod_config.path)

    if sim_dir.svzerod_3Dcoupling is not None:
        sim_dir.svzerod_3Dcoupling.set_inflow(full_inflow)
        sim_dir.svzerod_3Dcoupling.inflows[full_inflow.name] = full_inflow
        sim_dir.svzerod_3Dcoupling.to_json(sim_dir.svzerod_3Dcoupling.path)


def _normalize_solver_runscript(
    *,
    script_path: Path,
    nodes: int,
    procs_per_node: int,
    memory_gb: int,
    hours: int,
    partition: str,
    account: str | None,
    mail_user: str | None,
    mail_types: list[str] | None,
    svfsiplus_path: str,
) -> None:
    stage_dir = script_path.parent.resolve()
    output_path = stage_dir / "svFlowSolver.o%j"
    error_path = stage_dir / "svFlowSolver.e%j"
    lines = script_path.read_text(encoding="utf-8", errors="replace").splitlines()
    body: list[str] = []
    strip_existing_launch_tail = False
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("#!/"):
            continue
        if stripped.startswith("#SBATCH"):
            continue
        if "--mail-user" in stripped or "--mail-type" in stripped:
            continue
        if (
            stripped.startswith("cd ")
            or stripped.startswith('if [ -n "${{SLURM_CPUS_PER_TASK:-}}" ]')
            or stripped.startswith("srun ")
        ):
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


def _extract_result_step(path: Path) -> int:
    stem = path.stem
    token = stem.split("_")[-1]
    return int(token) if token.isdigit() else -1


def _result_vtus(result_dir: Path) -> list[Path]:
    return sorted(result_dir.glob("result_*.vtu"), key=_extract_result_step)


def _result_step_range(result_dir: Path) -> tuple[int, int, int]:
    vtus = _result_vtus(result_dir)
    if not vtus:
        raise RuntimeError(f"no result_*.vtu files found in {{result_dir}}")
    steps = [_extract_result_step(path) for path in vtus]
    if any(step < 0 for step in steps):
        raise RuntimeError(f"could not parse timestep IDs from result_*.vtu files in {{result_dir}}")
    if len(steps) == 1:
        return steps[0], steps[0], 1
    diffs = [right - left for left, right in zip(steps, steps[1:])]
    unique_diffs = sorted(set(diffs))
    if len(unique_diffs) != 1 or unique_diffs[0] <= 0:
        raise RuntimeError(
            "steady mean VTUs must be evenly spaced for traction averaging "
            f"(steps={{steps}})"
        )
    return steps[0], steps[-1], unique_diffs[0]


def _latest_result_vtu(root: Path) -> Path | None:
    candidates = [Path(path) for path in glob.glob(str(root / "*-procs" / "result_*.vtu"))]
    if not candidates:
        candidates = [Path(path) for path in glob.glob(str(root / "result_*.vtu"))]
    if not candidates:
        return None
    return max(candidates, key=lambda path: (_extract_result_step(path), path.stat().st_mtime))


def _result_dir_from_latest(root: Path) -> Path:
    latest = _latest_result_vtu(root)
    if latest is None:
        raise RuntimeError(f"no result_*.vtu files found under {{root}}")
    return latest.parent


def _force_xml_text(xml_path: Path, tag: str, value: str) -> None:
    if not xml_path.exists():
        raise RuntimeError(f"expected XML file missing: {{xml_path}}")
    tree = ET.parse(xml_path)
    root = tree.getroot()
    node = root.find(f".//{{tag}}")
    if node is None:
        raise RuntimeError(f"{{xml_path}} missing required XML tag {{tag}}")
    node.text = value
    ET.indent(root)
    tree.write(xml_path, encoding="utf-8", xml_declaration=True)


def _resolve_slurm_mail_user(execution_config: dict) -> str | None:
    slurm = execution_config.get("slurm") or {{}}
    if not isinstance(slurm, dict):
        return None
    value = slurm.get("mail_user")
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _resolve_slurm_mail_types(execution_config: dict) -> list[str]:
    slurm = execution_config.get("slurm") or {{}}
    if not isinstance(slurm, dict):
        return ["begin", "end"]
    value = slurm.get("mail_types", ["begin", "end"])
    if not isinstance(value, list):
        return ["begin", "end"]
    normalized = [str(item).strip() for item in value if str(item).strip()]
    return normalized or ["begin", "end"]


_NESTED_SBATCH_STRIP_ENV_VARS = (
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
)


def _nested_sbatch_env() -> dict:
    env = os.environ.copy()
    for name in _NESTED_SBATCH_STRIP_ENV_VARS:
        env.pop(name, None)
    return env


def _submit_job(script_path: Path) -> str:
    proc = subprocess.run(
        ["sbatch", "--parsable", "--chdir", str(script_path.parent), script_path.name],
        cwd=script_path.parent,
        capture_output=True,
        env=_nested_sbatch_env(),
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"sbatch failed rc={{proc.returncode}}: {{proc.stderr.strip()}}")
    stdout = proc.stdout.strip()
    if not stdout:
        raise RuntimeError("sbatch returned empty stdout")
    return stdout.split(";")[0].strip()


def _query_state(job_id: str) -> tuple:
    squeue = subprocess.run(
        ["squeue", "--job", job_id, "--noheader", "--format", "%T"],
        capture_output=True, text=True, check=False,
    )
    if squeue.returncode == 0:
        raw = squeue.stdout.strip().splitlines()
        if raw:
            return raw[0].strip().split()[0].strip().upper(), "squeue"
    sacct = subprocess.run(
        ["sacct", "-j", job_id, "--noheader", "--format", "State"],
        capture_output=True, text=True, check=False,
    )
    if sacct.returncode == 0:
        for line in sacct.stdout.splitlines():
            cleaned = line.strip()
            if not cleaned:
                continue
            state = cleaned.split()[0].split("+")[0].strip().upper()
            if state:
                return state, "sacct"
    return None, "unknown"


def _wait_for_completion(job_id: str, poll_seconds: int, timeout_seconds: int) -> tuple:
    success_states = {{"COMPLETED"}}
    failure_states = {{"FAILED", "CANCELLED", "TIMEOUT", "PREEMPTED", "OUT_OF_MEMORY", "NODE_FAIL", "BOOT_FAIL", "DEADLINE"}}
    active_states = {{"PENDING", "RUNNING", "CONFIGURING", "COMPLETING", "SUSPENDED", "RESIZING", "REQUEUED", "REQUEUE_HOLD", "SIGNALING", "SPECIAL_EXIT", "STAGE_OUT", "STOPPED"}}
    start = time.monotonic()
    last_state = None
    while True:
        elapsed = int(time.monotonic() - start)
        if elapsed > timeout_seconds:
            return False, f"timeout after {{timeout_seconds}}s (last_state={{last_state or 'unknown'}})"
        state, source = _query_state(job_id)
        if state:
            last_state = state
            print(f"[svzt] job {{job_id}} state: {{state}} ({{source}})", flush=True)
            if state in success_states:
                return True, state
            if state in failure_states:
                return False, state
            if state not in active_states:
                return False, f"unexpected terminal scheduler state: {{state}}"
        time.sleep(max(poll_seconds, 5))


def _generate_postop_prestress_file() -> Path:
    if str(threed_config.get("wall_model", "rigid")).lower() != "deformable":
        raise RuntimeError("postop prestress generation requires wall_model=deformable")
    if not tuned_clinical_targets.exists():
        raise RuntimeError(f"clinical targets file missing: {{tuned_clinical_targets}}")

    helper_sim = Simulation(
        path=str(remote_postop_dir),
        clinical_targets=str(tuned_clinical_targets),
        preop_dir=str(simulation_dir),
        postop_dir=str(simulation_dir),
        adapted_dir=None,
        zerod_config=str(tuned_zerod),
        convert_to_cm=False,
        mesh_scale_factor=mesh_scale_factor,
        wall_model=str(threed_config.get("wall_model", "deformable")),
        elasticity_modulus=float(threed_config.get("elasticity_modulus", 5062674.563165)),
        poisson_ratio=float(threed_config.get("poisson_ratio", 0.5)),
        shell_thickness=float(threed_config.get("shell_thickness", 0.12)),
        prestress_file=None,
        prestress_file_path=None,
        tissue_support=threed_config.get("tissue_support"),
        inflow_path={json.dumps(str(inflow_csv)) if inflow_csv else "None"},
        execution_config=solver_execution,
    )
    if helper_sim.inflow is None:
        raise RuntimeError("postop prestress generation requires an inflow waveform")

    steady_root = remote_postop_dir / "steady"
    prestress_root = remote_postop_dir / "prestress"
    existing = _latest_result_vtu(prestress_root)
    if existing is not None:
        print(f"[svzt] Reusing existing prestress result: {{existing}}", flush=True)
        return existing
    _safe_remove(steady_root)
    _safe_remove(prestress_root)
    steady_root.mkdir(parents=True, exist_ok=True)
    prestress_root.mkdir(parents=True, exist_ok=True)

    mean_dir = steady_root / "mean"
    mean_dir.mkdir(parents=True, exist_ok=True)
    mean_flow = float(np.mean(np.asarray(helper_sim.inflow.q, dtype=float)))
    mean_sim = SimulationDirectory.from_directory(
        path=str(mean_dir),
        mesh_complete=str(mesh_target),
        convert_to_cm=helper_sim.convert_to_cm,
        mesh_scale_factor=mesh_scale_factor,
    )
    mean_sim.generate_steady_sim(flow_rate=mean_flow, execution_config=solver_execution)
    mean_script = mean_dir / "run_solver.sh"
    _normalize_solver_runscript(
        script_path=mean_script,
        nodes=2,
        procs_per_node=24,
        memory_gb=int(threed_config.get("memory", 16)),
        hours=min(int(threed_config.get("hours", 20)), 6),
        partition=partition,
        account=account,
        mail_user=_resolve_slurm_mail_user(solver_execution),
        mail_types=_resolve_slurm_mail_types(solver_execution),
        svfsiplus_path=cluster_svfsiplus_path,
    )
    print("[svzt] Submitting mean steady simulation for prestress traction...", flush=True)
    mean_job_id = _submit_job(mean_script)
    print(f"[svzt] Mean steady job submitted: {{mean_job_id}}", flush=True)
    ok, terminal = _wait_for_completion(mean_job_id, poll_seconds, 21600)
    if not ok:
        raise RuntimeError(f"mean steady simulation did not complete successfully: {{terminal}}")
    print("[svzt] Mean steady simulation completed.", flush=True)

    wall_path = mesh_target / "walls_combined.vtp"
    if not wall_path.exists():
        raise RuntimeError(f"wall file missing for postop prestress generation: {{wall_path}}")
    mean_result_dir = _result_dir_from_latest(mean_dir)
    start, stop, stride = _result_step_range(mean_result_dir)
    traction_script = Path.home() / "scripts" / "calc_mean_wall_traction.py"
    if not traction_script.exists():
        raise RuntimeError(f"mean wall traction script missing: {{traction_script}}")

    traction_file = prestress_root / "rigid_wall_mean_traction.vtp"
    proc = subprocess.run(
        [
            os.fspath(Path(os.sys.executable)),
            str(traction_script),
            "--result-dir",
            str(mean_result_dir),
            "--wall",
            str(wall_path),
            "--start",
            str(start),
            "--stop",
            str(stop),
            "--stride",
            str(stride),
        ],
        cwd=prestress_root,
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            "mean wall traction calculation failed "
            f"rc={{proc.returncode}}: {{proc.stderr.strip() or proc.stdout.strip()}}"
        )
    if not traction_file.exists():
        raise RuntimeError(f"mean wall traction script did not write {{traction_file}}")

    prestress_sim = SimulationDirectory.from_directory(
        path=str(prestress_root),
        mesh_complete=str(mesh_target),
        mesh_scale_factor=mesh_scale_factor,
    )
    if getattr(prestress_sim, "svzerod_3Dcoupling", None) is not None:
        prestress_sim.svzerod_3Dcoupling.to_json(prestress_sim.svzerod_3Dcoupling.path)
    prestress_config = {{
        "n_tsteps": 20,
        "dt": 0.001,
        "vtk_save_increment": 1,
        "nodes": 1,
        "procs_per_node": 1,
        "memory": int(threed_config.get("memory", 16)),
        "hours": int(threed_config.get("hours", 20)),
        "simulation_mode": "prestress",
        "traction_file_path": str(traction_file),
        "wall_model": "deformable",
        "elasticity_modulus": threed_config.get("elasticity_modulus"),
        "poisson_ratio": threed_config.get("poisson_ratio"),
        "shell_thickness": threed_config.get("shell_thickness"),
        "tissue_support": threed_config.get("tissue_support"),
        "execution": solver_execution,
    }}
    prestress_sim.write_files(
        simname="Postop Prestress Simulation",
        user_input=False,
        sim_config=prestress_config,
    )
    _force_xml_text(prestress_root / "svFSIplus.xml", "Increment_in_saving_VTK_files", "1")
    prestress_script = prestress_root / "run_solver.sh"
    _normalize_solver_runscript(
        script_path=prestress_script,
        nodes=1,
        procs_per_node=1,
        memory_gb=int(prestress_config["memory"]),
        hours=int(prestress_config["hours"]),
        partition=partition,
        account=account,
        mail_user=_resolve_slurm_mail_user(solver_execution),
        mail_types=_resolve_slurm_mail_types(solver_execution),
        svfsiplus_path=cluster_svfsiplus_path,
    )
    print("[svzt] Submitting prestress simulation...", flush=True)
    prestress_job_id = _submit_job(prestress_script)
    print(f"[svzt] Prestress job submitted: {{prestress_job_id}}", flush=True)
    ok, terminal = _wait_for_completion(prestress_job_id, poll_seconds, cmm_timeout_seconds)
    if not ok:
        raise RuntimeError(f"prestress simulation did not complete successfully: {{terminal}}")
    print("[svzt] Prestress simulation completed.", flush=True)
    generated = _latest_result_vtu(prestress_root)
    if generated is None:
        raise RuntimeError(f"prestress simulation completed but no result_*.vtu found in {{prestress_root}}")
    return generated


if str(threed_config.get("wall_model", "rigid")).lower() == "deformable":
    threed_config["prestress_file_path"] = str(_generate_postop_prestress_file())

sim = SimulationDirectory.from_directory(
    path=str(simulation_dir),
    zerod_config=str(tuned_zerod),
    mesh_complete=str(mesh_target),
    threed_coupler=str(canonical_coupler),
    mesh_scale_factor=mesh_scale_factor,
)
_sync_postop_inflow(sim)
sim.write_files(simname="Postop Simulation", user_input=False, sim_config=threed_config)
run_solver_path = simulation_dir / "run_solver.sh"
_normalize_solver_runscript(
    script_path=run_solver_path,
    nodes=int(threed_config.get("nodes", 3)),
    procs_per_node=int(threed_config.get("procs_per_node", 24)),
    memory_gb=int(threed_config.get("memory", 16)),
    hours=int(threed_config.get("hours", 20)),
    partition=partition,
    account=account,
    mail_user=_resolve_slurm_mail_user(solver_execution),
    mail_types=_resolve_slurm_mail_types(solver_execution),
    svfsiplus_path=cluster_svfsiplus_path,
)
print("[svzt] Submitting postop CMM simulation...", flush=True)
cmm_job_id = _submit_job(run_solver_path)
print(f"[svzt] CMM job submitted: {{cmm_job_id}}", flush=True)
ok, terminal = _wait_for_completion(cmm_job_id, poll_seconds, cmm_timeout_seconds)
if not ok:
    raise RuntimeError(f"postop CMM simulation did not complete successfully: {{terminal}}")
print("[svzt] CMM simulation completed. Submitting postprocessing job...", flush=True)

postprocess_script_content = json.loads({postprocess_script_json})
postprocess_script_path = remote_postop_dir / "run_postop_postprocess.sh"
postprocess_script_path.write_text(postprocess_script_content, encoding="utf-8")
postprocess_script_path.chmod(0o755)
postprocess_job_id = _submit_job(postprocess_script_path)
print(f"[svzt] Postprocessing job submitted: {{postprocess_job_id}}", flush=True)

paraview_job_id = None
if paraview_enabled:
    paraview_root.mkdir(parents=True, exist_ok=True)
    paraview_logs_dir.mkdir(parents=True, exist_ok=True)
    paraview_output_dir.mkdir(parents=True, exist_ok=True)
    paraview_script_path.write_text(paraview_script_content, encoding="utf-8")
    paraview_slurm_path.write_text(paraview_slurm_content, encoding="utf-8")
    paraview_slurm_path.chmod(0o755)
    print(f"[svzt] Submitting {{paraview_stage}} ParaView visualization...", flush=True)
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
    print(f"[svzt] ParaView visualization job submitted: {{paraview_job_id}}", flush=True)
else:
    print(f"[svzt] ParaView visualization skipped: {{paraview_skip_reason}}", flush=True)

payload = {{
    "source_preop_iteration": {selected.iteration},
    "source_tuned_zerod_config": str(tuned_zerod),
    "source_canonical_coupler": str(canonical_coupler),
    "postop_mesh_complete": str(mesh_complete),
    "simulation_dir": str(simulation_dir),
    "prestress_file_path": threed_config.get("prestress_file_path"),
    "cmm_job_id": cmm_job_id,
    "postprocess_job_id": postprocess_job_id,
    "paraview_viz_job_id": paraview_job_id,
    "paraview_viz_submission_json": str(paraview_submission_metadata_path) if paraview_enabled else None,
}}
submission_path = remote_results_dir / "postop_submission.json"
submission_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
print("[svzt] Postop manager job complete.", flush=True)
PY
"""


def run_postop(
    workspace_root: str | Path,
    run_id: str,
    *,
    mode: ExecutionMode = ExecutionMode.DRY_RUN,
    transfer_adapter: FileTransferAdapter | None = None,
    scheduler_adapter: SchedulerAdapter | None = None,
    remote_exec_adapter: RemoteExecAdapter | None = None,
) -> PostopRunResult:
    root = detect_workspace_root(workspace_root)
    validated_run_id = validate_run_id(run_id)
    local_paths = build_local_run_paths(root, validated_run_id)
    manifest = read_manifest(local_paths.manifest)
    if manifest.converged_preop_iteration is None:
        raise ConfigError(
            f"run '{validated_run_id}' has no converged_preop_iteration; run svzt preop select first"
        )
    manifest, selected = _refresh_selected_reduced_config_from_local_iteration(
        manifest=manifest,
        local_paths=local_paths,
    )
    if selected is None:
        raise ConfigError(
            f"run '{validated_run_id}' has no converged_preop_iteration; run svzt preop select first"
        )
    config, cluster = _resolve_cluster_for_manifest(root, manifest)
    remote_run_dir = manifest.execution.remote_run_dir or manifest.remote.get("remote_run_dir")
    if not remote_run_dir:
        raise ConfigError(f"run '{validated_run_id}' is missing remote_run_dir")
    remote_layout = _postop_remote_layout(
        remote_run_dir=str(remote_run_dir),
        iteration=selected.iteration,
    )
    for path in remote_layout.values():
        validate_remote_write_path(
            path,
            cluster.remote_roots.runs_root,
            cluster.remote_roots.patient_data_root,
        )

    postop_paths = _postop_local_paths(local_paths, selected.iteration)
    for path in ("root", "inputs", "logs", "results"):
        postop_paths[path].mkdir(parents=True, exist_ok=True)

    plan = _build_postop_plan(
        manifest=manifest,
        cluster=cluster,
        local_paths=local_paths,
        postop_paths=postop_paths,
        remote_layout=remote_layout,
    )
    write_plan_json(plan, postop_paths["plan_json"])
    write_plan_yaml(plan, postop_paths["plan_yaml"])

    selected_payload = selected.model_dump(mode="json")
    (postop_paths["inputs"] / "converged_preop_iteration.yaml").write_text(
        yaml.safe_dump(selected_payload, sort_keys=True),
        encoding="utf-8",
    )
    prepared_paraview_job = None
    resolved_paraview_skip_reason = _paraview_skip_reason(
        config=config,
        cluster=cluster,
        manifest=manifest,
    )
    if resolved_paraview_skip_reason is None:
        prepared_paraview_job = _prepare_postop_paraview_viz_job(
            workspace_root=root,
            run_id=validated_run_id,
            manifest=manifest,
            cluster=cluster,
            remote_postop_dir=remote_layout["remote_postop_dir"],
            source_iteration=selected.iteration,
        )
    script_body = _render_postop_job_script(
        manifest=manifest,
        selected=selected,
        remote_layout=remote_layout,
        paraview_job=prepared_paraview_job,
        paraview_skip_reason=resolved_paraview_skip_reason,
    )
    postop_paths["job_script"].write_text(script_body, encoding="utf-8")

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
            submit_options=SlurmSubmitOptions(job_name=validated_run_id),
        )

    command_results = [
        transfer_adapter.ensure_remote_dir(remote_layout["remote_postop_dir"]),
        transfer_adapter.ensure_remote_dir(remote_layout["remote_inputs_dir"]),
        transfer_adapter.ensure_remote_dir(remote_layout["remote_logs_dir"]),
        transfer_adapter.ensure_remote_dir(remote_layout["remote_results_dir"]),
        transfer_adapter.sync(
            local_dir=str(postop_paths["inputs"]),
            remote_dir=remote_layout["remote_inputs_dir"],
            include=config.defaults.rsync.include_patterns,
            exclude=config.defaults.rsync.exclude_patterns,
            direction=SyncDirection.PUSH,
        ),
        transfer_adapter.push(
            str(postop_paths["job_script"]),
            remote_layout["remote_job_script_path"],
        ),
    ]
    submit_result = scheduler_adapter.submit(remote_layout["remote_job_script_path"])
    command_results.append(submit_result.command)

    manifest = read_manifest(local_paths.manifest)
    manifest.artifacts["postop_plan_files"] = [
        str(postop_paths["plan_json"]),
        str(postop_paths["plan_yaml"]),
    ]
    if mode == ExecutionMode.EXECUTE:
        manifest = record_submission(
            manifest,
            remote_run_dir=str(remote_run_dir),
            job_script_path=remote_layout["remote_job_script_path"],
            scheduler_type=cluster.scheduler.type,
            submitted_job_id=submit_result.job_id,
            mode=mode.value,
        )
        manifest = record_postop_submission(
            manifest,
            source_preop_iteration=selected.iteration,
            local_dir=str(postop_paths["root"]),
            remote_dir=remote_layout["remote_postop_dir"],
            local_job_script_path=str(postop_paths["job_script"]),
            remote_job_script_path=remote_layout["remote_job_script_path"],
            postop_job_id=submit_result.job_id,
            note="Explicit postop workflow submitted",
        )
        manifest = record_postprocess_submission(
            manifest,
            field_name="postop_postprocess",
            stage="postop",
            source_preop_iteration=selected.iteration,
            local_dir=str(postop_paths["results"] / "postprocess"),
            remote_dir=str(PurePosixPath(remote_layout["remote_results_dir"]) / "postprocess"),
            local_job_script_path=str(postop_paths["job_script"]),
            remote_job_script_path=remote_layout["remote_job_script_path"],
            scheduler_job_id=submit_result.job_id,
            note="Postop postprocess runs inside submitted postop job",
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
                note="ParaView visualization is submitted by the postop manager job after CMM completion",
            )
    else:
        manifest.postop_run = manifest.postop_run or None
    write_manifest(manifest, local_paths.manifest)

    return PostopRunResult(
        run_id=validated_run_id,
        source_preop_iteration=selected.iteration,
        mode=mode,
        plan_path=postop_paths["plan_yaml"],
        remote_postop_dir=remote_layout["remote_postop_dir"],
        remote_job_script_path=remote_layout["remote_job_script_path"],
        local_job_script_path=postop_paths["job_script"],
        submitted_job_id=submit_result.job_id,
        command_previews=[item.argv for item in command_results],
    )
