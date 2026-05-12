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
from svztagent.workflows.postprocess import _load_stage_target_payload, _python_bootstrap, submit_selected_preop_postprocess
from svztagent.workflows.tune_trees import _build_default_adapters


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


def select_converged_preop_iteration(
    workspace_root: str | Path,
    run_id: str,
    *,
    iteration: int,
    reason: str | None = None,
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
        remote_tuned_zerod_config=tuned_config,
        remote_canonical_coupler=remote_canonical_coupler,
        preop_job_id=preop_job_id,
    )
    write_manifest(updated, local_paths.manifest)

    postprocess_result = submit_selected_preop_postprocess(
        workspace_root=root,
        run_id=validated_run_id,
        iteration=iteration,
        transfer_adapter=transfer_adapter,
        scheduler_adapter=scheduler_adapter,
        remote_exec_adapter=remote_exec_adapter,
    )

    return PreopSelectionResult(
        run_id=validated_run_id,
        iteration=iteration,
        selection_kind=selection_kind,
        remote_preop_dir=remote_preop_dir,
        remote_tuned_zerod_config=tuned_config,
        remote_canonical_coupler=remote_canonical_coupler,
        postprocess_job_id=postprocess_result.submitted_job_id,
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
            outputs={"tuned_zerod_config": selected.remote_tuned_zerod_config},
            remote_paths={"read": [selected.remote_tuned_zerod_config, selected.remote_preop_dir], "write": []},
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
            remote_paths={"read": [selected.remote_tuned_zerod_config, postop_mesh], "write": [remote_layout["remote_job_script_path"]]},
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


def _render_postop_job_script(*, manifest, selected, remote_layout: dict[str, str]) -> str:
    svzerodtrees_paths = manifest.remote.get("svzerodtrees_paths", {})
    postop_mesh = svzerodtrees_paths.get("postop_mesh_complete")
    threed_config = manifest.remote.get("threed_defaults", {})
    mesh_scale_factor = manifest.patient.get("mesh_scale_factor") or 1.0
    patient_alias = str(manifest.patient.get("alias"))
    cluster_name = str(manifest.cluster.get("name"))
    workspace_root = detect_workspace_root(Path(manifest.local_paths.run_dir).parents[1])
    config = load_workspace_config(workspace_root)
    cluster = resolve_cluster(config, cluster_name)
    clinical_targets_payload = _load_stage_target_payload(workspace_root, patient_alias=patient_alias, stage="postop")
    svslicer_path = cluster.executables.svslicer_path
    fallback_clinical_targets = None
    bootstrap = _python_bootstrap(config)
    if svslicer_path is None:
        raise ConfigError("cluster executables.svslicer_path is required for postop postprocessing")
    centerline = svzerodtrees_paths.get("centerlines")
    if not centerline:
        raise ConfigError("manifest remote.svzerodtrees_paths.centerlines is required for postop postprocessing")
    inflow_csv = svzerodtrees_paths.get("inflow")
    return f"""#!/usr/bin/env bash
set -euo pipefail

mkdir -p "{remote_layout["remote_inputs_dir"]}" "{remote_layout["remote_results_dir"]}" "{remote_layout["remote_logs_dir"]}"

{bootstrap}

"${{PYTHON_BIN}}" - <<'PY'
from pathlib import Path
import json
import shutil

from svzerodtrees.simulation import SimulationDirectory
from svzerodtrees.post_processing import run_pulmonary_threed_postprocess_suite

remote_postop_dir = Path({json.dumps(remote_layout["remote_postop_dir"])})
remote_results_dir = Path({json.dumps(remote_layout["remote_results_dir"])})
mesh_complete = Path({json.dumps(str(postop_mesh))})
tuned_zerod = Path({json.dumps(selected.remote_tuned_zerod_config)})
threed_config = json.loads({json.dumps(json.dumps(threed_config, sort_keys=True))})
mesh_scale_factor = float({json.dumps(str(mesh_scale_factor))})

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

sim = SimulationDirectory.from_directory(
    path=str(simulation_dir),
    zerod_config=str(tuned_zerod),
    mesh_complete=str(mesh_target),
    threed_coupler=True,
    mesh_scale_factor=mesh_scale_factor,
)
sim.write_files(simname="Postop Simulation", user_input=False, sim_config=threed_config)
local_execution = dict(threed_config.get("execution", {{}}))
local_execution["mode"] = "local"
local_execution["executable"] = {json.dumps(cluster.executables.svfsiplus_path)}
sim.run(execution_config=local_execution)
payload = {{
    "source_preop_iteration": {selected.iteration},
    "source_tuned_zerod_config": str(tuned_zerod),
    "postop_mesh_complete": str(mesh_complete),
    "simulation_dir": str(simulation_dir),
}}
postprocess_result = run_pulmonary_threed_postprocess_suite(
    simulation_dir=str(simulation_dir),
    output_dir=str(remote_results_dir / "postprocess"),
    centerline={json.dumps(str(centerline))},
    stage="postop",
    svslicer_path={json.dumps(svslicer_path)},
    clinical_targets={json.dumps(clinical_targets_payload, sort_keys=True) if clinical_targets_payload is not None else "None"},
    inflow_csv={json.dumps(str(inflow_csv)) if inflow_csv else "None"},
)
payload["postprocess"] = postprocess_result
(remote_results_dir / "postop_submission.json").write_text(
    json.dumps(payload, indent=2, sort_keys=True),
    encoding="utf-8",
)
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
    selected = manifest.converged_preop_iteration
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
    script_body = _render_postop_job_script(
        manifest=manifest,
        selected=selected,
        remote_layout=remote_layout,
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
