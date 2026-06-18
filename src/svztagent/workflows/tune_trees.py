"""Tune trees workflow orchestration for planning and controlled execution."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from importlib.resources import files
from pathlib import Path, PurePosixPath
import json
import shutil
from typing import Callable

import yaml

from svztagent.config.load import (
    detect_workspace_root,
    load_workspace_config,
    resolve_cluster,
    resolve_patient_alias,
)
from svztagent.core.errors import AdapterExecutionError, ConfigError
from svztagent.core.monitor import (
    MonitorObservation,
    MonitorSettings,
    RunMonitorService,
    poll_scheduler_state,
)
from svztagent.core.manifest import (
    advance_iteration,
    copy_config_snapshot,
    create_manifest,
    mark_iteration_decision,
    mark_iteration_submitted,
    mark_progress_milestone,
    read_manifest,
    record_iteration_scheduler_state,
    record_lifecycle_transition,
    record_poll_observation,
    record_fetch,
    record_plan_path,
    record_submission,
    resolve_submitted_job_id,
    write_manifest,
)
from svztagent.core.paths import (
    LocalRunPaths,
    build_iteration_local_paths,
    build_local_run_paths,
    ensure_local_run_dirs,
    iteration_dir_name,
    validate_remote_write_path,
    validate_run_id,
)
from svztagent.core.plan import (
    ExecutionPlan,
    PlanStep,
    StepCategory,
    load_plan_yaml,
    utc_now_iso,
    write_plan_json,
    write_plan_yaml,
)
from svztagent.core.plan_render import render_execution_plan
from svztagent.core.plan_validate import assert_valid_execution_plan
from svztagent.core.state import RunLifecycleState, coerce_run_lifecycle_state
from svztagent.core.status import NormalizedRunState
from svztagent.hpc.executor import CommandExecutor
from svztagent.hpc.interfaces import (
    ExecutionMode,
    FileTransferAdapter,
    RemoteExecAdapter,
    SchedulerAdapter,
    SyncDirection,
)
from svztagent.hpc.rsync import RsyncFileTransferAdapter
from svztagent.hpc.slurm import SlurmSchedulerAdapter, SlurmSubmitOptions
from svztagent.hpc.ssh import SshRemoteExecAdapter

REDUCED_SEED_INPUT_FILENAME = "simplified_nonlinear_zerod.json"
FULL_PA_SEED_INPUT_FILENAME = "full_pa_zerod.json"


def _effective_tuning_model(tuning_model: str | None, iteration: int) -> str:
    normalized = str(tuning_model or "rri").strip().lower()
    if normalized == "full_pa" and iteration > 1:
        return "rri"
    return normalized


def _seed_input_filename(tuning_model: str | None = None, iteration: int = 1) -> str:
    if _effective_tuning_model(tuning_model, iteration) == "full_pa":
        return FULL_PA_SEED_INPUT_FILENAME
    return REDUCED_SEED_INPUT_FILENAME


def _iteration_impedance_config(impedance_config: dict, iteration: int) -> dict:
    rendered = dict(impedance_config)
    rendered["tuning_model"] = _effective_tuning_model(
        rendered.get("tuning_model"),
        iteration,
    )
    if float(rendered.get("diameter_scale") or 0.0) > 0.0:
        rendered["use_mean"] = False
    return rendered


def _emit_progress(
    progress_callback: Callable[[str], None] | None,
    message: str,
) -> None:
    if progress_callback is not None:
        progress_callback(message)


@dataclass(frozen=True)
class TuneExecutionResult:
    run_id: str
    iteration: int
    mode: ExecutionMode
    plan_path: Path
    remote_run_dir: str
    remote_job_script_path: str
    local_job_script_path: Path
    submitted_job_id: str
    command_previews: list[list[str]]


@dataclass(frozen=True)
class StatusQueryResult:
    run_id: str
    job_id: str
    raw_state: str | None
    normalized_state: NormalizedRunState
    source: str
    active_workflow: str = "tune"
    current_iteration: int = 1
    max_iterations: int = 1
    tracker_status: str = "active"
    stage_key: str = "startup"
    stage_label: str = "Starting iteration"
    stage_detail: str | None = None
    decision: str | None = None
    needs_review_reason: str | None = None
    progress_source: str = "unavailable"
    progress_warnings: list[str] | None = None
    preop_job_id: str | None = None
    preop_job_state_raw: str | None = None
    preop_job_state_normalized: NormalizedRunState | None = None
    postop_job_id: str | None = None
    postop_job_state_raw: str | None = None
    postop_job_state_normalized: NormalizedRunState | None = None
    adaptation_model: str | None = None
    adaptation_parameter_set: str | None = None
    adaptation_job_id: str | None = None
    adaptation_job_state_raw: str | None = None
    adaptation_job_state_normalized: NormalizedRunState | None = None
    failure_error_log_path: str | None = None
    failure_error_log_tail: str | None = None


@dataclass(frozen=True)
class IterationProgressArtifacts:
    driver_log: dict | None
    decision: dict | None
    metrics: dict | None
    source: str
    warnings: list[str]


@dataclass(frozen=True)
class ChildJobSnapshot:
    job_id: str
    raw_state: str | None
    normalized_state: NormalizedRunState | None


@dataclass(frozen=True)
class StageSnapshot:
    key: str
    label: str
    detail: str | None = None


@dataclass(frozen=True)
class FetchResult:
    run_id: str
    remote_run_dir: str
    local_output_dir: str
    pull_patterns: list[str]
    command_preview: list[str]


@dataclass(frozen=True)
class AdvanceIterationResult:
    run_id: str
    previous_iteration: int
    next_iteration: int | None
    tracker_status: str
    action: str
    submitted_job_id: str | None = None


@dataclass(frozen=True)
class WatchResult:
    run_id: str
    job_id: str
    initial_state: RunLifecycleState
    final_state: RunLifecycleState
    terminal_state: RunLifecycleState
    raw_scheduler_state: str | None
    terminal_reason: str | None
    poll_count: int
    remote_run_dir: str
    local_run_dir: str
    local_logs_dir: str
    remote_logs_dir: str | None
    job_script_path: str | None
    fetch_attempted: bool
    fetch_succeeded: bool | None
    fetch_error: str | None
    observations: list[MonitorObservation]


@dataclass(frozen=True)
class IterationDecisionPullResult:
    run_id: str
    iteration: int
    remote_results_dir: str
    local_results_dir: Path
    decision_path: Path
    metrics_path: Path
    decision: str | None
    command_preview: list[str]


@dataclass(frozen=True)
class AutoAdvanceIterationRecord:
    iteration: int
    terminal_state: RunLifecycleState
    decision: str | None
    advance_action: str
    submitted_job_id: str | None = None


@dataclass(frozen=True)
class AutoAdvanceResult:
    run_id: str
    final_action: str
    tracker_status: str
    final_iteration: int
    final_terminal_state: RunLifecycleState
    iterations: list[AutoAdvanceIterationRecord]


def generate_run_id(now: datetime | None = None) -> str:
    now = now or datetime.now(UTC)
    return f"run-{now.strftime('%Y%m%d-%H%M%S')}"


def _resolve_iteration(manifest, iteration: int | None) -> int:
    if iteration is not None:
        if iteration <= 0:
            raise ConfigError("iteration must be a positive integer")
        return iteration
    return manifest.tuning_iteration_tracker.current_iteration


def _iteration_remote_layout(
    *,
    runs_root: str,
    run_id: str,
    iteration: int,
) -> dict[str, str]:
    remote_run_dir = str(PurePosixPath(runs_root) / run_id)
    iter_name = iteration_dir_name(iteration)
    remote_iter_dir = str(PurePosixPath(remote_run_dir) / "iterations" / iter_name)
    return {
        "remote_run_dir": remote_run_dir,
        "iter_name": iter_name,
        "remote_iter_dir": remote_iter_dir,
        "remote_inputs_dir": str(PurePosixPath(remote_iter_dir) / "inputs"),
        "remote_results_dir": str(PurePosixPath(remote_iter_dir) / "results"),
        "remote_logs_dir": str(PurePosixPath(remote_iter_dir) / "logs"),
        "remote_job_script_path": str(PurePosixPath(remote_iter_dir) / "run_tune_iter.sh"),
    }


def _load_iteration_decision(path: Path) -> dict | None:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as stream:
        return yaml.safe_load(stream) or {}


def _load_mapping_artifact(path: Path, *, label: str, warnings: list[str]) -> dict | None:
    if not path.exists():
        return None

    try:
        with path.open("r", encoding="utf-8") as stream:
            payload = yaml.safe_load(stream)
    except Exception as exc:
        warnings.append(f"failed to parse {label}: {exc}")
        return None

    if payload is None:
        return {}
    if not isinstance(payload, dict):
        warnings.append(f"{label} must contain a mapping")
        return None
    return payload


def _current_iteration_record(manifest):
    tracker = manifest.tuning_iteration_tracker
    return next(
        (record for record in tracker.iterations if record.iteration == tracker.current_iteration),
        None,
    )


def _latest_adaptation_run(manifest):
    if not getattr(manifest, "adaptation_runs", None):
        return None
    return manifest.adaptation_runs[-1]


def _resolve_iteration_progress_remote_dir(*, manifest, cluster, run_id: str, iteration: int) -> str:
    current_record = _current_iteration_record(manifest)
    if current_record is not None and current_record.remote_dir:
        return str(current_record.remote_dir)

    remote_run_dir = manifest.execution.remote_run_dir or manifest.remote.get("remote_run_dir")
    if remote_run_dir:
        return str(PurePosixPath(str(remote_run_dir)) / "iterations" / iteration_dir_name(iteration))

    return _iteration_remote_layout(
        runs_root=cluster.remote_roots.runs_root,
        run_id=run_id,
        iteration=iteration,
    )["remote_iter_dir"]


def _load_iteration_progress_artifacts(
    *,
    run_id: str,
    iteration: int,
    local_paths: LocalRunPaths,
    manifest,
    cluster,
    transfer_adapter: FileTransferAdapter,
) -> IterationProgressArtifacts:
    warnings: list[str] = []
    local_iteration_paths = build_iteration_local_paths(local_paths, iteration)

    def _read_local() -> tuple[dict | None, dict | None, dict | None]:
        driver_log = _load_mapping_artifact(
            local_iteration_paths["logs"] / "iteration_driver_log.json",
            label="iteration_driver_log.json",
            warnings=warnings,
        )
        decision = _load_mapping_artifact(
            local_iteration_paths["decision"],
            label="iteration_decision.json",
            warnings=warnings,
        )
        metrics = _load_mapping_artifact(
            local_iteration_paths["metrics"],
            label="iteration_metrics.json",
            warnings=warnings,
        )
        return driver_log, decision, metrics

    driver_log, decision, metrics = _read_local()
    had_local_driver_log = driver_log is not None
    had_local_decision = decision is not None
    had_local_metrics = metrics is not None
    remote_pull_attempted = False

    if driver_log is None or decision is None or metrics is None:
        remote_iter_dir = _resolve_iteration_progress_remote_dir(
            manifest=manifest,
            cluster=cluster,
            run_id=run_id,
            iteration=iteration,
        )
        local_iteration_paths["logs"].mkdir(parents=True, exist_ok=True)
        local_iteration_paths["results"].mkdir(parents=True, exist_ok=True)

        remote_pull_attempted = True
        try:
            transfer_adapter.sync(
                local_dir=str(local_iteration_paths["logs"]),
                remote_dir=str(PurePosixPath(remote_iter_dir) / "logs"),
                include=["iteration_driver_log.json"],
                exclude=["*"],
                direction=SyncDirection.PULL,
            )
            transfer_adapter.sync(
                local_dir=str(local_iteration_paths["results"]),
                remote_dir=str(PurePosixPath(remote_iter_dir) / "results"),
                include=["iteration_decision.json", "iteration_metrics.json"],
                exclude=["*"],
                direction=SyncDirection.PULL,
            )
            driver_log, decision, metrics = _read_local()
        except Exception as exc:
            warnings.append(f"progress artifact pull failed for iter-{iteration:02d}: {exc}")

    loaded_any = any(payload is not None for payload in (driver_log, decision, metrics))
    loaded_via_remote_pull = (
        (not had_local_driver_log and driver_log is not None)
        or (not had_local_decision and decision is not None)
        or (not had_local_metrics and metrics is not None)
    )
    missing = [
        name
        for name, payload in (
            ("iteration_driver_log.json", driver_log),
            ("iteration_decision.json", decision),
            ("iteration_metrics.json", metrics),
        )
        if payload is None
    ]
    if missing:
        warnings.append(f"missing progress artifacts for iter-{iteration:02d}: {', '.join(missing)}")

    source = "unavailable"
    if loaded_any:
        source = "remote_pull" if loaded_via_remote_pull else "local"

    return IterationProgressArtifacts(
        driver_log=driver_log,
        decision=decision,
        metrics=metrics,
        source=source,
        warnings=warnings,
    )


def _extract_progress_steps(driver_log: dict | None) -> set[str]:
    raw_steps = driver_log.get("steps") if driver_log else None
    if not isinstance(raw_steps, list):
        return set()
    return {str(step) for step in raw_steps}


def _poll_child_job(
    *,
    job_id: str | None,
    scheduler_adapter: SchedulerAdapter,
    warnings: list[str],
    label: str,
) -> ChildJobSnapshot | None:
    if not job_id:
        return None

    try:
        result = poll_scheduler_state(scheduler_adapter, job_id)
    except Exception as exc:
        warnings.append(f"{label} job '{job_id}' status lookup failed: {exc}")
        return ChildJobSnapshot(
            job_id=job_id,
            raw_state=None,
            normalized_state=None,
        )

    return ChildJobSnapshot(
        job_id=job_id,
        raw_state=result.raw_state,
        normalized_state=result.normalized_state,
    )


def _format_job_state(state: NormalizedRunState | None, raw_state: str | None) -> str:
    if state is not None:
        return state.value
    if raw_state:
        return raw_state
    return "unknown"


def _derive_stage_snapshot(
    *,
    parent_state: NormalizedRunState,
    driver_log: dict | None,
    decision_payload: dict | None,
    preop_job: ChildJobSnapshot | None,
    postop_job: ChildJobSnapshot | None,
) -> StageSnapshot:
    steps = _extract_progress_steps(driver_log)
    decision = str(decision_payload.get("decision")) if decision_payload and decision_payload.get("decision") else None
    regenerated_config_path = (
        str(decision_payload.get("regenerated_config_path"))
        if decision_payload and decision_payload.get("regenerated_config_path")
        else None
    )
    postop_submission_requested = bool(
        decision_payload.get("postop_submission_requested", False) if decision_payload else False
    )
    needs_review_reason = (
        str(decision_payload.get("needs_review_reason"))
        if decision_payload and decision_payload.get("needs_review_reason")
        else None
    )
    driver_errors = driver_log.get("errors") if driver_log else None
    first_driver_error = (
        str(driver_errors[0])
        if isinstance(driver_errors, list) and driver_errors and driver_errors[0]
        else None
    )

    active_child_states = {RunLifecycleState.PENDING, RunLifecycleState.RUNNING}
    failed_child_states = {RunLifecycleState.FAILED, RunLifecycleState.CANCELLED}

    if postop_job is not None and postop_job.normalized_state in active_child_states:
        return StageSnapshot(
            key="postop_3d",
            label="3D postop simulation",
            detail=(
                f"Postop job {postop_job.job_id} is "
                f"{_format_job_state(postop_job.normalized_state, postop_job.raw_state)}."
            ),
        )
    if postop_job is not None and postop_job.normalized_state == RunLifecycleState.COMPLETED:
        return StageSnapshot(
            key="postop_complete",
            label="Postop submitted and completed",
            detail=f"Postop job {postop_job.job_id} completed.",
        )
    if postop_job is not None and postop_job.normalized_state in failed_child_states:
        return StageSnapshot(
            key="postop_failed",
            label="Postop submitted but failed",
            detail=(
                f"Postop job {postop_job.job_id} is "
                f"{_format_job_state(postop_job.normalized_state, postop_job.raw_state)}."
            ),
        )
    if decision == "needs_review":
        return StageSnapshot(
            key="needs_review",
            label="Needs review",
            detail=needs_review_reason or first_driver_error,
        )
    if decision == "not_close" and parent_state == RunLifecycleState.COMPLETED:
        detail = "Iteration finished with decision not_close."
        if regenerated_config_path:
            detail = f"Regenerated next-iteration seed: {regenerated_config_path}"
        return StageSnapshot(
            key="ready_for_next_iteration",
            label="Iteration complete, ready for next tuning iteration",
            detail=detail,
        )
    if decision == "converged":
        if postop_submission_requested and postop_job is not None:
            detail = f"Converged; postop job {postop_job.job_id} was submitted."
        elif postop_submission_requested:
            detail = "Converged; postop submission was requested."
        else:
            detail = "Converged; no postop submission was requested."
        return StageSnapshot(
            key="converged",
            label="Converged",
            detail=detail,
        )
    if "preop_completed" in steps:
        return StageSnapshot(
            key="post_preop_analysis",
            label="Post-preop analysis",
            detail="Computing metrics, clinical gating, and next-step outputs.",
        )
    preop_is_terminal = preop_job is not None and preop_job.normalized_state == RunLifecycleState.COMPLETED
    if "preop_submitted" in steps and not preop_is_terminal:
        detail = "Preop simulation is running."
        if preop_job is not None:
            detail = (
                f"Preop job {preop_job.job_id} is "
                f"{_format_job_state(preop_job.normalized_state, preop_job.raw_state)}."
            )
        return StageSnapshot(
            key="preop_3d",
            label="3D preop simulation",
            detail=detail,
        )
    if "preop_3d_setup_started" in steps and "preop_submitted" not in steps:
        return StageSnapshot(
            key="preop_setup",
            label="Preparing preop 3D simulation",
            detail="Building preop simulation inputs and submission artifacts.",
        )
    if "0d_tuning_started" in steps and "0d_tuning_completed" not in steps:
        return StageSnapshot(
            key="zerod_tuning",
            label="0D tuning",
            detail="Impedance tuning is in progress.",
        )
    if "0d_tuning_completed" in steps and "preop_3d_setup_started" not in steps:
        return StageSnapshot(
            key="post_zerod_validation",
            label="Validating tuned 0D result",
            detail="Checking tuned outputs before preop 3D setup.",
        )
    if parent_state == RunLifecycleState.PENDING:
        return StageSnapshot(
            key="queued",
            label="Queued",
            detail="Awaiting scheduler start.",
        )
    if parent_state in {RunLifecycleState.RUNNING, RunLifecycleState.UNKNOWN, RunLifecycleState.SUBMITTED}:
        return StageSnapshot(
            key="startup",
            label="Starting iteration",
            detail="Iteration job is active but progress artifacts are not available yet.",
        )
    return StageSnapshot(
        key="terminal_unknown",
        label="Iteration finished; progress details unavailable",
        detail="No iteration progress artifacts were available.",
    )


def init_run_workspace(
    workspace_root: str | Path,
    cluster_name: str,
    patient_alias: str,
    run_id: str,
) -> tuple[LocalRunPaths, dict]:
    root = detect_workspace_root(workspace_root)
    config = load_workspace_config(root)
    cluster = resolve_cluster(config, cluster_name)
    patient = resolve_patient_alias(config, cluster_name, patient_alias)

    validated_run_id = validate_run_id(run_id)
    local_paths = build_local_run_paths(root, validated_run_id)
    ensure_local_run_dirs(local_paths)
    copy_config_snapshot(root, local_paths.config_snapshot)

    manifest = create_manifest(
        run_id=validated_run_id,
        cluster=cluster,
        patient=patient,
        local_paths=local_paths,
        workspace_root=root,
        config=config,
    )
    write_manifest(manifest, local_paths.manifest)

    return local_paths, {
        "cluster": cluster,
        "patient": patient,
        "config": config,
        "manifest": manifest,
        "workspace_root": root,
    }


def load_or_init_run_workspace(
    workspace_root: str | Path,
    cluster_name: str,
    patient_alias: str,
    run_id: str,
) -> tuple[LocalRunPaths, dict]:
    root = detect_workspace_root(workspace_root)
    config = load_workspace_config(root)
    cluster = resolve_cluster(config, cluster_name)
    patient = resolve_patient_alias(config, cluster_name, patient_alias)
    validated_run_id = validate_run_id(run_id)

    local_paths = build_local_run_paths(root, validated_run_id)
    ensure_local_run_dirs(local_paths)

    if not local_paths.manifest.exists():
        copy_config_snapshot(root, local_paths.config_snapshot)
        manifest = create_manifest(
            run_id=validated_run_id,
            cluster=cluster,
            patient=patient,
            local_paths=local_paths,
            workspace_root=root,
            config=config,
        )
        write_manifest(manifest, local_paths.manifest)
    else:
        manifest = read_manifest(local_paths.manifest)
        if not local_paths.config_snapshot.exists():
            copy_config_snapshot(root, local_paths.config_snapshot)

    return local_paths, {
        "cluster": cluster,
        "patient": patient,
        "config": config,
        "manifest": manifest,
        "workspace_root": root,
    }


def _build_tune_plan_steps(
    *,
    run_id: str,
    iteration: int,
    cluster_name: str,
    cluster_host: str,
    cluster_user: str,
    remote_run_dir: str,
    remote_inputs_dir: str,
    remote_results_dir: str,
    remote_logs_dir: str,
    remote_script_path: str,
    patient_path: str,
    local_paths: LocalRunPaths,
    local_iteration_inputs_dir: Path,
) -> list[PlanStep]:
    scheduler_submit_preview = (
        "sbatch --parsable "
        f"--job-name {run_id} --partition <partition> --time <HH:MM:SS> "
        "--mem <memory> --cpus-per-task <count> "
        f"{remote_script_path}"
    )
    scheduler_status_preview = "squeue --job <job_id> --noheader --format %T"

    return [
        PlanStep(
            step_id="s01_resolve_patient_path",
            name="resolve_patient_path",
            category=StepCategory.RESOLVE_PATHS,
            description="Resolve and verify read-only patient source paths from workspace config.",
            inputs={"cluster": cluster_name, "patient_alias": patient_path.rsplit("/", 1)[-1]},
            outputs={"resolved_patient_path": patient_path},
            local_paths={},
            remote_paths={"read": [patient_path], "write": []},
            safety_notes=["Patient source-of-truth data is read-only and cannot be modified."],
            command_preview=[
                "svzt",
                "resolve-patient-path",
                "--cluster",
                cluster_name,
                "--patient-path",
                patient_path,
            ],
        ),
        PlanStep(
            step_id="s02_init_or_verify_local_run_dir",
            name="init_or_verify_local_run_dir",
            category=StepCategory.INIT_RUN,
            description="Create or verify deterministic local run directory structure.",
            inputs={"run_id": run_id},
            outputs={"local_run_dir": str(local_paths.run_dir)},
            dependencies=["s01_resolve_patient_path"],
            local_paths={
                "run_dir": str(local_paths.run_dir),
                "manifest": str(local_paths.manifest),
                "staged_inputs": str(local_paths.staged_inputs),
                "pulled_outputs": str(local_paths.pulled_outputs),
            },
            safety_notes=["Planning phase only; no remote side effects."],
            command_preview=["mkdir", "-p", str(local_paths.run_dir)],
        ),
        PlanStep(
            step_id="s03_snapshot_config_and_provenance",
            name="snapshot_config_and_provenance",
            category=StepCategory.SNAPSHOT_CONFIG,
            description="Snapshot workspace config and provenance metadata into the run directory.",
            outputs={"config_snapshot_dir": str(local_paths.config_snapshot)},
            dependencies=["s02_init_or_verify_local_run_dir"],
            local_paths={"config_snapshot": str(local_paths.config_snapshot)},
            safety_notes=["Config snapshot supports reproducible dry-run planning."],
            command_preview=["cp", "-R", "config/", str(local_paths.config_snapshot)],
        ),
        PlanStep(
            step_id="s04_define_staged_local_inputs",
            name="define_staged_local_inputs",
            category=StepCategory.STAGE_INPUTS,
            description="Define local staged input set for the tune workflow.",
            inputs={"patient_source_path": patient_path, "iteration": str(iteration)},
            outputs={"staged_inputs_dir": str(local_iteration_inputs_dir)},
            dependencies=["s03_snapshot_config_and_provenance"],
            local_paths={"staged_inputs": str(local_iteration_inputs_dir)},
            remote_paths={"read": [patient_path], "write": []},
            safety_notes=["Only planned artifact mapping is recorded in this phase."],
            command_preview=[
                "prepare_tune_inputs",
                "--patient-root",
                patient_path,
                "--out",
                str(local_iteration_inputs_dir),
            ],
        ),
        PlanStep(
            step_id="s05_define_remote_staging_destination",
            name="define_remote_staging_destination",
            category=StepCategory.PUSH_TO_CLUSTER,
            description="Define remote staging destination under runs_root for run-scoped writes.",
            outputs={"remote_inputs_dir": remote_inputs_dir},
            dependencies=["s04_define_staged_local_inputs"],
            local_paths={"staged_inputs": str(local_iteration_inputs_dir)},
            remote_paths={"read": [], "write": [remote_run_dir, remote_inputs_dir, remote_logs_dir]},
            safety_notes=["All remote writes must remain under configured runs_root."],
            command_preview=[
                "rsync",
                "-az",
                "--dry-run",
                str(local_iteration_inputs_dir) + "/",
                f"{cluster_user}@{cluster_host}:{remote_inputs_dir}",
            ],
        ),
        PlanStep(
            step_id="s06_define_job_script_generation",
            name="define_job_script_generation",
            category=StepCategory.GENERATE_JOB_SCRIPT,
            description="Define deterministic job script generation in remote run directory.",
            outputs={"remote_script_path": remote_script_path},
            dependencies=["s05_define_remote_staging_destination"],
            remote_paths={"read": [remote_inputs_dir], "write": [remote_script_path, remote_logs_dir]},
            safety_notes=["Job script is planned as a preview and not generated in Phase 2."],
            command_preview=[
                "generate_tune_job_script",
                "--inputs",
                remote_inputs_dir,
                "--results",
                remote_results_dir,
                "--logs",
                remote_logs_dir,
                "--output",
                remote_script_path,
            ],
        ),
        PlanStep(
            step_id="s07_define_scheduler_submission",
            name="define_scheduler_submission",
            category=StepCategory.SUBMIT_JOB,
            description="Define scheduler submission command preview for the generated script.",
            inputs={"script_path": remote_script_path},
            outputs={"job_id": "<job_id>"},
            dependencies=["s06_define_job_script_generation"],
            remote_paths={"read": [remote_script_path], "write": [remote_logs_dir]},
            safety_notes=["Submission is represented as preview only; no scheduler call is executed."],
            command_preview=["ssh", f"{cluster_user}@{cluster_host}", scheduler_submit_preview],
        ),
        PlanStep(
            step_id="s08_define_monitoring_strategy",
            name="define_monitoring_strategy",
            category=StepCategory.MONITOR_JOB,
            description="Define scheduler monitoring strategy for terminal job states.",
            inputs={"job_id": "<job_id>"},
            outputs={"terminal_status": "<PENDING|RUNNING|COMPLETED|FAILED>"},
            dependencies=["s07_define_scheduler_submission"],
            remote_paths={"read": [remote_logs_dir], "write": []},
            safety_notes=["Monitoring strategy is dry-run only in this phase."],
            command_preview=["ssh", f"{cluster_user}@{cluster_host}", scheduler_status_preview],
        ),
        PlanStep(
            step_id="s09_define_artifact_pullback",
            name="define_artifact_pullback",
            category=StepCategory.PULL_ARTIFACTS,
            description="Define deterministic artifact pullback from remote run outputs.",
            inputs={"remote_results_dir": remote_results_dir},
            outputs={"local_pulled_outputs_dir": str(local_paths.pulled_outputs)},
            dependencies=["s08_define_monitoring_strategy"],
            local_paths={"pulled_outputs": str(local_paths.pulled_outputs)},
            remote_paths={"read": [remote_results_dir], "write": []},
            safety_notes=["Artifact retrieval is planned only; no network transfer in Phase 2."],
            command_preview=[
                "rsync",
                "-az",
                "--dry-run",
                f"{cluster_user}@{cluster_host}:{remote_results_dir}/",
                str(local_paths.pulled_outputs) + "/",
            ],
        ),
        PlanStep(
            step_id="s10_define_postprocessing_hook",
            name="define_postprocessing_hook",
            category=StepCategory.POSTPROCESS,
            description="Define local postprocessing hook over pulled artifacts.",
            inputs={"pulled_outputs_dir": str(local_paths.pulled_outputs)},
            outputs={"metrics_json": str(local_paths.run_dir / "metrics.json")},
            dependencies=["s09_define_artifact_pullback"],
            local_paths={"pulled_outputs": str(local_paths.pulled_outputs)},
            safety_notes=["Postprocessing remains a command preview in this phase."],
            command_preview=[
                "python",
                "-m",
                "svztagent.postprocess.summarize",
                str(local_paths.pulled_outputs),
                str(local_paths.run_dir / "metrics.json"),
            ],
        ),
        PlanStep(
            step_id="s11_define_manifest_finalization",
            name="define_manifest_finalization",
            category=StepCategory.FINALIZE_MANIFEST,
            description="Define manifest finalization after dry-run plan generation.",
            outputs={"manifest_path": str(local_paths.manifest)},
            dependencies=["s10_define_postprocessing_hook"],
            local_paths={"manifest": str(local_paths.manifest)},
            safety_notes=["Manifest update records planning state only."],
            command_preview=["svzt", "update-progress", "--run-id", run_id, "--status", "planned"],
        ),
    ]


def plan_tune_trees(
    workspace_root: str | Path,
    cluster_name: str,
    patient_alias: str,
    run_id: str | None = None,
) -> ExecutionPlan:
    resolved_run_id = validate_run_id(run_id or generate_run_id())

    local_paths, ctx = load_or_init_run_workspace(
        workspace_root=workspace_root,
        cluster_name=cluster_name,
        patient_alias=patient_alias,
        run_id=resolved_run_id,
    )

    cluster = ctx["cluster"]
    patient = ctx["patient"]
    config = ctx["config"]
    manifest = ctx["manifest"]
    iteration = manifest.tuning_iteration_tracker.current_iteration
    local_iteration_paths = build_iteration_local_paths(local_paths, iteration)

    remote_layout = _iteration_remote_layout(
        runs_root=cluster.remote_roots.runs_root,
        run_id=resolved_run_id,
        iteration=iteration,
    )
    remote_run_dir = remote_layout["remote_run_dir"]
    remote_inputs_dir = remote_layout["remote_inputs_dir"]
    remote_results_dir = remote_layout["remote_results_dir"]
    remote_logs_dir = remote_layout["remote_logs_dir"]
    remote_script_path = remote_layout["remote_job_script_path"]

    validate_remote_write_path(remote_run_dir, cluster.remote_roots.runs_root)
    validate_remote_write_path(remote_layout["remote_iter_dir"], cluster.remote_roots.runs_root)
    validate_remote_write_path(remote_inputs_dir, cluster.remote_roots.runs_root)
    validate_remote_write_path(remote_results_dir, cluster.remote_roots.runs_root)
    validate_remote_write_path(remote_logs_dir, cluster.remote_roots.runs_root)
    validate_remote_write_path(remote_script_path, cluster.remote_roots.runs_root)

    steps = _build_tune_plan_steps(
        run_id=resolved_run_id,
        cluster_name=cluster.name,
        cluster_host=cluster.host,
        cluster_user=cluster.user,
        remote_run_dir=remote_run_dir,
        remote_inputs_dir=remote_inputs_dir,
        remote_results_dir=remote_results_dir,
        remote_logs_dir=remote_logs_dir,
        remote_script_path=remote_script_path,
        patient_path=patient.remote_path,
        local_paths=local_paths,
        iteration=iteration,
        local_iteration_inputs_dir=local_iteration_paths["staged_inputs"],
    )

    plan = ExecutionPlan(
        plan_id=f"plan-{resolved_run_id}-tune_trees",
        workflow_name="tune_trees",
        run_id=resolved_run_id,
        cluster=cluster.name,
        patient=patient.alias,
        created_at=utc_now_iso(),
        manifest_path=str(local_paths.manifest),
        local_run_dir=str(local_paths.run_dir),
        remote_run_dir=remote_run_dir,
        steps=steps,
        summary={
            "dry_run_only": True,
            "step_count": len(steps),
            "current_iteration": iteration,
            "iteration_dir": str(local_iteration_paths["root"]),
            "scheduler": cluster.scheduler.type,
            "terminal_step_ids": ["s09_define_artifact_pullback", "s11_define_manifest_finalization"],
            "include_patterns": config.defaults.rsync.include_patterns,
            "exclude_patterns": config.defaults.rsync.exclude_patterns,
        },
    )

    validation_results = assert_valid_execution_plan(
        plan=plan,
        runs_root=cluster.remote_roots.runs_root,
        patient_data_root=cluster.remote_roots.patient_data_root,
    )
    plan = plan.model_copy(update={"validation_results": validation_results})

    write_plan_json(plan, local_paths.execution_plan_json)
    write_plan_yaml(plan, local_paths.execution_plan_yaml)

    manifest = ctx["manifest"].model_copy(deep=True)
    planned_at = utc_now_iso()
    manifest.jobs = [
        {
            "job_id": "<job_id>",
            "status": "PLANNED",
            "scheduler": cluster.scheduler.type,
            "mode": "preview",
        }
    ]
    manifest.artifacts["plan_files"] = [
        str(local_paths.execution_plan_json),
        str(local_paths.execution_plan_yaml),
    ]
    manifest = record_plan_path(manifest, str(local_paths.execution_plan_yaml), at=planned_at)
    manifest = record_lifecycle_transition(
        manifest,
        to_state=RunLifecycleState.PLANNED,
        normalized_scheduler_state=RunLifecycleState.UNKNOWN,
        note="Execution plan generated",
        at=planned_at,
    )
    manifest = mark_progress_milestone(
        manifest=manifest,
        model_id="preop_model",
        milestone_id="planned",
        status="completed",
        note="Dry-run execution plan generated for preop BC tuning stage.",
        at=planned_at,
    )
    write_manifest(manifest, local_paths.manifest)

    return plan


def _load_or_generate_valid_plan(
    workspace_root: Path,
    cluster_name: str,
    patient_alias: str,
    run_id: str,
) -> ExecutionPlan:
    local_paths = build_local_run_paths(workspace_root, run_id)
    if local_paths.execution_plan_yaml.exists():
        plan = load_plan_yaml(local_paths.execution_plan_yaml)
    else:
        plan = plan_tune_trees(
            workspace_root=workspace_root,
            cluster_name=cluster_name,
            patient_alias=patient_alias,
            run_id=run_id,
        )
    config = load_workspace_config(workspace_root)
    cluster = resolve_cluster(config, cluster_name)
    assert_valid_execution_plan(
        plan=plan,
        runs_root=cluster.remote_roots.runs_root,
        patient_data_root=cluster.remote_roots.patient_data_root,
    )
    return plan


def _template_text() -> str:
    return files("svztagent").joinpath("templates", "slurm", "job_template.sh").read_text(
        encoding="utf-8"
    )


def _resolve_cluster_svfsiplus_path(*, cluster_name: str, configured_path: str) -> str:
    _ = cluster_name
    return configured_path


def _apply_cluster_svzerodsolver_build_dir(
    *,
    cluster_name: str,
    configured_path: str | None,
    threed_config: dict,
) -> dict:
    if configured_path is None or not str(configured_path).strip():
        raise ConfigError(
            "cluster executables.svzerodsolver_build_dir is required for 3D "
            f"svZeroDSolver-coupled workflows on cluster '{cluster_name}'"
        )

    resolved = dict(threed_config)
    solver_paths = dict(resolved.get("solver_paths") or {})
    solver_paths.setdefault("svzerodsolver_build_dir", configured_path)
    resolved["solver_paths"] = solver_paths
    return resolved


def _render_tune_job_script(
    *,
    run_id: str,
    iteration: int,
    remote_run_dir: str,
    remote_iter_dir: str,
    remote_inputs_dir: str,
    remote_results_dir: str,
    remote_logs_dir: str,
    remote_patient_path: str,
    remote_clinical_targets_path: str | None,
    remote_centerline_path: str | None,
    remote_inflow_path: str | None,
    remote_preop_mesh_complete_dir: str | None,
    remote_postop_mesh_complete_dir: str | None,
    cluster_svfsiplus_path: str,
    threed_config: dict,
    impedance_config: dict,
    mesh_scale_factor: float,
    scheduler_defaults: dict,
    env_hooks: list[str],
    python_executable: str,
    skip_zerod_tuning: bool = False,
) -> str:
    template = _template_text()
    env_block = "\n".join(env_hooks) if env_hooks else "# no environment hooks configured"
    scheduler_cpus = scheduler_defaults.get("cpus", "<count>")
    try:
        scheduler_cpus_int = int(scheduler_cpus)
    except (TypeError, ValueError):
        scheduler_cpus_int = None
    try:
        impedance_n_procs = int(impedance_config.get("n_procs"))
    except (TypeError, ValueError):
        impedance_n_procs = None
    if impedance_n_procs is not None:
        sbatch_cpus = str(max(scheduler_cpus_int or 0, impedance_n_procs))
    else:
        sbatch_cpus = str(scheduler_cpus)

    replacements = {
        "{{RUN_ID}}": run_id,
        "{{ITERATION}}": str(iteration),
        "{{REMOTE_RUN_DIR}}": remote_run_dir,
        "{{REMOTE_ITER_DIR}}": remote_iter_dir,
        "{{REMOTE_INPUTS_DIR}}": remote_inputs_dir,
        "{{REMOTE_RESULTS_DIR}}": remote_results_dir,
        "{{REMOTE_LOGS_DIR}}": remote_logs_dir,
        "{{REMOTE_PATIENT_PATH}}": remote_patient_path,
        "{{REMOTE_CLINICAL_TARGETS_PATH}}": remote_clinical_targets_path or "",
        "{{REMOTE_CENTERLINE_PATH}}": remote_centerline_path or "",
        "{{REMOTE_INFLOW_PATH}}": remote_inflow_path or "",
        "{{REMOTE_PREOP_MESH_COMPLETE_DIR}}": remote_preop_mesh_complete_dir or "",
        "{{REMOTE_POSTOP_MESH_COMPLETE_DIR}}": remote_postop_mesh_complete_dir or "",
        "{{CLUSTER_SVFSIPLUS_PATH}}": cluster_svfsiplus_path,
        "{{THREED_CONFIG_JSON}}": json.dumps(threed_config, sort_keys=True),
        "{{IMPEDANCE_CONFIG_JSON}}": json.dumps(impedance_config, sort_keys=True),
        "{{MESH_SCALE_FACTOR}}": str(mesh_scale_factor),
        "{{SKIP_ZEROD_TUNING_JSON}}": json.dumps(bool(skip_zerod_tuning)),
        "{{SBATCH_ACCOUNT}}": str(scheduler_defaults.get("account") or ""),
        "{{SBATCH_PARTITION}}": str(scheduler_defaults.get("partition", "<partition>")),
        "{{SBATCH_TIME}}": str(scheduler_defaults.get("wall_time", "<HH:MM:SS>")),
        "{{SBATCH_MEM}}": str(scheduler_defaults.get("mem", "<memory>")),
        "{{SBATCH_CPUS}}": sbatch_cpus,
        "{{ENV_HOOKS}}": env_block,
        "{{PYTHON_EXECUTABLE}}": python_executable.strip() or "python3",
    }
    rendered = template
    for key, value in replacements.items():
        rendered = rendered.replace(key, value)
    return rendered


def _link_or_copy_directory(source: Path, target: Path) -> None:
    if target.is_symlink() or target.is_file():
        target.unlink()
    elif target.is_dir():
        shutil.rmtree(target)

    try:
        target.symlink_to(source, target_is_directory=True)
    except OSError:
        shutil.copytree(source, target)


def _remote_seed_filename(remote_path: str) -> str:
    name = PurePosixPath(remote_path).name
    if not name:
        raise ConfigError(f"seed path must include a filename: {remote_path}")
    return name


def _stage_seed_from_remote(
    *,
    remote_seed_path: str,
    iteration_paths: dict[str, Path],
    transfer_adapter: FileTransferAdapter,
) -> Path | None:
    pull_dir = iteration_paths["staged_inputs"] / "_remote_seed_pull"
    pull_dir.mkdir(parents=True, exist_ok=True)
    result = transfer_adapter.pull(remote_seed_path, str(pull_dir))
    if result.dry_run:
        return None

    pulled_seed = pull_dir / _remote_seed_filename(remote_seed_path)
    if not pulled_seed.exists():
        raise ConfigError(
            "remote seed pull completed but staged file is missing: "
            f"{pulled_seed}"
        )
    return pulled_seed


def _stage_local_inflow_if_available(
    *,
    iteration_paths: dict[str, Path],
    patient_assets: dict[str, str] | None,
) -> Path | None:
    if not patient_assets:
        return None
    inflow_value = str(patient_assets.get("inflow", "")).strip()
    if not inflow_value:
        return None
    inflow_path = Path(inflow_value).expanduser()
    if not inflow_path.exists():
        return None

    target = iteration_paths["staged_inputs"] / "inflow.csv"
    shutil.copyfile(inflow_path, target)
    return target


def _stage_tune_inputs(
    local_paths: LocalRunPaths,
    plan: ExecutionPlan,
    patient_alias: str,
    *,
    iteration: int,
    seed_config_path: str | None = None,
    seed_input_filename: str = REDUCED_SEED_INPUT_FILENAME,
    patient_assets: dict[str, str] | None = None,
    transfer_adapter: FileTransferAdapter | None = None,
    remote_exec_adapter: RemoteExecAdapter | None = None,
    mode: ExecutionMode = ExecutionMode.DRY_RUN,
) -> Path:
    iteration_paths = build_iteration_local_paths(local_paths, iteration)
    iteration_paths["root"].mkdir(parents=True, exist_ok=True)
    iteration_paths["staged_inputs"].mkdir(parents=True, exist_ok=True)
    iteration_paths["results"].mkdir(parents=True, exist_ok=True)
    iteration_paths["logs"].mkdir(parents=True, exist_ok=True)

    stage_payload = {
        "run_id": plan.run_id,
        "workflow": plan.workflow_name,
        "plan_id": plan.plan_id,
        "patient_alias": patient_alias,
        "iteration": iteration,
        "seed_config_path": seed_config_path,
        "seed_input_filename": seed_input_filename,
        "iter1_seed_source": patient_assets.get("iteration1_seed_source")
        if patient_assets
        else None,
        "iter1_seed_path": patient_assets.get("iteration1_seed_path")
        if patient_assets
        else None,
        "generated_at": utc_now_iso(),
    }
    payload_path = iteration_paths["staged_inputs"] / "tune_inputs.yaml"
    payload_path.parent.mkdir(parents=True, exist_ok=True)
    with payload_path.open("w", encoding="utf-8") as stream:
        yaml.safe_dump(stage_payload, stream, sort_keys=True)

    seed: Path | None = Path(seed_config_path) if seed_config_path else None
    if seed is not None and not seed.exists():
        seed = None
    if (seed is None or not seed.exists()) and iteration > 1:
        remote_seed_path = str(seed_config_path or "").strip()
        if (
            remote_seed_path.startswith("/")
            and transfer_adapter is not None
            and mode == ExecutionMode.EXECUTE
        ):
            pulled_seed = _stage_seed_from_remote(
                remote_seed_path=remote_seed_path,
                iteration_paths=iteration_paths,
                transfer_adapter=transfer_adapter,
            )
            if pulled_seed is not None:
                seed = pulled_seed

        previous_paths = build_iteration_local_paths(local_paths, iteration - 1)
        pulled_results = (
            local_paths.pulled_outputs
            / "iterations"
            / iteration_dir_name(iteration - 1)
            / "results"
        )
        fallback_candidates = [
            pulled_results / "simplified_zerod_tuned_RRI.json",
            previous_paths["results"] / "simplified_zerod_tuned_RRI.json",
        ]
        for fallback_seed in fallback_candidates:
            if fallback_seed.exists():
                seed = fallback_seed
                break
    if seed is None and iteration == 1:
        iter1_seed_source = (
            patient_assets.get("iteration1_seed_source") if patient_assets else None
        )
        iter1_seed_path = (
            patient_assets.get("iteration1_seed_path") if patient_assets else None
        )
        if iter1_seed_source == "path":
            candidate = Path(str(iter1_seed_path or "")).expanduser()
            if candidate.exists():
                seed = candidate
            else:
                remote_seed_path = str(iter1_seed_path or "").strip()
                remote_seed_present: bool | None = None
                attempted_remote_pull = False
                if remote_seed_path.startswith("/"):
                    if mode == ExecutionMode.EXECUTE and remote_exec_adapter is not None:
                        try:
                            remote_exec_adapter.run(["test", "-f", remote_seed_path])
                            remote_seed_present = True
                        except AdapterExecutionError as exc:
                            if exc.returncode == 1:
                                remote_seed_present = False
                            else:
                                raise

                    if transfer_adapter is not None and remote_seed_present is not False:
                        attempted_remote_pull = True
                        pulled_seed = _stage_seed_from_remote(
                            remote_seed_path=remote_seed_path,
                            iteration_paths=iteration_paths,
                            transfer_adapter=transfer_adapter,
                        )
                        if pulled_seed is not None:
                            seed = pulled_seed
        elif iter1_seed_source == "generate":
            pass

    if seed is not None and seed.exists():
        target = iteration_paths["staged_inputs"] / seed_input_filename
        target.write_text(seed.read_text(encoding="utf-8"), encoding="utf-8")
    _stage_local_inflow_if_available(
        iteration_paths=iteration_paths,
        patient_assets=patient_assets,
    )
    return payload_path


def _build_default_adapters(
    *,
    cluster,
    config,
    run_id: str,
    mode: ExecutionMode,
) -> tuple[FileTransferAdapter, SchedulerAdapter, RemoteExecAdapter]:
    executor = CommandExecutor(mode=mode)
    remote = SshRemoteExecAdapter(
        user=cluster.user,
        host=cluster.host,
        executor=executor,
    )
    transfer = RsyncFileTransferAdapter(
        user=cluster.user,
        host=cluster.host,
        runs_root=cluster.remote_roots.runs_root,
        patient_data_root=cluster.remote_roots.patient_data_root,
        permanent_data_root=cluster.remote_roots.permanent_data_root,
        remote_exec=remote,
        executor=executor,
        default_include=config.defaults.rsync.include_patterns,
        default_exclude=config.defaults.rsync.exclude_patterns,
    )
    scheduler = SlurmSchedulerAdapter(
        remote_exec=remote,
        runs_root=cluster.remote_roots.runs_root,
        submit_options=SlurmSubmitOptions(
            job_name=run_id,
            account=config.defaults.scheduler.account,
            partition=config.defaults.scheduler.partition,
            wall_time=config.defaults.scheduler.wall_time,
            mem=config.defaults.scheduler.mem,
            cpus=config.defaults.scheduler.cpus,
        ),
    )
    return transfer, scheduler, remote


def run_tune_trees(
    workspace_root: str | Path,
    cluster_name: str,
    patient_alias: str,
    run_id: str | None = None,
    iteration: int | None = None,
    mode: ExecutionMode = ExecutionMode.DRY_RUN,
    transfer_adapter: FileTransferAdapter | None = None,
    scheduler_adapter: SchedulerAdapter | None = None,
    remote_exec_adapter: RemoteExecAdapter | None = None,
    skip_zerod_tuning: bool = False,
    progress_callback: Callable[[str], None] | None = None,
) -> TuneExecutionResult:
    resolved_run_id = validate_run_id(run_id or generate_run_id())
    _emit_progress(
        progress_callback,
        f"[svzt] Initializing tune run {resolved_run_id} for patient {patient_alias}",
    )
    local_paths, ctx = load_or_init_run_workspace(
        workspace_root=workspace_root,
        cluster_name=cluster_name,
        patient_alias=patient_alias,
        run_id=resolved_run_id,
    )
    workspace = Path(ctx["workspace_root"])
    cluster = ctx["cluster"]
    patient = ctx["patient"]
    config = ctx["config"]

    _emit_progress(progress_callback, "[svzt] Loading execution plan")
    plan = _load_or_generate_valid_plan(
        workspace_root=workspace,
        cluster_name=cluster_name,
        patient_alias=patient_alias,
        run_id=resolved_run_id,
    )

    manifest = read_manifest(local_paths.manifest)
    resolved_iteration = _resolve_iteration(manifest, iteration)
    remote_layout = _iteration_remote_layout(
        runs_root=cluster.remote_roots.runs_root,
        run_id=resolved_run_id,
        iteration=resolved_iteration,
    )
    remote_run_dir = remote_layout["remote_run_dir"]
    remote_iter_dir = remote_layout["remote_iter_dir"]
    remote_inputs_dir = remote_layout["remote_inputs_dir"]
    remote_results_dir = remote_layout["remote_results_dir"]
    remote_logs_dir = remote_layout["remote_logs_dir"]
    remote_job_script_path = remote_layout["remote_job_script_path"]
    local_iteration_paths = build_iteration_local_paths(local_paths, resolved_iteration)

    validate_remote_write_path(
        remote_run_dir,
        cluster.remote_roots.runs_root,
        cluster.remote_roots.patient_data_root,
    )
    validate_remote_write_path(
        remote_iter_dir,
        cluster.remote_roots.runs_root,
        cluster.remote_roots.patient_data_root,
    )
    validate_remote_write_path(
        remote_job_script_path,
        cluster.remote_roots.runs_root,
        cluster.remote_roots.patient_data_root,
    )

    _emit_progress(progress_callback, "[svzt] Preparing transfer and scheduler adapters")
    if transfer_adapter is None or scheduler_adapter is None or remote_exec_adapter is None:
        default_transfer, default_scheduler, default_remote = _build_default_adapters(
            cluster=cluster,
            config=config,
            run_id=resolved_run_id,
            mode=mode,
        )
        transfer_adapter = transfer_adapter or default_transfer
        scheduler_adapter = scheduler_adapter or default_scheduler
        remote_exec_adapter = remote_exec_adapter or default_remote

    prior_record = next(
        (
            rec
            for rec in manifest.tuning_iteration_tracker.iterations
            if rec.iteration == resolved_iteration - 1
        ),
        None,
    )
    seed_config = prior_record.regenerated_config_path if prior_record else None
    _emit_progress(
        progress_callback,
        f"[svzt] Staging inputs for iteration {resolved_iteration}",
    )
    _stage_tune_inputs(
        local_paths,
        plan,
        patient_alias=patient_alias,
        iteration=resolved_iteration,
        seed_config_path=seed_config,
        seed_input_filename=_seed_input_filename(
            patient.impedance.tuning_model,
            resolved_iteration,
        ),
        patient_assets=(
            patient.patient_assets.model_dump(mode="json")
            if patient.patient_assets is not None
            else None
        ),
        transfer_adapter=transfer_adapter,
        remote_exec_adapter=remote_exec_adapter,
        mode=mode,
    )

    local_iteration_paths["root"].mkdir(parents=True, exist_ok=True)
    local_script_path = local_iteration_paths["job_script"]
    cluster_svfsiplus_path = _resolve_cluster_svfsiplus_path(
        cluster_name=cluster.name,
        configured_path=cluster.executables.svfsiplus_path,
    )
    _emit_progress(progress_callback, "[svzt] Rendering job script")
    script_body = _render_tune_job_script(
        run_id=resolved_run_id,
        iteration=resolved_iteration,
        remote_run_dir=remote_run_dir,
        remote_iter_dir=remote_iter_dir,
        remote_inputs_dir=remote_inputs_dir,
        remote_results_dir=remote_results_dir,
        remote_logs_dir=remote_logs_dir,
        remote_patient_path=patient.remote_path,
        remote_clinical_targets_path=patient.patient_assets.clinical_targets
        if patient.patient_assets
        else None,
        remote_centerline_path=patient.patient_assets.centerlines if patient.patient_assets else None,
        remote_inflow_path=patient.patient_assets.inflow if patient.patient_assets else None,
        remote_preop_mesh_complete_dir=patient.patient_assets.preop_mesh_complete_dir
        if patient.patient_assets
        else None,
        remote_postop_mesh_complete_dir=patient.patient_assets.postop_mesh_complete_dir
        if patient.patient_assets
        else None,
        cluster_svfsiplus_path=cluster_svfsiplus_path,
        threed_config=_apply_cluster_svzerodsolver_build_dir(
            cluster_name=cluster.name,
            configured_path=cluster.executables.svzerodsolver_build_dir,
            threed_config=patient.threed.model_dump(mode="json"),
        ),
        impedance_config=_iteration_impedance_config(
            patient.impedance.model_dump(mode="json"),
            resolved_iteration,
        ),
        mesh_scale_factor=patient.mesh_scale_factor,
        scheduler_defaults=config.defaults.scheduler.model_dump(mode="json"),
        env_hooks=config.defaults.execution.env_activation_hooks,
        python_executable=config.defaults.execution.python_executable,
        skip_zerod_tuning=skip_zerod_tuning,
    )
    local_script_path.write_text(script_body, encoding="utf-8")

    command_results = []
    _emit_progress(progress_callback, "[svzt] Ensuring remote run directories")
    command_results.append(transfer_adapter.ensure_remote_dir(remote_run_dir))
    command_results.append(transfer_adapter.ensure_remote_dir(remote_iter_dir))
    command_results.append(transfer_adapter.ensure_remote_dir(remote_inputs_dir))
    command_results.append(transfer_adapter.ensure_remote_dir(remote_results_dir))
    command_results.append(transfer_adapter.ensure_remote_dir(remote_logs_dir))
    _emit_progress(progress_callback, "[svzt] Syncing staged inputs to cluster")
    command_results.append(
        transfer_adapter.sync(
            local_dir=str(local_iteration_paths["staged_inputs"]),
            remote_dir=remote_inputs_dir,
            include=config.defaults.rsync.include_patterns,
            exclude=config.defaults.rsync.exclude_patterns,
            direction=SyncDirection.PUSH,
        )
    )
    _emit_progress(progress_callback, "[svzt] Uploading job script")
    command_results.append(
        transfer_adapter.push(str(local_script_path), remote_job_script_path)
    )

    _emit_progress(
        progress_callback,
        "[svzt] Submitting scheduler job"
        if mode == ExecutionMode.EXECUTE
        else "[svzt] Previewing scheduler submission",
    )
    submit_result = scheduler_adapter.submit(remote_job_script_path)
    command_results.append(submit_result.command)

    _emit_progress(progress_callback, "[svzt] Recording manifest updates")
    manifest = record_plan_path(manifest, str(local_paths.execution_plan_yaml))
    manifest = record_submission(
        manifest,
        remote_run_dir=remote_run_dir,
        job_script_path=remote_job_script_path,
        scheduler_type=cluster.scheduler.type,
        submitted_job_id=submit_result.job_id,
        mode=mode.value,
    )
    if mode == ExecutionMode.EXECUTE:
        manifest = record_lifecycle_transition(
            manifest,
            to_state=RunLifecycleState.SUBMITTED,
            normalized_scheduler_state=RunLifecycleState.SUBMITTED,
            note="Scheduler submission completed",
        )
    else:
        current_lifecycle = coerce_run_lifecycle_state(manifest.execution.lifecycle_state)
        if current_lifecycle == RunLifecycleState.PLANNED:
            manifest = record_lifecycle_transition(
                manifest,
                to_state=RunLifecycleState.PLANNED,
                normalized_scheduler_state=RunLifecycleState.UNKNOWN,
                note="Dry-run submission preview generated",
            )
    manifest = mark_iteration_submitted(
        manifest,
        iteration=resolved_iteration,
        tune_job_id=submit_result.job_id,
        local_dir=str(local_iteration_paths["root"]),
        remote_dir=remote_iter_dir,
        job_script_path=remote_job_script_path,
        note=(
            "Iteration job submitted"
            if mode == ExecutionMode.EXECUTE
            else "Iteration submission preview generated"
        ),
    )
    manifest.artifacts["job_script_local"] = str(local_script_path)
    write_manifest(manifest, local_paths.manifest)

    return TuneExecutionResult(
        run_id=resolved_run_id,
        iteration=resolved_iteration,
        mode=mode,
        plan_path=local_paths.execution_plan_yaml,
        remote_run_dir=remote_run_dir,
        remote_job_script_path=remote_job_script_path,
        local_job_script_path=local_script_path,
        submitted_job_id=submit_result.job_id,
        command_previews=[item.argv for item in command_results],
    )


def _resolve_cluster_for_run(workspace_root: Path, run_id: str):
    local_paths = build_local_run_paths(workspace_root, run_id)
    manifest = read_manifest(local_paths.manifest)
    cluster_name = manifest.cluster.get("name")
    if not cluster_name:
        raise ConfigError("manifest cluster.name is required for execution commands")
    config = load_workspace_config(workspace_root)
    cluster = resolve_cluster(config, cluster_name)
    return local_paths, manifest, config, cluster


def _iteration_record_for_job(manifest, job_id: str):
    for record in manifest.tuning_iteration_tracker.iterations:
        if str(record.tune_job_id or "").strip() == job_id:
            return record
    current_iteration = manifest.tuning_iteration_tracker.current_iteration
    return next(
        (rec for rec in manifest.tuning_iteration_tracker.iterations if rec.iteration == current_iteration),
        None,
    )


def _dedupe_paths(paths: list[Path]) -> list[Path]:
    deduped: list[Path] = []
    seen: set[str] = set()
    for path in paths:
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(path)
    return deduped


def _dedupe_remote_paths(paths: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for path in paths:
        key = str(PurePosixPath(path))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(key)
    return deduped


def _local_error_log_dirs(*, _run_id: str, job_id: str, local_paths: LocalRunPaths, manifest) -> list[Path]:
    record = _iteration_record_for_job(manifest, job_id)
    dirs: list[Path] = []
    if record and record.local_dir:
        dirs.append(Path(record.local_dir) / "logs")
    if record:
        iter_name = iteration_dir_name(record.iteration)
        dirs.append(local_paths.pulled_outputs / "iterations" / iter_name / "logs")
    dirs.append(local_paths.pulled_outputs / "logs")
    dirs.append(local_paths.logs)
    return _dedupe_paths(dirs)


def _remote_error_log_dirs(*, _run_id: str, job_id: str, manifest) -> list[str]:
    record = _iteration_record_for_job(manifest, job_id)
    dirs: list[str] = []
    if record and record.remote_dir:
        dirs.append(str(PurePosixPath(record.remote_dir) / "logs"))

    remote_run_dir = manifest.execution.remote_run_dir or manifest.remote.get("remote_run_dir")
    if remote_run_dir:
        dirs.append(str(PurePosixPath(remote_run_dir) / "logs"))
        if record:
            dirs.append(
                str(
                    PurePosixPath(remote_run_dir)
                    / "iterations"
                    / iteration_dir_name(record.iteration)
                    / "logs"
                )
            )
    return _dedupe_remote_paths(dirs)


def _find_local_error_log(*, run_id: str, job_id: str, local_paths: LocalRunPaths, manifest) -> Path | None:
    expected = f"{run_id}_{job_id}.error"
    for directory in _local_error_log_dirs(
        _run_id=run_id,
        job_id=job_id,
        local_paths=local_paths,
        manifest=manifest,
    ):
        candidate = directory / expected
        if candidate.exists():
            return candidate
        for match in sorted(directory.glob(f"*_{job_id}.error")):
            if match.is_file():
                return match
    return None


def _tail_error_log(path: Path, max_lines: int = 40, max_chars: int = 4000) -> str | None:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None
    if not text.strip():
        return None
    tail_lines = text.splitlines()[-max_lines:]
    tail = "\n".join(tail_lines).strip()
    if len(tail) > max_chars:
        tail = tail[-max_chars:]
    return tail or None


def _resolve_failure_error_log(
    *,
    run_id: str,
    job_id: str,
    local_paths: LocalRunPaths,
    manifest,
    transfer_adapter: FileTransferAdapter | None,
) -> tuple[str | None, str | None]:
    local_error_path = _find_local_error_log(
        run_id=run_id,
        job_id=job_id,
        local_paths=local_paths,
        manifest=manifest,
    )
    if local_error_path is None and transfer_adapter is not None:
        include_pattern = f"*_{job_id}.error"
        for remote_logs_dir in _remote_error_log_dirs(_run_id=run_id, job_id=job_id, manifest=manifest):
            for local_logs_dir in _local_error_log_dirs(
                _run_id=run_id,
                job_id=job_id,
                local_paths=local_paths,
                manifest=manifest,
            ):
                local_logs_dir.mkdir(parents=True, exist_ok=True)
                try:
                    transfer_adapter.sync(
                        local_dir=str(local_logs_dir),
                        remote_dir=remote_logs_dir,
                        include=[include_pattern],
                        exclude=["*"],
                        direction=SyncDirection.PULL,
                    )
                except AdapterExecutionError:
                    continue
                local_error_path = _find_local_error_log(
                    run_id=run_id,
                    job_id=job_id,
                    local_paths=local_paths,
                    manifest=manifest,
                )
                if local_error_path is not None:
                    break
            if local_error_path is not None:
                break

    if local_error_path is None:
        return None, None
    return str(local_error_path), _tail_error_log(local_error_path)


def query_run_status(
    workspace_root: str | Path,
    run_id: str,
    mode: ExecutionMode = ExecutionMode.EXECUTE,
    scheduler_adapter: SchedulerAdapter | None = None,
    transfer_adapter: FileTransferAdapter | None = None,
) -> StatusQueryResult:
    validated_run_id = validate_run_id(run_id)
    root = detect_workspace_root(workspace_root)
    local_paths, manifest, config, cluster = _resolve_cluster_for_run(root, validated_run_id)

    job_id = resolve_submitted_job_id(manifest)
    if job_id is None:
        raise ConfigError(f"run '{validated_run_id}' has no submitted job id in manifest")

    if scheduler_adapter is None or transfer_adapter is None:
        default_transfer, default_scheduler, _ = _build_default_adapters(
            cluster=cluster,
            config=config,
            run_id=validated_run_id,
            mode=mode,
        )
        scheduler_adapter = scheduler_adapter or default_scheduler
        transfer_adapter = transfer_adapter or default_transfer

    poll_result = poll_scheduler_state(scheduler_adapter, job_id)
    updated = record_poll_observation(
        manifest,
        normalized_state=poll_result.normalized_state,
        raw_state=poll_result.raw_state,
        scheduler_source=poll_result.scheduler_source,
        terminal_reason=poll_result.terminal_reason,
    )
    updated = record_iteration_scheduler_state(
        updated,
        iteration=updated.tuning_iteration_tracker.current_iteration,
        state=poll_result.normalized_state,
    )
    write_manifest(updated, local_paths.manifest)

    tracker = updated.tuning_iteration_tracker
    current_iteration = tracker.current_iteration
    latest_adaptation = _latest_adaptation_run(updated)
    if latest_adaptation is not None and latest_adaptation.scheduler_job_id == job_id:
        state_label = poll_result.normalized_state.value
        detail = (
            f"Adaptation {latest_adaptation.model} using parameter set "
            f"{latest_adaptation.parameter_set} is {state_label}."
        )
        if poll_result.normalized_state == RunLifecycleState.COMPLETED:
            detail = (
                f"Adaptation {latest_adaptation.model} completed. "
                f"Comparison artifact: {latest_adaptation.comparison_path or '<pending>'}."
            )
        elif poll_result.normalized_state in {RunLifecycleState.FAILED, RunLifecycleState.CANCELLED}:
            detail = (
                f"Adaptation {latest_adaptation.model} ended with "
                f"{poll_result.normalized_state.value}."
            )
        failure_error_log_path = None
        failure_error_log_tail = None
        if poll_result.normalized_state in {RunLifecycleState.FAILED, RunLifecycleState.CANCELLED}:
            failure_error_log_path, failure_error_log_tail = _resolve_failure_error_log(
                run_id=validated_run_id,
                job_id=job_id,
                local_paths=local_paths,
                manifest=updated,
                transfer_adapter=transfer_adapter,
            )
        return StatusQueryResult(
            run_id=validated_run_id,
            job_id=job_id,
            raw_state=poll_result.raw_state,
            normalized_state=poll_result.normalized_state,
            source=poll_result.scheduler_source,
            active_workflow="adapt",
            current_iteration=current_iteration,
            max_iterations=tracker.max_iterations,
            tracker_status=tracker.status,
            stage_key="adaptation",
            stage_label="Adaptation workflow",
            stage_detail=detail,
            decision=None,
            needs_review_reason=None,
            progress_source="manifest",
            progress_warnings=[],
            adaptation_model=latest_adaptation.model,
            adaptation_parameter_set=latest_adaptation.parameter_set,
            adaptation_job_id=latest_adaptation.scheduler_job_id,
            adaptation_job_state_raw=poll_result.raw_state,
            adaptation_job_state_normalized=poll_result.normalized_state,
            failure_error_log_path=failure_error_log_path,
            failure_error_log_tail=failure_error_log_tail,
        )

    if updated.postop_run is not None and updated.postop_run.postop_job_id == job_id:
        detail = f"Explicit postop simulation is {poll_result.normalized_state.value}."
        if poll_result.normalized_state == RunLifecycleState.COMPLETED:
            detail = (
                "Explicit postop simulation completed. "
                f"Remote dir: {updated.postop_run.remote_dir}."
            )
        return StatusQueryResult(
            run_id=validated_run_id,
            job_id=job_id,
            raw_state=poll_result.raw_state,
            normalized_state=poll_result.normalized_state,
            source=poll_result.scheduler_source,
            active_workflow="postop",
            current_iteration=current_iteration,
            max_iterations=tracker.max_iterations,
            tracker_status=tracker.status,
            stage_key="postop",
            stage_label="Explicit postop workflow",
            stage_detail=detail,
            decision=None,
            needs_review_reason=None,
            progress_source="manifest",
            progress_warnings=[],
            postop_job_id=updated.postop_run.postop_job_id,
            postop_job_state_raw=poll_result.raw_state,
            postop_job_state_normalized=poll_result.normalized_state,
        )

    progress = _load_iteration_progress_artifacts(
        run_id=validated_run_id,
        iteration=current_iteration,
        local_paths=local_paths,
        manifest=updated,
        cluster=cluster,
        transfer_adapter=transfer_adapter,
    )
    progress_warnings = list(progress.warnings)
    driver_log = progress.driver_log
    decision_payload = progress.decision
    metrics_payload = progress.metrics

    preop_job_id = None
    if driver_log and driver_log.get("preop_job_id"):
        preop_job_id = str(driver_log.get("preop_job_id"))
    elif metrics_payload and metrics_payload.get("preop_job_id"):
        preop_job_id = str(metrics_payload.get("preop_job_id"))

    postop_job_id = None
    if decision_payload and decision_payload.get("postop_job_id"):
        postop_job_id = str(decision_payload.get("postop_job_id"))
    elif driver_log and driver_log.get("postop_job_id"):
        postop_job_id = str(driver_log.get("postop_job_id"))
    elif updated.postop_run is not None and updated.postop_run.postop_job_id:
        postop_job_id = str(updated.postop_run.postop_job_id)

    preop_job = _poll_child_job(
        job_id=preop_job_id,
        scheduler_adapter=scheduler_adapter,
        warnings=progress_warnings,
        label="preop",
    )
    postop_job = _poll_child_job(
        job_id=postop_job_id,
        scheduler_adapter=scheduler_adapter,
        warnings=progress_warnings,
        label="postop",
    )
    stage = _derive_stage_snapshot(
        parent_state=poll_result.normalized_state,
        driver_log=driver_log,
        decision_payload=decision_payload,
        preop_job=preop_job,
        postop_job=postop_job,
    )
    if (
        postop_job is None
        and updated.converged_preop_iteration is not None
        and tracker.status == "converged"
    ):
        stage = StageSnapshot(
            key="postop_ready",
            label="Converged preop iteration ready for postop",
            detail=(
                "Run explicit postop submission with "
                f"svzt run postop --run-id {validated_run_id}."
            ),
        )

    decision = str(decision_payload.get("decision")) if decision_payload and decision_payload.get("decision") else None
    needs_review_reason = None
    if decision_payload and decision_payload.get("needs_review_reason"):
        needs_review_reason = str(decision_payload.get("needs_review_reason"))
    elif driver_log:
        driver_errors = driver_log.get("errors")
        if isinstance(driver_errors, list) and driver_errors and driver_errors[0]:
            needs_review_reason = str(driver_errors[0])

    failure_error_log_path: str | None = None
    failure_error_log_tail: str | None = None
    if poll_result.normalized_state in {RunLifecycleState.FAILED, RunLifecycleState.CANCELLED}:
        failure_error_log_path, failure_error_log_tail = _resolve_failure_error_log(
            run_id=validated_run_id,
            job_id=job_id,
            local_paths=local_paths,
            manifest=updated,
            transfer_adapter=transfer_adapter,
        )

    return StatusQueryResult(
        run_id=validated_run_id,
        job_id=job_id,
        raw_state=poll_result.raw_state,
        normalized_state=poll_result.normalized_state,
        source=poll_result.scheduler_source,
        current_iteration=current_iteration,
        max_iterations=tracker.max_iterations,
        tracker_status=tracker.status,
        stage_key=stage.key,
        stage_label=stage.label,
        stage_detail=stage.detail,
        decision=decision,
        needs_review_reason=needs_review_reason,
        progress_source=progress.source,
        progress_warnings=progress_warnings,
        preop_job_id=preop_job.job_id if preop_job is not None else None,
        preop_job_state_raw=preop_job.raw_state if preop_job is not None else None,
        preop_job_state_normalized=preop_job.normalized_state if preop_job is not None else None,
        postop_job_id=postop_job.job_id if postop_job is not None else None,
        postop_job_state_raw=postop_job.raw_state if postop_job is not None else None,
        postop_job_state_normalized=postop_job.normalized_state if postop_job is not None else None,
        failure_error_log_path=failure_error_log_path,
        failure_error_log_tail=failure_error_log_tail,
    )


def watch_run_lifecycle(
    workspace_root: str | Path,
    run_id: str,
    *,
    mode: ExecutionMode = ExecutionMode.EXECUTE,
    scheduler_adapter: SchedulerAdapter | None = None,
    transfer_adapter: FileTransferAdapter | None = None,
    poll_interval_seconds: int | None = None,
    timeout_seconds: int | None = None,
    max_polls: int | None = None,
    fetch_on_complete: bool = False,
) -> WatchResult:
    validated_run_id = validate_run_id(run_id)
    root = detect_workspace_root(workspace_root)
    local_paths, _manifest, config, cluster = _resolve_cluster_for_run(root, validated_run_id)

    if scheduler_adapter is None:
        _, scheduler_adapter, _ = _build_default_adapters(
            cluster=cluster,
            config=config,
            run_id=validated_run_id,
            mode=mode,
        )

    effective_poll_interval = (
        poll_interval_seconds
        if poll_interval_seconds is not None
        else config.defaults.monitoring.poll_interval_seconds
    )
    settings = MonitorSettings(
        poll_interval_seconds=effective_poll_interval,
        timeout_seconds=timeout_seconds,
        max_polls=max_polls,
        fetch_on_complete=fetch_on_complete,
        fetch_on_failure=config.defaults.monitoring.fetch_on_failure,
    )

    monitor = RunMonitorService(scheduler_adapter=scheduler_adapter)
    summary = monitor.watch(manifest_path=local_paths.manifest, settings=settings)

    fetch_error: str | None = None
    fetch_attempted = False
    fetch_succeeded: bool | None = None

    should_fetch = (
        summary.terminal_state == RunLifecycleState.COMPLETED
        and fetch_on_complete
    ) or (
        summary.terminal_state in {RunLifecycleState.FAILED, RunLifecycleState.CANCELLED}
        and config.defaults.monitoring.fetch_on_failure
    )

    if should_fetch:
        fetch_attempted = True
        try:
            fetch_run_artifacts(
                workspace_root=root,
                run_id=validated_run_id,
                mode=mode,
                transfer_adapter=transfer_adapter,
            )
            fetch_succeeded = True
        except Exception as exc:
            fetch_error = str(exc)
            fetch_succeeded = False
            manifest = read_manifest(local_paths.manifest)
            manifest = record_fetch(
                manifest,
                fetched_artifacts=[],
                success=False,
            )
            write_manifest(manifest, local_paths.manifest)

    manifest = read_manifest(local_paths.manifest)
    manifest = record_iteration_scheduler_state(
        manifest,
        iteration=manifest.tuning_iteration_tracker.current_iteration,
        state=summary.terminal_state,
    )
    write_manifest(manifest, local_paths.manifest)
    manifest = read_manifest(local_paths.manifest)
    final_state = coerce_run_lifecycle_state(manifest.execution.lifecycle_state)
    terminal_state = summary.terminal_state

    remote_run_dir = manifest.execution.remote_run_dir or manifest.remote.get("remote_run_dir") or ""
    remote_logs_dir: str | None = None
    if remote_run_dir:
        remote_logs_dir = str(PurePosixPath(remote_run_dir) / "logs")

    return WatchResult(
        run_id=validated_run_id,
        job_id=summary.job_id,
        initial_state=summary.initial_state,
        final_state=final_state,
        terminal_state=terminal_state,
        raw_scheduler_state=manifest.execution.raw_scheduler_state,
        terminal_reason=manifest.execution.terminal_reason,
        poll_count=manifest.execution.poll_count,
        remote_run_dir=remote_run_dir,
        local_run_dir=manifest.local_paths.run_dir,
        local_logs_dir=manifest.local_paths.logs,
        remote_logs_dir=remote_logs_dir,
        job_script_path=manifest.execution.job_script_path,
        fetch_attempted=fetch_attempted or manifest.execution.fetch_attempted,
        fetch_succeeded=fetch_succeeded if fetch_attempted else manifest.execution.fetch_succeeded,
        fetch_error=fetch_error,
        observations=summary.observations,
    )


def _pull_iteration_decision_artifacts(
    *,
    run_id: str,
    iteration: int,
    local_paths: LocalRunPaths,
    cluster,
    transfer_adapter: FileTransferAdapter,
) -> IterationDecisionPullResult:
    remote_layout = _iteration_remote_layout(
        runs_root=cluster.remote_roots.runs_root,
        run_id=run_id,
        iteration=iteration,
    )
    remote_results_dir = remote_layout["remote_results_dir"]
    local_iteration_paths = build_iteration_local_paths(local_paths, iteration)
    local_results_dir = local_iteration_paths["results"]
    local_results_dir.mkdir(parents=True, exist_ok=True)

    sync_result = transfer_adapter.sync(
        local_dir=str(local_results_dir),
        remote_dir=remote_results_dir,
        include=[
            "iteration_decision.json",
            "iteration_metrics.json",
            "full_pa_zerod.json",
            "simplified_zerod_tuned_RRI.json",
        ],
        exclude=["*"],
        direction=SyncDirection.PULL,
    )

    decision_path = local_iteration_paths["decision"]
    if not decision_path.exists():
        raise ConfigError(
            f"run '{run_id}' iteration {iteration} decision artifact missing after pull: {decision_path}"
        )

    decision_payload = _load_iteration_decision(decision_path) or {}
    return IterationDecisionPullResult(
        run_id=run_id,
        iteration=iteration,
        remote_results_dir=remote_results_dir,
        local_results_dir=local_results_dir,
        decision_path=decision_path,
        metrics_path=local_iteration_paths["metrics"],
        decision=str(decision_payload.get("decision")) if decision_payload.get("decision") else None,
        command_preview=sync_result.argv,
    )


def watch_and_auto_advance_tuning(
    workspace_root: str | Path,
    run_id: str,
    *,
    mode: ExecutionMode = ExecutionMode.EXECUTE,
    scheduler_adapter: SchedulerAdapter | None = None,
    transfer_adapter: FileTransferAdapter | None = None,
    remote_exec_adapter: RemoteExecAdapter | None = None,
    poll_interval_seconds: int | None = None,
    timeout_seconds: int | None = None,
    max_polls: int | None = None,
    fetch_on_complete: bool = False,
) -> AutoAdvanceResult:
    if mode != ExecutionMode.EXECUTE:
        raise ConfigError("auto-advance requires execute mode")

    validated_run_id = validate_run_id(run_id)
    root = detect_workspace_root(workspace_root)
    local_paths, manifest, config, cluster = _resolve_cluster_for_run(root, validated_run_id)

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

    records: list[AutoAdvanceIterationRecord] = []

    while True:
        manifest = read_manifest(local_paths.manifest)
        current_iteration = manifest.tuning_iteration_tracker.current_iteration

        watch_result = watch_run_lifecycle(
            workspace_root=root,
            run_id=validated_run_id,
            mode=mode,
            scheduler_adapter=scheduler_adapter,
            transfer_adapter=transfer_adapter,
            poll_interval_seconds=poll_interval_seconds,
            timeout_seconds=timeout_seconds,
            max_polls=max_polls,
            fetch_on_complete=fetch_on_complete,
        )

        if watch_result.terminal_state in {RunLifecycleState.FAILED, RunLifecycleState.CANCELLED}:
            records.append(
                AutoAdvanceIterationRecord(
                    iteration=current_iteration,
                    terminal_state=watch_result.terminal_state,
                    decision=None,
                    advance_action="scheduler_terminal_failure",
                    submitted_job_id=None,
                )
            )
            latest = read_manifest(local_paths.manifest)
            return AutoAdvanceResult(
                run_id=validated_run_id,
                final_action="scheduler_terminal_failure",
                tracker_status=latest.tuning_iteration_tracker.status,
                final_iteration=current_iteration,
                final_terminal_state=watch_result.terminal_state,
                iterations=records,
            )

        pull_result = _pull_iteration_decision_artifacts(
            run_id=validated_run_id,
            iteration=current_iteration,
            local_paths=local_paths,
            cluster=cluster,
            transfer_adapter=transfer_adapter,
        )

        advance_result = advance_tune_iteration(
            workspace_root=root,
            run_id=validated_run_id,
            execute=True,
            transfer_adapter=transfer_adapter,
            scheduler_adapter=scheduler_adapter,
            remote_exec_adapter=remote_exec_adapter,
        )

        records.append(
            AutoAdvanceIterationRecord(
                iteration=current_iteration,
                terminal_state=watch_result.terminal_state,
                decision=pull_result.decision,
                advance_action=advance_result.action,
                submitted_job_id=advance_result.submitted_job_id,
            )
        )

        if advance_result.action == "advanced_and_submitted":
            continue
        if advance_result.action == "already_converged":
            return AutoAdvanceResult(
                run_id=validated_run_id,
                final_action="converged",
                tracker_status=advance_result.tracker_status,
                final_iteration=current_iteration,
                final_terminal_state=watch_result.terminal_state,
                iterations=records,
            )
        if advance_result.action == "paused_needs_review":
            return AutoAdvanceResult(
                run_id=validated_run_id,
                final_action="needs_review_pause",
                tracker_status=advance_result.tracker_status,
                final_iteration=current_iteration,
                final_terminal_state=watch_result.terminal_state,
                iterations=records,
            )
        if advance_result.action == "max_iter_failed":
            return AutoAdvanceResult(
                run_id=validated_run_id,
                final_action="max_iter_failed",
                tracker_status=advance_result.tracker_status,
                final_iteration=current_iteration,
                final_terminal_state=watch_result.terminal_state,
                iterations=records,
            )

        raise ConfigError(
            f"unexpected auto-advance action '{advance_result.action}' for run '{validated_run_id}'"
        )


def _unique_iteration_records_for_fetch(manifest) -> list:
    records_by_iteration: dict[int, object] = {}
    for record in manifest.tuning_iteration_tracker.iterations:
        records_by_iteration[int(record.iteration)] = record
    return [records_by_iteration[key] for key in sorted(records_by_iteration)]


def _gather_fetched_files(local_root: Path) -> list[str]:
    if not local_root.exists():
        return []
    return sorted(
        str(path.relative_to(local_root))
        for path in local_root.rglob("*")
        if path.is_file()
    )


def _missing_expected_artifacts(*, local_pull_root: Path, manifest) -> list[str]:
    missing: list[str] = []
    if not (local_pull_root / "manifest.yaml").exists():
        missing.append("manifest.yaml")

    for record in _unique_iteration_records_for_fetch(manifest):
        iter_name = iteration_dir_name(int(record.iteration))
        local_iter_root = local_pull_root / "iterations" / iter_name
        local_logs = local_iter_root / "logs"
        local_results = local_iter_root / "results"

        log_files = [path for path in local_logs.rglob("*") if path.is_file()] if local_logs.exists() else []
        result_files = (
            [path for path in local_results.rglob("*") if path.is_file()]
            if local_results.exists()
            else []
        )
        if not log_files and not result_files:
            missing.append(f"iterations/{iter_name}/logs_or_results")

    for record in getattr(manifest, "adaptation_runs", []) or []:
        model_dir = local_pull_root / "adaptation" / f"from-{iteration_dir_name(int(record.source_preop_iteration))}" / str(record.model).lower()
        results_dir = model_dir / "results"
        if not results_dir.exists():
            missing.append(str(results_dir.relative_to(local_pull_root)))
            continue
        if not any(path.is_file() for path in results_dir.rglob("*")):
            missing.append(str(results_dir.relative_to(local_pull_root)))
    return missing


def fetch_run_artifacts(
    workspace_root: str | Path,
    run_id: str,
    mode: ExecutionMode = ExecutionMode.EXECUTE,
    transfer_adapter: FileTransferAdapter | None = None,
) -> FetchResult:
    validated_run_id = validate_run_id(run_id)
    root = detect_workspace_root(workspace_root)
    local_paths, manifest, config, cluster = _resolve_cluster_for_run(root, validated_run_id)

    remote_run_dir = manifest.execution.remote_run_dir or manifest.remote.get("remote_run_dir")
    if not remote_run_dir:
        raise ConfigError(f"run '{validated_run_id}' is missing remote_run_dir in manifest")

    pull_patterns = list(manifest.artifacts.get("pull_patterns") or config.defaults.artifacts.pull)
    if transfer_adapter is None:
        transfer_adapter, _, _ = _build_default_adapters(
            cluster=cluster,
            config=config,
            run_id=validated_run_id,
            mode=mode,
        )

    local_pull_root = Path(manifest.local_paths.pulled_outputs)
    local_pull_root.mkdir(parents=True, exist_ok=True)

    sync_results = []
    sync_results.append(
        transfer_adapter.sync(
            local_dir=str(local_pull_root),
            remote_dir=remote_run_dir,
            include=["manifest.yaml"],
            exclude=["*"],
            direction=SyncDirection.PULL,
        )
    )

    for record in _unique_iteration_records_for_fetch(manifest):
        iter_name = iteration_dir_name(int(record.iteration))
        remote_iter_dir = str(record.remote_dir or "").strip()
        if not remote_iter_dir:
            remote_iter_dir = str(PurePosixPath(remote_run_dir) / "iterations" / iter_name)

        local_iter_root = local_pull_root / "iterations" / iter_name
        local_logs_dir = local_iter_root / "logs"
        local_results_dir = local_iter_root / "results"
        local_logs_dir.mkdir(parents=True, exist_ok=True)
        local_results_dir.mkdir(parents=True, exist_ok=True)

        sync_results.append(
            transfer_adapter.sync(
                local_dir=str(local_logs_dir),
                remote_dir=str(PurePosixPath(remote_iter_dir) / "logs"),
                include=[],
                exclude=[],
                direction=SyncDirection.PULL,
            )
        )
        sync_results.append(
            transfer_adapter.sync(
                local_dir=str(local_results_dir),
                remote_dir=str(PurePosixPath(remote_iter_dir) / "results"),
                include=[],
                exclude=[],
                direction=SyncDirection.PULL,
            )
        )

    for record in getattr(manifest, "adaptation_runs", []) or []:
        remote_adapt_dir = str(record.remote_dir or "").strip()
        if not remote_adapt_dir:
            continue
        local_adapt_root = (
            local_pull_root
            / "adaptation"
            / f"from-{iteration_dir_name(int(record.source_preop_iteration))}"
            / str(record.model).lower()
        )
        local_adapt_logs = local_adapt_root / "logs"
        local_adapt_results = local_adapt_root / "results"
        local_adapt_logs.mkdir(parents=True, exist_ok=True)
        local_adapt_results.mkdir(parents=True, exist_ok=True)
        sync_results.append(
            transfer_adapter.sync(
                local_dir=str(local_adapt_logs),
                remote_dir=str(PurePosixPath(remote_adapt_dir) / "logs"),
                include=[],
                exclude=[],
                direction=SyncDirection.PULL,
            )
        )
        sync_results.append(
            transfer_adapter.sync(
                local_dir=str(local_adapt_results),
                remote_dir=str(PurePosixPath(remote_adapt_dir) / "results"),
                include=[],
                exclude=[],
                direction=SyncDirection.PULL,
            )
        )

    executed_pull = any(not result.dry_run for result in sync_results)
    fetched_files = _gather_fetched_files(local_pull_root)
    fetched_artifacts = fetched_files or pull_patterns

    success = True
    missing_expected: list[str] = []
    if mode == ExecutionMode.EXECUTE and executed_pull:
        missing_expected = _missing_expected_artifacts(
            local_pull_root=local_pull_root,
            manifest=manifest,
        )
        success = not missing_expected

    updated = record_fetch(
        manifest,
        fetched_artifacts=fetched_artifacts,
        success=success,
    )
    write_manifest(updated, local_paths.manifest)

    if not success:
        raise ConfigError(
            "artifact pull completed but expected files are missing: "
            + ", ".join(missing_expected)
        )

    command_preview = sync_results[0].argv if sync_results else [
        "rsync",
        "<none>",
    ]
    return FetchResult(
        run_id=validated_run_id,
        remote_run_dir=remote_run_dir,
        local_output_dir=manifest.local_paths.pulled_outputs,
        pull_patterns=pull_patterns,
        command_preview=command_preview,
    )


def advance_tune_iteration(
    workspace_root: str | Path,
    run_id: str,
    *,
    max_iterations: int | None = None,
    execute: bool = False,
    transfer_adapter: FileTransferAdapter | None = None,
    scheduler_adapter: SchedulerAdapter | None = None,
    remote_exec_adapter: RemoteExecAdapter | None = None,
) -> AdvanceIterationResult:
    validated_run_id = validate_run_id(run_id)
    root = detect_workspace_root(workspace_root)
    local_paths, manifest, _config, _cluster = _resolve_cluster_for_run(root, validated_run_id)

    tracker = manifest.tuning_iteration_tracker
    if max_iterations is not None:
        if max_iterations < 1:
            raise ConfigError("max_iterations must be >= 1")
        if max_iterations < tracker.current_iteration:
            raise ConfigError(
                "max_iterations cannot be lower than the current iteration "
                f"({tracker.current_iteration})"
            )
        if max_iterations != tracker.max_iterations:
            tracker.max_iterations = max_iterations
            if manifest.progress_tracker is not None and manifest.progress_tracker.iterations is not None:
                manifest.progress_tracker.iterations["max"] = max_iterations
                manifest.progress_tracker.updated_at = utc_now_iso()
            manifest.updated_at = utc_now_iso()
            write_manifest(manifest, local_paths.manifest)

    current_iteration = tracker.current_iteration
    current_record = next(
        (rec for rec in tracker.iterations if rec.iteration == current_iteration),
        None,
    )
    if current_record is None:
        raise ConfigError(
            f"run '{validated_run_id}' missing iteration record for iter-{current_iteration:02d}"
        )

    # opportunistically load a local iteration decision artifact if available
    local_iter_paths = build_iteration_local_paths(local_paths, current_iteration)
    decision_payload = _load_iteration_decision(local_iter_paths["decision"])
    should_hydrate_decision = False
    if decision_payload:
        payload_regenerated_config = decision_payload.get("regenerated_config_path")
        payload_metrics = decision_payload.get("metrics")
        payload_deltas = decision_payload.get("deltas")
        should_hydrate_decision = (
            current_record.decision is None
            or (payload_regenerated_config and not current_record.regenerated_config_path)
            or (payload_metrics and current_record.metrics is None)
            or (payload_deltas and current_record.deltas is None)
        )

    if decision_payload and should_hydrate_decision:
        manifest = mark_iteration_decision(
            manifest,
            iteration=current_iteration,
            decision=str(
                decision_payload.get("decision")
                or current_record.decision
                or "not_close"
            ),
            metrics=payload_metrics or current_record.metrics,
            deltas=payload_deltas or current_record.deltas,
            regenerated_config_path=payload_regenerated_config
            or current_record.regenerated_config_path,
            postop_submission_requested=bool(
                decision_payload.get("postop_submission_requested", False)
            ),
        )
        write_manifest(manifest, local_paths.manifest)
        tracker = manifest.tuning_iteration_tracker
        current_record = next(
            (rec for rec in tracker.iterations if rec.iteration == current_iteration),
            current_record,
        )

    decision = current_record.decision
    if decision is None:
        raise ConfigError(
            f"run '{validated_run_id}' iteration {current_iteration} has no decision yet; "
            "record iteration_decision.json or mark decision in manifest first"
        )

    if decision == "needs_review":
        manifest = mark_iteration_decision(
            manifest,
            iteration=current_iteration,
            decision="needs_review",
            metrics=current_record.metrics,
            deltas=current_record.deltas,
            regenerated_config_path=current_record.regenerated_config_path,
            postop_submission_requested=False,
        )
        write_manifest(manifest, local_paths.manifest)
        return AdvanceIterationResult(
            run_id=validated_run_id,
            previous_iteration=current_iteration,
            next_iteration=None,
            tracker_status=manifest.tuning_iteration_tracker.status,
            action="paused_needs_review",
            submitted_job_id=None,
        )

    if decision == "converged":
        manifest = mark_iteration_decision(
            manifest,
            iteration=current_iteration,
            decision="converged",
            metrics=current_record.metrics,
            deltas=current_record.deltas,
            regenerated_config_path=current_record.regenerated_config_path,
            postop_submission_requested=current_record.postop_submission_requested,
        )
        write_manifest(manifest, local_paths.manifest)
        return AdvanceIterationResult(
            run_id=validated_run_id,
            previous_iteration=current_iteration,
            next_iteration=None,
            tracker_status=manifest.tuning_iteration_tracker.status,
            action="already_converged",
            submitted_job_id=None,
        )

    manifest = advance_iteration(manifest)
    write_manifest(manifest, local_paths.manifest)
    next_iteration = manifest.tuning_iteration_tracker.current_iteration

    if manifest.tuning_iteration_tracker.status == "failed_max_iter":
        manifest = mark_iteration_decision(
            manifest,
            iteration=current_iteration,
            decision="max_iter_failed",
            metrics=current_record.metrics,
            deltas=current_record.deltas,
            regenerated_config_path=current_record.regenerated_config_path,
            postop_submission_requested=False,
        )
        write_manifest(manifest, local_paths.manifest)
        return AdvanceIterationResult(
            run_id=validated_run_id,
            previous_iteration=current_iteration,
            next_iteration=None,
            tracker_status=manifest.tuning_iteration_tracker.status,
            action="max_iter_failed",
            submitted_job_id=None,
        )

    if not execute:
        return AdvanceIterationResult(
            run_id=validated_run_id,
            previous_iteration=current_iteration,
            next_iteration=next_iteration,
            tracker_status=manifest.tuning_iteration_tracker.status,
            action="advanced_no_submit",
            submitted_job_id=None,
        )

    submit_result = run_tune_trees(
        workspace_root=root,
        cluster_name=manifest.cluster["name"],
        patient_alias=manifest.patient["alias"],
        run_id=validated_run_id,
        iteration=next_iteration,
        mode=ExecutionMode.EXECUTE,
        transfer_adapter=transfer_adapter,
        scheduler_adapter=scheduler_adapter,
        remote_exec_adapter=remote_exec_adapter,
    )

    return AdvanceIterationResult(
        run_id=validated_run_id,
        previous_iteration=current_iteration,
        next_iteration=next_iteration,
        tracker_status="active",
        action="advanced_and_submitted",
        submitted_job_id=submit_result.submitted_job_id,
    )


def continue_tune_iteration(
    workspace_root: str | Path,
    run_id: str,
    *,
    execute: bool = False,
    transfer_adapter: FileTransferAdapter | None = None,
    scheduler_adapter: SchedulerAdapter | None = None,
    remote_exec_adapter: RemoteExecAdapter | None = None,
) -> AdvanceIterationResult:
    """Force-advance a tuning iteration that is stuck in needs_review due to a driver timeout.

    When the iteration driver job times out before writing a decision, the run is
    left with decision=needs_review.  This function treats such an iteration as
    not_close and advances to the next iteration so the pipeline can continue.
    Use only when svzt status reports an iteration_driver timeout as the
    needs_review_reason.
    """
    validated_run_id = validate_run_id(run_id)
    root = detect_workspace_root(workspace_root)
    local_paths, manifest, _config, _cluster = _resolve_cluster_for_run(root, validated_run_id)

    tracker = manifest.tuning_iteration_tracker
    current_iteration = tracker.current_iteration
    current_record = next(
        (rec for rec in tracker.iterations if rec.iteration == current_iteration),
        None,
    )
    if current_record is None:
        raise ConfigError(
            f"run '{validated_run_id}' missing iteration record for iter-{current_iteration:02d}"
        )

    # Try to load a local iteration decision artifact; if it says converged or not_close
    # we can just hand off to the normal advance path.
    local_iter_paths = build_iteration_local_paths(local_paths, current_iteration)
    decision_payload = _load_iteration_decision(local_iter_paths["decision"])
    decision = (
        str(decision_payload.get("decision") or "")
        if decision_payload
        else (current_record.decision or "")
    )

    if decision in {"converged", "not_close"}:
        return advance_tune_iteration(
            workspace_root=root,
            run_id=validated_run_id,
            execute=execute,
            transfer_adapter=transfer_adapter,
            scheduler_adapter=scheduler_adapter,
            remote_exec_adapter=remote_exec_adapter,
        )

    if decision and decision not in {"needs_review", "incomplete", ""}:
        raise ConfigError(
            f"run '{validated_run_id}' iteration {current_iteration} has unexpected decision "
            f"'{decision}'; svzt continue is only valid for needs_review / timeout states"
        )

    # Force the iteration to not_close so the pipeline can proceed.
    manifest = mark_iteration_decision(
        manifest,
        iteration=current_iteration,
        decision="not_close",
        metrics=current_record.metrics,
        deltas=current_record.deltas,
        regenerated_config_path=current_record.regenerated_config_path,
        postop_submission_requested=False,
    )
    write_manifest(manifest, local_paths.manifest)

    manifest = advance_iteration(manifest)
    write_manifest(manifest, local_paths.manifest)
    next_iteration = manifest.tuning_iteration_tracker.current_iteration

    if manifest.tuning_iteration_tracker.status == "failed_max_iter":
        return AdvanceIterationResult(
            run_id=validated_run_id,
            previous_iteration=current_iteration,
            next_iteration=None,
            tracker_status=manifest.tuning_iteration_tracker.status,
            action="max_iter_failed",
            submitted_job_id=None,
        )

    if not execute:
        return AdvanceIterationResult(
            run_id=validated_run_id,
            previous_iteration=current_iteration,
            next_iteration=next_iteration,
            tracker_status=manifest.tuning_iteration_tracker.status,
            action="timeout_bypassed_no_submit",
            submitted_job_id=None,
        )

    submit_result = run_tune_trees(
        workspace_root=root,
        cluster_name=manifest.cluster["name"],
        patient_alias=manifest.patient["alias"],
        run_id=validated_run_id,
        iteration=next_iteration,
        mode=ExecutionMode.EXECUTE,
        transfer_adapter=transfer_adapter,
        scheduler_adapter=scheduler_adapter,
        remote_exec_adapter=remote_exec_adapter,
    )

    return AdvanceIterationResult(
        run_id=validated_run_id,
        previous_iteration=current_iteration,
        next_iteration=next_iteration,
        tracker_status="active",
        action="timeout_bypassed_and_submitted",
        submitted_job_id=submit_result.submitted_job_id,
    )


def render_plan_human(plan: ExecutionPlan) -> str:
    return render_execution_plan(plan)
