"""Run manifest schema and persistence helpers."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import shutil

from pydantic import BaseModel, Field
import yaml

from svztagent.config.load import resolve_repository_locations
from svztagent.config.models import ClusterConfig, ResolvedPatient, WorkspaceConfig
from svztagent.core.errors import ConfigError
from svztagent.core.paths import LocalRunPaths
from svztagent.core.state import RunLifecycleState, TERMINAL_STATES, coerce_run_lifecycle_state
from svztagent.core.transitions import can_transition, transition_or_noop


class RunPaths(BaseModel):
    run_dir: str
    manifest: str
    progress_tracker: str
    iterations: str | None = None
    staged_inputs: str
    pulled_outputs: str
    logs: str


class ProgressMilestone(BaseModel):
    id: str
    description: str
    status: str = "pending"
    hit_at: str | None = None
    note: str | None = None


class ModelProgress(BaseModel):
    model_id: str
    label: str
    status: str = "pending"
    milestones: list[ProgressMilestone] = Field(default_factory=list)


class ProgressEvent(BaseModel):
    at: str
    model_id: str
    milestone_id: str
    status: str
    note: str | None = None


class ProgressTracker(BaseModel):
    schema_version: int = 1
    updated_at: str
    models: list[ModelProgress] = Field(default_factory=list)
    events: list[ProgressEvent] = Field(default_factory=list)
    iterations: dict | None = None


class IterationRecord(BaseModel):
    iteration: int
    status: str = "pending"
    local_dir: str | None = None
    remote_dir: str | None = None
    tune_job_id: str | None = None
    tune_job_script_path: str | None = None
    tune_job_state: str | None = None
    metrics: dict[str, float] | None = None
    deltas: dict[str, float] | None = None
    decision: str | None = None
    regenerated_config_path: str | None = None
    postop_submission_requested: bool = False
    postop_job_id: str | None = None
    updated_at: str | None = None
    notes: list[str] = Field(default_factory=list)


class ConvergedPreopIteration(BaseModel):
    iteration: int
    source_decision: str | None = None
    selection_kind: str
    reason: str | None = None
    selected_at: str
    selected_by_command: str = "svzt preop select"
    metrics: dict[str, float] | None = None
    deltas: dict[str, float] | None = None
    remote_iteration_dir: str
    remote_preop_dir: str
    remote_tuned_zerod_config: str
    remote_canonical_coupler: str
    preop_job_id: str | None = None


class PostopRunRecord(BaseModel):
    source_preop_iteration: int
    status: str = "pending"
    local_dir: str
    remote_dir: str
    local_job_script_path: str | None = None
    remote_job_script_path: str | None = None
    postop_job_id: str | None = None
    submitted_at: str | None = None
    updated_at: str | None = None
    notes: list[str] = Field(default_factory=list)


class PostprocessRunRecord(BaseModel):
    stage: str
    source_preop_iteration: int
    status: str = "pending"
    local_dir: str
    remote_dir: str
    local_job_script_path: str | None = None
    remote_job_script_path: str | None = None
    scheduler_job_id: str | None = None
    submitted_at: str | None = None
    updated_at: str | None = None
    fetched_artifacts: list[str] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


class ParaViewVizRecord(BaseModel):
    stage: str
    source_iteration: int
    status: str = "pending"
    local_dir: str
    remote_dir: str
    local_script_path: str | None = None
    remote_script_path: str | None = None
    scheduler_job_id: str | None = None
    submitted_at: str | None = None
    updated_at: str | None = None
    notes: list[str] = Field(default_factory=list)


class AdaptationInflowProvenance(BaseModel):
    source_path: str
    fingerprint: str | None = None
    metadata: dict[str, str | int | float | bool | None] = Field(default_factory=dict)


class AdaptationRunRecord(BaseModel):
    model: str
    mode: str
    parameter_set: str
    source_preop_iteration: int
    source_postop_job_id: str | None = None
    status: str = "pending"
    territory_scheme: str = "lpa_rpa"
    target_stage: str = "postop"
    local_dir: str
    remote_dir: str
    local_job_script_path: str | None = None
    remote_job_script_path: str | None = None
    scheduler_job_id: str | None = None
    submitted_at: str | None = None
    updated_at: str | None = None
    inflow_provenance: AdaptationInflowProvenance
    artifact_roots: dict[str, str] = Field(default_factory=dict)
    summary_path: str | None = None
    comparison_path: str | None = None
    notes: list[str] = Field(default_factory=list)


class TuningIterationTracker(BaseModel):
    current_iteration: int = 1
    max_iterations: int = 5
    status: str = "active"
    converged_iteration: int | None = None
    iterations: list[IterationRecord] = Field(default_factory=list)


class LifecycleHistoryEntry(BaseModel):
    at: str
    from_state: str
    to_state: str
    raw_scheduler_state: str | None = None
    normalized_scheduler_state: str | None = None
    scheduler_source: str | None = None
    reason: str | None = None
    note: str | None = None


class LifecycleTimestamps(BaseModel):
    submission_at: str | None = None
    first_pending_at: str | None = None
    first_running_at: str | None = None
    terminal_state_at: str | None = None
    fetch_at: str | None = None


class MonitorSessionSettings(BaseModel):
    poll_interval_seconds: int
    timeout_seconds: int | None = None
    max_polls: int | None = None
    fetch_on_complete: bool = False
    fetch_on_failure: bool = False


class ExecutionMetadata(BaseModel):
    plan_path: str | None = None
    remote_run_dir: str | None = None
    job_script_path: str | None = None
    submitted_job_id: str | None = None
    scheduler_type: str | None = None
    submission_timestamp: str | None = None
    last_known_scheduler_state: str | None = "unknown"

    lifecycle_state: str = RunLifecycleState.INITIALIZED.value
    raw_scheduler_state: str | None = None
    normalized_scheduler_state: str = RunLifecycleState.UNKNOWN.value
    lifecycle_history: list[LifecycleHistoryEntry] = Field(default_factory=list)
    lifecycle_timestamps: LifecycleTimestamps = Field(default_factory=LifecycleTimestamps)

    last_polled_at: str | None = None
    poll_count: int = 0
    terminal_reason: str | None = None
    monitor_settings: MonitorSessionSettings | None = None

    fetch_attempted: bool = False
    fetch_succeeded: bool | None = None
    fetch_timestamps: list[str] = Field(default_factory=list)
    retrieved_artifacts: list[str] = Field(default_factory=list)


class RunManifest(BaseModel):
    run_id: str
    created_at: str
    updated_at: str
    status: str
    cluster: dict
    patient: dict
    repos: dict
    remote: dict
    jobs: list[dict] = Field(default_factory=list)
    artifacts: dict = Field(default_factory=dict)
    local_paths: RunPaths
    progress_tracker: ProgressTracker | None = None
    execution: ExecutionMetadata = Field(default_factory=ExecutionMetadata)
    tuning_iteration_tracker: TuningIterationTracker = Field(
        default_factory=TuningIterationTracker
    )
    converged_preop_iteration: ConvergedPreopIteration | None = None
    postop_run: PostopRunRecord | None = None
    selected_preop_postprocess: PostprocessRunRecord | None = None
    postop_postprocess: PostprocessRunRecord | None = None
    adaptation_runs: list[AdaptationRunRecord] = Field(default_factory=list)
    paraview_viz_runs: list[ParaViewVizRecord] = Field(default_factory=list)


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def _default_progress_tracker(timestamp: str) -> ProgressTracker:
    return ProgressTracker(
        updated_at=timestamp,
        models=[
            ModelProgress(
                model_id="preop_model",
                label="Preop Model",
                milestones=[
                    ProgressMilestone(
                        id="planned",
                        description="Run plan includes preop BC tuning workflow.",
                    ),
                    ProgressMilestone(
                        id="bcs_tuned",
                        description="Preop boundary conditions tuned to preop targets.",
                    ),
                    ProgressMilestone(
                        id="preop_targets_verified",
                        description="Preop outputs evaluated against preop clinical targets.",
                    ),
                ],
            ),
            ModelProgress(
                model_id="postop_model",
                label="Postop Model",
                milestones=[
                    ProgressMilestone(
                        id="planned",
                        description="Run plan includes postop simulation workflow.",
                    ),
                    ProgressMilestone(
                        id="preop_bcs_applied",
                        description="Tuned preop BCs applied to postop model.",
                    ),
                    ProgressMilestone(
                        id="simulation_completed",
                        description="Postop model simulation completed.",
                    ),
                ],
            ),
            ModelProgress(
                model_id="adapted_model",
                label="Adapted Model",
                milestones=[
                    ProgressMilestone(
                        id="planned",
                        description="Run plan includes adaptation workflow.",
                    ),
                    ProgressMilestone(
                        id="adaptation_completed",
                        description="Microvascular adaptation model completed.",
                    ),
                    ProgressMilestone(
                        id="postop_targets_compared",
                        description="Adapted outputs compared to postop clinical targets.",
                    ),
                ],
            ),
        ],
        iterations={
            "current": 1,
            "max": 5,
            "status": "active",
            "records": [],
        },
    )


def _default_tuning_iteration_tracker() -> TuningIterationTracker:
    return TuningIterationTracker(
        current_iteration=1,
        max_iterations=5,
        status="active",
        converged_iteration=None,
        iterations=[IterationRecord(iteration=1, status="pending")],
    )


def _recompute_model_status(model_progress: ModelProgress) -> str:
    statuses = [milestone.status for milestone in model_progress.milestones]
    if any(status == "failed" for status in statuses):
        return "failed"
    if statuses and all(status == "completed" for status in statuses):
        return "completed"
    if any(status in {"in_progress", "completed"} for status in statuses):
        return "in_progress"
    return "pending"


def _current_lifecycle_state(manifest: RunManifest) -> RunLifecycleState:
    state = coerce_run_lifecycle_state(manifest.execution.lifecycle_state)
    if state != RunLifecycleState.UNKNOWN:
        return state
    status_state = coerce_run_lifecycle_state(manifest.status)
    if status_state != RunLifecycleState.UNKNOWN:
        return status_state
    return RunLifecycleState.UNKNOWN


def _update_lifecycle_timestamps(
    execution: ExecutionMetadata,
    *,
    target_state: RunLifecycleState,
    at: str,
) -> None:
    if target_state == RunLifecycleState.PENDING and execution.lifecycle_timestamps.first_pending_at is None:
        execution.lifecycle_timestamps.first_pending_at = at
    if target_state == RunLifecycleState.RUNNING and execution.lifecycle_timestamps.first_running_at is None:
        execution.lifecycle_timestamps.first_running_at = at
    if (
        target_state in TERMINAL_STATES
        and execution.lifecycle_timestamps.terminal_state_at is None
    ):
        execution.lifecycle_timestamps.terminal_state_at = at
    if target_state == RunLifecycleState.FETCHED and execution.lifecycle_timestamps.fetch_at is None:
        execution.lifecycle_timestamps.fetch_at = at


def resolve_submitted_job_id(manifest: RunManifest) -> str | None:
    if manifest.adaptation_runs:
        for record in reversed(manifest.adaptation_runs):
            candidate = str(record.scheduler_job_id or "").strip()
            if candidate and not candidate.startswith("<"):
                return candidate

    if manifest.postop_run is not None and manifest.postop_run.postop_job_id:
        candidate = str(manifest.postop_run.postop_job_id).strip()
        if candidate and not candidate.startswith("<"):
            return candidate

    tracker = manifest.tuning_iteration_tracker
    if tracker.iterations:
        active = next(
            (rec for rec in tracker.iterations if rec.iteration == tracker.current_iteration),
            None,
        )
        if active and active.tune_job_id:
            candidate = str(active.tune_job_id).strip()
            if candidate and not candidate.startswith("<"):
                return candidate

    candidate = manifest.execution.submitted_job_id
    if not candidate and manifest.jobs:
        candidate = str(manifest.jobs[0].get("job_id") or "")
    if candidate is None:
        return None
    job_id = str(candidate).strip()
    if not job_id or job_id.startswith("<"):
        return None
    return job_id


def _ensure_iteration_record(
    tracker: TuningIterationTracker,
    *,
    iteration: int,
    at: str | None = None,
) -> IterationRecord:
    record = next((rec for rec in tracker.iterations if rec.iteration == iteration), None)
    if record is None:
        record = IterationRecord(iteration=iteration, status="pending", updated_at=at)
        tracker.iterations.append(record)
        tracker.iterations = sorted(tracker.iterations, key=lambda item: item.iteration)
    return record


def mark_iteration_submitted(
    manifest: RunManifest,
    *,
    iteration: int,
    tune_job_id: str,
    local_dir: str,
    remote_dir: str,
    job_script_path: str,
    note: str | None = None,
    at: str | None = None,
) -> RunManifest:
    timestamp = at or _utc_now_iso()
    updated = manifest.model_copy(deep=True)
    tracker = updated.tuning_iteration_tracker
    tracker.current_iteration = iteration
    record = _ensure_iteration_record(tracker, iteration=iteration, at=timestamp)
    record.status = "submitted"
    record.tune_job_id = tune_job_id
    record.local_dir = local_dir
    record.remote_dir = remote_dir
    record.tune_job_script_path = job_script_path
    record.tune_job_state = RunLifecycleState.SUBMITTED.value
    record.updated_at = timestamp
    if note:
        record.notes.append(note)
    tracker.status = "active"

    if updated.progress_tracker is None:
        updated.progress_tracker = _default_progress_tracker(timestamp)
    if updated.progress_tracker.iterations is None:
        updated.progress_tracker.iterations = {
            "current": tracker.current_iteration,
            "max": tracker.max_iterations,
            "status": tracker.status,
            "records": [],
        }
    iterations_block = updated.progress_tracker.iterations
    iterations_block["current"] = tracker.current_iteration
    iterations_block["max"] = tracker.max_iterations
    iterations_block["status"] = tracker.status
    records = iterations_block.setdefault("records", [])
    records.append(
        {
            "iteration": iteration,
            "status": "submitted",
            "decision": None,
            "tune_job_id": tune_job_id,
            "updated_at": timestamp,
            "note": note,
        }
    )
    updated.progress_tracker.updated_at = timestamp
    updated.updated_at = timestamp
    return updated


def record_iteration_scheduler_state(
    manifest: RunManifest,
    *,
    iteration: int,
    state: RunLifecycleState | str,
    at: str | None = None,
) -> RunManifest:
    timestamp = at or _utc_now_iso()
    updated = manifest.model_copy(deep=True)
    tracker = updated.tuning_iteration_tracker
    record = _ensure_iteration_record(tracker, iteration=iteration, at=timestamp)
    normalized_state = coerce_run_lifecycle_state(state).value
    record.tune_job_state = normalized_state
    if normalized_state in {RunLifecycleState.RUNNING.value, RunLifecycleState.PENDING.value}:
        record.status = "in_progress"
    elif normalized_state in {
        RunLifecycleState.COMPLETED.value,
        RunLifecycleState.FAILED.value,
        RunLifecycleState.CANCELLED.value,
    }:
        record.status = "completed"
    record.updated_at = timestamp
    updated.updated_at = timestamp
    return updated


def mark_iteration_decision(
    manifest: RunManifest,
    *,
    iteration: int,
    decision: str,
    metrics: dict[str, float] | None = None,
    deltas: dict[str, float] | None = None,
    regenerated_config_path: str | None = None,
    postop_submission_requested: bool = False,
    max_iterations: int | None = None,
    at: str | None = None,
) -> RunManifest:
    timestamp = at or _utc_now_iso()
    updated = manifest.model_copy(deep=True)
    tracker = updated.tuning_iteration_tracker
    if max_iterations is not None:
        tracker.max_iterations = int(max_iterations)
    record = _ensure_iteration_record(tracker, iteration=iteration, at=timestamp)
    record.status = "completed"
    record.decision = decision
    record.metrics = metrics
    record.deltas = deltas
    record.regenerated_config_path = regenerated_config_path
    record.postop_submission_requested = postop_submission_requested
    record.updated_at = timestamp

    if decision == "converged":
        tracker.status = "converged"
        tracker.converged_iteration = iteration
    elif decision == "needs_review":
        tracker.status = "paused_review"
    elif decision == "max_iter_failed":
        tracker.status = "failed_max_iter"
    else:
        tracker.status = "active"

    if updated.progress_tracker is None:
        updated.progress_tracker = _default_progress_tracker(timestamp)
    if updated.progress_tracker.iterations is None:
        updated.progress_tracker.iterations = {
            "current": tracker.current_iteration,
            "max": tracker.max_iterations,
            "status": tracker.status,
            "records": [],
        }
    iterations_block = updated.progress_tracker.iterations
    iterations_block["current"] = tracker.current_iteration
    iterations_block["max"] = tracker.max_iterations
    iterations_block["status"] = tracker.status
    records = iterations_block.setdefault("records", [])
    records.append(
        {
            "iteration": iteration,
            "status": "completed",
            "decision": decision,
            "tune_job_id": record.tune_job_id,
            "updated_at": timestamp,
        }
    )
    updated.progress_tracker.updated_at = timestamp
    updated.updated_at = timestamp
    return updated


def record_converged_preop_iteration(
    manifest: RunManifest,
    *,
    iteration: int,
    source_decision: str | None,
    selection_kind: str,
    reason: str | None,
    metrics: dict[str, float] | None,
    deltas: dict[str, float] | None,
    remote_iteration_dir: str,
    remote_preop_dir: str,
    remote_tuned_zerod_config: str,
    remote_canonical_coupler: str,
    preop_job_id: str | None,
    at: str | None = None,
) -> RunManifest:
    timestamp = at or _utc_now_iso()
    updated = manifest.model_copy(deep=True)
    updated.converged_preop_iteration = ConvergedPreopIteration(
        iteration=iteration,
        source_decision=source_decision,
        selection_kind=selection_kind,
        reason=reason,
        selected_at=timestamp,
        metrics=metrics,
        deltas=deltas,
        remote_iteration_dir=remote_iteration_dir,
        remote_preop_dir=remote_preop_dir,
        remote_tuned_zerod_config=remote_tuned_zerod_config,
        remote_canonical_coupler=remote_canonical_coupler,
        preop_job_id=preop_job_id,
    )
    updated.updated_at = timestamp
    return updated


def record_postop_submission(
    manifest: RunManifest,
    *,
    source_preop_iteration: int,
    local_dir: str,
    remote_dir: str,
    local_job_script_path: str,
    remote_job_script_path: str,
    postop_job_id: str,
    note: str | None = None,
    at: str | None = None,
) -> RunManifest:
    timestamp = at or _utc_now_iso()
    updated = manifest.model_copy(deep=True)
    notes = [note] if note else []
    updated.postop_run = PostopRunRecord(
        source_preop_iteration=source_preop_iteration,
        status="submitted",
        local_dir=local_dir,
        remote_dir=remote_dir,
        local_job_script_path=local_job_script_path,
        remote_job_script_path=remote_job_script_path,
        postop_job_id=postop_job_id,
        submitted_at=timestamp,
        updated_at=timestamp,
        notes=notes,
    )
    if updated.progress_tracker is not None:
        updated = mark_progress_milestone(
            updated,
            "postop_model",
            "preop_bcs_applied",
            "completed",
            note=f"Postop submitted from converged preop iter-{source_preop_iteration:02d}.",
            at=timestamp,
        )
    updated.updated_at = timestamp
    return updated


def record_postprocess_submission(
    manifest: RunManifest,
    *,
    field_name: str,
    stage: str,
    source_preop_iteration: int,
    local_dir: str,
    remote_dir: str,
    local_job_script_path: str | None = None,
    remote_job_script_path: str | None = None,
    scheduler_job_id: str | None = None,
    note: str | None = None,
    at: str | None = None,
) -> RunManifest:
    timestamp = at or _utc_now_iso()
    updated = manifest.model_copy(deep=True)
    notes = [note] if note else []
    record = PostprocessRunRecord(
        stage=stage,
        source_preop_iteration=source_preop_iteration,
        status="submitted" if scheduler_job_id else "planned",
        local_dir=local_dir,
        remote_dir=remote_dir,
        local_job_script_path=local_job_script_path,
        remote_job_script_path=remote_job_script_path,
        scheduler_job_id=scheduler_job_id,
        submitted_at=timestamp if scheduler_job_id else None,
        updated_at=timestamp,
        notes=notes,
    )
    if field_name == "selected_preop_postprocess":
        updated.selected_preop_postprocess = record
    elif field_name == "postop_postprocess":
        updated.postop_postprocess = record
    else:
        raise ConfigError(f"unknown postprocess manifest field: {field_name}")
    updated.updated_at = timestamp
    return updated


def record_paraview_viz_submission(
    manifest: RunManifest,
    *,
    stage: str,
    source_iteration: int,
    local_dir: str,
    remote_dir: str,
    local_script_path: str | None = None,
    remote_script_path: str | None = None,
    scheduler_job_id: str | None = None,
    note: str | None = None,
    at: str | None = None,
) -> RunManifest:
    timestamp = at or _utc_now_iso()
    updated = manifest.model_copy(deep=True)
    record = ParaViewVizRecord(
        stage=stage,
        source_iteration=source_iteration,
        status="submitted" if scheduler_job_id else "planned",
        local_dir=local_dir,
        remote_dir=remote_dir,
        local_script_path=local_script_path,
        remote_script_path=remote_script_path,
        scheduler_job_id=scheduler_job_id,
        submitted_at=timestamp if scheduler_job_id else None,
        updated_at=timestamp,
        notes=[note] if note else [],
    )
    updated.paraview_viz_runs.append(record)
    updated.updated_at = timestamp
    return updated


def record_adaptation_submission(
    manifest: RunManifest,
    *,
    model: str,
    mode: str,
    parameter_set: str,
    source_preop_iteration: int,
    source_postop_job_id: str | None,
    territory_scheme: str,
    target_stage: str,
    local_dir: str,
    remote_dir: str,
    local_job_script_path: str,
    remote_job_script_path: str,
    scheduler_job_id: str,
    inflow_source_path: str,
    inflow_fingerprint: str | None,
    inflow_metadata: dict[str, str | int | float | bool | None] | None = None,
    artifact_roots: dict[str, str] | None = None,
    summary_path: str | None = None,
    comparison_path: str | None = None,
    note: str | None = None,
    at: str | None = None,
) -> RunManifest:
    timestamp = at or _utc_now_iso()
    updated = manifest.model_copy(deep=True)
    notes = [note] if note else []
    record = AdaptationRunRecord(
        model=model,
        mode=mode,
        parameter_set=parameter_set,
        source_preop_iteration=source_preop_iteration,
        source_postop_job_id=source_postop_job_id,
        status="submitted",
        territory_scheme=territory_scheme,
        target_stage=target_stage,
        local_dir=local_dir,
        remote_dir=remote_dir,
        local_job_script_path=local_job_script_path,
        remote_job_script_path=remote_job_script_path,
        scheduler_job_id=scheduler_job_id,
        submitted_at=timestamp,
        updated_at=timestamp,
        inflow_provenance=AdaptationInflowProvenance(
            source_path=inflow_source_path,
            fingerprint=inflow_fingerprint,
            metadata=inflow_metadata or {},
        ),
        artifact_roots=artifact_roots or {},
        summary_path=summary_path,
        comparison_path=comparison_path,
        notes=notes,
    )
    updated.adaptation_runs.append(record)
    if updated.progress_tracker is not None:
        updated = mark_progress_milestone(
            updated,
            "adapted_model",
            "planned",
            "completed",
            note=f"Adaptation {model} submitted with parameter set '{parameter_set}'.",
            at=timestamp,
        )
    updated.updated_at = timestamp
    return updated


def advance_iteration(
    manifest: RunManifest,
    *,
    at: str | None = None,
) -> RunManifest:
    timestamp = at or _utc_now_iso()
    updated = manifest.model_copy(deep=True)
    tracker = updated.tuning_iteration_tracker

    if tracker.status in {"converged", "paused_review"}:
        return updated

    current = tracker.current_iteration
    if current >= tracker.max_iterations:
        tracker.status = "failed_max_iter"
        record = _ensure_iteration_record(tracker, iteration=current, at=timestamp)
        if not record.decision:
            record.decision = "max_iter_failed"
        record.status = "completed"
        record.updated_at = timestamp
    else:
        tracker.current_iteration = current + 1
        tracker.status = "active"
        _ensure_iteration_record(tracker, iteration=tracker.current_iteration, at=timestamp)

    if updated.progress_tracker is None:
        updated.progress_tracker = _default_progress_tracker(timestamp)
    if updated.progress_tracker.iterations is None:
        updated.progress_tracker.iterations = {
            "current": tracker.current_iteration,
            "max": tracker.max_iterations,
            "status": tracker.status,
            "records": [],
        }
    updated.progress_tracker.iterations["current"] = tracker.current_iteration
    updated.progress_tracker.iterations["max"] = tracker.max_iterations
    updated.progress_tracker.iterations["status"] = tracker.status
    updated.progress_tracker.updated_at = timestamp
    updated.updated_at = timestamp
    return updated


def mark_progress_milestone(
    manifest: RunManifest,
    model_id: str,
    milestone_id: str,
    status: str,
    note: str | None = None,
    at: str | None = None,
) -> RunManifest:
    valid_status = {"pending", "in_progress", "completed", "failed"}
    if status not in valid_status:
        raise ConfigError(
            f"invalid milestone status '{status}'; expected one of {sorted(valid_status)}"
        )

    timestamp = at or _utc_now_iso()
    updated = manifest.model_copy(deep=True)
    if updated.progress_tracker is None:
        updated.progress_tracker = _default_progress_tracker(timestamp)

    tracker = updated.progress_tracker
    tracker.updated_at = timestamp

    model = next((item for item in tracker.models if item.model_id == model_id), None)
    if model is None:
        raise ConfigError(f"unknown progress model_id '{model_id}'")

    milestone = next((item for item in model.milestones if item.id == milestone_id), None)
    if milestone is None:
        raise ConfigError(f"unknown milestone '{milestone_id}' for model '{model_id}'")

    milestone.status = status
    milestone.hit_at = timestamp if status in {"in_progress", "completed", "failed"} else None
    milestone.note = note
    model.status = _recompute_model_status(model)

    tracker.events.append(
        ProgressEvent(
            at=timestamp,
            model_id=model_id,
            milestone_id=milestone_id,
            status=status,
            note=note,
        )
    )

    updated.updated_at = timestamp
    return updated


def set_monitor_session_settings(
    manifest: RunManifest,
    *,
    poll_interval_seconds: int,
    timeout_seconds: int | None,
    max_polls: int | None,
    fetch_on_complete: bool,
    fetch_on_failure: bool,
    at: str | None = None,
) -> RunManifest:
    timestamp = at or _utc_now_iso()
    updated = manifest.model_copy(deep=True)
    updated.execution.monitor_settings = MonitorSessionSettings(
        poll_interval_seconds=poll_interval_seconds,
        timeout_seconds=timeout_seconds,
        max_polls=max_polls,
        fetch_on_complete=fetch_on_complete,
        fetch_on_failure=fetch_on_failure,
    )
    updated.updated_at = timestamp
    return updated


def record_lifecycle_transition(
    manifest: RunManifest,
    *,
    to_state: RunLifecycleState | str,
    raw_scheduler_state: str | None = None,
    normalized_scheduler_state: RunLifecycleState | str | None = None,
    scheduler_source: str | None = None,
    reason: str | None = None,
    note: str | None = None,
    at: str | None = None,
) -> RunManifest:
    timestamp = at or _utc_now_iso()
    updated = manifest.model_copy(deep=True)

    source_state = _current_lifecycle_state(updated)
    target_state = coerce_run_lifecycle_state(to_state, default=source_state)
    normalized_state = coerce_run_lifecycle_state(
        normalized_scheduler_state,
        default=target_state,
    )

    changed = transition_or_noop(source_state, target_state)

    updated.status = target_state.value
    updated.execution.lifecycle_state = target_state.value
    updated.execution.normalized_scheduler_state = normalized_state.value
    updated.execution.last_known_scheduler_state = normalized_state.value
    if raw_scheduler_state is not None:
        updated.execution.raw_scheduler_state = raw_scheduler_state
    if reason and target_state in TERMINAL_STATES:
        updated.execution.terminal_reason = reason

    _update_lifecycle_timestamps(updated.execution, target_state=target_state, at=timestamp)

    if updated.jobs:
        updated.jobs[0]["status"] = target_state.value.upper()
        updated.jobs[0]["normalized_state"] = normalized_state.value
        updated.jobs[0]["last_checked_at"] = timestamp
        if raw_scheduler_state is not None:
            updated.jobs[0]["raw_state"] = raw_scheduler_state
        if scheduler_source is not None:
            updated.jobs[0]["scheduler_source"] = scheduler_source

    if changed:
        updated.execution.lifecycle_history.append(
            LifecycleHistoryEntry(
                at=timestamp,
                from_state=source_state.value,
                to_state=target_state.value,
                raw_scheduler_state=raw_scheduler_state,
                normalized_scheduler_state=normalized_state.value,
                scheduler_source=scheduler_source,
                reason=reason,
                note=note,
            )
        )

    updated.updated_at = timestamp
    return updated


def record_poll_observation(
    manifest: RunManifest,
    *,
    normalized_state: RunLifecycleState | str,
    raw_state: str | None,
    scheduler_source: str,
    terminal_reason: str | None = None,
    at: str | None = None,
) -> RunManifest:
    timestamp = at or _utc_now_iso()
    updated = manifest.model_copy(deep=True)
    updated.execution.poll_count += 1
    updated.execution.last_polled_at = timestamp
    updated.execution.raw_scheduler_state = raw_state
    updated.execution.normalized_scheduler_state = coerce_run_lifecycle_state(normalized_state).value
    updated.execution.last_known_scheduler_state = updated.execution.normalized_scheduler_state
    updated.updated_at = timestamp

    return record_lifecycle_transition(
        updated,
        to_state=normalized_state,
        raw_scheduler_state=raw_state,
        normalized_scheduler_state=normalized_state,
        scheduler_source=scheduler_source,
        reason=terminal_reason,
        at=timestamp,
    )


def write_progress_tracker(tracker: ProgressTracker, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(tracker.model_dump(mode="json"), f, sort_keys=True)


def create_manifest(
    run_id: str,
    cluster: ClusterConfig,
    patient: ResolvedPatient,
    local_paths: LocalRunPaths,
    workspace_root: Path,
    config: WorkspaceConfig,
) -> RunManifest:
    timestamp = _utc_now_iso()
    progress_tracker = _default_progress_tracker(timestamp)

    return RunManifest(
        run_id=run_id,
        created_at=timestamp,
        updated_at=timestamp,
        status=RunLifecycleState.INITIALIZED.value,
        cluster={
            "name": cluster.name,
            "host": cluster.host,
            "user": cluster.user,
            "scheduler_type": cluster.scheduler.type,
        },
        patient={
            "alias": patient.alias,
            "remote_path": patient.remote_path,
            "permanent_remote_path": patient.permanent_remote_path,
            "mesh_scale_factor": patient.mesh_scale_factor,
            "patient_assets": patient.patient_assets.model_dump(mode="json")
            if patient.patient_assets is not None
            else None,
            "data_policy": patient.data_policy,
        },
        repos=resolve_repository_locations(config, workspace_root),
        remote={
            "patient_data_root": cluster.remote_roots.patient_data_root,
            "permanent_data_root": cluster.remote_roots.permanent_data_root,
            "runs_root": cluster.remote_roots.runs_root,
            "remote_run_dir": f"{cluster.remote_roots.runs_root}/{run_id}",
            "svzerodtrees_paths": {
                "clinical_targets": patient.patient_assets.clinical_targets
                if patient.patient_assets
                else None,
                "inflow": patient.patient_assets.inflow if patient.patient_assets else None,
                "mesh_surfaces": patient.patient_assets.mesh_surfaces_dir
                if patient.patient_assets
                else None,
                "preop_mesh_complete": patient.patient_assets.preop_mesh_complete_dir
                if patient.patient_assets
                else None,
                "postop_mesh_complete": patient.patient_assets.postop_mesh_complete_dir
                if patient.patient_assets
                else None,
            "centerlines": patient.patient_assets.centerlines
                if patient.patient_assets
                else None,
            },
            "tuning_bc_type": patient.bc_type,
            "threed_defaults": patient.threed.model_dump(mode="json"),
            "impedance_defaults": patient.impedance.model_dump(mode="json"),
            "rcr_defaults": patient.rcr.model_dump(mode="json"),
            "adaptation_defaults": patient.adaptation.model_dump(mode="json"),
            "scheduler_defaults": config.defaults.scheduler.model_dump(mode="json"),
            "monitoring_defaults": config.defaults.monitoring.model_dump(mode="json"),
        },
        artifacts={
            "pull_patterns": config.defaults.artifacts.pull,
            "include_patterns": config.defaults.rsync.include_patterns,
            "exclude_patterns": config.defaults.rsync.exclude_patterns,
        },
        local_paths=RunPaths(
            run_dir=str(local_paths.run_dir),
            manifest=str(local_paths.manifest),
            progress_tracker=str(local_paths.progress_tracker),
            iterations=str(local_paths.iterations),
            staged_inputs=str(local_paths.staged_inputs),
            pulled_outputs=str(local_paths.pulled_outputs),
            logs=str(local_paths.logs),
        ),
        progress_tracker=progress_tracker,
        execution=ExecutionMetadata(
            remote_run_dir=f"{cluster.remote_roots.runs_root}/{run_id}",
            scheduler_type=cluster.scheduler.type,
            lifecycle_state=RunLifecycleState.INITIALIZED.value,
            normalized_scheduler_state=RunLifecycleState.UNKNOWN.value,
            last_known_scheduler_state=RunLifecycleState.UNKNOWN.value,
        ),
        tuning_iteration_tracker=_default_tuning_iteration_tracker(),
    )


def record_plan_path(manifest: RunManifest, plan_path: str, *, at: str | None = None) -> RunManifest:
    updated = manifest.model_copy(deep=True)
    updated.execution.plan_path = plan_path
    updated.updated_at = at or _utc_now_iso()
    return updated


def record_submission(
    manifest: RunManifest,
    *,
    remote_run_dir: str,
    job_script_path: str,
    scheduler_type: str,
    submitted_job_id: str,
    mode: str,
    at: str | None = None,
) -> RunManifest:
    timestamp = at or _utc_now_iso()
    updated = manifest.model_copy(deep=True)
    updated.execution.remote_run_dir = remote_run_dir
    updated.execution.job_script_path = job_script_path
    updated.execution.scheduler_type = scheduler_type
    updated.execution.submitted_job_id = submitted_job_id
    updated.execution.submission_timestamp = timestamp
    updated.execution.lifecycle_timestamps.submission_at = (
        updated.execution.lifecycle_timestamps.submission_at or timestamp
    )
    updated.execution.last_known_scheduler_state = RunLifecycleState.SUBMITTED.value
    updated.execution.normalized_scheduler_state = RunLifecycleState.SUBMITTED.value
    updated.updated_at = timestamp
    updated.jobs = [
        {
            "job_id": submitted_job_id,
            "status": RunLifecycleState.SUBMITTED.value.upper(),
            "scheduler": scheduler_type,
            "mode": mode,
            "submitted_at": timestamp,
            "job_script_path": job_script_path,
        }
    ]
    return updated


def record_scheduler_state(
    manifest: RunManifest,
    *,
    normalized_state: str,
    raw_state: str | None = None,
    at: str | None = None,
) -> RunManifest:
    return record_poll_observation(
        manifest,
        normalized_state=normalized_state,
        raw_state=raw_state,
        scheduler_source="status",
        at=at,
    )


def record_fetch(
    manifest: RunManifest,
    *,
    fetched_artifacts: list[str],
    at: str | None = None,
    success: bool = True,
) -> RunManifest:
    timestamp = at or _utc_now_iso()
    updated = manifest.model_copy(deep=True)
    updated.execution.fetch_attempted = True
    updated.execution.fetch_succeeded = success
    updated.execution.fetch_timestamps.append(timestamp)
    updated.execution.retrieved_artifacts = fetched_artifacts
    if success and updated.execution.lifecycle_timestamps.fetch_at is None:
        updated.execution.lifecycle_timestamps.fetch_at = timestamp
    updated.updated_at = timestamp
    updated.artifacts["last_fetch_at"] = timestamp
    updated.artifacts["retrieved_artifacts"] = fetched_artifacts

    current_state = _current_lifecycle_state(updated)
    if success and can_transition(current_state, RunLifecycleState.FETCHED):
        updated = record_lifecycle_transition(
            updated,
            to_state=RunLifecycleState.FETCHED,
            note="Artifacts fetched",
            at=timestamp,
        )
    return updated


def write_manifest(manifest: RunManifest, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(manifest.model_dump(mode="json"), f, sort_keys=True)

    if manifest.progress_tracker is not None:
        write_progress_tracker(manifest.progress_tracker, Path(manifest.local_paths.progress_tracker))


def read_manifest(path: Path) -> RunManifest:
    if not path.exists():
        raise ConfigError(f"manifest not found: {path}")
    try:
        with path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"Invalid YAML in {path}: {exc}") from exc
    try:
        manifest = RunManifest.model_validate(data)
    except Exception as exc:
        raise ConfigError(f"manifest validation failed at {path}: {exc}") from exc

    if manifest.execution.lifecycle_state == RunLifecycleState.INITIALIZED.value and manifest.status:
        status_state = coerce_run_lifecycle_state(manifest.status)
        if status_state != RunLifecycleState.UNKNOWN:
            manifest.execution.lifecycle_state = status_state.value
    if not manifest.execution.normalized_scheduler_state:
        manifest.execution.normalized_scheduler_state = RunLifecycleState.UNKNOWN.value
    if not manifest.execution.last_known_scheduler_state:
        manifest.execution.last_known_scheduler_state = manifest.execution.normalized_scheduler_state
    if manifest.progress_tracker is None:
        manifest.progress_tracker = _default_progress_tracker(_utc_now_iso())
    if manifest.progress_tracker.iterations is None:
        manifest.progress_tracker.iterations = {
            "current": manifest.tuning_iteration_tracker.current_iteration,
            "max": manifest.tuning_iteration_tracker.max_iterations,
            "status": manifest.tuning_iteration_tracker.status,
            "records": [],
        }
    if not manifest.tuning_iteration_tracker.iterations:
        manifest.tuning_iteration_tracker = _default_tuning_iteration_tracker()
    return manifest


def update_run_progress(
    manifest_path: Path,
    model_id: str,
    milestone_id: str,
    status: str,
    note: str | None = None,
) -> RunManifest:
    manifest = read_manifest(manifest_path)
    updated = mark_progress_milestone(
        manifest=manifest,
        model_id=model_id,
        milestone_id=milestone_id,
        status=status,
        note=note,
    )
    write_manifest(updated, manifest_path)
    return updated


def copy_config_snapshot(workspace_root: Path, config_snapshot_dir: Path) -> None:
    config_snapshot_dir.mkdir(parents=True, exist_ok=True)
    config_root = workspace_root / "config"
    required = ["clusters.yaml", "patients.yaml", "defaults.yaml"]
    optional = ["repositories.yaml"]

    missing = [name for name in required if not (config_root / name).exists()]
    if missing:
        raise ConfigError(
            "Cannot snapshot configs; missing files: " + ", ".join(missing)
        )

    for name in required:
        shutil.copy2(config_root / name, config_snapshot_dir / name)
    for name in optional:
        source = config_root / name
        if source.exists():
            shutil.copy2(source, config_snapshot_dir / name)
