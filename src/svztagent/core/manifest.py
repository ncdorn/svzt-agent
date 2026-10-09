"""Run manifest schema and persistence helpers."""

from __future__ import annotations

from datetime import UTC, datetime
import os
from pathlib import Path
import shutil
import tempfile

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
    # A promoted calibrated full-PA seed is kept separately from the tuning
    # driver's regenerated reduced seed.  This preserves the original result
    # and makes seed promotion append-only and auditable.
    calibrated_seed_path: str | None = None
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
    terminal_state: str | None = None
    descriptor_path: str | None = None
    input_digests: dict[str, str] = Field(default_factory=dict)
    output_digests: dict[str, str] = Field(default_factory=dict)
    notes: list[str] = Field(default_factory=list)


class CalibrationTransition(BaseModel):
    """Append-only state transition for an agent calibration stage."""

    at: str
    from_state: str
    to_state: str
    reason: str | None = None
    note: str | None = None


class CalibrationRunRecord(BaseModel):
    """Agent-owned provenance for one upstream calibration attempt.

    The agent records paths, digests, scheduler identity, and lifecycle only;
    validation of the scientific descriptor and calibration reports remains in
    svZeroDTrees.
    """

    calibration_id: str | None = None
    iteration: int
    stage: str = "preop"
    status: str = "planned"
    local_dir: str
    remote_dir: str
    local_job_script_path: str | None = None
    remote_job_script_path: str | None = None
    local_config_path: str | None = None
    remote_config_path: str | None = None
    scheduler_job_id: str | None = None
    postprocess_job_id: str | None = None
    dependency_job_id: str | None = None
    dependency_type: str = "afterok"
    submitted_at: str | None = None
    updated_at: str | None = None
    terminal_state: str | None = None
    input_artifacts: dict[str, str] = Field(default_factory=dict)
    input_digests: dict[str, str] = Field(default_factory=dict)
    output_artifacts: dict[str, str] = Field(default_factory=dict)
    output_digests: dict[str, str] = Field(default_factory=dict)
    lineage: dict[str, str] = Field(default_factory=dict)
    validated_lineage: bool = False
    promotion_status: str = "not_attempted"
    promotion_reason: str | None = None
    transitions: list[CalibrationTransition] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


class PromotionRecord(BaseModel):
    """Compare-and-set result for a calibrated next-iteration seed."""

    iteration: int
    status: str = "rejected"
    decision: str = "not_promoted"
    source_calibration_id: str | None = None
    calibration_input_digest: str | None = None
    tuned_model_digest: str | None = None
    descriptor_digest: str | None = None
    calibrated_model_digest: str | None = None
    candidate_path: str | None = None
    previous_seed_path: str | None = None
    promoted_seed_path: str | None = None
    reason: str | None = None
    validated_lineage: bool = False
    terminal_state: str | None = None
    at: str


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
    # Automatic preoperative calibration may run once per tuning iteration;
    # retain every postprocess record instead of overwriting the selected
    # preop handoff record used by the explicit postop workflow.
    preop_postprocess_runs: list[PostprocessRunRecord] = Field(default_factory=list)
    calibration_runs: list[CalibrationRunRecord] = Field(default_factory=list)
    promotion_records: list[PromotionRecord] = Field(default_factory=list)
    calibration_promotion: PromotionRecord | None = None
    last_known_good_promotion: PromotionRecord | None = None
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
    reset_previous_outcome: bool = False,
) -> RunManifest:
    """Record an iteration tune-job submission.

    With ``reset_previous_outcome`` (real resubmissions), an earlier attempt's
    decision, metrics, deltas, and seed paths are cleared so the new driver
    artifacts decide the iteration; the earlier attempt stays in
    ``progress_tracker.iterations.records`` and in a note.
    """
    timestamp = at or _utc_now_iso()
    updated = manifest.model_copy(deep=True)
    tracker = updated.tuning_iteration_tracker
    tracker.current_iteration = iteration
    record = _ensure_iteration_record(tracker, iteration=iteration, at=timestamp)
    if (
        reset_previous_outcome
        and record.tune_job_id
        and record.tune_job_id != tune_job_id
    ):
        record.notes.append(
            f"Resubmitted; previous tune job {record.tune_job_id} "
            f"ended with decision {record.decision}"
        )
        record.decision = None
        record.metrics = None
        record.deltas = None
        record.regenerated_config_path = None
        record.calibrated_seed_path = None
        record.postop_submission_requested = False
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


def record_driver_handoff(
    manifest: RunManifest,
    *,
    iteration: int,
    from_job_id: str,
    to_job_id: str,
    preop_job_id: str | None = None,
    at: str | None = None,
) -> RunManifest:
    """Track the post-3D driver job after the pre-3D tune job handed off to it.

    The split tune driver ends once it has queued the preop 3D run and a
    post-3D copy of itself (afterany on the 3D job); the post-3D job writes the
    iteration decision.  ``from_job_id`` must be the iteration's tracked tune
    job and must have ended.  The earlier job stays in ``jobs`` and the
    iteration's notes.
    """
    timestamp = at or _utc_now_iso()
    tracker = manifest.tuning_iteration_tracker
    record = next((rec for rec in tracker.iterations if rec.iteration == iteration), None)
    if record is None or str(record.tune_job_id or "") != str(from_job_id):
        raise ConfigError(
            f"iteration {iteration} does not track tune job {from_job_id}; cannot hand off to {to_job_id}"
        )
    if _current_lifecycle_state(manifest) not in TERMINAL_STATES:
        raise ConfigError(
            f"tune job {from_job_id} has not ended; cannot hand off to {to_job_id}"
        )
    updated = manifest.model_copy(deep=True)
    previous_jobs = [dict(job) for job in updated.jobs]
    template = previous_jobs[0] if previous_jobs else {}
    updated.jobs = [
        {
            "job_id": str(to_job_id),
            "status": RunLifecycleState.SUBMITTED.value.upper(),
            "scheduler": template.get("scheduler") or updated.execution.scheduler_type,
            "mode": template.get("mode") or "execute",
            "submitted_at": timestamp,
            "job_script_path": template.get("job_script_path") or updated.execution.job_script_path,
            "handoff_from_job_id": str(from_job_id),
            "preop_job_id": preop_job_id,
        },
        *previous_jobs,
    ]
    updated.execution.submitted_job_id = str(to_job_id)
    updated.execution.submission_timestamp = timestamp
    updated.execution.terminal_reason = None
    note = (
        f"Post-3D driver job {to_job_id} continues tune job {from_job_id}"
        + (f" after preop 3D job {preop_job_id}" if preop_job_id else "")
    )
    updated = record_lifecycle_transition(
        updated,
        to_state=RunLifecycleState.SUBMITTED,
        normalized_scheduler_state=RunLifecycleState.SUBMITTED,
        note=note,
        at=timestamp,
    )
    record = next(rec for rec in updated.tuning_iteration_tracker.iterations if rec.iteration == iteration)
    record.tune_job_id = str(to_job_id)
    record.tune_job_state = RunLifecycleState.SUBMITTED.value
    record.status = "submitted"
    record.notes.append(note)
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
    terminal_state: str | None = None,
    descriptor_path: str | None = None,
    input_digests: dict[str, str] | None = None,
    output_digests: dict[str, str] | None = None,
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
        terminal_state=terminal_state,
        descriptor_path=descriptor_path,
        input_digests=input_digests or {},
        output_digests=output_digests or {},
        notes=notes,
    )
    if field_name == "selected_preop_postprocess":
        updated.selected_preop_postprocess = record
    elif field_name == "preop_postprocess":
        updated.preop_postprocess_runs.append(record)
    elif field_name == "postop_postprocess":
        updated.postop_postprocess = record
    else:
        raise ConfigError(f"unknown postprocess manifest field: {field_name}")
    updated.updated_at = timestamp
    return updated


def _calibration_record_for_iteration(
    manifest: RunManifest,
    iteration: int,
) -> CalibrationRunRecord | None:
    for record in reversed(manifest.calibration_runs):
        if int(record.iteration) == int(iteration):
            return record
    return None


def _append_calibration_transition(
    record: CalibrationRunRecord,
    *,
    to_state: str,
    timestamp: str,
    reason: str | None = None,
    note: str | None = None,
) -> None:
    from_state = str(record.status or "planned")
    if from_state == to_state:
        return
    record.transitions.append(
        CalibrationTransition(
            at=timestamp,
            from_state=from_state,
            to_state=to_state,
            reason=reason,
            note=note,
        )
    )
    record.status = to_state


def record_calibration_submission(
    manifest: RunManifest,
    *,
    iteration: int,
    local_dir: str,
    remote_dir: str,
    local_job_script_path: str,
    remote_job_script_path: str,
    local_config_path: str | None,
    remote_config_path: str | None,
    scheduler_job_id: str,
    postprocess_job_id: str,
    input_artifacts: dict[str, str],
    input_digests: dict[str, str],
    lineage: dict[str, str] | None = None,
    calibration_id: str | None = None,
    note: str | None = None,
    at: str | None = None,
) -> RunManifest:
    """Persist a submitted calibration stage and all of its input identity.

    A fresh record is created for a changed input digest.  A matching active
    record is updated in place, which makes retry/resume idempotent while
    keeping prior changed attempts in the append-only manifest history.
    """

    timestamp = at or _utc_now_iso()
    updated = manifest.model_copy(deep=True)
    existing = _calibration_record_for_iteration(updated, iteration)
    if existing is not None and existing.input_digests != input_digests:
        _append_calibration_transition(
            existing,
            to_state="invalidated",
            timestamp=timestamp,
            reason="calibration input digest changed",
        )
        existing = None

    if existing is None:
        existing = CalibrationRunRecord(
            calibration_id=calibration_id,
            iteration=iteration,
            stage="preop",
            status="planned",
            local_dir=local_dir,
            remote_dir=remote_dir,
            local_job_script_path=local_job_script_path,
            remote_job_script_path=remote_job_script_path,
            local_config_path=local_config_path,
            remote_config_path=remote_config_path,
            input_artifacts=dict(input_artifacts),
            input_digests=dict(input_digests),
            lineage=dict(lineage or {}),
            postprocess_job_id=postprocess_job_id,
            dependency_job_id=postprocess_job_id,
            notes=[note] if note else [],
        )
        updated.calibration_runs.append(existing)
    else:
        existing.local_dir = local_dir
        existing.remote_dir = remote_dir
        existing.local_job_script_path = local_job_script_path
        existing.remote_job_script_path = remote_job_script_path
        existing.local_config_path = local_config_path
        existing.remote_config_path = remote_config_path
        existing.scheduler_job_id = scheduler_job_id
        existing.postprocess_job_id = postprocess_job_id
        existing.dependency_job_id = postprocess_job_id
        existing.input_artifacts = dict(input_artifacts)
        existing.input_digests = dict(input_digests)
        existing.lineage = dict(lineage or existing.lineage)
        if note:
            existing.notes.append(note)

    existing.scheduler_job_id = scheduler_job_id
    existing.submitted_at = timestamp
    existing.updated_at = timestamp
    _append_calibration_transition(
        existing,
        to_state="submitted",
        timestamp=timestamp,
        note=note,
    )
    updated.updated_at = timestamp
    return updated


def record_calibration_state(
    manifest: RunManifest,
    *,
    iteration: int,
    status: str,
    terminal_state: str | None = None,
    reason: str | None = None,
    note: str | None = None,
    at: str | None = None,
) -> RunManifest:
    """Append a calibration lifecycle transition without scientific parsing."""

    timestamp = at or _utc_now_iso()
    updated = manifest.model_copy(deep=True)
    record = _calibration_record_for_iteration(updated, iteration)
    if record is None:
        raise ConfigError(
            f"run '{updated.run_id}' missing calibration record for iter-{iteration:02d}"
        )
    _append_calibration_transition(
        record,
        to_state=str(status),
        timestamp=timestamp,
        reason=reason,
        note=note,
    )
    if terminal_state is not None:
        record.terminal_state = str(terminal_state)
    record.updated_at = timestamp
    updated.updated_at = timestamp
    return updated


def record_calibration_result(
    manifest: RunManifest,
    *,
    iteration: int,
    status: str,
    terminal_state: str | None = None,
    output_artifacts: dict[str, str] | None = None,
    output_digests: dict[str, str] | None = None,
    validated_lineage: bool = False,
    reason: str | None = None,
    at: str | None = None,
) -> RunManifest:
    timestamp = at or _utc_now_iso()
    updated = manifest.model_copy(deep=True)
    record = _calibration_record_for_iteration(updated, iteration)
    if record is None:
        raise ConfigError(
            f"run '{updated.run_id}' missing calibration record for iter-{iteration:02d}"
        )
    _append_calibration_transition(
        record,
        to_state=str(status),
        timestamp=timestamp,
        reason=reason,
    )
    record.terminal_state = terminal_state
    record.output_artifacts = dict(output_artifacts or {})
    record.output_digests = dict(output_digests or {})
    record.validated_lineage = bool(validated_lineage)
    record.promotion_reason = reason
    record.updated_at = timestamp
    updated.updated_at = timestamp
    return updated


def record_promotion_decision(
    manifest: RunManifest,
    *,
    iteration: int,
    status: str,
    decision: str,
    calibration_input_digest: str | None = None,
    source_calibration_id: str | None = None,
    tuned_model_digest: str | None = None,
    descriptor_digest: str | None = None,
    calibrated_model_digest: str | None = None,
    candidate_path: str | None = None,
    previous_seed_path: str | None = None,
    promoted_seed_path: str | None = None,
    reason: str | None = None,
    validated_lineage: bool = False,
    terminal_state: str | None = None,
    at: str | None = None,
) -> RunManifest:
    timestamp = at or _utc_now_iso()
    updated = manifest.model_copy(deep=True)
    record = PromotionRecord(
        iteration=iteration,
        status=status,
        decision=decision,
        source_calibration_id=source_calibration_id,
        calibration_input_digest=calibration_input_digest,
        tuned_model_digest=tuned_model_digest,
        descriptor_digest=descriptor_digest,
        calibrated_model_digest=calibrated_model_digest,
        candidate_path=candidate_path,
        previous_seed_path=previous_seed_path,
        promoted_seed_path=promoted_seed_path,
        reason=reason,
        validated_lineage=validated_lineage,
        terminal_state=terminal_state,
        at=timestamp,
    )
    updated.promotion_records.append(record)
    # Keep the current successful promotion as the last-known-good seed.  A
    # rejected candidate is still recorded in history but must not replace it.
    if status == "promoted" or decision == "promoted":
        updated.calibration_promotion = record
        updated.last_known_good_promotion = record
    calibration = _calibration_record_for_iteration(updated, iteration)
    if calibration is not None:
        calibration.promotion_status = status
        calibration.promotion_reason = reason
        calibration.updated_at = timestamp
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


def _atomic_write_text(path: Path, text: str) -> None:
    """Replace a manifest-side artifact atomically within its parent directory."""

    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=str(path.parent),
        text=True,
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_name, path)
    except Exception:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise


def write_progress_tracker(tracker: ProgressTracker, path: Path) -> None:
    payload = yaml.safe_dump(tracker.model_dump(mode="json"), sort_keys=True)
    _atomic_write_text(path, payload)


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
            "permanent_remote_path": patient.permanent_remote_path,
            "mesh_scale_factor": patient.mesh_scale_factor,
            "patient_assets": patient.patient_assets.model_dump(mode="json")
            if patient.patient_assets is not None
            else None,
            "data_policy": patient.data_policy,
        },
        repos=resolve_repository_locations(config, workspace_root),
        remote={
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
            "calibration_defaults": patient.calibration.model_dump(mode="json"),
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
    payload = yaml.safe_dump(manifest.model_dump(mode="json"), sort_keys=True)
    _atomic_write_text(path, payload)

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
    # New calibration fields are optional on disk so manifests created before
    # TASK-016 load unchanged.  Normalize the successful pointer for records
    # written by early TASK-016 versions that predate the explicit alias.
    if manifest.last_known_good_promotion is None and manifest.calibration_promotion is not None:
        if manifest.calibration_promotion.status == "promoted":
            manifest.last_known_good_promotion = manifest.calibration_promotion
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
