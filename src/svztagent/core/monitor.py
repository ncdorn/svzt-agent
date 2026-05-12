"""Run lifecycle monitoring service."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
import time
from typing import Callable

from pydantic import BaseModel, field_validator

from svztagent.core.errors import (
    MissingJobIdError,
    SchedulerLookupError,
    WatchMaxPollsExceededError,
    WatchTimeoutError,
)
from svztagent.core.manifest import (
    read_manifest,
    record_poll_observation,
    resolve_submitted_job_id,
    set_monitor_session_settings,
    write_manifest,
)
from svztagent.core.state import RunLifecycleState, coerce_run_lifecycle_state, is_terminal_state
from svztagent.core.status import normalize_slurm_state, terminal_reason_for_slurm_state
from svztagent.hpc.interfaces import SchedulerAdapter


class MonitorSettings(BaseModel):
    poll_interval_seconds: int = 30
    timeout_seconds: int | None = None
    max_polls: int | None = None
    fetch_on_complete: bool = False
    fetch_on_failure: bool = False

    @field_validator("poll_interval_seconds")
    @classmethod
    def _poll_interval_minimum(cls, value: int) -> int:
        if value < 5:
            raise ValueError("poll_interval_seconds must be >= 5")
        return value

    @field_validator("timeout_seconds")
    @classmethod
    def _timeout_positive(cls, value: int | None) -> int | None:
        if value is not None and value <= 0:
            raise ValueError("timeout_seconds must be positive")
        return value

    @field_validator("max_polls")
    @classmethod
    def _max_polls_positive(cls, value: int | None) -> int | None:
        if value is not None and value <= 0:
            raise ValueError("max_polls must be positive")
        return value


@dataclass(frozen=True)
class MonitorObservation:
    poll_count: int
    observed_at: str
    previous_state: RunLifecycleState
    normalized_state: RunLifecycleState
    raw_state: str | None
    scheduler_source: str
    used_accounting_fallback: bool
    terminal_reason: str | None


@dataclass(frozen=True)
class SchedulerPollResult:
    raw_state: str | None
    normalized_state: RunLifecycleState
    scheduler_source: str
    used_accounting_fallback: bool
    terminal_reason: str | None


@dataclass(frozen=True)
class MonitorSummary:
    run_id: str
    job_id: str
    initial_state: RunLifecycleState
    final_state: RunLifecycleState
    terminal_state: RunLifecycleState
    raw_scheduler_state: str | None
    terminal_reason: str | None
    poll_count: int
    started_at: str
    ended_at: str
    observations: list[MonitorObservation] = field(default_factory=list)
    fetch_attempted: bool = False
    fetch_succeeded: bool | None = None


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def poll_scheduler_state(scheduler_adapter: SchedulerAdapter, job_id: str) -> SchedulerPollResult:
    try:
        status_result = scheduler_adapter.status(job_id)
    except Exception as exc:
        raise SchedulerLookupError(f"scheduler status lookup failed for job '{job_id}': {exc}") from exc

    normalized = normalize_slurm_state(status_result.raw_state)
    reason = terminal_reason_for_slurm_state(status_result.raw_state)
    if normalized != RunLifecycleState.UNKNOWN:
        return SchedulerPollResult(
            raw_state=status_result.raw_state,
            normalized_state=normalized,
            scheduler_source=status_result.source,
            used_accounting_fallback=False,
            terminal_reason=reason,
        )

    try:
        accounting_result = scheduler_adapter.accounting(job_id)
    except Exception as exc:
        raise SchedulerLookupError(
            f"scheduler accounting lookup failed for job '{job_id}' after unknown status: {exc}"
        ) from exc

    normalized_accounting = normalize_slurm_state(accounting_result.raw_state)
    reason = terminal_reason_for_slurm_state(accounting_result.raw_state)
    return SchedulerPollResult(
        raw_state=accounting_result.raw_state,
        normalized_state=normalized_accounting,
        scheduler_source=accounting_result.source,
        used_accounting_fallback=True,
        terminal_reason=reason,
    )


class RunMonitorService:
    def __init__(
        self,
        scheduler_adapter: SchedulerAdapter,
        *,
        sleep_fn: Callable[[float], None] = time.sleep,
        monotonic_fn: Callable[[], float] = time.monotonic,
        now_fn: Callable[[], str] = _utc_now_iso,
    ):
        self.scheduler_adapter = scheduler_adapter
        self.sleep_fn = sleep_fn
        self.monotonic_fn = monotonic_fn
        self.now_fn = now_fn

    def watch(
        self,
        *,
        manifest_path: Path,
        settings: MonitorSettings,
    ) -> MonitorSummary:
        manifest = read_manifest(manifest_path)
        run_id = manifest.run_id
        job_id = resolve_submitted_job_id(manifest)
        if job_id is None:
            raise MissingJobIdError(f"run '{run_id}' has no submitted job id in manifest")

        started_at = self.now_fn()
        manifest = set_monitor_session_settings(
            manifest,
            poll_interval_seconds=settings.poll_interval_seconds,
            timeout_seconds=settings.timeout_seconds,
            max_polls=settings.max_polls,
            fetch_on_complete=settings.fetch_on_complete,
            fetch_on_failure=settings.fetch_on_failure,
            at=started_at,
        )
        write_manifest(manifest, manifest_path)

        initial_state = coerce_run_lifecycle_state(manifest.execution.lifecycle_state)
        observations: list[MonitorObservation] = []
        started_mono = self.monotonic_fn()

        if is_terminal_state(initial_state):
            return MonitorSummary(
                run_id=run_id,
                job_id=job_id,
                initial_state=initial_state,
                final_state=initial_state,
                terminal_state=initial_state,
                raw_scheduler_state=manifest.execution.raw_scheduler_state,
                terminal_reason=manifest.execution.terminal_reason,
                poll_count=manifest.execution.poll_count,
                started_at=started_at,
                ended_at=started_at,
                observations=observations,
                fetch_attempted=manifest.execution.fetch_attempted,
                fetch_succeeded=manifest.execution.fetch_succeeded,
            )

        while True:
            if settings.timeout_seconds is not None:
                elapsed = self.monotonic_fn() - started_mono
                if elapsed > settings.timeout_seconds:
                    raise WatchTimeoutError(
                        f"watch timed out for run '{run_id}' after {settings.timeout_seconds} seconds"
                    )

            if settings.max_polls is not None and manifest.execution.poll_count >= settings.max_polls:
                raise WatchMaxPollsExceededError(
                    f"watch exceeded max polls for run '{run_id}' (max_polls={settings.max_polls})"
                )

            polled_at = self.now_fn()
            poll_result = poll_scheduler_state(self.scheduler_adapter, job_id)
            previous_state = coerce_run_lifecycle_state(manifest.execution.lifecycle_state)

            manifest = record_poll_observation(
                manifest,
                normalized_state=poll_result.normalized_state,
                raw_state=poll_result.raw_state,
                scheduler_source=poll_result.scheduler_source,
                terminal_reason=poll_result.terminal_reason,
                at=polled_at,
            )
            write_manifest(manifest, manifest_path)

            current_state = coerce_run_lifecycle_state(manifest.execution.lifecycle_state)
            observations.append(
                MonitorObservation(
                    poll_count=manifest.execution.poll_count,
                    observed_at=polled_at,
                    previous_state=previous_state,
                    normalized_state=current_state,
                    raw_state=poll_result.raw_state,
                    scheduler_source=poll_result.scheduler_source,
                    used_accounting_fallback=poll_result.used_accounting_fallback,
                    terminal_reason=poll_result.terminal_reason,
                )
            )

            if is_terminal_state(current_state):
                ended_at = self.now_fn()
                return MonitorSummary(
                    run_id=run_id,
                    job_id=job_id,
                    initial_state=initial_state,
                    final_state=current_state,
                    terminal_state=current_state,
                    raw_scheduler_state=manifest.execution.raw_scheduler_state,
                    terminal_reason=manifest.execution.terminal_reason,
                    poll_count=manifest.execution.poll_count,
                    started_at=started_at,
                    ended_at=ended_at,
                    observations=observations,
                    fetch_attempted=manifest.execution.fetch_attempted,
                    fetch_succeeded=manifest.execution.fetch_succeeded,
                )

            self.sleep_fn(settings.poll_interval_seconds)
