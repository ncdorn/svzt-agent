from __future__ import annotations

import pytest

from svztagent.core.errors import ConfigError
from svztagent.core.manifest import (
    read_manifest,
    record_fetch,
    record_lifecycle_transition,
    record_poll_observation,
    write_manifest,
)
from svztagent.core.state import RunLifecycleState
from svztagent.workflows.tune_trees import init_run_workspace


def test_manifest_lifecycle_history_is_append_only(sample_config_files):
    paths, _ctx = init_run_workspace(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-history-001",
    )
    manifest = read_manifest(paths.manifest)

    manifest = record_lifecycle_transition(
        manifest,
        to_state=RunLifecycleState.PLANNED,
        at="2026-03-10T00:00:00+00:00",
    )
    manifest = record_lifecycle_transition(
        manifest,
        to_state=RunLifecycleState.SUBMITTED,
        at="2026-03-10T00:00:10+00:00",
    )
    manifest = record_poll_observation(
        manifest,
        normalized_state=RunLifecycleState.PENDING,
        raw_state="PENDING",
        scheduler_source="squeue",
        at="2026-03-10T00:00:20+00:00",
    )
    history_len_after_pending = len(manifest.execution.lifecycle_history)
    manifest = record_poll_observation(
        manifest,
        normalized_state=RunLifecycleState.PENDING,
        raw_state="PENDING",
        scheduler_source="squeue",
        at="2026-03-10T00:00:21+00:00",
    )
    assert len(manifest.execution.lifecycle_history) == history_len_after_pending

    manifest = record_poll_observation(
        manifest,
        normalized_state=RunLifecycleState.RUNNING,
        raw_state="RUNNING",
        scheduler_source="squeue",
        at="2026-03-10T00:01:00+00:00",
    )
    manifest = record_poll_observation(
        manifest,
        normalized_state=RunLifecycleState.FAILED,
        raw_state="OUT_OF_MEMORY",
        scheduler_source="sacct",
        terminal_reason="out_of_memory",
        at="2026-03-10T00:02:00+00:00",
    )
    manifest = record_fetch(
        manifest,
        fetched_artifacts=["manifest.yaml"],
        at="2026-03-10T00:03:00+00:00",
    )

    transitions = [(h.from_state, h.to_state) for h in manifest.execution.lifecycle_history]
    assert transitions == [
        ("initialized", "planned"),
        ("planned", "submitted"),
        ("submitted", "pending"),
        ("pending", "running"),
        ("running", "failed"),
        ("failed", "fetched"),
    ]
    assert manifest.execution.poll_count == 4
    assert manifest.execution.lifecycle_timestamps.submission_at is None
    assert manifest.execution.lifecycle_timestamps.first_pending_at == "2026-03-10T00:00:20+00:00"
    assert manifest.execution.lifecycle_timestamps.first_running_at == "2026-03-10T00:01:00+00:00"
    assert manifest.execution.lifecycle_timestamps.terminal_state_at == "2026-03-10T00:02:00+00:00"
    assert manifest.execution.lifecycle_timestamps.fetch_at == "2026-03-10T00:03:00+00:00"


def test_read_manifest_corrupt_file_raises_config_error(sample_config_files):
    paths, _ctx = init_run_workspace(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-history-corrupt",
    )
    paths.manifest.write_text("not: [valid\n", encoding="utf-8")

    with pytest.raises(ConfigError, match="manifest validation failed|Invalid YAML"):
        read_manifest(paths.manifest)


def test_lifecycle_history_persists_to_disk(sample_config_files):
    paths, _ctx = init_run_workspace(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-history-write",
    )
    manifest = read_manifest(paths.manifest)
    manifest = record_lifecycle_transition(
        manifest,
        to_state=RunLifecycleState.PLANNED,
        at="2026-03-10T01:00:00+00:00",
    )
    write_manifest(manifest, paths.manifest)

    loaded = read_manifest(paths.manifest)
    assert loaded.execution.lifecycle_history[-1].to_state == "planned"
