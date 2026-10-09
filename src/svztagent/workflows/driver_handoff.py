"""Follow the split tune driver from its pre-3D job to its post-3D job.

The tune driver no longer holds an allocation while the preop 3D run waits
and runs.  Its pre-3D job submits the 3D run and a post-3D copy of itself
(``--dependency=afterany:<3D job>``), writes
``results/iteration_handoff.json`` naming both jobs, and ends without an
iteration decision; the post-3D job writes the decision.  When the tracked
tune job has ended and that file names it as the pre-3D job, the manifest
switches to tracking the post-3D job (``record_driver_handoff``).
"""

from __future__ import annotations

import json
from pathlib import Path, PurePosixPath

from svztagent.core.manifest import (
    read_manifest,
    record_driver_handoff,
    resolve_submitted_job_id,
    write_manifest,
)
from svztagent.core.paths import LocalRunPaths, build_iteration_local_paths
from svztagent.core.state import TERMINAL_STATES, coerce_run_lifecycle_state
from svztagent.hpc.interfaces import FileTransferAdapter, SyncDirection

DRIVER_HANDOFF_FILENAME = "iteration_handoff.json"


def _load_handoff(path: Path) -> dict | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def follow_driver_handoff(
    *,
    local_paths: LocalRunPaths,
    transfer_adapter: FileTransferAdapter,
) -> str | None:
    """Record the post-3D job if the ended tune job handed off to it.

    Returns the post-3D job ID when the manifest now tracks it, else None
    (job still active, not the iteration's tune job, or no handoff for it).
    """
    manifest = read_manifest(local_paths.manifest)
    if coerce_run_lifecycle_state(manifest.execution.lifecycle_state) not in TERMINAL_STATES:
        return None
    job_id = resolve_submitted_job_id(manifest)
    tracker = manifest.tuning_iteration_tracker
    iteration = tracker.current_iteration
    record = next((rec for rec in tracker.iterations if rec.iteration == iteration), None)
    if job_id is None or record is None or str(record.tune_job_id or "") != job_id:
        return None
    if not record.remote_dir:
        return None

    local_results = build_iteration_local_paths(local_paths, iteration)["results"]
    local_results.mkdir(parents=True, exist_ok=True)
    transfer_adapter.sync(
        local_dir=str(local_results),
        remote_dir=str(PurePosixPath(record.remote_dir) / "results"),
        include=[DRIVER_HANDOFF_FILENAME],
        exclude=["*"],
        direction=SyncDirection.PULL,
    )
    handoff = _load_handoff(local_results / DRIVER_HANDOFF_FILENAME)
    if handoff is None:
        return None
    post3d_job_id = str(handoff.get("post3d_job_id") or "").strip()
    if (
        str(handoff.get("pre3d_job_id") or "").strip() != job_id
        or int(handoff.get("iteration") or 0) != iteration
        or not post3d_job_id
        or post3d_job_id == job_id
    ):
        return None

    updated = record_driver_handoff(
        manifest,
        iteration=iteration,
        from_job_id=job_id,
        to_job_id=post3d_job_id,
        preop_job_id=str(handoff.get("preop_job_id") or "") or None,
    )
    write_manifest(updated, local_paths.manifest)
    return post3d_job_id
