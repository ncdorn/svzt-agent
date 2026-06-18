"""Path validation and local run workspace helpers."""

from __future__ import annotations

from dataclasses import dataclass
import posixpath
from pathlib import Path, PurePosixPath
import re

from svztagent.core.errors import PathPolicyError, UnsafePathError

_RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


@dataclass(frozen=True)
class LocalRunPaths:
    run_dir: Path
    manifest: Path
    progress_tracker: Path
    iterations: Path
    staged_inputs: Path
    pulled_outputs: Path
    logs: Path
    config_snapshot: Path
    execution_plan_json: Path
    execution_plan_yaml: Path


def iteration_dir_name(iteration: int) -> str:
    if iteration <= 0:
        raise PathPolicyError("iteration must be positive")
    return f"iter-{iteration:02d}"


def build_iteration_local_paths(paths: LocalRunPaths, iteration: int) -> dict[str, Path]:
    name = iteration_dir_name(iteration)
    root = paths.iterations / name
    return {
        "root": root,
        "staged_inputs": root / "inputs",
        "results": root / "results",
        "logs": root / "logs",
        "decision": root / "results" / "iteration_decision.json",
        "metrics": root / "results" / "iteration_metrics.json",
        "job_script": root / "run_tune_iter.sh",
    }


def validate_run_id(run_id: str) -> str:
    if not run_id:
        raise PathPolicyError("run_id is required")
    if not _RUN_ID_RE.fullmatch(run_id):
        raise PathPolicyError(
            "run_id must match ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ and contain no path separators"
        )
    if "/" in run_id or ".." in run_id:
        raise PathPolicyError("run_id cannot contain path separators or traversal segments")
    return run_id


def _normalize_posix(path: str) -> PurePosixPath:
    if not path:
        raise UnsafePathError("remote path cannot be empty")
    if not path.startswith("/"):
        raise UnsafePathError(f"remote path must be absolute: '{path}'")

    normalized = posixpath.normpath(path)
    if not normalized.startswith("/"):
        raise UnsafePathError(f"remote path must remain absolute after normalization: '{path}'")
    if "/../" in f"{normalized}/" or normalized.endswith("/.."):
        raise UnsafePathError(f"remote path cannot contain traversal segments: '{path}'")
    return PurePosixPath(normalized)


def ensure_under_remote_root(path: str, root: str) -> bool:
    candidate = _normalize_posix(path)
    remote_root = _normalize_posix(root)
    return candidate == remote_root or remote_root in candidate.parents


def validate_remote_write_path(
    path: str,
    runs_root: str,
) -> None:
    if not ensure_under_remote_root(path, runs_root):
        raise PathPolicyError(
            f"remote write path '{path}' must stay under runs_root '{runs_root}'"
        )


def validate_remote_patient_read_path(path: str, patient_root: str) -> None:
    if not ensure_under_remote_root(path, patient_root):
        raise PathPolicyError(
            f"patient path '{path}' must stay under patient root '{patient_root}'"
        )


def build_local_run_paths(workspace_root: Path, run_id: str) -> LocalRunPaths:
    validated_run_id = validate_run_id(run_id)
    runs_root = workspace_root / "runs"
    run_dir = runs_root / validated_run_id
    return LocalRunPaths(
        run_dir=run_dir,
        manifest=run_dir / "manifest.yaml",
        progress_tracker=run_dir / "progress_tracker.yaml",
        iterations=run_dir / "iterations",
        staged_inputs=run_dir / "staged_inputs",
        pulled_outputs=run_dir / "pulled_outputs",
        logs=run_dir / "logs",
        config_snapshot=run_dir / "config_snapshot",
        execution_plan_json=run_dir / "execution_plan.json",
        execution_plan_yaml=run_dir / "execution_plan.yaml",
    )


def ensure_local_run_dirs(paths: LocalRunPaths) -> None:
    paths.iterations.mkdir(parents=True, exist_ok=True)
    paths.staged_inputs.mkdir(parents=True, exist_ok=True)
    paths.pulled_outputs.mkdir(parents=True, exist_ok=True)
    paths.logs.mkdir(parents=True, exist_ok=True)
    paths.config_snapshot.mkdir(parents=True, exist_ok=True)
