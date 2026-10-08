"""Agent-owned calibration scheduling and guarded full-PA promotion.

The scientific calibration implementation and artifact validation live in
``svZeroDTrees``.  This module only stages a public upstream YAML request,
submits it after a successful postprocess job, records immutable identities in
the run manifest, and applies a compare-and-set promotion when the upstream
job has published a validated result.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path, PurePosixPath
import shutil
import shlex
from typing import Any, Callable

import yaml

from svztagent.config.load import detect_workspace_root, load_workspace_config, resolve_cluster
from svztagent.core.errors import ConfigError
from svztagent.core.monitor import poll_scheduler_state
from svztagent.core.manifest import (
    CalibrationRunRecord,
    PromotionRecord,
    _utc_now_iso,
    read_manifest,
    record_calibration_result,
    record_calibration_state,
    record_calibration_submission,
    record_promotion_decision,
    write_manifest,
)
from svztagent.core.paths import (
    build_iteration_local_paths,
    build_local_run_paths,
    iteration_dir_name,
    validate_remote_write_path,
    validate_run_id,
)
from svztagent.core.status import NormalizedRunState
from svztagent.hpc.interfaces import (
    ExecutionMode,
    FileTransferAdapter,
    RemoteExecAdapter,
    SchedulerAdapter,
    SyncDirection,
)
from svztagent.hpc.slurm import SlurmSchedulerAdapter, SlurmSubmitOptions
from svztagent.workflows.postprocess import render_env_activation_hooks
from svztagent.workflows.tune_trees import _build_default_adapters


SUCCESS_TERMINAL_STATES = frozenset({"completed", "succeeded", "success"})
FAILURE_TERMINAL_STATES = frozenset({"failed", "cancelled", "canceled", "timeout"})


@dataclass(frozen=True)
class CalibrationExecutionResult:
    run_id: str
    iteration: int
    mode: ExecutionMode
    status: str
    reused: bool
    promoted: bool
    local_config_path: Path
    local_job_script_path: Path
    remote_config_path: str
    remote_job_script_path: str
    postprocess_job_id: str
    submitted_job_id: str | None
    command_previews: list[list[str]]


@dataclass(frozen=True)
class CalibrationStatusResult:
    run_id: str
    iteration: int
    status: str
    terminal_state: str | None
    scheduler_job_id: str | None
    postprocess_job_id: str | None
    dependency_job_id: str | None
    input_digests: dict[str, str]
    output_digests: dict[str, str]
    promotion_status: str
    promotion_reason: str | None


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def sha256_file(path: str | Path) -> str | None:
    """Return a file digest, or ``None`` when the artifact is not local yet."""

    candidate = Path(path).expanduser()
    if not candidate.is_file():
        return None
    digest = hashlib.sha256()
    with candidate.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _identity_digest(digests: dict[str, str]) -> str:
    encoded = json.dumps(digests, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _record_for_iteration(manifest, iteration: int) -> CalibrationRunRecord | None:
    for record in reversed(manifest.calibration_runs):
        if int(record.iteration) == int(iteration):
            return record
    return None


def _postprocess_record_for_iteration(manifest, iteration: int):
    records = list(getattr(manifest, "preop_postprocess_runs", []) or [])
    if manifest.selected_preop_postprocess is not None:
        records.append(manifest.selected_preop_postprocess)
    for record in reversed(records):
        if int(record.source_preop_iteration) == int(iteration):
            return record
    return None


def _resolve_local_candidate(
    *,
    run_root: Path,
    iteration: int,
    supplied: str | Path | None,
    names: tuple[str, ...],
) -> Path:
    if supplied is not None and str(supplied).strip():
        return Path(str(supplied)).expanduser()
    iteration_paths = build_iteration_local_paths(
        build_local_run_paths(run_root.parent.parent, run_root.name), iteration
    )
    for name in names:
        candidate = iteration_paths["results"] / name
        if candidate.exists():
            return candidate
    return iteration_paths["results"] / names[0]


def _remote_or_local_path(
    local_candidate: Path,
    *,
    remote_results_dir: str,
    default_name: str,
) -> str:
    if local_candidate.is_absolute() and str(local_candidate).startswith("/"):
        # Paths from the local run may not be meaningful on Sherlock.  Only
        # use an explicitly remote-looking path supplied by a caller.
        if "/iterations/" in str(local_candidate) and "/calibration/" not in str(local_candidate):
            return str(PurePosixPath(remote_results_dir) / default_name)
    return str(PurePosixPath(remote_results_dir) / default_name)


def _resolve_postprocess_artifacts(
    *,
    manifest,
    iteration: int,
    postprocess_record,
    suite_descriptor_path: str | Path | None,
) -> tuple[Path, str]:
    if suite_descriptor_path is not None and str(suite_descriptor_path).strip():
        local = Path(str(suite_descriptor_path)).expanduser()
    else:
        local = Path(str(postprocess_record.local_dir)) / "postprocess_suite_metadata.json"
        if not local.exists():
            local = local.parent.parent / "results" / "postprocess" / "postprocess_suite_metadata.json"
    remote_dir = str(postprocess_record.remote_dir or "").strip()
    if not remote_dir:
        remote_run = str(manifest.execution.remote_run_dir or manifest.remote.get("remote_run_dir") or "")
        remote_dir = str(
            PurePosixPath(remote_run)
            / "iterations"
            / iteration_dir_name(iteration)
            / "results"
            / "postprocess"
        )
    return local, str(PurePosixPath(remote_dir) / "postprocess_suite_metadata.json")


def _resolve_tuned_artifact(
    *,
    local_paths,
    iteration: int,
    iteration_record,
    tuned_model_path: str | Path | None,
) -> tuple[Path, str]:
    if tuned_model_path is not None and str(tuned_model_path).strip():
        local = Path(str(tuned_model_path)).expanduser()
    else:
        local_iter = build_iteration_local_paths(local_paths, iteration)
        candidates = (
            local_iter["results"] / "svzerod_3d_coupling_tuned.json",
            local_iter["results"] / "full_pa_zerod.json",
        )
        local = next((path for path in candidates if path.exists()), candidates[0])
        reported = _driver_reported_tuned_model(local_paths, iteration)
        if reported is not None:
            return local, reported
    remote_dir = str(iteration_record.remote_dir or "").strip()
    if not remote_dir:
        remote_run = str(local_paths.run_dir)
        remote_dir = str(PurePosixPath(remote_run) / "iterations" / iteration_dir_name(iteration))
    # The tune driver writes the tuned model under the iteration's results/.
    return local, str(PurePosixPath(remote_dir) / "results" / local.name)


def _driver_reported_tuned_model(local_paths, iteration: int) -> str | None:
    """Return the remote tuned model recorded in the fetched decision artifact."""

    decision_path = build_iteration_local_paths(local_paths, iteration)["decision"]
    if not decision_path.is_file():
        return None
    try:
        payload = yaml.safe_load(decision_path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError):
        return None
    if not isinstance(payload, dict):
        return None
    artifacts = payload.get("tuning_artifacts")
    value = str(artifacts.get("tuned_zerod_config") or "").strip() if isinstance(artifacts, dict) else ""
    return value if value.startswith("/") else None


def _calibration_layout(
    manifest, local_paths, iteration: int, remote_run_dir: str | None = None
) -> dict[str, Any]:
    local_root = local_paths.iterations / iteration_dir_name(iteration) / "calibration"
    remote_run = str(
        remote_run_dir
        or manifest.execution.remote_run_dir
        or manifest.remote.get("remote_run_dir")
        or ""
    ).strip()
    if not remote_run:
        raise ConfigError("manifest is missing remote_run_dir for calibration")
    remote_root = str(PurePosixPath(remote_run) / "iterations" / iteration_dir_name(iteration) / "calibration")
    return {
        "local_root": local_root,
        "local_inputs": local_root / "inputs",
        "local_results": local_root / "results",
        "local_logs": local_root / "logs",
        "local_config": local_root / "inputs" / "calibrate_0d_from_3d.yaml",
        "local_script": local_root / "run_calibration.sh",
        "remote_root": remote_root,
        "remote_inputs": str(PurePosixPath(remote_root) / "inputs"),
        "remote_results": str(PurePosixPath(remote_root) / "results"),
        "remote_logs": str(PurePosixPath(remote_root) / "logs"),
        "remote_config": str(PurePosixPath(remote_root) / "inputs" / "calibrate_0d_from_3d.yaml"),
        "local_request": local_root / "inputs" / "calibration_request.json",
        "remote_request": str(PurePosixPath(remote_root) / "inputs" / "calibration_request.json"),
        "remote_script": str(PurePosixPath(remote_root) / "run_calibration.sh"),
    }


def _render_calibration_script(
    *,
    remote_root: str,
    remote_logs_dir: str,
    remote_config_path: str,
    postprocess_job_id: str | None,
    python_executable: str = "python3",
    activation_hooks: list[str] | None = None,
    account: str | None = None,
    partition: str | None = None,
    wall_time: str | None = None,
    mem: str | None = None,
    cpus: str | None = None,
    dependency_via_submit: bool = False,
) -> str:
    """Render an upstream public calibration invocation.

    For agent-side submission ``afterok`` is deliberately emitted in the script
    itself.  This keeps the dependency visible and portable through the
    existing scheduler adapter, whose narrow interface accepts a script path
    rather than arbitrary sbatch flags.  The tune driver renders the script
    before the postprocess job exists, so with ``dependency_via_submit`` the
    driver supplies ``--dependency=afterok:<postprocess-job>`` at submission.
    """

    header = [
        f"#SBATCH --chdir={remote_root}",
        f"#SBATCH --output={remote_logs_dir}/slurm-%j.out",
        f"#SBATCH --error={remote_logs_dir}/calibration_%j.error",
    ]
    if not dependency_via_submit:
        if not str(postprocess_job_id or "").strip():
            raise ConfigError("calibration requires a postprocess scheduler job dependency")
        header.append(f"#SBATCH --dependency=afterok:{postprocess_job_id}")
    options = {
        "account": account,
        "partition": partition,
        "time": wall_time,
        "mem": mem,
        "cpus-per-task": cpus,
    }
    for key, value in options.items():
        if value:
            header.append(f"#SBATCH --{key}={value}")
    hooks = render_env_activation_hooks(activation_hooks)
    quoted_python = shlex.quote(str(python_executable))
    quoted_config = shlex.quote(str(remote_config_path))
    resolved_config = str(
        PurePosixPath(str(remote_config_path)).with_name(RESOLVED_CALIBRATION_CONFIG_FILENAME)
    )
    quoted_resolved = shlex.quote(resolved_config)
    return "\n".join(
        [
            "#!/usr/bin/env bash",
            *header,
            "set -euo pipefail",
            hooks,
            # target_focused QC needs MPA/LPA/RPA roles, which only exist once the
            # tuned model and its outlet mapping are on disk; resolve them here.
            f"{quoted_python} - {quoted_config} {quoted_resolved} <<'PY'",
            _RESOLVE_TARGETS_PY,
            "PY",
            f"{quoted_python} -m svzerodtrees.cli calibrate-0d-from-3d {quoted_resolved}",
            "",
        ]
    )


render_calibration_script = _render_calibration_script

RESOLVED_CALIBRATION_CONFIG_FILENAME = "calibrate_0d_from_3d.resolved.yaml"

# Runs on the cluster inside the calibration job.  Leaves the staged request
# untouched and writes the config actually passed to svZeroDTrees beside it.
_RESOLVE_TARGETS_PY = """import json
import sys
from pathlib import Path

import yaml

config_path, resolved_path = Path(sys.argv[1]), Path(sys.argv[2])
config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
calibration = config.setdefault("calibration", {})
enforcement = str((calibration.get("observation_qc") or {}).get("enforcement", "")).lower()
if enforcement == "target_focused" and not calibration.get("targets"):
    from svzerodtrees.calibration.target_roles import full_pa_calibration_targets

    model = Path(config["paths"]["zerod_config"])
    calibration["targets"] = full_pa_calibration_targets(
        model, model.parent / "outlet_cap_mapping.json"
    )
resolved_path.write_text(json.dumps(config, indent=2, sort_keys=True) + "\\n", encoding="utf-8")
print(f"[svzt] calibration config resolved: {resolved_path}")"""


def _build_calibration_config(
    *,
    remote_tuned_model: str,
    remote_descriptor: str,
    remote_output_config: str,
    remote_root: str,
    target_policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the upstream calibrate_0d_from_3d request (R-only, target_focused QC)."""

    payload: dict[str, Any] = {
        "version": 1,
        "workflow": "calibrate_0d_from_3d",
        "paths": {
            "root": remote_root,
            "zerod_config": remote_tuned_model,
            "output_config": remote_output_config,
        },
        "calibration": {
            "data_source": {
                "mode": "postprocess_suite",
                "postprocess_metadata_json": remote_descriptor,
            },
            # Resistance only: C (proximal compliance) and L stay as tuned.
            "parameters": {
                "vessels": {"default": ["R_poiseuille"], "overrides": {}},
                "junctions": {"default": [], "overrides": {}},
            },
            # svZeroDTrees' production profile: root waveform is fatal, the
            # whole-network checks are advisory, and the calibrated model must
            # meet the MPA pressure / RPA split targets the job resolves.
            "observation_qc": {"enforcement": "target_focused"},
        },
    }
    if target_policy:
        payload["calibration"].update(target_policy)
    return payload


def _ensure_preop_full_pa(manifest, iteration: int) -> None:
    model = str((manifest.remote.get("impedance_defaults") or {}).get("tuning_model", "rri"))
    if model.strip().lower() != "full_pa":
        raise ConfigError(
            f"calibration is only supported for preoperative full_pa runs; manifest tuning_model is {model!r}"
        )
    if iteration <= 0:
        raise ConfigError("calibration iteration must be positive")


def _configured_calibration_enabled(config, patient_alias: str) -> bool:
    patient = next((item for item in config.patients if item.alias == patient_alias), None)
    if patient is None:
        return False
    policy = patient.tuning.calibration if patient.tuning and patient.tuning.calibration else None
    if policy is not None:
        return bool(policy.enabled)
    return bool(config.defaults.tuning.calibration.enabled)


def _postprocess_job_id(manifest, iteration: int) -> str | None:
    record = _postprocess_record_for_iteration(manifest, iteration)
    if record is None:
        return None
    value = str(record.scheduler_job_id or "").strip()
    return value or None


def _input_identity(
    *,
    tuned_local: Path,
    tuned_remote: str,
    descriptor_local: Path,
    descriptor_remote: str,
    postprocess_job_id: str,
) -> tuple[dict[str, str], dict[str, str], str]:
    tuned_digest = sha256_file(tuned_local)
    descriptor_digest = sha256_file(descriptor_local)
    # A dry-run can be rendered before artifacts are fetched.  Missing values
    # remain explicit identities and therefore cannot satisfy promotion later.
    digests = {
        "tuned_model": tuned_digest or f"missing:{tuned_remote}",
        "postprocess_descriptor": descriptor_digest or f"missing:{descriptor_remote}",
        "postprocess_job": postprocess_job_id,
    }
    artifacts = {
        "tuned_model": tuned_remote,
        "postprocess_descriptor": descriptor_remote,
    }
    return artifacts, digests, _identity_digest(digests)


def _write_yaml(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=True), encoding="utf-8")


def _build_calibration_scheduler(*, cluster, config, remote_exec, run_id: str) -> SchedulerAdapter:
    return SlurmSchedulerAdapter(
        remote_exec=remote_exec,
        runs_root=cluster.remote_roots.runs_root,
        submit_options=SlurmSubmitOptions(
            job_name=f"{run_id}-calibration",
            account=config.defaults.scheduler.account,
            partition=config.defaults.scheduler.partition,
            wall_time=config.defaults.scheduler.wall_time,
            mem=config.defaults.scheduler.mem,
            cpus=config.defaults.scheduler.cpus,
        ),
    )


def run_calibration(
    workspace_root: str | Path,
    run_id: str,
    *,
    iteration: int | None = None,
    mode: ExecutionMode = ExecutionMode.DRY_RUN,
    transfer_adapter: FileTransferAdapter | None = None,
    scheduler_adapter: SchedulerAdapter | None = None,
    remote_exec_adapter: RemoteExecAdapter | None = None,
    tuned_model_path: str | Path | None = None,
    suite_descriptor_path: str | Path | None = None,
    ensure_postprocess: Callable[..., Any] | None = None,
) -> CalibrationExecutionResult:
    """Stage and submit one preoperative calibration job.

    A postprocess job record is mandatory.  Callers that own scheduling may
    supply ``ensure_postprocess`` to create it first; without that callback a
    missing prerequisite fails closed instead of guessing paths or submitting
    an un-dependent calibration job.
    """

    validated_run_id = validate_run_id(run_id)
    root = detect_workspace_root(workspace_root)
    local_paths = build_local_run_paths(root, validated_run_id)
    manifest = read_manifest(local_paths.manifest)
    config = load_workspace_config(root)
    cluster = resolve_cluster(config, str(manifest.cluster.get("name")))
    resolved_iteration = int(iteration or manifest.tuning_iteration_tracker.current_iteration)
    _ensure_preop_full_pa(manifest, resolved_iteration)
    if not _configured_calibration_enabled(config, str(manifest.patient.get("alias"))):
        raise ConfigError("preoperative full_pa calibration is disabled by configuration")

    postprocess_record = _postprocess_record_for_iteration(manifest, resolved_iteration)
    if postprocess_record is None and ensure_postprocess is not None:
        ensure_postprocess(
            workspace_root=root,
            run_id=validated_run_id,
            iteration=resolved_iteration,
            transfer_adapter=transfer_adapter,
            scheduler_adapter=scheduler_adapter,
            remote_exec_adapter=remote_exec_adapter,
        )
        manifest = read_manifest(local_paths.manifest)
        postprocess_record = _postprocess_record_for_iteration(manifest, resolved_iteration)
    if postprocess_record is None:
        raise ConfigError(
            f"preoperative postprocess must be scheduled before calibration for iteration {resolved_iteration}"
        )
    postprocess_job_id = str(postprocess_record.scheduler_job_id or "").strip()
    if not postprocess_job_id:
        raise ConfigError("preoperative postprocess record has no scheduler job ID")

    iteration_record = next(
        (item for item in manifest.tuning_iteration_tracker.iterations if item.iteration == resolved_iteration),
        None,
    )
    if iteration_record is None:
        raise ConfigError(f"missing tuning iteration record for iter-{resolved_iteration:02d}")

    layout = _calibration_layout(manifest, local_paths, resolved_iteration)
    tuned_local, tuned_remote = _resolve_tuned_artifact(
        local_paths=local_paths,
        iteration=resolved_iteration,
        iteration_record=iteration_record,
        tuned_model_path=tuned_model_path,
    )
    descriptor_local, descriptor_remote = _resolve_postprocess_artifacts(
        manifest=manifest,
        iteration=resolved_iteration,
        postprocess_record=postprocess_record,
        suite_descriptor_path=suite_descriptor_path,
    )
    input_artifacts, input_digests, identity = _input_identity(
        tuned_local=tuned_local,
        tuned_remote=tuned_remote,
        descriptor_local=descriptor_local,
        descriptor_remote=descriptor_remote,
        postprocess_job_id=postprocess_job_id,
    )

    existing = _record_for_iteration(manifest, resolved_iteration)
    if existing is not None and existing.input_digests == input_digests:
        if existing.status in {"submitted", "running", "completed", "succeeded"}:
            promoted = existing.promotion_status == "promoted"
            return CalibrationExecutionResult(
                run_id=validated_run_id,
                iteration=resolved_iteration,
                mode=mode,
                status=existing.status,
                reused=True,
                promoted=promoted,
                local_config_path=Path(existing.local_config_path or layout["local_config"]),
                local_job_script_path=Path(existing.local_job_script_path or layout["local_script"]),
                remote_config_path=existing.remote_config_path or layout["remote_config"],
                remote_job_script_path=existing.remote_job_script_path or layout["remote_script"],
                postprocess_job_id=postprocess_job_id,
                submitted_job_id=existing.scheduler_job_id,
                command_previews=[],
            )

    tuned_remote = tuned_remote
    descriptor_remote = descriptor_remote
    remote_output_config = str(PurePosixPath(layout["remote_results"]) / "calibrated_full_pa_zerod.json")
    calibration_yaml = _build_calibration_config(
        remote_tuned_model=tuned_remote,
        remote_descriptor=descriptor_remote,
        remote_output_config=remote_output_config,
        remote_root=layout["remote_root"],
    )
    _write_yaml(layout["local_config"], calibration_yaml)

    if transfer_adapter is None or scheduler_adapter is None or remote_exec_adapter is None:
        default_transfer, default_scheduler, default_remote = _build_default_adapters(
            cluster=cluster,
            config=config,
            run_id=f"{validated_run_id}-calibration",
            mode=mode,
        )
        transfer_adapter = transfer_adapter or default_transfer
        remote_exec_adapter = remote_exec_adapter or default_remote
        scheduler_adapter = scheduler_adapter or _build_calibration_scheduler(
            cluster=cluster,
            config=config,
            remote_exec=remote_exec_adapter,
            run_id=validated_run_id,
        )

    validate_remote_write_path(layout["remote_root"], cluster.remote_roots.runs_root)
    validate_remote_write_path(layout["remote_config"], cluster.remote_roots.runs_root)
    validate_remote_write_path(layout["remote_script"], cluster.remote_roots.runs_root)
    script = _render_calibration_script(
        remote_root=layout["remote_root"],
        remote_logs_dir=layout["remote_logs"],
        remote_config_path=layout["remote_config"],
        postprocess_job_id=postprocess_job_id,
        python_executable=config.defaults.execution.python_executable or "python3",
        activation_hooks=config.defaults.execution.env_activation_hooks,
        account=config.defaults.scheduler.account,
        partition=config.defaults.scheduler.partition,
        wall_time=config.defaults.scheduler.wall_time,
        mem=config.defaults.scheduler.mem,
        cpus=config.defaults.scheduler.cpus,
    )
    layout["local_script"].parent.mkdir(parents=True, exist_ok=True)
    layout["local_script"].write_text(script, encoding="utf-8")

    command_previews = [
        transfer_adapter.ensure_remote_dir(layout["remote_root"]),
        transfer_adapter.ensure_remote_dir(layout["remote_inputs"]),
        transfer_adapter.ensure_remote_dir(layout["remote_results"]),
        transfer_adapter.ensure_remote_dir(layout["remote_logs"]),
        transfer_adapter.push(str(layout["local_config"]), layout["remote_config"]),
        transfer_adapter.push(str(layout["local_script"]), layout["remote_script"]),
    ]
    if mode == ExecutionMode.DRY_RUN:
        submit_result = scheduler_adapter.submit(layout["remote_script"])
    else:
        submit_result = scheduler_adapter.submit(layout["remote_script"])
    command_previews.append(submit_result.command)

    manifest = read_manifest(local_paths.manifest)
    manifest = record_calibration_submission(
        manifest,
        iteration=resolved_iteration,
        calibration_id=f"calibration-{validated_run_id}-iter-{resolved_iteration:02d}-{identity[:12]}",
        local_dir=str(layout["local_root"]),
        remote_dir=layout["remote_root"],
        local_job_script_path=str(layout["local_script"]),
        remote_job_script_path=layout["remote_script"],
        local_config_path=str(layout["local_config"]),
        remote_config_path=layout["remote_config"],
        scheduler_job_id=submit_result.job_id,
        postprocess_job_id=postprocess_job_id,
        input_artifacts=input_artifacts,
        input_digests=input_digests,
        lineage={
            "tuned_model": input_digests["tuned_model"],
            "postprocess_descriptor": input_digests["postprocess_descriptor"],
            "postprocess_job_id": postprocess_job_id,
        },
        note=("Calibration submission preview generated" if mode == ExecutionMode.DRY_RUN else "Calibration submitted"),
    )
    write_manifest(manifest, local_paths.manifest)
    return CalibrationExecutionResult(
        run_id=validated_run_id,
        iteration=resolved_iteration,
        mode=mode,
        status="submitted",
        reused=False,
        promoted=False,
        local_config_path=layout["local_config"],
        local_job_script_path=layout["local_script"],
        remote_config_path=layout["remote_config"],
        remote_job_script_path=layout["remote_script"],
        postprocess_job_id=postprocess_job_id,
        submitted_job_id=submit_result.job_id,
        command_previews=[item.argv for item in command_previews],
    )


schedule_calibration = run_calibration
plan_calibration = run_calibration


def _promotion_candidate_path(record: CalibrationRunRecord) -> Path | None:
    for key in ("calibrated_model", "calibrated_config", "output_config"):
        value = str(record.output_artifacts.get(key) or "").strip()
        if value:
            return Path(value).expanduser()
    return None


def promote_calibrated_full_pa_seed(
    manifest,
    *,
    iteration: int,
    calibration_input_digest: str | None = None,
    candidate_path: str | Path | None = None,
    validated_lineage: bool | None = None,
    at: str | None = None,
):
    """Compare-and-set promotion of one terminal, validated calibration.

    Rejected candidates append a promotion record but never replace the
    successful promotion pointer or the previous iteration's seed.
    """

    record = _record_for_iteration(manifest, iteration)
    if record is None:
        raise ConfigError(f"missing calibration record for iter-{iteration:02d}")
    timestamp = at or _utc_now_iso()
    requested_digest = calibration_input_digest or _identity_digest(record.input_digests)
    expected_digest = _identity_digest(record.input_digests)
    candidate = Path(candidate_path).expanduser() if candidate_path else _promotion_candidate_path(record)
    terminal = str(record.terminal_state or "").strip().lower()
    success = str(record.status or "").strip().lower() in {"completed", "succeeded"}
    lineage_ok = record.validated_lineage if validated_lineage is None else bool(validated_lineage)
    candidate_digest = sha256_file(candidate) if candidate is not None else None

    reason: str | None = None
    if requested_digest != expected_digest:
        reason = "calibration input digest does not match manifest record"
    elif terminal not in SUCCESS_TERMINAL_STATES or not success:
        reason = "calibration is not in a successful terminal state"
    elif not lineage_ok:
        reason = "upstream calibration lineage has not been validated"
    elif candidate is None or candidate_digest is None:
        reason = "validated calibrated model artifact is missing"
    if reason is not None:
        return record_promotion_decision(
            manifest,
            iteration=iteration,
            status="rejected",
            decision="not_promoted",
            calibration_input_digest=requested_digest,
            source_calibration_id=record.calibration_id,
            tuned_model_digest=record.input_digests.get("tuned_model"),
            descriptor_digest=record.input_digests.get("postprocess_descriptor"),
            candidate_path=str(candidate) if candidate else None,
            reason=reason,
            validated_lineage=lineage_ok,
            terminal_state=record.terminal_state,
            at=timestamp,
        )

    # A stable copy under the current iteration makes the promoted seed
    # independent of temporary output locations while retaining source data.
    destination = Path(record.local_dir) / "results" / "calibrated_full_pa_zerod.promoted.json"
    destination.parent.mkdir(parents=True, exist_ok=True)
    if candidate.resolve() != destination.resolve():
        shutil.copyfile(candidate, destination)
    updated = record_promotion_decision(
        manifest,
        iteration=iteration,
        status="promoted",
        decision="promoted",
        calibration_input_digest=requested_digest,
        source_calibration_id=record.calibration_id,
        tuned_model_digest=record.input_digests.get("tuned_model"),
        descriptor_digest=record.input_digests.get("postprocess_descriptor"),
        calibrated_model_digest=candidate_digest,
        candidate_path=str(candidate),
        previous_seed_path=None,
        promoted_seed_path=str(destination),
        reason="terminal upstream calibration passed lineage and artifact gates",
        validated_lineage=True,
        terminal_state=record.terminal_state,
        at=timestamp,
    )
    iteration_record = next(
        (item for item in updated.tuning_iteration_tracker.iterations if item.iteration == iteration),
        None,
    )
    if iteration_record is None:
        raise ConfigError(f"missing tuning iteration record for iter-{iteration:02d}")
    iteration_record.calibrated_seed_path = str(destination)
    iteration_record.updated_at = timestamp
    updated.calibration_promotion = updated.last_known_good_promotion
    return updated


def complete_calibration(
    manifest,
    *,
    iteration: int,
    status: str,
    terminal_state: str | None = None,
    output_artifacts: dict[str, str] | None = None,
    output_digests: dict[str, str] | None = None,
    validated_lineage: bool = False,
    reason: str | None = None,
    at: str | None = None,
):
    """Record upstream terminal evidence and attempt guarded promotion."""

    updated = record_calibration_result(
        manifest,
        iteration=iteration,
        status=status,
        terminal_state=terminal_state or status,
        output_artifacts=output_artifacts,
        output_digests=output_digests,
        validated_lineage=validated_lineage,
        reason=reason,
        at=at,
    )
    if str(status).strip().lower() in {"completed", "succeeded"}:
        return promote_calibrated_full_pa_seed(
            updated,
            iteration=iteration,
            validated_lineage=validated_lineage,
            at=at,
        )
    return promote_calibrated_full_pa_seed(
        updated,
        iteration=iteration,
        validated_lineage=validated_lineage,
        at=at,
    )


finalize_calibration = complete_calibration


def finalize_calibration_if_ready(
    workspace_root: str | Path,
    run_id: str,
    *,
    iteration: int,
    transfer_adapter: FileTransferAdapter,
    scheduler_adapter: SchedulerAdapter,
) -> CalibrationStatusResult:
    """Fetch and promote a successful calibration once its scheduler job ends.

    The upstream command publishes the calibrated JSON only after it has
    validated descriptor lineage and its scientific quality gates.  The agent
    therefore treats a successful terminal job plus the published model and
    summary as the narrow handoff required for promotion.
    """

    root = detect_workspace_root(workspace_root)
    validated_run_id = validate_run_id(run_id)
    local_paths = build_local_run_paths(root, validated_run_id)
    manifest = read_manifest(local_paths.manifest)
    record = _record_for_iteration(manifest, iteration)
    if record is None or not record.scheduler_job_id:
        raise ConfigError(
            f"run '{validated_run_id}' has no submitted calibration for iter-{iteration:02d}"
        )

    observation = poll_scheduler_state(scheduler_adapter, record.scheduler_job_id)
    state = observation.normalized_state.value
    if state not in {"completed", "failed", "cancelled"}:
        manifest = record_calibration_state(
            manifest,
            iteration=iteration,
            status=state,
        )
        write_manifest(manifest, local_paths.manifest)
        return query_calibration_status(
            root,
            validated_run_id,
            iteration=iteration,
        )

    if state != "completed":
        manifest = complete_calibration(
            manifest,
            iteration=iteration,
            status="failed",
            terminal_state=state,
            validated_lineage=False,
            reason=f"calibration scheduler terminal state: {state}",
        )
        write_manifest(manifest, local_paths.manifest)
        return query_calibration_status(root, validated_run_id, iteration=iteration)

    layout = _calibration_layout(manifest, local_paths, iteration)
    postprocess_record = _postprocess_record_for_iteration(manifest, iteration)
    if postprocess_record is None:
        raise ConfigError(
            f"run '{validated_run_id}' is missing the calibration postprocess record for iter-{iteration:02d}"
        )
    local_results = layout["local_results"]
    local_results.mkdir(parents=True, exist_ok=True)
    transfer_adapter.sync(
        local_dir=str(local_results),
        remote_dir=layout["remote_results"],
        include=["calibrated_full_pa_zerod.json", "calibration_summary.json"],
        exclude=["*"],
        direction=SyncDirection.PULL,
    )
    descriptor_local, _ = _resolve_postprocess_artifacts(
        manifest=manifest,
        iteration=iteration,
        postprocess_record=postprocess_record,
        suite_descriptor_path=None,
    )
    descriptor_local.parent.mkdir(parents=True, exist_ok=True)
    transfer_adapter.sync(
        local_dir=str(descriptor_local.parent),
        remote_dir=str(postprocess_record.remote_dir),
        include=["postprocess_suite_metadata.json"],
        exclude=["*"],
        direction=SyncDirection.PULL,
    )

    candidate = local_results / "calibrated_full_pa_zerod.json"
    summary = local_results / "calibration_summary.json"
    summary_payload: dict[str, Any] | None = None
    if summary.is_file():
        try:
            parsed = json.loads(summary.read_text(encoding="utf-8"))
            summary_payload = parsed if isinstance(parsed, dict) else None
        except (OSError, json.JSONDecodeError):
            summary_payload = None
    valid_handoff = (
        candidate.is_file()
        and descriptor_local.is_file()
        and summary_payload is not None
        and summary_payload.get("status") == "ok"
    )
    if not valid_handoff:
        missing = [
            str(path)
            for path in (candidate, descriptor_local, summary)
            if not path.is_file()
        ]
        reason = (
            "calibration completed without a valid upstream publication handoff"
            + (f": missing {', '.join(missing)}" if missing else "")
        )
        manifest = complete_calibration(
            manifest,
            iteration=iteration,
            status="failed",
            terminal_state="completed",
            validated_lineage=False,
            reason=reason,
        )
    else:
        output_artifacts = {
            "calibrated_model": str(candidate),
            "calibration_summary": str(summary),
            "postprocess_descriptor": str(descriptor_local),
        }
        output_digests = {
            name: digest
            for name, path in output_artifacts.items()
            if (digest := sha256_file(path)) is not None
        }
        manifest = complete_calibration(
            manifest,
            iteration=iteration,
            status="completed",
            terminal_state="completed",
            output_artifacts=output_artifacts,
            output_digests=output_digests,
            validated_lineage=True,
            reason="upstream calibration published a validated full-PA model",
        )
    write_manifest(manifest, local_paths.manifest)
    return query_calibration_status(root, validated_run_id, iteration=iteration)


@dataclass(frozen=True)
class IterationCalibrationGate:
    """Outcome of ensuring an iteration's calibrated full-PA model exists.

    ``action`` is ``None`` once the calibrated model is promoted; otherwise it
    names the advance action that must be reported while the caller waits.
    """

    iteration: int
    action: str | None
    scheduler_job_id: str | None

    @property
    def promoted(self) -> bool:
        return self.action is None


def _default_adapters_if_missing(
    root: Path,
    manifest,
    run_id: str,
    *,
    transfer_adapter: FileTransferAdapter | None,
    scheduler_adapter: SchedulerAdapter | None,
    remote_exec_adapter: RemoteExecAdapter | None,
) -> tuple[FileTransferAdapter, SchedulerAdapter, RemoteExecAdapter]:
    if transfer_adapter is not None and scheduler_adapter is not None and remote_exec_adapter is not None:
        return transfer_adapter, scheduler_adapter, remote_exec_adapter
    config = load_workspace_config(root)
    cluster = resolve_cluster(config, str(manifest.cluster.get("name")))
    default_transfer, default_scheduler, default_remote = _build_default_adapters(
        cluster=cluster,
        config=config,
        run_id=run_id,
        mode=ExecutionMode.EXECUTE,
    )
    return (
        transfer_adapter or default_transfer,
        scheduler_adapter or default_scheduler,
        remote_exec_adapter or default_remote,
    )


def _pull_decision_if_missing(manifest, local_paths, iteration: int, transfer_adapter) -> None:
    local_decision = build_iteration_local_paths(local_paths, iteration)["decision"]
    if local_decision.is_file():
        return
    iteration_record = next(
        (item for item in manifest.tuning_iteration_tracker.iterations if item.iteration == iteration),
        None,
    )
    remote_iter_dir = str(getattr(iteration_record, "remote_dir", None) or "").strip()
    if not remote_iter_dir:
        return
    local_decision.parent.mkdir(parents=True, exist_ok=True)
    transfer_adapter.sync(
        local_dir=str(local_decision.parent),
        remote_dir=str(PurePosixPath(remote_iter_dir) / "results"),
        include=[local_decision.name],
        exclude=["*"],
        direction=SyncDirection.PULL,
    )


def ensure_iteration_calibration(
    workspace_root: str | Path,
    run_id: str,
    *,
    iteration: int,
    execute: bool,
    transfer_adapter: FileTransferAdapter | None = None,
    scheduler_adapter: SchedulerAdapter | None = None,
    remote_exec_adapter: RemoteExecAdapter | None = None,
) -> IterationCalibrationGate:
    """Record, finalize, or confirm the calibrated model for one iteration.

    Under ``calibrated_full_pa`` every completed iteration (not-close,
    converged, or final) needs its calibrated full-PA model: either as the next
    iteration's seed or as the converged preop model for postop.  The tune
    driver normally submits the postprocess and calibration jobs itself and
    reports their IDs in ``iteration_decision.json``; this gate records them.
    Agent-side submission is only the fallback for drivers that did not submit
    (older drivers, a failed driver ``sbatch``, or a forced ``continue``).
    Without ``execute`` nothing is submitted, polled, or pulled.
    """

    from svztagent.workflows.seed_generation import record_driver_submitted_calibration

    root = detect_workspace_root(workspace_root)
    validated_run_id = validate_run_id(run_id)
    local_paths = build_local_run_paths(root, validated_run_id)
    manifest = read_manifest(local_paths.manifest)
    record = _record_for_iteration(manifest, iteration)

    if record is None and execute:
        transfer_adapter, scheduler_adapter, remote_exec_adapter = _default_adapters_if_missing(
            root,
            manifest,
            validated_run_id,
            transfer_adapter=transfer_adapter,
            scheduler_adapter=scheduler_adapter,
            remote_exec_adapter=remote_exec_adapter,
        )
        # The driver's report decides between recording and falling back, so
        # it must be present locally before a fallback could duplicate jobs.
        _pull_decision_if_missing(manifest, local_paths, iteration, transfer_adapter)
    if record is None and record_driver_submitted_calibration(root, validated_run_id, iteration):
        manifest = read_manifest(local_paths.manifest)
        record = _record_for_iteration(manifest, iteration)

    if record is None:
        if not execute:
            return IterationCalibrationGate(iteration, "calibration_required", None)
        from svztagent.workflows.postprocess import submit_preop_iteration_postprocess

        submission = run_calibration(
            workspace_root=root,
            run_id=validated_run_id,
            iteration=iteration,
            mode=ExecutionMode.EXECUTE,
            transfer_adapter=transfer_adapter,
            scheduler_adapter=scheduler_adapter,
            remote_exec_adapter=remote_exec_adapter,
            ensure_postprocess=submit_preop_iteration_postprocess,
        )
        return IterationCalibrationGate(
            iteration, "calibration_submitted", submission.submitted_job_id
        )

    if record.promotion_status == "promoted":
        return IterationCalibrationGate(iteration, None, record.scheduler_job_id)

    if not execute:
        action = (
            "calibration_failed"
            if str(record.status).strip().lower() == "failed"
            else "calibration_pending"
        )
        return IterationCalibrationGate(iteration, action, record.scheduler_job_id)

    transfer_adapter, scheduler_adapter, remote_exec_adapter = _default_adapters_if_missing(
        root,
        manifest,
        validated_run_id,
        transfer_adapter=transfer_adapter,
        scheduler_adapter=scheduler_adapter,
        remote_exec_adapter=remote_exec_adapter,
    )
    status = finalize_calibration_if_ready(
        root,
        validated_run_id,
        iteration=iteration,
        transfer_adapter=transfer_adapter,
        scheduler_adapter=scheduler_adapter,
    )
    if status.promotion_status == "promoted":
        return IterationCalibrationGate(iteration, None, status.scheduler_job_id)
    action = "calibration_failed" if status.status == "failed" else "calibration_pending"
    return IterationCalibrationGate(iteration, action, status.scheduler_job_id)


def query_calibration_status(
    workspace_root: str | Path,
    run_id: str,
    *,
    iteration: int | None = None,
    scheduler_adapter: SchedulerAdapter | None = None,
) -> CalibrationStatusResult:
    root = detect_workspace_root(workspace_root)
    validated_run_id = validate_run_id(run_id)
    local_paths = build_local_run_paths(root, validated_run_id)
    manifest = read_manifest(local_paths.manifest)
    resolved_iteration = int(iteration or manifest.tuning_iteration_tracker.current_iteration)
    record = _record_for_iteration(manifest, resolved_iteration)
    if record is None:
        raise ConfigError(f"missing calibration record for iter-{resolved_iteration:02d}")
    if scheduler_adapter is not None and record.scheduler_job_id:
        from svztagent.core.monitor import poll_scheduler_state

        observation = poll_scheduler_state(scheduler_adapter, record.scheduler_job_id)
        normalized = observation.normalized_state.value
        updated = record_calibration_state(
            manifest,
            iteration=resolved_iteration,
            status="completed" if normalized == "completed" else "failed" if normalized in {"failed", "cancelled"} else normalized,
            terminal_state=normalized if normalized in SUCCESS_TERMINAL_STATES | FAILURE_TERMINAL_STATES else None,
        )
        write_manifest(updated, local_paths.manifest)
        record = _record_for_iteration(updated, resolved_iteration) or record
    return CalibrationStatusResult(
        run_id=validated_run_id,
        iteration=resolved_iteration,
        status=record.status,
        terminal_state=record.terminal_state,
        scheduler_job_id=record.scheduler_job_id,
        postprocess_job_id=record.postprocess_job_id,
        dependency_job_id=record.dependency_job_id,
        input_digests=dict(record.input_digests),
        output_digests=dict(record.output_digests),
        promotion_status=record.promotion_status,
        promotion_reason=record.promotion_reason,
    )


status_calibration = query_calibration_status


__all__ = [
    "CalibrationExecutionResult",
    "CalibrationStatusResult",
    "IterationCalibrationGate",
    "complete_calibration",
    "ensure_iteration_calibration",
    "finalize_calibration_if_ready",
    "finalize_calibration",
    "promote_calibrated_full_pa_seed",
    "plan_calibration",
    "query_calibration_status",
    "render_calibration_script",
    "run_calibration",
    "schedule_calibration",
    "sha256_file",
    "status_calibration",
]
