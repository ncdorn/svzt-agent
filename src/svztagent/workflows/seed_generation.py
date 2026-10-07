"""Driver-owned next-seed generation for tuning iterations.

Seed generation is one pipeline step with a policy-selected strategy, and the
tune driver job runs it in both cases:

- ``reduced_rri``: the driver regenerates ``simplified_zerod_tuned_RRI.json``
  inline after a ``not_close`` decision.
- ``calibrated_full_pa``: after a ``not_close`` or ``converged`` decision the
  driver submits the pre-rendered preop postprocess job and the dependent
  (``afterok``) full-PA calibration job, and reports both job IDs in
  ``iteration_decision.json``.

This module renders the calibrated-full-PA job inputs at ``run tune`` time and,
after the driver finishes, records the job IDs the driver reported in the
manifest.  Polling, fetching, lineage validation, and promotion stay with the
agent in :mod:`svztagent.workflows.calibrate`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import json
from pathlib import Path, PurePosixPath
from typing import Any

import yaml

from svztagent.core.errors import ConfigError
from svztagent.core.manifest import (
    read_manifest,
    record_calibration_submission,
    record_postprocess_submission,
    write_manifest,
)
from svztagent.core.paths import (
    build_iteration_local_paths,
    build_local_run_paths,
    validate_remote_write_path,
)
from svztagent.hpc.interfaces import RemoteExecAdapter
from svztagent.hpc.slurm import SlurmSubmitOptions, build_sbatch_command

REDUCED_RRI_STRATEGY = "reduced_rri"
CALIBRATED_FULL_PA_STRATEGY = "calibrated_full_pa"
DRIVER_SUBMITTED_STATUS = "submitted"


@dataclass(frozen=True)
class PreparedSeedGeneration:
    """Seed-generation spec for the driver plus the files it needs remotely."""

    spec: dict[str, Any]
    remote_dirs: list[str] = field(default_factory=list)
    uploads: list[tuple[Path, str]] = field(default_factory=list)


def reduced_rri_seed_generation() -> PreparedSeedGeneration:
    return PreparedSeedGeneration(spec={"strategy": REDUCED_RRI_STRATEGY})


def prepare_calibrated_full_pa_seed_generation(
    *,
    workspace_root: Path,
    manifest,
    config,
    cluster,
    iteration: int,
    remote_run_dir: str,
    remote_exec_adapter: RemoteExecAdapter,
) -> PreparedSeedGeneration:
    """Render the postprocess and calibration jobs the driver will submit.

    The tuned model path is only known once the driver has tuned, so the
    calibration request is rendered without ``paths.zerod_config``; the driver
    fills it in and writes the final ``calibrate_0d_from_3d.yaml``.
    """

    from svztagent.workflows.calibrate import (
        _build_calibration_config,
        _calibration_layout,
        _render_calibration_script,
    )
    from svztagent.workflows.postprocess import prepare_preop_postprocess_script

    runs_root = cluster.remote_roots.runs_root
    run_id = str(manifest.run_id)
    local_paths = build_local_run_paths(workspace_root, run_id)

    postprocess = prepare_preop_postprocess_script(
        workspace_root=workspace_root,
        manifest=manifest,
        config=config,
        cluster=cluster,
        iteration=iteration,
        remote_run_dir=remote_run_dir,
        remote_exec_adapter=remote_exec_adapter,
    )
    pp_remote = postprocess.remote_layout
    pp_local = postprocess.local_layout

    layout = _calibration_layout(manifest, local_paths, iteration, remote_run_dir=remote_run_dir)
    for key in ("local_inputs", "local_results", "local_logs"):
        layout[key].mkdir(parents=True, exist_ok=True)
    remote_descriptor = str(
        PurePosixPath(pp_remote["remote_results_dir"]) / "postprocess_suite_metadata.json"
    )
    request = _build_calibration_config(
        remote_tuned_model="",
        remote_descriptor=remote_descriptor,
        remote_output_config=str(
            PurePosixPath(layout["remote_results"]) / "calibrated_full_pa_zerod.json"
        ),
        remote_root=layout["remote_root"],
    )
    layout["local_request"].write_text(
        json.dumps(request, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    scheduler = config.defaults.scheduler
    layout["local_script"].write_text(
        _render_calibration_script(
            remote_root=layout["remote_root"],
            remote_logs_dir=layout["remote_logs"],
            remote_config_path=layout["remote_config"],
            postprocess_job_id=None,
            python_executable=config.defaults.execution.python_executable or "python3",
            activation_hooks=config.defaults.execution.env_activation_hooks,
            account=scheduler.account,
            partition=scheduler.partition,
            wall_time=scheduler.wall_time,
            mem=scheduler.mem,
            cpus=scheduler.cpus,
            dependency_via_submit=True,
        ),
        encoding="utf-8",
    )
    calibration_options = SlurmSubmitOptions(
        job_name=f"{run_id}-calibration",
        account=scheduler.account,
        partition=scheduler.partition,
        wall_time=scheduler.wall_time,
        mem=scheduler.mem,
        cpus=scheduler.cpus,
    )

    remote_dirs = [
        pp_remote["remote_root"],
        pp_remote["remote_inputs_dir"],
        pp_remote["remote_logs_dir"],
        pp_remote["remote_results_dir"],
        layout["remote_root"],
        layout["remote_inputs"],
        layout["remote_results"],
        layout["remote_logs"],
    ]
    uploads = [
        (pp_local["job_script"], pp_remote["remote_job_script_path"]),
        (layout["local_request"], layout["remote_request"]),
        (layout["local_script"], layout["remote_script"]),
    ]
    for path in [*remote_dirs, layout["remote_config"], *(remote for _, remote in uploads)]:
        validate_remote_write_path(path, runs_root)

    spec = {
        "strategy": CALIBRATED_FULL_PA_STRATEGY,
        "postprocess": {
            "sbatch_argv": build_sbatch_command(
                pp_remote["remote_job_script_path"], postprocess.submit_options
            ),
            "remote_script": pp_remote["remote_job_script_path"],
            "remote_logs_dir": pp_remote["remote_logs_dir"],
            "remote_results_dir": pp_remote["remote_results_dir"],
        },
        "calibration": {
            "sbatch_argv": build_sbatch_command(layout["remote_script"], calibration_options),
            "remote_root": layout["remote_root"],
            "remote_script": layout["remote_script"],
            "remote_logs_dir": layout["remote_logs"],
            "remote_request_path": layout["remote_request"],
            "remote_config_path": layout["remote_config"],
        },
    }
    return PreparedSeedGeneration(spec=spec, remote_dirs=remote_dirs, uploads=uploads)


def load_driver_seed_generation(local_paths, iteration: int) -> dict[str, Any] | None:
    """Return the ``seed_generation`` block of a fetched iteration decision."""

    decision_path = build_iteration_local_paths(local_paths, iteration)["decision"]
    if not decision_path.is_file():
        return None
    try:
        payload = yaml.safe_load(decision_path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError):
        return None
    if not isinstance(payload, dict):
        return None
    block = payload.get("seed_generation")
    return block if isinstance(block, dict) else None


def record_driver_submitted_calibration(workspace_root: Path, run_id: str, iteration: int) -> bool:
    """Record driver-submitted postprocess/calibration jobs in the manifest.

    Returns ``True`` when the driver reported both jobs and the manifest now
    holds their records, ``False`` when the driver did not submit them (older
    driver, failed submission, or no decision fetched yet).  Idempotent.
    """

    from svztagent.workflows.calibrate import (
        _calibration_layout,
        _input_identity,
        _postprocess_record_for_iteration,
        _record_for_iteration,
        _resolve_postprocess_artifacts,
    )
    from svztagent.workflows.postprocess import _selected_preop_local_paths

    local_paths = build_local_run_paths(workspace_root, run_id)
    manifest = read_manifest(local_paths.manifest)
    if _record_for_iteration(manifest, iteration) is not None:
        return True
    block = load_driver_seed_generation(local_paths, iteration)
    if (
        block is None
        or block.get("strategy") != CALIBRATED_FULL_PA_STRATEGY
        or block.get("status") != DRIVER_SUBMITTED_STATUS
    ):
        return False
    postprocess = block.get("postprocess") or {}
    calibration = block.get("calibration") or {}
    postprocess_job_id = str(postprocess.get("job_id") or "").strip()
    calibration_job_id = str(calibration.get("job_id") or "").strip()
    tuned_remote = str(block.get("tuned_zerod_config") or "").strip()
    if not postprocess_job_id or not calibration_job_id or not tuned_remote:
        raise ConfigError(
            f"iter-{iteration:02d} driver reported calibrated_full_pa seed generation "
            "without postprocess/calibration job IDs and a tuned model path"
        )

    if _postprocess_record_for_iteration(manifest, iteration) is None:
        pp_local = _selected_preop_local_paths(workspace_root, run_id, iteration)
        manifest = record_postprocess_submission(
            manifest,
            field_name="preop_postprocess",
            stage="preop_iteration",
            source_preop_iteration=iteration,
            local_dir=str(pp_local["root"]),
            remote_dir=str(postprocess.get("remote_results_dir") or ""),
            local_job_script_path=str(pp_local["job_script"]),
            remote_job_script_path=str(postprocess.get("remote_script") or ""),
            scheduler_job_id=postprocess_job_id,
            note="Preop postprocess submitted by the tune driver",
        )
    postprocess_record = _postprocess_record_for_iteration(manifest, iteration)
    layout = _calibration_layout(manifest, local_paths, iteration)
    tuned_local = build_iteration_local_paths(local_paths, iteration)["results"] / PurePosixPath(
        tuned_remote
    ).name
    descriptor_local, descriptor_remote = _resolve_postprocess_artifacts(
        manifest=manifest,
        iteration=iteration,
        postprocess_record=postprocess_record,
        suite_descriptor_path=None,
    )
    input_artifacts, input_digests, identity = _input_identity(
        tuned_local=tuned_local,
        tuned_remote=tuned_remote,
        descriptor_local=descriptor_local,
        descriptor_remote=descriptor_remote,
        postprocess_job_id=postprocess_job_id,
    )
    manifest = record_calibration_submission(
        manifest,
        iteration=iteration,
        calibration_id=f"calibration-{run_id}-iter-{iteration:02d}-{identity[:12]}",
        local_dir=str(layout["local_root"]),
        remote_dir=str(calibration.get("remote_root") or layout["remote_root"]),
        local_job_script_path=str(layout["local_script"]),
        remote_job_script_path=str(calibration.get("remote_script") or layout["remote_script"]),
        local_config_path=str(layout["local_request"]),
        remote_config_path=str(calibration.get("remote_config_path") or layout["remote_config"]),
        scheduler_job_id=calibration_job_id,
        postprocess_job_id=postprocess_job_id,
        input_artifacts=input_artifacts,
        input_digests=input_digests,
        lineage={
            "tuned_model": input_digests["tuned_model"],
            "postprocess_descriptor": input_digests["postprocess_descriptor"],
            "postprocess_job_id": postprocess_job_id,
        },
        note="Calibration submitted by the tune driver",
    )
    write_manifest(manifest, local_paths.manifest)
    return True


__all__ = [
    "CALIBRATED_FULL_PA_STRATEGY",
    "PreparedSeedGeneration",
    "REDUCED_RRI_STRATEGY",
    "load_driver_seed_generation",
    "prepare_calibrated_full_pa_seed_generation",
    "record_driver_submitted_calibration",
    "reduced_rri_seed_generation",
]
