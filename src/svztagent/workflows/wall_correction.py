"""Wall-correction step after the last tuning iteration.

The step reruns the source iteration's deformable-wall 3D simulation with the
same 0D BCs and a corrected wall (the rule in ``svztagent.core.wall_correction``),
postprocesses it, and calibrates the iteration's tuned 0D model to it (R, then
one proximal-compliance multiplier so the 0D reproduces the 3D pulse pressure).
The result is a tuned 3D run and a matching 0D model.

Two modes:

- ``rerun``: build the corrected 3D input from the iteration's own input
  (only the elasticity modulus and damping change) and chain solver ->
  postprocess -> calibration jobs with ``afterok`` dependencies.
- ``adopted``: verify an existing 3D directory (e.g. one produced before this
  step existed) against the rule, the iteration's BCs and the postprocess
  lineage, then submit only the calibration.

Records live in ``manifest.wall_correction_runs`` so the source iteration's
calibration and postprocess records are never replaced.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path, PurePosixPath
from typing import Any

from svztagent.config.load import detect_workspace_root, load_workspace_config, resolve_cluster
from svztagent.core.errors import ConfigError
from svztagent.core.manifest import (
    ConvergedPreopIteration,
    ProtocolDeclaration,
    WallCorrectionRecord,
    _utc_now_iso,
    latest_wall_correction,
    read_manifest,
    record_converged_preop_iteration,
    record_protocol_stage,
    record_wall_correction,
    write_manifest,
)
from svztagent.core.monitor import poll_scheduler_state
from svztagent.core.paths import (
    build_local_run_paths,
    iteration_dir_name,
    validate_remote_write_path,
    validate_run_id,
)
from svztagent.core.wall_correction import (
    WallCorrectionRule,
    compute_wall_correction,
    patch_wall_parameters,
    read_wall_parameters,
    relative_mismatch,
    wall_only_differences,
)
from svztagent.hpc.slurm import SlurmSchedulerAdapter, SlurmSubmitOptions
from svztagent.hpc.interfaces import (
    ExecutionMode,
    FileTransferAdapter,
    RemoteExecAdapter,
    SchedulerAdapter,
    SyncDirection,
)
from svztagent.workflows.calibrate import (
    _build_calibration_config,
    _build_calibration_scheduler,
    _ensure_preop_full_pa,
    _render_calibration_script,
    _write_yaml,
)
from svztagent.workflows.postprocess import (
    TUNED_ZEROD_CONFIG_FILENAME,
    _build_postprocess_scheduler_adapter,
    _load_stage_target_payload,
    _preop_postprocess_resources,
    _render_postprocess_script,
    _require_remote_centerline_path,
    _resolve_resistance_map_camera,
    _resolved_postprocess_worker_count,
    _svslicer_threads,
)
from svztagent.workflows.tune_trees import _build_default_adapters


SOLVER_INPUT = "svFSIplus.xml"
COUPLER = "svzerod_3Dcoupling.json"
INFLOW = "inflow.flow"
SOLVER_SCRIPT = "run_solver.sh"
DESCRIPTOR = "postprocess_suite_metadata.json"
CALIBRATED_MODEL = "calibrated_full_pa_zerod.json"
FINALIZE_COMMAND = "svzt preop finalize"
CALIBRATION_REPORTS = (
    CALIBRATED_MODEL,
    "calibration_summary.json",
    "calibration_targets.json",
    "calibration_compliance_fit.json",
    "calibration_replay.json",
)
# Exact floating-point agreement is expected for an adopted run built with the
# same rule; the tolerance only absorbs text round-tripping of the XML value.
ADOPT_RELATIVE_TOLERANCE = 1.0e-9


@dataclass(frozen=True)
class WallCorrectionResult:
    run_id: str
    correction_id: str
    source_iteration: int
    mode: ExecutionMode
    correction_mode: str
    rule: WallCorrectionRule
    plan_path: Path
    remote_dir: str
    remote_threed_dir: str
    remote_calibrated_zerod_config: str
    solver_job_id: str | None
    postprocess_job_id: str | None
    calibration_job_id: str | None
    command_previews: list[list[str]]


@dataclass(frozen=True)
class WallCorrectionStatus:
    run_id: str
    correction_id: str
    status: str
    job_states: dict[str, str]
    results: dict[str, object]
    # The correction published its matched 0D model, so ``svzt preop finalize``
    # can record it as the pre-op source; ``finalized``: it already is.
    finalizable: bool = False
    finalized: bool = False


@dataclass(frozen=True)
class PreopFinalizeResult:
    run_id: str
    correction_id: str
    # "written": first pre-op source; "replaced": a different source was
    # replaced (``previous``); "unchanged": already recorded, nothing written.
    action: str
    handoff: ConvergedPreopIteration
    previous: ConvergedPreopIteration | None
    protocol: ProtocolDeclaration | None = None
    protocol_changed: bool = False


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _remote_layout(remote_run_dir: str, iteration: int) -> dict[str, str]:
    iteration_dir = PurePosixPath(remote_run_dir) / "iterations" / iteration_dir_name(iteration)
    root = PurePosixPath(remote_run_dir) / "wall_correction" / iteration_dir_name(iteration)
    return {
        "iteration_dir": str(iteration_dir),
        "iteration_preop": str(iteration_dir / "preop"),
        "iteration_results": str(iteration_dir / "results"),
        "iteration1_preop": str(PurePosixPath(remote_run_dir) / "iterations" / iteration_dir_name(1) / "preop"),
        "tuned_model": str(iteration_dir / "results" / TUNED_ZEROD_CONFIG_FILENAME),
        "root": str(root),
        "preop": str(root / "preop"),
        "postprocess": str(root / "postprocess"),
        "postprocess_logs": str(root / "postprocess" / "logs"),
        "postprocess_results": str(root / "results" / "postprocess"),
        "calibration": str(root / "calibration"),
        "calibration_inputs": str(root / "calibration" / "inputs"),
        "calibration_results": str(root / "calibration" / "results"),
        "calibration_logs": str(root / "calibration" / "logs"),
    }


def _fetch(adapter: FileTransferAdapter, remote: str, local_dir: Path) -> Path:
    """Pull one remote file into ``local_dir`` (rsync directory semantics)."""

    local_dir.mkdir(parents=True, exist_ok=True)
    adapter.pull(remote, str(local_dir))
    local = local_dir / PurePosixPath(remote).name
    if not local.is_file():
        raise ConfigError(f"wall correction could not fetch required input {remote}")
    return local


def _target_pulse_pressure(root: Path, patient_alias: str) -> float:
    payload = _load_stage_target_payload(root, patient_alias=patient_alias, stage="preop")
    if payload is None:
        raise ConfigError(f"no preop clinical MPA pressure targets for {patient_alias} in config/clinical_targets.yaml")
    systolic, diastolic, _mean = payload["mpa_pressure"]
    return float(systolic) - float(diastolic)


def _iteration_pulse_pressure(decision_path: Path) -> float:
    metrics = json.loads(decision_path.read_text(encoding="utf-8")).get("metrics") or {}
    try:
        return float(metrics["mpa_sys"]) - float(metrics["mpa_dia"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ConfigError(f"{decision_path} has no 3D MPA systolic/diastolic metrics") from exc


def _insert_dependency(script: str, job_id: str) -> str:
    lines = script.splitlines()
    if not lines or not lines[0].startswith("#!"):
        raise ConfigError("postprocess script has no shebang line")
    return "\n".join([lines[0], f"#SBATCH --dependency=afterok:{job_id}", *lines[1:]]) + "\n"


def _relocate_solver_script(script: str, *, source_dir: str, target_dir: str, job_name: str) -> str:
    if source_dir not in script:
        raise ConfigError(f"{SOLVER_SCRIPT} does not reference the source 3D directory {source_dir}")
    relocated = script.replace(source_dir, target_dir)
    return relocated.replace("--job-name=svFlowSolver", f"--job-name={job_name}")


def _verify_adopted(
    *,
    rule: WallCorrectionRule,
    source_xml: str,
    adopted_xml: str,
    source_coupler: Path,
    adopted_coupler: Path,
    descriptor: Path,
    remote_descriptor: str,
    remote_tuned_model: str,
    tuned_model: Path,
) -> dict[str, object]:
    problems: list[str] = []
    modulus, damping = read_wall_parameters(adopted_xml)
    modulus_mismatch = relative_mismatch(rule.elasticity_modulus_corrected, modulus)
    if modulus_mismatch > ADOPT_RELATIVE_TOLERANCE:
        problems.append(
            f"elasticity modulus {modulus!r} differs from the rule's {rule.elasticity_modulus_corrected!r}"
        )
    if abs(damping - rule.damping_corrected) > 0.05:
        problems.append(f"damping {damping!r} differs from the rule's {rule.damping_corrected!r}")
    other = wall_only_differences(source_xml, adopted_xml)
    if other:
        problems.append("3D input differs from the source iteration beyond the wall: " + "; ".join(other[:5]))
    coupler_match = _sha256(source_coupler) == _sha256(adopted_coupler)
    if not coupler_match:
        problems.append(f"{COUPLER} differs from the source iteration's BCs")
    payload = json.loads(descriptor.read_text(encoding="utf-8"))
    if str(payload.get("status")) != "completed":
        problems.append(f"postprocess descriptor status is {payload.get('status')!r}")
    lineage = payload.get("lineage") or {}
    lineage_path = str(lineage.get("tuned_zerod_config_path") or "")
    resolved = PurePosixPath(remote_descriptor).parent
    for part in PurePosixPath(lineage_path).parts:
        resolved = resolved.parent if part == ".." else resolved / part
    if not lineage_path or str(resolved) != remote_tuned_model:
        problems.append(f"postprocess lineage names {lineage_path!r}, not {remote_tuned_model}")
    if str(lineage.get("tuned_zerod_config_sha256")) != _sha256(tuned_model):
        problems.append("postprocess lineage sha256 does not match the iteration's tuned 0D model")
    if problems:
        raise ConfigError("adopted 3D run failed verification: " + " | ".join(problems))
    return {
        "elasticity_modulus": modulus,
        "elasticity_modulus_relative_mismatch": modulus_mismatch,
        "damping": damping,
        "coupler_identical": coupler_match,
        "wall_only_difference": True,
        "postprocess_lineage": lineage_path,
    }


def run_wall_correction(
    workspace_root: str | Path,
    run_id: str,
    *,
    iteration: int | None = None,
    adopt_threed_dir: str | None = None,
    mode: ExecutionMode = ExecutionMode.DRY_RUN,
    transfer_adapter: FileTransferAdapter | None = None,
    scheduler_adapter: SchedulerAdapter | None = None,
    remote_exec_adapter: RemoteExecAdapter | None = None,
    fetch_adapter: FileTransferAdapter | None = None,
) -> WallCorrectionResult:
    """Plan (dry run) or submit the wall-correction step for one run.

    Inputs are always fetched read-only (``fetch_adapter``, execute mode by
    default) so the dry-run plan shows the real rule; directory creation,
    pushes and submissions follow ``mode``.
    """

    validated_run_id = validate_run_id(run_id)
    root = detect_workspace_root(workspace_root)
    local_paths = build_local_run_paths(root, validated_run_id)
    manifest = read_manifest(local_paths.manifest)
    config = load_workspace_config(root)
    cluster = resolve_cluster(config, str(manifest.cluster.get("name")))
    runs_root = cluster.remote_roots.runs_root
    source_iteration = int(iteration or manifest.tuning_iteration_tracker.current_iteration)
    _ensure_preop_full_pa(manifest, source_iteration)
    if source_iteration < 1:
        raise ConfigError("wall correction requires a completed tuning iteration")
    remote_run_dir = str(manifest.execution.remote_run_dir or manifest.remote.get("remote_run_dir") or "")
    if not remote_run_dir:
        raise ConfigError("manifest is missing remote_run_dir")
    layout = _remote_layout(remote_run_dir, source_iteration)
    policy = config.defaults.tuning.wall_correction

    # An injected scheduler (tests, callers that own scheduling) takes every
    # submission; otherwise each job gets its own submit options below.
    scheduler_injected = scheduler_adapter is not None
    if transfer_adapter is None or scheduler_adapter is None or remote_exec_adapter is None:
        default_transfer, default_scheduler, default_remote = _build_default_adapters(
            cluster=cluster, config=config, run_id=f"{validated_run_id}-wallcorr", mode=mode
        )
        transfer_adapter = transfer_adapter or default_transfer
        scheduler_adapter = scheduler_adapter or default_scheduler
        remote_exec_adapter = remote_exec_adapter or default_remote
    if fetch_adapter is None:
        fetch_adapter = (
            transfer_adapter
            if mode == ExecutionMode.EXECUTE
            else _build_default_adapters(
                cluster=cluster, config=config, run_id=f"{validated_run_id}-wallcorr", mode=ExecutionMode.EXECUTE
            )[0]
        )

    local_root = local_paths.run_dir / "wall_correction" / iteration_dir_name(source_iteration)
    source_local = local_root / "inputs" / iteration_dir_name(source_iteration)
    reference_local = local_root / "inputs" / iteration_dir_name(1)
    decision = _fetch(fetch_adapter, f"{layout['iteration_results']}/iteration_decision.json", source_local)
    source_xml_path = _fetch(fetch_adapter, f"{layout['iteration_preop']}/{SOLVER_INPUT}", source_local)
    reference_xml_path = _fetch(fetch_adapter, f"{layout['iteration1_preop']}/{SOLVER_INPUT}", reference_local)
    source_coupler = _fetch(fetch_adapter, f"{layout['iteration_preop']}/{COUPLER}", source_local)
    tuned_model = _fetch(fetch_adapter, layout["tuned_model"], source_local)

    source_xml = source_xml_path.read_text(encoding="utf-8")
    e_source, c_source = read_wall_parameters(source_xml)
    e_reference, c_reference = read_wall_parameters(reference_xml_path.read_text(encoding="utf-8"))
    rule = compute_wall_correction(
        pulse_pressure_3d_mmhg=_iteration_pulse_pressure(decision),
        pulse_pressure_target_mmhg=_target_pulse_pressure(root, str(manifest.patient.get("alias"))),
        elasticity_modulus_source=e_source,
        elasticity_modulus_reference=e_reference,
        damping_reference=c_reference,
        damping_source=c_source,
        alpha=policy.alpha,
        lambda_min=policy.lambda_min,
    )

    correction_mode = "adopted" if adopt_threed_dir else "rerun"
    verification: dict[str, object] = {}
    if adopt_threed_dir:
        threed_dir = str(PurePosixPath(adopt_threed_dir))
        validate_remote_write_path(threed_dir, runs_root)
        adopt_root = PurePosixPath(threed_dir).parent
        postprocess_dir = str(adopt_root / "postprocess")
        descriptor_remote = str(adopt_root / "results" / "postprocess" / DESCRIPTOR)
        adopted_local = local_root / "inputs" / "adopted"
        adopted_xml = _fetch(fetch_adapter, f"{threed_dir}/{SOLVER_INPUT}", adopted_local)
        adopted_coupler = _fetch(fetch_adapter, f"{threed_dir}/{COUPLER}", adopted_local)
        descriptor_local = _fetch(fetch_adapter, descriptor_remote, adopted_local)
        verification = _verify_adopted(
            rule=rule,
            source_xml=source_xml,
            adopted_xml=adopted_xml.read_text(encoding="utf-8"),
            source_coupler=source_coupler,
            adopted_coupler=adopted_coupler,
            descriptor=descriptor_local,
            remote_descriptor=descriptor_remote,
            remote_tuned_model=layout["tuned_model"],
            tuned_model=tuned_model,
        )
    else:
        threed_dir = layout["preop"]
        postprocess_dir = layout["postprocess"]
        descriptor_remote = f"{layout['postprocess_results']}/{DESCRIPTOR}"

    digests = {
        "source_threed_input": _sha256(source_xml_path),
        "coupler": _sha256(source_coupler),
        "tuned_model": _sha256(tuned_model),
        "decision": _sha256(decision),
    }
    identity = hashlib.sha256(
        json.dumps(
            {
                "digests": digests,
                "rule": rule.as_dict(),
                "mode": correction_mode,
                "threed": threed_dir,
                "policy": policy.model_dump(mode="json"),
            },
            sort_keys=True,
        ).encode()
    ).hexdigest()
    correction_id = f"wallcorr-{validated_run_id}-{iteration_dir_name(source_iteration)}-{identity[:10]}"
    existing = next((item for item in manifest.wall_correction_runs if item.correction_id == correction_id), None)
    if existing is not None and existing.status not in {"planned", "failed", "calibration_failed"} and mode == ExecutionMode.EXECUTE:
        raise ConfigError(f"wall correction {correction_id} is already {existing.status}; nothing resubmitted")

    for path in (layout["root"], layout["calibration"], threed_dir, postprocess_dir):
        validate_remote_write_path(path, runs_root)

    previews: list[list[str]] = []
    staged = local_root / "staged"
    solver_job_id = postprocess_job_id = None

    if correction_mode == "rerun":
        inflow = _fetch(fetch_adapter, f"{layout['iteration_preop']}/{INFLOW}", source_local)
        solver_script = _fetch(fetch_adapter, f"{layout['iteration_preop']}/{SOLVER_SCRIPT}", source_local)
        staged_preop = staged / "preop"
        staged_preop.mkdir(parents=True, exist_ok=True)
        # Mesh and face paths keep pointing at the source iteration's mesh
        # (read only); BC and inflow files are relative to the 3D directory.
        (staged_preop / SOLVER_INPUT).write_text(
            patch_wall_parameters(
                source_xml,
                elasticity_modulus=rule.elasticity_modulus_corrected,
                damping=rule.damping_corrected,
            ),
            encoding="utf-8",
        )
        (staged_preop / SOLVER_SCRIPT).write_text(
            _relocate_solver_script(
                solver_script.read_text(encoding="utf-8"),
                source_dir=layout["iteration_preop"],
                target_dir=layout["preop"],
                job_name=f"wallcorr-{validated_run_id}"[:64],
            ),
            encoding="utf-8",
        )
        (staged_preop / COUPLER).write_bytes(source_coupler.read_bytes())
        (staged_preop / INFLOW).write_bytes(inflow.read_bytes())
        previews.append(transfer_adapter.ensure_remote_dir(layout["preop"]).argv)
        for name in (SOLVER_INPUT, SOLVER_SCRIPT, COUPLER, INFLOW):
            previews.append(transfer_adapter.push(str(staged_preop / name), f"{layout['preop']}/{name}").argv)
        # run_solver.sh carries its own nodes/tasks/time header; submit it with a
        # job name only so the workspace scheduler defaults cannot override it.
        solver_scheduler = scheduler_adapter if scheduler_injected else SlurmSchedulerAdapter(
            remote_exec=remote_exec_adapter,
            runs_root=runs_root,
            submit_options=SlurmSubmitOptions(job_name=f"wallcorr-{validated_run_id}"[:64]),
        )
        solver_submit = solver_scheduler.submit(f"{layout['preop']}/{SOLVER_SCRIPT}")
        solver_job_id = solver_submit.job_id
        previews.append(solver_submit.command.argv)

        if cluster.executables.svslicer_path is None:
            raise ConfigError("cluster executables.svslicer_path is required for postprocessing")
        patient_alias = str(manifest.patient.get("alias"))
        svzerodtrees_paths = manifest.remote.get("svzerodtrees_paths", {})
        cpus_per_task, mem = _preop_postprocess_resources(config)
        camera_offset_dir, camera_view_up = _resolve_resistance_map_camera(config, patient_alias=patient_alias)
        fallback_csv = svzerodtrees_paths.get("clinical_targets")
        inflow_csv = svzerodtrees_paths.get("inflow")
        postprocess_script = _render_postprocess_script(
            config=config,
            cluster=cluster,
            remote_root=layout["postprocess"],
            remote_logs_dir=layout["postprocess_logs"],
            simulation_dir=layout["preop"],
            output_dir=layout["postprocess_results"],
            centerline=str(
                _require_remote_centerline_path(
                    manifest, workflow_label="wall-correction postprocessing", remote_exec_adapter=remote_exec_adapter
                )
            ),
            svslicer_path=cluster.executables.svslicer_path,
            stage="preop",
            clinical_targets_payload=_load_stage_target_payload(root, patient_alias=patient_alias, stage="preop"),
            fallback_clinical_targets_csv=str(fallback_csv) if fallback_csv else None,
            inflow_csv=str(inflow_csv) if inflow_csv else None,
            resistance_map_workers=_resolved_postprocess_worker_count(config),
            camera_offset_dir=camera_offset_dir,
            camera_view_up=camera_view_up,
            cpus_per_task=cpus_per_task,
            mem=mem,
            svslicer_threads=_svslicer_threads(config),
            tuned_zerod_config_path=layout["tuned_model"],
        )
        staged_postprocess = staged / "postprocess" / "run_postprocess.sh"
        staged_postprocess.parent.mkdir(parents=True, exist_ok=True)
        staged_postprocess.write_text(_insert_dependency(postprocess_script, str(solver_job_id)), encoding="utf-8")
        for path in (layout["postprocess"], layout["postprocess_logs"], layout["postprocess_results"]):
            previews.append(transfer_adapter.ensure_remote_dir(path).argv)
        previews.append(transfer_adapter.push(str(staged_postprocess), f"{layout['postprocess']}/run_postprocess.sh").argv)
        postprocess_scheduler = scheduler_adapter if scheduler_injected else _build_postprocess_scheduler_adapter(
            cluster=cluster,
            config=config,
            remote_exec=remote_exec_adapter,
            run_id=validated_run_id,
            cpus_per_task=cpus_per_task,
            mem=mem,
        )
        postprocess_submit = postprocess_scheduler.submit(f"{layout['postprocess']}/run_postprocess.sh")
        postprocess_job_id = postprocess_submit.job_id
        previews.append(postprocess_submit.command.argv)

    fit = policy.compliance_fit
    calibration_payload = _build_calibration_config(
        remote_tuned_model=layout["tuned_model"],
        remote_descriptor=descriptor_remote,
        remote_output_config=f"{layout['calibration_results']}/{CALIBRATED_MODEL}",
        remote_root=layout["calibration"],
    )
    calibration_payload["calibration"]["parameters"]["proximal_compliance_scale"] = {
        "enabled": True,
        "bounds": [float(fit.bounds[0]), float(fit.bounds[1])],
        "tolerance_mmhg": float(fit.tolerance_mmhg),
        "maximum_iterations": int(fit.maximum_iterations),
    }
    calibration_payload["calibration"].setdefault("solver", {})["rho_infty"] = float(policy.rho_infty)
    staged_calibration = staged / "calibration"
    _write_yaml(staged_calibration / "inputs" / "calibrate_0d_from_3d.yaml", calibration_payload)
    calibration_script = _render_calibration_script(
        remote_root=layout["calibration"],
        remote_logs_dir=layout["calibration_logs"],
        remote_config_path=f"{layout['calibration_inputs']}/calibrate_0d_from_3d.yaml",
        postprocess_job_id=postprocess_job_id,
        python_executable=config.defaults.execution.python_executable or "python3",
        activation_hooks=config.defaults.execution.env_activation_hooks,
        account=config.defaults.scheduler.account,
        partition=config.defaults.scheduler.partition,
        wall_time=config.defaults.scheduler.wall_time,
        mem=config.defaults.scheduler.mem,
        cpus=config.defaults.scheduler.cpus,
        # An adopted run's postprocess has already completed.
        dependency_via_submit=postprocess_job_id is None,
        gate_policy=policy.gate_policy,
    )
    (staged_calibration / "run_calibration.sh").write_text(calibration_script, encoding="utf-8")
    for path in (layout["calibration"], layout["calibration_inputs"], layout["calibration_results"], layout["calibration_logs"]):
        previews.append(transfer_adapter.ensure_remote_dir(path).argv)
    previews.append(
        transfer_adapter.push(
            str(staged_calibration / "inputs" / "calibrate_0d_from_3d.yaml"),
            f"{layout['calibration_inputs']}/calibrate_0d_from_3d.yaml",
        ).argv
    )
    previews.append(
        transfer_adapter.push(str(staged_calibration / "run_calibration.sh"), f"{layout['calibration']}/run_calibration.sh").argv
    )
    calibration_scheduler = scheduler_adapter if scheduler_injected else _build_calibration_scheduler(
        cluster=cluster, config=config, remote_exec=remote_exec_adapter, run_id=validated_run_id
    )
    calibration_submit = calibration_scheduler.submit(f"{layout['calibration']}/run_calibration.sh")
    previews.append(calibration_submit.command.argv)

    now = _utc_now_iso()
    record = WallCorrectionRecord(
        correction_id=correction_id,
        source_iteration=source_iteration,
        mode=correction_mode,
        status="planned" if mode == ExecutionMode.DRY_RUN else "submitted",
        created_at=now,
        updated_at=now,
        rule=rule.as_dict(),
        remote_dir=layout["root"],
        remote_threed_dir=threed_dir,
        remote_postprocess_dir=postprocess_dir,
        remote_descriptor_path=descriptor_remote,
        remote_tuned_zerod_config=layout["tuned_model"],
        remote_calibration_dir=layout["calibration"],
        remote_calibrated_zerod_config=f"{layout['calibration_results']}/{CALIBRATED_MODEL}",
        local_dir=str(local_root),
        solver_job_id=None if mode == ExecutionMode.DRY_RUN else solver_job_id,
        postprocess_job_id=None if mode == ExecutionMode.DRY_RUN else postprocess_job_id,
        calibration_job_id=None if mode == ExecutionMode.DRY_RUN else calibration_submit.job_id,
        calibration_status="not_submitted" if mode == ExecutionMode.DRY_RUN else "submitted",
        input_digests=digests,
        verification=verification,
        notes=[f"{correction_mode} wall correction {'planned (dry run)' if mode == ExecutionMode.DRY_RUN else 'submitted'}"],
    )
    plan_path = local_root / "wall_correction_plan.json"
    plan_path.parent.mkdir(parents=True, exist_ok=True)
    plan_path.write_text(
        json.dumps(
            {
                "correction_id": correction_id,
                "mode": mode.value,
                "correction_mode": correction_mode,
                "source_iteration": source_iteration,
                "rule": rule.as_dict(),
                "verification": verification,
                "remote": {
                    "threed_dir": threed_dir,
                    "descriptor": descriptor_remote,
                    "tuned_model": layout["tuned_model"],
                    "calibrated_model": record.remote_calibrated_zerod_config,
                },
                "input_digests": digests,
                "commands": previews,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    if mode == ExecutionMode.EXECUTE:
        manifest = record_wall_correction(read_manifest(local_paths.manifest), record)
        write_manifest(manifest, local_paths.manifest)
    return WallCorrectionResult(
        run_id=validated_run_id,
        correction_id=correction_id,
        source_iteration=source_iteration,
        mode=mode,
        correction_mode=correction_mode,
        rule=rule,
        plan_path=plan_path,
        remote_dir=layout["root"],
        remote_threed_dir=threed_dir,
        remote_calibrated_zerod_config=record.remote_calibrated_zerod_config,
        solver_job_id=record.solver_job_id,
        postprocess_job_id=record.postprocess_job_id,
        calibration_job_id=record.calibration_job_id,
        command_previews=previews,
    )


def _summarize_calibration(local_results: Path) -> dict[str, object]:
    def load(name: str) -> dict[str, Any]:
        path = local_results / name
        return json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {}

    targets = load("calibration_targets.json")
    fit = load("calibration_compliance_fit.json")
    candidate = targets.get("candidate") or {}
    baseline = (targets.get("baseline") or {}).get("targets") or {}
    return {
        "targets_status": targets.get("status"),
        # A passing report can carry a stale default "reason" field upstream.
        "targets_reason": targets.get("reason") if targets.get("status") != "pass" else None,
        "composite_baseline": baseline.get("composite_score"),
        "composite_candidate": candidate.get("composite_score"),
        "compliance_fit_status": fit.get("status"),
        "compliance_scale": fit.get("scale"),
        "target_pulse_pressure_mmhg": fit.get("target_pulse_pressure_mmhg"),
        "unscaled_pulse_pressure_mmhg": fit.get("unscaled_pulse_pressure_mmhg"),
        "fitted_pulse_pressure_mmhg": fit.get("fitted_pulse_pressure_mmhg"),
        "fit_failure_reason": fit.get("reason"),
        "calibrated_model_fetched": (local_results / CALIBRATED_MODEL).is_file(),
    }


def _wall_correction_handoff(record: WallCorrectionRecord) -> ConvergedPreopIteration:
    """The pre-op source for a completed wall correction.

    Post-op and adaptation read ``converged_preop_iteration``: the
    wall-corrected 3D directory and its BCs, the matched 0D model, the wall
    values to reuse instead of re-deriving E and damping, and the source
    iteration's results (tuned trees and outlet mapping).
    """

    tuning_results = str(PurePosixPath(record.remote_tuned_zerod_config).parent)
    return ConvergedPreopIteration(
        iteration=record.source_iteration,
        source_decision=None,
        selection_kind="wall_correction",
        reason=f"wall-corrected 3D run ({record.mode}) with its matched 0D model",
        selected_at=_utc_now_iso(),
        selected_by_command=FINALIZE_COMMAND,
        metrics=None,
        deltas=None,
        remote_iteration_dir=str(PurePosixPath(tuning_results).parent),
        remote_preop_dir=record.remote_threed_dir,
        remote_tuned_zerod_config=str(record.results["matched_zerod_model"]),
        remote_canonical_coupler=f"{record.remote_threed_dir}/{COUPLER}",
        preop_job_id=record.solver_job_id,
        wall_correction_id=record.correction_id,
        remote_tuning_results_dir=tuning_results,
        threed_overrides={
            "elasticity_modulus": float(record.rule["elasticity_modulus_corrected"]),
            "damping": float(record.rule["damping_corrected"]),
        },
    )


def _record_preop_handoff(manifest, record: WallCorrectionRecord):
    handoff = _wall_correction_handoff(record)
    return record_converged_preop_iteration(
        manifest,
        **handoff.model_dump(exclude={"selected_at"}),
        at=handoff.selected_at,
    )


def query_wall_correction_status(
    workspace_root: str | Path,
    run_id: str,
    *,
    correction_id: str | None = None,
    transfer_adapter: FileTransferAdapter | None = None,
    scheduler_adapter: SchedulerAdapter | None = None,
) -> WallCorrectionStatus:
    """Poll the step's jobs; once calibration ends, fetch its reports and record the outcome."""

    validated_run_id = validate_run_id(run_id)
    root = detect_workspace_root(workspace_root)
    local_paths = build_local_run_paths(root, validated_run_id)
    manifest = read_manifest(local_paths.manifest)
    record = _select_record(manifest, validated_run_id, correction_id)
    if transfer_adapter is None or scheduler_adapter is None:
        config = load_workspace_config(root)
        cluster = resolve_cluster(config, str(manifest.cluster.get("name")))
        default_transfer, default_scheduler, _remote = _build_default_adapters(
            cluster=cluster, config=config, run_id=f"{validated_run_id}-wallcorr", mode=ExecutionMode.EXECUTE
        )
        transfer_adapter = transfer_adapter or default_transfer
        scheduler_adapter = scheduler_adapter or default_scheduler

    states: dict[str, str] = {}
    for label, job_id in (
        ("solver", record.solver_job_id),
        ("postprocess", record.postprocess_job_id),
        ("calibration", record.calibration_job_id),
    ):
        if job_id:
            states[label] = poll_scheduler_state(scheduler_adapter, job_id).normalized_state.value
    calibration_state = states.get("calibration")
    status = record.status
    results = dict(record.results)
    if any(states.get(label) in {"failed", "cancelled"} for label in ("solver", "postprocess")):
        status = "failed"
    elif calibration_state in {"completed", "failed", "cancelled"}:
        local_results = Path(record.local_dir) / "calibration" / "results"
        local_results.mkdir(parents=True, exist_ok=True)
        transfer_adapter.sync(
            local_dir=str(local_results),
            remote_dir=f"{record.remote_calibration_dir}/results",
            include=list(CALIBRATION_REPORTS),
            exclude=["*"],
            direction=SyncDirection.PULL,
        )
        results = _summarize_calibration(local_results)
        published = calibration_state == "completed" and bool(results.get("calibrated_model_fetched"))
        status = "completed" if published else "calibration_failed"
        # The matched 0D model is the published calibrated model.  The tuned
        # model never saw this 3D run, so it is not substituted when
        # calibration publishes nothing; its score is kept for reference.
        if published:
            results.update(
                matched_zerod_model=record.remote_calibrated_zerod_config,
                matched_zerod_composite=results.get("composite_candidate"),
            )
        else:
            results.update(
                matched_zerod_model=None,
                matched_zerod_reason=(
                    results.get("targets_reason")
                    or results.get("fit_failure_reason")
                    or f"calibration did not publish (scheduler state {calibration_state})"
                ),
            )
    elif states:
        status = "running" if "running" in states.values() else "submitted"
    updated = record.model_copy(
        update={
            "status": status,
            "calibration_status": calibration_state or record.calibration_status,
            "terminal_state": calibration_state if calibration_state in {"completed", "failed", "cancelled"} else None,
            "results": results,
        }
    )
    # Only this wall-correction record changes; the pre-op source is written
    # by the explicit ``svzt preop finalize`` (finalize_preop_source).
    manifest = record_wall_correction(manifest, updated)
    write_manifest(manifest, local_paths.manifest)
    return WallCorrectionStatus(
        run_id=validated_run_id,
        correction_id=record.correction_id,
        status=status,
        job_states=states,
        results=results,
        finalizable=status == "completed" and bool(results.get("matched_zerod_model")),
        finalized=_handoff_matches(manifest.converged_preop_iteration, updated),
    )


# Handoff fields that record when and by which command it was written; every
# other field is the pre-op source itself and must match for a no-op finalize.
_HANDOFF_PROVENANCE_FIELDS = frozenset({"selected_at", "selected_by_command"})


def _handoff_content(handoff: ConvergedPreopIteration | None) -> dict[str, object] | None:
    if handoff is None:
        return None
    return {key: value for key, value in handoff.model_dump(mode="json").items() if key not in _HANDOFF_PROVENANCE_FIELDS}


def _handoff_matches(current: ConvergedPreopIteration | None, record: WallCorrectionRecord) -> bool:
    if current is None or current.wall_correction_id != record.correction_id:
        return False
    if record.status != "completed" or not record.results.get("matched_zerod_model"):
        return False
    return _handoff_content(current) == _handoff_content(_wall_correction_handoff(record))


def _select_record(manifest, run_id: str, correction_id: str | None) -> WallCorrectionRecord:
    record = (
        next((item for item in manifest.wall_correction_runs if item.correction_id == correction_id), None)
        if correction_id
        else latest_wall_correction(manifest)
    )
    if record is None:
        target = f"wall-correction record {correction_id!r}" if correction_id else "wall-correction record"
        raise ConfigError(f"run '{run_id}' has no {target}")
    return record


def finalize_preop_source(
    workspace_root: str | Path,
    run_id: str,
    *,
    correction_id: str | None = None,
    protocol: str | None = None,
) -> PreopFinalizeResult:
    """Record a completed wall correction as the run's pre-op source (``svzt preop finalize``).

    Local only (no remote calls).  Idempotent: if ``converged_preop_iteration``
    already names this correction with the same content, it is not rewritten.
    Refuses an unfinished or unpublished correction, and refuses to replace a
    different pre-op source once post-op has been submitted from it.

    ``protocol`` declares the run's protocol (e.g. ``preop_v1.1``); a run that
    declares one has its stage set to ``preop_finalized``.
    """

    validated_run_id = validate_run_id(run_id)
    root = detect_workspace_root(workspace_root)
    local_paths = build_local_run_paths(root, validated_run_id)
    manifest = read_manifest(local_paths.manifest)
    record = _select_record(manifest, validated_run_id, correction_id)
    if record.status != "completed":
        raise ConfigError(
            f"wall correction {record.correction_id} is {record.status}; "
            f"run svzt wall-correction-status {validated_run_id} until it completes"
        )
    if not record.results.get("matched_zerod_model"):
        raise ConfigError(
            f"wall correction {record.correction_id} published no matched 0D model "
            f"({record.results.get('matched_zerod_reason') or 'no reason recorded'}); nothing to finalize"
        )
    current = manifest.converged_preop_iteration
    if _handoff_matches(current, record):
        updated = _finalized_protocol(manifest, protocol)
        if updated is not manifest:
            write_manifest(updated, local_paths.manifest)
        return PreopFinalizeResult(
            run_id=validated_run_id,
            correction_id=record.correction_id,
            action="unchanged",
            handoff=current,
            previous=None,
            protocol=updated.protocol,
            protocol_changed=updated is not manifest,
        )
    if current is not None and manifest.postop_run is not None:
        raise ConfigError(
            f"run '{validated_run_id}' already submitted post-op from its current pre-op source "
            f"({current.selection_kind}, {current.wall_correction_id or iteration_dir_name(current.iteration)}); "
            "start a new run instead of replacing it"
        )
    with_protocol = _finalized_protocol(manifest, protocol)
    updated = _record_preop_handoff(with_protocol, record)
    write_manifest(updated, local_paths.manifest)
    return PreopFinalizeResult(
        run_id=validated_run_id,
        correction_id=record.correction_id,
        action="written" if current is None else "replaced",
        handoff=updated.converged_preop_iteration,
        previous=current,
        protocol=updated.protocol,
        protocol_changed=with_protocol is not manifest,
    )


def _finalized_protocol(manifest, protocol: str | None):
    """The manifest with its protocol at ``preop_finalized``; unchanged (same object) if nothing to do."""

    if protocol is None and manifest.protocol is None:
        return manifest
    return record_protocol_stage(manifest, stage="preop_finalized", name=protocol)
