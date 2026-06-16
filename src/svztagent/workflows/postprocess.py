"""Remote postprocess orchestration for selected preop and postop stages."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path, PurePosixPath
import json
from typing import Literal

import yaml

from svztagent.config.load import detect_workspace_root, load_workspace_config, resolve_cluster
from svztagent.core.errors import ConfigError
from svztagent.core.manifest import read_manifest, record_postprocess_submission, write_manifest
from svztagent.core.paths import build_iteration_local_paths, build_local_run_paths, iteration_dir_name, validate_run_id
from svztagent.hpc.interfaces import (
    ExecutionMode,
    FileTransferAdapter,
    RemoteExecAdapter,
    SchedulerAdapter,
    SyncDirection,
)
from svztagent.hpc.slurm import SlurmSchedulerAdapter, SlurmSubmitOptions
from svztagent.workflows.paraview_viz import _resolve_viz_config, submit_preop_paraview_viz
from svztagent.workflows.tune_trees import _build_default_adapters


@dataclass(frozen=True)
class PostprocessSubmissionResult:
    run_id: str
    stage: str
    source_preop_iteration: int
    remote_results_dir: str
    remote_job_script_path: str
    local_job_script_path: Path
    submitted_job_id: str
    command_previews: list[list[str]]


def _postprocess_workers_from_config(
    config,
) -> int | Literal["auto"]:
    return config.defaults.postprocess.resistance_map.workers


def _parse_positive_int(value: object) -> int | None:
    if isinstance(value, int):
        return value if value > 0 else None
    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned or cleaned.startswith("<") and cleaned.endswith(">"):
            return None
        try:
            parsed = int(cleaned)
        except ValueError:
            return None
        return parsed if parsed > 0 else None
    return None


def _resolved_postprocess_worker_count(
    config,
    *,
    fallback_cpus: int | None = None,
) -> int:
    workers = _postprocess_workers_from_config(config)
    if workers != "auto":
        return int(workers)
    parsed_fallback = _parse_positive_int(fallback_cpus)
    if parsed_fallback is not None:
        return parsed_fallback
    parsed_scheduler_cpus = _parse_positive_int(config.defaults.scheduler.cpus)
    if parsed_scheduler_cpus is not None:
        return parsed_scheduler_cpus
    return 4


def _build_postprocess_scheduler_adapter(
    *,
    cluster,
    config,
    remote_exec: RemoteExecAdapter,
    run_id: str,
    cpus_per_task: int | None,
    mem: str | None,
) -> SchedulerAdapter:
    cpus = config.defaults.scheduler.cpus
    if cpus_per_task is not None:
        cpus = str(cpus_per_task)
    submit_mem = mem if mem is not None else config.defaults.scheduler.mem
    return SlurmSchedulerAdapter(
        remote_exec=remote_exec,
        runs_root=cluster.remote_roots.runs_root,
        submit_options=SlurmSubmitOptions(
            job_name=run_id,
            account=config.defaults.scheduler.account,
            partition=config.defaults.scheduler.partition,
            wall_time=config.defaults.scheduler.wall_time,
            mem=submit_mem,
            cpus=cpus,
        ),
    )


def _load_stage_target_payload(
    workspace_root: Path,
    *,
    patient_alias: str,
    stage: str,
) -> dict[str, object] | None:
    path = workspace_root / "config" / "clinical_targets.yaml"
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as stream:
        payload = yaml.safe_load(stream) or {}
    if not isinstance(payload, dict):
        return None
    stage_payload = payload.get("clinical_targets", {}).get(stage, {}).get("patients", {})
    patient_payload = stage_payload.get(patient_alias)
    if not isinstance(patient_payload, dict):
        return None
    mpa_pressure = patient_payload.get("mpa_pressure")
    rpa_split = patient_payload.get("rpa_split")
    if not isinstance(mpa_pressure, list) or len(mpa_pressure) < 3 or rpa_split is None:
        return None
    return {
        "mpa_pressure": [float(mpa_pressure[0]), float(mpa_pressure[1]), float(mpa_pressure[2])],
        "rpa_split": float(rpa_split),
    }


def _python_bootstrap(config) -> str:
    hooks = "\n".join(config.defaults.execution.env_activation_hooks)
    python_executable = config.defaults.execution.python_executable or "python3"
    return f"""PYTHON_CANDIDATE="{python_executable}"
{hooks}
if [ -z "${{PYTHON_CANDIDATE}}" ]; then
  PYTHON_CANDIDATE="python3"
fi
if [ "${{PYTHON_CANDIDATE#*/}}" != "${{PYTHON_CANDIDATE}}" ]; then
  PYTHON_BIN="${{PYTHON_CANDIDATE}}"
else
  PYTHON_BIN="$(command -v "${{PYTHON_CANDIDATE}}" || command -v python3 || command -v python)"
fi
if [ -z "${{PYTHON_BIN}}" ]; then
  echo "[svzt] error: no Python interpreter found on PATH" >&2
  exit 1
fi
"""


def _render_postprocess_slurm_header(
    *,
    remote_root: str,
    remote_logs_dir: str,
    cpus_per_task: int | None = None,
    mem: str | None = None,
    account: str | None = None,
    partition: str | None = None,
    wall_time_hours: int | None = None,
    nodes: int | None = None,
    ntasks_per_node: int | None = None,
) -> str:
    header = [
        f"#SBATCH --chdir={remote_root}",
        f"#SBATCH --output={remote_logs_dir}/slurm-%j.out",
        f"#SBATCH --error={remote_logs_dir}/%x_%j.error",
    ]
    if account:
        header.append(f"#SBATCH --account={account}")
    if partition:
        header.append(f"#SBATCH --partition={partition}")
    if wall_time_hours is not None:
        header.append(f"#SBATCH --time={wall_time_hours}:00:00")
    if nodes is not None:
        header.append(f"#SBATCH --nodes={nodes}")
    if ntasks_per_node is not None:
        header.append(f"#SBATCH --ntasks-per-node={ntasks_per_node}")
    if cpus_per_task is not None:
        header.append(f"#SBATCH --cpus-per-task={cpus_per_task}")
    if mem is not None:
        header.append(f"#SBATCH --mem={mem}")
    return "\n".join(header)


def _stacked_centerline_timeseries_python_source() -> str:
    return """
import shutil
import vtk


def _read_polydata(path: Path) -> vtk.vtkPolyData:
    reader = vtk.vtkXMLPolyDataReader()
    reader.SetFileName(str(path))
    reader.Update()
    poly = vtk.vtkPolyData()
    poly.DeepCopy(reader.GetOutput())
    return poly


def _write_polydata(poly: vtk.vtkPolyData, path: Path) -> None:
    writer = vtk.vtkXMLPolyDataWriter()
    writer.SetFileName(str(path))
    writer.SetInputData(poly)
    if writer.Write() != 1:
        raise RuntimeError(f"failed to write polydata: {path}")


def _constant_int_array(name: str, value: int, size: int) -> vtk.vtkIntArray:
    array = vtk.vtkIntArray()
    array.SetName(name)
    array.SetNumberOfValues(size)
    for index in range(size):
        array.SetValue(index, int(value))
    return array


def _constant_double_array(name: str, value: float, size: int) -> vtk.vtkDoubleArray:
    array = vtk.vtkDoubleArray()
    array.SetName(name)
    array.SetNumberOfValues(size)
    for index in range(size):
        array.SetValue(index, float(value))
    return array


def _annotate_timestep_polydata(
    poly: vtk.vtkPolyData,
    *,
    frame_index: int,
    time_s: float,
    timestep_id: int | None,
) -> vtk.vtkPolyData:
    point_count = poly.GetNumberOfPoints()
    cell_count = poly.GetNumberOfCells()
    point_data = poly.GetPointData()
    cell_data = poly.GetCellData()

    point_data.AddArray(_constant_int_array("processed_timestep_index", frame_index, point_count))
    point_data.AddArray(_constant_double_array("processed_timestep_time_s", time_s, point_count))
    cell_data.AddArray(_constant_int_array("processed_timestep_index", frame_index, cell_count))
    cell_data.AddArray(_constant_double_array("processed_timestep_time_s", time_s, cell_count))

    if timestep_id is not None:
        point_data.AddArray(_constant_int_array("processed_timestep_id", timestep_id, point_count))
        cell_data.AddArray(_constant_int_array("processed_timestep_id", timestep_id, cell_count))
    return poly


def _preserve_intermediate_centerlines(intermediate_dir: Path):
    original_rmtree = shutil.rmtree
    protected_dir = intermediate_dir.expanduser().resolve(strict=False)

    def _wrapped_rmtree(path, *args, **kwargs):
        candidate = Path(path).expanduser().resolve(strict=False)
        if candidate == protected_dir:
            return None
        return original_rmtree(path, *args, **kwargs)

    shutil.rmtree = _wrapped_rmtree
    return original_rmtree


def _cleanup_intermediate_centerlines(intermediate_dir: Path, original_rmtree) -> None:
    shutil.rmtree = original_rmtree
    if intermediate_dir.exists():
        original_rmtree(intermediate_dir, ignore_errors=True)


def _is_unexpected_camera_kwarg_error(exc: Exception) -> bool:
    if not isinstance(exc, TypeError):
        return False
    message = str(exc)
    return (
        "unexpected keyword argument" in message
        and ("camera_offset_dir" in message or "camera_view_up" in message)
    )


def _run_postprocess_suite_with_optional_camera(run_postprocess, postprocess_kwargs: dict[str, object]):
    try:
        return run_postprocess(**postprocess_kwargs)
    except Exception as exc:
        if not _is_unexpected_camera_kwarg_error(exc):
            raise
        trimmed_kwargs = dict(postprocess_kwargs)
        trimmed_kwargs.pop("camera_offset_dir", None)
        trimmed_kwargs.pop("camera_view_up", None)
        return run_postprocess(**trimmed_kwargs)


def _write_stacked_centerline_timeseries(
    *,
    output_dir: Path,
    suite_metadata_path: Path,
    result: dict[str, object],
) -> dict[str, object]:
    resistance_result = result.get("resistance_map")
    if not isinstance(resistance_result, dict):
        raise RuntimeError("postprocess result missing resistance_map payload")
    mean_metadata_json = resistance_result.get("metadata_json")
    if not isinstance(mean_metadata_json, str) or not mean_metadata_json.strip():
        raise RuntimeError("postprocess result missing resistance_map metadata_json")

    mean_metadata_path = Path(mean_metadata_json).expanduser().resolve()
    mean_metadata = json.loads(mean_metadata_path.read_text(encoding="utf-8"))
    selected_frames = mean_metadata.get("selected_frames")
    if not isinstance(selected_frames, list) or not selected_frames:
        raise RuntimeError("resistance map metadata missing selected_frames")

    append = vtk.vtkAppendPolyData()
    processed_frames: list[dict[str, object]] = []
    for frame_index, frame in enumerate(selected_frames):
        if not isinstance(frame, dict):
            raise RuntimeError("selected_frames entries must be objects")
        raw_path = frame.get("path")
        raw_time = frame.get("time_s")
        if not isinstance(raw_path, str) or not raw_path.strip():
            raise RuntimeError("selected frame missing mapped centerline path")
        if raw_time is None:
            raise RuntimeError("selected frame missing time_s")
        mapped_path = Path(raw_path).expanduser().resolve()
        if not mapped_path.exists():
            raise FileNotFoundError(f"mapped centerline missing for stacked timeseries: {mapped_path}")
        timestep_id_raw = frame.get("timestep_id")
        timestep_id = int(timestep_id_raw) if timestep_id_raw is not None else None
        poly = _annotate_timestep_polydata(
            _read_polydata(mapped_path),
            frame_index=frame_index,
            time_s=float(raw_time),
            timestep_id=timestep_id,
        )
        append.AddInputData(poly)
        processed_frames.append(
            {
                "frame_index": frame_index,
                "mapped_path": str(mapped_path),
                "time_s": float(raw_time),
                "timestep_id": timestep_id,
                "source_frame_path": frame.get("source_frame_path"),
            }
        )

    append.Update()
    stacked_poly = vtk.vtkPolyData()
    stacked_poly.DeepCopy(append.GetOutput())

    output_path = output_dir / "centerline_timeseries_last_cycle.vtp"
    metadata_output_path = output_dir / "centerline_timeseries_last_cycle_metadata.json"
    _write_polydata(stacked_poly, output_path)

    stack_result = {
        "kind": "centerline_timeseries_last_cycle",
        "output_path": str(output_path),
        "metadata_json": str(metadata_output_path),
        "source_resistance_map_metadata_json": str(mean_metadata_path),
        "selected_frame_count": len(processed_frames),
        "point_count": int(stacked_poly.GetNumberOfPoints()),
        "cell_count": int(stacked_poly.GetNumberOfCells()),
        "processed_frames": processed_frames,
    }
    metadata_output_path.write_text(json.dumps(stack_result, indent=2, sort_keys=True), encoding="utf-8")

    suite_metadata = {}
    if suite_metadata_path.exists():
        suite_metadata = json.loads(suite_metadata_path.read_text(encoding="utf-8"))
    outputs = suite_metadata.get("outputs")
    if not isinstance(outputs, dict):
        outputs = {}
    outputs["centerline_timeseries_last_cycle_vtp"] = str(output_path)
    outputs["centerline_timeseries_last_cycle_metadata_json"] = str(metadata_output_path)
    suite_metadata["outputs"] = outputs
    suite_metadata["centerline_timeseries_last_cycle"] = stack_result
    suite_metadata_path.write_text(json.dumps(suite_metadata, indent=2, sort_keys=True), encoding="utf-8")

    result["centerline_timeseries_last_cycle"] = stack_result
    return stack_result
"""


def _render_postprocess_script(
    *,
    config,
    cluster,
    remote_root: str,
    remote_logs_dir: str,
    simulation_dir: str,
    output_dir: str,
    centerline: str,
    svslicer_path: str,
    stage: str,
    clinical_targets_payload: dict[str, object] | None,
    fallback_clinical_targets_csv: str | None = None,
    inflow_csv: str | None = None,
    resistance_map_workers: int | Literal["auto"] | None = None,
    camera_offset_dir: list[float] | None = None,
    camera_view_up: list[float] | None = None,
    cpus_per_task: int | None = None,
    mem: str | None = None,
    account: str | None = None,
    partition: str | None = None,
    wall_time_hours: int | None = None,
) -> str:
    clinical_targets_expr = (
        json.dumps(clinical_targets_payload, sort_keys=True)
        if clinical_targets_payload is not None
        else ("None" if fallback_clinical_targets_csv is None else json.dumps(fallback_clinical_targets_csv))
    )
    inflow_expr = "None" if inflow_csv is None else json.dumps(inflow_csv)
    camera_offset_expr = "None" if camera_offset_dir is None else repr(list(camera_offset_dir))
    camera_view_up_expr = "None" if camera_view_up is None else repr(list(camera_view_up))
    slurm_header = _render_postprocess_slurm_header(
        remote_root=remote_root,
        remote_logs_dir=remote_logs_dir,
        cpus_per_task=cpus_per_task,
        mem=mem,
        account=account,
        partition=partition,
        wall_time_hours=wall_time_hours,
    )
    return f"""#!/usr/bin/env bash
{slurm_header}
set -euo pipefail

{_python_bootstrap(config)}

mkdir -p {json.dumps(output_dir)}

"${{PYTHON_BIN}}" - <<'PY'
from pathlib import Path
import json

from svzerodtrees.post_processing import run_pulmonary_threed_postprocess_suite

{_stacked_centerline_timeseries_python_source()}

output_dir = Path({json.dumps(output_dir)})
submission_path = Path({json.dumps(str(PurePosixPath(output_dir) / "postprocess_submission.json"))})
    metadata_path = Path({json.dumps(str(PurePosixPath(output_dir) / "postprocess_suite_metadata.json"))})
try:
    postprocess_kwargs = {{
        "simulation_dir": {json.dumps(simulation_dir)},
        "output_dir": str(output_dir),
        "centerline": {json.dumps(centerline)},
        "stage": {json.dumps(stage)},
        "svslicer_path": {json.dumps(svslicer_path)},
        "clinical_targets": {clinical_targets_expr},
        "inflow_csv": {inflow_expr},
        "resistance_map_workers": {json.dumps(resistance_map_workers)},
    }}
    if {camera_offset_expr} is not None:
        postprocess_kwargs["camera_offset_dir"] = {camera_offset_expr}
    if {camera_view_up_expr} is not None:
        postprocess_kwargs["camera_view_up"] = {camera_view_up_expr}
    preserved_centerlines = output_dir / "resistance_map" / "intermediate_centerlines"
    original_rmtree = _preserve_intermediate_centerlines(preserved_centerlines)
    try:
        result = _run_postprocess_suite_with_optional_camera(
            run_pulmonary_threed_postprocess_suite,
            postprocess_kwargs,
        )
        _write_stacked_centerline_timeseries(
            output_dir=output_dir,
            suite_metadata_path=metadata_path,
            result=result,
        )
    finally:
        _cleanup_intermediate_centerlines(preserved_centerlines, original_rmtree)
except Exception as exc:
    payload = {{
        "status": "failed",
        "error": {{
            "type": type(exc).__name__,
            "message": str(exc),
        }},
        "metadata_json": str(metadata_path) if metadata_path.exists() else None,
    }}
    submission_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    raise
submission_path.write_text(
    json.dumps({{"status": "completed", "result": result}}, indent=2, sort_keys=True),
    encoding="utf-8",
)
PY
"""


def _resolve_resistance_map_camera(
    config,
    *,
    patient_alias: str,
) -> tuple[list[float] | None, list[float] | None]:
    patient_cfg = next((p for p in config.patients if p.alias == patient_alias), None)
    patient_override = (
        patient_cfg.postprocess.paraview_viz
        if patient_cfg is not None and patient_cfg.postprocess is not None
        else None
    )
    viz_cfg = _resolve_viz_config(config.defaults.postprocess.paraview_viz, patient_override)
    camera_offset_dir = list(viz_cfg.camera_offset_dir) if viz_cfg.camera_offset_dir is not None else None
    camera_view_up = list(viz_cfg.camera_view_up) if viz_cfg.camera_view_up is not None else None
    return camera_offset_dir, camera_view_up


def _selected_preop_local_paths(workspace_root: Path, run_id: str, iteration: int) -> dict[str, Path]:
    local_paths = build_local_run_paths(workspace_root, run_id)
    iter_paths = build_iteration_local_paths(local_paths, iteration)
    root = iter_paths["root"] / "postprocess"
    return {
        "root": root,
        "inputs": root / "inputs",
        "logs": root / "logs",
        "job_script": root / "run_postprocess.sh",
    }


def _selected_preop_remote_layout(remote_run_dir: str, iteration: int) -> dict[str, str]:
    remote_root = str(PurePosixPath(remote_run_dir) / "iterations" / iteration_dir_name(iteration) / "postprocess")
    return {
        "remote_root": remote_root,
        "remote_inputs_dir": str(PurePosixPath(remote_root) / "inputs"),
        "remote_logs_dir": str(PurePosixPath(remote_root) / "logs"),
        "remote_job_script_path": str(PurePosixPath(remote_root) / "run_postprocess.sh"),
        "remote_results_dir": str(
            PurePosixPath(remote_run_dir) / "iterations" / iteration_dir_name(iteration) / "results" / "postprocess"
        ),
    }


def _submit_paraview_viz_if_configured(
    *,
    cluster,
    config,
    patient_alias: str,
    root: Path,
    validated_run_id: str,
    iteration: int,
    transfer_adapter: FileTransferAdapter,
    remote_exec_adapter: RemoteExecAdapter,
) -> None:
    """Submit a paraview viz job alongside postprocess if pvpython and cycle_duration_s are set.

    Runs in parallel with the postprocess job (no SLURM dependency) since both operate
    directly on the raw VTU simulation files. Silently skips with a message if either
    prerequisite is missing rather than raising.
    """
    if cluster.executables.pvpython_path is None:
        print("[postprocess] ParaView viz skipped: pvpython_path not set in clusters.yaml")
        return

    patient_cfg = next((p for p in config.patients if p.alias == patient_alias), None)
    patient_override = (
        patient_cfg.postprocess.paraview_viz
        if patient_cfg is not None and patient_cfg.postprocess is not None
        else None
    )
    viz_cfg = _resolve_viz_config(config.defaults.postprocess.paraview_viz, patient_override)

    if viz_cfg.cycle_duration_s is None:
        print(
            "[postprocess] ParaView viz skipped: set cycle_duration_s in "
            f"patients.yaml under {patient_alias!r} → postprocess.paraview_viz"
        )
        return

    try:
        pviz_result = submit_preop_paraview_viz(
            workspace_root=root,
            run_id=validated_run_id,
            iteration=iteration,
            cycle_duration_s=viz_cfg.cycle_duration_s,
            transfer_adapter=transfer_adapter,
            scheduler_adapter=None,  # pviz builds its own with mem=32G, cpus=1
            remote_exec_adapter=remote_exec_adapter,
        )
        print(f"[postprocess] Also submitted ParaView viz job {pviz_result.submitted_job_id}")
    except Exception as exc:
        print(f"[postprocess] Warning: ParaView viz submission failed: {exc}")


def submit_selected_preop_postprocess(
    *,
    workspace_root: str | Path,
    run_id: str,
    iteration: int,
    transfer_adapter: FileTransferAdapter | None = None,
    scheduler_adapter: SchedulerAdapter | None = None,
    remote_exec_adapter: RemoteExecAdapter | None = None,
) -> PostprocessSubmissionResult:
    root = detect_workspace_root(workspace_root)
    validated_run_id = validate_run_id(run_id)
    local_paths = build_local_run_paths(root, validated_run_id)
    manifest = read_manifest(local_paths.manifest)
    config = load_workspace_config(root)
    cluster = resolve_cluster(config, str(manifest.cluster.get("name")))
    if cluster.executables.svslicer_path is None:
        raise ConfigError("cluster executables.svslicer_path is required for postprocessing")

    remote_run_dir = manifest.execution.remote_run_dir or manifest.remote.get("remote_run_dir")
    if not remote_run_dir:
        raise ConfigError(f"run '{validated_run_id}' is missing remote_run_dir")
    remote_layout = _selected_preop_remote_layout(str(remote_run_dir), iteration)
    local_layout = _selected_preop_local_paths(root, validated_run_id, iteration)
    for key in ("root", "inputs", "logs"):
        local_layout[key].mkdir(parents=True, exist_ok=True)

    patient_alias = str(manifest.patient.get("alias"))
    clinical_targets_payload = _load_stage_target_payload(root, patient_alias=patient_alias, stage="preop")
    svzerodtrees_paths = manifest.remote.get("svzerodtrees_paths", {})
    resistance_map_workers = _resolved_postprocess_worker_count(config)
    cpus_per_task = resistance_map_workers
    mem = None
    if cpus_per_task > 1:
        mem = config.defaults.postprocess.resistance_map.selected_preop_mem
    fallback_csv = svzerodtrees_paths.get("clinical_targets")
    centerline = svzerodtrees_paths.get("centerlines")
    inflow_csv = svzerodtrees_paths.get("inflow")
    if not centerline:
        raise ConfigError("manifest remote.svzerodtrees_paths.centerlines is required for postprocessing")

    simulation_dir = str(PurePosixPath(remote_layout["remote_results_dir"]).parent.parent / "preop")
    camera_offset_dir, camera_view_up = _resolve_resistance_map_camera(
        config,
        patient_alias=patient_alias,
    )
    script_body = _render_postprocess_script(
        config=config,
        cluster=cluster,
        remote_root=remote_layout["remote_root"],
        remote_logs_dir=remote_layout["remote_logs_dir"],
        simulation_dir=simulation_dir,
        output_dir=remote_layout["remote_results_dir"],
        centerline=str(centerline),
        svslicer_path=cluster.executables.svslicer_path,
        stage="preop",
        clinical_targets_payload=clinical_targets_payload,
        fallback_clinical_targets_csv=str(fallback_csv) if fallback_csv else None,
        inflow_csv=str(inflow_csv) if inflow_csv else None,
        resistance_map_workers=resistance_map_workers,
        camera_offset_dir=camera_offset_dir,
        camera_view_up=camera_view_up,
        cpus_per_task=cpus_per_task,
        mem=mem,
    )
    local_layout["job_script"].write_text(script_body, encoding="utf-8")

    if transfer_adapter is None or scheduler_adapter is None or remote_exec_adapter is None:
        default_transfer, default_scheduler, default_remote = _build_default_adapters(
            cluster=cluster,
            config=config,
            run_id=validated_run_id,
            mode=ExecutionMode.EXECUTE,
        )
        transfer_adapter = transfer_adapter or default_transfer
        remote_exec_adapter = remote_exec_adapter or default_remote
        if scheduler_adapter is None:
            scheduler_adapter = _build_postprocess_scheduler_adapter(
                cluster=cluster,
                config=config,
                remote_exec=remote_exec_adapter,
                run_id=validated_run_id,
                cpus_per_task=cpus_per_task,
                mem=mem,
            )

    command_results = [
        transfer_adapter.ensure_remote_dir(remote_layout["remote_root"]),
        transfer_adapter.ensure_remote_dir(remote_layout["remote_inputs_dir"]),
        transfer_adapter.ensure_remote_dir(remote_layout["remote_logs_dir"]),
        transfer_adapter.ensure_remote_dir(remote_layout["remote_results_dir"]),
        transfer_adapter.sync(
            local_dir=str(local_layout["inputs"]),
            remote_dir=remote_layout["remote_inputs_dir"],
            include=config.defaults.rsync.include_patterns,
            exclude=config.defaults.rsync.exclude_patterns,
            direction=SyncDirection.PUSH,
        ),
        transfer_adapter.push(str(local_layout["job_script"]), remote_layout["remote_job_script_path"]),
    ]
    submit_result = scheduler_adapter.submit(remote_layout["remote_job_script_path"])
    command_results.append(submit_result.command)

    manifest = read_manifest(local_paths.manifest)
    manifest = record_postprocess_submission(
        manifest,
        field_name="selected_preop_postprocess",
        stage="selected_preop",
        source_preop_iteration=iteration,
        local_dir=str(local_layout["root"]),
        remote_dir=remote_layout["remote_results_dir"],
        local_job_script_path=str(local_layout["job_script"]),
        remote_job_script_path=remote_layout["remote_job_script_path"],
        scheduler_job_id=submit_result.job_id,
        note="Selected preop postprocess submitted",
    )
    write_manifest(manifest, local_paths.manifest)

    # Submit ParaView viz job in parallel (independent of postprocess, same VTU files).
    # Requires pvpython_path in clusters.yaml and cycle_duration_s in the patient's
    # postprocess.paraview_viz config block.
    _submit_paraview_viz_if_configured(
        cluster=cluster,
        config=config,
        patient_alias=patient_alias,
        root=root,
        validated_run_id=validated_run_id,
        iteration=iteration,
        transfer_adapter=transfer_adapter,
        remote_exec_adapter=remote_exec_adapter,
    )

    return PostprocessSubmissionResult(
        run_id=validated_run_id,
        stage="selected_preop",
        source_preop_iteration=iteration,
        remote_results_dir=remote_layout["remote_results_dir"],
        remote_job_script_path=remote_layout["remote_job_script_path"],
        local_job_script_path=local_layout["job_script"],
        submitted_job_id=submit_result.job_id,
        command_previews=[result.argv for result in command_results],
    )
