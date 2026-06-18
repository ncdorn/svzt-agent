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
  PYTHON_BIN="$(command -v "${{PYTHON_CANDIDATE}}" || command -v python3)"
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


def _copy_array_with_name(array, name: str):
    copied = array.NewInstance()
    copied.DeepCopy(array)
    copied.SetName(name)
    return copied


def _point_scalar_array(data, source_path: Path, name: str, aliases: tuple[str, ...]):
    names = (name, *aliases)
    for candidate in names:
        if data.HasArray(candidate):
            array = data.GetArray(candidate)
            if array is not None and array.GetNumberOfComponents() == 1:
                return array
    raise RuntimeError(f"{source_path}: missing scalar point-data array (tried {names})")


def _strip_timestep_point_fields(data) -> None:
    to_remove = []
    for array_index in range(data.GetNumberOfArrays()):
        name = data.GetArrayName(array_index)
        if name is None:
            continue
        lowered = name.lower()
        if lowered in ("pressure", "velocity", "flow"):
            to_remove.append(name)
        elif lowered.startswith("pressure_") or lowered.startswith("velocity_") or lowered.startswith("flow_"):
            to_remove.append(name)
    for name in to_remove:
        data.RemoveArray(name)


def _validate_matching_geometry(reference: vtk.vtkPolyData, candidate: vtk.vtkPolyData, source_path: Path) -> None:
    if reference.GetNumberOfPoints() != candidate.GetNumberOfPoints():
        raise RuntimeError(f"mapped centerline point count changed for stacked timeseries: {source_path}")
    if reference.GetNumberOfCells() != candidate.GetNumberOfCells():
        raise RuntimeError(f"mapped centerline cell count changed for stacked timeseries: {source_path}")

    for point_index in range(reference.GetNumberOfPoints()):
        if reference.GetPoint(point_index) != candidate.GetPoint(point_index):
            raise RuntimeError(f"mapped centerline point coordinates changed for stacked timeseries: {source_path}")

    for cell_index in range(reference.GetNumberOfCells()):
        reference_cell = reference.GetCell(cell_index)
        candidate_cell = candidate.GetCell(cell_index)
        if reference_cell.GetNumberOfPoints() != candidate_cell.GetNumberOfPoints():
            raise RuntimeError(f"mapped centerline connectivity changed for stacked timeseries: {source_path}")
        for point_offset in range(reference_cell.GetNumberOfPoints()):
            if reference_cell.GetPointId(point_offset) != candidate_cell.GetPointId(point_offset):
                raise RuntimeError(f"mapped centerline connectivity changed for stacked timeseries: {source_path}")


def _add_zerod_timestep_array(base_data, frame_data, source_path: Path, frame_index: int, role: str) -> str:
    if role == "pressure":
        source_array = _point_scalar_array(frame_data, source_path, "Pressure", ("pressure",))
    elif role == "velocity":
        source_array = _point_scalar_array(frame_data, source_path, "Velocity", ("velocity", "Flow", "flow"))
    else:
        raise RuntimeError(f"unsupported stacked centerline role: {role}")

    target_name = f"{role}_{frame_index}"
    base_data.AddArray(_copy_array_with_name(source_array, target_name))
    return target_name


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


def _has_point_array(poly: vtk.vtkPolyData, name: str) -> bool:
    point_data = poly.GetPointData()
    for index in range(point_data.GetNumberOfArrays()):
        candidate = point_data.GetArrayName(index)
        if candidate == name:
            return True
    return False


def _repair_failed_suite_result_if_outputs_exist(
    *,
    output_dir: Path,
    suite_metadata_path: Path,
    result: dict[str, object],
) -> dict[str, object]:
    if not isinstance(result, dict) or result.get("status") != "failed":
        return result

    mean_vtp = output_dir / "resistance_map_mean.vtp"
    mean_metadata_json = output_dir / "resistance_map_metadata.json"
    mean_summary_csv = output_dir / "branch_resistance_summary.csv"
    mean_ranked_csv = output_dir / "ranked_stent_candidates.csv"
    systolic_vtp = output_dir / "resistance_map_systolic.vtp"
    systolic_metadata_json = output_dir / "resistance_map_systolic_metadata.json"
    systolic_summary_csv = output_dir / "branch_resistance_summary_systolic.csv"
    systolic_ranked_csv = output_dir / "ranked_stent_candidates_systolic.csv"

    required_paths = [
        mean_vtp,
        mean_metadata_json,
        mean_summary_csv,
        mean_ranked_csv,
        systolic_vtp,
        systolic_metadata_json,
        systolic_summary_csv,
        systolic_ranked_csv,
        suite_metadata_path,
    ]
    if any(not path.exists() for path in required_paths):
        return result

    mean_poly = _read_polydata(mean_vtp)
    systolic_poly = _read_polydata(systolic_vtp)
    if not _has_point_array(mean_poly, "BranchId") or not _has_point_array(systolic_poly, "BranchId"):
        return result

    mean_metadata = json.loads(mean_metadata_json.read_text(encoding="utf-8"))
    systolic_metadata = json.loads(systolic_metadata_json.read_text(encoding="utf-8"))
    repaired = json.loads(suite_metadata_path.read_text(encoding="utf-8"))

    repaired.pop("error", None)
    repaired["status"] = "completed"
    steps = repaired.get("steps")
    if not isinstance(steps, dict):
        steps = {}
        repaired["steps"] = steps

    mean_result = {
        "kind": "pulmonary_resistance_map",
        "metric_suffix": "mean",
        "output_dir": str(output_dir / "resistance_map"),
        "resistance_map": str(mean_vtp),
        "summary_csv": str(mean_summary_csv),
        "ranked_csv": str(mean_ranked_csv),
        "metadata_json": str(mean_metadata_json),
        "selected_frame_count": int(mean_metadata.get("selected_frame_count", 0)),
        "available_frame_count": int(mean_metadata.get("available_frame_count", 0)),
        "intermediate_dir": mean_metadata.get("intermediate_dir"),
    }
    systolic_result = {
        "kind": "pulmonary_resistance_map",
        "metric_suffix": "systolic",
        "output_dir": str(output_dir / "resistance_map_systolic"),
        "resistance_map": str(systolic_vtp),
        "summary_csv": str(systolic_summary_csv),
        "ranked_csv": str(systolic_ranked_csv),
        "metadata_json": str(systolic_metadata_json),
        "selected_frame_count": int(systolic_metadata.get("selected_frame_count", 0)),
        "available_frame_count": int(systolic_metadata.get("available_frame_count", 0)),
        "intermediate_dir": systolic_metadata.get("intermediate_dir"),
    }

    steps["resistance_map"] = {"status": "completed", "result": mean_result}
    steps["resistance_map_systolic"] = {"status": "completed", "result": systolic_result}
    repaired["resistance_map"] = mean_result
    repaired["resistance_map_systolic"] = systolic_result
    suite_metadata_path.write_text(json.dumps(repaired, indent=2, sort_keys=True), encoding="utf-8")
    return repaired


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
    reference_poly = None
    processed_frames: list[dict[str, object]] = []
    zerod_point_arrays: list[str] = []
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
        poly = _read_polydata(mapped_path)
        if reference_poly is None:
            reference_poly = poly
            _strip_timestep_point_fields(reference_poly.GetPointData())
        else:
            _validate_matching_geometry(reference_poly, poly, mapped_path)

        pressure_name = _add_zerod_timestep_array(
            reference_poly.GetPointData(),
            poly.GetPointData(),
            mapped_path,
            frame_index,
            "pressure",
        )
        velocity_name = _add_zerod_timestep_array(
            reference_poly.GetPointData(),
            poly.GetPointData(),
            mapped_path,
            frame_index,
            "velocity",
        )
        zerod_point_arrays.extend([pressure_name, velocity_name])

        processed_frames.append(
            {
                "frame_index": frame_index,
                "time_s": float(raw_time),
                "timestep_id": timestep_id,
                "source_frame_path": frame.get("source_frame_path"),
                "point_arrays": [pressure_name, velocity_name],
            }
        )

    if reference_poly is None:
        raise RuntimeError("resistance map metadata missing selected_frames")

    output_path = output_dir / "centerline_timeseries_last_cycle.vtp"
    metadata_output_path = output_dir / "centerline_timeseries_last_cycle_metadata.json"
    _write_polydata(reference_poly, output_path)

    stack_result = {
        "kind": "centerline_timeseries_last_cycle",
        "output_path": str(output_path),
        "metadata_json": str(metadata_output_path),
        "source_resistance_map_metadata_json": str(mean_metadata_path),
        "selected_frame_count": len(processed_frames),
        "point_count": int(reference_poly.GetNumberOfPoints()),
        "cell_count": int(reference_poly.GetNumberOfCells()),
        "zerod_point_arrays": zerod_point_arrays,
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
        result = _repair_failed_suite_result_if_outputs_exist(
            output_dir=output_dir,
            suite_metadata_path=metadata_path,
            result=result,
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
    manifest = read_manifest(build_local_run_paths(root, validated_run_id).manifest)
    existing_preop_pviz = next(
        (
            record
            for record in reversed(manifest.paraview_viz_runs)
            if record.stage == "preop" and int(record.source_iteration) == int(iteration)
        ),
        None,
    )
    if existing_preop_pviz is not None:
        print(
            "[postprocess] ParaView viz skipped: existing preop viz record "
            f"found for iteration {iteration} (job {existing_preop_pviz.scheduler_job_id or 'planned'})"
        )
        return

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
    threed_defaults = manifest.remote.get("threed_defaults", {})
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
