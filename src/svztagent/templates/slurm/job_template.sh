#!/usr/bin/env bash

#SBATCH --partition={{SBATCH_PARTITION}}
#SBATCH --time={{SBATCH_TIME}}
#SBATCH --mem={{SBATCH_MEM}}
#SBATCH --cpus-per-task={{SBATCH_CPUS}}
#SBATCH --output={{REMOTE_LOGS_DIR}}/slurm-%j.out
#SBATCH --error={{REMOTE_LOGS_DIR}}/%x_%j.error

echo "[svzt] run_id={{RUN_ID}}"
echo "[svzt] iteration={{ITERATION}}"
echo "[svzt] remote_run_dir={{REMOTE_RUN_DIR}}"
echo "[svzt] remote_iter_dir={{REMOTE_ITER_DIR}}"
echo "[svzt] python_executable={{PYTHON_EXECUTABLE}}"

# Optional environment activation hooks from workspace defaults.
{{ENV_HOOKS}}

PYTHON_CANDIDATE="{{PYTHON_EXECUTABLE}}"
if [ -z "${PYTHON_CANDIDATE}" ]; then
  PYTHON_CANDIDATE="python3"
fi

if [ "${PYTHON_CANDIDATE#*/}" != "${PYTHON_CANDIDATE}" ]; then
  if [ ! -x "${PYTHON_CANDIDATE}" ]; then
    echo "[svzt] error: configured python executable is not executable: ${PYTHON_CANDIDATE}" >&2
    exit 1
  fi
  PYTHON_BIN="${PYTHON_CANDIDATE}"
else
  if command -v "${PYTHON_CANDIDATE}" >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v "${PYTHON_CANDIDATE}")"
  elif command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python3)"
  elif command -v python >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python)"
  else
    echo "[svzt] error: no Python interpreter found on PATH" >&2
    exit 1
  fi
fi

echo "[svzt] python_bin=${PYTHON_BIN}"
"${PYTHON_BIN}" - <<'PY'
import importlib
import importlib.util
import sys

spec = importlib.util.find_spec("svzerodtrees")
if spec is None:
    print("[svzt] error: svzerodtrees is not importable in selected Python", file=sys.stderr)
    sys.exit(2)
print("[svzt] svZeroDTrees=" + str(spec.origin))

try:
    from svzerodtrees.simulation import Simulation, SimulationDirectory  # noqa: F401
except Exception as exc:
    print(f"[svzt] error: failed to import svzerodtrees.simulation: {exc}", file=sys.stderr)
    sys.exit(3)

try:
    tuning = importlib.import_module("svzerodtrees.tuning")
except Exception as exc:
    print(f"[svzt] error: failed to import svzerodtrees.tuning: {exc}", file=sys.stderr)
    sys.exit(4)

try:
    post_processing = importlib.import_module("svzerodtrees.post_processing")
except Exception as exc:
    print(f"[svzt] error: failed to import svzerodtrees.post_processing: {exc}", file=sys.stderr)
    sys.exit(6)

required_symbols = [
    "compute_centerline_mpa_metrics",
    "compute_flow_split_metrics",
    "evaluate_iteration_gate",
    "generate_reduced_pa_from_iteration",
    "run_impedance_tuning_for_iteration",
    "write_iteration_decision",
    "write_iteration_metrics",
]
missing_symbols = [name for name in required_symbols if not hasattr(tuning, name)]
if missing_symbols:
    print(
        "[svzt] error: svzerodtrees.tuning missing required symbols: "
        + ", ".join(missing_symbols),
        file=sys.stderr,
    )
    sys.exit(5)
if not hasattr(post_processing, "write_mpa_pressure_timeseries_csv"):
    print(
        "[svzt] error: svzerodtrees.post_processing missing required symbol: "
        "write_mpa_pressure_timeseries_csv",
        file=sys.stderr,
    )
    sys.exit(7)
PY

mkdir -p "{{REMOTE_INPUTS_DIR}}" "{{REMOTE_RESULTS_DIR}}" "{{REMOTE_LOGS_DIR}}"

# Iteration execution contract:
# 1) 0D tuning on the staged seed config
# 2) preop 3D simulation submission/execution
# 3) centerline + flow-split metric extraction
# 4) clinical gate evaluation
# 5) branch to reduced-PA regeneration or postop submission intent
"${PYTHON_BIN}" - <<'PY'
from __future__ import annotations

from pathlib import Path
import csv
import heapq
import json
import math
import os
import re
import shutil
import subprocess
import sys
import time
import traceback
import xml.etree.ElementTree as ET

import numpy as np

from svzerodtrees.simulation import Simulation, SimulationDirectory
from svzerodtrees.post_processing import write_mpa_pressure_timeseries_csv
from svzerodtrees.tuning import (
    compute_centerline_mpa_metrics,
    compute_flow_split_metrics,
    evaluate_iteration_gate,
    generate_reduced_pa_from_iteration,
    run_impedance_tuning_for_iteration,
    write_iteration_decision,
    write_iteration_metrics,
)

run_id = "{{RUN_ID}}"
iteration = int("{{ITERATION}}")
remote_run_dir = Path("{{REMOTE_RUN_DIR}}")
remote_iter_dir = Path("{{REMOTE_ITER_DIR}}")
remote_inputs_dir = Path("{{REMOTE_INPUTS_DIR}}")
remote_results_dir = Path("{{REMOTE_RESULTS_DIR}}")
remote_logs_dir = Path("{{REMOTE_LOGS_DIR}}")

clinical_targets_path = Path("{{REMOTE_CLINICAL_TARGETS_PATH}}") if "{{REMOTE_CLINICAL_TARGETS_PATH}}" else None
centerline_path = Path("{{REMOTE_CENTERLINE_PATH}}") if "{{REMOTE_CENTERLINE_PATH}}" else None
remote_inflow_path = Path("{{REMOTE_INFLOW_PATH}}") if "{{REMOTE_INFLOW_PATH}}" else None
staged_inflow_path = remote_inputs_dir / "inflow.csv"
preop_mesh_complete_path = Path("{{REMOTE_PREOP_MESH_COMPLETE_DIR}}") if "{{REMOTE_PREOP_MESH_COMPLETE_DIR}}" else None
postop_mesh_complete_path = Path("{{REMOTE_POSTOP_MESH_COMPLETE_DIR}}") if "{{REMOTE_POSTOP_MESH_COMPLETE_DIR}}" else None

cluster_svfsiplus_path = "{{CLUSTER_SVFSIPLUS_PATH}}"
threed_config = json.loads(r'''{{THREED_CONFIG_JSON}}''')
impedance_config = json.loads(r'''{{IMPEDANCE_CONFIG_JSON}}''')
skip_zerod_tuning = json.loads(r'''{{SKIP_ZEROD_TUNING_JSON}}''')
mesh_scale_factor = float("{{MESH_SCALE_FACTOR}}")
scheduler_defaults = {
    "account": "{{SBATCH_ACCOUNT}}",
    "partition": "{{SBATCH_PARTITION}}",
    "wall_time": "{{SBATCH_TIME}}",
    "mem": "{{SBATCH_MEM}}",
}

metrics_path = remote_results_dir / "iteration_metrics.json"
decision_path = remote_results_dir / "iteration_decision.json"

log = {
    "run_id": run_id,
    "iteration": iteration,
    "steps": [],
    "errors": [],
    "warnings": [],
    "preop_job_id": None,
    "postop_job_id": None,
    "prestress_job_id": None,
    "prestress_file_path": None,
    "prestress_traction_source": None,
    "preop_terminal_state": None,
}

metrics = {}
decision_payload = {
    "decision": "not_close",
    "close_to_targets": False,
    "thresholds": {
        "mpa_sys": 5.0,
        "mpa_dia": 3.0,
        "mpa_mean": 3.0,
        "rpa_split": 0.05,
    },
    "regenerated_config_path": None,
    "tuning_artifacts": {
        "optimized_params_csv": None,
        "stree_optimization_log": None,
        "pa_config_snapshot": None,
        "tuned_zerod_config": None,
    },
    "postop_submission_requested": False,
    "postop_job_id": None,
    "needs_review_reason": None,
}


def _mark_needs_review(reason: str) -> None:
    decision_payload["decision"] = "needs_review"
    decision_payload["close_to_targets"] = False
    decision_payload["postop_submission_requested"] = False
    decision_payload["needs_review_reason"] = reason
    log["errors"].append(reason)


def _safe_remove(path: Path) -> None:
    if path.is_symlink() or path.is_file():
        path.unlink()
    elif path.is_dir():
        shutil.rmtree(path)


def _link_or_copy_directory(source: Path, target: Path) -> None:
    _safe_remove(target)
    try:
        target.symlink_to(source, target_is_directory=True)
    except OSError:
        shutil.copytree(source, target)


def _resolve_tuning_inflow_path() -> Path | None:
    if staged_inflow_path.exists():
        return staged_inflow_path
    if remote_inflow_path is None or not remote_inflow_path.exists():
        return None
    if remote_inflow_path != staged_inflow_path:
        shutil.copyfile(remote_inflow_path, staged_inflow_path)
        log["steps"].append("inflow_staged_from_patient_assets")
    return staged_inflow_path


def _cycle_duration_from_inflow(inflow_path: Path | None) -> float | None:
    if inflow_path is None or not inflow_path.exists():
        return None
    with inflow_path.open(newline="") as ff:
        reader = csv.DictReader(ff)
        if reader.fieldnames is None:
            return None
        time_col = next(
            (name for name in ("t", "time", "time_s") if name in reader.fieldnames),
            None,
        )
        if time_col is None:
            return None
        times = []
        for row in reader:
            try:
                value = float(row[time_col])
            except (TypeError, ValueError):
                continue
            if math.isfinite(value):
                times.append(value)
    if len(times) < 2:
        return None
    duration = max(times) - min(times)
    return duration if math.isfinite(duration) and duration > 0.0 else None


def _write_inflow_flow_from_patient_csv(
    stage_dir: Path, inflow_csv: Path, sim_cfg: dict
) -> None:
    """Write inflow.flow from the patient source-of-truth inflow CSV.

    The reduced-order 0D seed used for tuning carries a scaled-down inflow
    (PA outlet scale). Always regenerating inflow.flow from the patient CSV
    ensures the 3D CMM simulation always sees the correct inflow magnitude,
    regardless of which iteration we are on.
    """
    import csv as _csv
    import numpy as _np

    times_raw: list[float] = []
    flows_raw: list[float] = []
    with inflow_csv.open(newline="", encoding="utf-8") as fh:
        reader = _csv.DictReader(fh)
        for row in reader:
            times_raw.append(float(row["t"]))
            flows_raw.append(float(row["q"]))

    times = _np.asarray(times_raw, dtype=float)
    flows = _np.asarray(flows_raw, dtype=float)
    times = times - float(times.min())

    period = float(times.max())
    dt_val = float(sim_cfg.get("dt", 0.0))
    if period > 0.0 and dt_val > 0.0:
        n_tsteps = max(int(round(period / dt_val)), 2)
    else:
        n_tsteps = max(int(times.size), 2)

    if times.size >= 2 and float(times[-1]) < period:
        times = _np.append(times, period)
        flows = _np.append(flows, flows[0])

    sample_times = _np.linspace(0.0, period, n_tsteps)
    sample_flows = _np.interp(sample_times, times, flows)

    inflow_flow_path = stage_dir / "inflow.flow"
    with inflow_flow_path.open("w", encoding="utf-8") as ff:
        ff.write(f"{n_tsteps} 16\n")
        for t, q in zip(sample_times, (-1.0 * sample_flows)):
            ff.write(f"{t} {q}\n")


def _generate_iteration_seed(seed_path: Path) -> None:
    if preop_mesh_complete_path is None:
        raise RuntimeError("preop mesh-complete path is not configured")
    if clinical_targets_path is None:
        raise RuntimeError("clinical targets path is not configured")
    if not preop_mesh_complete_path.exists():
        raise RuntimeError(f"preop mesh-complete directory missing: {preop_mesh_complete_path}")
    if not clinical_targets_path.exists():
        raise RuntimeError(f"clinical targets file missing: {clinical_targets_path}")

    seed_workspace = remote_iter_dir / "seed_generation"
    preop_dir = seed_workspace / "preop"
    postop_dir = seed_workspace / "postop"
    preop_dir.mkdir(parents=True, exist_ok=True)
    postop_dir.mkdir(parents=True, exist_ok=True)

    mesh_complete_target = preop_dir / "mesh-complete"
    _link_or_copy_directory(preop_mesh_complete_path, mesh_complete_target)

    sim = Simulation(
        path=str(seed_workspace),
        clinical_targets=str(clinical_targets_path),
        preop_dir="preop",
        postop_dir="postop",
        adapted_dir=None,
        inflow_path=str(remote_inflow_path) if remote_inflow_path is not None and remote_inflow_path.exists() else None,
    )
    # Seed generation only needs steady solves and the reduced 0D seed.  The
    # full pipeline reads optimized_params.csv for impedance BCs even when
    # optimize_bcs=False, so avoid that tuning-artifact path here.
    sim.run_steady_sims()
    sim.generate_simplified_nonlinear_zerod()

    generated = seed_workspace / "simplified_nonlinear_zerod.json"
    if not generated.exists():
        raise RuntimeError(f"svZeroDTrees seed generation did not write {generated}")

    seed_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(generated, seed_path)
    log["steps"].append("iteration_seed_generated")


_NESTED_SBATCH_STRIP_ENV_VARS = (
    "SBATCH_CPUS_PER_TASK",
    "SBATCH_MEM",
    "SBATCH_MEM_PER_CPU",
    "SBATCH_MEM_PER_NODE",
    "SBATCH_NTASKS",
    "SBATCH_NTASKS_PER_NODE",
    "SBATCH_NODES",
    "SLURM_CPUS_ON_NODE",
    "SLURM_CPUS_PER_TASK",
    "SLURM_JOB_CPUS_PER_NODE",
    "SLURM_JOB_NUM_NODES",
    "SLURM_MEM_PER_CPU",
    "SLURM_MEM_PER_NODE",
    "SLURM_NNODES",
    "SLURM_NPROCS",
    "SLURM_NTASKS",
    "SLURM_NTASKS_PER_NODE",
    "SLURM_TASKS_PER_NODE",
    "SLURM_TRES_PER_TASK",
)


def _nested_sbatch_env() -> dict[str, str]:
    env = os.environ.copy()
    for name in _NESTED_SBATCH_STRIP_ENV_VARS:
        env.pop(name, None)
    return env


def _normalize_solver_runscript(
    *,
    script_path: Path,
    nodes: int,
    procs_per_node: int,
    memory_gb: int,
    hours: int,
    partition: str,
    account: str | None,
    svfsiplus_path: str,
) -> None:
    stage_dir = script_path.parent.resolve()
    output_path = stage_dir / "svFlowSolver.o%j"
    error_path = stage_dir / "svFlowSolver.e%j"
    lines = script_path.read_text(encoding="utf-8", errors="replace").splitlines()
    body: list[str] = []
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("#!/"):
            continue
        if stripped.startswith("#SBATCH"):
            continue
        if stripped.startswith("srun "):
            continue
        if "--mail-user" in stripped or "--mail-type" in stripped:
            continue
        body.append(line)

    header = [
        "#!/usr/bin/env bash",
        "",
        "#SBATCH --job-name=svFlowSolver",
        f"#SBATCH --partition={partition}",
        f"#SBATCH --chdir={stage_dir}",
        f"#SBATCH --output={output_path}",
        f"#SBATCH --error={error_path}",
        f"#SBATCH --time={hours}:00:00",
        f"#SBATCH --nodes={nodes}",
        f"#SBATCH --ntasks-per-node={procs_per_node}",
        f"#SBATCH --mem={memory_gb}G",
    ]
    if account:
        header.insert(4, f"#SBATCH --account={account}")

    rendered = "\n".join(header) + "\n\n"
    if body:
        rendered += "\n".join(body) + "\n"
    rendered += f"cd {stage_dir}\n"
    rendered += 'if [ -n "${SLURM_CPUS_PER_TASK:-}" ] && [ -n "${SLURM_TRES_PER_TASK:-}" ]; then\n'
    rendered += '  case "${SLURM_TRES_PER_TASK}" in\n'
    rendered += '    cpu=*)\n'
    rendered += '      _svzt_tres_cpus="${SLURM_TRES_PER_TASK#cpu=}"\n'
    rendered += '      _svzt_tres_cpus="${_svzt_tres_cpus%%,*}"\n'
    rendered += '      if [ "${SLURM_CPUS_PER_TASK}" != "${_svzt_tres_cpus}" ]; then\n'
    rendered += '        unset SLURM_TRES_PER_TASK\n'
    rendered += '      fi\n'
    rendered += '      unset _svzt_tres_cpus\n'
    rendered += '      ;;\n'
    rendered += '  esac\n'
    rendered += 'fi\n'
    rendered += f"srun {svfsiplus_path} svFSIplus.xml\n"
    script_path.write_text(rendered, encoding="utf-8")
    script_path.chmod(0o755)


def _load_strict_json(path: Path) -> dict:
    def _reject_constant(value: str):
        raise ValueError(f"non-finite JSON value {value}")

    return json.loads(path.read_text(encoding="utf-8"), parse_constant=_reject_constant)


def _assert_finite_json(value, *, path: str = "$") -> None:
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"non-finite numeric value at {path}")
    if isinstance(value, dict):
        for key, item in value.items():
            _assert_finite_json(item, path=f"{path}.{key}")
    elif isinstance(value, list):
        for idx, item in enumerate(value):
            _assert_finite_json(item, path=f"{path}[{idx}]")


def _validate_canonical_coupler(stage_dir: Path) -> None:
    coupling_path = stage_dir / "svzerod_3Dcoupling.json"
    if not coupling_path.exists():
        raise RuntimeError(f"canonical 3D coupler was not written: {coupling_path}")

    payload = _load_strict_json(coupling_path)
    _assert_finite_json(payload)
    blocks = payload.get("external_solver_coupling_blocks")
    if not isinstance(blocks, list) or not blocks:
        raise RuntimeError(
            f"{coupling_path} must contain non-empty external_solver_coupling_blocks"
        )

    block_names = [str(block.get("name", "")).strip() for block in blocks if isinstance(block, dict)]
    if len(block_names) != len(blocks) or any(not name for name in block_names):
        raise RuntimeError(f"{coupling_path} contains malformed coupling block names")
    if len(set(block_names)) != len(block_names):
        raise RuntimeError(f"{coupling_path} contains duplicate coupling block names")


def _extract_result_step(path: Path) -> int:
    match = re.search(r"(\d+)\.vtu$", path.name)
    return int(match.group(1)) if match else -1


def _latest_result_vtu(sim_dir: Path) -> Path | None:
    candidates = list(sim_dir.glob("*-procs/result_*.vtu"))
    if not candidates:
        candidates = list(sim_dir.glob("result_*.vtu"))
    if not candidates:
        return None
    return max(candidates, key=lambda path: (_extract_result_step(path), path.stat().st_mtime))


def _result_vtus(result_dir: Path) -> list[Path]:
    return sorted(result_dir.glob("result_*.vtu"), key=_extract_result_step)


def _seed_generation_mean_result_dir() -> Path:
    seed_mean_dir = remote_run_dir / "iterations" / "iter-01" / "seed_generation" / "steady" / "mean"
    result_dirs = [
        path
        for path in sorted(seed_mean_dir.glob("*-procs"))
        if path.is_dir() and _result_vtus(path)
    ]
    if not result_dirs:
        raise RuntimeError(
            "prestress_file=generate requires seed-generation mean steady VTUs under "
            f"{seed_mean_dir}/*-procs/result_*.vtu"
        )
    return max(
        result_dirs,
        key=lambda path: (
            _extract_result_step(_result_vtus(path)[-1]),
            _result_vtus(path)[-1].stat().st_mtime,
        ),
    )


def _result_step_range(result_dir: Path) -> tuple[int, int, int]:
    vtus = _result_vtus(result_dir)
    if not vtus:
        raise RuntimeError(f"no result_*.vtu files found in {result_dir}")
    steps = [_extract_result_step(path) for path in vtus]
    if any(step < 0 for step in steps):
        raise RuntimeError(f"could not parse timestep IDs from result_*.vtu files in {result_dir}")
    if len(steps) == 1:
        return steps[0], steps[0], 1
    diffs = [right - left for left, right in zip(steps, steps[1:])]
    unique_diffs = sorted(set(diffs))
    if len(unique_diffs) != 1 or unique_diffs[0] <= 0:
        raise RuntimeError(
            "seed-generation mean steady VTUs must be evenly spaced for traction averaging "
            f"(steps={steps})"
        )
    return steps[0], steps[-1], unique_diffs[0]


def _generate_mpa_pressure_csv(preop_dir: Path) -> Path:
    """Use the shared svZeroDTrees helper to write mpa_pressure_vs_time.csv."""
    if centerline_path is None or not centerline_path.exists():
        raise FileNotFoundError(
            f"centerline_path is not configured or does not exist ({centerline_path}); "
            "cannot generate mpa_pressure_vs_time.csv"
        )
    pressure_csv = remote_results_dir / "mpa_pressure_vs_time.csv"
    summary = write_mpa_pressure_timeseries_csv(
        simulation_dir=preop_dir,
        centerline=centerline_path,
        output_csv=pressure_csv,
    )
    log["steps"].append(
        "centerline_pressure_csv_generated_from_svzerodtrees:"
        f"{summary['file_count']}_files:bif={summary['bifurcation_id']}"
    )
    return pressure_csv


def _ensure_generated_prestress_file() -> Path:
    if preop_mesh_complete_path is None:
        raise RuntimeError("preop mesh-complete path is not configured")
    wall_path = preop_mesh_complete_path / "walls_combined.vtp"
    if not wall_path.exists():
        raise RuntimeError(f"wall file missing for prestress generation: {wall_path}")

    prestress_dir = remote_run_dir / "prestress"
    prestress_dir.mkdir(parents=True, exist_ok=True)

    existing = _latest_result_vtu(prestress_dir)
    if existing is not None:
        log["prestress_file_path"] = str(existing)
        log["steps"].append("prestress_reused")
        return existing

    mean_result_dir = _seed_generation_mean_result_dir()
    start, stop, stride = _result_step_range(mean_result_dir)
    log["prestress_traction_source"] = str(mean_result_dir)
    log["steps"].append(
        f"prestress_traction_source:{mean_result_dir}:start={start}:stop={stop}:stride={stride}"
    )

    traction_script = Path.home() / "scripts" / "calc_mean_wall_traction.py"
    if not traction_script.exists():
        raise RuntimeError(f"mean wall traction script missing: {traction_script}")

    proc = subprocess.run(
        [
            sys.executable,
            str(traction_script),
            "--result-dir",
            str(mean_result_dir),
            "--wall",
            str(wall_path),
            "--start",
            str(start),
            "--stop",
            str(stop),
            "--stride",
            str(stride),
        ],
        cwd=prestress_dir,
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            "mean wall traction calculation failed "
            f"rc={proc.returncode}: {proc.stderr.strip() or proc.stdout.strip()}"
        )

    traction_file = prestress_dir / "rigid_wall_mean_traction.vtp"
    if not traction_file.exists():
        raise RuntimeError(f"mean wall traction script did not write {traction_file}")
    log["steps"].append("prestress_traction_generated")

    prestress_sim = SimulationDirectory.from_directory(
        path=str(prestress_dir),
        mesh_complete=str(preop_mesh_complete_path),
        mesh_scale_factor=mesh_scale_factor,
    )
    if getattr(prestress_sim, "svzerod_3Dcoupling", None) is not None:
        prestress_sim.svzerod_3Dcoupling.to_json(prestress_sim.svzerod_3Dcoupling.path)

    # svMultiPhysics writes result VTUs on its configured output cadence; 10
    # prestress steps can complete successfully without writing result_*.vtu.
    prestress_config = {
        "n_tsteps": 20,
        "dt": 0.001,
        "vtk_save_increment": 1,
        "nodes": 1,
        "procs_per_node": 1,
        "memory": int(threed_config.get("memory", 16)),
        "hours": int(threed_config.get("hours", 20)),
        "simulation_mode": "prestress",
        "traction_file_path": str(traction_file),
        "wall_model": "deformable",
        "elasticity_modulus": threed_config.get("elasticity_modulus"),
        "poisson_ratio": threed_config.get("poisson_ratio"),
        "shell_thickness": threed_config.get("shell_thickness"),
        "tissue_support": threed_config.get("tissue_support"),
    }
    prestress_sim.write_files(
        simname="Prestress Simulation",
        user_input=False,
        sim_config=prestress_config,
    )
    _force_xml_text(prestress_dir / "svFSIplus.xml", "Increment_in_saving_VTK_files", "1")

    run_solver_path = prestress_dir / "run_solver.sh"
    _normalize_solver_runscript(
        script_path=run_solver_path,
        nodes=1,
        procs_per_node=1,
        memory_gb=int(prestress_config["memory"]),
        hours=int(prestress_config["hours"]),
        partition=str(scheduler_defaults.get("partition") or "amarsden"),
        account=str(scheduler_defaults.get("account") or "").strip() or None,
        svfsiplus_path=cluster_svfsiplus_path,
    )

    prestress_job_id = _submit_job(run_solver_path)
    log["prestress_job_id"] = prestress_job_id
    log["steps"].append("prestress_submitted")

    ok, terminal = _wait_for_completion(
        prestress_job_id,
        poll_seconds=int(threed_config.get("wait_poll_seconds", 30)),
        timeout_seconds=int(threed_config.get("wait_timeout_seconds", 43200)),
    )
    if not ok:
        raise RuntimeError(f"prestress simulation did not complete successfully: {terminal}")

    generated = _latest_result_vtu(prestress_dir)
    if generated is None:
        raise RuntimeError(f"prestress simulation completed but no result_*.vtu found in {prestress_dir}")
    log["prestress_file_path"] = str(generated)
    log["steps"].append("prestress_completed")
    return generated


def _force_xml_text(xml_path: Path, tag: str, value: str) -> None:
    if not xml_path.exists():
        raise RuntimeError(f"expected XML file missing: {xml_path}")
    tree = ET.parse(xml_path)
    root = tree.getroot()
    node = root.find(f".//{tag}")
    if node is None:
        raise RuntimeError(f"{xml_path} missing required XML tag {tag}")
    node.text = value
    ET.indent(root)
    tree.write(xml_path, encoding="utf-8", xml_declaration=True)


def _resolve_prestress_file_path(sim_cfg: dict) -> str | None:
    if sim_cfg.get("prestress_file_path"):
        return str(sim_cfg["prestress_file_path"])

    prestress_setting = str(sim_cfg.get("prestress_file", "") or "").strip()
    prestress_mode = prestress_setting.lower()
    if (
        sim_cfg.get("wall_model", "deformable") == "deformable"
        and prestress_mode == "generate"
    ):
        return str(_ensure_generated_prestress_file())

    if (
        sim_cfg.get("wall_model", "deformable") == "deformable"
        and prestress_mode in {"auto", "from_steady_mean"}
    ):
        log["warnings"].append(
            "deformable run requested with prestress_file=auto, but auto prestress generation is not available in iteration script; continuing without Prestress_file_path"
        )
        return None

    if prestress_setting and prestress_mode not in {"auto", "from_steady_mean", "generate"}:
        return prestress_setting

    return None


def _submit_job(script_path: Path) -> str:
    proc = subprocess.run(
        ["sbatch", "--parsable", "--chdir", str(script_path.parent), script_path.name],
        cwd=script_path.parent,
        capture_output=True,
        env=_nested_sbatch_env(),
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"sbatch failed rc={proc.returncode}: {proc.stderr.strip()}")
    stdout = proc.stdout.strip()
    if not stdout:
        raise RuntimeError("sbatch returned empty stdout")
    return stdout.split(";")[0].strip()


def _query_state(job_id: str) -> tuple[str | None, str]:
    squeue = subprocess.run(
        ["squeue", "--job", job_id, "--noheader", "--format", "%T"],
        capture_output=True,
        text=True,
        check=False,
    )
    if squeue.returncode == 0:
        raw = squeue.stdout.strip().splitlines()
        if raw:
            return raw[0].strip().split()[0].strip().upper(), "squeue"

    sacct = subprocess.run(
        ["sacct", "-j", job_id, "--noheader", "--format", "State"],
        capture_output=True,
        text=True,
        check=False,
    )
    if sacct.returncode == 0:
        for line in sacct.stdout.splitlines():
            cleaned = line.strip()
            if not cleaned:
                continue
            state = cleaned.split()[0].split("+")[0].strip().upper()
            if state:
                return state, "sacct"

    return None, "unknown"


def _wait_for_completion(job_id: str, poll_seconds: int, timeout_seconds: int) -> tuple[bool, str | None]:
    success_states = {"COMPLETED"}
    failure_states = {
        "FAILED",
        "CANCELLED",
        "TIMEOUT",
        "PREEMPTED",
        "OUT_OF_MEMORY",
        "NODE_FAIL",
        "BOOT_FAIL",
        "DEADLINE",
    }
    active_states = {
        "PENDING",
        "RUNNING",
        "CONFIGURING",
        "COMPLETING",
        "SUSPENDED",
        "RESIZING",
        "REQUEUED",
        "REQUEUE_HOLD",
        "SIGNALING",
        "SPECIAL_EXIT",
        "STAGE_OUT",
        "STOPPED",
    }

    start = time.monotonic()
    last_state: str | None = None
    while True:
        elapsed = int(time.monotonic() - start)
        if elapsed > timeout_seconds:
            return False, f"timeout after {timeout_seconds}s (last_state={last_state or 'unknown'})"

        state, source = _query_state(job_id)
        if state:
            last_state = state
            log["steps"].append(f"job_poll:{job_id}:{source}:{state}")
            if state in success_states:
                return True, state
            if state in failure_states:
                return False, state
            if state not in active_states:
                return False, f"unexpected terminal scheduler state: {state}"

        time.sleep(max(poll_seconds, 5))


def _prepare_and_submit_stage(
    *,
    stage_name: str,
    mesh_complete_path: Path,
    zerod_config_path: Path,
) -> tuple[str, Path]:
    stage_dir = remote_iter_dir / stage_name
    stage_dir.mkdir(parents=True, exist_ok=True)

    mesh_target = stage_dir / "mesh-complete"
    _link_or_copy_directory(mesh_complete_path, mesh_target)

    sim = SimulationDirectory.from_directory(
        path=str(stage_dir),
        zerod_config=str(zerod_config_path),
        mesh_complete=str(mesh_target),
        threed_coupler=True,
        mesh_scale_factor=mesh_scale_factor,
    )

    sim_cfg = dict(threed_config)
    prestress_file_path = _resolve_prestress_file_path(sim_cfg)
    if prestress_file_path:
        sim_cfg["prestress_file_path"] = prestress_file_path

    sim.write_files(simname=f"{stage_name.capitalize()} Simulation", user_input=False, sim_config=sim_cfg)
    # The 0D seed for iterations 2+ has a reduced-order-PA inflow (scaled by
    # PA outlet area). Always overwrite inflow.flow from the patient CSV so the
    # 3D CMM simulation always uses the source-of-truth inflow magnitude.
    if str(sim_cfg.get("inflow_boundary_condition", "neumann")).lower() == "dirichlet":
        _stage_inflow = _resolve_tuning_inflow_path()
        if _stage_inflow is not None and _stage_inflow.exists():
            _write_inflow_flow_from_patient_csv(stage_dir, _stage_inflow, sim_cfg)
            log["steps"].append(f"{stage_name}_inflow_flow_written_from_patient_csv")
    if zerod_config_path.name == "svzerod_3d_coupling_tuned.json":
        provenance_path = stage_dir / zerod_config_path.name
        if zerod_config_path.resolve() != provenance_path.resolve():
            shutil.copy2(zerod_config_path, provenance_path)
        log["steps"].append(f"{stage_name}_tuned_source_staged")
    _validate_canonical_coupler(stage_dir)
    canonical_results_path = remote_results_dir / "svzerod_3Dcoupling.json"
    shutil.copy2(stage_dir / "svzerod_3Dcoupling.json", canonical_results_path)
    log["steps"].append(f"{stage_name}_canonical_coupler_staged_to_results")
    log["steps"].append(f"{stage_name}_canonical_coupler_validated")

    run_solver_path = stage_dir / "run_solver.sh"
    _normalize_solver_runscript(
        script_path=run_solver_path,
        nodes=int(sim_cfg.get("nodes", 3)),
        procs_per_node=int(sim_cfg.get("procs_per_node", 24)),
        memory_gb=int(sim_cfg.get("memory", 16)),
        hours=int(sim_cfg.get("hours", 20)),
        partition=str(scheduler_defaults.get("partition") or "amarsden"),
        account=str(scheduler_defaults.get("account") or "").strip() or None,
        svfsiplus_path=cluster_svfsiplus_path,
    )

    job_id = _submit_job(run_solver_path)
    return job_id, stage_dir


try:
    poll_seconds = int(threed_config.get("wait_poll_seconds", 30))
    timeout_seconds = int(threed_config.get("wait_timeout_seconds", 43200))
    tuned_config_path: Path | None = None
    tuning_model = str(impedance_config.get("tuning_model", "rri")).strip().lower()
    seed_filename = "full_pa_zerod.json" if tuning_model == "full_pa" else "simplified_nonlinear_zerod.json"
    staged_seed_path = remote_inputs_dir / seed_filename
    if skip_zerod_tuning:
        log["steps"].append("0d_tuning_skipped")
        tuned_config_path = remote_results_dir / "svzerod_3d_coupling_tuned.json"
        decision_payload["tuning_artifacts"].update(
            {
                "optimized_params_csv": str(remote_results_dir / "optimized_params.csv"),
                "pa_config_snapshot": str(remote_results_dir / "pa_config_tuning_snapshot.json"),
                "tuned_zerod_config": str(tuned_config_path),
            }
        )
        required_tuning_artifacts = [
            remote_results_dir / "optimized_params.csv",
            remote_results_dir / "pa_config_tuning_snapshot.json",
            tuned_config_path,
        ]
        missing_tuning_artifacts = [
            str(path) for path in required_tuning_artifacts if not path.exists()
        ]
        if missing_tuning_artifacts:
            _mark_needs_review(
                "skip_zerod_tuning requested but required tuning artifacts are missing: "
                + ", ".join(missing_tuning_artifacts)
            )
            tuned_config_path = None
    elif not staged_seed_path.exists():
        if tuning_model == "full_pa":
            _mark_needs_review(f"staged full-PA 0D seed missing: {staged_seed_path}")
        else:
            log["steps"].append("iteration_seed_generation_started")
            _generate_iteration_seed(staged_seed_path)

    if clinical_targets_path is None:
        _mark_needs_review("clinical targets path is not configured")
    elif not clinical_targets_path.exists():
        _mark_needs_review(f"clinical targets file missing: {clinical_targets_path}")
    elif preop_mesh_complete_path is None:
        _mark_needs_review("preop mesh-complete path is not configured")
    elif not preop_mesh_complete_path.exists():
        _mark_needs_review(f"preop mesh-complete directory missing: {preop_mesh_complete_path}")
    elif skip_zerod_tuning:
        pass
    elif not staged_seed_path.exists():
        _mark_needs_review(f"staged iteration seed missing: {staged_seed_path}")
    else:
        log["steps"].append("0d_tuning_started")
        mesh_surfaces_path = preop_mesh_complete_path / "mesh-surfaces"
        resolved_inflow_path = _resolve_tuning_inflow_path()

        # Seed structured-tree tuning from the previous iteration's optimized params when
        # available (iteration 1 always starts from tune_space.init defaults).
        previous_optimized_params = None
        if iteration > 1:
            prev_iter_label = f"iter-{iteration - 1:02d}"
            _prev_csv = remote_run_dir / "iterations" / prev_iter_label / "results" / "optimized_params.csv"
            if _prev_csv.exists():
                previous_optimized_params = _prev_csv
                log["steps"].append(f"previous_optimized_params_found:{_prev_csv}")
                print(f"[svzt] seeding tuning from previous iteration CSV: {_prev_csv}")
            else:
                log["warnings"].append(
                    f"previous iteration CSV not found ({_prev_csv}); using tune_space.init defaults"
                )
                print(f"[svzt] WARNING: previous iteration CSV not found ({_prev_csv}); using defaults")

        if not mesh_surfaces_path.exists():
            _mark_needs_review(f"mesh-surfaces directory missing: {mesh_surfaces_path}")
        elif bool(impedance_config.get("rescale_inflow")) and resolved_inflow_path is None:
            _mark_needs_review(
                "rescale_inflow=True but no inflow.csv was available for iteration tuning "
                f"(staged={staged_inflow_path}, patient={remote_inflow_path})"
            )
        else:
            tuning = run_impedance_tuning_for_iteration(
                iteration_dir=remote_iter_dir,
                seed_config=staged_seed_path,
                mesh_surfaces=mesh_surfaces_path,
                clinical_targets=clinical_targets_path,
                inflow_path=resolved_inflow_path,
                impedance_config=impedance_config,
                previous_optimized_params=previous_optimized_params,
            )
            decision_payload["tuning_artifacts"].update(
                {
                    "optimized_params_csv": tuning.get("optimized_params_csv"),
                    "stree_optimization_log": tuning.get("stree_optimization_log"),
                    "pa_config_snapshot": tuning.get("pa_config_snapshot"),
                    "tuned_zerod_config": tuning.get("tuned_zerod_config"),
                }
            )
            log["steps"].append("0d_tuning_completed")

        tuned_zerod_raw = decision_payload["tuning_artifacts"].get("tuned_zerod_config")
        tuned_config_path = Path(tuned_zerod_raw) if tuned_zerod_raw else None
        if decision_payload["decision"] == "needs_review":
            tuned_config_path = None
        elif tuned_config_path is None:
            _mark_needs_review("impedance tuning did not return tuned_zerod_config")
        elif not tuned_config_path.exists():
            _mark_needs_review(f"tuned 0D config missing after tuning: {tuned_config_path}")

    if decision_payload["decision"] != "needs_review" and tuned_config_path is not None:
        log["steps"].append("preop_3d_setup_started")

        preop_job_id, preop_dir = _prepare_and_submit_stage(
            stage_name="preop",
            mesh_complete_path=preop_mesh_complete_path,
            zerod_config_path=tuned_config_path,
        )
        log["preop_job_id"] = preop_job_id
        log["steps"].append("preop_submitted")

        ok, terminal = _wait_for_completion(
            preop_job_id,
            poll_seconds=poll_seconds,
            timeout_seconds=timeout_seconds,
        )
        log["preop_terminal_state"] = terminal
        if not ok:
            _mark_needs_review(f"preop simulation did not complete successfully: {terminal}")
        else:
            log["steps"].append("preop_completed")

            pressure_csv = remote_results_dir / "mpa_pressure_vs_time.csv"
            if not pressure_csv.exists():
                try:
                    _generate_mpa_pressure_csv(preop_dir)
                except Exception as exc:
                    _mark_needs_review(f"centerline_pressure_csv_generation_failed: {exc}")
                    log["errors"].append(traceback.format_exc())

            if pressure_csv.exists():
                pressure_cycle_duration = _cycle_duration_from_inflow(
                    staged_inflow_path if staged_inflow_path.exists() else remote_inflow_path
                )
                metrics.update(
                    compute_centerline_mpa_metrics(
                        pressure_csv=pressure_csv,
                        cycle_duration=pressure_cycle_duration,
                    )
                )
                if pressure_cycle_duration is not None:
                    log["steps"].append(
                        f"centerline_metrics_window:last_period:{pressure_cycle_duration}"
                    )
                log["steps"].append("centerline_metrics_loaded")
            else:
                _mark_needs_review(f"centerline pressure CSV missing: {pressure_csv}")

            try:
                metrics.update(compute_flow_split_metrics(simulation_dir=preop_dir))
                log["steps"].append("flow_split_metrics_loaded")
            except Exception as exc:
                _mark_needs_review(f"flow-split extraction failed: {exc}")

            if decision_payload["decision"] != "needs_review":
                if clinical_targets_path and clinical_targets_path.exists() and set(metrics.keys()) >= {"mpa_sys", "mpa_dia", "mpa_mean", "rpa_split"}:
                    gate = evaluate_iteration_gate(metrics=metrics, clinical_targets=clinical_targets_path)
                    decision_payload.update(gate)
                    log["steps"].append("clinical_gate_evaluated")
                else:
                    _mark_needs_review("clinical gate prerequisites missing (targets or required metrics)")

            if decision_payload["decision"] == "not_close":
                tuned_pa_config = decision_payload["tuning_artifacts"].get("pa_config_snapshot")
                try:
                    regen = generate_reduced_pa_from_iteration(
                        iteration_dir=preop_dir if preop_dir.exists() else remote_iter_dir,
                        tuned_pa_config=tuned_pa_config,
                        optimizer="Nelder-Mead",
                        nm_iter=5,
                        output_name="simplified_zerod_tuned_RRI.json",
                        tuning_iter=iteration,
                    )
                    regenerated_path_raw = regen.get("regenerated_config_path")
                    regenerated_path = (
                        Path(regenerated_path_raw)
                        if regenerated_path_raw is not None
                        else None
                    )
                    canonical_seed = remote_results_dir / "simplified_zerod_tuned_RRI.json"
                    if regenerated_path is None or not regenerated_path.exists():
                        _mark_needs_review(
                            f"reduced_pa_regeneration missing output: {regenerated_path_raw}"
                        )
                    else:
                        if regenerated_path.resolve() != canonical_seed.resolve():
                            shutil.copy2(regenerated_path, canonical_seed)
                        decision_payload["regenerated_config_path"] = str(canonical_seed)
                        log["steps"].append("reduced_pa_regenerated")
                except Exception as exc:
                    _mark_needs_review(f"reduced_pa_regeneration_failed: {exc}")
                    log["errors"].append(traceback.format_exc())

            elif decision_payload["decision"] == "converged":
                log["steps"].append("postop_ready_for_explicit_submission")

except Exception as exc:
    _mark_needs_review(f"iteration_driver_unhandled_error: {exc}")
    log["errors"].append(traceback.format_exc())

metrics_payload = {
    "run_id": run_id,
    "iteration": iteration,
    "clinical_targets": str(clinical_targets_path) if clinical_targets_path else None,
    "centerline": str(centerline_path) if centerline_path else None,
    "preop_mesh_complete": str(preop_mesh_complete_path) if preop_mesh_complete_path else None,
    "postop_mesh_complete": str(postop_mesh_complete_path) if postop_mesh_complete_path else None,
    "preop_job_id": log.get("preop_job_id"),
    "preop_terminal_state": log.get("preop_terminal_state"),
    **metrics,
}

decision_payload.update(
    {
        "run_id": run_id,
        "iteration": iteration,
    }
)

write_iteration_metrics(metrics_path, metrics_payload)
write_iteration_decision(decision_path, decision_payload)

with (remote_logs_dir / "iteration_driver_log.json").open("w", encoding="utf-8") as stream:
    json.dump(log, stream, indent=2, sort_keys=True)

print(f"[svzt] iteration metrics written: {metrics_path}")
print(f"[svzt] iteration decision written: {decision_path}")
PY
