#!/usr/bin/env bash

#SBATCH --partition={{SBATCH_PARTITION}}
#SBATCH --time={{SBATCH_TIME}}
#SBATCH --mem={{SBATCH_MEM}}
#SBATCH --cpus-per-task={{SBATCH_CPUS}}
{{SBATCH_NODES_DIRECTIVE}}
{{SBATCH_TASKS_DIRECTIVE}}
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
  else
    echo "[svzt] error: no Python interpreter found on PATH" >&2
    exit 1
  fi
fi

echo "[svzt] python_bin=${PYTHON_BIN}"
"${PYTHON_BIN}" - <<'PY' || exit $?
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
    "run_rcr_tuning_for_iteration",
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
# The rendered impedance config must be fully understood by this svZeroDTrees
# install; an older install would otherwise silently ignore new controls.
import json as _json
if ("{{TUNING_BC_TYPE}}".strip().lower() or "impedance") == "impedance":
    _rendered = _json.loads(r'''{{ZEROD_TUNING_CONFIG_JSON}}''')
    _supported = getattr(tuning, "SUPPORTED_IMPEDANCE_KEYS", None)
    if _supported is not None:
        _unknown = sorted(set(_rendered) - set(_supported))
    else:
        _new_policies = {"precapillary_fraction", "diastolic_offset"}
        _unknown = sorted(
            key
            for key in ("objective", "proximal_compliance", "polish", "tree_max_nodes", "leaf_resistance")
            if _rendered.get(key) is not None
        )
        if _rendered.get("keep_diastolic_target"):
            _unknown.append("keep_diastolic_target")
        if _rendered.get("wedge_pressure_policy") in _new_policies:
            _unknown.append("wedge_pressure_policy=" + str(_rendered["wedge_pressure_policy"]))
    if _unknown:
        print(
            "[svzt] error: this svZeroDTrees install does not support impedance tuning "
            "settings " + ", ".join(_unknown) + "; update svZeroDTrees on the cluster",
            file=sys.stderr,
        )
        sys.exit(8)
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
# 5) branch to reduced-PA regeneration (skipped under calibrated_full_pa) or postop submission intent
"${PYTHON_BIN}" - <<'PY'
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import csv
import heapq
import json
import math
import os
import re
import shutil
import signal
import subprocess
import sys
import time
import traceback
import uuid
import xml.etree.ElementTree as ET

import numpy as np
import vtk

import svzerodtrees.config as svzerodtrees_config
import svzerodtrees.tuning as svzerodtrees_tuning
from svzerodtrees.simulation import Simulation, SimulationDirectory
from svzerodtrees.post_processing import write_mpa_pressure_timeseries_csv
from svzerodtrees.tuning import (
    compute_centerline_mpa_metrics,
    compute_flow_split_metrics,
    evaluate_iteration_gate,
    generate_reduced_pa_from_iteration,
    run_impedance_tuning_for_iteration,
    run_rcr_tuning_for_iteration,
    summarize_pulmonary_zerod_config,
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
solver_execution = dict(threed_config.get("execution", {}))
solver_execution["mode"] = "slurm"
solver_execution["executable"] = cluster_svfsiplus_path
solver_execution["submit_command"] = "bash"
solver_execution["svfsiplus_path"] = cluster_svfsiplus_path
threed_config["execution"] = solver_execution
tuning_bc_type = "{{TUNING_BC_TYPE}}".strip().lower() or "impedance"
zerod_tuning_config = json.loads(r'''{{ZEROD_TUNING_CONFIG_JSON}}''')
# Iteration gate: with the likelihood objective, accept metrics within
# target_sigma measurement standard deviations instead of 10% relative (which
# would demand ~0.3 mmHg on a 3 mmHg diastolic target).
_gate_objective = zerod_tuning_config.get("objective") or {}
gate_kwargs = {}
if _gate_objective.get("type") == "likelihood" and _gate_objective.get("target_sigma") is not None:
    gate_kwargs = {
        "sigma": {
            "pressure_mmhg": float(_gate_objective.get("pressure_sigma_mmhg") or 2.0),
            "split": float(_gate_objective.get("split_sigma") or 0.02),
        },
        "sigma_multiple": float(_gate_objective["target_sigma"]),
    }
iteration1_seed_config = json.loads(r'''{{ITERATION1_SEED_CONFIG_JSON}}''')
skip_zerod_tuning = json.loads(r'''{{SKIP_ZEROD_TUNING_JSON}}''')
# Reuse this iteration's completed preop 3D result (recorded as COMPLETED by
# the previous driver run, with result VTUs present) and rerun only the
# post-3D steps.  Missing evidence is needs_review, never a silent resubmit.
reuse_preop_3d = json.loads(r'''{{REUSE_PREOP_3D_JSON}}''')
# Split driver: the pre-3D job (default) submits the preop 3D run, then this
# same script as a post-3D job (--dependency=afterany:<3D>,
# SVZT_DRIVER_PHASE=post3d) and exits instead of holding an allocation while the
# 3D run waits and runs.  The post-3D job reuses the 3D result like
# --reuse-preop-3d, with the 3D job's scheduler state as the evidence.
DRIVER_HANDOFF_FILENAME = "iteration_handoff.json"
driver_phase = (os.environ.get("SVZT_DRIVER_PHASE") or "pre3d").strip().lower()
if driver_phase not in {"pre3d", "post3d"}:
    raise SystemExit(f"[svzt] unknown SVZT_DRIVER_PHASE={driver_phase!r}")
if driver_phase == "post3d":
    reuse_preop_3d = True
    skip_zerod_tuning = True
handed_off_to_post3d = False
# How solver stages run (defaults.execution.nested_submission):
# - "sbatch": each stage is its own Slurm job, and the pre-3D driver hands off
#   to a post-3D driver job (above).
# - "in_allocation": this job holds the preop 3D node/task shape and runs each
#   stage with srun inside it (TACC forbids sbatch from compute nodes).  There
#   is no post-3D handoff, the stage "job ID" is this job's ID, and the
#   postprocess/calibration jobs are left to the agent, which submits them from
#   the login node.
nested_submission = "{{NESTED_SUBMISSION}}".strip().lower()
if nested_submission not in {"sbatch", "in_allocation"}:
    raise SystemExit(f"[svzt] unknown nested_submission={nested_submission!r}")
in_allocation = nested_submission == "in_allocation"
if in_allocation and driver_phase != "pre3d":
    raise SystemExit("[svzt] in_allocation drivers have no post-3D phase")
# Next-seed generation strategy chosen by the agent's seed policy:
# - "reduced_rri": regenerate simplified_zerod_tuned_RRI.json inline on not_close.
# - "calibrated_full_pa": after not_close or converged, submit the pre-rendered
#   preop postprocess job and the dependent (afterok) full-PA calibration job,
#   and report both job IDs in iteration_decision.json. The agent validates and
#   promotes the calibrated model; it seeds the next iteration or postop.
seed_generation = json.loads(r'''{{SEED_GENERATION_JSON}}''')
seed_strategy = str(seed_generation.get("strategy") or "reduced_rri")
regenerate_reduced_seed = seed_strategy != "calibrated_full_pa"
mesh_scale_factor = float("{{MESH_SCALE_FACTOR}}")
scheduler_defaults = {
    "account": "{{SBATCH_ACCOUNT}}",
    "partition": "{{SBATCH_PARTITION}}",
    "wall_time": "{{SBATCH_TIME}}",
    "mem": "{{SBATCH_MEM}}",
}

metrics_path = remote_results_dir / "iteration_metrics.json"
decision_path = remote_results_dir / "iteration_decision.json"
# Run-scoped preop cap renames (source file -> staged name); empty for most patients.
preop_mesh_surface_aliases = json.loads(r'''{{PREOP_MESH_SURFACE_ALIASES_JSON}}''')
# mesh_scale_factor != 1: the patient's geometry paths above already point at
# copies scaled to the true anatomy under runs_root; written once, here, before
# any stage reads them (empty for unit-scale patients).
geometry_scaling = json.loads(r'''{{GEOMETRY_SCALING_JSON}}''')

log = {
    "run_id": run_id,
    "iteration": iteration,
    "steps": [],
    "errors": [],
    "warnings": [],
    "preop_job_id": None,
    "postop_job_id": None,
    "prestress_job_id": None,
    "prestress_mean_steady_job_id": None,
    "prestress_file_path": None,
    "prestress_traction_source": None,
    "preop_terminal_state": None,
}

# After a NODE_FAIL, Slurm requeues this job: a new attempt starts under the
# same SLURM_JOB_ID (SLURM_RESTART_COUNT + 1) while the first attempt can keep
# running on the failed node, cut off from the controller.  Each attempt claims
# the job's attempt file at start.  An attempt that no longer owns it exits
# before any sbatch and before writing the driver log, handoff, metrics or
# decision, so a stale attempt cannot overwrite the requeued one's results.
DRIVER_SUPERSEDED_EXIT_CODE = 9
driver_attempt = {
    "job_id": os.environ.get("SLURM_JOB_ID") or "local",
    "restart_count": int(os.environ.get("SLURM_RESTART_COUNT") or 0),
    "phase": driver_phase,
    "node": os.environ.get("SLURMD_NODENAME") or os.uname().nodename,
    "pid": os.getpid(),
    "started_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    "nonce": uuid.uuid4().hex,
}
driver_attempt_path = remote_logs_dir / f"iteration_driver_attempt_{driver_attempt['job_id']}.json"
log["driver_attempt"] = driver_attempt


def _claim_driver_attempt() -> None:
    """Record this attempt as the owner of its job's driver writes (atomic replace)."""
    driver_attempt_path.parent.mkdir(parents=True, exist_ok=True)
    staging = driver_attempt_path.with_name(f"{driver_attempt_path.name}.{driver_attempt['nonce']}.tmp")
    staging.write_text(json.dumps(driver_attempt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(staging, driver_attempt_path)


def _exit_if_superseded(action: str) -> None:
    """Exit without writing when the attempt file no longer names this attempt."""
    try:
        owner = json.loads(driver_attempt_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        owner = {"error": f"{type(exc).__name__}: {exc}"}
    if isinstance(owner, dict) and owner.get("nonce") == driver_attempt["nonce"]:
        return
    print(
        f"[svzt] driver attempt {driver_attempt['restart_count']} of job {driver_attempt['job_id']} "
        f"(started {driver_attempt['started_at']} on {driver_attempt['node']}) no longer owns "
        f"{driver_attempt_path}: {json.dumps(owner, sort_keys=True)}; "
        f"exiting before {action} without writing driver results",
        file=sys.stderr,
        flush=True,
    )
    raise SystemExit(DRIVER_SUPERSEDED_EXIT_CODE)


def _handoff_from_earlier_job(handoff_path: Path) -> bool:
    """True when handoff_path was left by an earlier tune job, not an attempt of this one."""
    try:
        handoff = json.loads(handoff_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return False
    except (OSError, json.JSONDecodeError):
        return True
    return str(handoff.get("pre3d_job_id")) != driver_attempt["job_id"]


def _write_driver_results(metrics_payload: dict) -> None:
    """Write the driver log, then the post-3D handoff or the metrics and decision."""
    _exit_if_superseded("writing the driver log, handoff, metrics and decision")
    handoff_path = remote_results_dir / DRIVER_HANDOFF_FILENAME
    handing_off = handed_off_to_post3d and decision_payload["decision"] != "needs_review"
    # A pre-3D decision clears a handoff left by an earlier tune job.  It keeps
    # one naming this job: another attempt of it queued that post-3D driver.
    remove_handoff = False
    if not handing_off and driver_phase == "pre3d" and handoff_path.exists():
        remove_handoff = _handoff_from_earlier_job(handoff_path)
        if not remove_handoff:
            log["warnings"].append(
                f"kept {handoff_path}: another attempt of job {driver_attempt['job_id']} "
                "handed off to a post-3D driver"
            )

    with (remote_logs_dir / "iteration_driver_log.json").open("w", encoding="utf-8") as stream:
        json.dump(log, stream, indent=2, sort_keys=True)

    if handing_off:
        # The post-3D job writes the decision; the agent follows this handoff.
        handoff = {
            "run_id": run_id,
            "iteration": iteration,
            "pre3d_job_id": os.environ.get("SLURM_JOB_ID"),
            "preop_job_id": log.get("preop_job_id"),
            "post3d_job_id": log.get("post3d_job_id"),
        }
        handoff_path.write_text(json.dumps(handoff, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(f"[svzt] 3D run {handoff['preop_job_id']} handed off to post-3D driver {handoff['post3d_job_id']}")
        return
    if remove_handoff:
        handoff_path.unlink()
    write_iteration_metrics(metrics_path, metrics_payload)
    write_iteration_decision(decision_path, decision_payload)
    print(f"[svzt] iteration metrics written: {metrics_path}")
    print(f"[svzt] iteration decision written: {decision_path}")


def _stage_aliased_mesh_complete(source: Path, aliases: dict) -> Path:
    """Symlinked run-scoped copy of mesh-complete with renamed caps.

    Patient data stays read-only: every entry is a symlink to the original,
    and only the link names of aliased caps differ.  Re-running reuses the
    staged copy and fails on any link that points elsewhere.
    """
    source_surfaces = source / "mesh-surfaces"
    missing = sorted(name for name in aliases if not (source_surfaces / name).is_file())
    if missing:
        raise RuntimeError(f"mesh surface aliases name missing caps: {missing}")
    clashes = sorted(
        target for target in aliases.values() if (source_surfaces / target).exists()
    )
    if clashes:
        raise RuntimeError(f"mesh surface alias targets already exist: {clashes}")
    staged = remote_run_dir / "staged_inputs" / "preop-mesh-complete"
    staged_surfaces = staged / "mesh-surfaces"
    staged_surfaces.mkdir(parents=True, exist_ok=True)

    def _link(link: Path, target: Path) -> None:
        if link.is_symlink() or link.exists():
            if link.resolve() != target.resolve():
                raise RuntimeError(f"staged mesh entry {link} does not point to {target}")
            return
        link.symlink_to(target)

    for item in source.iterdir():
        if item.name != "mesh-surfaces":
            _link(staged / item.name, item)
    for item in source_surfaces.iterdir():
        _link(staged_surfaces / aliases.get(item.name, item.name), item)
    log["steps"].append(
        "preop_mesh_surfaces_aliased:"
        + ",".join(f"{src}->{dst}" for src, dst in sorted(aliases.items()))
    )
    return staged


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
        "zerod_pre_mapping_metrics": None,
    },
    "postop_submission_requested": False,
    "postop_job_id": None,
    "needs_review_reason": None,
}

if tuning_bc_type not in {"impedance", "rcr"}:
    raise RuntimeError(f"Unsupported tuning bc_type: {tuning_bc_type}")


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


def _seed_generation_simulation(seed_workspace: Path, **simulation_kwargs) -> Simulation:
    if preop_mesh_complete_path is None:
        raise RuntimeError("preop mesh-complete path is not configured")
    if clinical_targets_path is None:
        raise RuntimeError("clinical targets path is not configured")
    if not preop_mesh_complete_path.exists():
        raise RuntimeError(f"preop mesh-complete directory missing: {preop_mesh_complete_path}")
    if not clinical_targets_path.exists():
        raise RuntimeError(f"clinical targets file missing: {clinical_targets_path}")

    preop_dir = seed_workspace / "preop"
    postop_dir = seed_workspace / "postop"
    preop_dir.mkdir(parents=True, exist_ok=True)
    postop_dir.mkdir(parents=True, exist_ok=True)

    mesh_complete_target = preop_dir / "mesh-complete"
    _link_or_copy_directory(preop_mesh_complete_path, mesh_complete_target)

    return Simulation(
        path=str(seed_workspace),
        clinical_targets=str(clinical_targets_path),
        preop_dir="preop",
        postop_dir="postop",
        adapted_dir=None,
        inflow_path=str(remote_inflow_path) if remote_inflow_path is not None and remote_inflow_path.exists() else None,
        solver_paths=threed_config.get("solver_paths"),
        execution_config=threed_config.get("execution"),
        **simulation_kwargs,
    )


def _generate_iteration_seed(seed_path: Path) -> None:
    seed_workspace = remote_iter_dir / "seed_generation"
    sim = _seed_generation_simulation(seed_workspace)
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


def _generate_learned_iteration_seed(seed_path: Path) -> None:
    if iteration != 1:
        raise RuntimeError(
            "learned-zerod seed generation is supported only for iteration 1"
        )
    if centerline_path is None or not centerline_path.exists():
        raise RuntimeError(
            f"learned-zerod seed generation requires centerline input: {centerline_path}"
        )

    learned_config_type = getattr(
        svzerodtrees_config, "LearnedSeedGenerationConfig", None
    )
    learned_generator = getattr(
        svzerodtrees_tuning, "generate_full_pa_learned_seed", None
    )
    if learned_config_type is None or learned_generator is None:
        raise RuntimeError(
            "source=learned_zerod requires a svZeroDTrees installation exposing "
            "LearnedSeedGenerationConfig and generate_full_pa_learned_seed"
        )

    seed_workspace = remote_iter_dir / "seed_generation"
    source_seed_path = seed_workspace / "learned" / "source_0d_config.json"
    configured_source = str(iteration1_seed_config.get("path", "")).strip()
    if not configured_source:
        raise RuntimeError(
            "source=learned_zerod requires a configured input 0D JSON path"
        )
    configured_source_path = Path(configured_source).expanduser()
    if not configured_source_path.is_file():
        raise RuntimeError(
            "source=learned_zerod input 0D JSON is missing or unreadable: "
            f"{configured_source_path}"
        )
    source_seed_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        shutil.copy2(configured_source_path, source_seed_path)
    except OSError as exc:
        raise RuntimeError(
            "source=learned_zerod input 0D JSON could not be staged: "
            f"{configured_source_path}"
        ) from exc

    try:
        source_config = json.loads(source_seed_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            "source=learned_zerod input 0D JSON could not be parsed after staging: "
            f"{source_seed_path}"
        ) from exc
    if not isinstance(source_config, dict):
        raise RuntimeError(
            "source=learned_zerod input 0D JSON must contain a JSON object: "
            f"{source_seed_path}"
        )

    normalized_internal_junctions = 0
    junctions = source_config.get("junctions", [])
    if not isinstance(junctions, list):
        raise RuntimeError(
            "source=learned_zerod input 0D JSON has a non-list junctions field: "
            f"{source_seed_path}"
        )
    for junction in junctions:
        if not isinstance(junction, dict):
            raise RuntimeError(
                "source=learned_zerod input 0D JSON has a non-object junction: "
                f"{source_seed_path}"
            )
        junction_type = junction.get("junction_type")
        if (
            isinstance(junction_type, str)
            and junction_type.lower() == "internal_junction"
        ):
            junction["junction_type"] = "NORMAL_JUNCTION"
            normalized_internal_junctions += 1
    if normalized_internal_junctions:
        source_seed_path.write_text(
            json.dumps(source_config, indent=2) + "\n", encoding="utf-8"
        )
        log["learned_seed_normalized_internal_junctions"] = (
            normalized_internal_junctions
        )

    learned_output_dir = seed_workspace / "learned"
    generated = learned_generator(
        learned_config_type(
            method="learned_zerod",
            anatomy="pulmonary",
            input_zerod_config=str(source_seed_path),
            centerline=str(centerline_path),
            svzerodsolver=str(
                iteration1_seed_config.get(
                    "svzerodsolver_executable", "svzerodsolver"
                )
            ),
            output_dir=str(learned_output_dir),
            learned_zerod_executable=str(
                iteration1_seed_config.get(
                    "learned_zerod_executable", "learned-zerod"
                )
            ),
            output_filename="full_pa_zerod.json",
            keep_tmp=False,
        )
    )
    generated_seed_path = Path(generated.seed_path)
    if not generated_seed_path.exists():
        raise RuntimeError(
            "svZeroDTrees learned seed generation did not write "
            f"{generated_seed_path}"
        )
    seed_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(generated_seed_path, seed_path)
    log["learned_seed_path"] = str(generated_seed_path)
    log["learned_seed_metadata_path"] = str(generated.metadata_path)
    log["steps"].append("learned_iteration_seed_generated")


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
    mail_user: str | None,
    mail_types: list[str] | None,
    svfsiplus_path: str,
    explicit_task_count: bool = False,
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
    if mail_user:
        header.append(f"#SBATCH --mail-user={mail_user}")
        for mail_type in mail_types or ["begin", "end"]:
            header.append(f"#SBATCH --mail-type={mail_type}")

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
    if explicit_task_count:
        # Run inside the driver's allocation: srun would otherwise launch one
        # task per allocated slot (e.g. 48 for a 1-process prestress solve).
        rendered += (
            f"srun --nodes={nodes} --ntasks={nodes * procs_per_node} "
            f"{svfsiplus_path} svFSIplus.xml\n"
        )
    else:
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


def _seed_generation_mean_result_dirs(seed_mean_dir: Path) -> list[Path]:
    return [
        path
        for path in sorted(seed_mean_dir.glob("*-procs"))
        if path.is_dir() and _result_vtus(path)
    ]


def _run_seed_generation_mean_steady_sim(seed_mean_dir: Path) -> None:
    """Run the rigid mean-flow steady 3D solve that prestress tractions need.

    The reduced-seed path (source=generate) runs it as part of its sys/dia/mean
    steady solves; learned-zerod and path seeds do not, so prestress
    generation runs only the mean solve here, in the same directory, with the
    same inflow and mesh-complete.  Submission goes through the iteration
    script's scheduler helpers, as for the prestress solve.
    """
    seed_workspace = remote_run_dir / "iterations" / "iter-01" / "seed_generation"
    sim = _seed_generation_simulation(seed_workspace, mesh_scale_factor=mesh_scale_factor)
    mean_flow = float(np.mean(np.asarray(sim.inflow.q, dtype=float)))
    seed_mean_dir.mkdir(parents=True, exist_ok=True)
    steady_sim = SimulationDirectory.from_directory(
        path=str(seed_mean_dir),
        mesh_complete=sim.preop_dir.mesh_complete.path,
        convert_to_cm=sim.convert_to_cm,
        mesh_scale_factor=mesh_scale_factor,
    )
    steady_sim.solver_paths = threed_config.get("solver_paths")
    previous_cwd = os.getcwd()
    os.chdir(seed_mean_dir)
    try:
        steady_sim.generate_steady_sim(
            flow_rate=mean_flow, execution_config=threed_config.get("execution")
        )
    finally:
        os.chdir(previous_cwd)
    log["steps"].append(f"prestress_mean_steady_sim_generated:flow={mean_flow:.6g}")

    run_solver_path = seed_mean_dir / "run_solver.sh"
    if not run_solver_path.exists():
        raise RuntimeError(f"mean steady simulation did not write {run_solver_path}")
    _normalize_solver_runscript(
        script_path=run_solver_path,
        nodes=int(threed_config.get("nodes", 2)),
        procs_per_node=int(threed_config.get("procs_per_node", 24)),
        memory_gb=int(threed_config.get("memory", 16)),
        hours=6,
        partition=str(scheduler_defaults.get("partition") or "amarsden"),
        account=str(scheduler_defaults.get("account") or "").strip() or None,
        mail_user=_resolve_slurm_mail_user(threed_config),
        mail_types=_resolve_slurm_mail_types(threed_config),
        svfsiplus_path=cluster_svfsiplus_path,
        explicit_task_count=in_allocation,
    )
    steady_job_id = _start_solver_job(run_solver_path)
    log["prestress_mean_steady_job_id"] = steady_job_id
    log["steps"].append("prestress_mean_steady_sim_submitted")
    ok, terminal = _wait_for_solver_job(
        steady_job_id,
        run_solver_path,
        poll_seconds=int(threed_config.get("wait_poll_seconds", 30)),
        timeout_seconds=int(threed_config.get("wait_timeout_seconds", 43200)),
    )
    if not ok:
        raise RuntimeError(f"mean steady simulation did not complete successfully: {terminal}")
    log["steps"].append("prestress_mean_steady_sim_completed")


def _seed_generation_mean_result_dir() -> Path:
    seed_mean_dir = remote_run_dir / "iterations" / "iter-01" / "seed_generation" / "steady" / "mean"
    result_dirs = _seed_generation_mean_result_dirs(seed_mean_dir)
    if not result_dirs:
        _run_seed_generation_mean_steady_sim(seed_mean_dir)
        result_dirs = _seed_generation_mean_result_dirs(seed_mean_dir)
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


def _get_wall_nodes(path: Path) -> np.ndarray:
    reader = vtk.vtkXMLPolyDataReader()
    reader.SetFileName(str(path))
    reader.Update()
    model = reader.GetOutput()
    model_id = model.GetPointData().GetArray("GlobalNodeID")
    npts = model.GetNumberOfPoints()
    ids = np.zeros((npts, 1), dtype="int32")
    for i in range(npts):
        ids[i] = int(model_id.GetTuple1(i))
    return ids


def _load_vtu(path: Path):
    reader = vtk.vtkXMLUnstructuredGridReader()
    reader.SetFileName(str(path))
    reader.Update()
    return reader.GetOutput()


def _get_surface_data(mesh, surface_ids: np.ndarray, field: str) -> np.ndarray:
    data = mesh.GetPointData().GetArray(field)
    ncomp = data.GetNumberOfComponents()
    n = np.size(surface_ids)
    out = np.zeros((n, ncomp)) if ncomp > 1 else np.zeros((n,))
    for i in range(n):
        tup = data.GetTuple(int(surface_ids[i]) - 1)
        if ncomp == 1:
            out[i] = tup[0]
        else:
            for j in range(ncomp):
                out[i, j] = tup[j]
    return out


def _write_wall_field(values: np.ndarray, *, out_path: Path, wall_path: Path, name: str) -> None:
    reader = vtk.vtkXMLPolyDataReader()
    reader.SetFileName(str(wall_path))
    reader.Update()
    model = reader.GetOutput()
    npts = model.GetNumberOfPoints()

    arr = vtk.vtkDoubleArray()
    if values.ndim > 1:
        arr.SetNumberOfComponents(values.shape[1])
        arr.SetNumberOfTuples(npts)
        arr.SetName(name)
        for i in range(npts):
            arr.SetTuple3(i, values[i, 0], values[i, 1], values[i, 2])
    else:
        arr.SetNumberOfComponents(1)
        arr.SetNumberOfTuples(npts)
        arr.SetName(name)
        for i in range(npts):
            arr.SetTuple1(i, values[i])

    model.GetPointData().AddArray(arr)
    writer = vtk.vtkXMLPolyDataWriter()
    writer.SetInputData(model)
    writer.SetFileName(str(out_path))
    writer.Write()


def _write_mean_wall_traction_and_pressure(
    *,
    result_dir: Path,
    wall_path: Path,
    start: int,
    stop: int,
    stride: int,
    traction_path: Path,
    pressure_path: Path,
) -> None:
    wall_ids = _get_wall_nodes(wall_path)
    n_nodes = np.size(wall_ids)

    mean_t = np.zeros((n_nodes, 3), dtype="float64")
    mean_p = np.zeros((n_nodes,), dtype="float64")

    steps = list(range(start, stop + 1, stride))
    if not steps:
        raise RuntimeError("No timesteps selected. Check start/stop/stride.")

    for step in steps:
        vtu = result_dir / f"result_{step:04d}.vtu"
        if not vtu.exists():
            vtu = result_dir / f"result_{step:03d}.vtu"
        if not vtu.exists():
            raise FileNotFoundError(f"Missing result file for step {step}: {vtu}")

        mesh = _load_vtu(vtu)
        mean_t += _get_surface_data(mesh, wall_ids, "Traction")
        mean_p += _get_surface_data(mesh, wall_ids, "Pressure")

    mean_t /= float(len(steps))
    mean_p /= float(len(steps))

    _write_wall_field(mean_t, out_path=traction_path, wall_path=wall_path, name="Traction")
    _write_wall_field(mean_p, out_path=pressure_path, wall_path=wall_path, name="Pressure")


def _preop_dir_job_id(preop_dir: Path) -> str | None:
    """Job ID of the 3D run whose solver log (svFlowSolver.o<id>) is in preop_dir, newest first."""
    prefix = "svFlowSolver.o"
    solver_logs = [path for path in preop_dir.glob(f"{prefix}*") if path.name[len(prefix):].isdigit()]
    if not solver_logs:
        return None
    return max(solver_logs, key=lambda path: path.stat().st_mtime).name[len(prefix):]


def _completed_preop_job_for_reuse(preop_dir: Path) -> tuple[str | None, str | None]:
    """Job ID of this iteration's completed preop 3D run, or the reason it is unusable.

    Evidence: the previous driver log records the preop job as COMPLETED with
    a preop_completed step, and preop_dir holds result VTUs.  When preop_dir's
    own solver log names a different job (a later 3D run whose driver ended
    before recording it), that job's scheduler state is the evidence instead.
    """
    previous_log_path = remote_logs_dir / "iteration_driver_log.json"
    try:
        previous_log = json.loads(previous_log_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return None, f"previous driver log unreadable: {previous_log_path} ({exc})"
    job_id = previous_log.get("preop_job_id")
    log_job_id = job_id
    terminal = str(previous_log.get("preop_terminal_state") or "")
    dir_job_id = _preop_dir_job_id(preop_dir) if driver_phase != "post3d" else None
    if driver_phase == "post3d":
        if not job_id or "post3d_driver_submitted" not in " ".join(previous_log.get("steps") or []):
            return None, f"pre-3D driver log does not record a 3D handoff (job={job_id})"
        state, _source = _query_state(str(job_id))
        log["preop_terminal_state"] = state
        if state != "COMPLETED":
            return None, f"preop simulation did not complete successfully: {state or 'unknown'}"
    elif dir_job_id is not None and dir_job_id != str(job_id or ""):
        state, _source = _query_state(dir_job_id)
        if state != "COMPLETED":
            return None, (
                f"preop 3D job {dir_job_id} in {preop_dir} did not complete successfully: "
                f"{state or 'unknown'}"
            )
        log["warnings"].append(
            f"reuse_preop_3d: previous driver log names preop job {job_id}; "
            f"reusing job {dir_job_id} from its solver log in {preop_dir}"
        )
        job_id = dir_job_id
    elif not job_id or terminal.upper() != "COMPLETED" or "preop_completed" not in (previous_log.get("steps") or []):
        return None, (
            f"previous driver log does not record a completed preop job "
            f"(job={job_id}, state={terminal or None})"
        )
    if _latest_result_vtu(preop_dir) is None:
        return None, f"no preop result VTUs under {preop_dir}"
    # This run overwrites the driver log; keep it under the job it records.
    kept_name = "pre3d" if driver_phase == "post3d" else "preop"
    shutil.copy2(previous_log_path, remote_logs_dir / f"iteration_driver_log.{kept_name}_{log_job_id}.json")
    return str(job_id), None


def _older_than_3d_result(path: Path, preop_dir: Path) -> bool:
    """True when ``path`` predates the preop 3D result it is derived from.

    A rerun of the same iteration (--skip-zerod-tuning, --reuse-preop-3d)
    keeps results/; the pressure CSV from an earlier 3D run must not stand in
    for the new one (TST-STAN-3/5/9 kept their 2-cycle CSVs, 2026-10-14).
    """
    evidence = preop_dir / "svZeroD_data"
    if not evidence.exists():
        evidence = _latest_result_vtu(preop_dir)
    if evidence is None or not evidence.exists():
        return False
    stale = path.stat().st_mtime < evidence.stat().st_mtime
    if stale:
        log["steps"].append(f"stale_pressure_csv_regenerated:{path.name}")
    return stale


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
    traction_file = prestress_dir / "rigid_wall_mean_traction.vtp"
    pressure_file = prestress_dir / "rigid_wall_mean_pressure.vtp"
    _write_mean_wall_traction_and_pressure(
        result_dir=mean_result_dir,
        wall_path=wall_path,
        start=start,
        stop=stop,
        stride=stride,
        traction_path=traction_file,
        pressure_path=pressure_file,
    )
    if not traction_file.exists():
        raise RuntimeError(f"mean wall traction helper did not write {traction_file}")
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
        "execution": threed_config.get("execution"),
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
        mail_user=_resolve_slurm_mail_user(prestress_config),
        mail_types=_resolve_slurm_mail_types(prestress_config),
        svfsiplus_path=cluster_svfsiplus_path,
        explicit_task_count=in_allocation,
    )

    prestress_job_id = _start_solver_job(run_solver_path)
    log["prestress_job_id"] = prestress_job_id
    log["steps"].append("prestress_submitted")

    ok, terminal = _wait_for_solver_job(
        prestress_job_id,
        run_solver_path,
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


MATCH_PROXIMAL_COMPLIANCE = "match_proximal_compliance"


def _resolve_threed_elasticity_modulus(diagnostics_path_raw) -> None:
    """Set threed_config E from the tuned seed when it asks to match the 0D.

    elasticity_modulus = "match_proximal_compliance" gives the uniform 3D wall
    the seed's total proximal compliance: E = matched_uniform_wall_eh / h from
    this iteration's tuning_diagnostics.json.  Runs before any deformable 3D
    setup (preop and prestress) so every stage uses the same E.
    """
    if threed_config.get("elasticity_modulus") != MATCH_PROXIMAL_COMPLIANCE:
        return
    from svzerodtrees.tune_bcs.tuning_diagnostics import matched_wall_elasticity_modulus

    try:
        if not diagnostics_path_raw:
            raise ValueError("no tuning_diagnostics.json for this iteration")
        record = matched_wall_elasticity_modulus(
            diagnostics_path_raw, threed_config["shell_thickness"]
        )
    except Exception as exc:
        _mark_needs_review(f"threed_elasticity_modulus_unresolved: {exc}")
        return
    threed_config["elasticity_modulus"] = record["elasticity_modulus"]
    log["threed_elasticity_modulus"] = {"source": MATCH_PROXIMAL_COMPLIANCE, **record}
    log["steps"].append(f"threed_elasticity_modulus_matched:{record['elasticity_modulus']:.6g}")


def _carry_forward_outlet_mapping(config: dict) -> dict:
    """Reuse the previous iteration's validated cap-to-outlet mapping.

    A later full-PA iteration tunes the promoted calibrated model, which keeps
    the outlets and BC names (R-only calibration) but not the learned seed's
    centerline_node_ids, so the centerline strategy cannot re-derive the
    pairing.  The previous iteration's outlet_cap_mapping.json is that pairing,
    already validated against these caps; it is passed as an explicit mapping
    (the centerline stays, for proximal-compliance branch lengths).  An
    explicitly configured mapping or mode is left alone.
    """
    mode = str(config.get("outlet_mapping_mode") or "auto").strip().lower()
    if iteration <= 1 or config.get("outlet_mapping") is not None or mode not in {"auto", "centerline"}:
        return config
    previous = (
        remote_run_dir / "iterations" / f"iter-{iteration - 1:02d}" / "results" / "outlet_cap_mapping.json"
    )
    if not previous.exists():
        log["warnings"].append(f"outlet_mapping_not_carried_forward: {previous} missing")
        return config
    payload = json.loads(previous.read_text(encoding="utf-8"))
    pairs = [
        {"cap_name": Path(str(pair["cap_path"])).stem, "bc_name": str(pair["bc_name"])}
        for pair in payload.get("pairs") or []
    ]
    if not pairs:
        log["warnings"].append(f"outlet_mapping_not_carried_forward: no pairs in {previous}")
        return config
    carried = dict(config)
    carried["outlet_mapping_mode"] = "explicit"
    carried["outlet_mapping"] = pairs
    log["outlet_mapping_carried_forward"] = {
        "source": str(previous),
        "source_strategy": payload.get("strategy"),
        "pairs": len(pairs),
    }
    log["steps"].append(f"outlet_mapping_carried_forward:iter-{iteration - 1:02d}")
    return carried


def _zerod_periodicity_warnings(periodicity) -> list[str]:
    """Warnings when the 0D pre-mapping metrics may come from a transient cycle.

    svZeroDTrees reports ``periodicity`` (cycles simulated, final
    cycle-to-cycle MPA drift) when it ran the snapshot until periodic; older
    installs score the snapshot's own cycle count and report none.
    """
    if periodicity is None:
        return [
            "zerod_pre_mapping_metrics: svZeroDTrees did not report periodicity; "
            "metrics come from the snapshot's own cycle count and may be transient"
        ]
    if not periodicity.get("converged"):
        return [
            "zerod_pre_mapping_metrics: MPA pressure not periodic after "
            f"{periodicity.get('cycles_used')} cycles "
            f"(drift {periodicity.get('final_drift_mmhg')} mmHg)"
        ]
    return []


def _record_threed_wall_match(diagnostics_path_raw) -> None:
    """Compare the deformable 3D wall with the tuned 0D proximal compliance.

    svZeroDTrees reports the E*h of a uniform wall with the same total
    compliance as the seed's proximal compliance; a 3D wall more than 2x away
    from it gives the 3D-coupled model a different proximal compliance than
    the tuned 0D model (docs/TUNING_MODEL.md).  Diagnostic only.
    """
    try:
        if str(threed_config.get("wall_model", "deformable")) != "deformable" or not diagnostics_path_raw:
            return
        with open(diagnostics_path_raw, encoding="utf-8") as stream:
            proximal = (json.load(stream) or {}).get("proximal_compliance") or {}
        matched = proximal.get("matched_uniform_wall_eh")
        if not matched:
            return
        wall_eh = float(threed_config["elasticity_modulus"]) * float(threed_config["shell_thickness"])
        ratio = wall_eh / float(matched)
        log["threed_wall_vs_proximal_compliance"] = {
            "threed_wall_eh": wall_eh,
            "matched_uniform_wall_eh": float(matched),
            "ratio": ratio,
            "matched_elasticity_modulus": float(matched) / float(threed_config["shell_thickness"]),
        }
        if not 0.5 <= ratio <= 2.0:
            log["warnings"].append(
                f"threed_wall_mismatch: 3D wall E*h={wall_eh:.3g} is {ratio:.2f}x the E*h "
                f"({float(matched):.3g}) matching the tuned 0D proximal compliance"
            )
    except Exception as exc:  # diagnostic only
        log["warnings"].append(f"threed_wall_match_failed: {exc}")


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


def _resolve_slurm_mail_user(sim_cfg: dict) -> str | None:
    execution = sim_cfg.get("execution") or {}
    if not isinstance(execution, dict):
        return None
    slurm = execution.get("slurm") or {}
    if not isinstance(slurm, dict):
        return None
    value = slurm.get("mail_user")
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _resolve_slurm_mail_types(sim_cfg: dict) -> list[str]:
    execution = sim_cfg.get("execution") or {}
    if not isinstance(execution, dict):
        return ["begin", "end"]
    slurm = execution.get("slurm") or {}
    if not isinstance(slurm, dict):
        return ["begin", "end"]
    value = slurm.get("mail_types", ["begin", "end"])
    if not isinstance(value, list):
        return ["begin", "end"]
    normalized = [str(item).strip() for item in value if str(item).strip()]
    return normalized or ["begin", "end"]


def _parse_sbatch_job_id(stdout: str) -> str:
    # TACC's sbatch wrapper prints a login banner and filter checks on stdout
    # before the --parsable "<id>[;cluster]" line, so take the last such line.
    for line in reversed(stdout.splitlines()):
        job_id = line.strip().split(";")[0].strip()
        if job_id.isdigit():
            return job_id
    raise RuntimeError(f"unable to parse job id from sbatch stdout: {stdout!r}")


def _submit_job(script_path: Path) -> str:
    _exit_if_superseded(f"sbatch {script_path}")
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
    if not proc.stdout.strip():
        raise RuntimeError("sbatch returned empty stdout")
    return _parse_sbatch_job_id(proc.stdout)


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


def _job_attempt_history(job_id: str) -> list[dict] | None:
    """One record per scheduler attempt of job_id (sacct -D), oldest first; None if sacct fails."""
    sacct = subprocess.run(
        [
            "sacct", "-D", "-X", "-j", job_id, "--noheader", "--parsable2",
            "--format", "JobID,State,Start,End,NodeList",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if sacct.returncode != 0:
        return None
    attempts = []
    for line in sacct.stdout.splitlines():
        fields = line.strip().split("|")
        if len(fields) < 5 or fields[0].strip() != job_id:
            continue
        state = fields[1].split()
        attempts.append(
            {
                "state": state[0].upper() if state else "",
                "start": fields[2],
                "end": fields[3],
                "nodes": fields[4],
            }
        )
    return attempts


def _warn_if_job_was_requeued(job_id: str, label: str) -> None:
    """Record a warning when job_id ran more than once (e.g. NODE_FAIL, then a Slurm requeue).

    svMultiPhysics appends to svZeroD_data, so a requeued 3D run started before
    run_solver.sh moved stale data aside has one header block per attempt
    (tst-stan-1-20261016, job 48158596).
    """
    attempts = _job_attempt_history(job_id)
    if attempts is None:
        log["warnings"].append(f"{label}_attempt_history_unavailable: sacct -D failed for job {job_id}")
        return
    if len(attempts) <= 1:
        return
    log[f"{label}_attempt_history"] = attempts
    summary = ", ".join(f"{a['state'] or '?'}@{a['start']} on {a['nodes']}" for a in attempts)
    log["warnings"].append(
        f"{label}_requeued: job {job_id} ran {len(attempts)} attempts ({summary}); "
        "metrics come from the last attempt"
    )


def _flow_split_metrics_recording_warnings(preop_dir: Path) -> dict:
    """compute_flow_split_metrics, with svZeroDTrees RuntimeWarnings (e.g. a
    relaunched solver's extra svZeroD_data header blocks) copied to the log."""
    import warnings

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", RuntimeWarning)
        result = compute_flow_split_metrics(simulation_dir=preop_dir)
    for warning in caught:
        if issubclass(warning.category, RuntimeWarning):
            log["warnings"].append(f"flow_split_input: {warning.message}")
        else:
            warnings.showwarning(warning.message, warning.category, warning.filename, warning.lineno)
    return result


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

        # A stale attempt stops here rather than at its timeout.
        _exit_if_superseded(f"waiting on job {job_id}")
        time.sleep(max(poll_seconds, 5))


def _start_solver_job(script_path: Path) -> str:
    """Submit a solver stage's run_solver.sh, or, in-allocation, name it for _wait_for_solver_job.

    In-allocation the stage runs under this driver job, so its ID is the stage's
    job ID: scheduler queries and solver-log names (svFlowSolver.o<id>) keep
    working as evidence for fetch and --reuse-preop-3d.
    """
    if in_allocation:
        return str(driver_attempt["job_id"])
    return _submit_job(script_path)


def _wait_for_solver_job(
    job_id: str, script_path: Path, *, poll_seconds: int, timeout_seconds: int
) -> tuple[bool, str | None]:
    """Wait for a submitted stage, or, in-allocation, run it here and wait for its exit."""
    if in_allocation:
        return _run_in_allocation(
            script_path, job_id=job_id, poll_seconds=poll_seconds, timeout_seconds=timeout_seconds
        )
    return _wait_for_completion(job_id, poll_seconds=poll_seconds, timeout_seconds=timeout_seconds)


def _stop_process_group(proc: subprocess.Popen) -> None:
    for sig, grace in ((signal.SIGTERM, 30), (signal.SIGKILL, 10)):
        try:
            os.killpg(proc.pid, sig)
        except ProcessLookupError:
            return
        try:
            proc.wait(timeout=grace)
            return
        except subprocess.TimeoutExpired:
            continue


def _run_in_allocation(
    script_path: Path, *, job_id: str, poll_seconds: int, timeout_seconds: int
) -> tuple[bool, str | None]:
    """Run a normalized run_solver.sh with bash inside this job's allocation.

    Its srun line carries the stage's own --nodes/--ntasks; its #SBATCH lines
    are comments to bash.  stdout/stderr go where the batch job's would
    (svFlowSolver.o/e<job_id> in the stage dir).  Returns (ok, terminal) like
    _wait_for_completion: COMPLETED on exit code 0.
    """
    _exit_if_superseded(f"running {script_path} in the driver allocation")
    stage_dir = script_path.parent
    label = stage_dir.name
    log["steps"].append(f"in_allocation_run_started:{job_id}:{label}")
    with (stage_dir / f"svFlowSolver.o{job_id}").open("w") as out, (
        stage_dir / f"svFlowSolver.e{job_id}"
    ).open("w") as err:
        proc = subprocess.Popen(
            ["bash", script_path.name],
            cwd=stage_dir,
            stdout=out,
            stderr=err,
            start_new_session=True,
        )
        start = time.monotonic()
        while True:
            try:
                returncode = proc.wait(timeout=max(poll_seconds, 5))
                break
            except subprocess.TimeoutExpired:
                pass
            if time.monotonic() - start > timeout_seconds:
                _stop_process_group(proc)
                terminal = f"timeout after {timeout_seconds}s (in_allocation {label})"
                log["steps"].append(f"in_allocation_run:{job_id}:{label}:TIMEOUT")
                return False, terminal
            try:
                # A stale attempt stops its solver instead of running on.
                _exit_if_superseded(f"waiting on {script_path}")
            except SystemExit:
                _stop_process_group(proc)
                raise
    state = "COMPLETED" if returncode == 0 else "FAILED"
    log["steps"].append(f"in_allocation_run:{job_id}:{label}:{state}:rc={returncode}")
    return returncode == 0, state if returncode == 0 else f"FAILED (exit code {returncode})"


def _submit_post3d_driver(preop_job_id: str) -> str:
    """Queue this script as the post-3D job, to start once the 3D run ends."""
    script_path = remote_iter_dir / "run_tune_iter.sh"
    if not script_path.exists():
        raise RuntimeError(f"driver script missing: {script_path}")
    _exit_if_superseded("sbatch of the post-3D driver")
    argv = [
        "sbatch",
        "--parsable",
        f"--dependency=afterany:{preop_job_id}",
        "--job-name",
        os.environ.get("SLURM_JOB_NAME") or run_id,
        "--export",
        "ALL,SVZT_DRIVER_PHASE=post3d",
        "--chdir",
        str(script_path.parent),
        str(script_path),
    ]
    proc = subprocess.run(
        argv, capture_output=True, env=_nested_sbatch_env(), text=True, check=False
    )
    if proc.returncode != 0 or not proc.stdout.strip():
        raise RuntimeError(f"sbatch failed rc={proc.returncode}: {proc.stderr.strip()}")
    return _parse_sbatch_job_id(proc.stdout)


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
    if sim_cfg.get("elasticity_modulus") == MATCH_PROXIMAL_COMPLIANCE:
        raise RuntimeError(
            "elasticity_modulus=match_proximal_compliance was not resolved before the 3D stage"
        )
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
        mail_user=_resolve_slurm_mail_user(sim_cfg),
        mail_types=_resolve_slurm_mail_types(sim_cfg),
        svfsiplus_path=cluster_svfsiplus_path,
        explicit_task_count=in_allocation,
    )

    job_id = _start_solver_job(run_solver_path)
    return job_id, stage_dir


def _submit_prepared_sbatch(argv: list[str]) -> str:
    script_path = Path(argv[-1])
    _exit_if_superseded(f"sbatch {script_path}")
    proc = subprocess.run(
        argv,
        cwd=script_path.parent,
        capture_output=True,
        env=_nested_sbatch_env(),
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"sbatch failed rc={proc.returncode}: {proc.stderr.strip()}")
    if not proc.stdout.strip():
        raise RuntimeError("sbatch returned empty stdout")
    return _parse_sbatch_job_id(proc.stdout)


def _submit_calibrated_full_pa_seed_jobs(tuned_model: Path, *, defer: bool = False) -> dict:
    postprocess_spec = dict(seed_generation.get("postprocess") or {})
    calibration_spec = dict(seed_generation.get("calibration") or {})
    report = {
        "strategy": "calibrated_full_pa",
        "status": "failed",
        "tuned_zerod_config": str(tuned_model.resolve()),
        "postprocess": {key: postprocess_spec.get(key) for key in ("remote_script", "remote_results_dir")},
        "calibration": {
            key: calibration_spec.get(key)
            for key in ("remote_root", "remote_script", "remote_config_path")
        },
        "error": None,
    }
    if defer:
        # In-allocation drivers cannot sbatch; on advance the agent submits both
        # jobs from the login node (its fallback for any status but "submitted").
        report["status"] = "deferred_to_agent"
        return report
    try:
        request = json.loads(Path(calibration_spec["remote_request_path"]).read_text(encoding="utf-8"))
        request["paths"]["zerod_config"] = str(tuned_model.resolve())
        config_path = Path(calibration_spec["remote_config_path"])
        config_path.parent.mkdir(parents=True, exist_ok=True)
        # JSON is valid YAML for the upstream calibrate-0d-from-3d loader.
        config_path.write_text(json.dumps(request, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        for logs_dir in (postprocess_spec["remote_logs_dir"], calibration_spec["remote_logs_dir"]):
            Path(logs_dir).mkdir(parents=True, exist_ok=True)

        postprocess_job_id = _submit_prepared_sbatch(list(postprocess_spec["sbatch_argv"]))
        report["postprocess"]["job_id"] = postprocess_job_id
        calibration_argv = list(calibration_spec["sbatch_argv"])
        calibration_argv.insert(len(calibration_argv) - 1, f"--dependency=afterok:{postprocess_job_id}")
        report["calibration"]["job_id"] = _submit_prepared_sbatch(calibration_argv)
        report["status"] = "submitted"
    except Exception as exc:
        # The agent falls back to submitting these jobs itself on advance.
        report["error"] = f"{type(exc).__name__}: {exc}"
        log["warnings"].append(f"calibrated_full_pa_seed_submission_failed: {exc}")
    return report


# Claimed before any work; see driver_attempt above.
_claim_driver_attempt()

try:
    if geometry_scaling:
        from svzerodtrees.simulation.geometry_scale import stage_scaled_geometry

        scaling_sources = dict(geometry_scaling["sources"])
        postop_source = scaling_sources.get("postop_mesh_complete")
        if postop_source and not Path(postop_source).is_dir():
            # Optional asset: a patient without a postop mesh has nothing to scale.
            log["warnings"].append(f"scaled_geometry_postop_mesh_missing: {postop_source}")
            scaling_sources["postop_mesh_complete"] = None
        staged_geometry = stage_scaled_geometry(
            geometry_scaling["staged_root"],
            geometry_scaling["factor"],
            sources=scaling_sources,
            targets=geometry_scaling["targets"],
        )
        log["geometry_scaling"] = {
            key: staged_geometry[key] for key in ("scale", "staged_root", "reused")
        }
        log["steps"].append(
            ("scaled_geometry_reused:" if staged_geometry["reused"] else "scaled_geometry_staged:")
            + staged_geometry["staged_root"]
        )
    if preop_mesh_surface_aliases and preop_mesh_complete_path is not None:
        preop_mesh_complete_path = _stage_aliased_mesh_complete(
            preop_mesh_complete_path, preop_mesh_surface_aliases
        )
    poll_seconds = int(threed_config.get("wait_poll_seconds", 30))
    timeout_seconds = int(threed_config.get("wait_timeout_seconds", 43200))
    tuned_config_path: Path | None = None
    tuning_model = "rri"
    seed_filename = "simplified_nonlinear_zerod.json"
    if tuning_bc_type == "impedance":
        tuning_model = str(zerod_tuning_config.get("tuning_model", "rri")).strip().lower()
        seed_filename = "full_pa_zerod.json" if tuning_model == "full_pa" else "simplified_nonlinear_zerod.json"
    optimized_tuning_filename = (
        "optimized_rcr_params.csv" if tuning_bc_type == "rcr" else "optimized_params.csv"
    )
    staged_seed_path = remote_inputs_dir / seed_filename
    if skip_zerod_tuning:
        log["steps"].append("0d_tuning_skipped")
        tuned_config_path = remote_results_dir / "svzerod_3d_coupling_tuned.json"
        decision_payload["tuning_artifacts"].update(
            {
                "optimized_params_csv": str(remote_results_dir / optimized_tuning_filename),
                "pa_config_snapshot": str(remote_results_dir / "pa_config_tuning_snapshot.json"),
                "tuned_zerod_config": str(tuned_config_path),
            }
        )
        required_tuning_artifacts = [
            remote_results_dir / optimized_tuning_filename,
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
        else:
            skipped_diagnostics = remote_results_dir / "tuning_diagnostics.json"
            skipped_diagnostics_raw = str(skipped_diagnostics) if skipped_diagnostics.exists() else None
            if skipped_diagnostics_raw:
                decision_payload["tuning_artifacts"]["tuning_diagnostics"] = skipped_diagnostics_raw
            _resolve_threed_elasticity_modulus(skipped_diagnostics_raw)
            _record_threed_wall_match(skipped_diagnostics_raw)
    elif not staged_seed_path.exists():
        iteration1_seed_source = str(
            iteration1_seed_config.get("source", "path")
        ).strip().lower()
        if iteration == 1 and iteration1_seed_source == "learned_zerod":
            log["steps"].append("learned_iteration_seed_generation_started")
            _generate_learned_iteration_seed(staged_seed_path)
        elif tuning_model == "full_pa":
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
        if tuning_bc_type == "impedance" and iteration > 1:
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
        elif bool(zerod_tuning_config.get("rescale_inflow")) and resolved_inflow_path is None:
            _mark_needs_review(
                "rescale_inflow=True but no inflow.csv was available for iteration tuning "
                f"(staged={staged_inflow_path}, patient={remote_inflow_path})"
            )
        else:
            if tuning_bc_type == "impedance":
                if tuning_model == "full_pa":
                    zerod_tuning_config = _carry_forward_outlet_mapping(zerod_tuning_config)
                tuning = run_impedance_tuning_for_iteration(
                    iteration_dir=remote_iter_dir,
                    seed_config=staged_seed_path,
                    mesh_surfaces=mesh_surfaces_path,
                    clinical_targets=clinical_targets_path,
                    inflow_path=resolved_inflow_path,
                    impedance_config=zerod_tuning_config,
                    previous_optimized_params=previous_optimized_params,
                )
            else:
                tuning = run_rcr_tuning_for_iteration(
                    iteration_dir=remote_iter_dir,
                    seed_config=staged_seed_path,
                    mesh_surfaces=mesh_surfaces_path,
                    clinical_targets=clinical_targets_path,
                    inflow_path=resolved_inflow_path,
                    rcr_config=zerod_tuning_config,
                )
            decision_payload["tuning_artifacts"].update(
                {
                    "optimized_params_csv": tuning.get("optimized_params_csv"),
                    "stree_optimization_log": tuning.get("stree_optimization_log"),
                    "pa_config_snapshot": tuning.get("pa_config_snapshot"),
                    "tuned_zerod_config": tuning.get("tuned_zerod_config"),
                    "zerod_pre_mapping_metrics": str(remote_results_dir / "zerod_pre_mapping_metrics.json"),
                    # Present when svZeroDTrees writes them (impedance tuning).
                    "tuning_diagnostics": tuning.get("tuning_diagnostics"),
                    "seed_with_proximal_compliance": tuning.get("seed_with_proximal_compliance"),
                }
            )
            log["steps"].append("0d_tuning_completed")
            _resolve_threed_elasticity_modulus(tuning.get("tuning_diagnostics"))
            _record_threed_wall_match(tuning.get("tuning_diagnostics"))

            snapshot_path_raw = tuning.get("pa_config_snapshot")
            snapshot_path = Path(snapshot_path_raw) if snapshot_path_raw else None
            if snapshot_path is not None and snapshot_path.exists() and clinical_targets_path is not None:
                try:
                    # svZeroDTrees runs the snapshot until its MPA pressure is
                    # periodic (the snapshot keeps the seed's 2 cold-start
                    # cycles, whose cycle 2 is still transient).  Older
                    # installs score the seed's cycles and report no periodicity.
                    zerod_metrics = summarize_pulmonary_zerod_config(snapshot_path)
                    zerod_periodicity = zerod_metrics.get("periodicity")
                    log["warnings"].extend(_zerod_periodicity_warnings(zerod_periodicity))
                    zerod_gate = evaluate_iteration_gate(
                        metrics={key: float(zerod_metrics[key]) for key in ("mpa_sys", "mpa_dia", "mpa_mean", "rpa_split")},
                        clinical_targets=clinical_targets_path,
                        **gate_kwargs,
                    )
                    zerod_payload = {
                        "run_id": run_id,
                        "iteration": iteration,
                        "status": "ok",
                        "source_kind": "pa_config_tuning_snapshot",
                        "source_config_path": str(snapshot_path),
                        # Cycles simulated and final cycle-to-cycle MPA drift.
                        "periodicity": zerod_periodicity,
                        "metrics": {
                            key: zerod_metrics.get(key)
                            for key in (
                                "mpa_sys",
                                "mpa_dia",
                                "mpa_mean",
                                "rpa_split",
                                "mpa_flow",
                                "lpa_flow",
                                "rpa_flow",
                            )
                        },
                        "clinical_targets": zerod_gate.get("clinical_targets"),
                        "comparison": {
                            "targets": zerod_gate.get("clinical_targets"),
                            "signed_deltas": {
                                key: float(zerod_metrics[key]) - float(zerod_gate["clinical_targets"][key])
                                for key in ("mpa_sys", "mpa_dia", "mpa_mean", "rpa_split")
                            },
                            "absolute_deltas": zerod_gate.get("deltas"),
                            "thresholds": zerod_gate.get("thresholds"),
                            "within_threshold": {
                                key: bool(zerod_gate["deltas"][key] <= zerod_gate["thresholds"][key])
                                for key in ("mpa_sys", "mpa_dia", "mpa_mean", "rpa_split")
                            },
                            "close_to_targets": bool(zerod_gate.get("close_to_targets")),
                            "decision": zerod_gate.get("decision"),
                        },
                    }
                    _exit_if_superseded("writing zerod_pre_mapping_metrics.json")
                    write_iteration_metrics(remote_results_dir / "zerod_pre_mapping_metrics.json", zerod_payload)
                    log["steps"].append("zerod_pre_mapping_metrics_written")
                except Exception as exc:
                    log["warnings"].append(f"zerod_pre_mapping_metrics_failed: {exc}")

        tuned_zerod_raw = decision_payload["tuning_artifacts"].get("tuned_zerod_config")
        tuned_config_path = Path(tuned_zerod_raw) if tuned_zerod_raw else None
        if decision_payload["decision"] == "needs_review":
            tuned_config_path = None
        elif tuned_config_path is None:
            _mark_needs_review(f"{tuning_bc_type} tuning did not return tuned_zerod_config")
        elif not tuned_config_path.exists():
            _mark_needs_review(
                f"tuned 0D config missing after {tuning_bc_type} tuning: {tuned_config_path}"
            )

    if (
        decision_payload["decision"] != "needs_review"
        and tuned_config_path is not None
        and reuse_preop_3d
    ):
        preop_dir = remote_iter_dir / "preop"
        reused_job_id, reuse_error = _completed_preop_job_for_reuse(preop_dir)
        if reuse_error is not None:
            _mark_needs_review(
                reuse_error if driver_phase == "post3d" else f"reuse_preop_3d: {reuse_error}"
            )
        else:
            log["preop_job_id"] = reused_job_id
            log["preop_terminal_state"] = "COMPLETED"
            log["steps"].append(f"preop_reused:{reused_job_id}")

    if decision_payload["decision"] != "needs_review" and tuned_config_path is not None:
        if reuse_preop_3d:
            ok, terminal = True, "COMPLETED"
        else:
            log["steps"].append("preop_3d_setup_started")

            preop_job_id, preop_dir = _prepare_and_submit_stage(
                stage_name="preop",
                mesh_complete_path=preop_mesh_complete_path,
                zerod_config_path=tuned_config_path,
            )
            log["preop_job_id"] = preop_job_id
            log["steps"].append("preop_submitted")

            post3d_job_id = None
            if not in_allocation:
                try:
                    post3d_job_id = _submit_post3d_driver(preop_job_id)
                except Exception as exc:
                    log["warnings"].append(
                        f"post3d_driver_submission_failed: {exc}; waiting for the 3D run in this job"
                    )
            if post3d_job_id:
                log["post3d_job_id"] = post3d_job_id
                log["steps"].append(f"post3d_driver_submitted:{post3d_job_id}")
                handed_off_to_post3d = True
                ok, terminal = True, None
            else:
                ok, terminal = _wait_for_solver_job(
                    preop_job_id,
                    preop_dir / "run_solver.sh",
                    poll_seconds=poll_seconds,
                    timeout_seconds=timeout_seconds,
                )
                log["preop_terminal_state"] = terminal
        if handed_off_to_post3d:
            pass
        elif not ok:
            _mark_needs_review(f"preop simulation did not complete successfully: {terminal}")
        else:
            log["steps"].append("preop_completed")
            if log.get("preop_job_id"):
                _warn_if_job_was_requeued(str(log["preop_job_id"]), "preop_3d")

            pressure_csv = remote_results_dir / "mpa_pressure_vs_time.csv"
            if not pressure_csv.exists() or _older_than_3d_result(pressure_csv, preop_dir):
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
                metrics.update(_flow_split_metrics_recording_warnings(preop_dir))
                log["steps"].append("flow_split_metrics_loaded")
            except Exception as exc:
                _mark_needs_review(f"flow-split extraction failed: {exc}")

            if decision_payload["decision"] != "needs_review":
                if clinical_targets_path and clinical_targets_path.exists() and set(metrics.keys()) >= {"mpa_sys", "mpa_dia", "mpa_mean", "rpa_split"}:
                    gate = evaluate_iteration_gate(
                        metrics=metrics, clinical_targets=clinical_targets_path, **gate_kwargs
                    )
                    decision_payload.update(gate)
                    log["steps"].append("clinical_gate_evaluated")
                else:
                    _mark_needs_review("clinical gate prerequisites missing (targets or required metrics)")

            if decision_payload["decision"] == "not_close" and not regenerate_reduced_seed:
                log["steps"].append("reduced_pa_regeneration_skipped_calibrated_full_pa")

            elif decision_payload["decision"] == "not_close":
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

            if (
                seed_strategy == "calibrated_full_pa"
                and decision_payload["decision"] in {"not_close", "converged"}
            ):
                seed_report = _submit_calibrated_full_pa_seed_jobs(
                    tuned_config_path, defer=in_allocation
                )
                decision_payload["seed_generation"] = seed_report
                log["steps"].append(f"calibrated_full_pa_seed_jobs_{seed_report['status']}")
            elif (
                decision_payload["decision"] == "not_close"
                and decision_payload.get("regenerated_config_path")
            ):
                decision_payload["seed_generation"] = {
                    "strategy": "reduced_rri",
                    "status": "regenerated",
                    "regenerated_config_path": decision_payload["regenerated_config_path"],
                }

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

_write_driver_results(metrics_payload)
PY
