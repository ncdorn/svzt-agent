"""In-allocation tune drivers run their solver stages with srun instead of sbatch.

TACC Stampede3 forbids sbatch from compute nodes ("[TACC]: Job submission is
not allowed from this host"), so with ``defaults.execution.nested_submission:
in_allocation`` the driver requests the preop 3D shape up front and runs the
mean steady, prestress and preop 3D stages inside its own allocation.  The
default ``sbatch`` mode (Sherlock) is unchanged.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import signal
import subprocess
import time
from pathlib import Path

import pytest
from pydantic import ValidationError

from svztagent.config.models import ExecutionDefaults
from svztagent.hpc.interfaces import ExecutionMode
from svztagent.workflows.tune_trees import run_tune_trees

TEMPLATE = Path(__file__).resolve().parents[1] / "src" / "svztagent" / "templates" / "slurm" / "job_template.sh"
JOB = "3001234"


def _driver_function(name: str) -> str:
    template = TEMPLATE.read_text()
    start = template.index(f"def {name}(")
    return template[start : template.index("\n\n\n", start)]


def _set_nested_submission(workspace: Path, mode: str) -> None:
    defaults_path = workspace / "config" / "defaults.yaml"
    defaults_path.write_text(
        defaults_path.read_text(encoding="utf-8").replace(
            '    python_executable: "python3"\n',
            f'    python_executable: "python3"\n    nested_submission: "{mode}"\n',
            1,
        ),
        encoding="utf-8",
    )


def _render(workspace: Path, run_id: str):
    result = run_tune_trees(
        workspace_root=workspace,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id=run_id,
        mode=ExecutionMode.DRY_RUN,
    )
    rendered = result.local_job_script_path.read_text(encoding="utf-8")
    submit_preview = next(
        command for command in result.command_previews if command[0] == "ssh" and "sbatch --parsable" in command[-1]
    )
    return rendered, shlex.split(submit_preview[-1])


def test_nested_submission_defaults_to_sbatch_and_rejects_unknown_modes():
    assert ExecutionDefaults().nested_submission == "sbatch"
    assert ExecutionDefaults(nested_submission="in_allocation").nested_submission == "in_allocation"
    with pytest.raises(ValidationError):
        ExecutionDefaults(nested_submission="ssh")


def test_sbatch_mode_renders_a_single_task_driver(sample_config_files):
    defaults_path = sample_config_files / "config" / "defaults.yaml"
    defaults_path.write_text(
        defaults_path.read_text(encoding="utf-8").replace('cpus: "<count>"', 'cpus: "4"', 1),
        encoding="utf-8",
    )
    rendered, submit = _render(sample_config_files, "run-dry-default-mode")

    header = rendered.split('echo "[svzt] run_id', 1)[0]
    assert "#SBATCH --cpus-per-task=24" in header
    assert "--ntasks-per-node" not in header
    assert submit[submit.index("--cpus-per-task") + 1] == "24"
    assert 'nested_submission = "sbatch"' in rendered


def test_in_allocation_driver_requests_the_3d_shape_in_its_header_only(sample_config_files):
    _set_nested_submission(sample_config_files, "in_allocation")
    defaults_path = sample_config_files / "config" / "defaults.yaml"
    defaults_path.write_text(
        defaults_path.read_text(encoding="utf-8").replace('cpus: "<count>"', 'cpus: "4"\n    nodes: "1"', 1),
        encoding="utf-8",
    )

    rendered, submit = _render(sample_config_files, "run-dry-in-allocation")

    header = rendered.split('echo "[svzt] run_id', 1)[0]
    # threed defaults: 3 nodes x 24 procs per node.
    assert "#SBATCH --nodes=3" in header
    assert "#SBATCH --ntasks-per-node=24" in header
    assert "#SBATCH --cpus-per-task=1" in header
    # CLI flags override #SBATCH lines, so the submit command must not carry
    # the single-task shape (scheduler.nodes / cpus / tuning n_procs).
    assert "--nodes" not in submit
    assert "--cpus-per-task" not in submit
    assert 'nested_submission = "in_allocation"' in rendered


def test_in_allocation_runscript_names_its_own_task_count(tmp_path):
    namespace: dict = {"Path": Path}
    exec(_driver_function("_normalize_solver_runscript"), namespace)
    script = tmp_path / "run_solver.sh"
    kwargs = dict(
        nodes=2,
        procs_per_node=48,
        memory_gb=16,
        hours=2,
        partition="skx",
        account=None,
        mail_user=None,
        mail_types=None,
        svfsiplus_path="/bin/svmp",
    )

    script.write_text("#!/bin/bash\nsrun old\n", encoding="utf-8")
    namespace["_normalize_solver_runscript"](script_path=script, **kwargs)
    assert script.read_text().rstrip().endswith("\nsrun /bin/svmp svFSIplus.xml")

    script.write_text("#!/bin/bash\nsrun old\n", encoding="utf-8")
    namespace["_normalize_solver_runscript"](script_path=script, explicit_task_count=True, **kwargs)
    assert script.read_text().rstrip().endswith("\nsrun --nodes=2 --ntasks=96 /bin/svmp svFSIplus.xml")
    assert "srun old" not in script.read_text()


def _in_allocation_namespace(superseded=lambda action: None) -> dict:
    namespace = {
        "os": os,
        "signal": signal,
        "subprocess": subprocess,
        "time": time,
        "Path": Path,
        "log": {"steps": []},
        "in_allocation": True,
        "driver_attempt": {"job_id": JOB},
        "_exit_if_superseded": superseded,
        "_submit_job": lambda path: pytest.fail("in-allocation drivers must not sbatch"),
        "_wait_for_completion": lambda *a, **k: pytest.fail("in-allocation drivers must not poll"),
    }
    for name in ("_start_solver_job", "_wait_for_solver_job", "_stop_process_group", "_run_in_allocation"):
        exec(_driver_function(name), namespace)
    return namespace


def _stage(tmp_path: Path, body: str) -> Path:
    stage = tmp_path / "preop"
    stage.mkdir()
    script = stage / "run_solver.sh"
    script.write_text("#!/usr/bin/env bash\n#SBATCH --nodes=1\n" + body, encoding="utf-8")
    return script


def test_in_allocation_stage_runs_in_this_job_with_solver_logs(tmp_path):
    namespace = _in_allocation_namespace()
    script = _stage(tmp_path, 'echo "solver ran in $PWD"\necho warn >&2\n')

    job_id = namespace["_start_solver_job"](script)
    ok, terminal = namespace["_wait_for_solver_job"](job_id, script, poll_seconds=5, timeout_seconds=60)

    assert job_id == JOB
    assert (ok, terminal) == (True, "COMPLETED")
    assert (script.parent / f"svFlowSolver.o{JOB}").read_text() == f"solver ran in {script.parent.resolve()}\n"
    assert (script.parent / f"svFlowSolver.e{JOB}").read_text() == "warn\n"
    assert namespace["log"]["steps"] == [
        f"in_allocation_run_started:{JOB}:preop",
        f"in_allocation_run:{JOB}:preop:COMPLETED:rc=0",
    ]


def test_in_allocation_stage_failure_is_a_terminal_state(tmp_path):
    namespace = _in_allocation_namespace()
    script = _stage(tmp_path, "exit 3\n")

    ok, terminal = namespace["_wait_for_solver_job"](JOB, script, poll_seconds=5, timeout_seconds=60)

    assert (ok, terminal) == (False, "FAILED (exit code 3)")
    assert namespace["log"]["steps"][-1] == f"in_allocation_run:{JOB}:preop:FAILED:rc=3"


def test_in_allocation_stage_timeout_stops_the_solver(tmp_path):
    namespace = _in_allocation_namespace()
    marker = tmp_path / "still_running"
    script = _stage(tmp_path, f"sleep 30\ntouch {marker}\n")

    start = time.monotonic()
    ok, terminal = namespace["_wait_for_solver_job"](JOB, script, poll_seconds=0, timeout_seconds=1)

    assert not ok and terminal.startswith("timeout after 1s")
    assert time.monotonic() - start < 20
    assert not marker.exists()


def test_stale_in_allocation_attempt_stops_its_solver_and_exits(tmp_path):
    calls = []

    def superseded(action):
        calls.append(action)
        if len(calls) > 1:
            raise SystemExit(9)

    namespace = _in_allocation_namespace(superseded)
    marker = tmp_path / "still_running"
    script = _stage(tmp_path, f"sleep 30\ntouch {marker}\n")

    with pytest.raises(SystemExit):
        namespace["_wait_for_solver_job"](JOB, script, poll_seconds=0, timeout_seconds=600)

    assert calls[0].startswith("running ")
    assert not marker.exists()


def test_in_allocation_driver_defers_calibration_jobs_to_the_agent(tmp_path):
    namespace = {
        "json": json,
        "Path": Path,
        "log": {"steps": [], "warnings": []},
        "seed_generation": {
            "postprocess": {"remote_script": "/runs/pp.sh", "remote_results_dir": "/runs/pp"},
            "calibration": {"remote_root": "/runs/cal", "remote_script": "/runs/cal.sh"},
        },
        "_submit_prepared_sbatch": lambda argv: pytest.fail("in-allocation drivers must not sbatch"),
    }
    exec(_driver_function("_submit_calibrated_full_pa_seed_jobs"), namespace)

    report = namespace["_submit_calibrated_full_pa_seed_jobs"](tmp_path / "tuned.json", defer=True)

    # Any status but "submitted" makes the agent submit both jobs on advance.
    assert report["status"] == "deferred_to_agent"
    assert report["error"] is None
    assert namespace["log"]["warnings"] == []


def test_in_allocation_driver_never_hands_off_to_a_post3d_job():
    template = TEMPLATE.read_text()
    main = template[template.index("\n_claim_driver_attempt()\n") :]
    assert re.search(
        r"if not in_allocation:\n\s+try:\n\s+post3d_job_id = _submit_post3d_driver\(preop_job_id\)", main
    )
    assert "_wait_for_solver_job(\n                    preop_job_id," in main
    assert "defer=in_allocation" in main
