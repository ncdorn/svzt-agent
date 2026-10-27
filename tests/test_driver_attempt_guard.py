"""A requeued tune driver attempt owns its job's writes; a stale attempt exits.

After a NODE_FAIL, Slurm requeues the driver job (same job ID, new attempt)
while the first attempt can keep running on the failed node.  Each attempt
claims ``logs/iteration_driver_attempt_<job>.json`` at start and checks it
before any sbatch and before writing the driver log, handoff, metrics or
decision (TST-STAN-2, job 48156809, 2026-10-18).
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

import pytest

TEMPLATE = Path(__file__).resolve().parents[1] / "src" / "svztagent" / "templates" / "slurm" / "job_template.sh"
JOB, PREOP, POST3D = "48156809", "48160001", "48160002"
ATTEMPT_FUNCTIONS = (
    "_claim_driver_attempt",
    "_exit_if_superseded",
    "_handoff_from_earlier_job",
    "_write_driver_results",
)


def _driver_function(name: str) -> str:
    template = TEMPLATE.read_text()
    start = template.index(f"def {name}(")
    return template[start : template.index("\ndef ", start + 1)]


def _write_json(path, payload) -> None:
    Path(path).write_text(json.dumps(payload), encoding="utf-8")


def _attempt(tmp_path: Path, restart_count: int, *, phase: str = "pre3d", **overrides) -> dict:
    """Namespace of one driver attempt of JOB, sharing tmp_path's iteration dirs."""
    logs, results = tmp_path / "logs", tmp_path / "results"
    logs.mkdir(exist_ok=True)
    results.mkdir(exist_ok=True)
    attempt = {
        "job_id": JOB,
        "restart_count": restart_count,
        "phase": phase,
        "node": f"sh03-{restart_count:02d}",
        "pid": 1000 + restart_count,
        "started_at": f"2026-10-17T1{restart_count}:00:00+00:00",
        "nonce": f"nonce-{restart_count}",
    }
    namespace = {
        "json": json,
        "os": os,
        "sys": sys,
        "Path": Path,
        "run_id": "tst-stan-2-20261016",
        "iteration": 1,
        "driver_phase": phase,
        "driver_attempt": attempt,
        "driver_attempt_path": logs / f"iteration_driver_attempt_{JOB}.json",
        "DRIVER_SUPERSEDED_EXIT_CODE": 9,
        "DRIVER_HANDOFF_FILENAME": "iteration_handoff.json",
        "remote_logs_dir": logs,
        "remote_results_dir": results,
        "metrics_path": results / "iteration_metrics.json",
        "decision_path": results / "iteration_decision.json",
        "write_iteration_metrics": _write_json,
        "write_iteration_decision": _write_json,
        "handed_off_to_post3d": False,
        "decision_payload": {"decision": "not_close"},
        "log": {"steps": [], "warnings": [], "driver_attempt": attempt},
    }
    namespace.update(overrides)
    for name in ATTEMPT_FUNCTIONS:
        exec(_driver_function(name), namespace)
    return namespace


def _handed_off(tmp_path: Path, restart_count: int) -> dict:
    return _attempt(
        tmp_path,
        restart_count,
        handed_off_to_post3d=True,
        log={"steps": [f"post3d_driver_submitted:{POST3D}"], "warnings": [], "preop_job_id": PREOP, "post3d_job_id": POST3D},
    )


def test_attempt_that_owns_the_job_writes_its_results(tmp_path, monkeypatch):
    monkeypatch.setenv("SLURM_JOB_ID", JOB)
    attempt = _handed_off(tmp_path, 0)
    attempt["_claim_driver_attempt"]()

    attempt["_write_driver_results"]({})

    claimed = json.loads(attempt["driver_attempt_path"].read_text(encoding="utf-8"))
    assert claimed == attempt["driver_attempt"]
    handoff = json.loads((tmp_path / "results" / "iteration_handoff.json").read_text(encoding="utf-8"))
    assert handoff["pre3d_job_id"] == JOB and handoff["post3d_job_id"] == POST3D
    assert not list((tmp_path / "logs").glob("*.tmp"))


def test_stale_attempt_cannot_overwrite_the_requeued_attempts_handoff(tmp_path, monkeypatch, capsys):
    """The 2026-10-18 failure: the stale attempt's timeout decision removed the handoff."""
    monkeypatch.setenv("SLURM_JOB_ID", JOB)
    stale = _attempt(tmp_path, 0)
    stale["_claim_driver_attempt"]()
    requeued = _handed_off(tmp_path, 1)
    requeued["_claim_driver_attempt"]()
    requeued["_write_driver_results"]({})
    driver_log = (tmp_path / "logs" / "iteration_driver_log.json").read_text(encoding="utf-8")
    handoff = (tmp_path / "results" / "iteration_handoff.json").read_text(encoding="utf-8")

    stale["decision_payload"] = {
        "decision": "needs_review",
        "needs_review_reason": "mean steady simulation did not complete successfully: "
        "timeout after 129600s (last_state=PENDING)",
    }
    with pytest.raises(SystemExit) as exited:
        stale["_write_driver_results"]({})

    assert exited.value.code == 9
    assert (tmp_path / "logs" / "iteration_driver_log.json").read_text(encoding="utf-8") == driver_log
    assert (tmp_path / "results" / "iteration_handoff.json").read_text(encoding="utf-8") == handoff
    assert not (tmp_path / "results" / "iteration_decision.json").exists()
    assert "post3d_driver_submitted" in driver_log
    stderr = capsys.readouterr().err
    assert f"driver attempt 0 of job {JOB}" in stderr
    assert '"nonce": "nonce-1"' in stderr
    assert "without writing driver results" in stderr


@pytest.mark.parametrize("attempt_file", ["missing", "corrupt"])
def test_unverifiable_ownership_exits_without_writing(tmp_path, attempt_file):
    attempt = _attempt(tmp_path, 0)
    if attempt_file == "corrupt":
        attempt["driver_attempt_path"].write_text("{", encoding="utf-8")

    with pytest.raises(SystemExit):
        attempt["_write_driver_results"]({})

    assert not (tmp_path / "logs" / "iteration_driver_log.json").exists()
    assert not (tmp_path / "results" / "iteration_decision.json").exists()


def test_stale_attempt_stops_polling_instead_of_waiting_for_its_timeout(tmp_path):
    stale = _attempt(tmp_path, 0)
    stale["_claim_driver_attempt"]()
    _attempt(tmp_path, 1)["_claim_driver_attempt"]()
    sleeps: list[int] = []
    # A node cut off from the controller cannot query the scheduler.
    stale.update(
        {
            "_query_state": lambda job_id: (None, "unknown"),
            "time": type("T", (), {"monotonic": staticmethod(lambda: 0.0), "sleep": staticmethod(sleeps.append)}),
        }
    )
    exec(_driver_function("_wait_for_completion"), stale)

    with pytest.raises(SystemExit):
        stale["_wait_for_completion"]("48156900", poll_seconds=30, timeout_seconds=129600)

    assert sleeps == []


def test_stale_attempt_submits_nothing(tmp_path):
    stale = _attempt(tmp_path, 0)
    stale["_claim_driver_attempt"]()
    _attempt(tmp_path, 1)["_claim_driver_attempt"]()
    calls = []
    script = tmp_path / "run_solver.sh"
    script.write_text("#!/bin/bash\n", encoding="utf-8")
    (tmp_path / "run_tune_iter.sh").write_text("#!/bin/bash\n", encoding="utf-8")
    stale.update(
        {
            "subprocess": type("S", (), {"run": staticmethod(lambda argv, **kw: calls.append(argv))}),
            "_nested_sbatch_env": lambda: {},
            "remote_iter_dir": tmp_path,
        }
    )
    for name in ("_submit_job", "_submit_post3d_driver", "_submit_prepared_sbatch"):
        exec(_driver_function(name), stale)

    with pytest.raises(SystemExit):
        stale["_submit_job"](script)
    with pytest.raises(SystemExit):
        stale["_submit_post3d_driver"](PREOP)
    with pytest.raises(SystemExit):
        stale["_submit_prepared_sbatch"](["sbatch", "--parsable", str(script)])

    assert calls == []


@pytest.mark.parametrize("handoff_job, removed", [("48000000", True), (JOB, False)])
def test_pre3d_decision_keeps_a_handoff_from_another_attempt_of_this_job(tmp_path, handoff_job, removed):
    attempt = _attempt(tmp_path, 1)
    attempt["_claim_driver_attempt"]()
    handoff_path = tmp_path / "results" / "iteration_handoff.json"
    handoff_path.write_text(json.dumps({"pre3d_job_id": handoff_job, "post3d_job_id": POST3D}), encoding="utf-8")

    attempt["_write_driver_results"]({})

    assert handoff_path.exists() is not removed
    assert (tmp_path / "results" / "iteration_decision.json").exists()
    warnings = json.loads((tmp_path / "logs" / "iteration_driver_log.json").read_text(encoding="utf-8"))["warnings"]
    assert warnings == ([] if removed else [f"kept {handoff_path}: another attempt of job {JOB} handed off to a post-3D driver"])


def test_every_sbatch_call_and_result_write_is_guarded():
    template = TEMPLATE.read_text()
    driver = template[template.index("from __future__ import annotations") :]
    functions = {
        body.split("(")[0][len("def ") :]: body.split("\n\n\n")[0]
        for body in re.split(r"\n(?=def )", driver)
        if body.startswith("def ")
    }
    # Every subprocess call except the read-only squeue/sacct queries is an sbatch.
    read_only = ["_job_attempt_history", "_query_state"]
    calling = sorted(name for name, body in functions.items() if "subprocess.run(" in body)
    submitting = [name for name in calling if name not in read_only]
    assert calling == sorted(read_only + submitting)
    assert submitting == ["_submit_job", "_submit_post3d_driver", "_submit_prepared_sbatch"]
    for name in read_only:
        assert "sbatch" not in functions[name], name
    for name in submitting:
        assert "_exit_if_superseded(" in functions[name], name

    main = driver[driver.index("\n_claim_driver_attempt()\n") :]
    assert main.index("_claim_driver_attempt()") < main.index("\ntry:\n")
    assert driver.count('"iteration_driver_log.json").open("w"') == 1
    assert main.rstrip().endswith("\n_write_driver_results(metrics_payload)\nPY")
    guarded_write = (
        '_exit_if_superseded("writing zerod_pre_mapping_metrics.json")\n'
        '                    write_iteration_metrics(remote_results_dir / "zerod_pre_mapping_metrics.json"'
    )
    assert guarded_write in driver
