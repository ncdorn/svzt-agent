"""The post-3D driver warns when the preop 3D job was requeued.

tst-stan-1-20261016: 3D job 48158596 hit NODE_FAIL after 1660 steps, Slurm
requeued it under the same job ID, and svMultiPhysics appended the rerun to
svZeroD_data (two header blocks), which broke the flow split (2026-10-19).
"""

from __future__ import annotations

import warnings
from pathlib import Path
from types import SimpleNamespace

TEMPLATE = Path(__file__).resolve().parents[1] / "src" / "svztagent" / "templates" / "slurm" / "job_template.sh"
JOB = "48158596"
REQUEUED_SACCT = (
    f"{JOB}|NODE_FAIL|2026-10-17T08:00:00|2026-10-17T11:00:00|sh03-01n01\n"
    f"{JOB}|COMPLETED|2026-10-19T02:00:00|2026-10-19T09:00:00|sh03-02n04\n"
)


def _driver_function(name: str) -> str:
    template = TEMPLATE.read_text()
    start = template.index(f"def {name}(")
    return template[start : template.index("\ndef ", start + 1)]


def _namespace(sacct_stdout: str = "", returncode: int = 0, **overrides) -> dict:
    calls = []

    def fake_run(argv, **_kwargs):
        calls.append(argv)
        return SimpleNamespace(returncode=returncode, stdout=sacct_stdout, stderr="")

    namespace = {
        "Path": Path,
        "subprocess": SimpleNamespace(run=fake_run),
        "log": {"steps": [], "warnings": []},
        "calls": calls,
    }
    namespace.update(overrides)
    for name in ("_job_attempt_history", "_warn_if_job_was_requeued", "_flow_split_metrics_recording_warnings"):
        exec(_driver_function(name), namespace)
    return namespace


def test_requeued_preop_job_records_attempt_history_and_warning():
    namespace = _namespace(REQUEUED_SACCT)

    namespace["_warn_if_job_was_requeued"](JOB, "preop_3d")

    assert namespace["calls"][0][:5] == ["sacct", "-D", "-X", "-j", JOB]
    log = namespace["log"]
    assert [a["state"] for a in log["preop_3d_attempt_history"]] == ["NODE_FAIL", "COMPLETED"]
    assert log["preop_3d_attempt_history"][0]["nodes"] == "sh03-01n01"
    (warning,) = log["warnings"]
    assert warning.startswith(f"preop_3d_requeued: job {JOB} ran 2 attempts")
    assert "NODE_FAIL@2026-10-17T08:00:00 on sh03-01n01" in warning


def test_single_attempt_preop_job_is_not_flagged():
    namespace = _namespace(f"{JOB}|COMPLETED|2026-10-19T02:00:00|2026-10-19T09:00:00|sh03-02n04\n")

    namespace["_warn_if_job_was_requeued"](JOB, "preop_3d")

    assert namespace["log"] == {"steps": [], "warnings": []}


def test_attempt_history_ignores_other_jobs_and_strips_cancel_suffix():
    namespace = _namespace(
        f"{JOB}|CANCELLED by 123|2026-10-17T08:00:00|2026-10-17T09:00:00|sh03-01n01\n"
        f"{JOB}.batch|COMPLETED|2026-10-17T08:00:00|2026-10-17T09:00:00|sh03-01n01\n"
        "\n"
    )

    assert namespace["_job_attempt_history"](JOB) == [
        {"state": "CANCELLED", "start": "2026-10-17T08:00:00", "end": "2026-10-17T09:00:00", "nodes": "sh03-01n01"}
    ]


def test_unavailable_sacct_is_recorded_without_failing():
    namespace = _namespace(returncode=1)

    namespace["_warn_if_job_was_requeued"](JOB, "preop_3d")

    assert namespace["log"]["warnings"] == [
        f"preop_3d_attempt_history_unavailable: sacct -D failed for job {JOB}"
    ]


def test_flow_split_copies_svzerod_data_warnings_to_driver_log(tmp_path):
    def fake_flow_split(*, simulation_dir):
        assert simulation_dir == tmp_path
        warnings.warn(f"{simulation_dir}/svZeroD_data has 2 header blocks (lines 1, 1662)", RuntimeWarning)
        return {"lpa_flow": 40.0, "rpa_flow": 60.0, "rpa_split": 0.6}

    namespace = _namespace(compute_flow_split_metrics=fake_flow_split)

    result = namespace["_flow_split_metrics_recording_warnings"](tmp_path)

    assert result["rpa_split"] == 0.6
    assert namespace["log"]["warnings"] == [
        f"flow_split_input: {tmp_path}/svZeroD_data has 2 header blocks (lines 1, 1662)"
    ]
