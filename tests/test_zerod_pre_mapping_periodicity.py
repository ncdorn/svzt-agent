"""zerod_pre_mapping_metrics records whether the 0D snapshot was periodic.

svZeroDTrees runs the tuning snapshot until its MPA pressure is periodic and
reports ``periodicity``; the driver copies it into the payload and warns when
it is missing (older install: snapshot cycles scored) or not converged.
"""

from __future__ import annotations

from pathlib import Path

import pytest

TEMPLATE = Path(__file__).resolve().parents[1] / "src" / "svztagent" / "templates" / "slurm" / "job_template.sh"


def _driver_function(name: str) -> str:
    template = TEMPLATE.read_text()
    start = template.index(f"def {name}(")
    return template[start : template.index("\ndef ", start + 1)]


@pytest.fixture
def periodicity_warnings():
    namespace: dict = {}
    exec(_driver_function("_zerod_periodicity_warnings"), namespace)
    return namespace["_zerod_periodicity_warnings"]


def test_converged_periodicity_has_no_warning(periodicity_warnings):
    assert periodicity_warnings({"converged": True, "cycles_used": 6, "final_drift_mmhg": 0.02}) == []


def test_missing_periodicity_warns_that_metrics_may_be_transient(periodicity_warnings):
    (warning,) = periodicity_warnings(None)
    assert "did not report periodicity" in warning
    assert "transient" in warning


def test_unconverged_periodicity_reports_cycles_and_drift(periodicity_warnings):
    (warning,) = periodicity_warnings({"converged": False, "cycles_used": 24, "final_drift_mmhg": 0.4})
    assert "not periodic after 24 cycles" in warning
    assert "0.4 mmHg" in warning


def test_pre_mapping_payload_records_periodicity():
    template = TEMPLATE.read_text()
    start = template.index("zerod_metrics = summarize_pulmonary_zerod_config(snapshot_path)")
    block = template[start : template.index("zerod_pre_mapping_metrics_written", start)]
    assert "_zerod_periodicity_warnings(zerod_periodicity)" in block
    assert '"periodicity": zerod_periodicity' in block
