from __future__ import annotations

import pytest
from pydantic import ValidationError

from svztagent.core.plan import PlanStep, StepCategory


def test_plan_step_model_valid():
    step = PlanStep(
        step_id="s01",
        name="resolve_patient_path",
        category=StepCategory.RESOLVE_PATHS,
        description="Resolve patient path",
        inputs={"patient_alias": "TST-STAN-x"},
        outputs={"patient_path": "/scratch/path/TST-STAN-x"},
        dependencies=[],
        command_preview=["resolve_patient_path", "--patient", "TST-STAN-x"],
        local_paths={},
        remote_paths={"read": ["/scratch/path/TST-STAN-x"], "write": []},
        safety_notes=["Patient source path is read-only."],
    )
    assert step.step_id == "s01"
    assert step.category == StepCategory.RESOLVE_PATHS
    assert step.execution_policy.dry_run_only is True
    assert step.status.value == "pending"


def test_plan_step_model_missing_required_metadata_fails_fast():
    with pytest.raises(ValidationError):
        PlanStep(
            step_id="",
            name="resolve_patient_path",
            category=StepCategory.RESOLVE_PATHS,
            description="Resolve patient path",
        )


def test_plan_step_model_rejects_invalid_category():
    with pytest.raises(ValidationError):
        PlanStep(
            step_id="s01",
            name="resolve_patient_path",
            category="invalid_category",
            description="Resolve patient path",
        )
