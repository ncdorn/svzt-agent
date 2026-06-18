from __future__ import annotations

import pytest

from svztagent.core.errors import PlanValidationError
from svztagent.core.plan import ExecutionPlan, PlanStep, StepCategory
from svztagent.core.plan_validate import assert_valid_execution_plan, validate_execution_plan


RUNS_ROOT = "/scratch/users/ndorn/svzt_runs"


def _step(
    step_id: str,
    category: StepCategory,
    *,
    dependencies: list[str] | None = None,
    remote_write: list[str] | None = None,
) -> PlanStep:
    return PlanStep(
        step_id=step_id,
        name=step_id,
        category=category,
        description=f"{step_id} description",
        dependencies=dependencies or [],
        remote_paths={"read": [], "write": remote_write or []},
    )


def _base_plan(steps: list[PlanStep]) -> ExecutionPlan:
    return ExecutionPlan(
        plan_id="plan-run-123-tune_trees",
        workflow_name="tune_trees",
        run_id="run-123",
        cluster="sherlock",
        patient="TST-STAN-x",
        created_at="2026-03-09T00:00:00+00:00",
        manifest_path="/tmp/runs/run-123/manifest.yaml",
        local_run_dir="/tmp/runs/run-123",
        remote_run_dir=f"{RUNS_ROOT}/run-123",
        steps=steps,
    )


def test_execution_plan_validation_duplicate_step_ids_fail():
    plan = _base_plan(
        [
            _step("s01", StepCategory.RESOLVE_PATHS),
            _step("s01", StepCategory.FINALIZE_MANIFEST),
        ]
    )
    results = validate_execution_plan(plan, runs_root=RUNS_ROOT)
    assert results.is_valid is False
    assert any(item.code == "duplicate_step_id" for item in results.errors)


def test_execution_plan_validation_duplicate_dependencies_fail():
    plan = _base_plan(
        [
            _step("s01", StepCategory.RESOLVE_PATHS),
            _step("s02", StepCategory.PULL_ARTIFACTS, dependencies=["s01", "s01"]),
        ]
    )
    with pytest.raises(PlanValidationError, match="duplicate_dependency"):
        assert_valid_execution_plan(plan, runs_root=RUNS_ROOT)


def test_execution_plan_validation_missing_dependency_fails():
    plan = _base_plan(
        [
            _step("s01", StepCategory.RESOLVE_PATHS),
            _step("s02", StepCategory.PULL_ARTIFACTS, dependencies=["s99_missing"]),
        ]
    )
    with pytest.raises(PlanValidationError, match="missing_dependency"):
        assert_valid_execution_plan(plan, runs_root=RUNS_ROOT)


def test_execution_plan_validation_remote_write_outside_runs_root_fails():
    plan = _base_plan(
        [
            _step("s01", StepCategory.RESOLVE_PATHS),
            _step(
                "s02",
                StepCategory.PULL_ARTIFACTS,
                dependencies=["s01"],
                remote_write=["/tmp/not_allowed/run-123"],
            ),
        ]
    )
    with pytest.raises(PlanValidationError, match="remote_write_outside_runs_root"):
        assert_valid_execution_plan(plan, runs_root=RUNS_ROOT)


def test_execution_plan_validation_missing_terminal_step_fails():
    plan = _base_plan(
        [
            _step("s01", StepCategory.RESOLVE_PATHS),
            _step("s02", StepCategory.MONITOR_JOB, dependencies=["s01"]),
        ]
    )
    with pytest.raises(PlanValidationError, match="missing_terminal_step"):
        assert_valid_execution_plan(plan, runs_root=RUNS_ROOT)
