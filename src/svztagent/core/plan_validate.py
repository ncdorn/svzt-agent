"""Execution plan semantic validation."""

from __future__ import annotations

from collections import Counter

from svztagent.core.errors import PlanValidationError
from svztagent.core.paths import ensure_under_remote_root
from svztagent.core.plan import (
    ExecutionPlan,
    PlanValidationMessage,
    PlanValidationResults,
    StepCategory,
    ValidationLevel,
)


def _error(
    code: str,
    message: str,
    *,
    step_id: str | None = None,
) -> PlanValidationMessage:
    return PlanValidationMessage(
        code=code,
        message=message,
        step_id=step_id,
        level=ValidationLevel.ERROR,
    )


def validate_execution_plan(
    plan: ExecutionPlan,
    runs_root: str,
) -> PlanValidationResults:
    errors: list[PlanValidationMessage] = []
    warnings: list[PlanValidationMessage] = []

    step_ids = [step.step_id for step in plan.steps]
    duplicates = [sid for sid, count in Counter(step_ids).items() if count > 1]
    for step_id in sorted(duplicates):
        errors.append(
            _error(
                "duplicate_step_id",
                f"duplicate step_id detected: '{step_id}'",
                step_id=step_id,
            )
        )

    known_steps = set(step_ids)
    for step in plan.steps:
        if len(step.dependencies) != len(set(step.dependencies)):
            errors.append(
                _error(
                    "duplicate_dependency",
                    f"step '{step.step_id}' has duplicate dependency ids",
                    step_id=step.step_id,
                )
            )

        for dependency in step.dependencies:
            if dependency not in known_steps:
                errors.append(
                    _error(
                        "missing_dependency",
                        f"step '{step.step_id}' depends on unknown step '{dependency}'",
                        step_id=step.step_id,
                    )
                )

        for remote_write_path in step.remote_paths.write:
            if not ensure_under_remote_root(remote_write_path, runs_root):
                errors.append(
                    _error(
                        "remote_write_outside_runs_root",
                        f"remote write path '{remote_write_path}' must stay under runs_root '{runs_root}'",
                        step_id=step.step_id,
                    )
                )

    terminal_categories = {StepCategory.PULL_ARTIFACTS, StepCategory.FINALIZE_MANIFEST}
    if not any(step.category in terminal_categories for step in plan.steps):
        errors.append(
            _error(
                "missing_terminal_step",
                "plan must include at least one terminal step: pull_artifacts or finalize_manifest",
            )
        )

    return PlanValidationResults(
        is_valid=(len(errors) == 0),
        errors=errors,
        warnings=warnings,
    )


def assert_valid_execution_plan(
    plan: ExecutionPlan,
    runs_root: str,
) -> PlanValidationResults:
    results = validate_execution_plan(
        plan=plan,
        runs_root=runs_root,
    )
    if results.is_valid:
        return results

    messages = [
        f"[{item.code}] {item.message}"
        if item.step_id is None
        else f"[{item.code}] {item.step_id}: {item.message}"
        for item in results.errors
    ]
    raise PlanValidationError("Execution plan validation failed:\n- " + "\n- ".join(messages))
