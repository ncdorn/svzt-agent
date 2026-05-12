from __future__ import annotations

from svztagent.core.plan import (
    ExecutionPlan,
    PlanStep,
    StepCategory,
    load_plan_json,
    load_plan_yaml,
    write_plan_json,
    write_plan_yaml,
)


def _sample_plan() -> ExecutionPlan:
    return ExecutionPlan(
        plan_id="plan-run-777-tune_trees",
        workflow_name="tune_trees",
        run_id="run-777",
        cluster="sherlock",
        patient="TST-STAN-x",
        created_at="2026-03-09T00:00:00+00:00",
        manifest_path="/tmp/runs/run-777/manifest.yaml",
        local_run_dir="/tmp/runs/run-777",
        remote_run_dir="/scratch/users/ndorn/svzt_runs/run-777",
        steps=[
            PlanStep(
                step_id="s01",
                name="resolve_patient_path",
                category=StepCategory.RESOLVE_PATHS,
                description="Resolve patient path",
            ),
            PlanStep(
                step_id="s02",
                name="finalize_manifest",
                category=StepCategory.FINALIZE_MANIFEST,
                description="Finalize manifest",
                dependencies=["s01"],
            ),
        ],
        summary={"step_count": 2, "dry_run_only": True},
    )


def test_plan_json_roundtrip_stable(tmp_path):
    path = tmp_path / "execution_plan.json"
    plan = _sample_plan()
    write_plan_json(plan, path)
    loaded = load_plan_json(path)
    assert loaded.model_dump() == plan.model_dump()
    assert [step.step_id for step in loaded.steps] == ["s01", "s02"]


def test_plan_yaml_roundtrip_stable(tmp_path):
    path = tmp_path / "execution_plan.yaml"
    plan = _sample_plan()
    write_plan_yaml(plan, path)
    loaded = load_plan_yaml(path)
    assert loaded.model_dump() == plan.model_dump()
    assert [step.step_id for step in loaded.steps] == ["s01", "s02"]
