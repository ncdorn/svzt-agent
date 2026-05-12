from __future__ import annotations

from svztagent.core.manifest import read_manifest
from svztagent.core.plan import StepCategory, load_plan_json, load_plan_yaml
from svztagent.workflows.tune_trees import plan_tune_trees


def test_tune_plan_generation_base_case(sample_config_files):
    plan = plan_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-phase2-001",
    )

    expected_step_ids = [
        "s01_resolve_patient_path",
        "s02_init_or_verify_local_run_dir",
        "s03_snapshot_config_and_provenance",
        "s04_define_staged_local_inputs",
        "s05_define_remote_staging_destination",
        "s06_define_job_script_generation",
        "s07_define_scheduler_submission",
        "s08_define_monitoring_strategy",
        "s09_define_artifact_pullback",
        "s10_define_postprocessing_hook",
        "s11_define_manifest_finalization",
    ]

    assert [step.step_id for step in plan.steps] == expected_step_ids
    assert [step.category for step in plan.steps] == [
        StepCategory.RESOLVE_PATHS,
        StepCategory.INIT_RUN,
        StepCategory.SNAPSHOT_CONFIG,
        StepCategory.STAGE_INPUTS,
        StepCategory.PUSH_TO_CLUSTER,
        StepCategory.GENERATE_JOB_SCRIPT,
        StepCategory.SUBMIT_JOB,
        StepCategory.MONITOR_JOB,
        StepCategory.PULL_ARTIFACTS,
        StepCategory.POSTPROCESS,
        StepCategory.FINALIZE_MANIFEST,
    ]
    assert plan.validation_results.is_valid is True
    assert plan.local_run_dir.endswith("/runs/run-phase2-001")
    assert plan.manifest_path.endswith("/runs/run-phase2-001/manifest.yaml")
    assert plan.remote_run_dir.endswith("/run-phase2-001")
    assert plan.summary["current_iteration"] == 1
    assert "iterations/iter-01" in " ".join(plan.steps[3].command_preview)


def test_tune_plan_generation_writes_plan_and_manifest_updates(sample_config_files):
    plan = plan_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-phase2-002",
    )

    run_dir = sample_config_files / "runs" / "run-phase2-002"
    json_path = run_dir / "execution_plan.json"
    yaml_path = run_dir / "execution_plan.yaml"

    assert json_path.exists()
    assert yaml_path.exists()

    loaded_json = load_plan_json(json_path)
    loaded_yaml = load_plan_yaml(yaml_path)
    assert loaded_json.model_dump() == plan.model_dump()
    assert loaded_yaml.model_dump() == plan.model_dump()

    manifest = read_manifest(run_dir / "manifest.yaml")
    assert manifest.status == "planned"
    assert manifest.jobs[0]["mode"] == "preview"
    assert "execution_plan.json" in manifest.artifacts["plan_files"][0]
    assert "execution_plan.yaml" in manifest.artifacts["plan_files"][1]


def test_tune_plan_generation_command_previews_only(sample_config_files):
    plan = plan_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-phase2-003",
    )
    previews = [step.command_preview for step in plan.steps if step.command_preview]
    assert previews
    assert all(isinstance(command, list) for command in previews)
    assert any(command[0] == "rsync" for command in previews)
    assert any(command[0] == "ssh" for command in previews)
