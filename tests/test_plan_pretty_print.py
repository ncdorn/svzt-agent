from __future__ import annotations

from svztagent.core.plan_render import render_execution_plan
from svztagent.workflows.tune_trees import plan_tune_trees


def test_plan_pretty_print_renders_clean_summary(sample_config_files):
    plan = plan_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-print-001",
    )
    rendered = render_execution_plan(plan)

    assert "Plan ID: plan-run-print-001-tune_trees" in rendered
    assert "Workflow: tune_trees" in rendered
    assert "Steps:" in rendered
    assert "01. s01_resolve_patient_path [resolve_paths] status=pending" in rendered
    assert "11. s11_define_manifest_finalization [finalize_manifest] status=pending" in rendered
    assert "dependencies: s10_define_postprocessing_hook" in rendered
    assert "Validation: PASS (errors=0, warnings=0)" in rendered
