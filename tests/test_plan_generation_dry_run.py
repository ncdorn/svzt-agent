from __future__ import annotations

import pytest

from svztagent.hpc.ssh import SshCommandBuilder
from svztagent.workflows.tune_trees import plan_tune_trees


def test_plan_contains_expected_steps_in_order(sample_config_files):
    plan = plan_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-123",
    )

    step_ids = [step.step_id for step in plan.steps]
    assert step_ids == [
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
    assert plan.summary["dry_run_only"] is True
    assert (sample_config_files / "runs" / "run-123" / "execution_plan.json").exists()
    assert (sample_config_files / "runs" / "run-123" / "execution_plan.yaml").exists()
    assert (sample_config_files / "runs" / "run-123" / "progress_tracker.yaml").exists()


def test_remote_commands_are_allowlisted():
    ssh = SshCommandBuilder()
    cmd = ssh.build_remote_command("ndorn", "sherlock.stanford.edu", ["mkdir", "-p", "/tmp/x"])
    assert cmd[0] == "ssh"

    with pytest.raises(Exception):
        ssh.build_remote_command("ndorn", "sherlock.stanford.edu", ["rm", "-rf", "/tmp/x"])
