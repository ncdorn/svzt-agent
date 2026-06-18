from __future__ import annotations

import json
from pathlib import Path

import pytest
from svztagent.core.manifest import (
    advance_iteration,
    mark_iteration_decision,
    mark_iteration_submitted,
    read_manifest,
    record_lifecycle_transition,
    resolve_submitted_job_id,
    write_manifest,
)
from svztagent.core.errors import ConfigError
from svztagent.core.state import RunLifecycleState
from svztagent.hpc.fake import FakeFileTransferAdapter, FakeRemoteExecAdapter, FakeSchedulerAdapter
from svztagent.hpc.interfaces import CommandResult, ExecutionMode
from svztagent.workflows.tune_trees import (
    _stage_tune_inputs,
    advance_tune_iteration,
    init_run_workspace,
    plan_tune_trees,
)


def test_iteration_tracker_defaults_and_submission_resolution(sample_config_files):
    paths, _ctx = init_run_workspace(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-iter-001",
    )
    manifest = read_manifest(paths.manifest)
    assert manifest.tuning_iteration_tracker.current_iteration == 1

    manifest = mark_iteration_submitted(
        manifest,
        iteration=1,
        tune_job_id="12345",
        local_dir=str(paths.run_dir / "iterations" / "iter-01"),
        remote_dir="/scratch/users/ndorn/svzt_runs/run-iter-001/iterations/iter-01",
        job_script_path="/scratch/users/ndorn/svzt_runs/run-iter-001/iterations/iter-01/run_tune_iter.sh",
    )
    assert resolve_submitted_job_id(manifest) == "12345"


def test_iteration_tracker_decision_and_advance(sample_config_files):
    paths, _ctx = init_run_workspace(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-iter-002",
    )
    manifest = read_manifest(paths.manifest)

    manifest = mark_iteration_decision(
        manifest,
        iteration=1,
        decision="not_close",
        metrics={"mpa_sys": 40.0, "mpa_dia": 15.0, "mpa_mean": 25.0, "rpa_split": 0.42},
        deltas={"mpa_sys": 10.0, "mpa_dia": 8.0, "mpa_mean": 7.0, "rpa_split": 0.1},
        regenerated_config_path=str(paths.run_dir / "iterations" / "iter-01" / "results" / "simplified_zerod_tuned_RRI.json"),
    )
    manifest = advance_iteration(manifest)
    assert manifest.tuning_iteration_tracker.current_iteration == 2
    assert any(rec.iteration == 2 for rec in manifest.tuning_iteration_tracker.iterations)


def test_advance_tune_iteration_no_submit(sample_config_files):
    paths, _ctx = init_run_workspace(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-iter-003",
    )
    manifest = read_manifest(paths.manifest)
    manifest = mark_iteration_decision(
        manifest,
        iteration=1,
        decision="not_close",
        regenerated_config_path=str(paths.run_dir / "iterations" / "iter-01" / "results" / "simplified_zerod_tuned_RRI.json"),
    )
    write_manifest(manifest, paths.manifest)

    result = advance_tune_iteration(
        workspace_root=sample_config_files,
        run_id="run-iter-003",
        execute=False,
    )
    assert result.action == "advanced_no_submit"
    assert result.next_iteration == 2

    updated = read_manifest(paths.manifest)
    assert updated.tuning_iteration_tracker.current_iteration == 2


def test_advance_tune_iteration_max_iter_failure(sample_config_files):
    paths, _ctx = init_run_workspace(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-iter-004",
    )
    manifest = read_manifest(paths.manifest)
    manifest.tuning_iteration_tracker.max_iterations = 1
    manifest = mark_iteration_decision(manifest, iteration=1, decision="not_close")
    write_manifest(manifest, paths.manifest)

    result = advance_tune_iteration(
        workspace_root=sample_config_files,
        run_id="run-iter-004",
        execute=False,
    )
    assert result.action == "max_iter_failed"
    updated = read_manifest(paths.manifest)
    assert updated.tuning_iteration_tracker.status == "failed_max_iter"


def test_advance_tune_iteration_allows_raising_max_iterations(sample_config_files):
    paths, _ctx = init_run_workspace(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-iter-raise-cap",
    )
    manifest = read_manifest(paths.manifest)
    manifest.tuning_iteration_tracker.max_iterations = 1
    manifest = mark_iteration_decision(manifest, iteration=1, decision="not_close")
    write_manifest(manifest, paths.manifest)

    result = advance_tune_iteration(
        workspace_root=sample_config_files,
        run_id="run-iter-raise-cap",
        max_iterations=2,
        execute=False,
    )

    assert result.action == "advanced_no_submit"
    assert result.next_iteration == 2
    updated = read_manifest(paths.manifest)
    assert updated.tuning_iteration_tracker.current_iteration == 2
    assert updated.tuning_iteration_tracker.max_iterations == 2


def test_advance_tune_iteration_rejects_nonpositive_max_iterations(sample_config_files):
    paths, _ctx = init_run_workspace(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-iter-lower-cap",
    )

    with pytest.raises(ConfigError, match="max_iterations must be >= 1"):
        advance_tune_iteration(
            workspace_root=sample_config_files,
            run_id="run-iter-lower-cap",
            max_iterations=0,
            execute=False,
        )


def test_advance_tune_iteration_rejects_max_below_current_iteration(sample_config_files):
    paths, _ctx = init_run_workspace(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-iter-below-current-cap",
    )
    manifest = read_manifest(paths.manifest)
    manifest.tuning_iteration_tracker.current_iteration = 2
    write_manifest(manifest, paths.manifest)

    with pytest.raises(ConfigError, match="cannot be lower than the current iteration"):
        advance_tune_iteration(
            workspace_root=sample_config_files,
            run_id="run-iter-below-current-cap",
            max_iterations=1,
            execute=False,
        )


def test_advance_tune_iteration_needs_review_pauses_without_submit(sample_config_files):
    paths, _ctx = init_run_workspace(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-iter-needs-review",
    )
    manifest = read_manifest(paths.manifest)
    manifest = mark_iteration_decision(
        manifest,
        iteration=1,
        decision="needs_review",
    )
    write_manifest(manifest, paths.manifest)

    transfer = FakeFileTransferAdapter()
    scheduler = FakeSchedulerAdapter()
    remote = FakeRemoteExecAdapter()
    result = advance_tune_iteration(
        workspace_root=sample_config_files,
        run_id="run-iter-needs-review",
        execute=True,
        transfer_adapter=transfer,
        scheduler_adapter=scheduler,
        remote_exec_adapter=remote,
    )

    assert result.action == "paused_needs_review"
    assert result.next_iteration is None
    assert result.submitted_job_id is None
    assert len(scheduler.submit_calls) == 0

    updated = read_manifest(paths.manifest)
    assert updated.tuning_iteration_tracker.status == "paused_review"
    current = next(
        rec
        for rec in updated.tuning_iteration_tracker.iterations
        if rec.iteration == updated.tuning_iteration_tracker.current_iteration
    )
    assert current.decision == "needs_review"
    assert current.postop_submission_requested is False


def test_advance_tune_iteration_execute_submits_when_not_close(sample_config_files):
    paths, _ctx = init_run_workspace(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-iter-009",
    )
    plan_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-iter-009",
    )
    manifest = read_manifest(paths.manifest)
    manifest = mark_iteration_decision(
        manifest,
        iteration=1,
        decision="not_close",
        regenerated_config_path=str(
            paths.run_dir / "iterations" / "iter-01" / "results" / "simplified_zerod_tuned_RRI.json"
        ),
    )
    manifest = record_lifecycle_transition(manifest, to_state=RunLifecycleState.SUBMITTED)
    manifest = record_lifecycle_transition(manifest, to_state=RunLifecycleState.COMPLETED)
    write_manifest(manifest, paths.manifest)

    transfer = FakeFileTransferAdapter()
    scheduler = FakeSchedulerAdapter()
    remote = FakeRemoteExecAdapter()
    result = advance_tune_iteration(
        workspace_root=sample_config_files,
        run_id="run-iter-009",
        execute=True,
        transfer_adapter=transfer,
        scheduler_adapter=scheduler,
        remote_exec_adapter=remote,
    )

    assert result.action == "advanced_and_submitted"
    assert result.next_iteration == 2
    assert len(scheduler.submit_calls) == 1


def test_advance_tune_iteration_hydrates_remote_regenerated_path(sample_config_files):
    paths, _ctx = init_run_workspace(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-iter-hydrates-remote-seed",
    )
    plan_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-iter-hydrates-remote-seed",
    )
    remote_seed_path = (
        "/scratch/users/ndorn/svzt_runs/run-iter-hydrates-remote-seed/"
        "iterations/iter-01/results/simplified_zerod_tuned_RRI.json"
    )
    decision_path = paths.run_dir / "iterations" / "iter-01" / "results" / "iteration_decision.json"
    decision_path.parent.mkdir(parents=True, exist_ok=True)
    decision_path.write_text(
        json.dumps(
            {
                "decision": "not_close",
                "metrics": {"mpa_mean": 31.0},
                "deltas": {"mpa_mean": 1.0},
                "regenerated_config_path": remote_seed_path,
            }
        ),
        encoding="utf-8",
    )

    manifest = read_manifest(paths.manifest)
    manifest = mark_iteration_decision(
        manifest,
        iteration=1,
        decision="not_close",
    )
    manifest = record_lifecycle_transition(manifest, to_state=RunLifecycleState.SUBMITTED)
    manifest = record_lifecycle_transition(manifest, to_state=RunLifecycleState.COMPLETED)
    write_manifest(manifest, paths.manifest)

    class CopyingTransfer(FakeFileTransferAdapter):
        def pull(self, remote_path: str, local_path: str) -> CommandResult:
            self.pull_calls.append((remote_path, local_path))
            dst = Path(local_path) / Path(remote_path).name
            dst.parent.mkdir(parents=True, exist_ok=True)
            dst.write_text("{\"source\": \"hydrated-remote\"}", encoding="utf-8")
            return CommandResult(
                argv=["rsync", remote_path, local_path],
                returncode=0,
                stdout="pulled",
                stderr="",
                dry_run=False,
            )

    transfer = CopyingTransfer()
    result = advance_tune_iteration(
        workspace_root=sample_config_files,
        run_id="run-iter-hydrates-remote-seed",
        execute=True,
        transfer_adapter=transfer,
        scheduler_adapter=FakeSchedulerAdapter(),
        remote_exec_adapter=FakeRemoteExecAdapter(),
    )

    assert result.action == "advanced_and_submitted"
    assert transfer.pull_calls == [
        (
            remote_seed_path,
            str(paths.run_dir / "iterations" / "iter-02" / "inputs" / "_remote_seed_pull"),
        )
    ]
    staged_seed = paths.run_dir / "iterations" / "iter-02" / "inputs" / "simplified_nonlinear_zerod.json"
    assert staged_seed.exists()
    assert staged_seed.read_text(encoding="utf-8") == "{\"source\": \"hydrated-remote\"}"

    updated = read_manifest(paths.manifest)
    iter1 = next(rec for rec in updated.tuning_iteration_tracker.iterations if rec.iteration == 1)
    assert iter1.regenerated_config_path == remote_seed_path


def test_full_pa_advance_switches_to_reduced_rri_after_iteration1(sample_config_files):
    patient_alias = "TST-STAN-x"
    active_patient_path = sample_config_files / "remote_data" / "active" / patient_alias
    permanent_patient_path = sample_config_files / "remote_data" / "permanent" / patient_alias
    (sample_config_files / "config" / "patients.yaml").write_text(
        f"""
patients:
  - alias: "{patient_alias}"
    permanent_remote_path: "{permanent_patient_path.as_posix()}"
    data_policy: "read_only"
    tuning:
      impedance:
        tuning_model: "full_pa"
""".strip()
        + "\n",
        encoding="utf-8",
    )

    paths, _ctx = init_run_workspace(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias=patient_alias,
        run_id="run-iter-full-pa-to-rri",
    )
    plan_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias=patient_alias,
        run_id="run-iter-full-pa-to-rri",
    )
    reduced_seed = (
        paths.run_dir
        / "iterations"
        / "iter-01"
        / "results"
        / "simplified_zerod_tuned_RRI.json"
    )
    reduced_seed.parent.mkdir(parents=True, exist_ok=True)
    reduced_seed.write_text("{\"reduced\": true}", encoding="utf-8")

    manifest = read_manifest(paths.manifest)
    manifest = mark_iteration_decision(
        manifest,
        iteration=1,
        decision="not_close",
        regenerated_config_path=str(reduced_seed),
    )
    manifest = record_lifecycle_transition(manifest, to_state=RunLifecycleState.SUBMITTED)
    manifest = record_lifecycle_transition(manifest, to_state=RunLifecycleState.COMPLETED)
    write_manifest(manifest, paths.manifest)

    result = advance_tune_iteration(
        workspace_root=sample_config_files,
        run_id="run-iter-full-pa-to-rri",
        execute=True,
        transfer_adapter=FakeFileTransferAdapter(),
        scheduler_adapter=FakeSchedulerAdapter(),
        remote_exec_adapter=FakeRemoteExecAdapter(),
    )

    assert result.action == "advanced_and_submitted"
    iter2_inputs = paths.run_dir / "iterations" / "iter-02" / "inputs"
    assert (iter2_inputs / "simplified_nonlinear_zerod.json").exists()
    assert not (iter2_inputs / "full_pa_zerod.json").exists()
    rendered_script = (
        paths.run_dir / "iterations" / "iter-02" / "run_tune_iter.sh"
    ).read_text(encoding="utf-8")
    assert '"tuning_model": "rri"' in rendered_script


def test_advance_tune_iteration_execute_max_iter_failed_does_not_submit(sample_config_files):
    paths, _ctx = init_run_workspace(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-iter-010",
    )
    plan_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-iter-010",
    )
    manifest = read_manifest(paths.manifest)
    manifest.tuning_iteration_tracker.max_iterations = 1
    manifest = mark_iteration_decision(manifest, iteration=1, decision="not_close")
    manifest = record_lifecycle_transition(manifest, to_state=RunLifecycleState.SUBMITTED)
    manifest = record_lifecycle_transition(manifest, to_state=RunLifecycleState.COMPLETED)
    write_manifest(manifest, paths.manifest)

    transfer = FakeFileTransferAdapter()
    scheduler = FakeSchedulerAdapter()
    remote = FakeRemoteExecAdapter()
    result = advance_tune_iteration(
        workspace_root=sample_config_files,
        run_id="run-iter-010",
        execute=True,
        transfer_adapter=transfer,
        scheduler_adapter=scheduler,
        remote_exec_adapter=remote,
    )

    assert result.action == "max_iter_failed"
    assert len(scheduler.submit_calls) == 0


def test_stage_tune_inputs_uses_local_fallback_seed(sample_config_files):
    paths, _ctx = init_run_workspace(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-iter-005",
    )
    plan = plan_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-iter-005",
    )

    previous_seed = paths.run_dir / "iterations" / "iter-01" / "results" / "simplified_zerod_tuned_RRI.json"
    previous_seed.parent.mkdir(parents=True, exist_ok=True)
    previous_seed.write_text("{\"example\": true}", encoding="utf-8")

    _stage_tune_inputs(
        paths,
        plan,
        patient_alias="TST-STAN-x",
        iteration=2,
        seed_config_path="/does/not/exist.json",
    )

    staged_seed = paths.run_dir / "iterations" / "iter-02" / "inputs" / "simplified_nonlinear_zerod.json"
    assert staged_seed.exists()
    assert staged_seed.read_text(encoding="utf-8") == "{\"example\": true}"


def test_stage_tune_inputs_prefers_pulled_outputs_seed(sample_config_files):
    paths, _ctx = init_run_workspace(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-iter-005b",
    )
    plan = plan_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-iter-005b",
    )

    local_previous_seed = (
        paths.run_dir / "iterations" / "iter-01" / "results" / "simplified_zerod_tuned_RRI.json"
    )
    local_previous_seed.parent.mkdir(parents=True, exist_ok=True)
    local_previous_seed.write_text("{\"source\": \"local\"}", encoding="utf-8")

    pulled_previous_seed = (
        paths.pulled_outputs
        / "iterations"
        / "iter-01"
        / "results"
        / "simplified_zerod_tuned_RRI.json"
    )
    pulled_previous_seed.parent.mkdir(parents=True, exist_ok=True)
    pulled_previous_seed.write_text("{\"source\": \"pulled\"}", encoding="utf-8")

    _stage_tune_inputs(
        paths,
        plan,
        patient_alias="TST-STAN-x",
        iteration=2,
        seed_config_path="/does/not/exist.json",
    )

    staged_seed = paths.run_dir / "iterations" / "iter-02" / "inputs" / "simplified_nonlinear_zerod.json"
    assert staged_seed.exists()
    assert staged_seed.read_text(encoding="utf-8") == "{\"source\": \"pulled\"}"


def test_stage_tune_inputs_pulls_remote_previous_iteration_seed(sample_config_files):
    paths, _ctx = init_run_workspace(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-iter-005c",
    )
    plan = plan_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-iter-005c",
    )
    remote_seed_path = (
        "/scratch/users/ndorn/svzt_runs/run-iter-005c/"
        "iterations/iter-01/results/simplified_zerod_tuned_RRI.json"
    )

    class CopyingTransfer(FakeFileTransferAdapter):
        def pull(self, remote_path: str, local_path: str) -> CommandResult:
            self.pull_calls.append((remote_path, local_path))
            dst = Path(local_path) / Path(remote_path).name
            dst.parent.mkdir(parents=True, exist_ok=True)
            dst.write_text("{\"source\": \"remote\"}", encoding="utf-8")
            return CommandResult(
                argv=["rsync", remote_path, local_path],
                returncode=0,
                stdout="pulled",
                stderr="",
                dry_run=False,
            )

    _stage_tune_inputs(
        paths,
        plan,
        patient_alias="TST-STAN-x",
        iteration=2,
        seed_config_path=remote_seed_path,
        transfer_adapter=CopyingTransfer(),
        mode=ExecutionMode.EXECUTE,
    )

    staged_seed = paths.run_dir / "iterations" / "iter-02" / "inputs" / "simplified_nonlinear_zerod.json"
    assert staged_seed.exists()
    assert staged_seed.read_text(encoding="utf-8") == "{\"source\": \"remote\"}"


def test_stage_tune_inputs_iteration1_uses_configured_seed_path(sample_config_files):
    paths, _ctx = init_run_workspace(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-iter-006",
    )
    plan = plan_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-iter-006",
    )
    configured_seed = paths.run_dir / "custom_seed.json"
    configured_seed.write_text("{\"seed\": 1}", encoding="utf-8")

    _stage_tune_inputs(
        paths,
        plan,
        patient_alias="TST-STAN-x",
        iteration=1,
        patient_assets={
            "iteration1_seed_source": "path",
            "iteration1_seed_path": str(configured_seed),
        },
    )

    staged_seed = paths.run_dir / "iterations" / "iter-01" / "inputs" / "simplified_nonlinear_zerod.json"
    assert staged_seed.exists()
    assert staged_seed.read_text(encoding="utf-8") == "{\"seed\": 1}"


def test_stage_tune_inputs_full_pa_uses_full_pa_seed_filename(sample_config_files):
    paths, _ctx = init_run_workspace(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-iter-full-pa-seed",
    )
    plan = plan_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-iter-full-pa-seed",
    )
    configured_seed = paths.run_dir / "full_seed.json"
    configured_seed.write_text("{\"full\": true}", encoding="utf-8")

    _stage_tune_inputs(
        paths,
        plan,
        patient_alias="TST-STAN-x",
        iteration=1,
        seed_input_filename="full_pa_zerod.json",
        patient_assets={
            "iteration1_seed_source": "path",
            "iteration1_seed_path": str(configured_seed),
        },
    )

    staged_seed = paths.run_dir / "iterations" / "iter-01" / "inputs" / "full_pa_zerod.json"
    reduced_seed = (
        paths.run_dir
        / "iterations"
        / "iter-01"
        / "inputs"
        / "simplified_nonlinear_zerod.json"
    )
    assert staged_seed.exists()
    assert staged_seed.read_text(encoding="utf-8") == "{\"full\": true}"
    assert not reduced_seed.exists()


def test_stage_tune_inputs_iteration1_generate_leaves_seed_for_remote_driver(sample_config_files):
    paths, _ctx = init_run_workspace(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-iter-007",
    )
    plan = plan_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-iter-007",
    )
    local_preop = sample_config_files / "local-preop-mesh-complete"
    (local_preop / "mesh-surfaces").mkdir(parents=True, exist_ok=True)
    local_targets = sample_config_files / "local-clinical_targets.csv"
    local_targets.write_text("target,value\n", encoding="utf-8")
    local_inflow = sample_config_files / "local-inflow.csv"
    local_inflow.write_text("t,q\n0,0\n", encoding="utf-8")

    _stage_tune_inputs(
        paths,
        plan,
        patient_alias="TST-STAN-x",
        iteration=1,
        patient_assets={
            "iteration1_seed_source": "generate",
            "iteration1_seed_path": "/tmp/unused_seed.json",
            "preop_mesh_complete_dir": str(local_preop),
            "clinical_targets": str(local_targets),
            "inflow": str(local_inflow),
        },
    )

    staged_seed = paths.run_dir / "iterations" / "iter-01" / "inputs" / "simplified_nonlinear_zerod.json"
    assert not staged_seed.exists()


def test_stage_tune_inputs_stages_local_inflow_when_available(sample_config_files):
    paths, _ctx = init_run_workspace(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-iter-inflow-stage",
    )
    plan = plan_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-iter-inflow-stage",
    )
    patient_inflow = (
        sample_config_files
        / "remote_data"
        / "permanent"
        / "TST-STAN-x"
        / "inflow.csv"
    )

    _stage_tune_inputs(
        paths,
        plan,
        patient_alias="TST-STAN-x",
        iteration=1,
        patient_assets={
            "iteration1_seed_source": "path",
            "iteration1_seed_path": str(
                sample_config_files
                / "remote_data"
                / "permanent"
                / "TST-STAN-x"
                / "simplified_nonlinear_zerod.json"
            ),
            "inflow": str(patient_inflow),
        },
    )

    staged_inflow = paths.run_dir / "iterations" / "iter-01" / "inputs" / "inflow.csv"
    assert staged_inflow.exists()
    assert staged_inflow.read_text(encoding="utf-8") == patient_inflow.read_text(encoding="utf-8")


def test_stage_tune_inputs_iteration1_path_missing_leaves_seed_for_remote_driver(
    sample_config_files
):
    paths, _ctx = init_run_workspace(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-iter-008",
    )
    plan = plan_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-iter-008",
    )

    _stage_tune_inputs(
        paths,
        plan,
        patient_alias="TST-STAN-x",
        iteration=1,
        patient_assets={
            "iteration1_seed_source": "path",
            "iteration1_seed_path": str(paths.run_dir / "missing_seed.json"),
        },
    )

    staged_seed = paths.run_dir / "iterations" / "iter-01" / "inputs" / "simplified_nonlinear_zerod.json"
    assert not staged_seed.exists()


def test_stage_tune_inputs_iteration1_remote_path_pulls_seed(sample_config_files):
    paths, _ctx = init_run_workspace(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-iter-011",
    )
    plan = plan_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-iter-011",
    )

    remote_seed = sample_config_files / "remote_data" / "active" / "TST-STAN-x" / "remote_seed.json"
    remote_seed.write_text("{\"remote\": true}", encoding="utf-8")

    class CopyingTransfer(FakeFileTransferAdapter):
        def pull(self, remote_path: str, local_path: str) -> CommandResult:
            result = super().pull(remote_path, local_path)
            src = Path(remote_path)
            dst = Path(local_path) / src.name
            dst.parent.mkdir(parents=True, exist_ok=True)
            dst.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")
            return result

    _stage_tune_inputs(
        paths,
        plan,
        patient_alias="TST-STAN-x",
        iteration=1,
        patient_assets={
            "iteration1_seed_source": "path",
            "iteration1_seed_path": str(remote_seed),
        },
        transfer_adapter=CopyingTransfer(),
        mode=ExecutionMode.EXECUTE,
    )

    staged_seed = paths.run_dir / "iterations" / "iter-01" / "inputs" / "simplified_nonlinear_zerod.json"
    assert staged_seed.exists()
    assert staged_seed.read_text(encoding="utf-8") == "{\"remote\": true}"


def test_stage_tune_inputs_iteration1_remote_path_dry_run_skips_generate(
    sample_config_files,
):
    paths, _ctx = init_run_workspace(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-iter-012",
    )
    plan = plan_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-iter-012",
    )
    _stage_tune_inputs(
        paths,
        plan,
        patient_alias="TST-STAN-x",
        iteration=1,
        patient_assets={
            "iteration1_seed_source": "path",
            "iteration1_seed_path": "/scratch/users/ndorn/models/PPAS/tof-stent/TST-STAN-x/remote_seed.json",
        },
        transfer_adapter=FakeFileTransferAdapter(),
        mode=ExecutionMode.DRY_RUN,
    )

    staged_seed = paths.run_dir / "iterations" / "iter-01" / "inputs" / "simplified_nonlinear_zerod.json"
    assert not staged_seed.exists()
