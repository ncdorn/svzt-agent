from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from svztagent.core.errors import ConfigError
from svztagent.core.manifest import (
    mark_iteration_decision,
    read_manifest,
    record_lifecycle_transition,
    write_manifest,
)
from svztagent.core.state import RunLifecycleState
from svztagent.hpc.fake import (
    FakeFileTransferAdapter,
    FakeRemoteExecAdapter,
    FakeSchedulerAdapter,
)
from svztagent.hpc.interfaces import (
    CommandResult,
    ExecutionMode,
    SchedulerStatusResult,
    SubmitResult,
    SyncDirection,
)
from svztagent.workflows.postop import select_converged_preop_iteration
from svztagent.workflows.tune_trees import (
    advance_tune_iteration,
    continue_tune_iteration,
    init_run_workspace,
    plan_tune_trees,
    run_tune_trees,
    watch_and_auto_advance_tuning,
)


class CalibrationPublishingTransfer(FakeFileTransferAdapter):
    """Publish the upstream artifacts when the completion handoff pulls them."""

    def sync(
        self,
        local_dir: str,
        remote_dir: str,
        include: list[str] | None = None,
        exclude: list[str] | None = None,
        direction: SyncDirection = SyncDirection.PUSH,
    ) -> CommandResult:
        result = super().sync(
            local_dir=local_dir,
            remote_dir=remote_dir,
            include=include,
            exclude=exclude,
            direction=direction,
        )
        if direction == SyncDirection.PULL and remote_dir.endswith("/calibration/results"):
            destination = Path(local_dir)
            destination.mkdir(parents=True, exist_ok=True)
            (destination / "calibrated_full_pa_zerod.json").write_text(
                '{"calibrated": true}\n', encoding="utf-8"
            )
            (destination / "calibration_summary.json").write_text(
                json.dumps({"status": "ok"}), encoding="utf-8"
            )
        if direction == SyncDirection.PULL and remote_dir.endswith("/results/postprocess"):
            destination = Path(local_dir)
            destination.mkdir(parents=True, exist_ok=True)
            (destination / "postprocess_suite_metadata.json").write_text(
                '{}\n', encoding="utf-8"
            )
        return result


def _completed_status(job_id: str) -> SchedulerStatusResult:
    return SchedulerStatusResult(
        job_id=job_id,
        raw_state="COMPLETED",
        source="squeue",
        command=CommandResult(
            argv=["squeue"],
            returncode=0,
            stdout="COMPLETED",
            stderr="",
            dry_run=False,
        ),
    )


PATIENT_ALIAS = "TST-STAN-x"


def _write_full_pa_patient(sample_config_files, *, seed_policy: str = "calibrated_full_pa") -> None:
    permanent_path = sample_config_files / "remote_data" / "permanent" / PATIENT_ALIAS
    (sample_config_files / "config" / "patients.yaml").write_text(
        f"""
patients:
  - alias: "{PATIENT_ALIAS}"
    permanent_remote_path: "{permanent_path.as_posix()}"
    data_policy: "read_only"
    tuning:
      impedance:
        tuning_model: "full_pa"
      calibration:
        next_iteration_seed_policy: "{seed_policy}"
""".strip()
        + "\n",
        encoding="utf-8",
    )


def _completed_full_pa_run(
    sample_config_files,
    run_id: str,
    *,
    decision: str,
    max_iterations: int | None = None,
    seed_policy: str = "calibrated_full_pa",
):
    _write_full_pa_patient(sample_config_files, seed_policy=seed_policy)
    paths, _ = init_run_workspace(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias=PATIENT_ALIAS,
        run_id=run_id,
    )
    plan_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias=PATIENT_ALIAS,
        run_id=run_id,
    )
    tuned_model = paths.run_dir / "iterations" / "iter-01" / "results" / "svzerod_3d_coupling_tuned.json"
    tuned_model.parent.mkdir(parents=True, exist_ok=True)
    tuned_model.write_text('{"tuned": true}\n', encoding="utf-8")

    manifest = read_manifest(paths.manifest)
    if max_iterations is not None:
        manifest.tuning_iteration_tracker.max_iterations = max_iterations
    manifest = mark_iteration_decision(manifest, iteration=1, decision=decision)
    manifest = record_lifecycle_transition(manifest, to_state=RunLifecycleState.SUBMITTED)
    manifest = record_lifecycle_transition(manifest, to_state=RunLifecycleState.COMPLETED)
    write_manifest(manifest, paths.manifest)
    return paths


def _advance(sample_config_files, run_id, transfer, scheduler, remote, *, execute: bool = True):
    return advance_tune_iteration(
        workspace_root=sample_config_files,
        run_id=run_id,
        execute=execute,
        transfer_adapter=transfer,
        scheduler_adapter=scheduler,
        remote_exec_adapter=remote,
    )


def test_full_pa_not_close_calibrates_and_promotes_before_advancing(sample_config_files):
    run_id = "run-full-pa-calibration-cadence"
    paths = _completed_full_pa_run(sample_config_files, run_id, decision="not_close")

    transfer = CalibrationPublishingTransfer()
    scheduler = FakeSchedulerAdapter()
    remote = FakeRemoteExecAdapter()
    submitted = advance_tune_iteration(
        workspace_root=sample_config_files,
        run_id=run_id,
        execute=True,
        transfer_adapter=transfer,
        scheduler_adapter=scheduler,
        remote_exec_adapter=remote,
    )

    assert submitted.action == "calibration_submitted"
    assert submitted.next_iteration is None
    assert len(scheduler.submit_calls) == 2
    scheduled = read_manifest(paths.manifest)
    assert len(scheduled.preop_postprocess_runs) == 1
    assert len(scheduled.calibration_runs) == 1

    scheduler.queue_status_results([_completed_status(submitted.submitted_job_id or "calibration")])
    advanced = advance_tune_iteration(
        workspace_root=sample_config_files,
        run_id=run_id,
        execute=True,
        transfer_adapter=transfer,
        scheduler_adapter=scheduler,
        remote_exec_adapter=remote,
    )

    assert advanced.action == "advanced_and_submitted"
    assert advanced.next_iteration == 2
    updated = read_manifest(paths.manifest)
    first_iteration = updated.tuning_iteration_tracker.iterations[0]
    assert first_iteration.calibrated_seed_path is not None
    assert Path(first_iteration.calibrated_seed_path).is_file()
    assert updated.calibration_runs[0].promotion_status == "promoted"
    assert (
        paths.run_dir / "iterations" / "iter-02" / "inputs" / "full_pa_zerod.json"
    ).is_file()


def _submit_and_complete_calibration(sample_config_files, run_id, transfer, scheduler, remote):
    submitted = _advance(sample_config_files, run_id, transfer, scheduler, remote)
    assert submitted.action == "calibration_submitted"
    assert submitted.next_iteration is None
    scheduler.queue_status_results([_completed_status(submitted.submitted_job_id or "calibration")])
    return submitted


def test_full_pa_converged_still_calibrates_before_reporting_convergence(sample_config_files):
    run_id = "run-full-pa-converged-calibration"
    paths = _completed_full_pa_run(sample_config_files, run_id, decision="converged")
    transfer = CalibrationPublishingTransfer()
    scheduler = FakeSchedulerAdapter()
    remote = FakeRemoteExecAdapter()

    submitted = _advance(sample_config_files, run_id, transfer, scheduler, remote)
    assert submitted.action == "calibration_submitted"
    # postprocess + calibration only; no next tuning iteration
    assert len(scheduler.submit_calls) == 2
    scheduled = read_manifest(paths.manifest)
    assert len(scheduled.preop_postprocess_runs) == 1
    assert len(scheduled.calibration_runs) == 1

    pending_status = SchedulerStatusResult(
        job_id=submitted.submitted_job_id or "calibration",
        raw_state="RUNNING",
        source="squeue",
        command=CommandResult(argv=["squeue"], returncode=0, stdout="RUNNING", stderr="", dry_run=False),
    )
    scheduler.queue_status_results([pending_status])
    pending = _advance(sample_config_files, run_id, transfer, scheduler, remote)
    assert pending.action == "calibration_pending"

    scheduler.queue_status_results([_completed_status(submitted.submitted_job_id or "calibration")])
    converged = _advance(sample_config_files, run_id, transfer, scheduler, remote)
    assert converged.action == "already_converged"
    assert converged.next_iteration is None
    assert len(scheduler.submit_calls) == 2
    updated = read_manifest(paths.manifest)
    assert updated.tuning_iteration_tracker.current_iteration == 1
    assert updated.calibration_runs[0].promotion_status == "promoted"
    assert Path(updated.tuning_iteration_tracker.iterations[0].calibrated_seed_path).is_file()

    # Re-running after promotion is idempotent and submits nothing new.
    again = _advance(sample_config_files, run_id, transfer, scheduler, remote)
    assert again.action == "already_converged"
    assert len(scheduler.submit_calls) == 2


def test_full_pa_final_iteration_calibrates_before_max_iter_failure(sample_config_files):
    run_id = "run-full-pa-max-iter-calibration"
    paths = _completed_full_pa_run(
        sample_config_files, run_id, decision="not_close", max_iterations=1
    )
    transfer = CalibrationPublishingTransfer()
    scheduler = FakeSchedulerAdapter()
    remote = FakeRemoteExecAdapter()

    _submit_and_complete_calibration(sample_config_files, run_id, transfer, scheduler, remote)
    final = _advance(sample_config_files, run_id, transfer, scheduler, remote)

    assert final.action == "max_iter_failed"
    assert len(scheduler.submit_calls) == 2
    updated = read_manifest(paths.manifest)
    assert updated.calibration_runs[0].promotion_status == "promoted"


def test_full_pa_needs_review_pauses_without_calibration(sample_config_files):
    run_id = "run-full-pa-needs-review"
    paths = _completed_full_pa_run(sample_config_files, run_id, decision="needs_review")
    scheduler = FakeSchedulerAdapter()

    result = _advance(
        sample_config_files,
        run_id,
        CalibrationPublishingTransfer(),
        scheduler,
        FakeRemoteExecAdapter(),
    )

    assert result.action == "paused_needs_review"
    assert scheduler.submit_calls == []
    assert read_manifest(paths.manifest).calibration_runs == []


def test_full_pa_advance_without_execute_reports_required_calibration(sample_config_files):
    run_id = "run-full-pa-advance-preview"
    paths = _completed_full_pa_run(sample_config_files, run_id, decision="converged")
    scheduler = FakeSchedulerAdapter()

    result = _advance(
        sample_config_files,
        run_id,
        CalibrationPublishingTransfer(),
        scheduler,
        FakeRemoteExecAdapter(),
        execute=False,
    )

    assert result.action == "calibration_required"
    assert scheduler.submit_calls == []
    assert read_manifest(paths.manifest).calibration_runs == []


def test_continue_under_calibrated_full_pa_calibrates_before_advancing(sample_config_files):
    run_id = "run-full-pa-continue"
    paths = _completed_full_pa_run(sample_config_files, run_id, decision="needs_review")
    transfer = CalibrationPublishingTransfer()
    scheduler = FakeSchedulerAdapter()
    remote = FakeRemoteExecAdapter()

    forced = continue_tune_iteration(
        workspace_root=sample_config_files,
        run_id=run_id,
        execute=True,
        transfer_adapter=transfer,
        scheduler_adapter=scheduler,
        remote_exec_adapter=remote,
    )
    assert forced.action == "calibration_submitted"
    assert read_manifest(paths.manifest).tuning_iteration_tracker.current_iteration == 1

    scheduler.queue_status_results([_completed_status(forced.submitted_job_id or "calibration")])
    # advance-iter must honor the operator's not_close override.
    advanced = _advance(sample_config_files, run_id, transfer, scheduler, remote)
    assert advanced.action == "advanced_and_submitted"
    assert advanced.next_iteration == 2


def _write_patient(sample_config_files, *, tuning_model: str, seed_policy: str) -> None:
    permanent_path = sample_config_files / "remote_data" / "permanent" / PATIENT_ALIAS
    (sample_config_files / "config" / "patients.yaml").write_text(
        f"""
patients:
  - alias: "{PATIENT_ALIAS}"
    permanent_remote_path: "{permanent_path.as_posix()}"
    data_policy: "read_only"
    tuning:
      impedance:
        tuning_model: "{tuning_model}"
      calibration:
        next_iteration_seed_policy: "{seed_policy}"
""".strip()
        + "\n",
        encoding="utf-8",
    )


def _rendered_seed_generation(rendered: str) -> dict:
    marker = "seed_generation = json.loads(r'''"
    start = rendered.index(marker) + len(marker)
    return json.loads(rendered[start : rendered.index("''')", start)])


def _driver_python(rendered: str) -> str:
    body = re.split(r"<<'PY'[^\n]*\n", rendered)[2]
    return body[: body.index("\nPY\n")]


@pytest.mark.parametrize(
    ("seed_policy", "tuning_model", "expected_strategy"),
    [
        ("calibrated_full_pa", "full_pa", "calibrated_full_pa"),
        ("legacy_rri_after_first", "full_pa", "reduced_rri"),
        ("calibrated_full_pa", "rri", "reduced_rri"),
    ],
)
def test_driver_seed_generation_strategy_follows_policy(
    sample_config_files, seed_policy, tuning_model, expected_strategy
):
    _write_patient(sample_config_files, tuning_model=tuning_model, seed_policy=seed_policy)
    transfer = FakeFileTransferAdapter()
    result = run_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias=PATIENT_ALIAS,
        run_id=f"run-seed-{seed_policy.replace('_', '-')}-{tuning_model.replace('_', '-')}",
        mode=ExecutionMode.DRY_RUN,
        transfer_adapter=transfer,
        scheduler_adapter=FakeSchedulerAdapter(),
        remote_exec_adapter=FakeRemoteExecAdapter(),
    )

    rendered = result.local_job_script_path.read_text(encoding="utf-8")
    assert "{{SEED_GENERATION_JSON}}" not in rendered
    compile(_driver_python(rendered), "driver", "exec")
    spec = _rendered_seed_generation(rendered)
    assert spec["strategy"] == expected_strategy
    pushed = [remote for _, remote in transfer.push_calls]
    if expected_strategy == "reduced_rri":
        assert spec == {"strategy": "reduced_rri"}
        assert not any("/calibration/" in remote for remote in pushed)
        return

    run_root = f"{result.remote_run_dir}/iterations/iter-01"
    assert spec["postprocess"]["remote_script"] == f"{run_root}/postprocess/run_postprocess.sh"
    assert spec["postprocess"]["sbatch_argv"][:2] == ["sbatch", "--parsable"]
    assert spec["postprocess"]["sbatch_argv"][-1] == spec["postprocess"]["remote_script"]
    calibration = spec["calibration"]
    assert calibration["sbatch_argv"][-1] == f"{run_root}/calibration/run_calibration.sh"
    assert calibration["remote_config_path"] == f"{run_root}/calibration/inputs/calibrate_0d_from_3d.yaml"
    for remote in (
        spec["postprocess"]["remote_script"],
        calibration["remote_script"],
        calibration["remote_request_path"],
    ):
        assert remote in pushed
    local_calibration = result.local_job_script_path.parent / "calibration"
    script = (local_calibration / "run_calibration.sh").read_text(encoding="utf-8")
    assert "--dependency" not in script
    # Activation hooks run with nounset paused (Sherlock's /etc/bashrc reads PS1).
    assert "set -euo pipefail\nset +u\nsource ~/.bashrc\nconda activate svz\nset -u\n" in script
    postprocess_script = (
        result.local_job_script_path.parent / "postprocess" / "run_postprocess.sh"
    ).read_text(encoding="utf-8")
    assert "set +u\nsource ~/.bashrc\nconda activate svz\nset -u\n" in postprocess_script
    # The suite records the tuned model's lineage, which calibration requires.
    assert f'tuned_zerod_config = "{run_root}/results/svzerod_3d_coupling_tuned.json"' in postprocess_script
    assert 'postprocess_kwargs["tuned_zerod_config_path"] = tuned_zerod_config' in postprocess_script
    request = json.loads((local_calibration / "inputs" / "calibration_request.json").read_text())
    assert request["calibration"]["data_source"]["postprocess_metadata_json"] == (
        f"{run_root}/results/postprocess/postprocess_suite_metadata.json"
    )


def test_driver_submits_postprocess_then_dependent_calibration(sample_config_files, tmp_path):
    import ast
    import subprocess as real_subprocess

    _write_patient(sample_config_files, tuning_model="full_pa", seed_policy="calibrated_full_pa")
    result = run_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias=PATIENT_ALIAS,
        run_id="run-driver-seed-submit",
        mode=ExecutionMode.DRY_RUN,
        transfer_adapter=FakeFileTransferAdapter(),
        scheduler_adapter=FakeSchedulerAdapter(),
        remote_exec_adapter=FakeRemoteExecAdapter(),
    )
    rendered = result.local_job_script_path.read_text(encoding="utf-8")
    spec = _rendered_seed_generation(rendered)
    # Redirect the remote layout into tmp_path so the driver code can run locally.
    remote_root = result.remote_run_dir
    spec = json.loads(json.dumps(spec).replace(remote_root, str(tmp_path)))
    request_path = Path(spec["calibration"]["remote_request_path"])
    request_path.parent.mkdir(parents=True)
    request_path.write_text(
        (result.local_job_script_path.parent / "calibration" / "inputs" / "calibration_request.json").read_text()
    )
    tuned = tmp_path / "iterations" / "iter-01" / "results" / "svzerod_3d_coupling_tuned.json"
    tuned.parent.mkdir(parents=True)
    tuned.write_text("{}", encoding="utf-8")

    calls = []

    class FakeSubprocess:
        @staticmethod
        def run(argv, **_kwargs):
            calls.append(list(argv))
            return real_subprocess.CompletedProcess(argv, 0, stdout=f"{7000 + len(calls)}\n", stderr="")

    tree = ast.parse(_driver_python(rendered))
    wanted = {"_submit_prepared_sbatch", "_submit_calibrated_full_pa_seed_jobs"}
    module = ast.Module(
        body=[node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in wanted],
        type_ignores=[],
    )
    namespace = {
        "json": json,
        "Path": Path,
        "subprocess": FakeSubprocess,
        "seed_generation": spec,
        "log": {"warnings": []},
        "_nested_sbatch_env": lambda: {},
    }
    exec(compile(module, "driver", "exec"), namespace)

    report = namespace["_submit_calibrated_full_pa_seed_jobs"](tuned)

    assert report["status"] == "submitted", report
    assert report["postprocess"]["job_id"] == "7001"
    assert report["calibration"]["job_id"] == "7002"
    assert calls[0] == spec["postprocess"]["sbatch_argv"]
    assert calls[1][-2:] == ["--dependency=afterok:7001", spec["calibration"]["remote_script"]]
    written = json.loads(Path(spec["calibration"]["remote_config_path"]).read_text())
    assert written["paths"]["zerod_config"] == str(tuned.resolve())


def _write_driver_decision(paths, *, decision: str, status: str = "submitted") -> None:
    run_root = f"/scratch/svzt_runs/{paths.run_dir.name}/iterations/iter-01"
    manifest = read_manifest(paths.manifest)
    remote_run = manifest.execution.remote_run_dir or manifest.remote.get("remote_run_dir")
    if remote_run:
        run_root = f"{remote_run}/iterations/iter-01"
    seed = {
        "strategy": "calibrated_full_pa",
        "status": status,
        "tuned_zerod_config": f"{run_root}/results/svzerod_3d_coupling_tuned.json",
        "postprocess": {
            "remote_script": f"{run_root}/postprocess/run_postprocess.sh",
            "remote_results_dir": f"{run_root}/results/postprocess",
        },
        "calibration": {
            "remote_root": f"{run_root}/calibration",
            "remote_script": f"{run_root}/calibration/run_calibration.sh",
            "remote_config_path": f"{run_root}/calibration/inputs/calibrate_0d_from_3d.yaml",
        },
        "error": None if status == "submitted" else "RuntimeError: sbatch failed",
    }
    if status == "submitted":
        seed["postprocess"]["job_id"] = "8101"
        seed["calibration"]["job_id"] = "8102"
    decision_path = paths.run_dir / "iterations" / "iter-01" / "results" / "iteration_decision.json"
    decision_path.parent.mkdir(parents=True, exist_ok=True)
    decision_path.write_text(
        json.dumps({"decision": decision, "seed_generation": seed}), encoding="utf-8"
    )


@pytest.mark.parametrize(("decision", "final_action"), [("not_close", "advanced_and_submitted"), ("converged", "already_converged")])
def test_advance_records_driver_submitted_calibration_without_resubmitting(
    sample_config_files, decision, final_action
):
    run_id = f"run-driver-owned-{decision.replace('_', '-')}"
    paths = _completed_full_pa_run(sample_config_files, run_id, decision=decision)
    _write_driver_decision(paths, decision=decision)
    transfer = CalibrationPublishingTransfer()
    scheduler = FakeSchedulerAdapter()
    remote = FakeRemoteExecAdapter()

    preview = _advance(sample_config_files, run_id, transfer, scheduler, remote, execute=False)
    assert preview.action == "calibration_pending"
    assert preview.submitted_job_id == "8102"

    scheduler.queue_status_results([_completed_status("8102")])
    result = _advance(sample_config_files, run_id, transfer, scheduler, remote)

    assert result.action == final_action
    manifest = read_manifest(paths.manifest)
    assert manifest.preop_postprocess_runs[0].scheduler_job_id == "8101"
    calibration = manifest.calibration_runs[0]
    assert calibration.scheduler_job_id == "8102"
    assert calibration.postprocess_job_id == "8101"
    assert calibration.promotion_status == "promoted"
    assert calibration.input_artifacts["tuned_model"].endswith(
        "iter-01/results/svzerod_3d_coupling_tuned.json"
    )
    # The agent never submitted postprocess/calibration itself.
    expected_submits = 1 if decision == "not_close" else 0
    assert len(scheduler.submit_calls) == expected_submits


def test_advance_falls_back_when_driver_submission_failed(sample_config_files):
    run_id = "run-driver-submit-failed"
    paths = _completed_full_pa_run(sample_config_files, run_id, decision="not_close")
    _write_driver_decision(paths, decision="not_close", status="failed")
    scheduler = FakeSchedulerAdapter()

    result = _advance(
        sample_config_files, run_id, CalibrationPublishingTransfer(), scheduler, FakeRemoteExecAdapter()
    )

    assert result.action == "calibration_submitted"
    assert len(scheduler.submit_calls) == 2
    assert len(read_manifest(paths.manifest).calibration_runs) == 1


class DriverDecisionPublishingTransfer(CalibrationPublishingTransfer):
    def __init__(self, paths):
        super().__init__()
        self.paths = paths

    def sync(self, local_dir, remote_dir, include=None, exclude=None, direction=SyncDirection.PUSH):
        result = super().sync(
            local_dir=local_dir,
            remote_dir=remote_dir,
            include=include,
            exclude=exclude,
            direction=direction,
        )
        if direction == SyncDirection.PULL and include == ["iteration_decision.json"]:
            _write_driver_decision(self.paths, decision="converged")
        return result


def test_advance_pulls_missing_decision_before_deciding_to_fall_back(sample_config_files):
    run_id = "run-driver-decision-pull"
    paths = _completed_full_pa_run(sample_config_files, run_id, decision="converged")
    manifest = read_manifest(paths.manifest)
    manifest.tuning_iteration_tracker.iterations[0].remote_dir = "/scratch/svzt_runs/x/iterations/iter-01"
    write_manifest(manifest, paths.manifest)
    scheduler = FakeSchedulerAdapter()
    scheduler.queue_status_results([_completed_status("8102")])

    result = _advance(
        sample_config_files,
        run_id,
        DriverDecisionPublishingTransfer(paths),
        scheduler,
        FakeRemoteExecAdapter(),
    )

    assert result.action == "already_converged"
    assert scheduler.submit_calls == []
    assert read_manifest(paths.manifest).calibration_runs[0].scheduler_job_id == "8102"


def _write_selectable_iteration_artifacts(paths, *, decision: str) -> None:
    iter_dir = paths.run_dir / "iterations" / "iter-01"
    (iter_dir / "results").mkdir(parents=True, exist_ok=True)
    (iter_dir / "logs").mkdir(parents=True, exist_ok=True)
    tuned = f"/scratch/svzt_runs/{paths.run_dir.name}/iterations/iter-01/results/svzerod_3d_coupling_tuned.json"
    (iter_dir / "results" / "iteration_decision.json").write_text(
        json.dumps(
            {
                "decision": decision,
                "regenerated_config_path": None,
                "tuning_artifacts": {"tuned_zerod_config": tuned},
            }
        ),
        encoding="utf-8",
    )
    (iter_dir / "results" / "iteration_metrics.json").write_text(
        json.dumps({"preop_job_id": "991100"}), encoding="utf-8"
    )
    (iter_dir / "logs" / "iteration_driver_log.json").write_text(
        json.dumps({"steps": ["preop_completed"], "preop_job_id": "991100", "errors": []}),
        encoding="utf-8",
    )


def test_preop_select_uses_promoted_calibrated_full_pa_model(sample_config_files):
    run_id = "run-full-pa-postop-calibrated"
    paths = _completed_full_pa_run(sample_config_files, run_id, decision="converged")
    transfer = CalibrationPublishingTransfer()
    scheduler = FakeSchedulerAdapter()
    remote = FakeRemoteExecAdapter()
    _submit_and_complete_calibration(sample_config_files, run_id, transfer, scheduler, remote)
    assert _advance(sample_config_files, run_id, transfer, scheduler, remote).action == "already_converged"
    _write_selectable_iteration_artifacts(paths, decision="converged")

    selection = select_converged_preop_iteration(
        workspace_root=sample_config_files,
        run_id=run_id,
        iteration=1,
        submit_postprocess=False,
    )

    calibration = read_manifest(paths.manifest).calibration_runs[0]
    expected = f"{calibration.remote_dir}/results/calibrated_full_pa_zerod.json"
    assert selection.remote_tuned_zerod_config == expected
    selected = read_manifest(paths.manifest).converged_preop_iteration
    assert selected.remote_tuned_zerod_config == expected


def test_preop_select_requires_promoted_calibration_under_calibrated_full_pa(sample_config_files):
    run_id = "run-full-pa-postop-uncalibrated"
    paths = _completed_full_pa_run(sample_config_files, run_id, decision="converged")
    _write_selectable_iteration_artifacts(paths, decision="converged")

    with pytest.raises(ConfigError, match="no promoted full-PA calibration"):
        select_converged_preop_iteration(
            workspace_root=sample_config_files,
            run_id=run_id,
            iteration=1,
            submit_postprocess=False,
        )
    assert read_manifest(paths.manifest).converged_preop_iteration is None


class ConvergedDecisionTransfer(CalibrationPublishingTransfer):
    """Publish a converged iter-01 decision when the watcher pulls results."""

    def sync(self, local_dir, remote_dir, include=None, exclude=None, direction=SyncDirection.PUSH):
        result = super().sync(
            local_dir=local_dir,
            remote_dir=remote_dir,
            include=include,
            exclude=exclude,
            direction=direction,
        )
        if direction == SyncDirection.PULL and remote_dir.endswith("iter-01/results"):
            destination = Path(local_dir)
            destination.mkdir(parents=True, exist_ok=True)
            (destination / "iteration_decision.json").write_text(
                "decision: converged\n", encoding="utf-8"
            )
        return result


def test_auto_advance_waits_for_calibration_on_converged_full_pa(sample_config_files):
    _write_full_pa_patient(sample_config_files)
    run_id = "run-full-pa-auto-converged"
    scheduler = FakeSchedulerAdapter()
    scheduler.set_submit_result(
        SubmitResult(
            job_id="991100",
            command=CommandResult(argv=["sbatch"], returncode=0, stdout="991100", stderr="", dry_run=False),
        )
    )
    tune = run_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias=PATIENT_ALIAS,
        run_id=run_id,
        mode=ExecutionMode.EXECUTE,
        transfer_adapter=FakeFileTransferAdapter(),
        scheduler_adapter=scheduler,
        remote_exec_adapter=FakeRemoteExecAdapter(),
    )
    tuned_model = tune.local_job_script_path.parent / "results" / "svzerod_3d_coupling_tuned.json"
    tuned_model.parent.mkdir(parents=True, exist_ok=True)
    tuned_model.write_text('{"tuned": true}\n', encoding="utf-8")
    scheduler.queue_status_results([_completed_status("991100"), _completed_status("991100")])

    result = watch_and_auto_advance_tuning(
        workspace_root=sample_config_files,
        run_id=run_id,
        scheduler_adapter=scheduler,
        transfer_adapter=ConvergedDecisionTransfer(),
        remote_exec_adapter=FakeRemoteExecAdapter(),
        poll_interval_seconds=5,
        max_polls=10,
    )

    assert result.final_action == "converged"
    assert [record.advance_action for record in result.iterations] == ["already_converged"]
    # tune + postprocess + calibration, and no second tuning iteration
    assert len(scheduler.submit_calls) == 3
    manifest = read_manifest(sample_config_files / "runs" / run_id / "manifest.yaml")
    assert manifest.calibration_runs[0].promotion_status == "promoted"


def test_status_reports_driver_seed_generation_jobs(sample_config_files):
    from svztagent.workflows.tune_trees import query_run_status

    _write_full_pa_patient(sample_config_files)
    run_id = "run-status-seed-generation"
    scheduler = FakeSchedulerAdapter()
    run_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias=PATIENT_ALIAS,
        run_id=run_id,
        mode=ExecutionMode.EXECUTE,
        transfer_adapter=FakeFileTransferAdapter(),
        scheduler_adapter=scheduler,
        remote_exec_adapter=FakeRemoteExecAdapter(),
    )
    paths_run_dir = sample_config_files / "runs" / run_id

    class _Paths:
        run_dir = paths_run_dir
        manifest = paths_run_dir / "manifest.yaml"

    _write_driver_decision(_Paths, decision="not_close")
    status_scheduler = FakeSchedulerAdapter()
    status_scheduler.set_status_result(_completed_status("8101"))

    status = query_run_status(
        workspace_root=sample_config_files,
        run_id=run_id,
        scheduler_adapter=status_scheduler,
        transfer_adapter=FakeFileTransferAdapter(),
    )

    seed = status.seed_generation
    assert seed is not None
    assert (seed.strategy, seed.status) == ("calibrated_full_pa", "submitted")
    assert seed.postprocess_job_id == "8101"
    assert seed.calibration_job_id == "8102"
    assert seed.calibration_state == "completed"
