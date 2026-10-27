from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
import yaml

from svztagent.core.errors import ConfigError
from svztagent.core.manifest import read_manifest
from svztagent.core.wall_correction import (
    compute_wall_correction,
    patch_wall_parameters,
    read_wall_parameters,
    wall_only_differences,
)
from svztagent.hpc.fake import FakeFileTransferAdapter, FakeRemoteExecAdapter, FakeSchedulerAdapter
from svztagent.hpc.interfaces import (
    CommandResult,
    ExecutionMode,
    SchedulerStatusResult,
    SubmitResult,
    SyncDirection,
)
from svztagent.workflows.tune_trees import init_run_workspace
from svztagent.workflows.wall_correction import (
    finalize_preop_source,
    query_wall_correction_status,
    run_wall_correction,
)


PATIENT_ALIAS = "TST-STAN-x"
RUN_ID = "wallcorr-run"
E1, C1 = 1369189.0691717705, 10000.0
E2, C2 = 1020337.9086810068, 10000.0


def _solver_xml(preop_dir: str, *, modulus: float, damping: float, extra: str = "") -> str:
    return f"""<?xml version='1.0' encoding='utf-8'?>
<svMultiPhysicsFile version="0.1">
  <GeneralSimulationParameters>
    <Number_of_time_steps>6000</Number_of_time_steps>
    <Time_step_size>0.000375</Time_step_size>
  </GeneralSimulationParameters>
  <Add_mesh name="lumen">
    <Mesh_file_path>{preop_dir}/mesh-complete/mesh-complete.mesh.vtu</Mesh_file_path>
    <Add_face name="inflow">
      <Face_file_path>{preop_dir}/mesh-complete/mesh-surfaces/inflow.vtp</Face_file_path>
    </Add_face>
  </Add_mesh>
  <Add_equation type="FSI">
    <Configuration_file>svzerod_3Dcoupling.json</Configuration_file>
    <Elasticity_modulus>{modulus!r}</Elasticity_modulus>
    <Damping>{damping!r}</Damping>
    <Prestress_file_path>/runs/prestress/1-procs/result_020.vtu</Prestress_file_path>{extra}
  </Add_equation>
</svMultiPhysicsFile>
"""


# ---------------------------------------------------------------- pure rule


def test_rule_reproduces_the_protocol_v1_tst_stan_3_values():
    rule = compute_wall_correction(
        pulse_pressure_3d_mmhg=73.46761232578659 - 7.647444170714397,
        pulse_pressure_target_mmhg=58.0,
        elasticity_modulus_source=E2,
        elasticity_modulus_reference=E1,
        damping_reference=C1,
        damping_source=C2,
        alpha=0.55,
        lambda_min=0.5,
    )
    # _checks/elaw_iter3_20261023/tst-stan-3: E 810717.3883845098, damping 7694.9
    assert rule.lambda_applied == pytest.approx(0.7946, abs=1e-4)
    assert rule.elasticity_modulus_corrected == pytest.approx(810717.3883845098, rel=1e-12)
    assert rule.damping_corrected == 7694.9
    assert rule.floored is False


def test_rule_floors_lambda():
    rule = compute_wall_correction(
        pulse_pressure_3d_mmhg=60.0,
        pulse_pressure_target_mmhg=20.0,
        elasticity_modulus_source=2.0e5,
        elasticity_modulus_reference=2.0e5,
        damping_reference=4000.0,
        alpha=0.55,
        lambda_min=0.5,
    )
    assert rule.floored is True
    assert rule.lambda_applied == 0.5
    assert rule.elasticity_modulus_corrected == pytest.approx(1.0e5)
    assert rule.damping_corrected == round(4000.0 * 0.5**0.5, 1)


def test_rule_rejects_nonpositive_inputs():
    with pytest.raises(ConfigError, match="3D pulse pressure"):
        compute_wall_correction(
            pulse_pressure_3d_mmhg=0.0,
            pulse_pressure_target_mmhg=20.0,
            elasticity_modulus_source=1.0,
            elasticity_modulus_reference=1.0,
            damping_reference=1.0,
            alpha=0.55,
            lambda_min=0.5,
        )


def test_patch_changes_only_the_wall_and_compare_ignores_mesh_relocation():
    source = _solver_xml("/runs/r/iterations/iter-02/preop", modulus=E2, damping=C2)
    patched = patch_wall_parameters(source, elasticity_modulus=8.1e5, damping=7694.9)
    assert read_wall_parameters(patched) == (8.1e5, 7694.9)
    assert wall_only_differences(source, patched) == []
    relocated = patched.replace("/runs/r/iterations/iter-02/preop", "/runs/_checks/x/preop")
    assert wall_only_differences(source, relocated) == []
    changed = relocated.replace("<Time_step_size>0.000375", "<Time_step_size>0.0004")
    assert wall_only_differences(source, changed)


# ---------------------------------------------------------------- workflow


class RemoteFiles(FakeFileTransferAdapter):
    """Serve fixed remote files on pull and calibration reports on sync."""

    def __init__(self, files: dict[str, str | bytes], reports: dict[str, dict] | None = None):
        super().__init__()
        self.files = files
        self.reports = reports or {}

    def pull(self, remote_path: str, local_path: str) -> CommandResult:
        result = super().pull(remote_path, local_path)
        if remote_path in self.files:
            content = self.files[remote_path]
            # rsync semantics: the local path is a directory receiving the file.
            path = Path(local_path) / Path(remote_path).name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content if isinstance(content, bytes) else content.encode("utf-8"))
        return result

    def sync(self, local_dir, remote_dir, include=None, exclude=None, direction=SyncDirection.PUSH):
        result = super().sync(local_dir, remote_dir, include, exclude, direction)
        if direction == SyncDirection.PULL and remote_dir.endswith("/calibration/results"):
            for name, payload in self.reports.items():
                Path(local_dir, name).write_text(json.dumps(payload), encoding="utf-8")
        return result


class CountingScheduler(FakeSchedulerAdapter):
    def __init__(self, state: str = "COMPLETED"):
        super().__init__()
        self.state = state

    def submit(self, job_script_path: str) -> SubmitResult:
        self.submit_calls.append(job_script_path)
        job_id = str(1000 + len(self.submit_calls))
        return SubmitResult(
            job_id=job_id,
            command=CommandResult(argv=["sbatch", job_script_path], returncode=0, stdout=job_id, stderr="", dry_run=False),
        )

    def status(self, job_id: str) -> SchedulerStatusResult:
        return SchedulerStatusResult(
            job_id=job_id,
            raw_state=self.state,
            source="squeue",
            command=CommandResult(argv=["squeue"], returncode=0, stdout=self.state, stderr="", dry_run=False),
        )


@pytest.fixture
def full_pa_run(sample_config_files):
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
""".strip()
        + "\n",
        encoding="utf-8",
    )
    (sample_config_files / "config" / "clinical_targets.yaml").write_text(
        yaml.safe_dump(
            {"clinical_targets": {"preop": {"patients": {PATIENT_ALIAS: {"mpa_pressure": [67, 9, 32], "rpa_split": 0.446}}}}}
        ),
        encoding="utf-8",
    )
    init_run_workspace(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias=PATIENT_ALIAS,
        run_id=RUN_ID,
    )
    manifest = read_manifest(sample_config_files / "runs" / RUN_ID / "manifest.yaml")
    remote_run = str(manifest.execution.remote_run_dir)
    iter1, iter2 = f"{remote_run}/iterations/iter-01", f"{remote_run}/iterations/iter-02"
    tuned = b'{"vessels": []}\n'
    files: dict[str, str | bytes] = {
        f"{iter2}/results/iteration_decision.json": json.dumps(
            {"metrics": {"mpa_sys": 73.46761232578659, "mpa_dia": 7.647444170714397, "mpa_mean": 29.9}}
        ),
        f"{iter2}/preop/svFSIplus.xml": _solver_xml(f"{iter2}/preop", modulus=E2, damping=C2),
        f"{iter1}/preop/svFSIplus.xml": _solver_xml(f"{iter1}/preop", modulus=E1, damping=C1),
        f"{iter2}/preop/svzerod_3Dcoupling.json": b'{"bcs": "iteration-2"}\n',
        f"{iter2}/results/svzerod_3d_coupling_tuned.json": tuned,
        f"{iter2}/preop/inflow.flow": "6000 1\n0 1.0\n",
        f"{iter2}/preop/run_solver.sh": (
            "#!/usr/bin/env bash\n#SBATCH --job-name=svFlowSolver\n"
            f"#SBATCH --chdir={iter2}/preop\n#SBATCH --nodes=3\ncd {iter2}/preop\nsrun svmultiphysics svFSIplus.xml\n"
        ),
    }
    return sample_config_files, remote_run, files, hashlib.sha256(tuned).hexdigest()


def _adopted_files(remote_run: str, files: dict, tuned_sha: str, *, modulus=810717.3883845098, damping=7694.9, coupler=None):
    adopt = f"{Path(remote_run).parent}/_checks/elaw/tst-stan-x"
    iter2 = f"{remote_run}/iterations/iter-02"
    descriptor_dir = f"{adopt}/results/postprocess"
    relative = "../../../../../" + Path(remote_run).name + "/iterations/iter-02/results/svzerod_3d_coupling_tuned.json"
    files = dict(files)
    files[f"{adopt}/preop/svFSIplus.xml"] = _solver_xml(f"{adopt}/preop", modulus=modulus, damping=damping)
    files[f"{adopt}/preop/svzerod_3Dcoupling.json"] = coupler or files[f"{iter2}/preop/svzerod_3Dcoupling.json"]
    files[f"{descriptor_dir}/postprocess_suite_metadata.json"] = json.dumps(
        {"status": "completed", "lineage": {"tuned_zerod_config_path": relative, "tuned_zerod_config_sha256": tuned_sha}}
    )
    return f"{adopt}/preop", files


def test_adopt_dry_run_plans_only_the_calibration(full_pa_run):
    root, remote_run, files, tuned_sha = full_pa_run
    adopt_dir, files = _adopted_files(remote_run, files, tuned_sha)
    transfer, scheduler = RemoteFiles(files), CountingScheduler()
    result = run_wall_correction(
        root,
        RUN_ID,
        iteration=2,
        adopt_threed_dir=adopt_dir,
        mode=ExecutionMode.DRY_RUN,
        transfer_adapter=transfer,
        scheduler_adapter=scheduler,
        remote_exec_adapter=FakeRemoteExecAdapter(),
        fetch_adapter=transfer,
    )
    assert result.correction_mode == "adopted"
    assert result.rule.elasticity_modulus_corrected == pytest.approx(810717.3883845098, rel=1e-12)
    assert scheduler.submit_calls == [f"{remote_run}/wall_correction/iter-02/calibration/run_calibration.sh"]
    staged = result.plan_path.parent / "staged" / "calibration"
    request = yaml.safe_load((staged / "inputs" / "calibrate_0d_from_3d.yaml").read_text())
    params = request["calibration"]["parameters"]
    assert params["vessels"]["default"] == ["R_poiseuille"]
    # The gate policy is a resolver argument, not a key in the upstream request.
    assert "svzt_target_gate_policy" not in request["calibration"]
    assert "calibrate_0d_from_3d.resolved.yaml report_only <<'PY'" in (staged / "run_calibration.sh").read_text()
    assert request["calibration"]["solver"] == {"rho_infty": 0.2}
    assert params["proximal_compliance_scale"] == {
        "enabled": True,
        "bounds": [0.2, 5.0],
        "tolerance_mmhg": 0.05,
        "maximum_iterations": 40,
    }
    assert request["calibration"]["data_source"]["postprocess_metadata_json"].startswith(str(Path(adopt_dir).parent))
    assert request["paths"]["zerod_config"] == f"{remote_run}/iterations/iter-02/results/svzerod_3d_coupling_tuned.json"
    assert "--dependency" not in (staged / "run_calibration.sh").read_text()
    plan = json.loads(result.plan_path.read_text())
    assert plan["verification"]["coupler_identical"] is True
    # A dry run records nothing in the manifest.
    assert read_manifest(root / "runs" / RUN_ID / "manifest.yaml").wall_correction_runs == []


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"modulus": 9.0e5}, "elasticity modulus"),
        ({"damping": 8000.0}, "damping"),
        ({"coupler": b'{"bcs": "other"}\n'}, "differs from the source iteration's BCs"),
    ],
)
def test_adopt_rejects_a_run_that_does_not_match(full_pa_run, overrides, message):
    root, remote_run, files, tuned_sha = full_pa_run
    adopt_dir, files = _adopted_files(remote_run, files, tuned_sha, **overrides)
    transfer, scheduler = RemoteFiles(files), CountingScheduler()
    with pytest.raises(ConfigError, match=message):
        run_wall_correction(
            root,
            RUN_ID,
            iteration=2,
            adopt_threed_dir=adopt_dir,
            mode=ExecutionMode.EXECUTE,
            transfer_adapter=transfer,
            scheduler_adapter=scheduler,
            remote_exec_adapter=FakeRemoteExecAdapter(),
            fetch_adapter=transfer,
        )
    assert scheduler.submit_calls == []


def test_adopt_rejects_postprocess_lineage_from_another_model(full_pa_run):
    root, remote_run, files, _tuned_sha = full_pa_run
    adopt_dir, files = _adopted_files(remote_run, files, "0" * 64)
    transfer = RemoteFiles(files)
    with pytest.raises(ConfigError, match="lineage sha256"):
        run_wall_correction(
            root,
            RUN_ID,
            iteration=2,
            adopt_threed_dir=adopt_dir,
            mode=ExecutionMode.EXECUTE,
            transfer_adapter=transfer,
            scheduler_adapter=CountingScheduler(),
            remote_exec_adapter=FakeRemoteExecAdapter(),
            fetch_adapter=transfer,
        )


def test_rerun_execute_chains_solver_postprocess_and_calibration(full_pa_run):
    root, remote_run, files, _tuned_sha = full_pa_run
    transfer, scheduler = RemoteFiles(files), CountingScheduler()
    result = run_wall_correction(
        root,
        RUN_ID,
        iteration=2,
        mode=ExecutionMode.EXECUTE,
        transfer_adapter=transfer,
        scheduler_adapter=scheduler,
        remote_exec_adapter=FakeRemoteExecAdapter(),
        fetch_adapter=transfer,
    )
    wall = f"{remote_run}/wall_correction/iter-02"
    assert scheduler.submit_calls == [
        f"{wall}/preop/run_solver.sh",
        f"{wall}/postprocess/run_postprocess.sh",
        f"{wall}/calibration/run_calibration.sh",
    ]
    assert (result.solver_job_id, result.postprocess_job_id, result.calibration_job_id) == ("1001", "1002", "1003")
    staged = result.plan_path.parent / "staged"
    xml = (staged / "preop" / "svFSIplus.xml").read_text()
    assert read_wall_parameters(xml) == (pytest.approx(810717.3883845098, rel=1e-12), 7694.9)
    # Mesh paths keep pointing at the source iteration's mesh (read only).
    assert f"{remote_run}/iterations/iter-02/preop/mesh-complete/mesh-complete.mesh.vtu" in xml
    solver = (staged / "preop" / "run_solver.sh").read_text()
    assert f"cd {wall}/preop" in solver and "--job-name=wallcorr-" in solver and "iterations/iter-02/preop" not in solver
    postprocess = (staged / "postprocess" / "run_postprocess.sh").read_text()
    assert postprocess.splitlines()[1] == "#SBATCH --dependency=afterok:1001"
    assert f'"simulation_dir": "{wall}/preop"' in postprocess
    assert "#SBATCH --dependency=afterok:1002" in (staged / "calibration" / "run_calibration.sh").read_text()
    pushed = {remote for _local, remote in transfer.push_calls}
    assert f"{wall}/preop/svzerod_3Dcoupling.json" in pushed and f"{wall}/preop/inflow.flow" in pushed
    record = read_manifest(root / "runs" / RUN_ID / "manifest.yaml").wall_correction_runs[-1]
    assert record.mode == "rerun" and record.status == "submitted"
    assert record.remote_calibrated_zerod_config == f"{wall}/calibration/results/calibrated_full_pa_zerod.json"
    # The source iteration's own records are untouched.
    manifest = read_manifest(root / "runs" / RUN_ID / "manifest.yaml")
    assert manifest.calibration_runs == [] and manifest.preop_postprocess_runs == []


def test_execute_refuses_to_resubmit_an_active_correction(full_pa_run):
    root, remote_run, files, _tuned_sha = full_pa_run
    kwargs = dict(
        iteration=2,
        mode=ExecutionMode.EXECUTE,
        remote_exec_adapter=FakeRemoteExecAdapter(),
    )
    transfer = RemoteFiles(files)
    run_wall_correction(root, RUN_ID, transfer_adapter=transfer, scheduler_adapter=CountingScheduler(), fetch_adapter=transfer, **kwargs)
    with pytest.raises(ConfigError, match="already submitted"):
        run_wall_correction(root, RUN_ID, transfer_adapter=transfer, scheduler_adapter=CountingScheduler(), fetch_adapter=transfer, **kwargs)


def test_status_fetches_reports_and_records_the_matched_0d_model(full_pa_run):
    root, remote_run, files, _tuned_sha = full_pa_run
    reports = {
        "calibrated_full_pa_zerod.json": {"vessels": []},
        "calibration_targets.json": {
            "status": "pass",
            "candidate": {"composite_score": 1.2},
            "baseline": {"targets": {"composite_score": 3.4}},
        },
        "calibration_compliance_fit.json": {
            "status": "pass",
            "scale": 0.91,
            "target_pulse_pressure_mmhg": 58.6,
            "unscaled_pulse_pressure_mmhg": 57.7,
            "fitted_pulse_pressure_mmhg": 58.61,
        },
    }
    transfer = RemoteFiles(files, reports)
    run_wall_correction(
        root,
        RUN_ID,
        iteration=2,
        mode=ExecutionMode.EXECUTE,
        transfer_adapter=transfer,
        scheduler_adapter=CountingScheduler(),
        remote_exec_adapter=FakeRemoteExecAdapter(),
        fetch_adapter=transfer,
    )
    status = query_wall_correction_status(
        root, RUN_ID, transfer_adapter=transfer, scheduler_adapter=CountingScheduler("COMPLETED")
    )
    assert status.status == "completed"
    assert status.results["compliance_scale"] == 0.91
    assert status.results["composite_candidate"] == 1.2
    record = read_manifest(root / "runs" / RUN_ID / "manifest.yaml").wall_correction_runs[-1]
    assert record.status == "completed" and record.results["fitted_pulse_pressure_mmhg"] == 58.61
    assert record.results["matched_zerod_model"] == record.remote_calibrated_zerod_config
    assert record.results["matched_zerod_composite"] == 1.2
    # Status only records the correction; the pre-op source is an explicit finalize.
    assert status.finalizable and not status.finalized
    assert read_manifest(root / "runs" / RUN_ID / "manifest.yaml").converged_preop_iteration is None

    finalized = finalize_preop_source(root, RUN_ID)
    assert finalized.action == "written" and finalized.correction_id == record.correction_id
    handoff = read_manifest(root / "runs" / RUN_ID / "manifest.yaml").converged_preop_iteration
    assert handoff.selection_kind == "wall_correction" and handoff.wall_correction_id == record.correction_id
    assert handoff.selected_by_command == "svzt preop finalize"
    assert handoff.remote_preop_dir == record.remote_threed_dir
    assert handoff.remote_tuned_zerod_config == record.remote_calibrated_zerod_config
    assert handoff.remote_tuning_results_dir == f"{remote_run}/iterations/iter-02/results"
    assert handoff.threed_overrides == {
        "elasticity_modulus": pytest.approx(810717.3883845098, rel=1e-12),
        "damping": 7694.9,
    }

    # Idempotent: a second finalize, or a later status poll, rewrites nothing.
    manifest_path = root / "runs" / RUN_ID / "manifest.yaml"
    before = manifest_path.read_bytes()
    again = finalize_preop_source(root, RUN_ID, correction_id=record.correction_id)
    assert again.action == "unchanged" and manifest_path.read_bytes() == before
    polled = query_wall_correction_status(
        root, RUN_ID, transfer_adapter=transfer, scheduler_adapter=CountingScheduler("COMPLETED")
    )
    assert polled.finalized
    assert read_manifest(manifest_path).converged_preop_iteration == handoff


def _completed_correction(full_pa_run):
    root, remote_run, files, _tuned_sha = full_pa_run
    reports = {
        "calibrated_full_pa_zerod.json": {"vessels": []},
        "calibration_targets.json": {"status": "pass", "candidate": {"composite_score": 1.2}},
        "calibration_compliance_fit.json": {"status": "pass", "scale": 0.91},
    }
    transfer = RemoteFiles(files, reports)
    run_wall_correction(
        root,
        RUN_ID,
        iteration=2,
        mode=ExecutionMode.EXECUTE,
        transfer_adapter=transfer,
        scheduler_adapter=CountingScheduler(),
        remote_exec_adapter=FakeRemoteExecAdapter(),
        fetch_adapter=transfer,
    )
    query_wall_correction_status(root, RUN_ID, transfer_adapter=transfer, scheduler_adapter=CountingScheduler())
    return root


def test_finalize_refuses_an_unfinished_or_unknown_correction(full_pa_run):
    root, _remote_run, files, _tuned_sha = full_pa_run
    with pytest.raises(ConfigError, match="no wall-correction record"):
        finalize_preop_source(root, RUN_ID)
    transfer = RemoteFiles(files)
    run_wall_correction(
        root,
        RUN_ID,
        iteration=2,
        mode=ExecutionMode.EXECUTE,
        transfer_adapter=transfer,
        scheduler_adapter=CountingScheduler(),
        remote_exec_adapter=FakeRemoteExecAdapter(),
        fetch_adapter=transfer,
    )
    with pytest.raises(ConfigError, match="is submitted"):
        finalize_preop_source(root, RUN_ID)
    with pytest.raises(ConfigError, match="no wall-correction record 'wallcorr-other'"):
        finalize_preop_source(root, RUN_ID, correction_id="wallcorr-other")
    assert read_manifest(root / "runs" / RUN_ID / "manifest.yaml").converged_preop_iteration is None


def test_finalize_replaces_a_legacy_source_only_before_postop(full_pa_run):
    from svztagent.core.manifest import PostopRunRecord, record_converged_preop_iteration, write_manifest

    root = _completed_correction(full_pa_run)
    path = root / "runs" / RUN_ID / "manifest.yaml"
    legacy = record_converged_preop_iteration(
        read_manifest(path),
        iteration=2,
        source_decision="accept",
        selection_kind="manual",
        reason="legacy selection",
        metrics=None,
        deltas=None,
        remote_iteration_dir="/r/iter-02",
        remote_preop_dir="/r/iter-02/preop",
        remote_tuned_zerod_config="/r/iter-02/results/tuned.json",
        remote_canonical_coupler="/r/iter-02/preop/svzerod_3Dcoupling.json",
        preop_job_id="11",
    )
    write_manifest(legacy.model_copy(update={"postop_run": PostopRunRecord(source_preop_iteration=2, local_dir="/l", remote_dir="/r")}), path)
    with pytest.raises(ConfigError, match="already submitted post-op"):
        finalize_preop_source(root, RUN_ID)
    assert read_manifest(path).converged_preop_iteration.selection_kind == "manual"

    write_manifest(legacy, path)
    result = finalize_preop_source(root, RUN_ID)
    assert result.action == "replaced" and result.previous.selection_kind == "manual"
    assert read_manifest(path).converged_preop_iteration.selection_kind == "wall_correction"


def test_status_reports_a_failed_calibration_gate(full_pa_run):
    root, remote_run, files, _tuned_sha = full_pa_run
    transfer = RemoteFiles(
        files,
        {
            "calibration_targets.json": {
                "status": "fail",
                "candidate": {"composite_score": 2.2},
                "baseline": {"targets": {"composite_score": 1.4}},
            }
        },
    )
    run_wall_correction(
        root,
        RUN_ID,
        iteration=2,
        mode=ExecutionMode.EXECUTE,
        transfer_adapter=transfer,
        scheduler_adapter=CountingScheduler(),
        remote_exec_adapter=FakeRemoteExecAdapter(),
        fetch_adapter=transfer,
    )
    status = query_wall_correction_status(
        root, RUN_ID, transfer_adapter=transfer, scheduler_adapter=CountingScheduler("FAILED")
    )
    assert status.status in {"failed", "calibration_failed"}


def test_an_unpublished_calibration_has_no_matched_model(full_pa_run):
    root, remote_run, files, _tuned_sha = full_pa_run
    reports = {
        "calibration_targets.json": {
            "status": "fail",
            "candidate": {"composite_score": 2.2},
            "baseline": {"targets": {"composite_score": 1.4}},
        },
        "calibration_compliance_fit.json": {"status": "pass", "scale": 0.83},
    }
    transfer = RemoteFiles(files, reports)
    scheduler = CountingScheduler()
    run_wall_correction(
        root,
        RUN_ID,
        iteration=2,
        adopt_threed_dir=None,
        mode=ExecutionMode.EXECUTE,
        transfer_adapter=transfer,
        scheduler_adapter=scheduler,
        remote_exec_adapter=FakeRemoteExecAdapter(),
        fetch_adapter=transfer,
    )

    class SolverDoneCalibrationFailed(CountingScheduler):
        def status(self, job_id):
            result = super().status(job_id)
            state = "FAILED" if job_id == "1003" else "COMPLETED"
            return result.model_copy(update={"raw_state": state}) if hasattr(result, "model_copy") else SchedulerStatusResult(
                job_id=job_id, raw_state=state, source="squeue", command=result.command
            )

    status = query_wall_correction_status(
        root, RUN_ID, transfer_adapter=transfer, scheduler_adapter=SolverDoneCalibrationFailed()
    )
    assert status.status == "calibration_failed"
    assert status.results["matched_zerod_model"] is None
    assert status.results["composite_baseline"] == 1.4
    assert read_manifest(root / "runs" / RUN_ID / "manifest.yaml").converged_preop_iteration is None
    assert not status.finalizable
    with pytest.raises(ConfigError, match="is calibration_failed"):
        finalize_preop_source(root, RUN_ID)


def _run_resolver(tmp_path, calibration: dict, *args: str):
    """Run the job's target resolver against a stub svzerodtrees target-role builder."""

    import subprocess
    import sys

    from svztagent.workflows.calibrate import _RESOLVE_TARGETS_PY

    stub = tmp_path / "stub" / "svzerodtrees" / "calibration"
    stub.mkdir(parents=True)
    for package in (stub.parent, stub):
        (package / "__init__.py").write_text("", encoding="utf-8")
    (stub / "target_roles.py").write_text(
        "def full_pa_calibration_targets(model, mapping, gate_policy):\n"
        "    return {'gate_policy': gate_policy, 'mapping': mapping.name}\n",
        encoding="utf-8",
    )
    request = tmp_path / "calibrate.yaml"
    resolved = tmp_path / "calibrate.resolved.yaml"
    request.write_text(
        yaml.safe_dump({"paths": {"zerod_config": str(tmp_path / "model.json")}, "calibration": calibration}),
        encoding="utf-8",
    )
    proc = subprocess.run(
        [sys.executable, "-c", _RESOLVE_TARGETS_PY, str(request), str(resolved), *args],
        env={"PYTHONPATH": str(stub.parent.parent)},
        capture_output=True,
        text=True,
    )
    payload = json.loads(resolved.read_text(encoding="utf-8")) if resolved.is_file() else None
    return proc, payload


@pytest.mark.parametrize("gate_policy", ["report_only", "improvement_only"])
def test_calibration_resolver_passes_the_gate_policy_argument_upstream(tmp_path, gate_policy):
    proc, payload = _run_resolver(tmp_path, {"observation_qc": {"enforcement": "target_focused"}}, gate_policy)
    assert proc.returncode == 0, proc.stderr
    assert payload["calibration"]["targets"] == {"gate_policy": gate_policy, "mapping": "outlet_cap_mapping.json"}


def test_calibration_resolver_keeps_explicit_targets(tmp_path):
    targets = {"mpa_pressure": {"vessel": "v0"}}
    proc, payload = _run_resolver(
        tmp_path, {"observation_qc": {"enforcement": "target_focused"}, "targets": targets}, "report_only"
    )
    assert proc.returncode == 0, proc.stderr
    assert payload["calibration"]["targets"] == targets


@pytest.mark.parametrize(
    "calibration, args, message",
    [
        ({"svzt_target_gate_policy": "report_only"}, ("report_only",), "retired svzt_target_gate_policy"),
        ({}, ("anything_goes",), "unknown calibration gate policy"),
    ],
)
def test_calibration_resolver_refuses_bad_gate_inputs(tmp_path, calibration, args, message):
    proc, payload = _run_resolver(tmp_path, calibration, *args)
    assert proc.returncode != 0 and message in proc.stderr
    assert payload is None


def test_calibration_scripts_name_their_gate_policy():
    from svztagent.workflows.calibrate import render_calibration_script

    kwargs = dict(remote_root="/r", remote_logs_dir="/r/logs", remote_config_path="/r/inputs/c.yaml", postprocess_job_id="7")
    # Tuning-iteration calibrations keep the improvement gate.
    assert "c.resolved.yaml improvement_only <<'PY'" not in render_calibration_script(**kwargs)
    assert "calibrate_0d_from_3d.resolved.yaml improvement_only <<'PY'" in render_calibration_script(**kwargs)
    assert "calibrate_0d_from_3d.resolved.yaml report_only <<'PY'" in render_calibration_script(
        **kwargs, gate_policy="report_only"
    )
    with pytest.raises(ConfigError, match="gate_policy"):
        render_calibration_script(**kwargs, gate_policy="lenient")


def test_cli_preop_finalize_is_idempotent(full_pa_run, capsys):
    from svztagent.cli.main import main

    root = _completed_correction(full_pa_run)
    assert main(["--workspace-root", str(root), "preop", "finalize", "--run-id", RUN_ID]) == 0
    first = capsys.readouterr().out
    assert "Pre-op source: written" in first and "Wall: E 810717, damping 7694.9" in first
    assert main(["--workspace-root", str(root), "preop", "finalize", "--run-id", RUN_ID]) == 0
    assert "Pre-op source: unchanged" in capsys.readouterr().out


def test_finalize_declares_the_protocol_without_touching_an_identical_handoff(full_pa_run):
    root = _completed_correction(full_pa_run)
    path = root / "runs" / RUN_ID / "manifest.yaml"
    first = finalize_preop_source(root, RUN_ID)
    assert first.protocol is None and read_manifest(path).protocol is None
    handoff = read_manifest(path).converged_preop_iteration

    declared = finalize_preop_source(root, RUN_ID, protocol="preop_v1.1")
    assert declared.action == "unchanged" and declared.protocol_changed
    manifest = read_manifest(path)
    assert (manifest.protocol.name, manifest.protocol.stage) == ("preop_v1.1", "preop_finalized")
    assert manifest.converged_preop_iteration == handoff

    before = path.read_bytes()
    again = finalize_preop_source(root, RUN_ID)
    assert not again.protocol_changed and again.protocol.name == "preop_v1.1"
    assert path.read_bytes() == before
    with pytest.raises(ConfigError, match="already follows protocol 'preop_v1.1'"):
        finalize_preop_source(root, RUN_ID, protocol="preop_v2")


def test_record_protocol_stage_rules(full_pa_run):
    from svztagent.core.manifest import record_protocol_stage

    root, _remote_run, _files, _tuned_sha = full_pa_run
    manifest = read_manifest(root / "runs" / RUN_ID / "manifest.yaml")
    with pytest.raises(ConfigError, match="declares no protocol"):
        record_protocol_stage(manifest, stage="tuning")
    declared = record_protocol_stage(manifest, stage="tuning", name="preop_v1.1", at="2026-10-26T00:00:00+00:00")
    assert declared.protocol.stage == "tuning" and declared.protocol.updated_at == "2026-10-26T00:00:00+00:00"
    assert record_protocol_stage(declared, stage="tuning") is declared
    assert record_protocol_stage(declared, stage="wall_correction").protocol.stage == "wall_correction"
    with pytest.raises(ValueError):
        record_protocol_stage(declared, stage="postop")
