from __future__ import annotations

import json
from pathlib import Path

import yaml

from svztagent.core.manifest import (
    mark_iteration_decision,
    mark_iteration_submitted,
    read_manifest,
    write_manifest,
)
from svztagent.hpc.fake import FakeFileTransferAdapter, FakeRemoteExecAdapter, FakeSchedulerAdapter
from svztagent.hpc.interfaces import ExecutionMode
from svztagent.workflows.adapt import run_adapt
from svztagent.workflows.postop import run_postop, select_converged_preop_iteration
from svztagent.workflows.tune_trees import init_run_workspace, run_tune_trees


def _set_cluster_svfsiplus_path(workspace: Path, configured_path: str) -> None:
    clusters_path = workspace / "config" / "clusters.yaml"
    payload = yaml.safe_load(clusters_path.read_text(encoding="utf-8"))
    payload["clusters"][0]["executables"]["svfsiplus_path"] = configured_path
    clusters_path.write_text(
        yaml.safe_dump(payload, sort_keys=False),
        encoding="utf-8",
    )


def _enable_postop_mesh(workspace: Path) -> None:
    postop_mesh = (
        workspace
        / "remote_data"
        / "permanent"
        / "TST-STAN-x"
        / "postop-mesh-complete"
        / "mesh-surfaces"
    )
    postop_mesh.mkdir(parents=True, exist_ok=True)

    defaults_path = workspace / "config" / "defaults.yaml"
    defaults_text = defaults_path.read_text(encoding="utf-8")
    if "postop_mesh_complete_dir" in defaults_text:
        return
    defaults_path.write_text(
        defaults_text.replace(
            'preop_mesh_complete_dir: "preop-mesh-complete"\n',
            'preop_mesh_complete_dir: "preop-mesh-complete"\n'
            '    postop_mesh_complete_dir: "postop-mesh-complete"\n',
        ),
        encoding="utf-8",
    )


def _write_completed_iteration_artifacts(paths, *, iteration: int, decision: str) -> None:
    iter_dir = paths.run_dir / "iterations" / f"iter-{iteration:02d}"
    results_dir = iter_dir / "results"
    logs_dir = iter_dir / "logs"
    results_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    remote_results_dir = (
        f"/scratch/users/ndorn/svzt_runs/{paths.run_dir.name}/iterations/iter-{iteration:02d}/results"
    )
    (results_dir / "iteration_decision.json").write_text(
        json.dumps(
            {
                "decision": decision,
                "regenerated_config_path": (
                    f"{remote_results_dir}/simplified_zerod_tuned_RRI.json"
                ),
                "tuning_artifacts": {
                    "tuned_zerod_config": f"{remote_results_dir}/svzerod_3d_coupling_tuned.json",
                    "optimized_params_csv": "optimized_params.csv",
                    "pa_config_snapshot": "pa_config_tuning_snapshot.json",
                },
            }
        ),
        encoding="utf-8",
    )
    (results_dir / "iteration_metrics.json").write_text(
        json.dumps({"preop_job_id": "991100", "mpa_mean": 31.0}),
        encoding="utf-8",
    )
    (logs_dir / "iteration_driver_log.json").write_text(
        json.dumps(
            {
                "steps": ["preop_submitted", "preop_completed"],
                "preop_job_id": "991100",
                "errors": [],
                "warnings": [],
            }
        ),
        encoding="utf-8",
    )


def _prepare_selected_iteration(workspace: Path, *, run_id: str):
    _enable_postop_mesh(workspace)
    paths, _ctx = init_run_workspace(
        workspace_root=workspace,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id=run_id,
    )
    manifest = read_manifest(paths.manifest)
    manifest = mark_iteration_submitted(
        manifest,
        iteration=3,
        tune_job_id="990003",
        local_dir=str(paths.run_dir / "iterations" / "iter-03"),
        remote_dir=f"/scratch/users/ndorn/svzt_runs/{run_id}/iterations/iter-03",
        job_script_path=f"/scratch/users/ndorn/svzt_runs/{run_id}/iterations/iter-03/run_tune_iter.sh",
    )
    manifest = mark_iteration_decision(
        manifest,
        iteration=3,
        decision="converged",
        metrics={"mpa_mean": 31.0},
        deltas={"mpa_mean": 0.5},
        regenerated_config_path=(
            f"/scratch/users/ndorn/svzt_runs/{run_id}/iterations/iter-03/results/"
            "simplified_zerod_tuned_RRI.json"
        ),
    )
    write_manifest(manifest, paths.manifest)
    _write_completed_iteration_artifacts(paths, iteration=3, decision="converged")
    return paths


def test_svfsiplus_path_flows_into_svzerodtrees_runtime_config(sample_config_files):
    configured_path = "/opt/svfsiplus/bin/custom-svmultiphysics"
    default_path = "/home/users/ndorn/svMP-build/svMultiPhysics-build/bin/svmultiphysics"
    _set_cluster_svfsiplus_path(sample_config_files, configured_path)

    tune_result = run_tune_trees(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-svfsiplus-flow-tune",
        mode=ExecutionMode.DRY_RUN,
    )
    tune_script = tune_result.local_job_script_path.read_text(encoding="utf-8")
    assert f'cluster_svfsiplus_path = "{configured_path}"' in tune_script
    assert 'solver_execution["executable"] = cluster_svfsiplus_path' in tune_script
    assert 'solver_execution["svfsiplus_path"] = cluster_svfsiplus_path' in tune_script
    assert 'threed_config["execution"] = solver_execution' in tune_script
    assert default_path not in tune_script

    selected_paths = _prepare_selected_iteration(
        sample_config_files,
        run_id="run-svfsiplus-flow-explicit",
    )
    select_converged_preop_iteration(
        workspace_root=sample_config_files,
        run_id="run-svfsiplus-flow-explicit",
        iteration=3,
        reason="best tuned preop",
        transfer_adapter=FakeFileTransferAdapter(),
        scheduler_adapter=FakeSchedulerAdapter(),
        remote_exec_adapter=FakeRemoteExecAdapter(),
    )

    postop_result = run_postop(
        workspace_root=sample_config_files,
        run_id="run-svfsiplus-flow-explicit",
        mode=ExecutionMode.EXECUTE,
        transfer_adapter=FakeFileTransferAdapter(),
        scheduler_adapter=FakeSchedulerAdapter(),
        remote_exec_adapter=FakeRemoteExecAdapter(),
    )
    postop_script = postop_result.local_job_script_path.read_text(encoding="utf-8")
    assert f'cluster_svfsiplus_path = "{configured_path}"' in postop_script
    assert 'solver_execution["executable"] = cluster_svfsiplus_path' in postop_script
    assert 'solver_execution["svfsiplus_path"] = cluster_svfsiplus_path' in postop_script
    assert 'threed_config["execution"] = solver_execution' in postop_script
    assert default_path not in postop_script

    adapt_result = run_adapt(
        workspace_root=sample_config_files,
        run_id="run-svfsiplus-flow-explicit",
        model="M1",
        mode=ExecutionMode.DRY_RUN,
        transfer_adapter=FakeFileTransferAdapter(),
        scheduler_adapter=FakeSchedulerAdapter(),
        remote_exec_adapter=FakeRemoteExecAdapter(),
    )
    adapt_script = adapt_result.local_job_script_path.read_text(encoding="utf-8")
    assert f'cluster_svfsiplus_path = "{configured_path}"' in adapt_script
    assert 'solver_execution["executable"] = cluster_svfsiplus_path' in adapt_script
    assert 'solver_execution["svfsiplus_path"] = cluster_svfsiplus_path' in adapt_script
    assert 'threed_config["execution"] = solver_execution' in adapt_script
    assert "execution_config=solver_execution," in adapt_script
    assert 'execution_config={{"mode": "slurm", "submit_command": "bash"}}' not in adapt_script
    assert default_path not in adapt_script

    manifest = read_manifest(selected_paths.manifest)
    assert manifest.postop_run is not None
