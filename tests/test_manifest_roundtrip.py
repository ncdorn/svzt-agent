from __future__ import annotations

from svztagent.config.load import load_workspace_config, resolve_cluster, resolve_patient_alias
from svztagent.core.manifest import (
    copy_config_snapshot,
    create_manifest,
    read_manifest,
    record_converged_preop_iteration,
    record_postop_submission,
    update_run_progress,
    write_manifest,
)
from svztagent.core.paths import build_local_run_paths, ensure_local_run_dirs
from svztagent.workflows.tune_trees import init_run_workspace


def test_manifest_roundtrip(sample_config_files):
    config = load_workspace_config(sample_config_files)
    cluster = resolve_cluster(config, "sherlock")
    patient = resolve_patient_alias(config, "sherlock", "TST-STAN-x")
    local_paths = build_local_run_paths(sample_config_files, "run-001")
    ensure_local_run_dirs(local_paths)
    copy_config_snapshot(sample_config_files, local_paths.config_snapshot)

    manifest = create_manifest(
        run_id="run-001",
        cluster=cluster,
        patient=patient,
        local_paths=local_paths,
        workspace_root=sample_config_files,
        config=config,
    )
    write_manifest(manifest, local_paths.manifest)

    loaded = read_manifest(local_paths.manifest)
    assert loaded.run_id == "run-001"
    assert loaded.cluster["name"] == "sherlock"
    assert loaded.patient["alias"] == "TST-STAN-x"
    assert loaded.patient["patient_assets"]["clinical_targets"].endswith(
        "/clinical_targets.csv"
    )
    assert loaded.remote["svzerodtrees_paths"]["mesh_surfaces"].endswith(
        "/preop-mesh-complete/mesh-surfaces"
    )
    assert loaded.remote["svzerodtrees_paths"]["preop_mesh_complete"].endswith(
        "/preop-mesh-complete"
    )
    assert loaded.remote["svzerodtrees_paths"]["postop_mesh_complete"] is None
    assert loaded.remote["threed_defaults"]["wall_model"] == "deformable"
    assert loaded.remote["threed_defaults"]["inflow_boundary_condition"] == "neumann"
    assert loaded.remote["threed_defaults"]["prestress_file"] == "auto"
    assert loaded.remote["threed_defaults"]["tissue_support"]["enabled"] is True
    assert loaded.remote["threed_defaults"]["tissue_support"]["type"] == "uniform"
    assert loaded.remote["threed_defaults"]["tissue_support"]["stiffness"] == 1000.0
    assert loaded.remote["threed_defaults"]["tissue_support"]["damping"] == 10000.0
    assert loaded.local_paths.progress_tracker.endswith("/progress_tracker.yaml")
    assert loaded.progress_tracker is not None
    assert {m.model_id for m in loaded.progress_tracker.models} == {
        "preop_model",
        "postop_model",
        "adapted_model",
    }
    assert loaded.tuning_iteration_tracker.current_iteration == 1
    assert loaded.tuning_iteration_tracker.max_iterations == 5
    assert loaded.progress_tracker.iterations is not None
    assert loaded.progress_tracker.iterations["current"] == 1
    assert (local_paths.run_dir / "progress_tracker.yaml").exists()


def test_init_run_creates_expected_structure(sample_config_files):
    paths, _ctx = init_run_workspace(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-xyz",
    )

    assert paths.run_dir.exists()
    assert paths.manifest.exists()
    assert (paths.config_snapshot / "clusters.yaml").exists()
    assert (paths.config_snapshot / "patients.yaml").exists()
    assert (paths.config_snapshot / "defaults.yaml").exists()


def test_update_run_progress_persists_milestone_event(sample_config_files):
    paths, _ctx = init_run_workspace(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-progress-1",
    )

    updated = update_run_progress(
        manifest_path=paths.manifest,
        model_id="postop_model",
        milestone_id="planned",
        status="in_progress",
        note="Postop stage is queued for execution.",
    )

    postop_model = next(m for m in updated.progress_tracker.models if m.model_id == "postop_model")
    milestone = next(ms for ms in postop_model.milestones if ms.id == "planned")
    assert milestone.status == "in_progress"
    assert updated.progress_tracker.events[-1].milestone_id == "planned"
    assert updated.progress_tracker.events[-1].model_id == "postop_model"


def test_manifest_roundtrips_converged_preop_and_postop_records(sample_config_files):
    paths, _ctx = init_run_workspace(
        workspace_root=sample_config_files,
        cluster_name="sherlock",
        patient_alias="TST-STAN-x",
        run_id="run-handoff",
    )
    manifest = read_manifest(paths.manifest)
    manifest = record_converged_preop_iteration(
        manifest,
        iteration=5,
        source_decision="not_close",
        selection_kind="operator_promoted_best_completed",
        reason="best tuned preop",
        metrics={"mpa_mean": 31.2},
        deltas={"mpa_mean": 0.4},
        remote_iteration_dir="/scratch/users/ndorn/svzt_runs/run-handoff/iterations/iter-05",
        remote_preop_dir="/scratch/users/ndorn/svzt_runs/run-handoff/iterations/iter-05/preop",
        remote_tuned_zerod_config="/scratch/users/ndorn/svzt_runs/run-handoff/iterations/iter-05/results/svzerod_3d_coupling_tuned.json",
        remote_canonical_coupler="/scratch/users/ndorn/svzt_runs/run-handoff/iterations/iter-05/results/svzerod_3Dcoupling.json",
        preop_job_id="12345",
        at="2026-05-12T00:00:00+00:00",
    )
    manifest = record_postop_submission(
        manifest,
        source_preop_iteration=5,
        local_dir=str(paths.run_dir / "postop" / "from-iter-05"),
        remote_dir="/scratch/users/ndorn/svzt_runs/run-handoff/postop/from-iter-05",
        local_job_script_path=str(paths.run_dir / "postop" / "from-iter-05" / "run_postop.sh"),
        remote_job_script_path="/scratch/users/ndorn/svzt_runs/run-handoff/postop/from-iter-05/run_postop.sh",
        postop_job_id="67890",
        at="2026-05-12T00:01:00+00:00",
    )
    write_manifest(manifest, paths.manifest)

    loaded = read_manifest(paths.manifest)
    assert loaded.converged_preop_iteration is not None
    assert loaded.converged_preop_iteration.iteration == 5
    assert loaded.converged_preop_iteration.selection_kind == "operator_promoted_best_completed"
    assert loaded.postop_run is not None
    assert loaded.postop_run.postop_job_id == "67890"
