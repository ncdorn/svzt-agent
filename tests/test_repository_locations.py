from __future__ import annotations

from pathlib import Path
import shutil

import pytest

from svztagent.config.load import (
    load_workspace_config,
    resolve_cluster,
    resolve_patient_alias,
    resolve_repository_locations,
)
from svztagent.core.errors import ConfigError
from svztagent.core.manifest import copy_config_snapshot, create_manifest
from svztagent.core.paths import build_local_run_paths, ensure_local_run_dirs


def _switch_to_sibling_repo_layout(workspace: Path) -> dict[str, Path]:
    shutil.rmtree(workspace / "repos")
    sibling_root = workspace.parent
    paths = {}
    for name in ("svzt-agent", "svZeroDTrees", "svZeroDSolver"):
        path = sibling_root / name
        path.mkdir(parents=True, exist_ok=True)
        paths[name] = path
    return paths


def test_resolve_repository_locations_defaults_to_package_mode_without_sibling_checkouts(
    sample_config_files,
):
    config = load_workspace_config(sample_config_files)

    repos = resolve_repository_locations(config, sample_config_files)

    assert repos == {
        "svzt_agent": None,
        "svZeroDTrees": None,
        "svZeroDSolver": None,
    }


def test_resolve_repository_locations_prefers_sibling_checkouts(sample_config_files):
    sibling_root = sample_config_files.parent
    (sibling_root / "svzt-agent").mkdir()
    (sibling_root / "svZeroDTrees").mkdir()
    (sibling_root / "svZeroDSolver").mkdir()

    config = load_workspace_config(sample_config_files)
    repos = resolve_repository_locations(config, sample_config_files)

    assert repos["svzt_agent"] == str((sibling_root / "svzt-agent").resolve())
    assert repos["svZeroDTrees"] == str((sibling_root / "svZeroDTrees").resolve())
    assert repos["svZeroDSolver"] == str((sibling_root / "svZeroDSolver").resolve())


def test_resolve_repository_locations_supports_configured_paths(sample_config_files):
    sibling_root = sample_config_files.parent
    (sibling_root / "svzt-agent").mkdir()
    (sibling_root / "svZeroDTrees").mkdir()
    (sibling_root / "svZeroDSolver").mkdir()
    (sample_config_files / "config" / "repositories.yaml").write_text(
        """
repositories:
  svzt_agent: "../svzt-agent"
  svZeroDTrees: "../svZeroDTrees"
  svZeroDSolver: "../svZeroDSolver"
""".strip()
        + "\n",
        encoding="utf-8",
    )

    config = load_workspace_config(sample_config_files)
    repos = resolve_repository_locations(config, sample_config_files)

    assert repos["svzt_agent"] == str((sibling_root / "svzt-agent").resolve())
    assert repos["svZeroDTrees"] == str((sibling_root / "svZeroDTrees").resolve())
    assert repos["svZeroDSolver"] == str((sibling_root / "svZeroDSolver").resolve())


def test_resolve_repository_locations_rejects_missing_configured_paths(sample_config_files):
    (sample_config_files / "config" / "repositories.yaml").write_text(
        """
repositories:
  svzt_agent: "../missing-svzt-agent"
""".strip()
        + "\n",
        encoding="utf-8",
    )
    config = load_workspace_config(sample_config_files)

    with pytest.raises(ConfigError, match="Configured repository path for 'svzt_agent'"):
        resolve_repository_locations(config, sample_config_files)


def test_create_manifest_allows_package_mode_without_local_repo_checkouts(sample_config_files):
    shutil.rmtree(sample_config_files / "repos")

    config = load_workspace_config(sample_config_files)
    cluster = resolve_cluster(config, "sherlock")
    patient = resolve_patient_alias(config, "sherlock", "TST-STAN-x")
    local_paths = build_local_run_paths(sample_config_files, "run-package-mode")
    ensure_local_run_dirs(local_paths)

    manifest = create_manifest(
        run_id="run-package-mode",
        cluster=cluster,
        patient=patient,
        local_paths=local_paths,
        workspace_root=sample_config_files,
        config=config,
    )

    assert manifest.repos == {
        "svzt_agent": None,
        "svZeroDTrees": None,
        "svZeroDSolver": None,
    }


def test_create_manifest_preserves_run_contract_under_sibling_layout(sample_config_files):
    config = load_workspace_config(sample_config_files)
    cluster = resolve_cluster(config, "sherlock")
    patient = resolve_patient_alias(config, "sherlock", "TST-STAN-x")
    local_paths = build_local_run_paths(sample_config_files, "run-layout-contract")
    ensure_local_run_dirs(local_paths)
    legacy_manifest = create_manifest(
        run_id="run-layout-contract",
        cluster=cluster,
        patient=patient,
        local_paths=local_paths,
        workspace_root=sample_config_files,
        config=config,
    )

    sibling_paths = _switch_to_sibling_repo_layout(sample_config_files)
    sibling_config = load_workspace_config(sample_config_files)
    sibling_cluster = resolve_cluster(sibling_config, "sherlock")
    sibling_patient = resolve_patient_alias(sibling_config, "sherlock", "TST-STAN-x")
    sibling_manifest = create_manifest(
        run_id="run-layout-contract",
        cluster=sibling_cluster,
        patient=sibling_patient,
        local_paths=local_paths,
        workspace_root=sample_config_files,
        config=sibling_config,
    )

    assert sibling_manifest.cluster == legacy_manifest.cluster
    assert sibling_manifest.patient == legacy_manifest.patient
    assert sibling_manifest.remote == legacy_manifest.remote
    assert sibling_manifest.local_paths == legacy_manifest.local_paths
    assert sibling_manifest.repos == {
        "svzt_agent": str(sibling_paths["svzt-agent"].resolve()),
        "svZeroDTrees": str(sibling_paths["svZeroDTrees"].resolve()),
        "svZeroDSolver": str(sibling_paths["svZeroDSolver"].resolve()),
    }


def test_copy_config_snapshot_includes_optional_repositories_yaml(sample_config_files, tmp_path):
    (sample_config_files / "config" / "repositories.yaml").write_text(
        """
repositories:
  svzt_agent: "../svzt-agent"
""".strip()
        + "\n",
        encoding="utf-8",
    )

    snapshot_dir = tmp_path / "snapshot"
    copy_config_snapshot(sample_config_files, snapshot_dir)

    assert (snapshot_dir / "clusters.yaml").exists()
    assert (snapshot_dir / "patients.yaml").exists()
    assert (snapshot_dir / "defaults.yaml").exists()
    assert (snapshot_dir / "repositories.yaml").exists()
