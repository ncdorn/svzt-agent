"""Load and validate workspace configuration files."""

from __future__ import annotations

from pathlib import Path, PurePosixPath
import os
from typing import Any

import yaml

from svztagent.config.models import (
    ClusterConfig,
    ImpedanceTuningConfig,
    Iteration1SeedConfig,
    PatientAssetPaths,
    PatientDataLayoutDefaults,
    PatientConfig,
    ResolvedPatient,
    ThreedTuningConfig,
    WorkspaceConfig,
)
from svztagent.core.errors import ConfigError
from svztagent.core.paths import validate_remote_patient_read_path


def detect_workspace_root(workspace_root: str | Path | None = None) -> Path:
    if workspace_root is not None:
        candidate = Path(workspace_root).expanduser().resolve()
        _ensure_workspace_layout(candidate)
        return candidate

    env_root = os.environ.get("SVZ_WORKSPACE_ROOT")
    if env_root:
        candidate = Path(env_root).expanduser().resolve()
        _ensure_workspace_layout(candidate)
        return candidate

    cursor = Path.cwd().resolve()
    for candidate in [cursor, *cursor.parents]:
        if (candidate / "config" / "clusters.yaml").exists():
            _ensure_workspace_layout(candidate)
            return candidate

    raise ConfigError(
        "Could not detect workspace root. Set SVZ_WORKSPACE_ROOT or run from inside the workspace."
    )


def _ensure_workspace_layout(workspace_root: Path) -> None:
    expected = [
        workspace_root / "config" / "clusters.yaml",
        workspace_root / "config" / "patients.yaml",
        workspace_root / "config" / "defaults.yaml",
        workspace_root / "runs",
    ]
    missing = [str(path) for path in expected if not path.exists()]
    if missing:
        raise ConfigError(
            "Workspace root is missing required paths: " + ", ".join(missing)
        )


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ConfigError(f"Missing required config file: {path}")
    try:
        with path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"Invalid YAML in {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise ConfigError(f"Config file {path} must contain a mapping at top level")
    return data


def load_workspace_config(workspace_root: str | Path) -> WorkspaceConfig:
    root = Path(workspace_root).expanduser().resolve()
    _ensure_workspace_layout(root)

    clusters_raw = _load_yaml(root / "config" / "clusters.yaml")
    patients_raw = _load_yaml(root / "config" / "patients.yaml")
    defaults_raw = _load_yaml(root / "config" / "defaults.yaml")

    if "clusters" not in clusters_raw:
        raise ConfigError("config/clusters.yaml must define top-level key 'clusters'")
    if "patients" not in patients_raw:
        raise ConfigError("config/patients.yaml must define top-level key 'patients'")
    if "defaults" not in defaults_raw:
        raise ConfigError("config/defaults.yaml must define top-level key 'defaults'")

    try:
        return WorkspaceConfig.model_validate(
            {
                "clusters": clusters_raw["clusters"],
                "patients": patients_raw["patients"],
                "defaults": defaults_raw["defaults"],
            }
        )
    except Exception as exc:
        raise ConfigError(f"Workspace config validation failed: {exc}") from exc


def resolve_cluster(config: WorkspaceConfig, cluster_name: str) -> ClusterConfig:
    for cluster in config.clusters:
        if cluster.name == cluster_name:
            return cluster
    available = ", ".join(sorted(cluster.name for cluster in config.clusters))
    raise ConfigError(
        f"Unknown cluster '{cluster_name}'. Available clusters: {available or '<none>'}"
    )


def _resolve_patient_assets(
    patient_root: str,
    layout: PatientDataLayoutDefaults,
    iteration1_seed: Iteration1SeedConfig,
) -> PatientAssetPaths:
    root = PurePosixPath(patient_root)
    preop_mesh_complete = root / layout.preop_mesh_complete_dir
    mesh_surfaces = preop_mesh_complete / layout.mesh_surfaces_subdir
    postop_mesh_complete = (
        root / layout.postop_mesh_complete_dir
        if layout.postop_mesh_complete_dir is not None
        else None
    )
    postop_mesh_surfaces = (
        postop_mesh_complete / layout.mesh_surfaces_subdir
        if postop_mesh_complete is not None
        else None
    )
    seed_candidate = PurePosixPath(iteration1_seed.path)
    if seed_candidate.is_absolute():
        resolved_seed = str(seed_candidate)
    else:
        resolved_seed = str(root / seed_candidate)
    return PatientAssetPaths(
        clinical_targets=str(root / layout.clinical_targets_csv),
        centerlines=str(root / layout.centerlines_vtp),
        inflow=str(root / layout.inflow_csv),
        preop_mesh_complete_dir=str(preop_mesh_complete),
        mesh_surfaces_dir=str(mesh_surfaces),
        postop_mesh_complete_dir=str(postop_mesh_complete)
        if postop_mesh_complete is not None
        else None,
        postop_mesh_surfaces_dir=str(postop_mesh_surfaces)
        if postop_mesh_surfaces is not None
        else None,
        iteration1_seed_source=iteration1_seed.source,
        iteration1_seed_path=resolved_seed,
    )


def _resolve_iteration1_seed_config(
    config: WorkspaceConfig,
    patient: PatientConfig,
) -> Iteration1SeedConfig:
    patient_override = (
        patient.tuning.iteration1_seed
        if patient.tuning is not None and patient.tuning.iteration1_seed is not None
        else None
    )
    return patient_override or config.defaults.tuning.iteration1_seed


def _resolve_patient_threed_config(
    config: WorkspaceConfig,
    patient: PatientConfig,
) -> ThreedTuningConfig:
    merged = config.defaults.tuning.threed.model_dump(mode="json")
    override = (
        patient.tuning.threed
        if patient.tuning is not None and patient.tuning.threed is not None
        else None
    )
    if override is not None:
        merged.update(override.model_dump(mode="json", exclude_none=True))
    return ThreedTuningConfig.model_validate(merged)


def _resolve_patient_impedance_config(
    config: WorkspaceConfig,
    patient: PatientConfig,
) -> ImpedanceTuningConfig:
    merged = config.defaults.tuning.impedance.model_dump(mode="json")
    override = (
        patient.tuning.impedance
        if patient.tuning is not None and patient.tuning.impedance is not None
        else None
    )
    if override is not None:
        override_payload = override.model_dump(mode="json", exclude_none=True)
        # tune_space is full replacement at patient level (no patch-merge semantics).
        if "tune_space" in override_payload:
            merged["tune_space"] = override_payload.pop("tune_space")
        merged.update(override_payload)
    return ImpedanceTuningConfig.model_validate(merged)


def _resolve_patient_mesh_scale_factor(
    config: WorkspaceConfig,
    patient: PatientConfig,
) -> float:
    return (
        float(patient.mesh_scale_factor)
        if patient.mesh_scale_factor is not None
        else float(config.defaults.mesh_scale_factor)
    )


def resolve_patient_alias(
    config: WorkspaceConfig,
    cluster_name: str,
    patient_alias: str,
) -> ResolvedPatient:
    cluster = resolve_cluster(config, cluster_name)

    patient = None
    for candidate in config.patients:
        if candidate.alias == patient_alias:
            patient = candidate
            break

    if patient is None:
        available = ", ".join(sorted(p.alias for p in config.patients))
        raise ConfigError(
            f"Unknown patient alias '{patient_alias}'. Available aliases: {available or '<none>'}"
        )

    if patient.data_policy != "read_only":
        raise ConfigError(
            f"Patient alias '{patient_alias}' has data_policy='{patient.data_policy}', expected 'read_only'"
        )

    validate_remote_patient_read_path(
        patient.remote_path,
        cluster.remote_roots.patient_data_root,
    )

    if cluster.remote_roots.permanent_data_root is None:
        raise ConfigError(
            "cluster remote_roots.permanent_data_root is required for durable patient asset resolution"
        )
    if patient.permanent_remote_path is None:
        raise ConfigError(
            f"Patient alias '{patient_alias}' must define permanent_remote_path for durable asset resolution"
        )

    validate_remote_patient_read_path(
        patient.permanent_remote_path,
        cluster.remote_roots.permanent_data_root,
    )

    patient_assets = _resolve_patient_assets(
        patient.permanent_remote_path,
        config.defaults.patient_data_layout,
        _resolve_iteration1_seed_config(config, patient),
    )
    validate_remote_patient_read_path(
        patient_assets.clinical_targets,
        patient.permanent_remote_path,
    )
    validate_remote_patient_read_path(
        patient_assets.centerlines,
        patient.permanent_remote_path,
    )
    validate_remote_patient_read_path(
        patient_assets.inflow,
        patient.permanent_remote_path,
    )
    validate_remote_patient_read_path(
        patient_assets.preop_mesh_complete_dir,
        patient.permanent_remote_path,
    )
    validate_remote_patient_read_path(
        patient_assets.mesh_surfaces_dir,
        patient.permanent_remote_path,
    )
    if patient_assets.postop_mesh_complete_dir is not None:
        validate_remote_patient_read_path(
            patient_assets.postop_mesh_complete_dir,
            patient.permanent_remote_path,
        )
    if patient_assets.postop_mesh_surfaces_dir is not None:
        validate_remote_patient_read_path(
            patient_assets.postop_mesh_surfaces_dir,
            patient.permanent_remote_path,
        )
    if not patient_assets.iteration1_seed_path.startswith("/"):
        raise ConfigError("resolved iteration-1 seed path must be absolute")

    return ResolvedPatient(
        cluster_name=cluster.name,
        alias=patient.alias,
        remote_path=patient.remote_path,
        permanent_remote_path=patient.permanent_remote_path,
        patient_assets=patient_assets,
        threed=_resolve_patient_threed_config(config, patient),
        impedance=_resolve_patient_impedance_config(config, patient),
        mesh_scale_factor=_resolve_patient_mesh_scale_factor(config, patient),
        data_policy=patient.data_policy,
        patient_data_root=cluster.remote_roots.patient_data_root,
        permanent_data_root=cluster.remote_roots.permanent_data_root,
        runs_root=cluster.remote_roots.runs_root,
    )
