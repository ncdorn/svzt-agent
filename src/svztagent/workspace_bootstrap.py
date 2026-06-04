"""Workspace bootstrap and validation helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import os

from svztagent.config.load import (
    detect_workspace_root,
    load_workspace_config,
    resolve_repository_locations,
)
from svztagent.core.errors import ConfigError


_WORKSPACE_TEMPLATE_FILES: dict[str, str] = {
    "config/clusters.yaml": """clusters:
  - name: "example-cluster"
    host: "cluster.example.edu"
    user: "your-username"
    scheduler:
      type: "slurm"
    executables:
      svfsiplus_path: "/path/to/svmultiphysics"
      svslicer_path: "/path/to/svslicer"
    remote_roots:
      patient_data_root: "/path/to/patient-data"
      permanent_data_root: "/path/to/permanent-patient-data"
      runs_root: "/scratch/users/your-username/svzt_runs"
""",
    "config/patients.yaml": """patients:
  - alias: "EXAMPLE-PATIENT"
    remote_path: "/path/to/patient-data/EXAMPLE-PATIENT"
    permanent_remote_path: "/path/to/permanent-patient-data/EXAMPLE-PATIENT"
    data_policy: "read_only"
""",
    "config/defaults.yaml": """defaults:
  rsync:
    include_patterns: ["*.json", "*.yaml", "*.csv", "*.vtp", "*.vtu"]
    exclude_patterns: ["*.tmp", "__pycache__/**"]
  artifacts:
    pull: ["manifest.yaml", "results/**", "logs/**"]
  scheduler:
    account: null
    partition: "<partition>"
    wall_time: "12:00:00"
    mem: "16G"
    cpus: "24"
  execution:
    python_executable: "python3"
    env_activation_hooks:
      - "source ~/.bashrc"
  validation:
    require_dry_run_before_execute: true
    enforce_remote_write_root: true
  monitoring:
    poll_interval_seconds: 30
    fetch_on_failure: false
  mesh_scale_factor: 1.0
  tuning:
    iteration1_seed:
      source: "path"
      path: "simplified_nonlinear_zerod.json"
    threed:
      wall_model: "deformable"
      inflow_boundary_condition: "neumann"
      prestress_file: "auto"
    impedance:
      solver: "Nelder-Mead"
      nm_iter: 5
      n_procs: 24
  adaptation:
    default_model: "M2"
    models:
      m1:
        max_nodes: 200000
        wss_gain: 0.01
  patient_data_layout:
    clinical_targets_csv: "clinical_targets.csv"
    centerlines_vtp: "centerlines.vtp"
    inflow_csv: "inflow.csv"
    preop_mesh_complete_dir: "preop-mesh-complete"
    mesh_surfaces_subdir: "mesh-surfaces"
""",
    "config/clinical_targets.yaml": """clinical_targets:
  EXAMPLE-PATIENT:
    mpa_mean_mmHg: 25.0
    rpa_flow_split: 0.55
""",
    "config/repositories.yaml": """repositories:
  svzt_agent: "../svzt-agent"
  svZeroDTrees: "../svZeroDTrees"
  svZeroDSolver: "../../svZeroDSolver"
""",
}


@dataclass(frozen=True)
class WorkspaceInitResult:
    workspace_root: Path
    created_directories: list[Path]
    written_files: list[Path]


@dataclass(frozen=True)
class WorkspaceValidationResult:
    workspace_root: Path
    cluster_names: list[str]
    patient_aliases: list[str]
    repository_locations: dict[str, str | None]
    optional_config_files: dict[str, bool]


@dataclass(frozen=True)
class WorkspaceDoctorResult:
    workspace_root: Path
    repository_locations: dict[str, str | None]
    warnings: list[str] = field(default_factory=list)


def init_workspace(target_root: str | Path, *, force: bool = False) -> WorkspaceInitResult:
    root = Path(target_root).expanduser().resolve()
    if root.exists() and not root.is_dir():
        raise ConfigError(f"Workspace root exists and is not a directory: {root}")

    root.mkdir(parents=True, exist_ok=True)
    created_directories: list[Path] = []
    for relative_dir in ("config", "runs", "mirrors", "templates"):
        directory = root / relative_dir
        if not directory.exists():
            directory.mkdir(parents=True, exist_ok=True)
            created_directories.append(directory)

    existing = [
        str((root / relative_path))
        for relative_path in _WORKSPACE_TEMPLATE_FILES
        if (root / relative_path).exists()
    ]
    if existing and not force:
        raise ConfigError(
            "Refusing to overwrite existing workspace files: " + ", ".join(sorted(existing))
        )

    written_files: list[Path] = []
    for relative_path, content in _WORKSPACE_TEMPLATE_FILES.items():
        destination = root / relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(content.strip() + "\n", encoding="utf-8")
        written_files.append(destination)

    return WorkspaceInitResult(
        workspace_root=root,
        created_directories=created_directories,
        written_files=written_files,
    )


def validate_workspace_config(workspace_root: str | Path | None = None) -> WorkspaceValidationResult:
    root = detect_workspace_root(workspace_root)
    config = load_workspace_config(root)
    repository_locations = resolve_repository_locations(config, root)
    optional_config_files = {
        name: (root / "config" / name).exists()
        for name in ("clinical_targets.yaml", "repositories.yaml")
    }
    return WorkspaceValidationResult(
        workspace_root=root,
        cluster_names=sorted(cluster.name for cluster in config.clusters),
        patient_aliases=sorted(patient.alias for patient in config.patients),
        repository_locations=repository_locations,
        optional_config_files=optional_config_files,
    )


def doctor_workspace(workspace_root: str | Path | None = None) -> WorkspaceDoctorResult:
    validation = validate_workspace_config(workspace_root)
    warnings: list[str] = []

    env_root = os.environ.get("SVZ_WORKSPACE_ROOT")
    if env_root:
        resolved_env_root = Path(env_root).expanduser().resolve()
        if resolved_env_root != validation.workspace_root:
            warnings.append(
                "SVZ_WORKSPACE_ROOT does not match the detected workspace root: "
                f"{resolved_env_root} != {validation.workspace_root}"
            )

    if not validation.optional_config_files["clinical_targets.yaml"]:
        warnings.append(
            "Optional config/clinical_targets.yaml is missing; add it if you manage "
            "workspace-level clinical target overrides."
        )

    for repo_name, repo_path in validation.repository_locations.items():
        if repo_path is None:
            warnings.append(
                f"Repository location for {repo_name} is not present locally; "
                "package-mode execution remains supported, but local provenance "
                "snapshots will not record a checkout path."
            )

    return WorkspaceDoctorResult(
        workspace_root=validation.workspace_root,
        repository_locations=validation.repository_locations,
        warnings=warnings,
    )
