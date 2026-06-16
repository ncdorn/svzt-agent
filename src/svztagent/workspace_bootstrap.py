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
  - name: "sherlock"
    host: "login.sherlock.stanford.edu"
    user: "ndorn"
    communication:
      transport: "ssh"
      ssh_alias: "sherlock"
      command_mode: "non_interactive"
    scheduler:
      type: "slurm" # slurm|pbs|other
    executables:
      svfsiplus_path: "/home/users/ndorn/svMP-build/svMultiPhysics-build/bin/svmultiphysics" # absolute solver executable path on cluster
      svslicer_path: "/home/users/ndorn/svSlicer/Release-sherlock/svslicer" # absolute svSlicer executable path on cluster for resistance-map postprocessing
      pvpython_path: "/home/groups/amarsden/ParaView-5.13.3-osmesa-MPI-Linux-Python3.10-x86_64/bin/pvpython" # OSMesa offscreen ParaView Python for 3D CFD visualization
    remote_roots:
      patient_data_root: "/scratch/users/ndorn/models/PPAS/tof-stent" # read-only active data for simulation
      permanent_data_root: "/oak/stanford/groups/amarsden/ndorn/PPAS-study/tof-stent" # read-only long-term storage
      runs_root: "/scratch/users/ndorn/svzt_runs" # write allowed
    notes: "No secrets. Replace placeholders per environment."
""",
    "config/patients.yaml": """patients:
  - alias: "TST-STAN-1"
    remote_path: "/scratch/users/ndorn/models/PPAS/tof-stent/TST-STAN-1"
    permanent_remote_path: "/oak/stanford/groups/amarsden/ndorn/PPAS-study/tof-stent/TST-STAN-1"
    data_policy: "read_only"
    mesh_scale_factor: 1.147
    tuning:
      iteration1_seed:
        source: "generate"
      impedance:
        diameter_scale: 0.1
      threed:
        wall_model: "deformable"
        prestress_file: "/oak/stanford/groups/amarsden/ndorn/PPAS-study/tof-stent/TST-STAN-1/prestress/1-procs/result_009.vtu"
        dt: 0.0004
    adaptation:
      models:
        m1:
          terminal_resistance: 100000.0
    postprocess:
      paraview_viz:
        cycle_duration_s: 0.8
        camera_offset_dir:
          [0.1986115187444734, -0.3233299868216392, -0.9252087246907761]
        camera_view_up:
          [0.5705942973837406, -0.7293857273084497, 0.3773839008117323]
    notes: "Active simulation input directory on scratch"
  - alias: "TST-STAN-2"
    remote_path: "/scratch/users/ndorn/models/PPAS/tof-stent/TST-STAN-2"
    permanent_remote_path: "/oak/stanford/groups/amarsden/ndorn/PPAS-study/tof-stent/TST-STAN-2"
    data_policy: "read_only"
    mesh_scale_factor: 1.0
    tuning:
      iteration1_seed:
        source: "generate"
      impedance:
        diameter_scale: 0.1
      threed:
        wall_model: "deformable"
        prestress_file: "generate"
        dt: 0.0004
    postprocess:
      paraview_viz:
        cycle_duration_s: 0.75
        camera_offset_dir:
          [-0.13071549768520443, -0.6767690315671042, -0.7244978513264432]
        camera_view_up:
          [0.2017198545601613, -0.7336369592312861, 0.6489113285542947]
    notes: "Active simulation input directory on scratch"
  - alias: "TST-STAN-3"
    remote_path: "/scratch/users/ndorn/models/PPAS/tof-stent/TST-STAN-3"
    permanent_remote_path: "/oak/stanford/groups/amarsden/ndorn/PPAS-study/tof-stent/TST-STAN-3"
    data_policy: "read_only"
    mesh_scale_factor: 1.0
    tuning:
      iteration1_seed:
        source: "generate"
      impedance:
        diameter_scale: 0.1
      threed:
        wall_model: "deformable"
        prestress_file: "generate"
        dt: 0.0004
    postprocess:
      paraview_viz:
        cycle_duration_s: 0.75
        camera_offset_dir:
          [-0.12776678649593096, 0.9030795415827347, -0.41002803543565286]
        camera_view_up:
          [0.18205962045423338, -0.3850361222623854, -0.9047659803248606]
    notes: "Active simulation input directory on scratch"
  - alias: "TST-STAN-5"
    remote_path: "/scratch/users/ndorn/models/PPAS/tof-stent/TST-STAN-5"
    permanent_remote_path: "/oak/stanford/groups/amarsden/ndorn/PPAS-study/tof-stent/TST-STAN-5"
    data_policy: "read_only"
    mesh_scale_factor: 1.0
    notes: "Active simulation input directory on scratch"
    tuning:
      iteration1_seed:
        source: "generate"
      impedance:
        diameter_scale: 0.1
      threed:
        wall_model: "deformable"
        prestress_file: "/oak/stanford/groups/amarsden/ndorn/PPAS-study/tof-stent/TST-STAN-5/prestress/1-procs/result_009.vtu"
        dt: 0.0004
    postprocess:
      paraview_viz:
        cycle_duration_s: 0.8
        camera_offset_dir:
          [0.40843895287418513, -0.36806263530149963, -0.8352888831236498]
        camera_view_up:
          [0.2901210689043006, -0.8152983227207212, 0.5011171622950116]
  - alias: "TST-STAN-9"
    remote_path: "/scratch/users/ndorn/models/PPAS/tof-stent/TST-STAN-9"
    permanent_remote_path: "/oak/stanford/groups/amarsden/ndorn/PPAS-study/tof-stent/TST-STAN-9"
    data_policy: "read_only"
    mesh_scale_factor: 1.425
    tuning:
      iteration1_seed:
        source: "generate"
      impedance:
        diameter_scale: 0.1
      threed:
        wall_model: "deformable"
        prestress_file: "generate"
        dt: 0.0004
    notes: "Active simulation input directory on scratch"
    postprocess:
      paraview_viz:
        cycle_duration_s: 0.75
        camera_offset_dir:
          [0.34676233891863795, -0.07090412972254183, -0.9352692043983725]
        camera_view_up:
          [0.4023200220431169, -0.8895072263158575, 0.21659984809573574]
""",
    "config/defaults.yaml": """defaults:
  rsync:
    include_patterns:
      - "*.json"
      - "*.yaml"
      - "*.vtp"
      - "*.csv"
    exclude_patterns:
      - "*.tmp"
      - "*.log"
      - "__pycache__/"
  artifacts:
    pull:
      - "manifest.yaml"
      - "logs/**"
      - "results/**"
  scheduler:
    partition: "amarsden"
    wall_time: "24:00:00"
    mem: "16G"
    cpus: "4"
  execution:
    python_executable: "python3"
  postprocess:
    resistance_map:
      workers: "auto"
      selected_preop_mem: "64G"
    paraview_viz:
      image_resolution: [1920, 1080]
      camera_offset_dir: [0.1986115187444734, -0.3233299868216392, -0.9252087246907761]
      camera_view_up: [0.5705942973837406, -0.7293857273084497, 0.3773839008117323]
      pressure_field: "Pressure"
      velocity_field: "Velocity"
      wss_field: "WSS"
      displacement_field: "Displacement"
      wall_time_hours: 2
      mem: "32G"
      cpus: 1
  validation:
    require_dry_run_before_execute: true
    enforce_remote_write_root: true
  mesh_scale_factor: 1.0
  tuning:
    iteration1_seed:
      source: "path"
      path: "simplified_nonlinear_zerod.json"
    impedance:
      solver: "Nelder-Mead"
      nm_iter: 5
      n_procs: 24
      grid_search_init: true
      d_min: 0.01
      use_mean: true
      specify_diameter: true
      rescale_inflow: true
      convert_to_cm: false
      compliance_model: "olufsen"
      diameter_scale: 0.0
      diameter_std_cap: null
      allow_ordered_outlet_mapping: false
      tuning_model: "rri"
      tune_space:
        free:
          - name: "lpa.xi"
            init: 2.3
            lb: 0.0
            ub: 6.0
          - name: "lpa.eta_sym"
            init: 0.6
            lb: 0.3
            ub: 0.9
          - name: "rpa.xi"
            init: 2.3
            lb: 0.0
            ub: 6.0
          - name: "rpa.eta_sym"
            init: 0.7
            lb: 0.3
            ub: 0.9
          - name: "lpa.inductance"
            init: 1.0
            lb: 0.0
            ub: "inf"
          - name: "rpa.inductance"
            init: 1.0
            lb: 0.0
            ub: "inf"
          - name: "comp.lpa.k2"
            init: -75.0
            lb: -100.0
            ub: -1.0
        fixed:
          - name: "lrr"
            value: 10.0
          - name: "d_min"
            value: 0.01
        tied:
          - name: "comp.rpa.k2"
            other: "comp.lpa.k2"
            fn: "identity"
    threed:
      wall_model: "deformable"
      inflow_boundary_condition: "dirichlet"
      elasticity_modulus: 2500000
      poisson_ratio: 0.5
      shell_thickness: 0.2
      prestress_file: "auto"
      tissue_support:
        enabled: true
        type: "uniform"
        stiffness: 1000.0
        damping: 10000.0
        apply_along_normal_direction: true
        spatial_values_file_path: null
      n_tsteps: 4000
      dt: 0.0005
      nodes: 3
      procs_per_node: 24
      memory: 16
      hours: 20
      wait_poll_seconds: 30
      wait_timeout_seconds: 43200
  adaptation:
    default_model: "M2"
    territory_scheme: "lpa_rpa"
    target_stage: "postop"
    parameter_policy: "global_fixed"
    parameter_sets: {}
    models:
      m1:
        max_nodes: 200000
        wss_gain: 0.01
        terminal_resistance: 0.0
      m2:
        iterations: 1
        wss_gain: 1.0
        ims_gain: 1.0
        compliance_gain: 1.0
      m3:
        iterations: 1
        k_arr: [1.0, 1.0, 1.0, 1.0]
  patient_data_layout:
    clinical_targets_csv: "clinical_targets.csv"
    centerlines_vtp: "centerlines.vtp"
    inflow_csv: "inflow.csv"
    preop_mesh_complete_dir: "preop-mesh-complete"
    postop_mesh_complete_dir: "postop-meshes/clinical-postop-mesh-complete"
    mesh_surfaces_subdir: "mesh-surfaces"
""",
    "config/clinical_targets.yaml": """clinical_targets:
  source:
    type: "remote_csv"
    pulled_on: "2026-03-05"
    patient_data_root: "/oak/stanford/groups/amarsden/ndorn/PPAS-study/tof-stent"
  units:
    pressure: "mmHg"
    flow: "mL/s"
    split: "fraction"
  preop:
    patients:
      TST-STAN-1:
        source_csv: "/oak/stanford/groups/amarsden/ndorn/PPAS-study/tof-stent/TST-STAN-1/clinical_targets.csv"
        mpa_pressure: [38.0, 10.0, 23.0]
        mpa_flow: 21.6
        rpa_split: 0.71
        wedge_pressure: 8.0
        regurgitation: true
      TST-STAN-2:
        source_csv: "/oak/stanford/groups/amarsden/ndorn/PPAS-study/tof-stent/TST-STAN-2/clinical_targets.csv"
        mpa_pressure: [44.0, 14.0, 26.0]
        mpa_flow: 23.5
        rpa_split: 0.762
        wedge_pressure: 9.0
        regurgitation: false
      TST-STAN-3:
        source_csv: "/oak/stanford/groups/amarsden/ndorn/PPAS-study/tof-stent/TST-STAN-3/clinical_targets.csv"
        mpa_pressure: [67.0, 9.0, 32.0]
        mpa_flow: 78.0
        rpa_split: 0.446
        wedge_pressure: 6.0
        regurgitation: false
      TST-STAN-5:
        source_csv: "/oak/stanford/groups/amarsden/ndorn/PPAS-study/tof-stent/TST-STAN-5/clinical_targets.csv"
        mpa_pressure: [34.0, 3.0, 16.0]
        mpa_flow: 46.5
        rpa_split: 0.87
        wedge_pressure: 7.0
        regurgitation: true
      TST-STAN-9:
        source_csv: "/oak/stanford/groups/amarsden/ndorn/PPAS-study/tof-stent/TST-STAN-9/clinical_targets.csv"
        mpa_pressure: [29.0, 5.0, 15.0]
        mpa_flow: 31.35
        rpa_split: 0.903
        wedge_pressure: 5.0
        regurgitation: false
  postop:
    patients:
      TST-STAN-1:
        mpa_pressure: [25.0, 8.0, 15.0]
        mpa_flow: 21.6
        rpa_split: 0.555
        wedge_pressure: 8.0
      TST-STAN-2:
        mpa_pressure: [39.0, 16.0, 26.0]
        mpa_flow: 23.5
        rpa_split: 0.772
        wedge_pressure: 11.0
      TST-STAN-3:
        mpa_pressure: [64.0, 10.0, 30.0]
        mpa_flow: 78.0
        rpa_split: 0.468
        wedge_pressure: 6.0
      TST-STAN-5:
        mpa_pressure: [30.0, 8.0, 17.0]
        mpa_flow: 46.5
        rpa_split: 0.631
        wedge_pressure: 7.0
""",
    "config/repositories.yaml": """repositories:
  svzt_agent: "../svzt-agent"
  svZeroDTrees: "../svZeroDTrees"
  svZeroDSolver: "../../svZeroDSolver"
""",
}

_WORKSPACE_AGENTS_TEMPLATE = """# AGENTS.md

## How To Use This File
- Treat this file as the workspace router and rules-of-engagement brief.
- Follow the linked docs for workflow behavior, schemas, and CLI semantics.
- Keep this file short. Put implementation detail in repo docs, not here.

## Workspace Purpose
- This workspace is the local control plane for running `svzt-agent` workflows.
- It owns workspace config, run artifacts, manifests, and operator-facing workflow state.
- `svzt-agent/` remains the implementation home for orchestration code and CLI behavior.
- `svZeroDTrees/` and `svZeroDSolver/` remain upstream dependency repos, not the default target for workspace-policy changes.

## Authority Map
- Workspace config examples and local overrides: `config/`
- Run artifacts and manifests: `runs/`
- Operator workflow guide: `svzt-agent/docs/OPERATOR_RUNBOOK.md`
- Execution contract: `svzt-agent/docs/EXECUTION.md`
- Architecture and module ownership: `svzt-agent/docs/ARCHITECTURE.md`
- Monitoring and auto-advance semantics: `svzt-agent/docs/MONITORING.md`
- Patient-data safety contract: `svzt-agent/docs/PATIENT_DATA_CONTRACT.md`
- HPC safety rules: `svzt-agent/docs/HPC_SAFETY.md`

## Non-Negotiable Rules
- Patient data paths are read-only.
- Permanent archival data paths are read-only.
- All remote writes must stay under the configured `runs_root`.
- Dry-run and plan validation come before remote mutation.
- Manifests are the source of truth for run state and iteration history.
- Do not bypass typed adapters or path-policy checks with ad hoc shell logic.

## Workspace Layout
- `config/` stores cluster, patient, default, clinical-target, and repository-location YAML.
- `runs/` stores run-scoped manifests, plans, iterations, postop outputs, and fetched artifacts.
- `mirrors/` stores optional mirrored inputs or reference material when used.
- `templates/` stores intentionally versioned workspace templates only.

## Change Guidance
- Change `svzt-agent/` when behavior, planning, manifests, monitoring, or CLI flow needs to change.
- Change this workspace when config values, run metadata, or operator inputs need to change.
- Keep changes small, deterministic, and reviewable.
"""


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

    agents_path = root / "AGENTS.md"
    if not agents_path.exists():
        agents_path.write_text(_WORKSPACE_AGENTS_TEMPLATE.strip() + "\n", encoding="utf-8")
        written_files.append(agents_path)

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
