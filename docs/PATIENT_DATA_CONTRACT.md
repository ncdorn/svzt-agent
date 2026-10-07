# Patient Data Contract (Oak)

This contract defines the canonical layout for each patient directory under `permanent_data_root` (Oak).

## Canonical Permanent Directory Layout

For a patient rooted at:
`<permanent_data_root>/<patient-alias>`

the desired directory structure is:

```text
<patient-alias>/
├── centerlines.vtp
├── clinical_targets.csv
├── inflow.csv
├── preop-mesh-complete/
│   └── mesh-surfaces/
├── postop-meshes/
│   └── clinical-postop-mesh-complete/
│       └── mesh-surfaces/
├── prestress/
│   └── 1-procs/
│       └── result_009.vtu
└── zerod-models/
    └── baseline_0d_learned.json
```

`svzt-agent` treats this tree as read-only source data. The workflow may read
from any configured asset path under the patient root, but remote writes still
belong under `runs_root`, never under `permanent_data_root`.

## Required Patient Assets

For a patient root like:
`/oak/.../tof-stent/TST-STAN-x`

the agent resolves:

- `clinical_targets.csv`
- `centerlines.vtp`
- `inflow.csv`
- `preop-mesh-complete/`
- `preop-mesh-complete/mesh-surfaces/` (derived subpath for svZeroDTrees BC workflows)
- optional `postop-meshes/clinical-postop-mesh-complete/`
- optional `postop-meshes/clinical-postop-mesh-complete/mesh-surfaces/` (when postop simulation submission is expected)
- optional `prestress/1-procs/result_009.vtu` or another configured patient-level prestress VTU
- optional `zerod-models/baseline_0d_learned.json` for learned-seed workflows and seed-sweep campaigns

These names are configured centrally in `defaults.patient_data_layout`.

## Iteration-1 Seed Contract

Iteration-1 simplified 0D seed behavior is also config-driven:

- global default: `defaults.tuning.iteration1_seed`
- optional per-patient override: `patients[].tuning.iteration1_seed`
- relative `path` values resolve to `<patient>/...` under `permanent_remote_path`
- absolute `path` values are used directly
- `source=generate` produces the established reduced iteration-1 seed
- `source=learned_zerod` consumes the required configured `path` as an existing
  patient-level input 0D JSON and uses the patient `centerlines.vtp` plus the
  configured external executables to produce a validated full-PA seed under the
  run directory
- learned generation requires full-PA impedance tuning and never writes its
  generated seed or provenance back into the patient directory
- runtime behavior: if `source=path` and the file is missing, reduced tuning
  falls back to generation; a missing or unreadable `source=learned_zerod` path
  fails closed before learned-zeroD execution and never falls back to steady
  generation
- local learned sources may be staged into a child run's `inputs/` as
  `source_0d_config.json`; the driver always records its canonical provenance
  copy at `seed_generation/learned/source_0d_config.json` before writing
  `full_pa_zerod.json`. The run-scoped copy normalizes deprecated
  `internal_junction` values to `NORMAL_JUNCTION`; the patient-owned source
  remains unchanged.
- default-policy learned full-PA calibration is submitted by
  `advance-iter --execute` after each `not_close` iteration and must promote
  before the next full-PA iteration. No patient asset is mutated.

## svZeroDTrees Mapping

The resolved Oak assets map directly to svZeroDTrees path keys:

- `paths.clinical_targets` -> `<patient>/clinical_targets.csv`
- `paths.inflow` -> `<patient>/inflow.csv`
- `paths.mesh_surfaces` -> `<patient>/preop-mesh-complete/mesh-surfaces`
- `paths.preop_mesh_complete` -> `<patient>/preop-mesh-complete`
- `paths.postop_mesh_complete` -> optional `<patient>/postop-meshes/clinical-postop-mesh-complete`

Reference points in `svZeroDTrees`:

- Path schema includes `clinical_targets`, `mesh_surfaces`, and `inflow`:
  `svzerodtrees/config.py` (`PathsConfig` and `_parse_paths`).
- `tune_bcs` and `construct_trees` require `paths.mesh_surfaces` and `paths.clinical_targets`:
  `svzerodtrees/api.py`.
- Full pipeline uses `clinical_targets` and optional `inflow`:
  `svzerodtrees/api.py` -> `PipelineWorkflow.run`.

Centerlines are used for post-processing projection:

- `svzerodtrees/post_processing/project_to_centerline.py` (`map_0d_on_centerline`).

## 3D Tuning Runtime Config

3D runtime defaults are centrally configured at:
- `defaults.tuning.threed`

Optional per-patient overrides are supported at:
- `patients[].tuning.threed`

Merged values are resolved into the manifest and used to render each iteration script. The default wall model is `deformable` with `prestress_file=auto` and uniform CMM Robin `tissue_support` enabled (`stiffness=1000`, `damping=10000`, normal direction only).

`prestress_file` behavior is:
- absolute path: pass the prescribed VTU through unchanged as `Prestress_file_path`
- `generate`: compute a run-scoped prestress result under
  `<runs_root>/<run_id>/prestress/` from iteration-1 seed-generation
  `steady/mean` wall traction and reuse it for CMM stages
- `auto` / `from_steady_mean`: legacy unsupported modes in `svzt-agent`; the
  iteration script logs a warning and continues without `Prestress_file_path`

Generated prestress depends on seed-generation evidence already present under
the run directory. If those `steady/mean` VTUs are missing, the iteration enters
`needs_review` rather than creating an extra steady simulation.

Explicit postop staging also consumes the same resolved `threed` config, but it
must generate a fresh prestress field against the postop mesh for deformable
wall runs. The final postop transient solve still preserves the resolved YAML
`threed` settings and the selected iteration's canonical
`svzerod_3Dcoupling.json`; only the nested prestress helper temporarily
normalizes to a single-rank run so it can produce a
postop-mesh-compatible `Prestress_file_path`. If the same postop workspace
already contains a completed prestress result, the workflow reuses that
run-scoped VTU instead of falling back to a patient-level prestress path.

Spatial Robin tissue support may reference a VTP file through `tissue_support.spatial_values_file_path`; that file is read by svMultiPhysics from the staged simulation directory and should be provided as part of the patient/runtime input setup, not written back into patient source data.

When a patient-level prestress file is configured explicitly, the preferred
location is under `<patient>/prestress/` in the permanent data tree so the
input remains versioned with the rest of the patient reference data.

## Impedance Tuning Runtime Config

Impedance defaults are centrally configured at:
- `defaults.tuning.bc_type`
- `defaults.tuning.impedance`
- `defaults.tuning.rcr`

Optional per-patient overrides are supported at:
- `patients[].tuning.bc_type`
- `patients[].tuning.impedance`
- `patients[].tuning.rcr`

Merged values are resolved into the run context and used by the iteration script to run:
1. 0D BC tuning
   - impedance: `optimized_params.csv`, `stree_impedance_optimization.log`
   - RCR: `optimized_rcr_params.csv`
2. BC-to-3D mapping (`pa_config_tuning_snapshot.json`, `svzerod_3d_coupling_tuned.json`)

`tuning_model` selects whether svZeroDTrees tunes a reduced RRI PA model (`rri`)
or a full pulmonary 0D model (`full_pa`). `diameter_std_cap` is passed through
when configured to bound outlet-diameter variance handling, while
`diameter_scale` controls how much of the outlet diameter spread is applied.
Nonzero `diameter_scale` only affects outlet-specific trees; svZeroDTrees
therefore normalizes final tree assignment to `use_mean: false` whenever
`diameter_scale > 0`. Full-PA objective evaluations use the same final tree
policy unless `objective_tree_policy` is set; that optional block (`use_mean`,
`diameter_scale`, `diameter_std_cap`, `reference_diameter`) selects the tree
policy used only inside the optimizer, for example shared
`conductance_matched` LPA/RPA trees while the published config keeps
per-outlet trees. It is forwarded unchanged to svZeroDTrees (see svZeroDTrees
`docs/full_pa_calibration.md#objective-tree-policy`).
For `full_pa`, the tuning snapshot must preserve the full model vessel and
outlet-BC topology; a reduced PA/RRI snapshot is treated as invalid evidence.
`svzerod_3d_coupling_tuned.json` is a tuned 0D source artifact. The canonical
svMultiPhysics input is `svzerod_3Dcoupling.json`, generated from that source and
required to contain `external_solver_coupling_blocks`. Deprecated
`svZeroD_interface.dat` files are not part of the runtime contract.
Default-policy full-PA cases retain the full model across iterations: a
`not_close` decision schedules calibration of the exact tuned full-PA model,
whose promoted output becomes the next seed. Reduced-RRI continuation is only
the explicit `legacy_rri_after_first` policy.

Reduced RRI tuning starts from two LPA/RPA BCs, but the tuned 3D coupling artifact
is expanded back to one outlet BC per 3D cap before `svzerod_3Dcoupling.json` is
staged for svMultiPhysics.

Full learned 0D configs may contain one outlet BC per 3D cap. In that case,
svZeroDTrees validates a deterministic mesh-cap to BC mapping before
optimization: by stored outlet metadata, by BC name, or geometrically against
the centerline the seed was generated from. The patient `centerlines.vtp` is
read (never written) as the default `outlet_mapping_centerline` for full-PA
`auto`/`centerline` mapping. Order-based mapping (`serialized_cap_order`, or the
deprecated `allow_ordered_outlet_mapping`) is legacy-only and is wrong for
centerline-generated seeds.

Gate behavior remains unchanged: it still requires `results/mpa_pressure_vs_time.csv`.

## Why This Scales

- One global layout definition in defaults; no per-patient filename drift.
- Per-patient entries only provide the durable patient root (`permanent_remote_path`).
- Run manifests capture resolved asset paths for reproducibility and audits.
