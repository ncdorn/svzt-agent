# Execution Mode

## Commands
- `svzt init-workspace [<path>] [--force]`
- `svzt config validate`
- `svzt doctor`
- `svzt run tune --cluster <name> --patient <alias> [--run-id <id>] [--execute]`
- `svzt run tune-iter --cluster <name> --patient <alias> --run-id <id> [--iteration <n>] [--execute]`
- `svzt plan calibrate --run-id <id> [--iteration <n>]`
- `svzt run calibrate --run-id <id> [--iteration <n>] [--dry-run|--execute]`
- `svzt calibration-status <run-id> [--iteration <n>]`
- `svzt preop select --run-id <id> --iteration <n> [--reason <text>]`
- `svzt run postop --run-id <id> [--dry-run|--execute]`
- `svzt run adapt --run-id <id> --model M1|M2|M3 [--parameter-set <name>] [--dry-run|--execute]`
- `svzt postprocess cfd-results --run-id <id> [--source-json <path>] [--template <path>] [--output <path>] [--overwrite]`
- `svzt postprocess tuning-progress --run-id <id> [--output-dir <path>] [--overwrite]`
- `svzt campaign seed-sweep plan --cluster <name> [--campaign-id <id>] [--patients <alias> ...]`
- `svzt campaign seed-sweep run <campaign-id> [--dry-run|--execute]`
- `svzt campaign seed-sweep summarize <campaign-id>`
- `svzt campaign seed-sweep slides <campaign-id>`
- `svzt campaign adapt-benchmark plan [--campaign-id <id>] [--run-ids <run-id> ...] [--models M1 M2 M3] [--parameter-set <name>] [--benchmark-mode predict|retrospective_fit]`
- `svzt campaign adapt-benchmark run <campaign-id> [--dry-run|--execute]`
- `svzt campaign adapt-benchmark summarize <campaign-id>`
- `svzt advance-iter --run-id <id> [--max-iterations <n>] [--execute]`
- `svzt watch <run-id> [--auto-advance] [--poll-interval-seconds <n>] [--timeout-seconds <n>] [--max-polls <n>] [--fetch-on-complete]`
- `svzt status <run-id>`
- `svzt fetch <run-id> [--dry-run]`

## Workspace Repository Locations
- Core workspace config remains:
  - `config/clusters.yaml`
  - `config/patients.yaml`
  - `config/defaults.yaml`
- Optional local checkout overrides live in `config/repositories.yaml`:
  - `repositories.svzt_agent`
  - `repositories.svZeroDTrees`
  - `repositories.svZeroDSolver`
- Relative paths are resolved from the workspace root.
- If `config/repositories.yaml` is absent, `svzt-agent` auto-discovers sibling
  checkouts and otherwise records no local repo checkout paths.
- If a repository path is configured explicitly, it must exist locally.

## Workspace Bootstrap And Validation
- `svzt init-workspace` creates a new local workspace root with example config
  files for `clusters.yaml`, `patients.yaml`, `defaults.yaml`,
  `clinical_targets.yaml`, and `repositories.yaml`.
- The generated example YAML mirrors the current `../svz` control-plane config
  shape so a fresh sibling workspace starts from the same field structure and
  defaults used in active operation.
- The generated patient-data layout defaults assume permanent patient assets are
  organized under each `permanent_remote_path` with:
  `clinical_targets.csv`, `centerlines.vtp`, `inflow.csv`,
  `preop-mesh-complete/mesh-surfaces/`,
  `postop-meshes/clinical-postop-mesh-complete/mesh-surfaces/`, and optional
  `prestress/` plus `zerod-models/` subtrees. See
  `docs/PATIENT_DATA_CONTRACT.md` for the canonical tree.
- `svzt init-workspace` also creates `AGENTS.md` at the workspace root when it
  is missing so agent sessions started inside the workspace have a local router.
- The command also creates `runs/`, `mirrors/`, and `templates/` directories so
  a new workspace starts with the expected local structure.
- `svzt config validate` validates the required YAML config, reports cluster and
  patient counts, and resolves the repository-location contract.
- `svzt doctor` runs the same config validation and additionally reports
  workspace warnings such as missing optional config files or absent local repo
  checkouts.

## Full-PA calibration prerequisites

Use the global `--workspace-root` option, set `SVZ_WORKSPACE_ROOT`, or run
from inside the workspace. The required workspace files are
`config/clusters.yaml`, `config/patients.yaml`, `config/defaults.yaml`, and
`runs/`; `config/repositories.yaml` is optional. Relative repository paths are
resolved from the workspace root and are recorded in each manifest.

The cluster used for a calibration run must have an absolute
`remote_roots.runs_root`, a reachable SSH `host`/`user`, and the normal
`svfsiplus_path`. A postprocess stage additionally requires
`executables.svslicer_path`. The generated calibration script runs the
upstream `svzerodtrees` CLI with `defaults.execution.python_executable` after
the configured `defaults.execution.env_activation_hooks`; those hooks must
make the compatible `svZeroDTrees`, `pysvzerod`, VTK/svSlicer, and solver
runtime available on the remote host. `svzerodsolver_build_dir` is recorded
and passed through to upstream 3D configuration when needed. Patient source
paths are read-only, and all staged or generated files must remain below
`<runs_root>/<run_id>/`.

Enable the agent-owned policy only for the intended patient/stage. Scientific
calibration settings belong to the upstream YAML contract:

```yaml
defaults:
  execution:
    python_executable: python3
    env_activation_hooks: []       # e.g. module/venv activation on the cluster
  tuning:
    impedance:
      tuning_model: full_pa
      outlet_mapping_mode: auto
    calibration:
      enabled: true
      next_iteration_seed_policy: calibrated_full_pa
```

Use `patients[].tuning.calibration` for a patient override. The accepted seed
policies are `calibrated_full_pa` (the default) and the explicitly selected,
one-release compatibility policy `legacy_rri_after_first`. Under the default
policy, a full-PA iteration after iteration one fails closed unless its
predecessor has a terminal, lineage-valid calibration promotion, and
`svzt preop select` / `svzt run postop` fail closed unless the selected
iteration has one; postop then consumes the remote
`iterations/iter-XX/calibration/results/calibrated_full_pa_zerod.json` as its
0D model. An RRI run is not a calibration run, and the seed policy has no
effect on it. Unknown keys, contradictory mapping settings, and
invalid policy values fail during config validation.

## Iteration-1 Seed Configuration
- Configure iteration-1 seed once in YAML:
  - `defaults.tuning.iteration1_seed`
  - optional `patients[].tuning.iteration1_seed` override
- Supported source modes:
  - `path`: use configured `path`; if missing at run time, fallback to `generate`
  - `generate`: generate the established reduced seed via svZeroDTrees steady solves
  - `learned_zerod`: require the configured `path` as the input 0D JSON, copy it
    into the run-scoped `seed_generation/learned/source_0d_config.json` provenance
    path, then invoke svZeroDTrees' validated learned-zeroD adapter and stage the
    resulting full-PA model as `inputs/full_pa_zerod.json`
- `learned_zerod` is valid only with `bc_type: impedance` and
  `impedance.tuning_model: full_pa`; invalid combinations fail during config
  resolution.
- The learned generator defaults to `learned-zerod` and `svzerodsolver` on the
  activated remote environment's `PATH`. Override them with
  `iteration1_seed.learned_zerod_executable` and
  `iteration1_seed.svzerodsolver_executable` when needed.
- Relative `path` values are resolved under each patient's `permanent_remote_path`; absolute paths are used as-is. For `learned_zerod`, the path is required and a missing or unreadable source fails before the learned adapter is invoked; it never falls back to steady generation.
- When a learned source is available in the local workspace it is staged as
  `inputs/source_0d_config.json`; remote-only sources remain referenced by their
  resolved patient path and are checked on the execution host. In both cases the
  driver creates the canonical run-scoped provenance copy under
  `seed_generation/learned/` and leaves patient assets read-only.
- Before invoking learned-zeroD, the driver replaces deprecated
  `internal_junction` values with `NORMAL_JUNCTION` in that run-scoped copy.
  The source patient model is not modified, and the driver log records the
  number of normalized junctions.
- `source=generate` alone creates `seed_generation/preop`, `postop`, and `steady`
  artifacts. Learned runs do not create that legacy steady scaffolding.

```yaml
tuning:
  iteration1_seed:
    source: learned_zerod
    path: zerod-models/baseline_0d_learned.json
    # Optional when these commands are already on PATH:
    # learned_zerod_executable: /path/to/learned-zerod
    # svzerodsolver_executable: /path/to/svzerodsolver
  impedance:
    tuning_model: full_pa
    outlet_mapping_mode: auto   # or centerline; never serialized_cap_order
    # outlet_mapping_centerline defaults to the patient centerlines.vtp,
    # which is also the centerline learned-zerod generates the seed from.
```

## 0D Tuning Configuration
- Select the BC tuning mode in YAML:
  - `defaults.tuning.bc_type`
  - optional `patients[].tuning.bc_type` override
- Supported values:
  - `impedance`
  - `rcr`

## Impedance Tuning Configuration
- Configure impedance tuning defaults in YAML when `bc_type: impedance`:
  - `defaults.tuning.impedance`
  - optional `patients[].tuning.impedance` override
- Key controls:
  - `solver`, `nm_iter`, `n_procs`, `grid_search_init`
    (the tune job requests exactly `n_procs` CPUs; impedance tuning is
    single-process, so the svz workspace uses `n_procs: 1`)
  - `d_min`, `use_mean`, `specify_diameter`
  - `diameter_scale`, `diameter_std_cap`, `outlet_mapping_mode`, `outlet_mapping`,
    `outlet_mapping_centerline`
  - `objective_tree_policy` (`full_pa` only): optional tree policy used inside the
    optimizer objective, with optional `use_mean`, `diameter_scale`,
    `diameter_std_cap`, and `reference_diameter` (`arithmetic_mean` |
    `conductance_matched`)
  - `tuning_model`: `rri` for reduced PA/RRI tuning, or `full_pa` for learned full pulmonary 0D configs
  - `wedge_pressure_policy`: how svZeroDTrees derives the tree outlet `Pd`;
    `clamp_to_diastolic` (default, `min(wedge, diastolic MPA)`), `measured`
    (the measured value as-is), `precapillary_fraction`
    (`wedge + precapillary_fraction * (mean - wedge)`, regurgitant patients) or
    `diastolic_offset` (`diastolic - diastolic_offset_mmhg`, non-regurgitant
    patients; no wedge needed)
  - `keep_diastolic_target`: keep the diastolic term when its target is below `Pd`
  - `objective`: `{type: relative | likelihood, pressure_sigma_mmhg, split_sigma,
    target_sigma}`; with `likelihood` the Nelder-Mead target stop and the
    iteration gate accept metrics within `target_sigma` standard deviations
  - `proximal_compliance` (`full_pa`): `{wall_ehr}` thin-wall compliance on the
    rigid seed vessels
  - `tree_max_nodes`: structured-tree node budget (svZeroDTrees default 100000)
  - `polish` (`full_pa`, requires `objective_tree_policy`): `{maxfev,
    initial_simplex_step}` per-cap re-tune from the shared-tree optimum
  - `leaf_resistance` (`full_pa`, requires `wedge_pressure_policy: measured`):
    `{downstream_fraction}` capillary + venous resistance at every tree leaf;
    evaluated on TST-STAN-5 and not used by the svz workspace
    (`docs/TUNING_MODEL.md` §8)
  - `rescale_inflow`, `convert_to_cm`, `compliance_model`
  - `stopping`: Nelder-Mead stopping policy, enabled by default with
    `target_tolerance: 0.025`, `stall_window: null` (5 x free parameters),
    `stall_rel_improvement: 0.01`, `xatol`/`fatol: 1e-3` (bounds-normalized),
    `maxfev: 200`, `initial_simplex_step: 0.1`, and
    `restart_min_rel_improvement: 0.05`. `enabled: false` restores the
    historical maxiter-only runs. A patient-level block patches individual
    fields; an explicit `null` disables that rule. Semantics are owned by
    svZeroDTrees `docs/interface.md` (`stopping`).
- Tuning artifacts recorded in `iteration_decision.json` (`tuning_artifacts`)
  also include `tuning_diagnostics` (svZeroDTrees `tuning_diagnostics.json`:
  outlet pressure and policy, published vs optimizer fit, truncation, total
  compliance vs the SV/PP bracket, parameters at bounds, peak memory) and
  `seed_with_proximal_compliance` when enabled. With a polish the shared result
  is kept as `optimized_params_shared.csv` / `pa_config_tuning_snapshot_shared.json`.
- The job exits with code 8 before any remote work when the cluster's
  svZeroDTrees would ignore part of the rendered impedance config; update
  svZeroDTrees on the cluster (`svzt update`) and resubmit.
- The cohort configuration and its rationale are in `docs/TUNING_MODEL.md`.
- Policy: each iteration retunes from a staged seed under `inputs/`.
  Reduced RRI runs use `simplified_nonlinear_zerod.json`; `full_pa` runs use
  `full_pa_zerod.json`.
- The default Nelder-Mead repeat count is `nm_iter: 5`.
- `objective_tree_policy` separates the optimizer's tree policy from the final
  published policy (top-level `use_mean`, `diameter_scale`, `diameter_std_cap`).
  When omitted, objective evaluations use the final policy. Omitted fields
  inside the block inherit the final policy; an explicit
  `diameter_std_cap: null` means no cap. Only explicitly set fields are
  forwarded to svZeroDTrees, which owns cross-field validation (for example,
  `conductance_matched` requires `use_mean: true` in the block and per-outlet
  final trees). A patient-level block replaces the defaults block rather than
  merging with it. The key is rejected for `rri` and dropped for
  `legacy_rri_after_first` RRI iterations. See svZeroDTrees
  `docs/full_pa_calibration.md#objective-tree-policy`.
- Full-PA seeds with one outlet BC per cap require a deterministic cap-to-BC
  mapping resolved by svZeroDTrees. `outlet_mapping_mode` accepts
  `auto` (default for `full_pa`), `metadata`, `cap_name`, `centerline`,
  `serialized_cap_order`, and `explicit` (with `outlet_mapping`). `auto` tries
  metadata, then cap names, then centerline geometry when
  `outlet_mapping_centerline` is set.
- `centerline` pairs each cap with the nearest centerline outlet endpoint and
  takes the BC of the 0D vessel on that branch. It fails unless the seed is
  traceable to the given centerline (see svZeroDTrees
  `docs/full_pa_calibration.md`, "Geometric (`centerline`) mapping").
- `outlet_mapping_centerline` must be the centerline the seed was generated
  from, as a remote path. For `full_pa` with `auto` or `centerline`, patient
  resolution defaults it to the patient `centerlines.vtp`
  (`remote.svzerodtrees_paths.centerlines`); relative paths resolve against the
  patient root. It is rejected for other modes and for `rri`. The resolved value
  is recorded in the manifest `impedance_defaults` and the rendered job script.
- Do not use `serialized_cap_order` for centerline-generated or learnedZeroD
  seeds: their BCs follow centerline branch order while caps are sorted by
  filename, so the pairing is wrong (1/23 caps correct on TST-STAN-5). It
  remains only for legacy runs whose seed BC order was built from cap order.
  `allow_ordered_outlet_mapping` is an input-only deprecated alias for it and
  cannot be combined with the canonical keys.
- RRI iterations, including `legacy_rri_after_first` iterations of a `full_pa`
  run, never receive outlet mapping keys; svZeroDTrees rejects them for RRI.

## RCR Tuning Configuration
- Configure RCR tuning defaults in YAML when `bc_type: rcr`:
  - `defaults.tuning.rcr`
  - optional `patients[].tuning.rcr` override
- Key controls:
  - `solver` (currently `Nelder-Mead` only)
  - `n_procs`
  - `rescale_inflow`
  - `convert_to_cm`
- Policy: RCR iteration tuning always stages the reduced seed
  `simplified_nonlinear_zerod.json`.
- RCR tuning writes `optimized_rcr_params.csv`, `pa_config_tuning_snapshot.json`,
  and `svzerod_3d_coupling_tuned.json`.

## Full-PA postprocess → calibration handoff

The calibration stage consumes the exact tuned full-PA model used for the
preoperative 3D case plus one upstream postprocess-suite descriptor. The agent
does not construct or inspect the VTP/timeseries scientific payload. The
postprocess job invokes `svZeroDTrees` and publishes its descriptor/reports;
the agent records the descriptor path and delegates contract validation to the
upstream implementation. See the [svZeroDTrees production calibration
contract](https://github.com/ncdorn/svZeroDTrees/blob/main/docs/full_pa_calibration.md)
for units, descriptor fields, QC, replay, and target semantics.

The scheduler dependency is explicit in the generated calibration script:
`#SBATCH --dependency=afterok:<postprocess-job-id>`. A missing postprocess
record or scheduler job ID is an error; the agent never submits an
un-dependent calibration job or guesses a descriptor filename. A postprocess
failure consequently prevents the dependent calibration job from running and
must be diagnosed from the postprocess record/logs.

The explicit recovery/inspection sequence is:

```bash
svzt --workspace-root /path/to/workspace preop select --run-id <run-id> --iteration <n>
svzt --workspace-root /path/to/workspace run calibrate --run-id <run-id> --iteration <n> --execute
svzt --workspace-root /path/to/workspace calibration-status <run-id> --iteration <n>
```

Use `plan calibrate` only as an isolated preview while validating the current
implementation. The plan command persists a dry-run `submitted` record with a
`dryrun-*` job identity; running `run calibrate --execute` immediately after
that preview can be treated as an idempotent match and reuse the preview
instead of submitting a real job. This is a TASK-016 CLI idempotency defect;
do not use plan-then-execute as a production sequence until it is corrected.

`preop select` submits the standalone selected-preop postprocess job and
records `selected_preop_postprocess`; automatic continuation instead records a
per-iteration `preop_postprocess_runs[]` entry. Both use the normalized result root
`iterations/iter-XX/results/postprocess/`. `plan calibrate`/`run calibrate`
stage the calibration request under the iteration's `calibration/` directory
and record the `afterok` dependency. `calibration-status` reports the state,
terminal state, dependency/job IDs, input/output digests, and promotion result.

Under `calibrated_full_pa` (impedance tuning with `tuning_model: full_pa`),
every completed iteration produces a calibrated full-PA model regardless of its
decision: `not_close`, `converged`, and the final iteration at
`max_iterations`. A not-close iteration uses it as the next iteration's seed;
a converged iteration hands it to postop for the preop/postop comparison.
### Driver-owned seed generation

Next-seed generation is one tune-driver step whose strategy the seed policy
selects (`workflows/seed_generation.py`); `run tune` alone is enough for either:

- `reduced_rri` (`rri` model, `rcr` boundary conditions, or
  `legacy_rri_after_first`): on `not_close` the driver regenerates
  `simplified_zerod_tuned_RRI.json` inline.
- `calibrated_full_pa`: at `run tune` time the agent renders and pushes the
  iteration's preop postprocess script
  (`iter-XX/postprocess/run_postprocess.sh`), the calibration script
  (`iter-XX/calibration/run_calibration.sh`, no in-script dependency), and a
  calibration request without the tuned-model path
  (`iter-XX/calibration/inputs/calibration_request.json`). After a `not_close`
  or `converged` decision the driver writes the final
  `calibrate_0d_from_3d.yaml` with the tuned model it actually produced, runs
  the rendered `sbatch` argv for postprocess, then calibration with
  `--dependency=afterok:<postprocess-job>`, and exits. RRI regeneration is
  skipped (`reduced_pa_regeneration_skipped_calibrated_full_pa`).

The driver reports the outcome in `iteration_decision.json`:

```json
"seed_generation": {
  "strategy": "calibrated_full_pa",
  "status": "submitted",
  "tuned_zerod_config": "<remote iter>/results/svzerod_3d_coupling_tuned.json",
  "postprocess": {"job_id": "...", "remote_script": "...", "remote_results_dir": "..."},
  "calibration": {"job_id": "...", "remote_root": "...", "remote_script": "...", "remote_config_path": "..."},
  "error": null
}
```

(`reduced_rri` reports `{"strategy": "reduced_rri", "status": "regenerated",
"regenerated_config_path": ...}`.) A failed driver `sbatch` reports
`status: failed` with `error` and does not change the decision.

### Agent verification and promotion

`advance-iter --execute`, `continue --execute`, and `watch --auto-advance` no
longer submit seed jobs in the normal path. They pull `iteration_decision.json`
if it is missing locally, record the driver-reported postprocess and
calibration jobs in the manifest (`preop_postprocess_runs[]`,
`calibration_runs[]`), and poll calibration. `calibration_pending` means keep
waiting; once calibration completes the agent fetches the publication,
validates it, and promotes it before advancing, reporting `already_converged`,
or reporting `max_iter_failed`. `needs_review` pauses without calibration.
Without `--execute`, `advance-iter` submits, polls, and pulls nothing; it
records any driver-reported jobs from the local decision and reports
`calibration_required` / `calibration_pending` / `calibration_failed`.

Agent-side submission (`calibration_submitted`) is the fallback when the
driver did not submit: a driver from before this change, a driver `status:
failed`, or a forced `svzt continue` after a driver timeout.

`run calibrate` remains available for inspection and explicit retries, but is
not required for normal full-PA iteration cadence.

## Calibration artifacts and evidence

For iteration `n`, the agent-owned calibration layout is:

```text
runs/<run-id>/iterations/iter-XX/calibration/
├── inputs/calibrate_0d_from_3d.yaml
├── run_calibration.sh
├── logs/
└── results/
    ├── calibrated_full_pa_zerod.json             # upstream candidate
    └── calibrated_full_pa_zerod.promoted.json   # only after guarded promotion
```

The corresponding remote root is
`<runs_root>/<run-id>/iterations/iter-XX/calibration/`. The upstream job writes
its calibrated model and scientific reports under remote `results/`; the agent
records those paths/digests in `calibration_runs[]` and does not rewrite them.
The upstream report set is documented in its contract (QC, confirmation,
replay, targets, and summary alongside the output config).

The postprocess source is recorded separately under
`preop_postprocess_runs[]` (automatic/preop records) or
`selected_preop_postprocess` (the explicit selected-preop handoff), with the
normalized output root at `iterations/iter-XX/results/postprocess/`. The usual
handoff evidence includes `postprocess_submission.json`,
`postprocess_suite_metadata.json`, last-cycle centerline descriptor/data, and
resistance/metric outputs. `postop_postprocess` is a separate explicit
postoperative record and is not an implicit preop calibration source.

## Acceptance gates, retry, and rollback

Scientific acceptance is upstream-owned: observation/data-contract QC,
fixed-point confirmation, finite normalized output, replay stability, target
evaluation, and provenance validation must pass before upstream marks the
calibration lineage valid. A negative calibrated resistance is not an
agent-level rejection when the upstream contract says all required gates pass;
the upstream warning/report remains evidence.

The agent's promotion gate is deliberately narrower and mechanical. It
requires all of the following:

- the requested input identity digest equals the manifest identity for the
  tuned model, postprocess descriptor, and postprocess job ID;
- the calibration record is `completed`/`succeeded` with a successful terminal
  scheduler state;
- `validated_lineage: true` is recorded by the upstream completion handoff; and
- the calibrated output exists locally and has a SHA-256 digest.

Only then does compare-and-set promotion copy the candidate to
`calibrated_full_pa_zerod.promoted.json` and set the iteration's
`calibrated_seed_path`. Failed, stale, incomplete, mismatched, or missing
outputs append a rejected promotion record and never replace the last-known
good pointer. A matching active input identity is idempotent: rerunning the
stage reuses its existing submission instead of scheduling a duplicate. If an
input digest changes, the prior record is marked `invalidated` and a new
attempt is recorded; previous evidence remains in manifest history.

For a retry, inspect the manifest first, verify the postprocess job and input
digests, then rerun `run calibrate --execute` only after the failed/incomplete
attempt or its upstream dependency has been corrected. Do not hand-edit a
successful promotion or overwrite a prior iteration's seed. Dry-run mode
renders the YAML/script, adapter command previews, and manifest submission
evidence without remote mutation; execute mode performs the transfer and
submission.

Rollback means disabling calibration/promotion in the workspace policy and,
if compatibility is required, selecting `legacy_rri_after_first` explicitly.
Continue from already published tuned 0D/3D outputs; do not delete or rewrite
calibration reports, prior seeds, or manifest history. A rejected candidate
already leaves the previous `last_known_good_promotion` unchanged.

## Failure diagnostics

Start with `svzt status <run-id>` and
`svzt calibration-status <run-id> --iteration <n>`, then inspect the relevant
run-scoped evidence:

| Symptom | First evidence to inspect |
| --- | --- |
| Workspace/config rejection | `svzt config validate`, `svzt doctor`, and the config snapshot under `runs/<run-id>/config/` |
| Missing or invalid patient/remote path | `manifest.yaml` (`patient`, `remote`, `remote.svzerodtrees_paths`) and `docs/PATIENT_DATA_CONTRACT.md` |
| Postprocess did not publish a descriptor | `iterations/iter-XX/postprocess/logs/`, `postprocess_submission.json`, `postprocess_suite_metadata.json` |
| Calibration is pending forever | `calibration_runs[]` dependency/job IDs; inspect the postprocess scheduler state and `afterok` dependency |
| Calibration failed upstream | `iterations/iter-XX/calibration/logs/`, upstream QC/confirmation/replay/target/summary reports, and the terminal reason |
| Promotion was rejected | `calibration_runs[]`, `promotion_records[]`, and `promotion_reason`; check terminal state, input digest, lineage flag, and candidate path |
| Next full-PA iteration refuses to stage | prior iteration `calibrated_seed_path` and `last_known_good_promotion`; choose an explicit legacy policy only as a reviewed rollback |

Scheduler output is retained under the stage `logs/` directory. Adapter errors
include command/stdout/stderr context; unsafe paths, rejected commands, and
invalid state transitions fail fast. Preserve those records when escalating a
run—do not “repair” a failed stage by replacing its manifest evidence.

## Seed-Sweep Campaigns
- `svzt campaign seed-sweep plan` creates `runs/campaigns/<campaign-id>/campaign_manifest.yaml` plus one child workspace per patient/case.
- Without `--patients`, the default learned seed sweep targets TST-STAN-5 and creates exactly three child runs.
- Default cases compare learned full-0D seeds with `tuning_model=full_pa` and `diameter_scale=0.0`, learned full-0D seeds with `tuning_model=full_pa` and `diameter_scale=0.1`, and an RRI-prepared reduced seed from the learned reference with `tuning_model=rri`.
- svZeroDTrees normalizes final tree assignment to `use_mean: false` whenever `diameter_scale > 0`, so outlet diameter spread is applied in the tuned 0D source. Full-PA objective evaluations use that same final policy unless `objective_tree_policy` is configured; the default seed-sweep cases do not set it.
- The learned full-0D source is `baseline_0d_learned.json` under the patient's local `zerod-models` directory and is staged into each full-PA child run as `inputs/full_pa_zerod.json`; the reduced RRI case prepares `prepared_inputs/simplified_zerod_tuned_RRI.json` from that reference.
- The historical seed-sweep description used full-PA tuning only for iteration 1 and reduced RRI thereafter. Under the current default `calibrated_full_pa` policy, a full-PA child cannot advance after a `not_close` result without a successful calibration promotion; selecting the legacy reduced-RRI behavior must be explicit with `next_iteration_seed_policy: legacy_rri_after_first`. The campaign helper does not currently set that policy, so this is a known compatibility gap for multi-iteration full-PA seed sweeps.
- Each child workspace contains config snapshots and a normal `runs/<run-id>/` manifest/plan, so campaign runs remain reproducible without mutating root config.
- `summarize` writes `seed_sweep_summary.json` and `seed_sweep_summary.csv`; `slides` writes `seed_sweep_comparison.pptx`.

## 3D CMM Robin Configuration
- Configure 3D simulation defaults in YAML:
  - `defaults.tuning.threed`
  - optional `patients[].tuning.threed` override
- The svzt-agent default setup is deformable CMM with Robin tissue support enabled:
  - `wall_model: deformable`
  - `tissue_support.type: uniform`
  - `tissue_support.stiffness: 1000.0`
  - `tissue_support.damping: 10000.0`
  - `tissue_support.apply_along_normal_direction: true`
- Optional Slurm email directives for `svZeroDTrees` solver scripts are configured under:
  - `defaults.tuning.threed.execution.slurm.mail_user`
  - `defaults.tuning.threed.execution.slurm.mail_types`
  - optional `patients[].tuning.threed.execution.slurm.*` override
- `prestress_file` supports:
  - absolute path: pass the prescribed VTU through as `Prestress_file_path`
  - `generate`: compute run-scoped prestress from iteration-1 seed-generation
    `steady/mean` VTUs, then reuse `<runs_root>/<run_id>/prestress/*-procs/result_*.vtu`
  - `auto` / `from_steady_mean`: legacy unsupported modes in `svzt-agent`; the
    iteration script logs a warning and continues without `Prestress_file_path`
- Set `patients[].tuning.threed.tissue_support.enabled: false` when overriding a patient to `wall_model: rigid`.
- Spatial Robin support uses `tissue_support.type: spatial` with `spatial_values_file_path` pointing to a VTP file containing `Stiffness` and `Damping` arrays.

## Postprocess Configuration
- Configure workflow-owned resistance-map postprocess defaults in YAML:
  - `defaults.postprocess.resistance_map.workers`
  - `defaults.postprocess.resistance_map.selected_preop_mem`
- `workers` accepts:
  - `auto`: resolve to the full single-node postprocess allocation that
    `svzt-agent` requests for the job. Selected-preop prefers a numeric
    `defaults.scheduler.cpus`; explicit postop prefers the resolved 3D
    `procs_per_node`. When neither is numeric, it falls back to `4`.
  - positive integer: request that exact number of frame-mapping workers
- `selected_preop_mem` applies only to the standalone selected-preop postprocess
  Slurm job submitted by `svzt preop select`. It does not change the explicit
  postop solver job resources.

## Run-Scoped CFD Results JSON
- `svzt postprocess cfd-results` builds a finalized run-scoped CFD results JSON from the current template, an optional existing/source JSON, and the run's local selected-preop plus postop postprocess artifacts.
- Default template path: `<workspace_root>/data/cfd-results/cfd-results-template.json`
- Default output path: `<workspace_root>/runs/<run_id>/cfd-results.json`
- Merge order:
  - start from the template shape exactly
  - overlay matching fields from the source JSON to preserve curated/manual values
  - overwrite run-derived fields from local evidence
  - drop legacy keys that are not present in the template
- Pressure metrics derived from `mpa_pressure_vs_time.csv` use the final cardiac period when `cycle_duration_s` is available in the postprocess metadata; in particular, diastolic pressure is taken as the minimum over that last period rather than over the full transient trace.
- The command prefers systolic resistance-map artifacts when available and falls back to mean resistance summaries when selected-preop systolic outputs are missing.
- When postop postprocess artifacts are still missing, the command preserves any curated measured fields from the source JSON and carries forward the best available manifest-backed run status instead of clearing the state back to `pending`.
- This command is local normalization only. Remote generation and artifact fetch remain separate workflow/operator steps.

## Run-Scoped Tuning Progress Diagnostics
- `svzt postprocess tuning-progress` builds a run-scoped diagnostic bundle that compares:
  - tuned 0D pre-mapping metrics from `pa_config_tuning_snapshot.json`
  - existing 3D preop gate metrics from `iteration_metrics.json`
  - clinical targets and threshold bands from `iteration_decision.json`
- Default output dir: `<workspace_root>/runs/<run_id>/tuning-progress/`
- Outputs:
  - `tuning_progress.csv`
  - `tuning_progress.json`
  - `tuning_progress.png`
- For future runs, the iteration driver also writes `iterations/iter-XX/results/zerod_pre_mapping_metrics.json`.
- When the per-iteration 0D summary is missing, the command backfills it locally from `pa_config_tuning_snapshot.json` when that artifact is available under either:
  - `iterations/iter-XX/results/`
  - `pulled_outputs/iterations/iter-XX/results/`
- The backfilled gate uses the same thresholds as the job: sigma mode when
  `iteration_decision.json` records `gate_mode: sigma`, otherwise 10% relative.
- Missing historical 0D snapshot artifacts remain explicit gaps in the output; the command does not infer pre-mapping behavior from the post-mapping 3D-coupled config.

## Adaptation Configuration
- Configure adaptation defaults in YAML:
  - `defaults.adaptation`
  - optional `patients[].adaptation` override
- Supported production-facing selectors:
  - `default_model`: `M1|M2|M3`
  - `territory_scheme`: currently `lpa_rpa`
  - `target_stage`: currently `postop`
  - `parameter_policy`: currently `global_fixed`
  - `models.m1`, `models.m2`, `models.m3`
  - optional named `parameter_sets`
- `M1` is stabilized WSS-only structured-tree adaptation with frozen thickness.
- `M2` is territory-level homeostatic WSS + pressure/IMS structured-tree adaptation.
- `M3` wraps the higher-complexity CWSS+IMS ODE model behind the same workflow contract.
- Adaptation starts from the selected preop iteration's tuning artifacts. The
  manager passes `results/svzerod_3d_coupling_tuned.json` as `tuned_config`
  and `results/outlet_cap_mapping.json` as `outlet_cap_mapping` to
  `run_structured_tree_adaptation` (the mapping only when the file exists).
  Both paths appear in the plan (`a01` read paths, `summary.tuned_config`,
  `summary.outlet_cap_mapping`, `summary.outlet_cap_mapping_required`) and in
  the `adaptation_started` event.
  - svZeroDTrees rebuilds the tuned trees from the config's tree metadata
    (parameters, diameters, `max_nodes`) and pairs caps and BCs through the
    saved outlet mapping, never by list position.
  - It uses the tuned IMPEDANCE Pd as the outlet pressure.
  - svzt-agent does not pass `wedge_pressure_policy`: the Pd stored in the
    tuned config is authoritative, and the manifest's impedance defaults can
    predate the selected iteration.
- The manager job fails before adaptation if the tuned config is missing. It
  also fails if `outlet_cap_mapping.json` is missing for an iteration whose
  effective tuning model is `full_pa` (manifests without
  `calibration_defaults` predate the seed policy and resolve iterations after
  the first as `rri`). A cluster svZeroDTrees without the
  `tuned_config` argument fails with `TypeError`; run `svzt update`.
- svZeroDTrees then fails (`ValueError`) in these cases:
  - the postop coupler's outlet BCs differ from the tuned ones;
  - an outlet BC has no coupling block with a surface, or the surface is a
    different cap than the mapped one;
  - an M2 `parameter_set.max_nodes` disagrees with the tuned budget.
- For full-PA runs, use `M2`. `svzt run adapt --model M1|M3` raises
  `ConfigError` at plan time when the selected iteration was tuned with
  `full_pa`. Both models adapt one LPA and one RPA tree inside a reduced-order
  PA model. See `docs/TUNING_MODEL.md` §8.
- ParaView visualization follows the same stage-scoped artifact contract across
  selected pre-op, explicit post-op, and adaptation:
  - selected pre-op submits a sibling ParaView job immediately after the
    selected-preop quantitative postprocess job
  - explicit post-op manager submits a child ParaView job after the postop CMM
    job reaches `COMPLETED`
  - adaptation manager submits a child ParaView job after the adapted CMM job
    reaches `COMPLETED`
- Manager-owned ParaView jobs are submit-only. The parent postop/adaptation
  manager does not wait for ParaView completion.
- When enabled, ParaView outputs land under the stage results root:
  - `iterations/iter-XX/results/paraview_viz/`
  - `postop/from-iter-XX/results/paraview_viz/`
  - `adaptation/from-iter-XX/<model>/results/paraview_viz/`
- Manager-owned child submissions also write
  `results/paraview_viz/paraview_viz_submission.json` with the owner job id,
  child job id, script path, output dir, and submission timestamp.

## Dry-run vs execute
- Default for `svzt run tune`, `svzt run postop`, and `svzt run adapt` is dry-run.
- Dry-run validates plan/safety rules, renders script, and prints command previews.
- `--execute` enables remote directory creation, rsync transfers, and `sbatch` submission.

## Adapter boundary
Execution actions must go through adapter interfaces:
- `RemoteExecAdapter.run(...)`
- `FileTransferAdapter.ensure_remote_dir/push/pull/sync(...)`
- `SchedulerAdapter.submit/status/accounting/cancel(...)`

No workflow code should directly call subprocess or shell commands.

## Command previews
In dry-run mode each adapter returns deterministic command argv previews. These are included in CLI output and persisted via manifest metadata updates.

## Iteration Execution
- Tuning iterations run under a single run ID with remote/local subdirectories:
  - local: `runs/<run_id>/iterations/iter-XX/`
  - remote: `<runs_root>/<run_id>/iterations/iter-XX/`
- Iteration script writes machine-readable artifacts:
  - `iteration_metrics.json`
  - `iteration_decision.json`
  - `optimized_params.csv`
  - `stree_impedance_optimization.log`
  - `pa_config_tuning_snapshot.json`
  - `svzerod_3d_coupling_tuned.json`
- Iteration driver behavior:
  - runs 0D impedance tuning and maps tuned BCs to a 3D-coupled 0D config
  - when `prestress_file: generate` is configured, computes wall traction from
    iteration-1 seed-generation `steady/mean` results, submits a single-process
    prestress simulation, and passes the generated VTU to CMM as
    `Prestress_file_path`
  - preserves `svzerod_3d_coupling_tuned.json` as the tuned 0D source and generates canonical `svzerod_3Dcoupling.json` with `external_solver_coupling_blocks` before submitting the 3D solver
    - Upstream contract (svZeroDTrees `docs/interface.md`, "3D coupler cap pairing"):
      `generate_threed_coupler` takes each IMPEDANCE BC's coupling-block
      `surface` from the tuned tree `outlet_mapping`. It no longer uses the BC's
      position among the sorted mesh caps, which was wrong for `learned_zerod`
      / centerline-ordered seeds.
    - The preop job now fails with `ValueError` before the solver is submitted
      when an IMPEDANCE BC has no mapped cap, a mapped cap is not in the preop
      mesh, two BCs share a cap, or a mesh cap is left uncoupled.
    - RCR/RESISTANCE BCs without tree metadata couple to the cap of the same
      name; otherwise they keep the old order-based pairing and print a `WARNING`.
    - Postop passes the selected preop coupler as an explicit `threed_coupler`,
      and adaptation passes a copy with its BCs rewritten by name. Neither stage
      regenerates surfaces, so they inherit the preop pairing, and the preop
      caps must exist in the postop mesh under the same names.
  - validates the canonical `svzerod_3Dcoupling.json` directly; it does not require or generate deprecated `svZeroD_interface.dat`
  - links the generated coupling input into the patient preop 3D model directory for that iteration
  - submits preop 3D (`SimulationDirectory`) and waits up to the configured
    `wait_timeout_seconds` value, defaulting to 43200 seconds
  - requires `results/mpa_pressure_vs_time.csv` plus preop flow split for gating
  - `not_close` regenerates a reduced PA config for reduced-RRI or explicitly
    selected legacy-policy continuation; `advance-iter --execute` automatically
    submits the postprocess → calibration → promotion chain for default-policy
    full-PA continuation before it stages the next iteration
  - `converged` stops after writing decision/metrics/artifact metadata; postop
    submission is explicit via `svzt preop select` and `svzt run postop`
- Iteration success evidence for preop BC tuning:
  - 0D impedance tuning completed the configured `nm_iter` Nelder-Mead tuning/convergence budget and produced the expected tuning artifacts
  - `svzerod_3d_coupling_tuned.json` and canonical `svzerod_3Dcoupling.json` were generated from the tuned result
  - the canonical 3D coupler was linked into the preop simulation directory used for the solver submission
  - the cluster submission produced a preop solver job ID and retained solver `.o*`/`.e*` log paths
  - the solver `.o*` file shows successful timestep progress, with no fatal solver error in the corresponding `.e*` file
- A preop iteration is considered successfully running once the submitted 3D solver is printing successful timestep progress. Later clinical comparison, `not_close`/`converged` decisions, and postop submission are separate workflow outcomes.
- `svzt advance-iter` advances run state after a `not_close` decision and can submit the next iteration (`--execute`).
- `svzt advance-iter --max-iterations <n>` raises the manifest's iteration cap before advancing. This is the supported way to continue past the default cap of 5 on an existing run.
- `svzt advance-iter` returns a pause action for `needs_review` and does not submit a new iteration job.
- `svzt preop select` records `converged_preop_iteration` in the manifest. The
  selected iteration can be a formally converged iteration or an operator-promoted
  best completed iteration.
- `svzt preop select` now also stages and submits a selected-preop postprocess
  job that writes normalized artifacts under
  `runs/<run_id>/iterations/iter-XX/results/postprocess/` on the cluster and
  records `selected_preop_postprocess` in the manifest. Its Slurm stdout/stderr
  now live under `runs/<run_id>/iterations/iter-XX/postprocess/logs/`, and the
  job writes `postprocess_submission.json` plus
  `postprocess_suite_metadata.json` for success/failure evidence. In addition to
  the resistance-map artifacts, the selected-preop postprocess job now writes a
  stacked last-cycle centerline timeseries artifact at
  `results/postprocess/centerline_timeseries_last_cycle.vtp` plus companion
  `centerline_timeseries_last_cycle_metadata.json`, built from the per-timestep
  `svslicer` centerline projections used by the mean resistance-map pass as one
  centerline geometry with ZeroD-compatible `pressure_i` / `velocity_i` point
  arrays for `calibrate_0d_from_3d`. The
  generated resistance-map step now supports bounded frame-level parallelism through
  `defaults.postprocess.resistance_map.workers`; selected-preop jobs request
  matching `--cpus-per-task`, resolving `auto` against the selected-preop
  allocation, and when more than one worker is requested they also request
  `defaults.postprocess.resistance_map.selected_preop_mem`.
- `svzt run postop` consumes `converged_preop_iteration`, writes a postop plan
  under `runs/<run_id>/postop/from-iter-XX/`, and stages/submits the postop job
  under `<runs_root>/<run_id>/postop/from-iter-XX/`. If `--execute` is the first
  postop invocation, planning and validation still occur before remote mutation.
- Explicit postop runs now execute the upstream `svZeroDTrees` pulmonary 3D
  postprocess suite after solver completion and write normalized artifacts under
  `<runs_root>/<run_id>/postop/from-iter-XX/results/postprocess/`. The cluster
  config must provide `executables.svslicer_path`. Postop postprocess Slurm
  stdout/stderr live under `<runs_root>/<run_id>/postop/from-iter-XX/logs/`,
  and the job writes `postop_submission.json` plus
  `results/postprocess/postprocess_suite_metadata.json`. That same postop
  postprocess output directory now also includes
  `centerline_timeseries_last_cycle.vtp` and
  `centerline_timeseries_last_cycle_metadata.json` as separate artifacts from
  the resistance-map products. The postop postprocess step receives the same resistance-map worker setting, resolving `auto`
  against the single-node child postprocess allocation using the resolved 3D
  `procs_per_node`. The enclosing explicit postop wrapper now requests the same
  Slurm resources as the resolved
  `threed` config, so the final postop transient solve uses the same wall
  model, material properties, tissue support, timestep controls, node/task
  topology, and canonical `svzerod_3Dcoupling.json` selection as the
  corresponding preop stage. Before the postop transient inputs are written,
  the wrapper also rewrites the copied tuned 0D config and copied
  `svzerod_3Dcoupling.json` to the full patient inflow waveform so dirichlet
  `simulation/inflow.flow` is generated from the 3D-scale inflow rather than
  the reduced-order tuning inflow. Inside the explicit postop wrapper, the generated
  `simulation/run_solver.sh` is executed in-place so its `srun` launch path and
  module environment are preserved without submitting a nested Slurm job. For
  deformable postop runs, explicit postop also generates a fresh run-scoped
  postop-mesh prestress field before the final transient solve, unless that
  postop workspace already contains a completed prestress result to reuse; the
  nested prestress stage is normalized to a single-rank run while leaving the
  final transient resources unchanged.
- `svzt run adapt` is a separate explicit workflow. It requires both
  `converged_preop_iteration` and `postop_run`, writes a plan under
  `runs/<run_id>/adaptation/from-iter-XX/<model>/`, and stages/submits the
  adaptation manager job under
  `<runs_root>/<run_id>/adaptation/from-iter-XX/<model>/`.
- Explicit adaptation uses the same patient inflow source-of-truth contract as
  preop/postop 3D. Before adapted transient inputs are written, the workflow
  rewrites the copied adapted 0D config and copied adapted
  `svzerod_3Dcoupling.json` to the full patient waveform and then generates
  `simulation/inflow.flow` from that 3D-scale inflow. It never rescales inflow
  from reduced-order adaptation outputs.
- `svzt run adapt --model M1|M2|M3` records one `adaptation_runs[]` entry per
  submission, so multiple adaptation models can be run against the same source
  postop case without overwriting each other.
- The adaptation manager runs reduced-order adaptation first, writes:
  - `results/adaptation_summary.json`
  - `results/adaptation_metrics.json`
  - `results/adapted_svzerod_3Dcoupling.json`
  - `results/baseline_vs_adapted_comparison.json`
  - `results/reduced_pa_flow_split_convergence.csv` for `M1`
  - `results/reduced_pa_flow_split_convergence.png` for `M1`
  then runs one adapted 3D solve on the postop mesh and inline postprocess for
  both postop baseline and adapted prediction. Each inline postprocess output
  directory now includes a stacked last-cycle centerline timeseries artifact
  (`centerline_timeseries_last_cycle.vtp` plus
  `centerline_timeseries_last_cycle_metadata.json`) alongside the resistance-map
  outputs.
- `svzt status <run-id>` now combines parent scheduler state with iteration progress artifacts:
  - prints the active iteration and tracker status
  - reports the best-known stage within the active iteration (`0D` tuning, preop `3D`, post-preop analysis, postop `3D`, or terminal branch outcome)
  - polls child preop/postop jobs when their job IDs are present in iteration
    artifacts or explicit postop manifest records
  - prefers the latest explicit adaptation job when `adaptation_runs[]` exist,
    and reports the active adaptation model/parameter-set in CLI output
  - performs a targeted pull of `iteration_driver_log.json`, `iteration_decision.json`, and `iteration_metrics.json` for the active iteration when those artifacts are not already available locally
- `svzt fetch <run-id>` now also pulls adaptation logs/results under
  `pulled_outputs/adaptation/from-iter-XX/<model>/`.
- `svzt campaign adapt-benchmark` plans/runs/summarizes cohort comparisons by
  replaying `svzt run adapt` across completed postop runs and writing
  `adapt_benchmark_summary.json/csv` under
  `runs/campaigns/<campaign-id>/`.
- `svzt watch --auto-advance` composes monitor + decision pull + iteration advance/submit:
  - after each completed iteration, pulls:
    - remote: `<runs_root>/<run_id>/iterations/iter-XX/results/iteration_decision.json`
    - remote: `<runs_root>/<run_id>/iterations/iter-XX/results/iteration_metrics.json`
    - remote: `<runs_root>/<run_id>/iterations/iter-XX/results/simplified_zerod_tuned_RRI.json`
    - local destination: `runs/<run_id>/iterations/iter-XX/results/`
  - then executes `advance_tune_iteration(..., execute=True)` to submit the next iteration when applicable.
    Default-policy full-PA runs first complete the dependent postprocess and
    calibration jobs, then fetch and promote their publication.
  - halts with `final_action=needs_review_pause` when an iteration enters review-required state.
