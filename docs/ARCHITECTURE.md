# svzt-agent Architecture

## Overview
`svzt-agent` uses a ports-and-adapters architecture with explicit execution safety boundaries:

- Distribution name is `svzt-agent`, import package is `svztagent`, and the console entry point remains `svzt`.
- Importable source lives under `src/svztagent/`.
- Packaged runtime assets such as the Slurm job template live under `src/svztagent/templates/`.
- `svztagent.config`: typed workspace config loading and validation.
- `svztagent.core`: domain models (`RunManifest`, `ExecutionPlan`), path policy checks, manifest/status helpers.
- `svztagent.hpc`: typed interfaces + concrete adapters (`ssh`, `rsync`, `slurm`) and test fakes.
- `svztagent.workflows`: workflow orchestration (`tune_trees`, `postop`, `adapt`, and
  `calibrate`) for plan generation and controlled execution. `calibrate` owns
  scheduling/provenance and guarded seed promotion; scientific calibration stays
  in `svZeroDTrees`.
- `svztagent.campaigns`: bounded cross-run campaign orchestration built from normal workflow plans/manifests.
- `svztagent.cli`: operator entrypoints (`plan`, `run`, `status`, `watch`, `fetch`).

Workspace configuration is loaded from `config/clusters.yaml`, `config/patients.yaml`,
and `config/defaults.yaml`. An optional `config/repositories.yaml` can pin local
checkout locations for provenance in the shared `ppas-dev/` workspace. When it
is absent, `svzt-agent` auto-discovers sibling checkouts and otherwise runs in
package mode without requiring local upstream repo paths.

3D simulation setup is resolved through `svztagent.config` and passed into the
packaged iteration script as `threed_config`. The default setup is deformable CMM
with uniform Robin tissue support; `svZeroDTrees` owns the svMultiPhysics XML
emission for the wall `Tissue_support` block. Optional
`threed.execution.slurm.mail_user` and `mail_types` stay in the typed config and
flow through to the generated `run_solver.sh` files that `svZeroDTrees` writes.
Cluster-level `executables.svzerodsolver_build_dir` is injected alongside that
config so `svZeroDTrees` can resolve the `libsvzero_interface.so` path while
writing coupled svMultiPhysics XML.

The tuning config now resolves three coupled pieces per patient: `bc_type`
(`impedance` or `rcr`), the impedance-specific tuning block, and the
RCR-specific tuning block. The iteration driver dispatches to the matching
`svZeroDTrees` helper at runtime and expects the matching tuning artifact
filename (`optimized_params.csv` for impedance, `optimized_rcr_params.csv` for
RCR).

## Execution layer
All remote side effects are routed through typed adapters:

- `RemoteExecAdapter`: remote command execution with command allowlist enforcement.
- `FileTransferAdapter`: remote directory creation, push/pull/sync operations.
- `SchedulerAdapter`: scheduler submit/status/accounting/cancel operations.

`CommandExecutor` is the single subprocess boundary for adapter execution. Workflows never invoke shell commands directly.

## Safety invariants
- Patient source data is read-only and never writable by adapter methods.
- Remote writes are restricted to `runs_root`.
- Commands are argv-based and validated against an allowlist.
- Forbidden shell tokens (`;`, `&&`, `||`, pipes, redirection) are rejected before execution.
- Dry-run mode produces deterministic command previews and no remote side effects.

## Tune workflow execution flow
1. Resolve workspace/cluster/patient and run ID.
2. Resolve the active tuning iteration (or explicit `--iteration`).
3. Load existing validated plan or generate a new one.
4. Stage local iteration inputs and render deterministic Slurm script.
5. Ensure remote iteration directories exist under `runs_root`.
6. Push staged inputs and job script.
7. Submit via Slurm and capture job ID.
8. Persist submission metadata in the run manifest and `tuning_iteration_tracker`.
9. Monitor lifecycle transitions via `svzt status` (single poll) or `svzt watch` (continuous polling).
10. Optionally fetch artifacts after terminal completion via `svzt fetch` or `svzt watch --fetch-on-complete`.

Before rendering and submitting a tune iteration, the workflow resolves the
effective parent CPU count once as the active tuning configuration's `n_procs`
(scheduler `cpus` when `n_procs` is absent). The tune job's own work is the
0D tuning plus serial bookkeeping; 3D, prestress, postprocess and calibration
run as separate Slurm jobs, so extra CPUs would sit idle. Impedance tuning is
single-process, so the svz workspace sets `n_procs: 1`. That resolved value is
passed to both the rendered `#SBATCH --cpus-per-task` directive and the
default `SlurmSubmitOptions`; the generic Slurm adapter remains responsible
only for command construction, and injected scheduler adapters remain
unchanged.

## Explicit postop/adaptation workflows
- `svzt preop select` records the selected converged preop iteration and stages
  selected-preop postprocess.
- `svzt run postop` is a sibling explicit workflow that consumes the selected
  preop iteration and records `postop_run`.
- `svzt run adapt` is another sibling explicit workflow that consumes
  `converged_preop_iteration` + `postop_run`, records `adaptation_runs[]`, and
  preserves one manifest record per adaptation model/parameter-set submission.
- `svztagent.workflows.paraview_viz` owns stage-independent ParaView job
  preparation. Selected-preop submits that job directly; postop/adaptation
  managers consume the same staged job definition and submit it later as a
  stage-owned child job after CMM completion.
- Explicit adaptation reuses the same patient inflow source-of-truth contract as
  explicit postop by rewriting copied adapted 0D/coupling inputs to the full
  patient waveform before generating adapted transient files.

## Monitoring layer
- `svztagent.core.state`: canonical lifecycle states and terminal/active classification.
- `svztagent.core.transitions`: explicit allowed transition graph with invalid-transition errors.
- `svztagent.core.status`: scheduler normalization (`slurm` raw -> lifecycle state + terminal reason).
- `svztagent.core.monitor`: synchronous polling service with `squeue` -> `sacct` fallback.
- `svztagent.core.manifest`: append-only lifecycle history, poll counters, and lifecycle timestamps.
- `svztagent.core.seed_policy`: single resolution of whether an iteration uses the
  `calibrated_full_pa` cadence (impedance + full-PA + that policy); shared by the
  tune driver render (skip reduced RRI regeneration), advance/continue
  (calibration gate), and postop selection (calibrated 0D model).

This split keeps state modeling, scheduler polling, manifest mutation, and CLI rendering isolated and testable.

## Runtime prerequisites and configuration boundary

The workspace root is supplied with the global `--workspace-root` option, from
`SVZ_WORKSPACE_ROOT`, or by running inside a directory containing `config/` and
`runs/` (in that order of precedence). A valid workspace must contain:
`config/clusters.yaml`, `config/patients.yaml`, `config/defaults.yaml`, and
`runs/`. `config/repositories.yaml` is optional; when present, its paths are
resolved relative to the workspace and recorded as provenance.

For a remote full-PA run, the selected cluster entry must provide an absolute
`remote_roots.runs_root`, an SSH `host`/`user`, the solver executable
`executables.svfsiplus_path`, and the upstream runtime paths needed by the
selected stages. `executables.svslicer_path` is required for the postprocess
stage; `executables.svzerodsolver_build_dir` is passed through when the upstream
coupler writes its shared-library reference. The effective Python command and
remote environment are configured with `defaults.execution.python_executable`
and `defaults.execution.env_activation_hooks`. Patient source paths remain
read-only; all generated inputs, scripts, logs, reports, and candidate outputs
must be under the run's `<runs_root>/<run_id>/` directory.

A patient `tuning.iteration1_seed` override is field-wise: unspecified path
and executable settings inherit from `defaults.tuning.iteration1_seed`.

Iteration-1 seed selection is an orchestration setting. `source: generate`
retains the reduced svZeroDTrees seed workflow. `source: learned_zerod` is
restricted to full-PA impedance tuning: the remote iteration driver creates a
run-scoped reduced source model, delegates learned generation and validation to
the upstream `generate_full_pa_learned_seed` service, records its metadata, and
stages the resulting model as `inputs/full_pa_zerod.json`. External executable
identity is configuration, while seed construction and topology validation
remain owned by svZeroDTrees.

The agent-owned calibration policy is intentionally small:

```yaml
defaults:
  execution:
    python_executable: python3
    env_activation_hooks: []
  tuning:
    impedance:
      tuning_model: full_pa
      outlet_mapping_mode: auto
    calibration:
      enabled: true
      next_iteration_seed_policy: calibrated_full_pa
```

`patients[].tuning.calibration` overrides the defaults. `enabled` defaults to
true for the calibrated full-PA cadence and remains an explicit opt-out; it
does not contain scientific optimizer, observation, or target settings.
`calibrated_full_pa` requires a successful promotion before a later full-PA
iteration can be staged. The temporary
compatibility policy `legacy_rri_after_first` explicitly selects the reduced
RRI seed after iteration one. Full-PA mapping keys are resolved by the upstream
contract; the agent carries the resolved configuration and provenance but does
not implement another mapping policy. The one agent-side default is
`impedance.outlet_mapping_centerline`: for `full_pa` with `outlet_mapping_mode`
`auto` or `centerline`, `config/load.py` fills it with the patient
`centerlines.vtp` (relative values resolve against the patient root), and
`workflows/tune_trees.py` drops mapping keys from RRI iteration configs.
`impedance.objective_tree_policy` (full-PA only) is carried the same way: the
agent checks field types, forwards only explicitly set fields, and leaves
cross-field validation to svZeroDTrees. Unlike other impedance keys, a
patient-level block replaces the defaults block instead of merging with it,
like `tune_space`.
`impedance.stopping` is a typed pass-through of the svZeroDTrees Nelder-Mead
stopping policy. It defaults on; a patient-level block patches individual
fields over the defaults (explicit nulls survive), and it is forwarded for
both `full_pa` and RRI iterations.
`impedance.wedge_pressure_policy` (`clamp_to_diastolic` | `measured` |
`precapillary_fraction` | `diastolic_offset`, with `precapillary_fraction` and
`diastolic_offset_mmhg`) is a typed pass-through: the agent checks the value
and forwards it unchanged; svZeroDTrees applies it when reading
`clinical_targets.csv`. The physiological full-PA controls `objective`,
`keep_diastolic_target`, `proximal_compliance`, `tree_max_nodes`, `polish`, and
`leaf_resistance` are typed pass-throughs as well (`proximal_compliance`,
`polish`, and `leaf_resistance` are full-PA-only and are dropped from RRI
iteration configs; `leaf_resistance` also requires
`wedge_pressure_policy: measured`). Unset optional controls are omitted from
the rendered payload so an older cluster svZeroDTrees still accepts it.
`objective` and `polish` patch the workspace value field by field at patient
level; `proximal_compliance`, `leaf_resistance`, and `tree_max_nodes` replace
it; an explicit patient `null` clears any of them.
`config/tuning_checks.py` cross-checks per-patient choices (outlet policy vs
the `regurgitation` flag and wedge pressure in `config/clinical_targets.yaml`;
`proximal_compliance.wall_ehr` vs the deformable 3D wall's E*h/r) and reports
warnings through `config validate` and `doctor`. The job script refuses to run
(exit 8) when the cluster's svZeroDTrees does not support every rendered
impedance key (`svzerodtrees.tuning.SUPPORTED_IMPEDANCE_KEYS`). Model
rationale: `docs/TUNING_MODEL.md`.

## Full-PA stage graph and ownership

The auditable handoff is one run-scoped graph. Every arrow that crosses a
remote boundary is represented by a manifest record and, where applicable, a
scheduler job ID:

```text
initialized
  -> planned/staged
  -> tuning submitted -> 3D completed
  -> postprocess submitted -> postprocess terminal
  -> calibration submitted (afterok: postprocess job)
  -> calibration running -> calibration terminal
  -> promotion accepted -> calibrated_full_pa seed for next iteration
                            (or, when converged, the postop 0D model)
                         \-> promotion rejected -> retain prior seed/history
```

Tuning iteration outcomes still branch to `not_close`, `converged`,
`max_iter_failed`, or `needs_review`. A successful promoted seed is the only
source accepted for the next enabled full-PA iteration. The reduced seed
regenerated by the tuning driver is retained as evidence and is not silently
used for that policy.

The boundary between the two repositories is deliberately asymmetric:

| Upstream `svZeroDTrees` owns | `svzt-agent` owns |
| --- | --- |
| VTP/centerline transformations and the versioned postprocess descriptor | Stage ordering, local/remote run layout, and typed adapter calls |
| Observation units, data-contract/QC validation, fixed-point confirmation, replay, and target gates | SSH/rsync/Slurm submission, `afterok` dependencies, and scheduler normalization |
| `pysvzerod.calibrate` and calibrated branch parameters | Manifest lifecycle, input/output digests, retries, and operator diagnostics |
| Atomic publication of the calibrated scientific output and its reports | Compare-and-set promotion of a validated output as the next-iteration seed |

The agent invokes the upstream public suite/calibration interfaces and records
their returned paths and digests. It must not parse VTP, stack centerlines,
validate scientific observations, or optimize calibrated branch parameters.
See the [svZeroDTrees full-PA tuning and calibration guide](https://github.com/ncdorn/svZeroDTrees/blob/main/docs/full_pa_calibration.md)
for the scientific artifact schema and acceptance gates.

### Current TASK-016 integration status

Seed generation is one tune-driver step with a policy-selected strategy.
`workflows.seed_generation` renders the calibrated-full-PA postprocess and
calibration jobs at `run tune` time and records the job IDs the driver reports
in `iteration_decision.json`. After any decision except `needs_review`
(`not_close`, `converged`, or the final iteration) the driver submits
postprocess, then calibration with `afterok`.
`workflows.calibrate.ensure_iteration_calibration` owns the one-step agent
gate used by `advance-iter`, `continue`, and `watch --auto-advance`: record
driver-reported jobs, poll, fetch, validate, and promote before staging the
next tuning iteration or reporting convergence/max-iteration failure. It
submits agent-side only when the driver did not. `watch
--auto-advance` composes that same chain. The explicit calibration commands
remain available for inspection and retries.

There is also a known plan/execution edge case in the current implementation:
`plan calibrate` persists a dry-run `submitted` record using a `dryrun-*` job
identity. A subsequent execute request with the same input identity can reuse
that preview as an idempotent match. Until that is fixed, operators should
use an isolated plan preview or execute calibration directly, never plan then
execute the same calibration record.
