# svzt-agent Architecture

## Overview
`svzt-agent` uses a ports-and-adapters architecture with explicit execution safety boundaries:

- Distribution name is `svzt-agent`, import package is `svztagent`, and the console entry point remains `svzt`.
- Importable source lives under `src/svztagent/`.
- Packaged runtime assets such as the Slurm job template live under `src/svztagent/templates/`.
- `svztagent.config`: typed workspace config loading and validation.
- `svztagent.core`: domain models (`RunManifest`, `ExecutionPlan`), path policy checks, manifest/status helpers.
- `svztagent.hpc`: typed interfaces + concrete adapters (`ssh`, `rsync`, `slurm`) and test fakes.
- `svztagent.workflows`: workflow orchestration (`tune_trees`, `postop`, `adapt`) for plan generation and controlled execution.
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

This split keeps state modeling, scheduler polling, manifest mutation, and CLI rendering isolated and testable.
