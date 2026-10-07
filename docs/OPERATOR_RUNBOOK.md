# Operator Runbook: Running a Full Pipeline

This document is the practical start-to-finish guide for running a pulmonary BC tuning pipeline on Sherlock. It covers every command, when to run it, and what to do at each outcome.

## Prerequisites

Before starting:

- The workspace YAML is configured with your cluster, patient alias, and defaults (see [`docs/PATIENT_DATA_CONTRACT.md`](./PATIENT_DATA_CONTRACT.md)).
- Patient data is present on Sherlock under the configured `permanent_remote_path`: `mesh-complete/`, `centerlines.vtp`, `inflow.csv`, `clinical_targets.csv`.
- `svzt` is installed and either `SVZ_WORKSPACE_ROOT` is set or you pass `--workspace-root` to every command.
- For full-PA calibration, the cluster config also provides an absolute
  `remote_roots.runs_root`, `executables.svfsiplus_path`, and
  `executables.svslicer_path`; `defaults.execution.python_executable` and
  `env_activation_hooks` must select a compatible upstream `svZeroDTrees` /
  `pysvzerod` runtime on Sherlock.
- Full-PA calibration is enabled by default. Keep
  `next_iteration_seed_policy: calibrated_full_pa` unless a reviewed rollback
  explicitly selects `legacy_rri_after_first`; `calibration.enabled: false`
  is an explicit opt-out.

---

## Before Tuning a New Patient (Physiological Full-PA Model)
1. Initialize a new run for every patient. Runs started under the previous
   configuration (k3 = 0, unbounded xi) seed the next iteration with
   parameters outside the new bounds and fail into `needs_review`.
2. Confirm the patient's `clinical_targets.csv` and `inflow.csv` provenance
   (the inflow must be the patient's own MRI flow, not a rescaled template).
   `proximal_compliance` also needs `geometric_params` and `vessel_length` on
   every rigid seed vessel; a seed without them fails at the start of tuning,
   before any tree is built.
3. Set the outlet pressure policy: patients with pulmonary regurgitation get
   `tuning.impedance.wedge_pressure_policy: "precapillary_fraction"` in
   `config/patients.yaml` (needs a measured PCWP); the workspace default
   `diastolic_offset` covers patients without regurgitation.
4. Record `regurgitation` and `wedge_pressure` in `config/clinical_targets.yaml`
   and run `svzt config validate`; resolve or acknowledge every tuning warning.
   After the first tuning, check `threed_wall_vs_proximal_compliance` in
   `iteration_driver_log.json`; if the ratio is outside 0.5–2, set the
   patient's `tuning.threed.elasticity_modulus` to the logged
   `matched_elasticity_modulus`.
5. Make sure the cluster's svZeroDTrees is current (`svzt update`); otherwise the
   job exits with code 8 before running.
6. After tuning, read `results/tuning_diagnostics.json`: `published_fit`
   errors and chi2, `parameters_at_bounds`, `compliance_in_svpp_bracket`,
   `trees.n_truncated` and `inflow_consistency`. `docs/TUNING_MODEL.md`
   lists what to change if a patient does not fit.
7. Run adaptation with `--model M2`: it starts from the tuned per-cap trees,
   outlet mapping and Pd of the selected iteration. M1/M3 are rejected for
   full-PA runs. The job stops if `outlet_cap_mapping.json` is missing or the
   postop coupler pairs a BC with a different cap than the mapping
   (`docs/TUNING_MODEL.md` §8, item 5).

## Step 1 — Initialize the run

```bash
svzt init-run --cluster sherlock --patient <patient-alias> --run-id <run-id>
```

Creates the local run directory and manifest at `runs/<run-id>/`. The run ID is used in every subsequent command — choose something descriptive and dated, e.g. `tst-stan-5-current-20260507`.

---

## Step 2 — Dry-run to inspect the plan

```bash
svzt plan tune --cluster sherlock --patient <patient-alias> --run-id <run-id>
```

Prints the full execution plan: what will be staged, transferred, and submitted, along with all resolved paths and config values. Fix any config or path validation errors here before touching the cluster. The plan is also written to `runs/<run-id>/execution_plan.yaml`.

---

## Step 3 — Submit iteration 1

```bash
svzt run tune --cluster sherlock --patient <patient-alias> --run-id <run-id> --execute
```

This command:
1. Stages the configured iteration-1 seed, generates a reduced seed for
   `iteration1_seed.source: generate`, or generates a learned full-PA seed for
   `iteration1_seed.source: learned_zerod`
2. Uploads the job script and inputs to Sherlock via rsync
3. Submits the SLURM driver job and prints the job ID

The SLURM driver job is long-running. For each iteration it:
- Runs 0D BC tuning (impedance or RCR on the staged seed)
- Submits the preop 3D CMM job and waits for it
- Post-processes `result_*.vtu` files against the MPA centerline to generate `mpa_pressure_vs_time.csv`
- Evaluates the clinical gate against `clinical_targets.csv`
- Generates the next seed, then writes `iteration_decision.json` with a
  `seed_generation` block. Reduced-RRI and `legacy_rri_after_first` runs
  regenerate `simplified_zerod_tuned_RRI.json` inline on `not_close`. A
  default-policy (`calibrated_full_pa`) full-PA run instead submits the preop
  postprocess job and the dependent calibration job on `not_close` or
  `converged`, reports both job IDs, and exits.

---

## Step 4 — Monitor and advance

### Option A: Fully automated (recommended)

```bash
svzt watch <run-id> --auto-advance --fetch-on-complete
```

Runs until convergence, max iterations, or a `needs_review` pause. For each iteration it:
1. Polls SLURM until the driver job completes
2. Pulls `iteration_decision.json`, `iteration_metrics.json`, and any
   regenerated reduced-RRI seed
3. Under default-policy full-PA, for `not_close`, `converged`, or the final
   iteration: waits for the driver-submitted calibration, then verifies and
   promotes it
4. If `not_close`: seeds and submits the next iteration
5. If `converged`: exits cleanly; record the converged preop iteration and submit postop explicitly (postop uses the calibrated full-PA model)
6. If `needs_review`: exits with code 1 and prints the reason (no calibration)

### Option B: Manual iteration-by-iteration

Useful when you want to inspect results between iterations.

**Watch one iteration complete:**
```bash
svzt watch <run-id> --fetch-on-complete
```

**Check the outcome:**
```bash
svzt status <run-id>
```

Reports the decision (`converged` / `not_close` / `needs_review`), current iteration, and clinical metrics. Then act based on the decision:

**If `not_close`** — advance and submit the next iteration:
```bash
svzt advance-iter --run-id <run-id> --execute
```

If the run hit the default 5-iteration cap but you want to keep going, raise it explicitly when advancing:
```bash
svzt advance-iter --run-id <run-id> --max-iterations 8 --execute
```

**If `converged`** — record the converged preop iteration, then submit postop explicitly:
```bash
svzt preop select --run-id <run-id> --iteration <n> --reason "best tuned preop"
svzt run postop --run-id <run-id>          # dry-run preview
svzt run postop --run-id <run-id> --execute
```

`svzt preop select` now also submits a selected-preop postprocess job that
generates `mpa_pressure_vs_time.csv/png`, flow-split comparison artifacts, and
resistance-map outputs under `iterations/iter-XX/results/postprocess/`. It also
writes `centerline_timeseries_last_cycle.vtp` plus
`centerline_timeseries_last_cycle_metadata.json`, which convert the per-timestep
`svslicer` centerline projections from the last processed cardiac cycle into
one centerline geometry with ZeroD-compatible `pressure_i` / `velocity_i`
point arrays.
Scheduler logs for that job are written under
`iterations/iter-XX/postprocess/logs/`. The job also writes
`postprocess_submission.json` and `postprocess_suite_metadata.json` so partial
artifact generation and failure context are preserved. Resistance-map frame
mapping now supports bounded parallelism controlled by
`defaults.postprocess.resistance_map.workers`. Selected-preop jobs request
matching `--cpus-per-task`, resolving `auto` against the selected-preop
allocation, and when more than one worker is requested they also request
`defaults.postprocess.resistance_map.selected_preop_mem`.

### Full-PA calibration and seed promotion

For every completed default-policy preoperative `full_pa` iteration
(`not_close`, `converged`, or final; not `needs_review`), calibration consumes the exact tuned model plus the
`postprocess_suite_metadata.json` produced by the upstream postprocess suite.
The agent only stages the request, submits it after the postprocess job with an
`afterok` dependency, records paths/digests, and applies the final promotion
gate. It does not parse VTP, stack centerlines, validate observations, or tune
branch parameters. See the [svZeroDTrees production calibration
contract](https://github.com/ncdorn/svZeroDTrees/blob/main/docs/full_pa_calibration.md)
for the scientific input/output schema and quality gates.

The tune driver submits this dependency chain itself and reports the job IDs
(`svzt status` shows them). `svzt advance-iter --run-id <run-id> --execute`
records them, reports `calibration_pending` until calibration finishes, then
verifies, promotes, and advances; it submits the chain agent-side only when the
driver did not (older driver, failed driver `sbatch`, or `svzt continue`). The explicit operator sequence remains useful for recovery or
inspection:

```bash
# Use the global --workspace-root prefix when SVZ_WORKSPACE_ROOT is not set.
svzt preop select --run-id <run-id> --iteration <n> --reason "calibration source"
svzt run calibrate --run-id <run-id> --iteration <n> --execute
svzt calibration-status <run-id> --iteration <n>
```

Do not run `plan calibrate` immediately before the execute command in the
current implementation. The plan command persists a dry-run `submitted`
record with a `dryrun-*` job identity, and a matching execute request can
reuse that preview instead of submitting a real job. Treat this as a known
TASK-016 CLI defect; use the execute command directly after the postprocess
record exists. An isolated plan preview is still useful, but do not promote
its manifest record into a production run.

`svzt preop select` records `selected_preop_postprocess` and submits the
standalone postprocess job. The calibration request is staged at
`runs/<run-id>/iterations/iter-XX/calibration/` locally and at
`<runs_root>/<run-id>/iterations/iter-XX/calibration/` remotely. The generated
script runs `svzerodtrees.cli calibrate-0d-from-3d` and includes
`#SBATCH --dependency=afterok:<postprocess-job-id>`.

Promotion occurs only when the completion handoff records a successful
terminal state, matching input identity digest, `validated_lineage: true`, and
a locally available calibrated model with a SHA-256 digest. The promoted copy
is `calibration/results/calibrated_full_pa_zerod.promoted.json` and its path is
stored as the iteration's `calibrated_seed_path`. This is what permits the
next default-policy full-PA iteration to stage `full_pa_zerod.json`. For a
converged iteration, `svzt preop select` and `svzt run postop` use the remote
`calibration/results/calibrated_full_pa_zerod.json` as the postop 0D model and
fail closed if that iteration has no promoted calibration.

`watch --auto-advance` waits for the automatic postprocess/calibration chain,
fetches the calibrated publication, and only then submits the next full-PA
iteration. If the upstream publication is missing or invalid, the agent fails
closed and leaves the tuned result unpromoted.

Do not use this sequence for RRI or for automatic postoperative/adaptation
calibration. Those workflows remain explicit and are outside the preoperative
full-PA promotion policy.

### Calibration retry and rollback

For a retry, inspect the manifest and scheduler state first:

```bash
svzt status <run-id>
svzt calibration-status <run-id> --iteration <n>
```

Then inspect the postprocess dependency before resubmitting. Matching
`tuned_model`, `postprocess_descriptor`, and `postprocess_job` digests reuse an
existing active/terminal calibration record rather than creating a duplicate.
If any input digest changes, the old record is marked `invalidated` and a new
attempt is appended. A postprocess failure must be repaired upstream; do not
submit an independent calibration job without its `afterok` dependency.

If calibration fails, retain the tuned 0D/3D artifacts and diagnostic reports;
failure never promotes a candidate. To roll back, set
`calibration.enabled: false` and explicitly select
`next_iteration_seed_policy: legacy_rri_after_first` if continuing with the
historical reduced-RRI path is required. Rollback does not delete reports,
rewrite manifest history, or replace a last-known-good seed.

### Calibration failure diagnostics

Use `svzt calibration-status` for stage state, job/dependency IDs, digests, and
promotion reason. Then inspect, in order:

1. `runs/<run-id>/manifest.yaml`: `calibration_runs[]`,
   `promotion_records[]`, `last_known_good_promotion`, and the prior
   iteration's `calibrated_seed_path`.
2. The postprocess `postprocess_submission.json`,
   `postprocess_suite_metadata.json`, and scheduler logs under the iteration's
   `postprocess/` root.
3. Calibration `logs/` plus the upstream QC, confirmation, replay, target, and
   summary reports under `calibration/results/`.
4. `svzt config validate`/`svzt doctor` if paths, runtime activation, or
   executable resolution failed.

The upstream report explains scientific gate failures; adapter output explains
remote command or scheduler failures; `promotion_reason` explains mechanical
non-promotion (digest mismatch, non-terminal state, missing lineage, or
missing candidate). Preserve these artifacts when escalating.

**If `needs_review` due to a driver timeout** — the `svzt status` output prints a tip. Force-advance to the next iteration:
```bash
svzt continue <run-id> --execute
```

**If `needs_review` for any other reason** — inspect `runs/<run-id>/iterations/iter-NN/results/iteration_driver_log.json` locally. Fix the underlying issue, then re-submit just that iteration:
```bash
svzt run tune-iter --cluster sherlock --patient <patient-alias> --run-id <run-id> --execute
```
Add `--skip-zerod-tuning` to reuse existing 0D tuning artifacts and only redo the 3D submission.
Add `--iteration N --reuse-preop-3d` instead when the iteration's preop 3D run
already completed and only a post-3D step failed (centerline pressure CSV,
metrics, gate, postprocess/calibration submission): it implies
`--skip-zerod-tuning`, verifies that the previous driver log records the preop
job as `COMPLETED` and that `preop/` holds result VTUs, keeps that log as
`logs/iteration_driver_log.preop_<job>.json`, and reruns only the post-3D
steps. Without that evidence the iteration enters `needs_review`; it never
resubmits the 3D run.

---

## Fetching artifacts

Pull iteration artifacts locally at any time:

```bash
svzt fetch <run-id>           # pull configured artifacts
svzt fetch <run-id> --dry-run # preview rsync command only
```

Fetches whatever `defaults.artifacts.pull` specifies in the workspace YAML — typically `iteration_decision.json`, `iteration_metrics.json`, `mpa_pressure_vs_time.csv`, and `iteration_driver_log.json` for each iteration.

Selected-preop and explicit postop postprocess outputs are written under the run
tree at `iterations/iter-XX/results/postprocess/` and
`postop/from-iter-XX/results/postprocess/`.
Each postprocess directory now includes the separate
`centerline_timeseries_last_cycle.vtp` artifact and its metadata JSON alongside
the resistance-map products.
Their Slurm stdout/stderr logs live under `iterations/iter-XX/postprocess/logs/`
and `postop/from-iter-XX/logs/`.
Explicit postop runs receive the same resistance-map worker setting, resolving
`auto` against the single-node child postprocess allocation using the resolved
3D `procs_per_node`, but they do not modify the enclosing postop solver job
resource request.

## Build the finalized CFD results JSON

Once the selected-preop and explicit-postop postprocess artifacts are present
locally, normalize the run-scoped structured output:

```bash
svzt postprocess cfd-results --run-id <run-id>
```

Useful flags:
- `--source-json <path>` to migrate an older/manual JSON into the new template while refreshing run-derived fields
- `--overwrite` to replace an existing `runs/<run_id>/cfd-results.json`
- `--template <path>` or `--output <path>` for one-off migrations

The command reads local run artifacts only. If the needed postprocess files are
still remote, fetch or sync them first.

---

## Build tuning-progress diagnostics

To inspect how well the tuned 0D model matched the clinical targets before
BC-to-3D mapping at each iteration, generate the run-scoped tuning-progress
bundle:

```bash
svzt postprocess tuning-progress --run-id <run-id>
```

Outputs land under `runs/<run-id>/tuning-progress/`:
- `tuning_progress.csv`
- `tuning_progress.json`
- `tuning_progress.png`

The command overlays:
- 0D pre-mapping metrics from `pa_config_tuning_snapshot.json`
- 3D preop gate metrics from `iteration_metrics.json`
- targets and threshold bands from `iteration_decision.json`

For newer runs the iteration driver writes `zerod_pre_mapping_metrics.json`
directly. For older runs, the command backfills that summary from the local
snapshot when it exists in `iterations/.../results/` or `pulled_outputs/.../results/`.

---

## Convergence flow

```
iter 1 → not_close → iter 2 → not_close → ... → converged
                                      → svzt preop select → postprocess
                                      → (optional full_pa calibration → guarded promotion)
                                      → svzt run postop
                         ↘ needs_review → svzt continue  (timeout)
                                        → svzt run tune-iter  (other)
```

The parenthesized calibration path is automatic for every completed
default-policy full-PA iteration, including the converged one (via
`advance-iter --execute` or `watch --auto-advance`). A full-PA run using the default `calibrated_full_pa`
seed policy cannot advance to the next full-PA iteration
without a recorded successful promotion.

The clinical gate checks four metrics against `clinical_targets.csv`:

| Metric | Tolerance |
|---|---|
| MPA systolic pressure | ±5 mmHg |
| MPA diastolic pressure | ±3 mmHg |
| MPA mean pressure | ±3 mmHg |
| RPA flow split | ±5% |

When all four are within tolerance the driver marks `converged`. Postop 3D
submission is an explicit operator step using the manifest-recorded converged
preop iteration. In rare cases where the best tuned preop iteration is not the
last iteration, select that iteration with `svzt preop select`.

---

## Command reference

| Command | When to use |
|---|---|
| `svzt init-run --cluster C --patient P --run-id R` | Once, before anything else |
| `svzt plan tune --cluster C --patient P --run-id R` | Inspect plan, validate config |
| `svzt run tune --cluster C --patient P --run-id R --execute` | Submit iteration 1 |
| `svzt watch R --auto-advance --fetch-on-complete` | Fully automated monitoring loop |
| `svzt watch R --fetch-on-complete` | Watch one iteration |
| `svzt status R` | Check current state and decision |
| `svzt fetch R` | Pull artifacts locally |
| `svzt advance-iter --run-id R --execute` | Advance after `not_close` |
| `svzt preop select --run-id R --iteration N` | Record converged preop iteration |
| `svzt run postop --run-id R` | Preview explicit postop submission |
| `svzt run postop --run-id R --execute` | Submit explicit postop simulation |
| `svzt postprocess tuning-progress --run-id R` | Build run-scoped 0D vs 3D tuning diagnostics |
| `svzt continue R --execute` | Force-advance after driver timeout |
| `svzt run tune-iter --cluster C --patient P --run-id R --execute` | Re-submit a stuck/failed iteration |
| `svzt run tune-iter ... --skip-zerod-tuning --execute` | Re-submit 3D only, reuse 0D artifacts |
| `svzt run tune-iter ... --iteration N --reuse-preop-3d --execute` | Rerun post-3D steps on a completed preop 3D result |
