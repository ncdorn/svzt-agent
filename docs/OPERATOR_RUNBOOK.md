# Operator Runbook: Running a Full Pipeline

This document is the practical start-to-finish guide for running a pulmonary BC tuning pipeline on Sherlock. It covers every command, when to run it, and what to do at each outcome.

## Prerequisites

Before starting:

- The workspace YAML is configured with your cluster, patient alias, and defaults (see [`docs/PATIENT_DATA_CONTRACT.md`](./PATIENT_DATA_CONTRACT.md)).
- Patient data is present on Sherlock under the configured `permanent_remote_path`: `mesh-complete/`, `centerlines.vtp`, `inflow.csv`, `clinical_targets.csv`.
- `svzt` is installed and either `SVZ_WORKSPACE_ROOT` is set or you pass `--workspace-root` to every command.

---

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
1. Stages the iteration-1 seed config (or generates one on the cluster if `iteration1_seed.source: generate`)
2. Uploads the job script and inputs to Sherlock via rsync
3. Submits the SLURM driver job and prints the job ID

The SLURM driver job is long-running. For each iteration it:
- Runs 0D BC tuning (impedance or RCR on the staged seed)
- Submits the preop 3D CMM job and waits for it
- Post-processes `result_*.vtu` files against the MPA centerline to generate `mpa_pressure_vs_time.csv`
- Evaluates the clinical gate against `clinical_targets.csv`
- Writes `iteration_decision.json` and (if `not_close`) regenerates `simplified_zerod_tuned_RRI.json` as the seed for the next iteration

---

## Step 4 — Monitor and advance

### Option A: Fully automated (recommended)

```bash
svzt watch <run-id> --auto-advance --fetch-on-complete
```

Runs until convergence, max iterations, or a `needs_review` pause. For each iteration it:
1. Polls SLURM until the driver job completes
2. Pulls `iteration_decision.json`, `iteration_metrics.json`, and `simplified_zerod_tuned_RRI.json`
3. If `not_close`: seeds and submits the next iteration automatically
4. If `converged`: exits cleanly; record the converged preop iteration and submit postop explicitly
5. If `needs_review`: exits with code 1 and prints the reason

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

**If `needs_review` due to a driver timeout** — the `svzt status` output prints a tip. Force-advance to the next iteration:
```bash
svzt continue <run-id> --execute
```

**If `needs_review` for any other reason** — inspect `runs/<run-id>/iterations/iter-NN/results/iteration_driver_log.json` locally. Fix the underlying issue, then re-submit just that iteration:
```bash
svzt run tune-iter --cluster sherlock --patient <patient-alias> --run-id <run-id> --execute
```
Add `--skip-zerod-tuning` to reuse existing 0D tuning artifacts and only redo the 3D submission.

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
iter 1 → not_close → iter 2 → not_close → ... → converged → svzt preop select → svzt run postop
                                        ↘ needs_review → svzt continue  (timeout)
                                                       → svzt run tune-iter  (other)
```

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
