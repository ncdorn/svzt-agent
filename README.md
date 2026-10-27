# svzt-agent

|        |        |
|--------|--------|
| Package | [![Latest TestPyPI Version](https://img.shields.io/badge/TestPyPI-pending-lightgrey.svg)](https://test.pypi.org/project/svzt-agent/) |
| Source | [GitHub](https://github.com/ncdorn/svzt-agent) |
| Meta | [MIT License](./LICENSE) |

`svzt-agent` is the deterministic orchestration layer for running `svZeroDTrees` workflows on HPC. It owns config resolution, inspectable planning, bounded adapter-mediated execution, monitoring, manifests, and controlled iteration advancement.

## Install

For local development, use Hatch:

```bash
hatch run test:run
```

Once published, install the package with pip:

```bash
pip install svzt-agent
```

For TestPyPI validation:

```bash
pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple svzt-agent
```

## Quickstart

The installed console command remains `svzt`:

```bash
svzt init-workspace ./svz-workspace
svzt --workspace-root ./svz-workspace config validate
svzt --workspace-root ./svz-workspace doctor
svzt plan tune --cluster sherlock --patient TST-STAN-x --run-id demo-run
svzt run tune --cluster sherlock --patient TST-STAN-x --run-id demo-run --execute
svzt watch demo-run --fetch-on-complete --auto-advance
svzt advance-iter --run-id demo-run --execute
svzt run tune-iter --cluster sherlock --patient TST-STAN-x --run-id demo-run --iteration 1 --reuse-preop-3d --execute
svzt run wall-correction --run-id demo-run --execute   # after the last tuning iteration
svzt wall-correction-status demo-run                    # poll; fetches the calibration reports
svzt preop finalize --run-id demo-run                   # record the pre-op source for post-op
svzt run postop --run-id demo-run --execute
svzt postprocess cfd-results --run-id demo-run
svzt campaign seed-sweep plan --cluster sherlock --campaign-id tst-stan-5-learned
svzt update --message "sync local workflow changes" --execute
```

Cluster configs should provide `executables.svfsiplus_path`,
`executables.svzerodsolver_build_dir`, and `executables.svslicer_path`.
The `svZeroDSolver` build directory is injected into generated 3D configs so
`svZeroDTrees` can resolve `libsvzero_interface.so` without hardcoded home-directory paths.

`svzt init-workspace` bootstraps a local workspace with example `config/`
files, an `AGENTS.md` workspace router, plus `runs/`, `mirrors/`, and
`templates/` directories. `svzt config validate` checks the required YAML
config plus optional repository-location overrides, and `svzt doctor`
summarizes local workspace diagnostics and checkout discovery warnings. Both
report impedance tuning cross-checks (outlet pressure policy vs each patient's
regurgitation flag, proximal vs 3D wall stiffness); the physiological full-PA
tuning model and its configuration are described in `docs/TUNING_MODEL.md`.

`svzt update` is a local maintenance helper for the sibling-checkout setup. It
uses one commit message for both `svzt-agent` and `svZeroDTrees`, pushes the
current branch for each repo, then SSHes to Sherlock to run `git pull` in
`/home/users/ndorn/svZeroDTrees` followed by `pip install -e
/home/users/ndorn/svZeroDTrees`. Use `--dry-run` to preview the command list.

Resistance-map frame mapping for selected-preop and explicit postop
postprocessing is controlled by `defaults.postprocess.resistance_map.*`.
When `workers: auto` is left in place, `svzt-agent` resolves it to the full
single-node postprocess allocation instead of forcing a 4-worker cap:
selected-preop uses `defaults.scheduler.cpus` when it is numeric, and explicit
postop uses the resolved 3D `procs_per_node`. Both postprocess jobs request
matching `--cpus-per-task`; selected-preop also uses `selected_preop_mem` when
more than one worker is requested. svSlicer is OpenMP-parallel within a frame:
`svslicer_threads` (default 1) is exported as `OMP_NUM_THREADS`, selected-preop
jobs request `workers x svslicer_threads` CPUs, and explicit postop splits its
CPUs into `procs_per_node // svslicer_threads` frame workers.

Run-scoped CFD output JSONs are built locally with:

```bash
svzt --workspace-root /path/to/svz-workspace postprocess cfd-results --run-id <run-id>
```

By default this reads `data/cfd-results/cfd-results-template.json`, overlays any
existing `runs/<run_id>/cfd-results.json` if present, refreshes run-derived
fields from local artifacts and manifest-backed run status, and writes back to
`runs/<run_id>/cfd-results.json`.

Iteration-1 seed generation is selected in YAML. `source: generate` creates the
existing reduced seed, while `source: learned_zerod` first creates a run-scoped
source model and then invokes the validated svZeroDTrees learned-zeroD adapter
to publish `inputs/full_pa_zerod.json`. Both modes run in the Sherlock iteration
driver during `--execute`; dry-run previews leave the seed unstaged and render
the remote behavior. Learned generation requires full-PA impedance tuning and
`learned-zerod` plus `svzerodsolver` in the activated environment's `PATH`
unless explicit executable overrides are configured.

The default learned seed sweep targets TST-STAN-5 and creates three child runs:
two full pulmonary 0D learned-seed cases and one reduced RRI learned-seed case.
Full pulmonary cases stage the learned seed as `inputs/full_pa_zerod.json` and use
`outlet_mapping_mode: auto`, which falls back to geometric mapping against the
patient `centerlines.vtp` (serialized cap order mispairs learned-seed BCs); reduced
RRI cases use `inputs/simplified_nonlinear_zerod.json`.
Under the default `calibrated_full_pa` policy, later full-PA iterations require
a successfully promoted calibrated seed; `advance-iter --execute` and
`watch --auto-advance` verify and promote a calibrated full-PA model after every
completed iteration (`not_close`, `converged`, or the final iteration); the
converged model feeds postop. The tune driver submits the postprocess and
calibration jobs itself (just as it regenerates the simplified RRI seed inline
under the RRI policies), so `run tune` alone starts seed generation;
`svzt status` shows those jobs. Switching to reduced RRI after iteration 1
requires the explicit `legacy_rri_after_first` policy.

### Wall-correction step

After the last tuning iteration, `svzt run wall-correction` repeats that
iteration's deformable-wall 3D run with the same 0D BCs and a corrected wall,
then calibrates a 0D model to match it. This gives a tuned 3D run and a
matching 0D model (protocol v1, steps 7–8). The step works as follows:

- **Wall rule:** λ = (PP_target / PP_3D)^(1/α), with λ ≥ λ_min. Then
  E = λ·E_source, and Rule A damping is c = c₁·√(E / E₁), using the
  iteration-1 wall.
- **Settings:** `defaults.tuning.wall_correction` (`alpha`, `lambda_min`,
  `damping_rule`, `gate_policy`, `compliance_fit`, `rho_infty`) applies to the
  whole cohort, with no patient override. The workspace `defaults.yaml` sets
  every value explicitly (protocol v1.1: 0.55, 0.5, Rule A, `report_only`,
  ρ bounds [0.2, 5.0] / 0.05 mmHg / 40 iterations, 0.2).
- **Jobs:** the 3D run, the standard postprocess and an R + proximal-compliance
  calibration are chained with `afterok` dependencies. The calibration fits one
  multiplier on proximal C so the 0D model reproduces the 3D pulse pressure.

```bash
svzt run wall-correction --run-id <run> [--iteration N] --dry-run
svzt run wall-correction --run-id <run> --execute
svzt wall-correction-status <run> [--correction-id ID]
svzt preop finalize --run-id <run> [--correction-id ID] [--protocol preop_v1.1]
```

`wall-correction-status` only polls the jobs, fetches the calibration reports
and updates the wall-correction record. Once the matched 0D model is
published, it prints the finalize command. `svzt preop finalize` is the
explicit, idempotent transition that records the wall-corrected 3D run as the
run's pre-op source (`converged_preop_iteration`, `selection_kind:
wall_correction`); rerunning it changes nothing, and it refuses to replace a
different source once post-op has been submitted:
- **Post-op and adaptation reuse:**
  - its 3D directory and BCs;
  - its corrected E and damping;
  - its matched 0D model.
- **Tree parameters and outlet mapping** still come from the source iteration's
  `results/`.
- **`svzt preop select` is retired.** It exits with an error pointing to the
  steps above; existing manual handoffs still load and are still used.
- **`--protocol`** declares the frozen protocol the run follows
  (`manifest.protocol`, see docs/MANIFEST.md).

The five protocol v1 runs were registered with a since-removed
`--adopt-threed-dir` mode, which verified existing 3D runs against the rule
instead of rerunning them. Their `mode: adopted` records still load.

Inputs are fetched read-only even on `--dry-run`, so the plan
(`runs/<run>/wall_correction/iter-XX/wall_correction_plan.json`) shows the
real rule. Records go to `manifest.wall_correction_runs[]` and never replace the
source iteration's own calibration or postprocess records.

`wall-correction-status` records the **matched 0D model** for the corrected 3D run.
This is the model to compare with the post-op 0D model when predicting
adaptation, so it must reproduce this 3D run vessel by vessel.
- **Calibration:** R is calibrated to every vessel's 3D pressure drop and flow,
  then ρ matches the pulse.
- **Gate:** the step runs it with the `report_only` gate policy
  (`wall_correction.gate_policy`, passed to the job's target resolver as an
  argument; tuning-iteration calibrations pass `improvement_only`). Any replay-stable calibrated model is
  published, and the MPA and split scores are recorded rather than enforced. An
  improvement-over-tuned gate is the wrong test here, because the tuned model
  never saw this 3D run.
- **Integration damping:** the replays and the published model use
  `wall_correction.rho_infty` (0.2) as the generalized-α spectral radius. At the
  solver default of 0.5, TST-STAN-5's calibrated model diverged numerically.
  The tuned model's MPA pressures move by at most 0.02 mmHg.
- **If calibration publishes nothing:** there is no matched model, and the
  tuned model is not substituted.

The Python import package is `svztagent`:

```python
from svztagent.cli.main import main
```

## Safety Summary

- Patient source data stays read-only.
- Remote writes are restricted to the configured `runs_root`.
- All remote execution flows through typed SSH/rsync/Slurm adapters.
- Plans remain deterministic, inspectable, and validated before execution.
- Manifest state is the source of truth for lifecycle and iteration tracking.

## Documentation

The docs in [`docs/`](./docs/) remain the authoritative operator and architecture references. Key entry points:

- [`docs/OPERATOR_RUNBOOK.md`](./docs/OPERATOR_RUNBOOK.md) — start here for end-to-end pipeline runs
- [`docs/ARCHITECTURE.md`](./docs/ARCHITECTURE.md)
- [`docs/PLANNING.md`](./docs/PLANNING.md)
- [`docs/EXECUTION.md`](./docs/EXECUTION.md)
- [`docs/MONITORING.md`](./docs/MONITORING.md)
- [`docs/MANIFEST.md`](./docs/MANIFEST.md)
- [`docs/HPC_SAFETY.md`](./docs/HPC_SAFETY.md)

## Development

Hatch is the canonical dev workflow because the package requires `pydantic>=2.6` and the wrong global interpreter environment will fail fast.

```bash
hatch run test:run
hatch run build:check
hatch run docs:build
```

To work inside a Hatch environment directly:

```bash
hatch shell test.py3.11
```

## Contributing

See [`CONTRIBUTING.md`](./CONTRIBUTING.md) for contribution guidelines and [`DEVELOPMENT.md`](./DEVELOPMENT.md) for the packaging and environment workflow.

## Copyright

- Copyright © 2026 Nick Dorn.
- Free software distributed under the [MIT License](./LICENSE).
