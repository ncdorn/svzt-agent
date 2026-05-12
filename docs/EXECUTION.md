# Execution Mode

## Commands
- `svzt run tune --cluster <name> --patient <alias> [--run-id <id>] [--execute]`
- `svzt run tune-iter --cluster <name> --patient <alias> --run-id <id> [--iteration <n>] [--execute]`
- `svzt preop select --run-id <id> --iteration <n> [--reason <text>]`
- `svzt run postop --run-id <id> [--dry-run|--execute]`
- `svzt campaign seed-sweep plan --cluster <name> [--campaign-id <id>] [--patients <alias> ...]`
- `svzt campaign seed-sweep run <campaign-id> [--dry-run|--execute]`
- `svzt campaign seed-sweep summarize <campaign-id>`
- `svzt campaign seed-sweep slides <campaign-id>`
- `svzt advance-iter --run-id <id> [--execute]`
- `svzt watch <run-id> [--auto-advance] [--poll-interval-seconds <n>] [--timeout-seconds <n>] [--max-polls <n>] [--fetch-on-complete]`
- `svzt status <run-id>`
- `svzt fetch <run-id> [--dry-run]`

## Iteration-1 Seed Configuration
- Configure iteration-1 seed once in YAML:
  - `defaults.tuning.iteration1_seed`
  - optional `patients[].tuning.iteration1_seed` override
- Supported source modes:
  - `path`: use configured `path`; if missing at run time, fallback to `generate`
  - `generate`: always generate via svZeroDTrees
- Relative `path` values are resolved under each patient's `permanent_remote_path`; absolute paths are used as-is.
- Dry-run preview only generates the seed locally when the required patient assets are mounted locally.
- When those assets only exist on Sherlock/Oak, `svzt run tune` leaves `inputs/simplified_nonlinear_zerod.json` unstaged and the remote iteration driver generates it during execution.

## Impedance Tuning Configuration
- Configure impedance tuning defaults in YAML:
  - `defaults.tuning.impedance`
  - optional `patients[].tuning.impedance` override
- Key controls:
  - `solver`, `nm_iter`, `n_procs`, `grid_search_init`
  - `d_min`, `use_mean`, `specify_diameter`
  - `diameter_scale`, `diameter_std_cap`, `allow_ordered_outlet_mapping`
  - `tuning_model`: `rri` for reduced PA/RRI tuning, or `full_pa` for learned full pulmonary 0D configs
  - `rescale_inflow`, `convert_to_cm`, `compliance_model`
- Policy: each iteration retunes from a staged seed under `inputs/`.
  Reduced RRI runs use `simplified_nonlinear_zerod.json`; `full_pa` runs use
  `full_pa_zerod.json`.
- The default Nelder-Mead repeat count is `nm_iter: 5`.
- Full learned 0D seeds with many outlet BCs require deterministic cap-to-BC mapping by matching BC names or outlet metadata. Order-only mapping is rejected unless `allow_ordered_outlet_mapping: true` is set for a legacy run.

## Seed-Sweep Campaigns
- `svzt campaign seed-sweep plan` creates `runs/campaigns/<campaign-id>/campaign_manifest.yaml` plus one child workspace per patient/case.
- Without `--patients`, the default learned seed sweep targets TST-STAN-5 and creates exactly three child runs.
- Default cases compare learned full-0D seeds with `tuning_model=full_pa` and `diameter_scale=0.0`, learned full-0D seeds with `tuning_model=full_pa` and `diameter_scale=0.1`, and an RRI-prepared reduced seed from the learned reference with `tuning_model=rri`.
- svZeroDTrees normalizes final tree assignment to `use_mean: false` whenever `diameter_scale > 0`, so outlet diameter spread is applied in the tuned 0D source. Full-PA optimization still uses shared LPA/RPA mean trees during each objective evaluation.
- The learned full-0D source is `baseline_0d_learned.json` under the patient's local `zerod-models` directory and is staged into each full-PA child run as `inputs/full_pa_zerod.json`; the reduced RRI case prepares `prepared_inputs/simplified_zerod_tuned_RRI.json` from that reference.
- Full-PA child runs use `tuning_model=full_pa` only for iteration 1. If iteration 1 is `not_close`, the iteration driver regenerates `results/simplified_zerod_tuned_RRI.json`, and iteration 2 onward run reduced RRI tuning from `inputs/simplified_nonlinear_zerod.json`.
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
- `prestress_file` supports:
  - absolute path: pass the prescribed VTU through as `Prestress_file_path`
  - `generate`: compute run-scoped prestress from iteration-1 seed-generation
    `steady/mean` VTUs, then reuse `<runs_root>/<run_id>/prestress/*-procs/result_*.vtu`
  - `auto` / `from_steady_mean`: legacy unsupported modes in `svzt-agent`; the
    iteration script logs a warning and continues without `Prestress_file_path`
- Set `patients[].tuning.threed.tissue_support.enabled: false` when overriding a patient to `wall_model: rigid`.
- Spatial Robin support uses `tissue_support.type: spatial` with `spatial_values_file_path` pointing to a VTP file containing `Stiffness` and `Damping` arrays.

## Dry-run vs execute
- Default for `svzt run tune` and `svzt run postop` is dry-run.
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
  - validates the canonical `svzerod_3Dcoupling.json` directly; it does not require or generate deprecated `svZeroD_interface.dat`
  - links the generated coupling input into the patient preop 3D model directory for that iteration
  - submits preop 3D (`SimulationDirectory`) and waits up to the configured
    `wait_timeout_seconds` value, defaulting to 43200 seconds
  - requires `results/mpa_pressure_vs_time.csv` plus preop flow split for gating
  - `not_close` regenerates reduced PA config for next iteration
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
- `svzt advance-iter` returns a pause action for `needs_review` and does not submit a new iteration job.
- `svzt preop select` records `converged_preop_iteration` in the manifest. The
  selected iteration can be a formally converged iteration or an operator-promoted
  best completed iteration.
- `svzt preop select` now also stages and submits a selected-preop postprocess
  job that writes normalized artifacts under
  `runs/<run_id>/iterations/iter-XX/results/postprocess/` on the cluster and
  records `selected_preop_postprocess` in the manifest.
- `svzt run postop` consumes `converged_preop_iteration`, writes a postop plan
  under `runs/<run_id>/postop/from-iter-XX/`, and stages/submits the postop job
  under `<runs_root>/<run_id>/postop/from-iter-XX/`. If `--execute` is the first
  postop invocation, planning and validation still occur before remote mutation.
- Explicit postop runs now execute the upstream `svZeroDTrees` pulmonary 3D
  postprocess suite after solver completion and write normalized artifacts under
  `<runs_root>/<run_id>/postop/from-iter-XX/results/postprocess/`. The cluster
  config must provide `executables.svslicer_path`.
- `svzt status <run-id>` now combines parent scheduler state with iteration progress artifacts:
  - prints the active iteration and tracker status
  - reports the best-known stage within the active iteration (`0D` tuning, preop `3D`, post-preop analysis, postop `3D`, or terminal branch outcome)
  - polls child preop/postop jobs when their job IDs are present in iteration
    artifacts or explicit postop manifest records
  - performs a targeted pull of `iteration_driver_log.json`, `iteration_decision.json`, and `iteration_metrics.json` for the active iteration when those artifacts are not already available locally
- `svzt watch --auto-advance` composes monitor + decision pull + iteration advance/submit:
  - after each completed iteration, pulls:
    - remote: `<runs_root>/<run_id>/iterations/iter-XX/results/iteration_decision.json`
    - remote: `<runs_root>/<run_id>/iterations/iter-XX/results/iteration_metrics.json`
    - remote: `<runs_root>/<run_id>/iterations/iter-XX/results/simplified_zerod_tuned_RRI.json`
    - local destination: `runs/<run_id>/iterations/iter-XX/results/`
  - then executes `advance_tune_iteration(..., execute=True)` to submit the next iteration when applicable.
  - halts with `final_action=needs_review_pause` when an iteration enters review-required state.
