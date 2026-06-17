# Execution Mode

## Commands
- `svzt init-workspace [<path>] [--force]`
- `svzt config validate`
- `svzt doctor`
- `svzt run tune --cluster <name> --patient <alias> [--run-id <id>] [--execute]`
- `svzt run tune-iter --cluster <name> --patient <alias> --run-id <id> [--iteration <n>] [--execute]`
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
  `svslicer` centerline projections used by the mean resistance-map pass. The
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
  - halts with `final_action=needs_review_pause` when an iteration enters review-required state.
