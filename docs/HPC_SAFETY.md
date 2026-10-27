# HPC Safety Model

## Path safety
- `permanent_data_root` is read-only source-of-truth patient data.
- `runs_root` is the only allowed remote write root for agent-generated artifacts.
- Remote write paths are normalized and validated before command execution.
- Any write path outside `runs_root` is rejected.
- Scaled geometry copies for patients with `mesh_scale_factor` != 1 are written
  under `runs_root/_staged_geometry/` (docs/PATIENT_DATA_CONTRACT.md); stages
  then read geometry from there instead of from `permanent_data_root`.

## Command safety
- Remote commands are validated against an allowlist (`mkdir`, `test`, `sbatch`, `squeue`, `sacct`, `scancel`, `bash`).
- The tune driver job submits child Slurm jobs from inside the cluster: the preop
  3D job and, under `calibrated_full_pa`, the preop postprocess and calibration
  jobs. The postprocess/calibration scripts and their exact `sbatch` argv are
  rendered agent-side at `run tune` time, validated under `runs_root`, and
  embedded in the driver; the driver only adds `--dependency=afterok:<id>` and
  writes the calibration YAML under the run's `calibration/inputs/`. It reports
  every child job ID in `iteration_decision.json`, and the agent records them
  in the manifest before polling or promotion.
- With `defaults.execution.nested_submission: in_allocation` the driver
  submits nothing: it runs each stage's agent-normalized `run_solver.sh` (under
  the run's iteration directory) with `bash` and an explicit
  `srun --nodes --ntasks` inside its own allocation, checks attempt ownership
  before each run and at every poll, and stops the solver's process group on
  timeout or when superseded. Postprocess and calibration jobs are then
  submitted by the agent through the adapter (`sbatch` from the login node).
- Forbidden shell-control tokens are rejected.
- Adapters accept argv arrays, not arbitrary shell strings from workflow code.

## Failure behavior
- Unsafe paths and commands raise explicit exceptions (`PathPolicyError`, `UnsafePathError`, `CommandRejectedError`).
- Non-zero process exits raise `AdapterExecutionError` with argv/stdout/stderr context.
- Scheduler parse failures raise `SchedulerResponseError`.

## Determinism
- Command construction order is stable.
- Job script rendering is template-driven with explicit placeholders.
- Manifest updates for submission/status/fetch are explicit and append-only where applicable (`fetch_timestamps`).
