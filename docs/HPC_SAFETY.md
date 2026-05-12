# HPC Safety Model

## Path safety
- `patient_data_root` is read-only source-of-truth data.
- `runs_root` is the only allowed remote write root for agent-generated artifacts.
- Remote write paths are normalized and validated before command execution.
- Any write path outside `runs_root` or under `patient_data_root` is rejected.

## Command safety
- Remote commands are validated against an allowlist (`mkdir`, `test`, `sbatch`, `squeue`, `sacct`, `scancel`, `bash`).
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
