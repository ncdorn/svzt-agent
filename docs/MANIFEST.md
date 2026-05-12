# Manifest

`runs/<run_id>/manifest.yaml` is the source of truth for run lifecycle metadata.

## Lifecycle fields
Lifecycle metadata is stored under `execution`:
- `lifecycle_state`: current internal lifecycle state
- `raw_scheduler_state`: last raw scheduler state observed
- `normalized_scheduler_state`: last normalized scheduler state
- `last_known_scheduler_state`: backward-compatible alias for normalized state
- `lifecycle_history[]`: append-only transition records
- `lifecycle_timestamps`:
  - `submission_at`
  - `first_pending_at`
  - `first_running_at`
  - `terminal_state_at`
  - `fetch_at`
- `last_polled_at`
- `poll_count`
- `terminal_reason`
- `monitor_settings` (latest watch session settings)

## Iteration fields
Iteration metadata is stored under `tuning_iteration_tracker`:
- `current_iteration`
- `max_iterations`
- `status` (`active|converged|failed_max_iter|paused_review`)
- `converged_iteration`
- `iterations[]` (per-iteration records)

Each iteration record can include:
- iteration index
- local/remote iteration directories
- tune job id + script path + scheduler state
- metrics + deltas versus clinical targets
- branch decision (`not_close|converged|max_iter_failed|needs_review`)
- carry-forward config (`regenerated_config_path`)
- legacy postop submission intent / job id, if present in older iteration artifacts

## Converged preop handoff
`converged_preop_iteration` records the preop iteration to use for explicit
postop generation. It is written by:

```bash
svzt preop select --run-id <run-id> --iteration <n> [--reason <text>]
```

The selected iteration may be formally `converged` or an operator-promoted best
completed iteration. The record includes the original decision, selection kind,
reason, metrics/deltas, remote iteration/preop directories, tuned 0D artifact
path, canonical coupler path, and preop job id.

`postop_run` records explicit postop submission metadata written by
`svzt run postop --run-id <run-id> --execute`, including source preop iteration,
local/remote postop directories, script paths, and postop job id.

`selected_preop_postprocess` records the follow-on selected-preop postprocess
submission created by `svzt preop select`, including the source iteration,
local/remote postprocess directories, script paths, scheduler job id, and
artifact-fetch status.

`postop_postprocess` records the explicit postop postprocess submission created
alongside `postop_run`. It points at the normalized artifact root under
`postop/from-iter-XX/results/postprocess/`.

`remote.svzerodtrees_paths` includes resolved 3D assets:
- `mesh_surfaces`
- `preop_mesh_complete`
- optional `postop_mesh_complete`
- `centerlines`
- `inflow`
- `clinical_targets`

`remote.threed_defaults` stores the effective merged 3D tuning config used for iteration script rendering.

## Transition records
Each `execution.lifecycle_history[]` entry records:
- timestamp
- `from_state`
- `to_state`
- raw + normalized scheduler state
- scheduler source (`squeue` or `sacct`)
- optional reason/note

History is append-only. Same-state observations update poll metadata but do not append history entries.

## Fetch metadata
- `fetch_attempted`
- `fetch_succeeded`
- `fetch_timestamps[]`
- `retrieved_artifacts[]`

Fetch transitions to `fetched` when valid from terminal states.
