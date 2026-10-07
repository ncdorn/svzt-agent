# Manifest

`runs/<run_id>/manifest.yaml` is the source of truth for run lifecycle metadata.
It is written atomically and should be treated as append-only evidence: a retry
adds a new transition/attempt or updates the matching idempotent submission;
it must not rewrite a prior scientific result or seed.

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

## Full-PA postprocess and calibration records

`preop_postprocess_runs[]` is the per-iteration collection used by automatic or
future preoperative postprocess stages. The explicit `selected_preop_postprocess`
record is the current selected-preop handoff created by `svzt preop select`.
Both records carry `stage`, `source_preop_iteration`, `status`, local/remote
directories, script paths, `scheduler_job_id`, optional `terminal_state`,
`descriptor_path`, `input_digests`, `output_digests`, and `notes`.

For an iteration `n`, the normalized postprocess output is expected at:

```text
local:  runs/<run_id>/iterations/iter-XX/results/postprocess/
remote: <runs_root>/<run_id>/iterations/iter-XX/results/postprocess/
```

The job's submission/log context is under the iteration's `postprocess/`
directory. `postprocess_suite_metadata.json` is the handoff descriptor;
scientific validation and interpretation of that descriptor belong to
`svZeroDTrees`, not to this manifest schema.

`calibration_runs[]` contains one agent record per calibration input identity
and iteration. Under `calibrated_full_pa` the tune driver normally submits the
postprocess and calibration jobs and reports them in `iteration_decision.json`
(`seed_generation`); the agent then records them here and in
`preop_postprocess_runs[]` with the notes "Calibration submitted by the tune
driver" / "Preop postprocess submitted by the tune driver". `local_config_path`
is then the pre-rendered `calibration_request.json`; the driver writes the
final remote `calibrate_0d_from_3d.yaml`. The record fields are:

- identity and scope: `calibration_id`, `iteration`, `stage` (`preop`), and
  `status`;
- layout/submission: `local_dir`, `remote_dir`, local/remote config and job
  script paths, `scheduler_job_id`, `postprocess_job_id`,
  `dependency_job_id`, and `dependency_type` (`afterok`);
- timing/state: `submitted_at`, `updated_at`, `terminal_state`, and
  `transitions[]`;
- lineage: `input_artifacts`, `input_digests`, `output_artifacts`,
  `output_digests`, `lineage`, and `validated_lineage`;
- promotion: `promotion_status`, `promotion_reason`, and operator notes.

The agent-owned calibration directory is:

```text
local and remote:
  iterations/iter-XX/calibration/
  ├── inputs/calibrate_0d_from_3d.yaml
  ├── run_calibration.sh
  ├── logs/
  └── results/
      ├── calibrated_full_pa_zerod.json             # upstream candidate
      └── calibrated_full_pa_zerod.promoted.json   # only after promotion
```

The generated script embeds `#SBATCH --dependency=afterok:<postprocess-job-id>`.
Consequently, the calibration record must point to the exact postprocess job
that produced its descriptor; a missing record or job ID is a failed preflight,
not permission to submit an independent calibration job.

Calibration `status` is an agent stage status and may be `planned`, `submitted`,
`pending`, `running`, `completed`, `succeeded`, `failed`, `cancelled`, or
`invalidated`. `transitions[]` records the state changes with `at`,
`from_state`, `to_state`, and optional reason/note. A changed tuned-model,
descriptor, or postprocess-job identity marks the old attempt `invalidated`
before a new attempt is appended. Matching input digests make resubmission
idempotent for active/completed records.

The agent does not decide whether observations, fixed-point confirmation,
replay, or pulmonary targets pass. Upstream `svZeroDTrees` must publish its
successful summary and calibrated model; the agent records
`validated_lineage: true` only after fetching that publication; see the [upstream
full-PA tuning and calibration guide](https://github.com/ncdorn/svZeroDTrees/blob/main/docs/full_pa_calibration.md).

`paraview_viz_runs[]` records ParaView visualization submissions for selected
pre-op, explicit post-op, and adaptation stages. Selected-preop entries usually
carry the child scheduler job id immediately because the sibling job is
submitted directly by `svzt preop select`. Explicit postop/adaptation entries
are appended at launch time with `status: planned` because the child job id is
not known until the remote manager later submits the stage-owned ParaView job.
The remote truth for those manager-owned child submissions is recorded in
`results/paraview_viz/paraview_viz_submission.json`.

`runs/<run_id>/cfd-results.json` is a run-scoped derived artifact produced from
the template plus fetched/local postprocess evidence and manifest-backed run
status. It is not a manifest
field, and the manifest should only point to the source iteration and postprocess
artifacts used to build it.

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

## Calibration completion and guarded promotion

`promotion_records[]` is an append-only audit of every compare-and-set decision.
Each record carries:

- `iteration`, `status` (`promoted` or `rejected`), and `decision`
  (`promoted` or `not_promoted`);
- `source_calibration_id` and `calibration_input_digest`;
- `tuned_model_digest`, `descriptor_digest`, and (on success)
  `calibrated_model_digest`;
- `candidate_path`, `promoted_seed_path`, optional `previous_seed_path`,
  `terminal_state`, `validated_lineage`, `reason`, and `at`.

Promotion is accepted only when the requested calibration identity matches the
record's input digest, the calibration is both successful and terminal, the
fetched upstream publication confirms success, and the candidate exists
locally with a SHA-256 digest. On success the agent records
`validated_lineage: true` and copies the
candidate to:

```text
runs/<run_id>/iterations/iter-XX/calibration/results/
  calibrated_full_pa_zerod.promoted.json
```

and writes that path to the iteration's `calibrated_seed_path`. Rejected,
stale, incomplete, failed, or missing candidates still append a promotion
record but cannot replace `last_known_good_promotion`. The latter is the
compare-and-set pointer that a subsequent default-policy full-PA iteration may
use. `calibration_promotion` is retained as a compatibility alias for the
latest successful promotion.

The upstream scientific gates are intentionally not duplicated in the agent:
observation/data-contract QC, fixed-point confirmation, finite output,
replay stability, target evaluation, and provenance are owned by
`svZeroDTrees`. A successful scheduler state alone is insufficient for
promotion.

## Resume, retry, and rollback

On startup, `read_manifest` accepts manifests written before the calibration
fields existed; those manifests load with empty calibration/promotion history.
When the tuned model, descriptor, and postprocess job digests are unchanged,
the calibration workflow reuses an existing `submitted`/`running`/terminal
record. A changed identity invalidates the previous attempt and creates a new
record while retaining all old evidence. Atomic manifest writes make a
partial-write retry recoverable, but operators must still confirm scheduler
state before resubmitting.

Calibration failure retains the tuned 0D/3D outputs and records the failure and
non-promotion; it does not mutate the previous seed. Rollback is represented by
disabling the calibration policy and, if needed, selecting the explicit
`legacy_rri_after_first` seed policy. Rollback does not delete reports, rewrite
manifest history, or promote an unvalidated candidate.

## Fetch metadata
- `fetch_attempted`
- `fetch_succeeded`
- `fetch_timestamps[]`
- `retrieved_artifacts[]`

## Failure diagnostics

Use the manifest as the index for recovery. For calibration, compare
`calibration_runs[].input_digests` with the tuned-model and descriptor files,
then follow `dependency_job_id` to the postprocess scheduler record and
`scheduler_job_id` to the calibration record. `promotion_reason` explains
mechanical rejection (digest mismatch, non-terminal status, missing lineage,
or missing candidate). The stage `logs/` directories and upstream QC,
confirmation, replay, target, and summary reports explain scientific or
runtime failures; the agent records those paths but does not reinterpret them.

`svzt status <run-id>` reports the run/iteration branch, while
`svzt calibration-status <run-id> --iteration <n>` reports the calibration
terminal state, dependency/job IDs, digests, and promotion status. A dependent
calibration job left pending after a failed postprocess is expected scheduler
behavior; diagnose the postprocess job rather than submitting an independent
calibration retry.

Fetch transitions to `fetched` when valid from terminal states.
