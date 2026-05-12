# Operator UX Plan

## Command Surface
- Keep the command set compact: `init-run`, `plan tune`, `run tune`,
  `run tune-iter`, `preop select`, `run postop`, `status`, `watch`, `fetch`,
  `advance-iter`, `update-progress`.
- Improve defaults and output before adding more commands.
- Reserve new commands for genuinely distinct operator tasks such as campaign comparison or curated artifact summaries, not for minor formatting variations.

## Status And Watch Output
- `status` should present a compact summary first:
  - lifecycle state
  - active iteration
  - tracker status
  - stage label
  - last decision or review reason
  - key job IDs and failure log location when relevant
- `watch` should print terminal summaries that clearly differentiate completed, failed, cancelled, converged, paused-review, and max-iteration outcomes.
- Where local evidence is missing and remote pull is attempted, report that source explicitly so operators understand what was inferred versus fetched.

## Dry-Run Clarity
- Dry-run output should clearly separate:
  - resolved workspace/cluster/patient context
  - path safety summary
  - plan validation result
  - command previews
  - expected artifact contract
- Dry-run mode should continue to be the default for workflow execution.
- Operator output should make it obvious which steps are deterministic previews and which require `--execute`.

## Failure Messaging
- Failures should tell the operator:
  - what failed
  - whether it failed during validation, submission, watch, fetch, or decision handling
  - which artifact or log path is most relevant next
  - whether retry is safe or human review is required
- Avoid generic exception dumps in normal operator flows when a structured message can point to the next action safely.

## Campaign Iteration UX
- Iteration progress should be visible as a first-class concept in `status` and `watch`, not buried in manifest inspection.
- `watch --auto-advance` should explicitly show which iteration ended, what decision was read, and why the next action was submit, stop, or pause-for-review.
- When campaign/sweep support is added later, keep per-run visibility strong so operators can still reason about individual runs without learning a new abstraction first.

## Guardrails
- Operator UX should make safety visible, not implicit. Show writable roots, plan validation results, and dry-run/execute mode clearly.
- Do not add shortcuts that obscure whether the tool is mutating remote state.
- Error messages should preserve the rule that patient data is read-only and remote writes stay under `runs_root`.
