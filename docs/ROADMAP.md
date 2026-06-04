# svzt-agent Roadmap

## Architecture
- Preserve the current ports-and-adapters split across `svztagent.config`, `svztagent.core`, `svztagent.hpc`, `svztagent.workflows`, and `svztagent.cli`.
- The next major architectural change is workflow decomposition, not a new framework. `tune_trees.py` should become a workflow package with explicit modules for context, planning, staging, rendering, execution, status, fetch, advance, watch, and shared workflow-local types.
- Keep canonical lifecycle/state-machine behavior in `svztagent.core.state`, `svztagent.core.transitions`, and `svztagent.core.manifest`.
- Keep domain computation and scientific tuning/evaluation logic in upstream repos; `svzt-agent` should only stage inputs, invoke bounded contracts, and ingest deterministic artifacts.
- Keep repository discovery and provenance handling layout-agnostic so `svzt-agent` can run from installed packages or sibling checkouts under `ppas-dev/`.

## Operator UX
- Improve `status` so the operator can quickly see lifecycle state, active iteration, child-job state, best-known stage, latest decision, and likely recovery action.
- Keep explicit sibling workflows (`preop select`, `run postop`, `run adapt`) inspectable and rerunnable per stage/model rather than hiding adaptation behind postop submission.
- Improve `watch` summaries so terminal output clearly distinguishes scheduler failure, workflow review pause, convergence, and max-iteration exhaustion.
- Make dry-run output easier to read by separating plan summary, path safety summary, command previews, and expected artifact contract.
- Keep CLI changes minimal and high-value; favor clearer output and a few explicit subcommands over adding many new flags.

## Config Evolution
- Keep YAML as the top-level workspace configuration format, but document clearer ownership boundaries between cluster definitions, patient metadata, tuning defaults, and run-time overrides.
- Reduce config sprawl by reserving workspace root config for stable operator-managed settings and keeping ephemeral per-run state in manifests or staged artifacts.
- Make override precedence explicit and stable: cluster/patient/default config should be merged deterministically before workflow execution begins.
- If schema versioning is introduced later, migrations should be explicit and validated rather than inferred by ad hoc fallback code.

## Workflow Decomposition
- Split current workflow responsibilities into smaller modules without changing the public entrypoints consumed by `svztagent.cli.main`.
- Distinguish workflow policy from workflow plumbing:
  - planning and validation
  - local staging
  - script rendering
  - execution/submission
  - monitoring and status enrichment
  - fetch and artifact validation
  - iteration advancement and campaign control
- Keep orchestration glue thin: workflow entrypoints should compose helpers instead of containing most logic directly.

## Evaluation And Postprocess
- Expand beyond current placeholder summarization by defining deterministic contracts for metrics, decisions, warnings, and comparison outputs.
- Ensure evaluation artifacts are machine-readable first and human-readable second.
- Normalize fetched artifact expectations so status and watch flows do not need to guess which evidence files should exist.

## Sweeps And Comparisons
- Add sweep/campaign support only after single-run artifact organization and evaluation contracts are stable.
- Campaign comparison should consume normalized manifests and per-iteration evaluation artifacts rather than bespoke directory inspection.
- Seed-sweep campaign comparison now uses child run manifests plus normalized campaign summary CSV/JSON; future campaign types should follow the same artifact contract.
- Parameter sweep support should remain deterministic and bounded by explicit budgets, stop conditions, and validation rules.

## Hardening And Recovery
- Strengthen recovery semantics for paused review, scheduler failure, partial fetch, and resume-after-interruption cases.
- Make operator-facing failure guidance explicit: what failed, what evidence was fetched, what is safe to retry, and what requires human review.
- Keep fake-based tests comprehensive while adding selective higher-fidelity checks for the most failure-prone orchestration paths.

## Autonomy Phases
- Phase 0: deterministic evidence and artifact hardening.
- Phase 1: read-only LLM assistance for summaries and recommendations.
- Phase 2: bounded planning assistance for campaigns and sweeps.
- Phase 3: guarded execution assistance inside deterministic policy envelopes.
- Phase 4: campaign-level autonomy only after rollback, approval, comparison, and evidence contracts are stable.
