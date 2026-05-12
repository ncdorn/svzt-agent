# Autonomy Roadmap

## What Autonomy Means Here
- In this repo, autonomy should mean bounded assistance around planning, summarization, prioritization, and campaign management.
- It should not mean unrestricted command generation, path selection, scheduler control outside policy, or bypassing manifests and deterministic plans.
- The deterministic workflow engine remains the source of truth for all remote actions.

## Deterministic vs LLM-Assisted Boundaries
- Deterministic engine responsibilities:
  - config loading and validation
  - path-policy enforcement
  - plan generation
  - adapter-mediated execution
  - scheduler polling
  - manifest mutation
  - artifact validation
  - stop conditions and safety gates
- Safe LLM-assisted responsibilities:
  - summarize run status and likely next actions
  - explain failure evidence
  - compare iterations or campaigns from normalized artifacts
  - propose bounded future plans that deterministic code validates before use

## Phase 0
- Harden deterministic evidence flow first.
- Normalize decision, metric, and evaluation artifacts.
- Improve status readability and artifact discoverability.
- No new autonomy beyond basic read-only explanation.

## Phase 1
- Add read-only run and campaign summaries.
- Allow the model to explain failure modes, compare recent iterations, and recommend next operator actions.
- No mutation authority and no path or command generation.

## Phase 2
- Add bounded planning assistance for campaigns and sweeps.
- The model may propose candidate runs, parameter sweeps, or review queues, but deterministic validation must approve schemas, budgets, paths, and stop conditions.
- Human review remains the default before execution.

## Phase 3
- Add guarded execution assistance only inside explicit policy envelopes.
- Auto-advance remains deterministic.
- LLM assistance may prioritize which paused or failed runs deserve attention next, but it should not directly issue unbounded remediation actions.

## Phase 4
- Consider campaign-level autonomy only after:
  - artifact contracts are stable
  - comparison and resume/recovery semantics are mature
  - rollback and approval hooks exist
  - operators can audit why a choice was made from stored evidence

## Forbidden Patterns
- Freeform shell generation
- Direct remote path selection
- Writing outside configured `runs_root`
- Changing or mutating patient source data
- Bypassing manifests, plans, or validation rules
- Allowing the model to redefine safety policy or stop conditions at runtime

## Preconditions
- Stable workflow decomposition
- Stable config precedence
- Stable artifact organization
- Stable run/campaign summaries
- Clear operator override and approval pathways
