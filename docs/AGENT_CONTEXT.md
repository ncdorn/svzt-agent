# Agent Context

## Workspace Assumptions
- Workspace root: `~/svz`
- Config source: `~/svz/config/{clusters,patients,defaults}.yaml`
- Local runs root: `~/svz/runs/<run_id>/`
- Remote active patient root (`patient_data_root`) is read-only.
- Remote permanent patient root (`permanent_data_root`) is read-only when configured.
- Remote writes are restricted to configured `runs_root`.
- Canonical patient asset layout is resolved from `defaults.patient_data_layout`.

## Public Ownership
- `svztagent.config` owns typed workspace config loading and validation.
- `svztagent.core` owns plan models, path policy, manifest/state persistence, and lifecycle semantics.
- `svztagent.hpc` owns typed execution adapters and the subprocess boundary.
- `svztagent.workflows` owns workflow-specific orchestration policy.
- `svztagent.cli` owns operator-facing command parsing and output shaping.

## Current Reality
- This repo already supports deterministic plan generation, dry-run/execute workflow submission, lifecycle watch, artifact fetch, status enrichment, and iteration auto-advance.
- The main architectural problem is not missing capability; it is that too much workflow behavior is centralized in `src/svztagent/workflows/tune_trees.py`.
- Future refactors should follow `docs/DECOMPOSITION_PLAN.md` instead of inventing new structure ad hoc.

## Maintenance Rules
Update repo-local roadmap/context docs whenever any of the following materially change:
- public workflow entrypoints or CLI behavior
- manifest or progress-tracker structure
- config merge or override semantics
- workflow module boundaries
- adapter responsibilities
- fetched or evaluation artifact contracts
- autonomy behavior or approval boundaries

## Doc Sync Checklist
After major feature or refactor work, Codex must:
- update `README.md` if operator-facing behavior changed
- update `docs/ROADMAP.md` if planned direction changed
- update `docs/DECOMPOSITION_PLAN.md` if module boundaries or extraction order changed
- update `docs/CONFIG_EVOLUTION.md` if config ownership or precedence changed
- update `docs/TEST_STRATEGY.md` if fixture strategy or test layering changed
- update `docs/AUTONOMY_ROADMAP.md` if autonomy scope or safety boundaries changed
- update root docs when a repo-local change also changes workspace-wide policy
