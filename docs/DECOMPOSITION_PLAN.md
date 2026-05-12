# Decomposition Plan

## Current Responsibilities
`src/svztagent/workflows/tune_trees.py` currently owns:
- workflow-local result and status dataclasses
- run and iteration context resolution
- iteration path/layout calculation
- execution-plan construction
- local staging and seed/inflow handling
- Slurm script rendering
- default adapter construction
- run submission flow
- status polling and progress artifact enrichment
- watch and auto-advance control flow
- fetch orchestration and expected-artifact validation
- iteration advancement policy

This file is too central to remain the long-term implementation unit.

## Proposed Package Layout
- `src/svztagent/workflows/tune_trees/types.py`
  - workflow-local result objects such as execution, status, fetch, and auto-advance return types
- `src/svztagent/workflows/tune_trees/context.py`
  - run ID generation, workspace/run loading, iteration resolution, remote layout helpers
- `src/svztagent/workflows/tune_trees/planning.py`
  - plan-step construction, `plan_tune_trees`, plan loading/validation helpers
- `src/svztagent/workflows/tune_trees/staging.py`
  - seed resolution, local staging, inflow staging, iteration input payload generation
- `src/svztagent/workflows/tune_trees/render.py`
  - template lookup and job-script rendering
- `src/svztagent/workflows/tune_trees/execution.py`
  - adapter construction, remote directory creation, sync/push, submit, submission-time manifest updates
- `src/svztagent/workflows/tune_trees/status.py`
  - progress artifact loading, child-job polling, stage derivation, `query_run_status`
- `src/svztagent/workflows/tune_trees/fetch.py`
  - artifact pull orchestration and expected-artifact validation
- `src/svztagent/workflows/tune_trees/advance.py`
  - iteration decision ingestion and advancement policy
- `src/svztagent/workflows/tune_trees/watch.py`
  - lifecycle watch, auto-advance loop, and watch result assembly

## What Stays As Orchestration Glue
- Public workflow entrypoints should remain stable and easy for the CLI to import.
- Entry modules should compose helper modules and own only top-level workflow sequencing.
- Shared workflow policy should stay in the workflow package, but canonical lifecycle/state semantics must remain in `svztagent.core`.

## Extraction Sequence
- Stage 1: move workflow-local dataclasses and small pure helpers into `types.py` and `context.py`.
- Stage 2: move planning code into `planning.py` while preserving `plan_tune_trees` behavior and plan artifact formats.
- Stage 3: move staging and render logic into `staging.py` and `render.py`.
- Stage 4: move execution flow and adapter construction into `execution.py`.
- Stage 5: move status, fetch, advance, and watch flows into dedicated modules, preserving existing public function signatures.
- Stage 6: leave a thin package-level re-export layer so `svztagent.cli.main` does not need broad changes during the refactor.

## Stable Public API
During the refactor, preserve these public workflow entrypoints:
- `plan_tune_trees`
- `run_tune_trees`
- `query_run_status`
- `watch_run_lifecycle`
- `watch_and_auto_advance_tuning`
- `fetch_run_artifacts`
- `advance_tune_iteration`
- `render_plan_human`

Any signature changes should be treated as operator-facing changes and must trigger doc sync updates.

## Test Migration
- Keep existing behavior-level tests green while modules move.
- Add module-focused tests only after extraction is complete enough to reduce mocking overhead rather than increase it.
- Prefer scenario-based fixtures around planning, staging, watch, and fetch flows over testing internal helper call graphs.
- Ensure the refactor does not weaken coverage of path-policy enforcement, manifest mutation, scheduler normalization, or auto-advance outcomes.
