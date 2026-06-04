# AGENTS.md

## How To Use This File
- Treat this file as a navigation layer and rules-of-engagement brief.
- Do not treat this file as the source of truth for workflow behavior, schemas, or CLI semantics.
- Follow the linked docs for detail. If this file conflicts with repo docs, the repo docs win.
- Keep this file short. Push implementation detail into `docs/`, not into `AGENTS.md`.

## Repo Mission
- `svzt-agent` is the deterministic orchestration layer for running `svZeroDTrees` workflows on HPC.
- It owns config resolution, planning, staging, adapter-mediated execution, monitoring, manifests, and bounded iteration/campaign control.
- Its job is to make runs inspectable, reproducible, and safe.
- It is not the place for solver internals, numerical method rewrites, or domain-library redesign unless integration explicitly requires it.
- If a change is fundamentally about tuning math, model construction, or solver behavior, first ask whether it belongs in `svZeroDTrees` or `svZeroDSolver` instead.

**Slide generation integration (future):** The downstream output of completed runs will feed into the HTML slide-deck pipeline in `ppas-slide-generation/`. Postprocess helpers in `src/svztagent/postprocess/` are the intended extraction point for producing `cfd-results-template.json` records that conform to `ppas-slide-generation/cfd-results-data/README.md`. See `../AUTOMATE_REPO_SLIDES.md` for the full pipeline roadmap and stage tracker.

## Architecture Boundaries
- Keep config loading and schema logic separate from workflow orchestration.
- Keep plan generation and plan validation separate from execution.
- Keep remote execution, transfer, and scheduler behavior inside typed adapters.
- Keep workflow orchestration as composition glue, not as the home for every concern.
- Keep monitoring and lifecycle-state logic separate from CLI rendering.
- Keep evaluation and resubmission policy separate from execution side effects.
- Keep artifact summarization deterministic and machine-readable first.
- Keep CLI code focused on argument parsing, command wiring, and presentation.
- Do not collapse these boundaries for convenience. Move code to the owning layer instead.

## Where To Make Changes
- `src/svztagent/config/`: config schemas, loading, merge/override rules, validation.
- `src/svztagent/core/`: manifests, paths, plan models, plan validation, state transitions, monitoring, campaign state and policy.
- `src/svztagent/hpc/`: typed SSH/rsync/Slurm/subprocess adapters only.
- `src/svztagent/workflows/`: workflow-local orchestration glue and bounded workflow policy.
- `src/svztagent/postprocess/`: deterministic artifact parsing, metrics extraction, summarization helpers.
- `src/svztagent/cli/`: argument parsing, command dispatch, human-facing rendering.
- `src/svztagent/templates/`: packaged runtime templates only when a template contract intentionally changes.
- `tests/`: behavior-level regression coverage, fake adapters, deterministic fixtures.

## Refactor Priority Guidance
- `src/svztagent/workflows/tune_trees.py` is a refactor hotspot.
- Do not keep growing that file by default.
- Prefer extracting focused modules, classes, and functions with clear ownership.
- Keep public workflow entrypoints stable while decomposing unless an operator-facing change is intentional.
- Preserve behavior first. Refactor behind tests, not ahead of them.
- Use `docs/DECOMPOSITION_PLAN.md` as the authority for decomposition direction.

## Safety Invariants
- Patient data is read-only.
- All remote writes must remain under configured `runs_root`.
- All execution must go through typed adapters.
- No arbitrary shell passthrough.
- No workflow-level subprocess calls outside approved adapter boundaries.
- Plans must be deterministic, inspectable, and validated before execution.
- Dry-run and plan artifacts must remain meaningful and reviewable.
- Manifests are the source of truth for run lifecycle and iteration state.
- Hidden background autonomy loops are forbidden unless explicitly designed, bounded, tested, and documented.

## Testing Expectations
- Prefer local-only tests.
- Use fake adapters for HPC behavior.
- Standard tests must not require real SSH, Slurm, rsync, or cluster access.
- Update tests when manifests, state transitions, plan validation, monitoring, CLI behavior, or campaign logic changes.
- When refactoring `tune_trees.py`, lock behavior with tests before moving code.
- Preserve or improve coverage of path safety, manifest mutation, scheduler normalization, fetch behavior, and auto-advance policy.

## Required Doc Sync
- This checklist is required, not optional.
- CLI surface changes: update `README.md` and any affected operator-facing docs.
- Manifest schema or lifecycle changes: update `docs/MANIFEST.md`.
- Config schema or precedence changes: update `docs/ARCHITECTURE.md` and the relevant config/planning docs.
- Monitoring, watch, or auto-advance behavior changes: update `docs/MONITORING.md`.
- Workflow decomposition or module ownership changes: update `docs/ARCHITECTURE.md` and `docs/DECOMPOSITION_PLAN.md`.
- Campaign or evaluation artifact contract changes: update `docs/EXECUTION.md`, `docs/MANIFEST.md`, and roadmap/decomposition docs if behavior or expectations changed.
- Safety rule or execution-boundary changes: update `docs/HPC_SAFETY.md` and `docs/PATIENT_DATA_CONTRACT.md` when patient-path assumptions changed.
- Autonomy or agent-authority changes: update `docs/AUTONOMY_ROADMAP.md` and any affected roadmap docs.
- Test invocation or test-policy changes: update `docs/TESTING.md` and `docs/TEST_STRATEGY.md`.

## Anti-Patterns To Avoid
- Putting business logic in `src/svztagent/cli`.
- Putting orchestration logic directly into `src/svztagent/hpc` adapters.
- Putting all new workflow logic into `src/svztagent/workflows/tune_trees.py`.
- Mixing evaluation or decision logic with remote side effects.
- Mutating prior run artifacts in place when manifest/history or new iteration outputs should record the change.
- Adding shell-script sprawl when Python already owns the workflow and safety model.
- Introducing ad hoc dictionaries where typed models already exist or should exist.
- Hiding state outside manifests and validated workflow artifacts.

## Recommended Development Posture
- Start plan-first. Understand the planning/execution boundary before changing behavior.
- Prefer typed models and explicit validation over implicit conventions.
- Fail fast on unsafe paths, invalid state transitions, missing required artifacts, and unsupported commands.
- Prefer append-only history and explicit timestamps over silent mutation.
- Make state transitions explicit and auditable.
- Preserve deterministic dry-run output and inspectable evidence.
- Keep changes small, well-scoped, and reversible.

## Authority Map
- This file is a router, not the authority.
- Architecture and module ownership: `docs/ARCHITECTURE.md`
- Planning and plan validation: `docs/PLANNING.md`
- Execution flow, fetch behavior, and preop BC tuning success evidence: `docs/EXECUTION.md`
- Monitoring, watch, and auto-advance semantics: `docs/MONITORING.md`
- Manifest schema and lifecycle data: `docs/MANIFEST.md`
- HPC execution safety: `docs/HPC_SAFETY.md`
- Testing commands and local test expectations: `docs/TESTING.md`
- Testing philosophy and coverage direction: `docs/TEST_STRATEGY.md`
- Patient data contract and read-only assumptions: `docs/PATIENT_DATA_CONTRACT.md`
- Repo roadmap: `docs/ROADMAP.md`
- Workflow decomposition plan: `docs/DECOMPOSITION_PLAN.md`
- Autonomy boundaries: `docs/AUTONOMY_ROADMAP.md`
