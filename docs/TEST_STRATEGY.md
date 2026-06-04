# Test Strategy

## Current Strengths
- The repo already has strong local-only coverage for config loading, path policy, plan generation, adapter construction, manifest persistence, status normalization, fetch behavior, and auto-advance/watch flows.
- Fake adapters and temporary workspaces make the suite fast, deterministic, and independent of live HPC access.
- Current tests already exercise operator-visible workflow scenarios instead of only isolated unit helpers.

## Current Gaps
- The workflow monolith makes it harder to test responsibilities at the right level without broad scenario setup.
- Fixture organization is still mostly implicit inside `conftest.py` and ad hoc test-local setup rather than a named fixture taxonomy.
- Template and artifact contracts are tested indirectly; that is useful, but it leaves room for drift if rendering or fetch conventions change in multiple places at once.
- There is no clear policy yet for optional higher-fidelity tests that validate integration assumptions without becoming mandatory for normal local development.

## Fixture Taxonomy
- Move toward explicit fixture classes:
  - config fixtures
  - manifest/iteration fixtures
  - rendered template fixtures
  - fetched artifact fixtures
  - status/watch progression fixtures
- Put long-lived deterministic fixture data under `tests/fixtures/` instead of relying only on inline generated temporary content.
- Keep fixtures small and scenario-named so they explain intent rather than mirroring arbitrary run IDs.

## Fake Adapter Boundaries
- Continue to keep SSH/rsync/Slurm behavior fake by default.
- Fake adapters should remain the default way to test submission, monitoring, and fetch orchestration logic.
- Use fake adapters to validate command construction, allowed operations, and manifest mutations, not to emulate every behavior of the real cluster.

## Integration And Smoke Tests
- Keep the default suite fake-only and fast.
- Add optional smoke/integration layers only where they pay for themselves:
  - packaged resource checks for runtime templates
  - template rendering contract checks
  - manifest roundtrip compatibility checks
  - selected filesystem-heavy staging/fetch flows
- Any optional higher-fidelity test layer should be clearly marked and should not be required for routine local edits.

## Environment Policy
- Treat Hatch-managed environments as the supported way to run tests, docs, and build validation.
- Avoid assuming the system interpreter has the correct runtime dependencies installed.

## Regression Gates
- Refactors to workflow decomposition must preserve behavior-level coverage first.
- Changes to manifest schema, CLI output semantics, fetch contracts, or auto-advance policy should add or update scenario tests in the same change.
- Autonomy-related features must be tested with deterministic fixtures and fake adapters before any optional live validation is considered.
