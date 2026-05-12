# Config Evolution

## Current Pain Points
- The typed schema is already stronger than the current documentation, which makes the config feel more ad hoc than it really is.
- Workspace defaults currently mix stable infrastructure defaults, workflow defaults, and operator-tunable runtime behavior in one broad `defaults` tree.
- The current docs do not clearly explain which settings are meant to be stable workspace policy versus patient-specific overrides versus per-run generated state.
- Placeholder root templates and script registry files can create the impression that behavior is template-driven even when the real source of truth is Python orchestration code.

## Ownership Split
- Root workspace config should own stable operator-managed inputs:
  - clusters and remote roots
  - patient aliases and read-only source paths
  - stable default tuning and execution parameters
  - patient data layout conventions
- Repo-local code should own merge logic, validation, and derivation of effective runtime config.
- Per-run generated values belong in manifests and staged artifacts, not back in root config.

## Override Hierarchy
- Recommended precedence remains:
  - schema defaults in code
  - workspace `defaults.yaml`
  - patient-level overrides in `patients.yaml`
  - explicit CLI/runtime arguments where supported
  - generated run-time state in manifest or staged iteration artifacts
- Document override semantics explicitly, including where overrides patch fields and where they replace whole subtrees such as tune-space definitions.
- Avoid introducing environment-variable precedence except for workspace-root detection and explicitly documented operational needs.

## Normalization Targets
- Treat cluster connection/executable details as infrastructure config.
- Treat patient alias, read-only roots, and mesh/asset layout as patient metadata plus layout policy.
- Treat tuning defaults as reusable workflow defaults rather than patient identity.
- Treat iteration decisions, fetched artifact lists, and derived runtime paths as run state, not config.

## Validation Improvements
- Keep Pydantic validation strict and deterministic.
- Prefer validation that catches category errors early:
  - illegal absolute/relative path usage
  - invalid override ranges
  - missing or contradictory layout settings
  - unsupported precedence combinations
- Where fallback behavior exists, document it explicitly instead of relying on implicit behavior discovered by reading the workflow code.

## Migration Strategy
- No immediate schema migration is required for this documentation pass.
- If future changes materially reshape config, add explicit schema versioning or migration helpers rather than silently accepting multiple shapes forever.
- Migration work should preserve operator trust: config changes should be reviewable, validated, and documented in the same change set.
