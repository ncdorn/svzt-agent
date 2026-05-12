# Contributing

Contributions are welcome. This repository is the orchestration layer for `svZeroDTrees` HPC workflows, so correctness, safety boundaries, and reproducibility matter more than feature volume.

## Before You Start

- Read [`AGENTS.md`](./AGENTS.md) for repo-specific rules and navigation.
- Review the authoritative docs in [`docs/`](./docs/) before changing workflow behavior.
- Keep config, orchestration, adapters, monitoring, and CLI concerns in their owning modules.

## Development Workflow

1. Fork `https://github.com/ncdorn/svzt-agent`.
2. Clone your fork and create a branch from `main`.
3. Install [Hatch](https://hatch.pypa.io/latest/install/).
4. Run the relevant checks:

```bash
hatch run test:run
hatch run build:check
hatch run docs:build
```

5. Add or update tests with your change.
6. Update docs when operator-facing behavior, manifest/state semantics, config ownership, or module boundaries change.
7. Commit with a clear message and open a pull request.

## Pull Request Expectations

- Preserve deterministic behavior and safety invariants.
- Prefer local-only tests with fake adapters.
- Do not introduce arbitrary shell passthrough or bypass typed adapters.
- Keep changes scoped; if a refactor touches `src/svztagent/workflows/tune_trees.py`, lock behavior with tests first.
