# Testing

## Scope
The initial test suite validates:
- config loading and strict validation behavior,
- patient alias resolution and path policy checks,
- canonical patient asset path derivation from Oak layout defaults,
- manifest roundtrip correctness,
- deterministic dry-run plan generation,
- command allowlist enforcement,
- rsync/ssh/slurm adapter command construction,
- dry-run and fake-execute tune workflow scaffolding,
- scheduler status normalization and artifact fetch behavior.

## Run tests
```bash
hatch run test:run
```

Hatch is the canonical test runner because the package requires `pydantic>=2.6,<3` and importing under an arbitrary global interpreter can fail before the suite starts.

The importable package is in `src/`. The test suite still adds `src/` to `sys.path` in `tests/conftest.py` so local test execution remains straightforward inside the Hatch environment.

## Test design
Tests use local temporary directories and fake adapters. No SSH, rsync, Slurm, or HPC connectivity is required.
