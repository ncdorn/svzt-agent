# Development Guide

`svzt-agent` uses Hatch for packaging, test environments, docs builds, and distribution validation. This keeps the repo independent of whatever Python packages happen to be installed globally.

## Why Hatch

The project requires `pydantic>=2.6,<3` at runtime. Running the repo against a mismatched interpreter environment will fail during import, so the supported workflow is:

```bash
hatch run test:run
```

## Common Commands

Run the test suite:

```bash
hatch run test:run
```

Build wheel and sdist and validate the metadata:

```bash
hatch run build:check
```

Build the documentation:

```bash
hatch run docs:build
```

Serve docs locally with live reload:

```bash
hatch run docs:serve
```

Open an interactive environment:

```bash
hatch shell test.py3.11
```

## Package Layout

- Distribution name: `svzt-agent`
- Import package: `svztagent`
- Console command: `svzt`
- Source layout: `src/svztagent/`

## TestPyPI Readiness

Before publishing, verify:

1. `hatch run test:run` passes.
2. `hatch run build:check` produces valid distributions.
3. `hatch run docs:build` succeeds.
4. The built wheel includes the packaged Slurm template and CLI entry point.
