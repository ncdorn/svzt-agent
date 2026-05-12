# svzt-agent

|        |        |
|--------|--------|
| Package | [![Latest TestPyPI Version](https://img.shields.io/badge/TestPyPI-pending-lightgrey.svg)](https://test.pypi.org/project/svzt-agent/) |
| Source | [GitHub](https://github.com/ncdorn/svzt-agent) |
| Meta | [MIT License](./LICENSE) |

`svzt-agent` is the deterministic orchestration layer for running `svZeroDTrees` workflows on HPC. It owns config resolution, inspectable planning, bounded adapter-mediated execution, monitoring, manifests, and controlled iteration advancement.

## Install

For local development, use Hatch:

```bash
hatch run test:run
```

Once published, install the package with pip:

```bash
pip install svzt-agent
```

For TestPyPI validation:

```bash
pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple svzt-agent
```

## Quickstart

The installed console command remains `svzt`:

```bash
svzt plan tune --cluster sherlock --patient TST-STAN-x --run-id demo-run
svzt run tune --cluster sherlock --patient TST-STAN-x --run-id demo-run --execute
svzt watch demo-run --fetch-on-complete --auto-advance
svzt preop select --run-id demo-run --iteration 3 --reason "best tuned preop"
svzt run postop --run-id demo-run --execute
svzt campaign seed-sweep plan --cluster sherlock --campaign-id tst-stan-5-learned
```

Cluster configs must provide both `executables.svfsiplus_path` and
`executables.svslicer_path` for explicit postop and selected-preop 3D
postprocessing.

For `iteration1_seed.source: generate`, dry-run previews only materialize the seed locally when the required patient assets are mounted locally; otherwise the Sherlock iteration driver generates the seed during `--execute`.

The default learned seed sweep targets TST-STAN-5 and creates three child runs:
two full pulmonary 0D learned-seed cases and one reduced RRI learned-seed case.
Full pulmonary cases stage the learned seed as `inputs/full_pa_zerod.json`; reduced
RRI cases use `inputs/simplified_nonlinear_zerod.json`.
Full pulmonary cases switch to reduced RRI tuning after iteration 1 when the
iteration decision is `not_close`.

The Python import package is `svztagent`:

```python
from svztagent.cli.main import main
```

## Safety Summary

- Patient source data stays read-only.
- Remote writes are restricted to the configured `runs_root`.
- All remote execution flows through typed SSH/rsync/Slurm adapters.
- Plans remain deterministic, inspectable, and validated before execution.
- Manifest state is the source of truth for lifecycle and iteration tracking.

## Documentation

The docs in [`docs/`](./docs/) remain the authoritative operator and architecture references. Key entry points:

- [`docs/OPERATOR_RUNBOOK.md`](./docs/OPERATOR_RUNBOOK.md) — start here for end-to-end pipeline runs
- [`docs/ARCHITECTURE.md`](./docs/ARCHITECTURE.md)
- [`docs/PLANNING.md`](./docs/PLANNING.md)
- [`docs/EXECUTION.md`](./docs/EXECUTION.md)
- [`docs/MONITORING.md`](./docs/MONITORING.md)
- [`docs/MANIFEST.md`](./docs/MANIFEST.md)
- [`docs/HPC_SAFETY.md`](./docs/HPC_SAFETY.md)

## Development

Hatch is the canonical dev workflow because the package requires `pydantic>=2.6` and the wrong global interpreter environment will fail fast.

```bash
hatch run test:run
hatch run build:check
hatch run docs:build
```

To work inside a Hatch environment directly:

```bash
hatch shell test.py3.11
```

## Contributing

See [`CONTRIBUTING.md`](./CONTRIBUTING.md) for contribution guidelines and [`DEVELOPMENT.md`](./DEVELOPMENT.md) for the packaging and environment workflow.

## Copyright

- Copyright © 2026 Nick Dorn.
- Free software distributed under the [MIT License](./LICENSE).
