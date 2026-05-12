# svzt-agent Documentation

`svzt-agent` is the deterministic orchestration layer for `svZeroDTrees` workflows on HPC. This documentation keeps the existing operator and architecture docs intact while adding a package-oriented landing page and API reference.

## Contents

:::{toctree}
:maxdepth: 2
:caption: User and Operator Docs

OPERATOR_RUNBOOK
ARCHITECTURE
PLANNING
EXECUTION
MONITORING
MANIFEST
HPC_SAFETY
PATIENT_DATA_CONTRACT
TESTING
TEST_STRATEGY
ROADMAP
DECOMPOSITION_PLAN
CONFIG_EVOLUTION
OPERATOR_UX_PLAN
AUTONOMY_ROADMAP
AGENT_CONTEXT
API Reference <autoapi/index>
:::

## Package Facts

- Distribution name: `svzt-agent`
- Import package: `svztagent`
- Console command: `svzt`
- Source layout: `src/svztagent/`

## Local Workflows

```bash
hatch run test:run
hatch run build:check
hatch run docs:build
```
