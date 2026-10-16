# Execution Planning

## Why Plans Exist
`svzt-agent` uses an explicit execution planning layer so each workflow declares what it intends to do before any execution phase exists. This provides deterministic behavior, inspectable dry runs, and strict policy checks on remote paths and dependencies.

## Core Models
- `PlanStep`: one deterministic unit of intended work.
  - includes `step_id`, `category`, `description`, `inputs`, `outputs`, `dependencies`, `command_preview`, `local_paths`, `remote_paths`, `safety_notes`, `execution_policy`, and `status`.
- `ExecutionPlan`: complete workflow plan metadata plus ordered steps.
  - includes `plan_id`, `workflow_name`, `run_id`, `cluster`, `patient`, timestamps/paths, `summary`, and `validation_results`.
- Planning models use Pydantic v2 APIs (`field_validator`, `model_validator`, `model_dump`, `model_validate`).

## Validation Rules
`svztagent.core.plan_validate` enforces:

- duplicate `step_id` values fail
- duplicate dependencies within a step fail
- dependency references to missing step IDs fail
- remote write paths must remain under `runs_root`
- plan must include at least one terminal step: `pull_artifacts` or `finalize_manifest`

Validation failures raise `PlanValidationError` with explicit error codes.

## Dry-Run Behavior
Phase 2 is plan-only:

- no `rsync`, `ssh`, or scheduler submission is executed
- steps include `command_preview` only
- plan files are written to:
  - `runs/<run_id>/execution_plan.json`
  - `runs/<run_id>/execution_plan.yaml`

The plan records steps, paths and commands, not tuning parameters. The
resolved impedance configuration of a `run tune --dry-run` (tune space with
free parameters and bounds, `proximal_compliance`, objective) is rendered into
`runs/<run_id>/iterations/iter-NN/run_tune_iter.sh` (`zerod_tuning_config`);
review it there, e.g. to confirm a patient's `stiffness_scale` and xi bounds
before submission (`docs/TUNING_MODEL.md`).

## CLI
Generate a deterministic tune workflow plan:

```bash
svzt plan tune --cluster <name> --patient <alias> [--run-id <id>]
```

The command resolves config/paths, creates or loads the run manifest, validates the plan, writes plan artifacts, and prints a readable dry-run summary.
