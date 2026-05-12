"""Human-readable rendering for execution plans."""

from __future__ import annotations

import shlex

from svztagent.core.plan import ExecutionPlan, PlanStep


def _render_step(step: PlanStep, index: int) -> list[str]:
    lines: list[str] = []
    lines.append(f"{index:02d}. {step.step_id} [{step.category.value}] status={step.status.value}")
    lines.append(f"    name: {step.name}")
    lines.append(f"    description: {step.description}")
    lines.append("    dependencies: " + (", ".join(step.dependencies) if step.dependencies else "<none>"))

    if step.command_preview:
        preview = shlex.join(step.command_preview)
        lines.append(f"    command_preview: {preview}")

    if step.remote_paths.read:
        lines.append(f"    remote_read: {', '.join(step.remote_paths.read)}")
    if step.remote_paths.write:
        lines.append(f"    remote_write: {', '.join(step.remote_paths.write)}")
    if step.safety_notes:
        lines.append(f"    safety_notes: {'; '.join(step.safety_notes)}")

    return lines


def render_execution_plan(plan: ExecutionPlan) -> str:
    lines: list[str] = []
    lines.append(f"Plan ID: {plan.plan_id}")
    lines.append(f"Workflow: {plan.workflow_name}")
    lines.append(f"Run ID: {plan.run_id}")
    lines.append(f"Cluster: {plan.cluster}")
    lines.append(f"Patient: {plan.patient}")
    lines.append(f"Created: {plan.created_at}")
    lines.append(f"Manifest: {plan.manifest_path}")
    lines.append(f"Local run dir: {plan.local_run_dir}")
    lines.append(f"Remote run dir: {plan.remote_run_dir}")
    lines.append("")
    lines.append("Steps:")
    for index, step in enumerate(plan.steps, start=1):
        lines.extend(_render_step(step, index))
    lines.append("")
    lines.append(
        "Validation: "
        f"{'PASS' if plan.validation_results.is_valid else 'FAIL'} "
        f"(errors={len(plan.validation_results.errors)}, warnings={len(plan.validation_results.warnings)})"
    )
    for error in plan.validation_results.errors:
        prefix = f"{error.step_id}: " if error.step_id else ""
        lines.append(f"  - ERROR [{error.code}] {prefix}{error.message}")
    for warning in plan.validation_results.warnings:
        prefix = f"{warning.step_id}: " if warning.step_id else ""
        lines.append(f"  - WARNING [{warning.code}] {prefix}{warning.message}")

    return "\n".join(lines)
