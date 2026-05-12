"""Execution plan domain models and serialization helpers."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
import yaml


class StepCategory(str, Enum):
    RESOLVE_PATHS = "resolve_paths"
    INIT_RUN = "init_run"
    SNAPSHOT_CONFIG = "snapshot_config"
    STAGE_INPUTS = "stage_inputs"
    PUSH_TO_CLUSTER = "push_to_cluster"
    GENERATE_JOB_SCRIPT = "generate_job_script"
    SUBMIT_JOB = "submit_job"
    MONITOR_JOB = "monitor_job"
    PULL_ARTIFACTS = "pull_artifacts"
    POSTPROCESS = "postprocess"
    FINALIZE_MANIFEST = "finalize_manifest"


class StepStatus(str, Enum):
    PENDING = "pending"
    READY = "ready"
    BLOCKED = "blocked"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class ValidationLevel(str, Enum):
    ERROR = "error"
    WARNING = "warning"


class ExecutionPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dry_run_only: bool = True
    allow_execute: bool = False
    retryable: bool = False


class RemotePathSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    read: list[str] = Field(default_factory=list)
    write: list[str] = Field(default_factory=list)


class PlanValidationMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    code: str
    message: str
    step_id: str | None = None
    level: ValidationLevel = ValidationLevel.ERROR


class PlanValidationResults(BaseModel):
    model_config = ConfigDict(extra="forbid")

    is_valid: bool = True
    errors: list[PlanValidationMessage] = Field(default_factory=list)
    warnings: list[PlanValidationMessage] = Field(default_factory=list)


class PlanStep(BaseModel):
    model_config = ConfigDict(extra="forbid")

    step_id: str
    name: str
    category: StepCategory
    description: str
    inputs: dict[str, str] = Field(default_factory=dict)
    outputs: dict[str, str] = Field(default_factory=dict)
    dependencies: list[str] = Field(default_factory=list)
    command_preview: list[str] = Field(default_factory=list)
    local_paths: dict[str, str] = Field(default_factory=dict)
    remote_paths: RemotePathSpec = Field(default_factory=RemotePathSpec)
    safety_notes: list[str] = Field(default_factory=list)
    execution_policy: ExecutionPolicy = Field(default_factory=ExecutionPolicy)
    status: StepStatus = StepStatus.PENDING

    @field_validator("step_id", "name", "description")
    @classmethod
    def _required_non_empty(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("field must be non-empty")
        return cleaned

    @field_validator("dependencies")
    @classmethod
    def _dependency_ids_non_empty(cls, values: list[str]) -> list[str]:
        cleaned: list[str] = []
        for value in values:
            token = value.strip()
            if not token:
                raise ValueError("dependencies cannot contain empty values")
            cleaned.append(token)
        return cleaned

    @model_validator(mode="after")
    def _no_self_dependency(self) -> PlanStep:
        if self.step_id in self.dependencies:
            raise ValueError("dependencies cannot reference step_id itself")
        return self


class ExecutionPlan(BaseModel):
    model_config = ConfigDict(extra="forbid")

    plan_id: str
    workflow_name: str
    run_id: str
    cluster: str
    patient: str
    created_at: str
    manifest_path: str
    local_run_dir: str
    remote_run_dir: str
    steps: list[PlanStep] = Field(default_factory=list)
    summary: dict[str, Any] = Field(default_factory=dict)
    validation_results: PlanValidationResults = Field(default_factory=PlanValidationResults)

    @field_validator(
        "plan_id",
        "workflow_name",
        "run_id",
        "cluster",
        "patient",
        "created_at",
        "manifest_path",
        "local_run_dir",
        "remote_run_dir",
    )
    @classmethod
    def _required_plan_fields_non_empty(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("field must be non-empty")
        return cleaned


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def write_plan_json(plan: ExecutionPlan, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(plan.model_dump_json(indent=2), encoding="utf-8")


def write_plan_yaml(plan: ExecutionPlan, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as stream:
        yaml.safe_dump(plan.model_dump(mode="json"), stream, sort_keys=False)


def load_plan_json(path: Path) -> ExecutionPlan:
    data = path.read_text(encoding="utf-8")
    return ExecutionPlan.model_validate_json(data)


def load_plan_yaml(path: Path) -> ExecutionPlan:
    with path.open("r", encoding="utf-8") as stream:
        data = yaml.safe_load(stream) or {}
    return ExecutionPlan.model_validate(data)
