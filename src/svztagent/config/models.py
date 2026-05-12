"""Typed configuration schemas for workspace YAML files."""

from __future__ import annotations

from pathlib import PurePosixPath
from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator


class SchedulerConfig(BaseModel):
    type: Literal["slurm", "pbs", "other"]


class RemoteRoots(BaseModel):
    patient_data_root: str
    permanent_data_root: str | None = None
    runs_root: str

    @field_validator("patient_data_root", "permanent_data_root", "runs_root")
    @classmethod
    def _must_be_absolute(cls, value: str | None) -> str | None:
        if value is None:
            return value
        if not value.startswith("/"):
            raise ValueError("path must be absolute")
        return value


class ClusterExecutables(BaseModel):
    svfsiplus_path: str
    svslicer_path: str | None = None

    @field_validator("svfsiplus_path", "svslicer_path")
    @classmethod
    def _must_be_absolute(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if not value.startswith("/"):
            raise ValueError("path must be absolute")
        return value


class ClusterConfig(BaseModel):
    name: str
    host: str
    user: str
    scheduler: SchedulerConfig
    remote_roots: RemoteRoots
    executables: ClusterExecutables
    notes: str | None = None


class PatientConfig(BaseModel):
    alias: str
    remote_path: str
    permanent_remote_path: str | None = None
    data_policy: Literal["read_only", "mutable"] = "read_only"
    mesh_scale_factor: float | None = None
    tuning: "PatientTuningOverrides | None" = None
    notes: str | None = None

    @field_validator("remote_path", "permanent_remote_path")
    @classmethod
    def _patient_path_absolute(cls, value: str | None) -> str | None:
        if value is None:
            return value
        if not value.startswith("/"):
            raise ValueError("patient path must be absolute")
        return value

    @field_validator("mesh_scale_factor")
    @classmethod
    def _patient_mesh_scale_positive(cls, value: float | None) -> float | None:
        if value is None:
            return value
        if value <= 0.0:
            raise ValueError("mesh_scale_factor must be > 0")
        return value


class RsyncDefaults(BaseModel):
    include_patterns: list[str] = Field(default_factory=list)
    exclude_patterns: list[str] = Field(default_factory=list)


class ArtifactDefaults(BaseModel):
    pull: list[str] = Field(default_factory=list)


class SchedulerDefaults(BaseModel):
    account: str | None = None
    partition: str = "<partition>"
    wall_time: str = "<HH:MM:SS>"
    mem: str = "<memory>"
    cpus: str = "<count>"

    @field_validator("account", mode="before")
    @classmethod
    def _normalize_account(cls, value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = str(value).strip()
        if cleaned in {"", "<account>", "none", "None"}:
            return None
        return cleaned


class ExecutionDefaults(BaseModel):
    env_activation_hooks: list[str] = Field(default_factory=list)
    python_executable: str = "python3"

    @field_validator("python_executable")
    @classmethod
    def _python_executable_nonempty(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("python_executable cannot be empty")
        return cleaned


class ValidationDefaults(BaseModel):
    require_dry_run_before_execute: bool = True
    enforce_remote_write_root: bool = True


class MonitoringDefaults(BaseModel):
    poll_interval_seconds: int = 30
    fetch_on_failure: bool = False

    @field_validator("poll_interval_seconds")
    @classmethod
    def _poll_interval_minimum(cls, value: int) -> int:
        if value < 5:
            raise ValueError("poll_interval_seconds must be >= 5")
        return value


class Iteration1SeedConfig(BaseModel):
    source: Literal["path", "generate"] = "path"
    path: str = "simplified_nonlinear_zerod.json"

    @field_validator("path")
    @classmethod
    def _validate_seed_path(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("iteration-1 seed path cannot be empty")
        normalized = PurePosixPath(cleaned)
        if ".." in normalized.parts:
            raise ValueError("iteration-1 seed path cannot contain '..'")
        return str(normalized)


class TissueSupportConfig(BaseModel):
    enabled: bool = True
    type: Literal["uniform", "spatial"] = "uniform"
    stiffness: float | None = 1000.0
    damping: float | None = 10000.0
    apply_along_normal_direction: bool = True
    spatial_values_file_path: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _spatial_defaults(cls, data):
        if isinstance(data, dict) and str(data.get("type", "")).lower() == "spatial":
            data = dict(data)
            data.setdefault("stiffness", None)
            data.setdefault("damping", None)
        return data

    @field_validator("stiffness", "damping")
    @classmethod
    def _nonnegative_scalar(cls, value: float | None) -> float | None:
        if value is not None and value < 0.0:
            raise ValueError("tissue_support stiffness and damping must be non-negative")
        return value

    @field_validator("spatial_values_file_path")
    @classmethod
    def _validate_spatial_path(cls, value: str | None) -> str | None:
        if value is None:
            return value
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("tissue_support spatial_values_file_path cannot be empty")
        normalized = PurePosixPath(cleaned)
        if ".." in normalized.parts:
            raise ValueError("tissue_support spatial_values_file_path cannot contain '..'")
        return str(normalized)

    @model_validator(mode="after")
    def _validate_shape(self) -> "TissueSupportConfig":
        if not self.enabled:
            return self
        if self.type == "uniform":
            if self.stiffness is None or self.damping is None:
                raise ValueError("uniform tissue_support requires stiffness and damping")
            if self.spatial_values_file_path is not None:
                raise ValueError("uniform tissue_support forbids spatial_values_file_path")
        else:
            if not self.spatial_values_file_path:
                raise ValueError("spatial tissue_support requires spatial_values_file_path")
            if self.stiffness is not None or self.damping is not None:
                raise ValueError("spatial tissue_support forbids stiffness and damping")
        return self


class ThreedTuningConfig(BaseModel):
    wall_model: Literal["rigid", "deformable"] = "deformable"
    inflow_boundary_condition: Literal["neumann", "dirichlet"] = "neumann"
    elasticity_modulus: float = 5062674.563165
    poisson_ratio: float = 0.5
    shell_thickness: float = 0.12
    prestress_file: str | None = "auto"
    tissue_support: TissueSupportConfig | None = Field(default_factory=TissueSupportConfig)
    n_tsteps: int = 4000
    dt: float = 0.0005
    nodes: int = 3
    procs_per_node: int = 24
    memory: int = 16
    hours: int = 20
    wait_poll_seconds: int = 30
    wait_timeout_seconds: int = 43200

    @field_validator(
        "elasticity_modulus",
        "shell_thickness",
        "dt",
        mode="after",
    )
    @classmethod
    def _must_be_positive_float(cls, value: float) -> float:
        if value <= 0.0:
            raise ValueError("value must be > 0")
        return value

    @field_validator(
        "n_tsteps",
        "nodes",
        "procs_per_node",
        "memory",
        "hours",
        mode="after",
    )
    @classmethod
    def _must_be_positive_int(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("value must be > 0")
        return value

    @field_validator("wait_poll_seconds")
    @classmethod
    def _wait_poll_minimum(cls, value: int) -> int:
        if value < 5:
            raise ValueError("wait_poll_seconds must be >= 5")
        return value

    @field_validator("wait_timeout_seconds")
    @classmethod
    def _wait_timeout_positive(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("wait_timeout_seconds must be > 0")
        return value

    @field_validator("poisson_ratio")
    @classmethod
    def _poisson_bounds(cls, value: float) -> float:
        if not (-1.0 < value <= 0.5):
            raise ValueError("poisson_ratio must satisfy -1.0 < v <= 0.5")
        return value

    @model_validator(mode="after")
    def _tissue_support_requires_deformable(self) -> "ThreedTuningConfig":
        if (
            self.tissue_support is not None
            and self.tissue_support.enabled
            and self.wall_model != "deformable"
        ):
            raise ValueError("tissue_support is only valid with wall_model=deformable")
        return self


class PatientThreedOverrides(BaseModel):
    wall_model: Literal["rigid", "deformable"] | None = None
    inflow_boundary_condition: Literal["neumann", "dirichlet"] | None = None
    elasticity_modulus: float | None = None
    poisson_ratio: float | None = None
    shell_thickness: float | None = None
    prestress_file: str | None = None
    tissue_support: TissueSupportConfig | None = None
    n_tsteps: int | None = None
    dt: float | None = None
    nodes: int | None = None
    procs_per_node: int | None = None
    memory: int | None = None
    hours: int | None = None
    wait_poll_seconds: int | None = None
    wait_timeout_seconds: int | None = None


class FreeParamConfig(BaseModel):
    name: str
    init: float
    lb: float | str
    ub: float | str
    to_native: Literal["identity", "positive", "unit_interval"] = "identity"
    from_native: Literal["identity", "log", "logit"] = "identity"

    @staticmethod
    def _normalize_bound(value: float | str) -> float | str:
        if isinstance(value, (int, float)):
            return float(value)
        token = str(value).strip().lower()
        if token in {"inf", "+inf"}:
            return "inf"
        if token == "-inf":
            return "-inf"
        raise ValueError("bound strings must be one of inf, +inf, -inf")

    @staticmethod
    def _bound_to_float(value: float | str) -> float:
        if isinstance(value, str):
            if value == "inf":
                return float("inf")
            if value == "-inf":
                return float("-inf")
            raise ValueError(f"unsupported bound token: {value}")
        return float(value)

    @field_validator("name")
    @classmethod
    def _name_nonempty(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("name cannot be empty")
        return cleaned

    @field_validator("lb", "ub", mode="before")
    @classmethod
    def _validate_bound(cls, value: float | str) -> float | str:
        return cls._normalize_bound(value)

    @model_validator(mode="after")
    def _validate_bounds_order(self) -> "FreeParamConfig":
        lb_val = self._bound_to_float(self.lb)
        ub_val = self._bound_to_float(self.ub)
        if lb_val >= ub_val:
            raise ValueError("free parameter bounds must satisfy lb < ub")
        return self


class FixedParamConfig(BaseModel):
    name: str
    value: float

    @field_validator("name")
    @classmethod
    def _name_nonempty(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("name cannot be empty")
        return cleaned


class TiedParamConfig(BaseModel):
    name: str
    other: str
    fn: Literal["identity"] = "identity"

    @field_validator("name", "other")
    @classmethod
    def _name_nonempty(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("name cannot be empty")
        return cleaned


class TuneSpaceConfig(BaseModel):
    free: list[FreeParamConfig]
    fixed: list[FixedParamConfig] = Field(default_factory=list)
    tied: list[TiedParamConfig] = Field(default_factory=list)

    @staticmethod
    def _ensure_unique_names(params: list[BaseModel], *, key: str, label: str) -> None:
        names = [str(getattr(item, key)) for item in params]
        duplicates = sorted({name for name in names if names.count(name) > 1})
        if duplicates:
            raise ValueError(f"{label} contains duplicate names: {', '.join(duplicates)}")

    @field_validator("free")
    @classmethod
    def _nonempty_free(cls, value: list[FreeParamConfig]) -> list[FreeParamConfig]:
        if not value:
            raise ValueError("tune_space.free cannot be empty")
        return value

    @model_validator(mode="after")
    def _validate_uniqueness(self) -> "TuneSpaceConfig":
        self._ensure_unique_names(self.free, key="name", label="tune_space.free")
        self._ensure_unique_names(self.fixed, key="name", label="tune_space.fixed")
        self._ensure_unique_names(self.tied, key="name", label="tune_space.tied")
        return self


def _default_impedance_tune_space() -> TuneSpaceConfig:
    return TuneSpaceConfig.model_validate(
        {
            "free": [
                {"name": "lpa.xi", "init": 2.3, "lb": 0.0, "ub": 6.0},
                {"name": "lpa.eta_sym", "init": 0.6, "lb": 0.3, "ub": 0.9},
                {"name": "rpa.xi", "init": 2.3, "lb": 0.0, "ub": 6.0},
                {"name": "rpa.eta_sym", "init": 0.7, "lb": 0.3, "ub": 0.9},
                {"name": "lpa.inductance", "init": 1.0, "lb": 0.0, "ub": "inf"},
                {"name": "rpa.inductance", "init": 1.0, "lb": 0.0, "ub": "inf"},
                {"name": "comp.lpa.k2", "init": -75.0, "lb": -100.0, "ub": -1.0},
            ],
            "fixed": [
                {"name": "lrr", "value": 10.0},
                {"name": "d_min", "value": 0.01},
            ],
            "tied": [
                {"name": "comp.rpa.k2", "other": "comp.lpa.k2", "fn": "identity"},
            ],
        }
    )


class ImpedanceTuningConfig(BaseModel):
    solver: str = "Nelder-Mead"
    nm_iter: int = 5
    n_procs: int = 24
    grid_search_init: bool = True
    d_min: float = 0.01
    use_mean: bool = True
    specify_diameter: bool = True
    rescale_inflow: bool = True
    convert_to_cm: bool = False
    compliance_model: Literal["constant", "olufsen"] = "olufsen"
    diameter_scale: float = 0.0
    diameter_std_cap: float | None = None
    allow_ordered_outlet_mapping: bool = False
    tuning_model: Literal["rri", "full_pa"] = "rri"
    tune_space: TuneSpaceConfig = Field(default_factory=_default_impedance_tune_space)

    @field_validator("solver")
    @classmethod
    def _solver_nonempty(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("solver cannot be empty")
        return cleaned

    @field_validator("nm_iter", "n_procs")
    @classmethod
    def _positive_int_fields(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("value must be > 0")
        return value

    @field_validator("d_min")
    @classmethod
    def _positive_d_min(cls, value: float) -> float:
        if value <= 0.0:
            raise ValueError("d_min must be > 0")
        return value

    @field_validator("diameter_scale")
    @classmethod
    def _nonnegative_diameter_scale(cls, value: float) -> float:
        if value < 0.0:
            raise ValueError("diameter_scale must be >= 0")
        return value

    @field_validator("diameter_std_cap")
    @classmethod
    def _nonnegative_diameter_std_cap(cls, value: float | None) -> float | None:
        if value is not None and value < 0.0:
            raise ValueError("diameter_std_cap must be >= 0")
        return value


class PatientImpedanceOverrides(BaseModel):
    solver: str | None = None
    nm_iter: int | None = None
    n_procs: int | None = None
    grid_search_init: bool | None = None
    d_min: float | None = None
    use_mean: bool | None = None
    specify_diameter: bool | None = None
    rescale_inflow: bool | None = None
    convert_to_cm: bool | None = None
    compliance_model: Literal["constant", "olufsen"] | None = None
    diameter_scale: float | None = None
    diameter_std_cap: float | None = None
    allow_ordered_outlet_mapping: bool | None = None
    tuning_model: Literal["rri", "full_pa"] | None = None
    tune_space: TuneSpaceConfig | None = None


class TuningDefaults(BaseModel):
    iteration1_seed: Iteration1SeedConfig = Field(default_factory=Iteration1SeedConfig)
    threed: ThreedTuningConfig = Field(default_factory=ThreedTuningConfig)
    impedance: ImpedanceTuningConfig = Field(default_factory=ImpedanceTuningConfig)


class PatientTuningOverrides(BaseModel):
    iteration1_seed: Iteration1SeedConfig | None = None
    threed: PatientThreedOverrides | None = None
    impedance: PatientImpedanceOverrides | None = None


class PatientDataLayoutDefaults(BaseModel):
    clinical_targets_csv: str = "clinical_targets.csv"
    centerlines_vtp: str = "centerlines.vtp"
    inflow_csv: str = "inflow.csv"
    preop_mesh_complete_dir: str = "preop-mesh-complete"
    postop_mesh_complete_dir: str | None = None
    mesh_surfaces_subdir: str = "mesh-surfaces"

    @field_validator(
        "clinical_targets_csv",
        "centerlines_vtp",
        "inflow_csv",
        "preop_mesh_complete_dir",
        "postop_mesh_complete_dir",
        "mesh_surfaces_subdir",
    )
    @classmethod
    def _must_be_relative_posix(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if value.startswith("/"):
            raise ValueError("patient data layout paths must be relative")
        normalized = PurePosixPath(value)
        if ".." in normalized.parts:
            raise ValueError("patient data layout paths cannot contain '..'")
        cleaned = str(normalized)
        if cleaned.startswith("./"):
            cleaned = cleaned[2:]
        if cleaned in {"", "."}:
            raise ValueError("patient data layout path cannot be empty")
        return cleaned


class DefaultsConfig(BaseModel):
    rsync: RsyncDefaults = Field(default_factory=RsyncDefaults)
    artifacts: ArtifactDefaults = Field(default_factory=ArtifactDefaults)
    scheduler: SchedulerDefaults = Field(default_factory=SchedulerDefaults)
    execution: ExecutionDefaults = Field(default_factory=ExecutionDefaults)
    validation: ValidationDefaults = Field(default_factory=ValidationDefaults)
    monitoring: MonitoringDefaults = Field(default_factory=MonitoringDefaults)
    mesh_scale_factor: float = 1.0
    tuning: TuningDefaults = Field(default_factory=TuningDefaults)
    patient_data_layout: PatientDataLayoutDefaults = Field(
        default_factory=PatientDataLayoutDefaults
    )

    @field_validator("mesh_scale_factor")
    @classmethod
    def _mesh_scale_positive(cls, value: float) -> float:
        if value <= 0.0:
            raise ValueError("mesh_scale_factor must be > 0")
        return value


class WorkspaceConfig(BaseModel):
    clusters: list[ClusterConfig]
    patients: list[PatientConfig]
    defaults: DefaultsConfig


class PatientAssetPaths(BaseModel):
    clinical_targets: str
    centerlines: str
    inflow: str
    preop_mesh_complete_dir: str
    mesh_surfaces_dir: str
    postop_mesh_complete_dir: str | None = None
    postop_mesh_surfaces_dir: str | None = None
    iteration1_seed_source: Literal["path", "generate"]
    iteration1_seed_path: str


class ResolvedPatient(BaseModel):
    cluster_name: str
    alias: str
    remote_path: str
    permanent_remote_path: str | None = None
    patient_assets: PatientAssetPaths | None = None
    threed: ThreedTuningConfig
    impedance: ImpedanceTuningConfig
    mesh_scale_factor: float
    data_policy: str
    patient_data_root: str
    permanent_data_root: str | None = None
    runs_root: str
