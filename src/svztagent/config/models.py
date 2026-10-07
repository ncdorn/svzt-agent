"""Typed configuration schemas for workspace YAML files."""

from __future__ import annotations

from pathlib import PurePosixPath
import warnings
from typing import Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_serializer,
    model_validator,
)


class SchedulerConfig(BaseModel):
    type: Literal["slurm", "pbs", "other"]


class RemoteRoots(BaseModel):
    model_config = ConfigDict(extra="forbid")

    permanent_data_root: str | None = None
    runs_root: str

    @field_validator("permanent_data_root", "runs_root")
    @classmethod
    def _must_be_absolute(cls, value: str | None) -> str | None:
        if value is None:
            return value
        if not value.startswith("/"):
            raise ValueError("path must be absolute")
        return value


class ClusterExecutables(BaseModel):
    svfsiplus_path: str
    svzerodsolver_build_dir: str | None = None
    svslicer_path: str | None = None
    pvpython_path: str | None = None

    @field_validator(
        "svfsiplus_path",
        "svzerodsolver_build_dir",
        "svslicer_path",
        "pvpython_path",
    )
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


class PatientParaViewVizOverride(BaseModel):
    """Patient-specific camera overrides merged on top of defaults.postprocess.paraview_viz.

    Only set what differs per patient (typically camera_offset_dir + camera_view_up).
    All other ParaView settings (mem, wall_time_hours, image_resolution, field names)
    fall through to the global defaults.

    cycle_duration_s: cardiac cycle duration in seconds (period of the inflow waveform).
        Required for the paraview viz to identify the last cardiac cycle in the simulation.
        When set, the viz job is submitted automatically alongside postprocess jobs.
    """

    camera_offset_dir: list[float] | None = None
    camera_view_up: list[float] | None = None
    image_resolution: list[int] | None = None
    wall_time_hours: int | None = None
    mem: str | None = None
    cpus: int | None = None
    cycle_duration_s: float | None = None

    @field_validator("camera_offset_dir", "camera_view_up")
    @classmethod
    def _vec3(cls, value: list[float] | None) -> list[float] | None:
        if value is not None and len(value) != 3:
            raise ValueError("must be a 3-element list")
        return value

    @field_validator("image_resolution")
    @classmethod
    def _res(cls, value: list[int] | None) -> list[int] | None:
        if value is not None and (len(value) != 2 or any(v <= 0 for v in value)):
            raise ValueError("image_resolution must be [width, height] with positive integers")
        return value


class PatientPostprocessOverrides(BaseModel):
    """Patient-level postprocess block, mirroring defaults.postprocess structure."""

    paraview_viz: PatientParaViewVizOverride | None = None


class PatientConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    alias: str
    permanent_remote_path: str | None = None
    data_policy: Literal["read_only", "mutable"] = "read_only"
    mesh_scale_factor: float | None = None
    tuning: "PatientTuningOverrides | None" = None
    adaptation: "PatientAdaptationOverrides | None" = None
    postprocess: PatientPostprocessOverrides | None = None
    notes: str | None = None

    @field_validator("permanent_remote_path")
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


class ResistanceMapPostprocessConfig(BaseModel):
    workers: Literal["auto"] | int = "auto"
    selected_preop_mem: str = "64G"

    @field_validator("workers", mode="before")
    @classmethod
    def _normalize_workers(cls, value: object) -> Literal["auto"] | int:
        if value is None:
            return "auto"
        if isinstance(value, str):
            cleaned = value.strip()
            if cleaned == "auto":
                return "auto"
            try:
                value = int(cleaned)
            except ValueError as exc:
                raise ValueError("workers must be 'auto' or a positive integer") from exc
        if isinstance(value, int):
            if value <= 0:
                raise ValueError("workers must be > 0")
            return value
        raise ValueError("workers must be 'auto' or a positive integer")

    @field_validator("selected_preop_mem")
    @classmethod
    def _selected_preop_mem_nonempty(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("selected_preop_mem cannot be empty")
        return cleaned


class ParaViewVizConfig(BaseModel):
    image_resolution: list[int] = Field(default_factory=lambda: [1920, 1080])
    camera_offset_dir: list[float] = Field(default_factory=lambda: [1.0, -1.0, 1.0])
    # Optional explicit view-up vector.  When null the script auto-selects Z-up
    # (or Y-up when camera_offset_dir is nearly vertical).  Set this after reading
    # the value from the ParaView GUI Python Shell: GetActiveCamera().GetViewUp()
    camera_view_up: list[float] | None = None
    pressure_field: str = "Pressure"
    velocity_field: str = "Velocity"
    wss_field: str = "WSS"
    displacement_field: str = "Displacement"
    wall_time_hours: int = 2
    mem: str = "32G"
    cpus: int = 1
    # Cardiac cycle duration in seconds. Set per-patient in postprocess.paraview_viz.
    # When present, the viz job is submitted automatically alongside postprocess jobs.
    cycle_duration_s: float | None = None

    @field_validator("image_resolution")
    @classmethod
    def _image_resolution_valid(cls, value: list[int]) -> list[int]:
        if len(value) != 2 or any(v <= 0 for v in value):
            raise ValueError("image_resolution must be [width, height] with positive integers")
        return value

    @field_validator("camera_offset_dir")
    @classmethod
    def _camera_dir_valid(cls, value: list[float]) -> list[float]:
        if len(value) != 3:
            raise ValueError("camera_offset_dir must be a 3-element list")
        return value

    @field_validator("camera_view_up")
    @classmethod
    def _camera_view_up_valid(cls, value: list[float] | None) -> list[float] | None:
        if value is not None and len(value) != 3:
            raise ValueError("camera_view_up must be a 3-element list or null")
        return value

    @field_validator("wall_time_hours", "cpus")
    @classmethod
    def _positive_int(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("value must be > 0")
        return value


class PostprocessDefaults(BaseModel):
    resistance_map: ResistanceMapPostprocessConfig = Field(
        default_factory=ResistanceMapPostprocessConfig
    )
    paraview_viz: ParaViewVizConfig = Field(default_factory=ParaViewVizConfig)


class Iteration1SeedConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: Literal["path", "generate", "learned_zerod"] = "path"
    path: str = "simplified_nonlinear_zerod.json"
    learned_zerod_executable: str = "learned-zerod"
    svzerodsolver_executable: str = "svzerodsolver"

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

    @field_validator("learned_zerod_executable", "svzerodsolver_executable")
    @classmethod
    def _validate_executable(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("seed-generation executable cannot be empty")
        return cleaned


class CalibrationPolicyConfig(BaseModel):
    """Agent-owned enablement and next-seed policy for calibration.

    Scientific calibration settings and artifact validation remain owned by
    svZeroDTrees.  This small policy block only controls whether a future
    agent workflow may run calibration and which explicitly selected seed
    policy it may use.
    """

    model_config = ConfigDict(extra="forbid")

    # Full-PA iterations require a calibrated full-PA successor by default.
    # ``False`` remains an explicit opt-out for the legacy reduced-RRI policy.
    enabled: bool = True
    next_iteration_seed_policy: Literal[
        "calibrated_full_pa", "legacy_rri_after_first"
    ] = "calibrated_full_pa"

    @property
    def seed_policy(self) -> str:
        """Compatibility accessor for callers using the shorter label."""

        return self.next_iteration_seed_policy

    @model_validator(mode="before")
    @classmethod
    def _accept_seed_policy_alias(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        payload = dict(data)
        short = payload.pop("seed_policy", None)
        long = payload.get("next_iteration_seed_policy")
        if short is not None and long is not None and str(short) != str(long):
            raise ValueError(
                "calibration.seed_policy and "
                "calibration.next_iteration_seed_policy disagree"
            )
        if short is not None:
            payload["next_iteration_seed_policy"] = short
        return payload


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
    execution: "ThreedExecutionConfig" = Field(default_factory=lambda: ThreedExecutionConfig())

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
    execution: "PatientThreedExecutionOverrides | None" = None


class ThreedSlurmExecutionConfig(BaseModel):
    mail_user: str | None = None
    mail_types: list[str] = Field(default_factory=lambda: ["begin", "end"])

    @field_validator("mail_user")
    @classmethod
    def _mail_user_nonempty(cls, value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("mail_user cannot be empty")
        return cleaned

    @field_validator("mail_types", mode="before")
    @classmethod
    def _normalize_mail_types(cls, value: object) -> list[str]:
        if value is None:
            return []
        if not isinstance(value, list):
            raise ValueError("mail_types must be a list")
        normalized: list[str] = []
        for item in value:
            cleaned = str(item).strip()
            if not cleaned:
                raise ValueError("mail_types entries cannot be empty")
            normalized.append(cleaned)
        return normalized


class ThreedExecutionConfig(BaseModel):
    slurm: ThreedSlurmExecutionConfig = Field(default_factory=ThreedSlurmExecutionConfig)


class PatientThreedSlurmExecutionOverrides(BaseModel):
    mail_user: str | None = None
    mail_types: list[str] | None = None


class PatientThreedExecutionOverrides(BaseModel):
    slurm: PatientThreedSlurmExecutionOverrides | None = None


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


class ObjectiveTreePolicyConfig(BaseModel):
    """Full-PA tree policy used only inside the optimizer objective.

    Forwarded unchanged to svZeroDTrees, which owns cross-field validation
    (``resolve_objective_tree_policy``) and fills omitted fields from the final
    policy.  Only explicitly set fields are serialized so that omission keeps
    meaning "inherit"; an explicit ``diameter_std_cap: null`` means "no cap".
    """

    model_config = ConfigDict(extra="forbid")

    use_mean: bool | None = None
    diameter_scale: float | None = None
    diameter_std_cap: float | None = None
    reference_diameter: Literal["arithmetic_mean", "conductance_matched"] | None = None

    @field_validator("diameter_scale", "diameter_std_cap")
    @classmethod
    def _nonnegative(cls, value: float | None) -> float | None:
        if value is not None and value < 0.0:
            raise ValueError("value must be >= 0")
        return value

    @model_serializer(mode="wrap")
    def _serialize_set_fields(self, handler: Any) -> dict[str, Any]:
        data = handler(self)
        return {key: value for key, value in data.items() if key in self.model_fields_set}


# Outlet (distal) pressure policies forwarded to svZeroDTrees
# (``ClinicalTargets.from_csv``); see docs/TUNING_MODEL.md for the rationale.
WedgePressurePolicy = Literal[
    "clamp_to_diastolic", "measured", "precapillary_fraction", "diastolic_offset"
]


class TuningObjectiveConfig(BaseModel):
    """Impedance tuning objective forwarded to svZeroDTrees.

    ``relative`` is the historical weighted relative squared error;
    ``likelihood`` is sum(((model - target) / sigma)^2) with one measurement
    sigma for catheter pressures and one for the RPA flow split.
    """

    model_config = ConfigDict(extra="forbid")

    type: Literal["relative", "likelihood"] = "relative"
    pressure_sigma_mmhg: float = 2.0
    split_sigma: float = 0.02
    # Likelihood only: Nelder-Mead target stop and iteration gate use
    # |model - target| <= target_sigma * sigma; null disables the target stop.
    target_sigma: float | None = 1.0

    @field_validator("pressure_sigma_mmhg", "split_sigma")
    @classmethod
    def _positive_sigma(cls, value: float) -> float:
        if not value > 0.0:
            raise ValueError("sigma must be > 0")
        return value

    @field_validator("target_sigma")
    @classmethod
    def _positive_target_sigma(cls, value: float | None) -> float | None:
        if value is not None and not value > 0.0:
            raise ValueError("target_sigma must be > 0 or null")
        return value


class ProximalComplianceConfig(BaseModel):
    """Thin-wall compliance C = 3 A L / (2 Eh/r) on every full-PA seed vessel."""

    model_config = ConfigDict(extra="forbid")

    wall_ehr: float

    @field_validator("wall_ehr")
    @classmethod
    def _positive_ehr(cls, value: float) -> float:
        if not value > 0.0:
            raise ValueError("wall_ehr must be > 0")
        return value


class LeafResistanceConfig(BaseModel):
    """Per-leaf capillary + venous resistance at every structured tree (full_pa only).

    Each tree's leaves get one resistance to the outlet pressure that carries
    ``downstream_fraction`` of the tree's DC resistance; the outlet pressure is
    the measured PCWP (``wedge_pressure_policy: measured``).
    """

    model_config = ConfigDict(extra="forbid")

    downstream_fraction: float

    @field_validator("downstream_fraction")
    @classmethod
    def _fraction_range(cls, value: float) -> float:
        if not 0.0 < value < 1.0:
            raise ValueError("leaf_resistance.downstream_fraction must be in (0, 1)")
        return value


class PolishConfig(BaseModel):
    """Per-cap re-tune after a shared objective-tree fit (full_pa only)."""

    model_config = ConfigDict(extra="forbid")

    maxfev: int = 100
    # Optional smaller simplex for a local search around the shared optimum.
    initial_simplex_step: float | None = None

    @field_validator("maxfev")
    @classmethod
    def _positive_maxfev(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("maxfev must be > 0")
        return value

    @field_validator("initial_simplex_step")
    @classmethod
    def _simplex_step(cls, value: float | None) -> float | None:
        if value is not None and not 0.0 < value <= 0.5:
            raise ValueError("initial_simplex_step must be in (0, 0.5]")
        return value

    @model_serializer(mode="wrap")
    def _drop_unset_step(self, handler: Any) -> dict[str, Any]:
        data = handler(self)
        if data.get("initial_simplex_step") is None:
            data.pop("initial_simplex_step", None)
        return data


class NelderMeadStoppingConfig(BaseModel):
    """Clinically scaled Nelder-Mead stopping policy for impedance tuning.

    Forwarded to svZeroDTrees (``resolve_nelder_mead_stopping``), which owns
    the semantics.  ``enabled: false`` restores the historical maxiter-only
    runs.  ``target_tolerance`` is a relative error per objective metric;
    ``xatol`` is in bounds-normalized [0, 1] parameter units.
    """

    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    target_tolerance: float | None = 0.025
    stall_window: int | None = None  # None -> 5 x number of free parameters
    stall_rel_improvement: float | None = 0.01
    xatol: float = 1e-3
    fatol: float = 1e-3
    maxfev: int = 200
    initial_simplex_step: float = 0.1
    restart_min_rel_improvement: float | None = 0.05

    @field_validator(
        "target_tolerance", "stall_rel_improvement", "restart_min_rel_improvement", "xatol", "fatol"
    )
    @classmethod
    def _positive_or_none(cls, value: float | None) -> float | None:
        if value is not None and value <= 0.0:
            raise ValueError("value must be > 0")
        return value

    @field_validator("stall_window", "maxfev")
    @classmethod
    def _positive_count(cls, value: int | None) -> int | None:
        if value is not None and value <= 0:
            raise ValueError("value must be > 0")
        return value

    @field_validator("initial_simplex_step")
    @classmethod
    def _simplex_step_range(cls, value: float) -> float:
        if not 0.0 < value <= 0.5:
            raise ValueError("initial_simplex_step must be in (0, 0.5]")
        return value


class ImpedanceTuningConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

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
    tuning_model: Literal["rri", "full_pa"] = "rri"
    # How svZeroDTrees derives the tree outlet pressure Pd:
    # clamp_to_diastolic = min(wedge, diastolic MPA target) (upstream default);
    # measured = the measured wedge pressure as-is;
    # precapillary_fraction = wedge + precapillary_fraction * (mean - wedge)
    #   (regurgitant patients); diastolic_offset = diastolic - diastolic_offset_mmhg
    #   (non-regurgitant patients; needs no wedge).
    wedge_pressure_policy: WedgePressurePolicy = "clamp_to_diastolic"
    precapillary_fraction: float = 0.332
    diastolic_offset_mmhg: float = 2.0
    # Keep the diastolic term when its target is below Pd (regurgitation).
    keep_diastolic_target: bool = False
    # None keeps the historical relative objective.
    objective: TuningObjectiveConfig | None = None
    # full_pa only: seed-vessel thin-wall compliance; None keeps a rigid seed.
    proximal_compliance: ProximalComplianceConfig | None = None
    # Structured-tree node budget; None keeps the svZeroDTrees default (100k).
    tree_max_nodes: int | None = None
    # full_pa only, requires objective_tree_policy: per-cap polish budget.
    polish: PolishConfig | None = None
    # full_pa only, requires wedge_pressure_policy 'measured'.
    leaf_resistance: LeafResistanceConfig | None = None
    outlet_mapping_mode: Literal[
        "auto", "metadata", "cap_name", "centerline", "serialized_cap_order", "explicit"
    ] | None = None
    outlet_mapping: dict[str, str] | None = None
    # Centerline VTP the full-PA 0D seed was generated from.  Patient
    # resolution defaults it to the patient centerline for auto/centerline.
    outlet_mapping_centerline: str | None = None
    # Deprecated input-only alias.  The final typed config never serializes it
    # and canonical workflow code consumes only outlet_mapping_mode/outlet_mapping.
    allow_ordered_outlet_mapping: bool | None = Field(default=None, exclude=True)
    # Optional full_pa objective-only tree policy; None means the objective
    # uses the final policy (use_mean/diameter_scale/diameter_std_cap).
    objective_tree_policy: ObjectiveTreePolicyConfig | None = None
    stopping: NelderMeadStoppingConfig = Field(default_factory=NelderMeadStoppingConfig)
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

    @field_validator("precapillary_fraction")
    @classmethod
    def _fraction_range(cls, value: float) -> float:
        if not 0.0 <= value < 1.0:
            raise ValueError("precapillary_fraction must be in [0, 1)")
        return value

    @field_validator("diastolic_offset_mmhg")
    @classmethod
    def _nonnegative_offset(cls, value: float) -> float:
        if value < 0.0:
            raise ValueError("diastolic_offset_mmhg must be >= 0")
        return value

    @field_validator("tree_max_nodes")
    @classmethod
    def _positive_max_nodes(cls, value: int | None) -> int | None:
        if value is not None and value <= 0:
            raise ValueError("tree_max_nodes must be > 0")
        return value

    @model_validator(mode="after")
    def _validate_full_pa_only_controls(self) -> "ImpedanceTuningConfig":
        if self.tuning_model != "full_pa":
            if self.proximal_compliance is not None:
                raise ValueError("proximal_compliance is supported only for tuning_model='full_pa'")
            if self.polish is not None:
                raise ValueError("polish is supported only for tuning_model='full_pa'")
            if self.leaf_resistance is not None:
                raise ValueError("leaf_resistance is supported only for tuning_model='full_pa'")
        if self.leaf_resistance is not None and self.wedge_pressure_policy != "measured":
            raise ValueError(
                "leaf_resistance requires wedge_pressure_policy 'measured': the leaf resistance "
                "carries the capillary and venous pressure drop down to the measured PCWP"
            )
        if self.polish is not None and self.objective_tree_policy is None:
            raise ValueError(
                "polish requires objective_tree_policy: without it the optimizer already "
                "uses the final per-cap trees"
            )
        return self

    @model_validator(mode="before")
    @classmethod
    def _translate_legacy_outlet_mapping(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        payload = dict(data)
        legacy_supplied = "allow_ordered_outlet_mapping" in payload and payload[
            "allow_ordered_outlet_mapping"
        ] is not None
        canonical_supplied = (
            "outlet_mapping_mode" in payload
            and payload.get("outlet_mapping_mode") is not None
        )
        if legacy_supplied:
            warnings.warn(
                "allow_ordered_outlet_mapping is deprecated and maps to "
                "outlet_mapping_mode='serialized_cap_order', which mispairs caps "
                "and BCs on centerline-generated seeds; use outlet_mapping_mode "
                "'auto' or 'centerline' instead",
                DeprecationWarning,
                stacklevel=3,
            )
            if canonical_supplied:
                raise ValueError(
                    "allow_ordered_outlet_mapping cannot be combined with "
                    "outlet_mapping_mode"
                )
            if str(payload.get("tuning_model", "rri")).strip().lower() == "full_pa" and bool(
                payload["allow_ordered_outlet_mapping"]
            ):
                payload["outlet_mapping_mode"] = "serialized_cap_order"
        return payload

    @model_validator(mode="after")
    def _validate_outlet_mapping_contract(self) -> "ImpedanceTuningConfig":
        mode = self.outlet_mapping_mode
        if self.tuning_model == "full_pa" and mode is None:
            mode = "auto"
            object.__setattr__(self, "outlet_mapping_mode", mode)
        if self.outlet_mapping is not None:
            if mode != "explicit":
                raise ValueError(
                    "outlet_mapping requires outlet_mapping_mode='explicit'"
                )
            if not self.outlet_mapping:
                raise ValueError("outlet_mapping must not be empty")
            normalized: dict[str, str] = {}
            for cap, bc_name in self.outlet_mapping.items():
                cap_name = str(cap).strip()
                outlet_name = str(bc_name).strip()
                if not cap_name or not outlet_name:
                    raise ValueError(
                        "outlet_mapping keys and values must be non-empty"
                    )
                if cap_name in normalized:
                    raise ValueError(f"outlet_mapping contains duplicate cap '{cap_name}'")
                normalized[cap_name] = outlet_name
            object.__setattr__(self, "outlet_mapping", normalized)
        if self.outlet_mapping_centerline is not None:
            centerline = self.outlet_mapping_centerline.strip()
            if not centerline:
                raise ValueError("outlet_mapping_centerline must not be empty")
            if self.tuning_model == "full_pa" and mode not in {"auto", "centerline"}:
                raise ValueError(
                    "outlet_mapping_centerline is used only by "
                    "outlet_mapping_mode 'auto' or 'centerline'"
                )
            object.__setattr__(self, "outlet_mapping_centerline", centerline)
        if self.tuning_model == "rri" and (
            mode is not None
            or self.outlet_mapping is not None
            or self.outlet_mapping_centerline is not None
        ):
            raise ValueError(
                "outlet mapping controls are supported only for tuning_model='full_pa'"
            )
        if self.tuning_model == "rri" and self.objective_tree_policy is not None:
            raise ValueError(
                "objective_tree_policy is supported only for tuning_model='full_pa'"
            )
        return self


class RCRTuningConfig(BaseModel):
    solver: Literal["Nelder-Mead"] = "Nelder-Mead"
    n_procs: int = 24
    rescale_inflow: bool = True
    convert_to_cm: bool = False

    @field_validator("n_procs")
    @classmethod
    def _positive_n_procs(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("n_procs must be > 0")
        return value


class PatientRCROverrides(BaseModel):
    solver: Literal["Nelder-Mead"] | None = None
    n_procs: int | None = None
    rescale_inflow: bool | None = None
    convert_to_cm: bool | None = None


class PatientImpedanceOverrides(BaseModel):
    model_config = ConfigDict(extra="forbid")

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
    tuning_model: Literal["rri", "full_pa"] | None = None
    wedge_pressure_policy: WedgePressurePolicy | None = None
    precapillary_fraction: float | None = None
    diastolic_offset_mmhg: float | None = None
    keep_diastolic_target: bool | None = None
    # objective and polish patch the workspace block field-by-field;
    # proximal_compliance, leaf_resistance and tree_max_nodes replace it.  An explicit null
    # clears any of them (and objective_tree_policy / outlet_mapping_*) for
    # this patient; see _resolve_patient_impedance_config.
    objective: TuningObjectiveConfig | None = None
    proximal_compliance: ProximalComplianceConfig | None = None
    tree_max_nodes: int | None = None
    polish: PolishConfig | None = None
    # full_pa only, requires wedge_pressure_policy 'measured'.
    leaf_resistance: LeafResistanceConfig | None = None
    outlet_mapping_mode: Literal[
        "auto", "metadata", "cap_name", "centerline", "serialized_cap_order", "explicit"
    ] | None = None
    outlet_mapping: dict[str, str] | None = None
    # Centerline VTP the full-PA 0D seed was generated from.  Patient
    # resolution defaults it to the patient centerline for auto/centerline.
    outlet_mapping_centerline: str | None = None
    allow_ordered_outlet_mapping: bool | None = None
    objective_tree_policy: ObjectiveTreePolicyConfig | None = None
    # Field-level patch over the workspace stopping policy.
    stopping: NelderMeadStoppingConfig | None = None
    tune_space: TuneSpaceConfig | None = None


class TuningDefaults(BaseModel):
    bc_type: Literal["impedance", "rcr"] = "impedance"
    iteration1_seed: Iteration1SeedConfig = Field(default_factory=Iteration1SeedConfig)
    threed: ThreedTuningConfig = Field(default_factory=ThreedTuningConfig)
    impedance: ImpedanceTuningConfig = Field(default_factory=ImpedanceTuningConfig)
    rcr: RCRTuningConfig = Field(default_factory=RCRTuningConfig)
    calibration: CalibrationPolicyConfig = Field(default_factory=CalibrationPolicyConfig)


class PatientTuningOverrides(BaseModel):
    bc_type: Literal["impedance", "rcr"] | None = None
    iteration1_seed: Iteration1SeedConfig | None = None
    threed: PatientThreedOverrides | None = None
    impedance: PatientImpedanceOverrides | None = None
    rcr: PatientRCROverrides | None = None
    calibration: CalibrationPolicyConfig | None = None


class AdaptationModelConfig(BaseModel):
    iterations: int | None = None
    wss_gain: float | None = None
    ims_gain: float | None = None
    compliance_gain: float | None = None
    k_arr: list[float] | None = None
    terminal_resistance: float | None = None

    @field_validator("iterations")
    @classmethod
    def _iterations_positive(cls, value: int | None) -> int | None:
        if value is None:
            return value
        if value <= 0:
            raise ValueError("iterations must be > 0")
        return value

    @field_validator("k_arr")
    @classmethod
    def _validate_k_arr(cls, value: list[float] | None) -> list[float] | None:
        if value is None:
            return value
        if len(value) != 4:
            raise ValueError("k_arr must contain exactly 4 values")
        return [float(item) for item in value]


class AdaptationModelsConfig(BaseModel):
    m1: AdaptationModelConfig = Field(default_factory=AdaptationModelConfig)
    m2: AdaptationModelConfig = Field(
        default_factory=lambda: AdaptationModelConfig(
            iterations=1,
            wss_gain=1.0,
            ims_gain=1.0,
            compliance_gain=1.0,
        )
    )
    m3: AdaptationModelConfig = Field(
        default_factory=lambda: AdaptationModelConfig(
            iterations=1,
            k_arr=[1.0, 1.0, 1.0, 1.0],
        )
    )


class AdaptationParameterSet(BaseModel):
    m1: AdaptationModelConfig | None = None
    m2: AdaptationModelConfig | None = None
    m3: AdaptationModelConfig | None = None


class AdaptationDefaults(BaseModel):
    default_model: Literal["M1", "M2", "M3"] = "M2"
    territory_scheme: Literal["lpa_rpa"] = "lpa_rpa"
    target_stage: Literal["postop"] = "postop"
    parameter_policy: Literal["global_fixed"] = "global_fixed"
    parameter_sets: dict[str, AdaptationParameterSet] = Field(default_factory=dict)
    models: AdaptationModelsConfig = Field(default_factory=AdaptationModelsConfig)


class PatientAdaptationModelOverrides(BaseModel):
    iterations: int | None = None
    wss_gain: float | None = None
    ims_gain: float | None = None
    compliance_gain: float | None = None
    k_arr: list[float] | None = None
    terminal_resistance: float | None = None


class PatientAdaptationModelsOverrides(BaseModel):
    m1: PatientAdaptationModelOverrides | None = None
    m2: PatientAdaptationModelOverrides | None = None
    m3: PatientAdaptationModelOverrides | None = None


class PatientAdaptationOverrides(BaseModel):
    default_model: Literal["M1", "M2", "M3"] | None = None
    territory_scheme: Literal["lpa_rpa"] | None = None
    target_stage: Literal["postop"] | None = None
    parameter_policy: Literal["global_fixed"] | None = None
    parameter_sets: dict[str, AdaptationParameterSet] | None = None
    models: PatientAdaptationModelsOverrides | None = None


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
    postprocess: PostprocessDefaults = Field(default_factory=PostprocessDefaults)
    mesh_scale_factor: float = 1.0
    tuning: TuningDefaults = Field(default_factory=TuningDefaults)
    adaptation: AdaptationDefaults = Field(default_factory=AdaptationDefaults)
    patient_data_layout: PatientDataLayoutDefaults = Field(
        default_factory=PatientDataLayoutDefaults
    )

    @field_validator("mesh_scale_factor")
    @classmethod
    def _mesh_scale_positive(cls, value: float) -> float:
        if value <= 0.0:
            raise ValueError("mesh_scale_factor must be > 0")
        return value


class RepositoryLocationsConfig(BaseModel):
    svzt_agent: str | None = None
    svZeroDTrees: str | None = None
    svZeroDSolver: str | None = None

    @field_validator("svzt_agent", "svZeroDTrees", "svZeroDSolver")
    @classmethod
    def _repository_path_nonempty(cls, value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("repository path cannot be empty")
        return cleaned


class WorkspaceConfig(BaseModel):
    clusters: list[ClusterConfig]
    patients: list[PatientConfig]
    defaults: DefaultsConfig
    repositories: RepositoryLocationsConfig = Field(default_factory=RepositoryLocationsConfig)


class PatientAssetPaths(BaseModel):
    clinical_targets: str
    centerlines: str
    inflow: str
    preop_mesh_complete_dir: str
    mesh_surfaces_dir: str
    postop_mesh_complete_dir: str | None = None
    postop_mesh_surfaces_dir: str | None = None
    iteration1_seed_source: Literal["path", "generate", "learned_zerod"]
    iteration1_seed_path: str
    iteration1_seed_learned_zerod_executable: str
    iteration1_seed_svzerodsolver_executable: str


class ResolvedPatient(BaseModel):
    cluster_name: str
    alias: str
    permanent_remote_path: str | None = None
    patient_assets: PatientAssetPaths | None = None
    bc_type: Literal["impedance", "rcr"] = "impedance"
    threed: ThreedTuningConfig
    impedance: ImpedanceTuningConfig
    rcr: RCRTuningConfig
    calibration: CalibrationPolicyConfig
    adaptation: AdaptationDefaults
    mesh_scale_factor: float
    data_policy: str
    permanent_data_root: str | None = None
    runs_root: str
