"""Cross-checks of per-patient impedance tuning choices (warnings only).

Reported by ``svzt config validate`` and ``svzt doctor``; see
docs/TUNING_MODEL.md for the modeling rationale.

1. Outlet-pressure policy vs the preop ``regurgitation`` flag and wedge
   pressure in the optional ``config/clinical_targets.yaml``: regurgitant
   patients are expected on ``precapillary_fraction`` (which needs a measured
   wedge pressure); non-regurgitant patients on ``diastolic_offset``.
2. Proximal (0D seed) wall stiffness vs the deformable 3D wall: the 3D model
   stands in for the same vessels, so E*h/r of the 3D wall over a typical
   proximal radius range should be within ~2x of ``proximal_compliance.wall_ehr``.
   This is a coarse static check; the tuning job compares the 3D wall with the
   E*h that matches the seed's total proximal compliance
   (``threed_wall_vs_proximal_compliance`` in the iteration driver log).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from .models import MATCH_PROXIMAL_COMPLIANCE, WorkspaceConfig

# Proximal PA radius range [cm] used to express the 3D wall as E*h/r.
PROXIMAL_RADIUS_RANGE_CM = (0.2, 1.0)
WALL_EHR_TOLERANCE_FACTOR = 2.0


def _preop_targets(workspace_root: Path) -> dict[str, Any]:
    path = workspace_root / "config" / "clinical_targets.yaml"
    if not path.exists():
        return {}
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError:
        return {}
    return ((payload.get("clinical_targets") or {}).get("preop") or {}).get("patients") or {}


def tuning_model_warnings(config: WorkspaceConfig, workspace_root: str | Path) -> list[str]:
    # Imported here to avoid a load <-> checks import cycle.
    from .load import (
        _resolve_patient_bc_type,
        _resolve_patient_impedance_config,
        _resolve_patient_threed_config,
    )

    root = Path(workspace_root)
    targets = _preop_targets(root)
    warnings: list[str] = []
    for patient in config.patients:
        try:
            if _resolve_patient_bc_type(config, patient) != "impedance":
                continue
            impedance = _resolve_patient_impedance_config(config, patient)
            threed = _resolve_patient_threed_config(config, patient)
        except Exception as exc:  # resolution errors are reported elsewhere
            warnings.append(f"{patient.alias}: could not resolve tuning config ({exc})")
            continue
        policy = impedance.wedge_pressure_policy
        record = targets.get(patient.alias) or {}
        regurgitation = record.get("regurgitation")
        wedge = record.get("wedge_pressure")
        if impedance.leaf_resistance is not None:
            # Outlet at PCWP with a capillary + venous leaf resistance: one rule
            # for every patient, but it needs a measured wedge pressure.
            if record and wedge is None:
                warnings.append(
                    f"{patient.alias}: leaf_resistance uses the measured wedge pressure as the "
                    "outlet pressure, which clinical_targets.yaml does not list"
                )
        elif regurgitation is True and policy != "precapillary_fraction":
            warnings.append(
                f"{patient.alias}: clinical_targets.yaml marks pulmonary regurgitation but "
                f"wedge_pressure_policy is '{policy}' (expected 'precapillary_fraction')"
            )
        elif regurgitation is False and policy == "precapillary_fraction":
            warnings.append(
                f"{patient.alias}: no pulmonary regurgitation in clinical_targets.yaml but "
                "wedge_pressure_policy is 'precapillary_fraction' (expected 'diastolic_offset'); "
                "without backflow the outlet pressure may exceed the diastolic target"
            )
        if policy == "precapillary_fraction" and record and wedge is None:
            warnings.append(
                f"{patient.alias}: 'precapillary_fraction' needs a measured wedge_pressure, "
                "which clinical_targets.yaml does not list"
            )
        proximal = impedance.proximal_compliance
        if threed.elasticity_modulus == MATCH_PROXIMAL_COMPLIANCE:
            if proximal is None:
                warnings.append(
                    f"{patient.alias}: elasticity_modulus={MATCH_PROXIMAL_COMPLIANCE!r} needs "
                    "tuning.impedance.proximal_compliance; the 3D stage will fail without it"
                )
        elif proximal is not None and threed.wall_model == "deformable":
            eh = float(threed.elasticity_modulus) * float(threed.shell_thickness)
            r_lo, r_hi = PROXIMAL_RADIUS_RANGE_CM
            ehr_lo, ehr_hi = eh / r_hi, eh / r_lo
            if not (
                ehr_lo / WALL_EHR_TOLERANCE_FACTOR
                <= proximal.wall_ehr
                <= ehr_hi * WALL_EHR_TOLERANCE_FACTOR
            ):
                warnings.append(
                    f"{patient.alias}: proximal_compliance.wall_ehr={proximal.wall_ehr:.3g} "
                    f"dyn/cm^2 but the deformable 3D wall gives E*h/r of {ehr_lo:.3g}-{ehr_hi:.3g} "
                    f"for r in {r_lo}-{r_hi} cm; the 3D-coupled model will not reproduce the 0D "
                    "proximal compliance"
                )
    return warnings
