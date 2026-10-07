"""Next-iteration seed policy resolution shared by tuning, advance, and postop.

``calibrated_full_pa`` only has meaning for impedance tuning with the full-PA
model: every such iteration produces a calibrated full-PA model (the next
iteration's seed, or the converged preop model used for postop), and the
reduced RRI model is never regenerated.  Reduced-RRI tuning (``rri`` model or
``rcr`` boundary conditions) always regenerates its own reduced seed.
"""

from __future__ import annotations

from typing import Any

from svztagent.core.errors import ConfigError

CALIBRATED_FULL_PA_POLICY = "calibrated_full_pa"
LEGACY_RRI_AFTER_FIRST_POLICY = "legacy_rri_after_first"
SEED_POLICIES = frozenset({CALIBRATED_FULL_PA_POLICY, LEGACY_RRI_AFTER_FIRST_POLICY})


def normalize_seed_policy(seed_policy: str | None) -> str:
    policy = str(seed_policy or CALIBRATED_FULL_PA_POLICY).strip().lower()
    if policy not in SEED_POLICIES:
        raise ConfigError(f"unsupported full_pa next-iteration seed policy: {seed_policy!r}")
    return policy


def effective_tuning_model(
    tuning_model: str | None,
    iteration: int,
    seed_policy: str | None = CALIBRATED_FULL_PA_POLICY,
) -> str:
    """Return the tuning model an iteration actually ran (``rri`` or ``full_pa``).

    ``legacy_rri_after_first`` tunes only iteration 1 with the full-PA model.
    """

    normalized = str(tuning_model or "rri").strip().lower()
    policy = normalize_seed_policy(seed_policy)
    if normalized == "full_pa" and iteration > 1 and policy == LEGACY_RRI_AFTER_FIRST_POLICY:
        return "rri"
    return normalized


def manifest_effective_tuning_model(manifest: Any, iteration: int) -> str:
    """Resolve :func:`effective_tuning_model` for one iteration of a run manifest.

    Manifests always record ``calibration_defaults`` since the seed policy was
    introduced; runs without it tuned iterations after the first with RRI.
    """

    remote = getattr(manifest, "remote", None) or {}
    calibration_defaults = remote.get("calibration_defaults")
    seed_policy = (
        calibration_defaults.get("next_iteration_seed_policy")
        if isinstance(calibration_defaults, dict)
        else LEGACY_RRI_AFTER_FIRST_POLICY
    )
    return effective_tuning_model(
        (remote.get("impedance_defaults") or {}).get("tuning_model"),
        int(iteration),
        seed_policy,
    )


def uses_calibrated_full_pa(
    *,
    bc_type: str | None,
    effective_tuning_model: str | None,
    seed_policy: str | None,
) -> bool:
    """Return whether an iteration's successor comes from full-PA calibration."""

    return (
        str(bc_type or "impedance").strip().lower() == "impedance"
        and str(effective_tuning_model or "rri").strip().lower() == "full_pa"
        and normalize_seed_policy(seed_policy) == CALIBRATED_FULL_PA_POLICY
    )


def manifest_uses_calibrated_full_pa(manifest: Any) -> bool:
    """Resolve :func:`uses_calibrated_full_pa` from a run manifest's defaults.

    Under ``calibrated_full_pa`` the effective model is full-PA for every
    iteration, so the configured tuning model is the effective model.
    """

    remote = getattr(manifest, "remote", None) or {}
    impedance_defaults = remote.get("impedance_defaults") or {}
    calibration_defaults = remote.get("calibration_defaults") or {}
    return uses_calibrated_full_pa(
        bc_type=remote.get("tuning_bc_type"),
        effective_tuning_model=impedance_defaults.get("tuning_model"),
        seed_policy=calibration_defaults.get("next_iteration_seed_policy"),
    )


__all__ = [
    "CALIBRATED_FULL_PA_POLICY",
    "LEGACY_RRI_AFTER_FIRST_POLICY",
    "SEED_POLICIES",
    "effective_tuning_model",
    "manifest_effective_tuning_model",
    "manifest_uses_calibrated_full_pa",
    "normalize_seed_policy",
    "uses_calibrated_full_pa",
]
