"""The pre-op source that post-op and adaptation start from.

``converged_preop_iteration`` is written either by ``svzt preop finalize``
(``selection_kind: wall_correction``, the default since 2026-10-26) or, for
legacy runs, by the retired ``svzt preop select``.  ``resolve_preop_source``
is the only place that interprets it, so downstream workflows never branch on
how the handoff was made.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any

from svztagent.core.errors import ConfigError

WALL_CORRECTION_SELECTION = "wall_correction"

PREOP_SOURCE_HINT = (
    "run svzt run wall-correction, svzt wall-correction-status until it completes, "
    "then svzt preop finalize"
)


@dataclass(frozen=True)
class PreopSource:
    """Resolved pre-op inputs: every path is remote, every value final."""

    iteration: int
    selection_kind: str
    wall_correction_id: str | None
    # Pre-op 3D run directory (the wall-corrected run for a wall correction).
    remote_threed_dir: str
    # Its 0D-3D coupling file, i.e. the BCs post-op and adaptation reuse.
    remote_coupler: str
    # The 0D model of the pre-op state (the matched 0D model for a wall correction).
    remote_zerod_model: str
    # The source iteration's tuning results: optimized_params.csv, outlet_cap_mapping.json, tuned configs.
    remote_tuning_results_dir: str
    # ``remote.threed_defaults`` with the pre-op run's wall values applied.
    threed_config: dict[str, Any]
    # The command that recorded this source (for plan previews).
    handoff_command: tuple[str, ...]

    @property
    def is_wall_correction(self) -> bool:
        return self.selection_kind == WALL_CORRECTION_SELECTION


def apply_threed_overrides(threed_config: dict, overrides: dict[str, float] | None) -> dict:
    """Return ``threed_config`` with the pre-op 3D run's wall values applied.

    A wall-correction handoff carries the corrected elasticity modulus and
    tissue damping; downstream 3D runs must reuse them rather than re-derive E
    from the tuned 0D model (``match_proximal_compliance``) or fall back to
    the run's iteration-1 damping.
    """

    values = dict(overrides or {})
    updated = dict(threed_config)
    if "elasticity_modulus" in values:
        updated["elasticity_modulus"] = float(values["elasticity_modulus"])
    if "damping" in values:
        tissue_support = dict(updated.get("tissue_support") or {})
        tissue_support["damping"] = float(values["damping"])
        updated["tissue_support"] = tissue_support
    return updated


def resolve_preop_source(manifest) -> PreopSource:
    handoff = manifest.converged_preop_iteration
    if handoff is None:
        raise ConfigError(f"run '{manifest.run_id}' has no pre-op source; {PREOP_SOURCE_HINT}")
    # Legacy handoffs point at the iteration's own preop/, whose sibling
    # results/ holds the tuned trees; a wall correction names them explicitly.
    tuning_results = handoff.remote_tuning_results_dir or str(PurePosixPath(handoff.remote_preop_dir).parent / "results")
    if handoff.selection_kind == WALL_CORRECTION_SELECTION:
        if not handoff.wall_correction_id or not handoff.remote_tuning_results_dir:
            raise ConfigError(
                f"run '{manifest.run_id}' has a wall-correction pre-op source without its correction id "
                "or tuning results; rerun svzt preop finalize"
            )
        command = ("svzt", "preop", "finalize", "--run-id", manifest.run_id, "--correction-id", handoff.wall_correction_id)
    else:
        command = ("svzt", "preop", "select", "--run-id", manifest.run_id, "--iteration", str(handoff.iteration))
    return PreopSource(
        iteration=int(handoff.iteration),
        selection_kind=handoff.selection_kind,
        wall_correction_id=handoff.wall_correction_id,
        remote_threed_dir=handoff.remote_preop_dir,
        remote_coupler=handoff.remote_canonical_coupler,
        remote_zerod_model=handoff.remote_tuned_zerod_config,
        remote_tuning_results_dir=tuning_results,
        threed_config=apply_threed_overrides(dict((manifest.remote or {}).get("threed_defaults") or {}), handoff.threed_overrides),
        handoff_command=command,
    )
