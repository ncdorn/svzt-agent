"""Deterministic wall-correction rule and svMultiPhysics input checks.

Pure functions only: the workflow in ``svztagent.workflows.wall_correction``
fetches the inputs, applies these, and owns every remote side effect.

Rule (protocol v1, step 7): pulse pressure times the effective 3D proximal
compliance is constant across 3D runs at the same BCs, and the compliance
scales as E**-alpha, so the wall that reaches the clinical pulse pressure is

    lambda = (PP_target / PP_3D) ** (1 / alpha),  lambda >= lambda_min
    E_corrected = lambda * E_source

with Rule A tissue damping c = c_1 * sqrt(E_corrected / E_1), where c_1 is the
damping set at the iteration-1 wall E_1 (for a fixed geometry K = Eh/(0.75 r^2)
is proportional to E).
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import math
import re
import xml.etree.ElementTree as ET

from svztagent.core.errors import ConfigError


@dataclass(frozen=True)
class WallCorrectionRule:
    alpha: float
    lambda_min: float
    pulse_pressure_target_mmhg: float
    pulse_pressure_3d_mmhg: float
    lambda_raw: float
    lambda_applied: float
    floored: bool
    elasticity_modulus_source: float
    elasticity_modulus_corrected: float
    elasticity_modulus_reference: float
    damping_reference: float
    damping_source: float | None
    damping_corrected: float

    def as_dict(self) -> dict[str, float | bool | None]:
        return asdict(self)


def _positive(name: str, value: float) -> float:
    number = float(value)
    if not math.isfinite(number) or number <= 0.0:
        raise ConfigError(f"wall correction requires a positive finite {name}; got {value!r}")
    return number


def compute_wall_correction(
    *,
    pulse_pressure_3d_mmhg: float,
    pulse_pressure_target_mmhg: float,
    elasticity_modulus_source: float,
    elasticity_modulus_reference: float,
    damping_reference: float,
    alpha: float,
    lambda_min: float,
    damping_source: float | None = None,
) -> WallCorrectionRule:
    """Apply the wall-correction rule; damping is rounded to 0.1 as in the protocol runs."""

    pp_3d = _positive("3D pulse pressure", pulse_pressure_3d_mmhg)
    pp_target = _positive("target pulse pressure", pulse_pressure_target_mmhg)
    e_source = _positive("source elasticity modulus", elasticity_modulus_source)
    e_reference = _positive("iteration-1 elasticity modulus", elasticity_modulus_reference)
    c_reference = _positive("iteration-1 damping", damping_reference)
    exponent = _positive("alpha", alpha)
    floor = _positive("lambda_min", lambda_min)
    lambda_raw = (pp_target / pp_3d) ** (1.0 / exponent)
    lambda_applied = max(floor, lambda_raw)
    e_corrected = lambda_applied * e_source
    damping = round(c_reference * math.sqrt(e_corrected / e_reference), 1)
    return WallCorrectionRule(
        alpha=exponent,
        lambda_min=floor,
        pulse_pressure_target_mmhg=pp_target,
        pulse_pressure_3d_mmhg=pp_3d,
        lambda_raw=lambda_raw,
        lambda_applied=lambda_applied,
        floored=lambda_raw < floor,
        elasticity_modulus_source=e_source,
        elasticity_modulus_corrected=e_corrected,
        elasticity_modulus_reference=e_reference,
        damping_reference=c_reference,
        damping_source=None if damping_source is None else float(damping_source),
        damping_corrected=damping,
    )


_MODULUS = re.compile(r"(<Elasticity_modulus>)\s*([^<\s]+)\s*(</Elasticity_modulus>)")
_DAMPING = re.compile(r"(<Damping>)\s*([^<\s]+)\s*(</Damping>)")
# Path elements that legitimately differ between an iteration's 3D directory
# and a copy of it (mesh and face files are absolute paths into the run dir).
_RELOCATABLE_TAGS = frozenset({"Mesh_file_path", "Face_file_path"})


def read_wall_parameters(xml_text: str) -> tuple[float, float]:
    """Return (elasticity modulus, tissue damping) from an svMultiPhysics input."""

    moduli = _MODULUS.findall(xml_text)
    dampings = _DAMPING.findall(xml_text)
    if len(moduli) != 1 or len(dampings) != 1:
        raise ConfigError(
            "expected exactly one <Elasticity_modulus> and one <Damping> in the 3D input; "
            f"found {len(moduli)} and {len(dampings)}"
        )
    return float(moduli[0][1]), float(dampings[0][1])


def patch_wall_parameters(xml_text: str, *, elasticity_modulus: float, damping: float) -> str:
    """Replace the single elasticity modulus and damping values; nothing else changes."""

    read_wall_parameters(xml_text)
    patched = _MODULUS.sub(lambda m: f"{m.group(1)}{elasticity_modulus!r}{m.group(3)}", xml_text)
    return _DAMPING.sub(lambda m: f"{m.group(1)}{damping!r}{m.group(3)}", patched)


def _comparable_elements(xml_text: str) -> list[tuple[str, str, tuple[tuple[str, str], ...]]]:
    root = ET.fromstring(xml_text)
    rows = []
    for element in root.iter():
        if element.tag in _RELOCATABLE_TAGS | {"Elasticity_modulus", "Damping"}:
            continue
        rows.append((element.tag, (element.text or "").strip(), tuple(sorted(element.attrib.items()))))
    return rows


def wall_only_differences(source_xml: str, candidate_xml: str) -> list[str]:
    """List differences other than mesh paths, elasticity modulus and damping.

    An empty list means ``candidate_xml`` is the source 3D input with only the
    wall changed (and possibly relocated mesh paths).
    """

    source = _comparable_elements(source_xml)
    candidate = _comparable_elements(candidate_xml)
    if len(source) != len(candidate):
        return [f"element count differs: {len(source)} vs {len(candidate)}"]
    differences = []
    for index, (left, right) in enumerate(zip(source, candidate)):
        if left != right:
            differences.append(f"element {index}: {left[:2]} vs {right[:2]}")
    return differences


def relative_mismatch(expected: float, actual: float) -> float:
    return abs(float(actual) - float(expected)) / max(abs(float(expected)), 1e-300)

