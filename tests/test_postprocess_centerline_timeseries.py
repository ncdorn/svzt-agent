from __future__ import annotations

from pathlib import Path
import sys
import types

import pytest

from svztagent.workflows.postprocess import (
    _run_postprocess_suite_with_optional_camera,
    _stacked_centerline_timeseries_python_source,
)


def _load_upstream_contract_namespace(
    monkeypatch: pytest.MonkeyPatch,
    validator,
) -> dict[str, object]:
    post_processing = types.ModuleType("svzerodtrees.post_processing")
    post_processing.validate_centerline_timeseries_descriptor = validator
    package = types.ModuleType("svzerodtrees")
    package.post_processing = post_processing
    monkeypatch.setitem(sys.modules, "svzerodtrees", package)
    monkeypatch.setitem(sys.modules, "svzerodtrees.post_processing", post_processing)

    namespace: dict[str, object] = {"Path": Path}
    exec(_stacked_centerline_timeseries_python_source(), namespace)
    return namespace


def test_generated_timeseries_source_delegates_to_upstream_public_validator() -> None:
    source = _stacked_centerline_timeseries_python_source()

    assert "from svzerodtrees.post_processing import validate_centerline_timeseries_descriptor" in source
    assert "validate_centerline_timeseries_descriptor(suite_metadata_path)" in source
    assert "import vtk" not in source
    assert "import shutil" not in source
    assert "selected_frames" not in source
    compile(source, "<postprocess-upstream-contract>", "exec")


def test_upstream_consumer_validates_suite_descriptor_without_reimplementing_schema(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    descriptor_path = tmp_path / "postprocess_suite_metadata.json"
    descriptor_path.write_text("{}", encoding="utf-8")
    calls: list[Path] = []

    def validator(path):
        calls.append(Path(path))
        return {"artifact": {}}

    namespace = _load_upstream_contract_namespace(monkeypatch, validator)
    result = {
        "status": "completed",
        "centerline_timeseries": {"descriptor": str(descriptor_path)},
    }
    assert namespace["_consume_upstream_suite_descriptor"](
        suite_metadata_path=descriptor_path,
        result=result,
    ) is result
    assert calls == [descriptor_path]


def test_upstream_consumer_rejects_descriptor_path_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    descriptor_path = tmp_path / "postprocess_suite_metadata.json"
    descriptor_path.write_text("{}", encoding="utf-8")
    namespace = _load_upstream_contract_namespace(monkeypatch, lambda _path: {})

    with pytest.raises(RuntimeError, match="different from the requested path"):
        namespace["_consume_upstream_suite_descriptor"](
            suite_metadata_path=descriptor_path,
            result={
                "status": "completed",
                "centerline_timeseries": {"descriptor": str(tmp_path / "other.json")},
            },
        )


def test_camera_retry_is_limited_to_unexpected_camera_keyword_errors() -> None:
    calls: list[dict[str, object]] = []

    def old_suite(**kwargs):
        calls.append(kwargs)
        if "camera_offset_dir" in kwargs:
            raise TypeError("got an unexpected keyword argument 'camera_offset_dir'")
        return {"status": "completed"}

    kwargs = {"camera_offset_dir": [1.0, 0.0, 0.0], "simulation_dir": "/tmp/sim"}
    assert _run_postprocess_suite_with_optional_camera(old_suite, kwargs) == {
        "status": "completed"
    }
    assert calls == [kwargs, {"simulation_dir": "/tmp/sim"}]


def test_camera_retry_does_not_mask_other_type_errors() -> None:
    def broken_suite(**_kwargs):
        raise TypeError("invalid clinical_targets value")

    with pytest.raises(TypeError, match="invalid clinical_targets"):
        _run_postprocess_suite_with_optional_camera(
            broken_suite,
            {"camera_offset_dir": [1.0, 0.0, 0.0]},
        )


def test_env_activation_hooks_tolerate_unset_variables_under_nounset(tmp_path):
    import shutil
    import subprocess

    from svztagent.workflows.postprocess import render_env_activation_hooks

    bash = shutil.which("bash")
    if bash is None:
        pytest.skip("bash not available")
    # Stand-in for a login file that reads an unset variable, like /etc/bashrc's PS1.
    rc = tmp_path / "fake_bashrc"
    rc.write_text('if [ -z "$SVZT_TEST_UNSET_VAR" ]; then :; fi\n', encoding="utf-8")
    hooks = render_env_activation_hooks([f"source {rc}"])
    script = tmp_path / "job.sh"
    script.write_text(
        f"set -euo pipefail\n{hooks}\necho ok\necho \"$SVZT_TEST_STILL_UNSET\"\n",
        encoding="utf-8",
    )
    proc = subprocess.run([bash, str(script)], capture_output=True, text=True)
    assert proc.stdout.splitlines()[0] == "ok"
    # nounset is restored after the hooks.
    assert proc.returncode != 0 and "SVZT_TEST_STILL_UNSET" in proc.stderr
    assert render_env_activation_hooks([]) == ""
