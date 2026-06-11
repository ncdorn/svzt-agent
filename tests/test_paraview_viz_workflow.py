from __future__ import annotations

from svztagent.workflows.paraview_viz import _render_pvpython_script


def test_render_pvpython_script_uses_locked_pulmonary_postprocess_style():
    script = _render_pvpython_script(
        simulation_dir="/scratch/example/preop",
        output_dir="/scratch/example/results/paraview_viz",
        cycle_duration_s=0.8,
        dt=0.0005,
        image_resolution=[1920, 1080],
        camera_offset_dir=[0.25, -0.5, 0.75],
        camera_view_up=[0.0, 0.0, 1.0],
        pressure_field="Pressure",
        velocity_field="Velocity",
        wss_field="WSS",
        displacement_field="Displacement",
    )

    assert "SCALAR_BAR_POSITION = [0.885, 0.2]" in script
    assert "SCALAR_BAR_LENGTH = 0.66" in script
    assert "SCALAR_BAR_THICKNESS = 32" in script
    assert "SCALAR_BAR_TITLE_FONT_SIZE = 16" in script
    assert "SCALAR_BAR_LABEL_FONT_SIZE = 14" in script
    assert "SCALAR_BAR_TEXT_COLOR = [0.0, 0.0, 0.0]" in script
    assert 'view.OrientationAxesVisibility = 0' in script
    assert 'view.CenterAxesVisibility = 0' in script
    assert 'scalar_bar.WindowLocation = "Any Location"' in script
    assert 'scalar_bar.Orientation = "Vertical"' in script
    assert 'scalar_bar.TitleColor = SCALAR_BAR_TEXT_COLOR' in script
    assert 'scalar_bar.LabelColor = SCALAR_BAR_TEXT_COLOR' in script
    assert 'SaveScreenshot(str(out_path), view, TransparentBackground=1)' in script
    assert 'SaveScreenshot(str(fp), view, TransparentBackground=1)' in script
    assert 'otf.Points = [sr[0], 0.0, 0.5, 0.0, sr[1], 1.0, 0.5, 0.0]' in script
