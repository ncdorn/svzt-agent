from __future__ import annotations

from pathlib import Path
import json

import pytest

from svztagent.workflows.postprocess import _stacked_centerline_timeseries_python_source


vtk = pytest.importorskip("vtk")


def _write_centerline_frame(
    path: Path,
    *,
    pressure_values: tuple[float, float],
    velocity_values: tuple[float, float],
) -> None:
    points = vtk.vtkPoints()
    points.InsertNextPoint(0.0, 0.0, 0.0)
    points.InsertNextPoint(1.0, 0.0, 0.0)

    line = vtk.vtkLine()
    line.GetPointIds().SetId(0, 0)
    line.GetPointIds().SetId(1, 1)

    lines = vtk.vtkCellArray()
    lines.InsertNextCell(line)

    poly = vtk.vtkPolyData()
    poly.SetPoints(points)
    poly.SetLines(lines)

    branch_id = vtk.vtkIntArray()
    branch_id.SetName("BranchId")
    branch_id.SetNumberOfValues(2)
    branch_id.SetValue(0, 7)
    branch_id.SetValue(1, 7)
    poly.GetPointData().AddArray(branch_id)

    pressure = vtk.vtkDoubleArray()
    pressure.SetName("Pressure")
    pressure.SetNumberOfValues(2)
    for index, value in enumerate(pressure_values):
        pressure.SetValue(index, value)
    poly.GetPointData().AddArray(pressure)

    velocity = vtk.vtkDoubleArray()
    velocity.SetName("Velocity")
    velocity.SetNumberOfValues(2)
    for index, value in enumerate(velocity_values):
        velocity.SetValue(index, value)
    poly.GetPointData().AddArray(velocity)

    writer = vtk.vtkXMLPolyDataWriter()
    writer.SetFileName(str(path))
    writer.SetInputData(poly)
    assert writer.Write() == 1


def _load_embedded_writer_namespace() -> dict[str, object]:
    namespace: dict[str, object] = {"Path": Path, "json": json}
    exec(_stacked_centerline_timeseries_python_source(), namespace)
    return namespace


def test_stacked_centerline_writer_uses_one_geometry_with_timestep_arrays(tmp_path: Path) -> None:
    frame0 = tmp_path / "frame0.vtp"
    frame1 = tmp_path / "frame1.vtp"
    _write_centerline_frame(frame0, pressure_values=(10.0, 20.0), velocity_values=(1.0, 2.0))
    _write_centerline_frame(frame1, pressure_values=(30.0, 40.0), velocity_values=(3.0, 4.0))

    metadata_path = tmp_path / "resistance_map_metadata.json"
    metadata_path.write_text(
        json.dumps(
            {
                "selected_frames": [
                    {
                        "path": str(frame0),
                        "time_s": 0.8,
                        "timestep_id": 2000,
                        "source_frame_path": "/scratch/run/result_2000.vtu",
                    },
                    {
                        "path": str(frame1),
                        "time_s": 0.808,
                        "timestep_id": 2020,
                        "source_frame_path": "/scratch/run/result_2020.vtu",
                    },
                ],
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    namespace = _load_embedded_writer_namespace()
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    suite_metadata_path = output_dir / "postprocess_suite_metadata.json"
    result = {"resistance_map": {"metadata_json": str(metadata_path)}}

    stack_result = namespace["_write_stacked_centerline_timeseries"](
        output_dir=output_dir,
        suite_metadata_path=suite_metadata_path,
        result=result,
    )

    reader = vtk.vtkXMLPolyDataReader()
    reader.SetFileName(str(output_dir / "centerline_timeseries_last_cycle.vtp"))
    reader.Update()
    poly = reader.GetOutput()

    assert poly.GetNumberOfPoints() == 2
    assert poly.GetNumberOfCells() == 1
    assert poly.GetPointData().HasArray("BranchId") == 1
    assert poly.GetPointData().HasArray("Pressure") == 0
    assert poly.GetPointData().HasArray("Velocity") == 0
    assert poly.GetPointData().HasArray("pressure_0") == 1
    assert poly.GetPointData().HasArray("pressure_1") == 1
    assert poly.GetPointData().HasArray("velocity_0") == 1
    assert poly.GetPointData().HasArray("velocity_1") == 1

    pressure0 = poly.GetPointData().GetArray("pressure_0")
    pressure1 = poly.GetPointData().GetArray("pressure_1")
    assert [pressure0.GetTuple1(i) for i in range(2)] == [10.0, 20.0]
    assert [pressure1.GetTuple1(i) for i in range(2)] == [30.0, 40.0]

    assert poly.GetFieldData().GetArray("processed_timestep_time_s") is None
    assert poly.GetFieldData().GetArray("processed_timestep_id") is None

    assert stack_result["point_count"] == 2
    assert stack_result["cell_count"] == 1
    assert stack_result["zerod_point_arrays"] == [
        "pressure_0",
        "velocity_0",
        "pressure_1",
        "velocity_1",
    ]
