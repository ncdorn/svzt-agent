from importlib.resources import files


def test_packaged_job_template_is_available() -> None:
    template = files("svztagent").joinpath("templates", "slurm", "job_template.sh")

    assert template.is_file()
    assert "#!/usr/bin/env bash" in template.read_text(encoding="utf-8")
