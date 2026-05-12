"""Placeholder postprocessing step for Milestone 1."""

from __future__ import annotations

from pathlib import Path
import json


def summarize_artifacts(artifacts_dir: str | Path, output_path: str | Path) -> dict:
    artifacts_path = Path(artifacts_dir)
    output = Path(output_path)

    files = sorted(p.relative_to(artifacts_path).as_posix() for p in artifacts_path.rglob("*") if p.is_file())
    summary = {
        "artifacts_dir": str(artifacts_path),
        "file_count": len(files),
        "files": files,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Summarize pulled artifacts (stub)")
    parser.add_argument("artifacts_dir")
    parser.add_argument("output_path")
    args = parser.parse_args()

    summarize_artifacts(args.artifacts_dir, args.output_path)
