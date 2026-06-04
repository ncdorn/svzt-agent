from __future__ import annotations

import json

from svztagent.campaigns.adapt_benchmark import (
    plan_adapt_benchmark_campaign,
    summarize_adapt_benchmark_campaign,
)

from test_adapt_workflow import _prepare_adaptable_run


def test_plan_and_summarize_adapt_benchmark_campaign(sample_config_files):
    paths = _prepare_adaptable_run(sample_config_files, run_id="run-adapt-benchmark")
    comparison_dir = (
        paths.run_dir / "adaptation" / "from-iter-03" / "m2" / "results"
    )
    comparison_dir.mkdir(parents=True, exist_ok=True)
    (comparison_dir / "baseline_vs_adapted_comparison.json").write_text(
        json.dumps(
            {
                "baseline": {"mae": 2.0, "errors": {"rpa_split": 0.1}},
                "adapted": {"mae": 1.0, "errors": {"rpa_split": 0.05}},
                "improvement": {"mae_delta": 1.0},
            }
        ),
        encoding="utf-8",
    )

    manifest = plan_adapt_benchmark_campaign(
        workspace_root=sample_config_files,
        run_ids=["run-adapt-benchmark"],
        campaign_id="adapt-benchmark-test",
        models=["M2"],
        benchmark_mode="predict",
    )
    assert manifest["campaign_id"] == "adapt-benchmark-test"
    assert len(manifest["child_runs"]) == 1

    rows = summarize_adapt_benchmark_campaign(
        workspace_root=sample_config_files,
        campaign_id="adapt-benchmark-test",
    )
    assert len(rows) == 1
    assert rows[0]["model"] == "M2"
    assert rows[0]["adapted_mae"] == 1.0
    assert rows[0]["mae_delta"] == 1.0
