# Adaptation Comparison TODO

## Goal

Complete the adaptation comparison across the three planned conditions:

- `M1`: stabilized WSS-only structured-tree adaptation with frozen thickness
- `M2`: territory-level homeostatic WSS + pressure/IMS structured-tree adaptation
- `M3`: CWSS+IMS ODE adaptation behind the same workflow contract

Primary target: finish end-to-end comparison on postop-ready runs using the explicit `svzt` adaptation workflow and the `adapt-benchmark` campaign helpers.

## Current Status

- `M1` debugging is nearly complete and is the current focus.
- `adapt-benchmark` plan/run/summarize plumbing exists in `svzt-agent`.
- Comparison artifacts are expected at:
  - `runs/<run_id>/adaptation/from-iter-XX/<model>/results/baseline_vs_adapted_comparison.json`
  - `runs/campaigns/<campaign_id>/adapt_benchmark_summary.json`
  - `runs/campaigns/<campaign_id>/adapt_benchmark_summary.csv`

## Success Criteria

- `M1`, `M2`, and `M3` each run successfully from a converged preop iteration with a completed postop run.
- Each model produces a valid `baseline_vs_adapted_comparison.json`.
- Campaign summary rows exist for all selected run/model pairs.
- We can compare `baseline_mae`, `adapted_mae`, `mae_delta`, and `rpa_split` error changes across `M1/M2/M3`.
- We can compare pre-op, post-op, and adapted RPA split for each condition.
- We have a short written conclusion on which adaptation condition is most promising and what still needs tuning.
- The run-scoped CFD results JSON stays current as new adaptation results arrive.

## Immediate Tasks

- [ ] Finish `M1` debugging on the representative postop-ready run.
- [ ] Confirm `M1` writes all expected artifacts:
  - `adaptation_summary.json`
  - `adaptation_metrics.json`
  - `baseline_vs_adapted_comparison.json`
  - adapted CMM outputs and postprocess metadata
- [ ] Review the `M1` comparison deltas and make sure the signs/magnitudes are sensible.
- [ ] Confirm pre-op, post-op, and adapted RPA split are all captured for the `M1` check run.
- [ ] Freeze or record the `M1` parameter payload used for the comparison run.
- [ ] Refresh the run-scoped CFD results JSON after each completed adaptation result import.

## Model-by-Model Checklist

### `M1`

- [ ] Dry-run `svzt run adapt --run-id <run_id> --model M1`
- [ ] Execute `M1` on the selected source run
- [ ] Pull/fetch adaptation outputs if needed
- [ ] Inspect logs for manager events, adapted CMM submission, and completion
- [ ] Verify comparison JSON contents against clinical targets
- [ ] Record pre-op, post-op, and adapted RPA split for the `M1` run
- [ ] Update the CFD results JSON with the newest `postop_adapted` state for `M1`
- [ ] Decide whether `M1` is ready for campaign inclusion without more code changes

### `M2`

- [ ] Validate the configured `M2` parameter payload
- [ ] Run dry-run and execute paths for `M2`
- [ ] Confirm LPA/RPA territory updates and adapted coupler export complete cleanly
- [ ] Verify postprocess and comparison JSON generation
- [ ] Record pre-op, post-op, and adapted RPA split for each `M2` run
- [ ] Update the CFD results JSON with the newest `postop_adapted` state for `M2`
- [ ] Note whether `M2` improves metrics over baseline and over `M1`

### `M3`

- [ ] Validate the configured `M3` parameter payload
- [ ] Run dry-run and execute paths for `M3`
- [ ] Confirm CWSS+IMS adaptation completes without solver/runtime failures
- [ ] Verify postprocess and comparison JSON generation
- [ ] Record pre-op, post-op, and adapted RPA split for each `M3` run
- [ ] Update the CFD results JSON with the newest `postop_adapted` state for `M3`
- [ ] Note whether `M3` improves metrics over baseline and over `M1/M2`

## Benchmark Campaign

- [ ] Pick the final set of source `run_id`s for comparison
- [ ] Plan the campaign with all three models:

```bash
svzt campaign adapt-benchmark plan \
  --campaign-id <campaign_id> \
  --run-ids <run_id_1> <run_id_2> ... \
  --models M1 M2 M3 \
  --benchmark-mode predict
```

- [ ] Dry-run the benchmark campaign execution path
- [ ] Execute the campaign
- [ ] Summarize results:

```bash
svzt campaign adapt-benchmark summarize <campaign_id>
```

- [ ] Confirm summary JSON/CSV contains one row per run/model pair
- [ ] Refresh the finalized CFD results JSON for each run after the selected adapted result is available

## Comparison Review

- [ ] Build a compact comparison table for `M1/M2/M3`
- [ ] Check:
  - `baseline_mae`
  - `adapted_mae`
  - `mae_delta`
  - pre-op `rpa_split`
  - post-op `rpa_split`
  - adapted `rpa_split`
  - baseline vs adapted `rpa_split` error
- [ ] Flag any runs where adaptation makes the fit worse
- [ ] Separate workflow bugs from model-behavior problems
- [ ] Make sure the finalized CFD results JSON agrees with the comparison table values
- [ ] Write a short conclusion/recommendation for next tuning steps

## CFD Results JSON

- [ ] Keep the finalized run-scoped CFD results JSON updated as results come in
- [ ] Rebuild it after postop baseline artifacts are finalized
- [ ] Rebuild it again after each accepted adapted result
- [ ] Confirm `states.preop_tuned`, `states.postop_baseline`, and `states.postop_adapted` match the latest artifacts
- [ ] Confirm flow split fields in the JSON reflect the pre-op, post-op, and adapted RPA split values used in the comparison
- [ ] Preserve curated measured fields when refreshing run-derived values
- [ ] Record which model/parameter set is represented by the current `postop_adapted` state

```bash
svzt postprocess cfd-results --run-id <run_id> [--source-json <existing_json>] [--overwrite]
```

## Notes

- Start with `benchmark_mode=predict`; only use `retrospective_fit` if we explicitly decide we need that comparison.
- Keep this file focused on finishing the first end-to-end adaptation comparison, not on long-term adaptation roadmap items.
