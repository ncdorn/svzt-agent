"""CLI entrypoint for svzt-agent."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

from svztagent.config.load import detect_workspace_root
from svztagent.campaigns.seed_sweep import (
    plan_seed_sweep_campaign,
    run_seed_sweep_campaign,
    summarize_seed_sweep_campaign,
    write_seed_sweep_slides,
)
from svztagent.core.errors import SvztError
from svztagent.core.manifest import update_run_progress
from svztagent.core.paths import build_local_run_paths
from svztagent.core.state import RunLifecycleState
from svztagent.hpc.interfaces import ExecutionMode
from svztagent.workflows.postop import (
    run_postop,
    select_converged_preop_iteration,
)
from svztagent.workflows.tune_trees import (
    advance_tune_iteration,
    continue_tune_iteration,
    fetch_run_artifacts,
    init_run_workspace,
    plan_tune_trees,
    query_run_status,
    render_plan_human,
    run_tune_trees,
    watch_and_auto_advance_tuning,
    watch_run_lifecycle,
)


def _print_progress(message: str) -> None:
    print(message, flush=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="svzt", description="svzt-agent orchestration CLI")
    parser.add_argument(
        "--workspace-root",
        default=None,
        help="Path to workspace root (defaults to SVZ_WORKSPACE_ROOT or auto-detect)",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    init_run = subparsers.add_parser("init-run", help="Create run directory and manifest")
    init_run.add_argument("--cluster", required=True)
    init_run.add_argument("--patient", required=True)
    init_run.add_argument("--run-id", required=True)
    init_run.set_defaults(handler=cmd_init_run)

    plan = subparsers.add_parser("plan", help="Plan workflow actions")
    plan_subparsers = plan.add_subparsers(dest="plan_command", required=True)

    tune = plan_subparsers.add_parser("tune", help="Generate dry-run plan for tune workflow")
    tune.add_argument("--cluster", required=True)
    tune.add_argument("--patient", required=True)
    tune.add_argument("--run-id", required=False)
    tune.set_defaults(handler=cmd_plan_tune)

    run = subparsers.add_parser("run", help="Execute workflows")
    run_subparsers = run.add_subparsers(dest="run_command", required=True)
    run_tune = run_subparsers.add_parser("tune", help="Run tune workflow with HPC adapters")
    run_tune.add_argument("--cluster", required=True)
    run_tune.add_argument("--patient", required=True)
    run_tune.add_argument("--run-id", required=False)
    mode_group = run_tune.add_mutually_exclusive_group()
    mode_group.add_argument("--dry-run", action="store_true", help="Preview commands only (default)")
    mode_group.add_argument("--execute", action="store_true", help="Execute remote operations")
    run_tune.set_defaults(handler=cmd_run_tune)

    run_tune_iter = run_subparsers.add_parser(
        "tune-iter", help="Run one tuning iteration with HPC adapters"
    )
    run_tune_iter.add_argument("--cluster", required=True)
    run_tune_iter.add_argument("--patient", required=True)
    run_tune_iter.add_argument("--run-id", required=True)
    run_tune_iter.add_argument("--iteration", required=False, type=int)
    run_tune_iter.add_argument(
        "--skip-zerod-tuning",
        action="store_true",
        help="Reuse existing iteration 0D tuning artifacts and submit only the 3D stage",
    )
    mode_group_iter = run_tune_iter.add_mutually_exclusive_group()
    mode_group_iter.add_argument(
        "--dry-run", action="store_true", help="Preview commands only (default)"
    )
    mode_group_iter.add_argument(
        "--execute", action="store_true", help="Execute remote operations"
    )
    run_tune_iter.set_defaults(handler=cmd_run_tune_iter)

    run_postop_cmd = run_subparsers.add_parser(
        "postop", help="Run explicit postop simulation from converged preop iteration"
    )
    run_postop_cmd.add_argument("--run-id", required=True)
    mode_group_postop = run_postop_cmd.add_mutually_exclusive_group()
    mode_group_postop.add_argument(
        "--dry-run", action="store_true", help="Preview commands only (default)"
    )
    mode_group_postop.add_argument(
        "--execute", action="store_true", help="Execute remote operations"
    )
    run_postop_cmd.set_defaults(handler=cmd_run_postop)

    preop = subparsers.add_parser("preop", help="Manage converged preop iteration handoff")
    preop_subparsers = preop.add_subparsers(dest="preop_command", required=True)
    preop_select = preop_subparsers.add_parser(
        "select", help="Record the converged preop iteration for downstream postop"
    )
    preop_select.add_argument("--run-id", required=True)
    preop_select.add_argument("--iteration", required=True, type=int)
    preop_select.add_argument("--reason", required=False)
    preop_select.set_defaults(handler=cmd_preop_select)

    status = subparsers.add_parser("status", help="Query scheduler status for a run")
    status.add_argument("run_id")
    status.set_defaults(handler=cmd_status)

    fetch = subparsers.add_parser("fetch", help="Pull configured artifacts for a run")
    fetch.add_argument("run_id")
    fetch.add_argument("--dry-run", action="store_true", help="Preview rsync pull command only")
    fetch.set_defaults(handler=cmd_fetch)

    watch = subparsers.add_parser("watch", help="Watch run scheduler lifecycle to terminal state")
    watch.add_argument("run_id")
    watch.add_argument(
        "--poll-interval-seconds",
        type=int,
        default=None,
        help="Polling interval in seconds (defaults to config/defaults.yaml monitoring value)",
    )
    watch.add_argument(
        "--timeout-seconds",
        type=int,
        default=None,
        help="Optional timeout for watch loop",
    )
    watch.add_argument(
        "--max-polls",
        type=int,
        default=None,
        help="Optional hard limit on scheduler polls",
    )
    watch.add_argument(
        "--fetch-on-complete",
        action="store_true",
        help="Fetch artifacts automatically when terminal state is completed",
    )
    watch.add_argument(
        "--auto-advance",
        action="store_true",
        help="Monitor and auto-submit subsequent tuning iterations until convergence or failure",
    )
    watch.set_defaults(handler=cmd_watch)

    update_progress = subparsers.add_parser(
        "update-progress", help="Update run progress tracker milestone status"
    )
    update_progress.add_argument("--run-id", required=True)
    update_progress.add_argument("--model-id", required=True)
    update_progress.add_argument("--milestone-id", required=True)
    update_progress.add_argument(
        "--status",
        required=True,
        choices=["pending", "in_progress", "completed", "failed"],
    )
    update_progress.add_argument("--note", required=False)
    update_progress.set_defaults(handler=cmd_update_progress)

    advance_iter = subparsers.add_parser(
        "advance-iter",
        help="Advance tuning iteration based on latest decision and optionally submit next iteration",
    )
    advance_iter.add_argument("--run-id", required=True)
    advance_iter.add_argument(
        "--execute",
        action="store_true",
        help="Submit next iteration immediately after advancing",
    )
    advance_iter.set_defaults(handler=cmd_advance_iter)

    continue_cmd = subparsers.add_parser(
        "continue",
        help=(
            "Force-advance a tuning iteration stuck in needs_review due to a driver timeout "
            "and optionally submit the next iteration"
        ),
    )
    continue_cmd.add_argument("run_id")
    continue_cmd.add_argument(
        "--execute",
        action="store_true",
        help="Submit the next iteration immediately after advancing",
    )
    continue_cmd.set_defaults(handler=cmd_continue)

    campaign = subparsers.add_parser("campaign", help="Plan and summarize campaigns")
    campaign_subparsers = campaign.add_subparsers(dest="campaign_command", required=True)
    seed_sweep = campaign_subparsers.add_parser(
        "seed-sweep", help="Compare learned 0D iteration-1 seed strategies"
    )
    seed_sweep_subparsers = seed_sweep.add_subparsers(
        dest="seed_sweep_command", required=True
    )
    seed_plan = seed_sweep_subparsers.add_parser("plan", help="Plan seed-sweep campaign")
    seed_plan.add_argument("--cluster", required=True)
    seed_plan.add_argument("--campaign-id", required=False)
    seed_plan.add_argument("--patients", nargs="+", required=False)
    seed_plan.set_defaults(handler=cmd_campaign_seed_sweep_plan)

    seed_run = seed_sweep_subparsers.add_parser("run", help="Run seed-sweep campaign")
    seed_run.add_argument("campaign_id")
    mode_group_campaign = seed_run.add_mutually_exclusive_group()
    mode_group_campaign.add_argument("--dry-run", action="store_true", help="Preview commands only")
    mode_group_campaign.add_argument("--execute", action="store_true", help="Execute remote operations")
    seed_run.set_defaults(handler=cmd_campaign_seed_sweep_run)

    seed_summary = seed_sweep_subparsers.add_parser(
        "summarize", help="Summarize seed-sweep campaign"
    )
    seed_summary.add_argument("campaign_id")
    seed_summary.set_defaults(handler=cmd_campaign_seed_sweep_summarize)

    seed_slides = seed_sweep_subparsers.add_parser(
        "slides", help="Generate seed-sweep comparison slides"
    )
    seed_slides.add_argument("campaign_id")
    seed_slides.set_defaults(handler=cmd_campaign_seed_sweep_slides)

    return parser


def _resolve_mode(execute: bool) -> ExecutionMode:
    return ExecutionMode.EXECUTE if execute else ExecutionMode.DRY_RUN


def cmd_init_run(args: argparse.Namespace) -> int:
    workspace_root = detect_workspace_root(args.workspace_root)
    paths, _ = init_run_workspace(
        workspace_root=workspace_root,
        cluster_name=args.cluster,
        patient_alias=args.patient,
        run_id=args.run_id,
    )
    print(f"Initialized run workspace: {paths.run_dir}")
    print(f"Manifest: {paths.manifest}")
    return 0


def cmd_plan_tune(args: argparse.Namespace) -> int:
    workspace_root = detect_workspace_root(args.workspace_root)
    plan = plan_tune_trees(
        workspace_root=workspace_root,
        cluster_name=args.cluster,
        patient_alias=args.patient,
        run_id=args.run_id,
    )
    print(render_plan_human(plan))
    print(
        "Plan validation: "
        f"{'PASS' if plan.validation_results.is_valid else 'FAIL'} "
        f"(errors={len(plan.validation_results.errors)}, warnings={len(plan.validation_results.warnings)})"
    )
    local_run_dir = Path(plan.local_run_dir)
    print(f"Plan JSON: {local_run_dir / 'execution_plan.json'}")
    print(f"Plan YAML: {local_run_dir / 'execution_plan.yaml'}")
    return 0


def cmd_run_tune(args: argparse.Namespace) -> int:
    workspace_root = detect_workspace_root(args.workspace_root)
    mode = _resolve_mode(args.execute)
    _print_progress(
        f"[svzt] Starting tune workflow for patient {args.patient} on {args.cluster}"
    )
    result = run_tune_trees(
        workspace_root=workspace_root,
        cluster_name=args.cluster,
        patient_alias=args.patient,
        run_id=args.run_id,
        iteration=None,
        mode=mode,
        progress_callback=_print_progress,
    )
    print(f"Run ID: {result.run_id}")
    print(f"Iteration: {result.iteration}")
    print(f"Mode: {result.mode.value}")
    print(f"Plan: {result.plan_path}")
    print(f"Remote run dir: {result.remote_run_dir}")
    print(f"Remote job script: {result.remote_job_script_path}")
    print(f"Submitted job ID: {result.submitted_job_id}")
    print("Command previews:")
    for argv in result.command_previews:
        print(f"  - {' '.join(argv)}")
    print(f"Next: svzt status {result.run_id}")
    print(f"Next: svzt fetch {result.run_id}")
    return 0


def cmd_run_tune_iter(args: argparse.Namespace) -> int:
    workspace_root = detect_workspace_root(args.workspace_root)
    mode = _resolve_mode(args.execute)
    _print_progress(
        f"[svzt] Starting tune iteration workflow for patient {args.patient} on {args.cluster}"
    )
    result = run_tune_trees(
        workspace_root=workspace_root,
        cluster_name=args.cluster,
        patient_alias=args.patient,
        run_id=args.run_id,
        iteration=args.iteration,
        mode=mode,
        skip_zerod_tuning=args.skip_zerod_tuning,
        progress_callback=_print_progress,
    )
    print(f"Run ID: {result.run_id}")
    print(f"Iteration: {result.iteration}")
    print(f"Mode: {result.mode.value}")
    print(f"Plan: {result.plan_path}")
    print(f"Remote run dir: {result.remote_run_dir}")
    print(f"Remote job script: {result.remote_job_script_path}")
    print(f"Submitted job ID: {result.submitted_job_id}")
    print("Command previews:")
    for argv in result.command_previews:
        print(f"  - {' '.join(argv)}")
    return 0


def cmd_run_postop(args: argparse.Namespace) -> int:
    workspace_root = detect_workspace_root(args.workspace_root)
    mode = _resolve_mode(args.execute)
    result = run_postop(
        workspace_root=workspace_root,
        run_id=args.run_id,
        mode=mode,
    )
    print(f"Run ID: {result.run_id}")
    print(f"Source preop iteration: {result.source_preop_iteration}")
    print(f"Mode: {result.mode.value}")
    print(f"Plan: {result.plan_path}")
    print(f"Remote postop dir: {result.remote_postop_dir}")
    print(f"Remote job script: {result.remote_job_script_path}")
    print(f"Submitted job ID: {result.submitted_job_id}")
    print("Command previews:")
    for argv in result.command_previews:
        print(f"  - {' '.join(argv)}")
    return 0


def cmd_preop_select(args: argparse.Namespace) -> int:
    workspace_root = detect_workspace_root(args.workspace_root)
    result = select_converged_preop_iteration(
        workspace_root=workspace_root,
        run_id=args.run_id,
        iteration=args.iteration,
        reason=args.reason,
    )
    print(f"Run ID: {result.run_id}")
    print(f"Converged preop iteration: {result.iteration}")
    print(f"Selection kind: {result.selection_kind}")
    print(f"Preop dir: {result.remote_preop_dir}")
    print(f"Tuned 0D config: {result.remote_tuned_zerod_config}")
    print(f"Canonical coupler: {result.remote_canonical_coupler}")
    if result.postprocess_job_id:
        print(f"Selected-preop postprocess job ID: {result.postprocess_job_id}")
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    workspace_root = detect_workspace_root(args.workspace_root)
    result = query_run_status(
        workspace_root=workspace_root,
        run_id=args.run_id,
        mode=ExecutionMode.EXECUTE,
    )
    print(f"Run ID: {result.run_id}")
    print(f"Job ID: {result.job_id}")
    print(f"Scheduler source: {result.source}")
    print(f"Raw state: {result.raw_state or '<none>'}")
    print(f"Normalized state: {result.normalized_state.value}")
    print(f"Current iteration: {result.current_iteration} / {result.max_iterations}")
    print(f"Iteration tracker status: {result.tracker_status}")
    print(f"Current stage: {result.stage_label}")
    print(f"Stage detail: {result.stage_detail or '<none>'}")
    print(f"Decision: {result.decision or 'pending'}")
    if result.needs_review_reason:
        print(f"Needs review reason: {result.needs_review_reason}")
        if "timeout" in result.needs_review_reason.lower():
            print(
                f"  Tip: iteration driver timed out — run 'svzt continue {result.run_id} [--execute]' "
                "to force-advance to the next iteration"
            )
    print(f"Progress artifact source: {result.progress_source}")
    if result.preop_job_id:
        state = (
            result.preop_job_state_normalized.value
            if result.preop_job_state_normalized is not None
            else result.preop_job_state_raw or "<none>"
        )
        print(f"Preop job: {result.preop_job_id} ({state})")
    if result.postop_job_id:
        state = (
            result.postop_job_state_normalized.value
            if result.postop_job_state_normalized is not None
            else result.postop_job_state_raw or "<none>"
        )
        print(f"Postop job: {result.postop_job_id} ({state})")
    warnings = result.progress_warnings or []
    if warnings:
        print("Warnings:")
        for warning in warnings:
            print(f"  - {warning}")
    if result.failure_error_log_path:
        print(f"Failure error log: {result.failure_error_log_path}")
    if result.failure_error_log_tail:
        print("Failure error tail:")
        print(result.failure_error_log_tail)
    return 0


def cmd_fetch(args: argparse.Namespace) -> int:
    workspace_root = detect_workspace_root(args.workspace_root)
    result = fetch_run_artifacts(
        workspace_root=workspace_root,
        run_id=args.run_id,
        mode=ExecutionMode.DRY_RUN if args.dry_run else ExecutionMode.EXECUTE,
    )
    print(f"Run ID: {result.run_id}")
    print(f"Remote run dir: {result.remote_run_dir}")
    print(f"Local output dir: {result.local_output_dir}")
    print(f"Pull patterns: {', '.join(result.pull_patterns)}")
    print(f"Command preview: {' '.join(result.command_preview)}")
    return 0


def cmd_watch(args: argparse.Namespace) -> int:
    workspace_root = detect_workspace_root(args.workspace_root)
    if args.auto_advance:
        result = watch_and_auto_advance_tuning(
            workspace_root=workspace_root,
            run_id=args.run_id,
            mode=ExecutionMode.EXECUTE,
            poll_interval_seconds=args.poll_interval_seconds,
            timeout_seconds=args.timeout_seconds,
            max_polls=args.max_polls,
            fetch_on_complete=args.fetch_on_complete,
        )
        print(f"Run ID: {result.run_id}")
        print("Auto-advance summary")
        for record in result.iterations:
            print(
                f"- iteration={record.iteration} "
                f"terminal_state={record.terminal_state.value} "
                f"decision={record.decision or '<none>'} "
                f"action={record.advance_action} "
                f"submitted_job_id={record.submitted_job_id or '<none>'}"
            )
        print(f"Final action: {result.final_action}")
        print(f"Tracker status: {result.tracker_status}")
        print(f"Final terminal state: {result.final_terminal_state.value}")
        if result.final_action in {
            "scheduler_terminal_failure",
            "max_iter_failed",
            "needs_review_pause",
        }:
            return 1
        return 0

    result = watch_run_lifecycle(
        workspace_root=workspace_root,
        run_id=args.run_id,
        mode=ExecutionMode.EXECUTE,
        poll_interval_seconds=args.poll_interval_seconds,
        timeout_seconds=args.timeout_seconds,
        max_polls=args.max_polls,
        fetch_on_complete=args.fetch_on_complete,
    )

    print(f"Run ID: {result.run_id}")
    print(f"Job ID: {result.job_id}")
    print(f"Initial state: {result.initial_state.value}")
    for observation in result.observations:
        if observation.previous_state == observation.normalized_state:
            continue
        raw_state = observation.raw_state or "<none>"
        print(
            f"[{observation.observed_at}] "
            f"{observation.previous_state.value} -> {observation.normalized_state.value} "
            f"(raw={raw_state} source={observation.scheduler_source} poll={observation.poll_count})"
        )

    print("")
    print("Terminal summary")
    print(f"- normalized_state: {result.final_state.value}")
    print(f"- raw_scheduler_state: {result.raw_scheduler_state or '<none>'}")
    if result.terminal_reason:
        print(f"- terminal_reason: {result.terminal_reason}")
    print(f"- job_id: {result.job_id}")
    print(f"- local_run_dir: {result.local_run_dir}")
    print(f"- remote_run_dir: {result.remote_run_dir}")
    print(f"- local_logs_dir: {result.local_logs_dir}")
    if result.remote_logs_dir:
        print(f"- remote_logs_dir: {result.remote_logs_dir}")
    if result.job_script_path:
        print(f"- job_script_path: {result.job_script_path}")
    print(f"- fetch_attempted: {'yes' if result.fetch_attempted else 'no'}")
    if result.fetch_attempted:
        print(
            "- fetch_succeeded: "
            f"{'yes' if result.fetch_succeeded else 'no'}"
        )
    if result.fetch_error:
        print(f"- fetch_error: {result.fetch_error}")

    if result.terminal_state in {RunLifecycleState.FAILED, RunLifecycleState.CANCELLED}:
        return 1
    return 0


def cmd_update_progress(args: argparse.Namespace) -> int:
    workspace_root = detect_workspace_root(args.workspace_root)
    local_paths = build_local_run_paths(workspace_root, args.run_id)
    updated = update_run_progress(
        manifest_path=local_paths.manifest,
        model_id=args.model_id,
        milestone_id=args.milestone_id,
        status=args.status,
        note=args.note,
    )
    print(f"Updated progress tracker for run {updated.run_id}")
    print(f"Manifest: {local_paths.manifest}")
    print(f"Progress tracker: {local_paths.progress_tracker}")
    return 0


def cmd_advance_iter(args: argparse.Namespace) -> int:
    workspace_root = detect_workspace_root(args.workspace_root)
    result = advance_tune_iteration(
        workspace_root=workspace_root,
        run_id=args.run_id,
        execute=args.execute,
    )
    print(f"Run ID: {result.run_id}")
    print(f"Previous iteration: {result.previous_iteration}")
    print(f"Next iteration: {result.next_iteration if result.next_iteration is not None else '<none>'}")
    print(f"Tracker status: {result.tracker_status}")
    print(f"Action: {result.action}")
    if result.submitted_job_id:
        print(f"Submitted job ID: {result.submitted_job_id}")
    return 0


def cmd_continue(args: argparse.Namespace) -> int:
    workspace_root = detect_workspace_root(args.workspace_root)
    result = continue_tune_iteration(
        workspace_root=workspace_root,
        run_id=args.run_id,
        execute=args.execute,
    )
    print(f"Run ID: {result.run_id}")
    print(f"Previous iteration: {result.previous_iteration}")
    print(f"Next iteration: {result.next_iteration if result.next_iteration is not None else '<none>'}")
    print(f"Tracker status: {result.tracker_status}")
    print(f"Action: {result.action}")
    if result.submitted_job_id:
        print(f"Submitted job ID: {result.submitted_job_id}")
    if result.action in {"timeout_bypassed_no_submit", "timeout_bypassed_and_submitted"}:
        print("Note: previous iteration was force-advanced (driver timeout bypass)")
        if not args.execute:
            print(f"Next: svzt continue {args.run_id} --execute")
    return 0


def cmd_campaign_seed_sweep_plan(args: argparse.Namespace) -> int:
    workspace_root = detect_workspace_root(args.workspace_root)
    manifest = plan_seed_sweep_campaign(
        workspace_root=workspace_root,
        cluster_name=args.cluster,
        patients=args.patients,
        campaign_id=args.campaign_id,
    )
    print(f"Campaign ID: {manifest['campaign_id']}")
    print(f"Child runs: {len(manifest['child_runs'])}")
    print(
        "Campaign manifest: "
        f"{workspace_root / 'runs' / 'campaigns' / manifest['campaign_id'] / 'campaign_manifest.yaml'}"
    )
    return 0


def cmd_campaign_seed_sweep_run(args: argparse.Namespace) -> int:
    workspace_root = detect_workspace_root(args.workspace_root)
    manifest = run_seed_sweep_campaign(
        workspace_root=workspace_root,
        campaign_id=args.campaign_id,
        mode=_resolve_mode(args.execute),
    )
    print(f"Campaign ID: {manifest['campaign_id']}")
    for result in manifest.get("last_run_results", []):
        print(
            f"- {result['patient']} {result['case_id']}: "
            f"{result['mode']} job={result['submitted_job_id']}"
        )
    return 0


def cmd_campaign_seed_sweep_summarize(args: argparse.Namespace) -> int:
    workspace_root = detect_workspace_root(args.workspace_root)
    rows = summarize_seed_sweep_campaign(
        workspace_root=workspace_root,
        campaign_id=args.campaign_id,
    )
    print(f"Campaign ID: {args.campaign_id}")
    print(f"Rows: {len(rows)}")
    print(
        "Summary: "
        f"{workspace_root / 'runs' / 'campaigns' / args.campaign_id / 'seed_sweep_summary.csv'}"
    )
    return 0


def cmd_campaign_seed_sweep_slides(args: argparse.Namespace) -> int:
    workspace_root = detect_workspace_root(args.workspace_root)
    path = write_seed_sweep_slides(
        workspace_root=workspace_root,
        campaign_id=args.campaign_id,
    )
    print(f"Slides: {path}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        handler = args.handler
        return handler(args)
    except SvztError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
