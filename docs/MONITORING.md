# Monitoring

## Command
- `svzt watch <run-id> [--poll-interval-seconds N] [--timeout-seconds N] [--max-polls N] [--fetch-on-complete]`
- `svzt watch <run-id> ... [--auto-advance]`

`watch` runs a synchronous polling loop and exits when the run reaches a terminal lifecycle state.

With `--auto-advance`, `watch` performs a closed-loop iteration controller:
1. Monitor the current submitted iteration to terminal scheduler state.
2. Pull `iteration_decision.json` and `iteration_metrics.json` from remote iteration results into local `runs/<run_id>/iterations/iter-XX/results/`.
3. Advance + submit next iteration when decision is `not_close`.
4. Stop on `converged`, `failed_max_iter`, or terminal scheduler `failed/cancelled`.
   A converged preop iteration is ready for explicit postop handoff; postop is
   not auto-submitted by the tuning driver.

The iteration driver computes MPA pressure gate metrics from the last cardiac
period of `mpa_pressure_vs_time.csv`, using the staged inflow period when it is
available. This keeps monitoring and auto-advance decisions aligned with the
steady-state pressure window used for reduced RRI regeneration.

## Lifecycle normalization
Slurm scheduler states are normalized into internal lifecycle states:
- active: `pending`, `running`, `unknown`
- terminal: `completed`, `failed`, `cancelled`

The monitor records both the raw scheduler state and normalized state in the run manifest.

## Polling policy
1. Poll `squeue` for job state.
2. If state is missing/unknown, fall back to `sacct`.
3. Record poll metadata and lifecycle transitions.
4. Stop on terminal states.

Defaults:
- poll interval: `30` seconds
- minimum poll interval: `5` seconds
- timeout: disabled unless set
- max polls: disabled unless set
- In auto-advance mode, `timeout` and `max polls` apply per iteration watch cycle.

## Post-terminal behavior
- `--fetch-on-complete` triggers deterministic artifact fetch after `completed`.
- Optional fetch-on-failure is controlled by `defaults.monitoring.fetch_on_failure`.
- Monitor always prints a terminal summary including job/log path metadata and fetch outcome.

## Preop 3D Success Signal
For preop BC tuning, the earliest positive success signal for an iteration is
the child preop 3D solver printing successful timestep progress in its `.o*`
log. The matching `.e*` log is the first place to inspect solver-side errors.
If `.o*` shows timestep progress and `.e*` has no fatal error invalidating that
progress, the iteration has reached the successful-running state even though
clinical comparison, final scheduler completion, converged-preop selection, or
postop work may still be pending.

## Failure surface
Terminal failure output includes:
- normalized terminal state
- raw scheduler state
- terminal reason (if known)
- job ID
- local/remote run directories
- log and job script locations (if known)
- fetch attempt result
