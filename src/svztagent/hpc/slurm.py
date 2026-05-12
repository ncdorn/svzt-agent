"""Slurm scheduler adapter for submit/status/accounting/cancel operations."""

from __future__ import annotations

from dataclasses import dataclass

from svztagent.core.errors import SchedulerResponseError
from svztagent.core.paths import validate_remote_write_path
from svztagent.hpc.interfaces import CommandResult, SchedulerAdapter, SchedulerStatusResult, SubmitResult
from svztagent.hpc.ssh import SshRemoteExecAdapter


@dataclass(frozen=True)
class SlurmSubmitOptions:
    job_name: str
    account: str | None = None
    partition: str | None = None
    wall_time: str | None = None
    mem: str | None = None
    cpus: str | None = None


def build_sbatch_command(script_path: str, options: SlurmSubmitOptions) -> list[str]:
    cmd = ["sbatch", "--parsable", "--job-name", options.job_name]
    if options.account:
        cmd.extend(["--account", options.account])
    if options.partition:
        cmd.extend(["--partition", options.partition])
    if options.wall_time:
        cmd.extend(["--time", options.wall_time])
    if options.mem:
        cmd.extend(["--mem", options.mem])
    if options.cpus:
        cmd.extend(["--cpus-per-task", options.cpus])
    cmd.append(script_path)
    return cmd


def build_squeue_command(job_id: str) -> list[str]:
    return ["squeue", "--job", job_id, "--noheader", "--format", "%T"]


def build_sacct_command(job_id: str) -> list[str]:
    return ["sacct", "--jobs", job_id, "--format", "State", "--noheader", "--parsable2"]


def build_scancel_command(job_id: str) -> list[str]:
    return ["scancel", job_id]


def parse_sbatch_job_id(stdout: str) -> str:
    line = stdout.strip().splitlines()[0] if stdout.strip() else ""
    job_id = line.split(";")[0].strip()
    if not job_id:
        raise SchedulerResponseError(f"unable to parse job id from sbatch output: '{stdout}'")
    return job_id


def _first_non_empty_line(stdout: str) -> str | None:
    for line in stdout.splitlines():
        cleaned = line.strip()
        if cleaned:
            return cleaned
    return None


class SlurmSchedulerAdapter(SchedulerAdapter):
    def __init__(
        self,
        remote_exec: SshRemoteExecAdapter,
        runs_root: str,
        submit_options: SlurmSubmitOptions,
    ):
        self.remote_exec = remote_exec
        self.runs_root = runs_root
        self.submit_options = submit_options

    def submit(self, job_script_path: str) -> SubmitResult:
        validate_remote_write_path(job_script_path, self.runs_root)
        command = build_sbatch_command(job_script_path, self.submit_options)
        result = self.remote_exec.run(command)
        if result.dry_run:
            job_id = f"dryrun-{self.submit_options.job_name}"
            return SubmitResult(job_id=job_id, command=result)
        job_id = parse_sbatch_job_id(result.stdout)
        return SubmitResult(job_id=job_id, command=result)

    def status(self, job_id: str) -> SchedulerStatusResult:
        command = build_squeue_command(job_id)
        result = self.remote_exec.run(command)
        raw = _first_non_empty_line(result.stdout)
        return SchedulerStatusResult(
            job_id=job_id,
            raw_state=raw,
            source="squeue",
            command=result,
        )

    def accounting(self, job_id: str) -> SchedulerStatusResult:
        command = build_sacct_command(job_id)
        result = self.remote_exec.run(command)
        first_line = _first_non_empty_line(result.stdout)
        raw = first_line.split("|")[0].strip() if first_line else None
        return SchedulerStatusResult(
            job_id=job_id,
            raw_state=raw,
            source="sacct",
            command=result,
        )

    def cancel(self, job_id: str) -> CommandResult:
        return self.remote_exec.run(build_scancel_command(job_id))
