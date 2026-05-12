"""Fake adapters used for local unit tests without network dependencies."""

from __future__ import annotations

from svztagent.hpc.interfaces import (
    CommandResult,
    FileTransferAdapter,
    RemoteExecAdapter,
    SchedulerAdapter,
    SchedulerStatusResult,
    SubmitResult,
    SyncDirection,
)


class FakeRemoteExecAdapter(RemoteExecAdapter):
    def __init__(self):
        self.calls: list[tuple[list[str], str | None]] = []
        self.responses: list[CommandResult] = []

    def queue_response(self, result: CommandResult) -> None:
        self.responses.append(result)

    def validate_command(self, command: list[str]) -> None:
        if not command:
            raise ValueError("command must be non-empty")

    def run(self, command: list[str], cwd: str | None = None) -> CommandResult:
        self.validate_command(command)
        self.calls.append((list(command), cwd))
        if self.responses:
            return self.responses.pop(0)
        return CommandResult(
            argv=list(command),
            returncode=0,
            stdout="DRY-RUN: fake remote exec",
            stderr="",
            dry_run=True,
        )


class FakeFileTransferAdapter(FileTransferAdapter):
    def __init__(self):
        self.ensure_calls: list[str] = []
        self.push_calls: list[tuple[str, str]] = []
        self.pull_calls: list[tuple[str, str]] = []
        self.sync_calls: list[tuple[str, str, list[str], list[str], SyncDirection]] = []

    def ensure_remote_dir(self, path: str) -> CommandResult:
        self.ensure_calls.append(path)
        return CommandResult(
            argv=["ssh", "fake@host", f"mkdir -p {path}"],
            returncode=0,
            stdout="DRY-RUN: fake ensure_remote_dir",
            stderr="",
            dry_run=True,
        )

    def push(self, local_path: str, remote_path: str) -> CommandResult:
        self.push_calls.append((local_path, remote_path))
        return CommandResult(
            argv=["rsync", local_path, remote_path],
            returncode=0,
            stdout="DRY-RUN: fake push",
            stderr="",
            dry_run=True,
        )

    def pull(self, remote_path: str, local_path: str) -> CommandResult:
        self.pull_calls.append((remote_path, local_path))
        return CommandResult(
            argv=["rsync", remote_path, local_path],
            returncode=0,
            stdout="DRY-RUN: fake pull",
            stderr="",
            dry_run=True,
        )

    def sync(
        self,
        local_dir: str,
        remote_dir: str,
        include: list[str] | None = None,
        exclude: list[str] | None = None,
        direction: SyncDirection = SyncDirection.PUSH,
    ) -> CommandResult:
        include_rules = include or []
        exclude_rules = exclude or []
        self.sync_calls.append((local_dir, remote_dir, include_rules, exclude_rules, direction))
        return CommandResult(
            argv=["rsync", local_dir, remote_dir],
            returncode=0,
            stdout="DRY-RUN: fake sync",
            stderr="",
            dry_run=True,
        )


class FakeSchedulerAdapter(SchedulerAdapter):
    def __init__(self):
        self.submit_calls: list[str] = []
        self.status_calls: list[str] = []
        self.accounting_calls: list[str] = []
        self.cancel_calls: list[str] = []
        self._status_results: list[SchedulerStatusResult] = []
        self._accounting_results: list[SchedulerStatusResult] = []
        self._status_result = SchedulerStatusResult(
            job_id="unknown",
            raw_state="PENDING",
            source="squeue",
            command=CommandResult(argv=["squeue"], returncode=0, stdout="PENDING", stderr="", dry_run=True),
        )
        self._accounting_result = SchedulerStatusResult(
            job_id="unknown",
            raw_state="COMPLETED",
            source="sacct",
            command=CommandResult(argv=["sacct"], returncode=0, stdout="COMPLETED", stderr="", dry_run=True),
        )
        self._submit_result = SubmitResult(
            job_id="dryrun-fake",
            command=CommandResult(argv=["sbatch"], returncode=0, stdout="dryrun-fake", stderr="", dry_run=True),
        )

    def set_submit_result(self, result: SubmitResult) -> None:
        self._submit_result = result

    def set_status_result(self, result: SchedulerStatusResult) -> None:
        self._status_result = result

    def set_accounting_result(self, result: SchedulerStatusResult) -> None:
        self._accounting_result = result

    def queue_status_results(self, results: list[SchedulerStatusResult]) -> None:
        self._status_results.extend(results)

    def queue_accounting_results(self, results: list[SchedulerStatusResult]) -> None:
        self._accounting_results.extend(results)

    def submit(self, job_script_path: str) -> SubmitResult:
        self.submit_calls.append(job_script_path)
        return self._submit_result

    def status(self, job_id: str) -> SchedulerStatusResult:
        self.status_calls.append(job_id)
        if self._status_results:
            return self._status_results.pop(0)
        return self._status_result

    def accounting(self, job_id: str) -> SchedulerStatusResult:
        self.accounting_calls.append(job_id)
        if self._accounting_results:
            return self._accounting_results.pop(0)
        return self._accounting_result

    def cancel(self, job_id: str) -> CommandResult:
        self.cancel_calls.append(job_id)
        return CommandResult(
            argv=["scancel", job_id],
            returncode=0,
            stdout="",
            stderr="",
            dry_run=True,
        )
