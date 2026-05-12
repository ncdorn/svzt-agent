"""Typed ports for HPC execution concerns."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum


class ExecutionMode(str, Enum):
    DRY_RUN = "dry_run"
    EXECUTE = "execute"


class SyncDirection(str, Enum):
    PUSH = "push"
    PULL = "pull"


@dataclass(frozen=True)
class CommandResult:
    argv: list[str]
    returncode: int
    stdout: str
    stderr: str
    dry_run: bool


@dataclass(frozen=True)
class SubmitResult:
    job_id: str
    command: CommandResult


@dataclass(frozen=True)
class SchedulerStatusResult:
    job_id: str
    raw_state: str | None
    source: str
    command: CommandResult


class FileTransferAdapter(ABC):
    @abstractmethod
    def ensure_remote_dir(self, path: str) -> CommandResult:
        raise NotImplementedError

    @abstractmethod
    def push(self, local_path: str, remote_path: str) -> CommandResult:
        raise NotImplementedError

    @abstractmethod
    def pull(self, remote_path: str, local_path: str) -> CommandResult:
        raise NotImplementedError

    @abstractmethod
    def sync(
        self,
        local_dir: str,
        remote_dir: str,
        include: list[str] | None = None,
        exclude: list[str] | None = None,
        direction: SyncDirection = SyncDirection.PUSH,
    ) -> CommandResult:
        raise NotImplementedError


class RemoteExecAdapter(ABC):
    @abstractmethod
    def run(self, command: list[str], cwd: str | None = None) -> CommandResult:
        raise NotImplementedError

    @abstractmethod
    def validate_command(self, command: list[str]) -> None:
        raise NotImplementedError


class SchedulerAdapter(ABC):
    @abstractmethod
    def submit(self, job_script_path: str) -> SubmitResult:
        raise NotImplementedError

    @abstractmethod
    def status(self, job_id: str) -> SchedulerStatusResult:
        raise NotImplementedError

    @abstractmethod
    def accounting(self, job_id: str) -> SchedulerStatusResult:
        raise NotImplementedError

    @abstractmethod
    def cancel(self, job_id: str) -> CommandResult:
        raise NotImplementedError
