"""Rsync-backed file transfer adapter with safety policy checks."""

from __future__ import annotations

from pathlib import Path
from pathlib import PurePosixPath

from svztagent.core.errors import PathPolicyError
from svztagent.core.paths import validate_remote_write_path
from svztagent.hpc.executor import CommandExecutor
from svztagent.hpc.interfaces import CommandResult, FileTransferAdapter, SyncDirection
from svztagent.hpc.ssh import SshRemoteExecAdapter


def _with_trailing_slash(path: str) -> str:
    return path if path.endswith("/") else f"{path}/"


def _remote_spec(user: str, host: str, path: str) -> str:
    return f"{user}@{host}:{path}"


class RsyncFileTransferAdapter(FileTransferAdapter):
    def __init__(
        self,
        user: str,
        host: str,
        runs_root: str,
        patient_data_root: str,
        remote_exec: SshRemoteExecAdapter,
        executor: CommandExecutor,
        permanent_data_root: str | None = None,
        default_include: list[str] | None = None,
        default_exclude: list[str] | None = None,
    ):
        self.user = user
        self.host = host
        self.runs_root = runs_root
        self.patient_data_root = patient_data_root
        self.permanent_data_root = permanent_data_root
        self.remote_exec = remote_exec
        self.executor = executor
        self.default_include = default_include or []
        self.default_exclude = default_exclude or []

    def _base_args(self, include: list[str] | None = None, exclude: list[str] | None = None) -> list[str]:
        args = ["rsync", "-az"]
        if self.executor.mode.value == "dry_run":
            args.append("--dry-run")

        include_rules = include if include is not None else self.default_include
        exclude_rules = exclude if exclude is not None else self.default_exclude
        for pattern in include_rules:
            args.extend(["--include", pattern])
        for pattern in exclude_rules:
            args.extend(["--exclude", pattern])
        return args

    def _validate_remote_runs_path(self, remote_path: str) -> str:
        normalized = str(PurePosixPath(remote_path))
        validate_remote_write_path(
            path=normalized,
            runs_root=self.runs_root,
            patient_data_root=self.patient_data_root,
        )
        return normalized

    def _validate_remote_read_path(self, remote_path: str) -> str:
        normalized = str(PurePosixPath(remote_path))
        allowed_roots = [self.runs_root, self.patient_data_root]
        if self.permanent_data_root:
            allowed_roots.append(self.permanent_data_root)

        for root in allowed_roots:
            candidate = str(PurePosixPath(root))
            if normalized == candidate or normalized.startswith(f"{candidate}/"):
                return normalized

        raise PathPolicyError(
            f"remote read path '{remote_path}' must stay under one of: {', '.join(sorted(set(allowed_roots)))}"
        )

    def build_push_command(self, local_path: str, remote_path: str) -> list[str]:
        return [*self._base_args(), local_path, remote_path]

    def build_pull_command(self, remote_path: str, local_path: str) -> list[str]:
        return [*self._base_args(), remote_path, _with_trailing_slash(local_path)]

    def ensure_remote_dir(self, path: str) -> CommandResult:
        safe_path = self._validate_remote_runs_path(path)
        return self.remote_exec.run(["mkdir", "-p", safe_path])

    def push(self, local_path: str, remote_path: str) -> CommandResult:
        local = str(Path(local_path))
        safe_remote_path = self._validate_remote_runs_path(remote_path)
        remote = _remote_spec(self.user, self.host, safe_remote_path)
        return self.executor.run_local(self.build_push_command(local, remote))

    def pull(self, remote_path: str, local_path: str) -> CommandResult:
        safe_remote_path = self._validate_remote_read_path(remote_path)
        local = str(Path(local_path))
        remote = _remote_spec(self.user, self.host, safe_remote_path)
        return self.executor.run_local(self.build_pull_command(remote, local))

    def sync(
        self,
        local_dir: str,
        remote_dir: str,
        include: list[str] | None = None,
        exclude: list[str] | None = None,
        direction: SyncDirection = SyncDirection.PUSH,
    ) -> CommandResult:
        if direction == SyncDirection.PUSH:
            safe_remote_dir = self._validate_remote_runs_path(remote_dir)
        else:
            safe_remote_dir = self._validate_remote_read_path(remote_dir)
        remote = _remote_spec(self.user, self.host, safe_remote_dir)
        local = str(Path(local_dir))
        args = self._base_args(include=include, exclude=exclude)

        if direction == SyncDirection.PUSH:
            argv = [*args, _with_trailing_slash(local), remote]
        else:
            argv = [*args, _with_trailing_slash(remote), _with_trailing_slash(local)]
        return self.executor.run_local(argv)


class RsyncCommandBuilder:
    """Backward-compatible builder for legacy tests and plan previews."""

    def __init__(self, include_patterns: list[str] | None = None, exclude_patterns: list[str] | None = None):
        self.include_patterns = include_patterns or []
        self.exclude_patterns = exclude_patterns or []

    def _base_args(self) -> list[str]:
        args = ["rsync", "-az", "--delete", "--dry-run"]
        for pattern in self.include_patterns:
            args.extend(["--include", pattern])
        for pattern in self.exclude_patterns:
            args.extend(["--exclude", pattern])
        return args

    def build_push_command(self, local_path: str, remote_path: str) -> list[str]:
        return [*self._base_args(), _with_trailing_slash(local_path), remote_path]

    def build_pull_command(self, remote_path: str, local_path: str) -> list[str]:
        return [*self._base_args(), remote_path, _with_trailing_slash(local_path)]
