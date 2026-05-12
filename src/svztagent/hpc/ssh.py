"""SSH-backed remote command execution with allowlist validation."""

from __future__ import annotations

from dataclasses import dataclass, field
import shlex

from svztagent.core.errors import CommandRejectedError, PlanValidationError
from svztagent.hpc.executor import CommandExecutor
from svztagent.hpc.interfaces import CommandResult, RemoteExecAdapter

_FORBIDDEN_TOKENS = {";", "&&", "||", "|", ">", "<"}


@dataclass(frozen=True)
class RemoteCommandPolicy:
    allowed_commands: set[str] = field(
        default_factory=lambda: {"sbatch", "squeue", "sacct", "scancel", "mkdir", "test", "bash"}
    )


def validate_remote_command(argv: list[str], policy: RemoteCommandPolicy) -> None:
    if not argv:
        raise CommandRejectedError("remote command cannot be empty")

    primary = argv[0]
    if primary not in policy.allowed_commands:
        allowed = ", ".join(sorted(policy.allowed_commands))
        raise CommandRejectedError(f"remote command '{primary}' is not in allowlist: {allowed}")

    for token in argv:
        if token in _FORBIDDEN_TOKENS:
            raise CommandRejectedError(
                f"remote command contains forbidden shell token '{token}'"
            )
        if "\n" in token or "\r" in token:
            raise CommandRejectedError("remote command token cannot contain newline characters")


def _ssh_destination(user: str, host: str) -> str:
    if not user:
        raise CommandRejectedError("ssh user must be non-empty")
    if not host:
        raise CommandRejectedError("ssh host must be non-empty")
    return f"{user}@{host}"


class SshRemoteExecAdapter(RemoteExecAdapter):
    def __init__(
        self,
        user: str,
        host: str,
        executor: CommandExecutor,
        policy: RemoteCommandPolicy | None = None,
    ):
        self.user = user
        self.host = host
        self.executor = executor
        self.policy = policy or RemoteCommandPolicy()

    def validate_command(self, command: list[str]) -> None:
        validate_remote_command(command, self.policy)

    def build_ssh_argv(self, command: list[str], cwd: str | None = None) -> list[str]:
        self.validate_command(command)
        destination = _ssh_destination(self.user, self.host)
        remote_cmd = shlex.join(command)
        if cwd:
            if not cwd.startswith("/"):
                raise CommandRejectedError("remote cwd must be an absolute path")
            remote_cmd = f"cd {shlex.quote(cwd)} && {remote_cmd}"
        return ["ssh", destination, remote_cmd]

    def run(self, command: list[str], cwd: str | None = None) -> CommandResult:
        argv = self.build_ssh_argv(command, cwd=cwd)
        return self.executor.run_local(argv)


class SshCommandBuilder:
    """Backward-compatible command builder used by legacy plan-only tests."""

    def __init__(self, policy: RemoteCommandPolicy | None = None):
        self.policy = policy or RemoteCommandPolicy()

    def build_remote_command(self, user: str, host: str, argv: list[str]) -> list[str]:
        try:
            validate_remote_command(argv, self.policy)
        except CommandRejectedError as exc:
            raise PlanValidationError(str(exc)) from exc
        destination = _ssh_destination(user, host)
        remote_cmd = shlex.join(argv)
        return ["ssh", destination, remote_cmd]
