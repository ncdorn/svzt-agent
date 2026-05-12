"""Custom exceptions for svzt-agent."""


class SvztError(Exception):
    """Base exception for orchestrator failures."""


class ConfigError(SvztError):
    """Raised when workspace configuration is invalid."""


class PathPolicyError(SvztError):
    """Raised when a path policy boundary is violated."""


class PlanValidationError(SvztError):
    """Raised when an execution plan is invalid."""


class CliUsageError(SvztError):
    """Raised for invalid CLI argument combinations."""


class UnsafePathError(PathPolicyError):
    """Raised when a path fails strict execution safety checks."""


class CommandRejectedError(SvztError):
    """Raised when a command violates adapter allowlist rules."""


class AdapterExecutionError(SvztError):
    """Raised when a command execution adapter returns non-zero."""

    def __init__(self, argv: list[str], returncode: int, stdout: str, stderr: str):
        self.argv = list(argv)
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        command = " ".join(argv)
        super().__init__(
            f"command failed (returncode={returncode}): {command}\nstdout={stdout}\nstderr={stderr}"
        )


class SchedulerResponseError(SvztError):
    """Raised when scheduler output cannot be parsed safely."""


class InvalidStateTransitionError(SvztError):
    """Raised when a lifecycle transition is not permitted."""


class MissingJobIdError(SvztError):
    """Raised when a run manifest is missing a usable scheduler job id."""


class SchedulerLookupError(SvztError):
    """Raised when scheduler status/accounting lookups fail."""


class WatchTimeoutError(SvztError):
    """Raised when watch polling exceeds configured timeout."""


class WatchMaxPollsExceededError(SvztError):
    """Raised when watch polling exceeds configured poll count."""
