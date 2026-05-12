"""Local command execution helper with first-class dry-run support."""

from __future__ import annotations

from pathlib import Path
import subprocess

from svztagent.core.errors import AdapterExecutionError
from svztagent.hpc.interfaces import CommandResult, ExecutionMode


class CommandExecutor:
    def __init__(self, mode: ExecutionMode = ExecutionMode.DRY_RUN):
        self.mode = mode

    def run_local(self, argv: list[str], cwd: str | Path | None = None) -> CommandResult:
        if self.mode == ExecutionMode.DRY_RUN:
            return CommandResult(
                argv=list(argv),
                returncode=0,
                stdout="DRY-RUN: command not executed",
                stderr="",
                dry_run=True,
            )

        resolved_cwd: str | None
        if cwd is None:
            resolved_cwd = None
        else:
            resolved_cwd = str(Path(cwd))

        proc = subprocess.run(
            argv,
            cwd=resolved_cwd,
            capture_output=True,
            text=True,
            check=False,
        )
        result = CommandResult(
            argv=list(argv),
            returncode=proc.returncode,
            stdout=proc.stdout.strip(),
            stderr=proc.stderr.strip(),
            dry_run=False,
        )
        if proc.returncode != 0:
            raise AdapterExecutionError(
                argv=result.argv,
                returncode=result.returncode,
                stdout=result.stdout,
                stderr=result.stderr,
            )
        return result
