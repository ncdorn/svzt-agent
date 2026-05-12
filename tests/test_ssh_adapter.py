from __future__ import annotations

import pytest

from svztagent.core.errors import CommandRejectedError
from svztagent.hpc.executor import CommandExecutor
from svztagent.hpc.interfaces import ExecutionMode
from svztagent.hpc.ssh import SshRemoteExecAdapter


def _adapter() -> SshRemoteExecAdapter:
    return SshRemoteExecAdapter(
        user="ndorn",
        host="sherlock.stanford.edu",
        executor=CommandExecutor(mode=ExecutionMode.DRY_RUN),
    )


def test_ssh_command_construction_allowlisted():
    adapter = _adapter()
    result = adapter.run(["mkdir", "-p", "/scratch/users/ndorn/svzt_runs/run-001"])
    assert result.argv[0] == "ssh"
    assert result.argv[1] == "ndorn@sherlock.stanford.edu"
    assert "mkdir -p /scratch/users/ndorn/svzt_runs/run-001" in result.argv[2]


def test_ssh_rejects_unsafe_command():
    adapter = _adapter()
    with pytest.raises(CommandRejectedError):
        adapter.run(["rm", "-rf", "/scratch/users/ndorn/svzt_runs/run-001"])


def test_ssh_rejects_shell_control_token():
    adapter = _adapter()
    with pytest.raises(CommandRejectedError):
        adapter.run(["mkdir", "&&", "/tmp/x"])
