from __future__ import annotations

import pytest

from svztagent.core.errors import CommandRejectedError
from svztagent.hpc.executor import CommandExecutor
from svztagent.hpc.interfaces import ExecutionMode
from svztagent.hpc.ssh import RemoteCommandPolicy, SshRemoteExecAdapter


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


def test_ssh_custom_policy_supports_git_pull_with_remote_cwd():
    adapter = SshRemoteExecAdapter(
        user="ndorn",
        host="sherlock",
        executor=CommandExecutor(mode=ExecutionMode.DRY_RUN),
        policy=RemoteCommandPolicy(allowed_commands={"git", "pip"}),
    )

    result = adapter.run(["git", "pull"], cwd="/home/users/ndorn/svZeroDTrees")

    assert result.argv[0] == "ssh"
    assert result.argv[1] == "ndorn@sherlock"
    assert "cd /home/users/ndorn/svZeroDTrees && git pull" in result.argv[2]
