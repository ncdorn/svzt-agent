from __future__ import annotations

from pathlib import Path
import subprocess

from svztagent.hpc.fake import FakeRemoteExecAdapter
from svztagent.hpc.interfaces import ExecutionMode
from svztagent.maintenance import update as update_module
from svztagent.maintenance.update import (
    DEFAULT_REMOTE_SVZERODTREES_PATH,
    sync_local_repo,
    update_software,
)


def _run_git(argv: list[str], cwd: Path | None = None) -> str:
    proc = subprocess.run(
        argv,
        cwd=str(cwd) if cwd is not None else None,
        capture_output=True,
        text=True,
        check=True,
    )
    return proc.stdout.strip()


def _init_repo(tmp_path: Path, name: str) -> Path:
    bare_repo = tmp_path / f"{name}-origin.git"
    worktree = tmp_path / name
    _run_git(["git", "init", "--bare", str(bare_repo)])
    _run_git(["git", "clone", str(bare_repo), str(worktree)])
    _run_git(["git", "config", "user.email", "codex@example.com"], cwd=worktree)
    _run_git(["git", "config", "user.name", "Codex"], cwd=worktree)
    (worktree / "README.md").write_text(f"{name}\n", encoding="utf-8")
    _run_git(["git", "add", "README.md"], cwd=worktree)
    _run_git(["git", "commit", "-m", "initial"], cwd=worktree)
    branch = _run_git(["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=worktree)
    _run_git(["git", "push", "origin", branch], cwd=worktree)
    return worktree


def test_sync_local_repo_commits_and_pushes_changes(tmp_path):
    repo = _init_repo(tmp_path, "svzt-agent")
    branch = _run_git(["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=repo)
    (repo / "README.md").write_text("updated\n", encoding="utf-8")

    result = sync_local_repo(
        "svzt-agent",
        repo,
        commit_message="update software",
        mode=ExecutionMode.EXECUTE,
    )

    assert result.branch == branch
    assert result.had_changes is True
    assert result.committed is True
    assert result.pushed is True
    assert _run_git(["git", "log", "-1", "--pretty=%s"], cwd=repo) == "update software"
    assert [command.argv[:2] for command in result.command_results] == [
        ["git", "add"],
        ["git", "commit"],
        ["git", "push"],
    ]


def test_sync_local_repo_pushes_without_new_commit_when_clean(tmp_path):
    repo = _init_repo(tmp_path, "svZeroDTrees")

    result = sync_local_repo(
        "svZeroDTrees",
        repo,
        commit_message="unused message",
        mode=ExecutionMode.EXECUTE,
    )

    assert result.had_changes is False
    assert result.committed is False
    assert result.pushed is True
    assert len(result.command_results) == 1
    assert result.command_results[0].argv[:2] == ["git", "push"]


def test_update_software_refreshes_remote_after_local_repo_sync(tmp_path, monkeypatch):
    svzt_agent_repo = _init_repo(tmp_path, "svzt-agent")
    svzerodtrees_repo = _init_repo(tmp_path, "svZeroDTrees")
    (svzt_agent_repo / "README.md").write_text("svzt-agent updated\n", encoding="utf-8")
    (svzerodtrees_repo / "README.md").write_text("svZeroDTrees updated\n", encoding="utf-8")

    fake_remote = FakeRemoteExecAdapter()
    monkeypatch.setattr(update_module, "_detect_local_svzt_agent_repo", lambda: svzt_agent_repo)

    result = update_software(
        commit_message="refresh both repos",
        mode=ExecutionMode.EXECUTE,
        remote_exec=fake_remote,
    )

    assert [repo.name for repo in result.repositories] == ["svzt-agent", "svZeroDTrees"]
    assert all(repo.committed for repo in result.repositories)
    assert fake_remote.calls == [
        (["git", "pull"], DEFAULT_REMOTE_SVZERODTREES_PATH),
        (["pip", "install", "-e", DEFAULT_REMOTE_SVZERODTREES_PATH], None),
    ]
    assert _run_git(["git", "log", "-1", "--pretty=%s"], cwd=svzt_agent_repo) == "refresh both repos"
    assert _run_git(["git", "log", "-1", "--pretty=%s"], cwd=svzerodtrees_repo) == "refresh both repos"
