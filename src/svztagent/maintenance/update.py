"""Repository and Sherlock software refresh helpers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from svztagent.config.load import (
    detect_workspace_root,
    load_workspace_config,
    resolve_repository_locations,
)
from svztagent.core.errors import ConfigError
from svztagent.hpc.executor import CommandExecutor
from svztagent.hpc.interfaces import CommandResult, ExecutionMode, RemoteExecAdapter
from svztagent.hpc.ssh import RemoteCommandPolicy, SshRemoteExecAdapter

DEFAULT_REMOTE_USER = "ndorn"
DEFAULT_REMOTE_HOST = "sherlock"
DEFAULT_REMOTE_SVZERODTREES_PATH = "/home/users/ndorn/svZeroDTrees"


@dataclass(frozen=True)
class LocalRepoUpdateResult:
    name: str
    path: str
    branch: str
    had_changes: bool
    committed: bool
    pushed: bool
    command_results: list[CommandResult]


@dataclass(frozen=True)
class SoftwareUpdateResult:
    mode: ExecutionMode
    repositories: list[LocalRepoUpdateResult]
    remote_results: list[CommandResult]
    remote_user: str
    remote_host: str
    remote_svzerodtrees_path: str


def _detect_local_svzt_agent_repo() -> Path:
    for candidate in Path(__file__).resolve().parents:
        if (candidate / "pyproject.toml").exists() and (candidate / "src" / "svztagent").exists():
            return candidate
    raise ConfigError(
        "Could not detect the local svzt-agent checkout. "
        "Run this command from an editable/source checkout, or pass --workspace-root."
    )


def _resolve_repo_paths(workspace_root: str | Path | None) -> tuple[Path, Path]:
    if workspace_root is not None:
        detected_root = detect_workspace_root(workspace_root)
        config = load_workspace_config(detected_root)
        repos = resolve_repository_locations(config, detected_root)
        svzt_agent = repos.get("svzt_agent")
        svzerodtrees = repos.get("svZeroDTrees")
        if not svzt_agent:
            raise ConfigError("Could not resolve repository path for 'svzt_agent'")
        if not svzerodtrees:
            raise ConfigError("Could not resolve repository path for 'svZeroDTrees'")
        return Path(svzt_agent), Path(svzerodtrees)

    svzt_agent_repo = _detect_local_svzt_agent_repo()
    svzerodtrees_repo = (svzt_agent_repo.parent / "svZeroDTrees").resolve()
    if not svzerodtrees_repo.exists():
        raise ConfigError(
            f"Could not find sibling svZeroDTrees checkout at {svzerodtrees_repo}. "
            "Pass --workspace-root if the repo is configured elsewhere."
        )
    return svzt_agent_repo.resolve(), svzerodtrees_repo


def _git_current_branch(repo_path: Path, inspection_executor: CommandExecutor) -> str:
    result = inspection_executor.run_local(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
        cwd=repo_path,
    )
    branch = result.stdout.strip()
    if not branch or branch == "HEAD":
        raise ConfigError(f"Repository is on a detached HEAD: {repo_path}")
    return branch


def _git_has_changes(repo_path: Path, inspection_executor: CommandExecutor) -> bool:
    result = inspection_executor.run_local(["git", "status", "--short"], cwd=repo_path)
    return bool(result.stdout.strip())


def sync_local_repo(
    repo_name: str,
    repo_path: str | Path,
    commit_message: str,
    mode: ExecutionMode,
    inspection_executor: CommandExecutor | None = None,
) -> LocalRepoUpdateResult:
    resolved_repo_path = Path(repo_path).expanduser().resolve()
    if not resolved_repo_path.exists():
        raise ConfigError(f"Repository path does not exist for {repo_name}: {resolved_repo_path}")

    inspector = inspection_executor or CommandExecutor(mode=ExecutionMode.EXECUTE)
    executor = CommandExecutor(mode=mode)

    branch = _git_current_branch(resolved_repo_path, inspector)
    had_changes = _git_has_changes(resolved_repo_path, inspector)
    command_results: list[CommandResult] = []
    committed = False
    pushed = False

    if had_changes:
        command_results.append(executor.run_local(["git", "add", "-A"], cwd=resolved_repo_path))
        command_results.append(
            executor.run_local(["git", "commit", "-m", commit_message], cwd=resolved_repo_path)
        )
        committed = True

    command_results.append(
        executor.run_local(["git", "push", "origin", branch], cwd=resolved_repo_path)
    )
    pushed = True

    return LocalRepoUpdateResult(
        name=repo_name,
        path=str(resolved_repo_path),
        branch=branch,
        had_changes=had_changes,
        committed=committed,
        pushed=pushed,
        command_results=command_results,
    )


def _build_remote_exec(
    *,
    remote_user: str,
    remote_host: str,
    mode: ExecutionMode,
) -> SshRemoteExecAdapter:
    policy = RemoteCommandPolicy(allowed_commands={"git", "pip"})
    return SshRemoteExecAdapter(
        user=remote_user,
        host=remote_host,
        executor=CommandExecutor(mode=mode),
        policy=policy,
    )


def update_software(
    *,
    commit_message: str,
    mode: ExecutionMode,
    workspace_root: str | Path | None = None,
    remote_user: str = DEFAULT_REMOTE_USER,
    remote_host: str = DEFAULT_REMOTE_HOST,
    remote_svzerodtrees_path: str = DEFAULT_REMOTE_SVZERODTREES_PATH,
    remote_exec: RemoteExecAdapter | None = None,
    inspection_executor: CommandExecutor | None = None,
) -> SoftwareUpdateResult:
    svzt_agent_repo, svzerodtrees_repo = _resolve_repo_paths(workspace_root)
    inspector = inspection_executor or CommandExecutor(mode=ExecutionMode.EXECUTE)
    repo_results = [
        sync_local_repo(
            "svzt-agent",
            svzt_agent_repo,
            commit_message=commit_message,
            mode=mode,
            inspection_executor=inspector,
        ),
        sync_local_repo(
            "svZeroDTrees",
            svzerodtrees_repo,
            commit_message=commit_message,
            mode=mode,
            inspection_executor=inspector,
        ),
    ]

    remote_adapter = remote_exec or _build_remote_exec(
        remote_user=remote_user,
        remote_host=remote_host,
        mode=mode,
    )
    remote_results = [
        remote_adapter.run(["git", "pull"], cwd=remote_svzerodtrees_path),
        remote_adapter.run(["pip", "install", "-e", remote_svzerodtrees_path]),
    ]

    return SoftwareUpdateResult(
        mode=mode,
        repositories=repo_results,
        remote_results=remote_results,
        remote_user=remote_user,
        remote_host=remote_host,
        remote_svzerodtrees_path=remote_svzerodtrees_path,
    )
