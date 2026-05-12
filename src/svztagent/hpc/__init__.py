"""HPC adapters and interfaces."""

from svztagent.hpc.executor import CommandExecutor
from svztagent.hpc.fake import FakeFileTransferAdapter, FakeRemoteExecAdapter, FakeSchedulerAdapter
from svztagent.hpc.interfaces import (
    CommandResult,
    ExecutionMode,
    FileTransferAdapter,
    RemoteExecAdapter,
    SchedulerAdapter,
    SchedulerStatusResult,
    SubmitResult,
    SyncDirection,
)
from svztagent.hpc.rsync import RsyncFileTransferAdapter
from svztagent.hpc.slurm import SlurmSchedulerAdapter, SlurmSubmitOptions
from svztagent.hpc.ssh import RemoteCommandPolicy, SshRemoteExecAdapter

__all__ = [
    "CommandExecutor",
    "CommandResult",
    "ExecutionMode",
    "FakeFileTransferAdapter",
    "FakeRemoteExecAdapter",
    "FakeSchedulerAdapter",
    "FileTransferAdapter",
    "RemoteCommandPolicy",
    "RemoteExecAdapter",
    "RsyncFileTransferAdapter",
    "SchedulerAdapter",
    "SchedulerStatusResult",
    "SlurmSchedulerAdapter",
    "SlurmSubmitOptions",
    "SubmitResult",
    "SshRemoteExecAdapter",
    "SyncDirection",
]
