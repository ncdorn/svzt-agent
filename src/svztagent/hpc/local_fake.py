"""Backward-compatible imports for legacy fake adapter names."""

from __future__ import annotations

from svztagent.hpc.fake import FakeFileTransferAdapter, FakeRemoteExecAdapter, FakeSchedulerAdapter

LocalFakeTransfer = FakeFileTransferAdapter
LocalFakeRemoteExec = FakeRemoteExecAdapter
LocalFakeScheduler = FakeSchedulerAdapter
