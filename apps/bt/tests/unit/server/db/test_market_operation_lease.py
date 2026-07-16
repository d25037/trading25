from __future__ import annotations

import multiprocessing
from pathlib import Path
import time

import pytest

from src.infrastructure.db.market.market_operation_lease import (
    MarketOperationLease,
    MarketOperationLeaseError,
)


def _wait_for_exclusive(
    root: str,
    ready: multiprocessing.Queue[bool],
    queue: multiprocessing.Queue[float],
) -> None:
    ready.put(True)
    started = time.monotonic()
    with MarketOperationLease.acquire(Path(root), exclusive=True, blocking=True):
        queue.put(time.monotonic() - started)


def test_exclusive_lease_blocks_other_process_until_release(tmp_path: Path) -> None:
    root = tmp_path / "data"
    root.mkdir()
    context = multiprocessing.get_context("spawn")
    ready: multiprocessing.Queue[bool] = context.Queue()
    queue: multiprocessing.Queue[float] = context.Queue()
    held = MarketOperationLease.acquire(root, exclusive=True)
    process = context.Process(target=_wait_for_exclusive, args=(str(root), ready, queue))
    process.start()
    try:
        assert ready.get(timeout=5)
        time.sleep(0.2)
        assert queue.empty()
        held.release()
        process.join(timeout=5)
        assert process.exitcode == 0
        assert queue.get(timeout=1) >= 0.15
    finally:
        held.release()
        if process.is_alive():
            process.terminate()
            process.join(timeout=5)


def test_inherited_exclusive_descriptor_retains_fence(tmp_path: Path) -> None:
    root = tmp_path / "data"
    root.mkdir()
    parent = MarketOperationLease.acquire(root, exclusive=True)
    inherited_fd = parent.detach_for_inheritance()
    adopted = MarketOperationLease.adopt_inherited(root, inherited_fd)
    with pytest.raises(MarketOperationLeaseError, match="held by another process"):
        MarketOperationLease.acquire(root, exclusive=False)
    adopted.release()
    with MarketOperationLease.acquire(root, exclusive=False):
        pass
