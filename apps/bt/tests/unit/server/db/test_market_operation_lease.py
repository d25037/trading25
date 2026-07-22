from __future__ import annotations

import multiprocessing
from pathlib import Path
import time

from src.infrastructure.db.market.market_operation_lease import MarketOperationLease


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


def test_retained_runtime_fd_adoption_is_not_exposed() -> None:
    assert not hasattr(MarketOperationLease, "resolve_inherited_data_root")
    assert not hasattr(MarketOperationLease, "adopt_inherited")
    assert not hasattr(MarketOperationLease, "detach_for_inheritance")
