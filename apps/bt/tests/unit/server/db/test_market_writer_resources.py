from __future__ import annotations

import multiprocessing
from pathlib import Path
import shutil
import time

import pytest

from src.infrastructure.db.market.market_db import MarketDb
from src.infrastructure.db.market.market_operation_lease import (
    MarketOperationLease,
    MarketOperationLeaseError,
)
from src.infrastructure.db.market.market_source_identity import (
    MarketSourceIdentityError,
    inspect_market_source_identity,
)
from src.infrastructure.db.market.market_writer_resources import (
    MarketWriterResourceFactory,
)


def _open_waiting_writer(
    data_root: str,
    market_root: str,
    ready: multiprocessing.Queue[bool],
    result: multiprocessing.Queue[tuple[int, str | None]],
) -> None:
    ready.put(True)
    session = MarketWriterResourceFactory(
        data_root=Path(data_root),
        market_root=Path(market_root),
    ).open_existing()
    result.put(
        (
            session.identity.inode,
            session.handles.market_db.get_sync_metadata("test_generation"),
        )
    )
    token = session.close_writable_handles()
    read_only = session.reopen_read_only_and_release(token)
    read_only.close()


def test_market_db_writable_open_requires_factory_token(tmp_path: Path) -> None:
    with pytest.raises(PermissionError, match="writer resource factory"):
        MarketDb(str(tmp_path / "market.duckdb"), read_only=False)


def test_reset_and_open_v4_holds_lease_until_handles_close_and_reopens_read_only(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    market_root = data_root / "market-timeseries"
    factory = MarketWriterResourceFactory(data_root=data_root, market_root=market_root)

    session = factory.reset_and_open_v4()
    assert session.handles.market_db.get_market_schema_version() == 4
    token = session.close_writable_handles()
    read_only = session.reopen_read_only_and_release(token)
    try:
        assert read_only.market_db.get_market_schema_version() == 4
        with pytest.raises(PermissionError):
            read_only.market_db.set_sync_metadata("x", "y")
    finally:
        read_only.close()


def test_open_existing_rejects_wrong_schema_before_writable_open(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    market_root = data_root / "market-timeseries"
    factory = MarketWriterResourceFactory(data_root=data_root, market_root=market_root)
    session = factory.reset_and_open_v4()
    token = session.close_writable_handles()
    resources = session.reopen_read_only_and_release(token)
    resources.close()

    import duckdb

    connection = duckdb.connect(str(market_root / "market.duckdb"))
    connection.execute("DELETE FROM market_schema_version")
    connection.execute(
        "INSERT INTO market_schema_version VALUES (3, CURRENT_TIMESTAMP, 'wrong')"
    )
    connection.close()

    with pytest.raises(MarketSourceIdentityError, match="schema v4"):
        factory.open_existing()


def test_source_identity_rejects_symlink_and_reports_inode(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    market_root = data_root / "market-timeseries"
    factory = MarketWriterResourceFactory(data_root=data_root, market_root=market_root)
    session = factory.reset_and_open_v4()
    token = session.close_writable_handles()
    resources = session.reopen_read_only_and_release(token)
    resources.close()
    identity = inspect_market_source_identity(market_root / "market.duckdb")
    assert identity.inode > 0
    alias = market_root / "alias.duckdb"
    alias.symlink_to("market.duckdb")
    with pytest.raises(MarketSourceIdentityError, match="regular file"):
        inspect_market_source_identity(alias)


def test_open_existing_rejects_wrong_adjustment_mode(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    market_root = data_root / "market-timeseries"
    factory = MarketWriterResourceFactory(data_root=data_root, market_root=market_root)
    session = factory.reset_and_open_v4()
    token = session.close_writable_handles()
    resources = session.reopen_read_only_and_release(token)
    resources.close()
    import duckdb

    connection = duckdb.connect(str(market_root / "market.duckdb"))
    connection.execute(
        "UPDATE sync_metadata SET value = 'wrong' "
        "WHERE key = 'stock_price_adjustment_mode'"
    )
    connection.close()
    with pytest.raises(MarketSourceIdentityError, match="adjustment mode"):
        factory.open_existing()


def test_borrowed_inherited_exclusive_lease_remains_held_after_session(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    market_root = data_root / "market-timeseries"
    factory = MarketWriterResourceFactory(data_root=data_root, market_root=market_root)
    initial = factory.reset_and_open_v4()
    token = initial.close_writable_handles()
    resources = initial.reopen_read_only_and_release(token)
    resources.close()

    inherited = MarketOperationLease.acquire(data_root, exclusive=True)
    session = factory.open_existing(lease=inherited)
    token = session.close_writable_handles()
    resources = session.reopen_read_only_and_release(token)
    resources.close()
    assert inherited.fd >= 0
    with pytest.raises(MarketOperationLeaseError, match="held by another process"):
        MarketOperationLease.acquire(data_root, exclusive=False)
    inherited.release()


def test_close_failure_keeps_writer_lease_fenced(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    data_root = tmp_path / "data"
    market_root = data_root / "market-timeseries"
    session = MarketWriterResourceFactory(
        data_root=data_root,
        market_root=market_root,
    ).reset_and_open_v4()

    def fail_close() -> None:
        raise RuntimeError("close failed")

    original_close = session.handles.time_series_store.close
    monkeypatch.setattr(session.handles.time_series_store, "close", fail_close)
    with pytest.raises(RuntimeError, match="close failed"):
        session.close_writable_handles()
    assert session.fenced
    assert session.lease.fd >= 0
    monkeypatch.setattr(session.handles.time_series_store, "close", original_close)
    token = session.close_writable_handles()
    read_only = session.reopen_read_only_and_release(token)
    read_only.close()


def test_waiting_writer_re_resolves_replacement_inode_after_lease(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    market_root = data_root / "market-timeseries"
    factory = MarketWriterResourceFactory(data_root=data_root, market_root=market_root)
    old = factory.reset_and_open_v4()
    old.handles.market_db.set_sync_metadata("test_generation", "old")
    old_token = old.close_writable_handles()
    old_reader = old.reopen_read_only_and_release(old_token)

    replacement_data_root = tmp_path / "replacement-data"
    replacement_root = replacement_data_root / "market-timeseries"
    replacement = MarketWriterResourceFactory(
        data_root=replacement_data_root,
        market_root=replacement_root,
    ).reset_and_open_v4()
    replacement.handles.market_db.set_sync_metadata("test_generation", "new")
    replacement_token = replacement.close_writable_handles()
    replacement_reader = replacement.reopen_read_only_and_release(replacement_token)
    replacement_reader.close()
    replacement_inode = (replacement_root / "market.duckdb").stat().st_ino

    held = MarketOperationLease.acquire(data_root, exclusive=True)
    context = multiprocessing.get_context("spawn")
    ready: multiprocessing.Queue[bool] = context.Queue()
    result: multiprocessing.Queue[tuple[int, str | None]] = context.Queue()
    process = context.Process(
        target=_open_waiting_writer,
        args=(str(data_root), str(market_root), ready, result),
    )
    process.start()
    try:
        assert ready.get(timeout=5)
        time.sleep(0.2)
        assert result.empty()
        shutil.copyfile(
            replacement_root / "market.duckdb",
            market_root / "replacement.duckdb",
        )
        (market_root / "replacement.duckdb").replace(market_root / "market.duckdb")
        held.release()
        process.join(timeout=10)
        assert process.exitcode == 0
        opened_inode, generation = result.get(timeout=1)
        assert opened_inode != old_reader.identity.inode
        assert opened_inode != replacement_inode  # copy + replace creates a new active inode
        assert generation == "new"
        assert old_reader.market_db.get_sync_metadata("test_generation") == "old"
    finally:
        held.release()
        old_reader.close()
        if process.is_alive():
            process.terminate()
            process.join(timeout=5)
