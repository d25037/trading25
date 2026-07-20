from __future__ import annotations

import multiprocessing
import os
from pathlib import Path
import shutil
import time
from unittest.mock import MagicMock

import pytest

from src.infrastructure.db.market.duckdb_connection import MarketWriterToken
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
    ClosedMarketHandlesToken,
    MarketWriterConstructionFencedError,
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
    read_only = session.reopen_read_only(token)
    session.release_after_read_only_reopen(token)
    read_only.close()


def test_market_db_writable_open_requires_factory_token(tmp_path: Path) -> None:
    with pytest.raises(PermissionError, match="writer resource factory"):
        MarketDb(str(tmp_path / "market.duckdb"), read_only=False)


def test_writer_token_revalidates_live_lease_and_exact_source(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    market_root = data_root / "market-timeseries"
    market_root.mkdir(parents=True)
    db_path = market_root / "market.duckdb"
    lease = MarketOperationLease.acquire(data_root, exclusive=True)
    token = MarketWriterToken._from_writer_factory(lease, db_path)
    try:
        with pytest.raises(PermissionError, match="another Market source"):
            MarketDb(
                str(market_root / "other.duckdb"),
                read_only=False,
                writer_token=token,
            )
    finally:
        lease.release()

    with pytest.raises(MarketOperationLeaseError, match="not live and exclusive"):
        MarketDb(str(db_path), read_only=False, writer_token=token)


def test_writer_token_rejects_concrete_lease_with_unlocked_bound_fd(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    market_root = data_root / "market-timeseries"
    market_root.mkdir(parents=True)
    owner = MarketOperationLease.acquire(data_root, exclusive=True)
    unlocked_fd = os.open(owner.path, os.O_RDWR)
    fake = MarketOperationLease(
        data_root=data_root,
        path=owner.path,
        fd=unlocked_fd,
        exclusive=True,
        root_fd=os.dup(owner.root_fd),
    )
    try:
        with pytest.raises(MarketOperationLeaseError, match="does not own exclusivity"):
            MarketWriterToken._from_writer_factory(
                fake,
                market_root / "market.duckdb",
            )
    finally:
        fake.release()
        owner.release()


def test_second_same_process_writer_fails_fast_without_blocking(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    market_root = data_root / "market-timeseries"
    factory = MarketWriterResourceFactory(data_root=data_root, market_root=market_root)
    first = factory.reset_and_open_v4()

    started = time.monotonic()
    with pytest.raises(MarketOperationLeaseError, match="same process"):
        factory.open_existing(blocking=False)
    assert time.monotonic() - started < 0.5
    assert first.handles.market_db.get_market_schema_version() == 5

    token = first.close_writable_handles()
    resources = first.reopen_read_only(token)
    first.release_after_read_only_reopen(token)
    resources.close()


def test_same_process_writer_timeout_is_bounded(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    market_root = data_root / "market-timeseries"
    factory = MarketWriterResourceFactory(data_root=data_root, market_root=market_root)
    first = factory.reset_and_open_v4()

    started = time.monotonic()
    with pytest.raises(MarketOperationLeaseError, match="same process"):
        factory.open_existing(blocking=True, timeout=0.05)
    elapsed = time.monotonic() - started
    assert 0.04 <= elapsed < 0.5

    token = first.close_writable_handles()
    resources = first.reopen_read_only(token)
    first.release_after_read_only_reopen(token)
    resources.close()


def test_lease_acquire_failure_releases_same_process_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = tmp_path / "data"
    market_root = data_root / "market-timeseries"
    factory = MarketWriterResourceFactory(data_root=data_root, market_root=market_root)
    original_acquire = MarketOperationLease.acquire

    def fail_acquire(*args: object, **kwargs: object) -> MarketOperationLease:
        raise MarketOperationLeaseError("injected lease failure")

    monkeypatch.setattr(MarketOperationLease, "acquire", fail_acquire)
    with pytest.raises(MarketOperationLeaseError, match="injected"):
        factory.reset_and_open_v4(blocking=False)
    monkeypatch.setattr(MarketOperationLease, "acquire", original_acquire)

    session = factory.reset_and_open_v4(blocking=False)
    token = session.close_writable_handles()
    resources = session.reopen_read_only(token)
    session.release_after_read_only_reopen(token)
    resources.close()


def test_construction_close_failure_retains_process_and_file_fence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = tmp_path / "data"
    market_root = data_root / "market-timeseries"
    factory = MarketWriterResourceFactory(data_root=data_root, market_root=market_root)
    initial = factory.reset_and_open_v4()
    token = initial.close_writable_handles()
    resources = initial.reopen_read_only(token)
    initial.release_after_read_only_reopen(token)
    resources.close()

    import src.infrastructure.db.market.market_writer_resources as writer_module

    captured_leases: list[MarketOperationLease] = []
    captured_databases: list[MarketDb] = []
    original_acquire = MarketOperationLease.acquire
    original_close = MarketDb.close

    def capture_acquire(*args: object, **kwargs: object) -> MarketOperationLease:
        lease = original_acquire(*args, **kwargs)
        captured_leases.append(lease)
        return lease

    def fail_close(database: MarketDb) -> None:
        captured_databases.append(database)
        raise RuntimeError("injected close failure")

    monkeypatch.setattr(MarketOperationLease, "acquire", capture_acquire)
    monkeypatch.setattr(
        writer_module,
        "create_time_series_store",
        MagicMock(side_effect=RuntimeError("init failed")),
    )
    monkeypatch.setattr(MarketDb, "close", fail_close)
    try:
        with pytest.raises(MarketWriterConstructionFencedError, match="remains fenced"):
            factory.open_existing(blocking=False)
        assert captured_leases[0].fd >= 0
        with pytest.raises(MarketOperationLeaseError, match="same process"):
            factory.open_existing(blocking=False)
    finally:
        monkeypatch.setattr(MarketDb, "close", original_close)
        for database in captured_databases:
            original_close(database)
        for lease in captured_leases:
            lease.release()
        if factory._PROCESS_WRITER_LOCK.locked():
            factory._PROCESS_WRITER_LOCK.release()


def test_market_db_constructor_close_failure_retains_writer_fence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = tmp_path / "data"
    market_root = data_root / "market-timeseries"
    factory = MarketWriterResourceFactory(data_root=data_root, market_root=market_root)
    initial = factory.reset_and_open_v4()
    token = initial.close_writable_handles()
    resources = initial.reopen_read_only(token)
    initial.release_after_read_only_reopen(token)
    resources.close()

    captured_leases: list[MarketOperationLease] = []
    original_acquire = MarketOperationLease.acquire
    original_close = MarketDb.close

    def capture_acquire(*args: object, **kwargs: object) -> MarketOperationLease:
        lease = original_acquire(*args, **kwargs)
        captured_leases.append(lease)
        return lease

    def fail_schema(_database: MarketDb) -> None:
        raise RuntimeError("injected schema failure")

    def fail_close(_database: MarketDb) -> None:
        raise RuntimeError("injected constructor close failure")

    monkeypatch.setattr(MarketOperationLease, "acquire", capture_acquire)
    monkeypatch.setattr(MarketDb, "ensure_schema", fail_schema)
    monkeypatch.setattr(MarketDb, "close", fail_close)
    try:
        with pytest.raises(MarketWriterConstructionFencedError, match="remains fenced"):
            factory.open_existing(blocking=False)
        assert captured_leases[0].fd >= 0
        with pytest.raises(MarketOperationLeaseError, match="same process"):
            factory.open_existing(blocking=False)
    finally:
        monkeypatch.setattr(MarketDb, "close", original_close)
        for lease in captured_leases:
            lease.release()
        if factory._PROCESS_WRITER_LOCK.locked():
            factory._PROCESS_WRITER_LOCK.release()


def test_reset_and_open_v4_holds_lease_until_handles_close_and_reopens_read_only(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    market_root = data_root / "market-timeseries"
    factory = MarketWriterResourceFactory(data_root=data_root, market_root=market_root)

    session = factory.reset_and_open_v4()
    assert session.handles.market_db.get_market_schema_version() == 5
    token = session.close_writable_handles()
    read_only = session.reopen_read_only(token)
    session.release_after_read_only_reopen(token)
    try:
        assert read_only.market_db.get_market_schema_version() == 5
        with pytest.raises(PermissionError):
            read_only.market_db.set_sync_metadata("x", "y")
    finally:
        read_only.close()


def test_read_only_reopen_retains_writer_ownership_until_explicit_release(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    market_root = data_root / "market-timeseries"
    factory = MarketWriterResourceFactory(data_root=data_root, market_root=market_root)
    session = factory.reset_and_open_v4()
    token = session.close_writable_handles()

    read_only = session.reopen_read_only(token)
    assert session.lease.fd >= 0
    with pytest.raises(MarketOperationLeaseError, match="same process"):
        factory.open_existing(blocking=False)

    session.release_after_read_only_reopen(token)
    read_only.close()
    replacement = factory.open_existing(blocking=False)
    replacement_token = replacement.close_writable_handles()
    replacement_resources = replacement.reopen_read_only(replacement_token)
    replacement.release_after_read_only_reopen(replacement_token)
    replacement_resources.close()


def test_writer_ownership_cannot_release_before_read_only_reopen(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    market_root = data_root / "market-timeseries"
    session = MarketWriterResourceFactory(
        data_root=data_root, market_root=market_root
    ).reset_and_open_v4()
    token = session.close_writable_handles()

    with pytest.raises(PermissionError, match="read-only reopen"):
        session.release_after_read_only_reopen(token)

    read_only = session.reopen_read_only(token)
    session.release_after_read_only_reopen(token)
    read_only.close()


def test_maintenance_authority_requires_closed_handles_and_exact_session_token(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    market_root = data_root / "market-timeseries"
    session = MarketWriterResourceFactory(
        data_root=data_root, market_root=market_root
    ).reset_and_open_v4()

    with pytest.raises(PermissionError, match="closed handles"):
        session.authorize_maintenance(
            ClosedMarketHandlesToken(session.identity, id(session))
        )

    token = session.close_writable_handles()
    with pytest.raises(PermissionError, match="not valid"):
        session.authorize_maintenance(
            ClosedMarketHandlesToken(session.identity, id(session) + 1)
        )

    authority = session.authorize_maintenance(token)
    assert authority.data_root == data_root.absolute()
    assert authority.market_root == market_root.absolute()
    assert authority.identity == session.identity
    read_only = session.reopen_read_only(token)
    session.release_after_read_only_reopen(token)
    read_only.close()


def test_open_existing_rejects_wrong_schema_before_writable_open(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    market_root = data_root / "market-timeseries"
    factory = MarketWriterResourceFactory(data_root=data_root, market_root=market_root)
    session = factory.reset_and_open_v4()
    token = session.close_writable_handles()
    resources = session.reopen_read_only(token)
    session.release_after_read_only_reopen(token)
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
    resources = session.reopen_read_only(token)
    session.release_after_read_only_reopen(token)
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
    resources = session.reopen_read_only(token)
    session.release_after_read_only_reopen(token)
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


def test_borrowed_inherited_exclusive_lease_remains_held_after_session(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    market_root = data_root / "market-timeseries"
    factory = MarketWriterResourceFactory(data_root=data_root, market_root=market_root)
    initial = factory.reset_and_open_v4()
    token = initial.close_writable_handles()
    resources = initial.reopen_read_only(token)
    initial.release_after_read_only_reopen(token)
    resources.close()

    inherited = MarketOperationLease.acquire(data_root, exclusive=True)
    session = factory.open_existing(lease=inherited)
    token = session.close_writable_handles()
    resources = session.reopen_read_only(token)
    session.release_after_read_only_reopen(token)
    resources.close()
    assert inherited.fd >= 0
    with pytest.raises(MarketOperationLeaseError, match="held by another process"):
        MarketOperationLease.acquire(data_root, exclusive=False)
    inherited.release()


def test_close_failure_keeps_writer_lease_fenced(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
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
    read_only = session.reopen_read_only(token)
    session.release_after_read_only_reopen(token)
    read_only.close()


def test_waiting_writer_re_resolves_replacement_inode_after_lease(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    market_root = data_root / "market-timeseries"
    factory = MarketWriterResourceFactory(data_root=data_root, market_root=market_root)
    old = factory.reset_and_open_v4()
    old.handles.market_db.set_sync_metadata("test_generation", "old")
    old_token = old.close_writable_handles()
    old_reader = old.reopen_read_only(old_token)
    old.release_after_read_only_reopen(old_token)

    replacement_data_root = tmp_path / "replacement-data"
    replacement_root = replacement_data_root / "market-timeseries"
    replacement = MarketWriterResourceFactory(
        data_root=replacement_data_root,
        market_root=replacement_root,
    ).reset_and_open_v4()
    replacement.handles.market_db.set_sync_metadata("test_generation", "new")
    replacement_token = replacement.close_writable_handles()
    replacement_reader = replacement.reopen_read_only(replacement_token)
    replacement.release_after_read_only_reopen(replacement_token)
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
        assert (
            opened_inode != replacement_inode
        )  # copy + replace creates a new active inode
        assert generation == "new"
        assert old_reader.market_db.get_sync_metadata("test_generation") == "old"
    finally:
        held.release()
        old_reader.close()
        if process.is_alive():
            process.terminate()
            process.join(timeout=5)
