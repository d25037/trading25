"""Lease-bound writable Market resources and generation-safe read-only reopen."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import shutil
import stat
import threading
from typing import ClassVar, Protocol

from .duckdb_connection import issue_market_writer_token
from .managed_root import (
    assert_market_managed_root_safe,
    lexical_absolute,
    prepare_market_managed_root,
)
from .market_db import MarketDb
from .market_operation_lease import MarketOperationLease
from .market_source_identity import (
    MarketSourceIdentity,
    assert_same_market_source,
    capture_market_source_identity,
    inspect_market_source_identity,
)
from .time_series_store import MarketTimeSeriesStore, create_time_series_store


class JoinableWorker(Protocol):
    def join(self, timeout: float | None = None) -> object: ...
    def is_alive(self) -> bool: ...


@dataclass(frozen=True)
class ClosedMarketHandlesToken:
    """Task 12 extension point: exclusive lease is held and handles are closed."""

    identity: MarketSourceIdentity
    _session_id: int


@dataclass
class MarketWriterHandles:
    market_db: MarketDb
    time_series_store: MarketTimeSeriesStore

    def close(self) -> None:
        errors: list[BaseException] = []
        for resource in (self.time_series_store, self.market_db):
            try:
                resource.close()
            except BaseException as exc:
                errors.append(exc)
        if errors:
            primary = errors[0]
            for additional in errors[1:]:
                primary.add_note(f"Additional Market handle close failure: {additional}")
            raise primary


@dataclass
class ReadOnlyMarketResources:
    market_db: MarketDb
    time_series_store: MarketTimeSeriesStore
    identity: MarketSourceIdentity

    def close(self) -> None:
        errors: list[BaseException] = []
        for resource in (self.time_series_store, self.market_db):
            try:
                resource.close()
            except BaseException as exc:
                errors.append(exc)
        if errors:
            raise errors[0]


@dataclass
class MarketWriterSession:
    lease: MarketOperationLease
    handles: MarketWriterHandles
    identity: MarketSourceIdentity
    factory: "MarketWriterResourceFactory"
    workers: list[JoinableWorker] = field(default_factory=list)
    fenced: bool = False
    _handles_closed: bool = False
    _borrowed_shared_lease: bool = False
    _borrowed_exclusive_lease: bool = False
    _process_lock: threading.Lock | None = None

    def register_worker(self, worker: JoinableWorker) -> None:
        if self._handles_closed:
            raise RuntimeError("Cannot register a worker after Market handles close")
        self.workers.append(worker)

    def _join_workers(self) -> None:
        for worker in self.workers:
            worker.join()
            if worker.is_alive():
                raise RuntimeError("Market writer worker did not join")

    def close_writable_handles(self) -> ClosedMarketHandlesToken:
        if self._handles_closed:
            return ClosedMarketHandlesToken(self.identity, id(self))
        try:
            self._join_workers()
            self.handles.close()
            assert_same_market_source(self.identity)
        except BaseException:
            self.fenced = True
            raise
        self._handles_closed = True
        return ClosedMarketHandlesToken(self.identity, id(self))

    def reopen_read_only_and_release(
        self,
        token: ClosedMarketHandlesToken,
    ) -> ReadOnlyMarketResources:
        if not self._handles_closed or token._session_id != id(self):
            raise PermissionError("Closed-handles maintenance token is not valid")
        try:
            resources = self.factory.read_only_factory.open_existing()
        except BaseException:
            self.fenced = True
            raise
        if self._borrowed_shared_lease:
            self.lease.convert(exclusive=False)
        elif self._borrowed_exclusive_lease:
            pass
        else:
            self.lease.release()
        if self._process_lock is not None:
            self._process_lock.release()
            self._process_lock = None
        return resources


@dataclass(frozen=True)
class ReadOnlyMarketResourceFactory:
    market_root: Path

    def open_existing(self) -> ReadOnlyMarketResources:
        identity = inspect_market_source_identity(self.market_root / "market.duckdb")
        market_db = MarketDb(str(identity.path), read_only=True)
        try:
            store = create_time_series_store(
                backend="duckdb-parquet",
                duckdb_path=str(identity.path),
                parquet_dir=str(self.market_root / "parquet"),
                read_only=True,
            )
            if store is None:
                raise RuntimeError("DuckDB Market time-series store is unavailable")
            assert_same_market_source(identity)
        except BaseException:
            market_db.close()
            raise
        return ReadOnlyMarketResources(market_db, store, identity)


@dataclass(frozen=True)
class MarketWriterResourceFactory:
    data_root: Path
    market_root: Path

    def __post_init__(self) -> None:
        object.__setattr__(self, "data_root", lexical_absolute(self.data_root))
        object.__setattr__(self, "market_root", lexical_absolute(self.market_root))
        if self.market_root.parent != self.data_root:
            raise ValueError("Market root must be a direct child of the data root")

    @property
    def read_only_factory(self) -> ReadOnlyMarketResourceFactory:
        return ReadOnlyMarketResourceFactory(self.market_root)

    _PROCESS_WRITER_LOCK: ClassVar[threading.Lock] = threading.Lock()

    def _open_writable(
        self,
        lease: MarketOperationLease,
        *,
        borrowed_shared_lease: bool = False,
        borrowed_exclusive_lease: bool = False,
        process_lock: threading.Lock | None = None,
    ) -> MarketWriterSession:
        assert_market_managed_root_safe(self.data_root, self.market_root)
        identity = inspect_market_source_identity(self.market_root / "market.duckdb")
        assert_same_market_source(identity)
        token = issue_market_writer_token()
        market_db = MarketDb(str(identity.path), read_only=False, writer_token=token)
        try:
            assert_same_market_source(identity)
            store = create_time_series_store(
                backend="duckdb-parquet",
                duckdb_path=str(identity.path),
                parquet_dir=str(self.market_root / "parquet"),
                read_only=False,
                writer_token=token,
            )
            if store is None:
                raise RuntimeError("DuckDB Market time-series store is unavailable")
            assert_same_market_source(identity)
        except BaseException:
            market_db.close()
            raise
        return MarketWriterSession(
            lease=lease,
            handles=MarketWriterHandles(market_db, store),
            identity=identity,
            factory=self,
            _borrowed_shared_lease=borrowed_shared_lease,
            _borrowed_exclusive_lease=borrowed_exclusive_lease,
            _process_lock=process_lock,
        )

    def open_existing(
        self,
        *,
        blocking: bool = True,
        timeout: float | None = None,
        lease: MarketOperationLease | None = None,
    ) -> MarketWriterSession:
        process_lock = self._PROCESS_WRITER_LOCK
        process_lock.acquire()
        borrowed_shared_lease = lease is not None and not lease.exclusive
        borrowed_exclusive_lease = lease is not None and lease.exclusive
        if lease is None:
            lease = MarketOperationLease.acquire(
                self.data_root,
                exclusive=True,
                blocking=blocking,
                timeout=timeout,
            )
        elif borrowed_shared_lease:
            lease.convert(exclusive=True, blocking=blocking, timeout=timeout)
        try:
            return self._open_writable(
                lease,
                borrowed_shared_lease=borrowed_shared_lease,
                borrowed_exclusive_lease=borrowed_exclusive_lease,
                process_lock=process_lock,
            )
        except BaseException:
            if borrowed_shared_lease:
                lease.convert(exclusive=False)
            elif not borrowed_exclusive_lease:
                lease.release()
            process_lock.release()
            raise

    def reset_and_open_v4(
        self,
        *,
        blocking: bool = True,
        timeout: float | None = None,
        lease: MarketOperationLease | None = None,
    ) -> MarketWriterSession:
        self.data_root.mkdir(parents=True, exist_ok=True)
        process_lock = self._PROCESS_WRITER_LOCK
        process_lock.acquire()
        borrowed_shared_lease = lease is not None and not lease.exclusive
        borrowed_exclusive_lease = lease is not None and lease.exclusive
        if lease is None:
            lease = MarketOperationLease.acquire(
                self.data_root,
                exclusive=True,
                blocking=blocking,
                timeout=timeout,
            )
        elif borrowed_shared_lease:
            lease.convert(exclusive=True, blocking=blocking, timeout=timeout)
        try:
            prepare_market_managed_root(self.data_root, self.market_root)
            for path in (
                self.market_root / "market.duckdb",
                self.market_root / "market.duckdb.wal",
            ):
                if path.exists() or path.is_symlink():
                    mode = path.lstat().st_mode
                    if stat.S_ISLNK(mode) or not stat.S_ISREG(mode):
                        raise RuntimeError("Market reset target must be a regular file")
                    path.unlink()
            parquet = self.market_root / "parquet"
            if parquet.exists() or parquet.is_symlink():
                mode = parquet.lstat().st_mode
                if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
                    raise RuntimeError("Market parquet reset target must be a real directory")
                shutil.rmtree(parquet)
            token = issue_market_writer_token()
            db_path = self.market_root / "market.duckdb"
            market_db = MarketDb(str(db_path), read_only=False, writer_token=token)
            try:
                store = create_time_series_store(
                    backend="duckdb-parquet",
                    duckdb_path=str(db_path),
                    parquet_dir=str(parquet),
                    read_only=False,
                    writer_token=token,
                )
                if store is None:
                    raise RuntimeError("DuckDB Market time-series store is unavailable")
                schema_version = market_db.get_market_schema_version()
                adjustment_mode = market_db.get_sync_metadata(
                    "stock_price_adjustment_mode"
                )
                identity = capture_market_source_identity(
                    db_path,
                    schema_version=(schema_version if schema_version is not None else -1),
                    adjustment_mode=(adjustment_mode or ""),
                )
            except BaseException:
                market_db.close()
                raise
            return MarketWriterSession(
                lease=lease,
                handles=MarketWriterHandles(market_db, store),
                identity=identity,
                factory=self,
                _borrowed_shared_lease=borrowed_shared_lease,
                _borrowed_exclusive_lease=borrowed_exclusive_lease,
                _process_lock=process_lock,
            )
        except BaseException:
            if borrowed_shared_lease:
                lease.convert(exclusive=False)
            elif not borrowed_exclusive_lease:
                lease.release()
            process_lock.release()
            raise
