from __future__ import annotations

from datetime import UTC, datetime
from dataclasses import dataclass, field
from contextlib import contextmanager
from collections.abc import Callable
import errno
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import threading
import time
from types import SimpleNamespace

import pytest

from src.application.services import market_v4_cutover
from src.application.services.market_v4_cutover import (
    CutoverSafetyError,
    MarketSourceMetadata,
    MarketV4CutoverService,
    PromotionAppendStatus,
    PromotionIdentityEvidence,
    PromotionJournal,
    PromotionState,
    SmokeConfig,
    SmokeResult,
)
from src.entrypoints.http.schemas.db import MarketSchemaStats
from src.entrypoints.http.schemas.screening_job import ScreeningJobResponse
from src.infrastructure.db.market.market_db import MarketDb


def _screening_job_response(status: str) -> dict[str, object]:
    return ScreeningJobResponse(
        job_id="screen-1",
        status=status,
        created_at=datetime(2026, 7, 15, tzinfo=UTC),
        markets="0111",
        recentDays=10,
        sortBy="matchedDate",
        order="desc",
    ).model_dump(mode="json")


def test_market_schema_stats_requires_v4_by_default() -> None:
    assert MarketSchemaStats().requiredVersion == 4


def test_cutover_service_exposes_injected_boundaries(tmp_path: Path) -> None:
    assert hasattr(market_v4_cutover, "MarketV4CutoverService")
    assert hasattr(market_v4_cutover, "DuckDbAdapter")
    assert hasattr(market_v4_cutover, "RuntimeAdapter")
    atomic_exchange = market_v4_cutover.DarwinAtomicExchange()
    service = MarketV4CutoverService(
        tmp_path,
        duckdb=FakeDuckDb(),
        runtime=FakeRuntime(),
        disk_free_bytes=lambda _path: 1,
        now=lambda: "2026-07-16T00:00:00Z",
        code_version=lambda: "deadbeef",
        atomic_exchange=atomic_exchange,
    )

    assert service.atomic_exchange is atomic_exchange


def test_cutover_service_preserves_false_valued_atomic_exchange(
    tmp_path: Path,
) -> None:
    class FalseValuedAtomicExchange:
        def __bool__(self) -> bool:
            return False

        def exchange(
            self,
            managed_root: market_v4_cutover.ManagedRootFd,
            left: Path,
            right: Path,
        ) -> None:
            del managed_root, left, right

    atomic_exchange = FalseValuedAtomicExchange()
    service = MarketV4CutoverService(
        tmp_path,
        duckdb=FakeDuckDb(),
        runtime=FakeRuntime(),
        disk_free_bytes=lambda _path: 1,
        now=lambda: "2026-07-16T00:00:00Z",
        code_version=lambda: "deadbeef",
        atomic_exchange=atomic_exchange,
    )

    assert service.atomic_exchange is atomic_exchange


@dataclass
class FakeDuckDb:
    metadata: MarketSourceMetadata = MarketSourceMetadata(
        schema_version=3,
        adjustment_mode="local_projection_v1",
    )
    checkpoint_error: Exception | None = None
    leave_wal: bool = False
    checkpoint_calls: int = 0
    guard_lease_fds: list[int] = field(default_factory=list)

    def checkpoint_exclusive(
        self,
        directory_fd: int,
        filename: str,
        *,
        guard_lease_fd: int,
    ) -> MarketSourceMetadata:
        os.fstat(guard_lease_fd)
        self.guard_lease_fds.append(guard_lease_fd)
        self.checkpoint_calls += 1
        if self.checkpoint_error is not None:
            raise self.checkpoint_error
        if self.leave_wal:
            wal_fd = os.open(
                f"{filename}.wal",
                os.O_CREAT | os.O_WRONLY,
                0o600,
                dir_fd=directory_fd,
            )
            try:
                os.write(wal_fd, b"pending")
            finally:
                os.close(wal_fd)
        return self.metadata

    @contextmanager
    def checkpoint_snapshot(
        self,
        directory_fd: int,
        filename: str,
        *,
        guard_lease_fd: int,
    ):
        yield self.checkpoint_exclusive(
            directory_fd,
            filename,
            guard_lease_fd=guard_lease_fd,
        )

    def inspect(
        self,
        directory_fd: int,
        filename: str,
        *,
        guard_lease_fd: int,
    ) -> MarketSourceMetadata:
        os.fstat(guard_lease_fd)
        self.guard_lease_fds.append(guard_lease_fd)
        assert stat.S_ISREG(
            os.stat(filename, dir_fd=directory_fd, follow_symlinks=False).st_mode
        )
        return self.metadata


@dataclass
class FakeRuntime:
    stopped: bool = True
    active_jobs: tuple[str, ...] = ()
    apis: list[FakeApi] = field(default_factory=list)
    environments: list[dict[str, str]] = field(default_factory=list)
    stop_calls: int = 0
    cancel_calls: int = 0
    start_calls: int = 0
    retained_lease_fds: list[int] = field(default_factory=list)

    def assert_quiescent(self, _data_root: Path) -> None:
        if not self.stopped:
            raise CutoverSafetyError("FastAPI process must be stopped")
        if self.active_jobs:
            raise CutoverSafetyError(f"active jobs: {', '.join(self.active_jobs)}")

    def start(
        self,
        *,
        root_fd: int,
        market_fd: int,
        lease_fd: int,
        retained_lease_fd: int | None = None,
        environment: dict[str, str],
        log_path: Path,
        log_fd: int,
    ) -> FakeApi:
        self.start_calls += 1
        assert root_fd >= 0
        assert market_fd >= 0
        assert lease_fd >= 0
        if retained_lease_fd is not None:
            os.fstat(retained_lease_fd)
            self.retained_lease_fds.append(retained_lease_fd)
        self.environments.append(environment)
        del log_path
        os.write(log_fd, b"owned server\n")
        database_fd = os.open(
            "market.duckdb",
            os.O_CREAT | os.O_RDWR,
            0o600,
            dir_fd=market_fd,
        )
        os.close(database_fd)
        if not self.apis:
            raise AssertionError("No fake API configured")
        return self.apis.pop(0)

    def cancel_owned_work(self, _api: FakeApi) -> None:
        self.cancel_calls += 1

    def stop(self, _api: FakeApi) -> None:
        self.stop_calls += 1


class FakeApi:
    def __init__(self, *, invalid_lineage: bool = False, parity: bool = True) -> None:
        self.invalid_lineage = invalid_lineage
        self.parity = parity
        self.calls: list[tuple[str, str, dict[str, object] | None]] = []

    def request(
        self,
        method: str,
        path: str,
        payload: dict[str, object] | None = None,
    ) -> dict[str, object]:
        self.calls.append((method, path, payload))
        if path == "/api/db/stats":
            return {
                "schema": {"version": 4, "requiredVersion": 4, "current": True},
                "adjustedMetrics": {
                    "status": "ready",
                    "statementRows": 4,
                    "dailyValuationRows": 10,
                    "readyBasisCount": 2,
                },
            }
        if path == "/api/db/validate":
            return {
                "status": "healthy",
                "adjustedMetrics": {
                    "status": "invalid_lineage" if self.invalid_lineage else "ready",
                    "sourceStatementKeyCount": 2,
                    "expectedAdjustedStatementRows": 4,
                    "missingAdjustedStatementRows": 0,
                    "extraAdjustedStatementRows": 0,
                    "staleAdjustedStatementRows": 0,
                    "wrongBasisAdjustedStatementRows": 1 if self.invalid_lineage else 0,
                    "expectedDailyValuationRows": 10,
                    "missingDailyValuationRows": 0,
                    "extraDailyValuationRows": 0,
                    "wrongBasisDailyValuationRows": 0,
                },
            }
        if path == "/api/db/sync":
            return {"jobId": "sync-1", "status": "pending"}
        if path == "/api/db/sync/jobs/sync-1":
            return {"jobId": "sync-1", "status": "completed"}
        fundamental = {
            "asOfDate": "2026-07-14",
            "data": [{"date": "2026-03-31", "adjustedEps": 123.0}],
            "latestMetrics": {"eps": 123.0},
        }
        if path.startswith("/api/analytics/fundamentals/"):
            return fundamental
        if path == "/api/fundamentals/compute":
            if self.parity:
                return fundamental
            return {**fundamental, "asOfDate": "2026-07-15"}
        if path == "/api/analytics/screening/jobs":
            return _screening_job_response("pending")
        if path == "/api/analytics/screening/jobs/screen-1":
            return _screening_job_response("completed")
        if path == "/api/analytics/screening/result/screen-1":
            return {"results": [{"code": "7203"}]}
        if path.startswith("/api/analytics/fundamental-ranking"):
            return {"rankings": {"ratioHigh": [{"code": "7203"}]}}
        if path == "/api/dataset":
            return {"jobId": "dataset-1", "status": "pending"}
        if path == "/api/dataset/jobs/dataset-1":
            return {"jobId": "dataset-1", "status": "completed"}
        if path.startswith("/api/dataset/cutover-smoke-") and path.endswith("/info"):
            return {
                "snapshot": {
                    "schemaVersion": 3,
                    "sourceMarketSchemaVersion": 4,
                    "stockPriceAdjustmentMode": "local_projection_v2_event_time",
                },
                "validation": {"isValid": True},
            }
        if path.startswith("/api/dataset/cutover-smoke-") and "/sample?count=1" in path:
            return {"codes": ["7203"]}
        raise AssertionError(f"Unexpected API call: {method} {path}")


def _market_root(tmp_path: Path) -> Path:
    data_root = tmp_path / "xdg"
    market = data_root / "market-timeseries"
    (market / "parquet" / "stock_data").mkdir(parents=True)
    (market / "market.duckdb").write_bytes(b"duckdb-v3")
    (market / "parquet" / "stock_data" / "part.parquet").write_bytes(b"rows")
    return data_root


def _forbid_non_atomic_exchange_fallbacks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("non-atomic exchange fallback ran")

    monkeypatch.setattr(market_v4_cutover.ManagedRootFd, "copy_tree_create", fail)
    monkeypatch.setattr(market_v4_cutover, "_rename_exclusive_at", fail)


def _forbid_atomic_exchange_syscall(monkeypatch: pytest.MonkeyPatch) -> None:
    class ForbiddenRenameSwap:
        argtypes: object = None
        restype: object = None

        def __call__(self, *_args: object) -> int:
            raise AssertionError("atomic exchange syscall ran")

    monkeypatch.setattr(
        market_v4_cutover.ctypes,
        "CDLL",
        lambda *_args, **_kwargs: SimpleNamespace(
            renameatx_np=ForbiddenRenameSwap()
        ),
    )


def _exchange_identity(path: Path) -> tuple[int, int, int, bytes]:
    path_stat = path.stat()
    payload = path / "payload"
    payload_stat = payload.stat()
    return path_stat.st_ino, payload_stat.st_ino, path_stat.st_dev, payload.read_bytes()


def test_atomic_exchange_swaps_real_directories_without_changing_inodes(
    tmp_path: Path,
) -> None:
    root = tmp_path / "root"
    left = root / "left-parent" / "market"
    right = root / "right-parent" / "market"
    left.mkdir(parents=True)
    right.mkdir(parents=True)
    (left / "payload").write_bytes(b"left")
    (right / "payload").write_bytes(b"right")
    left_before = _exchange_identity(left)
    right_before = _exchange_identity(right)

    with market_v4_cutover.ManagedRootFd.open(root) as managed:
        market_v4_cutover.DarwinAtomicExchange().exchange(
            managed,
            Path("left-parent/market"),
            Path("right-parent/market"),
        )

    assert _exchange_identity(left) == right_before
    assert _exchange_identity(right) == left_before


def test_atomic_exchange_rejects_cross_device_before_syscall(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "root"
    left = root / "left"
    right = root / "right"
    left.mkdir(parents=True)
    right.mkdir()
    (left / "payload").write_bytes(b"left")
    (right / "payload").write_bytes(b"right")
    before = (_exchange_identity(left), _exchange_identity(right))
    _forbid_non_atomic_exchange_fallbacks(monkeypatch)
    _forbid_atomic_exchange_syscall(monkeypatch)
    real_stat = market_v4_cutover.os.stat
    real_open = market_v4_cutover.os.open
    real_fstat = market_v4_cutover.os.fstat
    right_leaf_fds: set[int] = set()

    def track_leaf_open(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        descriptor = real_open(path, flags, mode, dir_fd=dir_fd)
        if path == "right" and dir_fd is not None:
            right_leaf_fds.add(descriptor)
        return descriptor

    def cross_device_stat(
        path: str | bytes | int,
        *,
        dir_fd: int | None = None,
        follow_symlinks: bool = True,
    ) -> os.stat_result | SimpleNamespace:
        result = real_stat(path, dir_fd=dir_fd, follow_symlinks=follow_symlinks)
        if path == "right" and dir_fd is not None and not follow_symlinks:
            values = {name: getattr(result, name) for name in dir(result) if name.startswith("st_")}
            values["st_dev"] = result.st_dev + 1
            return SimpleNamespace(**values)
        return result

    def cross_device_fstat(fd: int) -> os.stat_result | SimpleNamespace:
        result = real_fstat(fd)
        if fd in right_leaf_fds:
            values = {
                name: getattr(result, name)
                for name in dir(result)
                if name.startswith("st_")
            }
            values["st_dev"] = result.st_dev + 1
            return SimpleNamespace(**values)
        return result

    monkeypatch.setattr(market_v4_cutover.os, "open", track_leaf_open)
    monkeypatch.setattr(market_v4_cutover.os, "stat", cross_device_stat)
    monkeypatch.setattr(market_v4_cutover.os, "fstat", cross_device_fstat)
    with market_v4_cutover.ManagedRootFd.open(root) as managed:
        with pytest.raises(CutoverSafetyError, match="same device"):
            market_v4_cutover.DarwinAtomicExchange().exchange(
                managed, Path("left"), Path("right")
            )

    assert (_exchange_identity(left), _exchange_identity(right)) == before


def test_atomic_exchange_rejects_unavailable_platform_without_fallback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "root"
    left = root / "left"
    right = root / "right"
    left.mkdir(parents=True)
    right.mkdir()
    (left / "payload").write_bytes(b"left")
    (right / "payload").write_bytes(b"right")
    before = (_exchange_identity(left), _exchange_identity(right))
    _forbid_non_atomic_exchange_fallbacks(monkeypatch)
    monkeypatch.setattr(market_v4_cutover.sys, "platform", "linux")

    def fail_libc_load(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("atomic exchange syscall binding loaded")

    monkeypatch.setattr(market_v4_cutover.ctypes, "CDLL", fail_libc_load)

    with market_v4_cutover.ManagedRootFd.open(root) as managed:
        with pytest.raises(CutoverSafetyError, match="unavailable"):
            market_v4_cutover.DarwinAtomicExchange().exchange(
                managed, Path("left"), Path("right")
            )

    assert (_exchange_identity(left), _exchange_identity(right)) == before


def test_atomic_exchange_rejects_symlink_leaf_and_parent_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "root"
    left_parent = root / "left-parent"
    right_parent = root / "right-parent"
    left_parent.mkdir(parents=True)
    right_parent.mkdir()
    (left_parent / "real-market").mkdir()
    (left_parent / "market").symlink_to("real-market")
    (right_parent / "market").mkdir()
    symlink_target_inode = (left_parent / "real-market").stat().st_ino
    right_inode = (right_parent / "market").stat().st_ino
    _forbid_non_atomic_exchange_fallbacks(monkeypatch)
    _forbid_atomic_exchange_syscall(monkeypatch)

    with market_v4_cutover.ManagedRootFd.open(root) as managed:
        with pytest.raises(CutoverSafetyError, match="real directory"):
            market_v4_cutover.DarwinAtomicExchange().exchange(
                managed,
                Path("left-parent/market"),
                Path("right-parent/market"),
            )

    assert (left_parent / "market").is_symlink()
    assert (left_parent / "market").stat().st_ino == symlink_target_inode
    assert (right_parent / "market").stat().st_ino == right_inode

    (left_parent / "market").unlink()
    (left_parent / "market").mkdir()
    (left_parent / "market/payload").write_bytes(b"left")
    (right_parent / "market/payload").write_bytes(b"right")
    before = (
        _exchange_identity(left_parent / "market"),
        _exchange_identity(right_parent / "market"),
    )

    with market_v4_cutover.ManagedRootFd.open(root) as managed:
        real_open_parent = managed.open_parent
        calls = 0

        def replace_parent_before_identity_check(
            relative: Path, *, create: bool = False
        ) -> tuple[int, str]:
            nonlocal calls
            calls += 1
            if calls == 3:
                left_parent.rename(root / "left-parent.detached")
                left_parent.mkdir()
            return real_open_parent(relative, create=create)

        monkeypatch.setattr(managed, "open_parent", replace_parent_before_identity_check)
        with pytest.raises(CutoverSafetyError, match="parent identity changed"):
            market_v4_cutover.DarwinAtomicExchange().exchange(
                managed,
                Path("left-parent/market"),
                Path("right-parent/market"),
            )

    assert _exchange_identity(root / "left-parent.detached/market") == before[0]
    assert _exchange_identity(right_parent / "market") == before[1]


def test_atomic_exchange_fsyncs_both_parents_after_swap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "root"
    left = root / "left-parent" / "market"
    right = root / "right-parent" / "market"
    left.mkdir(parents=True)
    right.mkdir(parents=True)
    (left / "payload").write_bytes(b"left")
    (right / "payload").write_bytes(b"right")
    parent_inodes = {left.parent.stat().st_ino, right.parent.stat().st_ino}
    fsynced_parent_inodes: list[int] = []
    real_fsync = market_v4_cutover.os.fsync

    def record_fsync(fd: int) -> None:
        inode = os.fstat(fd).st_ino
        if inode in parent_inodes:
            fsynced_parent_inodes.append(inode)
        real_fsync(fd)

    monkeypatch.setattr(market_v4_cutover.os, "fsync", record_fsync)
    with market_v4_cutover.ManagedRootFd.open(root) as managed:
        market_v4_cutover.DarwinAtomicExchange().exchange(
            managed,
            Path("left-parent/market"),
            Path("right-parent/market"),
        )

    assert fsynced_parent_inodes == [left.parent.stat().st_ino, right.parent.stat().st_ino]


def test_atomic_exchange_rejects_leaf_replacement_at_syscall_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "root"
    left = root / "left"
    right = root / "right"
    left.mkdir(parents=True)
    right.mkdir()
    (left / "payload").write_bytes(b"left")
    (right / "payload").write_bytes(b"right")

    class ReplaceLeafAtSyscall:
        argtypes: object = None
        restype: object = None

        def __call__(
            self,
            left_parent: int,
            left_name: bytes,
            _right_parent: int,
            _right_name: bytes,
            _flags: int,
        ) -> int:
            name = os.fsdecode(left_name)
            os.rename(
                name,
                f"{name}.detached",
                src_dir_fd=left_parent,
                dst_dir_fd=left_parent,
            )
            os.mkdir(name, dir_fd=left_parent)
            return 0

    monkeypatch.setattr(
        market_v4_cutover.ctypes,
        "CDLL",
        lambda *_args, **_kwargs: SimpleNamespace(
            renameatx_np=ReplaceLeafAtSyscall()
        ),
    )
    with market_v4_cutover.ManagedRootFd.open(root) as managed:
        with pytest.raises(CutoverSafetyError, match="leaf identity changed"):
            market_v4_cutover.DarwinAtomicExchange().exchange(
                managed, Path("left"), Path("right")
            )

    assert (root / "left.detached/payload").read_bytes() == b"left"
    assert (right / "payload").read_bytes() == b"right"


def test_atomic_exchange_attempts_both_parent_fsyncs_when_first_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "root"
    left = root / "left-parent" / "market"
    right = root / "right-parent" / "market"
    left.mkdir(parents=True)
    right.mkdir(parents=True)
    (left / "payload").write_bytes(b"left")
    (right / "payload").write_bytes(b"right")
    left_parent_inode = left.parent.stat().st_ino
    right_parent_inode = right.parent.stat().st_ino
    attempts: list[int] = []

    def fail_first_fsync(fd: int) -> None:
        inode = os.fstat(fd).st_ino
        attempts.append(inode)
        if inode == left_parent_inode:
            raise OSError(errno.EIO, "injected first parent fsync failure")

    monkeypatch.setattr(market_v4_cutover.os, "fsync", fail_first_fsync)
    with market_v4_cutover.ManagedRootFd.open(root) as managed:
        with pytest.raises(OSError, match="first parent fsync failure"):
            market_v4_cutover.DarwinAtomicExchange().exchange(
                managed,
                Path("left-parent/market"),
                Path("right-parent/market"),
            )

    assert attempts == [left_parent_inode, right_parent_inode]
    assert (left / "payload").read_bytes() == b"right"
    assert (right / "payload").read_bytes() == b"left"


def _write_report(
    data_root: Path, report_id: str, report: dict[str, object]
) -> None:
    report_dir = (
        data_root / "operations/market-v4-cutover/reports" / report_id
    )
    report_dir.mkdir(parents=True)
    (report_dir / "report.json").write_text(json.dumps(report))


def _service(
    data_root: Path,
    *,
    duckdb: FakeDuckDb | None = None,
    runtime: FakeRuntime | None = None,
    free_bytes: int = 10_000_000,
) -> MarketV4CutoverService:
    return MarketV4CutoverService(
        data_root,
        duckdb=duckdb or FakeDuckDb(),
        runtime=runtime or FakeRuntime(),
        disk_free_bytes=lambda _path: free_bytes,
        now=lambda: "2026-07-15T12:00:00Z",
        code_version=lambda: "deadbeef",
    )


def _promotion_payload(seed: int) -> dict[str, object]:
    digest = f"{seed:064x}"
    return {
        "marketDuckdb": {
            "device": seed,
            "inode": seed + 1,
            "size": seed + 2,
            "sha256": digest,
        },
        "parquetSha256": {
            "stock_data/part.parquet": {
                "device": seed,
                "inode": seed + 3,
                "size": seed + 4,
                "sha256": digest,
            }
        },
    }


def _promotion_location(seed: int) -> dict[str, object]:
    return {
        "directory": {"device": seed, "inode": seed + 1},
        "payload": _promotion_payload(seed),
    }


def _promotion_identities(
    state: PromotionState,
    *,
    backup_manifest_sha256: str = "a" * 64,
) -> PromotionIdentityEvidence:
    active = _promotion_location(10)
    retained = _promotion_location(20)
    quarantine = _promotion_location(30)
    holding = _promotion_location(40)
    locations: dict[
        str, dict[str, object] | None | tuple[str, ...]
    ] = {
        "active_current": active,
        "retained_current": retained,
        "quarantine_current": None,
        "holding_current": None,
        "detached_runtime_names": (),
    }
    if state in {
        PromotionState.RUNTIMES_DETACHED,
        PromotionState.PREPARED,
        PromotionState.EXCHANGED,
    }:
        locations["holding_current"] = holding
        locations["detached_runtime_names"] = (".cutover-runtime-source",)
    elif state in {
        PromotionState.QUARANTINED,
        PromotionState.ACTIVE_SMOKE_PASSED,
    }:
        locations["retained_current"] = None
        locations["quarantine_current"] = quarantine
        locations["holding_current"] = holding
        locations["detached_runtime_names"] = (".cutover-runtime-source",)
    elif state is PromotionState.REPORT_PERSISTED:
        locations["retained_current"] = None
        locations["quarantine_current"] = quarantine
        locations["detached_runtime_names"] = (".cutover-runtime-source",)
    elif state is PromotionState.COMMITTED:
        locations["retained_current"] = None
        locations["quarantine_current"] = quarantine
        locations["detached_runtime_names"] = (".cutover-runtime-source",)
    elif state is PromotionState.EXCHANGED_BACK:
        locations["holding_current"] = holding
        locations["detached_runtime_names"] = (".cutover-runtime-source",)
    elif state is PromotionState.ROLLED_BACK:
        locations["detached_runtime_names"] = (".cutover-runtime-source",)
    elif state is PromotionState.ROLLBACK_DEFERRED:
        locations["retained_current"] = None
        locations["quarantine_current"] = quarantine
        locations["holding_current"] = holding
        locations["detached_runtime_names"] = (".cutover-runtime-source",)
    return PromotionIdentityEvidence(
        active_before_directory={"device": 1, "inode": 2},
        active_before_payload=_promotion_payload(1),
        retained_v4_directory={"device": 3, "inode": 4},
        retained_v4_payload=_promotion_payload(3),
        backup_manifest_sha256=backup_manifest_sha256,
        backup_file_set_sha256="b" * 64,
        active_current=locations["active_current"],
        retained_current=locations["retained_current"],
        quarantine_current=locations["quarantine_current"],
        holding_current=locations["holding_current"],
        detached_runtime_names=locations["detached_runtime_names"],
    )


def _promotion_journal(
    data_root: Path,
    operation_id: str = "promotion-001",
    **kwargs: object,
) -> tuple[market_v4_cutover.ManagedRootFd, PromotionJournal]:
    data_root.mkdir(parents=True, exist_ok=True)
    managed = market_v4_cutover.ManagedRootFd.open(data_root)
    journal = PromotionJournal(
        managed,
        operation_id,
        now=lambda: "2026-07-16T00:00:00Z",
        **kwargs,
    )
    return managed, journal


def test_promotion_journal_appends_create_only_fsynced_records(
    tmp_path: Path,
) -> None:
    events: list[str] = []

    def file_fsync(fd: int) -> None:
        assert stat.S_ISREG(os.fstat(fd).st_mode)
        events.append("file")
        os.fsync(fd)

    def directory_fsync(fd: int) -> None:
        assert stat.S_ISDIR(os.fstat(fd).st_mode)
        events.append("directory")
        os.fsync(fd)

    managed, journal = _promotion_journal(
        tmp_path / "xdg",
        file_fsync=file_fsync,
        directory_fsync=directory_fsync,
    )
    try:
        result = journal.append(
            PromotionState.VALIDATED,
            identities=_promotion_identities(PromotionState.VALIDATED),
        )
        assert result.status is PromotionAppendStatus.COMMITTED
        assert result.record is not None
        record = result.record
        record_path = (
            tmp_path
            / "xdg/operations/market-v4-cutover/journals/promotion-001/00000001.json"
        )
        before = record_path.read_bytes()
        assert events.count("file") >= 3
        assert events.count("directory") >= 3
        assert record.sequence == 1
        assert journal.read_validated() == (record,)
        with pytest.raises(CutoverSafetyError, match="transition|create-only"):
            journal.append(
                PromotionState.VALIDATED,
                identities=_promotion_identities(PromotionState.VALIDATED),
            )
        assert record_path.read_bytes() == before
    finally:
        managed.close()

    calls = 0

    def failing_directory_fsync(_fd: int) -> None:
        nonlocal calls
        calls += 1
        raise OSError(errno.EIO, "injected directory fsync failure")

    failed_managed, failed = _promotion_journal(
        tmp_path / "failed-xdg",
        directory_fsync=failing_directory_fsync,
    )
    try:
        failed_result = failed.append(
            PromotionState.VALIDATED,
            identities=_promotion_identities(PromotionState.VALIDATED),
        )
        assert failed_result.status is PromotionAppendStatus.NOT_COMMITTED
        assert calls == 1
    finally:
        failed_managed.close()
    reload_managed, reload_failed = _promotion_journal(tmp_path / "failed-xdg")
    try:
        assert reload_failed.read_validated() == ()
    finally:
        reload_managed.close()


def test_promotion_journal_rejects_skipped_duplicate_or_regressed_state(
    tmp_path: Path,
) -> None:
    managed, journal = _promotion_journal(tmp_path / "xdg")
    try:
        with pytest.raises(CutoverSafetyError, match="transition"):
            journal.append(
                PromotionState.PREPARED,
                identities=_promotion_identities(PromotionState.PREPARED),
            )
        journal.append(
            PromotionState.VALIDATED,
            identities=_promotion_identities(PromotionState.VALIDATED),
        )
        for state in (PromotionState.VALIDATED, PromotionState.COMMITTED):
            with pytest.raises(CutoverSafetyError, match="transition|create-only"):
                journal.append(state, identities=_promotion_identities(state))
    finally:
        managed.close()


def test_promotion_journal_rejects_torn_or_unknown_record(tmp_path: Path) -> None:
    managed, journal = _promotion_journal(tmp_path / "xdg")
    try:
        journal.append(
            PromotionState.VALIDATED,
            identities=_promotion_identities(PromotionState.VALIDATED),
        )
        journal_dir = (
            tmp_path / "xdg/operations/market-v4-cutover/journals/promotion-001"
        )
        (journal_dir / "00000002.json").write_bytes(b'{"torn":')
        with pytest.raises(CutoverSafetyError, match="journal record"):
            journal.read_validated()
        (journal_dir / "00000002.json").unlink()
        (journal_dir / "README").write_text("unknown")
        with pytest.raises(CutoverSafetyError, match="unknown journal entry"):
            journal.read_validated()
    finally:
        managed.close()


def test_promotion_journal_rejects_operation_and_identity_mismatch(
    tmp_path: Path,
) -> None:
    managed, journal = _promotion_journal(tmp_path / "xdg")
    try:
        journal.append(
            PromotionState.VALIDATED,
            identities=_promotion_identities(PromotionState.VALIDATED),
        )
        path = (
            tmp_path
            / "xdg/operations/market-v4-cutover/journals/promotion-001/00000001.json"
        )
        payload = json.loads(path.read_text())
        payload["operation_id"] = "promotion-elsewhere"
        path.write_text(
            json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n"
        )
        with pytest.raises(CutoverSafetyError, match="operation"):
            journal.read_validated()
    finally:
        managed.close()

    identity_managed, identity_journal = _promotion_journal(
        tmp_path / "identity-xdg"
    )
    try:
        identity_journal.append(
            PromotionState.VALIDATED,
            identities=_promotion_identities(PromotionState.VALIDATED),
        )
        with pytest.raises(CutoverSafetyError, match="identity"):
            identity_journal.append(
                PromotionState.RUNTIMES_DETACHED,
                identities=_promotion_identities(
                    PromotionState.RUNTIMES_DETACHED,
                    backup_manifest_sha256="c" * 64,
                ),
            )
    finally:
        identity_managed.close()


def test_promotion_journal_reload_reconstructs_exact_state(tmp_path: Path) -> None:
    data_root = tmp_path / "xdg"
    managed, journal = _promotion_journal(data_root)
    try:
        expected = []
        attempts = []
        for state in (
            PromotionState.VALIDATED,
            PromotionState.RUNTIMES_DETACHED,
            PromotionState.PREPARED,
        ):
            result = journal.append(state, identities=_promotion_identities(state))
            assert result.status is PromotionAppendStatus.COMMITTED
            assert result.record is not None
            expected.append(result.record)
            attempts.append(result.attempt_id)
    finally:
        managed.close()

    reloaded_managed, reloaded = _promotion_journal(data_root)
    try:
        with pytest.raises(CutoverSafetyError, match="authorization"):
            reloaded.read_validated()
        recovered = reloaded.recover(attempts[-1])
        assert recovered.status is PromotionAppendStatus.COMMITTED
        assert reloaded.read_validated() == tuple(expected)
        raw = (
            data_root
            / "operations/market-v4-cutover/journals/promotion-001/00000003.json"
        ).read_bytes()
        assert raw == (
            json.dumps(
                json.loads(raw), sort_keys=True, separators=(",", ":")
            ).encode()
            + b"\n"
        )
    finally:
        reloaded_managed.close()


def test_promotion_journal_requires_exact_state_identity_schema(
    tmp_path: Path,
) -> None:
    managed, journal = _promotion_journal(tmp_path / "xdg")
    try:
        valid = _promotion_identities(PromotionState.VALIDATED)
        invalid_nested = PromotionIdentityEvidence(
            **{
                **valid.__dict__,
                "active_before_directory": {"device": 1, "inode": 2, "extra": 3},
            }
        )
        with pytest.raises(CutoverSafetyError, match="identity schema"):
            journal.append(PromotionState.VALIDATED, identities=invalid_nested)
        invalid_location = PromotionIdentityEvidence(
            **{**valid.__dict__, "holding_current": _promotion_location(40)}
        )
        with pytest.raises(CutoverSafetyError, match="identity schema"):
            journal.append(PromotionState.VALIDATED, identities=invalid_location)
        invalid_path_payload = _promotion_payload(1)
        parquet = invalid_path_payload["parquetSha256"]
        assert isinstance(parquet, dict)
        parquet["stock_data//part.parquet"] = parquet.pop(
            "stock_data/part.parquet"
        )
        invalid_path = PromotionIdentityEvidence(
            **{**valid.__dict__, "active_before_payload": invalid_path_payload}
        )
        with pytest.raises(CutoverSafetyError, match="identity schema"):
            journal.append(PromotionState.VALIDATED, identities=invalid_path)

        journal.append(PromotionState.VALIDATED, identities=valid)
        path = (
            tmp_path
            / "xdg/operations/market-v4-cutover/journals/promotion-001/00000001.json"
        )
        raw = json.loads(path.read_text())
        raw["identities"]["unknown"] = None
        path.write_text(json.dumps(raw, sort_keys=True, separators=(",", ":")) + "\n")
        with pytest.raises(CutoverSafetyError, match="identity schema"):
            journal.read_validated()
        raw["identities"].pop("unknown")
        raw["schema_version"] = True
        path.write_text(json.dumps(raw, sort_keys=True, separators=(",", ":")) + "\n")
        with pytest.raises(CutoverSafetyError, match="schema version"):
            journal.read_validated()
    finally:
        managed.close()


@pytest.mark.parametrize(
    ("artifact", "field_path"),
    [
        ("record", ("schema_version",)),
        ("record", ("sequence",)),
        ("record", ("identities", "active_before_directory", "device")),
        ("record", ("identities", "active_before_payload", "marketDuckdb", "size")),
        ("control", ("schema_version",)),
        ("control", ("control_sequence",)),
        ("control", ("target_sequence",)),
    ],
)
def test_promotion_journal_rejects_bool_for_every_integer_field(
    tmp_path: Path,
    artifact: str,
    field_path: tuple[str, ...],
) -> None:
    data_root = tmp_path / f"{artifact}-{'-'.join(field_path)}"
    managed, journal = _promotion_journal(data_root)
    try:
        result = journal.append(
            PromotionState.VALIDATED,
            identities=_promotion_identities(PromotionState.VALIDATED),
        )
        assert result.status is PromotionAppendStatus.COMMITTED
    finally:
        managed.close()
    if artifact == "record":
        path = data_root / (
            "operations/market-v4-cutover/journals/promotion-001/00000001.json"
        )
    else:
        path = data_root / (
            "operations/market-v4-cutover/journal-controls/promotion-001/"
            "00000001.intent.json"
        )
    payload = json.loads(path.read_text())
    target = payload
    for key in field_path[:-1]:
        target = target[key]
    target[field_path[-1]] = True
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n")

    reload_managed, reloaded = _promotion_journal(data_root)
    try:
        with pytest.raises(CutoverSafetyError, match="schema|sequence|identity|target"):
            reloaded.read_validated()
    finally:
        reload_managed.close()


@pytest.mark.parametrize(
    "paused_boundary",
    ["candidate_file_fsync_before", "journal_parent_fsync_before"],
)
def test_promotion_journal_never_publishes_to_ordinary_reader_during_append(
    tmp_path: Path,
    paused_boundary: str,
) -> None:
    reached = threading.Event()
    release = threading.Event()
    append_result: list[object] = []
    read_result: list[object] = []

    def boundary(stage: str) -> None:
        if stage == paused_boundary:
            reached.set()
            assert release.wait(5)

    managed, journal = _promotion_journal(
        tmp_path / "xdg", boundary_hook=boundary
    )
    try:
        append_thread = threading.Thread(
            target=lambda: append_result.append(
                journal.append(
                    PromotionState.VALIDATED,
                    identities=_promotion_identities(PromotionState.VALIDATED),
                )
            )
        )
        append_thread.start()
        assert reached.wait(5)
        reader = threading.Thread(
            target=lambda: read_result.append(journal.read_validated())
        )
        reader.start()
        time.sleep(0.05)
        assert reader.is_alive()
        assert read_result == []
        release.set()
        append_thread.join(5)
        reader.join(5)
        assert not append_thread.is_alive()
        assert not reader.is_alive()
        result = append_result[0]
        assert result.status is PromotionAppendStatus.COMMITTED
        assert read_result == [(result.record,)]
    finally:
        release.set()
        managed.close()


def test_promotion_journal_returns_indeterminate_when_cleanup_is_unprovable(
    tmp_path: Path,
) -> None:
    def fail_before_publication(stage: str) -> None:
        if stage in {"candidate_file_fsync_before", "cleanup_unlink_before"}:
            raise OSError(errno.EIO, f"injected {stage}")

    prepublish_root = tmp_path / "prepublish-xdg"
    pre_managed, prepublish = _promotion_journal(
        prepublish_root, boundary_hook=fail_before_publication
    )
    try:
        not_committed = prepublish.append(
            PromotionState.VALIDATED,
            identities=_promotion_identities(PromotionState.VALIDATED),
        )
        assert not_committed.status is PromotionAppendStatus.NOT_COMMITTED
        with pytest.raises(CutoverSafetyError, match="authorization"):
            prepublish.read_validated()
    finally:
        pre_managed.close()
    pre_reload_managed, pre_reload = _promotion_journal(prepublish_root)
    try:
        with pytest.raises(CutoverSafetyError, match="authorization"):
            pre_reload.read_validated()
    finally:
        pre_reload_managed.close()

    def fail_boundaries(stage: str) -> None:
        if stage in {"journal_parent_fsync_before", "cleanup_unlink_before"}:
            raise OSError(errno.EIO, f"injected {stage}")

    data_root = tmp_path / "xdg"
    managed, journal = _promotion_journal(
        data_root, boundary_hook=fail_boundaries
    )
    try:
        result = journal.append(
            PromotionState.VALIDATED,
            identities=_promotion_identities(PromotionState.VALIDATED),
        )
        assert result.status is PromotionAppendStatus.INDETERMINATE
        assert result.record is None
        with pytest.raises(CutoverSafetyError, match="unresolved intent"):
            journal.read_validated()
    finally:
        managed.close()

    reloaded_managed, reloaded = _promotion_journal(data_root)
    try:
        with pytest.raises(CutoverSafetyError, match="unresolved intent"):
            reloaded.read_validated()
    finally:
        reloaded_managed.close()

    def fail_resolution(stage: str) -> None:
        if stage == "resolution_parent_fsync_before":
            raise OSError(errno.EIO, "injected resolution fsync failure")

    resolution_root = tmp_path / "resolution-xdg"
    resolution_managed, resolution_journal = _promotion_journal(
        resolution_root, boundary_hook=fail_resolution
    )
    try:
        resolution_result = resolution_journal.append(
            PromotionState.VALIDATED,
            identities=_promotion_identities(PromotionState.VALIDATED),
        )
        assert resolution_result.status is PromotionAppendStatus.INDETERMINATE
        with pytest.raises(CutoverSafetyError, match="unresolved intent"):
            resolution_journal.read_validated()
    finally:
        resolution_managed.close()

    resolution_reload_managed, resolution_reload = _promotion_journal(
        resolution_root
    )
    try:
        with pytest.raises(CutoverSafetyError, match="unresolved intent"):
            resolution_reload.read_validated()
    finally:
        resolution_reload_managed.close()


def test_promotion_journal_recovery_adopts_only_exact_durable_candidate(
    tmp_path: Path,
) -> None:
    failed_once = False

    def fail_publication_fsync(stage: str) -> None:
        nonlocal failed_once
        if stage == "journal_parent_fsync_before" and not failed_once:
            failed_once = True
            raise OSError(errno.EIO, "injected publication fsync")
        if stage == "cleanup_unlink_before":
            raise OSError(errno.EIO, "injected cleanup failure")

    data_root = tmp_path / "xdg"
    managed, journal = _promotion_journal(
        data_root, boundary_hook=fail_publication_fsync
    )
    try:
        attempt = journal.append(
            PromotionState.VALIDATED,
            identities=_promotion_identities(PromotionState.VALIDATED),
        )
        assert attempt.status is PromotionAppendStatus.INDETERMINATE
    finally:
        managed.close()

    recovery_managed, recovery = _promotion_journal(data_root)
    try:
        recovered = recovery.recover(attempt.attempt_id)
        assert recovered.status is PromotionAppendStatus.COMMITTED
        assert recovered.record is not None
        assert recovery.read_validated() == (recovered.record,)
    finally:
        recovery_managed.close()


def test_promotion_journal_recovery_keeps_mismatch_fail_stopped(
    tmp_path: Path,
) -> None:
    def fail_boundaries(stage: str) -> None:
        if stage in {"journal_parent_fsync_before", "cleanup_unlink_before"}:
            raise OSError(errno.EIO, f"injected {stage}")

    data_root = tmp_path / "xdg"
    managed, journal = _promotion_journal(
        data_root, boundary_hook=fail_boundaries
    )
    try:
        attempt = journal.append(
            PromotionState.VALIDATED,
            identities=_promotion_identities(PromotionState.VALIDATED),
        )
        assert attempt.status is PromotionAppendStatus.INDETERMINATE
    finally:
        managed.close()
    candidate = data_root / (
        "operations/market-v4-cutover/journals/promotion-001/00000001.json"
    )
    candidate.write_bytes(candidate.read_bytes() + b" ")

    recovery_managed, recovery = _promotion_journal(data_root)
    try:
        with pytest.raises(CutoverSafetyError, match="candidate.*mismatch"):
            recovery.recover(attempt.attempt_id)
        with pytest.raises(CutoverSafetyError, match="unresolved intent"):
            recovery.read_validated()
    finally:
        recovery_managed.close()


def test_promotion_journal_serializes_append_read_and_recovery_cross_process(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "xdg"
    managed, journal = _promotion_journal(data_root)
    try:
        with journal._locked(exclusive=True):
            read_fd, write_fd = os.pipe()
            child = os.fork()
            if child == 0:
                os.close(read_fd)
                child_managed, child_journal = _promotion_journal(data_root)
                try:
                    child_journal.read_validated()
                    os.write(write_fd, b"done")
                finally:
                    child_managed.close()
                    os.close(write_fd)
                os._exit(0)
            os.close(write_fd)
            os.set_blocking(read_fd, False)
            time.sleep(0.05)
            with pytest.raises(BlockingIOError):
                os.read(read_fd, 4)
        os.set_blocking(read_fd, True)
        assert os.read(read_fd, 4) == b"done"
        os.close(read_fd)
        _pid, status = os.waitpid(child, 0)
        assert os.waitstatus_to_exitcode(status) == 0

        events: list[str] = []
        ancestor_managed, ancestor_journal = _promotion_journal(
            tmp_path / "ancestor-xdg", boundary_hook=events.append
        )
        result = ancestor_journal.append(
            PromotionState.VALIDATED,
            identities=_promotion_identities(PromotionState.VALIDATED),
        )
        assert result.status is PromotionAppendStatus.COMMITTED
        ancestor_events = {
            event for event in events if event.startswith("ancestor_parent_fsynced:")
        }
        assert {
            "ancestor_parent_fsynced:operations",
            "ancestor_parent_fsynced:operations/market-v4-cutover",
            "ancestor_parent_fsynced:operations/market-v4-cutover/journals",
            "ancestor_parent_fsynced:operations/market-v4-cutover/journals/promotion-001",
            "ancestor_parent_fsynced:operations/market-v4-cutover/journal-controls",
            "ancestor_parent_fsynced:operations/market-v4-cutover/journal-controls/promotion-001",
        } <= ancestor_events
        ancestor_managed.close()
    finally:
        managed.close()


def test_promotion_journal_fsyncs_both_control_parents_after_publication(
    tmp_path: Path,
) -> None:
    events: list[str] = []
    managed, journal = _promotion_journal(
        tmp_path / "xdg", boundary_hook=events.append
    )
    try:
        result = journal.append(
            PromotionState.VALIDATED,
            identities=_promotion_identities(PromotionState.VALIDATED),
        )
        assert result.status is PromotionAppendStatus.COMMITTED
        for stage in ("intent", "resolution"):
            publication = events.index(f"{stage}_control_publication_after")
            source = events.index(f"{stage}_source_parent_fsynced")
            destination = events.index(f"{stage}_destination_parent_fsynced")
            assert publication < source < destination
    finally:
        managed.close()


@pytest.mark.parametrize(
    "failure_boundary",
    [
        "ancestor_child_fsync_before:operations",
        "ancestor_parent_fsync_before:operations",
        "ancestor_child_fsync_before:operations/market-v4-cutover",
        "ancestor_parent_fsync_before:operations/market-v4-cutover",
        "ancestor_child_fsync_before:operations/market-v4-cutover/journals",
        "ancestor_parent_fsync_before:operations/market-v4-cutover/journals",
        "ancestor_child_fsync_before:operations/market-v4-cutover/journals/promotion-001",
        "ancestor_parent_fsync_before:operations/market-v4-cutover/journals/promotion-001",
        "ancestor_child_fsync_before:operations/market-v4-cutover/journal-controls",
        "ancestor_parent_fsync_before:operations/market-v4-cutover/journal-controls",
        "ancestor_child_fsync_before:operations/market-v4-cutover/journal-controls/promotion-001",
        "ancestor_parent_fsync_before:operations/market-v4-cutover/journal-controls/promotion-001",
        "ancestor_child_fsync_before:operations/market-v4-cutover/journal-controls/promotion-001/staging",
        "ancestor_parent_fsync_before:operations/market-v4-cutover/journal-controls/promotion-001/staging",
        "ancestor_child_fsync_before:operations/market-v4-cutover/journal-locks",
        "ancestor_parent_fsync_before:operations/market-v4-cutover/journal-locks",
    ],
)
def test_promotion_journal_fails_closed_at_every_ancestor_fsync_boundary(
    tmp_path: Path,
    failure_boundary: str,
) -> None:
    def fail_boundary(stage: str) -> None:
        if stage == failure_boundary:
            raise OSError(errno.EIO, f"injected {stage}")

    data_root = tmp_path / failure_boundary.replace("/", "_")
    managed, journal = _promotion_journal(data_root, boundary_hook=fail_boundary)
    try:
        result = journal.append(
            PromotionState.VALIDATED,
            identities=_promotion_identities(PromotionState.VALIDATED),
        )
        assert result.status is PromotionAppendStatus.NOT_COMMITTED
    finally:
        managed.close()
    reload_managed, reloaded = _promotion_journal(data_root)
    try:
        assert reloaded.read_validated() == ()
    finally:
        reload_managed.close()


@pytest.mark.parametrize(
    "failure_boundary",
    [
        "resolution_file_fsync_before",
        "resolution_parent_fsync_before",
        "resolution_source_parent_fsync_before",
        "resolution_destination_parent_fsync_before",
    ],
)
def test_promotion_journal_recovery_resolution_failure_is_indeterminate(
    tmp_path: Path,
    failure_boundary: str,
) -> None:
    first_failure = False

    def make_indeterminate(stage: str) -> None:
        nonlocal first_failure
        if stage == "journal_parent_fsync_before" and not first_failure:
            first_failure = True
            raise OSError(errno.EIO, "injected candidate fsync")
        if stage == "cleanup_unlink_before":
            raise OSError(errno.EIO, "injected cleanup")

    data_root = tmp_path / failure_boundary
    managed, journal = _promotion_journal(
        data_root, boundary_hook=make_indeterminate
    )
    try:
        attempt = journal.append(
            PromotionState.VALIDATED,
            identities=_promotion_identities(PromotionState.VALIDATED),
        )
        assert attempt.status is PromotionAppendStatus.INDETERMINATE
    finally:
        managed.close()

    def fail_recovery(stage: str) -> None:
        if stage == failure_boundary:
            raise OSError(errno.EIO, f"injected {stage}")

    recovery_managed, recovery = _promotion_journal(
        data_root, boundary_hook=fail_recovery
    )
    try:
        recovered = recovery.recover(attempt.attempt_id)
        assert recovered.status is PromotionAppendStatus.INDETERMINATE
        with pytest.raises(CutoverSafetyError, match="unresolved intent"):
            recovery.read_validated()
    finally:
        recovery_managed.close()


@pytest.mark.parametrize(
    ("late_phase", "expected"),
    [
        ("committed", PromotionAppendStatus.COMMITTED),
        ("published", PromotionAppendStatus.INDETERMINATE),
    ],
)
def test_promotion_journal_late_lock_exit_error_never_downgrades_phase(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    late_phase: str,
    expected: PromotionAppendStatus,
) -> None:
    boundary_failure = late_phase == "published"

    def boundary(stage: str) -> None:
        if boundary_failure and stage == "journal_parent_fsync_before":
            raise OSError(errno.EIO, "injected publication ambiguity")
        if boundary_failure and stage == "cleanup_unlink_before":
            raise OSError(errno.EIO, "injected cleanup ambiguity")

    managed, journal = _promotion_journal(
        tmp_path / late_phase, boundary_hook=boundary
    )
    original_locked = journal._locked

    @contextmanager
    def late_failing_lock(*, exclusive: bool):
        with original_locked(exclusive=exclusive):
            yield
        raise OSError(errno.EIO, "injected late lock exit")

    monkeypatch.setattr(journal, "_locked", late_failing_lock)
    try:
        result = journal.append(
            PromotionState.VALIDATED,
            identities=_promotion_identities(PromotionState.VALIDATED),
        )
        assert result.status is expected
    finally:
        managed.close()


def test_promotion_journal_validates_complete_control_before_staging(
    tmp_path: Path,
) -> None:
    events: list[str] = []
    data_root = tmp_path / "xdg"
    data_root.mkdir()
    managed = market_v4_cutover.ManagedRootFd.open(data_root)
    journal = PromotionJournal(
        managed,
        "promotion-001",
        now=lambda: "",
        boundary_hook=events.append,
    )
    try:
        with pytest.raises(CutoverSafetyError, match="timestamp"):
            journal.append(
                PromotionState.VALIDATED,
                identities=_promotion_identities(PromotionState.VALIDATED),
            )
        assert "intent_file_fsync_before" not in events
        staging = data_root / (
            "operations/market-v4-cutover/journal-controls/promotion-001/staging"
        )
        assert list(staging.iterdir()) == []
    finally:
        managed.close()


@pytest.mark.parametrize(
    "resolution_failure",
    [
        "resolution_source_parent_fsync_before",
        "resolution_destination_parent_fsync_before",
    ],
)
def test_promotion_journal_resolution_cleanup_failure_never_authorizes_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    resolution_failure: str,
) -> None:
    def fail_resolution(stage: str) -> None:
        if stage == resolution_failure:
            raise OSError(errno.EIO, f"injected {stage}")

    real_unlink = market_v4_cutover.os.unlink
    real_fsync = market_v4_cutover.os.fsync

    def fail_resolution_cleanup_unlink(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        *,
        dir_fd: int | None = None,
    ) -> None:
        if os.fspath(path).endswith(".resolution.json"):
            raise OSError(errno.EIO, "injected resolution cleanup unlink")
        real_unlink(path, dir_fd=dir_fd)

    def fail_resolution_cleanup_fsync(fd: int) -> None:
        names = os.listdir(fd) if stat.S_ISDIR(os.fstat(fd).st_mode) else []
        if any(name.endswith(".resolution.json") for name in names):
            raise OSError(errno.EIO, "injected resolution cleanup fsync")
        real_fsync(fd)

    data_root = tmp_path / resolution_failure
    managed, journal = _promotion_journal(
        data_root, boundary_hook=fail_resolution
    )
    monkeypatch.setattr(market_v4_cutover.os, "unlink", fail_resolution_cleanup_unlink)
    monkeypatch.setattr(market_v4_cutover.os, "fsync", fail_resolution_cleanup_fsync)
    try:
        result = journal.append(
            PromotionState.VALIDATED,
            identities=_promotion_identities(PromotionState.VALIDATED),
        )
        assert result.status is PromotionAppendStatus.INDETERMINATE
        with pytest.raises(CutoverSafetyError, match="authorization|fenced"):
            journal.read_validated()
    finally:
        managed.close()
        monkeypatch.undo()

    fresh_managed, fresh = _promotion_journal(data_root)
    try:
        with pytest.raises(CutoverSafetyError, match="authorization|fenced"):
            fresh.read_validated()
    finally:
        fresh_managed.close()


def test_promotion_journal_clean_append_authorizes_only_its_instance(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "xdg"
    managed, journal = _promotion_journal(data_root)
    try:
        result = journal.append(
            PromotionState.VALIDATED,
            identities=_promotion_identities(PromotionState.VALIDATED),
        )
        assert result.status is PromotionAppendStatus.COMMITTED
        assert journal.read_validated() == (result.record,)
    finally:
        managed.close()

    fresh_managed, fresh = _promotion_journal(data_root)
    try:
        with pytest.raises(CutoverSafetyError, match="authorization"):
            fresh.read_validated()
        recovered = fresh.recover(result.attempt_id)
        assert recovered.status is PromotionAppendStatus.COMMITTED
        assert fresh.read_validated() == (result.record,)
    finally:
        fresh_managed.close()


def test_promotion_journal_identity_drift_revokes_live_authorization(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "xdg"
    managed, journal = _promotion_journal(data_root)
    try:
        result = journal.append(
            PromotionState.VALIDATED,
            identities=_promotion_identities(PromotionState.VALIDATED),
        )
        assert result.status is PromotionAppendStatus.COMMITTED
        resolution = data_root / (
            "operations/market-v4-cutover/journal-controls/promotion-001/"
            "00000002.resolution.json"
        )
        replacement = resolution.with_suffix(".replacement")
        replacement.write_bytes(resolution.read_bytes())
        os.replace(replacement, resolution)
        with pytest.raises(CutoverSafetyError, match="authorization.*identity"):
            journal.read_validated()
    finally:
        managed.close()


def _changing_code_version(*versions: str):
    calls: list[str] = []
    iterator = iter(versions)

    def code_version() -> str:
        value = next(iterator)
        calls.append(value)
        return value

    return code_version, calls


def _retained_source(
    data_root: Path,
    *,
    source_report_id: str = "market-v4-rehearsal-20260715-r10",
    status: str = "passed",
) -> tuple[MarketV4CutoverService, Path, SmokeConfig]:
    config = SmokeConfig("7203", "production/smoke", "primeMarket")
    active_config = data_root / "config/default.yaml"
    active_config.parent.mkdir(parents=True, exist_ok=True)
    active_config.write_text("shared_config: {}\n")
    active_strategy = data_root / "strategies/production/smoke.yaml"
    active_strategy.parent.mkdir(parents=True, exist_ok=True)
    active_strategy.write_text("name: smoke\n")
    retained_root = (
        data_root
        / "operations/market-v4-cutover/rehearsals"
        / source_report_id
        / "root"
    )
    (retained_root / "market-timeseries/parquet/stock_data").mkdir(parents=True)
    (retained_root / "market-timeseries/market.duckdb").write_bytes(b"duckdb-v4")
    (
        retained_root / "market-timeseries/parquet/stock_data/part.parquet"
    ).write_bytes(b"retained-rows")
    shutil.copytree(data_root / "config", retained_root / "config")
    shutil.copytree(data_root / "strategies", retained_root / "strategies")
    service = _service(
        data_root,
        duckdb=FakeDuckDb(
            MarketSourceMetadata(4, "local_projection_v2_event_time")
        ),
    )
    _write_report(
        data_root,
        source_report_id,
        {
            "reportId": source_report_id,
            "phase": "rehearsal",
            "status": status,
            "codeVersion": "cafebabe",
            "targetRootFingerprint": service.root_fingerprint(data_root),
            "smokeConfig": {
                "symbol": config.symbol,
                "strategy": config.strategy,
                "datasetPreset": config.dataset_preset,
            },
            "serverProcessJoined": True,
            "workerProcessJoined": True,
            "errorMessage": "arbitrary source failure diagnostic",
        },
    )
    return service, retained_root, config


def _read_operation_report(data_root: Path, report_id: str) -> dict[str, object]:
    return json.loads(
        (
            data_root
            / "operations/market-v4-cutover/reports"
            / report_id
            / "report.json"
        ).read_text()
    )


def _retained_promotion_source(
    data_root: Path,
) -> tuple[MarketV4CutoverService, Path, SmokeConfig]:
    service, retained_root, config = _retained_source(data_root)
    service.runtime = FakeRuntime(apis=[FakeApi()])
    service.rehearse_retained(
        "market-v4-retained-20260715-r13",
        source_rehearsal_report_id="market-v4-rehearsal-20260715-r10",
        config=config,
        inherited_environment={},
    )
    with market_v4_cutover.MarketOperationLease.acquire(
        data_root, exclusive=True
    ):
        pass
    return service, retained_root, config


@pytest.mark.parametrize(
    "mutation",
    [
        "retained_report_sha_drift",
        "source_report_sha_drift",
        "provenance_drift",
        "retained_configuration_drift",
        "active_root_drift",
        "code_drift",
        "schema_v3",
        "wrong_adjustment_mode",
        "inexact_lineage",
        "database_identity_drift",
        "parquet_identity_drift",
        "source_root_replacement",
        "source_ancestor_replacement",
        "source_market_leaf_replacement",
        "live_retained_lease",
        "nonempty_wal",
        "mismatched_smoke_config",
        "unexpected_retained_artifact",
        "cross_device",
        "unavailable_exchange",
        "existing_report",
        "existing_journal",
        "existing_journal_control",
        "existing_journal_lock",
        "existing_holding",
        "existing_quarantine",
        "existing_backup_id",
        "existing_consumed_marker",
    ],
)
def test_promote_retained_rejects_ineligible_source_before_any_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    report_id = "market-v4-active-20260716"
    retained_report_id = "market-v4-retained-20260715-r13"
    backup_id = "market-v3-pre-v4-20260716"
    source_report_id = "market-v4-rehearsal-20260715-r10"
    retained_report_path = data_root / (
        f"operations/market-v4-cutover/reports/{retained_report_id}/report.json"
    )
    source_report_path = data_root / (
        f"operations/market-v4-cutover/reports/{source_report_id}/report.json"
    )
    competing_lease: market_v4_cutover.MarketOperationLease | None = None
    if mutation == "retained_report_sha_drift":
        original_snapshot = service._promotion_report_snapshot
        calls = 0

        def drift_retained_report(report: str):
            nonlocal calls
            result = original_snapshot(report)
            if report == retained_report_id and calls == 0:
                retained_report_path.write_bytes(
                    retained_report_path.read_bytes() + b" "
                )
                calls += 1
            return result

        monkeypatch.setattr(
            service, "_promotion_report_snapshot", drift_retained_report
        )
    elif mutation == "source_report_sha_drift":
        original_snapshot = service._promotion_report_snapshot
        calls = 0

        def drift_source_report(report: str):
            nonlocal calls
            result = original_snapshot(report)
            if report == source_report_id and calls == 0:
                source_report_path.write_bytes(source_report_path.read_bytes() + b" ")
                calls += 1
            return result

        monkeypatch.setattr(service, "_promotion_report_snapshot", drift_source_report)
    elif mutation == "provenance_drift":
        report = json.loads(retained_report_path.read_text())
        report["sourceRehearsalCodeVersion"] = "0" * 8
        retained_report_path.write_text(json.dumps(report))
    elif mutation == "retained_configuration_drift":
        (retained_root / "config/default.yaml").write_text("drift: true\n")
    elif mutation == "active_root_drift":
        (data_root / "config/default.yaml").write_text("drift: true\n")
    elif mutation == "code_drift":
        service.code_version, _calls = _changing_code_version("deadbeef", "cafebabe")
    elif mutation == "schema_v3":
        service.duckdb = FakeDuckDb(
            MarketSourceMetadata(3, "local_projection_v2_event_time")
        )
    elif mutation == "wrong_adjustment_mode":
        service.duckdb = FakeDuckDb(MarketSourceMetadata(4, "local_projection_v1"))
    elif mutation == "inexact_lineage":
        service.duckdb = FakeDuckDb(
            MarketSourceMetadata(
                4,
                "local_projection_v2_event_time",
                adjusted_metrics_ready=False,
            )
        )
    elif mutation == "database_identity_drift":
        database = retained_root / "market-timeseries/market.duckdb"
        database.write_bytes(database.read_bytes() + b"drift")
    elif mutation == "parquet_identity_drift":
        parquet = retained_root / "market-timeseries/parquet/stock_data/part.parquet"
        parquet.write_bytes(parquet.read_bytes() + b"drift")
    elif mutation == "source_root_replacement":
        detached = retained_root.with_name("root.detached")
        retained_root.rename(detached)
        retained_root.symlink_to(detached, target_is_directory=True)
    elif mutation == "source_ancestor_replacement":
        rehearsals = retained_root.parents[1]
        rehearsals.rename(rehearsals.with_name("rehearsals.detached"))
        rehearsals.mkdir()
    elif mutation == "source_market_leaf_replacement":
        market = retained_root / "market-timeseries"
        detached = retained_root / "market-timeseries.detached"
        market.rename(detached)
        shutil.copytree(detached, market)
    elif mutation == "live_retained_lease":
        competing_lease = market_v4_cutover.MarketOperationLease.acquire(
            retained_root, exclusive=True
        )
    elif mutation == "nonempty_wal":
        (retained_root / "market-timeseries/market.duckdb.wal").write_bytes(b"pending")
    elif mutation == "mismatched_smoke_config":
        config = SmokeConfig("9984", config.strategy, config.dataset_preset)
    elif mutation == "unexpected_retained_artifact":
        (retained_root / "market-timeseries/unexpected").write_bytes(b"foreign")
    elif mutation == "cross_device":

        def cross_device(
            _retained_lease: market_v4_cutover.MarketOperationLease,
        ) -> None:
            raise CutoverSafetyError("same device")

        monkeypatch.setattr(
            service, "_assert_promotion_exchange_capability", cross_device
        )
    elif mutation == "unavailable_exchange":
        monkeypatch.setattr(market_v4_cutover.sys, "platform", "linux")
    else:
        destination = {
            "existing_report": Path("reports") / report_id,
            "existing_journal": Path("journals") / report_id,
            "existing_journal_control": Path("journal-controls") / report_id,
            "existing_journal_lock": Path("journal-locks") / f"{report_id}.lock",
            "existing_holding": Path("holding") / report_id,
            "existing_quarantine": Path("quarantine") / report_id,
            "existing_backup_id": Path("backups") / backup_id,
            "existing_consumed_marker": Path("consumed") / f"{retained_report_id}.json",
        }[mutation]
        existing = data_root / "operations/market-v4-cutover" / destination
        if mutation == "existing_consumed_marker":
            existing.parent.mkdir(parents=True)
            existing.write_text("consumed")
        else:
            existing.mkdir(parents=True)

    mutation_events: list[str] = []

    def mutation_forbidden(*_args: object, **_kwargs: object) -> None:
        mutation_events.append("mutation")
        raise AssertionError("promotion mutation hook ran during eligibility")

    monkeypatch.setattr(service, "_backup_under_lease", mutation_forbidden)
    monkeypatch.setattr(service, "_managed_mutation_hook", mutation_forbidden)
    monkeypatch.setattr(service, "_rename_at_hook", mutation_forbidden)
    monkeypatch.setattr(service.atomic_exchange, "exchange", mutation_forbidden)
    service.runtime = FakeRuntime()

    try:
        with pytest.raises(CutoverSafetyError):
            with service._retained_promotion_eligibility_scope(
                report_id=report_id,
                retained_report_id=retained_report_id,
                backup_id=backup_id,
                config=config,
            ):
                raise AssertionError("ineligible promotion entered operation scope")
    finally:
        if competing_lease is not None:
            competing_lease.release()

    assert mutation_events == []
    assert service.runtime.start_calls == 0


def test_promote_retained_accepts_recorded_report_code_under_newer_clean_code(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service, _retained_root, config = _retained_promotion_source(data_root)
    retained_report = data_root / (
        "operations/market-v4-cutover/reports/"
        "market-v4-retained-20260715-r13/report.json"
    )
    payload = json.loads(retained_report.read_text())
    payload["codeVersion"] = "59f41f2e"
    retained_report.write_text(json.dumps(payload))
    service.code_version = lambda: "feedface"

    with service._retained_promotion_eligibility_scope(
        report_id="market-v4-active-20260716",
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
        config=config,
    ) as eligibility:
        assert eligibility.retained_report_id == "market-v4-retained-20260715-r13"


@pytest.mark.parametrize("missing_lock", ["active", "retained"])
def test_promote_retained_requires_existing_lock_without_recreating_it(
    tmp_path: Path,
    missing_lock: str,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    root = data_root if missing_lock == "active" else retained_root
    lock = root / ".market-timeseries.operation.lock"
    lock.unlink()

    with pytest.raises(CutoverSafetyError, match="lock"):
        with service._retained_promotion_eligibility_scope(
            report_id="market-v4-active-20260716",
            retained_report_id="market-v4-retained-20260715-r13",
            backup_id="market-v3-pre-v4-20260716",
            config=config,
        ):
            raise AssertionError("missing lock entered eligibility scope")

    assert not lock.exists()


def test_promote_retained_existing_lease_acquisition_is_metadata_read_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, _retained_root, config = _retained_promotion_source(data_root)
    real_open = market_v4_cutover.os.open
    lock_open_flags: list[int] = []

    def record_open(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        if os.fspath(path) == ".market-timeseries.operation.lock":
            lock_open_flags.append(flags)
        return real_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(market_v4_cutover.os, "open", record_open)
    monkeypatch.setattr(
        market_v4_cutover.os,
        "fchmod",
        lambda *_args: (_ for _ in ()).throw(
            AssertionError("eligibility lease changed lock metadata")
        ),
    )

    with service._retained_promotion_eligibility_scope(
        report_id="market-v4-active-20260716",
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
        config=config,
    ):
        pass

    assert len(lock_open_flags) == 2
    assert all(flags & os.O_CREAT == 0 for flags in lock_open_flags)


def test_existing_operation_lease_rejects_lock_replacement_at_flock_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = tmp_path / "xdg"
    data_root.mkdir()
    with market_v4_cutover.MarketOperationLease.acquire(
        data_root, exclusive=True
    ):
        pass
    lock = data_root / ".market-timeseries.operation.lock"
    real_flock = market_v4_cutover.fcntl.flock
    replaced = False

    def replace_before_flock(fd: int, operation: int) -> None:
        nonlocal replaced
        if operation & market_v4_cutover.fcntl.LOCK_EX and not replaced:
            replaced = True
            lock.rename(lock.with_suffix(".detached"))
            lock.write_bytes(b"replacement")
        real_flock(fd, operation)

    monkeypatch.setattr(market_v4_cutover.fcntl, "flock", replace_before_flock)

    with pytest.raises(CutoverSafetyError, match="identity"):
        market_v4_cutover.MarketOperationLease.acquire_existing(
            data_root, exclusive=True
        )


@pytest.mark.parametrize("late_drift", ["source_report", "active_payload"])
def test_promote_retained_rejects_late_eligibility_drift_before_yield(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    late_drift: str,
) -> None:
    data_root = _market_root(tmp_path)
    service, _retained_root, config = _retained_promotion_source(data_root)
    retained_report_id = "market-v4-retained-20260715-r13"
    source_report = data_root / (
        "operations/market-v4-cutover/reports/"
        "market-v4-rehearsal-20260715-r10/report.json"
    )
    active_database = data_root / "market-timeseries/market.duckdb"
    original_snapshot = service._promotion_report_snapshot
    retained_reads = 0

    def drift_at_final_boundary(report_id: str):
        nonlocal retained_reads
        result = original_snapshot(report_id)
        if report_id == retained_report_id:
            retained_reads += 1
            if retained_reads == 4:
                if late_drift == "source_report":
                    source_report.write_bytes(source_report.read_bytes() + b" ")
                else:
                    active_database.write_bytes(active_database.read_bytes() + b"drift")
        return result

    monkeypatch.setattr(
        service,
        "_promotion_report_snapshot",
        drift_at_final_boundary,
    )

    with pytest.raises(CutoverSafetyError, match="changed|identity"):
        with service._retained_promotion_eligibility_scope(
            report_id="market-v4-active-20260716",
            retained_report_id=retained_report_id,
            backup_id="market-v3-pre-v4-20260716",
            config=config,
        ):
            raise AssertionError("late drift entered eligibility scope")


def test_promote_retained_uses_active_then_retained_lock_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    acquired: list[Path] = []
    original_acquire = market_v4_cutover.MarketOperationLease.acquire_existing

    def recording_acquire(
        cls: type[market_v4_cutover.MarketOperationLease],
        root: Path,
        *,
        exclusive: bool,
        blocking: bool = False,
    ) -> market_v4_cutover.MarketOperationLease:
        del cls
        acquired.append(root)
        return original_acquire(root, exclusive=exclusive, blocking=blocking)

    monkeypatch.setattr(
        market_v4_cutover.MarketOperationLease,
        "acquire_existing",
        classmethod(recording_acquire),
    )

    with service._retained_promotion_eligibility_scope(
        report_id="market-v4-active-20260716",
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
        config=config,
    ) as eligibility:
        assert eligibility.retained_root == retained_root

    assert acquired == [data_root, retained_root]


def test_promote_retained_holds_both_leases_through_eligibility_scope(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)

    with service._retained_promotion_eligibility_scope(
        report_id="market-v4-active-20260716",
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
        config=config,
    ):
        for root in (data_root, retained_root):
            with pytest.raises(CutoverSafetyError, match="operation lease"):
                market_v4_cutover.MarketOperationLease.acquire(
                    root,
                    exclusive=True,
                )

    for root in (data_root, retained_root):
        with market_v4_cutover.MarketOperationLease.acquire(root, exclusive=True):
            pass


def _prepare_retained_promotion(
    service: MarketV4CutoverService,
    config: SmokeConfig,
    *,
    backup_id: str = "market-v3-pre-v4-20260716",
):
    report_id = "market-v4-active-20260716"
    with service._retained_promotion_eligibility_scope(
        report_id=report_id,
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id=backup_id,
        config=config,
    ) as eligibility:
        journal = PromotionJournal(
            service._managed(),
            report_id,
            now=lambda: "2026-07-16T00:00:00Z",
        )
        preparation = service._prepare_retained_promotion_under_leases(
            eligibility,
            backup_id=backup_id,
            journal=journal,
        )
        records = journal.read_validated()
    return preparation, records


def test_promotion_creates_and_verifies_backup_inside_active_lease(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, _retained_root, config = _retained_promotion_source(data_root)
    events: list[str] = []
    original_verify = service._verify_backup_managed

    def verify(backup_id: str, *, require_current_root: bool = True):
        assert service._active_lease is not None
        events.append("backup_verified")
        return original_verify(
            backup_id,
            require_current_root=require_current_root,
        )

    monkeypatch.setattr(service, "_verify_backup_managed", verify)
    monkeypatch.setattr(
        service,
        "_preflight_under_lease",
        lambda: (_ for _ in ()).throw(
            AssertionError("promotion used full-rebuild preflight")
        ),
    )

    preparation, records = _prepare_retained_promotion(service, config)

    backup = data_root / (
        "operations/market-v4-cutover/backups/market-v3-pre-v4-20260716"
    )
    assert events == ["backup_verified"]
    assert (backup / "payload/market.duckdb").read_bytes() == b"duckdb-v3"
    assert (backup / "payload/parquet/stock_data/part.parquet").read_bytes() == b"rows"
    assert preparation.backup_manifest_sha256 == service._sha256(
        backup / "manifest.json"
    )
    assert service._payload_manifest_entries(preparation.backup_payload_identity) == (
        service._payload_manifest_entries(preparation.eligibility.active_market_identity)
    )
    backup_database = preparation.backup_payload_identity["marketDuckdb"]
    active_database = preparation.eligibility.active_market_identity["marketDuckdb"]
    assert isinstance(backup_database, dict)
    assert isinstance(active_database, dict)
    assert (backup_database["device"], backup_database["inode"]) != (
        active_database["device"],
        active_database["inode"],
    )
    assert [record.state for record in records] == [
        PromotionState.VALIDATED,
        PromotionState.RUNTIMES_DETACHED,
        PromotionState.PREPARED,
    ]


def test_promotion_backup_requires_payload_bytes_plus_reserve_not_rebuild_space(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    source_bytes = sum(
        path.stat().st_size
        for path in (data_root / "market-timeseries").rglob("*")
        if path.is_file()
    )
    required_bytes = source_bytes + max(source_bytes // 20, 1)
    assert required_bytes < source_bytes * 4
    service, _retained_root, config = _retained_promotion_source(data_root)
    service.disk_free_bytes = lambda _path: required_bytes

    preparation, _records = _prepare_retained_promotion(service, config)

    assert preparation.backup_manifest_sha256

    low_root = _market_root(tmp_path / "low")
    low_service, _retained_root, low_config = _retained_promotion_source(low_root)
    low_service.disk_free_bytes = lambda _path: required_bytes - 1
    with low_service._retained_promotion_eligibility_scope(
        report_id="market-v4-active-20260716",
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
        config=low_config,
    ) as eligibility:
        journal = PromotionJournal(
            low_service._managed(),
            "market-v4-active-20260716",
            now=lambda: "2026-07-16T00:00:00Z",
        )
        with pytest.raises(CutoverSafetyError, match="free space"):
            low_service._prepare_retained_promotion_under_leases(
                eligibility,
                backup_id="market-v3-pre-v4-20260716",
                journal=journal,
            )
    assert not (
        low_root / "operations/market-v4-cutover/backups/market-v3-pre-v4-20260716"
    ).exists()


def test_promotion_rejects_backup_identity_mismatch_before_detach(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    runtime = retained_root / (
        "market-timeseries/.cutover-runtime-market-v4-retained-20260715-r13"
    )
    original_verify = service._verify_backup_managed

    def drift_after_verify(backup_id: str, *, require_current_root: bool = True):
        result = original_verify(
            backup_id,
            require_current_root=require_current_root,
        )
        database = data_root / "market-timeseries/market.duckdb"
        database.write_bytes(database.read_bytes() + b"drift")
        return result

    monkeypatch.setattr(service, "_verify_backup_managed", drift_after_verify)

    with pytest.raises(CutoverSafetyError, match="identity"):
        _prepare_retained_promotion(service, config)

    assert runtime.is_dir()
    assert not (
        data_root / "operations/market-v4-cutover/holding/market-v4-active-20260716"
    ).exists()


def test_promotion_rejects_duplicate_backup_manifest_path_before_detach(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    runtime = retained_root / (
        "market-timeseries/.cutover-runtime-market-v4-retained-20260715-r13"
    )
    original_copy = service._copy_backup_under_snapshot

    def duplicate_manifest_path(*args: object, **kwargs: object) -> None:
        original_copy(*args, **kwargs)
        manifest = data_root / (
            "operations/market-v4-cutover/backups/"
            "market-v3-pre-v4-20260716/manifest.json"
        )
        payload = json.loads(manifest.read_text())
        payload["files"].append(dict(payload["files"][0]))
        manifest.chmod(0o600)
        manifest.write_text(json.dumps(payload))

    monkeypatch.setattr(
        service,
        "_copy_backup_under_snapshot",
        duplicate_manifest_path,
    )

    with pytest.raises(CutoverSafetyError, match="duplicate"):
        _prepare_retained_promotion(service, config)

    assert runtime.is_dir()
    assert not (
        data_root / "operations/market-v4-cutover/holding/market-v4-active-20260716"
    ).exists()


def test_promotion_detaches_only_report_proven_runtimes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    fsynced_directories: list[Path] = []
    original_fsync_dir = market_v4_cutover.ManagedRootFd.fsync_dir

    def record_fsync_dir(
        managed: market_v4_cutover.ManagedRootFd,
        relative: Path,
    ) -> None:
        fsynced_directories.append(relative)
        original_fsync_dir(managed, relative)

    monkeypatch.setattr(
        market_v4_cutover.ManagedRootFd,
        "fsync_dir",
        record_fsync_dir,
    )
    service.runtime = FakeRuntime(apis=[FakeApi()])
    service.rehearse_retained(
        "market-v4-retained-20260715-r12",
        source_rehearsal_report_id="market-v4-rehearsal-20260715-r10",
        config=config,
        inherited_environment={},
    )
    market = retained_root / "market-timeseries"
    source_runtime = market / ".cutover-runtime-market-v4-rehearsal-20260715-r10"
    source_runtime.mkdir()
    (source_runtime / "evidence").write_text("source")
    (market / "duckdb-tmp").mkdir()
    (market / "market.duckdb.wal").touch()

    preparation, _records = _prepare_retained_promotion(service, config)

    assert preparation.detached_runtime_names == (
        ".cutover-runtime-market-v4-rehearsal-20260715-r10",
        ".cutover-runtime-market-v4-retained-20260715-r12",
        ".cutover-runtime-market-v4-retained-20260715-r13",
    )
    assert preparation.holding_directory_identity == {
        "device": preparation.holding_root.stat().st_dev,
        "inode": preparation.holding_root.stat().st_ino,
    }
    assert {artifact.name for artifact in preparation.detached_artifacts} == {
        ".cutover-runtime-market-v4-rehearsal-20260715-r10",
        ".cutover-runtime-market-v4-retained-20260715-r12",
        ".cutover-runtime-market-v4-retained-20260715-r13",
        "duckdb-tmp",
        "market.duckdb.wal",
    }
    source_evidence = next(
        artifact
        for artifact in preparation.detached_artifacts
        if artifact.name
        == ".cutover-runtime-market-v4-rehearsal-20260715-r10"
    )
    assert source_evidence.kind == "directory"
    assert source_evidence.files["evidence"]["sha256"] == service._sha256(
        preparation.holding_root
        / ".cutover-runtime-market-v4-rehearsal-20260715-r10/evidence"
    )
    assert set(path.name for path in preparation.holding_root.iterdir()) == {
        ".cutover-runtime-market-v4-rehearsal-20260715-r10",
        ".cutover-runtime-market-v4-retained-20260715-r12",
        ".cutover-runtime-market-v4-retained-20260715-r13",
        "duckdb-tmp",
        "market.duckdb.wal",
    }
    assert set(path.name for path in market.iterdir()) == {"market.duckdb", "parquet"}
    assert Path("operations/market-v4-cutover") in fsynced_directories
    assert Path("operations/market-v4-cutover/holding") in fsynced_directories


def test_promotion_rejects_prefix_matched_unproven_runtime(tmp_path: Path) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    unproven = retained_root / (
        "market-timeseries/.cutover-runtime-market-v4-retained-20260715-r13-copy"
    )
    unproven.mkdir()

    with pytest.raises(CutoverSafetyError, match="unexpected artifact"):
        _prepare_retained_promotion(service, config)

    assert unproven.is_dir()
    assert not (
        data_root / "operations/market-v4-cutover/backups/market-v3-pre-v4-20260716"
    ).exists()


def test_promotion_requires_canonical_payload_after_detach(tmp_path: Path) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    report_id = "market-v4-active-20260716"
    with service._retained_promotion_eligibility_scope(
        report_id=report_id,
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
        config=config,
    ) as eligibility:
        (retained_root / "market-timeseries/foreign").write_text("unexpected")
        journal = PromotionJournal(
            service._managed(),
            report_id,
            now=lambda: "2026-07-16T00:00:00Z",
        )
        with pytest.raises(CutoverSafetyError, match="canonical|unexpected"):
            service._prepare_retained_promotion_under_leases(
                eligibility,
                backup_id="market-v3-pre-v4-20260716",
                journal=journal,
            )
        assert [record.state for record in journal.read_validated()] == [
            PromotionState.VALIDATED
        ]


def test_promotion_aborts_on_not_committed_journal_append(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    report_id = "market-v4-active-20260716"
    runtime = retained_root / (
        "market-timeseries/.cutover-runtime-market-v4-retained-20260715-r13"
    )
    with service._retained_promotion_eligibility_scope(
        report_id=report_id,
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
        config=config,
    ) as eligibility:
        journal = PromotionJournal(
            service._managed(),
            report_id,
            now=lambda: "2026-07-16T00:00:00Z",
        )
        monkeypatch.setattr(
            journal,
            "append",
            lambda *_args, **_kwargs: market_v4_cutover.PromotionAppendResult(
                PromotionAppendStatus.NOT_COMMITTED,
                None,
                "attempt-not-committed",
            ),
        )
        with pytest.raises(CutoverSafetyError, match="not committed"):
            service._prepare_retained_promotion_under_leases(
                eligibility,
                backup_id="market-v3-pre-v4-20260716",
                journal=journal,
            )

    assert runtime.is_dir()
    assert not (
        data_root / f"operations/market-v4-cutover/holding/{report_id}"
    ).exists()


def test_promotion_indeterminate_journal_append_fences_both_leases(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    report_id = "market-v4-active-20260716"
    leaked_fds: tuple[int, int]
    with pytest.raises(CutoverSafetyError, match="indeterminate"):
        with service._retained_promotion_eligibility_scope(
            report_id=report_id,
            retained_report_id="market-v4-retained-20260715-r13",
            backup_id="market-v3-pre-v4-20260716",
            config=config,
        ) as eligibility:
            assert service._active_lease is not None
            assert service._retained_lease is not None
            leaked_fds = (service._active_lease.fd, service._retained_lease.fd)
            journal = PromotionJournal(
                service._managed(),
                report_id,
                now=lambda: "2026-07-16T00:00:00Z",
            )
            monkeypatch.setattr(
                journal,
                "append",
                lambda *_args, **_kwargs: market_v4_cutover.PromotionAppendResult(
                    PromotionAppendStatus.INDETERMINATE,
                    None,
                    "attempt-indeterminate",
                ),
            )
            service._prepare_retained_promotion_under_leases(
                eligibility,
                backup_id="market-v3-pre-v4-20260716",
                journal=journal,
            )

    try:
        for root in (data_root, retained_root):
            with pytest.raises(CutoverSafetyError, match="operation lease"):
                market_v4_cutover.MarketOperationLease.acquire_existing(
                    root,
                    exclusive=True,
                )
    finally:
        for fd in leaked_fds:
            os.close(fd)


@pytest.mark.parametrize(
    ("fault_state", "status"),
    [
        (PromotionState.RUNTIMES_DETACHED, PromotionAppendStatus.NOT_COMMITTED),
        (PromotionState.PREPARED, PromotionAppendStatus.NOT_COMMITTED),
        (PromotionState.RUNTIMES_DETACHED, PromotionAppendStatus.INDETERMINATE),
        (PromotionState.PREPARED, PromotionAppendStatus.INDETERMINATE),
    ],
)
def test_promotion_preparation_append_faults_stop_or_fence_at_exact_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault_state: PromotionState,
    status: PromotionAppendStatus,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    report_id = "market-v4-active-20260716"
    leaked_fds: tuple[int, int] | None = None
    with pytest.raises(CutoverSafetyError, match="indeterminate|not committed"):
        with service._retained_promotion_eligibility_scope(
            report_id=report_id,
            retained_report_id="market-v4-retained-20260715-r13",
            backup_id="market-v3-pre-v4-20260716",
            config=config,
        ) as eligibility:
            assert service._active_lease is not None
            assert service._retained_lease is not None
            journal = PromotionJournal(
                service._managed(),
                report_id,
                now=lambda: "2026-07-16T00:00:00Z",
            )
            original_append = journal.append

            def append(state: PromotionState, **kwargs: object):
                nonlocal leaked_fds
                if state is fault_state:
                    if status is PromotionAppendStatus.INDETERMINATE:
                        leaked_fds = (
                            service._active_lease.fd,
                            service._retained_lease.fd,
                        )
                    return market_v4_cutover.PromotionAppendResult(
                        status,
                        None,
                        f"attempt-{state.value}-{status.value}",
                    )
                return original_append(state, **kwargs)  # type: ignore[arg-type]

            monkeypatch.setattr(journal, "append", append)
            service._prepare_retained_promotion_under_leases(
                eligibility,
                backup_id="market-v3-pre-v4-20260716",
                journal=journal,
            )

    if status is PromotionAppendStatus.INDETERMINATE:
        assert leaked_fds is not None
        try:
            for root in (data_root, retained_root):
                with pytest.raises(CutoverSafetyError, match="operation lease"):
                    market_v4_cutover.MarketOperationLease.acquire_existing(
                        root,
                        exclusive=True,
                    )
        finally:
            for fd in leaked_fds:
                os.close(fd)


class _TestAtomicExchange:
    def require_capability(self) -> None:
        return None

    def exchange(
        self,
        managed_root: market_v4_cutover.ManagedRootFd,
        left: Path,
        right: Path,
    ) -> None:
        left_parent, left_name = managed_root.open_parent(left)
        right_parent, right_name = managed_root.open_parent(right)
        temporary = f".test-exchange-{time.time_ns()}"
        try:
            os.rename(
                left_name, temporary, src_dir_fd=left_parent, dst_dir_fd=left_parent
            )
            os.rename(
                right_name, left_name, src_dir_fd=right_parent, dst_dir_fd=left_parent
            )
            os.rename(
                temporary, right_name, src_dir_fd=left_parent, dst_dir_fd=right_parent
            )
            os.fsync(left_parent)
            os.fsync(right_parent)
        finally:
            os.close(left_parent)
            os.close(right_parent)


def _run_retained_promotion(
    service: MarketV4CutoverService,
    config: SmokeConfig,
    *,
    inherited_environment: dict[str, str] | None = None,
):
    report_id = "market-v4-active-20260716"
    with service._retained_promotion_eligibility_scope(
        report_id=report_id,
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
        config=config,
    ) as eligibility:
        journal = PromotionJournal(
            service._managed(),
            report_id,
            now=lambda: "2026-07-16T00:00:00Z",
        )
        preparation = service._prepare_retained_promotion_under_leases(
            eligibility,
            backup_id="market-v3-pre-v4-20260716",
            journal=journal,
        )
        result = service._promote_retained_under_leases(
            preparation,
            journal=journal,
            config=config,
            inherited_environment=inherited_environment or {},
        )
        states = tuple(record.state for record in journal.read_validated())
    return result, states


def _market_identity_at_root(
    service: MarketV4CutoverService,
    root: Path,
) -> dict[str, object]:
    with market_v4_cutover.ManagedRootFd.open(root) as managed:
        return service._market_tree_identity(managed.fd)


def test_public_promote_retained_runs_gated_promotion_and_recovers_same_id(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service, _retained_root, config = _retained_promotion_source(data_root)
    service.atomic_exchange = _TestAtomicExchange()
    runtime = FakeRuntime(apis=[FakeApi()])
    service.runtime = runtime

    result = service.promote_retained(
        "market-v4-active-20260716",
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
        config=config,
    )

    assert result.report_id == "market-v4-active-20260716"
    assert runtime.start_calls == 1
    fresh_service = _service(
        data_root,
        duckdb=FakeDuckDb(
            MarketSourceMetadata(4, "local_projection_v2_event_time")
        ),
    )
    assert (
        fresh_service.promote_retained(
            "market-v4-active-20260716",
            retained_report_id="market-v4-retained-20260715-r13",
            backup_id="market-v3-pre-v4-20260716",
            config=config,
        )
        == result
    )
    assert runtime.start_calls == 1


def test_promote_retained_atomically_activates_exact_payload_without_sync(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    service.atomic_exchange = _TestAtomicExchange()
    active_before = _market_identity_at_root(service, data_root)
    retained_before = _market_identity_at_root(service, retained_root)
    api = FakeApi()
    runtime = FakeRuntime(apis=[api])
    service.runtime = runtime
    monkeypatch.setattr(
        service,
        "_run_rebuild",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("retained promotion must not rebuild or sync")
        ),
    )

    result, states = _run_retained_promotion(
        service,
        config,
        inherited_environment={
            "PATH": os.environ.get("PATH", ""),
            "JQUANTS_API_KEY": "forbidden",
            "SERVICE_TOKEN": "forbidden",
            "AWS_SECRET_ACCESS_KEY": "forbidden",
            "CREDENTIAL_FILE": "forbidden",
            "JQUANTS_PLAN": "forbidden",
        },
    )

    active_after = _market_identity_at_root(service, data_root)
    quarantine = data_root / (
        "operations/market-v4-cutover/quarantine/market-v4-active-20260716"
    )
    assert result.report_id == "market-v4-active-20260716"
    assert active_after == retained_before
    quarantine_db = quarantine / "market.duckdb"
    active_before_db = active_before["marketDuckdb"]
    assert isinstance(active_before_db, dict)
    assert quarantine_db.stat().st_ino == active_before_db["inode"]
    assert service._sha256(quarantine_db) == active_before_db["sha256"]
    quarantine_fd = os.open(quarantine, os.O_RDONLY | os.O_DIRECTORY)
    try:
        assert service._market_payload_identity(quarantine_fd) == active_before
    finally:
        os.close(quarantine_fd)
    assert set(path.name for path in (data_root / "market-timeseries").iterdir()) == {
        "market.duckdb",
        "parquet",
    }
    assert runtime.start_calls == runtime.stop_calls == 1
    assert len(runtime.retained_lease_fds) == 1
    environment = runtime.environments[0]
    assert not any(
        token in name.upper()
        for name in environment
        for token in ("JQUANTS", "KEY", "TOKEN", "SECRET", "CREDENTIAL", "PLAN")
    )
    forbidden_paths = (
        "/api/db/sync",
        "/api/db/adjusted-metrics/materialize",
        "/api/db/stocks/refresh",
        "/api/db/intraday/sync",
    )
    assert all(
        not any(path.startswith(forbidden) for forbidden in forbidden_paths)
        for _method, path, _payload in api.calls
    )
    log = data_root / (
        "operations/market-v4-cutover/reports/market-v4-active-20260716/active-smoke.log"
    )
    assert "jquants_fetch" not in log.read_text().lower()
    assert states == (
        PromotionState.VALIDATED,
        PromotionState.RUNTIMES_DETACHED,
        PromotionState.PREPARED,
        PromotionState.EXCHANGED,
        PromotionState.QUARANTINED,
        PromotionState.ACTIVE_SMOKE_PASSED,
        PromotionState.CLEANUP_STAGED,
        PromotionState.REPORT_PERSISTED,
        PromotionState.COMMITTED,
    )


def test_promotion_recovery_detects_swap_after_prepared_before_exchanged_record(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    service.atomic_exchange = _TestAtomicExchange()
    active_before = _market_identity_at_root(service, data_root)
    retained_before = _market_identity_at_root(service, retained_root)
    report_id = "market-v4-active-20260716"

    with service._retained_promotion_eligibility_scope(
        report_id=report_id,
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
        config=config,
    ) as eligibility:
        journal = PromotionJournal(service._managed(), report_id, now=service.now)
        preparation = service._prepare_retained_promotion_under_leases(
            eligibility,
            backup_id="market-v3-pre-v4-20260716",
            journal=journal,
        )
        service.atomic_exchange.exchange(
            service._managed(),
            Path("market-timeseries"),
            service._managed_relative(retained_root / "market-timeseries"),
        )
        context = market_v4_cutover.RetainedPromotionContext(
            preparation=preparation,
            journal=journal,
        )

        service._rollback_retained_promotion(context, processes_joined=True)

        assert _market_identity_at_root(service, data_root) == active_before
        assert _market_identity_at_root(service, retained_root) == retained_before
        assert tuple(record.state for record in journal.read_validated())[-2:] == (
            PromotionState.EXCHANGED_BACK,
            PromotionState.ROLLED_BACK,
        )


def test_promotion_smoke_failure_exchanges_back_and_restores_exact_v3(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    service.atomic_exchange = _TestAtomicExchange()
    active_before = _market_identity_at_root(service, data_root)
    retained_before = _market_identity_at_root(service, retained_root)
    service.runtime = FakeRuntime(apis=[FakeApi(parity=False)])

    with pytest.raises(CutoverSafetyError, match="parity failed"):
        _run_retained_promotion(service, config)

    assert _market_identity_at_root(service, data_root) == active_before
    assert _market_identity_at_root(service, retained_root) == retained_before
    assert not (
        data_root
        / "operations/market-v4-cutover/consumed/"
        "market-v4-retained-20260715-r13.json"
    ).exists()
    journal = PromotionJournal(
        service._managed_root_fd
        if service._managed_root_fd is not None
        else market_v4_cutover.ManagedRootFd.open(data_root),
        "market-v4-active-20260716",
        now=service.now,
    )
    managed = journal._managed_root
    try:
        latest = journal.recovery_attempt_id()
        assert journal.recover(latest).status is PromotionAppendStatus.COMMITTED
        assert tuple(record.state for record in journal.read_validated())[-2:] == (
            PromotionState.EXCHANGED_BACK,
            PromotionState.ROLLED_BACK,
        )
    finally:
        if managed is not service._managed_root_fd:
            managed.close()


def test_promotion_unjoined_runtime_defers_rollback_and_fences_both_leases(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    service.atomic_exchange = _TestAtomicExchange()
    class UnjoinedRuntime(FakeRuntime):
        def stop(self, _api: FakeApi) -> None:
            self.stop_calls += 1
            raise market_v4_cutover.RuntimeStopError(
                "injected unjoined runtime", process_joined=False
            )

    service.runtime = UnjoinedRuntime(apis=[FakeApi()])
    leaked_fds: tuple[int, int]

    with pytest.raises(CutoverSafetyError, match="deferred"):
        with service._retained_promotion_eligibility_scope(
            report_id="market-v4-active-20260716",
            retained_report_id="market-v4-retained-20260715-r13",
            backup_id="market-v3-pre-v4-20260716",
            config=config,
        ) as eligibility:
            assert service._active_lease is not None
            assert service._retained_lease is not None
            leaked_fds = (service._active_lease.fd, service._retained_lease.fd)
            journal = PromotionJournal(
                service._managed(), "market-v4-active-20260716", now=service.now
            )
            preparation = service._prepare_retained_promotion_under_leases(
                eligibility,
                backup_id="market-v3-pre-v4-20260716",
                journal=journal,
            )
            service._promote_retained_under_leases(
                preparation,
                journal=journal,
                config=config,
                inherited_environment={},
            )

    try:
        for root in (data_root, retained_root):
            with pytest.raises(CutoverSafetyError, match="operation lease"):
                market_v4_cutover.MarketOperationLease.acquire_existing(
                    root, exclusive=True
                )
    finally:
        for fd in leaked_fds:
            os.close(fd)


def test_promotion_recovery_matching_incomplete_journal_rolls_back(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    service.atomic_exchange = _TestAtomicExchange()
    active_before = _market_identity_at_root(service, data_root)
    retained_before = _market_identity_at_root(service, retained_root)
    report_id = "market-v4-active-20260716"

    with service._retained_promotion_eligibility_scope(
        report_id=report_id,
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
        config=config,
    ) as eligibility:
        journal = PromotionJournal(service._managed(), report_id, now=service.now)
        service._prepare_retained_promotion_under_leases(
            eligibility,
            backup_id="market-v3-pre-v4-20260716",
            journal=journal,
        )
        service.atomic_exchange.exchange(
            service._managed(),
            Path("market-timeseries"),
            service._managed_relative(retained_root / "market-timeseries"),
        )

    assert service._recover_retained_promotion(
        report_id,
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
    ) is None
    assert _market_identity_at_root(service, data_root) == active_before
    assert _market_identity_at_root(service, retained_root) == retained_before


def test_promotion_recovery_rejects_mismatched_identity_without_mutation(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    service.atomic_exchange = _TestAtomicExchange()
    report_id = "market-v4-active-20260716"
    with service._retained_promotion_eligibility_scope(
        report_id=report_id,
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
        config=config,
    ) as eligibility:
        journal = PromotionJournal(service._managed(), report_id, now=service.now)
        service._prepare_retained_promotion_under_leases(
            eligibility,
            backup_id="market-v3-pre-v4-20260716",
            journal=journal,
        )
    active_before = _market_identity_at_root(service, data_root)
    retained_before = _market_identity_at_root(service, retained_root)

    with pytest.raises(CutoverSafetyError, match="identity|retained report"):
        service._recover_retained_promotion(
            report_id,
            retained_report_id="market-v4-retained-20260715-r12",
            backup_id="market-v3-pre-v4-20260716",
        )

    assert _market_identity_at_root(service, data_root) == active_before
    assert _market_identity_at_root(service, retained_root) == retained_before


def test_promotion_recovery_valid_committed_report_rejects_replay_without_mutation(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service, _retained_root, config = _retained_promotion_source(data_root)
    service.atomic_exchange = _TestAtomicExchange()
    service.runtime = FakeRuntime(apis=[FakeApi()])
    result, _states = _run_retained_promotion(service, config)
    active_before = _market_identity_at_root(service, data_root)

    recovered = service._recover_retained_promotion(
        result.report_id,
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
    )

    assert recovered == result
    assert _market_identity_at_root(service, data_root) == active_before


@pytest.mark.parametrize(
    "durable_boundary",
    [
        "exchange_fsynced",
        "exchanged_journaled",
        "quarantine_fsynced",
        "quarantined_journaled",
        "smoke_joined",
        "smoke_journaled",
        "held_cleanup_fsynced",
        "report_fsynced",
        "report_journaled",
        "consumed_marker_fsynced",
    ],
)
def test_promotion_failure_at_durable_boundary_restores_exact_v3(
    tmp_path: Path,
    durable_boundary: str,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    service.atomic_exchange = _TestAtomicExchange()
    service.runtime = FakeRuntime(apis=[FakeApi()])
    active_before = _market_identity_at_root(service, data_root)
    retained_before = _market_identity_at_root(service, retained_root)
    retained_artifacts_before = {
        path.name
        for path in (retained_root / "market-timeseries").iterdir()
        if path.name not in {"market.duckdb", "parquet"}
    }

    def fail_at_boundary(stage: str) -> None:
        if stage == durable_boundary:
            raise CutoverSafetyError(f"injected durable boundary: {stage}")

    service._promotion_boundary_hook = fail_at_boundary

    with pytest.raises(CutoverSafetyError, match="injected durable boundary"):
        _run_retained_promotion(service, config)

    assert _market_identity_at_root(service, data_root) == active_before
    assert _market_identity_at_root(service, retained_root) == retained_before
    assert retained_artifacts_before <= {
        path.name for path in (retained_root / "market-timeseries").iterdir()
    }
    assert not (
        data_root
        / "operations/market-v4-cutover/consumed/"
        "market-v4-retained-20260715-r13.json"
    ).exists()


def test_promotion_rollback_uses_verified_backup_only_after_exchange_back_fails(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    service.atomic_exchange = _TestAtomicExchange()
    active_before = _market_identity_at_root(service, data_root)
    retained_before = _market_identity_at_root(service, retained_root)

    class FailingExchange:
        def exchange(self, *_args: object) -> None:
            raise OSError("injected exchange-back failure")

    with service._retained_promotion_eligibility_scope(
        report_id="market-v4-active-20260716",
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
        config=config,
    ) as eligibility:
        journal = PromotionJournal(
            service._managed(), "market-v4-active-20260716", now=service.now
        )
        preparation = service._prepare_retained_promotion_under_leases(
            eligibility,
            backup_id="market-v3-pre-v4-20260716",
            journal=journal,
        )
        service.atomic_exchange.exchange(
            service._managed(),
            Path("market-timeseries"),
            service._managed_relative(retained_root / "market-timeseries"),
        )
        service.atomic_exchange = FailingExchange()  # type: ignore[assignment]
        service._rollback_retained_promotion(
            market_v4_cutover.RetainedPromotionContext(preparation, journal),
            processes_joined=True,
        )

    active_after = _market_identity_at_root(service, data_root)
    assert service._payload_manifest_entries(active_after) == (
        service._payload_manifest_entries(active_before)
    )
    assert _market_identity_at_root(service, retained_root) == retained_before


def test_promotion_rollback_reports_terminal_failure_when_both_paths_fail(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    service.atomic_exchange = _TestAtomicExchange()

    class FailingExchange:
        def exchange(self, *_args: object) -> None:
            raise OSError("injected exchange-back failure")

    with service._retained_promotion_eligibility_scope(
        report_id="market-v4-active-20260716",
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
        config=config,
    ) as eligibility:
        journal = PromotionJournal(
            service._managed(), "market-v4-active-20260716", now=service.now
        )
        preparation = service._prepare_retained_promotion_under_leases(
            eligibility,
            backup_id="market-v3-pre-v4-20260716",
            journal=journal,
        )
        service.atomic_exchange.exchange(
            service._managed(),
            Path("market-timeseries"),
            service._managed_relative(retained_root / "market-timeseries"),
        )
        service.atomic_exchange = FailingExchange()  # type: ignore[assignment]
        monkeypatch.setattr(
            service,
            "_restore_under_lease",
            lambda _backup_id: (_ for _ in ()).throw(
                CutoverSafetyError("injected restore failure")
            ),
        )

        with pytest.raises(CutoverSafetyError, match="Terminal promotion recovery"):
            service._rollback_retained_promotion(
                market_v4_cutover.RetainedPromotionContext(preparation, journal),
                processes_joined=True,
            )


def test_promotion_report_failure_restores_exact_staged_artifacts(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    service.atomic_exchange = _TestAtomicExchange()
    service.runtime = FakeRuntime(apis=[FakeApi()])
    runtime_names = {
        path.name
        for path in (retained_root / "market-timeseries").iterdir()
        if path.name.startswith(".cutover-runtime-")
    }

    def fail_after_report(stage: str) -> None:
        if stage == "report_fsynced":
            raise CutoverSafetyError("injected report crash")

    service._promotion_boundary_hook = fail_after_report

    with pytest.raises(CutoverSafetyError, match="injected report crash"):
        _run_retained_promotion(service, config)

    assert runtime_names <= {
        path.name for path in (retained_root / "market-timeseries").iterdir()
    }
    assert not (
        data_root
        / "operations/market-v4-cutover/cleanup-staging/"
        "market-v4-active-20260716"
    ).exists()


@pytest.mark.parametrize(
    "crash_boundary",
    ["committed_journaled", "cleanup_artifacts_deleted"],
)
def test_promotion_committed_recovery_completes_exact_pending_cleanup(
    tmp_path: Path,
    crash_boundary: str,
) -> None:
    data_root = _market_root(tmp_path)
    service, _retained_root, config = _retained_promotion_source(data_root)
    service.atomic_exchange = _TestAtomicExchange()
    service.runtime = FakeRuntime(apis=[FakeApi()])

    def crash_after_commit(stage: str) -> None:
        if stage == crash_boundary:
            raise RuntimeError("simulated process crash after commit")

    service._promotion_boundary_hook = crash_after_commit
    with pytest.raises(CutoverSafetyError, match="cleanup incomplete"):
        _run_retained_promotion(service, config)

    staging = data_root / (
        "operations/market-v4-cutover/cleanup-staging/"
        "market-v4-active-20260716"
    )
    if crash_boundary == "committed_journaled":
        assert staging.is_dir()
    else:
        assert not staging.exists()

    service._promotion_boundary_hook = lambda _stage: None
    result = service._recover_retained_promotion(
        "market-v4-active-20260716",
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
    )

    assert result is not None
    assert not staging.exists()
    assert (
        data_root
        / "operations/market-v4-cutover/cleanup-results/"
        "market-v4-active-20260716.json"
    ).is_file()


def test_promotion_exchange_exception_reinspects_completed_exchange(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    service.atomic_exchange = _TestAtomicExchange()
    active_before = _market_identity_at_root(service, data_root)
    retained_before = _market_identity_at_root(service, retained_root)

    class SwapThenRaise(_TestAtomicExchange):
        def exchange(self, *args: object) -> None:
            super().exchange(*args)  # type: ignore[arg-type]
            raise OSError("indeterminate exchange result")

    with service._retained_promotion_eligibility_scope(
        report_id="market-v4-active-20260716",
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
        config=config,
    ) as eligibility:
        journal = PromotionJournal(
            service._managed(), "market-v4-active-20260716", now=service.now
        )
        preparation = service._prepare_retained_promotion_under_leases(
            eligibility,
            backup_id="market-v3-pre-v4-20260716",
            journal=journal,
        )
        service.atomic_exchange.exchange(
            service._managed(),
            Path("market-timeseries"),
            service._managed_relative(retained_root / "market-timeseries"),
        )
        service.atomic_exchange = SwapThenRaise()
        monkeypatch.setattr(
            service,
            "_restore_under_lease",
            lambda _backup: (_ for _ in ()).throw(
                AssertionError("backup fallback must not run after completed exchange")
            ),
        )

        service._rollback_retained_promotion(
            market_v4_cutover.RetainedPromotionContext(preparation, journal),
            processes_joined=True,
        )

    assert _market_identity_at_root(service, data_root) == active_before
    assert _market_identity_at_root(service, retained_root) == retained_before


def test_promotion_backup_fallback_journal_is_truthful(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    service.atomic_exchange = _TestAtomicExchange()

    class FailingExchange:
        def exchange(self, *_args: object) -> None:
            raise OSError("exchange unavailable")

    with service._retained_promotion_eligibility_scope(
        report_id="market-v4-active-20260716",
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
        config=config,
    ) as eligibility:
        journal = PromotionJournal(
            service._managed(), "market-v4-active-20260716", now=service.now
        )
        preparation = service._prepare_retained_promotion_under_leases(
            eligibility,
            backup_id="market-v3-pre-v4-20260716",
            journal=journal,
        )
        service.atomic_exchange.exchange(
            service._managed(),
            Path("market-timeseries"),
            service._managed_relative(retained_root / "market-timeseries"),
        )
        service.atomic_exchange = FailingExchange()  # type: ignore[assignment]
        service._rollback_retained_promotion(
            market_v4_cutover.RetainedPromotionContext(preparation, journal),
            processes_joined=True,
        )
        final = journal.read_validated()[-1].identities

    assert final.rollback_mode == "backup_restore"
    assert final.quarantine_current is not None
    assert final.quarantine_current["directory"] == final.active_before_directory
    assert service._payload_manifest_entries(final.active_current["payload"]) == (
        service._payload_manifest_entries(final.active_before_payload)
    )


def test_promotion_recovery_validated_terminates_without_fake_exchange(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, _retained_root, config = _retained_promotion_source(data_root)
    report_id = "market-v4-active-20260716"
    monkeypatch.setattr(
        service,
        "_detach_retained_artifacts",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            CutoverSafetyError("stop after validated")
        ),
    )
    with service._retained_promotion_eligibility_scope(
        report_id=report_id,
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
        config=config,
    ) as eligibility:
        journal = PromotionJournal(service._managed(), report_id, now=service.now)
        with pytest.raises(CutoverSafetyError, match="stop after validated"):
            service._prepare_retained_promotion_under_leases(
                eligibility,
                backup_id="market-v3-pre-v4-20260716",
                journal=journal,
            )

    assert service._recover_retained_promotion(
        report_id,
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
    ) is None
    with market_v4_cutover.ManagedRootFd.open(data_root) as managed:
        recovered = PromotionJournal(managed, report_id, now=service.now)
        recovered.recover(recovered.recovery_attempt_id())
        states = tuple(record.state for record in recovered.read_validated())
    assert states == (PromotionState.VALIDATED, PromotionState.ROLLED_BACK)


def test_promotion_recovery_runtimes_detached_restores_exact_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    report_id = "market-v4-active-20260716"
    runtime_names = {
        path.name
        for path in (retained_root / "market-timeseries").iterdir()
        if path.name.startswith(".cutover-runtime-")
    }
    with service._retained_promotion_eligibility_scope(
        report_id=report_id,
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
        config=config,
    ) as eligibility:
        journal = PromotionJournal(service._managed(), report_id, now=service.now)
        original_append = journal.append

        def stop_before_prepared(
            state: PromotionState,
            **kwargs: object,
        ) -> market_v4_cutover.PromotionAppendResult:
            if state is PromotionState.PREPARED:
                return market_v4_cutover.PromotionAppendResult(
                    PromotionAppendStatus.NOT_COMMITTED,
                    None,
                    "attempt-prepared-not-committed",
                )
            return original_append(state, **kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr(journal, "append", stop_before_prepared)
        with pytest.raises(CutoverSafetyError, match="not committed"):
            service._prepare_retained_promotion_under_leases(
                eligibility,
                backup_id="market-v3-pre-v4-20260716",
                journal=journal,
            )

    assert service._recover_retained_promotion(
        report_id,
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
    ) is None
    assert runtime_names <= {
        path.name for path in (retained_root / "market-timeseries").iterdir()
    }


def test_promotion_committed_recovery_rejects_report_supplied_quarantine_path(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service, _retained_root, config = _retained_promotion_source(data_root)
    service.atomic_exchange = _TestAtomicExchange()
    service.runtime = FakeRuntime(apis=[FakeApi()])
    result, _states = _run_retained_promotion(service, config)
    report_path = data_root / result.report_path
    report = json.loads(report_path.read_text())
    report["quarantinePath"] = (
        "operations/market-v4-cutover/backups/market-v3-pre-v4-20260716/payload"
    )
    report_path.write_text(json.dumps(report))
    marker = data_root / report["sourceConsumed"]["markerPath"]
    marker_payload = json.loads(marker.read_text())
    marker_payload["promotionReportSha256"] = service._sha256(report_path)
    marker.write_text(json.dumps(marker_payload))

    with pytest.raises(CutoverSafetyError, match="quarantine is invalid"):
        service._recover_retained_promotion(
            result.report_id,
            retained_report_id="market-v4-retained-20260715-r13",
            backup_id="market-v3-pre-v4-20260716",
        )


def test_promotion_recovery_missing_success_report_after_exchange_rolls_back(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    service.atomic_exchange = _TestAtomicExchange()
    active_before = _market_identity_at_root(service, data_root)
    report_id = "market-v4-active-20260716"
    with service._retained_promotion_eligibility_scope(
        report_id=report_id,
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
        config=config,
    ) as eligibility:
        journal = PromotionJournal(service._managed(), report_id, now=service.now)
        service._prepare_retained_promotion_under_leases(
            eligibility,
            backup_id="market-v3-pre-v4-20260716",
            journal=journal,
        )
        base = journal.read_validated()[-1].identities
        service.atomic_exchange.exchange(
            service._managed(),
            Path("market-timeseries"),
            service._managed_relative(retained_root / "market-timeseries"),
        )
        exchanged = service._promotion_identities(
            base,
            active_current=service._market_location_identity(
                service._active_lease_fd_root()
            ),
            retained_current=service._market_location_identity(
                service._retained_lease_fd_root()
            ),
            quarantine_current=None,
            holding_current=base.holding_current,
        )
        service._append_preparation_state(
            journal, PromotionState.EXCHANGED, exchanged
        )

    assert not (
        data_root
        / "operations/market-v4-cutover/reports/"
        f"{report_id}/report.json"
    ).exists()
    assert service._recover_retained_promotion(
        report_id,
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
    ) is None
    assert _market_identity_at_root(service, data_root) == active_before


@pytest.mark.parametrize("rollback_mode", ["atomic_exchange", "backup_restore"])
def test_promotion_recovery_resumes_after_exchanged_back_without_duplicate_append(
    tmp_path: Path,
    rollback_mode: str,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    service.atomic_exchange = _TestAtomicExchange()
    report_id = "market-v4-active-20260716"

    class FailingExchange:
        def exchange(self, *_args: object) -> None:
            raise OSError("exchange unavailable")

    def crash_after_exchanged_back(stage: str) -> None:
        if stage == "exchanged_back_journaled":
            raise RuntimeError("crash after exchanged-back")

    with service._retained_promotion_eligibility_scope(
        report_id=report_id,
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
        config=config,
    ) as eligibility:
        journal = PromotionJournal(service._managed(), report_id, now=service.now)
        preparation = service._prepare_retained_promotion_under_leases(
            eligibility,
            backup_id="market-v3-pre-v4-20260716",
            journal=journal,
        )
        service.atomic_exchange.exchange(
            service._managed(),
            Path("market-timeseries"),
            service._managed_relative(retained_root / "market-timeseries"),
        )
        if rollback_mode == "backup_restore":
            service.atomic_exchange = FailingExchange()  # type: ignore[assignment]
        service._promotion_boundary_hook = crash_after_exchanged_back
        with pytest.raises(RuntimeError, match="crash after exchanged-back"):
            service._rollback_retained_promotion(
                market_v4_cutover.RetainedPromotionContext(preparation, journal),
                processes_joined=True,
            )

    service._promotion_boundary_hook = lambda _stage: None
    assert service._recover_retained_promotion(
        report_id,
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
    ) is None
    with market_v4_cutover.ManagedRootFd.open(data_root) as managed:
        recovered = PromotionJournal(managed, report_id, now=service.now)
        recovered.recover(recovered.recovery_attempt_id())
        records = recovered.read_validated()
    assert sum(
        record.state is PromotionState.EXCHANGED_BACK for record in records
    ) == 1
    assert records[-1].state is PromotionState.ROLLED_BACK
    assert records[-1].identities.rollback_mode == rollback_mode


@pytest.mark.parametrize(
    "crash_boundary",
    ["first_artifact", "all_artifacts"],
)
def test_promotion_recovery_reconciles_split_partial_artifact_restoration(
    tmp_path: Path,
    crash_boundary: str,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    service.atomic_exchange = _TestAtomicExchange()
    report_id = "market-v4-active-20260716"
    preparation: market_v4_cutover.RetainedPromotionPreparation
    move_count = 0

    def crash_during_restore(stage: str) -> None:
        nonlocal move_count
        if stage.startswith("rollback_artifact_moved:"):
            move_count += 1
            if crash_boundary == "first_artifact" and move_count == 1:
                raise RuntimeError("crash after first artifact")
        if stage == "rollback_artifacts_reconciled" and crash_boundary == "all_artifacts":
            raise RuntimeError("crash after all artifacts")

    with service._retained_promotion_eligibility_scope(
        report_id=report_id,
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
        config=config,
    ) as eligibility:
        journal = PromotionJournal(service._managed(), report_id, now=service.now)
        preparation = service._prepare_retained_promotion_under_leases(
            eligibility,
            backup_id="market-v3-pre-v4-20260716",
            journal=journal,
        )
        service.atomic_exchange.exchange(
            service._managed(),
            Path("market-timeseries"),
            service._managed_relative(retained_root / "market-timeseries"),
        )
        service._promotion_boundary_hook = crash_during_restore
        with pytest.raises(RuntimeError, match="crash after"):
            service._rollback_retained_promotion(
                market_v4_cutover.RetainedPromotionContext(preparation, journal),
                processes_joined=True,
            )

    if crash_boundary == "first_artifact":
        # Prove a second restart may stop at another exact move and remains resumable.
        def crash_once_more(stage: str) -> None:
            if stage == "rollback_artifacts_reconciled":
                raise RuntimeError("second recovery crash")

        service._promotion_boundary_hook = crash_once_more
        with pytest.raises(RuntimeError, match="second recovery crash"):
            service._recover_retained_promotion(
                report_id,
                retained_report_id="market-v4-retained-20260715-r13",
                backup_id="market-v3-pre-v4-20260716",
            )

    service._promotion_boundary_hook = lambda _stage: None
    assert service._recover_retained_promotion(
        report_id,
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
    ) is None
    retained_market_fd = os.open(
        retained_root / "market-timeseries", os.O_RDONLY | os.O_DIRECTORY
    )
    try:
        for artifact in preparation.detached_artifacts:
            assert service._held_artifact_evidence(
                retained_market_fd, artifact.name
            ) == artifact
    finally:
        os.close(retained_market_fd)


def _filesystem_identity_snapshot(root: Path) -> tuple[tuple[object, ...], ...]:
    snapshot: list[tuple[object, ...]] = []
    for path in sorted((root, *root.rglob("*"))):
        relative = path.relative_to(root).as_posix() or "."
        path_stat = path.lstat()
        payload: object = None
        if stat.S_ISREG(path_stat.st_mode):
            payload = hashlib.sha256(path.read_bytes()).hexdigest()
        elif stat.S_ISLNK(path_stat.st_mode):
            payload = os.readlink(path)
        snapshot.append(
            (
                relative,
                path_stat.st_mode,
                path_stat.st_dev,
                path_stat.st_ino,
                path_stat.st_nlink,
                path_stat.st_size,
                path_stat.st_mtime_ns,
                payload,
            )
        )
    return tuple(snapshot)


@pytest.mark.parametrize("mutation", ["missing", "duplicate", "drift", "extra"])
def test_restore_held_artifacts_preflight_failure_has_zero_mutation(
    tmp_path: Path,
    mutation: str,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_promotion_source(data_root)
    report_id = "market-v4-active-20260716"
    (retained_root / "market-timeseries/market.duckdb.wal").touch()

    with service._retained_promotion_eligibility_scope(
        report_id=report_id,
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
        config=config,
    ) as eligibility:
        journal = PromotionJournal(service._managed(), report_id, now=service.now)
        preparation = service._prepare_retained_promotion_under_leases(
            eligibility,
            backup_id="market-v3-pre-v4-20260716",
            journal=journal,
        )
        later_artifact = preparation.detached_artifacts[-1]
        assert later_artifact.kind == "regular"
        staged_later = preparation.holding_root / later_artifact.name
        retained_later = retained_root / "market-timeseries" / later_artifact.name
        if mutation == "missing":
            staged_later.rename(preparation.holding_root.parent / "missing-later")
        elif mutation == "duplicate":
            os.link(staged_later, retained_later)
        elif mutation == "drift":
            staged_later.write_bytes(b"drift")
        else:
            (preparation.holding_root / "unexpected-extra").write_bytes(b"extra")

        before = _filesystem_identity_snapshot(data_root)
        with pytest.raises(CutoverSafetyError):
            service._restore_held_promotion_artifacts(preparation)
        assert _filesystem_identity_snapshot(data_root) == before


def test_promote_retained_report_contract_is_exact_and_strict(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, _retained_root, config = _retained_promotion_source(data_root)
    service.atomic_exchange = _TestAtomicExchange()
    service.runtime = FakeRuntime(apis=[FakeApi()])
    captured_expectations: list[market_v4_cutover.RetainedPromotionReportExpectation] = []
    original_builder = service._build_retained_promotion_report

    def capture_expectation(
        expectation: market_v4_cutover.RetainedPromotionReportExpectation,
    ) -> dict[str, object]:
        captured_expectations.append(expectation)
        return original_builder(expectation)

    monkeypatch.setattr(
        service,
        "_build_retained_promotion_report",
        capture_expectation,
    )

    result, _states = _run_retained_promotion(service, config)
    report = json.loads((data_root / result.report_path).read_text())
    assert len(captured_expectations) >= 1
    expectation = captured_expectations[-1]

    assert report["activationMode"] == "retained_atomic_exchange"
    assert report["reportId"] == "market-v4-active-20260716"
    assert report["codeVersion"] == "deadbeef"
    assert report["retainedReport"]["reportId"] == "market-v4-retained-20260715-r13"
    assert report["sourceReport"]["reportId"] == "market-v4-rehearsal-20260715-r10"
    for evidence in (report["retainedReport"], report["sourceReport"]):
        assert set(evidence) == {"reportId", "codeVersion", "reportSha256"}
        assert len(evidence["reportSha256"]) == 64
    assert set(report["payloadIdentities"]) == {
        "activeBefore",
        "backup",
        "retainedSource",
        "activated",
        "activeAfter",
    }
    backup_payload = report["payloadIdentities"]["backup"]
    active_before_payload = report["payloadIdentities"]["activeBefore"]
    assert service._payload_manifest_entries(backup_payload) == (
        service._payload_manifest_entries(active_before_payload)
    )
    assert backup_payload != active_before_payload
    assert report["backupEvidence"]["contentEquivalentToActiveBefore"] is True
    assert report["backupEvidence"]["physicalIdentityDistinct"] is True
    assert (
        report["payloadIdentities"]["retainedSource"]
        == report["payloadIdentities"]["activated"]
        == report["payloadIdentities"]["activeAfter"]
    )
    assert report["filesystemEvidence"]["sameDevice"] is True
    assert report["filesystemEvidence"]["atomicExchange"] is True
    assert report["journal"] == {
        "operationId": "market-v4-active-20260716",
        "finalState": "committed",
    }
    assert report["backupId"] == "market-v3-pre-v4-20260716"
    assert report["quarantinePath"].endswith("/quarantine/market-v4-active-20260716")
    assert report["runtimeCleanup"]["activeRuntimeRemoved"] is True
    assert report["runtimeCleanup"]["removedArtifacts"] == []
    assert report["runtimeCleanup"]["cleanupDisposition"] == "pending_post_commit"
    assert report["runtimeCleanup"]["cleanupStagingPath"].endswith(
        "/cleanup-staging/market-v4-active-20260716"
    )
    assert report["runtimeCleanup"]["cleanupResultPath"].endswith(
        "/cleanup-results/market-v4-active-20260716.json"
    )
    assert (data_root / report["runtimeCleanup"]["cleanupResultPath"]).is_file()
    assert report["runtimeCleanup"]["holdingDirectory"] == (
        expectation.runtime_cleanup["holdingDirectory"]
    )
    assert report["noSync"] is True
    assert report["noJQuants"] is True
    assert report["serverProcessJoined"] is True
    assert report["workerProcessJoined"] is True
    assert report["sourceConsumed"]["retainedReportId"] == (
        "market-v4-retained-20260715-r13"
    )
    marker = data_root / report["sourceConsumed"]["markerPath"]
    assert marker.is_file()
    assert report["rollbackInstructions"]
    assert not service._retained_promotion_report_contract_valid(report)
    assert service._retained_promotion_report_contract_valid(
        report,
        expectation=expectation,
    )

    missing = json.loads(json.dumps(report))
    missing.pop("noSync")
    extra = json.loads(json.dumps(report))
    extra["compatibility"] = True
    mismatch = json.loads(json.dumps(report))
    mismatch["activationMode"] = "copy"
    extra_api = json.loads(json.dumps(report))
    extra_api["apiChecks"].append("/api/db/sync")
    nested_extra = json.loads(json.dumps(report))
    nested_extra["retainedReport"]["compatibility"] = True
    directory_mismatch = json.loads(json.dumps(report))
    directory_mismatch["filesystemEvidence"]["activeAfterDirectory"]["inode"] += 1
    missing_semantic_check = json.loads(json.dumps(report))
    missing_semantic_check["semanticSmoke"]["checks"].pop()
    empty_lineage = json.loads(json.dumps(report))
    empty_lineage["semanticSmoke"]["adjustedMetrics"] = {}
    retained_sha_mismatch = json.loads(json.dumps(report))
    retained_sha_mismatch["retainedReport"]["reportSha256"] = "0" * 64
    source_code_mismatch = json.loads(json.dumps(report))
    source_code_mismatch["sourceReport"]["codeVersion"] = "invented-code-version"
    backup_inode_mismatch = json.loads(json.dumps(report))
    backup_inode_mismatch["payloadIdentities"]["backup"]["marketDuckdb"][
        "inode"
    ] += 1
    backup_evidence_mismatch = json.loads(json.dumps(report))
    backup_evidence_mismatch["backupEvidence"]["physicalIdentityDistinct"] = False
    artifact_inode_mismatch = json.loads(json.dumps(report))
    artifact_inode_mismatch["runtimeCleanup"]["detachedArtifacts"][0]["identity"][
        "inode"
    ] += 1
    artifact_name_mismatch = json.loads(json.dumps(report))
    artifact_name_mismatch["runtimeCleanup"]["detachedArtifacts"][0]["name"] = (
        "invented-runtime"
    )
    quarantine_mismatch = json.loads(json.dumps(report))
    quarantine_mismatch["quarantinePath"] += "-other"
    journal_mismatch = json.loads(json.dumps(report))
    journal_mismatch["journal"]["finalState"] = "report_persisted"
    for mutation_index, candidate in enumerate((
        missing,
        extra,
        mismatch,
        extra_api,
        nested_extra,
        directory_mismatch,
        missing_semantic_check,
        empty_lineage,
        retained_sha_mismatch,
        source_code_mismatch,
        backup_inode_mismatch,
        backup_evidence_mismatch,
        artifact_inode_mismatch,
        artifact_name_mismatch,
        quarantine_mismatch,
        journal_mismatch,
    )):
        assert not service._retained_promotion_report_contract_valid(
            candidate,
            expectation=expectation,
        ), mutation_index


def test_promote_retained_rejects_same_byte_backup_inode_swap_during_report_publish(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service, _retained_root, config = _retained_promotion_source(data_root)
    service.atomic_exchange = _TestAtomicExchange()
    service.runtime = FakeRuntime(apis=[FakeApi()])
    backup_database = data_root / (
        "operations/market-v4-cutover/backups/market-v3-pre-v4-20260716/"
        "payload/market.duckdb"
    )
    original_identity: tuple[int, int] | None = None

    def replace_with_same_bytes(stage: str) -> None:
        nonlocal original_identity
        if stage != "after_temp_fsync" or original_identity is not None:
            return
        original_stat = backup_database.stat()
        original_identity = (original_stat.st_dev, original_stat.st_ino)
        replacement = backup_database.with_name("market.duckdb.replacement")
        payload_directory = backup_database.parent
        directory_mode = payload_directory.stat().st_mode
        payload_directory.chmod(0o700)
        try:
            replacement.write_bytes(backup_database.read_bytes())
            replacement.chmod(original_stat.st_mode)
            replacement.replace(backup_database)
        finally:
            payload_directory.chmod(directory_mode)

    service._report_publish_hook = replace_with_same_bytes

    with pytest.raises(CutoverSafetyError, match="backup physical identity changed"):
        _run_retained_promotion(service, config)

    assert original_identity is not None
    assert (backup_database.stat().st_dev, backup_database.stat().st_ino) != (
        original_identity
    )
    assert not (
        data_root
        / "operations/market-v4-cutover/reports/market-v4-active-20260716/report.json"
    ).exists()


@pytest.mark.parametrize("mutation", ["replacement", "missing", "extra"])
def test_promote_retained_rejects_held_artifact_identity_drift_before_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    data_root = _market_root(tmp_path)
    service, _retained_root, config = _retained_promotion_source(data_root)
    service.atomic_exchange = _TestAtomicExchange()
    service.runtime = FakeRuntime(apis=[FakeApi()])
    original_cleanup = service._complete_committed_promotion_cleanup
    observed: dict[str, Path] = {}

    def mutate_then_cleanup(
        preparation: market_v4_cutover.RetainedPromotionPreparation,
        *,
        operation_id: str,
        report_sha256: str,
    ) -> None:
        root = service._cleanup_staging_root(operation_id)
        names = tuple(artifact.name for artifact in preparation.detached_artifacts)
        target_name = preparation.detached_runtime_names[0]
        target = root / target_name
        if mutation == "replacement":
            original = root / f"{target_name}.original"
            target.rename(original)
            target.mkdir()
            (target / "replacement").write_text("foreign")
            observed["original"] = original
            observed["replacement"] = target
        elif mutation == "missing":
            moved = root.parent / f"{target_name}.moved"
            target.rename(moved)
            observed["moved"] = moved
        else:
            foreign = root / "foreign"
            foreign.write_text("foreign")
            observed["foreign"] = foreign
        for name in names:
            if name != target_name or mutation == "extra":
                assert (root / name).exists()
        original_cleanup(
            preparation,
            operation_id=operation_id,
            report_sha256=report_sha256,
        )

    monkeypatch.setattr(
        service, "_complete_committed_promotion_cleanup", mutate_then_cleanup
    )

    with pytest.raises(CutoverSafetyError, match="cleanup incomplete"):
        _run_retained_promotion(service, config)

    assert observed
    assert all(path.exists() for path in observed.values())


@pytest.fixture
def guard_lease_fd(tmp_path: Path):
    with market_v4_cutover.MarketOperationLease.acquire(
        tmp_path,
        exclusive=False,
    ) as lease:
        yield lease.fd


def test_preflight_fails_closed_when_server_or_jobs_are_active(tmp_path: Path) -> None:
    data_root = _market_root(tmp_path)

    with pytest.raises(CutoverSafetyError, match="FastAPI process must be stopped"):
        _service(data_root, runtime=FakeRuntime(stopped=False)).preflight()
    with pytest.raises(CutoverSafetyError, match="active jobs: sync"):
        _service(data_root, runtime=FakeRuntime(active_jobs=("sync",))).preflight()


def test_preflight_requires_exclusive_checkpoint_and_empty_wal(tmp_path: Path) -> None:
    data_root = _market_root(tmp_path)
    with pytest.raises(CutoverSafetyError, match="exclusive writable"):
        _service(
            data_root,
            duckdb=FakeDuckDb(checkpoint_error=RuntimeError("locked")),
        ).preflight()

    with pytest.raises(CutoverSafetyError, match="WAL"):
        _service(data_root, duckdb=FakeDuckDb(leave_wal=True)).preflight()


def test_preflight_unjoined_worker_transfers_active_lease(tmp_path: Path) -> None:
    data_root = _market_root(tmp_path)

    class GuardHoldingDuckDb(FakeDuckDb):
        retained_guard_fd = -1

        def checkpoint_exclusive(
            self,
            directory_fd: int,
            filename: str,
            *,
            guard_lease_fd: int,
        ) -> MarketSourceMetadata:
            del directory_fd, filename
            self.retained_guard_fd = os.dup(guard_lease_fd)
            raise market_v4_cutover.WorkerShutdownError(
                "injected unjoined checkpoint worker",
                process_joined=False,
            )

    duckdb = GuardHoldingDuckDb()
    service = _service(data_root, duckdb=duckdb)

    with pytest.raises(CutoverSafetyError):
        service.preflight()

    try:
        with pytest.raises(CutoverSafetyError, match="operation lease"):
            market_v4_cutover.MarketOperationLease.acquire(
                data_root,
                exclusive=False,
            )
    finally:
        os.close(duckdb.retained_guard_fd)

    with market_v4_cutover.MarketOperationLease.acquire(data_root, exclusive=True):
        pass


def test_preflight_rejects_market_root_symlink_before_duckdb_mutation(tmp_path: Path) -> None:
    data_root = _market_root(tmp_path)
    external = tmp_path / "external-market"
    shutil.move(data_root / "market-timeseries", external)
    (data_root / "market-timeseries").symlink_to(external, target_is_directory=True)
    duckdb = FakeDuckDb()

    with pytest.raises(CutoverSafetyError, match="symlink"):
        _service(data_root, duckdb=duckdb).preflight()

    assert duckdb.checkpoint_calls == 0
    assert (external / "market.duckdb").read_bytes() == b"duckdb-v3"


def test_preflight_rejects_selected_data_root_symlink_before_external_mutation(
    tmp_path: Path,
) -> None:
    external = _market_root(tmp_path / "external")
    selected = tmp_path / "selected-root"
    selected.symlink_to(external, target_is_directory=True)
    duckdb = FakeDuckDb()

    with pytest.raises(CutoverSafetyError, match="symlink"):
        _service(selected, duckdb=duckdb).preflight()

    assert duckdb.checkpoint_calls == 0
    assert not (external / ".market-timeseries.operation.lock").exists()


def test_preflight_rejects_symlink_in_selected_root_ancestor(tmp_path: Path) -> None:
    external_parent = tmp_path / "external-parent"
    data_root = _market_root(external_parent)
    alias = tmp_path / "alias"
    alias.symlink_to(external_parent, target_is_directory=True)
    selected = alias / data_root.name
    duckdb = FakeDuckDb()

    with pytest.raises(CutoverSafetyError, match="symlink"):
        _service(selected, duckdb=duckdb).preflight()

    assert duckdb.checkpoint_calls == 0
    assert not (data_root / ".market-timeseries.operation.lock").exists()


def test_preflight_requires_capacity_for_backup_rebuild_and_staging(tmp_path: Path) -> None:
    data_root = _market_root(tmp_path)
    with pytest.raises(CutoverSafetyError, match="Insufficient free space"):
        _service(data_root, free_bytes=1).preflight()


def test_backup_is_recursive_checksummed_and_immutable(tmp_path: Path) -> None:
    data_root = _market_root(tmp_path)
    service = _service(data_root)

    result = service.backup("backup-001")

    backup_dir = data_root / "operations/market-v4-cutover/backups/backup-001"
    manifest = json.loads((backup_dir / "manifest.json").read_text())
    assert result.backup_id == "backup-001"
    assert [entry["path"] for entry in manifest["files"]] == [
        "market.duckdb",
        "parquet/stock_data/part.parquet",
    ]
    assert manifest["source"] == {
        "schemaVersion": 3,
        "stockPriceAdjustmentMode": "local_projection_v1",
    }
    assert all(len(entry["sha256"]) == 64 for entry in manifest["files"])
    assert not (backup_dir.stat().st_mode & 0o200)
    assert service.verify_backup("backup-001").backup_id == "backup-001"


def test_backup_manifest_uses_operation_start_code_identity(tmp_path: Path) -> None:
    data_root = _market_root(tmp_path)
    code_version, calls = _changing_code_version("deadbeef", "deadbeef-dirty")
    service = _service(data_root)
    service.code_version = code_version

    service.backup("captured-identity")

    manifest = json.loads(
        (
            data_root
            / "operations/market-v4-cutover/backups/captured-identity/manifest.json"
        ).read_text()
    )
    assert manifest["codeVersion"] == "deadbeef"
    assert calls == ["deadbeef"]


def test_backup_fails_for_existing_destination_symlink_and_checksum_mismatch(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service = _service(data_root)
    backup_parent = data_root / "operations/market-v4-cutover/backups"
    (backup_parent / "existing").mkdir(parents=True)
    with pytest.raises(CutoverSafetyError, match="already exists"):
        service.backup("existing")

    symlink = data_root / "market-timeseries/parquet/link"
    symlink.symlink_to(data_root / "market-timeseries/market.duckdb")
    with pytest.raises(CutoverSafetyError, match="symlink"):
        service.backup("with-link")
    symlink.unlink()

    service.backup("corrupt")
    copied_db = backup_parent / "corrupt/payload/market.duckdb"
    copied_db.chmod(0o600)
    copied_db.write_bytes(b"duckdb-X3")
    with pytest.raises(CutoverSafetyError, match="checksum mismatch"):
        service.verify_backup("corrupt")


@pytest.mark.parametrize("redirected_component", ["operations", "backups"])
def test_backup_rejects_redirected_operation_component_without_external_writes(
    tmp_path: Path,
    redirected_component: str,
) -> None:
    data_root = _market_root(tmp_path)
    external = tmp_path / f"external-{redirected_component}"
    external.mkdir()
    if redirected_component == "operations":
        (data_root / "operations").symlink_to(external, target_is_directory=True)
    else:
        cutover_root = data_root / "operations/market-v4-cutover"
        cutover_root.mkdir(parents=True)
        (cutover_root / "backups").symlink_to(external, target_is_directory=True)

    with pytest.raises(CutoverSafetyError, match="symlink"):
        _service(data_root).backup("must-not-escape")

    assert list(external.iterdir()) == []


def test_restore_requires_explicit_verified_backup_and_preserves_it(tmp_path: Path) -> None:
    data_root = _market_root(tmp_path)
    service = _service(data_root)
    with pytest.raises(CutoverSafetyError, match="explicit backup ID"):
        service.restore(None)

    service.backup("before")
    market = data_root / "market-timeseries"
    shutil.rmtree(market)
    market.mkdir()
    (market / "market.duckdb").write_bytes(b"failed-v4")

    result = service.restore("before")

    assert (market / "market.duckdb").read_bytes() == b"duckdb-v3"
    assert market.stat().st_mode & 0o200
    assert (market / "market.duckdb").stat().st_mode & 0o200
    assert (data_root / "operations/market-v4-cutover/backups/before").exists()
    assert result.quarantine_path is not None
    assert (data_root / result.quarantine_path / "market.duckdb").read_bytes() == b"failed-v4"


def test_smoke_requires_market_v4_exact_lineage_and_semantic_api_parity(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service = _service(
        data_root,
        duckdb=FakeDuckDb(
            MarketSourceMetadata(4, "local_projection_v2_event_time")
        ),
    )
    config = SmokeConfig(symbol="7203", strategy="production/smoke", dataset_preset="primeMarket")

    api = FakeApi()
    result = service.smoke(api, config, operation_id="smoke-001")

    assert result.schema_version == 4
    assert result.adjustment_mode == "local_projection_v2_event_time"
    assert result.checks == (
        "market_metadata",
        "adjusted_metrics_lineage",
        "fundamentals_parity",
        "screening",
        "fundamental_ranking",
        "dataset_create_info_open",
    )
    assert ("GET", "/api/analytics/screening/jobs/screen-1", None) in api.calls

    with pytest.raises(CutoverSafetyError, match="adjusted-metric lineage"):
        service.smoke(FakeApi(invalid_lineage=True), config, operation_id="smoke-002")
    with pytest.raises(CutoverSafetyError, match="Fundamentals GET/POST parity"):
        service.smoke(FakeApi(parity=False), config, operation_id="smoke-003")


def test_smoke_reads_adjustment_mode_directly_from_duckdb(tmp_path: Path) -> None:
    data_root = _market_root(tmp_path)
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v1")),
    )
    with pytest.raises(CutoverSafetyError, match="adjustment mode"):
        service.smoke(
            FakeApi(),
            SmokeConfig("7203", "production/smoke", "primeMarket"),
            operation_id="smoke-mode",
        )


def test_smoke_uses_operation_scoped_create_only_dataset(tmp_path: Path) -> None:
    data_root = _market_root(tmp_path)
    api = FakeApi()
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
    )

    service.smoke(
        api,
        SmokeConfig("7203", "production/smoke", "primeMarket"),
        operation_id="report-ABC",
    )

    create = next(call for call in api.calls if call[:2] == ("POST", "/api/dataset"))
    assert create[2] == {
        "name": "cutover-smoke-report-ABC",
        "preset": "primeMarket",
        "overwrite": False,
    }
    assert all(call[1] != "/api/dataset/cutover-smoke/info" for call in api.calls)


def test_standalone_smoke_worker_unjoined_transfers_shared_guard_lease(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)

    class GuardHoldingDuckDb(FakeDuckDb):
        retained_guard_fd = -1

        def inspect(
            self,
            directory_fd: int,
            filename: str,
            *,
            guard_lease_fd: int,
        ) -> MarketSourceMetadata:
            del directory_fd, filename
            self.retained_guard_fd = os.dup(guard_lease_fd)
            raise market_v4_cutover.WorkerShutdownError(
                "injected unjoined inspect worker",
                process_joined=False,
            )

    duckdb = GuardHoldingDuckDb(
        MarketSourceMetadata(4, "local_projection_v2_event_time")
    )
    service = _service(data_root, duckdb=duckdb)

    with pytest.raises(market_v4_cutover.WorkerShutdownError):
        service.smoke(
            FakeApi(),
            SmokeConfig("7203", "production/smoke", "primeMarket"),
            operation_id="standalone-worker-transfer",
        )

    try:
        with pytest.raises(CutoverSafetyError, match="operation lease"):
            market_v4_cutover.MarketOperationLease.acquire(
                data_root,
                exclusive=True,
            )
    finally:
        os.close(duckdb.retained_guard_fd)

    with market_v4_cutover.MarketOperationLease.acquire(data_root, exclusive=True):
        pass


def test_rehearsal_uses_isolated_paths_and_credential_safe_report(tmp_path: Path) -> None:
    data_root = _market_root(tmp_path)
    rehearsal_api = FakeApi()
    runtime = FakeRuntime(apis=[rehearsal_api])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
        runtime=runtime,
    )

    result = service.rehearse(
        "rehearsal-001",
        SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={
            "JQUANTS_API_KEY": "super-secret",
            "TRADING25_DATA_DIR": "/active/leak",
            "MARKET_TIMESERIES_DIR": "/active/market",
            "DATASET_BASE_PATH": "/active/datasets",
            "PORTFOLIO_DB_PATH": "/active/portfolio.db",
            "TRADING25_STRATEGIES_DIR": "/active/strategies",
            "TRADING25_BACKTEST_DIR": "/active/backtest",
            "TRADING25_DEFAULT_CONFIG_PATH": "/active/default.yaml",
        },
    )

    report_path = data_root / result.report_path
    report_text = report_path.read_text()
    report = json.loads(report_text)
    assert report["status"] == "passed"
    assert report["reportId"] == "rehearsal-001"
    assert report["rehearsalMode"] == "full_rebuild"
    assert report["serverProcessJoined"] is True
    assert report["workerProcessJoined"] is True
    assert report["targetRootFingerprint"] == service.root_fingerprint(data_root)
    assert "super-secret" not in report_text
    assert str(data_root) not in report_text
    assert runtime.stop_calls == 1
    environment = runtime.environments[0]
    runtime_name = ".cutover-runtime-rehearsal-001"
    assert environment["TRADING25_DATA_DIR"] == runtime_name
    assert environment["MARKET_TIMESERIES_DIR"] == "."
    assert environment["MARKET_DB_PATH"] == "market.duckdb"
    assert environment["DATASET_BASE_PATH"] == f"{runtime_name}/datasets"
    assert environment["PORTFOLIO_DB_PATH"] == f"{runtime_name}/portfolio.db"
    assert environment["TRADING25_STRATEGIES_DIR"] == f"{runtime_name}/strategies"
    assert environment["TRADING25_BACKTEST_DIR"] == f"{runtime_name}/backtest"
    assert (
        environment["TRADING25_DEFAULT_CONFIG_PATH"]
        == f"{runtime_name}/config/default.yaml"
    )
    assert environment["JQUANTS_API_KEY"] == "super-secret"
    assert "TRADING25_RUNTIME_CAPABILITY" not in environment
    api_calls = runtime.environments and report["apiChecks"]
    assert "/api/db/adjusted-metrics/materialize" not in api_calls
    sync_payload = next(
        payload
        for method, path, payload in rehearsal_api.calls
        if method == "POST" and path == "/api/db/sync"
    )
    assert sync_payload is not None
    assert sync_payload["resetBeforeSync"] is False


def test_operation_report_emits_supplied_rehearsal_provenance(tmp_path: Path) -> None:
    data_root = _market_root(tmp_path)
    service = _service(data_root)
    market_identity = {"device": 1, "inode": 2}

    report = service._operation_report(
        report_id="retained-rehearsal",
        phase="rehearsal",
        status="passed",
        duration_seconds=1.0,
        api_checks=(),
        server_log="rehearsals/retained-rehearsal/server.log",
        evidence=None,
        phases=(),
        config=SmokeConfig("7203", "production/smoke", "primeMarket"),
        code_version="deadbeef",
        rehearsal_mode="retained_market_smoke",
        source_rehearsal_report_id="full-rehearsal",
        source_rehearsal_code_version="deadbeef",
        source_retained_root_fingerprint="root-fingerprint",
        source_market_identity_before=market_identity,
        source_market_identity_after=market_identity,
    )

    assert report["sourceRehearsalReportId"] == "full-rehearsal"
    assert report["sourceRehearsalCodeVersion"] == "deadbeef"
    assert report["sourceRetainedRootFingerprint"] == "root-fingerprint"
    assert report["sourceMarketIdentityBefore"] == market_identity
    assert report["sourceMarketIdentityAfter"] == market_identity


@pytest.mark.parametrize(
    "mutation",
    [
        "missing_source_report",
        "same_report_id",
        "wrong_smoke_config",
        "active_fingerprint_drift",
        "source_status_cleanup_deferred",
        "source_server_unjoined",
        "source_worker_unjoined",
        "source_root_symlink",
        "configuration_drift",
        "schema_v3",
        "wrong_adjustment_mode",
    ],
)
def test_rehearse_retained_rejects_ineligible_source(
    tmp_path: Path,
    mutation: str,
) -> None:
    data_root = _market_root(tmp_path)
    source_id = "market-v4-rehearsal-20260715-r10"
    service, retained_root, config = _retained_source(
        data_root,
        source_report_id=source_id,
    )
    report_id = "retained-r12"
    source_report_path = (
        data_root
        / "operations/market-v4-cutover/reports"
        / source_id
        / "report.json"
    )
    source_report = json.loads(source_report_path.read_text())
    if mutation == "missing_source_report":
        source_report_path.unlink()
    elif mutation == "same_report_id":
        report_id = source_id
    elif mutation == "wrong_smoke_config":
        source_report["smokeConfig"] = {**source_report["smokeConfig"], "symbol": "9984"}
        source_report_path.write_text(json.dumps(source_report))
    elif mutation == "active_fingerprint_drift":
        source_report["targetRootFingerprint"] = "0" * 64
        source_report_path.write_text(json.dumps(source_report))
    elif mutation == "source_status_cleanup_deferred":
        source_report["status"] = "stop_failed_cleanup_deferred"
        source_report_path.write_text(json.dumps(source_report))
    elif mutation == "source_server_unjoined":
        source_report["serverProcessJoined"] = False
        source_report_path.write_text(json.dumps(source_report))
    elif mutation == "source_worker_unjoined":
        source_report["workerProcessJoined"] = False
        source_report_path.write_text(json.dumps(source_report))
    elif mutation == "source_root_symlink":
        external = tmp_path / "external-retained"
        shutil.move(retained_root, external)
        retained_root.symlink_to(external, target_is_directory=True)
    elif mutation == "configuration_drift":
        (retained_root / "config/default.yaml").write_text("drift: true\n")
    elif mutation == "schema_v3":
        service.duckdb = FakeDuckDb(
            MarketSourceMetadata(3, "local_projection_v2_event_time")
        )
    elif mutation == "wrong_adjustment_mode":
        service.duckdb = FakeDuckDb(MarketSourceMetadata(4, "local_projection_v1"))

    runtime = FakeRuntime(apis=[FakeApi()])
    service.runtime = runtime

    with pytest.raises(CutoverSafetyError):
        service.rehearse_retained(
            report_id,
            source_rehearsal_report_id=source_id,
            config=config,
            inherited_environment={},
        )

    assert runtime.start_calls == 0
    if mutation != "same_report_id":
        assert not (
            data_root / "operations/market-v4-cutover/reports" / report_id
        ).exists()


@pytest.mark.parametrize("source_status", ["passed", "failed"])
def test_rehearse_retained_smokes_current_code_without_market_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    source_status: str,
) -> None:
    data_root = _market_root(tmp_path)
    source_id = "market-v4-rehearsal-20260715-r10"
    service, retained_root, config = _retained_source(
        data_root,
        source_report_id=source_id,
        status=source_status,
    )
    api = FakeApi()
    runtime = FakeRuntime(apis=[api])
    service.runtime = runtime
    smoke_result = SmokeResult(
        schema_version=4,
        adjustment_mode="local_projection_v2_event_time",
        checks=("market_metadata", "semantic_smoke"),
        api_paths=("/api/db/stats", "/api/analytics/fundamentals/7203"),
        lineage={"readyBasisCount": 2},
    )
    monkeypatch.setattr(service, "smoke", lambda *_args, **_kwargs: smoke_result)

    result = service.rehearse_retained(
        "retained-r12",
        source_rehearsal_report_id=source_id,
        config=config,
        inherited_environment={},
    )

    report = _read_operation_report(data_root, "retained-r12")
    assert result.report_id == "retained-r12"
    assert runtime.start_calls == 1
    assert runtime.stop_calls == 1
    assert runtime.environments[0]["TRADING25_RUNTIME_CAPABILITY"] == (
        "retained_market_smoke"
    )
    assert all("/api/db/sync" not in path for _method, path, _payload in api.calls)
    assert all("materialize" not in path for _method, path, _payload in api.calls)
    assert report["status"] == "passed"
    assert report["codeVersion"] == "deadbeef"
    assert report["rehearsalMode"] == "retained_market_smoke"
    assert report["sourceRehearsalReportId"] == source_id
    assert report["sourceRehearsalCodeVersion"] == "cafebabe"
    assert report["sourceRetainedRootFingerprint"] == service.root_fingerprint(
        retained_root
    )
    assert report["sourceMarketIdentityBefore"] == report["sourceMarketIdentityAfter"]
    assert report["apiChecks"] == list(smoke_result.api_paths)
    assert report["schemaCoverage"] == {
        "schemaVersion": 4,
        "stockPriceAdjustmentMode": "local_projection_v2_event_time",
        "adjustedMetrics": smoke_result.lineage,
    }
    assert report["phases"][0]["name"] == "retained_market_smoke"
    assert report["serverProcessJoined"] is True
    assert report["workerProcessJoined"] is True
    runtime_root = (
        retained_root / "market-timeseries/.cutover-runtime-retained-r12"
    )
    assert (runtime_root / "config/default.yaml").is_file()
    assert (runtime_root / "strategies/production/smoke.yaml").is_file()


def test_rehearse_retained_real_smoke_traverses_semantic_paths_without_mutation(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service, _retained_root, config = _retained_source(data_root)
    api = FakeApi()
    service.runtime = FakeRuntime(apis=[api])

    result = service.rehearse_retained(
        "retained-real-smoke",
        source_rehearsal_report_id="market-v4-rehearsal-20260715-r10",
        config=config,
        inherited_environment={},
    )

    report = _read_operation_report(data_root, result.report_id)
    paths = [path for _method, path, _payload in api.calls]
    assert report["apiChecks"] == paths
    assert "/api/db/stats" in paths
    assert "/api/db/validate" in paths
    assert "/api/fundamentals/compute" in paths
    assert "/api/analytics/screening/jobs" in paths
    assert "/api/analytics/fundamental-ranking" in paths
    assert "/api/dataset" in paths
    assert any(path.endswith("/info") for path in paths)
    assert any("/sample?count=1" in path for path in paths)
    assert all("/api/db/sync" not in path for path in paths)
    assert all("materialize" not in path for path in paths)
    assert all("stocks/refresh" not in path for path in paths)
    parquet_identity = report["sourceMarketIdentityBefore"]["parquetSha256"]
    parquet_file_identity = parquet_identity["stock_data/part.parquet"]
    assert isinstance(parquet_file_identity["device"], int)
    assert isinstance(parquet_file_identity["inode"], int)
    assert parquet_file_identity["size"] == len(b"retained-rows")
    assert len(parquet_file_identity["sha256"]) == 64


@pytest.mark.parametrize("existing_destination", ["report", "runtime"])
def test_rehearse_retained_rejects_destinations_before_creating_peer_artifact(
    tmp_path: Path,
    existing_destination: str,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_source(data_root)
    report_id = f"retained-existing-{existing_destination}"
    report_dir = data_root / "operations/market-v4-cutover/reports" / report_id
    runtime_dir = retained_root / "market-timeseries" / f".cutover-runtime-{report_id}"
    if existing_destination == "report":
        report_dir.mkdir(parents=True)
    else:
        runtime_dir.mkdir(parents=True)
    runtime = FakeRuntime(apis=[FakeApi()])
    service.runtime = runtime

    with pytest.raises(CutoverSafetyError):
        service.rehearse_retained(
            report_id,
            source_rehearsal_report_id="market-v4-rehearsal-20260715-r10",
            config=config,
            inherited_environment={},
        )

    assert runtime.start_calls == 0
    if existing_destination == "report":
        assert not runtime_dir.exists()
    else:
        assert not report_dir.exists()


def test_rehearse_retained_requires_ready_lineage_before_resource_creation(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_source(data_root)
    service.duckdb.inspect = lambda *_args, **_kwargs: SimpleNamespace(
        schema_version=4,
        adjustment_mode="local_projection_v2_event_time",
        adjusted_metrics_ready=False,
    )
    runtime = FakeRuntime(apis=[FakeApi()])
    service.runtime = runtime

    with pytest.raises(CutoverSafetyError):
        service.rehearse_retained(
            "retained-lineage-not-ready",
            source_rehearsal_report_id="market-v4-rehearsal-20260715-r10",
            config=config,
            inherited_environment={},
        )

    assert runtime.start_calls == 0
    assert not (
        retained_root / "market-timeseries/.cutover-runtime-retained-lineage-not-ready"
    ).exists()
    assert not (
        data_root / "operations/market-v4-cutover/reports/retained-lineage-not-ready"
    ).exists()


def test_rehearse_retained_preserves_foreign_runtime_created_during_reservation_race(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_source(data_root)
    runtime_name = ".cutover-runtime-retained-runtime-race"
    runtime_path = retained_root / "market-timeseries" / runtime_name
    original_open_dir = market_v4_cutover.ManagedRootFd.open_dir
    raced = False

    def create_foreign_runtime_before_exclusive_open(
        managed: market_v4_cutover.ManagedRootFd,
        relative: Path,
        *,
        create: bool = False,
        exclusive_leaf: bool = False,
    ) -> int:
        nonlocal raced
        if relative == Path("market-timeseries") / runtime_name and not raced:
            raced = True
            runtime_path.mkdir()
            (runtime_path / "foreign-owner").write_text("keep")
        return original_open_dir(
            managed,
            relative,
            create=create,
            exclusive_leaf=exclusive_leaf,
        )

    monkeypatch.setattr(
        market_v4_cutover.ManagedRootFd,
        "open_dir",
        create_foreign_runtime_before_exclusive_open,
    )

    with pytest.raises(CutoverSafetyError, match="rehearsal failed"):
        service.rehearse_retained(
            "retained-runtime-race",
            source_rehearsal_report_id="market-v4-rehearsal-20260715-r10",
            config=config,
            inherited_environment={},
        )

    assert raced is True
    assert (runtime_path / "foreign-owner").read_text() == "keep"


@pytest.mark.parametrize("mutated_input", ["config", "strategy"])
def test_rehearse_retained_rejects_descriptor_configuration_mutation_during_smoke(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutated_input: str,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_source(data_root)
    service.runtime = FakeRuntime(apis=[FakeApi()])
    smoke_result = SmokeResult(
        4,
        "local_projection_v2_event_time",
        ("market_metadata",),
        ("/api/db/stats",),
        {"readyBasisCount": 2},
    )

    def mutate(*_args: object, **_kwargs: object) -> SmokeResult:
        target = (
            retained_root / "config/default.yaml"
            if mutated_input == "config"
            else retained_root / "strategies/production/smoke.yaml"
        )
        target.write_text("mutated: true\n")
        return smoke_result

    monkeypatch.setattr(service, "smoke", mutate)
    with pytest.raises(CutoverSafetyError):
        service.rehearse_retained(
            f"retained-mutated-{mutated_input}",
            source_rehearsal_report_id="market-v4-rehearsal-20260715-r10",
            config=config,
            inherited_environment={},
        )
    report = _read_operation_report(data_root, f"retained-mutated-{mutated_input}")
    assert report["status"] == "failed"


def test_rehearse_retained_rejects_incoherent_runtime_strategy_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_source(data_root)
    service.runtime = FakeRuntime(apis=[FakeApi()])
    original_copy = market_v4_cutover.ManagedRootFd.copy_tree_create

    def raced_copy(managed, source: Path, target: Path) -> None:
        strategy = retained_root / "strategies/production/smoke.yaml"
        original = strategy.read_bytes()
        strategy.write_text("raced: true\n")
        try:
            original_copy(managed, source, target)
        finally:
            strategy.write_bytes(original)

    monkeypatch.setattr(market_v4_cutover.ManagedRootFd, "copy_tree_create", raced_copy)
    with pytest.raises(CutoverSafetyError):
        service.rehearse_retained(
            "retained-incoherent-runtime",
            source_rehearsal_report_id="market-v4-rehearsal-20260715-r10",
            config=config,
            inherited_environment={},
        )


def test_rehearse_retained_publication_boundary_invalidates_drifted_report(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_source(data_root)
    service.runtime = FakeRuntime(apis=[FakeApi()])
    mutated = False

    def drift_after_publish(stage: str) -> None:
        nonlocal mutated
        if stage == "after_publish" and not mutated:
            mutated = True
            (retained_root / "config/default.yaml").write_text("drift: true\n")

    service._report_publish_hook = drift_after_publish
    with pytest.raises(CutoverSafetyError):
        service.rehearse_retained(
            "retained-publication-drift",
            source_rehearsal_report_id="market-v4-rehearsal-20260715-r10",
            config=config,
            inherited_environment={},
        )
    report_path = (
        data_root
        / "operations/market-v4-cutover/reports/retained-publication-drift/report.json"
    )
    if report_path.exists():
        assert json.loads(report_path.read_text())["status"] != "passed"


@pytest.mark.parametrize("market_target", ["market.duckdb", "parquet/stock_data/part.parquet"])
def test_rehearse_retained_rejects_market_tree_mutation_after_smoke(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    market_target: str,
) -> None:
    data_root = _market_root(tmp_path)
    source_id = "market-v4-rehearsal-20260715-r10"
    service, retained_root, config = _retained_source(data_root)
    runtime = FakeRuntime(apis=[FakeApi()])
    service.runtime = runtime
    smoke_result = SmokeResult(
        4,
        "local_projection_v2_event_time",
        ("market_metadata",),
        ("/api/db/stats",),
        {"readyBasisCount": 2},
    )

    def mutate_market_after_smoke(*_args: object, **_kwargs: object) -> SmokeResult:
        target = retained_root / "market-timeseries" / market_target
        target.write_bytes(target.read_bytes() + b"changed")
        return smoke_result

    monkeypatch.setattr(service, "smoke", mutate_market_after_smoke)

    with pytest.raises(CutoverSafetyError, match="retained Market tree changed"):
        service.rehearse_retained(
            "retained-mutated",
            source_rehearsal_report_id=source_id,
            config=config,
            inherited_environment={},
        )

    report = _read_operation_report(data_root, "retained-mutated")
    assert report["status"] == "failed"
    assert report["sourceMarketIdentityBefore"] != report["sourceMarketIdentityAfter"]
    assert runtime.stop_calls == 1


@pytest.mark.parametrize(
    ("failure", "expected_status", "server_joined", "worker_joined"),
    [
        ("code_drift", "failed", True, True),
        ("active_fingerprint_drift", "failed", True, True),
        ("smoke", "failed", True, True),
        ("runtime_stop_joined", "failed", True, True),
        ("runtime_stop_unjoined", "stop_failed_cleanup_deferred", False, True),
        ("worker_unjoined", "stop_failed_cleanup_deferred", True, False),
    ],
)
def test_rehearse_retained_failure_cleanup_and_join_verdicts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
    expected_status: str,
    server_joined: bool,
    worker_joined: bool,
) -> None:
    data_root = _market_root(tmp_path)
    source_id = "market-v4-rehearsal-20260715-r10"
    service, _retained_root, config = _retained_source(data_root)
    smoke_result = SmokeResult(
        4,
        "local_projection_v2_event_time",
        ("market_metadata",),
        ("/api/db/stats",),
        {"readyBasisCount": 2},
    )

    class StopFailingRuntime(FakeRuntime):
        def stop(self, _api: FakeApi) -> None:
            self.stop_calls += 1
            raise market_v4_cutover.RuntimeStopError(
                "injected stop failure",
                process_joined=failure == "runtime_stop_joined",
            )

    runtime: FakeRuntime
    if failure.startswith("runtime_stop"):
        runtime = StopFailingRuntime(apis=[FakeApi()])
    else:
        runtime = FakeRuntime(apis=[FakeApi()])
    service.runtime = runtime

    def smoke(*_args: object, **_kwargs: object) -> SmokeResult:
        if failure == "active_fingerprint_drift":
            (data_root / "config/default.yaml").write_text("drift: true\n")
        if failure == "smoke":
            raise RuntimeError("injected smoke failure")
        if failure == "worker_unjoined":
            raise market_v4_cutover.WorkerShutdownError(
                "injected worker failure",
                process_joined=False,
            )
        return smoke_result

    monkeypatch.setattr(service, "smoke", smoke)
    if failure == "code_drift":
        service.code_version, _calls = _changing_code_version(
            "deadbeef",
            "deadbeef",
            "cafebabe",
        )

    with pytest.raises(CutoverSafetyError, match="Retained Market rehearsal failed"):
        service.rehearse_retained(
            f"retained-{failure}",
            source_rehearsal_report_id=source_id,
            config=config,
            inherited_environment={},
        )

    report = _read_operation_report(data_root, f"retained-{failure}")
    assert report["status"] == expected_status
    assert report["serverProcessJoined"] is server_joined
    assert report["workerProcessJoined"] is worker_joined
    assert runtime.start_calls == 1
    assert runtime.stop_calls >= 1


def test_rehearse_retained_rejects_path_replacement_without_writing_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_source(data_root)
    runtime = FakeRuntime(apis=[FakeApi()])
    service.runtime = runtime
    detached_root = retained_root.with_name("detached-root")
    original_prepare = service._prepare_retained_runtime

    def replace_then_prepare(
        root: Path,
        *,
        runtime_name: str,
        root_fd: int | None = None,
        on_reserved: Callable[[], None] | None = None,
    ) -> None:
        root.rename(detached_root)
        shutil.copytree(detached_root, root)
        original_prepare(
            root,
            runtime_name=runtime_name,
            root_fd=root_fd,
            on_reserved=on_reserved,
        )

    monkeypatch.setattr(service, "_prepare_retained_runtime", replace_then_prepare)

    with pytest.raises(CutoverSafetyError):
        service.rehearse_retained(
            "retained-path-race",
            source_rehearsal_report_id="market-v4-rehearsal-20260715-r10",
            config=config,
            inherited_environment={},
        )

    assert not (
        retained_root / "market-timeseries/.cutover-runtime-retained-path-race"
    ).exists()
    assert runtime.start_calls == 0


def test_rehearse_retained_rejects_prelease_same_content_root_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_source(data_root)
    runtime = FakeRuntime(apis=[FakeApi()])
    service.runtime = runtime
    detached_root = retained_root.with_name("prelease-original-root")
    original_acquire = market_v4_cutover.MarketOperationLease.acquire
    replaced = False

    def replace_before_acquire(
        cls: type[market_v4_cutover.MarketOperationLease],
        lease_root: Path,
        *,
        exclusive: bool,
        blocking: bool = False,
    ) -> market_v4_cutover.MarketOperationLease:
        del cls
        nonlocal replaced
        if lease_root == retained_root and not replaced:
            replaced = True
            lease_root.rename(detached_root)
            shutil.copytree(detached_root, lease_root)
        return original_acquire(
            lease_root,
            exclusive=exclusive,
            blocking=blocking,
        )

    monkeypatch.setattr(
        market_v4_cutover.MarketOperationLease,
        "acquire",
        classmethod(replace_before_acquire),
    )

    with pytest.raises(CutoverSafetyError):
        service.rehearse_retained(
            "retained-prelease-root-race",
            source_rehearsal_report_id="market-v4-rehearsal-20260715-r10",
            config=config,
            inherited_environment={},
        )

    assert replaced is True
    assert runtime.start_calls == 0
    assert not (
        retained_root
        / "market-timeseries/.cutover-runtime-retained-prelease-root-race"
    ).exists()


def test_rehearse_retained_rejects_ancestor_symlink_to_leased_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_source(data_root)
    runtime = FakeRuntime(apis=[FakeApi()])
    service.runtime = runtime
    source_directory = retained_root.parent
    detached_source_directory = source_directory.with_name("detached-source-directory")
    original_prepare = service._prepare_retained_runtime
    substituted = False

    def substitute_ancestor_then_prepare(
        root: Path,
        *,
        runtime_name: str,
        root_fd: int | None = None,
        on_reserved: Callable[[], None] | None = None,
    ) -> None:
        nonlocal substituted
        source_directory.rename(detached_source_directory)
        source_directory.symlink_to(
            detached_source_directory,
            target_is_directory=True,
        )
        substituted = True
        original_prepare(
            root,
            runtime_name=runtime_name,
            root_fd=root_fd,
            on_reserved=on_reserved,
        )

    monkeypatch.setattr(
        service,
        "_prepare_retained_runtime",
        substitute_ancestor_then_prepare,
    )

    with pytest.raises(CutoverSafetyError):
        service.rehearse_retained(
            "retained-ancestor-symlink-race",
            source_rehearsal_report_id="market-v4-rehearsal-20260715-r10",
            config=config,
            inherited_environment={},
        )

    assert substituted is True
    assert runtime.start_calls == 0
    assert not (
        detached_source_directory
        / "root/market-timeseries/.cutover-runtime-retained-ancestor-symlink-race"
    ).exists()


def test_rehearse_retained_revalidates_code_immediately_before_runtime_start(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service, _retained_root, config = _retained_source(data_root)
    runtime = FakeRuntime(apis=[FakeApi()])
    service.runtime = runtime
    service.code_version, calls = _changing_code_version("deadbeef", "cafebabe")

    with pytest.raises(CutoverSafetyError, match="Retained Market rehearsal failed"):
        service.rehearse_retained(
            "retained-prestart-code-drift",
            source_rehearsal_report_id="market-v4-rehearsal-20260715-r10",
            config=config,
            inherited_environment={},
        )

    assert calls == ["deadbeef", "cafebabe"]
    assert runtime.start_calls == 0


@pytest.mark.parametrize(
    "relative_target",
    ["market-timeseries/market.duckdb", "market-timeseries/parquet/stock_data/part.parquet"],
)
def test_market_tree_identity_rejects_same_content_replacement_during_hash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    relative_target: str,
) -> None:
    data_root = _market_root(tmp_path)
    _service, retained_root, _config = _retained_source(data_root)
    target = retained_root / relative_target
    target_inode = target.stat().st_ino
    original_read = os.read
    replaced = False

    def replace_during_read(fd: int, size: int) -> bytes:
        nonlocal replaced
        if not replaced and os.fstat(fd).st_ino == target_inode:
            replaced = True
            payload = target.read_bytes()
            target.rename(target.with_suffix(target.suffix + ".replaced"))
            target.write_bytes(payload)
        return original_read(fd, size)

    monkeypatch.setattr(os, "read", replace_during_read)
    root_fd = os.open(retained_root, market_v4_cutover._DIR_OPEN_FLAGS)
    try:
        with pytest.raises(CutoverSafetyError, match="changed during identity hashing"):
            MarketV4CutoverService._market_tree_identity(root_fd)
    finally:
        os.close(root_fd)
    assert replaced is True


@pytest.mark.parametrize("unjoined_process", ["server", "worker"])
def test_rehearse_retained_unjoined_process_keeps_competing_lease_blocked(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    unjoined_process: str,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_source(data_root)

    class LeaseHoldingRuntime(FakeRuntime):
        retained_lease_fd = -1

        def start(self, **kwargs: object) -> FakeApi:
            self.start_calls += 1
            if unjoined_process == "server":
                self.retained_lease_fd = os.dup(int(kwargs["lease_fd"]))
                raise market_v4_cutover.RuntimeStopError(
                    "injected unjoined server",
                    process_joined=False,
                )
            return super().start(**kwargs)

    runtime = LeaseHoldingRuntime(apis=[FakeApi()])
    service.runtime = runtime

    if unjoined_process == "worker":
        def unjoined_smoke(
            *_args: object,
            **kwargs: object,
        ) -> SmokeResult:
            runtime.retained_lease_fd = os.dup(int(kwargs["guard_lease_fd"]))
            raise market_v4_cutover.WorkerShutdownError(
                "injected unjoined worker",
                process_joined=False,
            )

        monkeypatch.setattr(service, "smoke", unjoined_smoke)

    with pytest.raises(CutoverSafetyError, match="Retained Market rehearsal failed"):
        service.rehearse_retained(
            f"retained-unjoined-{unjoined_process}",
            source_rehearsal_report_id="market-v4-rehearsal-20260715-r10",
            config=config,
            inherited_environment={},
        )

    try:
        with pytest.raises(CutoverSafetyError, match="operation lease"):
            market_v4_cutover.MarketOperationLease.acquire(
                retained_root,
                exclusive=False,
            )
    finally:
        os.close(runtime.retained_lease_fd)

    with market_v4_cutover.MarketOperationLease.acquire(
        retained_root,
        exclusive=True,
    ):
        pass


def test_rehearsal_failure_report_keeps_start_identity_and_original_error(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)

    class OriginalRebuildError(RuntimeError):
        pass

    class FailingApi(FakeApi):
        def request(
            self,
            method: str,
            path: str,
            payload: dict[str, object] | None = None,
        ) -> dict[str, object]:
            del method, path, payload
            raise OriginalRebuildError("injected rebuild failure")

    runtime = FakeRuntime(apis=[FailingApi()])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
        runtime=runtime,
    )
    code_version, _calls = _changing_code_version(
        "deadbeef", "deadbeef-dirty"
    )
    service.code_version = code_version

    with pytest.raises(CutoverSafetyError, match="rehearsal failed"):
        service.rehearse(
            "rehearsal-original-error",
            SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    report = json.loads(
        (
            data_root
            / "operations/market-v4-cutover/reports/rehearsal-original-error/report.json"
        ).read_text()
    )
    assert runtime.stop_calls == 1
    assert report["status"] == "failed"
    assert report["codeVersion"] == "deadbeef"
    assert report["errorType"] == "OriginalRebuildError"


@pytest.mark.parametrize(
    ("process_joined", "expected_status"),
    [
        (False, "stop_failed_cleanup_deferred"),
        (True, "failed"),
    ],
)
def test_rehearsal_cleanup_join_verdict_preserves_primary_error(
    tmp_path: Path,
    process_joined: bool,
    expected_status: str,
) -> None:
    data_root = _market_root(tmp_path)

    class OriginalRebuildError(RuntimeError):
        pass

    class FailingApi(FakeApi):
        def request(
            self,
            method: str,
            path: str,
            payload: dict[str, object] | None = None,
        ) -> dict[str, object]:
            del method, path, payload
            raise OriginalRebuildError("injected rebuild failure")

    class UnjoinedRuntime(FakeRuntime):
        def stop(self, _api: FakeApi) -> None:
            self.stop_calls += 1
            raise market_v4_cutover.RuntimeStopError(
                "owned rehearsal process stop result",
                process_joined=process_joined,
            )

    runtime = UnjoinedRuntime(apis=[FailingApi()])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
        runtime=runtime,
    )

    with pytest.raises(CutoverSafetyError, match="rehearsal failed"):
        service.rehearse(
            "rehearsal-unjoined-cleanup",
            SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    report = json.loads(
        (
            data_root
            / "operations/market-v4-cutover/reports/rehearsal-unjoined-cleanup/report.json"
        ).read_text()
    )
    assert runtime.cancel_calls == 1
    assert runtime.stop_calls == 1
    assert report["status"] == expected_status
    assert report["errorType"] == "OriginalRebuildError"
    assert report["stopErrorType"] == "RuntimeStopError"
    assert report["serverProcessJoined"] is process_joined
    assert report["codeVersion"] == "deadbeef"


def test_rehearsal_cancel_failure_is_diagnostic_when_stop_proves_join(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)

    class CancelFailingRuntime(FakeRuntime):
        def cancel_owned_work(self, _api: FakeApi) -> None:
            self.cancel_calls += 1
            raise OSError("injected cancel failure")

    runtime = CancelFailingRuntime(apis=[FakeApi(invalid_lineage=True)])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
        runtime=runtime,
    )

    with pytest.raises(CutoverSafetyError, match="rehearsal failed"):
        service.rehearse(
            "rehearsal-cancel-diagnostic",
            SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    report = json.loads(
        (
            data_root
            / "operations/market-v4-cutover/reports/rehearsal-cancel-diagnostic/report.json"
        ).read_text()
    )
    assert runtime.cancel_calls == 1
    assert runtime.stop_calls == 1
    assert report["status"] == "failed"
    assert report["errorType"] == "CutoverSafetyError"
    assert report["cleanupErrorType"] == "OSError"
    assert "stopErrorType" not in report
    assert report["serverProcessJoined"] is True


def test_rehearsal_report_preserves_bounded_redacted_terminal_job_diagnostic(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    secret = "jquants-secret-value"

    class FailedSyncApi(FakeApi):
        def request(
            self,
            method: str,
            path: str,
            payload: dict[str, object] | None = None,
        ) -> dict[str, object]:
            if path == "/api/db/sync":
                return {"jobId": "sync-1", "status": "pending"}
            if path == "/api/db/sync/jobs/sync-1":
                return {
                    "jobId": "sync-1",
                    "status": "failed",
                    "progress": {
                        "stage": "stock_data",
                        "message": f"bulk plan unavailable under {data_root}",
                    },
                    "result": {
                        "errors": ["bulk coverage missing", "no REST fallback"],
                    },
                    "error": f"BulkFetchRequiredError token={secret} "
                    + ("x" * 4_000),
                }
            return super().request(method, path, payload)

    service = _service(data_root, runtime=FakeRuntime(apis=[FailedSyncApi()]))

    with pytest.raises(CutoverSafetyError, match="rehearsal failed"):
        service.rehearse(
            "rehearsal-job-diagnostic",
            SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={"JQUANTS_API_KEY": secret},
        )

    report = json.loads(
        (
            data_root
            / "operations/market-v4-cutover/reports/rehearsal-job-diagnostic/report.json"
        ).read_text()
    )
    diagnostic = report["errorMessage"]
    assert "sync job ended with status failed" in diagnostic
    assert "stage=stock_data" in diagnostic
    assert "bulk plan unavailable" in diagnostic
    assert "BulkFetchRequiredError" in diagnostic
    assert "bulk coverage missing" in diagnostic
    assert secret not in diagnostic
    assert str(data_root) not in diagnostic
    assert "<redacted-secret>" in diagnostic
    assert "<data-root>" in diagnostic
    assert len(diagnostic) <= 1_024


@pytest.mark.parametrize(
    ("process_joined", "expected_status"),
    [
        (False, "stop_failed_cleanup_deferred"),
        (True, "failed"),
    ],
)
def test_rehearsal_startup_error_uses_embedded_join_verdict(
    tmp_path: Path,
    process_joined: bool,
    expected_status: str,
) -> None:
    data_root = _market_root(tmp_path)

    class StartupFailingRuntime(FakeRuntime):
        retained_lease_fd = -1

        def start(self, **kwargs: object) -> FakeApi:
            inherited_fd = os.dup(int(kwargs["lease_fd"]))
            if process_joined:
                os.close(inherited_fd)
            else:
                self.retained_lease_fd = inherited_fd
            raise market_v4_cutover.RuntimeStopError(
                "injected startup join verdict",
                process_joined=process_joined,
            )

    runtime = StartupFailingRuntime()
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
        runtime=runtime,
    )

    with pytest.raises(CutoverSafetyError, match="rehearsal failed"):
        service.rehearse(
            f"rehearsal-startup-joined-{process_joined}",
            SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    report = json.loads(
        (
            data_root
            / "operations/market-v4-cutover/reports"
            / f"rehearsal-startup-joined-{process_joined}/report.json"
        ).read_text()
    )
    assert report["status"] == expected_status
    assert report["errorType"] == "RuntimeStopError"
    assert report["stopErrorType"] == "RuntimeStopError"
    assert report["serverProcessJoined"] is process_joined

    rehearsal_root = (
        data_root
        / "operations/market-v4-cutover/rehearsals"
        / f"rehearsal-startup-joined-{process_joined}/root"
    )
    if not process_joined:
        try:
            with pytest.raises(CutoverSafetyError, match="operation lease"):
                market_v4_cutover.MarketOperationLease.acquire(
                    rehearsal_root,
                    exclusive=False,
                )
        finally:
            os.close(runtime.retained_lease_fd)

    with market_v4_cutover.MarketOperationLease.acquire(
        rehearsal_root,
        exclusive=True,
    ):
        pass


def test_rehearsal_identity_drift_cannot_publish_passed_report(tmp_path: Path) -> None:
    data_root = _market_root(tmp_path)
    runtime = FakeRuntime(apis=[FakeApi()])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
        runtime=runtime,
    )
    code_version, _calls = _changing_code_version(
        "deadbeef", "deadbeef-dirty"
    )
    service.code_version = code_version

    with pytest.raises(CutoverSafetyError, match="rehearsal failed"):
        service.rehearse(
            "rehearsal-code-drift",
            SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    report = json.loads(
        (
            data_root
            / "operations/market-v4-cutover/reports/rehearsal-code-drift/report.json"
        ).read_text()
    )
    assert report["status"] == "failed"
    assert report["codeVersion"] == "deadbeef"
    assert report["errorType"] == "CutoverSafetyError"


def test_rehearsal_rejects_concurrent_strategy_edit_and_stale_report(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    (data_root / "config").mkdir()
    (data_root / "config/default.yaml").write_text("mode: market\n")
    strategy = data_root / "strategies/production/smoke.yaml"
    strategy.parent.mkdir(parents=True)
    strategy.write_text("value: before\n")

    class EditingRuntime(FakeRuntime):
        def stop(self, api: FakeApi) -> None:
            super().stop(api)
            strategy.write_text("value: after\n")

    runtime = EditingRuntime(apis=[FakeApi()])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
        runtime=runtime,
    )
    original_fingerprint = service.root_fingerprint(data_root)

    with pytest.raises(CutoverSafetyError, match="rehearsal failed"):
        service.rehearse(
            "raced-rehearsal",
            SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    report = json.loads(
        (data_root / "operations/market-v4-cutover/reports/raced-rehearsal/report.json").read_text()
    )
    assert report["status"] == "failed"
    assert report["targetRootFingerprint"] == original_fingerprint
    with pytest.raises(CutoverSafetyError, match="passing rehearsal report"):
        service.cutover(
            "active-from-stale",
            rehearsal_report_id="raced-rehearsal",
            backup_id="unused",
            config=SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )


@pytest.mark.parametrize("failure_stage", ["after_temp_fsync", "after_publish"])
def test_rehearsal_report_publish_failure_never_leaves_passed_evidence(
    tmp_path: Path,
    failure_stage: str,
) -> None:
    data_root = _market_root(tmp_path)
    runtime = FakeRuntime(apis=[FakeApi()])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
        runtime=runtime,
    )
    failed_once = False

    def inject(stage: str) -> None:
        nonlocal failed_once
        if stage == failure_stage and not failed_once:
            failed_once = True
            raise OSError(f"injected {stage}")

    service._report_publish_hook = inject
    with pytest.raises(CutoverSafetyError, match="rehearsal failed"):
        service.rehearse(
            f"report-{failure_stage}",
            SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    report_path = (
        data_root
        / f"operations/market-v4-cutover/reports/report-{failure_stage}/report.json"
    )
    if report_path.exists():
        assert json.loads(report_path.read_text())["status"] != "passed"
    assert not list(report_path.parent.glob(".report.json.*.tmp"))


@pytest.mark.parametrize("failure_stage", ["after_temp_fsync", "after_publish"])
def test_active_report_publish_failure_restores_without_passed_evidence(
    tmp_path: Path,
    failure_stage: str,
) -> None:
    data_root = _market_root(tmp_path)
    runtime = FakeRuntime(apis=[FakeApi(), FakeApi(), FakeApi()])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
        runtime=runtime,
    )
    service.backup("before-report-failure")
    service.rehearse(
        "passing-before-report-failure",
        SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={},
    )
    failed_once = False

    def inject(stage: str) -> None:
        nonlocal failed_once
        if stage == failure_stage and not failed_once:
            failed_once = True
            raise OSError(f"injected {stage}")

    service._report_publish_hook = inject
    with pytest.raises(CutoverSafetyError, match="restored backup"):
        service.cutover(
            f"active-{failure_stage}",
            rehearsal_report_id="passing-before-report-failure",
            backup_id="before-report-failure",
            config=SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    assert (data_root / "market-timeseries/market.duckdb").read_bytes() == b"duckdb-v3"
    report_path = (
        data_root
        / f"operations/market-v4-cutover/reports/active-{failure_stage}/report.json"
    )
    if report_path.exists():
        assert json.loads(report_path.read_text())["status"] != "passed"
    assert not list(report_path.parent.glob(".report.json.*.tmp"))


def test_cutover_rechecks_fingerprint_after_runtime_start_and_restores(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    (data_root / "config").mkdir()
    (data_root / "config/default.yaml").write_text("mode: market\n")
    strategy = data_root / "strategies/production/smoke.yaml"
    strategy.parent.mkdir(parents=True)
    strategy.write_text("value: before\n")

    class StartEditingRuntime(FakeRuntime):
        starts = 0

        def start(
            self,
            *,
            root_fd: int,
            market_fd: int,
            lease_fd: int,
            environment: dict[str, str],
            log_path: Path,
            log_fd: int,
        ) -> FakeApi:
            api = super().start(
                root_fd=root_fd,
                market_fd=market_fd,
                lease_fd=lease_fd,
                environment=environment,
                log_path=log_path,
                log_fd=log_fd,
            )
            self.starts += 1
            if self.starts == 2:
                strategy.write_text("value: changed-during-start\n")
            return api

    runtime = StartEditingRuntime(apis=[FakeApi(), FakeApi()])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
        runtime=runtime,
    )
    service.backup("before-start-race")
    service.rehearse(
        "passing-before-start-race",
        SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={},
    )

    with pytest.raises(CutoverSafetyError, match="active market is unchanged"):
        service.cutover(
            "active-start-race",
            rehearsal_report_id="passing-before-start-race",
            backup_id="before-start-race",
            config=SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    assert (data_root / "market-timeseries/market.duckdb").read_bytes() == b"duckdb-v3"
    report_path = data_root / "operations/market-v4-cutover/reports/active-start-race/report.json"
    if report_path.exists():
        assert json.loads(report_path.read_text())["status"] != "passed"


@pytest.mark.parametrize("mutation", ["mkdir", "copy", "write"])
def test_operation_parent_swap_never_writes_external_tree(
    tmp_path: Path,
    mutation: str,
) -> None:
    data_root = _market_root(tmp_path)
    external = tmp_path / "external-operations"
    external.mkdir()
    runtime = FakeRuntime(apis=[FakeApi()])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
        runtime=runtime,
    )
    swapped = False

    def swap_parent(stage: str) -> None:
        nonlocal swapped
        if stage != mutation or swapped:
            return
        swapped = True
        cutover_root = data_root / "operations/market-v4-cutover"
        detached = data_root / "operations/market-v4-cutover.detached"
        cutover_root.rename(detached)
        cutover_root.symlink_to(external, target_is_directory=True)

    service._managed_mutation_hook = swap_parent
    with pytest.raises(CutoverSafetyError):
        if mutation in {"mkdir", "copy"}:
            service.backup(f"swap-{mutation}")
        else:
            service.rehearse(
                "swap-write",
                SmokeConfig("7203", "production/smoke", "primeMarket"),
                inherited_environment={},
            )

    assert swapped is True
    assert list(external.iterdir()) == []


def test_cutover_requires_exact_passing_rehearsal_and_verified_backup(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    runtime = FakeRuntime(apis=[FakeApi(), FakeApi(), FakeApi()])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
        runtime=runtime,
    )
    service.backup("backup-001")
    service.rehearse(
        "rehearsal-001",
        SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={},
    )

    with pytest.raises(CutoverSafetyError, match="passing rehearsal report"):
        service.cutover(
            "active-bad",
            rehearsal_report_id="missing",
            backup_id="backup-001",
            config=SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    result = service.cutover(
        "active-001",
        rehearsal_report_id="rehearsal-001",
        backup_id="backup-001",
        config=SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={},
    )
    report = json.loads((data_root / result.report_path).read_text())
    assert report["status"] == "passed"
    assert report["backupManifest"] == "backups/backup-001/manifest.json"
    assert report["rehearsalReportId"] == "rehearsal-001"
    assert report["phases"][-1]["name"] == "activated_market_smoke"
    assert runtime.stop_calls == 3


@pytest.mark.parametrize(
    "malformation",
    [
        "missing_mode",
        "missing_server_join",
        "false_server_join",
        "missing_worker_join",
        "false_worker_join",
    ],
)
def test_cutover_rejects_rehearsal_without_explicit_passing_evidence(
    tmp_path: Path,
    malformation: str,
) -> None:
    data_root = _market_root(tmp_path)
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
        runtime=FakeRuntime(apis=[FakeApi()]),
    )
    smoke_config = SmokeConfig("7203", "production/smoke", "primeMarket")
    service.rehearse(
        "passing-rehearsal",
        smoke_config,
        inherited_environment={},
    )
    report = json.loads(
        (
            data_root
            / "operations/market-v4-cutover/reports/passing-rehearsal/report.json"
        ).read_text()
    )
    report.update(
        {
            "rehearsalMode": "full_rebuild",
            "serverProcessJoined": True,
            "workerProcessJoined": True,
        }
    )
    field = {
        "missing_mode": "rehearsalMode",
        "missing_server_join": "serverProcessJoined",
        "false_server_join": "serverProcessJoined",
        "missing_worker_join": "workerProcessJoined",
        "false_worker_join": "workerProcessJoined",
    }[malformation]
    if malformation.startswith("missing_"):
        report.pop(field)
    else:
        report[field] = False
    report_id = f"malformed-{malformation}"
    report["reportId"] = report_id
    _write_report(data_root, report_id, report)

    with pytest.raises(CutoverSafetyError, match="exact passing rehearsal"):
        service.cutover(
            f"active-{malformation}",
            rehearsal_report_id=report_id,
            backup_id="unverified-backup",
            config=smoke_config,
            inherited_environment={},
        )

    assert not (
        data_root
        / "operations/market-v4-cutover/staging"
        / f"active-{malformation}"
    ).exists()


@pytest.mark.parametrize(
    "malformation",
    [
        "missing_source_report_id",
        "empty_source_report_id",
        "missing_source_code_version",
        "empty_source_code_version",
        "missing_source_root_fingerprint",
        "empty_source_root_fingerprint",
        "missing_market_identity_before",
        "missing_market_identity_after",
        "changed_market_identity_after",
    ],
)
def test_cutover_rejects_retained_rehearsal_without_exact_source_evidence(
    tmp_path: Path,
    malformation: str,
) -> None:
    data_root = _market_root(tmp_path)
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
        runtime=FakeRuntime(apis=[FakeApi()]),
    )
    smoke_config = SmokeConfig("7203", "production/smoke", "primeMarket")
    service.rehearse(
        "passing-rehearsal",
        smoke_config,
        inherited_environment={},
    )
    report = json.loads(
        (
            data_root
            / "operations/market-v4-cutover/reports/passing-rehearsal/report.json"
        ).read_text()
    )
    report.update(
        {
            "rehearsalMode": "retained_market_smoke",
            "serverProcessJoined": True,
            "workerProcessJoined": True,
            "sourceRehearsalReportId": "passing-rehearsal",
            "sourceRehearsalCodeVersion": "deadbeef",
            "sourceRetainedRootFingerprint": "retained-root-fingerprint",
            "sourceMarketIdentityBefore": {"device": 1, "inode": 2},
            "sourceMarketIdentityAfter": {"device": 1, "inode": 2},
        }
    )
    field = {
        "missing_source_report_id": "sourceRehearsalReportId",
        "empty_source_report_id": "sourceRehearsalReportId",
        "missing_source_code_version": "sourceRehearsalCodeVersion",
        "empty_source_code_version": "sourceRehearsalCodeVersion",
        "missing_source_root_fingerprint": "sourceRetainedRootFingerprint",
        "empty_source_root_fingerprint": "sourceRetainedRootFingerprint",
        "missing_market_identity_before": "sourceMarketIdentityBefore",
        "missing_market_identity_after": "sourceMarketIdentityAfter",
        "changed_market_identity_after": "sourceMarketIdentityAfter",
    }[malformation]
    if malformation.startswith("missing_"):
        report.pop(field)
    elif malformation.startswith("empty_"):
        report[field] = ""
    else:
        report[field] = {"device": 1, "inode": 3}
    report_id = f"retained-{malformation}"
    report["reportId"] = report_id
    _write_report(data_root, report_id, report)

    with pytest.raises(CutoverSafetyError, match="exact passing rehearsal"):
        service.cutover(
            f"active-{malformation}",
            rehearsal_report_id=report_id,
            backup_id="unverified-backup",
            config=smoke_config,
            inherited_environment={},
        )

    assert not (
        data_root
        / "operations/market-v4-cutover/staging"
        / f"active-{malformation}"
    ).exists()


@pytest.mark.parametrize(
    "source_mutation",
    [
        "missing_report",
        "wrong_report_id",
        "wrong_phase",
        "wrong_status",
        "wrong_target_fingerprint",
        "unjoined_server",
        "unjoined_worker",
        "root_symlink",
        "root_fingerprint_drift",
    ],
)
def test_cutover_reresolves_retained_rehearsal_provenance_before_backup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    source_mutation: str,
) -> None:
    data_root = _market_root(tmp_path)
    source_id = "market-v4-rehearsal-20260715-r10"
    service, retained_root, config = _retained_source(data_root)
    service.runtime = FakeRuntime(apis=[FakeApi()])
    smoke_result = SmokeResult(
        4,
        "local_projection_v2_event_time",
        ("market_metadata",),
        ("/api/db/stats",),
        {"readyBasisCount": 2},
    )
    monkeypatch.setattr(service, "smoke", lambda *_args, **_kwargs: smoke_result)
    service.rehearse_retained(
        "retained-cutover-evidence",
        source_rehearsal_report_id=source_id,
        config=config,
        inherited_environment={},
    )
    source_report_path = (
        data_root
        / "operations/market-v4-cutover/reports"
        / source_id
        / "report.json"
    )
    source_report = json.loads(source_report_path.read_text())
    if source_mutation == "missing_report":
        source_report_path.unlink()
    elif source_mutation == "wrong_report_id":
        source_report["reportId"] = "different-source"
        source_report_path.write_text(json.dumps(source_report))
    elif source_mutation == "wrong_phase":
        source_report["phase"] = "cutover"
        source_report_path.write_text(json.dumps(source_report))
    elif source_mutation == "wrong_status":
        source_report["status"] = "stop_failed_cleanup_deferred"
        source_report_path.write_text(json.dumps(source_report))
    elif source_mutation == "wrong_target_fingerprint":
        source_report["targetRootFingerprint"] = "0" * 64
        source_report_path.write_text(json.dumps(source_report))
    elif source_mutation == "unjoined_server":
        source_report["serverProcessJoined"] = False
        source_report_path.write_text(json.dumps(source_report))
    elif source_mutation == "unjoined_worker":
        source_report["workerProcessJoined"] = False
        source_report_path.write_text(json.dumps(source_report))
    elif source_mutation == "root_symlink":
        detached = tmp_path / "cutover-detached-retained"
        retained_root.rename(detached)
        retained_root.symlink_to(detached, target_is_directory=True)
    elif source_mutation == "root_fingerprint_drift":
        (retained_root / "config/default.yaml").write_text("drift: true\n")

    backup_verified = False

    def unexpected_backup_verification(_backup_id: str):
        nonlocal backup_verified
        backup_verified = True
        raise AssertionError("backup verification must not run")

    monkeypatch.setattr(service, "verify_backup", unexpected_backup_verification)

    with pytest.raises(CutoverSafetyError, match="exact passing rehearsal"):
        service.cutover(
            f"active-provenance-{source_mutation}",
            rehearsal_report_id="retained-cutover-evidence",
            backup_id="must-not-verify",
            config=config,
            inherited_environment={},
        )

    assert backup_verified is False
    assert not (
        data_root
        / "operations/market-v4-cutover/staging"
        / f"active-provenance-{source_mutation}"
    ).exists()


@pytest.mark.parametrize(
    "evidence_mutation",
    [
        "missing_api_checks",
        "malformed_schema_coverage",
        "missing_retained_phase",
        "forged_equal_identity",
        "post_report_market_replacement",
    ],
)
def test_cutover_rejects_inexact_retained_evidence_before_backup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    evidence_mutation: str,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_source(data_root)
    service.runtime = FakeRuntime(apis=[FakeApi()])
    service.rehearse_retained(
        "retained-exact-evidence",
        source_rehearsal_report_id="market-v4-rehearsal-20260715-r10",
        config=config,
        inherited_environment={},
    )
    report_path = (
        data_root
        / "operations/market-v4-cutover/reports/retained-exact-evidence/report.json"
    )
    report = json.loads(report_path.read_text())
    if evidence_mutation == "missing_api_checks":
        report.pop("apiChecks")
    elif evidence_mutation == "malformed_schema_coverage":
        report["schemaCoverage"] = {
            "schemaVersion": 4,
            "stockPriceAdjustmentMode": "local_projection_v2_event_time",
            "adjustedMetrics": {"readyBasisCount": 0},
        }
    elif evidence_mutation == "missing_retained_phase":
        report["phases"] = []
    elif evidence_mutation == "forged_equal_identity":
        report["sourceMarketIdentityBefore"] = {"forged": True}
        report["sourceMarketIdentityAfter"] = {"forged": True}
    elif evidence_mutation == "post_report_market_replacement":
        market_db = retained_root / "market-timeseries/market.duckdb"
        market_db.write_bytes(market_db.read_bytes() + b"replaced")
    if evidence_mutation != "post_report_market_replacement":
        report_path.write_text(json.dumps(report))

    backup_verified = False

    def unexpected_backup_verification(_backup_id: str):
        nonlocal backup_verified
        backup_verified = True
        raise AssertionError("backup verification must not run")

    monkeypatch.setattr(service, "verify_backup", unexpected_backup_verification)
    with pytest.raises(CutoverSafetyError, match="exact passing rehearsal"):
        service.cutover(
            f"active-inexact-{evidence_mutation}",
            rehearsal_report_id="retained-exact-evidence",
            backup_id="must-not-verify",
            config=config,
            inherited_environment={},
        )
    assert backup_verified is False


def test_cutover_rejects_retained_evidence_without_screening_status_poll_before_backup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, _retained_root, config = _retained_source(data_root)
    service.runtime = FakeRuntime(apis=[FakeApi()])
    service.rehearse_retained(
        "retained-without-screening-poll",
        source_rehearsal_report_id="market-v4-rehearsal-20260715-r10",
        config=config,
        inherited_environment={},
    )
    report_path = data_root / (
        "operations/market-v4-cutover/reports/"
        "retained-without-screening-poll/report.json"
    )
    report = json.loads(report_path.read_text())
    report["apiChecks"] = [
        path
        for path in report["apiChecks"]
        if path != "/api/analytics/screening/jobs/screen-1"
    ]
    report_path.write_text(json.dumps(report))
    backup_verified = False

    def unexpected_backup_verification(_backup_id: str):
        nonlocal backup_verified
        backup_verified = True
        raise AssertionError("backup verification must not run")

    monkeypatch.setattr(service, "verify_backup", unexpected_backup_verification)
    with pytest.raises(CutoverSafetyError, match="exact passing rehearsal"):
        service.cutover(
            "active-without-screening-poll",
            rehearsal_report_id="retained-without-screening-poll",
            backup_id="must-not-verify",
            config=config,
            inherited_environment={},
        )

    assert backup_verified is False


@pytest.mark.parametrize(
    "mutation",
    [
        "missing_sync_status",
        "missing_sync_phase",
        "missing_semantic_phase",
        "malformed_schema_coverage",
    ],
)
def test_cutover_rejects_inexact_full_rebuild_evidence_before_backup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    data_root = _market_root(tmp_path)
    service = _service(
        data_root,
        duckdb=FakeDuckDb(
            MarketSourceMetadata(4, "local_projection_v2_event_time")
        ),
        runtime=FakeRuntime(apis=[FakeApi()]),
    )
    config = SmokeConfig("7203", "production/smoke", "primeMarket")
    service.rehearse("full-exact-evidence", config, inherited_environment={})
    report_path = data_root / (
        "operations/market-v4-cutover/reports/full-exact-evidence/report.json"
    )
    report = json.loads(report_path.read_text())
    if mutation == "missing_sync_status":
        report["apiChecks"] = [
            path for path in report["apiChecks"] if "/api/db/sync/jobs/" not in path
        ]
    elif mutation == "missing_sync_phase":
        report["phases"] = [
            phase
            for phase in report["phases"]
            if phase["name"] != "initial_sync_and_adjusted_metrics_pit"
        ]
    elif mutation == "missing_semantic_phase":
        report["phases"] = [
            phase for phase in report["phases"] if phase["name"] != "semantic_smoke"
        ]
    else:
        report["schemaCoverage"] = {"schemaVersion": 4}
    report_path.write_text(json.dumps(report))
    backup_verified = False

    def unexpected_backup_verification(_backup_id: str):
        nonlocal backup_verified
        backup_verified = True
        raise AssertionError("backup verification must not run")

    monkeypatch.setattr(service, "verify_backup", unexpected_backup_verification)
    with pytest.raises(CutoverSafetyError, match="exact passing rehearsal"):
        service.cutover(
            f"active-full-inexact-{mutation}",
            rehearsal_report_id="full-exact-evidence",
            backup_id="must-not-verify",
            config=config,
            inherited_environment={},
        )

    assert backup_verified is False


def test_cutover_accepts_exact_retained_evidence(tmp_path: Path) -> None:
    data_root = _market_root(tmp_path)
    service, _retained_root, config = _retained_source(data_root)
    service.runtime = FakeRuntime(apis=[FakeApi(), FakeApi(), FakeApi()])
    service.backup("retained-exact-backup")
    service.rehearse_retained(
        "retained-exact-cutover",
        source_rehearsal_report_id="market-v4-rehearsal-20260715-r10",
        config=config,
        inherited_environment={},
    )

    result = service.cutover(
        "active-from-retained-exact",
        rehearsal_report_id="retained-exact-cutover",
        backup_id="retained-exact-backup",
        config=config,
        inherited_environment={},
    )

    assert _read_operation_report(data_root, result.report_id)["status"] == "passed"


def test_rehearse_retained_mutation_failure_preserves_completed_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_source(data_root)
    service.runtime = FakeRuntime(apis=[FakeApi()])
    original_smoke = service.smoke

    def mutate_after_real_smoke(*args: object, **kwargs: object) -> SmokeResult:
        result = original_smoke(*args, **kwargs)
        market_db = retained_root / "market-timeseries/market.duckdb"
        market_db.write_bytes(market_db.read_bytes() + b"changed")
        return result

    monkeypatch.setattr(service, "smoke", mutate_after_real_smoke)
    with pytest.raises(CutoverSafetyError) as captured:
        service.rehearse_retained(
            "retained-preserved-failure-evidence",
            source_rehearsal_report_id="market-v4-rehearsal-20260715-r10",
            config=config,
            inherited_environment={},
        )
    mutation_error = getattr(
        market_v4_cutover,
        "RetainedMarketMutationError",
        None,
    )
    assert mutation_error is not None
    assert isinstance(captured.value.__cause__, mutation_error)
    report = _read_operation_report(data_root, "retained-preserved-failure-evidence")
    assert report["apiChecks"]
    assert report["phases"][0]["name"] == "retained_market_smoke"


def test_retained_runbook_enumerates_all_forbidden_mutations() -> None:
    runbook = (
        Path(__file__).resolve().parents[6]
        / "docs/runbooks/market-v4-cutover.md"
    ).read_text()
    for operation in (
        "sync",
        "reset",
        "repair",
        "stock refresh",
        "intraday sync",
        "adjusted-metric materialization",
    ):
        assert operation in runbook


def test_cutover_failure_stops_owned_server_and_restores_backup(tmp_path: Path) -> None:
    data_root = _market_root(tmp_path)
    runtime = FakeRuntime(
        apis=[FakeApi(), FakeApi(), FakeApi(invalid_lineage=True)]
    )
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
        runtime=runtime,
    )
    service.backup("backup-001")
    service.rehearse(
        "rehearsal-001",
        SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={},
    )
    original = (data_root / "market-timeseries/market.duckdb").read_bytes()

    with pytest.raises(CutoverSafetyError, match="restored backup backup-001"):
        service.cutover(
            "active-failed",
            rehearsal_report_id="rehearsal-001",
            backup_id="backup-001",
            config=SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    assert runtime.cancel_calls == 1
    assert runtime.stop_calls == 3
    assert (data_root / "market-timeseries/market.duckdb").read_bytes() == original
    assert (data_root / "operations/market-v4-cutover/backups/backup-001").exists()


def test_cutover_stage_failure_leaves_active_market_identity_untouched(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    runtime = FakeRuntime(apis=[FakeApi(), FakeApi(invalid_lineage=True)])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
        runtime=runtime,
    )
    service.backup("stage-failure-backup")
    service.rehearse(
        "stage-failure-rehearsal",
        SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={},
    )
    active = data_root / "market-timeseries"
    active_before = active.stat()
    database_before = (active / "market.duckdb").read_bytes()

    with pytest.raises(CutoverSafetyError):
        service.cutover(
            "stage-failure-active",
            rehearsal_report_id="stage-failure-rehearsal",
            backup_id="stage-failure-backup",
            config=SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    active_after = active.stat()
    assert (active_after.st_dev, active_after.st_ino) == (
        active_before.st_dev,
        active_before.st_ino,
    )
    assert (active / "market.duckdb").read_bytes() == database_before
    quarantine = data_root / "operations/market-v4-cutover/quarantine"
    assert not quarantine.exists() or not list(quarantine.iterdir())
    assert (
        data_root
        / "operations/market-v4-cutover/staging/stage-failure-active/root/market-timeseries"
    ).is_dir()


def test_cutover_preactivation_failure_report_survives_code_drift(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    runtime = FakeRuntime(apis=[FakeApi()])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
        runtime=runtime,
    )
    service.backup("preactivation-drift-backup")
    service.rehearse(
        "preactivation-drift-rehearsal",
        SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={},
    )
    runtime.apis.append(FakeApi(invalid_lineage=True))
    code_version, _calls = _changing_code_version(
        "deadbeef", "deadbeef", "deadbeef-dirty"
    )
    service.code_version = code_version
    active_before = (data_root / "market-timeseries/market.duckdb").read_bytes()

    with pytest.raises(CutoverSafetyError, match="active market is unchanged"):
        service.cutover(
            "preactivation-code-drift",
            rehearsal_report_id="preactivation-drift-rehearsal",
            backup_id="preactivation-drift-backup",
            config=SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    assert (data_root / "market-timeseries/market.duckdb").read_bytes() == active_before
    report = json.loads(
        (
            data_root
            / "operations/market-v4-cutover/reports/preactivation-code-drift/report.json"
        ).read_text()
    )
    assert report["status"] == "failed_active_untouched"
    assert report["codeVersion"] == "deadbeef"
    assert report["errorType"] == "CutoverSafetyError"


def test_cutover_postactivation_identity_drift_restores_backup(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    runtime = FakeRuntime(apis=[FakeApi(), FakeApi(), FakeApi()])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
        runtime=runtime,
    )
    original = (data_root / "market-timeseries/market.duckdb").read_bytes()
    service.backup("postactivation-drift-backup")
    service.rehearse(
        "postactivation-drift-rehearsal",
        SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={},
    )
    code_version, _calls = _changing_code_version("deadbeef", "deadbeef-dirty")
    service.code_version = code_version

    with pytest.raises(CutoverSafetyError, match="restored backup"):
        service.cutover(
            "postactivation-code-drift",
            rehearsal_report_id="postactivation-drift-rehearsal",
            backup_id="postactivation-drift-backup",
            config=SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    assert (data_root / "market-timeseries/market.duckdb").read_bytes() == original
    report = json.loads(
        (
            data_root
            / "operations/market-v4-cutover/reports/postactivation-code-drift/report.json"
        ).read_text()
    )
    assert report["status"] == "failed_restored"
    assert report["codeVersion"] == "deadbeef"
    assert report["errorType"] == "CutoverSafetyError"


def test_cutover_steady_code_identity_passes(tmp_path: Path) -> None:
    data_root = _market_root(tmp_path)
    runtime = FakeRuntime(apis=[FakeApi(), FakeApi(), FakeApi()])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
        runtime=runtime,
    )
    service.backup("steady-identity-backup")
    service.rehearse(
        "steady-identity-rehearsal",
        SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={},
    )
    code_version, calls = _changing_code_version("deadbeef", "deadbeef")
    service.code_version = code_version

    result = service.cutover(
        "steady-identity-cutover",
        rehearsal_report_id="steady-identity-rehearsal",
        backup_id="steady-identity-backup",
        config=SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={},
    )

    report = json.loads((data_root / result.report_path).read_text())
    assert report["status"] == "passed"
    assert report["codeVersion"] == "deadbeef"
    assert calls == ["deadbeef", "deadbeef"]


def test_cutover_parent_swap_after_stage_start_never_touches_external_market(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    external = tmp_path / "external-market"
    external.mkdir()
    external_db = external / "market.duckdb"
    external_db.write_bytes(b"external-must-not-change")

    class SwappingRuntime(FakeRuntime):
        starts = 0

        def start(
            self,
            *,
            root_fd: int,
            market_fd: int,
            lease_fd: int,
            environment: dict[str, str],
            log_path: Path,
            log_fd: int,
        ) -> FakeApi:
            api = super().start(
                root_fd=root_fd,
                market_fd=market_fd,
                lease_fd=lease_fd,
                environment=environment,
                log_path=log_path,
                log_fd=log_fd,
            )
            self.starts += 1
            if self.starts == 2:
                active = data_root / "market-timeseries"
                active.rename(data_root / "market-timeseries.detached")
                active.symlink_to(external, target_is_directory=True)
            return api

    runtime = SwappingRuntime(apis=[FakeApi(), FakeApi()])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
        runtime=runtime,
    )
    service.backup("swap-after-start-backup")
    service.rehearse(
        "swap-after-start-rehearsal",
        SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={},
    )

    with pytest.raises(CutoverSafetyError, match="active market is unchanged"):
        service.cutover(
            "swap-after-start-active",
            rehearsal_report_id="swap-after-start-rehearsal",
            backup_id="swap-after-start-backup",
            config=SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    assert external_db.read_bytes() == b"external-must-not-change"
    assert not (external / "market.duckdb.wal").exists()
    assert (
        data_root / "market-timeseries.detached/market.duckdb"
    ).read_bytes() == b"duckdb-v3"
    assert (
        data_root
        / "operations/market-v4-cutover/staging/swap-after-start-active/root/market-timeseries/market.duckdb"
    ).is_file()


def test_cutover_stage_root_swap_finishes_pinned_smoke_then_rejects_activation(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    external = tmp_path / "external-stage-root"
    (external / "market-timeseries").mkdir(parents=True)
    external_db = external / "market-timeseries/market.duckdb"
    external_db.write_bytes(b"external-stage-must-not-change")
    stage_api = FakeApi()

    class StageRootSwappingRuntime(FakeRuntime):
        starts = 0

        def start(
            self,
            *,
            root_fd: int,
            market_fd: int,
            lease_fd: int,
            environment: dict[str, str],
            log_path: Path,
            log_fd: int,
        ) -> FakeApi:
            api = super().start(
                root_fd=root_fd,
                market_fd=market_fd,
                lease_fd=lease_fd,
                environment=environment,
                log_path=log_path,
                log_fd=log_fd,
            )
            self.starts += 1
            if self.starts == 2:
                root = (
                    data_root
                    / "operations/market-v4-cutover/staging/stage-root-swap-active/root"
                )
                root.rename(root.with_name("root.detached"))
                root.symlink_to(external, target_is_directory=True)
            return api

    runtime = StageRootSwappingRuntime(apis=[FakeApi(), stage_api])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
        runtime=runtime,
    )
    service.backup("stage-root-swap-backup")
    service.rehearse(
        "stage-root-swap-rehearsal",
        SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={},
    )

    with pytest.raises(CutoverSafetyError, match="active market is unchanged"):
        service.cutover(
            "stage-root-swap-active",
            rehearsal_report_id="stage-root-swap-rehearsal",
            backup_id="stage-root-swap-backup",
            config=SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    assert any(path == "/api/db/validate" for _method, path, _payload in stage_api.calls)
    assert external_db.read_bytes() == b"external-stage-must-not-change"
    assert (data_root / "market-timeseries/market.duckdb").read_bytes() == b"duckdb-v3"


def test_cutover_market_leaf_swap_keeps_sync_on_inherited_directory_fd(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    external_market = tmp_path / "external-market-leaf"
    external_market.mkdir()
    external_db = external_market / "market.duckdb"
    external_db.write_bytes(b"external-leaf-must-not-change")

    class DirectoryBoundSyncApi(FakeApi):
        market_fd: int | None = None

        def request(
            self,
            method: str,
            path: str,
            payload: dict[str, object] | None = None,
        ) -> dict[str, object]:
            if method == "POST" and path == "/api/db/sync":
                assert self.market_fd is not None
                marker_fd = os.open(
                    "sync-marker",
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                    0o600,
                    dir_fd=self.market_fd,
                )
                os.write(marker_fd, b"pinned")
                os.close(marker_fd)
            return super().request(method, path, payload)

    stage_api = DirectoryBoundSyncApi()

    class MarketLeafSwappingRuntime(FakeRuntime):
        starts = 0

        def start(
            self,
            *,
            root_fd: int,
            market_fd: int,
            lease_fd: int,
            environment: dict[str, str],
            log_path: Path,
            log_fd: int,
        ) -> FakeApi:
            api = super().start(
                root_fd=root_fd,
                market_fd=market_fd,
                lease_fd=lease_fd,
                environment=environment,
                log_path=log_path,
                log_fd=log_fd,
            )
            self.starts += 1
            if self.starts == 2:
                assert api is stage_api
                stage_api.market_fd = market_fd
                root = (
                    data_root
                    / "operations/market-v4-cutover/staging/market-leaf-swap-active/root"
                )
                market = root / "market-timeseries"
                market.rename(root / "market-timeseries.detached")
                market.symlink_to(external_market, target_is_directory=True)
            return api

    runtime = MarketLeafSwappingRuntime(apis=[FakeApi(), stage_api])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
        runtime=runtime,
    )
    service.backup("market-leaf-swap-backup")
    service.rehearse(
        "market-leaf-swap-rehearsal",
        SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={},
    )

    with pytest.raises(CutoverSafetyError, match="active market is unchanged"):
        service.cutover(
            "market-leaf-swap-active",
            rehearsal_report_id="market-leaf-swap-rehearsal",
            backup_id="market-leaf-swap-backup",
            config=SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    detached_market = (
        data_root
        / "operations/market-v4-cutover/staging/market-leaf-swap-active/root/market-timeseries.detached"
    )
    assert (detached_market / "sync-marker").read_bytes() == b"pinned"
    assert external_db.read_bytes() == b"external-leaf-must-not-change"
    assert not (external_market / "sync-marker").exists()
    assert (data_root / "market-timeseries/market.duckdb").read_bytes() == b"duckdb-v3"


def test_cutover_cross_parent_market_move_confines_non_market_writes(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    external = tmp_path / "external-parent"
    (external / "datasets").mkdir(parents=True)
    external_dataset_marker = external / "datasets/marker"
    external_dataset_marker.write_bytes(b"external-dataset")
    external_portfolio = external / "portfolio.db"
    external_portfolio.write_bytes(b"external-portfolio")

    class RuntimePathWritingApi(FakeApi):
        market_fd: int | None = None
        environment: dict[str, str] | None = None

        def request(
            self,
            method: str,
            path: str,
            payload: dict[str, object] | None = None,
        ) -> dict[str, object]:
            if method == "POST" and path == "/api/db/sync":
                assert self.market_fd is not None
                assert self.environment is not None
                dataset_path = Path(self.environment["DATASET_BASE_PATH"]) / "marker"
                dataset_fd = os.open(
                    dataset_path,
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                    0o600,
                    dir_fd=self.market_fd,
                )
                os.write(dataset_fd, b"pinned-runtime")
                os.close(dataset_fd)
                portfolio_fd = os.open(
                    self.environment["PORTFOLIO_DB_PATH"],
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                    0o600,
                    dir_fd=self.market_fd,
                )
                os.write(portfolio_fd, b"pinned-runtime")
                os.close(portfolio_fd)
            return super().request(method, path, payload)

    stage_api = RuntimePathWritingApi()

    class CrossParentMovingRuntime(FakeRuntime):
        starts = 0

        def start(
            self,
            *,
            root_fd: int,
            market_fd: int,
            lease_fd: int,
            environment: dict[str, str],
            log_path: Path,
            log_fd: int,
        ) -> FakeApi:
            api = super().start(
                root_fd=root_fd,
                market_fd=market_fd,
                lease_fd=lease_fd,
                environment=environment,
                log_path=log_path,
                log_fd=log_fd,
            )
            self.starts += 1
            if self.starts == 2:
                assert api is stage_api
                stage_api.market_fd = market_fd
                stage_api.environment = environment
                stage_root = (
                    data_root
                    / "operations/market-v4-cutover/staging/cross-parent-active/root"
                )
                market = stage_root / "market-timeseries"
                market.rename(external / "moved-stage-market")
                market.mkdir()
                (market / "market.duckdb").write_bytes(b"replacement")
            return api

    runtime = CrossParentMovingRuntime(apis=[FakeApi(), stage_api])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
        runtime=runtime,
    )
    service.backup("cross-parent-backup")
    service.rehearse(
        "cross-parent-rehearsal",
        SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={},
    )

    with pytest.raises(CutoverSafetyError, match="active market is unchanged"):
        service.cutover(
            "cross-parent-active",
            rehearsal_report_id="cross-parent-rehearsal",
            backup_id="cross-parent-backup",
            config=SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    moved_runtime = external / "moved-stage-market/.cutover-runtime-cross-parent-active"
    assert (moved_runtime / "datasets/marker").read_bytes() == b"pinned-runtime"
    assert (moved_runtime / "portfolio.db").read_bytes() == b"pinned-runtime"
    assert external_dataset_marker.read_bytes() == b"external-dataset"
    assert external_portfolio.read_bytes() == b"external-portfolio"
    assert (data_root / "market-timeseries/market.duckdb").read_bytes() == b"duckdb-v3"


def test_cutover_report_write_failure_is_inside_restore_boundary(tmp_path: Path) -> None:
    data_root = _market_root(tmp_path)
    runtime = FakeRuntime(apis=[FakeApi(), FakeApi(), FakeApi()])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
        runtime=runtime,
    )
    service.backup("backup-001")
    service.rehearse(
        "rehearsal-001",
        SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={},
    )
    original_write = service._write_report

    def fail_active_report(report_id: str, report: dict[str, object]) -> Path:
        if report_id == "active-write-fail":
            raise OSError("injected fsync failure")
        return original_write(report_id, report)

    service._write_report = fail_active_report  # type: ignore[method-assign]
    with pytest.raises(CutoverSafetyError, match="restored backup"):
        service.cutover(
            "active-write-fail",
            rehearsal_report_id="rehearsal-001",
            backup_id="backup-001",
            config=SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )
    assert (data_root / "market-timeseries/market.duckdb").read_bytes() == b"duckdb-v3"


def test_cutover_defers_restore_when_active_server_stop_is_unproven(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)

    class UnjoinedRuntime(FakeRuntime):
        def stop(self, _api: FakeApi) -> None:
            self.stop_calls += 1
            if self.stop_calls >= 3:
                raise market_v4_cutover.RuntimeStopError(
                    "injected unjoined process",
                    process_joined=False,
                )

    runtime = UnjoinedRuntime(apis=[FakeApi(), FakeApi(), FakeApi()])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
        runtime=runtime,
    )
    service.backup("stop-deferred-backup")
    service.rehearse(
        "stop-deferred-rehearsal",
        SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={},
    )
    restore_called = False

    def forbidden_restore(_backup_id: str) -> None:
        nonlocal restore_called
        restore_called = True
        raise AssertionError("restore must not run while the server may be alive")

    monkeypatch.setattr(service, "restore", forbidden_restore)
    with pytest.raises(CutoverSafetyError, match="restore is deferred"):
        service.cutover(
            "stop-deferred-active",
            rehearsal_report_id="stop-deferred-rehearsal",
            backup_id="stop-deferred-backup",
            config=SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    assert restore_called is False
    report = json.loads(
        (
            data_root
            / "operations/market-v4-cutover/reports/stop-deferred-active/report.json"
        ).read_text()
    )
    assert report["status"] == "stop_failed_restore_deferred"
    assert (
        data_root
        / "operations/market-v4-cutover/backups/stop-deferred-backup"
    ).is_dir()


def test_cutover_unjoined_stop_keeps_primary_and_secondary_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)

    class OriginalActiveSmokeError(RuntimeError):
        pass

    class UnjoinedCleanupRuntime(FakeRuntime):
        def stop(self, api: FakeApi) -> None:
            self.stop_calls += 1
            if self.stop_calls == 3:
                raise market_v4_cutover.RuntimeStopError(
                    "injected unjoined cleanup",
                    process_joined=False,
                )

    runtime = UnjoinedCleanupRuntime(apis=[FakeApi(), FakeApi(), FakeApi()])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
        runtime=runtime,
    )
    service.backup("secondary-stop-backup")
    service.rehearse(
        "secondary-stop-rehearsal",
        SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={},
    )
    original_smoke = service.smoke
    smoke_calls = 0

    def fail_active_smoke(*args: object, **kwargs: object) -> object:
        nonlocal smoke_calls
        smoke_calls += 1
        if smoke_calls == 2:
            raise OriginalActiveSmokeError("injected active smoke failure")
        return original_smoke(*args, **kwargs)

    monkeypatch.setattr(service, "smoke", fail_active_smoke)

    with pytest.raises(CutoverSafetyError, match="restore is deferred"):
        service.cutover(
            "secondary-stop-active",
            rehearsal_report_id="secondary-stop-rehearsal",
            backup_id="secondary-stop-backup",
            config=SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    report = json.loads(
        (
            data_root
            / "operations/market-v4-cutover/reports/secondary-stop-active/report.json"
        ).read_text()
    )
    assert report["status"] == "stop_failed_restore_deferred"
    assert report["errorType"] == "OriginalActiveSmokeError"
    assert report["stopErrorType"] == "RuntimeStopError"
    assert report["serverProcessJoined"] is False


def test_cutover_unjoined_active_server_transfers_active_lease(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)

    class LeaseHoldingRuntime(FakeRuntime):
        inherited_by_api: dict[int, int] = {}
        retained_unjoined_fd = -1

        def start(self, **kwargs: object) -> FakeApi:
            api = super().start(**kwargs)  # type: ignore[arg-type]
            self.inherited_by_api[id(api)] = os.dup(int(kwargs["lease_fd"]))
            return api

        def stop(self, api: FakeApi) -> None:
            self.stop_calls += 1
            inherited_fd = self.inherited_by_api.pop(id(api))
            if self.stop_calls == 3:
                self.retained_unjoined_fd = inherited_fd
                raise market_v4_cutover.RuntimeStopError(
                    "injected unjoined active server",
                    process_joined=False,
                )
            os.close(inherited_fd)

    runtime = LeaseHoldingRuntime(apis=[FakeApi(), FakeApi(), FakeApi()])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
        runtime=runtime,
    )
    service.backup("active-lease-transfer-backup")
    service.rehearse(
        "active-lease-transfer-rehearsal",
        SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={},
    )

    with pytest.raises(CutoverSafetyError, match="restore is deferred"):
        service.cutover(
            "active-lease-transfer-cutover",
            rehearsal_report_id="active-lease-transfer-rehearsal",
            backup_id="active-lease-transfer-backup",
            config=SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    try:
        with pytest.raises(CutoverSafetyError, match="operation lease"):
            market_v4_cutover.MarketOperationLease.acquire(
                data_root,
                exclusive=False,
            )
    finally:
        os.close(runtime.retained_unjoined_fd)

    with market_v4_cutover.MarketOperationLease.acquire(data_root, exclusive=True):
        pass


def test_cutover_unjoined_staging_server_transfers_staging_lease(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)

    class LeaseHoldingRuntime(FakeRuntime):
        inherited_by_api: dict[int, int] = {}
        retained_unjoined_fd = -1

        def start(self, **kwargs: object) -> FakeApi:
            api = super().start(**kwargs)  # type: ignore[arg-type]
            self.inherited_by_api[id(api)] = os.dup(int(kwargs["lease_fd"]))
            return api

        def stop(self, api: FakeApi) -> None:
            self.stop_calls += 1
            inherited_fd = self.inherited_by_api[id(api)]
            if self.stop_calls >= 2:
                self.retained_unjoined_fd = inherited_fd
                raise market_v4_cutover.RuntimeStopError(
                    "injected unjoined staging server",
                    process_joined=False,
                )
            os.close(inherited_fd)
            del self.inherited_by_api[id(api)]

    runtime = LeaseHoldingRuntime(apis=[FakeApi(), FakeApi()])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
        runtime=runtime,
    )
    service.backup("staging-lease-transfer-backup")
    service.rehearse(
        "staging-lease-transfer-rehearsal",
        SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={},
    )

    with pytest.raises(CutoverSafetyError, match="restore is deferred"):
        service.cutover(
            "staging-lease-transfer-cutover",
            rehearsal_report_id="staging-lease-transfer-rehearsal",
            backup_id="staging-lease-transfer-backup",
            config=SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    staging_root = (
        data_root
        / "operations/market-v4-cutover/staging/staging-lease-transfer-cutover/root"
    )
    try:
        with pytest.raises(CutoverSafetyError, match="operation lease"):
            market_v4_cutover.MarketOperationLease.acquire(
                staging_root,
                exclusive=False,
            )
    finally:
        os.close(runtime.retained_unjoined_fd)

    with market_v4_cutover.MarketOperationLease.acquire(
        staging_root,
        exclusive=True,
    ):
        pass
    with market_v4_cutover.MarketOperationLease.acquire(data_root, exclusive=True):
        pass


def test_cutover_unjoined_staging_worker_transfers_staging_lease(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)

    class GuardHoldingDuckDb(FakeDuckDb):
        fail_inspect = False
        retained_guard_fd = -1

        def inspect(
            self,
            directory_fd: int,
            filename: str,
            *,
            guard_lease_fd: int,
        ) -> MarketSourceMetadata:
            if self.fail_inspect:
                self.retained_guard_fd = os.dup(guard_lease_fd)
                raise market_v4_cutover.WorkerShutdownError(
                    "injected unjoined staging worker",
                    process_joined=False,
                )
            return super().inspect(
                directory_fd,
                filename,
                guard_lease_fd=guard_lease_fd,
            )

    duckdb = GuardHoldingDuckDb(
        MarketSourceMetadata(4, "local_projection_v2_event_time")
    )
    runtime = FakeRuntime(apis=[FakeApi(), FakeApi()])
    service = _service(data_root, duckdb=duckdb, runtime=runtime)
    service.backup("staging-worker-transfer-backup")
    service.rehearse(
        "staging-worker-transfer-rehearsal",
        SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={},
    )
    duckdb.fail_inspect = True

    with pytest.raises(CutoverSafetyError, match="restore is deferred"):
        service.cutover(
            "staging-worker-transfer-cutover",
            rehearsal_report_id="staging-worker-transfer-rehearsal",
            backup_id="staging-worker-transfer-backup",
            config=SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    staging_root = (
        data_root
        / "operations/market-v4-cutover/staging/staging-worker-transfer-cutover/root"
    )
    try:
        with pytest.raises(CutoverSafetyError, match="operation lease"):
            market_v4_cutover.MarketOperationLease.acquire(
                staging_root,
                exclusive=False,
            )
    finally:
        os.close(duckdb.retained_guard_fd)

    with market_v4_cutover.MarketOperationLease.acquire(
        staging_root,
        exclusive=True,
    ):
        pass


def test_cutover_restore_failure_keeps_primary_and_restore_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    runtime = FakeRuntime(apis=[FakeApi(), FakeApi(), FakeApi(invalid_lineage=True)])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
        runtime=runtime,
    )
    service.backup("secondary-restore-backup")
    service.rehearse(
        "secondary-restore-rehearsal",
        SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={},
    )

    class InjectedRestoreError(OSError):
        pass

    def fail_restore(_backup_id: str) -> None:
        raise InjectedRestoreError("injected restore failure")

    monkeypatch.setattr(service, "restore", fail_restore)

    with pytest.raises(CutoverSafetyError, match="explicit restore also failed"):
        service.cutover(
            "secondary-restore-active",
            rehearsal_report_id="secondary-restore-rehearsal",
            backup_id="secondary-restore-backup",
            config=SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    report = json.loads(
        (
            data_root
            / "operations/market-v4-cutover/reports/secondary-restore-active/report.json"
        ).read_text()
    )
    assert report["status"] == "restore_failed"
    assert report["errorType"] == "CutoverSafetyError"
    assert report["restoreErrorType"] == "InjectedRestoreError"


def test_cutover_defers_restore_when_active_start_fails_before_api_unjoined(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)

    class ActiveStartFailureRuntime(FakeRuntime):
        starts = 0

        def start(self, **kwargs: object) -> FakeApi:
            self.starts += 1
            if self.starts == 3:
                raise market_v4_cutover.RuntimeStopError(
                    "active startup child remains alive",
                    process_joined=False,
                )
            return super().start(**kwargs)  # type: ignore[arg-type]

    runtime = ActiveStartFailureRuntime(apis=[FakeApi(), FakeApi()])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
        runtime=runtime,
    )
    service.backup("start-unjoined-backup")
    service.rehearse(
        "start-unjoined-rehearsal",
        SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={},
    )
    restore_called = False

    def forbidden_restore(_backup_id: str) -> None:
        nonlocal restore_called
        restore_called = True
        raise AssertionError("restore must not run while startup child may be alive")

    monkeypatch.setattr(service, "restore", forbidden_restore)
    with pytest.raises(CutoverSafetyError, match="restore is deferred"):
        service.cutover(
            "start-unjoined-active",
            rehearsal_report_id="start-unjoined-rehearsal",
            backup_id="start-unjoined-backup",
            config=SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    assert restore_called is False
    report = json.loads(
        (
            data_root
            / "operations/market-v4-cutover/reports/start-unjoined-active/report.json"
        ).read_text()
    )
    assert report["status"] == "stop_failed_restore_deferred"
    assert report["errorType"] == "RuntimeStopError"


def test_cutover_restores_when_active_start_failure_proves_child_joined(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)

    class JoinedActiveStartFailureRuntime(FakeRuntime):
        starts = 0

        def start(self, **kwargs: object) -> FakeApi:
            self.starts += 1
            if self.starts == 3:
                raise market_v4_cutover.RuntimeStopError(
                    "active startup child joined",
                    process_joined=True,
                )
            return super().start(**kwargs)  # type: ignore[arg-type]

    runtime = JoinedActiveStartFailureRuntime(apis=[FakeApi(), FakeApi()])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
        runtime=runtime,
    )
    original = (data_root / "market-timeseries/market.duckdb").read_bytes()
    service.backup("start-joined-backup")
    service.rehearse(
        "start-joined-rehearsal",
        SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={},
    )

    with pytest.raises(CutoverSafetyError, match="restored backup"):
        service.cutover(
            "start-joined-active",
            rehearsal_report_id="start-joined-rehearsal",
            backup_id="start-joined-backup",
            config=SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    assert (data_root / "market-timeseries/market.duckdb").read_bytes() == original


def test_cutover_defers_restore_when_duckdb_worker_join_is_unproven(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    runtime = FakeRuntime(apis=[FakeApi(), FakeApi(), FakeApi()])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
        runtime=runtime,
    )
    service.backup("worker-stop-deferred-backup")
    service.rehearse(
        "worker-stop-deferred-rehearsal",
        SmokeConfig("7203", "production/smoke", "primeMarket"),
        inherited_environment={},
    )
    original_smoke = service.smoke
    smoke_calls = 0

    def fail_active_smoke(*args: object, **kwargs: object) -> object:
        nonlocal smoke_calls
        smoke_calls += 1
        if smoke_calls == 2:
            try:
                raise RuntimeError("primary active smoke failure")
            except RuntimeError as primary:
                raise market_v4_cutover.WorkerShutdownError(
                    "injected unjoined DuckDB worker",
                    process_joined=False,
                ) from primary
        return original_smoke(*args, **kwargs)

    monkeypatch.setattr(service, "smoke", fail_active_smoke)
    restore_called = False

    def forbidden_restore(_backup_id: str) -> None:
        nonlocal restore_called
        restore_called = True
        raise AssertionError("restore must not run while a DuckDB worker may be alive")

    monkeypatch.setattr(service, "restore", forbidden_restore)
    with pytest.raises(CutoverSafetyError, match="restore is deferred"):
        service.cutover(
            "worker-stop-deferred-active",
            rehearsal_report_id="worker-stop-deferred-rehearsal",
            backup_id="worker-stop-deferred-backup",
            config=SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    assert restore_called is False
    report = json.loads(
        (
            data_root
            / "operations/market-v4-cutover/reports/worker-stop-deferred-active/report.json"
        ).read_text()
    )
    assert report["status"] == "stop_failed_restore_deferred"
    assert report["errorType"] == "WorkerShutdownError"
    assert report["workerProcessJoined"] is False


def test_restore_rolls_quarantine_back_if_stage_activation_fails(tmp_path: Path) -> None:
    data_root = _market_root(tmp_path)
    service = _service(data_root)
    service.backup("before")
    active = data_root / "market-timeseries"
    (active / "market.duckdb").write_bytes(b"failed-v4")
    calls = 0

    def fail_stage_once(source: Path, target: Path) -> None:
        nonlocal calls
        calls += 1
        if source.name.startswith("market-timeseries.restore-"):
            raise OSError("injected activation failure")

    service._rename_at_hook = fail_stage_once
    with pytest.raises(CutoverSafetyError, match="activation"):
        service.restore("before")

    assert (active / "market.duckdb").read_bytes() == b"failed-v4"
    assert calls >= 3


def test_restore_can_repeat_same_backup_without_quarantine_collision(tmp_path: Path) -> None:
    data_root = _market_root(tmp_path)
    service = _service(data_root)
    service.backup("before")

    first = service.restore("before")
    second = service.restore("before")

    assert first.quarantine_path != second.quarantine_path
    assert first.quarantine_path and (data_root / first.quarantine_path).exists()
    assert second.quarantine_path and (data_root / second.quarantine_path).exists()


def test_unknown_or_dirty_code_identity_fails_before_backup_mutation(tmp_path: Path) -> None:
    data_root = _market_root(tmp_path)
    for identity in ("unknown", "deadbeef-dirty"):
        service = MarketV4CutoverService(
            data_root,
            duckdb=FakeDuckDb(),
            runtime=FakeRuntime(),
            disk_free_bytes=lambda _path: 10_000_000,
            now=lambda: "2026-07-15T12:00:00Z",
            code_version=lambda identity=identity: identity,
        )
        with pytest.raises(CutoverSafetyError, match="code identity"):
            service.backup(f"backup-{identity}")


def test_root_fingerprint_binds_filesystem_and_config_strategy_content(tmp_path: Path) -> None:
    data_root = _market_root(tmp_path)
    (data_root / "config").mkdir()
    (data_root / "config/default.yaml").write_text("mode: one\n")
    (data_root / "strategies/production").mkdir(parents=True)
    strategy = data_root / "strategies/production/smoke.yaml"
    strategy.write_text("value: one\n")
    service = _service(data_root)
    first = service.root_fingerprint(data_root)
    strategy.write_text("value: two\n")
    assert service.root_fingerprint(data_root) != first

    copied = tmp_path / "copied-root"
    shutil.copytree(data_root, copied)
    assert service.root_fingerprint(copied) != service.root_fingerprint(data_root)


def test_default_runtime_adapter_is_available() -> None:
    assert hasattr(market_v4_cutover, "DefaultDuckDbAdapter")
    assert hasattr(market_v4_cutover, "SubprocessRuntimeAdapter")


def test_owned_server_argv_uses_uvicorn_without_cli_port_kill_path() -> None:
    argv = market_v4_cutover.SubprocessRuntimeAdapter.server_argv(
        41234,
        market_fd=8,
    )
    assert argv.count("--port") == 1
    assert argv[-2:] == ["--port", "41234"]
    assert "bt" not in argv
    assert "8" in argv


def test_owned_server_passes_root_and_lease_fds_to_bootstrap(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    captured: dict[str, object] = {}

    class ExitedProcess:
        def poll(self) -> int:
            return 1

        def wait(self, timeout: float) -> int:
            del timeout
            return 1

    def fake_popen(argv: list[str], **kwargs: object) -> ExitedProcess:
        captured["argv"] = argv
        captured.update(kwargs)
        return ExitedProcess()

    root = tmp_path / "stage-root"
    root.mkdir()
    (root / "market-timeseries").mkdir()
    lock = root / ".market-timeseries.operation.lock"
    root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    market_fd = os.open(root / "market-timeseries", os.O_RDONLY | os.O_DIRECTORY)
    lease_fd = os.open(lock, os.O_CREAT | os.O_RDWR, 0o600)
    retained_lock = tmp_path / "retained.operation.lock"
    retained_lease_fd = os.open(retained_lock, os.O_CREAT | os.O_RDWR, 0o600)
    log_fd = os.open(tmp_path / "server.log", os.O_CREAT | os.O_RDWR, 0o600)
    try:
        monkeypatch.setattr(market_v4_cutover.subprocess, "Popen", fake_popen)
        runtime = market_v4_cutover.SubprocessRuntimeAdapter()
        with pytest.raises(CutoverSafetyError, match="exited during startup"):
            runtime.start(
                root_fd=root_fd,
                market_fd=market_fd,
                lease_fd=lease_fd,
                retained_lease_fd=retained_lease_fd,
                environment={},
                log_path=tmp_path / "server.log",
                log_fd=log_fd,
            )
    finally:
        os.close(log_fd)
        os.close(retained_lease_fd)
        os.close(lease_fd)
        os.close(market_fd)
        os.close(root_fd)

    assert set(captured["pass_fds"]) == {
        root_fd,
        market_fd,
        lease_fd,
        retained_lease_fd,
    }
    assert captured["env"]["TRADING25_RETAINED_MARKET_OPERATION_LOCK_FD"] == str(
        retained_lease_fd
    )
    assert str(market_fd) in captured["argv"]


def test_owned_server_real_bootstrap_runs_from_inherited_root_fd(
    tmp_path: Path,
) -> None:
    root = tmp_path / "stage-root"
    for relative in ("market-timeseries", "datasets", "config", "strategies", "backtest"):
        (root / relative).mkdir(parents=True, exist_ok=True)
    (root / "config/default.yaml").write_text("default: {}\n")
    runtime_name = ".cutover-runtime-bootstrap"
    for relative in ("datasets", "config", "strategies", "backtest"):
        (root / "market-timeseries" / runtime_name / relative).mkdir(
            parents=True,
            exist_ok=True,
        )
    (root / "market-timeseries" / runtime_name / "config/default.yaml").write_text(
        "default: {}\n"
    )
    log_path = tmp_path / "real-server.log"
    log_fd = os.open(log_path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
    runtime = market_v4_cutover.SubprocessRuntimeAdapter(
        startup_timeout_seconds=20,
    )
    with market_v4_cutover.MarketOperationLease.acquire(
        root,
        exclusive=True,
    ) as lease:
        market_fd = os.open(
            root / "market-timeseries",
            os.O_RDONLY | os.O_DIRECTORY,
        )
        environment = dict(os.environ)
        environment.update(
            {
                "XDG_DATA_HOME": f"{runtime_name}/xdg-data-home",
                "TRADING25_DATA_DIR": runtime_name,
                "MARKET_TIMESERIES_DIR": ".",
                "MARKET_DB_PATH": "market.duckdb",
                "DATASET_BASE_PATH": f"{runtime_name}/datasets",
                "PORTFOLIO_DB_PATH": f"{runtime_name}/portfolio.db",
                "TRADING25_STRATEGIES_DIR": f"{runtime_name}/strategies",
                "TRADING25_BACKTEST_DIR": f"{runtime_name}/backtest",
                "TRADING25_DEFAULT_CONFIG_PATH": f"{runtime_name}/config/default.yaml",
            }
        )
        try:
            api = runtime.start(
                root_fd=lease.root_fd,
                market_fd=market_fd,
                lease_fd=lease.fd,
                environment=environment,
                log_path=log_path,
                log_fd=log_fd,
            )
            assert api.request("GET", "/api/health")["status"] == "healthy"
            runtime.stop(api)
        finally:
            os.close(market_fd)
            os.close(log_fd)


def test_owned_server_log_redacts_secrets_and_local_paths(tmp_path: Path) -> None:
    log = tmp_path / "server.log"
    log.write_text("key=super-secret db=/Users/me/active/market.duckdb\n")
    log_fd = os.open(log, os.O_RDWR)
    try:
        market_v4_cutover.SubprocessRuntimeAdapter.redact_log_fd(
            log_fd,
            {
                "JQUANTS_API_KEY": "super-secret",
                "MARKET_DB_PATH": "/Users/me/active/market.duckdb",
            },
        )
    finally:
        os.close(log_fd)
    retained = log.read_text()
    assert "super-secret" not in retained
    assert "/Users/me" not in retained
    assert "<redacted-secret>" in retained
    assert log.stat().st_mode & 0o777 == 0o600


def test_owned_server_log_does_not_redact_relative_dot_path_as_punctuation(
    tmp_path: Path,
) -> None:
    log = tmp_path / "server.log"
    log.write_text("127.0.0.1 loaded module.py; key=super-secret\n")
    log_fd = os.open(log, os.O_RDWR)
    try:
        market_v4_cutover.SubprocessRuntimeAdapter.redact_log_fd(
            log_fd,
            {
                "JQUANTS_API_KEY": "super-secret",
                "MARKET_TIMESERIES_DIR": ".",
                "MARKET_DB_PATH": "market.duckdb",
            },
        )
    finally:
        os.close(log_fd)

    retained = log.read_text()
    assert "127.0.0.1 loaded module.py" in retained
    assert "super-secret" not in retained
    assert "<redacted-secret>" in retained


def test_fixed_port_health_is_not_probed_after_root_scoped_lease(
    monkeypatch, tmp_path: Path
) -> None:
    def must_not_probe(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("fixed-port health probe must not run")

    monkeypatch.setattr(market_v4_cutover, "urlopen", must_not_probe)
    runtime = market_v4_cutover.SubprocessRuntimeAdapter()
    runtime.assert_quiescent(tmp_path)


def test_http_adapter_tracks_exact_job_id_field_for_each_create_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payloads = {
        "/api/db/sync": {"jobId": "sync-1"},
        "/api/db/adjusted-metrics/materialize": {"jobId": "materialize-1"},
        "/api/analytics/screening/jobs": _screening_job_response("pending"),
        "/api/dataset": {"jobId": "dataset-1"},
    }

    class JsonResponse:
        def __init__(self, payload: dict[str, object]) -> None:
            self.payload = payload

        def __enter__(self) -> JsonResponse:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def read(self) -> bytes:
            return json.dumps(self.payload).encode()

    def fake_urlopen(request: object, *, timeout: float) -> JsonResponse:
        assert timeout == 30.0
        path = getattr(request, "selector")
        return JsonResponse(payloads[path])

    monkeypatch.setattr(market_v4_cutover, "urlopen", fake_urlopen)
    api = market_v4_cutover.HttpApiAdapter("http://unused")

    for path in payloads:
        api.request("POST", path, {})

    assert api.owned_jobs == {
        "sync": "sync-1",
        "materialize": "materialize-1",
        "screening": "screen-1",
        "dataset": "dataset-1",
    }


def test_http_adapter_does_not_accept_camel_case_screening_job_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class JsonResponse:
        def __enter__(self) -> JsonResponse:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def read(self) -> bytes:
            return b'{"jobId":"legacy-screen"}'

    monkeypatch.setattr(
        market_v4_cutover,
        "urlopen",
        lambda *_args, **_kwargs: JsonResponse(),
    )
    api = market_v4_cutover.HttpApiAdapter("http://unused")

    api.request("POST", "/api/analytics/screening/jobs", {})

    assert api.owned_jobs == {}


def test_operation_lease_blocks_unrecognized_server_and_allows_owner(tmp_path: Path) -> None:
    data_root = _market_root(tmp_path)
    lease_cls = getattr(market_v4_cutover, "MarketOperationLease")

    with lease_cls.acquire(data_root, exclusive=True) as lease:
        with pytest.raises(CutoverSafetyError, match="operation lease"):
            lease_cls.acquire(data_root, exclusive=False)
        inherited = lease_cls.adopt_inherited(data_root, os.dup(lease.fd))
        inherited.release()
        with pytest.raises(CutoverSafetyError, match="operation lease"):
            lease_cls.acquire(data_root, exclusive=False)

    with lease_cls.acquire(data_root, exclusive=False):
        pass


def test_operation_lease_transfer_holds_lock_until_inherited_fd_closes(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    lease = market_v4_cutover.MarketOperationLease.acquire(data_root, exclusive=True)
    inherited_server_fd = os.dup(lease.fd)
    inherited_worker_fd = os.dup(lease.fd)
    lease.unlock_on_release = False
    lease.release()
    try:
        with pytest.raises(CutoverSafetyError, match="operation lease"):
            market_v4_cutover.MarketOperationLease.acquire(
                data_root,
                exclusive=False,
            )
        with pytest.raises(CutoverSafetyError, match="operation lease"):
            market_v4_cutover.MarketOperationLease.acquire(
                data_root,
                exclusive=True,
            )
        os.close(inherited_server_fd)
        inherited_server_fd = -1
        with pytest.raises(CutoverSafetyError, match="operation lease"):
            market_v4_cutover.MarketOperationLease.acquire(
                data_root,
                exclusive=True,
            )
    finally:
        if inherited_server_fd >= 0:
            os.close(inherited_server_fd)
        os.close(inherited_worker_fd)

    with market_v4_cutover.MarketOperationLease.acquire(data_root, exclusive=True):
        pass


def test_rehearsal_unjoined_server_transfers_lease_to_inherited_fd(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)

    class FailingApi(FakeApi):
        def request(
            self,
            method: str,
            path: str,
            payload: dict[str, object] | None = None,
        ) -> dict[str, object]:
            del method, path, payload
            raise RuntimeError("injected rebuild failure")

    class LeaseHoldingRuntime(FakeRuntime):
        inherited_lease_fd = -1

        def start(self, **kwargs: object) -> FakeApi:
            api = super().start(**kwargs)  # type: ignore[arg-type]
            self.inherited_lease_fd = os.dup(int(kwargs["lease_fd"]))
            return api

        def stop(self, _api: FakeApi) -> None:
            self.stop_calls += 1
            raise market_v4_cutover.RuntimeStopError(
                "injected unjoined server",
                process_joined=False,
            )

    runtime = LeaseHoldingRuntime(apis=[FailingApi()])
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
        runtime=runtime,
    )

    with pytest.raises(CutoverSafetyError, match="rehearsal failed"):
        service.rehearse(
            "rehearsal-lease-transfer",
            SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    try:
        with pytest.raises(CutoverSafetyError, match="operation lease"):
            market_v4_cutover.MarketOperationLease.acquire(
                data_root
                / "operations/market-v4-cutover/rehearsals/rehearsal-lease-transfer/root",
                exclusive=False,
            )
    finally:
        os.close(runtime.inherited_lease_fd)

    with market_v4_cutover.MarketOperationLease.acquire(
        data_root
        / "operations/market-v4-cutover/rehearsals/rehearsal-lease-transfer/root",
        exclusive=True,
    ):
        pass


def test_rehearsal_unjoined_worker_transfers_lease_to_worker_guard_fd(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)

    class GuardHoldingDuckDb(FakeDuckDb):
        retained_guard_fd = -1

        def inspect(
            self,
            directory_fd: int,
            filename: str,
            *,
            guard_lease_fd: int,
        ) -> MarketSourceMetadata:
            del directory_fd, filename
            self.retained_guard_fd = os.dup(guard_lease_fd)
            raise market_v4_cutover.WorkerShutdownError(
                "injected unjoined rehearsal worker",
                process_joined=False,
            )

    duckdb = GuardHoldingDuckDb(
        MarketSourceMetadata(4, "local_projection_v2_event_time")
    )
    service = _service(
        data_root,
        duckdb=duckdb,
        runtime=FakeRuntime(apis=[FakeApi()]),
    )

    with pytest.raises(CutoverSafetyError, match="rehearsal failed"):
        service.rehearse(
            "rehearsal-worker-transfer",
            SmokeConfig("7203", "production/smoke", "primeMarket"),
            inherited_environment={},
        )

    rehearsal_root = (
        data_root
        / "operations/market-v4-cutover/rehearsals/rehearsal-worker-transfer/root"
    )
    report = json.loads(
        (
            data_root
            / "operations/market-v4-cutover/reports/rehearsal-worker-transfer/report.json"
        ).read_text()
    )
    assert report["status"] == "stop_failed_cleanup_deferred"
    assert report["workerProcessJoined"] is False
    try:
        with pytest.raises(CutoverSafetyError, match="operation lease"):
            market_v4_cutover.MarketOperationLease.acquire(
                rehearsal_root,
                exclusive=False,
            )
    finally:
        os.close(duckdb.retained_guard_fd)

    with market_v4_cutover.MarketOperationLease.acquire(
        rehearsal_root,
        exclusive=True,
    ):
        pass


def test_duckdb_worker_inherits_guard_lease_fd_until_process_exit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = _market_root(tmp_path)
    directory_fd = os.open(data_root / "market-timeseries", os.O_RDONLY | os.O_DIRECTORY)
    lease = market_v4_cutover.MarketOperationLease.acquire(data_root, exclusive=True)
    monkeypatch.setattr(
        market_v4_cutover.DefaultDuckDbAdapter,
        "_worker_argv",
        staticmethod(
            lambda _operation, _directory_fd, _guard_lease_fd, _filename: [
                market_v4_cutover.sys.executable,
                "-c",
                "import time; time.sleep(60)",
            ]
        ),
    )
    process = None
    try:
        process = market_v4_cutover.DefaultDuckDbAdapter._start_worker(
            "inspect",
            directory_fd,
            "market.duckdb",
            guard_lease_fd=lease.fd,
        )
        lease.unlock_on_release = False
        lease.release()
        with pytest.raises(CutoverSafetyError, match="operation lease"):
            market_v4_cutover.MarketOperationLease.acquire(data_root, exclusive=False)
    finally:
        if process is not None:
            process.terminate()
            process.wait(timeout=5)
        if lease.fd >= 0:
            lease.release()
        os.close(directory_fd)

    with market_v4_cutover.MarketOperationLease.acquire(data_root, exclusive=True):
        pass


def test_inherited_unlocked_matching_fd_establishes_exclusive_lease(tmp_path: Path) -> None:
    data_root = _market_root(tmp_path)
    lock_path = data_root / ".market-timeseries.operation.lock"
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    inherited = market_v4_cutover.MarketOperationLease.adopt_inherited(data_root, fd)
    try:
        with pytest.raises(CutoverSafetyError, match="operation lease"):
            market_v4_cutover.MarketOperationLease.acquire(data_root, exclusive=False)
    finally:
        inherited.release()


def test_inherited_matching_fd_rejects_competing_shared_lease(tmp_path: Path) -> None:
    data_root = _market_root(tmp_path)
    shared = market_v4_cutover.MarketOperationLease.acquire(data_root, exclusive=False)
    unlocked_fd = os.open(shared.path, os.O_RDWR)
    try:
        with pytest.raises(CutoverSafetyError, match="exclusive|operation lease"):
            market_v4_cutover.MarketOperationLease.adopt_inherited(data_root, unlocked_fd)
    finally:
        os.close(unlocked_fd)
        shared.release()


def test_inherited_root_fd_avoids_reopening_swapped_lexical_root(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    parent = market_v4_cutover.MarketOperationLease.acquire(
        data_root,
        exclusive=True,
    )
    inherited_lock_fd = os.dup(parent.fd)
    inherited_root_fd = os.dup(parent.root_fd)
    detached = tmp_path / "data-root-detached"
    data_root.rename(detached)
    external = tmp_path / "external-root"
    external.mkdir()
    data_root.symlink_to(external, target_is_directory=True)
    try:
        adopted = market_v4_cutover.MarketOperationLease.adopt_inherited(
            data_root,
            inherited_lock_fd,
            root_fd=inherited_root_fd,
        )
        adopted.release()
    finally:
        parent.release()

    assert list(external.iterdir()) == []


def test_default_duckdb_adapter_checkpoints_and_reads_raw_metadata(
    tmp_path: Path,
    guard_lease_fd: int,
) -> None:
    import duckdb

    db_path = tmp_path / "market.duckdb"
    connection = duckdb.connect(str(db_path))
    connection.execute("CREATE TABLE market_schema_version(version INTEGER)")
    connection.execute("INSERT INTO market_schema_version VALUES (4)")
    connection.execute("CREATE TABLE sync_metadata(key VARCHAR, value VARCHAR)")
    connection.execute(
        "INSERT INTO sync_metadata VALUES "
        "('stock_price_adjustment_mode', 'local_projection_v2_event_time')"
    )
    connection.close()

    adapter = market_v4_cutover.DefaultDuckDbAdapter()
    directory_fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        assert adapter.checkpoint_exclusive(
            directory_fd,
            "market.duckdb",
            guard_lease_fd=guard_lease_fd,
        ) == MarketSourceMetadata(4, "local_projection_v2_event_time", False)
        assert adapter.inspect(
            directory_fd,
            "market.duckdb",
            guard_lease_fd=guard_lease_fd,
        ) == MarketSourceMetadata(4, "local_projection_v2_event_time", False)
    finally:
        os.close(directory_fd)


def test_rehearse_retained_rejects_real_duckdb_inexact_lineage_before_runtime(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    service, retained_root, config = _retained_source(data_root)
    db_path = retained_root / "market-timeseries/market.duckdb"
    db_path.unlink()
    market_db = MarketDb(str(db_path))
    try:
        market_db._execute(
            """
            INSERT INTO statements (
                code, disclosed_date, earnings_per_share, type_of_current_period
            ) VALUES ('7203', '2024-05-10', 100, 'FY')
            """
        )
        market_db._execute(
            """
            INSERT INTO stock_adjustment_bases (
                code, basis_id, valid_from, valid_to_exclusive,
                adjustment_through_date, source_fingerprint,
                materialized_through_date, status
            ) VALUES (
                '7203', 'ready-7203', '2024-01-01', NULL,
                '2024-12-30', 'fp', '2024-12-30', 'ready'
            )
            """
        )
        market_db._execute(
            """
            INSERT INTO statement_metrics_adjusted (
                code, disclosed_date, period_end, period_type, price_basis_date,
                raw_eps, basis_version
            ) VALUES (
                '7203', '2024-05-10', '2024-05-10', 'FY', '2024-12-30',
                999, 'ready-7203'
            )
            """
        )
        market_db._execute(
            """
            INSERT INTO stock_data_raw (
                code, date, open, high, low, close, volume, adjustment_factor
            ) VALUES ('7203', '2024-06-03', 100, 110, 90, 105, 1000, 1)
            """
        )
        market_db._execute(
            """
            INSERT INTO stock_adjustment_basis_segments (
                code, basis_id, source_date_from, source_date_to_exclusive,
                cumulative_factor
            ) VALUES ('7203', 'ready-7203', '2024-01-01', NULL, 1)
            """
        )
        market_db._execute(
            """
            INSERT INTO daily_valuation (
                code, date, close, price_basis_date, basis_version
            ) VALUES ('7203', '2024-06-03', 105, '2024-12-30', 'ready-7203')
            """
        )
    finally:
        market_db.close()

    runtime = FakeRuntime(apis=[FakeApi()])
    service.duckdb = market_v4_cutover.DefaultDuckDbAdapter()
    service.runtime = runtime

    with pytest.raises(CutoverSafetyError, match="rehearsal failed") as exc_info:
        service.rehearse_retained(
            "retained-real-inexact-lineage",
            source_rehearsal_report_id="market-v4-rehearsal-20260715-r10",
            config=config,
            inherited_environment={},
        )

    assert "lineage is not ready" in str(exc_info.value.__cause__)
    assert runtime.start_calls == 0
    assert not (
        retained_root
        / "market-timeseries/.cutover-runtime-retained-real-inexact-lineage"
    ).exists()


def test_directory_bound_adapter_keeps_real_duckdb_bound_after_parent_swap(
    tmp_path: Path,
    guard_lease_fd: int,
) -> None:
    import duckdb

    data_root = tmp_path / "data"
    market_root = data_root / "market-timeseries"
    market_root.mkdir(parents=True)
    db_path = market_root / "market.duckdb"
    connection = duckdb.connect(str(db_path))
    connection.execute("CREATE TABLE market_schema_version(version INTEGER)")
    connection.execute("INSERT INTO market_schema_version VALUES (4)")
    connection.execute("CREATE TABLE sync_metadata(key VARCHAR, value VARCHAR)")
    connection.execute(
        "INSERT INTO sync_metadata VALUES "
        "('stock_price_adjustment_mode', 'local_projection_v2_event_time')"
    )
    connection.close()

    external = tmp_path / "external"
    external.mkdir()
    external_db = external / "market.duckdb"
    external_db.write_bytes(b"external-must-not-change")
    external_before = external_db.read_bytes()
    retained_fd = os.open(market_root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        detached = data_root / "market-timeseries.detached"
        market_root.rename(detached)
        market_root.symlink_to(external, target_is_directory=True)

        adapter = market_v4_cutover.DefaultDuckDbAdapter()
        assert adapter.checkpoint_exclusive(
            retained_fd,
            "market.duckdb",
            guard_lease_fd=guard_lease_fd,
        ) == MarketSourceMetadata(4, "local_projection_v2_event_time", False)
        assert adapter.inspect(
            retained_fd,
            "market.duckdb",
            guard_lease_fd=guard_lease_fd,
        ) == MarketSourceMetadata(4, "local_projection_v2_event_time", False)
    finally:
        os.close(retained_fd)

    assert external_db.read_bytes() == external_before


def test_directory_bound_checkpoint_snapshot_holds_worker_until_release(
    tmp_path: Path,
    guard_lease_fd: int,
) -> None:
    import duckdb

    db_path = tmp_path / "market.duckdb"
    connection = duckdb.connect(str(db_path))
    connection.execute("CREATE TABLE market_schema_version(version INTEGER)")
    connection.execute("INSERT INTO market_schema_version VALUES (4)")
    connection.close()
    directory_fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    adapter = market_v4_cutover.DefaultDuckDbAdapter()
    writer_probe = [
        market_v4_cutover.sys.executable,
        "-c",
        (
            "import duckdb,sys;"
            "connection=duckdb.connect(sys.argv[1],read_only=False);"
            "connection.close()"
        ),
        str(db_path),
    ]
    try:
        with adapter.checkpoint_snapshot(
            directory_fd,
            "market.duckdb",
            guard_lease_fd=guard_lease_fd,
        ):
            locked = market_v4_cutover.subprocess.run(
                writer_probe,
                capture_output=True,
                check=False,
            )
            assert locked.returncode != 0
        released = market_v4_cutover.subprocess.run(
            writer_probe,
            capture_output=True,
            check=False,
        )
        assert released.returncode == 0
    finally:
        os.close(directory_fd)


def test_checkpoint_worker_timeout_is_killed_without_masking_body_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    guard_lease_fd: int,
) -> None:
    class Pipe:
        closed = False

        def write(self, _payload: bytes) -> None:
            pass

        def close(self) -> None:
            self.closed = True

    class HungProcess:
        stdin = Pipe()
        stderr = Pipe()
        terminated = False
        killed = False
        communicated = False

        def wait(self, timeout: float) -> int:
            if self.killed:
                return -9
            raise market_v4_cutover.subprocess.TimeoutExpired("worker", timeout)

        def terminate(self) -> None:
            self.terminated = True

        def kill(self) -> None:
            self.killed = True

        def communicate(self, timeout: float) -> tuple[bytes, bytes]:
            del timeout
            self.communicated = True
            return b"", b""

    class BodyError(RuntimeError):
        pass

    process = HungProcess()
    release_pipe = process.stdin
    adapter = market_v4_cutover.DefaultDuckDbAdapter()
    monkeypatch.setattr(
        adapter, "_start_worker", lambda *_args, **_kwargs: process
    )
    monkeypatch.setattr(
        adapter,
        "_read_metadata",
        lambda _process: MarketSourceMetadata(4, "local_projection_v2_event_time"),
    )
    directory_fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        with pytest.raises(BodyError):
            with adapter.checkpoint_snapshot(
                directory_fd,
                "market.duckdb",
                guard_lease_fd=guard_lease_fd,
            ):
                raise BodyError("original body failure")
    finally:
        os.close(directory_fd)

    assert process.terminated is True
    assert process.killed is True
    assert process.communicated is True
    assert release_pipe.closed is True


def test_checkpoint_worker_broken_release_is_reaped_and_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    guard_lease_fd: int,
) -> None:
    class BrokenPipe:
        closed = False

        def write(self, _payload: bytes) -> None:
            raise BrokenPipeError("worker closed release pipe")

        def close(self) -> None:
            self.closed = True

    class ExitedProcess:
        stdin = BrokenPipe()
        stderr = BrokenPipe()
        communicated = False

        def wait(self, timeout: float) -> int:
            del timeout
            return 0

        def communicate(self, timeout: float) -> tuple[bytes, bytes]:
            del timeout
            self.communicated = True
            return b"", b""

    process = ExitedProcess()
    release_pipe = process.stdin
    adapter = market_v4_cutover.DefaultDuckDbAdapter()
    monkeypatch.setattr(
        adapter, "_start_worker", lambda *_args, **_kwargs: process
    )
    monkeypatch.setattr(
        adapter,
        "_read_metadata",
        lambda _process: MarketSourceMetadata(4, "local_projection_v2_event_time"),
    )
    directory_fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        with pytest.raises(CutoverSafetyError, match="release"):
            with adapter.checkpoint_snapshot(
                directory_fd,
                "market.duckdb",
                guard_lease_fd=guard_lease_fd,
            ):
                pass
    finally:
        os.close(directory_fd)

    assert process.communicated is True
    assert release_pipe.closed is True


def test_inspect_worker_timeout_is_killed_reaped_and_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    guard_lease_fd: int,
) -> None:
    class Pipe:
        closed = False

        def close(self) -> None:
            self.closed = True

    class HungProcess:
        stdin = Pipe()
        terminated = False
        killed = False
        communicated = False

        def wait(self, timeout: float) -> int:
            if self.killed:
                return -9
            raise market_v4_cutover.subprocess.TimeoutExpired("worker", timeout)

        def terminate(self) -> None:
            self.terminated = True

        def kill(self) -> None:
            self.killed = True

        def communicate(self, timeout: float) -> tuple[bytes, bytes]:
            del timeout
            self.communicated = True
            return b"", b""

    process = HungProcess()
    stdin_pipe = process.stdin
    adapter = market_v4_cutover.DefaultDuckDbAdapter()
    monkeypatch.setattr(
        adapter, "_start_worker", lambda *_args, **_kwargs: process
    )
    monkeypatch.setattr(
        adapter,
        "_read_metadata",
        lambda _process: MarketSourceMetadata(4, "local_projection_v2_event_time"),
    )
    directory_fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        with pytest.raises(CutoverSafetyError, match="timed out"):
            adapter.inspect(
                directory_fd,
                "market.duckdb",
                guard_lease_fd=guard_lease_fd,
            )
    finally:
        os.close(directory_fd)

    assert process.terminated is True
    assert process.killed is True
    assert process.communicated is True
    assert stdin_pipe.closed is True


def test_inspect_worker_pre_metadata_hang_is_bounded_and_reaped(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    guard_lease_fd: int,
) -> None:
    adapter = market_v4_cutover.DefaultDuckDbAdapter()
    monkeypatch.setattr(
        market_v4_cutover.DefaultDuckDbAdapter,
        "_WORKER_EXIT_TIMEOUT_SECONDS",
        0.05,
    )
    monkeypatch.setattr(
        market_v4_cutover.DefaultDuckDbAdapter,
        "_WORKER_STOP_TIMEOUT_SECONDS",
        1.0,
    )
    process = market_v4_cutover.subprocess.Popen(
        [market_v4_cutover.sys.executable, "-c", "import time; time.sleep(60)"],
        stdin=market_v4_cutover.subprocess.PIPE,
        stdout=market_v4_cutover.subprocess.PIPE,
        stderr=market_v4_cutover.subprocess.PIPE,
    )
    monkeypatch.setattr(
        adapter, "_start_worker", lambda *_args, **_kwargs: process
    )
    directory_fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        with pytest.raises(CutoverSafetyError, match="metadata timed out"):
            adapter.inspect(
                directory_fd,
                "market.duckdb",
                guard_lease_fd=guard_lease_fd,
            )
    finally:
        os.close(directory_fd)

    assert process.poll() is not None


def test_inspect_worker_partial_metadata_hang_is_bounded_and_reaped(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    guard_lease_fd: int,
) -> None:
    adapter = market_v4_cutover.DefaultDuckDbAdapter()
    monkeypatch.setattr(
        market_v4_cutover.DefaultDuckDbAdapter,
        "_WORKER_EXIT_TIMEOUT_SECONDS",
        0.05,
    )
    monkeypatch.setattr(
        market_v4_cutover.DefaultDuckDbAdapter,
        "_WORKER_STOP_TIMEOUT_SECONDS",
        1.0,
    )
    process = market_v4_cutover.subprocess.Popen(
        [
            market_v4_cutover.sys.executable,
            "-c",
            "import sys,time; sys.stdout.write('{'); sys.stdout.flush(); time.sleep(60)",
        ],
        stdin=market_v4_cutover.subprocess.PIPE,
        stdout=market_v4_cutover.subprocess.PIPE,
        stderr=market_v4_cutover.subprocess.PIPE,
    )
    monkeypatch.setattr(
        adapter, "_start_worker", lambda *_args, **_kwargs: process
    )
    directory_fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        with pytest.raises(CutoverSafetyError, match="metadata timed out"):
            adapter.inspect(
                directory_fd,
                "market.duckdb",
                guard_lease_fd=guard_lease_fd,
            )
    finally:
        os.close(directory_fd)

    assert process.poll() is not None


def test_inspect_unkillable_worker_cleanup_remains_bounded_and_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    guard_lease_fd: int,
) -> None:
    class Pipe:
        closed = False

        def close(self) -> None:
            self.closed = True

    class UnkillableProcess:
        stdin = Pipe()
        terminate_calls = 0
        kill_calls = 0
        communicate_calls = 0

        def wait(self, timeout: float) -> int:
            raise market_v4_cutover.subprocess.TimeoutExpired("worker", timeout)

        def terminate(self) -> None:
            self.terminate_calls += 1

        def kill(self) -> None:
            self.kill_calls += 1

        def communicate(self, timeout: float) -> tuple[bytes, bytes]:
            self.communicate_calls += 1
            raise market_v4_cutover.subprocess.TimeoutExpired("worker", timeout)

    process = UnkillableProcess()
    adapter = market_v4_cutover.DefaultDuckDbAdapter()
    monkeypatch.setattr(
        adapter, "_start_worker", lambda *_args, **_kwargs: process
    )
    monkeypatch.setattr(
        adapter,
        "_read_metadata",
        lambda _process: MarketSourceMetadata(4, "local_projection_v2_event_time"),
    )
    directory_fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        with pytest.raises(CutoverSafetyError, match="shutdown failed"):
            adapter.inspect(
                directory_fd,
                "market.duckdb",
                guard_lease_fd=guard_lease_fd,
            )
    finally:
        os.close(directory_fd)

    assert process.terminate_calls == 1
    assert process.kill_calls == 2
    assert process.communicate_calls == 2


@pytest.mark.parametrize("denied_signal", ["terminate", "kill"])
def test_inspect_worker_signal_errors_return_explicit_unjoined_verdict(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    denied_signal: str,
    guard_lease_fd: int,
) -> None:
    class Pipe:
        def close(self) -> None:
            pass

    class SignalDeniedProcess:
        stdin = Pipe()

        def wait(self, timeout: float) -> int:
            raise market_v4_cutover.subprocess.TimeoutExpired("worker", timeout)

        def terminate(self) -> None:
            if denied_signal == "terminate":
                raise PermissionError("terminate denied")

        def kill(self) -> None:
            if denied_signal == "kill":
                raise PermissionError("kill denied")

        def communicate(self, timeout: float) -> tuple[bytes, bytes]:
            raise market_v4_cutover.subprocess.TimeoutExpired("worker", timeout)

    process = SignalDeniedProcess()
    adapter = market_v4_cutover.DefaultDuckDbAdapter()
    monkeypatch.setattr(
        adapter, "_start_worker", lambda *_args, **_kwargs: process
    )
    monkeypatch.setattr(
        adapter,
        "_read_metadata",
        lambda _process: MarketSourceMetadata(4, "local_projection_v2_event_time"),
    )
    directory_fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        with pytest.raises(market_v4_cutover.WorkerShutdownError) as captured:
            adapter.inspect(
                directory_fd,
                "market.duckdb",
                guard_lease_fd=guard_lease_fd,
            )
    finally:
        os.close(directory_fd)

    assert captured.value.process_joined is False
    assert isinstance(captured.value, CutoverSafetyError)


def test_copy_tree_create_closes_source_fd_when_target_open_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "root"
    (root / "source").mkdir(parents=True)
    with market_v4_cutover.ManagedRootFd.open(root) as managed:
        original_open_dir = managed.open_dir
        retained_source_fd = -1

        def fail_target_open(
            relative: Path,
            *,
            create: bool = False,
            exclusive_leaf: bool = False,
        ) -> int:
            nonlocal retained_source_fd
            if relative == Path("source"):
                retained_source_fd = original_open_dir(relative)
                return retained_source_fd
            assert relative == Path("target")
            assert create is True
            assert exclusive_leaf is True
            raise OSError("injected target open failure")

        monkeypatch.setattr(managed, "open_dir", fail_target_open)
        with pytest.raises(OSError, match="target open failure"):
            managed.copy_tree_create(Path("source"), Path("target"))

        assert retained_source_fd >= 0
        with pytest.raises(OSError):
            os.fstat(retained_source_fd)


def test_runtime_cancels_screening_and_dataset_jobs_before_polling_terminal() -> None:
    class RecordingApi(market_v4_cutover.HttpApiAdapter):
        def __init__(self) -> None:
            super().__init__("http://unused")
            self.events: list[tuple[str, str]] = []

        def request(
            self,
            method: str,
            path: str,
            payload: dict[str, object] | None = None,
        ) -> dict[str, object]:
            del payload
            self.events.append((method, path))
            if method in {"POST", "DELETE"}:
                return {"status": "cancelled"}
            return {"status": "cancelled"}

    api = RecordingApi()
    api.owned_jobs = {"screening": "screen-1", "dataset": "data-1"}
    market_v4_cutover.SubprocessRuntimeAdapter().cancel_owned_work(api)

    assert api.events == [
        ("POST", "/api/analytics/screening/jobs/screen-1/cancel"),
        ("GET", "/api/analytics/screening/jobs/screen-1"),
        ("DELETE", "/api/dataset/jobs/data-1"),
        ("GET", "/api/dataset/jobs/data-1"),
    ]


def test_runtime_treats_cancel_400_as_safe_when_followup_status_is_terminal() -> None:
    class TerminalRaceApi(market_v4_cutover.HttpApiAdapter):
        def __init__(self) -> None:
            super().__init__("http://unused")
            self.events: list[tuple[str, str]] = []

        def request(
            self,
            method: str,
            path: str,
            payload: dict[str, object] | None = None,
        ) -> dict[str, object]:
            del payload
            self.events.append((method, path))
            if method == "DELETE":
                raise CutoverSafetyError("HTTP 400: job is already failed")
            return {"status": "failed"}

    api = TerminalRaceApi()
    api.owned_jobs = {"sync": "sync-1"}

    market_v4_cutover.SubprocessRuntimeAdapter().cancel_owned_work(api)

    assert api.events == [
        ("DELETE", "/api/db/sync/jobs/sync-1"),
        ("GET", "/api/db/sync/jobs/sync-1"),
    ]
