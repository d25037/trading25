"""Shared direct-owner fixtures for Market v5 cutover tests."""

from __future__ import annotations

from datetime import UTC, datetime
from dataclasses import dataclass, field, replace
from contextlib import contextmanager
import json
import os
from pathlib import Path
import stat
import time
from types import SimpleNamespace

import pytest

import src.application.services.market_v4_cutover.filesystem as filesystem_module
from src.application.contracts.market_data_plane import ProviderVintageStats
from src.application.services.market_v4_cutover.contracts import (
    AtomicExchange,
    MarketSourceMetadata,
)
from src.application.services.market_v4_cutover.service import MarketV4CutoverService
from src.infrastructure.db.market.managed_root import CutoverSafetyError
from src.infrastructure.db.market import managed_root, market_operation_lease
from src.entrypoints.http.schemas.screening_job import ScreeningJobResponse


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
        if self.metadata.provider_vintage is None and self.metadata.schema_version == 5:
            return replace(
                self.metadata,
                provider_vintage=_ready_provider_vintage_payload(),
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
    active_lease_fds: list[int] = field(default_factory=list)
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
        self.active_lease_fds.append(lease_fd)
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


def _ready_provider_vintage_payload() -> dict[str, object]:
    return ProviderVintageStats(
        providerPlan="standard",
        providerAsOf="2026-07-20",
        providerAsOfRange={"min": "2026-07-20", "max": "2026-07-20"},
        effectiveCoverage={"min": "2016-07-20", "max": "2026-07-18"},
        sourceFingerprint="a" * 64,
        providerWindowCoherent=True,
        providerWindowCount=2,
        readyProviderWindowCount=2,
        providerWindowFingerprintCount=2,
        adjustmentEventCount=1,
        adjustmentEventFingerprintCount=1,
        currentBasisStatementCount=4,
        currentBasisStateCount=2,
        fundamentalsAdjustmentBasisDate="2026-07-18",
        sourceStatementKeyCount=2,
        expectedAdjustedStatementRows=4,
        status="ready",
    ).model_dump(mode="json")


class FakeApi:
    def __init__(
        self,
        *,
        invalid_lineage: bool = False,
        parity: bool = True,
        provider_vintage: dict[str, object] | None = None,
        dataset_snapshot: dict[str, object] | None = None,
    ) -> None:
        self.invalid_lineage = invalid_lineage
        self.parity = parity
        self.provider_vintage = provider_vintage or _ready_provider_vintage_payload()
        vintage = self.provider_vintage
        coverage = vintage["effectiveCoverage"]
        assert isinstance(coverage, dict)
        self.dataset_snapshot = dataset_snapshot or {
            "schemaVersion": 4,
            "sourceMarketSchemaVersion": 5,
            "stockPriceAdjustmentMode": "provider_adjusted_v1",
            "providerPlan": vintage["providerPlan"],
            "providerAsOf": vintage["providerAsOf"],
            "providerCoverageStart": coverage["min"],
            "providerCoverageEnd": coverage["max"],
            "providerSourceFingerprint": vintage["sourceFingerprint"],
            "fundamentalsAdjustmentBasisDate": vintage[
                "fundamentalsAdjustmentBasisDate"
            ],
        }
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
                "schema": {"version": 5, "requiredVersion": 5, "current": True},
                "providerVintage": dict(self.provider_vintage),
            }
        if path == "/api/db/validate":
            provider_vintage = dict(self.provider_vintage)
            if self.invalid_lineage:
                provider_vintage["status"] = "invalid"
                provider_vintage["wrongBasisAdjustedStatementRows"] = 1
            return {
                "status": "healthy",
                "providerVintage": provider_vintage,
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
                "snapshot": dict(self.dataset_snapshot),
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

    monkeypatch.setattr(managed_root.ManagedRootFd, "copy_tree_create", fail)
    monkeypatch.setattr(filesystem_module, "_rename_exclusive_at", fail)


def _forbid_atomic_exchange_syscall(monkeypatch: pytest.MonkeyPatch) -> None:
    class ForbiddenRenameSwap:
        argtypes: object = None
        restype: object = None

        def __call__(self, *_args: object) -> int:
            raise AssertionError("atomic exchange syscall ran")

    monkeypatch.setattr(
        filesystem_module.ctypes,
        "CDLL",
        lambda *_args, **_kwargs: SimpleNamespace(renameatx_np=ForbiddenRenameSwap()),
    )


def _exchange_identity(path: Path) -> tuple[int, int, int, bytes]:
    path_stat = path.stat()
    payload = path / "payload"
    payload_stat = payload.stat()
    return path_stat.st_ino, payload_stat.st_ino, path_stat.st_dev, payload.read_bytes()


def _write_report(data_root: Path, report_id: str, report: dict[str, object]) -> None:
    report_dir = data_root / "operations/market-v5-cutover/reports" / report_id
    report_dir.mkdir(parents=True)
    (report_dir / "report.json").write_text(json.dumps(report))


def _service(
    data_root: Path,
    *,
    duckdb: FakeDuckDb | None = None,
    runtime: FakeRuntime | None = None,
    free_bytes: int = 10_000_000,
    atomic_exchange: AtomicExchange | None = None,
) -> MarketV4CutoverService:
    return MarketV4CutoverService(
        data_root,
        duckdb=duckdb or FakeDuckDb(),
        runtime=runtime or FakeRuntime(),
        disk_free_bytes=lambda _path: free_bytes,
        now=lambda: "2026-07-15T12:00:00Z",
        code_version=lambda: "deadbeef",
        atomic_exchange=(
            _TestAtomicExchange() if atomic_exchange is None else atomic_exchange
        ),
    )


def _changing_code_version(*versions: str):
    calls: list[str] = []
    iterator = iter(versions)

    def code_version() -> str:
        value = next(iterator)
        calls.append(value)
        return value

    return code_version, calls


class _TestAtomicExchange:
    def require_capability(self) -> None:
        return None

    def exchange(
        self,
        managed_root: managed_root.ManagedRootFd,
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


@pytest.fixture
def guard_lease_fd(tmp_path: Path):
    with market_operation_lease.MarketOperationLease.acquire(
        tmp_path,
        exclusive=False,
    ) as lease:
        yield lease.fd
