"""Shared direct-owner fixtures for Market v4 cutover tests."""

from __future__ import annotations

from datetime import UTC, datetime
from dataclasses import dataclass, field, replace
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import time
from types import SimpleNamespace

import pytest

import src.application.services.market_v4_cutover.filesystem as filesystem_module
from src.application.services.market_v4_cutover.contracts import (
    MarketSourceMetadata,
    PromotionIdentityEvidence,
    PromotionJournalRecord,
    PromotionState,
    RetainedPromotionPreparation,
    SmokeConfig,
)
from src.application.services.market_v4_cutover.journal import PromotionJournal
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
                    "currentBasisStatementCount": 4,
                    "dailyValuationRows": 10,
                    "readyProviderWindowCount": 2,
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
    report_dir = data_root / "operations/market-v4-cutover/reports" / report_id
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
        atomic_exchange=_TestAtomicExchange(),
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
    locations: dict[str, dict[str, object] | None | tuple[str, ...]] = {
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
        locations["promotion_report_sha256"] = "c" * 64
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
        promotion_report_sha256=locations.get("promotion_report_sha256"),
    )


def _promotion_journal(
    data_root: Path,
    operation_id: str = "promotion-001",
    **kwargs: object,
) -> tuple[managed_root.ManagedRootFd, PromotionJournal]:
    data_root.mkdir(parents=True, exist_ok=True)
    managed = managed_root.ManagedRootFd.open(data_root)
    journal = PromotionJournal(
        managed,
        operation_id,
        now=lambda: "2026-07-16T00:00:00Z",
        **kwargs,
    )
    return managed, journal


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
    (retained_root / "market-timeseries/parquet/stock_data/part.parquet").write_bytes(
        b"retained-rows"
    )
    shutil.copytree(data_root / "config", retained_root / "config")
    shutil.copytree(data_root / "strategies", retained_root / "strategies")
    service = _service(
        data_root,
        duckdb=FakeDuckDb(MarketSourceMetadata(4, "local_projection_v2_event_time")),
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
    service._workspace.runtime = FakeRuntime(apis=[FakeApi()])
    service.rehearse_retained(
        "market-v4-retained-20260715-r13",
        source_rehearsal_report_id="market-v4-rehearsal-20260715-r10",
        config=config,
        inherited_environment={},
    )
    with market_operation_lease.MarketOperationLease.acquire(data_root, exclusive=True):
        pass
    return service, retained_root, config


def _prepare_retained_promotion(
    service: MarketV4CutoverService,
    config: SmokeConfig,
    *,
    backup_id: str = "market-v3-pre-v4-20260716",
):
    report_id = "market-v4-active-20260716"
    with service._promotion._transaction._retained_promotion_eligibility_scope(
        report_id=report_id,
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id=backup_id,
        config=config,
    ) as eligibility:
        journal = PromotionJournal(
            service._workspace.managed(),
            report_id,
            now=lambda: "2026-07-16T00:00:00Z",
        )
        preparation = service._promotion._artifacts._prepare_retained_promotion_under_leases(
            eligibility,
            backup_id=backup_id,
            journal=journal,
        )
        records = journal.read_validated()
    return preparation, records


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


def _run_retained_promotion(
    service: MarketV4CutoverService,
    config: SmokeConfig,
    *,
    inherited_environment: dict[str, str] | None = None,
):
    report_id = "market-v4-active-20260716"
    with service._promotion._transaction._retained_promotion_eligibility_scope(
        report_id=report_id,
        retained_report_id="market-v4-retained-20260715-r13",
        backup_id="market-v3-pre-v4-20260716",
        config=config,
    ) as eligibility:
        journal = PromotionJournal(
            service._workspace.managed(),
            report_id,
            now=lambda: "2026-07-16T00:00:00Z",
        )
        preparation = service._promotion._artifacts._prepare_retained_promotion_under_leases(
            eligibility,
            backup_id="market-v3-pre-v4-20260716",
            journal=journal,
        )
        result = service._promotion._promote_retained_under_leases(
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
    with managed_root.ManagedRootFd.open(root) as managed:
        return service._market_identity.market_tree_identity(managed.fd)


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


def _owned_temp_collision_records(
    preparation: RetainedPromotionPreparation,
    base: PromotionIdentityEvidence,
    *,
    states: tuple[PromotionState, ...] = (
        PromotionState.ACTIVE_SMOKE_PASSED,
        PromotionState.CLEANUP_STAGED,
        PromotionState.EXCHANGED_BACK,
    ),
    rollback_mode: str = "atomic_exchange",
    detached_artifacts: tuple[dict[str, object], ...] | None = None,
) -> tuple[PromotionJournalRecord, ...]:
    evidence = (
        tuple(artifact.to_mapping() for artifact in preparation.detached_artifacts)
        if detached_artifacts is None
        else detached_artifacts
    )
    identities = replace(
        base,
        detached_artifacts=evidence,
        rollback_mode=rollback_mode,
    )
    return tuple(
        PromotionJournalRecord(
            sequence=index,
            state=state,
            operation_id="market-v4-active-20260716",
            identities=identities,
            created_at="2026-07-16T00:00:00Z",
        )
        for index, state in enumerate(states, start=10)
    )


@pytest.fixture
def guard_lease_fd(tmp_path: Path):
    with market_operation_lease.MarketOperationLease.acquire(
        tmp_path,
        exclusive=False,
    ) as lease:
        yield lease.fd
