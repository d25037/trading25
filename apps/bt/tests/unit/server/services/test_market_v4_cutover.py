from __future__ import annotations

from dataclasses import dataclass, field
import json
from pathlib import Path
import shutil

import pytest

from src.application.services import market_v4_cutover
from src.application.services.market_v4_cutover import (
    CutoverSafetyError,
    MarketSourceMetadata,
    MarketV4CutoverService,
    SmokeConfig,
)
from src.entrypoints.http.schemas.db import MarketSchemaStats


def test_market_schema_stats_requires_v4_by_default() -> None:
    assert MarketSchemaStats().requiredVersion == 4


def test_cutover_service_exposes_injected_boundaries() -> None:
    assert hasattr(market_v4_cutover, "MarketV4CutoverService")
    assert hasattr(market_v4_cutover, "DuckDbAdapter")
    assert hasattr(market_v4_cutover, "RuntimeAdapter")


@dataclass
class FakeDuckDb:
    metadata: MarketSourceMetadata = MarketSourceMetadata(
        schema_version=3,
        adjustment_mode="local_projection_v1",
    )
    checkpoint_error: Exception | None = None
    leave_wal: bool = False

    def checkpoint_exclusive(self, db_path: Path) -> MarketSourceMetadata:
        if self.checkpoint_error is not None:
            raise self.checkpoint_error
        if self.leave_wal:
            db_path.with_suffix(".duckdb.wal").write_bytes(b"pending")
        return self.metadata

    def inspect(self, db_path: Path) -> MarketSourceMetadata:
        assert db_path.is_file()
        return self.metadata


@dataclass
class FakeRuntime:
    stopped: bool = True
    active_jobs: tuple[str, ...] = ()
    apis: list[FakeApi] = field(default_factory=list)
    environments: list[dict[str, str]] = field(default_factory=list)
    stop_calls: int = 0
    cancel_calls: int = 0

    def assert_quiescent(self, _data_root: Path) -> None:
        if not self.stopped:
            raise CutoverSafetyError("FastAPI process must be stopped")
        if self.active_jobs:
            raise CutoverSafetyError(f"active jobs: {', '.join(self.active_jobs)}")

    def start(
        self,
        _data_root: Path,
        environment: dict[str, str],
        log_path: Path,
    ) -> FakeApi:
        self.environments.append(environment)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text("owned server\n")
        market = _data_root / "market-timeseries"
        market.mkdir(parents=True, exist_ok=True)
        (market / "market.duckdb").touch(exist_ok=True)
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
            return {"jobId": "screen-1", "status": "pending"}
        if path == "/api/analytics/screening/jobs/screen-1":
            return {"jobId": "screen-1", "status": "completed"}
        if path == "/api/analytics/screening/result/screen-1":
            return {"results": [{"code": "7203"}]}
        if path.startswith("/api/analytics/fundamental-ranking"):
            return {"rankings": {"ratioHigh": [{"code": "7203"}]}}
        if path == "/api/dataset":
            return {"jobId": "dataset-1", "status": "pending"}
        if path == "/api/dataset/jobs/dataset-1":
            return {"jobId": "dataset-1", "status": "completed"}
        if path == "/api/dataset/cutover-smoke/info":
            return {
                "snapshot": {
                    "schemaVersion": 3,
                    "sourceMarketSchemaVersion": 4,
                    "stockPriceAdjustmentMode": "local_projection_v2_event_time",
                },
                "validation": {"isValid": True},
            }
        if path == "/api/dataset/cutover-smoke/stocks":
            return {"items": [{"code": "7203"}]}
        raise AssertionError(f"Unexpected API call: {method} {path}")


def _market_root(tmp_path: Path) -> Path:
    data_root = tmp_path / "xdg"
    market = data_root / "market-timeseries"
    (market / "parquet" / "stock_data").mkdir(parents=True)
    (market / "market.duckdb").write_bytes(b"duckdb-v3")
    (market / "parquet" / "stock_data" / "part.parquet").write_bytes(b"rows")
    return data_root


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

    result = service.smoke(FakeApi(), config)

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

    with pytest.raises(CutoverSafetyError, match="adjusted-metric lineage"):
        service.smoke(FakeApi(invalid_lineage=True), config)
    with pytest.raises(CutoverSafetyError, match="Fundamentals GET/POST parity"):
        service.smoke(FakeApi(parity=False), config)


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
        )


def test_rehearsal_uses_isolated_paths_and_credential_safe_report(tmp_path: Path) -> None:
    data_root = _market_root(tmp_path)
    runtime = FakeRuntime(apis=[FakeApi()])
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
    assert report["targetRootFingerprint"] == service.root_fingerprint(data_root)
    assert "super-secret" not in report_text
    assert str(data_root) not in report_text
    assert runtime.stop_calls == 1
    environment = runtime.environments[0]
    rehearsal_root = data_root / "operations/market-v4-cutover/rehearsals/rehearsal-001/root"
    assert environment["TRADING25_DATA_DIR"] == str(rehearsal_root)
    assert environment["MARKET_TIMESERIES_DIR"] == str(rehearsal_root / "market-timeseries")
    assert environment["MARKET_DB_PATH"] == str(rehearsal_root / "market-timeseries/market.duckdb")
    assert environment["DATASET_BASE_PATH"] == str(rehearsal_root / "datasets")
    assert environment["PORTFOLIO_DB_PATH"] == str(rehearsal_root / "portfolio.db")
    assert environment["TRADING25_STRATEGIES_DIR"] == str(rehearsal_root / "strategies")
    assert environment["TRADING25_BACKTEST_DIR"] == str(rehearsal_root / "backtest")
    assert environment["TRADING25_DEFAULT_CONFIG_PATH"] == str(rehearsal_root / "config/default.yaml")
    assert environment["JQUANTS_API_KEY"] == "super-secret"
    api_calls = runtime.environments and report["apiChecks"]
    assert "/api/db/adjusted-metrics/materialize" not in api_calls


def test_cutover_requires_exact_passing_rehearsal_and_verified_backup(
    tmp_path: Path,
) -> None:
    data_root = _market_root(tmp_path)
    runtime = FakeRuntime(apis=[FakeApi(), FakeApi()])
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


def test_cutover_failure_stops_owned_server_and_restores_backup(tmp_path: Path) -> None:
    data_root = _market_root(tmp_path)
    runtime = FakeRuntime(apis=[FakeApi(), FakeApi(invalid_lineage=True)])
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
    assert runtime.stop_calls == 2
    assert (data_root / "market-timeseries/market.duckdb").read_bytes() == original
    assert (data_root / "operations/market-v4-cutover/backups/backup-001").exists()


def test_default_runtime_adapter_is_available() -> None:
    assert hasattr(market_v4_cutover, "DefaultDuckDbAdapter")
    assert hasattr(market_v4_cutover, "SubprocessRuntimeAdapter")


def test_owned_server_argv_uses_uvicorn_without_cli_port_kill_path() -> None:
    argv = market_v4_cutover.SubprocessRuntimeAdapter.server_argv(41234)
    assert argv.count("--port") == 1
    assert argv[-2:] == ["--port", "41234"]
    assert "bt" not in argv


def test_owned_server_log_redacts_secrets_and_local_paths(tmp_path: Path) -> None:
    log = tmp_path / "server.log"
    log.write_text("key=super-secret db=/Users/me/active/market.duckdb\n")
    market_v4_cutover.SubprocessRuntimeAdapter.redact_log_file(
        log,
        {
            "JQUANTS_API_KEY": "super-secret",
            "MARKET_DB_PATH": "/Users/me/active/market.duckdb",
        },
    )
    retained = log.read_text()
    assert "super-secret" not in retained
    assert "/Users/me" not in retained
    assert "<redacted-secret>" in retained
    assert log.stat().st_mode & 0o777 == 0o600


def test_default_duckdb_adapter_checkpoints_and_reads_raw_metadata(tmp_path: Path) -> None:
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
    assert adapter.checkpoint_exclusive(db_path) == MarketSourceMetadata(
        4, "local_projection_v2_event_time"
    )
    assert adapter.inspect(db_path) == MarketSourceMetadata(
        4, "local_projection_v2_event_time"
    )
