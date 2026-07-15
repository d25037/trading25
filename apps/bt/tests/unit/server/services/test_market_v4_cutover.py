from __future__ import annotations

from dataclasses import dataclass, field
from contextlib import contextmanager
import json
import os
from pathlib import Path
import shutil
import stat

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
    checkpoint_calls: int = 0

    def checkpoint_exclusive(
        self,
        directory_fd: int,
        filename: str,
    ) -> MarketSourceMetadata:
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
    def checkpoint_snapshot(self, directory_fd: int, filename: str):
        yield self.checkpoint_exclusive(directory_fd, filename)

    def inspect(self, directory_fd: int, filename: str) -> MarketSourceMetadata:
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
        environment: dict[str, str],
        log_path: Path,
        log_fd: int,
    ) -> FakeApi:
        assert root_fd >= 0
        assert market_fd >= 0
        assert lease_fd >= 0
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

    result = service.smoke(FakeApi(), config, operation_id="smoke-001")

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
    api_calls = runtime.environments and report["apiChecks"]
    assert "/api/db/adjusted-metrics/materialize" not in api_calls
    sync_payload = next(
        payload
        for method, path, payload in rehearsal_api.calls
        if method == "POST" and path == "/api/db/sync"
    )
    assert sync_payload is not None
    assert sync_payload["resetBeforeSync"] is False


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
    log_fd = os.open(tmp_path / "server.log", os.O_CREAT | os.O_RDWR, 0o600)
    try:
        monkeypatch.setattr(market_v4_cutover.subprocess, "Popen", fake_popen)
        runtime = market_v4_cutover.SubprocessRuntimeAdapter()
        with pytest.raises(CutoverSafetyError, match="exited during startup"):
            runtime.start(
                root_fd=root_fd,
                market_fd=market_fd,
                lease_fd=lease_fd,
                environment={},
                log_path=tmp_path / "server.log",
                log_fd=log_fd,
            )
    finally:
        os.close(log_fd)
        os.close(lease_fd)
        os.close(market_fd)
        os.close(root_fd)

    assert set(captured["pass_fds"]) == {root_fd, market_fd, lease_fd}
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


def test_fixed_port_health_is_not_probed_after_root_scoped_lease(
    monkeypatch, tmp_path: Path
) -> None:
    def must_not_probe(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("fixed-port health probe must not run")

    monkeypatch.setattr(market_v4_cutover, "urlopen", must_not_probe)
    runtime = market_v4_cutover.SubprocessRuntimeAdapter()
    runtime.assert_quiescent(tmp_path)


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
    directory_fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        assert adapter.checkpoint_exclusive(
            directory_fd,
            "market.duckdb",
        ) == MarketSourceMetadata(4, "local_projection_v2_event_time")
        assert adapter.inspect(directory_fd, "market.duckdb") == MarketSourceMetadata(
            4, "local_projection_v2_event_time"
        )
    finally:
        os.close(directory_fd)


def test_directory_bound_adapter_keeps_real_duckdb_bound_after_parent_swap(
    tmp_path: Path,
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
        assert adapter.checkpoint_exclusive(retained_fd, "market.duckdb") == MarketSourceMetadata(
            4, "local_projection_v2_event_time"
        )
        assert adapter.inspect(retained_fd, "market.duckdb") == MarketSourceMetadata(
            4, "local_projection_v2_event_time"
        )
    finally:
        os.close(retained_fd)

    assert external_db.read_bytes() == external_before


def test_directory_bound_checkpoint_snapshot_holds_worker_until_release(
    tmp_path: Path,
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
        with adapter.checkpoint_snapshot(directory_fd, "market.duckdb"):
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
    monkeypatch.setattr(adapter, "_start_worker", lambda *_args: process)
    monkeypatch.setattr(
        adapter,
        "_read_metadata",
        lambda _process: MarketSourceMetadata(4, "local_projection_v2_event_time"),
    )
    directory_fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        with pytest.raises(BodyError):
            with adapter.checkpoint_snapshot(directory_fd, "market.duckdb"):
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
    monkeypatch.setattr(adapter, "_start_worker", lambda *_args: process)
    monkeypatch.setattr(
        adapter,
        "_read_metadata",
        lambda _process: MarketSourceMetadata(4, "local_projection_v2_event_time"),
    )
    directory_fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        with pytest.raises(CutoverSafetyError, match="release"):
            with adapter.checkpoint_snapshot(directory_fd, "market.duckdb"):
                pass
    finally:
        os.close(directory_fd)

    assert process.communicated is True
    assert release_pipe.closed is True


def test_inspect_worker_timeout_is_killed_reaped_and_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
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
    monkeypatch.setattr(adapter, "_start_worker", lambda *_args: process)
    monkeypatch.setattr(
        adapter,
        "_read_metadata",
        lambda _process: MarketSourceMetadata(4, "local_projection_v2_event_time"),
    )
    directory_fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        with pytest.raises(CutoverSafetyError, match="timed out"):
            adapter.inspect(directory_fd, "market.duckdb")
    finally:
        os.close(directory_fd)

    assert process.terminated is True
    assert process.killed is True
    assert process.communicated is True
    assert stdin_pipe.closed is True


def test_inspect_worker_pre_metadata_hang_is_bounded_and_reaped(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
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
    monkeypatch.setattr(adapter, "_start_worker", lambda *_args: process)
    directory_fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        with pytest.raises(CutoverSafetyError, match="metadata timed out"):
            adapter.inspect(directory_fd, "market.duckdb")
    finally:
        os.close(directory_fd)

    assert process.poll() is not None


def test_inspect_worker_partial_metadata_hang_is_bounded_and_reaped(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
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
    monkeypatch.setattr(adapter, "_start_worker", lambda *_args: process)
    directory_fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        with pytest.raises(CutoverSafetyError, match="metadata timed out"):
            adapter.inspect(directory_fd, "market.duckdb")
    finally:
        os.close(directory_fd)

    assert process.poll() is not None


def test_inspect_unkillable_worker_cleanup_remains_bounded_and_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
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
    monkeypatch.setattr(adapter, "_start_worker", lambda *_args: process)
    monkeypatch.setattr(
        adapter,
        "_read_metadata",
        lambda _process: MarketSourceMetadata(4, "local_projection_v2_event_time"),
    )
    directory_fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        with pytest.raises(CutoverSafetyError, match="shutdown failed"):
            adapter.inspect(directory_fd, "market.duckdb")
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
    monkeypatch.setattr(adapter, "_start_worker", lambda *_args: process)
    monkeypatch.setattr(
        adapter,
        "_read_metadata",
        lambda _process: MarketSourceMetadata(4, "local_projection_v2_event_time"),
    )
    directory_fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        with pytest.raises(market_v4_cutover.WorkerShutdownError) as captured:
            adapter.inspect(directory_fd, "market.duckdb")
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
