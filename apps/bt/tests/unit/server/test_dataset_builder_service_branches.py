from __future__ import annotations

import asyncio
import importlib
import json
import threading
from pathlib import Path
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

import src.application.services.dataset_builder_service as dataset_builder_service
from src.application.services.dataset_builder_service import (
    DatasetJobData,
    DatasetResult,
    _build_dataset,
    _convert_stocks,
    start_dataset_build,
)
from src.application.services.dataset_builder_copy_stages import (
    preflight_event_time_pit_source,
)
from src.application.services.dataset_presets import PresetConfig
from src.application.services.generic_job_manager import GenericJobManager
from src.application.contracts.jobs import JobStatus
from src.infrastructure.db.dataset_io.dataset_writer import (
    DatasetSnapshotError,
    DatasetWriter,
)
from src.infrastructure.db.market.market_reader import MarketDbReader
from src.infrastructure.db.market.market_db import MarketDb
from src.infrastructure.db.market.dataset_snapshot_reader import (
    DatasetSnapshotReader,
    validate_dataset_snapshot,
)
from tests.unit.server.db.test_dataset_event_time_basis_snapshot import (
    _build_v4_market_with_two_regimes,
)

_STATEMENT_COLUMNS = (
    "code", "disclosed_date", "earnings_per_share", "profit", "equity",
    "type_of_current_period", "type_of_document",
    "next_year_forecast_earnings_per_share", "bps", "sales",
    "operating_profit", "forecast_operating_profit",
    "next_year_forecast_operating_profit", "ordinary_profit",
    "operating_cash_flow", "dividend_fy", "forecast_dividend_fy",
    "next_year_forecast_dividend_fy", "payout_ratio",
    "forecast_payout_ratio", "next_year_forecast_payout_ratio", "forecast_eps",
    "investing_cash_flow", "financing_cash_flow", "cash_and_equivalents",
    "total_assets", "shares_outstanding", "treasury_shares",
)


@pytest.fixture
def isolated_dataset_manager(monkeypatch):
    manager: GenericJobManager = GenericJobManager()
    monkeypatch.setattr(dataset_builder_service, "dataset_job_manager", manager)
    return manager


@pytest.fixture(autouse=True)
def stub_manifest_writer_for_dummy_db_paths(monkeypatch, request):
    if request.node.name in {
        "test_build_dataset_writes_manifest_v3_payload_at_v2_path",
        "test_build_dataset_rerun_keeps_logical_checksum_reproducible",
        "test_build_dataset_manifest_uses_duckdb_state_as_sot",
        "test_build_dataset_direct_copy_generates_valid_snapshot_and_warnings",
        "test_builder_publishes_complete_event_time_bundle",
        "test_builder_omits_statements_after_persisted_snapshot_cutoff",
        "test_builder_pins_all_stages_to_one_source_vintage",
        "test_builder_cancellation_after_pit_copy_leaves_closed_partial_without_manifest",
        "test_real_cancel_job_waiting_during_replace_observes_completed_bundle",
        "test_real_cancel_immediately_after_publish_lock_release_sees_completed",
        "test_real_cancel_job_runs_while_manifest_checksum_worker_is_blocked",
    }:
        return

    def _fake_write_dataset_manifest(
        *,
        snapshot_path: str,
        dataset_name: str,
        preset_name: str,
        manifest_path=None,
    ) -> str:
        del dataset_name, preset_name
        if manifest_path is not None:
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            manifest_path.write_text("{}", encoding="utf-8")
            return str(manifest_path)
        return str(dataset_builder_service._manifest_path_for_snapshot(snapshot_path))

    monkeypatch.setattr(dataset_builder_service, "_write_dataset_manifest", _fake_write_dataset_manifest)


async def _create_job(
    manager: GenericJobManager,
    *,
    name: str = "dataset",
    preset: str = "quickTesting",
    overwrite: bool = False,
):
    job = await manager.create_job(
        DatasetJobData(
            name=name,
            preset=preset,
            overwrite=overwrite,
        )
    )
    assert job is not None
    return job


def _daily_bar_row() -> dict[str, int | str]:
    return {
        "Date": "2026-01-01",
        "O": 1,
        "H": 2,
        "L": 1,
        "C": 2,
        "Vo": 100,
    }


def _master_row(code: str, name: str) -> dict[str, str]:
    return {"Code": code, "MktNm": "プライム", "CoName": name}


def _statement_payload(
    code: str,
    disclosed_date: str,
    **values: object,
) -> tuple[object, ...]:
    payload: dict[str, object | None] = {
        column: None for column in _STATEMENT_COLUMNS
    }
    payload["code"] = code
    payload["disclosed_date"] = disclosed_date
    payload.update(values)
    return tuple(payload[column] for column in _STATEMENT_COLUMNS)


def _create_market_source_duckdb(base_dir: Path) -> Path:
    duckdb = importlib.import_module("duckdb")
    source_path = base_dir / "market.duckdb"
    market_db = MarketDb(str(source_path))
    market_db.close()
    conn = duckdb.connect(str(source_path))
    try:
        conn.executemany(
            "INSERT INTO stocks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    "1111",
                    "Alpha",
                    "ALPHA",
                    "0111",
                    "プライム",
                    "7",
                    "輸送用機器",
                    "3050",
                    "輸送用機器",
                    "TOPIX Core30",
                    "2001-01-01",
                    None,
                    None,
                ),
                (
                    "2222",
                    "Beta",
                    "BETA",
                    "0111",
                    "プライム",
                    "9",
                    "情報・通信業",
                    "5250",
                    "情報・通信業",
                    "TOPIX Large70",
                    "2002-02-02",
                    None,
                    None,
                ),
            ],
        )
        conn.executemany(
            "INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("1111", "2026-01-01", 10.0, 12.0, 9.0, 11.0, 1000, 1.0, "2026-01-01T00:00:00+00:00"),
                ("1111", "2026-01-02", 11.0, 13.0, 10.0, 12.0, None, 1.0, "2026-01-02T00:00:00+00:00"),
                ("11110", "2026-01-02", None, None, None, None, 1100, 1.1, "2026-01-02T01:00:00+00:00"),
                ("2222", "2026-01-01", 20.0, None, 19.0, 20.5, 2000, 1.0, "2026-01-01T00:00:00+00:00"),
            ],
        )
        conn.execute(
            "INSERT INTO topix_data VALUES (?, ?, ?, ?, ?, ?)",
            ("2026-01-01", 2000.0, 2010.0, 1990.0, 2005.0, "2026-01-01T00:00:00+00:00"),
        )
        conn.execute(
            "INSERT INTO indices_data VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("0040", "2026-01-01", 500.0, 510.0, 495.0, 505.0, "Sector 40", "2026-01-01T00:00:00+00:00"),
        )
        conn.executemany(
            "INSERT INTO margin_data VALUES (?, ?, ?, ?)",
            [
                ("1111", "2026-01-01", 1000.0, None),
                ("11110", "2026-01-01", 9999.0, 500.0),
                ("22220", "2026-01-01", 300.0, 200.0),
            ],
        )
        conn.executemany(
            f"INSERT INTO statements ({', '.join(_STATEMENT_COLUMNS)}) "
            f"VALUES ({', '.join('?' for _ in _STATEMENT_COLUMNS)})",
            [
                _statement_payload(
                    "11110",
                    "2026-01-01",
                    earnings_per_share=10.0,
                    profit=500.0,
                    forecast_eps=12.0,
                    type_of_current_period="FY",
                    type_of_document="AnnualReport",
                ),
                _statement_payload(
                    "22220",
                    "2026-01-01",
                    earnings_per_share=20.0,
                    profit=600.0,
                    forecast_eps=21.0,
                    type_of_current_period="FY",
                ),
            ],
        )
        conn.executemany(
            "INSERT INTO stock_data_raw VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)",
            [
                ("1111", "2026-01-01", 10.0, 12.0, 9.0, 11.0, 1000, 1.0),
                ("2222", "2026-01-01", 20.0, 21.0, 19.0, 20.5, 2000, 1.0),
            ],
        )
        conn.executemany(
            """
            INSERT INTO stock_master_daily VALUES (
                '2026-01-01', ?, ?, NULL, '0111', 'Prime', ?, ?,
                ?, ?, ?, ?, NULL
            )
            """,
            [
                ("1111", "Alpha", "7", "Transport", "3050", "Transport", "TOPIX Core30", "2001-01-01"),
                ("2222", "Beta", "9", "IT", "5250", "IT", "TOPIX Large70", "2002-02-02"),
            ],
        )
        conn.executemany(
            """
            INSERT INTO stock_adjustment_bases VALUES (
                ?, ?, '2026-01-01', NULL, '2026-01-01', ?,
                '2026-01-01', 'ready', NULL, NULL
            )
            """,
            [
                ("1111", "event-pit-v1:1111:2026-01-01", "fp-1111"),
                ("2222", "event-pit-v1:2222:2026-01-01", "fp-2222"),
            ],
        )
        conn.executemany(
            "INSERT INTO stock_adjustment_basis_segments VALUES (?, ?, '2026-01-01', NULL, 1.0)",
            [
                ("1111", "event-pit-v1:1111:2026-01-01"),
                ("2222", "event-pit-v1:2222:2026-01-01"),
            ],
        )
        conn.executemany(
            """
            INSERT INTO statement_metrics_adjusted (
                code, disclosed_date, period_end, period_type, price_basis_date,
                adjusted_eps, basis_version
            ) VALUES (?, '2026-01-01', '2026-01-01', 'FY', '2026-01-01', ?, ?)
            """,
            [
                ("1111", 10.0, "event-pit-v1:1111:2026-01-01"),
                ("2222", 20.0, "event-pit-v1:2222:2026-01-01"),
            ],
        )
        conn.executemany(
            """
            INSERT INTO daily_valuation (
                code, date, price_basis_date, close, eps, basis_version,
                statement_disclosed_date
            ) VALUES (?, '2026-01-01', '2026-01-01', ?, ?, ?, '2026-01-01')
            """,
            [
                ("1111", 11.0, 10.0, "event-pit-v1:1111:2026-01-01"),
                ("2222", 20.5, 20.0, "event-pit-v1:2222:2026-01-01"),
            ],
        )
    finally:
        conn.close()
    return source_path


def _create_builder_two_regime_source(base_dir: Path) -> Path:
    source_path = _build_v4_market_with_two_regimes(base_dir)
    duckdb = importlib.import_module("duckdb")
    conn = duckdb.connect(str(source_path))
    try:
        conn.execute(
            """
            INSERT INTO stocks VALUES (
                '7203', 'Toyota', NULL, '0111', 'Prime', '6', 'Auto',
                '3700', 'Transport', NULL, '1949-05-16', NULL, NULL
            )
            """
        )
        conn.executemany(
            """
            INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)
            """,
            [
                ("7203", "2024-01-04", 100.0, 110.0, 90.0, 105.0, 1000, 1.0),
                ("7203", "2024-06-28", 200.0, 210.0, 190.0, 205.0, 2000, 0.5),
                ("7203", "2024-12-30", 220.0, 230.0, 210.0, 225.0, 2200, 1.0),
            ],
        )
    finally:
        conn.close()
    return source_path


def test_convert_stocks_maps_jquants_fields():
    rows = _convert_stocks(
        [
            {
                "Code": "72030",
                "CoName": "Toyota",
                "Mkt": "0111",
                "MktNm": "プライム",
                "S17": "6",
                "S17Nm": "輸送用機器",
                "S33": "3700",
                "S33Nm": "輸送用機器",
                "ScaleCat": "TOPIX Core30",
                "Date": "2020-01-01",
            }
        ]
    )
    assert len(rows) == 1
    assert rows[0]["code"] == "7203"
    assert rows[0]["company_name"] == "Toyota"
    assert rows[0]["market_name"] == "プライム"


def test_immutable_market_snapshot_isolated_from_concurrent_source_mutation(tmp_path):
    source = _create_builder_two_regime_source(tmp_path)
    snapshot = tmp_path / "pinned.duckdb"

    dataset_builder_service._create_immutable_market_snapshot(str(source), snapshot)
    conn = importlib.import_module("duckdb").connect(str(source))
    try:
        conn.execute(
            "UPDATE stock_data_raw SET close = 9999 "
            "WHERE code = '7203' AND date = '2024-12-30'"
        )
    finally:
        conn.close()

    pinned = importlib.import_module("duckdb").connect(str(snapshot), read_only=True)
    try:
        close = pinned.execute(
            "SELECT close FROM stock_data_raw WHERE code = '7203' AND date = '2024-12-30'"
        ).fetchone()[0]
    finally:
        pinned.close()
    assert close == 225.0


def test_immutable_market_snapshot_works_with_live_read_only_source_connection(tmp_path):
    source = _create_builder_two_regime_source(tmp_path)
    snapshot = tmp_path / "pinned-with-reader.duckdb"
    duckdb = importlib.import_module("duckdb")
    live_reader_connection = duckdb.connect(str(source), read_only=True)
    try:
        assert live_reader_connection.execute("SELECT count(*) FROM stock_data_raw").fetchone()[0]

        dataset_builder_service._create_immutable_market_snapshot(str(source), snapshot)

        assert live_reader_connection.execute("SELECT count(*) FROM stock_data_raw").fetchone()[0]
    finally:
        live_reader_connection.close()

    pinned = duckdb.connect(str(snapshot), read_only=True)
    try:
        assert pinned.execute("SELECT count(*) FROM stock_data_raw").fetchone()[0] > 0
    finally:
        pinned.close()


@pytest.mark.asyncio
async def test_cancellation_waits_for_immutable_source_snapshot_worker(
    monkeypatch, isolated_dataset_manager, tmp_path
):
    job = await _create_job(
        isolated_dataset_manager,
        name="cancel-source-snapshot",
        preset="quickTesting",
    )
    source = tmp_path / "market.duckdb"
    source.touch()
    started = threading.Event()
    release = threading.Event()
    finished = threading.Event()

    def blocked_snapshot(_source, _target):
        started.set()
        release.wait(2)
        finished.set()

    monkeypatch.setattr(
        dataset_builder_service,
        "_create_immutable_market_snapshot",
        blocked_snapshot,
    )
    monkeypatch.setattr(
        dataset_builder_service,
        "get_preset",
        lambda _name: PresetConfig(markets=["prime"]),
    )
    task = asyncio.create_task(
        _build_dataset(
            job,
            MagicMock(),
            MagicMock(),
            source_duckdb_path=str(source),
        )
    )
    await asyncio.to_thread(started.wait, 1)

    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert finished.is_set()


@pytest.mark.asyncio
async def test_dataset_writer_worker_close_without_writer_is_noop():
    worker = dataset_builder_service._DatasetWriterWorker("/tmp/unused-dataset-writer")

    await worker.close()


@pytest.mark.asyncio
async def test_query_market_rows_awaits_coroutine_results():
    class _CoroutineReader:
        def query(self, sql: str, params: tuple[object, ...] = ()):
            del sql, params

            async def _rows():
                return [{"value": 1}]

            return _rows()

    rows = await dataset_builder_service._query_market_rows(cast(Any, _CoroutineReader()), "SELECT 1")

    assert rows == [{"value": 1}]


@pytest.mark.asyncio
async def test_build_dataset_returns_error_for_unknown_preset(isolated_dataset_manager):
    job = await _create_job(isolated_dataset_manager, preset="unknown")
    resolver = MagicMock()
    resolver.get_dataset_path.return_value = "/tmp/unknown"
    reader = MagicMock()

    result = await _build_dataset(
        job,
        resolver,
        reader,
        source_duckdb_path="/unused-for-unknown-preset",
    )
    assert result.success is False
    assert result.errors == ["Unknown preset: unknown"]


@pytest.mark.asyncio
async def test_build_dataset_rejects_missing_source_before_destination_access(
    isolated_dataset_manager,
):
    job = await _create_job(isolated_dataset_manager)
    resolver = MagicMock()

    with pytest.raises(ValueError, match="source_duckdb_path is required"):
        await _build_dataset(
            job,
            resolver,
            MagicMock(),
            source_duckdb_path=cast(Any, None),
        )

    resolver.get_dataset_path.assert_not_called()


@pytest.mark.asyncio
async def test_build_dataset_direct_copy_generates_valid_snapshot_and_warnings(
    monkeypatch,
    isolated_dataset_manager,
    tmp_path,
):
    job = await _create_job(isolated_dataset_manager, name="direct-copy", preset="quickTesting")
    resolver = MagicMock()
    resolver.get_dataset_path.return_value = str(tmp_path / "direct-copy")
    source_duckdb_path = _create_market_source_duckdb(tmp_path)

    preset = PresetConfig(
        markets=["prime"],
        include_topix=True,
        include_margin=True,
        include_sector_indices=True,
    )
    monkeypatch.setattr(dataset_builder_service, "get_preset", lambda _name: preset)

    class TrackingMarketReader(MarketDbReader):
        def __init__(self, db_path: str) -> None:
            super().__init__(db_path)
            self.close_calls = 0

        def close(self) -> None:
            self.close_calls += 1
            super().close()

    reader = TrackingMarketReader(str(source_duckdb_path))
    try:
        result = await _build_dataset(
            job,
            resolver,
            reader,
            source_duckdb_path=str(source_duckdb_path),
        )
        assert reader.close_calls == 0
    finally:
        reader.close()

    assert result.success is True
    assert result.totalStocks == 2
    assert result.processedStocks == 2
    assert result.warnings is not None
    assert any("No valid OHLCV rows for 1 stocks" in warning for warning in result.warnings)
    assert any("Skipped incomplete OHLCV rows for 1 stocks" in warning for warning in result.warnings)

    snapshot_dir = tmp_path / "direct-copy"
    manifest = validate_dataset_snapshot(snapshot_dir)
    assert manifest.dataset.name == "direct-copy"
    assert manifest.logicalCounts.stocks == 2
    assert manifest.logicalCounts.stock_data == 2
    assert manifest.logicalCounts.topix_data == 1
    assert manifest.logicalCounts.indices_data == 1
    assert manifest.logicalCounts.margin_data == 2
    assert manifest.logicalCounts.statements == 2
    assert manifest.coverage.totalStocks == 2
    assert manifest.coverage.stocksWithQuotes == 1
    assert manifest.coverage.stocksWithMargin == 2
    assert manifest.coverage.stocksWithStatements == 2


@pytest.mark.asyncio
async def test_builder_publishes_complete_event_time_bundle(
    monkeypatch,
    isolated_dataset_manager,
    tmp_path,
):
    job = await _create_job(
        isolated_dataset_manager,
        name="two-regime",
        preset="quickTesting",
    )
    resolver = MagicMock()
    resolver.get_dataset_path.return_value = str(tmp_path / "dataset")
    source = _create_builder_two_regime_source(tmp_path)
    preset = PresetConfig(
        markets=["prime"],
        include_topix=False,
        include_margin=False,
        include_sector_indices=False,
    )
    monkeypatch.setattr(dataset_builder_service, "get_preset", lambda _name: preset)
    reader = MarketDbReader(str(source))
    try:
        result = await _build_dataset(
            job,
            resolver,
            reader,
            source_duckdb_path=str(source),
        )
    finally:
        reader.close()

    assert result.success is True
    snapshot_dir = Path(result.outputPath)
    manifest = json.loads((snapshot_dir / "manifest.v2.json").read_text())
    assert manifest["schemaVersion"] == 3
    assert manifest["source"]["marketSchemaVersion"] == 4
    assert all(
        manifest["logicalCounts"][table] > 0
        for table in (
            "stock_data_raw",
            "stock_master_daily",
            "stock_adjustment_bases",
            "stock_adjustment_basis_segments",
            "statement_metrics_adjusted",
            "daily_valuation",
        )
    )
    snapshot_reader = DatasetSnapshotReader(snapshot_dir)
    try:
        assert snapshot_reader.resolve_adjustment_basis("7203", "2024-06-27").valid_from == "2024-01-04"
        assert snapshot_reader.resolve_adjustment_basis("7203", "2024-06-28").valid_from == "2024-06-28"
    finally:
        snapshot_reader.close()


@pytest.mark.asyncio
async def test_builder_omits_statements_after_persisted_snapshot_cutoff(
    monkeypatch,
    isolated_dataset_manager,
    tmp_path,
):
    job = await _create_job(
        isolated_dataset_manager,
        name="statement-cutoff",
        preset="quickTesting",
    )
    resolver = MagicMock()
    resolver.get_dataset_path.return_value = str(tmp_path / "statement-cutoff")
    source = _create_builder_two_regime_source(tmp_path)
    duckdb = importlib.import_module("duckdb")
    conn = duckdb.connect(str(source))
    try:
        conn.execute(
            """
            INSERT INTO statements (
                code, disclosed_date, earnings_per_share, type_of_current_period
            ) VALUES ('7203', '2025-01-01', 999.0, 'FY')
            """
        )
        conn.executemany(
            "INSERT INTO topix_data VALUES (?, ?, ?, ?, ?, NULL)",
            [
                ("2024-12-30", 100.0, 101.0, 99.0, 100.5),
                ("2025-01-01", 200.0, 201.0, 199.0, 200.5),
            ],
        )
        conn.executemany(
            "INSERT INTO indices_data VALUES (?, ?, ?, ?, ?, ?, ?, NULL)",
            [
                ("0040", "2024-12-30", 10.0, 11.0, 9.0, 10.5, "Sector 40"),
                ("0040", "2025-01-01", 20.0, 21.0, 19.0, 20.5, "Sector 40"),
            ],
        )
        conn.executemany(
            "INSERT INTO margin_data VALUES ('7203', ?, ?, ?)",
            [
                ("2024-12-30", 1000.0, 500.0),
                ("2025-01-01", 2000.0, 1000.0),
            ],
        )
    finally:
        conn.close()
    preset = PresetConfig(
        markets=["prime"],
        include_topix=True,
        include_margin=True,
        include_sector_indices=True,
    )
    monkeypatch.setattr(dataset_builder_service, "get_preset", lambda _name: preset)
    monkeypatch.setattr(
        dataset_builder_service,
        "get_index_catalog_codes",
        lambda: {"0040"},
    )
    reader = MarketDbReader(str(source))
    try:
        result = await _build_dataset(
            job,
            resolver,
            reader,
            source_duckdb_path=str(source),
        )
    finally:
        reader.close()

    assert result.success is True
    snapshot_reader = DatasetSnapshotReader(Path(result.outputPath))
    try:
        assert [
            str(row.disclosed_date)
            for row in snapshot_reader.get_statements("7203", actual_only=False)
        ] == ["2024-05-10"]
        assert str(snapshot_reader.get_stock_ohlcv("7203")[-1].date) == "2024-12-30"
        assert [str(row.date) for row in snapshot_reader.get_topix()] == ["2024-12-30"]
        assert [str(row.date) for row in snapshot_reader.get_index_data("0040")] == [
            "2024-12-30"
        ]
        assert [str(row.date) for row in snapshot_reader.get_margin("7203")] == [
            "2024-12-30"
        ]
        assert [
            str(row.disclosed_date)
            for row in snapshot_reader.get_statements_batch(
                ["7203"], actual_only=False
            )["7203"]
        ] == ["2024-05-10"]
    finally:
        snapshot_reader.close()


def test_event_time_source_preflight_requires_statements_table(tmp_path: Path) -> None:
    source = _create_builder_two_regime_source(tmp_path)
    duckdb = importlib.import_module("duckdb")
    conn = duckdb.connect(str(source))
    try:
        conn.execute("DROP TABLE statements")
    finally:
        conn.close()

    with pytest.raises(DatasetSnapshotError, match="statements"):
        preflight_event_time_pit_source(str(source))


@pytest.mark.asyncio
async def test_builder_pins_all_stages_to_one_source_vintage(
    monkeypatch,
    isolated_dataset_manager,
    tmp_path,
):
    job = await _create_job(
        isolated_dataset_manager,
        name="pinned-source",
        preset="quickTesting",
    )
    resolver = MagicMock()
    resolver.get_dataset_path.return_value = str(tmp_path / "pinned-source")
    source = _create_builder_two_regime_source(tmp_path)
    preset = PresetConfig(
        markets=["prime"],
        include_topix=False,
        include_margin=False,
        include_sector_indices=False,
    )
    monkeypatch.setattr(dataset_builder_service, "get_preset", lambda _name: preset)
    original_master_loader = dataset_builder_service._load_market_stock_master

    async def mutate_source_after_master_read(reader):
        result = await original_master_loader(reader)
        conn = importlib.import_module("duckdb").connect(str(source))
        try:
            conn.execute(
                "INSERT INTO stock_data VALUES "
                "('7203', '2025-01-02', 9999, 9999, 9999, 9999, 1, 1, NULL)"
            )
        finally:
            conn.close()
        return result

    monkeypatch.setattr(
        dataset_builder_service,
        "_load_market_stock_master",
        mutate_source_after_master_read,
    )
    reader = MarketDbReader(str(source))
    try:
        result = await _build_dataset(
            job,
            resolver,
            reader,
            source_duckdb_path=str(source),
        )
    finally:
        reader.close()

    assert result.success is True
    source_conn = importlib.import_module("duckdb").connect(str(source), read_only=True)
    try:
        assert source_conn.execute(
            "SELECT close FROM stock_data WHERE code = '7203' AND date = '2025-01-02'"
        ).fetchall() == [(9999.0,)]
    finally:
        source_conn.close()
    conn = importlib.import_module("duckdb").connect(
        str(Path(result.outputPath) / "dataset.duckdb"), read_only=True
    )
    try:
        rows = conn.execute(
            "SELECT close FROM stock_data WHERE code = '7203' AND date = '2025-01-02'"
        ).fetchall()
    finally:
        conn.close()
    assert rows == []


@pytest.mark.asyncio
async def test_builder_cancellation_after_pit_copy_leaves_closed_partial_without_manifest(
    monkeypatch,
    isolated_dataset_manager,
    tmp_path,
):
    job = await _create_job(
        isolated_dataset_manager,
        name="cancel-pit",
        preset="quickTesting",
    )
    resolver = MagicMock()
    snapshot_dir = tmp_path / "cancel-pit"
    resolver.get_dataset_path.return_value = str(snapshot_dir)
    source = _create_builder_two_regime_source(tmp_path)
    preset = PresetConfig(
        markets=["prime"],
        include_topix=False,
        include_margin=False,
        include_sector_indices=False,
    )
    monkeypatch.setattr(dataset_builder_service, "get_preset", lambda _name: preset)

    class CancellingWriter(DatasetWriter):
        instances: list["CancellingWriter"] = []

        def __init__(self, path: str) -> None:
            super().__init__(path)
            self.closed_for_test = False
            self.__class__.instances.append(self)

        def copy_event_time_pit_from_source(self, **kwargs):
            result = super().copy_event_time_pit_from_source(**kwargs)
            job.cancelled.set()
            return result

        def close(self) -> None:
            super().close()
            self.closed_for_test = True

    monkeypatch.setattr(dataset_builder_service, "DatasetWriter", CancellingWriter)
    reader = MarketDbReader(str(source))
    try:
        result = await _build_dataset(
            job,
            resolver,
            reader,
            source_duckdb_path=str(source),
        )
    finally:
        reader.close()

    assert result.success is False
    assert result.errors == ["Cancelled"]
    assert CancellingWriter.instances
    assert all(writer.closed_for_test for writer in CancellingWriter.instances)
    assert not (snapshot_dir / "manifest.v2.json").exists()


@pytest.mark.asyncio
async def test_builder_cancellation_during_manifest_checksum_never_publishes_manifest(
    monkeypatch,
    isolated_dataset_manager,
    tmp_path,
):
    job = await _create_job(
        isolated_dataset_manager,
        name="cancel-manifest",
        preset="quickTesting",
    )
    resolver = MagicMock()
    snapshot_dir = tmp_path / "cancel-manifest"
    resolver.get_dataset_path.return_value = str(snapshot_dir)
    source = _create_builder_two_regime_source(tmp_path)
    preset = PresetConfig(
        markets=["prime"],
        include_topix=False,
        include_margin=False,
        include_sector_indices=False,
    )
    monkeypatch.setattr(dataset_builder_service, "get_preset", lambda _name: preset)

    def cancel_after_checksums(*, manifest_path: Path, **_kwargs) -> str:
        manifest_path.write_text('{"complete": true}', encoding="utf-8")
        job.cancelled.set()
        return str(manifest_path)

    monkeypatch.setattr(dataset_builder_service, "_write_dataset_manifest", cancel_after_checksums)
    reader = MarketDbReader(str(source))
    try:
        result = await _build_dataset(
            job,
            resolver,
            reader,
            source_duckdb_path=str(source),
        )
    finally:
        reader.close()

    assert result.success is False
    assert result.errors == ["Cancelled"]
    assert not (snapshot_dir / "manifest.v2.json").exists()


@pytest.mark.asyncio
async def test_real_cancel_job_runs_while_manifest_checksum_worker_is_blocked(
    monkeypatch,
    isolated_dataset_manager,
    tmp_path,
):
    resolver = MagicMock()
    snapshot_dir = tmp_path / "cancel-checksum-worker"
    resolver.get_dataset_path.return_value = str(snapshot_dir)
    source = _create_builder_two_regime_source(tmp_path)
    preset = PresetConfig(
        markets=["prime"],
        include_topix=False,
        include_margin=False,
        include_sector_indices=False,
    )
    monkeypatch.setattr(dataset_builder_service, "get_preset", lambda _name: preset)
    original_write = dataset_builder_service._write_dataset_manifest
    entered = threading.Event()
    release = threading.Event()
    worker_timed_out = threading.Event()

    def blocked_checksum(**kwargs):
        entered.set()
        if not release.wait(0.5):
            worker_timed_out.set()
        return original_write(**kwargs)

    monkeypatch.setattr(dataset_builder_service, "_write_dataset_manifest", blocked_checksum)
    reader = MarketDbReader(str(source))
    job = await start_dataset_build(
        DatasetJobData(name="cancel-checksum-worker", preset="quickTesting"),
        resolver,
        reader,
        str(source),
    )
    assert job is not None and job.task is not None
    try:
        assert await asyncio.to_thread(entered.wait, 1.0)
        cancel_task = asyncio.create_task(
            isolated_dataset_manager.cancel_job(job.job_id)
        )
        while not job.cancelled.is_set():
            await asyncio.sleep(0)
        assert not cancel_task.done()
        assert job.status != JobStatus.CANCELLED
        release.set()
        assert await cancel_task is True
        await job.task
    finally:
        release.set()
        reader.close()

    assert not worker_timed_out.is_set()
    assert job.status == JobStatus.CANCELLED
    assert job.result is None
    assert not (snapshot_dir / "manifest.v2.json").exists()
    assert not list(snapshot_dir.glob(".manifest.v2.json.*.tmp"))


@pytest.mark.asyncio
async def test_real_cancel_job_waiting_during_replace_observes_completed_bundle(
    monkeypatch,
    isolated_dataset_manager,
    tmp_path,
):
    resolver = MagicMock()
    snapshot_dir = tmp_path / "cancel-during-replace"
    resolver.get_dataset_path.return_value = str(snapshot_dir)
    source = _create_builder_two_regime_source(tmp_path)
    preset = PresetConfig(
        markets=["prime"], include_topix=False,
        include_margin=False, include_sector_indices=False,
    )
    monkeypatch.setattr(dataset_builder_service, "get_preset", lambda _name: preset)
    loop = asyncio.get_running_loop()
    original_replace = Path.replace
    cancel_result: list[bool] = []
    cancel_thread: list[threading.Thread] = []

    def racing_replace(path: Path, target: Path):
        if path.name.startswith(".manifest.v2.json"):
            def request_cancel() -> None:
                future = asyncio.run_coroutine_threadsafe(
                    isolated_dataset_manager.cancel_job(job.job_id, wait=False),
                    loop,
                )
                cancel_result.append(future.result(timeout=2.0))

            thread = threading.Thread(target=request_cancel)
            cancel_thread.append(thread)
            thread.start()
        return original_replace(path, target)

    monkeypatch.setattr(Path, "replace", racing_replace)
    reader = MarketDbReader(str(source))
    job = await start_dataset_build(
        DatasetJobData(name="cancel-during-replace", preset="quickTesting"),
        resolver,
        reader,
        str(source),
    )
    assert job is not None and job.task is not None
    try:
        await job.task
        for thread in cancel_thread:
            await asyncio.to_thread(thread.join, 2.0)
    finally:
        reader.close()

    assert cancel_result == [False]
    assert job.status == JobStatus.COMPLETED
    assert job.result is not None and job.result.success is True
    assert isolated_dataset_manager.get_active_job() is None
    assert validate_dataset_snapshot(snapshot_dir).schemaVersion == 3
    assert not list(snapshot_dir.glob(".manifest.v2.json.*.tmp"))


@pytest.mark.asyncio
async def test_real_cancel_immediately_after_publish_lock_release_sees_completed(
    monkeypatch,
    isolated_dataset_manager,
    tmp_path,
):
    resolver = MagicMock()
    snapshot_dir = tmp_path / "cancel-after-publish"
    resolver.get_dataset_path.return_value = str(snapshot_dir)
    source = _create_builder_two_regime_source(tmp_path)
    preset = PresetConfig(
        markets=["prime"], include_topix=False,
        include_margin=False, include_sector_indices=False,
    )
    monkeypatch.setattr(dataset_builder_service, "get_preset", lambda _name: preset)
    original_complete = isolated_dataset_manager.complete_job_with_publication
    published = asyncio.Event()
    allow_builder_return = asyncio.Event()

    async def gated_complete(*args, **kwargs):
        result = await original_complete(*args, **kwargs)
        published.set()
        await allow_builder_return.wait()
        return result

    monkeypatch.setattr(
        isolated_dataset_manager,
        "complete_job_with_publication",
        gated_complete,
    )
    reader = MarketDbReader(str(source))
    job = await start_dataset_build(
        DatasetJobData(name="cancel-after-publish", preset="quickTesting"),
        resolver,
        reader,
        str(source),
    )
    assert job is not None and job.task is not None
    try:
        await published.wait()
        cancel_won = await isolated_dataset_manager.cancel_job(job.job_id, wait=False)
        allow_builder_return.set()
        await job.task
    finally:
        allow_builder_return.set()
        reader.close()

    assert cancel_won is False
    assert job.status == JobStatus.COMPLETED
    assert job.result is not None and job.result.success is True
    assert isolated_dataset_manager.get_active_job() is None
    assert validate_dataset_snapshot(snapshot_dir).schemaVersion == 3
    assert not list(snapshot_dir.glob(".manifest.v2.json.*.tmp"))
@pytest.mark.asyncio
async def test_builder_v4_preflight_failure_preserves_overwrite_target(
    isolated_dataset_manager,
    tmp_path,
):
    job = await _create_job(
        isolated_dataset_manager,
        name="preflight",
        preset="quickTesting",
        overwrite=True,
    )
    resolver = MagicMock()
    snapshot_dir = tmp_path / "preflight"
    snapshot_dir.mkdir()
    sentinel = snapshot_dir / "keep.txt"
    sentinel.write_text("unchanged", encoding="utf-8")
    resolver.get_dataset_path.return_value = str(snapshot_dir)
    resolver.get_artifact_paths.return_value = [str(snapshot_dir)]
    source = _create_builder_two_regime_source(tmp_path)
    duckdb = importlib.import_module("duckdb")
    conn = duckdb.connect(str(source))
    try:
        conn.execute("DELETE FROM market_schema_version")
        conn.execute("INSERT INTO market_schema_version VALUES (3, '2026-07-14', 'legacy')")
    finally:
        conn.close()
    reader = MarketDbReader(str(source))
    try:
        with pytest.raises(RuntimeError, match="Market schema version 4"):
            await _build_dataset(
                job,
                resolver,
                reader,
                source_duckdb_path=str(source),
            )
    finally:
        reader.close()

    assert sentinel.read_text(encoding="utf-8") == "unchanged"


@pytest.mark.asyncio
async def test_start_dataset_build_marks_failed_on_timeout(
    monkeypatch, isolated_dataset_manager, tmp_path
):
    data = DatasetJobData(name="timeout", preset="quickTesting")
    resolver = MagicMock()
    client = AsyncMock()
    source = tmp_path / "market.duckdb"
    source.touch()
    client.db_path = str(source)

    async def fake_wait_for(coro, timeout):
        coro.close()
        raise asyncio.TimeoutError()

    monkeypatch.setattr(dataset_builder_service.asyncio, "wait_for", fake_wait_for)

    job = await start_dataset_build(data, resolver, client, str(source))
    assert job is not None
    assert job.task is not None
    await job.task

    stored = isolated_dataset_manager.get_job(job.job_id)
    assert stored is not None
    assert stored.status == JobStatus.FAILED
    assert stored.error == "Dataset build timed out after 35 minutes"


@pytest.mark.asyncio
async def test_start_dataset_build_uses_fixed_timeout(
    monkeypatch, isolated_dataset_manager, tmp_path
):
    data = DatasetJobData(name="timeout-fixed", preset="quickTesting")
    resolver = MagicMock()
    client = AsyncMock()
    source = tmp_path / "market.duckdb"
    source.touch()
    client.db_path = str(source)
    timeout_values: list[int] = []

    async def fake_wait_for(coro, timeout):
        coro.close()
        timeout_values.append(timeout)
        raise asyncio.TimeoutError()

    monkeypatch.setattr(dataset_builder_service.asyncio, "wait_for", fake_wait_for)

    job = await start_dataset_build(data, resolver, client, str(source))
    assert job is not None
    assert job.task is not None
    await job.task

    stored = isolated_dataset_manager.get_job(job.job_id)
    assert stored is not None
    assert stored.status == JobStatus.FAILED
    assert stored.error == "Dataset build timed out after 35 minutes"
    assert timeout_values == [35 * 60]


@pytest.mark.asyncio
async def test_start_dataset_build_marks_failed_on_unexpected_error(
    monkeypatch, isolated_dataset_manager, tmp_path
):
    data = DatasetJobData(name="error", preset="quickTesting")
    resolver = MagicMock()
    client = AsyncMock()
    source = tmp_path / "market.duckdb"
    source.touch()
    client.db_path = str(source)

    async def fake_wait_for(coro, timeout):
        coro.close()
        raise RuntimeError("dataset exploded")

    monkeypatch.setattr(dataset_builder_service.asyncio, "wait_for", fake_wait_for)

    job = await start_dataset_build(data, resolver, client, str(source))
    assert job is not None
    assert job.task is not None
    await job.task

    stored = isolated_dataset_manager.get_job(job.job_id)
    assert stored is not None
    assert stored.status == JobStatus.FAILED
    assert stored.error == "dataset exploded"


@pytest.mark.asyncio
async def test_start_dataset_build_handles_cancelled_error(
    monkeypatch, isolated_dataset_manager, tmp_path
):
    data = DatasetJobData(name="cancelled", preset="quickTesting")
    resolver = MagicMock()
    client = AsyncMock()
    source = tmp_path / "market.duckdb"
    source.touch()
    client.db_path = str(source)

    async def fake_wait_for(coro, timeout):
        coro.close()
        raise asyncio.CancelledError()

    monkeypatch.setattr(dataset_builder_service.asyncio, "wait_for", fake_wait_for)

    job = await start_dataset_build(data, resolver, client, str(source))
    assert job is not None
    assert job.task is not None
    await job.task

    stored = isolated_dataset_manager.get_job(job.job_id)
    assert stored is not None
    assert stored.status == JobStatus.PENDING
    assert stored.error is None


@pytest.mark.asyncio
async def test_start_dataset_build_skips_complete_when_job_cancelled(
    monkeypatch, isolated_dataset_manager, tmp_path
):
    data = DatasetJobData(name="skip-complete", preset="quickTesting")
    resolver = MagicMock()
    client = AsyncMock()
    source = tmp_path / "market.duckdb"
    source.touch()
    client.db_path = str(source)

    async def fake_build(job, resolver, client, *, source_duckdb_path=None):
        del resolver, client, source_duckdb_path
        job.cancelled.set()
        return DatasetResult(success=True, totalStocks=1, processedStocks=1)

    monkeypatch.setattr(dataset_builder_service, "_build_dataset", fake_build)

    job = await start_dataset_build(data, resolver, client, str(source))
    assert job is not None
    assert job.task is not None
    await job.task

    stored = isolated_dataset_manager.get_job(job.job_id)
    assert stored is not None
    assert stored.status == JobStatus.PENDING
    assert stored.result is None


@pytest.mark.asyncio
async def test_start_dataset_build_completes_when_not_cancelled(
    monkeypatch, isolated_dataset_manager, tmp_path
):
    data = DatasetJobData(name="completed", preset="quickTesting")
    resolver = MagicMock()
    client = AsyncMock()
    source = tmp_path / "market.duckdb"
    source.touch()
    client.db_path = str(source)

    async def fake_build(job, resolver, client, *, source_duckdb_path=None):
        del job, resolver, client, source_duckdb_path
        return DatasetResult(success=True, totalStocks=1, processedStocks=1, outputPath="/tmp/completed.db")

    monkeypatch.setattr(dataset_builder_service, "_build_dataset", fake_build)

    job = await start_dataset_build(data, resolver, client, str(source))
    assert job is not None
    assert job.task is not None
    await job.task

    stored = isolated_dataset_manager.get_job(job.job_id)
    assert stored is not None
    assert stored.status == JobStatus.COMPLETED
    assert stored.result is not None
    assert stored.result.success is True
