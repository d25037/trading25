from __future__ import annotations

import asyncio
import importlib
import inspect
import json
import sqlite3
import threading
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock, MagicMock, call

import pytest

import src.application.services.dataset_builder_service as dataset_builder_service
from src.application.services.dataset_builder_service import (
    DatasetJobData,
    DatasetResult,
    _build_dataset,
    _convert_stocks,
    start_dataset_build,
)
from src.application.services.dataset_presets import PresetConfig
from src.application.services.generic_job_manager import GenericJobManager
from src.entrypoints.http.schemas.job import JobStatus
from src.infrastructure.db.dataset_io.dataset_writer import StockDataCopyCodeStats
from src.infrastructure.db.market.market_reader import MarketDbReader
from src.infrastructure.db.market.dataset_snapshot_reader import validate_dataset_snapshot
from src.infrastructure.db.market.query_helpers import expand_stock_code, normalize_stock_code


@pytest.fixture
def isolated_dataset_manager(monkeypatch):
    manager: GenericJobManager = GenericJobManager()
    monkeypatch.setattr(dataset_builder_service, "dataset_job_manager", manager)
    return manager


@pytest.fixture(autouse=True)
def stub_manifest_writer_for_dummy_db_paths(monkeypatch, request):
    if request.node.name in {
        "test_build_dataset_writes_manifest_v2",
        "test_build_dataset_rerun_keeps_logical_checksum_reproducible",
        "test_build_dataset_manifest_uses_duckdb_state_as_sot",
        "test_build_dataset_direct_copy_generates_valid_snapshot_and_warnings",
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


class _LegacyEndpointMarketReader:
    def __init__(
        self,
        fetch: Callable[[str, dict[str, str] | None], list[dict[str, object]] | Awaitable[list[dict[str, object]]]],
    ) -> None:
        self._fetch = fetch

    def _call(self, path: str, params: dict[str, str] | None = None) -> list[dict[str, object]]:
        result = self._fetch(path, params)
        if inspect.isawaitable(result):
            return asyncio.run(_await_rows(result))
        return result

    def query(self, sql: str, params: tuple[object, ...] = ()) -> list[dict[str, object]]:
        normalized = " ".join(sql.split()).lower()
        if " from stocks " in f" {normalized} ":
            rows = self._call("/equities/master")
            return [
                {
                    "code": normalize_stock_code(str(row.get("Code", "") or "")),
                    "company_name": str(row.get("CoName", "") or ""),
                    "company_name_english": row.get("CoNameEn"),
                    "market_code": str(row.get("Mkt", "") or ""),
                    "market_name": str(row.get("MktNm", "") or ""),
                    "sector_17_code": str(row.get("S17", "") or ""),
                    "sector_17_name": str(row.get("S17Nm", "") or ""),
                    "sector_33_code": str(row.get("S33", "") or ""),
                    "sector_33_name": str(row.get("S33Nm", "") or ""),
                    "scale_category": row.get("ScaleCat"),
                    "listed_date": str(row.get("Date", "") or ""),
                }
                for row in rows
            ]
        if " from stock_data " in f" {normalized} ":
            result: list[dict[str, object]] = []
            for code in sorted({normalize_stock_code(str(value)) for value in params}):
                for row in self._call("/equities/bars/daily", {"code": expand_stock_code(code)}):
                    result.append(
                        {
                            "code": code,
                            "date": row.get("Date"),
                            "open": row.get("O"),
                            "high": row.get("H"),
                            "low": row.get("L"),
                            "close": row.get("C"),
                            "volume": row.get("Vo"),
                            "adjustment_factor": row.get("AdjFactor"),
                            "created_at": row.get("created_at"),
                        }
                    )
            return result
        if " from topix_data " in f" {normalized} ":
            return [
                {
                    "date": row.get("Date"),
                    "open": row.get("O"),
                    "high": row.get("H"),
                    "low": row.get("L"),
                    "close": row.get("C"),
                    "created_at": row.get("created_at"),
                }
                for row in self._call("/indices/bars/daily/topix")
            ]
        if " from indices_data " in f" {normalized} ":
            result = []
            for code in sorted({str(value) for value in params}):
                result.extend(
                    {
                        "code": row.get("Code", code),
                        "date": row.get("Date"),
                        "open": row.get("O"),
                        "high": row.get("H"),
                        "low": row.get("L"),
                        "close": row.get("C"),
                        "sector_name": row.get("SectorName"),
                        "created_at": row.get("created_at"),
                    }
                    for row in self._call("/indices/bars/daily", {"code": code})
                )
            return result
        if " from statements " in f" {normalized} ":
            result = []
            for code in sorted({normalize_stock_code(str(value)) for value in params}):
                for row in self._call("/fins/summary", {"code": expand_stock_code(code)}):
                    payload: dict[str, object] = {
                        column: None for column in dataset_builder_service._STATEMENT_COLUMNS
                    }
                    payload["code"] = code
                    payload["disclosed_date"] = row.get("DisclosedDate") or row.get("DiscDate")
                    result.append(payload)
            return result
        if " from margin_data " in f" {normalized} ":
            result = []
            for code in sorted({normalize_stock_code(str(value)) for value in params}):
                for row in self._call("/markets/margin-interest", {"code": expand_stock_code(code)}):
                    result.append(
                        {
                            "code": code,
                            "date": row.get("Date"),
                            "long_margin_volume": row.get("LongVol"),
                            "short_margin_volume": row.get("ShrtVol"),
                        }
                    )
            return result
        raise AssertionError(f"Unexpected query: {sql}")

def _reader_from_fetch(fetcher) -> _LegacyEndpointMarketReader:
    return _LegacyEndpointMarketReader(fetcher)


async def _await_rows(result: Awaitable[list[dict[str, object]]]) -> list[dict[str, object]]:
    return await result


def _statement_payload(
    code: str,
    disclosed_date: str,
    **values: object,
) -> tuple[object, ...]:
    payload: dict[str, object | None] = {
        column: None for column in dataset_builder_service._STATEMENT_COLUMNS
    }
    payload["code"] = code
    payload["disclosed_date"] = disclosed_date
    payload.update(values)
    return tuple(payload[column] for column in dataset_builder_service._STATEMENT_COLUMNS)


def _create_market_source_duckdb(base_dir: Path) -> Path:
    duckdb = importlib.import_module("duckdb")
    source_path = base_dir / "market.duckdb"
    conn = duckdb.connect(str(source_path))
    try:
        conn.execute(
            """
            CREATE TABLE stocks (
                code TEXT PRIMARY KEY,
                company_name TEXT NOT NULL,
                company_name_english TEXT,
                market_code TEXT NOT NULL,
                market_name TEXT NOT NULL,
                sector_17_code TEXT NOT NULL,
                sector_17_name TEXT NOT NULL,
                sector_33_code TEXT NOT NULL,
                sector_33_name TEXT NOT NULL,
                scale_category TEXT,
                listed_date TEXT NOT NULL,
                created_at TEXT,
                updated_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE stock_data (
                code TEXT,
                date TEXT,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT,
                adjustment_factor DOUBLE,
                created_at TEXT,
                PRIMARY KEY (code, date)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE topix_data (
                date TEXT PRIMARY KEY,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                created_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE indices_data (
                code TEXT,
                date TEXT,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                sector_name TEXT,
                created_at TEXT,
                PRIMARY KEY (code, date)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE margin_data (
                code TEXT,
                date TEXT,
                long_margin_volume DOUBLE,
                short_margin_volume DOUBLE,
                PRIMARY KEY (code, date)
            )
            """
        )
        conn.execute(
            f"""
            CREATE TABLE statements (
                {", ".join(
                    f"{column} {'TEXT' if column in ('code', 'disclosed_date', 'type_of_current_period', 'type_of_document') else 'DOUBLE'}"
                    for column in dataset_builder_service._STATEMENT_COLUMNS
                )},
                PRIMARY KEY (code, disclosed_date)
            )
            """
        )

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
            f"INSERT INTO statements VALUES ({', '.join('?' for _ in dataset_builder_service._STATEMENT_COLUMNS)})",
            [
                _statement_payload(
                    "1111",
                    "2026-01-31",
                    earnings_per_share=10.0,
                    profit=None,
                    type_of_current_period="FY",
                    type_of_document="AnnualReport",
                ),
                _statement_payload(
                    "11110",
                    "2026-01-31",
                    earnings_per_share=99.0,
                    profit=500.0,
                    forecast_eps=12.0,
                ),
                _statement_payload(
                    "22220",
                    "2026-01-31",
                    earnings_per_share=20.0,
                    profit=600.0,
                    forecast_eps=21.0,
                ),
            ],
        )
    finally:
        conn.close()
    return source_path


class _StaticRowsMarketReader:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self._rows = rows

    def query(self, sql: str, params: tuple[object, ...] = ()) -> list[dict[str, object]]:
        del sql, params
        return list(self._rows)


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


@pytest.mark.asyncio
async def test_dataset_writer_worker_close_without_writer_is_noop():
    worker = dataset_builder_service._DatasetWriterWorker("/tmp/unused-dataset-writer")

    await worker.close()


def test_normalize_index_code_handles_empty_and_short_numeric_values():
    assert dataset_builder_service._normalize_index_code(None) == ""
    assert dataset_builder_service._normalize_index_code("40") == "0040"


@pytest.mark.asyncio
async def test_query_market_rows_awaits_coroutine_results():
    class _CoroutineReader:
        def query(self, sql: str, params: tuple[object, ...] = ()):
            del sql, params

            async def _rows():
                return [{"value": 1}]

            return _rows()

    rows = await dataset_builder_service._query_market_rows(_CoroutineReader(), "SELECT 1")

    assert rows == [{"value": 1}]


@pytest.mark.asyncio
async def test_load_market_index_data_batch_returns_empty_when_no_codes():
    rows = await dataset_builder_service._load_market_index_data_batch(_StaticRowsMarketReader([]), [])

    assert rows == {}


@pytest.mark.asyncio
async def test_load_market_index_data_batch_skips_rows_without_date_or_code():
    reader = _StaticRowsMarketReader(
        [
            {
                "code": "0040",
                "date": "",
                "open": 1,
                "high": 1,
                "low": 1,
                "close": 1,
                "sector_name": "Sector",
                "created_at": "2026-01-01T00:00:00+00:00",
            },
            {
                "code": None,
                "date": "2026-01-01",
                "open": 1,
                "high": 1,
                "low": 1,
                "close": 1,
                "sector_name": "Sector",
                "created_at": "2026-01-01T00:00:00+00:00",
            },
        ]
    )

    rows = await dataset_builder_service._load_market_index_data_batch(reader, ["0040"])

    assert rows == {}


@pytest.mark.asyncio
async def test_load_market_stock_data_batch_merges_alias_rows_before_validation():
    reader = _StaticRowsMarketReader(
        [
            {
                "code": "1111",
                "date": "2026-01-01",
                "open": None,
                "high": 12,
                "low": 9,
                "close": 11,
                "volume": 100,
                "adjustment_factor": None,
                "created_at": None,
            },
            {
                "code": "11110",
                "date": "2026-01-01",
                "open": 10,
                "high": 12,
                "low": 9,
                "close": 11,
                "volume": 100,
                "adjustment_factor": 1.0,
                "created_at": "2026-01-02T00:00:00+00:00",
            },
        ]
    )

    rows = await dataset_builder_service._load_market_stock_data_batch(reader, ["1111"])

    assert rows == {
        "1111": [
            {
                "Date": "2026-01-01",
                "O": 10,
                "H": 12,
                "L": 9,
                "C": 11,
                "Vo": 100,
                "AdjFactor": 1.0,
                "created_at": "2026-01-02T00:00:00+00:00",
            }
        ]
    }


@pytest.mark.asyncio
async def test_load_market_statements_batch_merges_alias_rows_by_disclosed_date():
    reader = _StaticRowsMarketReader(
        [
            {
                "code": "1111",
                "disclosed_date": "2026-01-01",
                "earnings_per_share": 1.0,
                "profit": 10.0,
                "equity": 20.0,
                "type_of_current_period": "FY",
                "type_of_document": "Earnings",
                "next_year_forecast_earnings_per_share": None,
                "bps": 30.0,
                "sales": 40.0,
                "operating_profit": 50.0,
                "ordinary_profit": 60.0,
                "operating_cash_flow": 70.0,
                "dividend_fy": 1.5,
                "forecast_dividend_fy": None,
                "next_year_forecast_dividend_fy": None,
                "payout_ratio": 15.0,
                "forecast_payout_ratio": None,
                "next_year_forecast_payout_ratio": None,
                "forecast_eps": None,
                "investing_cash_flow": None,
                "financing_cash_flow": None,
                "cash_and_equivalents": None,
                "total_assets": 80.0,
                "shares_outstanding": None,
                "treasury_shares": None,
            },
            {
                "code": "11110",
                "disclosed_date": "2026-01-01",
                "earnings_per_share": None,
                "profit": None,
                "equity": None,
                "type_of_current_period": None,
                "type_of_document": None,
                "next_year_forecast_earnings_per_share": 2.0,
                "bps": None,
                "sales": None,
                "operating_profit": None,
                "ordinary_profit": None,
                "operating_cash_flow": None,
                "dividend_fy": None,
                "forecast_dividend_fy": 1.8,
                "next_year_forecast_dividend_fy": 2.1,
                "payout_ratio": None,
                "forecast_payout_ratio": 22.0,
                "next_year_forecast_payout_ratio": 24.0,
                "forecast_eps": 2.2,
                "investing_cash_flow": 5.0,
                "financing_cash_flow": 6.0,
                "cash_and_equivalents": 7.0,
                "total_assets": None,
                "shares_outstanding": 1000.0,
                "treasury_shares": 100.0,
            },
        ]
    )

    rows = await dataset_builder_service._load_market_statements_batch(reader, ["1111"])

    assert rows == {
        "1111": [
            {
                "code": "1111",
                "disclosed_date": "2026-01-01",
                "earnings_per_share": 1.0,
                "profit": 10.0,
                "equity": 20.0,
                "type_of_current_period": "FY",
                "type_of_document": "Earnings",
                "next_year_forecast_earnings_per_share": 2.0,
                "bps": 30.0,
                "sales": 40.0,
                "operating_profit": 50.0,
                "ordinary_profit": 60.0,
                "operating_cash_flow": 70.0,
                "dividend_fy": 1.5,
                "forecast_dividend_fy": 1.8,
                "next_year_forecast_dividend_fy": 2.1,
                "payout_ratio": 15.0,
                "forecast_payout_ratio": 22.0,
                "next_year_forecast_payout_ratio": 24.0,
                "forecast_eps": 2.2,
                "investing_cash_flow": 5.0,
                "financing_cash_flow": 6.0,
                "cash_and_equivalents": 7.0,
                "total_assets": 80.0,
                "shares_outstanding": 1000.0,
                "treasury_shares": 100.0,
            }
        ]
    }


@pytest.mark.asyncio
async def test_build_dataset_returns_error_for_unknown_preset(isolated_dataset_manager):
    job = await _create_job(isolated_dataset_manager, preset="unknown")
    resolver = MagicMock()
    resolver.get_dataset_path.return_value = "/tmp/unknown.db"
    reader = MagicMock()

    result = await _build_dataset(job, resolver, reader)
    assert result.success is False
    assert result.errors == ["Unknown preset: unknown"]


@pytest.mark.asyncio
async def test_build_dataset_returns_cancelled_before_master_fetch(monkeypatch, isolated_dataset_manager):
    job = await _create_job(isolated_dataset_manager, preset="quickTesting")
    job.cancelled.set()
    resolver = MagicMock()
    resolver.get_dataset_path.return_value = "/tmp/cancelled.db"
    reader = MagicMock()

    monkeypatch.setattr(
        dataset_builder_service,
        "get_preset",
        lambda name: PresetConfig(markets=["prime"]),
    )

    result = await _build_dataset(job, resolver, reader)
    assert result.success is False
    assert result.errors == ["Cancelled"]
    reader.query.assert_not_called()


@pytest.mark.asyncio
async def test_build_dataset_returns_error_when_no_stock_matches_preset(monkeypatch, isolated_dataset_manager):
    job = await _create_job(isolated_dataset_manager, preset="quickTesting")
    resolver = MagicMock()
    resolver.get_dataset_path.return_value = "/tmp/empty.db"

    async def fake_get_paginated(path: str, params=None):
        del params
        assert path == "/equities/master"
        return [{"Code": "99990", "MktNm": "グロース"}]

    reader = _reader_from_fetch(fake_get_paginated)

    monkeypatch.setattr(
        dataset_builder_service,
        "get_preset",
        lambda name: PresetConfig(markets=["prime"]),
    )

    result = await _build_dataset(job, resolver, reader)
    assert result.success is False
    assert result.errors == ["No stocks matched the preset filters"]


@pytest.mark.asyncio
async def test_build_dataset_success_copies_all_enabled_tables(monkeypatch, isolated_dataset_manager):
    job = await _create_job(isolated_dataset_manager, preset="full")
    resolver = MagicMock()
    resolver.get_dataset_path.return_value = "/tmp/full.db"

    preset = PresetConfig(
        markets=["prime"],
        include_topix=True,
        include_statements=True,
        include_margin=True,
    )
    monkeypatch.setattr(dataset_builder_service, "get_preset", lambda _name: preset)

    class DummyWriter:
        instances: list["DummyWriter"] = []

        def __init__(self, db_path: str):
            self.db_path = db_path
            self.calls: list[str] = []
            self.closed = False
            DummyWriter.instances.append(self)

        def upsert_stocks(self, rows):
            self.calls.append("stocks")
            return len(rows)

        def set_dataset_info(self, key: str, value: str):
            self.calls.append(f"info:{key}")

        def upsert_stock_data(self, rows):
            self.calls.append("stock_data")
            return len(rows)

        def upsert_topix_data(self, rows):
            self.calls.append("topix")
            return len(rows)

        def upsert_statements(self, rows):
            self.calls.append("statements")
            return len(rows)

        def upsert_margin_data(self, rows):
            self.calls.append("margin")
            return len(rows)

        def close(self):
            self.closed = True

    monkeypatch.setattr(dataset_builder_service, "DatasetWriter", DummyWriter)

    async def fake_get_paginated(path: str, params=None):
        if path == "/equities/master":
            return [
                _master_row("11110", "A"),
                _master_row("22220", "B"),
            ]
        if path == "/equities/bars/daily":
            return [_daily_bar_row()]
        if path == "/indices/bars/daily/topix":
            return [{"Date": "2026-01-01", "O": 1, "H": 2, "L": 1, "C": 2}]
        if path == "/fins/summary":
            return [{"Code": params.get("code"), "DisclosedDate": "2026-01-01"}]
        if path == "/markets/margin-interest":
            return [{"Date": "2026-01-01", "LongVol": 10, "ShrtVol": 5}]
        return []

    reader = _reader_from_fetch(fake_get_paginated)

    result = await _build_dataset(job, resolver, reader)

    assert result.success is True
    assert result.totalStocks == 2
    assert result.processedStocks == 2
    assert result.outputPath == "/tmp/full"
    assert result.warnings == []

    writer = DummyWriter.instances[-1]
    assert "stocks" in writer.calls
    assert "stock_data" in writer.calls
    assert "topix" in writer.calls
    assert "statements" in writer.calls
    assert "margin" in writer.calls
    assert writer.closed is True


@pytest.mark.asyncio
async def test_build_dataset_returns_partial_result_when_cancelled_during_stock_loop(
    monkeypatch,
    isolated_dataset_manager,
):
    job = await _create_job(isolated_dataset_manager, preset="quick")
    resolver = MagicMock()
    resolver.get_dataset_path.return_value = "/tmp/partial.db"

    preset = PresetConfig(
        markets=["prime"],
        include_topix=False,
        include_statements=False,
        include_margin=False,
    )
    monkeypatch.setattr(dataset_builder_service, "get_preset", lambda _name: preset)

    class DummyWriter:
        instances: list["DummyWriter"] = []

        def __init__(self, db_path: str):
            self.closed = False
            DummyWriter.instances.append(self)

        def upsert_stocks(self, rows):
            return len(rows)

        def set_dataset_info(self, key: str, value: str):
            return None

        def upsert_stock_data(self, rows):
            return len(rows)

        def close(self):
            self.closed = True

    monkeypatch.setattr(dataset_builder_service, "DatasetWriter", DummyWriter)

    async def fake_get_paginated(path: str, params=None):
        if path == "/equities/master":
            return [
                _master_row("11110", "A"),
                _master_row("22220", "B"),
            ]
        if path == "/equities/bars/daily":
            job.cancelled.set()
            return [_daily_bar_row()]
        return []

    reader = _reader_from_fetch(fake_get_paginated)

    result = await _build_dataset(job, resolver, reader)
    assert result.success is False
    assert result.processedStocks == 0
    assert result.errors == ["Cancelled"]
    assert DummyWriter.instances[-1].closed is True


@pytest.mark.asyncio
async def test_build_dataset_keeps_dataset_writer_on_one_worker_thread(
    monkeypatch,
    isolated_dataset_manager,
):
    job = await _create_job(isolated_dataset_manager, preset="quick")
    resolver = MagicMock()
    resolver.get_dataset_path.return_value = "/tmp/thread-affinity.db"

    preset = PresetConfig(
        markets=["prime"],
        include_topix=False,
        include_statements=False,
        include_margin=False,
        include_sector_indices=False,
    )
    monkeypatch.setattr(dataset_builder_service, "get_preset", lambda _name: preset)

    main_thread_id = threading.get_ident()
    thread_ids: list[int] = []

    class DummyWriter:
        def __init__(self, db_path: str):
            del db_path
            thread_ids.append(threading.get_ident())

        def upsert_stocks(self, rows):
            thread_ids.append(threading.get_ident())
            return len(rows)

        def set_dataset_info(self, key: str, value: str):
            del key, value
            thread_ids.append(threading.get_ident())
            return None

        def upsert_stock_data(self, rows):
            thread_ids.append(threading.get_ident())
            return len(rows)

        def close(self):
            thread_ids.append(threading.get_ident())

    monkeypatch.setattr(dataset_builder_service, "DatasetWriter", DummyWriter)

    async def fake_get_paginated(path: str, params=None):
        if path == "/equities/master":
            return [_master_row("11110", "A")]
        if path == "/equities/bars/daily":
            return [_daily_bar_row()]
        return []

    reader = _reader_from_fetch(fake_get_paginated)

    result = await _build_dataset(job, resolver, reader)

    assert result.success is True
    assert thread_ids
    assert len(set(thread_ids)) == 1
    assert thread_ids[0] != main_thread_id


@pytest.mark.asyncio
async def test_build_dataset_writes_manifest_v2(monkeypatch, isolated_dataset_manager, tmp_path):
    job = await _create_job(isolated_dataset_manager, preset="quick")
    resolver = MagicMock()
    db_path = tmp_path / "manifest.db"
    resolver.get_dataset_path.return_value = str(db_path)

    preset = PresetConfig(
        markets=["prime"],
        include_topix=False,
        include_statements=False,
        include_margin=False,
        include_sector_indices=False,
    )
    monkeypatch.setattr(dataset_builder_service, "get_preset", lambda _name: preset)

    async def fake_get_paginated(path: str, params=None):
        if path == "/equities/master":
            return [_master_row("11110", "A")]
        if path == "/equities/bars/daily":
            return [_daily_bar_row()]
        return []

    reader = _reader_from_fetch(fake_get_paginated)

    result = await _build_dataset(job, resolver, reader)
    assert result.success is True

    manifest_path = tmp_path / "manifest" / "manifest.v2.json"
    assert manifest_path.exists() is True
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    validated = validate_dataset_snapshot(manifest_path.parent)

    assert manifest["schemaVersion"] == 2
    assert manifest["dataset"]["name"] == "dataset"
    assert manifest["dataset"]["duckdbFile"] == "dataset.duckdb"
    assert manifest["counts"]["stocks"] == 1
    assert manifest["coverage"]["stocksWithQuotes"] == 1
    assert manifest["checksums"]["duckdbSha256"]
    assert manifest["checksums"]["logicalSha256"]
    assert manifest["checksums"]["parquet"]["stocks.parquet"]
    assert manifest["checksums"]["parquet"]["stock_data.parquet"]
    assert manifest["dateRange"] == {"min": "2026-01-01", "max": "2026-01-01"}
    assert validated.dataset.name == "dataset"


@pytest.mark.asyncio
async def test_build_dataset_rerun_keeps_logical_checksum_reproducible(
    monkeypatch,
    isolated_dataset_manager,
    tmp_path,
):
    resolver = MagicMock()
    db_path = tmp_path / "repro.db"
    resolver.get_dataset_path.return_value = str(db_path)

    preset = PresetConfig(
        markets=["prime"],
        include_topix=False,
        include_statements=False,
        include_margin=False,
        include_sector_indices=False,
    )
    monkeypatch.setattr(dataset_builder_service, "get_preset", lambda _name: preset)

    async def fake_get_paginated(path: str, params=None):
        if path == "/equities/master":
            return [_master_row("11110", "A")]
        if path == "/equities/bars/daily":
            return [_daily_bar_row()]
        return []

    reader = _reader_from_fetch(fake_get_paginated)

    job_first = await _create_job(isolated_dataset_manager, name="repro", preset="quick")
    first_result = await _build_dataset(job_first, resolver, reader)
    assert first_result.success is True
    isolated_dataset_manager.complete_job(job_first.job_id, first_result)
    first_manifest_path = tmp_path / "repro" / "manifest.v2.json"
    first_manifest = json.loads(first_manifest_path.read_text(encoding="utf-8"))

    job_second = await _create_job(isolated_dataset_manager, name="repro", preset="quick")
    second_result = await _build_dataset(job_second, resolver, reader)
    assert second_result.success is True
    isolated_dataset_manager.complete_job(job_second.job_id, second_result)
    second_manifest = json.loads(first_manifest_path.read_text(encoding="utf-8"))

    assert first_manifest["checksums"]["logicalSha256"] == second_manifest["checksums"]["logicalSha256"]
    assert first_manifest["counts"] == second_manifest["counts"]
    assert first_manifest["coverage"] == second_manifest["coverage"]


@pytest.mark.asyncio
async def test_build_dataset_manifest_uses_duckdb_state_as_sot(
    monkeypatch,
    isolated_dataset_manager,
    tmp_path,
):
    job = await _create_job(isolated_dataset_manager, name="duckdb-sot", preset="quick")
    resolver = MagicMock()
    db_path = tmp_path / "duckdb-sot.db"
    resolver.get_dataset_path.return_value = str(db_path)

    preset = PresetConfig(
        markets=["prime"],
        include_topix=False,
        include_statements=False,
        include_margin=False,
        include_sector_indices=False,
    )
    monkeypatch.setattr(dataset_builder_service, "get_preset", lambda _name: preset)

    async def fake_get_paginated(path: str, params=None):
        if path == "/equities/master":
            return [_master_row("11110", "A")]
        if path == "/equities/bars/daily":
            return [_daily_bar_row()]
        return []

    reader = _reader_from_fetch(fake_get_paginated)

    result = await _build_dataset(job, resolver, reader)
    assert result.success is True

    compatibility_db = tmp_path / "duckdb-sot" / "dataset.db"
    conn = sqlite3.connect(compatibility_db)
    conn.execute("CREATE TABLE IF NOT EXISTS stocks (code TEXT PRIMARY KEY)")
    conn.execute("INSERT OR REPLACE INTO stocks (code) VALUES ('9999')")
    conn.commit()
    conn.close()

    manifest_path = tmp_path / "duckdb-sot" / "manifest.v2.json"
    dataset_builder_service._write_dataset_manifest(
        snapshot_path=str(db_path),
        dataset_name="duckdb-sot",
        preset_name="quick",
        manifest_path=manifest_path,
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert manifest["counts"]["stocks"] == 1
    assert manifest["coverage"]["totalStocks"] == 1
    assert validate_dataset_snapshot(tmp_path / "duckdb-sot").dataset.name == "duckdb-sot"


@pytest.mark.asyncio
async def test_build_dataset_raises_when_manifest_generation_fails(monkeypatch, isolated_dataset_manager, tmp_path):
    job = await _create_job(isolated_dataset_manager, preset="quick")
    resolver = MagicMock()
    resolver.get_dataset_path.return_value = str(tmp_path / "manifest-fail.db")

    preset = PresetConfig(
        markets=["prime"],
        include_topix=False,
        include_statements=False,
        include_margin=False,
        include_sector_indices=False,
    )
    monkeypatch.setattr(dataset_builder_service, "get_preset", lambda _name: preset)

    def _raise_manifest_error(*, snapshot_path: str, dataset_name: str, preset_name: str, manifest_path=None) -> str:
        del snapshot_path, dataset_name, preset_name, manifest_path
        raise RuntimeError("manifest failed")

    monkeypatch.setattr(dataset_builder_service, "_write_dataset_manifest", _raise_manifest_error)

    async def fake_get_paginated(path: str, params=None):
        if path == "/equities/master":
            return [_master_row("11110", "A")]
        if path == "/equities/bars/daily":
            return [_daily_bar_row()]
        return []

    reader = _reader_from_fetch(fake_get_paginated)

    with pytest.raises(RuntimeError, match="manifest failed"):
        await _build_dataset(job, resolver, reader)


@pytest.mark.asyncio
async def test_build_dataset_handles_empty_stock_rows_and_progress_mod10(monkeypatch, isolated_dataset_manager):
    job = await _create_job(isolated_dataset_manager, preset="quick")
    resolver = MagicMock()
    resolver.get_dataset_path.return_value = "/tmp/no_rows.db"

    preset = PresetConfig(
        markets=["prime"],
        include_topix=False,
        include_statements=False,
        include_margin=False,
    )
    monkeypatch.setattr(dataset_builder_service, "get_preset", lambda _name: preset)

    progress_messages: list[str] = []
    original_update_progress = isolated_dataset_manager.update_progress

    def _capture_progress(job_id, progress):
        progress_messages.append(progress.message)
        original_update_progress(job_id, progress)

    monkeypatch.setattr(dataset_builder_service.dataset_job_manager, "update_progress", _capture_progress)

    class DummyWriter:
        instances: list["DummyWriter"] = []

        def __init__(self, db_path: str):
            self.stock_data_calls = 0
            self.closed = False
            DummyWriter.instances.append(self)

        def upsert_stocks(self, rows):
            return len(rows)

        def set_dataset_info(self, key: str, value: str):
            return None

        def upsert_stock_data(self, rows):
            self.stock_data_calls += 1
            return len(rows)

        def close(self):
            self.closed = True

    monkeypatch.setattr(dataset_builder_service, "DatasetWriter", DummyWriter)

    async def fake_get_paginated(path: str, params=None):
        if path == "/equities/master":
            return [
                {"Code": f"{i:04d}0", "MktNm": "プライム", "CoName": f"S{i}"}
                for i in range(10)
            ]
        if path == "/equities/bars/daily":
            return []
        return []

    reader = _reader_from_fetch(fake_get_paginated)

    result = await _build_dataset(job, resolver, reader)
    assert result.success is True
    assert result.processedStocks == 10
    assert any("Stock data from market.duckdb: 10/10" in message for message in progress_messages)
    writer = DummyWriter.instances[-1]
    assert writer.stock_data_calls == 0
    assert writer.closed is True


@pytest.mark.asyncio
async def test_build_dataset_queries_stock_data_per_batch(monkeypatch, isolated_dataset_manager):
    job = await _create_job(isolated_dataset_manager, preset="quick")
    resolver = MagicMock()
    resolver.get_dataset_path.return_value = "/tmp/batch.db"

    preset = PresetConfig(
        markets=["prime"],
        include_topix=False,
        include_statements=False,
        include_margin=False,
        include_sector_indices=False,
    )
    monkeypatch.setattr(dataset_builder_service, "get_preset", lambda _name: preset)

    class DummyWriter:
        def __init__(self, db_path: str):
            return None

        def upsert_stocks(self, rows):
            return len(rows)

        def set_dataset_info(self, key: str, value: str):
            return None

        def upsert_stock_data(self, rows):
            return len(rows)

        def close(self):
            return None

    monkeypatch.setattr(dataset_builder_service, "DatasetWriter", DummyWriter)
    monkeypatch.setattr(dataset_builder_service, "_BATCH_COPY_SIZE", 2)

    async def fake_get_paginated(path: str, params=None) -> list[dict[str, object]]:
        if path == "/equities/master":
            return cast(
                list[dict[str, object]],
                [
                    _master_row("11110", "A"),
                    _master_row("22220", "B"),
                    _master_row("33330", "C"),
                ],
            )
        if path == "/equities/bars/daily":
            return cast(list[dict[str, object]], [_daily_bar_row()])
        return []

    class CountingReader(_LegacyEndpointMarketReader):
        def __init__(self, fetch):
            super().__init__(fetch)
            self.stock_data_query_count = 0

        def query(self, sql: str, params: tuple[object, ...] = ()) -> list[dict[str, object]]:
            normalized = " ".join(sql.split()).lower()
            if " from stock_data " in f" {normalized} ":
                self.stock_data_query_count += 1
            return super().query(sql, params)

    reader = CountingReader(
        cast(
            Callable[
                [str, dict[str, str] | None],
                list[dict[str, object]] | Awaitable[list[dict[str, object]]],
            ],
            fake_get_paginated,
        )
    )

    result = await _build_dataset(job, resolver, reader)
    assert result.success is True
    assert result.processedStocks == 3
    assert reader.stock_data_query_count == 2


@pytest.mark.asyncio
async def test_build_dataset_overwrite_removes_legacy_db_artifact(
    monkeypatch,
    isolated_dataset_manager,
    tmp_path,
):
    job = await _create_job(isolated_dataset_manager, name="overwrite", preset="quick", overwrite=True)
    legacy_db = tmp_path / "overwrite.db"
    legacy_db.write_text("legacy", encoding="utf-8")

    resolver = MagicMock()
    resolver.get_dataset_path.return_value = str(tmp_path / "overwrite" / "dataset.db")
    resolver.get_artifact_paths.return_value = [str(legacy_db)]

    preset = PresetConfig(
        markets=["prime"],
        include_topix=False,
        include_statements=False,
        include_margin=False,
        include_sector_indices=False,
    )
    monkeypatch.setattr(dataset_builder_service, "get_preset", lambda _name: preset)

    class DummyWriter:
        def __init__(self, db_path: str):
            return None

        def upsert_stocks(self, rows):
            return len(rows)

        def set_dataset_info(self, key: str, value: str):
            return None

        def upsert_stock_data(self, rows):
            return len(rows)

        def close(self):
            return None

    monkeypatch.setattr(dataset_builder_service, "DatasetWriter", DummyWriter)

    async def fake_get_paginated(path: str, params=None):
        if path == "/equities/master":
            return [_master_row("11110", "A")]
        if path == "/equities/bars/daily":
            return [_daily_bar_row()]
        return []

    reader = _reader_from_fetch(fake_get_paginated)

    result = await _build_dataset(job, resolver, reader)

    assert result.success is True
    assert resolver.method_calls[0] == call.evict("overwrite")
    assert legacy_db.exists() is False


@pytest.mark.asyncio
async def test_build_dataset_topix_skips_fetch_when_cancelled_before_topix(monkeypatch, isolated_dataset_manager):
    job = await _create_job(isolated_dataset_manager, preset="topix")
    resolver = MagicMock()
    resolver.get_dataset_path.return_value = "/tmp/topix_skip.db"

    preset = PresetConfig(
        markets=["prime"],
        include_topix=True,
        include_statements=False,
        include_margin=False,
    )
    monkeypatch.setattr(dataset_builder_service, "get_preset", lambda _name: preset)

    class DummyWriter:
        def __init__(self, db_path: str):
            self.closed = False

        def upsert_stocks(self, rows):
            return len(rows)

        def set_dataset_info(self, key: str, value: str):
            return None

        def upsert_stock_data(self, rows):
            return len(rows)

        def close(self):
            self.closed = True

    monkeypatch.setattr(dataset_builder_service, "DatasetWriter", DummyWriter)

    async def fake_get_paginated(path: str, params=None):
        if path == "/equities/master":
            return [_master_row("11110", "A")]
        if path == "/equities/bars/daily":
            job.cancelled.set()
            return [_daily_bar_row()]
        if path == "/indices/bars/daily/topix":
            raise AssertionError("topix should not be fetched when cancelled")
        return []

    reader = _reader_from_fetch(fake_get_paginated)

    result = await _build_dataset(job, resolver, reader)
    assert result.success is False
    assert result.processedStocks == 0
    assert result.errors == ["Cancelled"]


@pytest.mark.asyncio
@pytest.mark.parametrize("topix_rows,expected_topix_calls", [([{"Date": "2026-01-01", "O": 1, "H": 2, "L": 1, "C": 2}], 1), ([], 0)])
async def test_build_dataset_topix_rows_true_and_false_branches(
    monkeypatch,
    isolated_dataset_manager,
    topix_rows,
    expected_topix_calls,
):
    job = await _create_job(isolated_dataset_manager, preset="topix")
    resolver = MagicMock()
    resolver.get_dataset_path.return_value = "/tmp/topix_rows.db"

    preset = PresetConfig(
        markets=["prime"],
        include_topix=True,
        include_statements=False,
        include_margin=False,
    )
    monkeypatch.setattr(dataset_builder_service, "get_preset", lambda _name: preset)

    class DummyWriter:
        instances: list["DummyWriter"] = []

        def __init__(self, db_path: str):
            self.topix_calls = 0
            DummyWriter.instances.append(self)

        def upsert_stocks(self, rows):
            return len(rows)

        def set_dataset_info(self, key: str, value: str):
            return None

        def upsert_stock_data(self, rows):
            return len(rows)

        def upsert_topix_data(self, rows):
            self.topix_calls += 1
            return len(rows)

        def close(self):
            return None

    monkeypatch.setattr(dataset_builder_service, "DatasetWriter", DummyWriter)

    async def fake_get_paginated(path: str, params=None):
        if path == "/equities/master":
            return [_master_row("11110", "A")]
        if path == "/equities/bars/daily":
            return [_daily_bar_row()]
        if path == "/indices/bars/daily/topix":
            return topix_rows
        return []

    reader = _reader_from_fetch(fake_get_paginated)

    result = await _build_dataset(job, resolver, reader)
    assert result.success is True
    writer = DummyWriter.instances[-1]
    assert writer.topix_calls == expected_topix_calls


@pytest.mark.asyncio
async def test_build_dataset_fetches_sector_indices_from_catalog(monkeypatch, isolated_dataset_manager):
    job = await _create_job(isolated_dataset_manager, preset="indices")
    resolver = MagicMock()
    resolver.get_dataset_path.return_value = "/tmp/indices.db"

    preset = PresetConfig(
        markets=["prime"],
        include_topix=False,
        include_statements=False,
        include_margin=False,
        include_sector_indices=True,
    )
    monkeypatch.setattr(dataset_builder_service, "get_preset", lambda _name: preset)
    monkeypatch.setattr(dataset_builder_service, "get_index_catalog_codes", lambda: {"0040", "0050", "0500"})

    class DummyWriter:
        instances: list["DummyWriter"] = []

        def __init__(self, db_path: str):
            self.indices_calls = 0
            DummyWriter.instances.append(self)

        def upsert_stocks(self, rows):
            return len(rows)

        def set_dataset_info(self, key: str, value: str):
            return None

        def upsert_stock_data(self, rows):
            return len(rows)

        def upsert_indices_data(self, rows):
            self.indices_calls += 1
            return len(rows)

        def close(self):
            return None

    monkeypatch.setattr(dataset_builder_service, "DatasetWriter", DummyWriter)

    async def fake_get_paginated(path: str, params=None):
        if path == "/equities/master":
            return [_master_row("11110", "A")]
        if path == "/equities/bars/daily":
            return [_daily_bar_row()]
        if path == "/indices/bars/daily":
            code = params.get("code") if params else None
            if code in {"0040", "0050"}:
                return [{"Date": "2026-01-01", "Code": code, "O": 1, "H": 2, "L": 1, "C": 2}]
            return []
        return []

    reader = _reader_from_fetch(fake_get_paginated)

    result = await _build_dataset(job, resolver, reader)
    assert result.success is True
    writer = DummyWriter.instances[-1]
    assert writer.indices_calls == 1


@pytest.mark.asyncio
async def test_build_dataset_skips_incomplete_ohlcv_rows_without_failing_stock(monkeypatch, isolated_dataset_manager):
    job = await _create_job(isolated_dataset_manager, preset="ohlcv")
    resolver = MagicMock()
    resolver.get_dataset_path.return_value = "/tmp/ohlcv.db"

    preset = PresetConfig(
        markets=["prime"],
        include_topix=False,
        include_statements=False,
        include_margin=False,
        include_sector_indices=False,
    )
    monkeypatch.setattr(dataset_builder_service, "get_preset", lambda _name: preset)

    class DummyWriter:
        instances: list["DummyWriter"] = []

        def __init__(self, db_path: str):
            self.stock_data_rows = 0
            DummyWriter.instances.append(self)

        def upsert_stocks(self, rows):
            return len(rows)

        def set_dataset_info(self, key: str, value: str):
            return None

        def upsert_stock_data(self, rows):
            self.stock_data_rows += len(rows)
            return len(rows)

        def close(self):
            return None

    monkeypatch.setattr(dataset_builder_service, "DatasetWriter", DummyWriter)

    async def fake_get_paginated(path: str, params=None):
        if path == "/equities/master":
            return [_master_row("11110", "A")]
        if path == "/equities/bars/daily":
            return [
                _daily_bar_row(),
                {"Date": "2026-01-02", "O": None, "H": 2, "L": 1, "C": 2, "Vo": 100},
            ]
        return []

    reader = _reader_from_fetch(fake_get_paginated)

    result = await _build_dataset(job, resolver, reader)
    assert result.success is True
    assert result.processedStocks == 1
    assert result.warnings is not None
    assert any("Skipped incomplete OHLCV rows" in warning for warning in result.warnings)
    writer = DummyWriter.instances[-1]
    assert writer.stock_data_rows == 1


@pytest.mark.asyncio
async def test_build_dataset_direct_copy_generates_valid_snapshot_and_warnings(
    monkeypatch,
    isolated_dataset_manager,
    tmp_path,
):
    job = await _create_job(isolated_dataset_manager, name="direct-copy", preset="quickTesting")
    resolver = MagicMock()
    resolver.get_dataset_path.return_value = str(tmp_path / "direct-copy.db")
    source_duckdb_path = _create_market_source_duckdb(tmp_path)

    preset = PresetConfig(
        markets=["prime"],
        include_topix=True,
        include_statements=True,
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
    assert manifest.counts.stocks == 2
    assert manifest.counts.stock_data == 2
    assert manifest.counts.topix_data == 1
    assert manifest.counts.indices_data == 1
    assert manifest.counts.margin_data == 2
    assert manifest.counts.statements == 2
    assert manifest.coverage.totalStocks == 2
    assert manifest.coverage.stocksWithQuotes == 1
    assert manifest.coverage.stocksWithMargin == 2
    assert manifest.coverage.stocksWithStatements == 2


@pytest.mark.asyncio
async def test_build_dataset_direct_copy_reopens_writer_after_stock_metadata(
    monkeypatch,
    isolated_dataset_manager,
    tmp_path,
):
    job = await _create_job(isolated_dataset_manager, name="direct-copy-reopen", preset="quickTesting")
    resolver = MagicMock()
    resolver.get_dataset_path.return_value = "/tmp/direct-copy-reopen.db"

    preset = PresetConfig(
        markets=["prime"],
        include_topix=True,
        include_statements=True,
        include_margin=True,
        include_sector_indices=True,
    )
    monkeypatch.setattr(dataset_builder_service, "get_preset", lambda _name: preset)
    monkeypatch.setattr(dataset_builder_service, "get_index_catalog_codes", lambda: {"0040"})

    class DummyWriter:
        instances: list["DummyWriter"] = []

        def __init__(self, db_path: str):
            self.calls: list[tuple[str, object]] = []
            self.closed = False
            DummyWriter.instances.append(self)

        def upsert_stocks(self, rows):
            self.calls.append(("upsert_stocks", len(rows)))
            return len(rows)

        def set_dataset_info(self, key: str, value: str):
            self.calls.append(("set_dataset_info", key))
            return None

        def copy_stock_data_from_source(self, *, source_duckdb_path: str, normalized_codes: list[str]):
            del source_duckdb_path
            self.calls.append(("copy_stock_data_from_source", tuple(normalized_codes)))
            return dataset_builder_service.StockDataCopyResult(
                inserted_rows=len(normalized_codes),
                code_stats={
                    code: StockDataCopyCodeStats(
                        total_rows=1,
                        valid_rows=1,
                        skipped_rows=0,
                    )
                    for code in normalized_codes
                },
            )

        def copy_topix_data_from_source(self, *, source_duckdb_path: str):
            del source_duckdb_path
            self.calls.append(("copy_topix_data_from_source", None))
            return 1

        def copy_indices_data_from_source(self, *, source_duckdb_path: str, normalized_codes: list[str]):
            del source_duckdb_path
            self.calls.append(("copy_indices_data_from_source", tuple(normalized_codes)))
            return len(normalized_codes)

        def copy_statements_from_source(self, *, source_duckdb_path: str, normalized_codes: list[str]):
            del source_duckdb_path
            self.calls.append(("copy_statements_from_source", tuple(normalized_codes)))
            return len(normalized_codes)

        def copy_margin_data_from_source(self, *, source_duckdb_path: str, normalized_codes: list[str]):
            del source_duckdb_path
            self.calls.append(("copy_margin_data_from_source", tuple(normalized_codes)))
            return len(normalized_codes)

        def close(self):
            self.closed = True
            self.calls.append(("close", None))
            return None

    monkeypatch.setattr(dataset_builder_service, "DatasetWriter", DummyWriter)
    source_duckdb_path = tmp_path / "source-market.duckdb"
    source_duckdb_path.write_text("", encoding="utf-8")

    async def fake_get_paginated(path: str, params=None):
        del params
        if path == "/equities/master":
            return [_master_row("11110", "A")]
        raise AssertionError(f"Unexpected legacy fetch: {path}")

    reader = _reader_from_fetch(fake_get_paginated)

    result = await _build_dataset(
        job,
        resolver,
        reader,
        source_duckdb_path=str(source_duckdb_path),
    )

    assert result.success is True
    assert len(DummyWriter.instances) == 2

    metadata_writer, copy_writer = DummyWriter.instances
    assert metadata_writer.closed is True
    assert copy_writer.closed is True
    assert [call[0] for call in metadata_writer.calls] == [
        "upsert_stocks",
        "set_dataset_info",
        "set_dataset_info",
        "set_dataset_info",
        "close",
    ]
    assert [call[0] for call in copy_writer.calls] == [
        "copy_stock_data_from_source",
        "copy_topix_data_from_source",
        "copy_indices_data_from_source",
        "copy_statements_from_source",
        "copy_margin_data_from_source",
        "set_dataset_info",
        "set_dataset_info",
        "close",
    ]


@pytest.mark.asyncio
async def test_build_dataset_statements_handles_empty_rows_and_cancel_break(monkeypatch, isolated_dataset_manager):
    job = await _create_job(isolated_dataset_manager, preset="statements")
    resolver = MagicMock()
    resolver.get_dataset_path.return_value = "/tmp/statements_break.db"

    preset = PresetConfig(
        markets=["prime"],
        include_topix=False,
        include_statements=True,
        include_margin=False,
    )
    monkeypatch.setattr(dataset_builder_service, "get_preset", lambda _name: preset)

    class DummyWriter:
        instances: list["DummyWriter"] = []

        def __init__(self, db_path: str):
            self.statement_calls = 0
            DummyWriter.instances.append(self)

        def upsert_stocks(self, rows):
            return len(rows)

        def set_dataset_info(self, key: str, value: str):
            return None

        def upsert_stock_data(self, rows):
            return len(rows)

        def upsert_statements(self, rows):
            self.statement_calls += 1
            return len(rows)

        def close(self):
            return None

    monkeypatch.setattr(dataset_builder_service, "DatasetWriter", DummyWriter)

    async def fake_get_paginated(path: str, params=None):
        if path == "/equities/master":
            return [
                _master_row("11110", "A"),
                _master_row("22220", "B"),
            ]
        if path == "/equities/bars/daily":
            return [_daily_bar_row()]
        if path == "/fins/summary":
            job.cancelled.set()
            return []
        return []

    reader = _reader_from_fetch(fake_get_paginated)

    result = await _build_dataset(job, resolver, reader)
    assert result.success is False
    assert result.errors == ["Cancelled"]
    writer = DummyWriter.instances[-1]
    assert writer.statement_calls == 0


@pytest.mark.asyncio
async def test_build_dataset_margin_handles_empty_rows_and_cancel_break(monkeypatch, isolated_dataset_manager):
    job = await _create_job(isolated_dataset_manager, preset="margin")
    resolver = MagicMock()
    resolver.get_dataset_path.return_value = "/tmp/margin_break.db"

    preset = PresetConfig(
        markets=["prime"],
        include_topix=False,
        include_statements=False,
        include_margin=True,
    )
    monkeypatch.setattr(dataset_builder_service, "get_preset", lambda _name: preset)

    class DummyWriter:
        instances: list["DummyWriter"] = []

        def __init__(self, db_path: str):
            self.margin_calls = 0
            DummyWriter.instances.append(self)

        def upsert_stocks(self, rows):
            return len(rows)

        def set_dataset_info(self, key: str, value: str):
            return None

        def upsert_stock_data(self, rows):
            return len(rows)

        def upsert_margin_data(self, rows):
            self.margin_calls += 1
            return len(rows)

        def close(self):
            return None

    monkeypatch.setattr(dataset_builder_service, "DatasetWriter", DummyWriter)

    async def fake_get_paginated(path: str, params=None):
        if path == "/equities/master":
            return [
                _master_row("11110", "A"),
                _master_row("22220", "B"),
            ]
        if path == "/equities/bars/daily":
            return [_daily_bar_row()]
        if path == "/markets/margin-interest":
            job.cancelled.set()
            return []
        return []

    reader = _reader_from_fetch(fake_get_paginated)

    result = await _build_dataset(job, resolver, reader)
    assert result.success is False
    assert result.errors == ["Cancelled"]
    writer = DummyWriter.instances[-1]
    assert writer.margin_calls == 0


@pytest.mark.asyncio
async def test_start_dataset_build_marks_failed_on_timeout(monkeypatch, isolated_dataset_manager):
    data = DatasetJobData(name="timeout", preset="quickTesting")
    resolver = MagicMock()
    client = AsyncMock()

    async def fake_wait_for(coro, timeout):
        coro.close()
        raise asyncio.TimeoutError()

    monkeypatch.setattr(dataset_builder_service.asyncio, "wait_for", fake_wait_for)

    job = await start_dataset_build(data, resolver, client)
    assert job is not None
    assert job.task is not None
    await job.task

    stored = isolated_dataset_manager.get_job(job.job_id)
    assert stored is not None
    assert stored.status == JobStatus.FAILED
    assert stored.error == "Dataset build timed out after 35 minutes"


@pytest.mark.asyncio
async def test_start_dataset_build_uses_fixed_timeout(monkeypatch, isolated_dataset_manager):
    data = DatasetJobData(name="timeout-fixed", preset="quickTesting")
    resolver = MagicMock()
    client = AsyncMock()
    timeout_values: list[int] = []

    async def fake_wait_for(coro, timeout):
        coro.close()
        timeout_values.append(timeout)
        raise asyncio.TimeoutError()

    monkeypatch.setattr(dataset_builder_service.asyncio, "wait_for", fake_wait_for)

    job = await start_dataset_build(data, resolver, client)
    assert job is not None
    assert job.task is not None
    await job.task

    stored = isolated_dataset_manager.get_job(job.job_id)
    assert stored is not None
    assert stored.status == JobStatus.FAILED
    assert stored.error == "Dataset build timed out after 35 minutes"
    assert timeout_values == [35 * 60]


@pytest.mark.asyncio
async def test_start_dataset_build_marks_failed_on_unexpected_error(monkeypatch, isolated_dataset_manager):
    data = DatasetJobData(name="error", preset="quickTesting")
    resolver = MagicMock()
    client = AsyncMock()

    async def fake_wait_for(coro, timeout):
        coro.close()
        raise RuntimeError("dataset exploded")

    monkeypatch.setattr(dataset_builder_service.asyncio, "wait_for", fake_wait_for)

    job = await start_dataset_build(data, resolver, client)
    assert job is not None
    assert job.task is not None
    await job.task

    stored = isolated_dataset_manager.get_job(job.job_id)
    assert stored is not None
    assert stored.status == JobStatus.FAILED
    assert stored.error == "dataset exploded"


@pytest.mark.asyncio
async def test_start_dataset_build_handles_cancelled_error(monkeypatch, isolated_dataset_manager):
    data = DatasetJobData(name="cancelled", preset="quickTesting")
    resolver = MagicMock()
    client = AsyncMock()

    async def fake_wait_for(coro, timeout):
        coro.close()
        raise asyncio.CancelledError()

    monkeypatch.setattr(dataset_builder_service.asyncio, "wait_for", fake_wait_for)

    job = await start_dataset_build(data, resolver, client)
    assert job is not None
    assert job.task is not None
    await job.task

    stored = isolated_dataset_manager.get_job(job.job_id)
    assert stored is not None
    assert stored.status == JobStatus.PENDING
    assert stored.error is None


@pytest.mark.asyncio
async def test_start_dataset_build_skips_complete_when_job_cancelled(monkeypatch, isolated_dataset_manager):
    data = DatasetJobData(name="skip-complete", preset="quickTesting")
    resolver = MagicMock()
    client = AsyncMock()

    async def fake_build(job, resolver, client, *, source_duckdb_path=None):
        del resolver, client, source_duckdb_path
        job.cancelled.set()
        return DatasetResult(success=True, totalStocks=1, processedStocks=1)

    monkeypatch.setattr(dataset_builder_service, "_build_dataset", fake_build)

    job = await start_dataset_build(data, resolver, client)
    assert job is not None
    assert job.task is not None
    await job.task

    stored = isolated_dataset_manager.get_job(job.job_id)
    assert stored is not None
    assert stored.status == JobStatus.PENDING
    assert stored.result is None


@pytest.mark.asyncio
async def test_start_dataset_build_completes_when_not_cancelled(monkeypatch, isolated_dataset_manager):
    data = DatasetJobData(name="completed", preset="quickTesting")
    resolver = MagicMock()
    client = AsyncMock()

    async def fake_build(job, resolver, client, *, source_duckdb_path=None):
        del job, resolver, client, source_duckdb_path
        return DatasetResult(success=True, totalStocks=1, processedStocks=1, outputPath="/tmp/completed.db")

    monkeypatch.setattr(dataset_builder_service, "_build_dataset", fake_build)

    job = await start_dataset_build(data, resolver, client)
    assert job is not None
    assert job.task is not None
    await job.task

    stored = isolated_dataset_manager.get_job(job.job_id)
    assert stored is not None
    assert stored.status == JobStatus.COMPLETED
    assert stored.result is not None
    assert stored.result.success is True
