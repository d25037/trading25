from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from src.infrastructure.db.dataset_io.dataset_writer import DatasetWriter
from src.infrastructure.db.market.dataset_snapshot_reader import (
    DatasetSnapshotReader,
    build_dataset_snapshot_logical_checksum,
    inspect_dataset_snapshot_duckdb,
    validate_dataset_snapshot,
)


def _write_manifest_v2(snapshot_dir: Path) -> None:
    duckdb_path = snapshot_dir / "dataset.duckdb"
    parquet_dir = snapshot_dir / "parquet"
    inspection = inspect_dataset_snapshot_duckdb(duckdb_path)
    manifest = {
        "schemaVersion": 2,
        "generatedAt": "2026-03-09T00:00:00+00:00",
        "dataset": {
            "name": "sample",
            "preset": "quickTesting",
            "duckdbFile": "dataset.duckdb",
            "parquetDir": "parquet",
        },
        "source": {
            "backend": "duckdb-parquet",
        },
        "counts": inspection.counts.model_dump(),
        "coverage": inspection.coverage.model_dump(),
        "checksums": {
            "duckdbSha256": hashlib.sha256(duckdb_path.read_bytes()).hexdigest(),
            "logicalSha256": build_dataset_snapshot_logical_checksum(
                counts=inspection.counts,
                coverage=inspection.coverage,
                date_range=inspection.date_range,
            ),
            "parquet": {
                parquet_file.name: hashlib.sha256(parquet_file.read_bytes()).hexdigest()
                for parquet_file in sorted(parquet_dir.glob("*.parquet"))
            },
        },
    }
    if inspection.date_range is not None:
        manifest["dateRange"] = inspection.date_range.model_dump()
    (snapshot_dir / "manifest.v2.json").write_text(json.dumps(manifest), encoding="utf-8")


def _create_snapshot(tmp_path: Path) -> Path:
    snapshot_dir = tmp_path / "sample"
    writer = DatasetWriter(str(snapshot_dir))
    writer.upsert_stocks([
        {
            "code": "7203",
            "company_name": "Toyota",
            "market_code": "0111",
            "market_name": "プライム",
            "sector_17_code": "7",
            "sector_17_name": "輸送用機器",
            "sector_33_code": "3050",
            "sector_33_name": "輸送用機器",
            "listed_date": "1949-05-16",
        }
    ])
    writer.set_dataset_info("preset", "quickTesting")
    writer.close()
    _write_manifest_v2(snapshot_dir)
    return snapshot_dir


def _create_rich_snapshot(tmp_path: Path) -> Path:
    snapshot_dir = tmp_path / "rich"
    writer = DatasetWriter(str(snapshot_dir))
    writer.upsert_stocks([
        {
            "code": "7203",
            "company_name": "トヨタ自動車",
            "company_name_english": "TOYOTA",
            "market_code": "0111",
            "market_name": "プライム",
            "sector_17_code": "7",
            "sector_17_name": "輸送用機器",
            "sector_33_code": "3050",
            "sector_33_name": "輸送用機器",
            "scale_category": "TOPIX Core30",
            "listed_date": "1949-05-16",
        },
        {
            "code": "9984",
            "company_name": "ソフトバンクグループ",
            "company_name_english": "SOFTBANK GROUP",
            "market_code": "0111",
            "market_name": "プライム",
            "sector_17_code": "9",
            "sector_17_name": "情報・通信業",
            "sector_33_code": "5250",
            "sector_33_name": "情報・通信業",
            "scale_category": "TOPIX Large70",
            "listed_date": "1994-07-22",
        },
    ])
    writer.upsert_stock_data([
        {
            "code": "7203",
            "date": "2024-01-04",
            "open": 100,
            "high": 110,
            "low": 90,
            "close": 105,
            "volume": 1000,
            "adjustment_factor": 1.0,
        },
        {
            "code": "7203",
            "date": "2024-01-05",
            "open": 105,
            "high": 115,
            "low": 95,
            "close": 110,
            "volume": 1100,
            "adjustment_factor": 1.0,
        },
        {
            "code": "9984",
            "date": "2024-01-04",
            "open": 200,
            "high": 210,
            "low": 190,
            "close": 205,
            "volume": 500,
            "adjustment_factor": 1.0,
        },
    ])
    writer.upsert_topix_data([
        {
            "date": "2024-01-04",
            "open": 2500,
            "high": 2520,
            "low": 2490,
            "close": 2510,
        }
    ])
    writer.upsert_indices_data([
        {
            "code": "0010",
            "date": "2024-01-04",
            "open": 100,
            "high": 102,
            "low": 99,
            "close": 101,
            "sector_name": "食料品",
        }
    ])
    writer.upsert_margin_data([
        {
            "code": "7203",
            "date": "2024-01-04",
            "long_margin_volume": 50000,
            "short_margin_volume": 30000,
        },
        {
            "code": "9984",
            "date": "2024-01-04",
            "long_margin_volume": 40000,
            "short_margin_volume": 20000,
        },
    ])
    writer.upsert_statements([
        {
            "code": "7203",
            "disclosed_date": "2024-01-30",
            "earnings_per_share": 150.0,
            "profit": 2000000,
            "equity": 5000000,
            "type_of_current_period": "FY",
            "type_of_document": "AnnualReport",
            "next_year_forecast_earnings_per_share": 160.0,
            "forecast_eps": 165.0,
        },
        {
            "code": "7203",
            "disclosed_date": "2024-04-30",
            "earnings_per_share": 45.0,
            "profit": 550000,
            "equity": 5100000,
            "type_of_current_period": "1Q",
            "type_of_document": "QuarterlyReport",
            "next_year_forecast_earnings_per_share": 170.0,
            "forecast_eps": 172.0,
        },
        {
            "code": "7203",
            "disclosed_date": "2024-07-30",
            "earnings_per_share": 48.0,
            "profit": 600000,
            "equity": 5200000,
            "type_of_current_period": "Q1",
            "type_of_document": "QuarterlyReport",
            "next_year_forecast_earnings_per_share": 175.0,
            "forecast_eps": 176.0,
        },
        {
            "code": "7203",
            "disclosed_date": "2024-10-30",
            "type_of_current_period": "FY",
            "type_of_document": "ForecastRevision",
            "next_year_forecast_earnings_per_share": 180.0,
            "forecast_eps": 180.0,
        },
    ])
    writer.set_dataset_info("preset", "primeMarket")
    writer.close()
    _write_manifest_v2(snapshot_dir)
    return snapshot_dir


def test_validate_dataset_snapshot_accepts_valid_bundle(tmp_path: Path) -> None:
    snapshot_dir = _create_snapshot(tmp_path)

    manifest = validate_dataset_snapshot(snapshot_dir)

    assert manifest.dataset.name == "sample"
    assert manifest.schemaVersion == 2


def test_dataset_snapshot_reader_reads_duckdb_bundle(tmp_path: Path) -> None:
    snapshot_dir = _create_snapshot(tmp_path)

    reader = DatasetSnapshotReader(str(snapshot_dir))
    try:
        assert reader.get_dataset_info()["preset"] == "quickTesting"
    finally:
        reader.close()


def test_validate_dataset_snapshot_rejects_checksum_mismatch(tmp_path: Path) -> None:
    snapshot_dir = _create_snapshot(tmp_path)
    (snapshot_dir / "dataset.duckdb").write_text("tampered", encoding="utf-8")

    with pytest.raises(RuntimeError, match="checksum mismatch"):
        validate_dataset_snapshot(snapshot_dir)


def test_validate_dataset_snapshot_rejects_logical_checksum_mismatch(tmp_path: Path) -> None:
    snapshot_dir = _create_snapshot(tmp_path)
    manifest_path = snapshot_dir / "manifest.v2.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["checksums"]["logicalSha256"] = "stale"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(RuntimeError, match="logical checksum mismatch"):
        validate_dataset_snapshot(snapshot_dir)


def test_validate_dataset_snapshot_rejects_manifest_v1(tmp_path: Path) -> None:
    snapshot_dir = _create_snapshot(tmp_path)
    manifest_v1_path = snapshot_dir / "manifest.v1.json"
    manifest_v1_path.write_text(
        json.dumps(
            {
                "schemaVersion": 1,
                "generatedAt": "2026-03-09T00:00:00+00:00",
                "dataset": {
                    "name": "sample",
                    "preset": "quickTesting",
                    "duckdbFile": "dataset.duckdb",
                    "parquetDir": "parquet",
                },
                "source": {"backend": "duckdb-parquet"},
                "counts": {},
                "coverage": {},
                "checksums": {
                    "duckdbSha256": "stale",
                    "logicalSha256": "stale",
                    "parquet": {},
                },
            }
        ),
        encoding="utf-8",
    )
    (snapshot_dir / "manifest.v2.json").unlink()

    with pytest.raises(FileNotFoundError):
        validate_dataset_snapshot(snapshot_dir)


def test_dataset_snapshot_reader_public_methods_cover_duckdb_bundle(tmp_path: Path) -> None:
    snapshot_dir = _create_rich_snapshot(tmp_path)

    reader = DatasetSnapshotReader(str(snapshot_dir))
    try:
        assert reader.snapshot_dir == snapshot_dir
        assert reader.manifest.schemaVersion == 2
        assert reader.conn is reader.conn

        assert reader.query_one("SELECT 1 AS one").one == 1
        assert reader.query_one("SELECT 1 AS one WHERE FALSE") is None

        stocks = reader.get_stocks(market="0111")
        assert [row.code for row in stocks] == ["7203", "9984"]
        assert reader.get_stocks(sector="輸送用機器")[0].company_name == "トヨタ自動車"
        assert "code" in stocks[0].keys()
        assert tuple(value for _, value in stocks[0].items())[0] == "7203"
        assert stocks[0].values()[0] == "7203"
        assert dict(iter(stocks[0]))["company_name"] == "トヨタ自動車"

        statement_columns = reader._get_statements_columns()
        assert "forecast_eps" in statement_columns
        assert reader._get_statements_columns() is statement_columns

        ohlcv = reader.get_stock_ohlcv("72030", start="2024-01-05", end="2024-01-05")
        assert len(ohlcv) == 1
        assert ohlcv[0]["close"] == 110.0

        ohlcv_batch = reader.get_ohlcv_batch(["7203", "9999"])
        assert len(ohlcv_batch["7203"]) == 2
        assert ohlcv_batch["9999"] == []
        filtered_ohlcv_batch = reader.get_ohlcv_batch(
            ["7203", "9999"],
            start="2024-01-05",
            end="2024-01-05",
        )
        assert len(filtered_ohlcv_batch["7203"]) == 1
        assert filtered_ohlcv_batch["9999"] == []

        assert reader.get_topix(end="2024-01-04")[0].close == 2510.0
        assert reader.get_indices()[0].sector_name == "食料品"
        assert reader.get_index_data("0010")[0].open == 100.0

        assert reader.get_margin(code="99840")[0].long_margin_volume == 40000.0
        margin_batch = reader.get_margin_batch(["7203", "9999"])
        assert len(margin_batch["7203"]) == 1
        assert margin_batch["9999"] == []
        filtered_margin_batch = reader.get_margin_batch(
            ["7203", "9999"],
            start="2024-01-05",
            end="2024-01-05",
        )
        assert filtered_margin_batch["7203"] == []
        assert filtered_margin_batch["9999"] == []

        statements = reader.get_statements("7203", period_type="1Q")
        assert {row.type_of_current_period for row in statements} == {"1Q", "Q1"}
        assert len(reader.get_statements("7203", actual_only=False)) == 4
        statements_batch = reader.get_statements_batch(
            ["7203", "9999"],
            period_type="FY",
            actual_only=False,
        )
        assert [row.disclosed_date for row in statements_batch["7203"]] == [
            "2024-01-30",
            "2024-10-30",
        ]
        assert statements_batch["9999"] == []

        assert reader.get_sectors() == [
            {"code": "3050", "name": "輸送用機器"},
            {"code": "5250", "name": "情報・通信業"},
        ]
        assert reader.get_sector_mapping() == {
            "3050": "輸送用機器",
            "5250": "情報・通信業",
        }
        assert reader.get_sector_stock_mapping()["輸送用機器"] == ["7203"]
        assert [row.code for row in reader.get_sector_stocks("輸送用機器")] == ["7203"]

        assert reader.get_dataset_info()["preset"] == "primeMarket"
        assert reader.get_stock_count() == 2
        assert [row.stockCode for row in reader.get_stock_list_with_counts(min_records=0)] == [
            "7203",
            "9984",
        ]
        assert reader.get_index_list_with_counts(min_records=0)[0].indexCode == "0010"
        assert reader.get_margin_list(min_records=0)[0].stockCode == "7203"

        assert reader.search_stocks("7203", exact=True)[0].match_type == "exact"
        assert reader.search_stocks("トヨ", exact=False)[0]["match_type"] == "partial"
        assert reader.get_sample_codes(size=2, seed=42) == reader.get_sample_codes(size=2, seed=42)
        assert reader.get_table_counts() == {
            "stocks": 2,
            "stock_data": 3,
            "topix_data": 1,
            "indices_data": 1,
            "margin_data": 2,
            "statements": 4,
            "dataset_info": 1,
        }
        assert reader.get_date_range() == {"min": "2024-01-04", "max": "2024-01-05"}
        assert reader.get_sectors_with_count()[0].sectorName == "情報・通信業"
        assert reader.get_stocks_with_quotes_count() == 2
        assert reader.get_stocks_with_margin_count() == 2
        assert reader.get_stocks_with_statements_count() == 1
        assert reader.get_stocks_without_quotes_count() == 0
        assert reader.get_fk_orphan_counts() == {
            "stockDataOrphans": 0,
            "marginDataOrphans": 0,
            "statementsOrphans": 0,
        }
    finally:
        reader.close()
