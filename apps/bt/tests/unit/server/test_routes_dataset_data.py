"""Dataset Data ルートの統合テスト"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from src.entrypoints.http.routes import dataset_data as dataset_data_routes
from src.entrypoints.http.app import create_app
from src.application.services.dataset_resolver import DatasetResolver
from src.infrastructure.db.dataset_io.dataset_writer import DatasetWriter
from src.infrastructure.db.market.dataset_snapshot_reader import (
    build_dataset_snapshot_logical_checksum,
    inspect_dataset_snapshot_duckdb,
)


def _write_manifest_v2(snapshot_dir: Path, name: str) -> None:
    duckdb_path = snapshot_dir / "dataset.duckdb"
    parquet_dir = snapshot_dir / "parquet"
    inspection = inspect_dataset_snapshot_duckdb(duckdb_path)
    manifest = {
        "schemaVersion": 2,
        "generatedAt": "2026-03-14T00:00:00+00:00",
        "dataset": {
            "name": name,
            "preset": "primeMarket",
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


def _build_snapshot(base_dir: Path, name: str) -> None:
    snapshot_dir = base_dir / name
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
            "company_name": "ソフトバンク",
            "company_name_english": "SB",
            "market_code": "0111",
            "market_name": "プライム",
            "sector_17_code": "9",
            "sector_17_name": "情報・通信業",
            "sector_33_code": "3700",
            "sector_33_name": "情報・通信業",
            "scale_category": "TOPIX Core30",
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
            "bps": 3000,
            "sales": 20000000,
            "operating_profit": 1500000,
            "ordinary_profit": 1600000,
            "operating_cash_flow": 1800000,
            "dividend_fy": 60.0,
            "forecast_dividend_fy": 62.0,
            "next_year_forecast_dividend_fy": 64.0,
            "payout_ratio": 30.0,
            "forecast_payout_ratio": 32.0,
            "next_year_forecast_payout_ratio": 34.0,
            "forecast_eps": 165.0,
            "investing_cash_flow": -500000,
            "financing_cash_flow": -300000,
            "cash_and_equivalents": 4000000,
            "total_assets": 50000000,
            "shares_outstanding": 330000000,
            "treasury_shares": 10000000,
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
            "bps": 3050,
            "sales": 5200000,
            "operating_profit": 420000,
            "ordinary_profit": 430000,
            "operating_cash_flow": 510000,
            "dividend_fy": 15.0,
            "forecast_dividend_fy": 16.0,
            "next_year_forecast_dividend_fy": 17.0,
            "payout_ratio": 28.0,
            "forecast_payout_ratio": 29.0,
            "next_year_forecast_payout_ratio": 30.0,
            "forecast_eps": 172.0,
            "investing_cash_flow": -110000,
            "financing_cash_flow": -90000,
            "cash_and_equivalents": 4100000,
            "total_assets": 50100000,
            "shares_outstanding": 331000000,
            "treasury_shares": 10000000,
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
            "bps": 3100,
            "sales": 5400000,
            "operating_profit": 450000,
            "ordinary_profit": 460000,
            "operating_cash_flow": 530000,
            "dividend_fy": 16.0,
            "forecast_dividend_fy": 17.0,
            "next_year_forecast_dividend_fy": 18.0,
            "payout_ratio": 29.0,
            "forecast_payout_ratio": 30.0,
            "next_year_forecast_payout_ratio": 31.0,
            "forecast_eps": 176.0,
            "investing_cash_flow": -100000,
            "financing_cash_flow": -85000,
            "cash_and_equivalents": 4200000,
            "total_assets": 50200000,
            "shares_outstanding": 332000000,
            "treasury_shares": 10000000,
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
    _write_manifest_v2(snapshot_dir, name)


@pytest.fixture(scope="module")
def test_dataset_dir(tmp_path_factory):
    """テスト用の DuckDB snapshot ディレクトリ"""
    tmp_path = tmp_path_factory.mktemp("dataset-data-routes")
    _build_snapshot(tmp_path, "test-market")
    return str(tmp_path)


@pytest.fixture(scope="module")
def client(test_dataset_dir: str):
    """テスト用 FastAPI クライアント"""
    app = create_app()
    app.state.dataset_resolver = DatasetResolver(test_dataset_dir)
    test_client = TestClient(app, raise_server_exceptions=False)
    yield test_client
    test_client.close()


class TestDatasetDataRoutes:
    def test_stocks_list(self, client: TestClient) -> None:
        resp = client.get("/api/dataset/test-market/stocks?min_records=0")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2
        assert data[0]["stockCode"] in ["7203", "9984"]

    def test_stocks_list_not_found(self, client: TestClient) -> None:
        resp = client.get("/api/dataset/nonexistent/stocks")
        assert resp.status_code == 404

    def test_dataset_resolver_not_initialized(self, test_dataset_dir: str) -> None:
        app = create_app()
        # 初期化漏れを再現
        app.state.dataset_resolver = None
        local_client = TestClient(app, raise_server_exceptions=False)
        resp = local_client.get("/api/dataset/test-market/stocks")
        assert resp.status_code == 422

    def test_stock_ohlcv(self, client: TestClient) -> None:
        resp = client.get("/api/dataset/test-market/stocks/7203/ohlcv")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2
        assert data[0]["date"] == "2024-01-04"
        assert data[0]["volume"] == 1000

    def test_ohlcv_batch(self, client: TestClient) -> None:
        resp = client.get("/api/dataset/test-market/stocks/ohlcv/batch?codes=7203,9984")
        assert resp.status_code == 200
        data = resp.json()
        assert "7203" in data
        assert len(data["7203"]) == 2
        assert len(data["9984"]) == 1

    def test_ohlcv_batch_too_many(self, client: TestClient) -> None:
        codes = ",".join([f"000{i}" for i in range(101)])
        resp = client.get(f"/api/dataset/test-market/stocks/ohlcv/batch?codes={codes}")
        assert resp.status_code == 400

    def test_topix(self, client: TestClient) -> None:
        resp = client.get("/api/dataset/test-market/topix")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert "volume" not in data[0]  # TOPIX has no volume

    def test_indices_list(self, client: TestClient) -> None:
        resp = client.get("/api/dataset/test-market/indices?min_records=0")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["indexCode"] == "0010"

    def test_index_data(self, client: TestClient) -> None:
        resp = client.get("/api/dataset/test-market/indices/0010")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1

    def test_margin_list(self, client: TestClient) -> None:
        resp = client.get("/api/dataset/test-market/margin?min_records=0")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2

    def test_margin_single(self, client: TestClient) -> None:
        resp = client.get("/api/dataset/test-market/margin/7203")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["longMarginVolume"] == 50000

    def test_margin_batch(self, client: TestClient) -> None:
        resp = client.get("/api/dataset/test-market/margin/batch?codes=7203,9984")
        assert resp.status_code == 200
        data = resp.json()
        assert "7203" in data

    def test_margin_batch_too_many(self, client: TestClient) -> None:
        codes = ",".join([f"000{i}" for i in range(101)])
        resp = client.get(f"/api/dataset/test-market/margin/batch?codes={codes}")
        assert resp.status_code == 400

    def test_statements_single(self, client: TestClient) -> None:
        resp = client.get("/api/dataset/test-market/statements/7203")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 3
        assert data[0]["disclosedDate"] == "2024-01-30"
        assert data[0]["earningsPerShare"] == 150.0

    def test_statements_batch(self, client: TestClient) -> None:
        resp = client.get("/api/dataset/test-market/statements/batch?codes=7203,9984")
        assert resp.status_code == 200
        data = resp.json()
        assert "7203" in data
        assert len(data["7203"]) == 3

    def test_statements_single_fy_filter(self, client: TestClient) -> None:
        resp = client.get("/api/dataset/test-market/statements/7203?period_type=FY")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["typeOfCurrentPeriod"] == "FY"

    def test_statements_single_1q_filter_includes_legacy_q1(
        self, client: TestClient
    ) -> None:
        resp = client.get("/api/dataset/test-market/statements/7203?period_type=1Q")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2
        assert {row["typeOfCurrentPeriod"] for row in data} == {"1Q", "Q1"}

    def test_statements_single_actual_only_false(self, client: TestClient) -> None:
        resp = client.get("/api/dataset/test-market/statements/7203?actual_only=false")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 4
        assert any(row["disclosedDate"] == "2024-10-30" for row in data)

    def test_statements_single_date_range(self, client: TestClient) -> None:
        resp = client.get(
            "/api/dataset/test-market/statements/7203?start_date=2024-04-01&end_date=2024-08-01"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert [row["disclosedDate"] for row in data] == ["2024-04-30", "2024-07-30"]

    def test_statements_batch_with_filters(self, client: TestClient) -> None:
        resp = client.get(
            "/api/dataset/test-market/statements/batch?codes=7203,9984&period_type=FY&actual_only=false"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "7203" in data
        assert len(data["7203"]) == 2
        assert {row["disclosedDate"] for row in data["7203"]} == {
            "2024-01-30",
            "2024-10-30",
        }

    def test_statements_batch_too_many(self, client: TestClient) -> None:
        codes = ",".join([f"000{i}" for i in range(101)])
        resp = client.get(f"/api/dataset/test-market/statements/batch?codes={codes}")
        assert resp.status_code == 400

    def test_sectors(self, client: TestClient) -> None:
        resp = client.get("/api/dataset/test-market/sectors")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) >= 1
        assert any(s["sectorName"] == "輸送用機器" for s in data)

    def test_sector_mapping(self, client: TestClient) -> None:
        resp = client.get("/api/dataset/test-market/sectors/mapping")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, dict)

    def test_sector_stock_mapping(self, client: TestClient) -> None:
        resp = client.get("/api/dataset/test-market/sectors/stock-mapping")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, dict)
        assert "輸送用機器" in data

    def test_sector_stocks(self, client: TestClient) -> None:
        resp = client.get("/api/dataset/test-market/sectors/%E8%BC%B8%E9%80%81%E7%94%A8%E6%A9%9F%E5%99%A8/stocks")
        assert resp.status_code == 200
        data = resp.json()
        assert "7203" in data

    def test_sector_stocks_invalid_uri_encoding(self, client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
        def _raise(_value: str) -> str:
            raise ValueError("bad encoding")

        monkeypatch.setattr(dataset_data_routes, "unquote", _raise)
        resp = client.get("/api/dataset/test-market/sectors/invalid/stocks")
        assert resp.status_code == 400
