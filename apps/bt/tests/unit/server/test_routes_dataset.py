"""Dataset Management ルートの統合テスト"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from src.application.services.dataset_resolver import DatasetResolver
from src.entrypoints.http.app import create_app
from src.infrastructure.db.dataset_io.dataset_writer import DatasetWriter


def _build_snapshot(base_dir: Path, name: str) -> None:
    writer = DatasetWriter(str(base_dir / f"{name}.db"))
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
    writer.set_dataset_info("preset", "primeMarket")
    writer.set_dataset_info("created_at", "2026-01-01T00:00:00+00:00")
    writer.set_dataset_info("stock_count", "2")
    writer.close()


@pytest.fixture
def test_dataset_dir(tmp_path):
    """テスト用のデータセットディレクトリ"""
    _build_snapshot(Path(tmp_path), "test-market")
    return str(tmp_path)


@pytest.fixture
def client(test_dataset_dir: str, monkeypatch: pytest.MonkeyPatch):
    from src.infrastructure.db.market import dataset_snapshot_reader as reader_module

    def _fake_validate(snapshot_dir: str | Path):
        snapshot_path = Path(snapshot_dir)
        return reader_module.DatasetSnapshotManifest.model_validate(
            {
                "schemaVersion": 1,
                "generatedAt": "2026-03-09T00:00:00+00:00",
                "dataset": {
                    "name": snapshot_path.name,
                    "preset": "primeMarket",
                    "duckdbFile": "dataset.duckdb",
                    "compatibilityDbFile": "dataset.db",
                    "parquetDir": "parquet",
                },
                "source": {
                    "backend": "duckdb-parquet",
                    "compatibilityArtifact": "dataset.db",
                },
                "counts": {
                    "stocks": 2,
                    "stock_data": 2,
                    "topix_data": 0,
                    "indices_data": 0,
                    "margin_data": 0,
                    "statements": 0,
                    "dataset_info": 3,
                },
                "coverage": {
                    "totalStocks": 2,
                    "stocksWithQuotes": 2,
                    "stocksWithStatements": 0,
                    "stocksWithMargin": 0,
                },
                "checksums": {
                    "duckdbSha256": "x",
                    "compatibilityDbSha256": "y",
                    "logicalSha256": "z",
                    "parquet": {},
                },
            }
        )

    monkeypatch.setattr(reader_module, "validate_dataset_snapshot", _fake_validate)
    app = create_app()
    app.state.dataset_resolver = DatasetResolver(test_dataset_dir)
    return TestClient(app, raise_server_exceptions=False)


class TestDatasetManagementRoutes:
    def test_list_datasets(self, client: TestClient) -> None:
        resp = client.get("/api/dataset")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["name"] == "test-market"
        assert data[0]["fileSize"] > 0
        assert data[0]["preset"] == "primeMarket"
        assert data[0]["createdAt"] == "2026-01-01T00:00:00+00:00"

    def test_list_datasets_handles_missing_dataset_info_table(self, client: TestClient, test_dataset_dir: str) -> None:
        broken_db_path = Path(test_dataset_dir) / "broken.db"
        conn = sqlite3.connect(broken_db_path)
        conn.executescript("""
            CREATE TABLE stocks (
                code TEXT PRIMARY KEY, company_name TEXT NOT NULL,
                company_name_english TEXT, market_code TEXT NOT NULL,
                market_name TEXT NOT NULL, sector_17_code TEXT NOT NULL,
                sector_17_name TEXT NOT NULL, sector_33_code TEXT NOT NULL,
                sector_33_name TEXT NOT NULL, scale_category TEXT,
                listed_date TEXT NOT NULL, created_at TEXT, updated_at TEXT
            );
            CREATE TABLE stock_data (
                code TEXT NOT NULL, date TEXT NOT NULL,
                open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL,
                close REAL NOT NULL, volume INTEGER NOT NULL,
                adjustment_factor REAL, created_at TEXT,
                PRIMARY KEY (code, date)
            );
            CREATE TABLE topix_data (
                date TEXT PRIMARY KEY, open REAL NOT NULL, high REAL NOT NULL,
                low REAL NOT NULL, close REAL NOT NULL, created_at TEXT
            );
            CREATE TABLE indices_data (
                code TEXT NOT NULL, date TEXT NOT NULL,
                open REAL, high REAL, low REAL, close REAL,
                sector_name TEXT, created_at TEXT,
                PRIMARY KEY (code, date)
            );
            CREATE TABLE margin_data (
                code TEXT NOT NULL, date TEXT NOT NULL,
                long_margin_volume REAL, short_margin_volume REAL,
                PRIMARY KEY (code, date)
            );
            CREATE TABLE statements (
                code TEXT NOT NULL, disclosed_date TEXT NOT NULL,
                earnings_per_share REAL, profit REAL, equity REAL,
                type_of_current_period TEXT, type_of_document TEXT,
                next_year_forecast_earnings_per_share REAL,
                bps REAL, sales REAL, operating_profit REAL,
                ordinary_profit REAL, operating_cash_flow REAL,
                dividend_fy REAL, forecast_dividend_fy REAL,
                next_year_forecast_dividend_fy REAL,
                payout_ratio REAL, forecast_payout_ratio REAL,
                next_year_forecast_payout_ratio REAL, forecast_eps REAL,
                investing_cash_flow REAL, financing_cash_flow REAL,
                cash_and_equivalents REAL, total_assets REAL,
                shares_outstanding REAL, treasury_shares REAL,
                PRIMARY KEY (code, disclosed_date)
            );
        """)
        conn.close()

        resp = client.get("/api/dataset")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2
        broken = next(item for item in data if item["name"] == "broken")
        assert broken["preset"] is None
        assert broken["createdAt"] is None

    def test_dataset_info(self, client: TestClient) -> None:
        resp = client.get("/api/dataset/test-market/info")
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "test-market"
        assert data["snapshot"]["createdAt"] == "2026-01-01T00:00:00+00:00"
        assert data["snapshot"]["totalStocks"] == 2
        assert data["snapshot"]["preset"] == "primeMarket"
        assert data["snapshot"]["validation"]["isValid"] is True
        assert data["stats"]["totalStocks"] == 2
        assert data["stats"]["totalQuotes"] == 2
        assert data["stats"]["dateRange"]["from"] == "2024-01-04"
        assert data["stats"]["dateRange"]["to"] == "2024-01-04"
        assert data["validation"]["details"]["dataCoverage"]["stocksWithQuotes"] == 2
        assert data["validation"]["details"]["stockCountValidation"]["isWithinRange"] is True

    def test_dataset_info_not_found(self, client: TestClient) -> None:
        resp = client.get("/api/dataset/nonexistent/info")
        assert resp.status_code == 404

    def test_dataset_sample(self, client: TestClient) -> None:
        resp = client.get("/api/dataset/test-market/sample?count=2&seed=42")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["codes"]) == 2

    def test_dataset_sample_deterministic(self, client: TestClient) -> None:
        resp1 = client.get("/api/dataset/test-market/sample?count=2&seed=42")
        resp2 = client.get("/api/dataset/test-market/sample?count=2&seed=42")
        assert resp1.json()["codes"] == resp2.json()["codes"]

    def test_dataset_search(self, client: TestClient) -> None:
        resp = client.get("/api/dataset/test-market/search?q=トヨタ")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["results"]) >= 1
        assert data["results"][0]["code"] == "7203"

    def test_dataset_search_by_code(self, client: TestClient) -> None:
        resp = client.get("/api/dataset/test-market/search?q=7203")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["results"]) >= 1

    def test_dataset_search_not_found(self, client: TestClient) -> None:
        resp = client.get("/api/dataset/nonexistent/search?q=test")
        assert resp.status_code == 404

    def test_delete_dataset(self, client: TestClient, test_dataset_dir: str) -> None:
        # Create a separate dataset for deletion
        delete_dir = Path(test_dataset_dir) / "to-delete"
        _build_snapshot(Path(test_dataset_dir), "to-delete")
        legacy_db = Path(test_dataset_dir) / "to-delete.db"
        legacy_db.write_text("", encoding="utf-8")

        resp = client.delete("/api/dataset/to-delete")
        assert resp.status_code == 200
        assert resp.json()["success"] is True
        assert not delete_dir.exists()
        assert not legacy_db.exists()

    def test_delete_nonexistent(self, client: TestClient) -> None:
        resp = client.delete("/api/dataset/nonexistent")
        assert resp.status_code == 404
