"""Dataset Data ルートの統合テスト"""

from __future__ import annotations

import os
import sqlite3

import pytest
from fastapi.testclient import TestClient

from src.server.app import create_app
from src.server.services.dataset_resolver import DatasetResolver


@pytest.fixture
def test_dataset_dir(tmp_path):
    """テスト用のデータセットディレクトリ + .db ファイル"""
    db_path = os.path.join(str(tmp_path), "test-market.db")
    conn = sqlite3.connect(db_path)
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
        CREATE TABLE dataset_info (
            key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT
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
            dividend_fy REAL, forecast_eps REAL,
            investing_cash_flow REAL, financing_cash_flow REAL,
            cash_and_equivalents REAL, total_assets REAL,
            shares_outstanding REAL, treasury_shares REAL,
            PRIMARY KEY (code, disclosed_date)
        );

        INSERT INTO stocks VALUES ('7203', 'トヨタ自動車', 'TOYOTA', '0111', 'プライム', '7', '輸送用機器', '3050', '輸送用機器', 'TOPIX Core30', '1949-05-16', NULL, NULL);
        INSERT INTO stocks VALUES ('9984', 'ソフトバンク', 'SB', '0111', 'プライム', '9', '情報・通信業', '3700', '情報・通信業', 'TOPIX Core30', '1994-07-22', NULL, NULL);

        INSERT INTO stock_data VALUES ('7203', '2024-01-04', 100, 110, 90, 105, 1000, 1.0, NULL);
        INSERT INTO stock_data VALUES ('7203', '2024-01-05', 105, 115, 95, 110, 1100, 1.0, NULL);
        INSERT INTO stock_data VALUES ('9984', '2024-01-04', 200, 210, 190, 205, 500, 1.0, NULL);

        INSERT INTO topix_data VALUES ('2024-01-04', 2500, 2520, 2490, 2510, NULL);

        INSERT INTO indices_data VALUES ('0010', '2024-01-04', 100, 102, 99, 101, '食料品', NULL);

        INSERT INTO margin_data VALUES ('7203', '2024-01-04', 50000, 30000);
        INSERT INTO margin_data VALUES ('9984', '2024-01-04', 40000, 20000);

        INSERT INTO statements VALUES ('7203', '2024-01-30', 150.0, 2000000, 5000000, 'FY', 'AnnualReport', 160.0, 3000, 20000000, 1500000, 1600000, 1800000, 60.0, 165.0, -500000, -300000, 4000000, 50000000, 330000000, 10000000);

        INSERT INTO dataset_info VALUES ('preset', 'primeMarket', NULL);
    """)
    conn.close()
    return str(tmp_path)


@pytest.fixture
def client(test_dataset_dir: str):
    """テスト用 FastAPI クライアント"""
    app = create_app()
    app.state.dataset_resolver = DatasetResolver(test_dataset_dir)
    return TestClient(app, raise_server_exceptions=False)


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

    def test_statements_single(self, client: TestClient) -> None:
        resp = client.get("/api/dataset/test-market/statements/7203")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["disclosedDate"] == "2024-01-30"
        assert data[0]["earningsPerShare"] == 150.0

    def test_statements_batch(self, client: TestClient) -> None:
        resp = client.get("/api/dataset/test-market/statements/batch?codes=7203,9984")
        assert resp.status_code == 200
        data = resp.json()
        assert "7203" in data

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
