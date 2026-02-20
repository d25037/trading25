"""Dataset Management ルートの統合テスト"""

from __future__ import annotations

import os
import sqlite3

import pytest
from fastapi.testclient import TestClient

from src.server.app import create_app
from src.server.services.dataset_resolver import DatasetResolver


@pytest.fixture
def test_dataset_dir(tmp_path):
    """テスト用のデータセットディレクトリ"""
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
            dividend_fy REAL, forecast_dividend_fy REAL,
            next_year_forecast_dividend_fy REAL,
            payout_ratio REAL, forecast_payout_ratio REAL,
            next_year_forecast_payout_ratio REAL, forecast_eps REAL,
            investing_cash_flow REAL, financing_cash_flow REAL,
            cash_and_equivalents REAL, total_assets REAL,
            shares_outstanding REAL, treasury_shares REAL,
            PRIMARY KEY (code, disclosed_date)
        );

        INSERT INTO stocks VALUES ('7203', 'トヨタ自動車', 'TOYOTA', '0111', 'プライム', '7', '輸送用機器', '3050', '輸送用機器', 'TOPIX Core30', '1949-05-16', NULL, NULL);
        INSERT INTO stocks VALUES ('9984', 'ソフトバンク', 'SB', '0111', 'プライム', '9', '情報・通信業', '3700', '情報・通信業', 'TOPIX Core30', '1994-07-22', NULL, NULL);

        INSERT INTO stock_data VALUES ('7203', '2024-01-04', 100, 110, 90, 105, 1000, 1.0, NULL);
        INSERT INTO stock_data VALUES ('9984', '2024-01-04', 200, 210, 190, 205, 500, 1.0, NULL);

        INSERT INTO dataset_info VALUES ('preset', 'primeMarket', NULL);
    """)
    conn.close()
    return str(tmp_path)


@pytest.fixture
def client(test_dataset_dir: str):
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

    def test_dataset_info(self, client: TestClient) -> None:
        resp = client.get("/api/dataset/test-market/info")
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "test-market"
        assert data["snapshot"]["totalStocks"] == 2
        assert data["snapshot"]["preset"] == "primeMarket"
        assert data["snapshot"]["validation"]["isValid"] is True

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
        db_path = os.path.join(test_dataset_dir, "to-delete.db")
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE dataset_info (key TEXT PRIMARY KEY, value TEXT)")
        conn.close()

        resp = client.delete("/api/dataset/to-delete")
        assert resp.status_code == 200
        assert resp.json()["success"] is True
        assert not os.path.exists(db_path)

    def test_delete_nonexistent(self, client: TestClient) -> None:
        resp = client.delete("/api/dataset/nonexistent")
        assert resp.status_code == 404
