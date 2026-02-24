"""DB Stats + Validate ルートの統合テスト"""

from __future__ import annotations

import os
import sqlite3

import pytest
from fastapi.testclient import TestClient

from src.entrypoints.http.app import create_app
from src.infrastructure.db.market.market_db import MarketDb


@pytest.fixture
def market_db_path(tmp_path):
    """テスト用 market.db"""
    db_path = os.path.join(str(tmp_path), "market.db")
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
        CREATE TABLE sync_metadata (
            key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT
        );
        CREATE TABLE index_master (
            code TEXT PRIMARY KEY, name TEXT NOT NULL,
            name_english TEXT, category TEXT NOT NULL,
            data_start_date TEXT, created_at TEXT, updated_at TEXT
        );

        INSERT INTO stocks VALUES ('7203', 'トヨタ', 'TOYOTA', '0111', 'プライム', '7', '輸送用機器', '3050', '輸送用機器', 'TOPIX Core30', '1949-05-16', NULL, NULL);
        INSERT INTO stocks VALUES ('9984', 'SBG', 'SB', '0112', 'スタンダード', '9', '情報・通信', '3700', '情報・通信', NULL, '1994-07-22', NULL, NULL);

        INSERT INTO stock_data VALUES ('7203', '2024-01-04', 100, 110, 90, 105, 1000, 1.0, NULL);
        INSERT INTO stock_data VALUES ('7203', '2024-01-05', 105, 115, 95, 110, 1100, 0.5, NULL);
        INSERT INTO stock_data VALUES ('9984', '2024-01-04', 200, 210, 190, 205, 500, 1.0, NULL);

        INSERT INTO topix_data VALUES ('2024-01-04', 2500, 2520, 2490, 2510, NULL);
        INSERT INTO topix_data VALUES ('2024-01-05', 2510, 2530, 2500, 2520, NULL);
        INSERT INTO topix_data VALUES ('2024-01-06', 2520, 2540, 2510, 2530, NULL);

        INSERT INTO indices_data VALUES ('0010', '2024-01-04', 100, 102, 99, 101, '食料品', NULL);

        INSERT INTO index_master VALUES ('0010', '食料品', 'Food', 'sector33', NULL, NULL, NULL);

        INSERT INTO sync_metadata VALUES ('init_completed', 'true', NULL);
        INSERT INTO sync_metadata VALUES ('last_sync_date', '2024-01-06T10:00:00', NULL);
        INSERT INTO sync_metadata VALUES ('failed_dates', '["2024-01-03"]', NULL);
    """)
    conn.close()
    return db_path


@pytest.fixture
def client(market_db_path: str):
    app = create_app()
    app.state.market_db = MarketDb(market_db_path, read_only=False)
    return TestClient(app, raise_server_exceptions=False)


class TestDbStatsRoute:
    def test_stats_success(self, client: TestClient) -> None:
        resp = client.get("/api/db/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert data["initialized"] is True
        assert data["lastSync"] == "2024-01-06T10:00:00"
        assert data["topix"]["count"] == 3
        assert data["topix"]["dateRange"]["min"] == "2024-01-04"
        assert data["stocks"]["total"] == 2
        assert "プライム" in data["stocks"]["byMarket"]
        assert data["stockData"]["count"] == 3
        assert data["stockData"]["dateCount"] == 2
        assert data["indices"]["masterCount"] == 1
        assert data["indices"]["dataCount"] == 1
        assert data["fundamentals"]["count"] == 0
        assert data["fundamentals"]["primeCoverage"]["primeStocks"] >= 1
        assert data["databaseSize"] >= 0

    def test_stats_no_db(self) -> None:
        app = create_app()
        app.state.market_db = None
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.get("/api/db/stats")
        assert resp.status_code == 422


class TestDbValidateRoute:
    def test_validate_warning(self, client: TestClient) -> None:
        resp = client.get("/api/db/validate")
        assert resp.status_code == 200
        data = resp.json()
        # Has missing dates (topix has 2024-01-06 but stock_data doesn't)
        assert data["status"] in ["healthy", "warning"]
        assert data["initialized"] is True
        assert data["lastSync"] == "2024-01-06T10:00:00"
        assert data["topix"]["count"] == 3
        assert data["stocks"]["total"] == 2
        # stock_data has dates for 2024-01-04 and 2024-01-05 only
        # topix has 2024-01-06 too -> 1 missing date
        assert data["stockData"]["missingDatesCount"] >= 1
        # Adjustment events: 7203 on 2024-01-05 has adjustment_factor=0.5
        assert data["adjustmentEventsCount"] >= 1
        assert data["adjustmentEvents"][0]["adjustmentFactor"] == 0.5
        assert data["stocksNeedingRefreshCount"] >= 1
        assert data["failedDatesCount"] == 1
        assert "fundamentals" in data
        assert len(data["recommendations"]) > 0

    def test_validate_not_initialized(self, tmp_path) -> None:
        db_path = os.path.join(str(tmp_path), "empty.db")
        conn = sqlite3.connect(db_path)
        conn.executescript("""
            CREATE TABLE stocks (code TEXT PRIMARY KEY, company_name TEXT NOT NULL, company_name_english TEXT, market_code TEXT NOT NULL, market_name TEXT NOT NULL, sector_17_code TEXT NOT NULL, sector_17_name TEXT NOT NULL, sector_33_code TEXT NOT NULL, sector_33_name TEXT NOT NULL, scale_category TEXT, listed_date TEXT NOT NULL, created_at TEXT, updated_at TEXT);
            CREATE TABLE stock_data (code TEXT NOT NULL, date TEXT NOT NULL, open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL, close REAL NOT NULL, volume INTEGER NOT NULL, adjustment_factor REAL, created_at TEXT, PRIMARY KEY (code, date));
            CREATE TABLE topix_data (date TEXT PRIMARY KEY, open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL, close REAL NOT NULL, created_at TEXT);
            CREATE TABLE indices_data (code TEXT NOT NULL, date TEXT NOT NULL, open REAL, high REAL, low REAL, close REAL, sector_name TEXT, created_at TEXT, PRIMARY KEY (code, date));
            CREATE TABLE sync_metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT);
            CREATE TABLE index_master (code TEXT PRIMARY KEY, name TEXT NOT NULL, name_english TEXT, category TEXT NOT NULL, data_start_date TEXT, created_at TEXT, updated_at TEXT);
        """)
        conn.close()
        app = create_app()
        app.state.market_db = MarketDb(db_path, read_only=False)
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.get("/api/db/validate")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "error"
        assert data["initialized"] is False
