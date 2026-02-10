"""DB Sync + Refresh ルートの統合テスト"""

from __future__ import annotations

import os
import sqlite3
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from src.server.app import create_app
from src.lib.market_db.market_db import MarketDb


@pytest.fixture
def market_db_path(tmp_path):
    db_path = os.path.join(str(tmp_path), "market.db")
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE stocks (code TEXT PRIMARY KEY, company_name TEXT NOT NULL, company_name_english TEXT, market_code TEXT NOT NULL, market_name TEXT NOT NULL, sector_17_code TEXT NOT NULL, sector_17_name TEXT NOT NULL, sector_33_code TEXT NOT NULL, sector_33_name TEXT NOT NULL, scale_category TEXT, listed_date TEXT NOT NULL, created_at TEXT, updated_at TEXT);
        CREATE TABLE stock_data (code TEXT NOT NULL, date TEXT NOT NULL, open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL, close REAL NOT NULL, volume INTEGER NOT NULL, adjustment_factor REAL, created_at TEXT, PRIMARY KEY (code, date));
        CREATE TABLE topix_data (date TEXT PRIMARY KEY, open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL, close REAL NOT NULL, created_at TEXT);
        CREATE TABLE indices_data (code TEXT NOT NULL, date TEXT NOT NULL, open REAL, high REAL, low REAL, close REAL, sector_name TEXT, created_at TEXT, PRIMARY KEY (code, date));
        CREATE TABLE sync_metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT);
        CREATE TABLE index_master (code TEXT PRIMARY KEY, name TEXT NOT NULL, name_english TEXT, category TEXT NOT NULL, data_start_date TEXT, created_at TEXT, updated_at TEXT);

        INSERT INTO sync_metadata VALUES ('init_completed', 'true', NULL);
        INSERT INTO sync_metadata VALUES ('last_sync_date', '2024-01-06T10:00:00', NULL);

        INSERT INTO topix_data VALUES ('2024-01-04', 2500, 2520, 2490, 2510, NULL);
        INSERT INTO topix_data VALUES ('2024-01-05', 2510, 2530, 2500, 2520, NULL);
    """)
    conn.close()
    return db_path


@pytest.fixture
def client(market_db_path: str):
    app = create_app()
    app.state.market_db = MarketDb(market_db_path, read_only=False)
    mock_client = MagicMock()
    mock_client.has_api_key = True
    app.state.jquants_client = mock_client
    return TestClient(app, raise_server_exceptions=False)


class TestSyncRoutes:
    def test_sync_start(self, client: TestClient) -> None:
        with patch("src.server.routes.db.start_sync", new_callable=AsyncMock) as mock_start:
            mock_job = MagicMock()
            mock_job.job_id = "test-job-123"
            mock_job.data.resolved_mode = "incremental"
            mock_job.cancelled = MagicMock()
            mock_start.return_value = mock_job

            resp = client.post("/api/db/sync", json={"mode": "incremental"})
            assert resp.status_code == 202
            data = resp.json()
            assert data["jobId"] == "test-job-123"
            assert data["mode"] == "incremental"
            assert data["status"] == "pending"

    def test_sync_conflict(self, client: TestClient) -> None:
        with patch("src.server.routes.db.start_sync", new_callable=AsyncMock) as mock_start:
            mock_start.return_value = None  # Active job exists
            resp = client.post("/api/db/sync", json={"mode": "auto"})
            assert resp.status_code == 409

    def test_get_sync_job_not_found(self, client: TestClient) -> None:
        resp = client.get("/api/db/sync/jobs/nonexistent-id")
        assert resp.status_code == 404

    def test_cancel_sync_job_not_found(self, client: TestClient) -> None:
        resp = client.delete("/api/db/sync/jobs/nonexistent-id")
        assert resp.status_code == 404


class TestRefreshRoute:
    def test_refresh_success(self, client: TestClient) -> None:
        with patch("src.server.services.stock_refresh_service.refresh_stocks") as mock_refresh:
            from src.server.schemas.db import RefreshResponse, RefreshStockResult
            mock_refresh.return_value = RefreshResponse(
                totalStocks=1,
                successCount=1,
                failedCount=0,
                totalApiCalls=1,
                totalRecordsStored=100,
                results=[RefreshStockResult(code="7203", success=True, recordsFetched=100, recordsStored=100)],
                lastUpdated="2024-01-06T10:00:00",
            )
            resp = client.post("/api/db/stocks/refresh", json={"codes": ["7203"]})
            assert resp.status_code == 200
            data = resp.json()
            assert data["totalStocks"] == 1
            assert data["successCount"] == 1

    def test_refresh_validation_error(self, client: TestClient) -> None:
        resp = client.post("/api/db/stocks/refresh", json={"codes": []})
        assert resp.status_code == 422

    def test_refresh_no_db(self) -> None:
        app = create_app()
        app.state.market_db = None
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.post("/api/db/stocks/refresh", json={"codes": ["7203"]})
        assert resp.status_code == 422
