"""DB Sync + Refresh ルートの統合テスト"""

from __future__ import annotations

import os
import sqlite3
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from src.entrypoints.http.app import create_app
from src.entrypoints.http.routes import db as db_routes
from src.entrypoints.http.schemas.db import SyncDataPlaneRequest, SyncRequest
from src.entrypoints.http.schemas.job import JobStatus
from src.infrastructure.db.market.market_db import MarketDb


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
        default_store = MagicMock()
        client.app.state.market_time_series_store = default_store

        with patch("src.entrypoints.http.routes.db.start_sync", new_callable=AsyncMock) as mock_start:
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
            assert mock_start.await_count == 1
            assert mock_start.await_args.kwargs["time_series_store"] is default_store
            assert mock_start.await_args.kwargs["close_time_series_store"] is False

    def test_sync_start_with_data_plane_override(self, client: TestClient) -> None:
        client.app.state.market_time_series_store = None
        with (
            patch("src.entrypoints.http.routes.db.start_sync", new_callable=AsyncMock) as mock_start,
            patch("src.entrypoints.http.routes.db.create_time_series_store") as mock_create_store,
        ):
            override_store = MagicMock()
            mock_create_store.return_value = override_store

            mock_job = MagicMock()
            mock_job.job_id = "test-job-override"
            mock_job.data.resolved_mode = "incremental"
            mock_job.cancelled = MagicMock()
            mock_start.return_value = mock_job

            resp = client.post(
                "/api/db/sync",
                json={
                    "mode": "incremental",
                    "dataPlane": {"backend": "duckdb-parquet"},
                },
            )

            assert resp.status_code == 202
            assert mock_create_store.call_count == 1
            assert mock_start.await_count == 1
            assert mock_start.await_args.kwargs["time_series_store"] is override_store
            assert mock_start.await_args.kwargs["close_time_series_store"] is False

    def test_sync_start_with_duckdb_data_plane_uses_app_store(self, client: TestClient) -> None:
        default_store = MagicMock()
        client.app.state.market_time_series_store = default_store

        with (
            patch("src.entrypoints.http.routes.db.start_sync", new_callable=AsyncMock) as mock_start,
            patch("src.entrypoints.http.routes.db.create_time_series_store") as mock_create_store,
        ):
            mock_job = MagicMock()
            mock_job.job_id = "test-job-default"
            mock_job.data.resolved_mode = "incremental"
            mock_job.cancelled = MagicMock()
            mock_start.return_value = mock_job

            resp = client.post(
                "/api/db/sync",
                json={"mode": "incremental", "dataPlane": {"backend": "duckdb-parquet"}},
            )

            assert resp.status_code == 202
            assert mock_create_store.call_count == 0
            assert mock_start.await_args.kwargs["time_series_store"] is default_store
            assert mock_start.await_args.kwargs["close_time_series_store"] is False

    def test_sync_start_duckdb_only_returns_422_when_backend_unavailable(self, client: TestClient) -> None:
        client.app.state.market_time_series_store = None
        with (
            patch("src.entrypoints.http.routes.db.start_sync", new_callable=AsyncMock) as mock_start,
            patch("src.entrypoints.http.routes.db.create_time_series_store") as mock_create_store,
        ):
            mock_create_store.return_value = None

            resp = client.post(
                "/api/db/sync",
                json={"mode": "incremental", "dataPlane": {"backend": "duckdb-parquet"}},
            )

            assert resp.status_code == 422
            body = resp.json()
            assert body["error"] == "Unprocessable Entity"
            assert "DuckDB market time-series store is unavailable" in body["message"]
            assert mock_start.await_count == 0

    def test_sync_conflict(self, client: TestClient) -> None:
        client.app.state.market_time_series_store = MagicMock()
        with patch("src.entrypoints.http.routes.db.start_sync", new_callable=AsyncMock) as mock_start:
            mock_start.return_value = None  # Active job exists
            resp = client.post("/api/db/sync", json={"mode": "auto"})
            assert resp.status_code == 409

    def test_sync_conflict_does_not_close_app_store(self, client: TestClient) -> None:
        with (
            patch("src.entrypoints.http.routes.db.start_sync", new_callable=AsyncMock) as mock_start,
            patch("src.entrypoints.http.routes.db.create_time_series_store") as mock_create_store,
        ):
            override_store = MagicMock()
            client.app.state.market_time_series_store = None
            mock_create_store.return_value = override_store
            mock_start.return_value = None

            resp = client.post(
                "/api/db/sync",
                json={"mode": "incremental", "dataPlane": {"backend": "duckdb-parquet"}},
            )

            assert resp.status_code == 409
            override_store.close.assert_not_called()

    def test_sync_start_exception_does_not_close_app_store(self, client: TestClient) -> None:
        with (
            patch("src.entrypoints.http.routes.db.start_sync", new_callable=AsyncMock) as mock_start,
            patch("src.entrypoints.http.routes.db.create_time_series_store") as mock_create_store,
        ):
            override_store = MagicMock()
            client.app.state.market_time_series_store = None
            mock_create_store.return_value = override_store
            mock_start.side_effect = RuntimeError("sync exploded")

            resp = client.post(
                "/api/db/sync",
                json={"mode": "incremental", "dataPlane": {"backend": "duckdb-parquet"}},
            )

            assert resp.status_code == 500
            override_store.close.assert_not_called()

    def test_resolve_time_series_store_rejects_unsupported_backend(self, client: TestClient) -> None:
        request = MagicMock()
        request.app.state.market_time_series_store = MagicMock()
        body = SyncRequest.model_construct(
            mode="incremental",
            dataPlane=SyncDataPlaneRequest.model_construct(backend="sqlite"),
        )

        with pytest.raises(HTTPException) as exc_info:
            db_routes._resolve_time_series_store(request, body)

        assert "Unsupported dataPlane backend" in str(exc_info.value.detail)

    def test_get_sync_job_not_found(self, client: TestClient) -> None:
        resp = client.get("/api/db/sync/jobs/nonexistent-id")
        assert resp.status_code == 404

    def test_get_active_sync_job_returns_null_when_idle(self, client: TestClient) -> None:
        with patch("src.entrypoints.http.routes.db.sync_job_manager.get_active_job") as mock_get_active:
            mock_get_active.return_value = None

            resp = client.get("/api/db/sync/jobs/active")
            assert resp.status_code == 200
            assert resp.json() is None

    def test_get_active_sync_job_success(self, client: TestClient) -> None:
        with patch("src.entrypoints.http.routes.db.sync_job_manager.get_active_job") as mock_get_active:
            job = MagicMock()
            job.job_id = "job-active"
            job.status = JobStatus.RUNNING
            job.data.resolved_mode = "incremental"
            job.data.mode.value = "incremental"
            job.progress = None
            job.result = None
            job.created_at = datetime.now(UTC)
            job.started_at = None
            job.completed_at = None
            job.error = None
            mock_get_active.return_value = job

            resp = client.get("/api/db/sync/jobs/active")
            assert resp.status_code == 200
            body = resp.json()
            assert body["jobId"] == "job-active"
            assert body["status"] == "running"
            assert body["mode"] == "incremental"

    def test_get_sync_job_success(self, client: TestClient) -> None:
        with patch("src.entrypoints.http.routes.db.sync_job_manager.get_job") as mock_get_job:
            job = MagicMock()
            job.job_id = "job-1"
            job.status = JobStatus.RUNNING
            job.data.resolved_mode = ""
            job.data.mode.value = "auto"
            job.progress = None
            job.result = None
            job.created_at = datetime.now(UTC)
            job.started_at = None
            job.completed_at = None
            job.error = None
            mock_get_job.return_value = job

            resp = client.get("/api/db/sync/jobs/job-1")
            assert resp.status_code == 200
            assert resp.json()["jobId"] == "job-1"

    def test_cancel_sync_job_not_found(self, client: TestClient) -> None:
        resp = client.delete("/api/db/sync/jobs/nonexistent-id")
        assert resp.status_code == 404

    def test_cancel_sync_job_invalid_status(self, client: TestClient) -> None:
        with patch("src.entrypoints.http.routes.db.sync_job_manager.get_job") as mock_get_job:
            job = MagicMock()
            job.status = JobStatus.COMPLETED
            mock_get_job.return_value = job

            resp = client.delete("/api/db/sync/jobs/job-1")
            assert resp.status_code == 400

    def test_cancel_sync_job_success(self, client: TestClient) -> None:
        with (
            patch("src.entrypoints.http.routes.db.sync_job_manager.get_job") as mock_get_job,
            patch("src.entrypoints.http.routes.db.sync_job_manager.cancel_job", new_callable=AsyncMock) as mock_cancel,
        ):
            job = MagicMock()
            job.status = JobStatus.RUNNING
            mock_get_job.return_value = job

            resp = client.delete("/api/db/sync/jobs/job-1")
            assert resp.status_code == 200
            assert resp.json()["success"] is True
            mock_cancel.assert_awaited_once_with("job-1")


class TestRefreshRoute:
    def test_refresh_success(self, client: TestClient) -> None:
        with (
            patch("src.entrypoints.http.routes.db._get_market_time_series_store") as mock_store,
            patch("src.application.services.stock_refresh_service.refresh_stocks") as mock_refresh,
        ):
            from src.entrypoints.http.schemas.db import RefreshResponse, RefreshStockResult
            mock_store.return_value = MagicMock()
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

    def test_refresh_no_jquants_client(self, market_db_path: str) -> None:
        app = create_app()
        app.state.market_db = MarketDb(market_db_path, read_only=False)
        app.state.jquants_client = None
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.post("/api/db/stocks/refresh", json={"codes": ["7203"]})
        assert resp.status_code == 422

    def test_refresh_not_initialized(self, market_db_path: str) -> None:
        app = create_app()
        market_db = MarketDb(market_db_path, read_only=False)
        market_db.set_sync_metadata("init_completed", "false")
        app.state.market_db = market_db
        mock_client = MagicMock()
        mock_client.has_api_key = True
        app.state.jquants_client = mock_client
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.post("/api/db/stocks/refresh", json={"codes": ["7203"]})
        assert resp.status_code == 422
