"""DB Sync + Refresh ルートの統合テスト"""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from src.entrypoints.http.app import create_app
from src.entrypoints.http.routes import db as db_routes
from src.entrypoints.http.schemas.db import SyncDataPlaneRequest, SyncRequest
from src.entrypoints.http.schemas.job import JobStatus
from src.application.services.sync_stream_manager import SyncStreamEvent
from src.infrastructure.db.market.market_db import MarketDb


@pytest.fixture
def market_db_path(tmp_path):
    db_path = os.path.join(str(tmp_path), "market.duckdb")
    db = MarketDb(db_path, read_only=False)
    db.upsert_topix_data([
        {"date": "2024-01-04", "open": 2500, "high": 2520, "low": 2490, "close": 2510},
        {"date": "2024-01-05", "open": 2510, "high": 2530, "low": 2500, "close": 2520},
    ])
    db.set_sync_metadata("init_completed", "true")
    db.set_sync_metadata("last_sync_date", "2024-01-06T10:00:00")
    db.close()
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
            assert mock_start.await_args.kwargs["enforce_bulk_for_stock_data"] is False

    def test_sync_start_with_bulk_enforcement(self, client: TestClient) -> None:
        default_store = MagicMock()
        client.app.state.market_time_series_store = default_store

        with patch("src.entrypoints.http.routes.db.start_sync", new_callable=AsyncMock) as mock_start:
            mock_job = MagicMock()
            mock_job.job_id = "test-job-bulk-on"
            mock_job.data.resolved_mode = "incremental"
            mock_job.cancelled = MagicMock()
            mock_start.return_value = mock_job

            resp = client.post(
                "/api/db/sync",
                json={"mode": "incremental", "enforceBulkForStockData": True},
            )

            assert resp.status_code == 202
            assert mock_start.await_count == 1
            assert mock_start.await_args.kwargs["time_series_store"] is default_store
            assert mock_start.await_args.kwargs["enforce_bulk_for_stock_data"] is True

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
            job.data.enforce_bulk_for_stock_data = False
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
            assert body["enforceBulkForStockData"] is False

    def test_get_sync_job_success(self, client: TestClient) -> None:
        with patch("src.entrypoints.http.routes.db.sync_job_manager.get_job") as mock_get_job:
            job = MagicMock()
            job.job_id = "job-1"
            job.status = JobStatus.RUNNING
            job.data.resolved_mode = ""
            job.data.mode.value = "auto"
            job.data.enforce_bulk_for_stock_data = False
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

    def test_get_sync_job_fetch_details_success(self, client: TestClient) -> None:
        with patch("src.entrypoints.http.routes.db.sync_job_manager.get_job") as mock_get_job:
            job = MagicMock()
            job.job_id = "job-1"
            job.status = JobStatus.RUNNING
            job.data.resolved_mode = "incremental"
            job.data.mode.value = "incremental"
            job.data.fetch_details = [
                {
                    "eventType": "strategy",
                    "stage": "stock_data",
                    "endpoint": "/equities/bars/daily",
                    "method": "bulk",
                    "targetLabel": "42 dates",
                    "reason": "bulk_estimate_lower",
                    "reasonDetail": None,
                    "estimatedRestCalls": 120,
                    "estimatedBulkCalls": 6,
                    "plannerApiCalls": 1,
                    "fallback": False,
                    "fallbackReason": None,
                    "timestamp": "2026-03-05T00:00:00+00:00",
                }
            ]
            mock_get_job.return_value = job

            resp = client.get("/api/db/sync/jobs/job-1/fetch-details")
            assert resp.status_code == 200
            body = resp.json()
            assert body["jobId"] == "job-1"
            assert body["latest"]["endpoint"] == "/equities/bars/daily"
            assert body["latest"]["eventType"] == "strategy"
            assert len(body["items"]) == 1

    def test_get_sync_job_fetch_details_not_found(self, client: TestClient) -> None:
        resp = client.get("/api/db/sync/jobs/nonexistent-id/fetch-details")
        assert resp.status_code == 404

    def test_stream_sync_job_success(self, client: TestClient) -> None:
        with (
            patch("src.entrypoints.http.routes.db.sync_job_manager.get_job") as mock_get_job,
            patch("src.entrypoints.http.routes.db.sync_stream_manager.subscribe") as mock_subscribe,
            patch("src.entrypoints.http.routes.db.sync_stream_manager.unsubscribe") as mock_unsubscribe,
        ):
            job = MagicMock()
            job.job_id = "job-1"
            job.status = JobStatus.RUNNING
            job.data.resolved_mode = "incremental"
            job.data.mode.value = "incremental"
            job.data.enforce_bulk_for_stock_data = True
            job.data.fetch_details = [
                {
                    "eventType": "strategy",
                    "stage": "stock_data",
                    "endpoint": "/equities/bars/daily",
                    "method": "bulk",
                    "targetLabel": "42 dates",
                    "reason": "bulk_estimate_lower",
                    "reasonDetail": None,
                    "estimatedRestCalls": 120,
                    "estimatedBulkCalls": 6,
                    "plannerApiCalls": 1,
                    "fallback": False,
                    "fallbackReason": None,
                    "timestamp": "2026-03-05T00:00:00Z",
                }
            ]
            job.progress = None
            job.result = None
            job.created_at = datetime.now(UTC)
            job.started_at = None
            job.completed_at = None
            job.error = None
            mock_get_job.return_value = job

            queue = asyncio.Queue()
            queue.put_nowait(
                SyncStreamEvent(
                    event="fetch-detail",
                    payload=job.data.fetch_details[0],
                )
            )
            queue.put_nowait(SyncStreamEvent(event="job"))
            queue.put_nowait(None)
            mock_subscribe.return_value = queue

            resp = client.get("/api/db/sync/jobs/job-1/stream")

        assert resp.status_code == 200
        assert "event: snapshot" in resp.text
        assert "event: fetch-detail" in resp.text
        assert "event: job" in resp.text
        assert '"enforceBulkForStockData": true' in resp.text
        assert "/equities/bars/daily" in resp.text
        mock_unsubscribe.assert_called_once()

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

    def test_cancel_sync_job_returns_409_when_job_finishes_during_cancel(self, client: TestClient) -> None:
        with (
            patch("src.entrypoints.http.routes.db.sync_job_manager.get_job") as mock_get_job,
            patch("src.entrypoints.http.routes.db.sync_job_manager.cancel_job", new_callable=AsyncMock) as mock_cancel,
            patch("src.entrypoints.http.routes.db.sync_stream_manager.publish") as mock_publish,
            patch("src.entrypoints.http.routes.db.sync_stream_manager.close") as mock_close,
        ):
            running_job = MagicMock()
            running_job.status = JobStatus.RUNNING

            completed_job = MagicMock()
            completed_job.status = JobStatus.COMPLETED

            mock_get_job.side_effect = [running_job, completed_job]
            mock_cancel.return_value = False

            resp = client.delete("/api/db/sync/jobs/job-1")

            assert resp.status_code == 409
            assert "already finished while cancelling" in resp.json()["message"]
            mock_publish.assert_not_called()
            mock_close.assert_not_called()


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
