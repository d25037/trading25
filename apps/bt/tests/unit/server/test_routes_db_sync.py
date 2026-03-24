"""DB Sync + Refresh ルートの統合テスト"""

from __future__ import annotations

import asyncio
import os
import shutil
from collections.abc import Generator
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
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


async def _collect_sync_events(job_id: str) -> list[dict[str, str]]:
    return [event async for event in db_routes._sync_job_event_generator(job_id)]


@pytest.fixture(scope="module")
def market_db_template_path(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("db-sync-routes")
    db_path = os.path.join(str(tmp_path), "market-template.duckdb")
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
def market_db_path(tmp_path, market_db_template_path: str):
    db_path = os.path.join(str(tmp_path), "market.duckdb")
    shutil.copyfile(market_db_template_path, db_path)
    return db_path


@pytest.fixture(scope="module")
def app_client() -> Generator[TestClient, None, None]:
    app = create_app()
    client = TestClient(app, raise_server_exceptions=False)
    try:
        yield client
    finally:
        client.close()


@pytest.fixture
def client(app_client: TestClient, market_db_path: str) -> Generator[TestClient, None, None]:
    market_db = MarketDb(market_db_path, read_only=False)
    mock_client = MagicMock()
    mock_client.has_api_key = True
    app_client.app.state.market_db = market_db
    app_client.app.state.jquants_client = mock_client
    try:
        yield app_client
    finally:
        market_db.close()
        app_client.app.state.market_db = None
        app_client.app.state.jquants_client = None
        app_client.app.state.market_time_series_store = None


class TestSyncRoutes:
    def test_create_market_resources_closes_db_when_store_unavailable(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        dummy_market_db = MagicMock()
        monkeypatch.setattr(
            db_routes,
            "_market_timeseries_paths",
            lambda: (Path("/tmp/test-market.duckdb"), Path("/tmp/test-parquet")),
        )
        monkeypatch.setattr(db_routes, "MarketDb", MagicMock(return_value=dummy_market_db))
        monkeypatch.setattr(db_routes, "create_time_series_store", MagicMock(return_value=None))

        with pytest.raises(RuntimeError, match="DuckDB market time-series store is unavailable"):
            db_routes._create_market_resources()

        dummy_market_db.close.assert_called_once()

    def test_close_resource_handles_missing_non_callable_and_exception(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        warning = MagicMock()
        monkeypatch.setattr(db_routes.logger, "warning", warning)

        db_routes._close_resource(None, label="none")
        db_routes._close_resource(object(), label="plain-object")

        broken = MagicMock()
        broken.close.side_effect = RuntimeError("boom")
        db_routes._close_resource(broken, label="broken")

        warning.assert_called_once()

    def test_install_market_reader_services_assigns_services(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        request = MagicMock()
        request.app.state = SimpleNamespace()
        reader = MagicMock()
        market_data_service = MagicMock()
        roe_service = MagicMock()
        margin_service = MagicMock()
        chart_service = MagicMock()

        monkeypatch.setattr(db_routes, "MarketDbReader", MagicMock(return_value=reader))
        monkeypatch.setattr(db_routes, "MarketDataService", MagicMock(return_value=market_data_service))
        monkeypatch.setattr(db_routes, "create_market_roe_service", MagicMock(return_value=roe_service))
        monkeypatch.setattr(
            db_routes,
            "create_market_margin_analytics_service",
            MagicMock(return_value=margin_service),
        )
        monkeypatch.setattr(db_routes, "ChartService", MagicMock(return_value=chart_service))

        db_routes._install_market_reader_services(request, "/tmp/market.duckdb")

        assert request.app.state.market_reader is reader
        assert request.app.state.market_data_service is market_data_service
        assert request.app.state.roe_service is roe_service
        assert request.app.state.margin_analytics_service is margin_service
        assert request.app.state.chart_service is chart_service

    def test_install_market_reader_services_degrades_when_reader_init_fails(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        request = MagicMock()
        request.app.state = SimpleNamespace()
        warning = MagicMock()
        monkeypatch.setattr(db_routes.logger, "warning", warning)
        monkeypatch.setattr(db_routes, "MarketDbReader", MagicMock(side_effect=RuntimeError("reader failed")))
        monkeypatch.setattr(db_routes, "MarketDataService", MagicMock())
        monkeypatch.setattr(db_routes, "create_market_roe_service", MagicMock(return_value="roe"))
        monkeypatch.setattr(
            db_routes,
            "create_market_margin_analytics_service",
            MagicMock(return_value="margin"),
        )
        monkeypatch.setattr(db_routes, "ChartService", MagicMock(return_value="chart"))

        db_routes._install_market_reader_services(request, "/tmp/market.duckdb")

        assert request.app.state.market_reader is None
        assert request.app.state.market_data_service is None
        assert request.app.state.roe_service == "roe"
        assert request.app.state.margin_analytics_service == "margin"
        assert request.app.state.chart_service == "chart"
        warning.assert_called_once()

    def test_reset_market_resources_replaces_app_state_and_deletes_old_files(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        duckdb_path = tmp_path / "market.duckdb"
        wal_path = tmp_path / "market.duckdb.wal"
        parquet_dir = tmp_path / "parquet"
        parquet_dir.mkdir()
        (parquet_dir / "stock_data.parquet").write_text("old")
        duckdb_path.write_text("old")
        wal_path.write_text("wal")

        current_reader = MagicMock()
        current_store = MagicMock()
        current_market_db = MagicMock()
        request = MagicMock()
        request.app.state = SimpleNamespace(
            market_reader=current_reader,
            market_data_service=MagicMock(),
            roe_service=MagicMock(),
            margin_analytics_service=MagicMock(),
            chart_service=MagicMock(),
            market_db=current_market_db,
            market_time_series_store=current_store,
        )
        new_market_db = MagicMock()
        new_store = MagicMock()
        install_services = MagicMock()
        close_cached = MagicMock()

        monkeypatch.setattr(db_routes, "_market_timeseries_paths", lambda: (duckdb_path, parquet_dir))
        monkeypatch.setattr(db_routes, "_create_market_resources", MagicMock(return_value=(new_market_db, new_store)))
        monkeypatch.setattr(db_routes, "_install_market_reader_services", install_services)
        monkeypatch.setattr(db_routes, "close_all_cached_data_access_clients", close_cached)

        market_db, store = db_routes._reset_market_resources(request)

        assert (market_db, store) == (new_market_db, new_store)
        current_reader.close.assert_called_once()
        current_store.close.assert_called_once()
        current_market_db.close.assert_called_once()
        close_cached.assert_called_once()
        assert not duckdb_path.exists()
        assert not wal_path.exists()
        assert not parquet_dir.exists()
        assert request.app.state.market_db is new_market_db
        assert request.app.state.market_time_series_store is new_store
        install_services.assert_called_once_with(request, str(duckdb_path))

    def test_get_db_stats_routes_to_service(self, monkeypatch: pytest.MonkeyPatch) -> None:
        request = MagicMock()
        request.app.state = SimpleNamespace(
            market_db=MagicMock(),
            market_time_series_store=MagicMock(),
        )
        stats_response = MagicMock()
        get_market_stats = MagicMock(return_value=stats_response)
        monkeypatch.setattr(db_routes.db_stats_service, "get_market_stats", get_market_stats)

        assert db_routes.get_db_stats(request) is stats_response
        get_market_stats.assert_called_once_with(
            request.app.state.market_db,
            time_series_store=request.app.state.market_time_series_store,
        )

    def test_get_db_validate_routes_to_service(self, monkeypatch: pytest.MonkeyPatch) -> None:
        request = MagicMock()
        request.app.state = SimpleNamespace(
            market_db=MagicMock(),
            market_time_series_store=MagicMock(),
        )
        validation_response = MagicMock()
        validate_market_db = MagicMock(return_value=validation_response)
        monkeypatch.setattr(db_routes.db_validation_service, "validate_market_db", validate_market_db)

        assert db_routes.get_db_validate(request) is validation_response
        validate_market_db.assert_called_once_with(
            request.app.state.market_db,
            time_series_store=request.app.state.market_time_series_store,
        )

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

    def test_sync_start_with_reset_before_sync(self, client: TestClient) -> None:
        default_store = MagicMock()
        client.app.state.market_time_series_store = default_store

        with patch("src.entrypoints.http.routes.db.start_sync", new_callable=AsyncMock) as mock_start:
            mock_job = MagicMock()
            mock_job.job_id = "test-job-reset"
            mock_job.data.resolved_mode = "initial"
            mock_job.cancelled = MagicMock()
            mock_start.return_value = mock_job

            resp = client.post(
                "/api/db/sync",
                json={"mode": "initial", "resetBeforeSync": True},
            )

            assert resp.status_code == 202
            assert mock_start.await_count == 1
            assert mock_start.await_args.kwargs["reset_before_sync"] is True
            assert callable(mock_start.await_args.kwargs["reset_market_snapshot"])

    def test_sync_start_rejects_reset_before_sync_outside_initial_mode(self, client: TestClient) -> None:
        client.app.state.market_time_series_store = MagicMock()

        with patch("src.entrypoints.http.routes.db.start_sync", new_callable=AsyncMock) as mock_start:
            resp = client.post(
                "/api/db/sync",
                json={"mode": "incremental", "resetBeforeSync": True},
            )

        assert resp.status_code == 422
        assert mock_start.await_count == 0

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

    @pytest.mark.asyncio
    async def test_sync_job_event_generator_emits_error_when_job_missing(self) -> None:
        with (
            patch("src.entrypoints.http.routes.db.sync_stream_manager.subscribe") as mock_subscribe,
            patch("src.entrypoints.http.routes.db.sync_stream_manager.unsubscribe") as mock_unsubscribe,
            patch("src.entrypoints.http.routes.db.sync_job_manager.get_job", return_value=None),
        ):
            mock_subscribe.return_value = asyncio.Queue()
            events = await _collect_sync_events("missing-job")

        assert len(events) == 1
        assert events[0]["event"] == "error"
        assert "missing-job" in events[0]["data"]
        mock_unsubscribe.assert_called_once()

    @pytest.mark.asyncio
    async def test_sync_job_event_generator_emits_heartbeat_and_stops_for_terminal_or_missing_jobs(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        running_job = MagicMock()
        running_job.job_id = "job-1"
        running_job.status = JobStatus.RUNNING
        running_job.data.resolved_mode = "incremental"
        running_job.data.mode.value = "incremental"
        running_job.data.enforce_bulk_for_stock_data = False
        running_job.data.fetch_details = []
        running_job.progress = None
        running_job.result = None
        running_job.created_at = datetime.now(UTC)
        running_job.started_at = None
        running_job.completed_at = None
        running_job.error = None

        completed_job = MagicMock()
        completed_job.job_id = "job-2"
        completed_job.status = JobStatus.COMPLETED
        completed_job.data.resolved_mode = "initial"
        completed_job.data.mode.value = "initial"
        completed_job.data.enforce_bulk_for_stock_data = False
        completed_job.data.fetch_details = []
        completed_job.progress = None
        completed_job.result = None
        completed_job.created_at = datetime.now(UTC)
        completed_job.started_at = None
        completed_job.completed_at = None
        completed_job.error = None

        queue = asyncio.Queue()
        queue.put_nowait(None)

        original_wait_for = db_routes.asyncio.wait_for
        call_count = 0

        async def fake_wait_for(coro, timeout: float):
            nonlocal call_count
            del timeout
            call_count += 1
            if call_count == 1:
                coro.close()
                raise asyncio.TimeoutError()
            return await original_wait_for(coro, timeout=0.01)

        with (
            patch("src.entrypoints.http.routes.db.sync_stream_manager.subscribe", return_value=queue),
            patch("src.entrypoints.http.routes.db.sync_stream_manager.unsubscribe") as mock_unsubscribe,
            patch("src.entrypoints.http.routes.db.sync_job_manager.get_job", side_effect=[running_job, completed_job]),
        ):
            monkeypatch.setattr(db_routes.asyncio, "wait_for", fake_wait_for)
            events = await _collect_sync_events("job-1")

        assert [event["event"] for event in events] == ["snapshot", "heartbeat"]
        mock_unsubscribe.assert_called_once()

    @pytest.mark.asyncio
    async def test_sync_job_event_generator_returns_when_latest_job_disappears(self) -> None:
        running_job = MagicMock()
        running_job.job_id = "job-1"
        running_job.status = JobStatus.RUNNING
        running_job.data.resolved_mode = "incremental"
        running_job.data.mode.value = "incremental"
        running_job.data.enforce_bulk_for_stock_data = False
        running_job.data.fetch_details = []
        running_job.progress = None
        running_job.result = None
        running_job.created_at = datetime.now(UTC)
        running_job.started_at = None
        running_job.completed_at = None
        running_job.error = None

        queue = asyncio.Queue()
        queue.put_nowait(SyncStreamEvent(event="job"))

        with (
            patch("src.entrypoints.http.routes.db.sync_stream_manager.subscribe", return_value=queue),
            patch("src.entrypoints.http.routes.db.sync_stream_manager.unsubscribe") as mock_unsubscribe,
            patch("src.entrypoints.http.routes.db.sync_job_manager.get_job", side_effect=[running_job, None]),
        ):
            events = await _collect_sync_events("job-1")

        assert len(events) == 1
        assert events[0]["event"] == "snapshot"
        mock_unsubscribe.assert_called_once()

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

    def test_stream_sync_job_not_found(self, client: TestClient) -> None:
        resp = client.get("/api/db/sync/jobs/nonexistent-id/stream")
        assert resp.status_code == 404

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

    def test_refresh_no_db(self, app_client: TestClient) -> None:
        app_client.app.state.market_db = None
        resp = app_client.post("/api/db/stocks/refresh", json={"codes": ["7203"]})
        assert resp.status_code == 422

    def test_refresh_no_jquants_client(self, app_client: TestClient, market_db_path: str) -> None:
        market_db = MarketDb(market_db_path, read_only=False)
        app_client.app.state.market_db = market_db
        app_client.app.state.jquants_client = None
        try:
            resp = app_client.post("/api/db/stocks/refresh", json={"codes": ["7203"]})
            assert resp.status_code == 422
        finally:
            market_db.close()

    def test_refresh_not_initialized(self, app_client: TestClient, market_db_path: str) -> None:
        market_db = MarketDb(market_db_path, read_only=False)
        market_db.set_sync_metadata("init_completed", "false")
        app_client.app.state.market_db = market_db
        app_client.app.state.market_time_series_store = MagicMock()
        mock_client = MagicMock()
        mock_client.has_api_key = True
        app_client.app.state.jquants_client = mock_client
        try:
            resp = app_client.post("/api/db/stocks/refresh", json={"codes": ["7203"]})
            assert resp.status_code == 422
        finally:
            market_db.close()
            app_client.app.state.market_time_series_store = None

    def test_refresh_rejects_legacy_stock_snapshot(self, app_client: TestClient) -> None:
        market_db = MagicMock()
        market_db.is_initialized.return_value = True
        market_db.is_legacy_stock_price_snapshot.return_value = True
        app_client.app.state.market_db = market_db
        app_client.app.state.market_time_series_store = MagicMock()
        mock_client = MagicMock()
        mock_client.has_api_key = True
        app_client.app.state.jquants_client = mock_client
        resp = app_client.post("/api/db/stocks/refresh", json={"codes": ["7203"]})
        assert resp.status_code == 422
        assert "Legacy market.duckdb detected" in resp.json()["message"]
        app_client.app.state.market_time_series_store = None
