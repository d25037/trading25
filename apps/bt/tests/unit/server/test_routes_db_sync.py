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
from pydantic import ValidationError

from src.entrypoints.http.app import create_app
from src.entrypoints.http.routes import db as db_routes
from src.entrypoints.http.schemas.db import (
    CreateSyncJobResponse,
    SyncFetchDetailsResponse,
    SyncJobResponse,
    SyncRequest,
)
from src.application.contracts.jobs import JobStatus
from src.application.services.sync_stream_manager import SyncStreamEvent
from src.shared.contracts import market_maintenance as maintenance_contracts
from tests.unit.server.db.market_writer_test_support import open_market_db
from tests.unit.server.db.market_writer_test_support import (
    connect_market_duckdb_for_test,
)
from tests.unit.server.db.market_writer_test_support import publish_topix_data


@pytest.mark.parametrize("removed_mode", ["auto", "repair"])
@pytest.mark.parametrize(
    ("response_type", "payload"),
    [
        (
            CreateSyncJobResponse,
            {
                "jobId": "job-1",
                "status": "pending",
                "estimatedApiCalls": 1,
            },
        ),
        (
            SyncJobResponse,
            {
                "jobId": "job-1",
                "status": "running",
                "startedAt": "2026-07-22T00:00:00+00:00",
            },
        ),
        (
            SyncFetchDetailsResponse,
            {
                "jobId": "job-1",
                "status": "running",
            },
        ),
    ],
)
def test_sync_response_contracts_reject_removed_modes(
    removed_mode: str,
    response_type: type[
        CreateSyncJobResponse | SyncJobResponse | SyncFetchDetailsResponse
    ],
    payload: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        response_type.model_validate({**payload, "mode": removed_mode})


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("route", "starter_name", "body", "operation"),
    [
        (
            db_routes.start_sync_job,
            "start_sync",
            SyncRequest(mode="incremental"),
            "incremental_sync",
        ),
    ],
)
async def test_job_start_cancellation_finalizes_reserved_writer_before_reraise(
    monkeypatch: pytest.MonkeyPatch,
    route: object,
    starter_name: str,
    body: SyncRequest | None,
    operation: str,
) -> None:
    session = object.__new__(db_routes.MarketWriterSession)
    app = SimpleNamespace(
        state=SimpleNamespace(
            market_writer_session=None,
            market_writer_owner=None,
            jquants_client=MagicMock(),
        )
    )
    request = MagicMock(app=app)
    request.state = SimpleNamespace()

    def reserve(_request: object) -> tuple[object, object]:
        owner = object()
        app.state.market_writer_session = session
        app.state.market_writer_owner = owner
        request.state.market_writer_owner = owner
        return MagicMock(), MagicMock()

    async def cancel_during_job_create(*_args: object, **_kwargs: object) -> None:
        raise asyncio.CancelledError

    finalized: list[dict[str, object]] = []

    async def finalize(_request: object, **kwargs: object) -> None:
        finalized.append(kwargs)
        app.state.market_writer_session = None
        app.state.market_writer_owner = None

    monkeypatch.setattr(db_routes, "_prepare_market_write_resources", reserve)
    monkeypatch.setattr(db_routes, starter_name, cancel_during_job_create)
    monkeypatch.setattr(db_routes, "_finalize_direct_market_write", finalize)
    monkeypatch.setattr(db_routes.sync_job_manager, "get_active_job", lambda: None)

    with pytest.raises(asyncio.CancelledError):
        if body is None:
            await route(request)  # type: ignore[operator]
        else:
            await route(request, body)  # type: ignore[operator]

    assert finalized == [
        {
            "operation": operation,
            "operation_outcome": maintenance_contracts.MarketOperationOutcome.CANCELLED,
            "operation_error": "Request cancelled while creating Market job",
        }
    ]
    assert app.state.market_writer_session is None
    assert app.state.market_writer_owner is None


@pytest.mark.asyncio
async def test_reset_initial_job_conflict_does_not_finalize_active_incremental_writer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    active_owner = object()
    active_session = object.__new__(db_routes.MarketWriterSession)
    active_session.close_writable_handles = MagicMock()
    app = SimpleNamespace(
        state=SimpleNamespace(
            market_writer_session=active_session,
            market_writer_owner=active_owner,
            jquants_client=MagicMock(),
        )
    )
    request = MagicMock(app=app)
    request.state = SimpleNamespace()
    finalize = AsyncMock()
    monkeypatch.setattr(db_routes, "start_sync", AsyncMock(return_value=None))
    monkeypatch.setattr(db_routes, "_finalize_direct_market_write", finalize)

    with pytest.raises(HTTPException) as exc_info:
        await db_routes.start_sync_job(
            request,
            SyncRequest(mode="initial", resetBeforeSync=True),
        )

    assert exc_info.value.status_code == 409
    finalize.assert_not_awaited()
    active_session.close_writable_handles.assert_not_called()
    assert app.state.market_writer_session is active_session
    assert app.state.market_writer_owner is active_owner


@pytest.mark.asyncio
async def test_direct_finalizer_release_compensation_updates_http_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = SimpleNamespace(state=SimpleNamespace(market_maintenance=None))
    request = MagicMock(app=app)
    monkeypatch.setattr(db_routes, "_build_market_finalizer", MagicMock())

    async def release_fails(_finalizer: object, **kwargs: object) -> object:
        passed = maintenance_contracts.MarketMaintenanceRecord(
            evidenceStatus="valid",
            outcome="passed",
            operation="intraday_sync",
            recordedAt="2026-07-16T00:00:00+00:00",
            compacted=False,
            trigger="none",
            beforeBytes=1,
            afterBytes=1,
            durationMs=1,
            validation="passed",
            schemaFingerprint="schema",
            tableCounts={},
            semanticDigests={},
        )
        decision = db_routes.MarketFinalizationDecision(
            terminal_outcome=maintenance_contracts.MarketOperationOutcome.SUCCEEDED,
            maintenance=passed,
        )
        kwargs["stage_terminal"](decision)  # type: ignore[operator]
        failed = db_routes.MarketFinalizationDecision(
            terminal_outcome=maintenance_contracts.MarketOperationOutcome.FAILED,
            maintenance=maintenance_contracts.MarketMaintenanceRecord.failed(
                operation="intraday_sync",
                recorded_at="2026-07-16T00:00:00+00:00",
                error="Writer ownership release incomplete: unlock failed",
            ),
            error="Writer ownership release failed: unlock failed",
        )
        kwargs["publish_terminal"](failed)  # type: ignore[operator]
        return failed

    monkeypatch.setattr(
        db_routes,
        "finalize_market_operation_joined",
        release_fails,
    )

    decision = await db_routes._finalize_direct_market_write(
        request,
        operation="intraday_sync",
        operation_outcome=maintenance_contracts.MarketOperationOutcome.SUCCEEDED,
    )

    assert decision.terminal_outcome is maintenance_contracts.MarketOperationOutcome.FAILED
    assert app.state.market_maintenance.outcome is maintenance_contracts.MaintenanceOutcome.FAILED


def test_failed_resource_attach_keeps_writer_ownership_discoverable() -> None:
    owner = object()
    session = object.__new__(db_routes.MarketWriterSession)
    session.fenced = False
    state = SimpleNamespace(
        market_writer_owner=owner,
        market_writer_session=session,
        market_db=object(),
        market_time_series_store=object(),
        market_reader=object(),
        market_data_service=object(),
    )
    app = SimpleNamespace(state=state)
    resources = MagicMock()
    resources.identity.path = Path("/unavailable/market.duckdb")
    resources.close.side_effect = RuntimeError("resource close failed")

    with (
        patch.object(
            db_routes,
            "MarketDbReader",
            side_effect=RuntimeError("reader construction failed"),
        ),
        pytest.raises(RuntimeError, match="reader construction failed"),
    ):
        db_routes._attach_finalized_market_resources(
            app,
            owner,
            resources,
            MagicMock(),
        )

    assert state.market_writer_owner is owner
    assert state.market_writer_session is session
    assert session.fenced is True
    assert state.market_db is None
    assert state.market_time_series_store is None
    assert state.market_reader is None
    assert state.market_data_service is None


async def _collect_sync_events(job_id: str) -> list[dict[str, str]]:
    return [event async for event in db_routes._sync_job_event_generator(job_id)]


@pytest.fixture(scope="module")
def market_db_template_path(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("db-sync-routes")
    db_path = os.path.join(str(tmp_path), "market-template.duckdb")
    db = open_market_db(db_path, read_only=False)
    publish_topix_data(
        db,
        [
            {
                "date": "2024-01-04",
                "open": 2500,
                "high": 2520,
                "low": 2490,
                "close": 2510,
            },
            {
                "date": "2024-01-05",
                "open": 2510,
                "high": 2530,
                "low": 2500,
                "close": 2520,
            },
        ],
    )
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
def client(
    app_client: TestClient, market_db_path: str
) -> Generator[TestClient, None, None]:
    market_db = open_market_db(market_db_path, read_only=False)
    mock_client = MagicMock()
    mock_client.has_api_key = True
    mock_client.plan = "premium"
    app_client.app.state.market_db = market_db
    app_client.app.state.jquants_client = mock_client
    try:
        yield app_client
    finally:
        session = getattr(app_client.app.state, "market_writer_session", None)
        if session is not None:
            token = session.close_writable_handles()
            read_only = session.reopen_read_only(token)
            session.release_after_read_only_reopen(token)
            read_only.close()
            app_client.app.state.market_writer_session = None
        else:
            market_db.close()
        app_client.app.state.market_db = None
        app_client.app.state.jquants_client = None
        app_client.app.state.market_time_series_store = None


class TestSyncRoutes:
    @pytest.mark.parametrize("mode", ["auto", "repair", "unknown"])
    def test_sync_request_rejects_removed_modes(self, mode: str) -> None:
        with pytest.raises(ValidationError):
            SyncRequest(mode=mode)  # type: ignore[arg-type]

    def test_sync_request_defaults_to_incremental(self) -> None:
        assert SyncRequest().mode == "incremental"

    def test_sync_request_requires_reset_for_initial(self) -> None:
        with pytest.raises(ValidationError, match="requires resetBeforeSync=true"):
            SyncRequest(mode="initial")

    def test_remember_market_paths_normalizes_owned_runtime_relative_db_path(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        market_root = tmp_path / "market-timeseries"
        market_root.mkdir()
        monkeypatch.chdir(market_root)
        request = MagicMock()
        request.app.state = SimpleNamespace(
            market_db=SimpleNamespace(db_path="market.duckdb")
        )

        duckdb_path, parquet_dir = db_routes._remember_market_paths(request)

        assert duckdb_path == market_root / "market.duckdb"
        assert parquet_dir == market_root / "parquet"
        assert request.app.state.market_duckdb_path == str(duckdb_path)
        assert request.app.state.market_parquet_dir == str(parquet_dir)

    def test_incremental_missing_market_fails_without_resetting_orphan_parquet(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        market_root = tmp_path / "market-timeseries"
        market_root.mkdir()
        duckdb_path = market_root / "market.duckdb"
        parquet_dir = market_root / "parquet"
        parquet_dir.mkdir()
        orphan = parquet_dir / "orphan.parquet"
        orphan.write_bytes(b"must survive")
        request = MagicMock()
        request.state = SimpleNamespace()
        lease = SimpleNamespace(exclusive=True)
        request.app.state = SimpleNamespace(
            market_writer_session=None,
            market_writer_owner=None,
            market_operation_lease=lease,
        )
        factory = MagicMock()
        monkeypatch.setattr(
            db_routes,
            "_remember_market_paths",
            MagicMock(return_value=(duckdb_path, parquet_dir)),
        )
        clear = MagicMock()
        monkeypatch.setattr(db_routes, "_clear_market_resources", clear)
        monkeypatch.setattr(db_routes, "_writer_factory", MagicMock(return_value=factory))

        with pytest.raises(RuntimeError, match="RESET initial"):
            db_routes._prepare_market_write_resources(request)

        clear.assert_not_called()
        factory.reset_and_open.assert_not_called()
        factory.open_existing.assert_not_called()
        assert orphan.read_bytes() == b"must survive"

    def test_second_writer_request_does_not_clear_or_close_owner_session(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        owner_request = MagicMock()
        owner_request.state = SimpleNamespace()
        owner_request.app.state = SimpleNamespace(
            market_writer_session=None,
            market_writer_owner=None,
        )
        contender_request = MagicMock()
        contender_request.state = SimpleNamespace()
        contender_request.app = owner_request.app
        session = object.__new__(db_routes.MarketWriterSession)
        session.handles = SimpleNamespace(
            market_db=MagicMock(),
            time_series_store=MagicMock(),
        )
        session.close_writable_handles = MagicMock()
        factory = MagicMock()
        factory.open_existing.return_value = session
        duckdb_path = tmp_path / "market.duckdb"
        duckdb_path.touch()
        monkeypatch.setattr(
            db_routes,
            "_remember_market_paths",
            MagicMock(return_value=(duckdb_path, tmp_path / "parquet")),
        )
        clear = MagicMock()
        monkeypatch.setattr(db_routes, "_clear_market_resources", clear)
        monkeypatch.setattr(
            db_routes, "_writer_factory", MagicMock(return_value=factory)
        )

        db_routes._prepare_market_write_resources(owner_request)
        with pytest.raises(HTTPException) as exc_info:
            db_routes._prepare_market_write_resources(contender_request)
        assert exc_info.value.status_code == 409
        assert clear.call_count == 1
        session.close_writable_handles.assert_not_called()

        with pytest.raises(RuntimeError, match="common finalizer"):
            db_routes._restore_unreserved_read_only_resources(contender_request)
        session.close_writable_handles.assert_not_called()

    @pytest.mark.asyncio
    async def test_writer_reservation_keeps_event_loop_live_across_await_window(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        app = SimpleNamespace(
            state=SimpleNamespace(market_writer_session=None, market_writer_owner=None)
        )
        owner_request = MagicMock(app=app)
        owner_request.state = SimpleNamespace()
        contender_request = MagicMock(app=app)
        contender_request.state = SimpleNamespace()
        session = object.__new__(db_routes.MarketWriterSession)
        session.handles = SimpleNamespace(
            market_db=MagicMock(),
            time_series_store=MagicMock(),
        )
        factory = MagicMock()
        factory.open_existing.return_value = session
        duckdb_path = tmp_path / "market.duckdb"
        duckdb_path.touch()
        monkeypatch.setattr(
            db_routes,
            "_remember_market_paths",
            MagicMock(return_value=(duckdb_path, tmp_path / "parquet")),
        )
        clear = MagicMock()
        monkeypatch.setattr(db_routes, "_clear_market_resources", clear)
        monkeypatch.setattr(
            db_routes, "_writer_factory", MagicMock(return_value=factory)
        )

        owner_ready = asyncio.Event()
        release_owner = asyncio.Event()
        heartbeat = asyncio.Event()

        async def owner_mutator() -> None:
            db_routes._prepare_market_write_resources(owner_request)
            owner_ready.set()
            await release_owner.wait()

        async def contender_mutator() -> None:
            await owner_ready.wait()
            with pytest.raises(HTTPException) as exc_info:
                db_routes._prepare_market_write_resources(contender_request)
            assert exc_info.value.status_code == 409
            heartbeat.set()
            release_owner.set()

        await asyncio.wait_for(
            asyncio.gather(owner_mutator(), contender_mutator()),
            timeout=0.5,
        )
        assert heartbeat.is_set()
        assert clear.call_count == 1
        app.state.market_writer_session = None
        app.state.market_writer_owner = None

    def test_create_market_resources_uses_read_only_factory(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        dummy_market_db = MagicMock()
        dummy_store = MagicMock()
        resources = SimpleNamespace(
            market_db=dummy_market_db,
            time_series_store=dummy_store,
        )
        factory = MagicMock()
        factory.read_only_factory.open_existing.return_value = resources
        monkeypatch.setattr(
            db_routes,
            "_market_timeseries_paths",
            lambda: (Path("/tmp/test-market.duckdb"), Path("/tmp/test-parquet")),
        )
        monkeypatch.setattr(
            db_routes, "MarketWriterResourceFactory", MagicMock(return_value=factory)
        )

        assert db_routes._create_market_resources() == (dummy_market_db, dummy_store)
        factory.read_only_factory.open_existing.assert_called_once()

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
        monkeypatch.setattr(
            db_routes, "MarketDataService", MagicMock(return_value=market_data_service)
        )
        monkeypatch.setattr(
            db_routes, "create_market_roe_service", MagicMock(return_value=roe_service)
        )
        monkeypatch.setattr(
            db_routes,
            "create_market_margin_analytics_service",
            MagicMock(return_value=margin_service),
        )
        monkeypatch.setattr(
            db_routes, "ChartService", MagicMock(return_value=chart_service)
        )

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
        monkeypatch.setattr(
            db_routes,
            "MarketDbReader",
            MagicMock(side_effect=RuntimeError("reader failed")),
        )
        monkeypatch.setattr(db_routes, "MarketDataService", MagicMock())
        monkeypatch.setattr(
            db_routes, "create_market_roe_service", MagicMock(return_value="roe")
        )
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
            market_operation_lease=None,
        )
        new_market_db = MagicMock()
        new_store = MagicMock()
        install_services = MagicMock()
        close_cached = MagicMock()

        monkeypatch.setattr(
            db_routes, "_market_timeseries_paths", lambda: (duckdb_path, parquet_dir)
        )
        session = SimpleNamespace(
            handles=SimpleNamespace(
                market_db=new_market_db, time_series_store=new_store
            )
        )
        factory = MagicMock()
        factory.reset_and_open.return_value = session
        monkeypatch.setattr(
            db_routes, "_writer_factory", MagicMock(return_value=factory)
        )
        monkeypatch.setattr(
            db_routes, "_install_market_reader_services", install_services
        )
        monkeypatch.setattr(
            db_routes, "close_all_cached_data_access_clients", close_cached
        )

        market_db, store = db_routes._reset_market_resources(request)

        assert (market_db, store) == (new_market_db, new_store)
        current_reader.close.assert_called_once()
        current_store.close.assert_called_once()
        current_market_db.close.assert_called_once()
        close_cached.assert_called_once()
        assert request.app.state.market_db is new_market_db
        assert request.app.state.market_time_series_store is new_store
        install_services.assert_not_called()
        factory.reset_and_open.assert_called_once_with(blocking=False, lease=None)

    @pytest.mark.parametrize(
        "failing_resource_name",
        ["market_reader", "market_time_series_store", "market_db"],
    )
    def test_reset_market_resources_aborts_before_deletion_when_handle_close_fails(
        self,
        failing_resource_name: str,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        market_root = tmp_path / "market-timeseries"
        market_root.mkdir()
        duckdb_path = market_root / "market.duckdb"
        wal_path = market_root / "market.duckdb.wal"
        parquet_dir = market_root / "parquet"
        parquet_dir.mkdir()
        parquet_file = parquet_dir / "stock_data.parquet"
        duckdb_path.write_text("original database")
        wal_path.write_text("original wal")
        parquet_file.write_text("original parquet")

        current_reader = MagicMock()
        current_store = MagicMock()
        current_market_db = MagicMock()
        resources = {
            "market_reader": current_reader,
            "market_time_series_store": current_store,
            "market_db": current_market_db,
        }
        resources[failing_resource_name].close.side_effect = RuntimeError(
            f"injected {failing_resource_name} close failure"
        )
        market_data_service = MagicMock()
        roe_service = MagicMock()
        margin_service = MagicMock()
        chart_service = MagicMock()
        request = MagicMock()
        request.app.state = SimpleNamespace(
            market_reader=current_reader,
            market_data_service=market_data_service,
            roe_service=roe_service,
            margin_analytics_service=margin_service,
            chart_service=chart_service,
            market_db=current_market_db,
            market_time_series_store=current_store,
            market_operation_lease=None,
        )
        factory = MagicMock()
        monkeypatch.setattr(
            db_routes, "_market_timeseries_paths", lambda: (duckdb_path, parquet_dir)
        )
        monkeypatch.setattr(
            db_routes, "_writer_factory", MagicMock(return_value=factory)
        )
        close_cached = MagicMock()
        monkeypatch.setattr(
            db_routes, "close_all_cached_data_access_clients", close_cached
        )

        with pytest.raises(RuntimeError, match="failed to close"):
            db_routes._reset_market_resources(request)

        factory.reset_and_open.assert_not_called()
        assert duckdb_path.read_text() == "original database"
        assert wal_path.read_text() == "original wal"
        assert parquet_file.read_text() == "original parquet"
        assert request.app.state.market_reader is current_reader
        assert request.app.state.market_time_series_store is current_store
        assert request.app.state.market_db is current_market_db
        assert request.app.state.market_data_service is market_data_service
        assert request.app.state.roe_service is roe_service
        assert request.app.state.margin_analytics_service is margin_service
        assert request.app.state.chart_service is chart_service
        for resource in resources.values():
            resource.close.assert_called_once_with()
        close_cached.assert_not_called()

    def test_get_db_stats_routes_to_service(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        request = MagicMock()
        request.app.state = SimpleNamespace(
            market_db=MagicMock(),
            market_time_series_store=MagicMock(),
        )
        stats_response = MagicMock()
        get_market_stats = MagicMock(return_value=stats_response)
        monkeypatch.setattr(
            db_routes.db_stats_service, "get_market_stats", get_market_stats
        )

        assert db_routes.get_db_stats(request) is stats_response
        get_market_stats.assert_called_once_with(
            request.app.state.market_db,
            time_series_store=request.app.state.market_time_series_store,
        )

    def test_get_db_validate_routes_to_service(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        request = MagicMock()
        market_db = MagicMock()
        market_db.validate_schema.return_value = {"valid": True}
        request.app.state = SimpleNamespace(
            market_db=market_db,
            market_time_series_store=MagicMock(),
        )
        validation_response = MagicMock()
        validate_market_db = MagicMock(return_value=validation_response)
        monkeypatch.setattr(
            db_routes.db_validation_service, "validate_market_db", validate_market_db
        )

        assert db_routes.get_db_validate(request) is validation_response
        validate_market_db.assert_called_once_with(
            request.app.state.market_db,
            time_series_store=request.app.state.market_time_series_store,
        )

    def test_db_validate_rejects_legacy_bigint_adjusted_volume_with_reset_guidance(
        self,
        app_client: TestClient,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        market_root = tmp_path / "market-timeseries"
        db_path = market_root / "market.duckdb"
        market_db = open_market_db(str(db_path), read_only=False)
        market_db.set_sync_metadata("init_completed", "true")
        market_db.close()
        with connect_market_duckdb_for_test(str(db_path)) as connection:
            connection.execute(
                "ALTER TABLE stock_data_raw ALTER adjusted_volume TYPE BIGINT"
            )
            connection.execute("ALTER TABLE stock_data ALTER volume TYPE BIGINT")

        read_only_db = open_market_db(str(db_path), read_only=True)
        forbidden_store_open = MagicMock(
            side_effect=AssertionError(
                "incompatible Market data must not be opened through the time-series store"
            )
        )
        monkeypatch.setattr(
            db_routes, "_get_market_time_series_store", forbidden_store_open
        )
        app_client.app.state.market_db = read_only_db
        app_client.app.state.market_time_series_store = None
        try:
            response = app_client.get("/api/db/validate")
        finally:
            read_only_db.close()
            app_client.app.state.market_time_series_store = None
            app_client.app.state.market_db = None

        assert response.status_code == 200, response.text
        forbidden_store_open.assert_not_called()
        payload = response.json()
        assert payload["status"] == "error"
        assert any(
            "RESET initial" in recommendation
            and "incompatible Market root" in recommendation
            for recommendation in payload["recommendations"]
        )

    def test_sync_start(self, client: TestClient) -> None:
        default_store = MagicMock()
        client.app.state.market_time_series_store = default_store

        with patch(
            "src.entrypoints.http.routes.db.start_sync", new_callable=AsyncMock
        ) as mock_start:
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
            assert (
                mock_start.await_args.kwargs["time_series_store"] is not default_store
            )
            assert mock_start.await_args.kwargs["enforce_bulk_for_stock_data"] is False
            finalizer_provider = mock_start.await_args.kwargs["market_finalizer"]
            assert callable(finalizer_provider)

    def test_adjusted_metrics_materialize_routes_are_removed(self, client: TestClient) -> None:
        assert client.post("/api/db/adjusted-metrics/materialize").status_code == 404
        assert (
            client.get("/api/db/adjusted-metrics/materialize/jobs/active").status_code
            == 404
        )
        assert (
            client.get("/api/db/adjusted-metrics/materialize/jobs/materialize-job-1").status_code
            == 404
        )
        assert (
            client.delete("/api/db/adjusted-metrics/materialize/jobs/materialize-job-1").status_code
            == 404
        )

    def test_sync_start_with_bulk_enforcement(self, client: TestClient) -> None:
        default_store = MagicMock()
        client.app.state.market_time_series_store = default_store

        with patch(
            "src.entrypoints.http.routes.db.start_sync", new_callable=AsyncMock
        ) as mock_start:
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
            assert (
                mock_start.await_args.kwargs["time_series_store"] is not default_store
            )
            assert mock_start.await_args.kwargs["enforce_bulk_for_stock_data"] is True

    def test_sync_start_with_reset_before_sync(self, client: TestClient) -> None:
        default_store = MagicMock()
        client.app.state.market_time_series_store = default_store

        with patch(
            "src.entrypoints.http.routes.db.start_sync", new_callable=AsyncMock
        ) as mock_start:
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
            assert mock_start.await_args.args[1] is None
            assert mock_start.await_args.kwargs["time_series_store"] is None
            assert mock_start.await_args.kwargs["reset_before_sync"] is True
            assert callable(mock_start.await_args.kwargs["reset_market_snapshot"])

    def test_sync_start_reset_initial_does_not_open_existing_market_handles(
        self,
        app_client: TestClient,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        app_client.app.state.market_db = None
        app_client.app.state.market_time_series_store = None
        app_client.app.state.jquants_client = MagicMock(has_api_key=True)
        monkeypatch.setattr(
            db_routes,
            "_get_market_db",
            MagicMock(side_effect=AssertionError("old MarketDb must not be opened")),
        )
        monkeypatch.setattr(
            db_routes,
            "_get_market_time_series_store",
            MagicMock(side_effect=AssertionError("old store must not be opened")),
        )

        with patch(
            "src.entrypoints.http.routes.db.start_sync", new_callable=AsyncMock
        ) as mock_start:
            mock_job = MagicMock()
            mock_job.job_id = "test-job-reset-missing-root"
            mock_job.data.resolved_mode = "initial"
            mock_start.return_value = mock_job

            response = app_client.post(
                "/api/db/sync",
                json={"mode": "initial", "resetBeforeSync": True},
            )

        assert response.status_code == 202, response.text
        assert mock_start.await_args.args[1] is None
        assert mock_start.await_args.kwargs["time_series_store"] is None

    def test_incremental_rejects_legacy_bigint_adjusted_volume_before_schema_write(
        self,
        app_client: TestClient,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        market_root = tmp_path / "market-timeseries"
        db_path = market_root / "market.duckdb"
        seeded_db = open_market_db(str(db_path), read_only=False)
        seeded_db.close()
        with connect_market_duckdb_for_test(str(db_path)) as connection:
            connection.execute(
                "ALTER TABLE stock_data_raw ALTER adjusted_volume TYPE BIGINT"
            )
            connection.execute("ALTER TABLE stock_data ALTER volume TYPE BIGINT")

        app_client.app.state.market_db = open_market_db(str(db_path), read_only=True)
        app_client.app.state.market_time_series_store = None
        app_client.app.state.jquants_client = MagicMock(has_api_key=True)
        ensure_schema_calls = 0
        original_ensure_schema = db_routes.MarketDb.ensure_schema

        def track_ensure_schema(market_db: object) -> None:
            nonlocal ensure_schema_calls
            ensure_schema_calls += 1
            original_ensure_schema(market_db)  # type: ignore[arg-type]

        monkeypatch.setattr(db_routes.MarketDb, "ensure_schema", track_ensure_schema)

        try:
            with patch(
                "src.entrypoints.http.routes.db.start_sync", new_callable=AsyncMock
            ) as mock_start:
                mock_job = MagicMock()
                mock_job.job_id = "must-not-start"
                mock_job.data.resolved_mode = "incremental"
                mock_start.return_value = mock_job
                response = app_client.post(
                    "/api/db/sync",
                    json={"mode": "incremental"},
                )

            assert response.status_code == 422, response.text
            assert "RESET initial" in response.json()["message"]
            assert ensure_schema_calls == 0
            mock_start.assert_not_awaited()
        finally:
            session = getattr(app_client.app.state, "market_writer_session", None)
            if session is not None:
                token = session.close_writable_handles()
                read_only = session.reopen_read_only(token)
                session.release_after_read_only_reopen(token)
                read_only.close()
                app_client.app.state.market_writer_session = None
            current_db = getattr(app_client.app.state, "market_db", None)
            if current_db is not None:
                current_db.close()
            app_client.app.state.market_db = None
            app_client.app.state.market_time_series_store = None
            app_client.app.state.jquants_client = None


    def test_sync_start_rejects_reset_before_sync_outside_initial_mode(
        self, client: TestClient
    ) -> None:
        client.app.state.market_time_series_store = MagicMock()

        with patch(
            "src.entrypoints.http.routes.db.start_sync", new_callable=AsyncMock
        ) as mock_start:
            resp = client.post(
                "/api/db/sync",
                json={"mode": "incremental", "resetBeforeSync": True},
            )

        assert resp.status_code == 422
        assert mock_start.await_count == 0

    def test_sync_start_with_data_plane_override(self, client: TestClient) -> None:
        client.app.state.market_time_series_store = None
        with patch(
            "src.entrypoints.http.routes.db.start_sync", new_callable=AsyncMock
        ) as mock_start:
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
            assert mock_start.await_count == 1
            assert mock_start.await_args.kwargs["time_series_store"] is not None

    def test_sync_start_with_duckdb_data_plane_uses_app_store(
        self, client: TestClient
    ) -> None:
        default_store = MagicMock()
        client.app.state.market_time_series_store = default_store

        with patch(
            "src.entrypoints.http.routes.db.start_sync", new_callable=AsyncMock
        ) as mock_start:
            mock_job = MagicMock()
            mock_job.job_id = "test-job-default"
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
            assert (
                mock_start.await_args.kwargs["time_series_store"] is not default_store
            )

    def test_sync_start_duckdb_only_returns_422_when_backend_unavailable(
        self, client: TestClient
    ) -> None:
        client.app.state.market_time_series_store = None
        with (
            patch(
                "src.entrypoints.http.routes.db.start_sync", new_callable=AsyncMock
            ) as mock_start,
            patch(
                "src.entrypoints.http.routes.db._prepare_market_write_resources",
                side_effect=RuntimeError(
                    "DuckDB market time-series store is unavailable"
                ),
            ),
        ):
            resp = client.post(
                "/api/db/sync",
                json={
                    "mode": "incremental",
                    "dataPlane": {"backend": "duckdb-parquet"},
                },
            )

            assert resp.status_code == 422
            body = resp.json()
            assert body["error"] == "Unprocessable Entity"
            assert "DuckDB market time-series store is unavailable" in body["message"]
            assert mock_start.await_count == 0

    def test_sync_conflict(self, client: TestClient) -> None:
        client.app.state.market_time_series_store = MagicMock()
        with patch(
            "src.entrypoints.http.routes.db.start_sync", new_callable=AsyncMock
        ) as mock_start:
            mock_start.return_value = None  # Active job exists
            resp = client.post("/api/db/sync", json={"mode": "incremental"})
            assert resp.status_code == 409

    def test_sync_conflict_restores_read_only_resources(
        self, client: TestClient
    ) -> None:
        with patch(
            "src.entrypoints.http.routes.db.start_sync", new_callable=AsyncMock
        ) as mock_start:
            client.app.state.market_time_series_store = None
            mock_start.return_value = None

            resp = client.post(
                "/api/db/sync",
                json={
                    "mode": "incremental",
                    "dataPlane": {"backend": "duckdb-parquet"},
                },
            )

            assert resp.status_code == 409
            assert client.app.state.market_writer_session is None
            assert client.app.state.market_time_series_store is not None

    def test_sync_start_exception_restores_read_only_resources(
        self, client: TestClient
    ) -> None:
        with patch(
            "src.entrypoints.http.routes.db.start_sync", new_callable=AsyncMock
        ) as mock_start:
            client.app.state.market_time_series_store = None
            mock_start.side_effect = RuntimeError("sync exploded")

            resp = client.post(
                "/api/db/sync",
                json={
                    "mode": "incremental",
                    "dataPlane": {"backend": "duckdb-parquet"},
                },
            )

            assert resp.status_code == 500
            assert client.app.state.market_writer_session is None
            assert client.app.state.market_time_series_store is not None

    def test_get_sync_job_not_found(self, client: TestClient) -> None:
        resp = client.get("/api/db/sync/jobs/nonexistent-id")
        assert resp.status_code == 404

    def test_get_active_sync_job_returns_null_when_idle(
        self, client: TestClient
    ) -> None:
        with patch(
            "src.entrypoints.http.routes.db.sync_job_manager.get_active_job"
        ) as mock_get_active:
            mock_get_active.return_value = None

            resp = client.get("/api/db/sync/jobs/active")
            assert resp.status_code == 200, resp.text
            assert resp.json() is None

    def test_get_active_sync_job_success(self, client: TestClient) -> None:
        with patch(
            "src.entrypoints.http.routes.db.sync_job_manager.get_active_job"
        ) as mock_get_active:
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
            assert resp.status_code == 200, resp.text
            body = resp.json()
            assert body["jobId"] == "job-active"
            assert body["status"] == "running"
            assert body["mode"] == "incremental"
            assert body["enforceBulkForStockData"] is False
            assert body["maintenance"]["evidenceStatus"] == "never_run"

    def test_get_sync_job_success(self, client: TestClient) -> None:
        with patch(
            "src.entrypoints.http.routes.db.sync_job_manager.get_job"
        ) as mock_get_job:
            job = MagicMock()
            job.job_id = "job-1"
            job.status = JobStatus.RUNNING
            job.data.resolved_mode = ""
            job.data.mode.value = "incremental"
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
        with patch(
            "src.entrypoints.http.routes.db.sync_job_manager.get_job"
        ) as mock_get_job:
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
            patch(
                "src.entrypoints.http.routes.db.sync_stream_manager.subscribe"
            ) as mock_subscribe,
            patch(
                "src.entrypoints.http.routes.db.sync_stream_manager.unsubscribe"
            ) as mock_unsubscribe,
            patch(
                "src.entrypoints.http.routes.db.sync_job_manager.get_job",
                return_value=None,
            ),
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
            patch(
                "src.entrypoints.http.routes.db.sync_stream_manager.subscribe",
                return_value=queue,
            ),
            patch(
                "src.entrypoints.http.routes.db.sync_stream_manager.unsubscribe"
            ) as mock_unsubscribe,
            patch(
                "src.entrypoints.http.routes.db.sync_job_manager.get_job",
                side_effect=[running_job, completed_job],
            ),
        ):
            monkeypatch.setattr(db_routes.asyncio, "wait_for", fake_wait_for)
            events = await _collect_sync_events("job-1")

        assert [event["event"] for event in events] == ["snapshot", "heartbeat"]
        mock_unsubscribe.assert_called_once()

    @pytest.mark.asyncio
    async def test_sync_job_event_generator_returns_when_latest_job_disappears(
        self,
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

        queue = asyncio.Queue()
        queue.put_nowait(SyncStreamEvent(event="job"))

        with (
            patch(
                "src.entrypoints.http.routes.db.sync_stream_manager.subscribe",
                return_value=queue,
            ),
            patch(
                "src.entrypoints.http.routes.db.sync_stream_manager.unsubscribe"
            ) as mock_unsubscribe,
            patch(
                "src.entrypoints.http.routes.db.sync_job_manager.get_job",
                side_effect=[running_job, None],
            ),
        ):
            events = await _collect_sync_events("job-1")

        assert len(events) == 1
        assert events[0]["event"] == "snapshot"
        mock_unsubscribe.assert_called_once()

    def test_stream_sync_job_success(self, client: TestClient) -> None:
        with (
            patch(
                "src.entrypoints.http.routes.db.sync_job_manager.get_job"
            ) as mock_get_job,
            patch(
                "src.entrypoints.http.routes.db.sync_stream_manager.subscribe"
            ) as mock_subscribe,
            patch(
                "src.entrypoints.http.routes.db.sync_stream_manager.unsubscribe"
            ) as mock_unsubscribe,
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
        with patch(
            "src.entrypoints.http.routes.db.sync_job_manager.get_job"
        ) as mock_get_job:
            job = MagicMock()
            job.status = JobStatus.COMPLETED
            mock_get_job.return_value = job

            resp = client.delete("/api/db/sync/jobs/job-1")
            assert resp.status_code == 400

    def test_cancel_sync_job_success(self, client: TestClient) -> None:
        with (
            patch(
                "src.entrypoints.http.routes.db.sync_job_manager.get_job"
            ) as mock_get_job,
            patch(
                "src.entrypoints.http.routes.db.sync_job_manager.cancel_job",
                new_callable=AsyncMock,
            ) as mock_cancel,
        ):
            job = MagicMock()
            job.status = JobStatus.RUNNING
            mock_get_job.return_value = job

            resp = client.delete("/api/db/sync/jobs/job-1")
            assert resp.status_code == 200
            assert resp.json()["success"] is True
            mock_cancel.assert_awaited_once_with("job-1")

    def test_cancel_sync_job_returns_409_when_job_finishes_during_cancel(
        self, client: TestClient
    ) -> None:
        with (
            patch(
                "src.entrypoints.http.routes.db.sync_job_manager.get_job"
            ) as mock_get_job,
            patch(
                "src.entrypoints.http.routes.db.sync_job_manager.cancel_job",
                new_callable=AsyncMock,
            ) as mock_cancel,
            patch(
                "src.entrypoints.http.routes.db.sync_stream_manager.publish"
            ) as mock_publish,
            patch(
                "src.entrypoints.http.routes.db.sync_stream_manager.close"
            ) as mock_close,
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


class TestIntradaySyncRoute:
    def test_intraday_sync_success(self, client: TestClient) -> None:
        client.app.state.market_time_series_store = MagicMock()
        with patch(
            "src.entrypoints.http.routes.db.intraday_sync_service.sync_intraday_data",
            new_callable=AsyncMock,
        ) as mock_sync:
            from src.application.contracts.market_data_plane import IntradaySyncResponse

            mock_sync.return_value = IntradaySyncResponse(
                success=True,
                mode="rest",
                requestedCodes=1,
                storedCodes=1,
                datesProcessed=1,
                recordsFetched=2,
                recordsStored=2,
                apiCalls=1,
                selectedFiles=0,
                cacheHits=0,
                cacheMisses=0,
                skippedRows=0,
                lastUpdated="2026-04-15T08:00:00+00:00",
            )

            resp = client.post(
                "/api/db/intraday/sync",
                json={"date": "2026-04-14", "codes": ["9984"]},
            )

            assert resp.status_code == 200, resp.text
            data = resp.json()
            assert data["mode"] == "rest"
            assert data["recordsStored"] == 2
            assert data["maintenance"]["outcome"] == "passed"
            assert mock_sync.await_count == 1

    def test_intraday_sync_validation_error(self, client: TestClient) -> None:
        resp = client.post(
            "/api/db/intraday/sync", json={"mode": "rest", "date": "2026-04-14"}
        )
        assert resp.status_code == 422


class TestRefreshRoute:
    def test_refresh_success(self, client: TestClient) -> None:
        with (
            patch(
                "src.entrypoints.http.routes.db._get_market_time_series_store"
            ) as mock_store,
            patch(
                "src.application.services.stock_refresh_service.refresh_stocks"
            ) as mock_refresh,
            patch(
                "src.entrypoints.http.routes.db.AdjustedMetricsMaterializer"
            ) as materializer,
        ):
            from src.application.contracts.market_data_plane import (
                RefreshResponse,
                RefreshStockResult,
            )

            mock_store.return_value = MagicMock()
            mock_refresh.return_value = RefreshResponse(
                totalStocks=1,
                successCount=1,
                failedCount=0,
                totalApiCalls=1,
                totalRecordsStored=100,
                results=[
                    RefreshStockResult(
                        code="7203", success=True, recordsFetched=100, recordsStored=100
                    )
                ],
                lastUpdated="2024-01-06T10:00:00",
            )
            resp = client.post("/api/db/stocks/refresh", json={"codes": ["7203"]})
            assert resp.status_code == 200
            data = resp.json()
            assert data["totalStocks"] == 1
            assert data["successCount"] == 1
            assert data["maintenance"]["outcome"] == "passed"
            materializer.return_value.rebuild_current_basis.assert_called_once_with(
                ["7203"]
            )

    def test_refresh_validation_error(self, client: TestClient) -> None:
        resp = client.post("/api/db/stocks/refresh", json={"codes": []})
        assert resp.status_code == 422

    def test_refresh_no_db(self, app_client: TestClient) -> None:
        app_client.app.state.market_db = None
        resp = app_client.post("/api/db/stocks/refresh", json={"codes": ["7203"]})
        assert resp.status_code == 422

    def test_refresh_no_jquants_client(
        self, app_client: TestClient, market_db_path: str
    ) -> None:
        market_db = open_market_db(market_db_path, read_only=False)
        app_client.app.state.market_db = market_db
        app_client.app.state.jquants_client = None
        try:
            resp = app_client.post("/api/db/stocks/refresh", json={"codes": ["7203"]})
            assert resp.status_code == 422, resp.text
        finally:
            market_db.close()

    def test_refresh_not_initialized(
        self, app_client: TestClient, market_db_path: str
    ) -> None:
        market_db = open_market_db(market_db_path, read_only=False)
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

    def test_refresh_rejects_legacy_stock_snapshot(
        self, app_client: TestClient
    ) -> None:
        from src.shared.contracts.market_maintenance import (
            MaintenanceEvidenceStatus,
            MaintenanceOutcome,
            MarketMaintenanceRecord,
            MarketOperationOutcome,
        )
        from src.application.services.market_maintenance_finalizer import (
            MarketFinalizationDecision,
        )

        market_db = MagicMock()
        market_db.is_initialized.return_value = True
        market_db.is_legacy_stock_price_snapshot.return_value = True
        app_client.app.state.market_db = market_db
        app_client.app.state.market_time_series_store = MagicMock()
        mock_client = MagicMock()
        mock_client.has_api_key = True
        app_client.app.state.jquants_client = mock_client
        with (
            patch(
                "src.entrypoints.http.routes.db._prepare_market_write_resources"
            ) as prepare,
            patch(
                "src.entrypoints.http.routes.db._finalize_direct_market_write",
                new_callable=AsyncMock,
                return_value=MarketFinalizationDecision(
                    terminal_outcome=MarketOperationOutcome.FAILED,
                    maintenance=MarketMaintenanceRecord(
                        evidenceStatus=MaintenanceEvidenceStatus.VALID,
                        outcome=MaintenanceOutcome.PASSED,
                        operation="stock_refresh",
                        recordedAt="2026-07-16T00:00:00+00:00",
                        compacted=False,
                        trigger="none",
                        beforeBytes=1024,
                        afterBytes=1024,
                        durationMs=1.0,
                        validation="passed",
                        schemaFingerprint="schema-v4",
                        tableCounts={},
                        semanticDigests={},
                    ),
                    error="Legacy market.duckdb detected",
                ),
            ),
        ):
            prepare.return_value = (market_db, MagicMock())
            resp = app_client.post("/api/db/stocks/refresh", json={"codes": ["7203"]})
        assert resp.status_code == 422
        assert "Legacy market.duckdb detected" in resp.json()["message"]
        app_client.app.state.market_time_series_store = None
