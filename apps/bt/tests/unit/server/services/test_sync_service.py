from __future__ import annotations

import asyncio
from typing import Any

import pytest

from src.application.services import sync_service
from src.application.services.generic_job_manager import GenericJobManager
from src.application.services.sync_service import SyncMode
from src.entrypoints.http.schemas.db import SyncResult
from src.infrastructure.db.market.market_db import METADATA_KEYS
from src.infrastructure.db.market.time_series_store import TimeSeriesInspection


class DummyMarketDb:
    def __init__(self, last_sync_date: str | None = None) -> None:
        self._last_sync_date = last_sync_date
        self.ensure_schema_calls = 0

    def get_sync_metadata(self, key: str) -> str | None:
        if key != METADATA_KEYS["LAST_SYNC_DATE"]:
            return None
        return self._last_sync_date

    def ensure_schema(self) -> None:
        self.ensure_schema_calls += 1


class DummyTimeSeriesStore:
    def __init__(
        self,
        *,
        inspection: TimeSeriesInspection | None = None,
        inspect_error: Exception | None = None,
    ) -> None:
        self.close_calls = 0
        self._inspection = inspection or TimeSeriesInspection(source="duckdb-parquet")
        self._inspect_error = inspect_error

    def inspect(
        self,
        *,
        missing_stock_dates_limit: int = 0,
        statement_non_null_columns: list[str] | None = None,
    ) -> TimeSeriesInspection:
        del missing_stock_dates_limit, statement_non_null_columns
        if self._inspect_error is not None:
            raise self._inspect_error
        return self._inspection

    def close(self) -> None:
        self.close_calls += 1


class DummyJQuantsClient:
    async def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        del path, params
        return {"data": []}

    async def get_paginated(self, path: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        del path, params
        return []


class StrategyProbe:
    def __init__(
        self,
        *,
        result: SyncResult | None = None,
        error: Exception | None = None,
        emit_progress: bool = True,
    ) -> None:
        self._result = result or SyncResult(success=True, totalApiCalls=1)
        self._error = error
        self._emit_progress = emit_progress
        self.captured_ctx: Any = None

    async def execute(self, ctx: Any) -> SyncResult:
        self.captured_ctx = ctx
        if self._emit_progress:
            ctx.on_progress("stock_data", 1, 2, "running")
            if ctx.on_fetch_detail is not None:
                ctx.on_fetch_detail(
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
                    }
                )
        if self._error is not None:
            raise self._error
        return self._result


@pytest.fixture
def isolated_manager(monkeypatch: pytest.MonkeyPatch) -> GenericJobManager:
    manager: GenericJobManager = GenericJobManager()
    monkeypatch.setattr(sync_service, "sync_job_manager", manager)
    return manager


def test_resolve_mode_prefers_requested_non_auto() -> None:
    market_db = DummyMarketDb(last_sync_date=None)
    assert sync_service._resolve_mode(SyncMode.INDICES_ONLY, market_db) == "indices-only"
    assert sync_service._resolve_mode(SyncMode.REPAIR, market_db) == "repair"


def test_resolve_mode_auto_uses_metadata_anchor() -> None:
    assert sync_service._resolve_mode(
        SyncMode.AUTO,
        DummyMarketDb(last_sync_date="2026-03-01T00:00:00+00:00"),
        time_series_store=DummyTimeSeriesStore(),
    ) == "incremental"


def test_resolve_mode_auto_uses_timeseries_snapshot_when_last_sync_missing() -> None:
    empty_store = DummyTimeSeriesStore(inspection=TimeSeriesInspection(source="duckdb-parquet"))
    assert sync_service._resolve_mode(
        SyncMode.AUTO,
        DummyMarketDb(last_sync_date=None),
        time_series_store=empty_store,
    ) == "initial"

    populated_store = DummyTimeSeriesStore(
        inspection=TimeSeriesInspection(
            source="duckdb-parquet",
            stock_count=10,
            stock_max="2026-03-04",
        )
    )
    assert sync_service._resolve_mode(
        SyncMode.AUTO,
        DummyMarketDb(last_sync_date=None),
        time_series_store=populated_store,
    ) == "incremental"


def test_resolve_mode_auto_raises_when_inspection_fails() -> None:
    failing_store = DummyTimeSeriesStore(inspect_error=RuntimeError("inspect failed"))
    with pytest.raises(RuntimeError, match="DuckDB inspection failed while resolving AUTO sync mode"):
        sync_service._resolve_mode(
            SyncMode.AUTO,
            DummyMarketDb(last_sync_date=None),
            time_series_store=failing_store,
        )


@pytest.mark.asyncio
async def test_start_sync_requires_duckdb_store(isolated_manager: GenericJobManager) -> None:
    del isolated_manager
    with pytest.raises(RuntimeError, match="DuckDB time-series store is required for sync"):
        await sync_service.start_sync(
            SyncMode.AUTO,
            DummyMarketDb(),
            DummyJQuantsClient(),
            time_series_store=None,
        )


@pytest.mark.asyncio
async def test_start_sync_returns_none_when_manager_rejects_job(
    monkeypatch: pytest.MonkeyPatch,
    isolated_manager: GenericJobManager,
) -> None:
    strategy = StrategyProbe()
    monkeypatch.setattr(sync_service, "get_strategy", lambda _mode: strategy)

    async def _reject_new_job(_data: Any) -> None:
        return None

    monkeypatch.setattr(isolated_manager, "create_job", _reject_new_job)

    job = await sync_service.start_sync(
        SyncMode.AUTO,
        DummyMarketDb(),
        DummyJQuantsClient(),
        time_series_store=DummyTimeSeriesStore(),
    )
    assert job is None


@pytest.mark.asyncio
async def test_start_sync_completes_job_and_passes_bulk_enforcement(
    monkeypatch: pytest.MonkeyPatch,
    isolated_manager: GenericJobManager,
) -> None:
    strategy = StrategyProbe()
    monkeypatch.setattr(sync_service, "get_strategy", lambda _mode: strategy)

    market_db = DummyMarketDb(last_sync_date="2026-03-01T00:00:00+00:00")
    store = DummyTimeSeriesStore()
    job = await sync_service.start_sync(
        SyncMode.AUTO,
        market_db,
        DummyJQuantsClient(),
        time_series_store=store,
        close_time_series_store=True,
    )
    assert job is not None
    assert job.task is not None
    await job.task

    stored = isolated_manager.get_job(job.job_id)
    assert stored is not None
    assert stored.status.value == "completed"
    assert stored.result is not None and stored.result.success is True
    assert stored.progress is not None and stored.progress.percentage == 50.0
    assert strategy.captured_ctx is not None
    assert strategy.captured_ctx.enforce_bulk_for_stock_data is False
    assert stored.data.enforce_bulk_for_stock_data is False
    assert len(stored.data.fetch_details) == 1
    assert stored.data.fetch_details[0]["endpoint"] == "/equities/bars/daily"
    assert "timestamp" in stored.data.fetch_details[0]
    assert market_db.ensure_schema_calls == 1
    assert store.close_calls == 1


@pytest.mark.asyncio
async def test_start_sync_passes_requested_bulk_enforcement(
    monkeypatch: pytest.MonkeyPatch,
    isolated_manager: GenericJobManager,
) -> None:
    strategy = StrategyProbe()
    monkeypatch.setattr(sync_service, "get_strategy", lambda _mode: strategy)

    job = await sync_service.start_sync(
        SyncMode.INCREMENTAL,
        DummyMarketDb(last_sync_date="2026-03-01T00:00:00+00:00"),
        DummyJQuantsClient(),
        time_series_store=DummyTimeSeriesStore(),
        enforce_bulk_for_stock_data=True,
    )
    assert job is not None and job.task is not None
    await job.task

    stored = isolated_manager.get_job(job.job_id)
    assert stored is not None
    assert strategy.captured_ctx is not None
    assert strategy.captured_ctx.enforce_bulk_for_stock_data is True
    assert stored.data.enforce_bulk_for_stock_data is True


@pytest.mark.asyncio
async def test_start_sync_marks_failed_on_timeout(
    monkeypatch: pytest.MonkeyPatch,
    isolated_manager: GenericJobManager,
) -> None:
    strategy = StrategyProbe(emit_progress=False)
    monkeypatch.setattr(sync_service, "get_strategy", lambda _mode: strategy)

    async def _timeout(coro: Any, *, timeout: float) -> Any:
        del timeout
        coro.close()
        raise asyncio.TimeoutError()

    monkeypatch.setattr(sync_service.asyncio, "wait_for", _timeout)

    job = await sync_service.start_sync(
        SyncMode.INITIAL,
        DummyMarketDb(),
        DummyJQuantsClient(),
        time_series_store=DummyTimeSeriesStore(),
    )
    assert job is not None and job.task is not None
    await job.task

    stored = isolated_manager.get_job(job.job_id)
    assert stored is not None
    assert stored.status.value == "failed"
    assert stored.error is not None and "timed out" in stored.error


@pytest.mark.asyncio
async def test_start_sync_marks_failed_on_unexpected_exception(
    monkeypatch: pytest.MonkeyPatch,
    isolated_manager: GenericJobManager,
) -> None:
    strategy = StrategyProbe(error=RuntimeError("boom"))
    monkeypatch.setattr(sync_service, "get_strategy", lambda _mode: strategy)

    job = await sync_service.start_sync(
        SyncMode.INCREMENTAL,
        DummyMarketDb(last_sync_date="2026-03-01T00:00:00+00:00"),
        DummyJQuantsClient(),
        time_series_store=DummyTimeSeriesStore(),
    )
    assert job is not None and job.task is not None
    await job.task

    stored = isolated_manager.get_job(job.job_id)
    assert stored is not None
    assert stored.status.value == "failed"
    assert stored.error == "boom"


@pytest.mark.asyncio
async def test_start_sync_skips_completion_when_job_already_cancelled(
    monkeypatch: pytest.MonkeyPatch,
    isolated_manager: GenericJobManager,
) -> None:
    strategy = StrategyProbe()
    monkeypatch.setattr(sync_service, "get_strategy", lambda _mode: strategy)
    monkeypatch.setattr(isolated_manager, "is_cancelled", lambda _job_id: True)

    job = await sync_service.start_sync(
        SyncMode.INITIAL,
        DummyMarketDb(),
        DummyJQuantsClient(),
        time_series_store=DummyTimeSeriesStore(),
    )
    assert job is not None and job.task is not None
    await job.task

    stored = isolated_manager.get_job(job.job_id)
    assert stored is not None
    # on_progress updates pending -> running, but complete should be skipped.
    assert stored.status.value == "running"
    assert stored.result is None
