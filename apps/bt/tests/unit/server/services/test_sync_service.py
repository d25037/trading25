from __future__ import annotations

import asyncio
from typing import Any, cast
from unittest.mock import MagicMock

import pytest

from src.application.services import sync_service
from src.application.services.generic_job_manager import GenericJobManager
from src.application.services.sync_service import SyncMode
from src.entrypoints.http.schemas.db import SyncResult
from src.infrastructure.db.market.market_db import METADATA_KEYS
from src.infrastructure.db.market.time_series_store import TimeSeriesInspection


class DummyMarketDb:
    def __init__(
        self,
        last_sync_date: str | None = None,
        *,
        legacy_stock_snapshot: bool = False,
    ) -> None:
        self._last_sync_date = last_sync_date
        self._legacy_stock_snapshot = legacy_stock_snapshot
        self.ensure_schema_calls = 0
        self.metadata: dict[str, str] = {}

    def get_sync_metadata(self, key: str) -> str | None:
        if key in self.metadata:
            return self.metadata[key]
        if key != METADATA_KEYS["LAST_SYNC_DATE"]:
            return None
        return self._last_sync_date

    def set_sync_metadata(self, key: str, value: str) -> None:
        self.metadata[key] = value

    def ensure_schema(self) -> None:
        self.ensure_schema_calls += 1

    def is_legacy_stock_price_snapshot(self) -> bool:
        return self._legacy_stock_snapshot


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


def _market_db(
    last_sync_date: str | None = None,
    *,
    legacy_stock_snapshot: bool = False,
) -> sync_service.SyncServiceMarketDbLike:
    return cast(
        sync_service.SyncServiceMarketDbLike,
        DummyMarketDb(
            last_sync_date=last_sync_date,
            legacy_stock_snapshot=legacy_stock_snapshot,
        ),
    )


def _time_series_store(
    *,
    inspection: TimeSeriesInspection | None = None,
    inspect_error: Exception | None = None,
) -> sync_service.SyncServiceTimeSeriesStoreLike:
    return cast(
        sync_service.SyncServiceTimeSeriesStoreLike,
        DummyTimeSeriesStore(inspection=inspection, inspect_error=inspect_error),
    )


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


class CancelledStrategy:
    async def execute(self, ctx: Any) -> SyncResult:
        del ctx
        raise asyncio.CancelledError()


class FetchBurstStrategy:
    def __init__(self, detail_count: int) -> None:
        self.detail_count = detail_count

    async def execute(self, ctx: Any) -> SyncResult:
        for idx in range(self.detail_count):
            ctx.on_fetch_detail(
                {
                    "eventType": "strategy",
                    "stage": "stock_data",
                    "endpoint": "/equities/bars/daily",
                    "method": "bulk",
                    "targetLabel": str(idx),
                    "reason": "bulk_estimate_lower",
                    "reasonDetail": None,
                    "estimatedRestCalls": 120,
                    "estimatedBulkCalls": 6,
                    "plannerApiCalls": 1,
                    "fallback": False,
                    "fallbackReason": None,
                }
            )
        return SyncResult(success=True, totalApiCalls=1)


@pytest.fixture
def isolated_manager(monkeypatch: pytest.MonkeyPatch) -> GenericJobManager:
    manager: GenericJobManager = GenericJobManager()
    monkeypatch.setattr(sync_service, "sync_job_manager", manager)
    return manager


def test_resolve_mode_prefers_requested_non_auto() -> None:
    market_db = _market_db(last_sync_date=None)
    assert sync_service._resolve_mode(SyncMode.INITIAL, market_db) == "initial"
    assert sync_service._resolve_mode(SyncMode.INCREMENTAL, market_db) == "incremental"
    assert sync_service._resolve_mode(SyncMode.REPAIR, market_db) == "repair"


def test_resolve_mode_auto_uses_metadata_anchor() -> None:
    assert sync_service._resolve_mode(
        SyncMode.AUTO,
        _market_db(last_sync_date="2026-03-01T00:00:00+00:00"),
        time_series_store=_time_series_store(),
    ) == "incremental"


def test_publish_sync_job_event_closes_stream_only_when_requested(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stream_manager = MagicMock()
    monkeypatch.setattr(sync_service, "sync_stream_manager", stream_manager)

    sync_service._publish_sync_job_event("job-1")
    sync_service._publish_sync_job_event("job-2", close_stream=True)

    assert stream_manager.publish.call_count == 2
    stream_manager.close.assert_called_once_with("job-2")


def test_resolve_mode_auto_defaults_to_initial_without_timeseries_store() -> None:
    assert sync_service._resolve_mode(
        SyncMode.AUTO,
        _market_db(last_sync_date=None),
        time_series_store=None,
    ) == "initial"


def test_resolve_mode_auto_uses_timeseries_snapshot_when_last_sync_missing() -> None:
    empty_store = _time_series_store(inspection=TimeSeriesInspection(source="duckdb-parquet"))
    assert sync_service._resolve_mode(
        SyncMode.AUTO,
        _market_db(last_sync_date=None),
        time_series_store=empty_store,
    ) == "initial"

    populated_store = _time_series_store(
        inspection=TimeSeriesInspection(
            source="duckdb-parquet",
            stock_count=10,
            stock_max="2026-03-04",
        )
    )
    assert sync_service._resolve_mode(
        SyncMode.AUTO,
        _market_db(last_sync_date=None),
        time_series_store=populated_store,
    ) == "incremental"


def test_resolve_mode_auto_raises_when_inspection_fails() -> None:
    failing_store = _time_series_store(inspect_error=RuntimeError("inspect failed"))
    with pytest.raises(RuntimeError, match="DuckDB inspection failed while resolving AUTO sync mode"):
        sync_service._resolve_mode(
            SyncMode.AUTO,
            _market_db(last_sync_date=None),
            time_series_store=failing_store,
        )


@pytest.mark.asyncio
async def test_start_sync_requires_duckdb_store(isolated_manager: GenericJobManager) -> None:
    del isolated_manager
    with pytest.raises(RuntimeError, match="DuckDB time-series store is required for sync"):
        await sync_service.start_sync(
            SyncMode.AUTO,
            _market_db(),
            DummyJQuantsClient(),
            time_series_store=None,
        )


@pytest.mark.asyncio
async def test_start_sync_rejects_legacy_stock_snapshot(
    isolated_manager: GenericJobManager,
) -> None:
    del isolated_manager
    with pytest.raises(RuntimeError, match="Legacy market.duckdb detected"):
        await sync_service.start_sync(
            SyncMode.INCREMENTAL,
            _market_db(
                last_sync_date="2026-03-01T00:00:00+00:00",
                legacy_stock_snapshot=True,
            ),
            DummyJQuantsClient(),
            time_series_store=_time_series_store(),
        )


@pytest.mark.asyncio
async def test_start_sync_rejects_reset_before_sync_outside_initial_mode(
    isolated_manager: GenericJobManager,
) -> None:
    del isolated_manager
    with pytest.raises(RuntimeError, match="resetBeforeSync is supported only for initial sync"):
        await sync_service.start_sync(
            SyncMode.INCREMENTAL,
            _market_db(last_sync_date="2026-03-01T00:00:00+00:00"),
            DummyJQuantsClient(),
            time_series_store=_time_series_store(),
            reset_before_sync=True,
        )


@pytest.mark.asyncio
async def test_start_sync_requires_reset_callback_when_reset_enabled(
    isolated_manager: GenericJobManager,
) -> None:
    del isolated_manager
    with pytest.raises(RuntimeError, match="resetBeforeSync requires a reset callback"):
        await sync_service.start_sync(
            SyncMode.INITIAL,
            _market_db(),
            DummyJQuantsClient(),
            time_series_store=_time_series_store(),
            reset_before_sync=True,
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
        _market_db(),
        DummyJQuantsClient(),
        time_series_store=_time_series_store(),
    )
    assert job is None


@pytest.mark.asyncio
async def test_start_sync_completes_job_and_passes_bulk_enforcement(
    monkeypatch: pytest.MonkeyPatch,
    isolated_manager: GenericJobManager,
) -> None:
    strategy = StrategyProbe()
    monkeypatch.setattr(sync_service, "get_strategy", lambda _mode: strategy)
    stream_manager = MagicMock()
    monkeypatch.setattr(sync_service, "sync_stream_manager", stream_manager)

    market_db = _market_db(last_sync_date="2026-03-01T00:00:00+00:00")
    store = _time_series_store()
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
    assert market_db.metadata[METADATA_KEYS["STOCK_PRICE_ADJUSTMENT_MODE"]] == (
        "local_projection_v1"
    )
    assert store.close_calls == 1
    published_events = [call.args[1] for call in stream_manager.publish.call_args_list]
    assert [event.event for event in published_events].count("job") >= 2
    fetch_detail_event = next(event for event in published_events if event.event == "fetch-detail")
    assert fetch_detail_event.payload is not None
    assert fetch_detail_event.payload["endpoint"] == "/equities/bars/daily"
    assert "timestamp" in fetch_detail_event.payload
    stream_manager.close.assert_called_once_with(job.job_id)


@pytest.mark.asyncio
async def test_start_sync_resets_market_snapshot_before_initial_sync(
    monkeypatch: pytest.MonkeyPatch,
    isolated_manager: GenericJobManager,
) -> None:
    strategy = StrategyProbe(emit_progress=False)
    monkeypatch.setattr(sync_service, "get_strategy", lambda _mode: strategy)

    old_market_db = DummyMarketDb(legacy_stock_snapshot=True)
    old_store = DummyTimeSeriesStore()
    reset_market_db = DummyMarketDb()
    reset_store = DummyTimeSeriesStore()
    reset_calls = 0

    def reset_snapshot() -> tuple[sync_service.SyncServiceMarketDbLike, sync_service.SyncServiceTimeSeriesStoreLike]:
        nonlocal reset_calls
        reset_calls += 1
        return (
            cast(sync_service.SyncServiceMarketDbLike, reset_market_db),
            cast(sync_service.SyncServiceTimeSeriesStoreLike, reset_store),
        )

    job = await sync_service.start_sync(
        SyncMode.INITIAL,
        cast(sync_service.SyncServiceMarketDbLike, old_market_db),
        DummyJQuantsClient(),
        time_series_store=cast(sync_service.SyncServiceTimeSeriesStoreLike, old_store),
        reset_before_sync=True,
        reset_market_snapshot=reset_snapshot,
    )
    assert job is not None and job.task is not None
    await job.task

    stored = isolated_manager.get_job(job.job_id)
    assert stored is not None
    assert stored.status.value == "completed"
    assert reset_calls == 1
    assert old_market_db.ensure_schema_calls == 0
    assert METADATA_KEYS["STOCK_PRICE_ADJUSTMENT_MODE"] not in old_market_db.metadata
    assert reset_market_db.ensure_schema_calls == 1
    assert reset_market_db.metadata[METADATA_KEYS["STOCK_PRICE_ADJUSTMENT_MODE"]] == "local_projection_v1"
    assert strategy.captured_ctx is not None
    assert strategy.captured_ctx.market_db is reset_market_db
    assert strategy.captured_ctx.time_series_store is reset_store
    assert stored.progress is not None
    assert stored.progress.stage == "reset"
    assert stored.progress.percentage == 100.0


@pytest.mark.asyncio
async def test_start_sync_passes_requested_bulk_enforcement(
    monkeypatch: pytest.MonkeyPatch,
    isolated_manager: GenericJobManager,
) -> None:
    strategy = StrategyProbe()
    monkeypatch.setattr(sync_service, "get_strategy", lambda _mode: strategy)

    job = await sync_service.start_sync(
        SyncMode.INCREMENTAL,
        _market_db(last_sync_date="2026-03-01T00:00:00+00:00"),
        DummyJQuantsClient(),
        time_series_store=_time_series_store(),
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
async def test_start_sync_trims_fetch_details_to_max_window(
    monkeypatch: pytest.MonkeyPatch,
    isolated_manager: GenericJobManager,
) -> None:
    strategy = FetchBurstStrategy(detail_count=205)
    monkeypatch.setattr(sync_service, "get_strategy", lambda _mode: strategy)

    job = await sync_service.start_sync(
        SyncMode.INCREMENTAL,
        _market_db(last_sync_date="2026-03-01T00:00:00+00:00"),
        DummyJQuantsClient(),
        time_series_store=_time_series_store(),
    )
    assert job is not None and job.task is not None
    await job.task

    stored = isolated_manager.get_job(job.job_id)
    assert stored is not None
    assert len(stored.data.fetch_details) == 200
    assert stored.data.fetch_details[0]["targetLabel"] == "5"
    assert stored.data.fetch_details[-1]["targetLabel"] == "204"


@pytest.mark.asyncio
async def test_start_sync_does_not_close_store_when_flag_disabled(
    monkeypatch: pytest.MonkeyPatch,
    isolated_manager: GenericJobManager,
) -> None:
    strategy = StrategyProbe(emit_progress=False)
    monkeypatch.setattr(sync_service, "get_strategy", lambda _mode: strategy)
    store = _time_series_store()

    job = await sync_service.start_sync(
        SyncMode.INCREMENTAL,
        _market_db(last_sync_date="2026-03-01T00:00:00+00:00"),
        DummyJQuantsClient(),
        time_series_store=store,
        close_time_series_store=False,
    )
    assert job is not None and job.task is not None
    await job.task

    stored = isolated_manager.get_job(job.job_id)
    assert stored is not None
    assert stored.status.value == "completed"
    assert store.close_calls == 0


@pytest.mark.asyncio
async def test_start_sync_closes_stream_when_job_is_marked_cancelled_after_execute(
    monkeypatch: pytest.MonkeyPatch,
    isolated_manager: GenericJobManager,
) -> None:
    strategy = StrategyProbe(emit_progress=False)
    monkeypatch.setattr(sync_service, "get_strategy", lambda _mode: strategy)
    stream_manager = MagicMock()
    monkeypatch.setattr(sync_service, "sync_stream_manager", stream_manager)
    monkeypatch.setattr(isolated_manager, "is_cancelled", lambda _job_id: True)

    job = await sync_service.start_sync(
        SyncMode.INITIAL,
        _market_db(),
        DummyJQuantsClient(),
        time_series_store=_time_series_store(),
    )
    assert job is not None and job.task is not None
    await job.task

    stored = isolated_manager.get_job(job.job_id)
    assert stored is not None
    assert stored.status.value == "pending"
    stream_manager.publish.assert_called_once()
    published_event = stream_manager.publish.call_args.args[1]
    assert published_event.event == "job"
    stream_manager.close.assert_called_once_with(job.job_id)


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
        _market_db(),
        DummyJQuantsClient(),
        time_series_store=_time_series_store(),
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
        _market_db(last_sync_date="2026-03-01T00:00:00+00:00"),
        DummyJQuantsClient(),
        time_series_store=_time_series_store(),
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
    stream_manager = MagicMock()
    monkeypatch.setattr(sync_service, "sync_stream_manager", stream_manager)
    monkeypatch.setattr(isolated_manager, "is_cancelled", lambda _job_id: True)

    job = await sync_service.start_sync(
        SyncMode.INITIAL,
        _market_db(),
        DummyJQuantsClient(),
        time_series_store=_time_series_store(),
    )
    assert job is not None and job.task is not None
    await job.task

    stored = isolated_manager.get_job(job.job_id)
    assert stored is not None
    # on_progress updates pending -> running, but complete should be skipped.
    assert stored.status.value == "running"
    assert stored.result is None
    published_events = [call.args[1] for call in stream_manager.publish.call_args_list]
    assert published_events[-1].event == "job"
    stream_manager.close.assert_called_once_with(job.job_id)


@pytest.mark.asyncio
async def test_start_sync_propagates_cancelled_error_for_cancelled_job(
    monkeypatch: pytest.MonkeyPatch,
    isolated_manager: GenericJobManager,
) -> None:
    monkeypatch.setattr(sync_service, "get_strategy", lambda _mode: CancelledStrategy())
    stream_manager = MagicMock()
    monkeypatch.setattr(sync_service, "sync_stream_manager", stream_manager)
    monkeypatch.setattr(isolated_manager, "is_cancelled", lambda _job_id: True)

    job = await sync_service.start_sync(
        SyncMode.INITIAL,
        _market_db(),
        DummyJQuantsClient(),
        time_series_store=_time_series_store(),
    )
    assert job is not None and job.task is not None
    with pytest.raises(asyncio.CancelledError):
        await job.task

    stream_manager.publish.assert_called_once()
    stream_manager.close.assert_called_once_with(job.job_id)


@pytest.mark.asyncio
async def test_start_sync_propagates_cancelled_error_without_closing_completed_job_stream(
    monkeypatch: pytest.MonkeyPatch,
    isolated_manager: GenericJobManager,
) -> None:
    monkeypatch.setattr(sync_service, "get_strategy", lambda _mode: CancelledStrategy())
    stream_manager = MagicMock()
    monkeypatch.setattr(sync_service, "sync_stream_manager", stream_manager)
    monkeypatch.setattr(isolated_manager, "is_cancelled", lambda _job_id: False)

    job = await sync_service.start_sync(
        SyncMode.INITIAL,
        _market_db(),
        DummyJQuantsClient(),
        time_series_store=_time_series_store(),
    )
    assert job is not None and job.task is not None
    with pytest.raises(asyncio.CancelledError):
        await job.task

    stream_manager.publish.assert_not_called()
    stream_manager.close.assert_called_once_with(job.job_id)
