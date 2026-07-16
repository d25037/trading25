from __future__ import annotations

import asyncio
import threading
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock

import pytest

from src.application.services import sync_service
from src.shared.contracts.market_maintenance import (
    MaintenanceEvidenceStatus,
    MaintenanceOutcome,
    MarketMaintenanceRecord,
    MarketOperationOutcome,
)
from src.application.services.generic_job_manager import GenericJobManager
from src.application.services.market_maintenance_finalizer import (
    MarketFinalizationDecision,
)
from src.application.contracts.jobs import JobStatus
from src.application.services.sync_service import SyncMode
from src.application.contracts.market_data_plane import SyncResult
from src.infrastructure.db.market.market_db import MARKET_SCHEMA_VERSION, METADATA_KEYS
from src.infrastructure.db.market.time_series_store import TimeSeriesInspection


class DummyMarketDb:
    def __init__(
        self,
        last_sync_date: str | None = None,
        *,
        legacy_stock_snapshot: bool = False,
        schema_version: int = MARKET_SCHEMA_VERSION,
    ) -> None:
        self._last_sync_date = last_sync_date
        self._legacy_stock_snapshot = legacy_stock_snapshot
        self._schema_version = schema_version
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

    def get_market_schema_version(self) -> int | None:
        return self._schema_version

    def is_market_schema_current(self) -> bool:
        return self._schema_version == MARKET_SCHEMA_VERSION


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
    async def get(
        self, path: str, params: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        del path, params
        return {"data": []}

    async def get_paginated(
        self, path: str, params: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        del path, params
        return []


def _market_db(
    last_sync_date: str | None = None,
    *,
    legacy_stock_snapshot: bool = False,
    schema_version: int = MARKET_SCHEMA_VERSION,
) -> sync_service.SyncServiceMarketDbLike:
    return cast(
        sync_service.SyncServiceMarketDbLike,
        DummyMarketDb(
            last_sync_date=last_sync_date,
            legacy_stock_snapshot=legacy_stock_snapshot,
            schema_version=schema_version,
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


class ImmediateFinalizer:
    def finalize(self, **kwargs: Any) -> MarketFinalizationDecision:
        decision = MarketFinalizationDecision(
            terminal_outcome=kwargs["operation_outcome"],
            maintenance=MarketMaintenanceRecord(
                evidenceStatus=MaintenanceEvidenceStatus.VALID,
                outcome=MaintenanceOutcome.PASSED,
                operation="incremental_sync",
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
            ),
            error=kwargs.get("operation_error"),
        )
        stage = kwargs.get("stage_terminal")
        if callable(stage):
            stage(decision)
        kwargs["publish_terminal"](decision)
        return decision


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


@pytest.fixture
def isolated_materialize_manager(monkeypatch: pytest.MonkeyPatch) -> GenericJobManager:
    manager: GenericJobManager = GenericJobManager()
    monkeypatch.setattr(
        sync_service, "adjusted_metrics_materialize_job_manager", manager
    )
    return manager


def test_resolve_mode_prefers_requested_non_auto() -> None:
    market_db = _market_db(last_sync_date=None)
    assert sync_service._resolve_mode(SyncMode.INITIAL, market_db) == "initial"
    assert sync_service._resolve_mode(SyncMode.INCREMENTAL, market_db) == "incremental"
    assert sync_service._resolve_mode(SyncMode.REPAIR, market_db) == "repair"


def test_resolve_mode_auto_uses_metadata_anchor() -> None:
    assert (
        sync_service._resolve_mode(
            SyncMode.AUTO,
            _market_db(last_sync_date="2026-03-01T00:00:00+00:00"),
            time_series_store=_time_series_store(),
        )
        == "incremental"
    )


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
    assert (
        sync_service._resolve_mode(
            SyncMode.AUTO,
            _market_db(last_sync_date=None),
            time_series_store=None,
        )
        == "initial"
    )


def test_resolve_mode_auto_uses_timeseries_snapshot_when_last_sync_missing() -> None:
    empty_store = _time_series_store(
        inspection=TimeSeriesInspection(source="duckdb-parquet")
    )
    assert (
        sync_service._resolve_mode(
            SyncMode.AUTO,
            _market_db(last_sync_date=None),
            time_series_store=empty_store,
        )
        == "initial"
    )

    populated_store = _time_series_store(
        inspection=TimeSeriesInspection(
            source="duckdb-parquet",
            stock_count=10,
            stock_max="2026-03-04",
        )
    )
    assert (
        sync_service._resolve_mode(
            SyncMode.AUTO,
            _market_db(last_sync_date=None),
            time_series_store=populated_store,
        )
        == "incremental"
    )


def test_resolve_mode_auto_raises_when_inspection_fails() -> None:
    failing_store = _time_series_store(inspect_error=RuntimeError("inspect failed"))
    with pytest.raises(
        RuntimeError, match="DuckDB inspection failed while resolving AUTO sync mode"
    ):
        sync_service._resolve_mode(
            SyncMode.AUTO,
            _market_db(last_sync_date=None),
            time_series_store=failing_store,
        )


@pytest.mark.asyncio
async def test_start_sync_requires_duckdb_store(
    isolated_manager: GenericJobManager,
) -> None:
    del isolated_manager
    with pytest.raises(
        RuntimeError, match="DuckDB time-series store is required for sync"
    ):
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


def test_prepare_market_db_rejects_v3_with_reset_only_recovery() -> None:
    market_db = _market_db(schema_version=3)

    with pytest.raises(
        RuntimeError,
        match=r"version: 3, required: 4.*initial sync with reset enabled",
    ):
        sync_service._prepare_market_db_for_sync(market_db)


@pytest.mark.asyncio
async def test_start_sync_rejects_reset_before_sync_outside_initial_mode(
    isolated_manager: GenericJobManager,
) -> None:
    del isolated_manager
    with pytest.raises(
        RuntimeError, match="resetBeforeSync is supported only for initial sync"
    ):
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
        "local_projection_v2_event_time"
    )
    assert store.close_calls == 0
    published_events = [call.args[1] for call in stream_manager.publish.call_args_list]
    assert [event.event for event in published_events].count("job") >= 2
    fetch_detail_event = next(
        event for event in published_events if event.event == "fetch-detail"
    )
    assert fetch_detail_event.payload is not None
    assert fetch_detail_event.payload["endpoint"] == "/equities/bars/daily"
    assert "timestamp" in fetch_detail_event.payload
    stream_manager.close.assert_called_once_with(job.job_id)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mode", "last_sync_date"),
    [
        (SyncMode.INITIAL, None),
        (SyncMode.INCREMENTAL, "2026-03-01T00:00:00+00:00"),
        (SyncMode.REPAIR, "2026-03-01T00:00:00+00:00"),
    ],
)
async def test_start_sync_injects_adjusted_metrics_materializer_for_write_strategy(
    monkeypatch: pytest.MonkeyPatch,
    isolated_manager: GenericJobManager,
    mode: SyncMode,
    last_sync_date: str | None,
) -> None:
    strategy = StrategyProbe(emit_progress=False)
    monkeypatch.setattr(sync_service, "get_strategy", lambda _mode: strategy)

    market_db = _market_db(last_sync_date=last_sync_date)
    job = await sync_service.start_sync(
        mode,
        market_db,
        DummyJQuantsClient(),
        time_series_store=_time_series_store(),
    )
    assert job is not None and job.task is not None
    await job.task

    stored = isolated_manager.get_job(job.job_id)
    assert stored is not None
    assert stored.status.value == "completed"
    assert strategy.captured_ctx.materialize_adjusted_metrics is not None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mode", "last_sync_date", "expected_timeout_minutes"),
    [
        (SyncMode.INITIAL, None, 240),
        (SyncMode.INCREMENTAL, "2026-03-01T00:00:00+00:00", 35),
        (SyncMode.REPAIR, "2026-03-01T00:00:00+00:00", 35),
        (SyncMode.AUTO, None, 240),
        (SyncMode.AUTO, "2026-03-01T00:00:00+00:00", 35),
    ],
)
async def test_start_sync_uses_timeout_for_resolved_mode(
    monkeypatch: pytest.MonkeyPatch,
    isolated_manager: GenericJobManager,
    mode: SyncMode,
    last_sync_date: str | None,
    expected_timeout_minutes: int,
) -> None:
    strategy = StrategyProbe(emit_progress=False)
    monkeypatch.setattr(sync_service, "get_strategy", lambda _mode: strategy)
    observed_timeouts: list[float] = []
    original_wait_for = asyncio.wait_for

    async def capture_wait_for(coro: Any, *, timeout: float) -> Any:
        observed_timeouts.append(timeout)
        return await original_wait_for(coro, timeout=timeout)

    monkeypatch.setattr(sync_service.asyncio, "wait_for", capture_wait_for)

    job = await sync_service.start_sync(
        mode,
        _market_db(last_sync_date=last_sync_date),
        DummyJQuantsClient(),
        time_series_store=_time_series_store(),
    )
    assert job is not None and job.task is not None
    await job.task

    assert observed_timeouts == [expected_timeout_minutes * 60]


@pytest.mark.asyncio
async def test_start_sync_marks_failed_when_strategy_result_is_unsuccessful(
    monkeypatch: pytest.MonkeyPatch,
    isolated_manager: GenericJobManager,
) -> None:
    strategy = StrategyProbe(
        result=SyncResult(
            success=False,
            totalApiCalls=0,
            errors=["JQuants API error (403): /indices/bars/daily/topix"],
        )
    )
    monkeypatch.setattr(sync_service, "get_strategy", lambda _mode: strategy)
    stream_manager = MagicMock()
    monkeypatch.setattr(sync_service, "sync_stream_manager", stream_manager)

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
    assert stored.error == "JQuants API error (403): /indices/bars/daily/topix"
    assert stored.result is not None
    assert stored.result.success is False
    assert stored.result.errors == [
        "JQuants API error (403): /indices/bars/daily/topix"
    ]
    stream_manager.close.assert_called_once_with(job.job_id)


@pytest.mark.asyncio
async def test_failed_sync_result_overrides_racing_cancel_and_keeps_evidence(
    monkeypatch: pytest.MonkeyPatch,
    isolated_manager: GenericJobManager,
) -> None:
    strategy = StrategyProbe(
        result=SyncResult(success=False, totalApiCalls=1, errors=["fetch failed"])
    )
    monkeypatch.setattr(sync_service, "get_strategy", lambda _mode: strategy)
    monkeypatch.setattr(isolated_manager, "is_cancelled", lambda _job_id: True)
    stream_manager = MagicMock()
    monkeypatch.setattr(sync_service, "sync_stream_manager", stream_manager)

    job = await sync_service.start_sync(
        SyncMode.INCREMENTAL,
        _market_db(last_sync_date="2026-03-01T00:00:00+00:00"),
        DummyJQuantsClient(),
        time_series_store=_time_series_store(),
        market_finalizer=ImmediateFinalizer(),  # type: ignore[arg-type]
    )
    assert job is not None and job.task is not None
    await job.task

    stored = isolated_manager.get_job(job.job_id)
    assert stored is not None
    assert stored.status is JobStatus.FAILED
    assert stored.error == "fetch failed"
    assert stored.result is not None and stored.result.success is False
    assert stored.data.maintenance.outcome is MaintenanceOutcome.PASSED
    stream_manager.close.assert_called_once_with(job.job_id)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation_success", "cancel_requested", "release_fails", "expected_status"),
    [
        (True, False, False, JobStatus.COMPLETED),
        (False, False, False, JobStatus.FAILED),
        (True, True, False, JobStatus.CANCELLED),
        (False, True, False, JobStatus.FAILED),
        (True, False, True, JobStatus.FAILED),
    ],
)
async def test_sync_terminal_is_invisible_during_delayed_release(
    monkeypatch: pytest.MonkeyPatch,
    isolated_manager: GenericJobManager,
    operation_success: bool,
    cancel_requested: bool,
    release_fails: bool,
    expected_status: JobStatus,
) -> None:
    strategy = StrategyProbe(
        result=SyncResult(
            success=operation_success,
            totalApiCalls=1,
            errors=[] if operation_success else ["fetch failed"],
        ),
        emit_progress=False,
    )
    monkeypatch.setattr(sync_service, "get_strategy", lambda _mode: strategy)
    monkeypatch.setattr(
        isolated_manager, "is_cancelled", lambda _job_id: cancel_requested
    )
    stream_manager = MagicMock()
    monkeypatch.setattr(sync_service, "sync_stream_manager", stream_manager)
    release_started = threading.Event()
    allow_release = threading.Event()

    class DelayedReleaseFinalizer:
        def finalize(self, **kwargs: Any) -> MarketFinalizationDecision:
            passed = ImmediateFinalizer().finalize(
                operation_outcome=kwargs["operation_outcome"],
                operation_error=kwargs["operation_error"],
                publish_terminal=lambda _decision: None,
            )
            kwargs["stage_terminal"](passed)
            release_started.set()
            allow_release.wait()
            decision = passed
            if release_fails:
                decision = MarketFinalizationDecision(
                    terminal_outcome=MarketOperationOutcome.FAILED,
                    maintenance=MarketMaintenanceRecord.failed(
                        operation="incremental_sync",
                        recorded_at="2026-07-16T00:00:00+00:00",
                        error="Writer ownership release incomplete: unlock failed",
                    ),
                    error="Writer ownership release failed: unlock failed",
                )
            kwargs["publish_terminal"](decision)
            return decision

    job = await sync_service.start_sync(
        SyncMode.INCREMENTAL,
        _market_db(last_sync_date="2026-03-01T00:00:00+00:00"),
        DummyJQuantsClient(),
        time_series_store=_time_series_store(),
        market_finalizer=DelayedReleaseFinalizer(),  # type: ignore[arg-type]
    )
    assert job is not None and job.task is not None
    assert await asyncio.to_thread(release_started.wait, 1)

    assert job.status in {JobStatus.PENDING, JobStatus.RUNNING}
    assert job.data.maintenance.outcome is MaintenanceOutcome.NEVER_RUN
    stream_manager.close.assert_not_called()

    allow_release.set()
    await job.task
    assert job.status is expected_status
    if release_fails:
        assert job.data.maintenance.outcome is MaintenanceOutcome.FAILED
    else:
        assert job.data.maintenance.outcome is MaintenanceOutcome.PASSED
    stream_manager.close.assert_called_once_with(job.job_id)


@pytest.mark.asyncio
async def test_sync_stream_terminal_failure_replaces_clean_status_with_failed(
    monkeypatch: pytest.MonkeyPatch,
    isolated_manager: GenericJobManager,
) -> None:
    monkeypatch.setattr(
        sync_service, "get_strategy", lambda _mode: StrategyProbe(emit_progress=False)
    )
    stream_manager = MagicMock()
    stream_manager.close.side_effect = RuntimeError("SSE close failed")
    monkeypatch.setattr(sync_service, "sync_stream_manager", stream_manager)

    job = await sync_service.start_sync(
        SyncMode.INCREMENTAL,
        _market_db(last_sync_date="2026-03-01T00:00:00+00:00"),
        DummyJQuantsClient(),
        time_series_store=_time_series_store(),
        market_finalizer=ImmediateFinalizer(),  # type: ignore[arg-type]
    )
    assert job is not None and job.task is not None
    with pytest.raises(RuntimeError, match="terminal publication incomplete"):
        await job.task

    stored = isolated_manager.get_job(job.job_id)
    assert stored is not None
    assert stored.status is JobStatus.FAILED
    assert stored.error is not None
    assert "SSE close failed" in stored.error
    assert stored.data.maintenance.outcome is MaintenanceOutcome.FAILED
    assert stored.data.maintenance.recoveryCommand == "uv run bt market-maintain"


@pytest.mark.asyncio
async def test_sync_release_failure_compensates_staged_success_with_failed_job(
    monkeypatch: pytest.MonkeyPatch,
    isolated_manager: GenericJobManager,
) -> None:
    monkeypatch.setattr(
        sync_service, "get_strategy", lambda _mode: StrategyProbe(emit_progress=False)
    )

    class ReleaseFailingFinalizer(ImmediateFinalizer):
        def finalize(self, **kwargs: Any) -> MarketFinalizationDecision:
            provisional = MarketFinalizationDecision(
                terminal_outcome=MarketOperationOutcome.SUCCEEDED,
                maintenance=ImmediateFinalizer()
                .finalize(
                    operation_outcome=MarketOperationOutcome.SUCCEEDED,
                    operation_error=None,
                    publish_terminal=lambda _decision: None,
                )
                .maintenance,
            )
            kwargs["stage_terminal"](provisional)
            failed = MarketFinalizationDecision(
                terminal_outcome=MarketOperationOutcome.FAILED,
                maintenance=MarketMaintenanceRecord.failed(
                    operation="incremental_sync",
                    recorded_at="2026-07-16T00:00:00+00:00",
                    error="Writer ownership release incomplete: unlock failed",
                ),
                error="Writer ownership release failed: unlock failed",
            )
            kwargs["publish_terminal"](failed)
            return failed

    job = await sync_service.start_sync(
        SyncMode.INCREMENTAL,
        _market_db(last_sync_date="2026-03-01T00:00:00+00:00"),
        DummyJQuantsClient(),
        time_series_store=_time_series_store(),
        market_finalizer=ReleaseFailingFinalizer(),  # type: ignore[arg-type]
    )
    assert job is not None and job.task is not None
    await job.task

    stored = isolated_manager.get_job(job.job_id)
    assert stored is not None
    assert stored.status is JobStatus.FAILED
    assert stored.error is not None and "unlock failed" in stored.error
    assert stored.data.maintenance.outcome is MaintenanceOutcome.FAILED


@pytest.mark.asyncio
async def test_start_adjusted_metrics_materialization_runs_as_separate_job(
    monkeypatch: pytest.MonkeyPatch,
    isolated_materialize_manager: GenericJobManager,
) -> None:
    class FakeMaterializer:
        def __init__(self, market_db: object) -> None:
            self.market_db = market_db

        def reconcile(self, **kwargs: Any) -> object:
            kwargs["on_progress"](0, 1, "7203", 0)
            kwargs["on_progress"](1, 1, "7203", 4)
            return SimpleNamespace(
                completed_codes=1,
                total_codes=1,
                basis_count=4,
                published_basis_count=4,
                ready_basis_count=3,
                statement_rows=3,
                daily_valuation_rows=5,
                daily_technical_metric_rows=7,
                daily_valuation_latest_date="2026-05-16",
                active_price_basis_date="2026-05-15",
                active_basis_version="event-pit-v1:7203:2026-05-15",
            )

    monkeypatch.setattr(sync_service, "AdjustedMetricsMaterializer", FakeMaterializer)
    market_db = cast(sync_service.MarketDb, object())

    job = await sync_service.start_adjusted_metrics_materialization(market_db)
    assert job is not None and job.task is not None
    await job.task

    stored = isolated_materialize_manager.get_job(job.job_id)
    assert stored is not None
    assert stored.status.value == "completed"
    assert stored.progress is not None
    assert stored.progress.stage == "complete"
    assert stored.progress.completedCodes == 1
    assert stored.progress.totalCodes == 1
    assert stored.progress.currentCode is None
    assert stored.progress.publishedBasisCount == 4
    assert stored.result is not None
    assert stored.result.basisCount == 4
    assert stored.result.readyBasisCount == 3
    assert stored.result.statementRows == 3
    assert stored.result.dailyValuationRows == 5
    assert stored.result.dailyTechnicalMetricRows == 7
    assert stored.result.dailyValuationLatestDate == "2026-05-16"
    assert stored.result.activePriceBasisDate == "2026-05-15"
    assert stored.result.activeBasisVersion == "event-pit-v1:7203:2026-05-15"


@pytest.mark.asyncio
async def test_materialization_job_remains_nonterminal_until_market_finalizer(
    monkeypatch: pytest.MonkeyPatch,
    isolated_materialize_manager: GenericJobManager,
) -> None:
    from src.shared.contracts.market_maintenance import (
        MaintenanceEvidenceStatus,
        MaintenanceOutcome,
        MarketMaintenanceRecord,
    )
    from src.application.services.market_maintenance_finalizer import (
        MarketFinalizationDecision,
    )

    async def fake_materialization(*_args: object, **_kwargs: object) -> object:
        return SimpleNamespace(
            completed_codes=0,
            total_codes=0,
            basis_count=0,
            published_basis_count=0,
            ready_basis_count=0,
            statement_rows=0,
            daily_valuation_rows=0,
            daily_technical_metric_rows=0,
            daily_valuation_latest_date=None,
            active_price_basis_date=None,
            active_basis_version=None,
        )

    monkeypatch.setattr(
        sync_service,
        "run_shielded_materialization",
        fake_materialization,
    )
    finalizer_started = threading.Event()
    allow_finalizer = threading.Event()

    class BlockingFinalizer:
        def finalize(self, **kwargs: Any) -> MarketFinalizationDecision:
            decision = MarketFinalizationDecision(
                terminal_outcome=kwargs["operation_outcome"],
                maintenance=MarketMaintenanceRecord(
                    evidenceStatus=MaintenanceEvidenceStatus.VALID,
                    outcome=MaintenanceOutcome.PASSED,
                    operation="adjusted_metrics_materialization",
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
                error=kwargs["operation_error"],
            )
            kwargs["stage_terminal"](decision)
            finalizer_started.set()
            allow_finalizer.wait()
            kwargs["publish_terminal"](decision)
            return decision

    job = await sync_service.start_adjusted_metrics_materialization(
        cast(sync_service.MarketDb, object()),
        market_finalizer=BlockingFinalizer(),
    )
    assert job is not None and job.task is not None
    assert await asyncio.to_thread(finalizer_started.wait, 1)

    assert job.status not in {
        JobStatus.COMPLETED,
        JobStatus.FAILED,
        JobStatus.CANCELLED,
    }
    allow_finalizer.set()
    await job.task
    assert job.status is JobStatus.COMPLETED
    assert job.data.maintenance.outcome is MaintenanceOutcome.PASSED
    assert MarketOperationOutcome.SUCCEEDED.value == "succeeded"


@pytest.mark.asyncio
async def test_standalone_materialization_uses_production_timeout(
    monkeypatch: pytest.MonkeyPatch,
    isolated_materialize_manager: GenericJobManager,
) -> None:
    observed_timeouts: list[float] = []

    async def fake_run_shielded_materialization(
        materializer: object,
        *,
        timeout_seconds: float,
        on_progress: object,
    ) -> object:
        del materializer, on_progress
        observed_timeouts.append(timeout_seconds)
        return SimpleNamespace(
            completed_codes=0,
            total_codes=0,
            basis_count=0,
            published_basis_count=0,
            ready_basis_count=0,
            statement_rows=0,
            daily_valuation_rows=0,
            daily_technical_metric_rows=0,
            daily_valuation_latest_date=None,
            active_price_basis_date=None,
            active_basis_version=None,
        )

    monkeypatch.setattr(
        sync_service,
        "run_shielded_materialization",
        fake_run_shielded_materialization,
    )

    job = await sync_service.start_adjusted_metrics_materialization(
        cast(sync_service.MarketDb, object())
    )
    assert job is not None and job.task is not None
    await job.task

    assert observed_timeouts == [120 * 60]


@pytest.mark.asyncio
async def test_sync_nested_materialization_uses_production_timeout(
    monkeypatch: pytest.MonkeyPatch,
    isolated_manager: GenericJobManager,
) -> None:
    observed_timeouts: list[float] = []

    class MaterializingStrategy:
        async def execute(self, ctx: Any) -> SyncResult:
            assert ctx.materialize_adjusted_metrics is not None
            await ctx.materialize_adjusted_metrics()
            return SyncResult(success=True)

    async def fake_run_shielded_materialization(
        materializer: object,
        *,
        timeout_seconds: float,
        on_progress: object,
    ) -> object:
        del materializer, on_progress
        observed_timeouts.append(timeout_seconds)
        return object()

    monkeypatch.setattr(
        sync_service, "get_strategy", lambda _mode: MaterializingStrategy()
    )
    monkeypatch.setattr(
        sync_service,
        "run_shielded_materialization",
        fake_run_shielded_materialization,
    )

    job = await sync_service.start_sync(
        SyncMode.INITIAL,
        _market_db(),
        DummyJQuantsClient(),
        time_series_store=_time_series_store(),
    )
    assert job is not None and job.task is not None
    await job.task

    assert observed_timeouts == [120 * 60]


@pytest.mark.asyncio
async def test_materialization_cancel_joins_worker_before_close_finish_and_terminal_status(
    monkeypatch: pytest.MonkeyPatch,
    isolated_materialize_manager: GenericJobManager,
) -> None:
    worker_started = threading.Event()
    allow_code_boundary = threading.Event()
    worker_finished = threading.Event()
    cancel_seen_at_boundary = threading.Event()

    class BlockingMaterializer:
        def __init__(self, market_db: object) -> None:
            del market_db

        def reconcile(self, **kwargs: Any) -> object:
            worker_started.set()
            allow_code_boundary.wait()
            if kwargs["cancel_requested"]():
                cancel_seen_at_boundary.set()
            worker_finished.set()
            return SimpleNamespace(
                completed_codes=0,
                total_codes=1,
                basis_count=0,
                published_basis_count=0,
                ready_basis_count=0,
                statement_rows=0,
                daily_valuation_rows=0,
                daily_technical_metric_rows=0,
                daily_valuation_latest_date=None,
                active_price_basis_date=None,
                active_basis_version=None,
            )

    monkeypatch.setattr(
        sync_service, "AdjustedMetricsMaterializer", BlockingMaterializer
    )
    job = await sync_service.start_adjusted_metrics_materialization(
        cast(sync_service.MarketDb, object()),
    )
    assert job is not None and job.task is not None
    assert await asyncio.to_thread(worker_started.wait, 1)

    cancel_task = asyncio.create_task(
        isolated_materialize_manager.cancel_job(job.job_id, wait=True)
    )
    try:
        await asyncio.sleep(0.05)
        assert not worker_finished.is_set()
        assert job.status != JobStatus.CANCELLED
        assert not cancel_task.done()
    finally:
        allow_code_boundary.set()

    assert await cancel_task is True
    assert worker_finished.is_set()
    assert cancel_seen_at_boundary.is_set()
    assert job.status == JobStatus.CANCELLED


@pytest.mark.asyncio
async def test_repeated_materialization_cancel_cannot_detach_blocked_worker(
    monkeypatch: pytest.MonkeyPatch,
    isolated_materialize_manager: GenericJobManager,
) -> None:
    worker_started = threading.Event()
    allow_code_boundary = threading.Event()
    worker_finished = threading.Event()

    class BlockingMaterializer:
        def __init__(self, market_db: object) -> None:
            del market_db

        def reconcile(self, **kwargs: Any) -> object:
            worker_started.set()
            allow_code_boundary.wait()
            assert kwargs["cancel_requested"]()
            worker_finished.set()
            return SimpleNamespace()

    monkeypatch.setattr(
        sync_service, "AdjustedMetricsMaterializer", BlockingMaterializer
    )
    job = await sync_service.start_adjusted_metrics_materialization(
        cast(sync_service.MarketDb, object()),
    )
    assert job is not None and job.task is not None
    assert await asyncio.to_thread(worker_started.wait, 1)

    cancel_tasks: list[asyncio.Task[bool]] = []
    try:
        for _ in range(3):
            cancel_tasks.append(
                asyncio.create_task(
                    isolated_materialize_manager.cancel_job(job.job_id, wait=True)
                )
            )
            await asyncio.sleep(0)
        await asyncio.sleep(0.05)
        assert not worker_finished.is_set()
        assert job.status != JobStatus.CANCELLED
    finally:
        allow_code_boundary.set()

    results = await asyncio.gather(*cancel_tasks)
    assert any(results)
    assert worker_finished.is_set()
    assert job.status == JobStatus.CANCELLED


@pytest.mark.asyncio
async def test_materialization_timeout_joins_worker_before_failed_status(
    monkeypatch: pytest.MonkeyPatch,
    isolated_materialize_manager: GenericJobManager,
) -> None:
    worker_started = threading.Event()
    allow_code_boundary = threading.Event()
    worker_finished = threading.Event()

    class BlockingMaterializer:
        def __init__(self, market_db: object) -> None:
            del market_db

        def reconcile(self, **kwargs: Any) -> object:
            worker_started.set()
            allow_code_boundary.wait()
            assert kwargs["cancel_requested"]()
            worker_finished.set()
            return SimpleNamespace()

    monkeypatch.setattr(
        sync_service, "AdjustedMetricsMaterializer", BlockingMaterializer
    )
    monkeypatch.setattr(
        sync_service,
        "ADJUSTED_METRICS_MATERIALIZATION_TIMEOUT_MINUTES",
        0.001,
    )
    job = await sync_service.start_adjusted_metrics_materialization(
        cast(sync_service.MarketDb, object())
    )
    assert job is not None and job.task is not None
    assert await asyncio.to_thread(worker_started.wait, 1)

    try:
        await asyncio.sleep(0.1)
        assert not worker_finished.is_set()
        assert job.status != JobStatus.FAILED
        assert not job.task.done()
    finally:
        allow_code_boundary.set()

    await job.task
    assert worker_finished.is_set()
    assert job.status == JobStatus.FAILED
    assert job.error is not None and "timed out" in job.error


@pytest.mark.asyncio
async def test_sync_timeout_joins_materializer_before_failed_status(
    monkeypatch: pytest.MonkeyPatch,
    isolated_manager: GenericJobManager,
) -> None:
    worker_started = threading.Event()
    allow_code_boundary = threading.Event()
    worker_finished = threading.Event()

    class MaterializingStrategy:
        async def execute(self, ctx: Any) -> SyncResult:
            assert ctx.materialize_adjusted_metrics is not None
            await ctx.materialize_adjusted_metrics()
            return SyncResult(success=True)

    class BlockingMaterializer:
        def __init__(self, market_db: object) -> None:
            del market_db

        def reconcile(self, **kwargs: Any) -> object:
            del kwargs
            worker_started.set()
            allow_code_boundary.wait()
            worker_finished.set()
            return object()

    market_db = DummyMarketDb()
    store = DummyTimeSeriesStore()
    monkeypatch.setattr(
        sync_service, "get_strategy", lambda _mode: MaterializingStrategy()
    )
    monkeypatch.setattr(
        sync_service, "AdjustedMetricsMaterializer", BlockingMaterializer
    )
    monkeypatch.setattr(sync_service, "INITIAL_SYNC_JOB_TIMEOUT_MINUTES", 0.001)

    job = await sync_service.start_sync(
        SyncMode.INITIAL,
        cast(sync_service.SyncServiceMarketDbLike, market_db),
        DummyJQuantsClient(),
        time_series_store=cast(sync_service.SyncServiceTimeSeriesStoreLike, store),
    )
    assert job is not None and job.task is not None
    assert await asyncio.to_thread(worker_started.wait, 1)

    try:
        await asyncio.sleep(0.1)
        assert store.close_calls == 0
        assert not worker_finished.is_set()
        assert not job.task.done()
    finally:
        allow_code_boundary.set()

    await job.task
    assert worker_finished.is_set()
    assert store.close_calls == 0
    assert job.status == JobStatus.FAILED
    assert job.error is not None and "timed out" in job.error


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

    def reset_snapshot() -> tuple[
        sync_service.SyncServiceMarketDbLike,
        sync_service.SyncServiceTimeSeriesStoreLike,
    ]:
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
    assert reset_market_db.metadata[METADATA_KEYS["STOCK_PRICE_ADJUSTMENT_MODE"]] == (
        "local_projection_v2_event_time"
    )
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
async def test_start_sync_service_does_not_own_or_close_store(
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
    )
    assert job is not None and job.task is not None
    await job.task

    stored = isolated_manager.get_job(job.job_id)
    assert stored is not None
    assert stored.status.value == "completed"
    assert store.close_calls == 0


@pytest.mark.asyncio
async def test_start_sync_commits_cancelled_terminal_before_closing_stream_after_execute(
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
    assert stored.status.value == "cancelled"
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
async def test_sync_cancel_waits_for_market_finalizer_before_terminal_and_stream_close(
    monkeypatch: pytest.MonkeyPatch,
    isolated_manager: GenericJobManager,
) -> None:
    from src.shared.contracts.market_maintenance import (
        MaintenanceEvidenceStatus,
        MaintenanceOutcome,
        MarketMaintenanceRecord,
    )
    from src.application.services.market_maintenance_finalizer import (
        MarketFinalizationDecision,
    )

    strategy = StrategyProbe(emit_progress=False)
    monkeypatch.setattr(sync_service, "get_strategy", lambda _mode: strategy)
    stream_manager = MagicMock()
    monkeypatch.setattr(sync_service, "sync_stream_manager", stream_manager)
    finalizer_started = threading.Event()
    allow_finalizer = threading.Event()

    class BlockingFinalizer:
        def finalize(
            self,
            *,
            operation_outcome: MarketOperationOutcome,
            publish_terminal: Any,
            stage_terminal: Any,
            operation_error: str | None = None,
        ) -> MarketFinalizationDecision:
            decision = MarketFinalizationDecision(
                terminal_outcome=operation_outcome,
                maintenance=MarketMaintenanceRecord(
                    evidenceStatus=MaintenanceEvidenceStatus.VALID,
                    outcome=MaintenanceOutcome.PASSED,
                    operation="initial_sync",
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
                error=operation_error,
            )
            stage_terminal(decision)
            finalizer_started.set()
            allow_finalizer.wait()
            publish_terminal(decision)
            return decision

    job = await sync_service.start_sync(
        SyncMode.INITIAL,
        _market_db(),
        DummyJQuantsClient(),
        time_series_store=_time_series_store(),
        market_finalizer=BlockingFinalizer(),
    )
    assert job is not None and job.task is not None
    assert await asyncio.to_thread(finalizer_started.wait, 1)

    assert job.status not in {
        JobStatus.COMPLETED,
        JobStatus.FAILED,
        JobStatus.CANCELLED,
    }
    stream_manager.close.assert_not_called()

    cancel_task = asyncio.create_task(isolated_manager.cancel_job(job.job_id))
    await asyncio.sleep(0)
    assert not cancel_task.done()
    assert job.status not in {
        JobStatus.COMPLETED,
        JobStatus.FAILED,
        JobStatus.CANCELLED,
    }
    stream_manager.close.assert_not_called()

    allow_finalizer.set()
    assert await cancel_task is True
    assert job.status is JobStatus.CANCELLED
    assert job.data.maintenance.outcome is MaintenanceOutcome.PASSED
    stream_manager.close.assert_called_once_with(job.job_id)


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
async def test_start_sync_commits_cancelled_terminal_when_job_already_cancelled(
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
    assert stored.status.value == "cancelled"
    assert stored.result is not None
    published_events = [call.args[1] for call in stream_manager.publish.call_args_list]
    assert published_events[-1].event == "job"
    stream_manager.close.assert_called_once_with(job.job_id)


@pytest.mark.asyncio
async def test_start_sync_joins_requested_cancel_and_publishes_cancelled_terminal(
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
    await job.task

    stream_manager.publish.assert_called_once()
    stream_manager.close.assert_called_once_with(job.job_id)
    assert job.status is JobStatus.CANCELLED


@pytest.mark.asyncio
async def test_start_sync_unexpected_cancel_finalizes_stream_before_propagating(
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

    stream_manager.publish.assert_called_once()
    stream_manager.close.assert_called_once_with(job.job_id)
