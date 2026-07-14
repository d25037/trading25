"""
Sync Service

DB Sync のオーケストレーション。
GenericJobManager を使用してバックグラウンド同期を管理する。
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any, Protocol, cast

from loguru import logger

from src.application.services.adjusted_metrics_materializer import (
    AdjustedMetricsMaterializer,
)
from src.application.services.adjusted_metrics_materialization_run import (
    MaterializationProgress,
    run_shielded_materialization,
)
from src.infrastructure.db.market.market_db import (
    LOCAL_STOCK_PRICE_ADJUSTMENT_MODE,
    MARKET_SCHEMA_VERSION,
    METADATA_KEYS,
    MarketDb,
)
from src.infrastructure.db.market.time_series_store import TimeSeriesInspection
from src.entrypoints.http.schemas.db import SyncProgress, SyncResult
from src.entrypoints.http.schemas.db import AdjustedMetricsMaterializeResult
from src.application.services.generic_job_manager import GenericJobManager, JobInfo
from src.application.services.sync_stream_manager import SyncStreamEvent, sync_stream_manager
from src.application.services.sync_strategies import (
    SyncContext,
    SyncClientLike,
    SyncMarketDbLike,
    SyncTimeSeriesStoreLike,
    get_strategy,
)
from src.shared.config.reliability import SYNC_JOB_TIMEOUT_MINUTES


class SyncServiceMarketDbLike(SyncMarketDbLike, Protocol):
    def ensure_schema(self) -> None: ...
    def is_legacy_stock_price_snapshot(self) -> bool: ...
    def get_market_schema_version(self) -> int | None: ...
    def is_market_schema_current(self) -> bool: ...


class SyncServiceTimeSeriesStoreLike(SyncTimeSeriesStoreLike, Protocol):
    def close(self) -> None: ...


class SyncServiceClientLike(SyncClientLike, Protocol):
    pass


type ResetMarketSnapshot = Callable[
    [],
    tuple[SyncServiceMarketDbLike, SyncServiceTimeSeriesStoreLike],
]


class SyncMode(str, Enum):
    AUTO = "auto"
    INITIAL = "initial"
    INCREMENTAL = "incremental"
    REPAIR = "repair"


@dataclass
class SyncJobData:
    mode: SyncMode
    resolved_mode: str = ""
    enforce_bulk_for_stock_data: bool = False
    fetch_details: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class AdjustedMetricsMaterializeJobData:
    mode: str = "full"


# Module-level manager instance
sync_job_manager: GenericJobManager[SyncJobData, SyncProgress, SyncResult] = GenericJobManager()
adjusted_metrics_materialize_job_manager: GenericJobManager[
    AdjustedMetricsMaterializeJobData,
    SyncProgress,
    AdjustedMetricsMaterializeResult,
] = GenericJobManager()
_MAX_FETCH_DETAILS = 200


def _publish_sync_job_event(job_id: str, *, close_stream: bool = False) -> None:
    sync_stream_manager.publish(job_id, SyncStreamEvent(event="job"))
    if close_stream:
        sync_stream_manager.close(job_id)


def _inspection_has_existing_snapshot(inspection: TimeSeriesInspection) -> bool:
    return any(
        (
            inspection.topix_count > 0,
            inspection.stock_count > 0,
            inspection.indices_count > 0,
            inspection.margin_count > 0,
            inspection.statements_count > 0,
            bool(inspection.topix_max),
            bool(inspection.stock_max),
            bool(inspection.indices_max),
            bool(inspection.margin_max),
            bool(inspection.latest_statement_disclosed_date),
        )
    )


def _resolve_mode(
    mode: SyncMode,
    market_db: SyncServiceMarketDbLike,
    *,
    time_series_store: SyncServiceTimeSeriesStoreLike | None = None,
) -> str:
    """auto モードを実際の戦略に解決"""
    if mode != SyncMode.AUTO:
        return mode.value

    last_sync = market_db.get_sync_metadata(METADATA_KEYS["LAST_SYNC_DATE"])
    if last_sync:
        return "incremental"

    if time_series_store is None:
        return "initial"

    try:
        inspection = time_series_store.inspect(missing_stock_dates_limit=1)
    except Exception as e:  # noqa: BLE001 - preserve backend failure details
        raise RuntimeError(f"DuckDB inspection failed while resolving AUTO sync mode: {e}") from e

    return "incremental" if _inspection_has_existing_snapshot(inspection) else "initial"


def _legacy_stock_snapshot_message() -> str:
    return (
        "Legacy market.duckdb detected. Reset market-timeseries/market.duckdb "
        "and market-timeseries/parquet, or run initial sync with reset enabled."
    )


def _incompatible_market_schema_message(version: int | None = None) -> str:
    observed = "missing" if version is None else str(version)
    return (
        f"Incompatible market.duckdb schema detected (version: {observed}, "
        f"required: {MARKET_SCHEMA_VERSION}). Run initial sync with reset enabled "
        "(resetBeforeSync=true) "
        "to recreate market-timeseries/market.duckdb and market-timeseries/parquet."
    )


def _prepare_market_db_for_sync(market_db: SyncServiceMarketDbLike) -> None:
    market_db.ensure_schema()
    if not market_db.is_market_schema_current():
        raise RuntimeError(
            _incompatible_market_schema_message(market_db.get_market_schema_version())
        )
    if market_db.is_legacy_stock_price_snapshot():
        raise RuntimeError(_legacy_stock_snapshot_message())
    market_db.set_sync_metadata(
        METADATA_KEYS["STOCK_PRICE_ADJUSTMENT_MODE"],
        LOCAL_STOCK_PRICE_ADJUSTMENT_MODE,
    )


async def start_adjusted_metrics_materialization(
    market_db: MarketDb,
    *,
    close_market_db: bool = False,
    on_finish: Callable[[], None] | None = None,
) -> JobInfo[
    AdjustedMetricsMaterializeJobData,
    SyncProgress,
    AdjustedMetricsMaterializeResult,
] | None:
    """Start a standalone full adjusted metrics materialization job."""
    job = await adjusted_metrics_materialize_job_manager.create_job(
        AdjustedMetricsMaterializeJobData()
    )
    if job is None:
        return None

    def on_progress(progress: MaterializationProgress) -> None:
        pct = (
            progress.completed_codes / progress.total_codes * 100
            if progress.total_codes > 0
            else 0
        )
        adjusted_metrics_materialize_job_manager.update_progress(
            job.job_id,
            SyncProgress(
                stage=progress.stage,
                current=progress.completed_codes,
                total=progress.total_codes,
                percentage=pct,
                message=(
                    f"Materializing code {progress.current_code}"
                    if progress.current_code is not None
                    else "Materializing adjusted metrics"
                ),
                completedCodes=progress.completed_codes,
                totalCodes=progress.total_codes,
                currentCode=progress.current_code,
                publishedBasisCount=progress.published_basis_count,
            ),
        )

    async def _run() -> None:
        try:
            result = await run_shielded_materialization(
                AdjustedMetricsMaterializer(market_db),
                timeout_seconds=SYNC_JOB_TIMEOUT_MINUTES * 60,
                on_progress=on_progress,
            )
            response = AdjustedMetricsMaterializeResult(
                success=True,
                basisCount=result.basis_count,
                readyBasisCount=result.ready_basis_count,
                statementRows=result.statement_rows,
                dailyValuationRows=result.daily_valuation_rows,
                dailyTechnicalMetricRows=result.daily_technical_metric_rows,
                dailyValuationLatestDate=result.daily_valuation_latest_date,
                activePriceBasisDate=result.active_price_basis_date,
                activeBasisVersion=result.active_basis_version,
            )
            adjusted_metrics_materialize_job_manager.update_progress(
                job.job_id,
                SyncProgress(
                    stage="complete",
                    current=result.completed_codes,
                    total=result.total_codes,
                    percentage=100,
                    message="Adjusted and technical metrics materialization complete.",
                    completedCodes=result.completed_codes,
                    totalCodes=result.total_codes,
                    currentCode=None,
                    publishedBasisCount=result.published_basis_count,
                ),
            )
            adjusted_metrics_materialize_job_manager.complete_job(job.job_id, response)
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            adjusted_metrics_materialize_job_manager.fail_job(
                job.job_id,
                (
                    "Adjusted metrics materialization timed out after "
                    f"{SYNC_JOB_TIMEOUT_MINUTES} minutes"
                ),
            )
        except Exception as e:
            logger.exception(f"Adjusted metrics materialization job {job.job_id} failed: {e}")
            adjusted_metrics_materialize_job_manager.fail_job(job.job_id, str(e))
        finally:
            if close_market_db:
                try:
                    await asyncio.to_thread(market_db.close)
                except Exception as e:  # pragma: no cover - close失敗はログのみ
                    logger.warning(f"Failed to close market DB for adjusted metrics job {job.job_id}: {e}")
            if on_finish is not None:
                try:
                    await asyncio.to_thread(on_finish)
                except Exception as e:  # pragma: no cover - restore失敗はログのみ
                    logger.warning(f"Failed to run adjusted metrics finish callback for job {job.job_id}: {e}")

    task = asyncio.create_task(_run())
    job.task = task
    return job


async def start_sync(
    mode: SyncMode,
    market_db: SyncServiceMarketDbLike,
    jquants_client: SyncServiceClientLike,
    time_series_store: SyncServiceTimeSeriesStoreLike | None = None,
    close_time_series_store: bool = False,
    close_market_db: bool = False,
    on_finish: Callable[[], None] | None = None,
    enforce_bulk_for_stock_data: bool = False,
    reset_before_sync: bool = False,
    reset_market_snapshot: ResetMarketSnapshot | None = None,
) -> JobInfo[SyncJobData, SyncProgress, SyncResult] | None:
    """Sync ジョブを作成して開始。アクティブジョブがある場合は None。"""
    if time_series_store is None:
        raise RuntimeError("DuckDB time-series store is required for sync")
    if reset_before_sync:
        if mode is not SyncMode.INITIAL:
            raise RuntimeError("resetBeforeSync is supported only for initial sync")
        if reset_market_snapshot is None:
            raise RuntimeError("resetBeforeSync requires a reset callback")
        resolved_mode = SyncMode.INITIAL.value
    else:
        _prepare_market_db_for_sync(market_db)
        resolved_mode = _resolve_mode(mode, market_db, time_series_store=time_series_store)
    data = SyncJobData(
        mode=mode,
        resolved_mode=resolved_mode,
        enforce_bulk_for_stock_data=enforce_bulk_for_stock_data,
    )
    job = await sync_job_manager.create_job(data)
    if job is None:
        return None

    strategy = get_strategy(resolved_mode)
    job.data.resolved_mode = resolved_mode

    async def _run() -> None:
        current_market_db = market_db
        current_time_series_store = time_series_store

        def on_fetch_detail(detail: dict[str, Any]) -> None:
            entry = {
                **detail,
                "timestamp": detail.get("timestamp") or datetime.now(UTC).isoformat(),
            }
            job.data.fetch_details.append(entry)
            if len(job.data.fetch_details) > _MAX_FETCH_DETAILS:
                del job.data.fetch_details[: len(job.data.fetch_details) - _MAX_FETCH_DETAILS]
            sync_stream_manager.publish(
                job.job_id,
                SyncStreamEvent(event="fetch-detail", payload=entry),
            )

        def on_progress(stage: str, current: int, total: int, message: str) -> None:
            pct = (current / total * 100) if total > 0 else 0
            sync_job_manager.update_progress(
                job.job_id,
                SyncProgress(stage=stage, current=current, total=total, percentage=pct, message=message),
            )
            _publish_sync_job_event(job.job_id)

        try:
            if reset_before_sync:
                on_progress("reset", 0, 1, "Resetting market.duckdb and parquet before initial sync...")
                assert reset_market_snapshot is not None
                current_market_db, current_time_series_store = await asyncio.to_thread(reset_market_snapshot)
                on_progress("reset", 1, 1, "Reset complete. Starting initial sync...")
                _prepare_market_db_for_sync(current_market_db)

            async def materialize_adjusted_metrics() -> None:
                def on_materialization_progress(
                    progress: MaterializationProgress,
                ) -> None:
                    pct = (
                        progress.completed_codes / progress.total_codes * 100
                        if progress.total_codes > 0
                        else 0
                    )
                    sync_job_manager.update_progress(
                        job.job_id,
                        SyncProgress(
                            stage=progress.stage,
                            current=progress.completed_codes,
                            total=progress.total_codes,
                            percentage=pct,
                            message=(
                                f"Materializing code {progress.current_code}"
                                if progress.current_code is not None
                                else "Materializing adjusted metrics"
                            ),
                            completedCodes=progress.completed_codes,
                            totalCodes=progress.total_codes,
                            currentCode=progress.current_code,
                            publishedBasisCount=progress.published_basis_count,
                        ),
                    )
                    _publish_sync_job_event(job.job_id)

                await run_shielded_materialization(
                    AdjustedMetricsMaterializer(
                        cast(MarketDb, current_market_db)
                    ),
                    timeout_seconds=SYNC_JOB_TIMEOUT_MINUTES * 60,
                    on_progress=on_materialization_progress,
                )

            ctx = SyncContext(
                client=jquants_client,
                market_db=current_market_db,
                time_series_store=current_time_series_store,
                cancelled=job.cancelled,
                on_progress=on_progress,
                on_fetch_detail=on_fetch_detail,
                enforce_bulk_for_stock_data=enforce_bulk_for_stock_data,
                materialize_adjusted_metrics=(
                    materialize_adjusted_metrics
                    if resolved_mode
                    in {
                        SyncMode.INITIAL.value,
                        SyncMode.INCREMENTAL.value,
                        SyncMode.REPAIR.value,
                    }
                    else None
                ),
            )
            result = await asyncio.wait_for(strategy.execute(ctx), timeout=SYNC_JOB_TIMEOUT_MINUTES * 60)
            if sync_job_manager.is_cancelled(job.job_id):
                _publish_sync_job_event(job.job_id, close_stream=True)
                return
            if not result.success:
                error_message = "; ".join(result.errors) if result.errors else "Sync failed"
                sync_job_manager.fail_job(job.job_id, error_message)
                stored_job = sync_job_manager.get_job(job.job_id)
                if stored_job is not None:
                    stored_job.result = result
                _publish_sync_job_event(job.job_id, close_stream=True)
                return
            sync_job_manager.complete_job(job.job_id, result)
            _publish_sync_job_event(job.job_id, close_stream=True)
        except asyncio.TimeoutError:
            sync_job_manager.fail_job(job.job_id, f"Sync timed out after {SYNC_JOB_TIMEOUT_MINUTES} minutes")
            _publish_sync_job_event(job.job_id, close_stream=True)
        except asyncio.CancelledError:
            if sync_job_manager.is_cancelled(job.job_id):
                _publish_sync_job_event(job.job_id, close_stream=True)
            else:
                sync_stream_manager.close(job.job_id)
            raise
        except Exception as e:
            logger.exception(f"Sync job {job.job_id} failed: {e}")
            sync_job_manager.fail_job(job.job_id, str(e))
            _publish_sync_job_event(job.job_id, close_stream=True)
        finally:
            if close_time_series_store and current_time_series_store is not None:
                try:
                    await asyncio.to_thread(current_time_series_store.close)
                except Exception as e:  # pragma: no cover - close失敗はログのみ
                    logger.warning(f"Failed to close time-series store for job {job.job_id}: {e}")
            if close_market_db and current_market_db is not None:
                close = getattr(current_market_db, "close", None)
                if callable(close):
                    try:
                        await asyncio.to_thread(close)
                    except Exception as e:  # pragma: no cover - close失敗はログのみ
                        logger.warning(f"Failed to close market DB for job {job.job_id}: {e}")
            if on_finish is not None:
                try:
                    await asyncio.to_thread(on_finish)
                except Exception as e:  # pragma: no cover - restore失敗はログのみ
                    logger.warning(f"Failed to run sync finish callback for job {job.job_id}: {e}")

    task = asyncio.create_task(_run())
    job.task = task

    return job
