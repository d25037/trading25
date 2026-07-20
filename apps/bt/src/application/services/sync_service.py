"""
Sync Service

DB Sync のオーケストレーション。
GenericJobManager を使用してバックグラウンド同期を管理する。
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
import threading
from typing import Any, Protocol

from loguru import logger

from src.application.services.adjusted_metrics_materializer import (
    AdjustedMetricsMaterializer,
)
from src.application.contracts.jobs import JobStatus
from src.shared.contracts import market_maintenance as maintenance_contracts
from src.application.services.market_maintenance_finalizer import (
    MarketFinalizationDecision,
    MarketMaintenanceFinalizer,
    finalize_market_operation_joined,
)
from src.application.services.adjusted_metrics_materialization_run import (
    MaterializationProgress,
    run_shielded_materialization,
)
from src.infrastructure.db.market.market_db import (
    PROVIDER_STOCK_PRICE_ADJUSTMENT_MODE,
    MARKET_SCHEMA_VERSION,
    METADATA_KEYS,
    MarketDb,
)
from src.infrastructure.db.market.time_series_store import TimeSeriesInspection
from src.application.contracts.market_data_plane import SyncProgress, SyncResult
from src.application.contracts.market_data_plane import AdjustedMetricsMaterializeResult
from src.application.services.generic_job_manager import GenericJobManager, JobInfo
from src.application.services.sync_stream_manager import (
    SyncStreamEvent,
    sync_stream_manager,
)
from src.application.services.sync_strategies import (
    SyncContext,
    SyncClientLike,
    SyncMarketDbLike,
    SyncTimeSeriesStoreLike,
    get_strategy,
)
from src.shared.config.reliability import (
    ADJUSTED_METRICS_MATERIALIZATION_TIMEOUT_MINUTES,
    INITIAL_SYNC_JOB_TIMEOUT_MINUTES,
    SYNC_JOB_TIMEOUT_MINUTES,
)
from src.shared.config.settings import get_settings


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
type MarketFinalizerProvider = Callable[[], MarketMaintenanceFinalizer]
type RecomputeAffectedStockCodes = Callable[[frozenset[str]], Awaitable[None]]


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
    maintenance: maintenance_contracts.MarketMaintenanceRecord = field(
        default_factory=maintenance_contracts.MarketMaintenanceRecord.never_run
    )


@dataclass
class AdjustedMetricsMaterializeJobData:
    mode: str = "full"
    maintenance: maintenance_contracts.MarketMaintenanceRecord = field(
        default_factory=maintenance_contracts.MarketMaintenanceRecord.never_run
    )


# Module-level manager instance
sync_job_manager: GenericJobManager[SyncJobData, SyncProgress, SyncResult] = (
    GenericJobManager()
)
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
        raise RuntimeError(
            f"DuckDB inspection failed while resolving AUTO sync mode: {e}"
        ) from e

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
        PROVIDER_STOCK_PRICE_ADJUSTMENT_MODE,
    )


async def start_adjusted_metrics_materialization(
    market_db: MarketDb,
    *,
    market_finalizer: MarketMaintenanceFinalizer
    | MarketFinalizerProvider
    | None = None,
) -> (
    JobInfo[
        AdjustedMetricsMaterializeJobData,
        SyncProgress,
        AdjustedMetricsMaterializeResult,
    ]
    | None
):
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
        operation_outcome = maintenance_contracts.MarketOperationOutcome.SUCCEEDED
        operation_error: str | None = None
        response: AdjustedMetricsMaterializeResult | None = None
        propagate_cancel = False
        try:
            result = await run_shielded_materialization(
                AdjustedMetricsMaterializer(market_db),
                timeout_seconds=ADJUSTED_METRICS_MATERIALIZATION_TIMEOUT_MINUTES * 60,
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
        except asyncio.CancelledError:
            operation_outcome = maintenance_contracts.MarketOperationOutcome.CANCELLED
            propagate_cancel = (
                not adjusted_metrics_materialize_job_manager.is_cancelled(job.job_id)
            )
        except asyncio.TimeoutError:
            operation_outcome = maintenance_contracts.MarketOperationOutcome.TIMED_OUT
            operation_error = (
                "Adjusted metrics materialization timed out after "
                f"{ADJUSTED_METRICS_MATERIALIZATION_TIMEOUT_MINUTES} minutes"
            )
        except Exception as e:
            logger.exception(
                f"Adjusted metrics materialization job {job.job_id} failed: {e}"
            )
            operation_outcome = maintenance_contracts.MarketOperationOutcome.FAILED
            operation_error = str(e)

        def commit_terminal(decision: MarketFinalizationDecision) -> None:
            job.data.maintenance = decision.maintenance
            if (
                decision.terminal_outcome is maintenance_contracts.MarketOperationOutcome.SUCCEEDED
                and adjusted_metrics_materialize_job_manager.is_cancelled(job.job_id)
            ):
                status = JobStatus.CANCELLED
            elif decision.terminal_outcome is maintenance_contracts.MarketOperationOutcome.SUCCEEDED:
                status = JobStatus.COMPLETED
            elif decision.terminal_outcome is maintenance_contracts.MarketOperationOutcome.CANCELLED:
                status = JobStatus.CANCELLED
            else:
                status = JobStatus.FAILED
            adjusted_metrics_materialize_job_manager.finalize_job(
                job.job_id,
                status=status,
                result=response,
                error=decision.error,
            )

        if market_finalizer is not None:
            resolved_finalizer = (
                market_finalizer() if callable(market_finalizer) else market_finalizer
            )
            loop = asyncio.get_running_loop()
            provisional_terminal: list[MarketFinalizationDecision] = []

            def publish_from_worker(decision: MarketFinalizationDecision) -> None:
                if len(provisional_terminal) != 1:
                    raise RuntimeError(
                        "Materialization terminal decision was not staged exactly once"
                    )
                committed = threading.Event()
                publication_error: list[BaseException] = []

                def publish_on_loop() -> None:
                    try:
                        commit_terminal(decision)
                    except BaseException as exc:
                        publication_error.append(exc)
                    finally:
                        committed.set()

                loop.call_soon_threadsafe(publish_on_loop)
                committed.wait()
                if publication_error:
                    raise publication_error[0]

            await finalize_market_operation_joined(
                resolved_finalizer,
                operation_outcome=operation_outcome,
                operation_error=operation_error,
                stage_terminal=provisional_terminal.append,
                publish_terminal=publish_from_worker,
            )
        else:
            commit_terminal(
                MarketFinalizationDecision(
                    terminal_outcome=operation_outcome,
                    maintenance=maintenance_contracts.MarketMaintenanceRecord.never_run(),
                    error=operation_error,
                )
            )

        if propagate_cancel:
            raise asyncio.CancelledError

    task = asyncio.create_task(_run())
    job.task = task
    return job


async def start_sync(
    mode: SyncMode,
    market_db: SyncServiceMarketDbLike,
    jquants_client: SyncServiceClientLike,
    time_series_store: SyncServiceTimeSeriesStoreLike | None = None,
    enforce_bulk_for_stock_data: bool = False,
    reset_before_sync: bool = False,
    reset_market_snapshot: ResetMarketSnapshot | None = None,
    market_finalizer: MarketMaintenanceFinalizer
    | MarketFinalizerProvider
    | None = None,
    recompute_affected_stock_codes: RecomputeAffectedStockCodes | None = None,
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
        resolved_mode = _resolve_mode(
            mode, market_db, time_series_store=time_series_store
        )
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
    sync_timeout_minutes = (
        INITIAL_SYNC_JOB_TIMEOUT_MINUTES
        if resolved_mode == SyncMode.INITIAL.value
        else SYNC_JOB_TIMEOUT_MINUTES
    )

    async def _run() -> None:
        current_market_db = market_db
        current_time_series_store = time_series_store
        stock_progress = {
            "stockRowsAppended": 0,
            "affectedStockCodes": 0,
            "stockCodesReplaced": 0,
            "stockRowsReplaced": 0,
        }

        def on_fetch_detail(detail: dict[str, Any]) -> None:
            entry = {
                **detail,
                "timestamp": detail.get("timestamp") or datetime.now(UTC).isoformat(),
            }
            job.data.fetch_details.append(entry)
            if len(job.data.fetch_details) > _MAX_FETCH_DETAILS:
                del job.data.fetch_details[
                    : len(job.data.fetch_details) - _MAX_FETCH_DETAILS
                ]
            sync_stream_manager.publish(
                job.job_id,
                SyncStreamEvent(event="fetch-detail", payload=entry),
            )

        def on_progress(stage: str, current: int, total: int, message: str) -> None:
            pct = (current / total * 100) if total > 0 else 0
            sync_job_manager.update_progress(
                job.job_id,
                SyncProgress(
                    stage=stage,
                    current=current,
                    total=total,
                    percentage=pct,
                    message=message,
                    stockRowsAppended=stock_progress["stockRowsAppended"],
                    affectedStockCodes=stock_progress["affectedStockCodes"],
                    stockCodesReplaced=stock_progress["stockCodesReplaced"],
                    stockRowsReplaced=stock_progress["stockRowsReplaced"],
                ),
            )
            _publish_sync_job_event(job.job_id)

        def on_stock_commit(
            appended_rows: int,
            affected_codes: int,
            replaced_codes: int,
            replaced_rows: int,
        ) -> None:
            stock_progress.update(
                stockRowsAppended=appended_rows,
                affectedStockCodes=affected_codes,
                stockCodesReplaced=replaced_codes,
                stockRowsReplaced=replaced_rows,
            )

        operation_outcome = maintenance_contracts.MarketOperationOutcome.SUCCEEDED
        operation_error: str | None = None
        operation_result: SyncResult | None = None
        propagate_cancel = False

        try:
            if reset_before_sync:
                on_progress(
                    "reset",
                    0,
                    1,
                    "Resetting market.duckdb and parquet before initial sync...",
                )
                assert reset_market_snapshot is not None
                current_market_db, current_time_series_store = await asyncio.to_thread(
                    reset_market_snapshot
                )
                on_progress("reset", 1, 1, "Reset complete. Starting initial sync...")
                _prepare_market_db_for_sync(current_market_db)

            ctx = SyncContext(
                client=jquants_client,
                market_db=current_market_db,
                time_series_store=current_time_series_store,
                cancelled=job.cancelled,
                on_progress=on_progress,
                on_fetch_detail=on_fetch_detail,
                enforce_bulk_for_stock_data=enforce_bulk_for_stock_data,
                provider_plan=(
                    str(getattr(jquants_client, "plan", "")).strip().lower()
                    or get_settings().jquants_plan.strip().lower()
                ),
                recompute_affected_stock_codes=recompute_affected_stock_codes,
                on_stock_commit=on_stock_commit,
            )
            operation_result = await asyncio.wait_for(
                strategy.execute(ctx), timeout=sync_timeout_minutes * 60
            )
            if not operation_result.success:
                operation_outcome = maintenance_contracts.MarketOperationOutcome.FAILED
                operation_error = (
                    "; ".join(operation_result.errors)
                    if operation_result.errors
                    else "Sync failed"
                )
            elif sync_job_manager.is_cancelled(job.job_id):
                operation_outcome = maintenance_contracts.MarketOperationOutcome.CANCELLED
        except asyncio.TimeoutError:
            operation_outcome = maintenance_contracts.MarketOperationOutcome.TIMED_OUT
            operation_error = f"Sync timed out after {sync_timeout_minutes} minutes"
        except asyncio.CancelledError:
            operation_outcome = maintenance_contracts.MarketOperationOutcome.CANCELLED
            propagate_cancel = not sync_job_manager.is_cancelled(job.job_id)
        except Exception as e:
            logger.exception(f"Sync job {job.job_id} failed: {e}")
            operation_outcome = maintenance_contracts.MarketOperationOutcome.FAILED
            operation_error = str(e)

        def commit_terminal(decision: MarketFinalizationDecision) -> None:
            job.data.maintenance = decision.maintenance
            if (
                decision.terminal_outcome is maintenance_contracts.MarketOperationOutcome.SUCCEEDED
                and sync_job_manager.is_cancelled(job.job_id)
            ):
                status = JobStatus.CANCELLED
            elif decision.terminal_outcome is maintenance_contracts.MarketOperationOutcome.SUCCEEDED:
                status = JobStatus.COMPLETED
            elif decision.terminal_outcome is maintenance_contracts.MarketOperationOutcome.CANCELLED:
                status = JobStatus.CANCELLED
            else:
                status = JobStatus.FAILED
            sync_job_manager.finalize_job(
                job.job_id,
                status=status,
                result=operation_result,
                error=decision.error,
            )
            try:
                _publish_sync_job_event(job.job_id, close_stream=True)
            except BaseException as exc:
                publication_error = (
                    f"Market terminal publication incomplete: {exc}. "
                    "Writer ownership remains fenced; retry recovery after shutdown."
                )
                job.data.maintenance = maintenance_contracts.MarketMaintenanceRecord.failed(
                    operation=f"{job.data.resolved_mode or job.data.mode.value}_sync",
                    recorded_at=datetime.now(UTC).isoformat(),
                    error=publication_error,
                )
                sync_job_manager.mark_terminal_publication_failed(
                    job.job_id,
                    result=operation_result,
                    error=publication_error,
                )
                raise RuntimeError(publication_error) from exc

        if market_finalizer is not None:
            resolved_finalizer = (
                market_finalizer() if callable(market_finalizer) else market_finalizer
            )
            loop = asyncio.get_running_loop()
            provisional_terminal: list[MarketFinalizationDecision] = []

            def publish_from_worker(decision: MarketFinalizationDecision) -> None:
                if len(provisional_terminal) != 1:
                    raise RuntimeError(
                        "Sync terminal decision was not staged exactly once"
                    )
                committed = threading.Event()
                publication_error: list[BaseException] = []

                def publish_on_loop() -> None:
                    try:
                        commit_terminal(decision)
                    except BaseException as exc:
                        publication_error.append(exc)
                    finally:
                        committed.set()

                loop.call_soon_threadsafe(publish_on_loop)
                committed.wait()
                if publication_error:
                    raise publication_error[0]

            await finalize_market_operation_joined(
                resolved_finalizer,
                operation_outcome=operation_outcome,
                operation_error=operation_error,
                stage_terminal=provisional_terminal.append,
                publish_terminal=publish_from_worker,
            )
        else:
            commit_terminal(
                MarketFinalizationDecision(
                    terminal_outcome=operation_outcome,
                    maintenance=maintenance_contracts.MarketMaintenanceRecord.never_run(),
                    error=operation_error,
                )
            )

        if propagate_cancel:
            raise asyncio.CancelledError

    task = asyncio.create_task(_run())
    job.task = task

    return job
