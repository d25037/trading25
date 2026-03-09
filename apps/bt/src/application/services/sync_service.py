"""
Sync Service

DB Sync のオーケストレーション。
GenericJobManager を使用してバックグラウンド同期を管理する。
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any, Protocol

from loguru import logger

from src.infrastructure.db.market.market_db import METADATA_KEYS
from src.infrastructure.db.market.time_series_store import TimeSeriesInspection
from src.entrypoints.http.schemas.db import SyncProgress, SyncResult
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


class SyncServiceTimeSeriesStoreLike(SyncTimeSeriesStoreLike, Protocol):
    def close(self) -> None: ...


class SyncServiceClientLike(SyncClientLike, Protocol):
    pass


class SyncMode(str, Enum):
    AUTO = "auto"
    INITIAL = "initial"
    INCREMENTAL = "incremental"
    INDICES_ONLY = "indices-only"
    REPAIR = "repair"


@dataclass
class SyncJobData:
    mode: SyncMode
    resolved_mode: str = ""
    enforce_bulk_for_stock_data: bool = False
    fetch_details: list[dict[str, Any]] = field(default_factory=list)


# Module-level manager instance
sync_job_manager: GenericJobManager[SyncJobData, SyncProgress, SyncResult] = GenericJobManager()
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


async def start_sync(
    mode: SyncMode,
    market_db: SyncServiceMarketDbLike,
    jquants_client: SyncServiceClientLike,
    time_series_store: SyncServiceTimeSeriesStoreLike | None = None,
    close_time_series_store: bool = False,
    enforce_bulk_for_stock_data: bool = False,
) -> JobInfo[SyncJobData, SyncProgress, SyncResult] | None:
    """Sync ジョブを作成して開始。アクティブジョブがある場合は None。"""
    if time_series_store is None:
        raise RuntimeError("DuckDB time-series store is required for sync")
    market_db.ensure_schema()
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

        ctx = SyncContext(
            client=jquants_client,
            market_db=market_db,
            time_series_store=time_series_store,
            cancelled=job.cancelled,
            on_progress=on_progress,
            on_fetch_detail=on_fetch_detail,
            enforce_bulk_for_stock_data=enforce_bulk_for_stock_data,
        )
        try:
            result = await asyncio.wait_for(strategy.execute(ctx), timeout=SYNC_JOB_TIMEOUT_MINUTES * 60)
            if sync_job_manager.is_cancelled(job.job_id):
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
            if close_time_series_store and time_series_store is not None:
                try:
                    await asyncio.to_thread(time_series_store.close)
                except Exception as e:  # pragma: no cover - close失敗はログのみ
                    logger.warning(f"Failed to close time-series store for job {job.job_id}: {e}")

    task = asyncio.create_task(_run())
    job.task = task

    return job
