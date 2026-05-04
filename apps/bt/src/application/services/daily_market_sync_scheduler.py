"""Daily market DB incremental sync scheduler.

Runs inside the FastAPI process when enabled. The scheduler wakes at a
configured JST wall-clock time, checks whether J-Quants has TOPIX data for the
current date, and starts the existing incremental market DB sync job only on
actual trading days.
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime, time, timedelta
from types import SimpleNamespace
from typing import Any, Protocol, cast
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Request
from loguru import logger

from src.application.services.sync_service import SyncMode, start_sync

_JST = ZoneInfo("Asia/Tokyo")
_DEFAULT_CHECK_ENDPOINT = "/indices/bars/daily/topix"


class ScheduledSyncJQuantsClientLike(Protocol):
    @property
    def has_api_key(self) -> bool: ...

    async def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]: ...

    async def get_paginated(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        max_pages: int = 10,
    ) -> list[dict[str, Any]]: ...


class DailyMarketSyncSchedulerSettingsLike(Protocol):
    market_sync_scheduler_enabled: bool
    market_sync_scheduler_time_jst: str
    market_sync_scheduler_enforce_bulk_for_stock_data: bool


def _to_jst(now: datetime | None = None) -> datetime:
    resolved = now or datetime.now(_JST)
    if resolved.tzinfo is None:
        return resolved.replace(tzinfo=_JST)
    return resolved.astimezone(_JST)


def parse_schedule_time_jst(value: str) -> time:
    text = value.strip()
    try:
        hour_text, minute_text = text.split(":", maxsplit=1)
        parsed = time(int(hour_text), int(minute_text))
    except (TypeError, ValueError) as exc:
        raise ValueError("MARKET_SYNC_SCHEDULER_TIME_JST must be HH:MM") from exc
    return parsed


def seconds_until_next_schedule(
    *,
    now: datetime | None = None,
    schedule_time: time,
) -> float:
    current = _to_jst(now)
    target = datetime.combine(current.date(), schedule_time, tzinfo=_JST)
    if target <= current:
        target += timedelta(days=1)
    return (target - current).total_seconds()


def _jquants_date_param(target_date: date) -> str:
    return target_date.strftime("%Y%m%d")


def _row_matches_date(row: dict[str, Any], target_date: date) -> bool:
    expected_compact = target_date.strftime("%Y%m%d")
    expected_iso = target_date.isoformat()
    raw_value = row.get("Date", row.get("date"))
    if raw_value is None:
        return False
    text = str(raw_value).strip()
    return text in {expected_compact, expected_iso}


async def is_jquants_trading_day(
    jquants_client: ScheduledSyncJQuantsClientLike,
    *,
    target_date: date,
    endpoint: str = _DEFAULT_CHECK_ENDPOINT,
) -> bool:
    """Return True when J-Quants exposes TOPIX data for target_date."""
    if target_date.weekday() >= 5:
        return False

    rows = await jquants_client.get_paginated(
        endpoint,
        params={"date": _jquants_date_param(target_date)},
        max_pages=1,
    )
    return any(_row_matches_date(row, target_date) for row in rows)


async def trigger_scheduled_incremental_sync(
    app: FastAPI,
    *,
    jquants_client: ScheduledSyncJQuantsClientLike,
    target_date: date | None = None,
    enforce_bulk_for_stock_data: bool = False,
) -> str:
    """Check trading day and start an incremental sync job.

    Returns a small status string for logs/tests:
    - no_api_key
    - non_trading_day
    - already_running
    - started:<job_id>
    - setup_error:<message>
    """
    if not jquants_client.has_api_key:
        logger.info("Scheduled market sync skipped: JQuants API key is not configured")
        return "no_api_key"

    current_date = target_date or _to_jst().date()
    if not await is_jquants_trading_day(jquants_client, target_date=current_date):
        logger.info("Scheduled market sync skipped: {} is not a J-Quants trading day", current_date.isoformat())
        return "non_trading_day"

    # Reuse the HTTP route resource choreography so scheduled sync has the same
    # read-only reader close/restore behavior as POST /api/db/sync.
    from src.entrypoints.http.routes import db as db_routes

    request_like = cast(Request, SimpleNamespace(app=app))
    try:
        market_db, time_series_store = db_routes._prepare_market_write_resources(request_like)  # noqa: SLF001
    except RuntimeError as exc:
        logger.warning("Scheduled market sync setup failed: {}", exc)
        return f"setup_error:{exc}"

    try:
        job = await start_sync(
            SyncMode.INCREMENTAL,
            market_db,
            jquants_client,
            time_series_store=time_series_store,
            close_time_series_store=True,
            close_market_db=True,
            on_finish=lambda: db_routes._restore_read_only_market_resources(request_like),  # noqa: SLF001
            enforce_bulk_for_stock_data=enforce_bulk_for_stock_data,
        )
    except Exception:
        db_routes._restore_read_only_market_resources(request_like)  # noqa: SLF001
        raise

    if job is None:
        db_routes._restore_read_only_market_resources(request_like)  # noqa: SLF001
        logger.info("Scheduled market sync skipped: another sync job is already running")
        return "already_running"

    logger.info("Scheduled market sync started: jobId={} date={}", job.job_id, current_date.isoformat())
    return f"started:{job.job_id}"


async def run_daily_market_sync_scheduler(
    app: FastAPI,
    *,
    settings: DailyMarketSyncSchedulerSettingsLike,
    jquants_client: ScheduledSyncJQuantsClientLike,
) -> None:
    schedule_time = parse_schedule_time_jst(settings.market_sync_scheduler_time_jst)
    logger.info("Scheduled market sync enabled: daily at {} JST", schedule_time.strftime("%H:%M"))

    while True:
        await asyncio.sleep(seconds_until_next_schedule(schedule_time=schedule_time))
        try:
            await trigger_scheduled_incremental_sync(
                app,
                jquants_client=jquants_client,
                enforce_bulk_for_stock_data=settings.market_sync_scheduler_enforce_bulk_for_stock_data,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - scheduler should survive one failed wake
            logger.exception("Scheduled market sync failed: {}", exc)
