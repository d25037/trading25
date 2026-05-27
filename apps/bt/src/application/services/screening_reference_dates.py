"""Reference-date and DB-inspection helpers for market screening."""

from __future__ import annotations

import os
from collections.abc import Callable
from typing import Any, Protocol

from loguru import logger

from src.shared.models.config import SharedConfig


class QueryOneReadable(Protocol):
    def query_one(self, sql: str, params: tuple[Any, ...] = ()) -> Any | None:
        ...


def resolve_date_range(
    *,
    shared_config: SharedConfig,
    reference_date: str | None,
    recent_days: int,
    get_latest_market_date: Callable[[], str | None],
    resolve_history_trading_days: Callable[[int], int],
    get_trading_date_before: Callable[[str, int], str | None],
) -> tuple[str | None, str | None]:
    """shared_config とクエリ日付からロード対象期間を解決する。"""
    start_date = shared_config.start_date or None
    end_date = shared_config.end_date or None

    if reference_date:
        end_date = reference_date
    elif end_date is None:
        end_date = get_latest_market_date()

    if start_date is None and end_date is not None:
        history_days = resolve_history_trading_days(recent_days)
        if history_days > 1:
            start_date = get_trading_date_before(end_date, history_days - 1)

    return start_date, end_date


def resolve_history_trading_days(
    recent_days: int,
    *,
    default_days: int,
    env_name: str = "BT_SCREENING_HISTORY_TRADING_DAYS",
) -> int:
    """screening 読み込み対象の営業日本数を決定する。"""
    resolved_default_days = default_days
    configured = os.getenv(env_name)
    if configured is not None:
        try:
            value = int(configured)
            if value > 0:
                resolved_default_days = value
            else:
                raise ValueError("must be > 0")
        except ValueError:
            logger.warning(
                f"Invalid {env_name}. Fallback to default.",
                value=configured,
            )

    return max(recent_days, resolved_default_days)


def get_latest_market_date(reader: QueryOneReadable) -> str | None:
    try:
        row = reader.query_one("SELECT MAX(date) as max_date FROM stock_data")
    except Exception:
        return None
    if row is None:
        return None
    return row["max_date"]


def get_latest_stock_master_date(
    reader: QueryOneReadable,
    *,
    table_exists,
) -> str | None:
    if not table_exists("stock_master_daily"):
        return None
    try:
        row = reader.query_one("SELECT MAX(date) as max_date FROM stock_master_daily")
    except Exception:
        return None
    if row is None:
        return None
    return row["max_date"]


def table_exists(reader: QueryOneReadable, table_name: str) -> bool:
    try:
        row = reader.query_one(
            """
            SELECT 1 AS exists
            FROM information_schema.tables
            WHERE lower(table_name) = lower(?)
            LIMIT 1
            """,
            (table_name,),
        )
    except Exception:
        return False
    return row is not None


def stock_master_daily_has_date(
    reader: QueryOneReadable,
    as_of_date: str,
    *,
    table_exists,
) -> bool:
    if not table_exists("stock_master_daily"):
        return False
    try:
        row = reader.query_one(
            "SELECT 1 AS exists FROM stock_master_daily WHERE date = ? LIMIT 1",
            (as_of_date,),
        )
    except Exception:
        return False
    return row is not None


def get_trading_date_before(reader: QueryOneReadable, date: str, offset: int) -> str | None:
    if offset < 0:
        return date
    try:
        row = reader.query_one(
            "SELECT DISTINCT date FROM stock_data WHERE date <= ? ORDER BY date DESC LIMIT 1 OFFSET ?",
            (date, offset),
        )
    except Exception:
        return None
    if row is None:
        try:
            oldest = reader.query_one(
                "SELECT MIN(date) AS min_date FROM stock_data WHERE date <= ?",
                (date,),
            )
        except Exception:
            return None
        if oldest is None:
            return None
        return oldest["min_date"]
    return row["date"]
