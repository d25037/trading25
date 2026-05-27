"""Small time-series upsert writers for MarketDb."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from src.infrastructure.db.market.market_schema import (
    LOCAL_STOCK_PRICE_ADJUSTMENT_MODE,
    METADATA_KEYS,
)

ExecuteMany = Callable[[str, list[tuple[Any, ...]]], None]
SetMetadata = Callable[[str, str], None]


def upsert_stock_data(
    executemany: ExecuteMany,
    set_sync_metadata: SetMetadata,
    rows: list[dict[str, Any]],
) -> int:
    if not rows:
        return 0
    params = [
        (
            row.get("code"),
            row.get("date"),
            row.get("open"),
            row.get("high"),
            row.get("low"),
            row.get("close"),
            row.get("volume"),
            row.get("adjustment_factor"),
            row.get("created_at"),
        )
        for row in rows
    ]
    executemany(
        """
        INSERT INTO stock_data_raw (
            code, date, open, high, low, close, volume, adjustment_factor, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (code, date) DO UPDATE
        SET open = excluded.open,
            high = excluded.high,
            low = excluded.low,
            close = excluded.close,
            volume = excluded.volume,
            adjustment_factor = excluded.adjustment_factor,
            created_at = excluded.created_at
        """,
        params,
    )
    executemany(
        """
        INSERT INTO stock_data (
            code, date, open, high, low, close, volume, adjustment_factor, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (code, date) DO UPDATE
        SET open = excluded.open,
            high = excluded.high,
            low = excluded.low,
            close = excluded.close,
            volume = excluded.volume,
            adjustment_factor = excluded.adjustment_factor,
            created_at = excluded.created_at
        """,
        params,
    )
    set_sync_metadata(
        METADATA_KEYS["STOCK_PRICE_ADJUSTMENT_MODE"],
        LOCAL_STOCK_PRICE_ADJUSTMENT_MODE,
    )
    return len(rows)


def upsert_stock_minute_data(executemany: ExecuteMany, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    params = [
        (
            row.get("code"),
            row.get("date"),
            row.get("time"),
            row.get("open"),
            row.get("high"),
            row.get("low"),
            row.get("close"),
            row.get("volume"),
            row.get("turnover_value"),
            row.get("created_at"),
        )
        for row in rows
    ]
    executemany(
        """
        INSERT INTO stock_data_minute_raw (
            code, date, time, open, high, low, close, volume, turnover_value, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (code, date, time) DO UPDATE
        SET open = excluded.open,
            high = excluded.high,
            low = excluded.low,
            close = excluded.close,
            volume = excluded.volume,
            turnover_value = excluded.turnover_value,
            created_at = excluded.created_at
        """,
        params,
    )
    return len(rows)


def upsert_topix_data(executemany: ExecuteMany, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    params = [
        (
            row.get("date"),
            row.get("open"),
            row.get("high"),
            row.get("low"),
            row.get("close"),
            row.get("created_at"),
        )
        for row in rows
    ]
    executemany(
        """
        INSERT INTO topix_data (date, open, high, low, close, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT (date) DO UPDATE
        SET open = excluded.open,
            high = excluded.high,
            low = excluded.low,
            close = excluded.close,
            created_at = excluded.created_at
        """,
        params,
    )
    return len(rows)


def upsert_indices_data(executemany: ExecuteMany, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    params = [
        (
            row.get("code"),
            row.get("date"),
            row.get("open"),
            row.get("high"),
            row.get("low"),
            row.get("close"),
            row.get("sector_name"),
            row.get("created_at"),
        )
        for row in rows
    ]
    executemany(
        """
        INSERT INTO indices_data (code, date, open, high, low, close, sector_name, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (code, date) DO UPDATE
        SET open = excluded.open,
            high = excluded.high,
            low = excluded.low,
            close = excluded.close,
            sector_name = excluded.sector_name,
            created_at = excluded.created_at
        """,
        params,
    )
    return len(rows)


def upsert_options_225_data(executemany: ExecuteMany, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    params = [
        (
            row.get("code"),
            row.get("date"),
            row.get("whole_day_open"),
            row.get("whole_day_high"),
            row.get("whole_day_low"),
            row.get("whole_day_close"),
            row.get("night_session_open"),
            row.get("night_session_high"),
            row.get("night_session_low"),
            row.get("night_session_close"),
            row.get("day_session_open"),
            row.get("day_session_high"),
            row.get("day_session_low"),
            row.get("day_session_close"),
            row.get("volume"),
            row.get("open_interest"),
            row.get("turnover_value"),
            row.get("contract_month"),
            row.get("strike_price"),
            row.get("only_auction_volume"),
            row.get("emergency_margin_trigger_division"),
            row.get("put_call_division"),
            row.get("last_trading_day"),
            row.get("special_quotation_day"),
            row.get("settlement_price"),
            row.get("theoretical_price"),
            row.get("base_volatility"),
            row.get("underlying_price"),
            row.get("implied_volatility"),
            row.get("interest_rate"),
            row.get("created_at"),
        )
        for row in rows
    ]
    executemany(
        """
        INSERT INTO options_225_data (
            code, date, whole_day_open, whole_day_high, whole_day_low, whole_day_close,
            night_session_open, night_session_high, night_session_low, night_session_close,
            day_session_open, day_session_high, day_session_low, day_session_close,
            volume, open_interest, turnover_value, contract_month, strike_price,
            only_auction_volume, emergency_margin_trigger_division, put_call_division,
            last_trading_day, special_quotation_day, settlement_price, theoretical_price,
            base_volatility, underlying_price, implied_volatility, interest_rate, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (code, date) DO UPDATE
        SET whole_day_open = excluded.whole_day_open,
            whole_day_high = excluded.whole_day_high,
            whole_day_low = excluded.whole_day_low,
            whole_day_close = excluded.whole_day_close,
            night_session_open = excluded.night_session_open,
            night_session_high = excluded.night_session_high,
            night_session_low = excluded.night_session_low,
            night_session_close = excluded.night_session_close,
            day_session_open = excluded.day_session_open,
            day_session_high = excluded.day_session_high,
            day_session_low = excluded.day_session_low,
            day_session_close = excluded.day_session_close,
            volume = excluded.volume,
            open_interest = excluded.open_interest,
            turnover_value = excluded.turnover_value,
            contract_month = excluded.contract_month,
            strike_price = excluded.strike_price,
            only_auction_volume = excluded.only_auction_volume,
            emergency_margin_trigger_division = excluded.emergency_margin_trigger_division,
            put_call_division = excluded.put_call_division,
            last_trading_day = excluded.last_trading_day,
            special_quotation_day = excluded.special_quotation_day,
            settlement_price = excluded.settlement_price,
            theoretical_price = excluded.theoretical_price,
            base_volatility = excluded.base_volatility,
            underlying_price = excluded.underlying_price,
            implied_volatility = excluded.implied_volatility,
            interest_rate = excluded.interest_rate,
            created_at = excluded.created_at
        """,
        params,
    )
    return len(rows)


def upsert_margin_data(executemany: ExecuteMany, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    params = [
        (
            row.get("code"),
            row.get("date"),
            row.get("long_margin_volume"),
            row.get("short_margin_volume"),
        )
        for row in rows
    ]
    executemany(
        """
        INSERT INTO margin_data (code, date, long_margin_volume, short_margin_volume)
        VALUES (?, ?, ?, ?)
        ON CONFLICT (code, date) DO UPDATE
        SET long_margin_volume = excluded.long_margin_volume,
            short_margin_volume = excluded.short_margin_volume
        """,
        params,
    )
    return len(rows)
