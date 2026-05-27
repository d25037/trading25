"""Margin data loading helpers for market screening."""

from __future__ import annotations

from typing import Any

import pandas as pd

from src.infrastructure.data_access.loaders.margin_loaders import transform_margin_df
from src.infrastructure.db.market.market_reader import MarketDbQueryable
from src.infrastructure.db.market.query_helpers import normalize_stock_code, stock_code_query_candidates


def attach_margin(
    reader: MarketDbQueryable,
    result: dict[str, dict[str, pd.DataFrame]],
    daily_index_by_code: dict[str, pd.DatetimeIndex],
    *,
    start_date: str | None,
    end_date: str | None,
) -> list[str]:
    warnings: list[str] = []
    codes = list(daily_index_by_code.keys())
    if not codes:
        return warnings

    try:
        rows = query_margin_rows(reader, codes, start_date, end_date)
    except Exception as e:  # noqa: BLE001 - backend error path
        if _is_missing_table_error(e):
            warnings.append("market margin_data table is missing; margin signals may be skipped")
            return warnings
        raise

    margin_map = group_margin_rows(rows)
    for code, daily_index in daily_index_by_code.items():
        margin_df = margin_map.get(code)
        if margin_df is None or margin_df.empty:
            continue
        try:
            result.setdefault(code, {})["margin_daily"] = (
                transform_margin_df(margin_df)
                .reindex(daily_index)
                .ffill()
                .fillna(0)
            )
        except Exception as e:  # noqa: BLE001 - screening should continue
            warnings.append(f"{code} margin transform failed ({e})")

    return warnings


def _is_missing_table_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return "no such table" in message or ("does not exist" in message and "table" in message)


def query_margin_rows(
    reader: MarketDbQueryable,
    stock_codes: list[str],
    start_date: str | None,
    end_date: str | None,
) -> list[Any]:
    query_codes = stock_code_query_candidates(stock_codes)
    placeholders = ",".join("?" for _ in query_codes)
    sql = f"""
        SELECT
            code,
            date,
            long_margin_volume,
            short_margin_volume
        FROM margin_data
        WHERE code IN ({placeholders})
    """
    params: list[Any] = list(query_codes)

    if start_date:
        sql += " AND date >= ?"
        params.append(start_date)
    if end_date:
        sql += " AND date <= ?"
        params.append(end_date)

    sql += " ORDER BY code, date"
    return reader.query(sql, tuple(params))


def group_margin_rows(rows: list[Any]) -> dict[str, pd.DataFrame]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        code = normalize_stock_code(str(row["code"]))
        grouped.setdefault(code, []).append(
            {
                "date": row["date"],
                "longMarginVolume": row["long_margin_volume"],
                "shortMarginVolume": row["short_margin_volume"],
            }
        )

    result: dict[str, pd.DataFrame] = {}
    for code, records in grouped.items():
        if not records:
            continue
        df = pd.DataFrame(records)
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date").sort_index()
        result[code] = df
    return result
