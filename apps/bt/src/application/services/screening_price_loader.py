"""Daily price loading helpers for market screening."""

from __future__ import annotations

from typing import Any

import pandas as pd

from src.infrastructure.db.market.market_reader import MarketDbQueryable
from src.infrastructure.db.market.query_helpers import normalize_stock_code, stock_code_query_candidates


def normalize_codes(stock_codes: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for stock_code in stock_codes:
        code = normalize_stock_code(stock_code)
        if not code or code in seen:
            continue
        seen.add(code)
        deduped.append(code)
    return deduped


def load_daily_by_code(
    reader: MarketDbQueryable,
    stock_codes: list[str],
    *,
    start_date: str | None,
    end_date: str | None,
) -> dict[str, pd.DataFrame]:
    query_codes = stock_code_query_candidates(stock_codes)
    placeholders = ",".join("?" for _ in query_codes)
    sql = f"""
        SELECT code, date, open, high, low, close, volume
        FROM stock_data
        WHERE code IN ({placeholders})
    """
    params: list[str] = list(query_codes)
    conds: list[str] = []

    if start_date:
        conds.append("date >= ?")
        params.append(start_date)
    if end_date:
        conds.append("date <= ?")
        params.append(end_date)
    if conds:
        sql += " AND " + " AND ".join(conds)
    sql += " ORDER BY code, date"

    rows = reader.query(sql, tuple(params))

    grouped: dict[str, list[Any]] = {}
    for row in rows:
        grouped.setdefault(normalize_stock_code(str(row["code"])), []).append(row)

    return {
        code: rows_to_ohlcv_df(grouped.get(code, []))
        for code in stock_codes
    }


def rows_to_ohlcv_df(rows: list[Any]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(
        [
            {
                "date": row["date"],
                "Open": row["open"],
                "High": row["high"],
                "Low": row["low"],
                "Close": row["close"],
                "Volume": row["volume"],
            }
            for row in rows
        ]
    )
    df["date"] = pd.to_datetime(df["date"])
    return df.set_index("date").sort_index()


def rows_to_ohlc_df(rows: list[Any]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(
        [
            {
                "date": row["date"],
                "Open": row["open"],
                "High": row["high"],
                "Low": row["low"],
                "Close": row["close"],
            }
            for row in rows
        ]
    )
    df["date"] = pd.to_datetime(df["date"])
    return df.set_index("date").sort_index()
