"""Market OHLCV Loader

market.db から OHLCV/TOPIX を DataFrame として読み込む共通ローダ。
Indicator/Signal で source=market を扱う際の重複実装を避ける。
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from src.lib.market_db.market_reader import MarketDbReader
from src.lib.market_db.query_helpers import stock_code_candidates


def _rows_to_dataframe(rows: list[Any]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame([dict(row) for row in rows])


def load_stock_ohlcv_df(
    reader: MarketDbReader,
    stock_code: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """market.db から銘柄 OHLCV を DataFrame で取得する。"""
    candidates = stock_code_candidates(stock_code)
    placeholders = ",".join("?" for _ in candidates)

    row = reader.query_one(
        f"SELECT code FROM stocks WHERE code IN ({placeholders}) "
        "ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END LIMIT 1",
        tuple(candidates),
    )
    if row is None:
        return pd.DataFrame()

    db_code = row["code"]
    sql = "SELECT date, open, high, low, close, volume FROM stock_data WHERE code = ?"
    params: list[str] = [db_code]

    if start_date:
        sql += " AND date >= ?"
        params.append(start_date)
    if end_date:
        sql += " AND date <= ?"
        params.append(end_date)

    sql += " ORDER BY date"
    rows = reader.query(sql, tuple(params))
    if not rows:
        return pd.DataFrame()

    df = _rows_to_dataframe(rows)
    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)
    df = df[["open", "high", "low", "close", "volume"]]
    df.columns = pd.Index(["Open", "High", "Low", "Close", "Volume"])
    return df


def load_topix_df(
    reader: MarketDbReader,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """market.db から TOPIX OHLC を DataFrame で取得する。"""
    sql = "SELECT date, open, high, low, close FROM topix_data"
    params: list[str] = []
    conditions: list[str] = []

    if start_date:
        conditions.append("date >= ?")
        params.append(start_date)
    if end_date:
        conditions.append("date <= ?")
        params.append(end_date)

    if conditions:
        sql += " WHERE " + " AND ".join(conditions)
    sql += " ORDER BY date"

    rows = reader.query(sql, tuple(params))
    if not rows:
        return pd.DataFrame()

    df = _rows_to_dataframe(rows)
    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)
    df = df[["open", "high", "low", "close"]]
    df.columns = pd.Index(["Open", "High", "Low", "Close"])
    return df
