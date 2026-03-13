"""Market OHLCV Loader

DuckDB market time-series から OHLCV/TOPIX を DataFrame として読み込む共通ローダ。
Indicator/Signal で source=market を扱う際の重複実装を避ける。
"""

from __future__ import annotations

from typing import Any, Protocol

import pandas as pd

from src.infrastructure.db.market.market_reader import MarketDbReader
from src.infrastructure.db.market.query_helpers import stock_code_candidates


class MarketReaderLookup(Protocol):
    def query_one(self, sql: str, params: tuple[str, ...] = ()) -> Any: ...


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
    """DuckDB から銘柄 OHLCV を DataFrame で取得する。"""
    candidates = stock_code_candidates(stock_code)
    if not candidates:
        return pd.DataFrame()

    placeholders = ",".join("?" for _ in candidates)
    where_conditions = [f"code IN ({placeholders})"]
    params: list[str] = list(candidates)
    if start_date:
        where_conditions.append("date >= ?")
        params.append(start_date)
    if end_date:
        where_conditions.append("date <= ?")
        params.append(end_date)

    sql = f"""
        WITH ranked AS (
            SELECT
                date,
                open,
                high,
                low,
                close,
                volume,
                ROW_NUMBER() OVER (
                    PARTITION BY date
                    ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END
                ) AS rn
            FROM stock_data
            WHERE {" AND ".join(where_conditions)}
        )
        SELECT date, open, high, low, close, volume
        FROM ranked
        WHERE rn = 1
        ORDER BY date
    """
    rows = reader.query(sql, tuple(params))
    if not rows:
        return pd.DataFrame()

    df = _rows_to_dataframe(rows)
    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)
    df = df[["open", "high", "low", "close", "volume"]]
    df.columns = pd.Index(["Open", "High", "Low", "Close", "Volume"])
    return df


def stock_exists_in_reader(reader: MarketReaderLookup, stock_code: str) -> bool:
    """Return whether the snapshot contains the given code in stocks or stock_data."""
    candidates = stock_code_candidates(stock_code)
    if not candidates:
        return False

    placeholders = ",".join("?" for _ in candidates)
    for table in ("stocks", "stock_data"):
        row = reader.query_one(
            f"SELECT code FROM {table} WHERE code IN ({placeholders}) "
            "ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END LIMIT 1",
            tuple(candidates),
        )
        if row is not None:
            return True

    return False


def load_topix_df(
    reader: MarketDbReader,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """DuckDB から TOPIX OHLC を DataFrame で取得する。"""
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
