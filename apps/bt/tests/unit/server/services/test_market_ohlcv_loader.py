"""market_ohlcv_loader tests."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from src.infrastructure.db.market.market_reader import MarketDbReader
from src.application.services.market_ohlcv_loader import (
    _rows_to_dataframe,
    load_stock_ohlcv_df,
    load_topix_df,
    stock_exists_in_reader,
)


def test_rows_to_dataframe_empty() -> None:
    """空配列は空DataFrameになる。"""
    df = _rows_to_dataframe([])
    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_load_stock_ohlcv_df_success(market_db_path: str) -> None:
    reader = MarketDbReader(market_db_path)
    try:
        df = load_stock_ohlcv_df(reader, "7203")
        assert list(df.columns) == ["Open", "High", "Low", "Close", "Volume"]
        assert isinstance(df.index, pd.DatetimeIndex)
        assert len(df) == 3
    finally:
        reader.close()


def test_load_stock_ohlcv_df_with_date_filters(market_db_path: str) -> None:
    reader = MarketDbReader(market_db_path)
    try:
        df = load_stock_ohlcv_df(
            reader,
            "72030",
            start_date="2024-01-16",
            end_date="2024-01-16",
        )
        assert len(df) == 1
        assert float(df.iloc[0]["Close"]) == 2515.0
    finally:
        reader.close()


def test_load_stock_ohlcv_df_not_found(market_db_path: str) -> None:
    reader = MarketDbReader(market_db_path)
    try:
        df = load_stock_ohlcv_df(reader, "0000")
        assert df.empty
    finally:
        reader.close()


def test_load_stock_ohlcv_df_out_of_range_returns_empty(market_db_path: str) -> None:
    reader = MarketDbReader(market_db_path)
    try:
        df = load_stock_ohlcv_df(
            reader,
            "7203",
            start_date="2030-01-01",
            end_date="2030-12-31",
        )
        assert df.empty
    finally:
        reader.close()


def test_load_stock_ohlcv_df_handles_mixed_code_formats(tmp_path: Path) -> None:
    db_path = str(tmp_path / "mixed-stock-codes.duckdb")
    conn = duckdb.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE stock_data (
                code TEXT NOT NULL,
                date TEXT NOT NULL,
                open DOUBLE NOT NULL,
                high DOUBLE NOT NULL,
                low DOUBLE NOT NULL,
                close DOUBLE NOT NULL,
                volume BIGINT NOT NULL,
                adjustment_factor DOUBLE,
                created_at TEXT,
                PRIMARY KEY (code, date)
            )
            """
        )
        conn.execute(
            "INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("72030", "2024-01-15", 2500.0, 2520.0, 2490.0, 2510.0, 1_000_000, 1.0, None),
        )
        conn.execute(
            "INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("72030", "2024-01-16", 2510.0, 2530.0, 2500.0, 2512.0, 1_100_000, 1.0, None),
        )
        # 同日重複（4桁/5桁混在）時は4桁を優先する。
        conn.execute(
            "INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("7203", "2024-01-16", 2512.0, 2535.0, 2505.0, 2515.0, 1_200_000, 1.0, None),
        )
    finally:
        conn.close()

    reader = MarketDbReader(db_path)
    try:
        df = load_stock_ohlcv_df(reader, "7203")
        assert len(df) == 2
        assert float(df.loc[pd.Timestamp("2024-01-15"), "Close"]) == 2510.0
        assert float(df.loc[pd.Timestamp("2024-01-16"), "Close"]) == 2515.0
    finally:
        reader.close()


def test_load_topix_df_success(market_db_path: str) -> None:
    reader = MarketDbReader(market_db_path)
    try:
        df = load_topix_df(reader)
        assert list(df.columns) == ["Open", "High", "Low", "Close"]
        assert isinstance(df.index, pd.DatetimeIndex)
        assert len(df) == 3
    finally:
        reader.close()


def test_load_topix_df_with_date_filters(market_db_path: str) -> None:
    reader = MarketDbReader(market_db_path)
    try:
        df = load_topix_df(
            reader,
            start_date="2024-01-16",
            end_date="2024-01-16",
        )
        assert len(df) == 1
        assert float(df.iloc[0]["Close"]) == 2510.0
    finally:
        reader.close()


def test_load_topix_df_out_of_range_returns_empty(market_db_path: str) -> None:
    reader = MarketDbReader(market_db_path)
    try:
        df = load_topix_df(
            reader,
            start_date="2030-01-01",
            end_date="2030-12-31",
        )
        assert df.empty
    finally:
        reader.close()


def test_stock_exists_in_reader_returns_true(market_db_path: str) -> None:
    reader = MarketDbReader(market_db_path)
    try:
        assert stock_exists_in_reader(reader, "7203") is True
    finally:
        reader.close()


def test_stock_exists_in_reader_returns_false(market_db_path: str) -> None:
    reader = MarketDbReader(market_db_path)
    try:
        assert stock_exists_in_reader(reader, "0000") is False
    finally:
        reader.close()
