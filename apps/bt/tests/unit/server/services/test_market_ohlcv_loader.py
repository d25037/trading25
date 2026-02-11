"""market_ohlcv_loader tests."""

from __future__ import annotations

import pandas as pd

from src.lib.market_db.market_reader import MarketDbReader
from src.server.services.market_ohlcv_loader import (
    _rows_to_dataframe,
    load_stock_ohlcv_df,
    load_topix_df,
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
