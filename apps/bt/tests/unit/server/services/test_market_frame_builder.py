from __future__ import annotations

import pandas as pd

from src.shared.utils.market_frames import (
    rows_to_ohlc_frame,
    rows_to_ohlcv_frame,
)


def test_rows_to_ohlcv_frame_builds_sorted_datetime_index_frame() -> None:
    frame = rows_to_ohlcv_frame(
        [
            {
                "date": "2026-01-02",
                "open": 101.0,
                "high": 105.0,
                "low": 100.0,
                "close": 104.0,
                "volume": 2000,
            },
            {
                "date": "2026-01-01",
                "open": 100.0,
                "high": 103.0,
                "low": 99.0,
                "close": 102.0,
                "volume": 1000,
            },
        ]
    )

    assert list(frame.columns) == ["Open", "High", "Low", "Close", "Volume"]
    assert frame.index.tolist() == [
        pd.Timestamp("2026-01-01"),
        pd.Timestamp("2026-01-02"),
    ]
    assert frame.loc[pd.Timestamp("2026-01-02"), "Close"] == 104.0


def test_rows_to_ohlc_frame_excludes_volume_and_returns_empty_for_no_rows() -> None:
    frame = rows_to_ohlc_frame(
        [
            {
                "date": "2026-01-01",
                "open": 100.0,
                "high": 103.0,
                "low": 99.0,
                "close": 102.0,
                "volume": 1000,
            }
        ]
    )

    assert list(frame.columns) == ["Open", "High", "Low", "Close"]
    assert rows_to_ohlc_frame([]).empty
