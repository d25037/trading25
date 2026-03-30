from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.domains.strategy.signals.universe_rank_bucket import (
    build_universe_rank_bucket_feature_panel,
    universe_rank_bucket_signal,
)


def _build_universe(periods: int = 100) -> tuple[pd.DatetimeIndex, dict[str, dict[str, pd.DataFrame]]]:
    dates = pd.date_range("2025-01-01", periods=periods, freq="D")
    universe: dict[str, dict[str, pd.DataFrame]] = {}

    for idx in range(20):
        code = str(1001 + idx)
        close_growth = 0.004 - idx * 0.00015
        volume_growth = 0.006 - idx * 0.0002
        close = 100.0 * np.power(1.0 + close_growth, np.arange(periods))
        volume = 1000.0 * np.power(1.0 + volume_growth, np.arange(periods))
        universe[code] = {
            "daily": pd.DataFrame(
                {
                    "Close": close,
                    "Volume": volume,
                },
                index=dates,
            )
        }

    return dates, universe


def test_universe_rank_bucket_signal_matches_target_bucket_and_volume_split() -> None:
    dates, universe = _build_universe()

    q1_high = universe_rank_bucket_signal(
        stock_code="1001",
        target_index=dates,
        universe_multi_data=universe,
        universe_member_codes=list(universe.keys()),
        price_bucket="q1",
        volume_bucket="high",
        min_constituents=20,
    )
    q1_low = universe_rank_bucket_signal(
        stock_code="1002",
        target_index=dates,
        universe_multi_data=universe,
        universe_member_codes=list(universe.keys()),
        price_bucket="q1",
        volume_bucket="low",
        min_constituents=20,
    )
    q456_high = universe_rank_bucket_signal(
        stock_code="1007",
        target_index=dates,
        universe_multi_data=universe,
        universe_member_codes=list(universe.keys()),
        price_bucket="q456",
        volume_bucket="high",
        min_constituents=20,
    )
    q10_low = universe_rank_bucket_signal(
        stock_code="1020",
        target_index=dates,
        universe_multi_data=universe,
        universe_member_codes=list(universe.keys()),
        price_bucket="q10",
        volume_bucket="low",
        min_constituents=20,
    )
    other_any = universe_rank_bucket_signal(
        stock_code="1003",
        target_index=dates,
        universe_multi_data=universe,
        universe_member_codes=list(universe.keys()),
        price_bucket="other",
        volume_bucket="any",
        min_constituents=20,
    )

    assert not q1_high.iloc[:79].any()
    assert bool(q1_high.iloc[-1]) is True
    assert bool(q1_low.iloc[-1]) is True
    assert bool(q456_high.iloc[-1]) is True
    assert bool(q10_low.iloc[-1]) is True
    assert bool(other_any.iloc[-1]) is True


def test_universe_rank_bucket_signal_returns_false_when_universe_is_too_small() -> None:
    dates, universe = _build_universe()
    small_universe = {code: universe[code] for code in list(universe)[:5]}

    signal = universe_rank_bucket_signal(
        stock_code="1001",
        target_index=dates,
        universe_multi_data=small_universe,
        universe_member_codes=list(small_universe.keys()),
        price_bucket="q1",
        min_constituents=10,
    )

    assert bool(signal.any()) is False


def test_build_universe_rank_bucket_feature_panel_returns_empty_for_invalid_payloads() -> None:
    dates = pd.date_range("2025-01-01", periods=3, freq="D")
    universe = {
        "1001": {},
        "1002": {
            "daily": pd.DataFrame({"Close": [1.0, 2.0, 3.0]}, index=dates),
        },
        "1003": {
            "daily": pd.DataFrame({"Volume": [100.0, 110.0, 120.0]}, index=dates),
        },
        "1004": "invalid",
    }

    feature_panel = build_universe_rank_bucket_feature_panel(
        universe_multi_data=universe,
        universe_member_codes=list(universe.keys()),
        price_sma_period=2,
        volume_short_period=2,
        volume_long_period=3,
    )

    assert feature_panel.empty


def test_build_universe_rank_bucket_feature_panel_returns_empty_when_features_never_mature() -> None:
    dates = pd.date_range("2025-01-01", periods=3, freq="D")
    universe = {
        "1001": {
            "daily": pd.DataFrame(
                {
                    "Close": [100.0, 101.0, 102.0],
                    "Volume": [1000.0, 1010.0, 1020.0],
                },
                index=dates,
            )
        }
    }

    feature_panel = build_universe_rank_bucket_feature_panel(
        universe_multi_data=universe,
        universe_member_codes=["1001"],
        price_sma_period=5,
        volume_short_period=5,
        volume_long_period=10,
    )

    assert feature_panel.empty


def test_universe_rank_bucket_signal_validates_arguments() -> None:
    dates, universe = _build_universe()

    with pytest.raises(ValueError, match="min_constituents must be >= 2"):
        universe_rank_bucket_signal(
            stock_code="1001",
            target_index=dates,
            universe_multi_data=universe,
            min_constituents=1,
        )

    with pytest.raises(ValueError, match="unsupported price_bucket"):
        universe_rank_bucket_signal(
            stock_code="1001",
            target_index=dates,
            universe_multi_data=universe,
            price_bucket="q2",
        )

    with pytest.raises(ValueError, match="unsupported volume_bucket"):
        universe_rank_bucket_signal(
            stock_code="1001",
            target_index=dates,
            universe_multi_data=universe,
            volume_bucket="mid",
        )


def test_universe_rank_bucket_signal_handles_empty_index_and_missing_cached_rows() -> None:
    dates, universe = _build_universe()

    empty_signal = universe_rank_bucket_signal(
        stock_code="1001",
        target_index=pd.DatetimeIndex([]),
        universe_multi_data=universe,
    )
    assert empty_signal.empty

    no_rows_signal = universe_rank_bucket_signal(
        stock_code="9999",
        target_index=dates,
        universe_multi_data=universe,
        feature_panel=pd.DataFrame(
            {
                "date": dates,
                "stock_code": ["1001"] * len(dates),
                "price_count": [20] * len(dates),
                "price_bucket": ["q1"] * len(dates),
                "volume_bucket": ["high"] * len(dates),
            }
        ),
    )
    assert bool(no_rows_signal.any()) is False


def test_universe_rank_bucket_signal_returns_false_when_cached_feature_panel_is_empty() -> None:
    dates, universe = _build_universe()

    signal = universe_rank_bucket_signal(
        stock_code="1001",
        target_index=dates,
        universe_multi_data=universe,
        feature_panel=pd.DataFrame(),
    )

    assert bool(signal.any()) is False
