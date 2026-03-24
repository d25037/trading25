"""Index open-gap regime signal for same-day availability-aware strategies."""

from typing import Literal, cast

import pandas as pd

from src.shared.models.signals import normalize_bool_series

IndexOpenGapRegime = Literal[
    "down_large",
    "down_medium",
    "flat",
    "up_medium",
    "up_large",
]


def index_open_gap_regime_signal(
    index_data: pd.DataFrame,
    gap_threshold_1_pct: float = 1.0,
    gap_threshold_2_pct: float = 2.0,
    regime: IndexOpenGapRegime = "down_medium",
) -> pd.Series:
    """Return a boolean signal for the current-session benchmark open-gap regime."""
    if index_data is None or index_data.empty:
        raise ValueError("index_data が空またはNoneです")

    required_columns = {"Open", "Close"}
    missing_columns = required_columns - set(index_data.columns)
    if missing_columns:
        raise ValueError("index_data に 'Open' と 'Close' カラムが必要です")

    open_prices = index_data["Open"]
    prev_close = index_data["Close"].shift(1)
    valid = open_prices.notna() & prev_close.notna() & prev_close.ne(0)
    gap_pct = ((open_prices - prev_close) / prev_close) * 100.0

    if gap_pct.dtype == object:
        gap_pct = pd.to_numeric(gap_pct, errors="coerce")
    gap_pct = gap_pct.round(4)

    if regime == "down_large":
        signal = valid & (gap_pct <= -gap_threshold_2_pct)
    elif regime == "down_medium":
        signal = (
            valid
            & (gap_pct > -gap_threshold_2_pct)
            & (gap_pct <= -gap_threshold_1_pct)
        )
    elif regime == "flat":
        signal = (
            valid
            & (gap_pct > -gap_threshold_1_pct)
            & (gap_pct < gap_threshold_1_pct)
        )
    elif regime == "up_medium":
        signal = (
            valid
            & (gap_pct >= gap_threshold_1_pct)
            & (gap_pct < gap_threshold_2_pct)
        )
    elif regime == "up_large":
        signal = valid & (gap_pct >= gap_threshold_2_pct)
    else:
        raise ValueError(f"regime が不正です: {regime}")

    return cast(pd.Series, normalize_bool_series(signal))
