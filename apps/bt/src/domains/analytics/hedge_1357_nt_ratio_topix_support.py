from __future__ import annotations

from collections.abc import Sequence
from typing import Literal

import pandas as pd

from src.domains.analytics.nt_ratio_change_stock_overnight_distribution import (
    NT_RATIO_BUCKET_ORDER,
    NtRatioBucketKey,
    NtRatioReturnStats,
    format_nt_ratio_bucket_label,
)
from src.domains.analytics.topix_close_stock_overnight_distribution import (
    CLOSE_BUCKET_ORDER,
    STOCK_GROUP_ORDER as _STOCK_GROUP_ORDER,
    CloseBucketKey,
    SourceMode,
    StockGroup,
    TopixCloseReturnStats,
    _date_where_clause,
    _normalize_code_sql,
    _open_analysis_connection,
    _validate_selected_groups,
    format_close_bucket_label,
)

TargetName = Literal[
    "next_overnight",
    "next_intraday",
    "next_close_to_close",
    "forward_3d_close_to_close",
    "forward_5d_close_to_close",
]
RuleName = Literal[
    "shock_topix_le_negative_threshold_2",
    "shock_joint_adverse",
    "trend_ma_bearish",
    "trend_macd_negative",
    "hybrid_bearish_joint",
]
SplitName = Literal["overall", "discovery", "validation"]

TARGET_ORDER: tuple[TargetName, ...] = (
    "next_overnight",
    "next_intraday",
    "next_close_to_close",
    "forward_3d_close_to_close",
    "forward_5d_close_to_close",
)
RULE_ORDER: tuple[RuleName, ...] = (
    "shock_topix_le_negative_threshold_2",
    "shock_joint_adverse",
    "trend_ma_bearish",
    "trend_macd_negative",
    "hybrid_bearish_joint",
)
SPLIT_ORDER: tuple[SplitName, ...] = ("overall", "discovery", "validation")
STOCK_GROUP_ORDER: tuple[StockGroup, ...] = _STOCK_GROUP_ORDER

_NIKKEI_SYNTHETIC_INDEX_CODE = "N225_UNDERPX"
_ETF_CODE = "1357"
_DISCOVERY_END_DATE = "2021-12-31"
_VALIDATION_START_DATE = "2022-01-01"
_BETA_LOOKBACK_DAYS = 60
_BETA_MIN_PERIODS = 20
_MACD_FAST_PERIOD = 12
_MACD_SLOW_PERIOD = 26
_MACD_SIGNAL_PERIOD = 9
_MACD_BASIS = "ema_adjust_false"

_TARGET_COLUMN_MAP: dict[TargetName, tuple[str, str]] = {
    "next_overnight": (
        "long_next_overnight_return",
        "etf_next_overnight_return",
    ),
    "next_intraday": (
        "long_next_intraday_return",
        "etf_next_intraday_return",
    ),
    "next_close_to_close": (
        "long_next_close_to_close_return",
        "etf_next_close_to_close_return",
    ),
    "forward_3d_close_to_close": (
        "long_forward_3d_close_to_close_return",
        "etf_forward_3d_close_to_close_return",
    ),
    "forward_5d_close_to_close": (
        "long_forward_5d_close_to_close_return",
        "etf_forward_5d_close_to_close_return",
    ),
}


def _filter_date_range_df(
    df: pd.DataFrame,
    *,
    start_date: str | None,
    end_date: str | None,
) -> pd.DataFrame:
    filtered = df.copy()
    if start_date:
        filtered = filtered[filtered["date"] >= start_date]
    if end_date:
        filtered = filtered[filtered["date"] <= end_date]
    return filtered.reset_index(drop=True)


def _validate_fixed_weights(fixed_weights: Sequence[float]) -> tuple[float, ...]:
    validated: list[float] = []
    for raw_value in fixed_weights:
        value = float(raw_value)
        if value <= 0:
            raise ValueError("fixed_weights must contain positive values")
        validated.append(value)
    if not validated:
        raise ValueError("fixed_weights must contain at least one positive value")
    return tuple(validated)


__all__ = [
    "CLOSE_BUCKET_ORDER",
    "NT_RATIO_BUCKET_ORDER",
    "RULE_ORDER",
    "SPLIT_ORDER",
    "STOCK_GROUP_ORDER",
    "TARGET_ORDER",
    "CloseBucketKey",
    "NtRatioBucketKey",
    "NtRatioReturnStats",
    "RuleName",
    "SourceMode",
    "SplitName",
    "StockGroup",
    "TargetName",
    "TopixCloseReturnStats",
    "_BETA_LOOKBACK_DAYS",
    "_BETA_MIN_PERIODS",
    "_DISCOVERY_END_DATE",
    "_ETF_CODE",
    "_MACD_BASIS",
    "_MACD_FAST_PERIOD",
    "_MACD_SIGNAL_PERIOD",
    "_MACD_SLOW_PERIOD",
    "_NIKKEI_SYNTHETIC_INDEX_CODE",
    "_TARGET_COLUMN_MAP",
    "_VALIDATION_START_DATE",
    "_date_where_clause",
    "_filter_date_range_df",
    "_normalize_code_sql",
    "_open_analysis_connection",
    "_validate_fixed_weights",
    "_validate_selected_groups",
    "format_close_bucket_label",
    "format_nt_ratio_bucket_label",
]
