"""
Baseline-based signal implementations.

This module centralizes three distinct concepts that were previously blurred:
- deviation: distance from a baseline
- position: which side of a baseline price is on
- cross: event where price crosses a baseline
"""

from __future__ import annotations

import pandas as pd
from loguru import logger

from src.domains.strategy.indicators import (
    compute_moving_average,
    compute_volume_weighted_ema,
)


def _compute_baseline(
    ohlc_data: pd.DataFrame,
    baseline_type: str,
    baseline_period: int,
) -> pd.Series:
    if "Close" not in ohlc_data.columns:
        raise ValueError("ohlc_data に 'Close' カラムが必要です")

    close = ohlc_data["Close"]
    if baseline_type == "sma":
        return compute_moving_average(close, baseline_period, ma_type="sma")
    if baseline_type == "ema":
        return compute_moving_average(close, baseline_period, ma_type="ema")
    if baseline_type == "vwema":
        if "Volume" not in ohlc_data.columns:
            raise ValueError(
                "baseline_type='vwema' では ohlc_data に 'Volume' カラムが必要です"
            )
        return compute_volume_weighted_ema(close, ohlc_data["Volume"], baseline_period)
    raise ValueError(f"未対応のベースラインタイプ: {baseline_type} (sma/ema/vwemaのみ)")


def _select_price(ohlc_data: pd.DataFrame, price_column: str) -> pd.Series:
    column_map = {
        "close": "Close",
        "high": "High",
        "low": "Low",
    }
    try:
        column_name = column_map[price_column]
    except KeyError as exc:
        raise ValueError(
            f"不正なprice_column: {price_column} (close/high/lowのみ)"
        ) from exc

    if column_name not in ohlc_data.columns:
        raise ValueError(f"ohlc_data に '{column_name}' カラムが必要です")
    return ohlc_data[column_name]


def deviation_signal(
    price: pd.Series,
    baseline: pd.Series,
    threshold: float,
    direction: str = "below",
) -> pd.Series:
    logger.debug(
        f"基準線乖離シグナル: 処理開始 (閾値={threshold:.1%}, 方向={direction}, データ長={len(price)})"
    )

    deviation_ratio = (price - baseline) / baseline

    if direction == "below":
        signal = (deviation_ratio <= -threshold) & baseline.notna() & price.notna()
    elif direction == "above":
        signal = (deviation_ratio >= threshold) & baseline.notna() & price.notna()
    else:
        raise ValueError(f"不正なdirection: {direction} (below/aboveのみ)")

    result = signal.fillna(False)
    logger.debug(
        f"基準線乖離シグナル: 処理完了 (閾値={threshold:.1%}, 方向={direction}, True: {result.sum()}/{len(result)})"
    )
    return result


def position_signal(
    price: pd.Series,
    baseline: pd.Series,
    direction: str = "above",
) -> pd.Series:
    logger.debug(f"基準線位置シグナル: 処理開始 (方向={direction}, データ長={len(price)})")

    if direction == "above":
        signal = (price > baseline) & baseline.notna() & price.notna()
    elif direction == "below":
        signal = (price < baseline) & baseline.notna() & price.notna()
    else:
        raise ValueError(f"不正なdirection: {direction} (above/belowのみ)")

    result = signal.fillna(False)
    logger.debug(
        f"基準線位置シグナル: 処理完了 (方向={direction}, True: {result.sum()}/{len(result)})"
    )
    return result


def cross_signal(
    price: pd.Series,
    baseline: pd.Series,
    direction: str = "above",
    lookback_days: int = 1,
) -> pd.Series:
    logger.debug(
        f"基準線クロスシグナル: 処理開始 (方向={direction}, lookback={lookback_days}, データ長={len(price)})"
    )

    if direction == "above":
        signal = (price > baseline) & (price.shift(1) <= baseline.shift(1))
    elif direction == "below":
        signal = (price < baseline) & (price.shift(1) >= baseline.shift(1))
    else:
        raise ValueError(f"不正なdirection: {direction} (above/belowのみ)")

    result = signal.fillna(False)
    if lookback_days > 1:
        result = (result.astype(int).rolling(lookback_days).max() >= 1).fillna(False)

    logger.debug(
        f"基準線クロスシグナル: 処理完了 (方向={direction}, True: {result.sum()}/{len(result)})"
    )
    return result


def baseline_deviation_signal(
    ohlc_data: pd.DataFrame,
    baseline_type: str,
    baseline_period: int,
    deviation_threshold: float,
    direction: str = "below",
) -> pd.Series:
    logger.debug(
        f"基準線乖離: ベースライン={baseline_type}({baseline_period}), 閾値={deviation_threshold:.1%}, 方向={direction}"
    )

    close = ohlc_data["Close"]
    baseline = _compute_baseline(ohlc_data, baseline_type, baseline_period)
    return deviation_signal(close, baseline, deviation_threshold, direction)


def baseline_position_signal(
    ohlc_data: pd.DataFrame,
    baseline_type: str,
    baseline_period: int,
    direction: str = "above",
    price_column: str = "close",
) -> pd.Series:
    logger.debug(
        f"基準線位置: ベースライン={baseline_type}({baseline_period}), 方向={direction}, 価格={price_column}"
    )

    baseline = _compute_baseline(ohlc_data, baseline_type, baseline_period)
    price = _select_price(ohlc_data, price_column)
    return position_signal(price, baseline, direction)


def baseline_cross_signal(
    ohlc_data: pd.DataFrame,
    baseline_type: str,
    baseline_period: int,
    direction: str = "above",
    lookback_days: int = 1,
    price_column: str = "close",
) -> pd.Series:
    logger.debug(
        f"基準線クロス: ベースライン={baseline_type}({baseline_period}), 方向={direction}, lookback={lookback_days}, 価格={price_column}"
    )

    baseline = _compute_baseline(ohlc_data, baseline_type, baseline_period)
    price = _select_price(ohlc_data, price_column)
    return cross_signal(price, baseline, direction, lookback_days)
