"""Trading-value freshness signals."""

from __future__ import annotations

import pandas as pd
from loguru import logger

from src.shared.models.signals import normalize_bool_series


def _trading_value_ema_ratio_signal(
    close: pd.Series,
    volume: pd.Series,
    *,
    ratio_threshold: float,
    ema_period: int,
    baseline_period: int,
    direction: str,
) -> pd.Series:
    """EMA(売買代金) と ADV の関係から freshness/stale-volume 条件を判定する。"""
    from src.domains.strategy.indicators import (
        compute_moving_average,
        compute_trading_value_ma,
    )

    trading_value = close * volume / 1e8
    trading_value_ema = compute_moving_average(
        trading_value,
        ema_period,
        ma_type="ema",
    )
    adv = compute_trading_value_ma(close, volume, baseline_period)
    if direction == "above":
        result = normalize_bool_series(trading_value_ema >= adv * ratio_threshold)
    elif direction == "below":
        result = normalize_bool_series(trading_value_ema < adv * ratio_threshold)
    else:
        raise ValueError(f"Unsupported direction: {direction}")

    return result


def trading_value_ema_ratio_above_signal(
    close: pd.Series,
    volume: pd.Series,
    ratio_threshold: float = 1.0,
    ema_period: int = 3,
    baseline_period: int = 20,
) -> pd.Series:
    """EMA(売買代金) が ADV を上回る freshness 条件を判定する。"""
    logger.debug(
        "売買代金EMA比率上側: ratio_threshold={}, ema_period={}, baseline_period={}",
        ratio_threshold,
        ema_period,
        baseline_period,
    )
    result = _trading_value_ema_ratio_signal(
        close,
        volume,
        ratio_threshold=ratio_threshold,
        ema_period=ema_period,
        baseline_period=baseline_period,
        direction="above",
    )

    logger.debug(
        "売買代金EMA比率上側: 完了 (True: {}/{})",
        int(result.sum()),
        len(result),
    )
    return result


def trading_value_ema_ratio_below_signal(
    close: pd.Series,
    volume: pd.Series,
    ratio_threshold: float = 0.9,
    ema_period: int = 3,
    baseline_period: int = 20,
) -> pd.Series:
    """EMA(売買代金) が ADV を下回る stale-volume 条件を判定する。"""
    logger.debug(
        "売買代金EMA比率下側: ratio_threshold={}, ema_period={}, baseline_period={}",
        ratio_threshold,
        ema_period,
        baseline_period,
    )
    result = _trading_value_ema_ratio_signal(
        close,
        volume,
        ratio_threshold=ratio_threshold,
        ema_period=ema_period,
        baseline_period=baseline_period,
        direction="below",
    )

    logger.debug(
        "売買代金EMA比率下側: 完了 (True: {}/{})",
        int(result.sum()),
        len(result),
    )
    return result
