"""
Volume ratio signal implementations.
"""

from __future__ import annotations

import pandas as pd
from loguru import logger


def _volume_ratio_signal(
    volume: pd.Series,
    ratio_threshold: float,
    short_period: int,
    long_period: int,
    ma_type: str,
    direction: str,
) -> pd.Series:
    from src.domains.strategy.indicators import compute_volume_mas

    volume_short_ma, volume_long_ma = compute_volume_mas(
        volume, short_period, long_period, ma_type
    )

    if direction == "above":
        result = (volume_short_ma > volume_long_ma * ratio_threshold).fillna(False)
    elif direction == "below":
        result = (volume_short_ma < volume_long_ma * ratio_threshold).fillna(False)
    else:
        raise ValueError(f"不正なdirection: {direction} (above/belowのみ)")
    return result


def volume_ratio_above_signal(
    volume: pd.Series,
    ratio_threshold: float = 1.5,
    short_period: int = 20,
    long_period: int = 100,
    ma_type: str = "sma",
) -> pd.Series:
    """短期出来高MAが長期出来高MAを上回る比率条件を判定する。"""
    logger.debug(
        "出来高比率上側: ma_type={}, ratio_threshold={}, short={}, long={}",
        ma_type,
        ratio_threshold,
        short_period,
        long_period,
    )
    result = _volume_ratio_signal(
        volume,
        ratio_threshold=ratio_threshold,
        short_period=short_period,
        long_period=long_period,
        ma_type=ma_type,
        direction="above",
    )
    logger.debug("出来高比率上側: 完了 (True: {}/{})", int(result.sum()), len(result))
    return result


def volume_ratio_below_signal(
    volume: pd.Series,
    ratio_threshold: float = 0.7,
    short_period: int = 20,
    long_period: int = 100,
    ma_type: str = "sma",
) -> pd.Series:
    """短期出来高MAが長期出来高MAを下回る比率条件を判定する。"""
    logger.debug(
        "出来高比率下側: ma_type={}, ratio_threshold={}, short={}, long={}",
        ma_type,
        ratio_threshold,
        short_period,
        long_period,
    )
    result = _volume_ratio_signal(
        volume,
        ratio_threshold=ratio_threshold,
        short_period=short_period,
        long_period=long_period,
        ma_type=ma_type,
        direction="below",
    )
    logger.debug("出来高比率下側: 完了 (True: {}/{})", int(result.sum()), len(result))
    return result


def volume_signal(
    volume: pd.Series,
    direction: str = "surge",
    threshold: float = 1.5,
    short_period: int = 20,
    long_period: int = 100,
    ma_type: str = "sma",
) -> pd.Series:
    """【レガシー】旧 direction 形式の出来高比率シグナル。"""
    logger.warning(
        "volume_signal はレガシーです。"
        "volume_ratio_above_signal / volume_ratio_below_signal に移行してください。"
    )

    if direction == "surge":
        return volume_ratio_above_signal(
            volume,
            ratio_threshold=threshold,
            short_period=short_period,
            long_period=long_period,
            ma_type=ma_type,
        )
    if direction == "drop":
        return volume_ratio_below_signal(
            volume,
            ratio_threshold=threshold,
            short_period=short_period,
            long_period=long_period,
            ma_type=ma_type,
        )
    raise ValueError(f"不正なdirection: {direction} (surge/dropのみ)")
