"""
Volume ratio signal implementations.
"""

from __future__ import annotations

import pandas as pd
from loguru import logger

from src.shared.models.signals import normalize_bool_series


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
        result = normalize_bool_series(
            volume_short_ma > volume_long_ma * ratio_threshold
        )
    elif direction == "below":
        result = normalize_bool_series(
            volume_short_ma < volume_long_ma * ratio_threshold
        )
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


def _threshold_signal(
    series: pd.Series,
    *,
    threshold: float,
    condition: str,
) -> pd.Series:
    if condition == "above":
        return normalize_bool_series(series >= threshold)
    if condition == "below":
        return normalize_bool_series(series < threshold)
    raise ValueError(f"不正なcondition: {condition} (above/belowのみ)")


def cmf_threshold_signal(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    volume: pd.Series,
    period: int = 20,
    threshold: float = 0.05,
    condition: str = "above",
) -> pd.Series:
    """Chaikin Money Flow の閾値条件を判定する。"""
    from src.domains.strategy.indicators import compute_chaikin_money_flow

    cmf = compute_chaikin_money_flow(high, low, close, volume, period=period)
    result = _threshold_signal(cmf, threshold=threshold, condition=condition)
    logger.debug(
        "CMF閾値: period={}, threshold={}, condition={}, 完了 (True: {}/{})",
        period,
        threshold,
        condition,
        int(result.sum()),
        len(result),
    )
    return result


def chaikin_oscillator_signal(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    volume: pd.Series,
    fast_period: int = 3,
    slow_period: int = 10,
    threshold: float = 0.0,
    condition: str = "above",
) -> pd.Series:
    """Chaikin oscillator の閾値条件を判定する。"""
    from src.domains.strategy.indicators import compute_chaikin_oscillator

    oscillator = compute_chaikin_oscillator(
        high,
        low,
        close,
        volume,
        fast_period=fast_period,
        slow_period=slow_period,
    )
    result = _threshold_signal(oscillator, threshold=threshold, condition=condition)
    logger.debug(
        "Chaikin oscillator: fast={}, slow={}, threshold={}, condition={}, 完了 (True: {}/{})",
        fast_period,
        slow_period,
        threshold,
        condition,
        int(result.sum()),
        len(result),
    )
    return result


def obv_flow_score_signal(
    close: pd.Series,
    volume: pd.Series,
    lookback_period: int = 20,
    threshold: float = 0.05,
    condition: str = "above",
) -> pd.Series:
    """OBV flow score の閾値条件を判定する。"""
    from src.domains.strategy.indicators import compute_on_balance_volume_score

    score = compute_on_balance_volume_score(
        close,
        volume,
        lookback_period=lookback_period,
    )
    result = _threshold_signal(score, threshold=threshold, condition=condition)
    logger.debug(
        "OBV flow score: lookback={}, threshold={}, condition={}, 完了 (True: {}/{})",
        lookback_period,
        threshold,
        condition,
        int(result.sum()),
        len(result),
    )
    return result


def accumulation_pressure_signal(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    volume: pd.Series,
    cmf_period: int = 20,
    chaikin_fast_period: int = 3,
    chaikin_slow_period: int = 10,
    obv_lookback_period: int = 20,
    cmf_threshold: float = 0.05,
    chaikin_oscillator_threshold: float = 0.0,
    obv_score_threshold: float = 0.05,
    min_votes: int = 2,
) -> pd.Series:
    """CMF/Chaikin/OBV のうち min_votes 以上が買い集めproxyを満たすか判定する。"""
    if not 1 <= min_votes <= 3:
        raise ValueError("min_votesは1〜3のみ指定可能です")

    cmf_signal = cmf_threshold_signal(
        high,
        low,
        close,
        volume,
        period=cmf_period,
        threshold=cmf_threshold,
        condition="above",
    )
    chaikin_signal = chaikin_oscillator_signal(
        high,
        low,
        close,
        volume,
        fast_period=chaikin_fast_period,
        slow_period=chaikin_slow_period,
        threshold=chaikin_oscillator_threshold,
        condition="above",
    )
    obv_signal = obv_flow_score_signal(
        close,
        volume,
        lookback_period=obv_lookback_period,
        threshold=obv_score_threshold,
        condition="above",
    )
    votes = (
        cmf_signal.astype(int)
        + chaikin_signal.astype(int)
        + obv_signal.astype(int)
    )
    result = normalize_bool_series(votes >= min_votes)
    logger.debug(
        "Accumulation pressure: cmf_period={}, chaikin={}/{}, obv_lookback={}, min_votes={}, 完了 (True: {}/{})",
        cmf_period,
        chaikin_fast_period,
        chaikin_slow_period,
        obv_lookback_period,
        min_votes,
        int(result.sum()),
        len(result),
    )
    return result
