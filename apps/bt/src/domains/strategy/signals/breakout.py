"""
Breakout and level-based signal implementations.
"""

from __future__ import annotations

import pandas as pd
from loguru import logger

from src.shared.models.signals import normalize_bool_series

from .baseline import baseline_cross_signal, cross_signal, position_signal


def _recent_true(signal: pd.Series, lookback_days: int) -> pd.Series:
    result = normalize_bool_series(signal)
    if lookback_days <= 1:
        return result
    return normalize_bool_series(
        result.astype(int).rolling(lookback_days, min_periods=1).max() >= 1
    )


def _period_extrema_hits(
    price: pd.Series,
    period: int,
    direction: str,
) -> tuple[pd.Series, pd.Series]:
    if direction == "high":
        extrema = price.rolling(period).max()
        hits = price >= extrema
    elif direction == "low":
        extrema = price.rolling(period).min()
        hits = price <= extrema
    else:
        raise ValueError(f"不正なdirection: {direction} (high/lowのみ)")

    valid = price.notna() & extrema.notna()
    return normalize_bool_series(hits & valid), normalize_bool_series(valid)


def period_extrema_break_signal(
    price: pd.Series,
    period: int = 20,
    direction: str = "high",
    lookback_days: int = 1,
) -> pd.Series:
    """期間高値/安値のブレイクイベントを検出する。"""
    logger.debug(
        "期間極値ブレイク: period={}日, lookback={}日, direction={}",
        period,
        lookback_days,
        direction,
    )

    hits, _valid = _period_extrema_hits(price, period, direction)
    event = hits & ~normalize_bool_series(hits.shift(1))
    result = _recent_true(event, lookback_days)

    logger.debug(
        "期間極値ブレイク: 完了 (True: {}/{})",
        int(result.sum()),
        len(result),
    )
    return result


def period_extrema_position_signal(
    price: pd.Series,
    period: int = 20,
    direction: str = "high",
    state: str = "at_extrema",
    lookback_days: int = 1,
) -> pd.Series:
    """期間高値/安値圏にいるかどうかの状態を判定する。"""
    logger.debug(
        "期間極値位置: period={}日, lookback={}日, direction={}, state={}",
        period,
        lookback_days,
        direction,
        state,
    )

    hits, valid = _period_extrema_hits(price, period, direction)
    recent_hits = _recent_true(hits, lookback_days)

    if state == "at_extrema":
        result = recent_hits
    elif state == "away_from_extrema":
        result = normalize_bool_series(valid & ~recent_hits)
    else:
        raise ValueError(f"不正なstate: {state} (at_extrema/away_from_extremaのみ)")

    logger.debug(
        "期間極値位置: 完了 (True: {}/{})",
        int(result.sum()),
        len(result),
    )
    return result


def _select_level_price(
    close: pd.Series,
    low: pd.Series | None,
    price_column: str,
) -> pd.Series:
    if price_column == "close":
        return close
    if price_column == "low":
        if low is None:
            raise ValueError("price_column='low'の場合、lowデータが必須です")
        return low
    raise ValueError(f"不正なprice_column: {price_column} (close/lowのみ)")


def _compute_atr_support_level(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    lookback_period: int,
    atr_multiplier: float,
) -> pd.Series:
    from src.domains.strategy.indicators import compute_atr_support_line

    return compute_atr_support_line(high, low, close, lookback_period, atr_multiplier)


def atr_support_position_signal(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    lookback_period: int = 20,
    atr_multiplier: float = 2.0,
    direction: str = "below",
    price_column: str = "close",
) -> pd.Series:
    """ATRサポートラインに対する位置状態を判定する。"""
    logger.debug(
        "ATRサポート位置: lookback={}日, ATR倍率={}, direction={}, 価格={}",
        lookback_period,
        atr_multiplier,
        direction,
        price_column,
    )

    support_line = _compute_atr_support_level(
        high, low, close, lookback_period, atr_multiplier
    )
    price = _select_level_price(close, low, price_column)
    return position_signal(price, support_line, direction)


def atr_support_cross_signal(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    lookback_period: int = 20,
    atr_multiplier: float = 2.0,
    direction: str = "below",
    lookback_days: int = 1,
    price_column: str = "close",
) -> pd.Series:
    """ATRサポートラインのクロスイベントを判定する。"""
    logger.debug(
        "ATRサポートクロス: lookback={}日, ATR倍率={}, direction={}, event_lookback={}, 価格={}",
        lookback_period,
        atr_multiplier,
        direction,
        lookback_days,
        price_column,
    )

    support_line = _compute_atr_support_level(
        high, low, close, lookback_period, atr_multiplier
    )
    price = _select_level_price(close, low, price_column)
    return cross_signal(price, support_line, direction, lookback_days)


def _compute_retracement_level(
    high: pd.Series,
    lookback_period: int,
    retracement_level: float,
) -> pd.Series:
    highest = high.rolling(window=lookback_period).max()
    return highest * (1.0 - retracement_level)


def retracement_position_signal(
    high: pd.Series,
    close: pd.Series,
    low: pd.Series | None = None,
    lookback_period: int = 20,
    retracement_level: float = 0.382,
    direction: str = "below",
    price_column: str = "close",
) -> pd.Series:
    """リトレースメント水準に対する位置状態を判定する。"""
    logger.debug(
        "リトレースメント位置: lookback={}日, level={}, direction={}, 価格={}",
        lookback_period,
        retracement_level,
        direction,
        price_column,
    )

    level = _compute_retracement_level(high, lookback_period, retracement_level)
    price = _select_level_price(close, low, price_column)
    return position_signal(price, level, direction)


def retracement_cross_signal(
    high: pd.Series,
    close: pd.Series,
    low: pd.Series | None = None,
    lookback_period: int = 20,
    retracement_level: float = 0.382,
    direction: str = "below",
    lookback_days: int = 1,
    price_column: str = "close",
) -> pd.Series:
    """リトレースメント水準のクロスイベントを判定する。"""
    logger.debug(
        "リトレースメントクロス: lookback={}日, level={}, direction={}, event_lookback={}, 価格={}",
        lookback_period,
        retracement_level,
        direction,
        lookback_days,
        price_column,
    )

    level = _compute_retracement_level(high, lookback_period, retracement_level)
    price = _select_level_price(close, low, price_column)
    return cross_signal(price, level, direction, lookback_days)


# ===== レガシー互換ラッパー =====


def period_breakout_signal(
    price: pd.Series,
    period: int = 20,
    direction: str = "high",
    condition: str = "break",
    lookback_days: int = 1,
) -> pd.Series:
    logger.warning(
        "period_breakout_signal はレガシーです。"
        "period_extrema_break_signal / period_extrema_position_signal に移行してください。"
    )
    if condition == "break":
        return period_extrema_break_signal(price, period, direction, lookback_days)
    if condition == "maintained":
        return period_extrema_position_signal(
            price,
            period,
            direction,
            state="away_from_extrema",
            lookback_days=lookback_days,
        )
    raise ValueError(f"不正なcondition: {condition} (break/maintainedのみ)")


def atr_support_break_signal(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    lookback_period: int = 20,
    atr_multiplier: float = 2.0,
    direction: str = "break",
    price_column: str = "close",
) -> pd.Series:
    logger.warning(
        "atr_support_break_signal はレガシーです。"
        "atr_support_position_signal / atr_support_cross_signal に移行してください。"
    )
    direction_map = {"break": "below", "recovery": "above"}
    try:
        normalized = direction_map[direction]
    except KeyError as exc:
        raise ValueError(
            f"不正なdirection: {direction} (break/recoveryのみ)"
        ) from exc
    return atr_support_position_signal(
        high,
        low,
        close,
        lookback_period=lookback_period,
        atr_multiplier=atr_multiplier,
        direction=normalized,
        price_column=price_column,
    )


def retracement_signal(
    high: pd.Series,
    close: pd.Series,
    low: pd.Series | None = None,
    lookback_period: int = 20,
    retracement_level: float = 0.382,
    direction: str = "break",
    price_column: str = "close",
) -> pd.Series:
    logger.warning(
        "retracement_signal はレガシーです。"
        "retracement_position_signal / retracement_cross_signal に移行してください。"
    )
    direction_map = {"break": "below", "recovery": "above"}
    try:
        normalized = direction_map[direction]
    except KeyError as exc:
        raise ValueError(
            f"不正なdirection: {direction} (break/recoveryのみ)"
        ) from exc
    return retracement_position_signal(
        high,
        close,
        low=low,
        lookback_period=lookback_period,
        retracement_level=retracement_level,
        direction=normalized,
        price_column=price_column,
    )


def threshold_breakout_signal(
    ohlc_data: pd.DataFrame,
    threshold_type: str,
    period: int,
    direction: str = "upward",
    price_column: str = "high",
) -> pd.Series:
    """
    【レガシー】閾値ブレイクアウトシグナル

    統合版の period_extrema_break_signal / baseline_cross_signal に移行予定
    """
    logger.warning(
        "threshold_breakout_signal はレガシー関数です。"
        "period_extrema_break_signal または baseline_cross_signal に移行してください。"
    )

    if price_column == "high":
        price = ohlc_data["High"]
    elif price_column == "low":
        price = ohlc_data["Low"]
    else:
        price = ohlc_data["Close"]

    if threshold_type in ["rolling_max", "rolling_min"]:
        dir_map = {"upward": "high", "downward": "low"}
        return period_extrema_break_signal(
            price,
            period=period,
            direction=dir_map.get(direction, "high"),
            lookback_days=1,
        )
    if threshold_type in ["sma", "ema"]:
        dir_map = {"upward": "above", "downward": "below"}
        return baseline_cross_signal(
            ohlc_data,
            baseline_type=threshold_type,
            baseline_period=period,
            direction=dir_map.get(direction, "above"),
            lookback_days=1,
            price_column=price_column,
        )
    raise ValueError(f"未対応の閾値タイプ: {threshold_type}")
