"""
共通インジケーター計算関数

signal関数とindicator serviceの両方から呼ばれる計算ロジック。
全て pd.Series[float] を返す（NaN/inf除去・丸めは呼び出し側の責務）。
"""

from __future__ import annotations

from typing import Literal

import numpy as np
import pandas as pd
import vectorbt as vbt


def compute_atr_support_line(
    high: pd.Series[float],
    low: pd.Series[float],
    close: pd.Series[float],
    lookback_period: int,
    atr_multiplier: float,
) -> pd.Series[float]:
    """ATRサポートライン = Highest Close(N) - EMA(TrueRange, N) × 倍率

    Pine Script準拠:
      highest_close = ta.highest(close, lookback_period)
      atr_value = ta.ema(trueRange, lookback_period)
      support_line = highest_close - (atr_value * atr_multiplier)
    """
    tr1 = high - low
    tr2 = abs(high - close.shift(1))
    tr3 = abs(low - close.shift(1))
    true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr_ema: pd.Series[float] = vbt.MA.run(true_range, lookback_period, ewm=True).ma
    highest_close = close.rolling(window=lookback_period).max()
    return highest_close - (atr_ema * atr_multiplier)


def compute_volume_mas(
    volume: pd.Series[float],
    short_period: int,
    long_period: int,
    ma_type: str = "sma",
) -> tuple[pd.Series[float], pd.Series[float]]:
    """出来高の短期/長期MA"""
    ewm = ma_type == "ema"
    short_ma: pd.Series[float] = vbt.MA.run(volume, short_period, ewm=ewm).ma
    long_ma: pd.Series[float] = vbt.MA.run(volume, long_period, ewm=ewm).ma
    return short_ma, long_ma


def compute_trading_value_ma(
    close: pd.Series[float],
    volume: pd.Series[float],
    period: int,
) -> pd.Series[float]:
    """売買代金MA (億円単位)"""
    trading_value = close * volume / 1e8
    ma: pd.Series[float] = vbt.MA.run(trading_value, period).ma
    return ma


def compute_nbar_support(
    low: pd.Series[float],
    period: int,
) -> pd.Series[float]:
    """N日安値サポート"""
    support: pd.Series[float] = low.rolling(period).min()
    return support


def _compute_rolling_downside_std(
    returns: pd.Series[float],
    lookback_period: int,
) -> pd.Series[float]:
    """Sortino分母: 負リターンのみのローリング標準偏差"""
    negative_only = returns.where(returns < 0)
    return negative_only.rolling(window=lookback_period, min_periods=2).std()


def compute_risk_adjusted_return(
    close: pd.Series[float],
    lookback_period: int = 60,
    ratio_type: Literal["sharpe", "sortino"] = "sortino",
) -> pd.Series[float]:
    """リスク調整リターン (Sharpe / Sortino) を計算"""
    if ratio_type not in ("sharpe", "sortino"):
        raise ValueError(f"不正なratio_type: {ratio_type} (sharpe/sortinoのみ)")

    close_clean = close.replace([np.inf, -np.inf], np.nan)
    returns = close_clean.pct_change()

    rolling_mean = returns.rolling(
        window=lookback_period,
        min_periods=lookback_period,
    ).mean()

    if ratio_type == "sharpe":
        rolling_denominator = returns.rolling(
            window=lookback_period,
            min_periods=lookback_period,
        ).std()
    else:
        rolling_denominator = _compute_rolling_downside_std(returns, lookback_period)

    ratio = pd.Series(np.nan, index=close.index, dtype=float)
    valid_mask = rolling_denominator > 0
    ratio[valid_mask] = (rolling_mean[valid_mask] / rolling_denominator[valid_mask]) * np.sqrt(252)
    return ratio
