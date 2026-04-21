"""
共通インジケーター計算関数

signal関数とindicator serviceの両方から呼ばれる計算ロジック。
全て pd.Series[float] を返す（NaN/inf除去・丸めは呼び出し側の責務）。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import numpy as np
import pandas as pd

MovingAverageType = Literal["sma", "ema"]


@dataclass(frozen=True, slots=True)
class MACDResult:
    macd: pd.Series[float]
    signal: pd.Series[float]
    histogram: pd.Series[float]


@dataclass(frozen=True, slots=True)
class BollingerBandsResult:
    upper: pd.Series[float]
    middle: pd.Series[float]
    lower: pd.Series[float]


def compute_moving_average(
    series: pd.Series[float],
    period: int,
    ma_type: MovingAverageType = "sma",
) -> pd.Series[float]:
    """単純/指数移動平均を計算する。"""
    if ma_type == "sma":
        return series.rolling(window=period, min_periods=period).mean()
    if ma_type == "ema":
        return series.ewm(span=period, adjust=False, min_periods=period).mean()
    raise ValueError(f"未対応のma_type: {ma_type} (sma/emaのみ)")


def compute_rsi(
    close: pd.Series[float],
    period: int = 14,
) -> pd.Series[float]:
    """VectorBT 既定値互換の RSI を計算する。"""
    delta = close.diff()
    gains = delta.clip(lower=0)
    losses = -delta.clip(upper=0)

    avg_gain = gains.rolling(window=period, min_periods=period).mean()
    avg_loss = losses.rolling(window=period, min_periods=period).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))

    flat_mask = (avg_gain == 0) & (avg_loss == 0)
    rsi = rsi.mask(flat_mask, 50.0)
    rsi = rsi.mask((avg_loss == 0) & (avg_gain > 0), 100.0)
    rsi = rsi.mask((avg_gain == 0) & (avg_loss > 0), 0.0)
    return rsi


def compute_macd(
    close: pd.Series[float],
    fast_period: int = 12,
    slow_period: int = 26,
    signal_period: int = 9,
) -> MACDResult:
    """VectorBT 既定値互換の MACD line / signal line / histogram を計算する。"""
    fast_ma = compute_moving_average(close, fast_period, ma_type="sma")
    slow_ma = compute_moving_average(close, slow_period, ma_type="sma")
    macd_line = fast_ma - slow_ma
    signal_line = compute_moving_average(macd_line, signal_period, ma_type="sma")
    histogram = macd_line - signal_line
    return MACDResult(
        macd=macd_line,
        signal=signal_line,
        histogram=histogram,
    )


def compute_bollinger_bands(
    close: pd.Series[float],
    window: int = 20,
    alpha: float = 2.0,
) -> BollingerBandsResult:
    """ボリンジャーバンドを計算する。"""
    middle = close.rolling(window=window, min_periods=window).mean()
    std = close.rolling(window=window, min_periods=window).std(ddof=0)
    band_width = std * alpha
    return BollingerBandsResult(
        upper=middle + band_width,
        middle=middle,
        lower=middle - band_width,
    )


def _compute_true_range(
    high: pd.Series[float],
    low: pd.Series[float],
    close: pd.Series[float],
) -> pd.Series[float]:
    tr1 = high - low
    tr2 = abs(high - close.shift(1))
    tr3 = abs(low - close.shift(1))
    return pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)


def compute_atr(
    high: pd.Series[float],
    low: pd.Series[float],
    close: pd.Series[float],
    period: int = 14,
) -> pd.Series[float]:
    """ATR を計算する。"""
    true_range = _compute_true_range(high, low, close)
    return compute_moving_average(true_range, period, ma_type="ema")


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
    true_range = _compute_true_range(high, low, close)
    atr_ema = compute_moving_average(
        true_range,
        lookback_period,
        ma_type="ema",
    )
    highest_close = close.rolling(window=lookback_period).max()
    return highest_close - (atr_ema * atr_multiplier)


def compute_volume_mas(
    volume: pd.Series[float],
    short_period: int,
    long_period: int,
    ma_type: str = "sma",
) -> tuple[pd.Series[float], pd.Series[float]]:
    """出来高の短期/長期MA"""
    if ma_type == "median":
        short_ma = volume.rolling(window=short_period, min_periods=short_period).median()
        long_ma = volume.rolling(window=long_period, min_periods=long_period).median()
        return short_ma, long_ma
    if ma_type not in ("sma", "ema"):
        raise ValueError(f"未対応のma_type: {ma_type} (sma/ema/medianのみ)")

    resolved_ma_type: MovingAverageType = "ema" if ma_type == "ema" else "sma"
    short_ma = compute_moving_average(volume, short_period, ma_type=resolved_ma_type)
    long_ma = compute_moving_average(volume, long_period, ma_type=resolved_ma_type)
    return short_ma, long_ma


def compute_trading_value_ma(
    close: pd.Series[float],
    volume: pd.Series[float],
    period: int,
) -> pd.Series[float]:
    """売買代金MA (億円単位)"""
    trading_value = close * volume / 1e8
    return compute_moving_average(trading_value, period)


def compute_volume_weighted_ema(
    close: pd.Series[float],
    volume: pd.Series[float],
    period: int,
) -> pd.Series[float]:
    """出来高加重EMA (VWEMA)。

    定義: EMA(close * volume, period) / EMA(volume, period)
    """
    weighted_price = close * volume
    weighted_price_ema = compute_moving_average(
        weighted_price,
        period,
        ma_type="ema",
    )
    volume_ema = compute_moving_average(volume, period, ma_type="ema")
    return weighted_price_ema / volume_ema.replace(0, np.nan)


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
    ratio[valid_mask] = (
        rolling_mean[valid_mask] / rolling_denominator[valid_mask]
    ) * np.sqrt(252)
    return ratio
