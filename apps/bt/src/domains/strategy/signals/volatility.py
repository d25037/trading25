"""
ボラティリティシグナル

ボラティリティに基づく銘柄シグナル生成機能を提供します。
"""

from typing import Protocol, cast

import pandas as pd
import vectorbt as vbt
from loguru import logger


class _BollingerBandsLike(Protocol):
    upper: pd.Series
    middle: pd.Series
    lower: pd.Series


def volatility_relative_signal(
    stock_price: pd.Series,
    benchmark_price: pd.Series,
    lookback_period: int = 200,
    threshold_multiplier: float = 1.0,
) -> pd.Series:
    """
    銘柄のボラティリティがベンチマーク（TOPIX等）と比較して閾値以下かを判定

    Args:
        stock_price: 銘柄の終値価格データ
        benchmark_price: ベンチマーク価格データ（TOPIX等）
        lookback_period: ボラティリティ計算期間（デフォルト: 200日）
        threshold_multiplier: ベンチマークボラティリティに対する倍率（デフォルト: 1.0倍）

    Returns:
        pd.Series: シグナル結果（True: 条件を満たす、False: 条件を満たさない）

    Example:
        >>> # 各銘柄の過去200日のボラティリティがTOPIXの100%倍以下の銘柄を抽出
        >>> signal_result = volatility_relative_signal(
        ...     stock_close, topix_close,
        ...     lookback_period=200,
        ...     threshold_multiplier=1.0
        ... )
    """
    # Phase 2: ローリングボラティリティでの比較
    # 共通インデックスに統一
    common_index = stock_price.index.intersection(benchmark_price.index)
    stock_aligned = stock_price.reindex(common_index)
    benchmark_aligned = benchmark_price.reindex(common_index)

    # リターン計算
    stock_returns = stock_aligned.pct_change()
    benchmark_returns = benchmark_aligned.pct_change()

    # ローリングボラティリティ計算（十分なデータがある期間のみ）
    stock_vol = stock_returns.rolling(
        window=lookback_period, min_periods=lookback_period
    ).std() * (252**0.5)
    benchmark_vol = benchmark_returns.rolling(
        window=lookback_period, min_periods=lookback_period
    ).std() * (252**0.5)

    # 相対ボラティリティ比較（銘柄ボラティリティ <= ベンチマーク * 閾値倍率）
    signal = (stock_vol <= benchmark_vol * threshold_multiplier).fillna(False)

    # 元のインデックスに戻す
    result = pd.Series(False, index=stock_price.index)
    result.loc[common_index] = signal

    return result


def rolling_volatility_signal(
    price: pd.Series,
    window: int = 20,
    threshold: float = 0.3,
) -> pd.Series:
    """
    ローリングボラティリティシグナル

    指定期間の年率ボラティリティが閾値以下の場合にTrueを返す

    Args:
        price: 価格データ
        window: ローリング期間（デフォルト: 20日）
        threshold: ボラティリティ閾値（年率、デフォルト: 0.3 = 30%）

    Returns:
        pd.Series: ボラティリティが閾値以下の場合にTrue
    """
    returns = price.pct_change()
    rolling_vol = returns.rolling(window=window, min_periods=window).std() * (252**0.5)
    return (rolling_vol <= threshold).fillna(False)


def volatility_percentile_signal(
    price: pd.Series,
    window: int = 20,
    lookback: int = 252,
    percentile: float = 50.0,
) -> pd.Series:
    """
    ボラティリティパーセンタイルシグナル

    現在のボラティリティが過去のパーセンタイル以下の場合にTrueを返す

    Args:
        price: 価格データ
        window: ボラティリティ計算期間（デフォルト: 20日）
        lookback: パーセンタイル計算期間（デフォルト: 252日）
        percentile: パーセンタイル閾値（デフォルト: 50.0 = 中央値）

    Returns:
        pd.Series: 現在ボラティリティがパーセンタイル以下の場合にTrue
    """
    returns = price.pct_change()
    rolling_vol = returns.rolling(window=window, min_periods=window).std() * (252**0.5)

    # ローリングパーセンタイル計算
    vol_percentile = rolling_vol.rolling(
        window=lookback, min_periods=lookback
    ).quantile(percentile / 100.0)

    return (rolling_vol <= vol_percentile).fillna(False)


def low_volatility_stock_screen_signal(
    price: pd.Series,
    min_price: float = 100.0,
    max_volatility: float = 0.25,
    window: int = 60,
) -> pd.Series:
    """
    低ボラティリティ株スクリーニングシグナル

    価格と低ボラティリティの組み合わせでスクリーニング

    Args:
        price: 価格データ
        min_price: 最低価格（デフォルト: 100円）
        max_volatility: 最大ボラティリティ（年率、デフォルト: 0.25 = 25%）
        window: ボラティリティ計算期間（デフォルト: 60日）

    Returns:
        pd.Series: 低ボラティリティ株条件を満たす場合にTrue
    """
    returns = price.pct_change()
    rolling_vol = returns.rolling(window=window, min_periods=window).std() * (252**0.5)

    # 価格条件 AND ボラティリティ条件
    return ((price >= min_price) & (rolling_vol <= max_volatility)).fillna(False)


def _resolve_bollinger_band(
    bbands: _BollingerBandsLike,
    level: str,
) -> pd.Series:
    if level == "upper":
        return bbands.upper
    if level == "middle":
        return bbands.middle
    if level == "lower":
        return bbands.lower
    raise ValueError(f"不正なlevel: {level} (upper/middle/lowerのみ)")


def _level_position_signal(
    price: pd.Series,
    level_series: pd.Series,
    direction: str,
) -> pd.Series:
    if direction == "below":
        return (price <= level_series).fillna(False)
    if direction == "above":
        return (price >= level_series).fillna(False)
    raise ValueError(f"不正なdirection: {direction} (above/belowのみ)")


def _level_cross_signal(
    price: pd.Series,
    level_series: pd.Series,
    direction: str,
    lookback_days: int,
) -> pd.Series:
    if direction == "below":
        raw = (price < level_series) & (price.shift(1) >= level_series.shift(1))
    elif direction == "above":
        raw = (price > level_series) & (price.shift(1) <= level_series.shift(1))
    else:
        raise ValueError(f"不正なdirection: {direction} (above/belowのみ)")

    result = raw.fillna(False)
    if lookback_days > 1:
        result = (result.astype(int).rolling(lookback_days).max() >= 1).fillna(False)
    return result


def bollinger_position_signal(
    ohlc_data: pd.DataFrame,
    window: int = 20,
    alpha: float = 2.0,
    level: str = "upper",
    direction: str = "below",
) -> pd.Series:
    """
    ボリンジャーバンド位置シグナル

    終値が指定したボリンジャーバンド水準の上側/下側にいるかを判定する。

    Args:
        ohlc_data: OHLCVデータ（Close含む）
        window: ボリンジャーバンド期間（デフォルト: 20日）
        alpha: 標準偏差倍率（デフォルト: 2.0σ）
        level: 判定対象バンド（upper/middle/lower）
        direction: 判定方向（above/below）

    Returns:
        pd.Series[bool]: 条件を満たす場合にTrue
    """
    logger.debug(
        "ボリンジャーバンド位置シグナル: 処理開始 (期間={}, α={}, level={}, direction={})",
        window,
        alpha,
        level,
        direction,
    )

    close = ohlc_data["Close"]

    # ボリンジャーバンド計算（VectorBT実装）
    bbands = cast(_BollingerBandsLike, vbt.BBANDS.run(close, window=window, alpha=alpha))
    band = _resolve_bollinger_band(bbands, level)
    result = _level_position_signal(close, band, direction)

    logger.debug(
        "ボリンジャーバンド位置シグナル: 処理完了 (True: {}/{})",
        result.sum(),
        len(result),
    )
    return result


def bollinger_cross_signal(
    ohlc_data: pd.DataFrame,
    window: int = 20,
    alpha: float = 2.0,
    level: str = "upper",
    direction: str = "below",
    lookback_days: int = 1,
) -> pd.Series:
    """終値が指定したボリンジャーバンド水準をクロスしたイベントを判定する。"""

    logger.debug(
        "ボリンジャーバンドクロスシグナル: 処理開始 (期間={}, α={}, level={}, direction={}, lookback={})",
        window,
        alpha,
        level,
        direction,
        lookback_days,
    )

    close = ohlc_data["Close"]
    bbands = cast(_BollingerBandsLike, vbt.BBANDS.run(close, window=window, alpha=alpha))
    band = _resolve_bollinger_band(bbands, level)
    result = _level_cross_signal(close, band, direction, lookback_days)

    logger.debug(
        "ボリンジャーバンドクロスシグナル: 処理完了 (True: {}/{})",
        result.sum(),
        len(result),
    )
    return result


def bollinger_bands_signal(
    ohlc_data: pd.DataFrame,
    window: int = 20,
    alpha: float = 2.0,
    position: str = "below_upper",
) -> pd.Series:
    """Backward-compatible alias for the legacy Bollinger position signal API."""

    position_map = {
        "below_upper": ("upper", "below"),
        "above_lower": ("lower", "above"),
        "above_middle": ("middle", "above"),
        "below_middle": ("middle", "below"),
        "above_upper": ("upper", "above"),
        "below_lower": ("lower", "below"),
    }

    resolved = position_map.get(position)
    if resolved is None:
        raise ValueError(
            f"不正なposition: {position} "
            "(below_upper/above_lower/above_middle/below_middle/above_upper/below_lowerのみ)"
        )

    level, direction = resolved
    return bollinger_position_signal(
        ohlc_data=ohlc_data,
        window=window,
        alpha=alpha,
        level=level,
        direction=direction,
    )
