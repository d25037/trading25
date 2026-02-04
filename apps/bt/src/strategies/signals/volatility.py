"""
ボラティリティシグナル

ボラティリティに基づく銘柄シグナル生成機能を提供します。
"""

import pandas as pd
import vectorbt as vbt
from loguru import logger


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


def bollinger_bands_signal(
    ohlc_data: pd.DataFrame,
    window: int = 20,
    alpha: float = 2.0,
    position: str = "below_upper",
) -> pd.Series:
    """
    ボリンジャーバンドシグナル（汎用・エントリー/エグジット両用）

    終値がボリンジャーバンドのどの位置にあるかを判定する。
    既存シグナル（volume_surge_signal等）と同じく、条件判定のみを行う。

    Args:
        ohlc_data: OHLCVデータ（Close含む）
        window: ボリンジャーバンド期間（デフォルト: 20日）
        alpha: 標準偏差倍率（デフォルト: 2.0σ）
        position: 判定位置
            - "below_upper": 終値が上限以下（エントリー用: 過熱判定回避）
            - "above_lower": 終値が下限以上（エントリー用: 売られすぎ回避）
            - "above_middle": 終値が中央線以上（エントリー用: トレンド確認）
            - "below_middle": 終値が中央線以下（エントリー用: 平均回帰）
            - "above_upper": 終値が上限以上（エグジット用: 過熱利確）
            - "below_lower": 終値が下限以下（エグジット用: 売られすぎ損切り）

    Returns:
        pd.Series[bool]: 条件を満たす場合にTrue

    Examples:
        >>> # エントリー: 過熱していない（BB上限以下）
        >>> entry_signal = bollinger_bands_signal(
        ...     ohlc_data, window=20, alpha=2.0, position="below_upper"
        ... )
        >>>
        >>> # エグジット: 過熱利確（BB上限以上）
        >>> exit_signal = bollinger_bands_signal(
        ...     ohlc_data, window=20, alpha=2.0, position="above_upper"
        ... )
    """
    logger.debug(
        f"ボリンジャーバンドシグナル: 処理開始 (期間={window}, α={alpha}, 位置={position})"
    )

    close = ohlc_data["Close"]

    # ボリンジャーバンド計算（VectorBT実装）
    bbands = vbt.BBANDS.run(close, window=window, alpha=alpha)

    # 位置判定
    if position == "below_upper":
        signal = close <= bbands.upper
    elif position == "above_lower":
        signal = close >= bbands.lower
    elif position == "above_middle":
        signal = close >= bbands.middle
    elif position == "below_middle":
        signal = close <= bbands.middle
    elif position == "above_upper":
        signal = close >= bbands.upper
    elif position == "below_lower":
        signal = close <= bbands.lower
    else:
        raise ValueError(
            f"不正なposition: {position} "
            "(below_upper/above_lower/above_middle/below_middle/above_upper/below_lowerのみ)"
        )

    result = signal.fillna(False)

    logger.debug(
        f"ボリンジャーバンドシグナル: 処理完了 (位置={position}, True: {result.sum()}/{len(result)})"
    )
    return result
