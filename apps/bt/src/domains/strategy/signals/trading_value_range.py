"""
売買代金範囲シグナル実装

VectorBTベースの売買代金範囲判定シグナル関数を提供
流動性が適度にある銘柄の絞り込みに使用
"""

import pandas as pd
import vectorbt as vbt
from loguru import logger


def trading_value_range_signal(
    close: pd.Series,
    volume: pd.Series,
    period: int = 20,
    min_threshold: float = 0.5,
    max_threshold: float = 100.0,
) -> pd.Series:
    """
    売買代金範囲シグナル（X日平均売買代金が範囲内を判定）

    Args:
        close: 終値データ
        volume: 出来高データ
        period: 移動平均期間（デフォルト20日）
        min_threshold: 最小閾値（億円単位、デフォルト0.5億円）
        max_threshold: 最大閾値（億円単位、デフォルト100億円）

    Returns:
        pd.Series[bool]: min_threshold ≤ 売買代金 ≤ max_threshold の場合にTrue

    Examples:
        >>> # 流動性範囲フィルター（エントリー用: 50-200億円）
        >>> trading_value_range_signal(close, volume, period=20, min_threshold=50.0, max_threshold=200.0)
        >>>
        >>> # 流動性異常警告（エグジット用: 範囲外で売却）
        >>> trading_value_range_signal(close, volume, period=20, min_threshold=0.5, max_threshold=1000.0)
    """
    logger.debug(
        f"売買代金範囲シグナル: 期間={period}日, 範囲={min_threshold}-{max_threshold}億円"
    )

    # 空Seriesの場合は空Seriesを返す
    if len(close) == 0 or len(volume) == 0:
        logger.debug("売買代金範囲シグナル: 空データのため空Seriesを返します")
        return pd.Series([], dtype=bool)

    # 売買代金計算（億円単位）
    trading_value = close * volume / 1e8

    # 売買代金移動平均計算
    trading_value_ma = vbt.indicators.MA.run(
        trading_value, period, short_name="TradingValue_MA"
    ).ma

    # 範囲判定: min_threshold ≤ trading_value_ma ≤ max_threshold
    result = (
        (trading_value_ma >= min_threshold) & (trading_value_ma <= max_threshold)
    ).fillna(False)

    logger.debug(
        f"売買代金範囲シグナル: 処理完了 (True: {result.sum()}/{len(result)})"
    )
    return result
