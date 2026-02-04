"""
売買代金シグナル実装

VectorBTベースの売買代金関連シグナル関数を提供
売買代金 = 終値 × 出来高（日本株市場では出来高の大部分が寄り・引けに集中）
"""

import pandas as pd
from loguru import logger


def trading_value_signal(
    close: pd.Series,
    volume: pd.Series,
    direction: str = "above",
    period: int = 20,
    threshold_value: float = 1.0,
) -> pd.Series:
    """
    売買代金シグナル（X日平均売買代金が閾値以上/以下を判定）

    Args:
        close: 終値データ
        volume: 出来高データ
        direction: 売買代金判定方向
            - "above": X日平均売買代金が閾値以上
            - "below": X日平均売買代金が閾値以下
        period: 移動平均期間（デフォルト20日）
        threshold_value: 売買代金閾値（億円単位、デフォルト1.0億円）

    Returns:
        pd.Series[bool]: 条件を満たす場合にTrue

    Examples:
        >>> # 20日平均売買代金が1億円以上（エントリー用）
        >>> trading_value_signal(close, volume, direction="above", period=20, threshold_value=1.0)
        >>>
        >>> # 20日平均売買代金が0.5億円以下（エグジット用）
        >>> trading_value_signal(close, volume, direction="below", period=20, threshold_value=0.5)
    """
    logger.debug(
        f"売買代金シグナル: 方向={direction}, 期間={period}日, 閾値={threshold_value}億円"
    )

    # 売買代金移動平均計算（共通関数使用）
    from src.utils.indicators import compute_trading_value_ma

    trading_value_ma = compute_trading_value_ma(close, volume, period)

    # direction分岐
    if direction == "above":
        # X日平均売買代金が閾値以上
        result = (trading_value_ma >= threshold_value).fillna(False)
    else:  # direction == "below"
        # X日平均売買代金が閾値以下
        result = (trading_value_ma < threshold_value).fillna(False)

    logger.debug(f"売買代金シグナル: 処理完了 (True: {result.sum()}/{len(result)})")
    return result
