"""
RSI閾値シグナル実装

RSIの閾値判定（RSI > 80、RSI < 30等）を行う汎用シグナル関数
"""

import pandas as pd
import vectorbt as vbt
from loguru import logger


def rsi_threshold_signal(
    close: pd.Series,
    period: int = 14,
    threshold: float = 30.0,
    condition: str = "below",
) -> pd.Series:
    """
    RSI閾値シグナル

    RSIが指定閾値との条件を満たす場合にTrueを返す。
    既存シグナル（volume_surge_signal等）と同じく、条件判定のみを行う。

    Args:
        close: 終値データ
        period: RSI計算期間
        threshold: RSI閾値（0-100）
        condition: 閾値条件
            - "below": RSI < threshold（売られすぎ判定）
            - "above": RSI > threshold（買われすぎ判定）

    Returns:
        pd.Series[bool]: 条件を満たす場合にTrue

    Examples:
        >>> # 売られすぎ判定（エントリー用）
        >>> entry = rsi_threshold_signal(close, period=14, threshold=30, condition="below")
        >>>
        >>> # 買われすぎ判定（エグジット用）
        >>> exit = rsi_threshold_signal(close, period=9, threshold=70, condition="above")
    """
    logger.debug(
        f"RSI閾値シグナル: 処理開始 (期間={period}, 閾値={threshold}, 条件={condition})"
    )

    # VectorBTでRSI計算
    rsi = vbt.RSI.run(close, period).rsi

    # 閾値判定
    if condition == "below":
        # RSI < threshold（売られすぎ判定）
        signal = rsi < threshold
    elif condition == "above":
        # RSI > threshold（買われすぎ判定）
        signal = rsi > threshold
    else:
        raise ValueError(f"不正なcondition: {condition} (below/aboveのみ)")

    result = signal.fillna(False)

    logger.debug(
        f"RSI閾値シグナル: 処理完了 (条件={condition}, True: {result.sum()}/{len(result)})"
    )
    return result
