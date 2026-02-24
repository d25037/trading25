"""
RSIスプレッドシグナル実装

短期RSIと長期RSIの差分（スプレッド）が閾値を超える/下回る場合にシグナル発火
"""

import pandas as pd
import vectorbt as vbt
from loguru import logger


def rsi_spread_signal(
    close: pd.Series,
    fast_period: int = 9,
    slow_period: int = 14,
    threshold: float = 10.0,
    condition: str = "above",
) -> pd.Series:
    """
    RSIスプレッドシグナル

    短期RSIと長期RSIの差分（スプレッド）を計算し、
    閾値との比較でシグナルを生成する。

    Args:
        close: 終値データ
        fast_period: 短期RSI期間（デフォルト: 9）
        slow_period: 長期RSI期間（デフォルト: 14）
        threshold: スプレッド閾値（デフォルト: 10.0）
        condition: 閾値条件
            - "above": スプレッド > threshold（短期RSIが長期RSIより閾値以上高い・強気乖離）
            - "below": スプレッド < -threshold（短期RSIが長期RSIより閾値以上低い・弱気乖離）

    Returns:
        pd.Series[bool]: 条件を満たす場合にTrue

    Examples:
        >>> # エントリー: 短期RSIが長期RSIより10以上高い（上昇モメンタム強）
        >>> entry = rsi_spread_signal(close, fast_period=9, slow_period=14, threshold=10, condition="above")
        >>>
        >>> # エグジット: 短期RSIが長期RSIより10以上低い（下降モメンタム強）
        >>> exit = rsi_spread_signal(close, fast_period=9, slow_period=14, threshold=10, condition="below")

    Raises:
        ValueError: fast_period >= slow_periodの場合
        ValueError: 不正なconditionの場合
    """
    logger.debug(
        f"RSIスプレッドシグナル: 処理開始 (短期={fast_period}, 長期={slow_period}, "
        f"閾値={threshold}, 条件={condition})"
    )

    # パラメータバリデーション
    if fast_period >= slow_period:
        raise ValueError(
            f"短期期間({fast_period})は長期期間({slow_period})より小さい必要があります"
        )

    # VectorBTでRSI計算（短期・長期）
    rsi_fast = vbt.RSI.run(close, fast_period).rsi
    rsi_slow = vbt.RSI.run(close, slow_period).rsi

    # スプレッド計算（短期RSI - 長期RSI）
    spread = rsi_fast - rsi_slow

    # 閾値判定
    if condition == "above":
        # スプレッド > threshold（短期RSIが長期RSIより閾値以上高い）
        signal = spread > threshold
    elif condition == "below":
        # スプレッド < -threshold（短期RSIが長期RSIより閾値以上低い）
        signal = spread < -threshold
    else:
        raise ValueError(f"不正なcondition: {condition} (above/belowのみ)")

    result = signal.fillna(False)

    logger.debug(
        f"RSIスプレッドシグナル: 処理完了 (条件={condition}, True: {result.sum()}/{len(result)})"
    )
    return result
