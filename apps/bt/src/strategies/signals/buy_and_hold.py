"""
Buy&Hold戦略シグナル実装

最初の有効取引日にエントリーシグナルを生成する
"""

import pandas as pd
from loguru import logger


def generate_buy_and_hold_signals(
    close: pd.Series,
) -> pd.Series:
    """
    全日程エントリー可能シグナル生成（統一Signalsシステム用）

    全日程Trueを返すことで、フィルター適用前の基本シグナルとして機能する。
    実際のエントリー条件は、SignalProcessorによるAND条件絞り込みで決定される。

    Args:
        close: 終値データ（pd.Series[float]）

    Returns:
        pd.Series[bool]: 全日程True（フィルター適用前の基本シグナル）
    """
    logger.debug("Buy&Holdシグナル: 処理開始（全日程エントリー可能）")

    # 全日程Trueで初期化（フィルター適用前の基本シグナル）
    entries = pd.Series(True, index=close.index)

    logger.debug(f"Buy&Holdシグナル: 処理完了 (True: {entries.sum()}/{len(entries)})")
    return entries
