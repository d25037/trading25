"""
出来高シグナル実装

VectorBTベースの出来高関連シグナル関数を提供
"""

import pandas as pd
from loguru import logger


def volume_signal(
    volume: pd.Series,
    direction: str = "surge",
    threshold: float = 1.5,
    short_period: int = 20,
    long_period: int = 100,
    ma_type: str = "sma",
) -> pd.Series:
    """
    出来高シグナル（direction統一設計）

    Args:
        volume: 出来高データ
        direction: 出来高方向
            - "surge": 出来高急増（短期 > 長期 × threshold）
            - "drop": 出来高減少（短期 < 長期 × threshold）
        threshold: 倍率閾値（surge時: >1.0推奨、drop時: <1.0推奨）
        short_period: 短期移動平均期間（デフォルト20）
        long_period: 長期移動平均期間（デフォルト100）
        ma_type: 移動平均タイプ（sma/ema）

    Returns:
        pd.Series[bool]: 条件を満たす場合にTrue

    Examples:
        >>> # 出来高急増シグナル（エントリー用・SMA）
        >>> volume_signal(volume, direction="surge", threshold=1.5, ma_type="sma")
        >>>
        >>> # 出来高減少シグナル（エグジット用・EMA）
        >>> volume_signal(volume, direction="drop", threshold=0.7, ma_type="ema")
    """
    logger.debug(
        f"出来高シグナル: タイプ={ma_type}, 方向={direction}, 閾値={threshold}, 短期={short_period}, 長期={long_period}"
    )

    # 出来高移動平均計算（共通関数使用）
    from src.domains.strategy.indicators import compute_volume_mas

    volume_short_ma, volume_long_ma = compute_volume_mas(
        volume, short_period, long_period, ma_type
    )

    # direction分岐
    if direction == "surge":
        # 出来高急増条件: 短期平均 > 長期平均 × 閾値
        result = (volume_short_ma > volume_long_ma * threshold).fillna(False)
    else:  # direction == "drop"
        # 出来高減少条件: 短期平均 < 長期平均 × 閾値
        result = (volume_short_ma < volume_long_ma * threshold).fillna(False)

    logger.debug(f"出来高シグナル: 処理完了 (True: {result.sum()}/{len(result)})")
    return result
