"""
リスク調整リターンシグナル実装

シャープレシオ・ソルティノレシオベースの銘柄選別シグナル
「この期間でこれくらい上昇したが、分散はこれくらいあった」を数値化
"""

from __future__ import annotations

from typing import Literal, cast

import pandas as pd
from loguru import logger

from src.domains.strategy.indicators import compute_risk_adjusted_return


def risk_adjusted_return_signal(
    close: pd.Series[float],
    lookback_period: int = 60,
    threshold: float = 1.0,
    ratio_type: str = "sortino",
    condition: str = "above",
) -> pd.Series[bool]:
    """
    リスク調整リターンシグナル

    シャープレシオまたはソルティノレシオが閾値との条件を満たす場合にTrueを返す。
    エントリー・エグジット両用設計。

    Args:
        close: 終値データ
        lookback_period: 計算期間（日数）
        threshold: リスク調整リターン閾値
        ratio_type: 計算タイプ
            - "sharpe": シャープレシオ（全体分散）
            - "sortino": ソルティノレシオ（ダウンサイド分散のみ）
        condition: 閾値条件
            - "above": ratio >= threshold（高リスク調整リターン銘柄選別）
            - "below": ratio < threshold（低リスク調整リターン警告）

    Returns:
        pd.Series[bool]: 条件を満たす場合にTrue

    Examples:
        >>> # 高シャープレシオ銘柄選別（エントリーフィルター）
        >>> entry = risk_adjusted_return_signal(
        ...     close, lookback_period=60, threshold=1.5,
        ...     ratio_type="sortino", condition="above"
        ... )
        >>>
        >>> # 低リスク調整リターン警告（エグジットトリガー）
        >>> exit = risk_adjusted_return_signal(
        ...     close, lookback_period=60, threshold=0.5,
        ...     ratio_type="sortino", condition="below"
        ... )
    """
    logger.debug(
        f"リスク調整リターンシグナル: 処理開始 "
        f"(期間={lookback_period}, 閾値={threshold}, タイプ={ratio_type}, 条件={condition})"
    )

    if ratio_type not in ["sharpe", "sortino"]:
        raise ValueError(f"不正なratio_type: {ratio_type} (sharpe/sortinoのみ)")
    if condition not in ["above", "below"]:
        raise ValueError(f"不正なcondition: {condition} (above/belowのみ)")

    if len(close) == 0:
        return pd.Series(dtype=bool)

    validated_ratio_type = cast(Literal["sharpe", "sortino"], ratio_type)

    ratio = compute_risk_adjusted_return(
        close=close,
        lookback_period=lookback_period,
        ratio_type=validated_ratio_type,
    )

    # 閾値判定
    signal: pd.Series[bool] = ratio >= threshold if condition == "above" else ratio < threshold
    result: pd.Series[bool] = signal.fillna(False).astype(bool)

    logger.debug(
        f"リスク調整リターンシグナル: 処理完了 "
        f"(タイプ={ratio_type}, 条件={condition}, True: {result.sum()}/{len(result)})"
    )
    return result
