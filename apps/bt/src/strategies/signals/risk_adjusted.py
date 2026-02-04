"""
リスク調整リターンシグナル実装

シャープレシオ・ソルティノレシオベースの銘柄選別シグナル
「この期間でこれくらい上昇したが、分散はこれくらいあった」を数値化
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from loguru import logger

ANNUALIZATION_FACTOR: float = np.sqrt(252)


def _compute_rolling_downside_std(
    returns: pd.Series[float], lookback_period: int
) -> pd.Series[float]:
    """
    ベクトル化されたダウンサイド標準偏差計算

    負のリターンのみを対象にローリング標準偏差を計算する。
    pandas rolling().std()を使用することで、apply()よりも大幅に高速化。

    Args:
        returns: 日次リターン系列
        lookback_period: 計算期間（日数）

    Returns:
        ローリングダウンサイド標準偏差
    """
    # 負のリターンのみを抽出（正のリターンはNaNに）
    negative_only: pd.Series[float] = returns.where(returns < 0)

    # pandasのrolling().std()を使用（内部でddof=1を適用）
    # min_periods=2 で少なくとも2つの負のリターンが必要
    result: pd.Series[float] = negative_only.rolling(
        window=lookback_period, min_periods=2
    ).std()

    return result


def _compute_rolling_denominator(
    returns: pd.Series[float],
    lookback_period: int,
    ratio_type: str,
) -> pd.Series[float]:
    """レシオの分母となるローリング標準偏差を計算"""
    if ratio_type == "sharpe":
        # min_periods=lookback_period で十分なデータが揃うまでNaNを返す
        return returns.rolling(window=lookback_period, min_periods=lookback_period).std()

    return _compute_rolling_downside_std(returns, lookback_period)


def _compute_ratio_with_zero_division_protection(
    rolling_mean: pd.Series[float],
    rolling_denominator: pd.Series[float],
    index: pd.Index,
) -> pd.Series[float]:
    """ゼロ除算対策付きレシオ計算"""
    ratio: pd.Series[float] = pd.Series(np.nan, index=index, dtype=float)
    valid_mask: pd.Series[bool] = rolling_denominator > 0
    ratio[valid_mask] = (
        rolling_mean[valid_mask] / rolling_denominator[valid_mask]
    ) * ANNUALIZATION_FACTOR
    return ratio


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

    # Inf値をNaNに置換（NaN/Inf検証）
    close_clean: pd.Series[float] = close.replace([np.inf, -np.inf], np.nan)

    # 日次リターン計算
    returns: pd.Series[float] = close_clean.pct_change()

    # ローリング統計計算（min_periods=lookback_periodで十分なデータが揃うまでNaN）
    rolling_mean: pd.Series[float] = returns.rolling(
        window=lookback_period, min_periods=lookback_period
    ).mean()
    rolling_denominator: pd.Series[float] = _compute_rolling_denominator(
        returns, lookback_period, ratio_type
    )

    # レシオ計算（ゼロ除算対策付き）
    ratio: pd.Series[float] = _compute_ratio_with_zero_division_protection(
        rolling_mean, rolling_denominator, close.index
    )

    # 閾値判定
    signal: pd.Series[bool] = ratio >= threshold if condition == "above" else ratio < threshold
    result: pd.Series[bool] = signal.fillna(False).astype(bool)

    logger.debug(
        f"リスク調整リターンシグナル: 処理完了 "
        f"(タイプ={ratio_type}, 条件={condition}, True: {result.sum()}/{len(result)})"
    )
    return result
