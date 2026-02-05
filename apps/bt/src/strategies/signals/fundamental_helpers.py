"""
財務指標シグナル — 内部ヘルパー関数

成長率・閾値・比率・連続期間チェック等の共通計算ロジックを提供
"""

from __future__ import annotations

from typing import Literal

import numpy as np
import pandas as pd

__all__ = [
    "_calc_growth_signal",
    "_calc_threshold_signal",
    "_calc_ratio_signal",
    "_calc_consecutive_threshold_signal",
]


def _calc_growth_signal(
    data: pd.Series[float],
    periods: int,
    growth_threshold: float,
    condition: Literal["above", "below"] = "above",
    max_growth: float = 10.0,
) -> pd.Series[bool]:
    """
    成長率シグナル計算の共通ロジック（決算期間ベース）

    ffill済みデータから値の変化点（=決算発表日）を検出し、
    N期間前の発表値との成長率を計算する

    Args:
        data: ffill済みの財務データ（日次インデックス）
        periods: 成長率計算期間（決算期数、例: 1=前期比、4=4期前比）
        growth_threshold: 成長率閾値
        condition: 条件（above=閾値以上、below=閾値以下）
        max_growth: 最大成長率（異常値除外用）

    Returns:
        pd.Series[bool]: 成長率条件を満たす場合True

    Note:
        - 値の変化点で発表日を判定するため、全期間で同一値の場合は
          発表回数=1とみなされ、periods > 0 では比較対象がなくFalseを返す
        - 過去値が0以下またはNaNの場合はFalseを返す
        - 成長率が極端に高い値（max_growth以上）の場合はFalseを返す
    """
    if periods <= 0:
        # periods=0は現在値との比較（常に成長率0%）
        return pd.Series(False, index=data.index)

    # 値の変化点を検出（=決算発表日）、最初の値も発表日とみなす
    is_release_date = data.diff() != 0
    if len(is_release_date) > 0:
        is_release_date.iloc[0] = True

    # 発表日のインデックスを抽出
    release_dates = is_release_date[is_release_date].index
    if len(release_dates) <= periods:
        # 比較に必要な期間数が不足
        return pd.Series(False, index=data.index)

    # 発表日のデータのみを抽出してshift（N期間前の発表値を取得）
    release_values = data.loc[release_dates]
    past_values = release_values.shift(periods)

    # 成長率計算
    growth_rate = (release_values - past_values) / past_values.where(
        past_values > 0, np.nan
    )

    # 条件判定
    if condition == "above":
        threshold_condition = growth_rate >= growth_threshold
    else:  # below
        threshold_condition = growth_rate < growth_threshold

    meets_condition = (
        threshold_condition
        & growth_rate.notna()
        & (growth_rate < max_growth)
        & (release_values > 0)
    )

    # 結果を日次インデックスに展開（ffill）
    daily_result = pd.Series(np.nan, index=data.index)
    daily_result.loc[meets_condition.index] = meets_condition.astype(float)

    return daily_result.ffill().fillna(0.0).astype(bool) & data.notna()


def _calc_threshold_signal(
    data: pd.Series[float],
    threshold: float,
    condition: Literal["above", "below"] = "above",
    require_positive: bool = True,
) -> pd.Series[bool]:
    """
    閾値比較シグナル計算の共通ロジック

    Args:
        data: 比較対象データ
        threshold: 閾値
        condition: 条件（above=閾値以上、below=閾値以下）
        require_positive: 正の値を要求するか

    Returns:
        pd.Series[bool]: 条件を満たす場合True
    """
    if condition == "above":
        base_condition = (data >= threshold) & data.notna()
    else:
        base_condition = (data < threshold) & data.notna()

    if require_positive:
        base_condition = base_condition & (data > 0)
    return base_condition.fillna(False)


def _calc_ratio_signal(
    ratio: pd.Series[float],
    threshold: float,
    condition: Literal["above", "below"],
    exclude_negative: bool = True,
) -> pd.Series[bool]:
    """
    比率シグナル計算の共通ロジック（PER、PBR、PEG等）

    Args:
        ratio: 比率データ（PER、PBR、PEG等）
        threshold: 閾値
        condition: 条件（above=閾値以上、below=閾値以下）
        exclude_negative: 負の値を除外するか（デフォルトTrue）

    Returns:
        pd.Series[bool]: 条件を満たす場合True
    """
    valid_ratio = ratio.notna() & (ratio > 0) if exclude_negative else ratio.notna()
    threshold_condition = ratio < threshold if condition == "below" else ratio >= threshold
    return (threshold_condition & valid_ratio).fillna(False)


def _calc_consecutive_threshold_signal(
    data: pd.Series[float],
    threshold: float,
    condition: Literal["above", "below"],
    consecutive_periods: int,
) -> pd.Series[bool]:
    """
    連続期間での閾値チェック

    ffill済みデータから値の変化点（=決算発表日）を検出し、
    直近N回分の発表値が全て条件を満たすかチェック

    Args:
        data: ffill済みの財務データ（日次インデックス）
        threshold: 閾値
        condition: 条件（above=閾値以上、below=閾値より小さい）
        consecutive_periods: 連続期間数

    Returns:
        pd.Series[bool]: 直近N回分の発表値が全て条件を満たす場合True

    Note:
        値の変化点で発表日を判定するため、全期間で同一値の場合は
        発表回数=1とみなされ、consecutive_periods > 1 では全てFalseを返す
    """
    if consecutive_periods <= 1:
        return _calc_threshold_signal(data, threshold, condition, require_positive=False)

    # 値の変化点を検出（=決算発表日）、最初の値も発表日とみなす
    is_release_date = data.diff() != 0
    if len(is_release_date) > 0:
        is_release_date.iloc[0] = True

    # 閾値条件の判定
    meets_threshold = data >= threshold if condition == "above" else data < threshold

    # 発表日のインデックスを抽出
    release_dates = is_release_date[is_release_date].index
    if len(release_dates) < consecutive_periods:
        return pd.Series(False, index=data.index)

    # 発表日ごとの条件をrolling windowでチェック
    release_results = meets_threshold.loc[release_dates]
    consecutive_met = (
        release_results.rolling(window=consecutive_periods, min_periods=consecutive_periods)
        .sum()
        .eq(consecutive_periods)
    )

    # 結果を日次インデックスに展開（ffill）
    daily_result = pd.Series(np.nan, index=data.index)
    daily_result.loc[consecutive_met.index] = consecutive_met.astype(float)

    return daily_result.ffill().fillna(0.0).astype(bool) & data.notna()
