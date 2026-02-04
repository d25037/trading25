"""
財務指標シグナル — 成長率系

EPS・利益・売上高の成長率およびForward EPS成長率シグナルを提供
"""

from __future__ import annotations

from typing import Literal

import numpy as np
import pandas as pd

from .fundamental_helpers import _calc_growth_signal


def is_growing_eps(
    eps: pd.Series[float],
    growth_threshold: float = 0.1,
    periods: int = 1,
    condition: Literal["above", "below"] = "above",
) -> pd.Series[bool]:
    """
    EPS成長率シグナル

    EPS（1株当たり利益）の成長率を計算し、
    指定した条件で閾値と比較してTrueを返すシグナル

    Args:
        eps: EPS（1株当たり利益）データ（日次インデックスに補完済み想定）
        growth_threshold: 成長率閾値（デフォルト0.1 = 10%）
        periods: 成長率計算期間（決算期数、デフォルト1 = 前期比、例: FYなら前年比）
        condition: 条件（above=閾値以上、below=閾値以下）

    Returns:
        pd.Series[bool]: 条件を満たす場合にTrue

    Note:
        - ffill済みデータから決算発表日を検出し、N期間前の発表値と比較
        - 過去のEPSが0以下またはNaNの場合はFalseを返す
        - 成長率が極端に高い値（10倍以上）の場合はFalseを返す（異常値処理）
        - 推奨period_type: "FY"（通期比較）、periods=1で前年比較
    """
    return _calc_growth_signal(eps, periods, growth_threshold, condition)


def is_expected_growth_eps(
    eps: pd.Series[float],
    next_year_forecast_eps: pd.Series[float],
    growth_threshold: float = 0.1,
    condition: Literal["above", "below"] = "above",
) -> pd.Series[bool]:
    """
    Forward EPS Growth（将来EPS成長率）シグナル

    来年予想EPSと現在EPSから将来EPS成長率を計算し、
    指定した条件で閾値と比較してTrueを返すシグナル

    Args:
        eps: 現在EPS（1株当たり利益）データ（日次インデックスに補完済み想定）
        next_year_forecast_eps: 来年予想EPS（日次インデックスに補完済み想定）
        growth_threshold: 成長率閾値（デフォルト0.1 = 10%）
        condition: 条件（above=閾値以上、below=閾値以下）

    Returns:
        pd.Series[bool]: 条件を満たす場合にTrue

    Note:
        - 現在EPSまたは来年予想EPSが0以下またはNaNの場合はFalseを返す
        - 成長率が極端に高い値（10倍以上）の場合はFalseを返す（異常値処理）
        - 既存のis_growing_eps()とは異なり、予想データを使用した将来成長率
        - 推奨period_type: "FY"（NextYearForecastEPSはFYのみで発表）
    """
    forward_growth_rate = (next_year_forecast_eps - eps) / eps.where(eps > 0, np.nan)

    if condition == "above":
        threshold_condition = forward_growth_rate >= growth_threshold
    else:  # below
        threshold_condition = forward_growth_rate < growth_threshold

    return (
        threshold_condition
        & forward_growth_rate.notna()
        & (forward_growth_rate < 10.0)
        & (eps > 0)
        & (next_year_forecast_eps > 0)
    ).fillna(False)


def is_growing_profit(
    profit: pd.Series[float],
    growth_threshold: float = 0.1,
    periods: int = 1,
    condition: Literal["above", "below"] = "above",
) -> pd.Series[bool]:
    """
    Profit成長率シグナル

    利益（Profit）の成長率を計算し、
    指定した条件で閾値と比較してTrueを返すシグナル

    Args:
        profit: 利益データ（日次インデックスに補完済み想定）
        growth_threshold: 成長率閾値（デフォルト0.1 = 10%）
        periods: 成長率計算期間（決算期数、デフォルト1 = 前期比、例: FYなら前年比）
        condition: 条件（above=閾値以上、below=閾値以下）

    Returns:
        pd.Series[bool]: 条件を満たす場合にTrue

    Note:
        - ffill済みデータから決算発表日を検出し、N期間前の発表値と比較
        - 過去の利益が0以下またはNaNの場合はFalseを返す
        - 成長率が極端に高い値（10倍以上）の場合はFalseを返す（異常値処理）
        - 推奨period_type: "FY"（通期比較）、periods=1で前年比較
    """
    return _calc_growth_signal(profit, periods, growth_threshold, condition)


def is_growing_sales(
    sales: pd.Series[float],
    growth_threshold: float = 0.1,
    periods: int = 1,
    condition: Literal["above", "below"] = "above",
) -> pd.Series[bool]:
    """
    Sales成長率シグナル

    売上高（Sales）の成長率を計算し、
    指定した条件で閾値と比較してTrueを返すシグナル

    Args:
        sales: 売上高データ（日次インデックスに補完済み想定）
        growth_threshold: 成長率閾値（デフォルト0.1 = 10%）
        periods: 成長率計算期間（決算期数、デフォルト1 = 前期比、例: FYなら前年比）
        condition: 条件（above=閾値以上、below=閾値以下）

    Returns:
        pd.Series[bool]: 条件を満たす場合にTrue

    Note:
        - ffill済みデータから決算発表日を検出し、N期間前の発表値と比較
        - 過去の売上高が0以下またはNaNの場合はFalseを返す
        - 成長率が極端に高い値（10倍以上）の場合はFalseを返す（異常値処理）
        - 推奨period_type: "FY"（通期比較）、periods=1で前年比較
        - "all"使用時はperiods=4で前年同期比較
    """
    return _calc_growth_signal(sales, periods, growth_threshold, condition)
