"""
財務指標シグナル — 成長率系

EPS・利益・売上高・1株配当の成長率と、Forward EPS/Forward 1株配当成長率シグナルを提供
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


def is_forecast_eps_above_recent_fy_actuals(
    actual_eps: pd.Series[float],
    latest_forecast_eps: pd.Series[float],
    lookback_fy_count: int = 3,
    fy_release_marker: pd.Series | None = None,
    fy_period_key: pd.Series | None = None,
) -> pd.Series[bool]:
    """
    最新予想EPS > 直近FY実績EPS（X回）シグナル

    FY実績EPSの更新点を抽出し、直近FY X回の最大実績EPSを計算する。
    最新予想EPSがその値を上回る場合にTrueを返す。

    Args:
        actual_eps: 実績EPS（日次インデックスに補完済み想定）
        latest_forecast_eps: 最新予想EPS（日次インデックスに補完済み想定）
        lookback_fy_count: 比較対象に使う直近FY実績EPS回数（年数）
        fy_release_marker: FY開示イベント判定に使う系列（開示日更新）
        fy_period_key: FY期別キー（同一FYの複数開示を1回として扱うため）

    Returns:
        pd.Series[bool]: 条件を満たす場合にTrue
    """
    if lookback_fy_count < 1:
        raise ValueError("lookback_fy_count must be >= 1")

    actual = pd.to_numeric(actual_eps, errors="coerce")
    forecast = pd.to_numeric(latest_forecast_eps, errors="coerce")
    actual = actual.where(np.isfinite(actual), np.nan)
    forecast = forecast.where(np.isfinite(forecast), np.nan)

    actual_ffill = actual.ffill()

    if fy_period_key is not None:
        period_key = fy_period_key.reindex(actual_ffill.index)
        period_key = period_key.where(period_key.notna(), np.nan)
        actual_release_mask = period_key.notna() & (
            period_key.ne(period_key.shift()) | period_key.shift().isna()
        )
    elif fy_release_marker is not None:
        marker = pd.to_datetime(fy_release_marker, errors="coerce")
        marker = marker.reindex(actual_ffill.index)
        actual_release_mask = marker.notna() & (
            marker.ne(marker.shift()) | marker.shift().isna()
        )
    else:
        # FY開示日が取得できない場合は実績EPSの値変化点をFYイベントとして扱う
        actual_release_mask = actual_ffill.notna() & (
            actual_ffill.ne(actual_ffill.shift()) | actual_ffill.shift().isna()
        )
    actual_release_values = actual_ffill[actual_release_mask]

    recent_window_max = actual_release_values.rolling(
        window=lookback_fy_count,
        min_periods=lookback_fy_count,
    ).max()
    recent_window_max_daily = recent_window_max.reindex(actual_ffill.index).ffill()

    return (
        (forecast > recent_window_max_daily)
        & forecast.notna()
        & recent_window_max_daily.notna()
    ).fillna(False)


def is_expected_growth_dividend_per_share(
    dividend_fy: pd.Series[float],
    next_year_forecast_dividend_fy: pd.Series[float],
    growth_threshold: float = 0.05,
    condition: Literal["above", "below"] = "above",
) -> pd.Series[bool]:
    """
    Forward 1株配当成長率シグナル

    来年予想配当と現在配当から将来配当成長率を計算し、
    指定した条件で閾値と比較してTrueを返すシグナル

    Args:
        dividend_fy: 現在の通期配当データ（日次インデックスに補完済み想定）
        next_year_forecast_dividend_fy: 来年予想通期配当（日次インデックスに補完済み想定）
        growth_threshold: 成長率閾値（デフォルト0.05 = 5%）
        condition: 条件（above=閾値以上、below=閾値以下）

    Returns:
        pd.Series[bool]: 条件を満たす場合にTrue

    Note:
        - 現在配当または来年予想配当が0以下またはNaNの場合はFalseを返す
        - 成長率が極端に高い値（10倍以上）の場合はFalseを返す（異常値処理）
        - 推奨period_type: "FY"（NextYearForecastDividendFYはFYベース）
    """
    forward_growth_rate = (next_year_forecast_dividend_fy - dividend_fy) / dividend_fy.where(
        dividend_fy > 0, np.nan
    )

    if condition == "above":
        threshold_condition = forward_growth_rate >= growth_threshold
    else:  # below
        threshold_condition = forward_growth_rate < growth_threshold

    return (
        threshold_condition
        & forward_growth_rate.notna()
        & (forward_growth_rate < 10.0)
        & (dividend_fy > 0)
        & (next_year_forecast_dividend_fy > 0)
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


def is_growing_dividend_per_share(
    dividend_fy: pd.Series[float],
    growth_threshold: float = 0.1,
    periods: int = 1,
    condition: Literal["above", "below"] = "above",
) -> pd.Series[bool]:
    """
    1株配当成長率シグナル

    1株配当（DividendFY）の成長率を計算し、
    指定した条件で閾値と比較してTrueを返すシグナル

    Args:
        dividend_fy: 1株配当データ（日次インデックスに補完済み想定）
        growth_threshold: 成長率閾値（デフォルト0.1 = 10%）
        periods: 成長率計算期間（決算期数、デフォルト1 = 前期比）
        condition: 条件（above=閾値以上、below=閾値以下）

    Returns:
        pd.Series[bool]: 条件を満たす場合にTrue

    Note:
        - ffill済みデータから決算発表日を検出し、N期間前の発表値と比較
        - 過去の配当が0以下またはNaNの場合はFalseを返す
        - 成長率が極端に高い値（10倍以上）の場合はFalseを返す（異常値処理）
        - 推奨period_type: "FY"（通期配当の比較）
    """
    return _calc_growth_signal(dividend_fy, periods, growth_threshold, condition)
