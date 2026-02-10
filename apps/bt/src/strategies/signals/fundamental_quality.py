"""
財務指標シグナル — 収益性・品質系

ROE・ROA・営業利益率・配当利回りに基づく品質判定シグナルを提供
"""

from __future__ import annotations

from typing import Literal

import numpy as np
import pandas as pd

from .fundamental_helpers import _calc_threshold_signal


def is_high_roe(
    roe: pd.Series[float],
    threshold: float = 10.0,
    condition: Literal["above", "below"] = "above",
) -> pd.Series[bool]:
    """
    ROE（Return on Equity）シグナル

    既に計算済みのROEデータを使用して、
    指定した条件で閾値と比較してTrueを返すシグナル

    Args:
        roe: ROEデータ（%単位、ローダーで日次補完済み）
        threshold: ROE閾値（デフォルト10.0 = 10%）
        condition: 条件（above=閾値以上、below=閾値以下）

    Returns:
        pd.Series[bool]: 条件を満たす場合にTrue

    Note:
        - ROEがNaNの場合はFalseを返す
        - ROEが負の値の場合はFalseを返す（損失企業除外）
        - 推奨period_type: "FY"（通期利益ベース）
    """
    return _calc_threshold_signal(roe, threshold, condition)


def is_high_roa(
    roa: pd.Series[float],
    threshold: float = 5.0,
    condition: Literal["above", "below"] = "above",
) -> pd.Series[bool]:
    """
    ROA（Return on Assets）シグナル

    既に計算済みのROAデータを使用して、
    指定した条件で閾値と比較してTrueを返すシグナル

    Args:
        roa: ROAデータ（%単位、ローダーで日次補完済み）
        threshold: ROA閾値（デフォルト5.0 = 5%）
        condition: 条件（above=閾値以上、below=閾値以下）

    Returns:
        pd.Series[bool]: 条件を満たす場合にTrue

    Note:
        - ROAがNaNの場合はFalseを返す
        - ROAが負の値の場合はFalseを返す（損失企業除外）
        - 推奨period_type: "FY"（通期利益ベース）
    """
    return _calc_threshold_signal(roa, threshold, condition)


def is_high_operating_margin(
    operating_margin: pd.Series[float],
    threshold: float = 10.0,
    condition: Literal["above", "below"] = "above",
) -> pd.Series[bool]:
    """
    高営業利益率シグナル

    営業利益率（Operating Margin）が指定した条件で閾値と比較してTrueを返すシグナル

    Args:
        operating_margin: 営業利益率データ（%単位、日次インデックスに補完済み想定）
        threshold: 営業利益率閾値（デフォルト10.0 = 10%）
        condition: 条件（above=閾値以上、below=閾値以下）

    Returns:
        pd.Series[bool]: 条件を満たす場合にTrue

    Note:
        - 営業利益率がNaNの場合はFalseを返す
        - 営業利益率が負の値の場合はFalseを返す（損失企業除外）
        - 推奨period_type: "FY"（通期比較）、"ALL"も可（四半期ごとの最新情報）
    """
    return _calc_threshold_signal(operating_margin, threshold, condition)


def is_high_dividend_yield(
    dividend_fy: pd.Series[float],
    close: pd.Series[float],
    threshold: float = 2.0,
    condition: Literal["above", "below"] = "above",
) -> pd.Series[bool]:
    """
    配当利回りシグナル

    配当利回り（Dividend Yield）= (DividendFY / Close) * 100 を計算し、
    指定した条件で閾値と比較してTrueを返すシグナル

    Args:
        dividend_fy: 通期配当データ（日次インデックスに補完済み想定）
        close: 終値データ（日次）
        threshold: 配当利回り閾値（デフォルト2.0 = 2%）
        condition: 条件（above=閾値以上、below=閾値以下）

    Returns:
        pd.Series[bool]: 条件を満たす場合にTrue

    Note:
        - 配当がNaNまたは0以下の場合はFalseを返す
        - 株価がNaNまたは0以下の場合はFalseを返す
        - 配当利回りが極端に高い値（30%以上）の場合はFalseを返す（異常値処理）
        - 推奨period_type: "FY"（DividendFYはFYのみで発表）
    """
    max_yield = 30.0
    dividend_yield = (dividend_fy / close.where(close > 0, np.nan)) * 100

    base_signal = _calc_threshold_signal(dividend_yield, threshold, condition)
    return (base_signal & (dividend_yield < max_yield) & (dividend_fy > 0)).fillna(
        False
    )
