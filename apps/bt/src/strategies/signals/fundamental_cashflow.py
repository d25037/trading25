"""
財務指標シグナル — キャッシュフロー系

営業CF・簡易FCF・CFO利回り・FCF利回りに基づくシグナルを提供
"""

from __future__ import annotations

from typing import Literal

import numpy as np
import pandas as pd

from .fundamental_helpers import (
    _calc_consecutive_threshold_signal,
    _calc_market_cap,
    _calc_threshold_signal,
)


def operating_cash_flow_threshold(
    operating_cash_flow: pd.Series[float],
    threshold: float = 0.0,
    condition: Literal["above", "below"] = "above",
    consecutive_periods: int = 1,
) -> pd.Series[bool]:
    """
    営業キャッシュフロー閾値シグナル

    営業キャッシュフロー（CFO）が指定した条件で閾値と比較してTrueを返すシグナル

    Args:
        operating_cash_flow: 営業キャッシュフローデータ（日次インデックスに補完済み想定）
        threshold: 営業CF閾値（デフォルト0.0）
        condition: 条件（above=閾値より大きい、below=閾値より小さい）
        consecutive_periods: 連続期間数（直近N回分の決算発表で条件を満たす必要がある）

    Returns:
        pd.Series[bool]: 条件を満たす場合にTrue

    Note:
        - 営業CFがNaNの場合はFalseを返す
        - consecutive_periods > 1 の場合、直近N回分の決算発表で条件を満たす必要がある
        - 推奨period_type: "2Q" または "all"（営業CFは中間決算でも発表）
    """
    if consecutive_periods > 1:
        return _calc_consecutive_threshold_signal(
            operating_cash_flow, threshold, condition, consecutive_periods
        )
    return _calc_threshold_signal(
        operating_cash_flow, threshold, condition, require_positive=False
    )


def simple_fcf_threshold(
    operating_cash_flow: pd.Series[float],
    investing_cash_flow: pd.Series[float],
    threshold: float = 0.0,
    condition: Literal["above", "below"] = "above",
    consecutive_periods: int = 1,
) -> pd.Series[bool]:
    """
    簡易FCF（CFO + CFI）閾値シグナル

    簡易フリーキャッシュフロー（営業CF + 投資CF）が指定した条件で
    閾値と比較してTrueを返すシグナル

    Args:
        operating_cash_flow: 営業キャッシュフローデータ（日次インデックスに補完済み想定）
        investing_cash_flow: 投資キャッシュフローデータ（日次インデックスに補完済み想定）
        threshold: 簡易FCF閾値（デフォルト0.0）
        condition: 条件（above=閾値以上、below=閾値以下）
        consecutive_periods: 連続期間数（直近N回分の決算発表で条件を満たす必要がある）

    Returns:
        pd.Series[bool]: 条件を満たす場合にTrue

    Note:
        - 営業CFまたは投資CFがNaNの場合はFalseを返す
        - 簡易FCF = 営業CF + 投資CF（通常、投資CFは負の値）
        - consecutive_periods > 1 の場合、直近N回分の決算発表で条件を満たす必要がある
        - 推奨period_type: "FY"（CFデータはFYで一貫性あり）
    """
    fcf = operating_cash_flow + investing_cash_flow
    if consecutive_periods > 1:
        return _calc_consecutive_threshold_signal(
            fcf, threshold, condition, consecutive_periods
        )
    return _calc_threshold_signal(fcf, threshold, condition, require_positive=False)


def cfo_yield_threshold(
    close: pd.Series[float],
    operating_cash_flow: pd.Series[float],
    shares_outstanding: pd.Series[int],
    treasury_shares: pd.Series[int],
    threshold: float = 5.0,
    condition: Literal["above", "below"] = "above",
    use_floating_shares: bool = True,
) -> pd.Series[bool]:
    """
    CFO利回り（営業キャッシュフロー/時価総額）シグナル

    CFO利回り = (CFO / 時価総額) × 100 [%]
    時価総額 = 終値 × 株式数

    Args:
        close: 終値データ（日次）
        operating_cash_flow: 営業キャッシュフローデータ（日次インデックスに補完済み想定）
        shares_outstanding: 発行済み株式数（日次インデックスに補完済み想定）
        treasury_shares: 自己株式数（日次インデックスに補完済み想定）
        threshold: CFO利回り閾値（デフォルト5.0 = 5%）
        condition: 条件（above=閾値以上、below=閾値以下）
        use_floating_shares: 株式数の計算方法
            - True (デフォルト): 流通株式 = 発行済み - 自己株式
            - False: 発行済み株式全体

    Returns:
        pd.Series[bool]: 条件を満たす場合にTrue

    Note:
        - 株式数が0以下またはNaNの場合はFalseを返す
        - 時価総額が0以下またはNaNの場合はFalseを返す
        - CFOが負の場合も計算対象（負の利回りとなる）
        - 推奨period_type: "FY"（CFOデータはFYで一貫性あり）
    """
    market_cap = _calc_market_cap(
        close, shares_outstanding, treasury_shares, use_floating_shares
    )
    cfo_yield = (operating_cash_flow / market_cap.where(market_cap > 0, np.nan)) * 100

    return _calc_threshold_signal(cfo_yield, threshold, condition, require_positive=False)


def simple_fcf_yield_threshold(
    close: pd.Series[float],
    operating_cash_flow: pd.Series[float],
    investing_cash_flow: pd.Series[float],
    shares_outstanding: pd.Series[int],
    treasury_shares: pd.Series[int],
    threshold: float = 5.0,
    condition: Literal["above", "below"] = "above",
    use_floating_shares: bool = True,
) -> pd.Series[bool]:
    """
    簡易FCF利回り（(CFO+CFI)/時価総額）シグナル

    simple FCF利回り = ((CFO + CFI) / 時価総額) × 100 [%]
    時価総額 = 終値 × 株式数

    Args:
        close: 終値データ（日次）
        operating_cash_flow: 営業キャッシュフローデータ（日次インデックスに補完済み想定）
        investing_cash_flow: 投資キャッシュフローデータ（日次インデックスに補完済み想定）
        shares_outstanding: 発行済み株式数（日次インデックスに補完済み想定）
        treasury_shares: 自己株式数（日次インデックスに補完済み想定）
        threshold: 簡易FCF利回り閾値（デフォルト5.0 = 5%）
        condition: 条件（above=閾値以上、below=閾値以下）
        use_floating_shares: 株式数の計算方法
            - True (デフォルト): 流通株式 = 発行済み - 自己株式
            - False: 発行済み株式全体

    Returns:
        pd.Series[bool]: 条件を満たす場合にTrue

    Note:
        - 株式数が0以下またはNaNの場合はFalseを返す
        - 時価総額が0以下またはNaNの場合はFalseを返す
        - 簡易FCF = 営業CF + 投資CF（通常、投資CFは負の値）
        - 簡易FCFが負の場合も計算対象（負の利回りとなる）
        - 推奨period_type: "FY"（CFデータはFYで一貫性あり）
    """
    market_cap = _calc_market_cap(
        close, shares_outstanding, treasury_shares, use_floating_shares
    )
    fcf = operating_cash_flow + investing_cash_flow
    fcf_yield = (fcf / market_cap.where(market_cap > 0, np.nan)) * 100

    return _calc_threshold_signal(fcf_yield, threshold, condition, require_positive=False)
