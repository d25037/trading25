"""
財務指標シグナル — バリュエーション系

PER・PBR・B/M・PEGに基づく割安判定シグナルを提供
"""

from __future__ import annotations

from typing import Literal

import numpy as np
import pandas as pd

from .fundamental_helpers import _calc_ratio_signal


def is_undervalued_by_per(
    close: pd.Series[float],
    eps: pd.Series[float],
    threshold: float = 15.0,
    condition: Literal["above", "below"] = "below",
    exclude_negative: bool = True,
) -> pd.Series[bool]:
    """
    PER（Price-to-Earnings Ratio）シグナル

    株価がEPSの何倍で取引されているかを示すPERを計算し、
    指定した条件で閾値と比較してTrueを返すシグナル

    Args:
        close: 終値データ（日次）
        eps: EPS（1株当たり利益）データ（日次インデックスに補完済み想定）
        threshold: PER閾値（デフォルト15.0）
        condition: 条件（below=閾値以下、above=閾値以上）
        exclude_negative: 負のPER（損失企業）を除外するか（デフォルトTrue）

    Returns:
        pd.Series[bool]: 条件を満たす場合にTrue

    Note:
        - EPSが0以下またはNaNの場合はFalseを返す
        - exclude_negative=Trueの場合、PERが負の値はFalseを返す（損失企業除外）
        - 推奨period_type: "FY"（通期EPSベースで計算）
    """
    per = close / eps.where(eps > 0, np.nan)
    return _calc_ratio_signal(per, threshold, condition, exclude_negative)


def is_undervalued_by_pbr(
    close: pd.Series[float],
    bps: pd.Series[float],
    threshold: float = 1.0,
    condition: Literal["above", "below"] = "below",
    exclude_negative: bool = True,
) -> pd.Series[bool]:
    """
    PBR（Price-to-Book Ratio）シグナル

    株価がBPS（1株当たり純資産）の何倍で取引されているかを示すPBRを計算し、
    指定した条件で閾値と比較してTrueを返すシグナル

    Args:
        close: 終値データ
        bps: BPS（1株当たり純資産）データ
        threshold: PBR閾値（デフォルト1.0）
        condition: 条件（below=閾値以下、above=閾値以上）
        exclude_negative: 負のPBR（債務超過企業）を除外するか（デフォルトTrue）

    Returns:
        pd.Series[bool]: 条件を満たす場合にTrue

    Note:
        - BPSが0以下またはNaNの場合はFalseを返す
        - インデックスが一致しない場合は自動的にアライメントされる
        - exclude_negative=Trueの場合、PBRが負の値はFalseを返す（債務超過企業除外）
        - 推奨period_type: "FY"（BPSはFYのみで発表）
    """
    common_index = close.index.intersection(bps.index)
    close_common = close.reindex(common_index)
    bps_common = bps.reindex(common_index)

    pbr = close_common / bps_common.where(bps_common > 0, np.nan)
    pbr_signal = _calc_ratio_signal(pbr, threshold, condition, exclude_negative)

    result = pd.Series(False, index=close.index)
    result.loc[common_index] = pbr_signal
    return result.fillna(False)


def is_high_book_to_market(
    close: pd.Series[float],
    bps: pd.Series[float],
    threshold: float = 1.0,
    condition: Literal["above", "below"] = "above",
    exclude_negative: bool = True,
) -> pd.Series[bool]:
    """
    B/M（Book-to-Market Ratio）シグナル

    B/M = BPS / Close を計算し、指定した条件で閾値と比較してTrueを返すシグナル

    Args:
        close: 終値データ
        bps: BPS（1株当たり純資産）データ
        threshold: B/M閾値（デフォルト1.0）
        condition: 条件（above=閾値以上、below=閾値以下）
        exclude_negative: 負のB/Mを除外するか（デフォルトTrue）

    Returns:
        pd.Series[bool]: 条件を満たす場合にTrue

    Note:
        - Closeが0以下またはNaNの場合はFalseを返す
        - BPSが0以下またはNaNの場合はFalseを返す
    """
    common_index = close.index.intersection(bps.index)
    close_common = close.reindex(common_index)
    bps_common = bps.reindex(common_index)

    valid_bps = bps_common.where(bps_common > 0, np.nan)
    valid_close = close_common.where(close_common > 0, np.nan)
    book_to_market = valid_bps / valid_close
    bm_signal = _calc_ratio_signal(book_to_market, threshold, condition, exclude_negative)

    result = pd.Series(False, index=close.index)
    result.loc[common_index] = bm_signal
    return result.fillna(False)


def is_undervalued_growth_by_peg(
    close: pd.Series[float],
    eps: pd.Series[float],
    next_year_forecast_eps: pd.Series[float],
    threshold: float = 1.0,
    condition: Literal["above", "below"] = "below",
) -> pd.Series[bool]:
    """
    PEG Ratio（Price/Earnings to Growth）シグナル

    PEG Ratio = (株価 / 現在EPS) / EPS成長率 を計算し、
    指定した条件で閾値と比較してTrueを返すシグナル

    Args:
        close: 終値データ（日次）
        eps: 現在EPS（1株当たり利益）データ（日次インデックスに補完済み想定）
        next_year_forecast_eps: 来年予想EPS（日次インデックスに補完済み想定）
        threshold: PEG Ratio閾値（デフォルト1.0）
        condition: 条件（below=閾値以下、above=閾値以上）

    Returns:
        pd.Series[bool]: 条件を満たす場合にTrue

    Note:
        - 現在EPSまたは来年予想EPSが0以下またはNaNの場合はFalseを返す
        - EPS成長率が0以下（負成長）の場合はFalseを返す
        - 推奨period_type: "FY"（NextYearForecastEPSはFYのみで発表）
    """
    valid_eps = eps.where(eps > 0, np.nan)
    eps_growth_rate = (next_year_forecast_eps - eps) / valid_eps
    per = close / valid_eps
    peg_ratio = per / eps_growth_rate.where(eps_growth_rate > 0, np.nan)
    return _calc_ratio_signal(peg_ratio, threshold, condition)
