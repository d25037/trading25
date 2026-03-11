"""
共通財務計算関数

signal関数とserver serviceの両方から呼ばれる計算ロジック。
"""

from __future__ import annotations

from typing import TYPE_CHECKING, TypeAlias

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    NumericSeries: TypeAlias = pd.Series[float] | pd.Series[int]
else:
    NumericSeries = pd.Series


def calc_market_cap(
    close: pd.Series[float],
    shares_outstanding: NumericSeries,
    treasury_shares: NumericSeries,
    use_floating_shares: bool = True,
) -> pd.Series[float]:
    """時価総額を計算（ベクトル化版: シグナル・バックテスト用）

    Args:
        close: 終値（日次）
        shares_outstanding: 発行済み株式数
        treasury_shares: 自己株式数
        use_floating_shares: True=流通株式(発行済み-自己株式), False=発行済み全体

    Returns:
        時価総額（円）。株式数が0以下の場合はNaN。
    """
    shares_outstanding_float = pd.to_numeric(shares_outstanding, errors="coerce").astype(float)
    treasury_shares_float = pd.to_numeric(treasury_shares, errors="coerce").astype(float)

    shares = (
        shares_outstanding_float - treasury_shares_float.fillna(0.0)
        if use_floating_shares
        else shares_outstanding_float
    )
    return close * shares.where(shares > 0, np.nan)


def calc_market_cap_scalar(
    stock_price: float,
    shares_outstanding: float,
    treasury_shares: float | None = None,
) -> float | None:
    """時価総額を計算（スカラー版: APIサーバー用）

    Args:
        stock_price: 株価
        shares_outstanding: 発行済み株式数
        treasury_shares: 自己株式数（Noneの場合は0扱い）

    Returns:
        時価総額（円）。無効な場合はNone。
    """
    if stock_price <= 0 or shares_outstanding is None:
        return None
    actual_shares = shares_outstanding - (treasury_shares or 0)
    if actual_shares <= 0:
        return None
    return stock_price * actual_shares
