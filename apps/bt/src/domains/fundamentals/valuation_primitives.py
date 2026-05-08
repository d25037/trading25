"""Shared primitive semantics for valuation calculations."""

from __future__ import annotations

import math

import numpy as np
import pandas as pd


def positive_ratio(
    numerator: float | None,
    denominator: float | None,
) -> float | None:
    """Return a finite positive ratio, or None for undefined valuation cases."""
    if numerator is None or denominator is None:
        return None
    if denominator <= 0:
        return None
    value = numerator / denominator
    return value if math.isfinite(value) and value > 0 else None


def valuation_ratio(
    price: float | None,
    per_share_value: float | None,
) -> float | None:
    return positive_ratio(price, per_share_value)


def valuation_ratio_series(
    price: pd.Series[float],
    per_share_value: pd.Series[float],
) -> pd.Series[float]:
    ratio = price / per_share_value.where(per_share_value > 0, np.nan)
    return ratio.where(np.isfinite(ratio) & (ratio > 0), np.nan)


def market_cap_from_price_and_shares(
    price: float | None,
    shares: float | None,
) -> float | None:
    if price is None or shares is None or price <= 0 or shares <= 0:
        return None
    value = price * shares
    return value if math.isfinite(value) else None
