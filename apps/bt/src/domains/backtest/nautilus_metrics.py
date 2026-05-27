"""Small metric helpers for Nautilus verification artifacts."""

from __future__ import annotations

import math
from typing import Any

import pandas as pd


def annualized_sharpe_ratio(returns: pd.Series) -> float | None:
    non_null = returns.dropna()
    if non_null.empty:
        return 0.0
    std = float(non_null.std(ddof=0))
    if std <= 0.0:
        return 0.0
    return float((non_null.mean() / std) * math.sqrt(252.0))


def annualized_sortino_ratio(returns: pd.Series) -> float | None:
    non_null = returns.dropna()
    if non_null.empty:
        return None
    downside = non_null[non_null < 0.0]
    downside_std = float(downside.std(ddof=0)) if not downside.empty else 0.0
    if downside_std <= 0.0:
        return None
    return float((non_null.mean() / downside_std) * math.sqrt(252.0))


def profit_factor(trade_records: list[dict[str, Any]]) -> float | None:
    gross_profit = sum(float(record["pnl"]) for record in trade_records if float(record["pnl"]) > 0.0)
    gross_loss = abs(sum(float(record["pnl"]) for record in trade_records if float(record["pnl"]) < 0.0))
    if gross_profit <= 0.0 and gross_loss <= 0.0:
        return None
    if gross_loss <= 0.0:
        return None
    return gross_profit / gross_loss
