"""Immutable application contract for one canonical Fundamentals PIT read."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Literal
from collections.abc import Sequence

import pandas as pd

from src.infrastructure.external_api.jquants_client import StockInfo


FundamentalsPitReason = Literal[
    "stock_not_listed_as_of",
    "provider_window_required",
    "current_adjusted_metrics_required",
    "stock_master_snapshot_required",
    "pit_snapshot_inconsistent",
]


@dataclass(frozen=True)
class FundamentalsPitSnapshot:
    requested_cutoff_date: date | None
    knowledge_cutoff_date: date
    effective_market_date: date
    stock_master_snapshot_date: date
    fundamentals_adjustment_basis_date: date
    provider_as_of: str
    provider_coverage_start: date
    provider_coverage_end: date
    stock_info: StockInfo
    statements: pd.DataFrame
    adjusted_statement_metrics: Sequence[dict[str, Any]]
    daily_valuation: Sequence[dict[str, Any]]
    ohlcv: pd.DataFrame
    prime_liquidity_panel: pd.DataFrame


class FundamentalsPitSnapshotError(RuntimeError):
    def __init__(self, reason: FundamentalsPitReason, message: str) -> None:
        self.reason = reason
        super().__init__(message)
