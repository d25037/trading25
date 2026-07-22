"""Immutable application contract for one canonical Fundamentals PIT read."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from collections.abc import Sequence
from typing import Any

import pandas as pd

from src.infrastructure.external_api.jquants_client import StockInfo
from src.shared.fundamentals_pit_errors import (
    FundamentalsPitReason,
    FundamentalsPitSnapshotError,
)

__all__ = [
    "FundamentalsPitReason",
    "FundamentalsPitSnapshot",
    "FundamentalsPitSnapshotError",
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
