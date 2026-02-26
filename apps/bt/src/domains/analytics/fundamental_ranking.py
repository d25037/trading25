"""Fundamental ranking domain calculations."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

from src.shared.models.types import normalize_period_type
from src.shared.utils.share_adjustment import (
    is_valid_share_count as _is_valid_share_count_shared,
    resolve_latest_quarterly_baseline_shares,
)


_QUARTER_PERIODS = {"1Q", "2Q", "3Q"}


@dataclass
class StatementRow:
    code: str
    disclosed_date: str
    period_type: str
    earnings_per_share: float | None
    forecast_eps: float | None
    next_year_forecast_earnings_per_share: float | None
    shares_outstanding: float | None
    fy_cycle_key: str | None = None


@dataclass
class ForecastValue:
    value: float
    disclosed_date: str
    period_type: str
    source: Literal["revised", "fy"]


@dataclass
class LatestFyRow:
    disclosed_date: str
    period_type: str
    shares_outstanding: float | None
    forecast_value: float | None


@dataclass
class FundamentalItem:
    code: str
    company_name: str
    market_code: str
    sector_33_name: str
    current_price: float
    volume: float
    eps_value: float
    disclosed_date: str
    period_type: str
    source: Literal["revised", "fy"]


def normalize_period_label(period_type: str | None) -> str:
    normalized = normalize_period_type(period_type)
    if normalized is None:
        return ""
    return normalized


def round_eps(value: float) -> float:
    return round(value, 2)


def round_ratio(value: float) -> float:
    return round(value, 4)


def is_valid_share_count(value: float | None) -> bool:
    return _is_valid_share_count_shared(value)


def adjust_per_share_value(
    raw_value: float | None,
    current_shares: float | None,
    baseline_shares: float | None,
) -> float | None:
    if raw_value is None:
        return None
    if not (is_valid_share_count(current_shares) and is_valid_share_count(baseline_shares)):
        return round_eps(raw_value)
    assert current_shares is not None
    assert baseline_shares is not None
    adjusted = raw_value * (current_shares / baseline_shares)
    return round_eps(adjusted)


def to_nullable_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def resolve_fy_cycle_key(disclosed_date: str) -> str:
    try:
        return datetime.fromisoformat(disclosed_date).strftime("%Y")
    except ValueError:
        return disclosed_date


def calculate_eps_ratio(forecast_value: float, actual_value: float) -> float | None:
    if not math.isfinite(forecast_value) or not math.isfinite(actual_value):
        return None
    if math.isclose(actual_value, 0.0, abs_tol=1e-12):
        return None
    ratio = forecast_value / actual_value
    if not math.isfinite(ratio):
        return None
    return round_ratio(ratio)


class FundamentalRankingCalculator:
    def resolve_baseline_shares(self, rows: list[StatementRow]) -> float | None:
        snapshots = [
            (row.period_type, row.disclosed_date, row.shares_outstanding)
            for row in rows
        ]
        return resolve_latest_quarterly_baseline_shares(snapshots)

    def resolve_latest_actual_snapshot(
        self,
        rows: list[StatementRow],
        baseline_shares: float | None,
    ) -> ForecastValue | None:
        sorted_rows = sorted(rows, key=lambda row: row.disclosed_date, reverse=True)
        for row in sorted_rows:
            if row.period_type != "FY":
                continue
            adjusted = adjust_per_share_value(
                row.earnings_per_share,
                row.shares_outstanding,
                baseline_shares,
            )
            if adjusted is None:
                continue
            return ForecastValue(
                value=adjusted,
                disclosed_date=row.disclosed_date,
                period_type=row.period_type,
                source="fy",
            )
        return None

    def resolve_recent_actual_eps_max(
        self,
        rows: list[StatementRow],
        baseline_shares: float | None,
        lookback_fy_count: int,
    ) -> float | None:
        if lookback_fy_count < 1:
            raise ValueError("lookback_fy_count must be >= 1")

        sorted_rows = sorted(rows, key=lambda row: row.disclosed_date, reverse=True)
        recent_values: list[float] = []
        seen_cycles: set[str] = set()

        for row in sorted_rows:
            if row.period_type != "FY":
                continue
            cycle_key = row.fy_cycle_key or row.disclosed_date
            if cycle_key in seen_cycles:
                continue
            adjusted = adjust_per_share_value(
                row.earnings_per_share,
                row.shares_outstanding,
                baseline_shares,
            )
            if adjusted is None:
                continue
            seen_cycles.add(cycle_key)
            recent_values.append(adjusted)
            if len(recent_values) >= lookback_fy_count:
                break

        if len(recent_values) < lookback_fy_count:
            return None
        return max(recent_values)

    def resolve_latest_fy_row(self, rows: list[StatementRow]) -> LatestFyRow | None:
        sorted_rows = sorted(rows, key=lambda row: row.disclosed_date, reverse=True)
        for row in sorted_rows:
            if row.period_type != "FY":
                continue
            forecast_value = (
                row.next_year_forecast_earnings_per_share
                if row.next_year_forecast_earnings_per_share is not None
                else row.forecast_eps
            )
            return LatestFyRow(
                disclosed_date=row.disclosed_date,
                period_type=row.period_type,
                shares_outstanding=row.shares_outstanding,
                forecast_value=forecast_value,
            )
        return None

    def resolve_latest_fy_forecast_snapshot(
        self,
        fy_row: LatestFyRow | None,
        baseline_shares: float | None,
    ) -> ForecastValue | None:
        if fy_row is None:
            return None
        adjusted = adjust_per_share_value(
            fy_row.forecast_value,
            fy_row.shares_outstanding,
            baseline_shares,
        )
        if adjusted is None:
            return None
        return ForecastValue(
            value=adjusted,
            disclosed_date=fy_row.disclosed_date,
            period_type=fy_row.period_type,
            source="fy",
        )

    def resolve_latest_revised_forecast_snapshot(
        self,
        rows: list[StatementRow],
        baseline_shares: float | None,
        fy_disclosed_date: str,
    ) -> ForecastValue | None:
        sorted_rows = sorted(rows, key=lambda row: row.disclosed_date, reverse=True)
        for row in sorted_rows:
            if row.period_type not in _QUARTER_PERIODS:
                continue
            if row.disclosed_date <= fy_disclosed_date:
                continue
            raw_revised = (
                row.forecast_eps
                if row.forecast_eps is not None
                else row.next_year_forecast_earnings_per_share
            )
            adjusted = adjust_per_share_value(
                raw_revised,
                row.shares_outstanding,
                baseline_shares,
            )
            if adjusted is None:
                continue
            return ForecastValue(
                value=adjusted,
                disclosed_date=row.disclosed_date,
                period_type=row.period_type,
                source="revised",
            )
        return None

    def resolve_latest_forecast_snapshot(
        self,
        rows: list[StatementRow],
        baseline_shares: float | None,
    ) -> ForecastValue | None:
        latest_fy_row = self.resolve_latest_fy_row(rows)
        if latest_fy_row is None:
            return None

        revised = self.resolve_latest_revised_forecast_snapshot(
            rows,
            baseline_shares,
            latest_fy_row.disclosed_date,
        )
        if revised is not None:
            return revised
        return self.resolve_latest_fy_forecast_snapshot(latest_fy_row, baseline_shares)

    def resolve_latest_ratio_snapshot(
        self,
        actual_snapshot: ForecastValue | None,
        forecast_snapshot: ForecastValue | None,
    ) -> ForecastValue | None:
        if actual_snapshot is None or forecast_snapshot is None:
            return None

        ratio = calculate_eps_ratio(
            forecast_value=forecast_snapshot.value,
            actual_value=actual_snapshot.value,
        )
        if ratio is None:
            return None

        return ForecastValue(
            value=ratio,
            disclosed_date=forecast_snapshot.disclosed_date,
            period_type=forecast_snapshot.period_type,
            source=forecast_snapshot.source,
        )

    def build_fundamental_item(
        self,
        stock_row: Any,
        snapshot: ForecastValue,
    ) -> FundamentalItem:
        return FundamentalItem(
            code=str(stock_row["code"]),
            company_name=str(stock_row["company_name"]),
            market_code=str(stock_row["market_code"]),
            sector_33_name=str(stock_row["sector_33_name"]),
            current_price=float(stock_row["current_price"]),
            volume=float(stock_row["volume"]),
            eps_value=snapshot.value,
            disclosed_date=snapshot.disclosed_date,
            period_type=snapshot.period_type,
            source=snapshot.source,
        )

    def rank_fundamental_items(
        self,
        items: list[FundamentalItem],
        limit: int,
        *,
        descending: bool,
    ) -> list[FundamentalItem]:
        if descending:
            sorted_items = sorted(items, key=lambda item: (-item.eps_value, item.code))
        else:
            sorted_items = sorted(items, key=lambda item: (item.eps_value, item.code))
        return sorted_items[:limit]
