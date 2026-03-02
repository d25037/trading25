"""
Screening result selection and sorting.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any, Literal, Protocol, TypeVar

from src.shared.models.signals import Signals

ScreeningSortBy = Literal["matchedDate", "stockCode", "bestStrategyScore", "matchStrategyCount"]
SortOrder = Literal["asc", "desc"]


class StockLike(Protocol):
    """Minimal stock fields required to build screening response items."""

    @property
    def code(self) -> str:
        ...

    @property
    def company_name(self) -> str:
        ...

    @property
    def scale_category(self) -> str | None:
        ...

    @property
    def sector_33_name(self) -> str | None:
        ...


class MatchedStrategyLike(Protocol):
    @property
    def strategyName(self) -> str:
        ...

    @property
    def matchedDate(self) -> str:
        ...

    @property
    def strategyScore(self) -> float | None:
        ...


class ScreeningResultLike(Protocol):
    @property
    def stockCode(self) -> str:
        ...

    @property
    def matchedDate(self) -> str:
        ...

    @property
    def bestStrategyScore(self) -> float | None:
        ...

    @property
    def matchStrategyCount(self) -> int:
        ...


ResultRow = TypeVar("ResultRow", bound=ScreeningResultLike)


def _default_format_date(value: Any) -> str:
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d")
    return str(value).split("T", 1)[0]


def find_recent_match_date(
    signals: Signals,
    recent_days: int,
    *,
    format_date: Callable[[Any], str] = _default_format_date,
) -> str | None:
    """Return latest date where entry is True and exit is False within recent window."""
    entries = signals.entries.fillna(False).astype(bool)
    exits = signals.exits.fillna(False).astype(bool)
    candidates = entries & (~exits)

    recent = candidates.tail(recent_days)
    if not recent.any():
        return None

    matched_index = recent[recent].index[-1]
    return format_date(matched_index)


def pick_best_strategy(matched_strategies: Sequence[MatchedStrategyLike]) -> MatchedStrategyLike:
    """Pick best strategy by score, fallback to latest matched date when all scores are null."""
    if not matched_strategies:
        raise ValueError("matched_strategies is empty")

    non_null = [item for item in matched_strategies if item.strategyScore is not None]
    if non_null:
        return max(
            non_null,
            key=lambda item: (
                item.strategyScore if item.strategyScore is not None else float("-inf"),
                item.strategyName,
            ),
        )

    return max(matched_strategies, key=lambda item: (item.matchedDate, item.strategyName))


def build_result_item(
    stock: StockLike,
    matched_date: str,
    matched_strategies: Sequence[MatchedStrategyLike],
) -> dict[str, Any]:
    """Build response item from per-stock aggregation."""
    sorted_strategies = sorted(
        matched_strategies,
        key=lambda item: (
            item.strategyScore is None,
            -(item.strategyScore or 0.0),
            item.strategyName,
        ),
    )
    best = pick_best_strategy(sorted_strategies)

    return {
        "stockCode": stock.code,
        "companyName": stock.company_name,
        "scaleCategory": stock.scale_category,
        "sector33Name": stock.sector_33_name,
        "matchedDate": matched_date,
        "bestStrategyName": best.strategyName,
        "bestStrategyScore": best.strategyScore,
        "matchStrategyCount": len(sorted_strategies),
        "matchedStrategies": sorted_strategies,
    }


def sort_results(
    results: list[ResultRow],
    sort_by: ScreeningSortBy,
    order: SortOrder,
) -> list[ResultRow]:
    """Sort screening results while keeping null scores always at tail."""
    if sort_by == "bestStrategyScore":
        if order == "asc":
            return sorted(
                results,
                key=lambda row: (
                    row.bestStrategyScore is None,
                    row.bestStrategyScore if row.bestStrategyScore is not None else float("inf"),
                    row.stockCode,
                ),
            )

        return sorted(
            results,
            key=lambda row: (
                row.bestStrategyScore is None,
                -(row.bestStrategyScore or 0.0),
                row.stockCode,
            ),
        )

    reverse = order == "desc"

    if sort_by == "matchedDate":
        return sorted(results, key=lambda row: (row.matchedDate, row.stockCode), reverse=reverse)

    if sort_by == "stockCode":
        return sorted(results, key=lambda row: row.stockCode, reverse=reverse)

    if sort_by == "matchStrategyCount":
        return sorted(
            results,
            key=lambda row: (row.matchStrategyCount, row.stockCode),
            reverse=reverse,
        )

    return results
