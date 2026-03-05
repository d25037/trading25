from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pandas as pd
import pytest

from src.domains.analytics.screening_results import (
    build_result_item,
    find_recent_match_date,
    pick_best_strategy,
    sort_results,
)
from src.entrypoints.http.schemas.screening import (
    MatchedStrategyItem,
    ScreeningResultItem,
)
from src.shared.models.signals import Signals


def test_find_recent_match_date_uses_entry_true_and_exit_false() -> None:
    index = pd.to_datetime(["2026-01-01", "2026-01-02", "2026-01-03"])
    signals = Signals(
        entries=pd.Series([True, True, True], index=index),
        exits=pd.Series([False, True, False], index=index),
    )

    assert find_recent_match_date(signals, recent_days=3) == "2026-01-03"


def test_find_recent_match_date_returns_none_when_recent_window_has_no_match() -> None:
    index = pd.to_datetime(["2026-01-01", "2026-01-02"])
    signals = Signals(
        entries=pd.Series([True, True], index=index),
        exits=pd.Series([True, True], index=index),
    )

    assert find_recent_match_date(signals, recent_days=2) is None


def test_pick_best_strategy_uses_latest_match_when_scores_are_all_null() -> None:
    items = [
        MatchedStrategyItem(strategyName="x", matchedDate="2026-01-01", strategyScore=None),
        MatchedStrategyItem(strategyName="y", matchedDate="2026-01-02", strategyScore=None),
    ]

    assert pick_best_strategy(items).strategyName == "y"


def test_pick_best_strategy_raises_for_empty_input() -> None:
    with pytest.raises(ValueError):
        pick_best_strategy([])


def test_build_result_item_sorts_matched_strategies_by_score_desc() -> None:
    stock = SimpleNamespace(
        code="7203",
        company_name="Toyota",
        scale_category="Large",
        sector_33_name="輸送用機器",
    )
    matched = [
        MatchedStrategyItem(strategyName="b", matchedDate="2026-01-03", strategyScore=None),
        MatchedStrategyItem(strategyName="a", matchedDate="2026-01-02", strategyScore=1.2),
        MatchedStrategyItem(strategyName="c", matchedDate="2026-01-01", strategyScore=0.8),
    ]

    item = build_result_item(stock, "2026-01-03", matched)

    assert item["stockCode"] == "7203"
    assert item["bestStrategyName"] == "a"
    assert [s.strategyName for s in item["matchedStrategies"]] == ["a", "c", "b"]


def test_sort_results_keeps_null_score_last_for_both_orders() -> None:
    scored = ScreeningResultItem(
        stockCode="1001",
        companyName="A",
        matchedDate="2026-01-03",
        bestStrategyName="s1",
        bestStrategyScore=1.2,
        matchStrategyCount=1,
        matchedStrategies=[MatchedStrategyItem(strategyName="s1", matchedDate="2026-01-03", strategyScore=1.2)],
    )
    missing = ScreeningResultItem(
        stockCode="1002",
        companyName="B",
        matchedDate="2026-01-04",
        bestStrategyName="s2",
        bestStrategyScore=None,
        matchStrategyCount=1,
        matchedStrategies=[MatchedStrategyItem(strategyName="s2", matchedDate="2026-01-04", strategyScore=None)],
    )

    desc = sort_results([missing, scored], "bestStrategyScore", "desc")
    asc = sort_results([missing, scored], "bestStrategyScore", "asc")

    assert [row.stockCode for row in desc] == ["1001", "1002"]
    assert [row.stockCode for row in asc] == ["1001", "1002"]


def test_sort_results_supports_non_score_keys_and_fallback() -> None:
    row1 = ScreeningResultItem(
        stockCode="1001",
        companyName="A",
        matchedDate="2026-01-03",
        bestStrategyName="s1",
        bestStrategyScore=1.0,
        matchStrategyCount=2,
        matchedStrategies=[],
    )
    row2 = ScreeningResultItem(
        stockCode="1002",
        companyName="B",
        matchedDate="2026-01-01",
        bestStrategyName="s2",
        bestStrategyScore=2.0,
        matchStrategyCount=1,
        matchedStrategies=[],
    )

    by_date = sort_results([row1, row2], "matchedDate", "asc")
    by_code = sort_results([row2, row1], "stockCode", "desc")
    by_count = sort_results([row2, row1], "matchStrategyCount", "desc")
    passthrough = sort_results([row1], cast(Any, "unknown"), "asc")

    assert [row.stockCode for row in by_date] == ["1002", "1001"]
    assert [row.stockCode for row in by_code] == ["1002", "1001"]
    assert [row.stockCode for row in by_count] == ["1001", "1002"]
    assert passthrough == [row1]
