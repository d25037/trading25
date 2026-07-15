from __future__ import annotations

import pytest

from src.infrastructure.db.market.market_mutations import (
    MarketMutationStats,
    SemanticDeltaResult,
    deterministic_last_wins,
)


def test_market_mutation_stats_reports_only_rows_that_changed_storage() -> None:
    stats = MarketMutationStats(
        input=11,
        inserted=3,
        updated=2,
        unchanged=6,
        deleted=4,
    )

    assert stats.mutated_rows == 9


def test_market_mutation_stats_empty_result_is_explicit() -> None:
    assert MarketMutationStats.empty().mutated_rows == 0
    assert MarketMutationStats.empty().input == 0


def test_deterministic_last_wins_replaces_duplicate_without_reordering_keys() -> None:
    rows = [
        {"code": "7203", "date": "2026-01-01", "value": 1},
        {"code": "6758", "date": "2026-01-01", "value": 2},
        {"code": "7203", "date": "2026-01-01", "value": 3},
    ]

    assert deterministic_last_wins(rows, key_columns=("code", "date")) == [
        {"code": "7203", "date": "2026-01-01", "value": 3},
        {"code": "6758", "date": "2026-01-01", "value": 2},
    ]


def test_semantic_delta_result_exposes_exact_mutated_keys_dates_and_codes() -> None:
    result = SemanticDeltaResult(
        stats=MarketMutationStats(input=3, inserted=1, updated=1, unchanged=1, deleted=0),
        inserted_keys=(("7203", "2026-01-01"),),
        updated_keys=(("6758", "2026-01-02"),),
        affected_dates=frozenset({"2026-01-01", "2026-01-02"}),
        affected_codes=frozenset({"7203", "6758"}),
    )

    assert result.mutated_rows == 2
    assert result.mutated_keys == (
        ("7203", "2026-01-01"),
        ("6758", "2026-01-02"),
    )


def test_market_mutation_stats_rejects_unclassified_input_rows() -> None:
    with pytest.raises(ValueError, match=r"inserted \+ updated \+ unchanged"):
        MarketMutationStats(input=2, inserted=1, updated=0, unchanged=0, deleted=0)
