from __future__ import annotations

from src.infrastructure.db.market.market_mutations import MarketMutationStats


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
