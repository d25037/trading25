"""
PRIME ex TOPIX500 SMA-ratio rank / future close research analytics.

This module re-exports the shared SMA-ratio research workflow with the
PRIME ex TOPIX500 universe preset.
"""

from __future__ import annotations

from src.domains.analytics.topix100_sma_ratio_rank_future_close import (
    HORIZON_ORDER,
    METRIC_ORDER,
    QUARTILE_ORDER,
    RANKING_FEATURE_ORDER,
    Topix100SmaRatioRankFutureCloseResearchResult,
    get_prime_ex_topix500_sma_ratio_rank_future_close_available_date_range,
    run_prime_ex_topix500_sma_ratio_rank_future_close_research,
)

PrimeExTopix500SmaRatioRankFutureCloseResearchResult = (
    Topix100SmaRatioRankFutureCloseResearchResult
)

__all__ = [
    "HORIZON_ORDER",
    "METRIC_ORDER",
    "QUARTILE_ORDER",
    "RANKING_FEATURE_ORDER",
    "PrimeExTopix500SmaRatioRankFutureCloseResearchResult",
    "get_prime_ex_topix500_sma_ratio_rank_future_close_available_date_range",
    "run_prime_ex_topix500_sma_ratio_rank_future_close_research",
]
