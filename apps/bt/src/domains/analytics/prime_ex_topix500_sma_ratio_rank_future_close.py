"""
PRIME ex TOPIX500 SMA-ratio rank / future close research analytics.

This module re-exports the shared SMA-ratio research workflow with the
PRIME ex TOPIX500 universe preset.
"""

from __future__ import annotations

from src.domains.analytics.topix100_sma_ratio_rank_future_close import (
    DECILE_ORDER,
    HORIZON_ORDER,
    METRIC_ORDER,
    PRIME_EX_TOPIX500_SMA_RATIO_RESEARCH_EXPERIMENT_ID,
    RANKING_FEATURE_ORDER,
    Topix100SmaRatioRankFutureCloseResearchResult,
    get_prime_ex_topix500_sma_ratio_rank_future_close_bundle_path_for_run_id,
    get_prime_ex_topix500_sma_ratio_rank_future_close_available_date_range,
    get_prime_ex_topix500_sma_ratio_rank_future_close_latest_bundle_path,
    load_prime_ex_topix500_sma_ratio_rank_future_close_research_bundle,
    run_prime_ex_topix500_sma_ratio_rank_future_close_research,
    write_prime_ex_topix500_sma_ratio_rank_future_close_research_bundle,
)

PrimeExTopix500SmaRatioRankFutureCloseResearchResult = (
    Topix100SmaRatioRankFutureCloseResearchResult
)

__all__ = [
    "HORIZON_ORDER",
    "METRIC_ORDER",
    "DECILE_ORDER",
    "RANKING_FEATURE_ORDER",
    "PRIME_EX_TOPIX500_SMA_RATIO_RESEARCH_EXPERIMENT_ID",
    "PrimeExTopix500SmaRatioRankFutureCloseResearchResult",
    "get_prime_ex_topix500_sma_ratio_rank_future_close_bundle_path_for_run_id",
    "get_prime_ex_topix500_sma_ratio_rank_future_close_available_date_range",
    "get_prime_ex_topix500_sma_ratio_rank_future_close_latest_bundle_path",
    "load_prime_ex_topix500_sma_ratio_rank_future_close_research_bundle",
    "run_prime_ex_topix500_sma_ratio_rank_future_close_research",
    "write_prime_ex_topix500_sma_ratio_rank_future_close_research_bundle",
]
