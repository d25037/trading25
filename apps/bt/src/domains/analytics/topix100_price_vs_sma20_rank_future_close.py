"""
Backward-compatible TOPIX100 price-vs-SMA20 rank / future close research API.

The implementation now lives in `topix100_price_vs_sma_rank_future_close.py`,
which supports the wider `price / SMA20|50|100` family. This module keeps the
legacy SMA20-only entrypoint and constants for existing callers.
"""

from __future__ import annotations

from src.domains.analytics.topix100_price_vs_sma_rank_future_close import (
    COMBINED_BUCKET_LABEL_MAP as _COMBINED_BUCKET_LABEL_MAP,
    COMBINED_BUCKET_ORDER as _COMBINED_BUCKET_ORDER,
    GROUP_HYPOTHESIS_LABELS as _GROUP_HYPOTHESIS_LABELS,
    PRIMARY_VOLUME_FEATURE as _PRIMARY_VOLUME_FEATURE,
    PRIMARY_VOLUME_FEATURE_LABEL as _PRIMARY_VOLUME_FEATURE_LABEL,
    PRICE_BUCKET_LABEL_MAP as _PRICE_BUCKET_LABEL_MAP,
    SPLIT_HYPOTHESIS_LABELS as _SPLIT_HYPOTHESIS_LABELS,
    Topix100PriceVsSmaRankFutureCloseResearchResult as Topix100PriceVsSma20RankFutureCloseResearchResult,
    VOLUME_BUCKET_LABEL_MAP as _VOLUME_BUCKET_LABEL_MAP,
    get_topix100_price_vs_sma_rank_future_close_available_date_range,
    run_topix100_price_vs_sma_rank_future_close_research,
)

PRIMARY_PRICE_FEATURE = "price_vs_sma_20_gap"
PRIMARY_PRICE_FEATURE_LABEL = "Price vs SMA 20 Gap"
PRIMARY_VOLUME_FEATURE = _PRIMARY_VOLUME_FEATURE
PRIMARY_VOLUME_FEATURE_LABEL = _PRIMARY_VOLUME_FEATURE_LABEL
PRICE_BUCKET_LABEL_MAP = _PRICE_BUCKET_LABEL_MAP
VOLUME_BUCKET_LABEL_MAP = _VOLUME_BUCKET_LABEL_MAP
COMBINED_BUCKET_ORDER = _COMBINED_BUCKET_ORDER
COMBINED_BUCKET_LABEL_MAP = _COMBINED_BUCKET_LABEL_MAP
GROUP_HYPOTHESIS_LABELS = _GROUP_HYPOTHESIS_LABELS
SPLIT_HYPOTHESIS_LABELS = _SPLIT_HYPOTHESIS_LABELS


def get_topix100_price_vs_sma20_rank_future_close_available_date_range(
    db_path: str,
) -> tuple[str | None, str | None]:
    return get_topix100_price_vs_sma_rank_future_close_available_date_range(db_path)


def run_topix100_price_vs_sma20_rank_future_close_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    lookback_years: int = 10,
    min_constituents_per_day: int = 80,
) -> Topix100PriceVsSma20RankFutureCloseResearchResult:
    return run_topix100_price_vs_sma_rank_future_close_research(
        db_path,
        start_date=start_date,
        end_date=end_date,
        lookback_years=lookback_years,
        min_constituents_per_day=min_constituents_per_day,
        price_sma_windows=(20,),
    )
