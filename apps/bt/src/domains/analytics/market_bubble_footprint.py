"""Public experiment facade for market-bubble footprint research."""

from src.domains.analytics.market_bubble_footprint_support import (
    BUBBLE_FOOTPRINT_ID,
    DEFAULT_FOOTPRINT_HORIZONS,
    DEFAULT_MARKET_SCOPES,
    DEFAULT_MIN_OBSERVATIONS,
    DEFAULT_OBSERVATION_SAMPLE_LIMIT,
    DEFAULT_RERATING_SIGNAL_HORIZONS,
    DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    RERATING_BUBBLE_REGIME_ID,
    BubbleFootprintResult,
    Frequency,
    ReratingBubbleRegimeResult,
    build_bubble_footprint_summary_markdown,
    build_rerating_bubble_regime_summary_markdown,
    run_market_bubble_footprint_research,
    run_rerating_bubble_regime_forward_response_research,
    write_bubble_footprint_bundle,
    write_rerating_bubble_regime_bundle,
)

BUBBLE_FOOTPRINT_EXPERIMENT_ID = BUBBLE_FOOTPRINT_ID
RERATING_BUBBLE_REGIME_EXPERIMENT_ID = RERATING_BUBBLE_REGIME_ID

__all__ = [
    "BUBBLE_FOOTPRINT_EXPERIMENT_ID",
    "DEFAULT_FOOTPRINT_HORIZONS",
    "DEFAULT_MARKET_SCOPES",
    "DEFAULT_MIN_OBSERVATIONS",
    "DEFAULT_OBSERVATION_SAMPLE_LIMIT",
    "DEFAULT_RERATING_SIGNAL_HORIZONS",
    "DEFAULT_SEVERE_LOSS_THRESHOLD_PCT",
    "RERATING_BUBBLE_REGIME_EXPERIMENT_ID",
    "BubbleFootprintResult",
    "Frequency",
    "ReratingBubbleRegimeResult",
    "build_bubble_footprint_summary_markdown",
    "build_rerating_bubble_regime_summary_markdown",
    "run_market_bubble_footprint_research",
    "run_rerating_bubble_regime_forward_response_research",
    "write_bubble_footprint_bundle",
    "write_rerating_bubble_regime_bundle",
]
