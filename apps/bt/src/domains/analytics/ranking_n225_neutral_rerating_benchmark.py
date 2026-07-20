"""Neutral-rerating public variant of the shared Nikkei 225 benchmark."""

from pathlib import Path
from typing import Iterable, Sequence

from src.domains.analytics.ranking_n225_rerating_benchmark_support import (
    DEFAULT_HORIZONS,
    DEFAULT_LIQUIDITY_REGIMES,
    DEFAULT_MARKET_SCOPES,
    DEFAULT_MIN_OBSERVATIONS_N225,
    DEFAULT_OBSERVATION_SAMPLE_LIMIT,
    DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    RANKING_N225_NEUTRAL_RERATING_BENCHMARK_ID,
    RankingN225NeutralReratingBenchmarkResult,
    build_summary_markdown,
    run_ranking_n225_rerating_benchmark_research,
    write_ranking_n225_neutral_rerating_benchmark_bundle,
)

RANKING_N225_NEUTRAL_RERATING_BENCHMARK_EXPERIMENT_ID = (
    RANKING_N225_NEUTRAL_RERATING_BENCHMARK_ID
)


def run_ranking_n225_neutral_rerating_benchmark_research(
    db_path: str | Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    market_scopes: Sequence[str] = DEFAULT_MARKET_SCOPES,
    liquidity_regimes: Sequence[str] = DEFAULT_LIQUIDITY_REGIMES,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS_N225,
    severe_loss_threshold_pct: float = DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    observation_sample_limit: int = DEFAULT_OBSERVATION_SAMPLE_LIMIT,
) -> RankingN225NeutralReratingBenchmarkResult:
    return run_ranking_n225_rerating_benchmark_research(
        db_path,
        start_date=start_date,
        end_date=end_date,
        horizons=horizons,
        market_scopes=market_scopes,
        liquidity_regimes=liquidity_regimes,
        min_observations=min_observations,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
        observation_sample_limit=observation_sample_limit,
    )


__all__ = [
    "DEFAULT_HORIZONS",
    "DEFAULT_LIQUIDITY_REGIMES",
    "DEFAULT_MARKET_SCOPES",
    "DEFAULT_MIN_OBSERVATIONS_N225",
    "DEFAULT_OBSERVATION_SAMPLE_LIMIT",
    "DEFAULT_SEVERE_LOSS_THRESHOLD_PCT",
    "RANKING_N225_NEUTRAL_RERATING_BENCHMARK_EXPERIMENT_ID",
    "RankingN225NeutralReratingBenchmarkResult",
    "build_summary_markdown",
    "run_ranking_n225_neutral_rerating_benchmark_research",
    "write_ranking_n225_neutral_rerating_benchmark_bundle",
]
