"""Nikkei 225 benchmark readout for crowded-rerating Daily Ranking candidates."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Iterable, Sequence

import pandas as pd

from src.domains.analytics.earnings_holdthrough_expectancy_report import (
    _top_rows_for_markdown,
)
from src.domains.analytics.ranking_color_evidence import (
    DEFAULT_MARKET_SCOPES,
    DEFAULT_OBSERVATION_SAMPLE_LIMIT,
    DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
)
from src.domains.analytics.ranking_n225_neutral_rerating_benchmark import (
    DEFAULT_HORIZONS,
    DEFAULT_MIN_OBSERVATIONS_N225,
    RankingN225NeutralReratingBenchmarkResult,
    run_ranking_n225_neutral_rerating_benchmark_research,
)
from src.domains.analytics.research_bundle import ResearchBundleInfo, write_research_bundle

RANKING_N225_CROWDED_RERATING_BENCHMARK_EXPERIMENT_ID = (
    "market-behavior/ranking-n225-crowded-rerating-benchmark"
)
DEFAULT_LIQUIDITY_REGIMES: tuple[str, ...] = ("crowded_rerating",)


def run_ranking_n225_crowded_rerating_benchmark_research(
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
    result = run_ranking_n225_neutral_rerating_benchmark_research(
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
    return replace(
        result,
        signal_summary_df=_rename_all_signal(result.signal_summary_df),
        signal_benchmark_comparison_df=_rename_all_signal(
            result.signal_benchmark_comparison_df
        ),
        yearly_signal_summary_df=_rename_all_signal(result.yearly_signal_summary_df),
    )


def write_ranking_n225_crowded_rerating_benchmark_bundle(
    result: RankingN225NeutralReratingBenchmarkResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=RANKING_N225_CROWDED_RERATING_BENCHMARK_EXPERIMENT_ID,
        module="src.domains.analytics.ranking_n225_crowded_rerating_benchmark",
        function="run_ranking_n225_crowded_rerating_benchmark_research",
        params={
            "horizons": list(result.horizons),
            "market_scopes": list(result.market_scopes),
            "liquidity_regimes": list(result.liquidity_regimes),
            "min_observations": result.min_observations,
            "severe_loss_threshold_pct": result.severe_loss_threshold_pct,
        },
        db_path=result.db_path,
        analysis_start_date=result.analysis_start_date,
        analysis_end_date=result.analysis_end_date,
        result_metadata={
            "source_mode": str(result.source_mode),
            "source_detail": result.source_detail,
            "market_source": result.market_source,
            "observation_count": result.observation_count,
            "primary_outcome": "forward_close_n225_excess_return_{horizon}d_pct",
            "comparison_outcome": "forward_close_excess_return_{horizon}d_pct",
        },
        result_tables={
            "coverage_diagnostics_df": result.coverage_diagnostics_df,
            "signal_summary_df": result.signal_summary_df,
            "signal_benchmark_comparison_df": result.signal_benchmark_comparison_df,
            "yearly_signal_summary_df": result.yearly_signal_summary_df,
            "observation_sample_df": result.observation_sample_df,
        },
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def build_summary_markdown(result: RankingN225NeutralReratingBenchmarkResult) -> str:
    parts = [
        "# Ranking N225 Crowded Rerating Benchmark",
        "",
        "## Metadata",
        "",
        f"- db_path: `{result.db_path}`",
        f"- source_mode: `{result.source_mode}`",
        f"- source_detail: `{result.source_detail}`",
        f"- market_source: `{result.market_source}`",
        f"- analysis_start_date: `{result.analysis_start_date}`",
        f"- analysis_end_date: `{result.analysis_end_date}`",
        f"- horizons: `{', '.join(str(item) for item in result.horizons)}`",
        f"- market_scopes: `{', '.join(result.market_scopes)}`",
        f"- liquidity_regimes: `{', '.join(result.liquidity_regimes)}`",
        f"- observation_count: `{result.observation_count}`",
        "",
        "## Coverage Diagnostics",
        "",
        _top_rows_for_markdown(result.coverage_diagnostics_df, limit=30),
        "",
        "## Signal Summary",
        "",
        _top_rows_for_markdown(result.signal_summary_df, limit=120),
        "",
        "## Signal Benchmark Comparison",
        "",
        _top_rows_for_markdown(result.signal_benchmark_comparison_df, limit=160),
        "",
        "## Yearly Signal Summary",
        "",
        _top_rows_for_markdown(result.yearly_signal_summary_df, limit=160),
    ]
    return "\n".join(parts)


def _rename_all_signal(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "signal" not in frame.columns:
        return frame
    renamed = frame.copy()
    renamed.loc[renamed["signal"] == "neutral_all", "signal"] = "crowded_all"
    if "signal_label" in renamed.columns:
        renamed.loc[
            renamed["signal"] == "crowded_all",
            "signal_label",
        ] = "Crowded Rerating: all"
    return renamed
