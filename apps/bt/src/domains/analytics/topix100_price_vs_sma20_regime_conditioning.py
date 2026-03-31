"""
TOPIX100 price-vs-20SMA regime conditioning research analytics.

This conditions the `price_vs_sma_20_gap x volume_sma_20_80` bucket discussion
on same-day TOPIX close-return and NT-ratio-return regimes.
"""

from __future__ import annotations

from dataclasses import asdict, fields
from pathlib import Path
from typing import Any

import pandas as pd

from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    find_latest_research_bundle_path,
    get_research_bundle_dir,
    load_research_bundle_info,
    load_research_bundle_tables,
    write_research_bundle,
)
from src.domains.analytics.topix100_price_vs_sma20_rank_future_close import (
    run_topix100_price_vs_sma20_rank_future_close_research,
)
from src.domains.analytics.topix_regime_conditioning_core import (
    DEFAULT_SIGMA_THRESHOLD_1,
    DEFAULT_SIGMA_THRESHOLD_2,
    _build_horizon_panel,
    _build_regime_assignments_df,
    _build_regime_daily_means,
    _build_regime_day_counts,
    _build_regime_group_daily_means,
    _build_regime_group_day_counts,
    _build_regime_group_hypothesis,
    _build_regime_group_pairwise_significance,
    _build_regime_hypothesis,
    _build_regime_market_df,
    _build_regime_pairwise_significance,
    _query_market_regime_history,
    _summarize_regime_daily_means,
    _summarize_regime_group_daily_means,
    NtRatioReturnStats,
    TopixRegimeConditioningResearchResult,
    TopixCloseReturnStats,
    REGIME_TYPE_ORDER as _REGIME_TYPE_ORDER,
)
from src.domains.analytics.topix_close_stock_overnight_distribution import (
    _open_analysis_connection,
)

REGIME_TYPE_ORDER = _REGIME_TYPE_ORDER
TOPIX100_PRICE_VS_SMA20_REGIME_RESEARCH_EXPERIMENT_ID = (
    "market-behavior/topix100-price-vs-sma20-regime-conditioning"
)


def run_topix100_price_vs_sma20_regime_conditioning_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    lookback_years: int = 10,
    min_constituents_per_day: int = 80,
    sigma_threshold_1: float = DEFAULT_SIGMA_THRESHOLD_1,
    sigma_threshold_2: float = DEFAULT_SIGMA_THRESHOLD_2,
) -> TopixRegimeConditioningResearchResult:
    if sigma_threshold_1 <= 0:
        raise ValueError("sigma_threshold_1 must be positive")
    if sigma_threshold_2 <= sigma_threshold_1:
        raise ValueError("sigma_threshold_2 must be greater than sigma_threshold_1")

    base_result = run_topix100_price_vs_sma20_rank_future_close_research(
        db_path,
        start_date=start_date,
        end_date=end_date,
        lookback_years=lookback_years,
        min_constituents_per_day=min_constituents_per_day,
    )

    with _open_analysis_connection(db_path) as ctx:
        raw_market_df = _query_market_regime_history(
            ctx.connection,
            end_date=base_result.analysis_end_date,
        )
        regime_market_df, topix_close_stats, nt_ratio_stats = _build_regime_market_df(
            raw_market_df,
            start_date=base_result.analysis_start_date,
            end_date=base_result.analysis_end_date,
            sigma_threshold_1=sigma_threshold_1,
            sigma_threshold_2=sigma_threshold_2,
        )

    regime_assignments_df = _build_regime_assignments_df(regime_market_df)
    regime_day_counts_df = _build_regime_day_counts(regime_assignments_df)
    regime_group_day_counts_df = _build_regime_group_day_counts(regime_assignments_df)
    split_panel_df = base_result.price_volume_split_panel_df.copy()
    horizon_panel_df = _build_horizon_panel(split_panel_df)
    regime_daily_means_df = _build_regime_daily_means(
        horizon_panel_df,
        regime_assignments_df,
    )
    regime_summary_df = _summarize_regime_daily_means(regime_daily_means_df)
    regime_pairwise_significance_df = _build_regime_pairwise_significance(
        regime_daily_means_df
    )
    regime_hypothesis_df = _build_regime_hypothesis(regime_pairwise_significance_df)
    regime_group_daily_means_df = _build_regime_group_daily_means(
        horizon_panel_df,
        regime_assignments_df,
    )
    regime_group_summary_df = _summarize_regime_group_daily_means(
        regime_group_daily_means_df
    )
    regime_group_pairwise_significance_df = _build_regime_group_pairwise_significance(
        regime_group_daily_means_df
    )
    regime_group_hypothesis_df = _build_regime_group_hypothesis(
        regime_group_pairwise_significance_df
    )

    return TopixRegimeConditioningResearchResult(
        db_path=db_path,
        source_mode=base_result.source_mode,
        source_detail=base_result.source_detail,
        available_start_date=base_result.available_start_date,
        available_end_date=base_result.available_end_date,
        analysis_start_date=base_result.analysis_start_date,
        analysis_end_date=base_result.analysis_end_date,
        sigma_threshold_1=sigma_threshold_1,
        sigma_threshold_2=sigma_threshold_2,
        universe_constituent_count=base_result.topix100_constituent_count,
        valid_date_count=base_result.valid_date_count,
        topix_close_stats=topix_close_stats,
        nt_ratio_stats=nt_ratio_stats,
        regime_market_df=regime_market_df,
        regime_day_counts_df=regime_day_counts_df,
        regime_group_day_counts_df=regime_group_day_counts_df,
        split_panel_df=split_panel_df,
        horizon_panel_df=horizon_panel_df,
        regime_daily_means_df=regime_daily_means_df,
        regime_summary_df=regime_summary_df,
        regime_pairwise_significance_df=regime_pairwise_significance_df,
        regime_hypothesis_df=regime_hypothesis_df,
        regime_group_daily_means_df=regime_group_daily_means_df,
        regime_group_summary_df=regime_group_summary_df,
        regime_group_pairwise_significance_df=regime_group_pairwise_significance_df,
        regime_group_hypothesis_df=regime_group_hypothesis_df,
    )


def write_topix100_price_vs_sma20_regime_conditioning_research_bundle(
    result: TopixRegimeConditioningResearchResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    result_metadata, result_tables = _split_price_vs_sma20_regime_result_payload(result)
    return write_research_bundle(
        experiment_id=TOPIX100_PRICE_VS_SMA20_REGIME_RESEARCH_EXPERIMENT_ID,
        module=__name__,
        function="run_topix100_price_vs_sma20_regime_conditioning_research",
        params={
            "start_date": result.analysis_start_date,
            "end_date": result.analysis_end_date,
            "sigma_threshold_1": result.sigma_threshold_1,
            "sigma_threshold_2": result.sigma_threshold_2,
        },
        db_path=result.db_path,
        analysis_start_date=result.analysis_start_date,
        analysis_end_date=result.analysis_end_date,
        result_metadata=result_metadata,
        result_tables=result_tables,
        summary_markdown=_build_price_vs_sma20_regime_bundle_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_topix100_price_vs_sma20_regime_conditioning_research_bundle(
    bundle_path: str | Path,
) -> TopixRegimeConditioningResearchResult:
    info = load_research_bundle_info(bundle_path)
    tables = load_research_bundle_tables(bundle_path)
    return _build_price_vs_sma20_regime_result_from_payload(
        dict(info.result_metadata),
        tables,
    )


def get_topix100_price_vs_sma20_regime_conditioning_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        TOPIX100_PRICE_VS_SMA20_REGIME_RESEARCH_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_topix100_price_vs_sma20_regime_conditioning_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        TOPIX100_PRICE_VS_SMA20_REGIME_RESEARCH_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


def _split_price_vs_sma20_regime_result_payload(
    result: TopixRegimeConditioningResearchResult,
) -> tuple[dict[str, Any], dict[str, pd.DataFrame]]:
    metadata: dict[str, Any] = {}
    tables: dict[str, pd.DataFrame] = {}
    for field in fields(result):
        value = getattr(result, field.name)
        if isinstance(value, pd.DataFrame):
            tables[field.name] = value
            continue
        if field.name == "topix_close_stats" and value is not None:
            metadata[field.name] = asdict(value)
            continue
        if field.name == "nt_ratio_stats" and value is not None:
            metadata[field.name] = asdict(value)
            continue
        metadata[field.name] = value
    return metadata, tables


def _build_price_vs_sma20_regime_result_from_payload(
    metadata: dict[str, Any],
    tables: dict[str, pd.DataFrame],
) -> TopixRegimeConditioningResearchResult:
    topix_close_stats_payload = metadata.get("topix_close_stats")
    nt_ratio_stats_payload = metadata.get("nt_ratio_stats")
    return TopixRegimeConditioningResearchResult(
        db_path=str(metadata["db_path"]),
        source_mode=str(metadata["source_mode"]),
        source_detail=str(metadata["source_detail"]),
        available_start_date=metadata.get("available_start_date"),
        available_end_date=metadata.get("available_end_date"),
        analysis_start_date=metadata.get("analysis_start_date"),
        analysis_end_date=metadata.get("analysis_end_date"),
        sigma_threshold_1=float(metadata["sigma_threshold_1"]),
        sigma_threshold_2=float(metadata["sigma_threshold_2"]),
        universe_constituent_count=int(metadata["universe_constituent_count"]),
        valid_date_count=int(metadata["valid_date_count"]),
        topix_close_stats=(
            TopixCloseReturnStats(**topix_close_stats_payload)
            if topix_close_stats_payload
            else None
        ),
        nt_ratio_stats=(
            NtRatioReturnStats(**nt_ratio_stats_payload)
            if nt_ratio_stats_payload
            else None
        ),
        regime_market_df=tables["regime_market_df"],
        regime_day_counts_df=tables["regime_day_counts_df"],
        regime_group_day_counts_df=tables["regime_group_day_counts_df"],
        split_panel_df=tables["split_panel_df"],
        horizon_panel_df=tables["horizon_panel_df"],
        regime_daily_means_df=tables["regime_daily_means_df"],
        regime_summary_df=tables["regime_summary_df"],
        regime_pairwise_significance_df=tables["regime_pairwise_significance_df"],
        regime_hypothesis_df=tables["regime_hypothesis_df"],
        regime_group_daily_means_df=tables["regime_group_daily_means_df"],
        regime_group_summary_df=tables["regime_group_summary_df"],
        regime_group_pairwise_significance_df=tables[
            "regime_group_pairwise_significance_df"
        ],
        regime_group_hypothesis_df=tables["regime_group_hypothesis_df"],
    )


def _build_price_vs_sma20_regime_bundle_summary_markdown(
    result: TopixRegimeConditioningResearchResult,
) -> str:
    summary_lines = [
        "# TOPIX100 Price vs SMA20 Regime Conditioning",
        "",
        f"- Source mode: `{result.source_mode}`",
        f"- Source detail: `{result.source_detail}`",
        f"- Analysis range: `{result.analysis_start_date} -> {result.analysis_end_date}`",
        f"- Universe count: `{result.universe_constituent_count}`",
        f"- Valid dates: `{result.valid_date_count}`",
        f"- Sigma thresholds: `{result.sigma_threshold_1}` / `{result.sigma_threshold_2}`",
    ]
    return "\n".join(summary_lines)
