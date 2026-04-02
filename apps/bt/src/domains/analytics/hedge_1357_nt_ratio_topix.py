"""
1357 x NT ratio / TOPIX hedge research analytics.

This module is read-only. It aligns local market snapshot data for:
- 1357 (Nikkei double inverse ETF)
- TOPIX
- N225_UNDERPX

It then evaluates rule-based hedge overlays against proxy long baskets derived
from the local stock snapshot.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any, cast

import pandas as pd

from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    find_latest_research_bundle_path,
    get_research_bundle_dir,
    load_research_bundle_info,
    load_research_bundle_tables,
    write_research_bundle,
)
from src.domains.analytics.hedge_1357_nt_ratio_topix_market_frame import (
    _build_nt_ratio_stats,
    _build_topix_close_stats,
    _bucket_nt_ratio_return,
    _bucket_topix_close_return,
    _calculate_market_features,
    _query_group_daily_returns,
    _query_market_daily_frame,
)
from src.domains.analytics.hedge_1357_nt_ratio_topix_strategy_metrics import (
    _build_annual_rule_summary,
    _build_beta_neutral_weights,
    _build_etf_strategy_metrics,
    _build_hedge_metrics,
    _expected_shortfall,
)
from src.domains.analytics.hedge_1357_nt_ratio_topix_summary_tables import (
    _build_etf_strategy_split_comparison,
    _build_joint_forward_summary,
    _build_rule_signal_summary,
    _build_shortlist,
    _build_split_comparison,
)
from src.domains.analytics.hedge_1357_nt_ratio_topix_support import (
    RULE_ORDER,
    STOCK_GROUP_ORDER,
    TARGET_ORDER,
    NtRatioReturnStats,
    SourceMode,
    StockGroup,
    TopixCloseReturnStats,
    _DISCOVERY_END_DATE,
    _MACD_BASIS,
    _MACD_FAST_PERIOD,
    _MACD_SIGNAL_PERIOD,
    _MACD_SLOW_PERIOD,
    _VALIDATION_START_DATE,
    _filter_date_range_df,
    _open_analysis_connection,
    _validate_fixed_weights,
    _validate_selected_groups,
)

HEDGE_1357_NT_RATIO_TOPIX_RESEARCH_EXPERIMENT_ID = (
    "market-behavior/hedge-1357-nt-ratio-topix"
)


@dataclass(frozen=True)
class Hedge1357NtRatioTopixResearchResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    discovery_end_date: str
    validation_start_date: str
    sigma_threshold_1: float
    sigma_threshold_2: float
    selected_groups: tuple[StockGroup, ...]
    fixed_weights: tuple[float, ...]
    macd_basis: str
    macd_fast_period: int
    macd_slow_period: int
    macd_signal_period: int
    topix_close_stats: TopixCloseReturnStats | None
    nt_ratio_stats: NtRatioReturnStats | None
    daily_market_df: pd.DataFrame
    daily_proxy_returns_df: pd.DataFrame
    joint_forward_summary_df: pd.DataFrame
    rule_signal_summary_df: pd.DataFrame
    hedge_metrics_df: pd.DataFrame
    etf_strategy_metrics_df: pd.DataFrame
    etf_strategy_split_comparison_df: pd.DataFrame
    split_comparison_df: pd.DataFrame
    annual_rule_summary_df: pd.DataFrame
    shortlist_df: pd.DataFrame


def _serialize_topix_close_stats(
    stats: TopixCloseReturnStats | None,
) -> dict[str, Any] | None:
    if stats is None:
        return None
    return {field.name: getattr(stats, field.name) for field in fields(TopixCloseReturnStats)}


def _deserialize_topix_close_stats(
    payload: dict[str, Any] | None,
) -> TopixCloseReturnStats | None:
    if payload is None:
        return None
    return TopixCloseReturnStats(
        sample_count=int(payload["sample_count"]),
        mean_return=float(payload["mean_return"]),
        std_return=float(payload["std_return"]),
        sigma_threshold_1=float(payload["sigma_threshold_1"]),
        sigma_threshold_2=float(payload["sigma_threshold_2"]),
        threshold_1=float(payload["threshold_1"]),
        threshold_2=float(payload["threshold_2"]),
        min_return=payload.get("min_return"),
        q25_return=payload.get("q25_return"),
        median_return=payload.get("median_return"),
        q75_return=payload.get("q75_return"),
        max_return=payload.get("max_return"),
    )


def _serialize_nt_ratio_stats(
    stats: NtRatioReturnStats | None,
) -> dict[str, Any] | None:
    if stats is None:
        return None
    return {field.name: getattr(stats, field.name) for field in fields(NtRatioReturnStats)}


def _deserialize_nt_ratio_stats(
    payload: dict[str, Any] | None,
) -> NtRatioReturnStats | None:
    if payload is None:
        return None
    return NtRatioReturnStats(
        sample_count=int(payload["sample_count"]),
        mean_return=float(payload["mean_return"]),
        std_return=float(payload["std_return"]),
        sigma_threshold_1=float(payload["sigma_threshold_1"]),
        sigma_threshold_2=float(payload["sigma_threshold_2"]),
        lower_threshold_2=float(payload["lower_threshold_2"]),
        lower_threshold_1=float(payload["lower_threshold_1"]),
        upper_threshold_1=float(payload["upper_threshold_1"]),
        upper_threshold_2=float(payload["upper_threshold_2"]),
        min_return=payload.get("min_return"),
        q25_return=payload.get("q25_return"),
        median_return=payload.get("median_return"),
        q75_return=payload.get("q75_return"),
        max_return=payload.get("max_return"),
    )


def get_1357_nt_ratio_topix_available_date_range(
    db_path: str,
) -> tuple[str | None, str | None]:
    with _open_analysis_connection(db_path) as ctx:
        market_df = _query_market_daily_frame(ctx.connection)
    return str(market_df["date"].min()), str(market_df["date"].max())


def run_1357_nt_ratio_topix_hedge_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    sigma_threshold_1: float = 1.0,
    sigma_threshold_2: float = 2.0,
    selected_groups: Sequence[str] | None = None,
    fixed_weights: Sequence[float] = (0.1, 0.2, 0.3, 0.4, 0.5),
) -> Hedge1357NtRatioTopixResearchResult:
    validated_groups = _validate_selected_groups(selected_groups)
    validated_weights = _validate_fixed_weights(fixed_weights)
    with _open_analysis_connection(db_path) as ctx:
        source_mode = ctx.source_mode
        source_detail = ctx.source_detail
        market_df_raw = _query_market_daily_frame(ctx.connection)
        available_start_date = str(market_df_raw["date"].min()) if not market_df_raw.empty else None
        available_end_date = str(market_df_raw["date"].max()) if not market_df_raw.empty else None
        market_df_filtered = _filter_date_range_df(
            market_df_raw,
            start_date=start_date,
            end_date=end_date,
        )
        if market_df_filtered.empty:
            raise ValueError("No aligned market rows were found in the selected date range")
        stats_input_df = market_df_filtered.copy()
        stats_input_df["topix_close_return"] = stats_input_df["topix_close"].pct_change()
        stats_input_df["nt_ratio"] = stats_input_df["n225_close"] / stats_input_df["topix_close"]
        stats_input_df["nt_ratio_return"] = stats_input_df["nt_ratio"].pct_change()
        topix_close_stats = _build_topix_close_stats(
            stats_input_df,
            sigma_threshold_1=sigma_threshold_1,
            sigma_threshold_2=sigma_threshold_2,
        )
        nt_ratio_stats = _build_nt_ratio_stats(
            stats_input_df,
            sigma_threshold_1=sigma_threshold_1,
            sigma_threshold_2=sigma_threshold_2,
        )
        daily_market_df = _calculate_market_features(
            market_df_filtered,
            topix_close_stats=topix_close_stats,
            nt_ratio_stats=nt_ratio_stats,
        )
        group_returns_df = _query_group_daily_returns(
            ctx.connection,
            start_date=start_date,
            end_date=end_date,
            selected_groups=validated_groups,
        )
    daily_proxy_returns_df = group_returns_df.merge(
        daily_market_df[
            [
                "date",
                "next_date",
                "split",
                "calendar_year",
                "topix_close",
                "topix_close_return",
                "topix_close_bucket_key",
                "topix_close_bucket_label",
                "nt_ratio",
                "nt_ratio_return",
                "nt_ratio_bucket_key",
                "nt_ratio_bucket_label",
                "topix_ma20",
                "topix_ma60",
                "topix_ma20_slope_5d",
                "topix_macd_histogram",
                "etf_next_overnight_return",
                "etf_next_intraday_return",
                "etf_next_close_to_close_return",
                "etf_forward_3d_close_to_close_return",
                "etf_forward_5d_close_to_close_return",
                "shock_topix_le_negative_threshold_2",
                "shock_joint_adverse",
                "trend_ma_bearish",
                "trend_macd_negative",
                "hybrid_bearish_joint",
            ]
        ],
        on=["date", "next_date"],
        how="inner",
    )
    daily_proxy_returns_df = _build_beta_neutral_weights(daily_proxy_returns_df)
    analysis_valid = daily_proxy_returns_df.dropna(
        subset=[
            "topix_close_return",
            "nt_ratio_return",
            "etf_next_overnight_return",
            "etf_next_intraday_return",
            "etf_next_close_to_close_return",
            "etf_forward_3d_close_to_close_return",
            "etf_forward_5d_close_to_close_return",
            "long_next_overnight_return",
            "long_next_intraday_return",
            "long_next_close_to_close_return",
            "long_forward_3d_close_to_close_return",
            "long_forward_5d_close_to_close_return",
        ]
    )
    analysis_start_date = (
        str(analysis_valid["date"].min()) if not analysis_valid.empty else None
    )
    analysis_end_date = str(analysis_valid["date"].max()) if not analysis_valid.empty else None
    joint_forward_summary_df = _build_joint_forward_summary(
        daily_market_df,
        topix_close_stats=topix_close_stats,
        nt_ratio_stats=nt_ratio_stats,
    )
    rule_signal_summary_df = _build_rule_signal_summary(daily_market_df)
    hedge_metrics_df = _build_hedge_metrics(
        daily_proxy_returns_df,
        fixed_weights=validated_weights,
    )
    etf_strategy_metrics_df = _build_etf_strategy_metrics(daily_market_df)
    etf_strategy_split_comparison_df = _build_etf_strategy_split_comparison(
        etf_strategy_metrics_df
    )
    split_comparison_df = _build_split_comparison(hedge_metrics_df)
    annual_rule_summary_df = _build_annual_rule_summary(
        daily_proxy_returns_df,
        fixed_weights=validated_weights,
    )
    shortlist_df = _build_shortlist(split_comparison_df)
    return Hedge1357NtRatioTopixResearchResult(
        db_path=db_path,
        source_mode=source_mode,
        source_detail=source_detail,
        available_start_date=available_start_date,
        available_end_date=available_end_date,
        analysis_start_date=analysis_start_date,
        analysis_end_date=analysis_end_date,
        discovery_end_date=_DISCOVERY_END_DATE,
        validation_start_date=_VALIDATION_START_DATE,
        sigma_threshold_1=sigma_threshold_1,
        sigma_threshold_2=sigma_threshold_2,
        selected_groups=validated_groups,
        fixed_weights=validated_weights,
        macd_basis=_MACD_BASIS,
        macd_fast_period=_MACD_FAST_PERIOD,
        macd_slow_period=_MACD_SLOW_PERIOD,
        macd_signal_period=_MACD_SIGNAL_PERIOD,
        topix_close_stats=topix_close_stats,
        nt_ratio_stats=nt_ratio_stats,
        daily_market_df=daily_market_df,
        daily_proxy_returns_df=daily_proxy_returns_df,
        joint_forward_summary_df=joint_forward_summary_df,
        rule_signal_summary_df=rule_signal_summary_df,
        hedge_metrics_df=hedge_metrics_df,
        etf_strategy_metrics_df=etf_strategy_metrics_df,
        etf_strategy_split_comparison_df=etf_strategy_split_comparison_df,
        split_comparison_df=split_comparison_df,
        annual_rule_summary_df=annual_rule_summary_df,
        shortlist_df=shortlist_df,
    )


def write_1357_nt_ratio_topix_hedge_research_bundle(
    result: Hedge1357NtRatioTopixResearchResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    result_metadata, result_tables = _split_result_payload(result)
    return write_research_bundle(
        experiment_id=HEDGE_1357_NT_RATIO_TOPIX_RESEARCH_EXPERIMENT_ID,
        module=__name__,
        function="run_1357_nt_ratio_topix_hedge_research",
        params={
            "start_date": result.analysis_start_date,
            "end_date": result.analysis_end_date,
            "sigma_threshold_1": result.sigma_threshold_1,
            "sigma_threshold_2": result.sigma_threshold_2,
            "selected_groups": list(result.selected_groups),
            "fixed_weights": list(result.fixed_weights),
        },
        db_path=result.db_path,
        analysis_start_date=result.analysis_start_date,
        analysis_end_date=result.analysis_end_date,
        result_metadata=result_metadata,
        result_tables=result_tables,
        summary_markdown=_build_research_bundle_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_1357_nt_ratio_topix_hedge_research_bundle(
    bundle_path: str | Path,
) -> Hedge1357NtRatioTopixResearchResult:
    info = load_research_bundle_info(bundle_path)
    tables = load_research_bundle_tables(bundle_path)
    return _build_result_from_payload(dict(info.result_metadata), tables)


def get_1357_nt_ratio_topix_hedge_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        HEDGE_1357_NT_RATIO_TOPIX_RESEARCH_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_1357_nt_ratio_topix_hedge_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        HEDGE_1357_NT_RATIO_TOPIX_RESEARCH_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


def _split_result_payload(
    result: Hedge1357NtRatioTopixResearchResult,
) -> tuple[dict[str, Any], dict[str, pd.DataFrame]]:
    metadata = {
        "db_path": result.db_path,
        "source_mode": result.source_mode,
        "source_detail": result.source_detail,
        "available_start_date": result.available_start_date,
        "available_end_date": result.available_end_date,
        "analysis_start_date": result.analysis_start_date,
        "analysis_end_date": result.analysis_end_date,
        "discovery_end_date": result.discovery_end_date,
        "validation_start_date": result.validation_start_date,
        "sigma_threshold_1": result.sigma_threshold_1,
        "sigma_threshold_2": result.sigma_threshold_2,
        "selected_groups": list(result.selected_groups),
        "fixed_weights": list(result.fixed_weights),
        "macd_basis": result.macd_basis,
        "macd_fast_period": result.macd_fast_period,
        "macd_slow_period": result.macd_slow_period,
        "macd_signal_period": result.macd_signal_period,
        "topix_close_stats": _serialize_topix_close_stats(result.topix_close_stats),
        "nt_ratio_stats": _serialize_nt_ratio_stats(result.nt_ratio_stats),
    }
    tables = {
        "daily_market_df": result.daily_market_df,
        "daily_proxy_returns_df": result.daily_proxy_returns_df,
        "joint_forward_summary_df": result.joint_forward_summary_df,
        "rule_signal_summary_df": result.rule_signal_summary_df,
        "hedge_metrics_df": result.hedge_metrics_df,
        "etf_strategy_metrics_df": result.etf_strategy_metrics_df,
        "etf_strategy_split_comparison_df": result.etf_strategy_split_comparison_df,
        "split_comparison_df": result.split_comparison_df,
        "annual_rule_summary_df": result.annual_rule_summary_df,
        "shortlist_df": result.shortlist_df,
    }
    return metadata, tables


def _build_result_from_payload(
    metadata: dict[str, Any],
    tables: dict[str, pd.DataFrame],
) -> Hedge1357NtRatioTopixResearchResult:
    return Hedge1357NtRatioTopixResearchResult(
        db_path=str(metadata["db_path"]),
        source_mode=metadata["source_mode"],
        source_detail=str(metadata["source_detail"]),
        available_start_date=metadata.get("available_start_date"),
        available_end_date=metadata.get("available_end_date"),
        analysis_start_date=metadata.get("analysis_start_date"),
        analysis_end_date=metadata.get("analysis_end_date"),
        discovery_end_date=str(metadata["discovery_end_date"]),
        validation_start_date=str(metadata["validation_start_date"]),
        sigma_threshold_1=float(metadata["sigma_threshold_1"]),
        sigma_threshold_2=float(metadata["sigma_threshold_2"]),
        selected_groups=cast(
            tuple[StockGroup, ...],
            tuple(str(value) for value in metadata["selected_groups"]),
        ),
        fixed_weights=tuple(float(value) for value in metadata["fixed_weights"]),
        macd_basis=str(metadata["macd_basis"]),
        macd_fast_period=int(metadata["macd_fast_period"]),
        macd_slow_period=int(metadata["macd_slow_period"]),
        macd_signal_period=int(metadata["macd_signal_period"]),
        topix_close_stats=_deserialize_topix_close_stats(
            metadata.get("topix_close_stats")
        ),
        nt_ratio_stats=_deserialize_nt_ratio_stats(metadata.get("nt_ratio_stats")),
        daily_market_df=tables["daily_market_df"],
        daily_proxy_returns_df=tables["daily_proxy_returns_df"],
        joint_forward_summary_df=tables["joint_forward_summary_df"],
        rule_signal_summary_df=tables["rule_signal_summary_df"],
        hedge_metrics_df=tables["hedge_metrics_df"],
        etf_strategy_metrics_df=tables["etf_strategy_metrics_df"],
        etf_strategy_split_comparison_df=tables["etf_strategy_split_comparison_df"],
        split_comparison_df=tables["split_comparison_df"],
        annual_rule_summary_df=tables["annual_rule_summary_df"],
        shortlist_df=tables["shortlist_df"],
    )


def _build_research_bundle_summary_markdown(
    result: Hedge1357NtRatioTopixResearchResult,
) -> str:
    summary_lines = [
        "# 1357 x NT Ratio / TOPIX Hedge Research",
        "",
        "## Snapshot",
        "",
        f"- Source mode: `{result.source_mode}`",
        f"- Available range: `{result.available_start_date} -> {result.available_end_date}`",
        f"- Analysis range: `{result.analysis_start_date} -> {result.analysis_end_date}`",
        f"- Discovery / Validation split: `<= {result.discovery_end_date} / >= {result.validation_start_date}`",
        f"- Selected groups: `{', '.join(result.selected_groups)}`",
        f"- Fixed weights: `{', '.join(f'{value:.2f}' for value in result.fixed_weights)}`",
        "",
        "## Current Read",
        "",
    ]
    if result.topix_close_stats is not None:
        summary_lines.append(
            f"- TOPIX close sample count: `{result.topix_close_stats.sample_count}`"
        )
    if result.nt_ratio_stats is not None:
        summary_lines.append(
            f"- NT ratio sample count: `{result.nt_ratio_stats.sample_count}`"
        )
    shortlist = result.shortlist_df.copy()
    if shortlist.empty:
        summary_lines.append("- Shortlist was empty after filtering.")
    else:
        top_row = shortlist.sort_values("score", ascending=False).iloc[0]
        summary_lines.append(
            "- Top shortlist rule was "
            f"`{top_row['stock_group']}` x `{top_row['target_name']}` x "
            f"`{top_row['rule_name']}` x `{top_row['weight_label']}` "
            f"with score `{float(top_row['score']):+.4f}`."
        )
    summary_lines.extend(
        [
            "",
            "## Artifact Tables",
            "",
            *[
                f"- `{table_name}`"
                for table_name in _split_result_payload(result)[1].keys()
            ],
        ]
    )
    return "\n".join(summary_lines)


__all__ = [
    "Hedge1357NtRatioTopixResearchResult",
    "RULE_ORDER",
    "STOCK_GROUP_ORDER",
    "TARGET_ORDER",
    "_bucket_nt_ratio_return",
    "_bucket_topix_close_return",
    "_expected_shortfall",
    "get_1357_nt_ratio_topix_available_date_range",
    "run_1357_nt_ratio_topix_hedge_research",
]
