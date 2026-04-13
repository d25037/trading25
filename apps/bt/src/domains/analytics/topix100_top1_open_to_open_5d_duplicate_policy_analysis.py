"""
Duplicate-handling analysis for TOPIX100 Top1 open-to-open 5D.

This isolates how much performance comes from allowing the same stock to be
stacked across multiple sleeves.

Policies:

- allow_stack: current implementation, always buy Top1 even if already held
- skip_if_held: if Top1 is already active, skip that day's new entry
- next_unique_within_top5: if Top1 is already active, use the best non-held name
  within the same day's Top5 candidates; if none exists, skip
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

import pandas as pd

from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    find_latest_research_bundle_path,
    get_research_bundle_dir,
    load_dataclass_research_bundle,
    load_research_bundle_info,
    write_dataclass_research_bundle,
)
from src.domains.analytics.topix100_streak_353_next_session_open_to_open_5d_lightgbm_walkforward import (
    load_topix100_streak_353_next_session_open_to_open_5d_lightgbm_walkforward_research_bundle,
)
from src.domains.analytics.topix100_top1_open_to_open_5d_fixed_committee_overlay import (
    DEFAULT_FIXED_COMMITTEE_LOW_THRESHOLD,
    DEFAULT_HOLDING_SESSION_COUNT,
    DEFAULT_SLEEVE_COUNT,
    DEFAULT_TOP1_MODEL_NAME,
    _assign_trade_sleeves,
    _build_exposure_summary_df,
    _build_fixed_committee_daily_df,
    _build_market_interval_df,
    _build_portfolio_daily_df,
    _build_portfolio_stats_df,
    _build_relative_performance_df,
    _build_stock_interval_df,
    _build_trade_integrity_df,
    _expand_trade_intervals,
    _lookup_series_row,
    _resolve_optional_committee_bundle_path,
    _resolve_top1_bundle_path,
    _subset_analysis_interval_df,
)
from src.domains.analytics.topix_close_return_streaks import (
    DEFAULT_VALIDATION_RATIO,
    _normalize_positive_int_sequence,
)
from src.domains.analytics.topix_close_stock_overnight_distribution import (
    SourceMode,
    _fetch_date_range,
    _open_analysis_connection,
)
from src.domains.analytics.topix_downside_return_standard_deviation_shock_confirmation_committee_overlay import (
    DEFAULT_COMMITTEE_HIGH_THRESHOLDS,
    DEFAULT_COMMITTEE_MEAN_WINDOW_DAYS,
    DEFAULT_DOWNSIDE_RETURN_STANDARD_DEVIATION_WINDOW_DAYS,
    DEFAULT_FIXED_BREADTH_VOTE_THRESHOLD,
    DEFAULT_FIXED_CONFIRMATION_MODE,
    DEFAULT_FIXED_REDUCED_EXPOSURE_RATIO,
    _build_committee_id,
    load_topix_downside_return_standard_deviation_shock_confirmation_committee_overlay_research_bundle,
)
from src.domains.analytics.topix_downside_return_standard_deviation_exposure_timing import (
    _normalize_non_negative_float_sequence,
)

DuplicatePolicy = Literal["allow_stack", "skip_if_held", "next_unique_within_top5"]
DEFAULT_DUPLICATE_POLICIES: tuple[DuplicatePolicy, ...] = (
    "allow_stack",
    "skip_if_held",
    "next_unique_within_top5",
)
TOPIX100_TOP1_OPEN_TO_OPEN_5D_DUPLICATE_POLICY_ANALYSIS_EXPERIMENT_ID = (
    "market-behavior/topix100-top1-open-to-open-5d-duplicate-policy-analysis"
)
_RESULT_TABLE_NAMES: tuple[str, ...] = (
    "policy_selection_event_df",
    "policy_trade_schedule_df",
    "policy_trade_integrity_df",
    "policy_portfolio_daily_df",
    "policy_portfolio_stats_df",
    "policy_relative_performance_df",
    "policy_exposure_summary_df",
    "policy_concentration_summary_df",
)


@dataclass(frozen=True)
class Topix100Top1OpenToOpen5dDuplicatePolicyAnalysisResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    top1_bundle_path: str
    top1_bundle_run_id: str | None
    committee_bundle_path: str | None
    committee_bundle_run_id: str | None
    model_name: str
    fallback_candidate_top_k: int
    sleeve_count: int
    holding_session_count: int
    duplicate_policies: tuple[str, ...]
    downside_return_standard_deviation_window_days: int
    committee_mean_window_days: tuple[int, ...]
    committee_high_thresholds: tuple[float, ...]
    committee_low_threshold: float
    committee_trend_vote_threshold: int
    committee_breadth_vote_threshold: int
    committee_confirmation_mode: str
    committee_reduced_exposure_ratio: float
    min_constituents_per_day: int
    validation_ratio: float
    committee_candidate_id: str
    policy_selection_event_df: pd.DataFrame
    policy_trade_schedule_df: pd.DataFrame
    policy_trade_integrity_df: pd.DataFrame
    policy_portfolio_daily_df: pd.DataFrame
    policy_portfolio_stats_df: pd.DataFrame
    policy_relative_performance_df: pd.DataFrame
    policy_exposure_summary_df: pd.DataFrame
    policy_concentration_summary_df: pd.DataFrame


def run_topix100_top1_open_to_open_5d_duplicate_policy_analysis(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    top1_bundle_path: str | Path | None = None,
    committee_bundle_path: str | Path | None = None,
    model_name: str = DEFAULT_TOP1_MODEL_NAME,
    fallback_candidate_top_k: int = 5,
    sleeve_count: int = DEFAULT_SLEEVE_COUNT,
    holding_session_count: int = DEFAULT_HOLDING_SESSION_COUNT,
    duplicate_policies: Sequence[DuplicatePolicy] | None = None,
    downside_return_standard_deviation_window_days: int = DEFAULT_DOWNSIDE_RETURN_STANDARD_DEVIATION_WINDOW_DAYS,
    committee_mean_window_days: Sequence[int] | None = None,
    committee_high_thresholds: Sequence[float] | None = None,
    committee_low_threshold: float = DEFAULT_FIXED_COMMITTEE_LOW_THRESHOLD,
    committee_trend_vote_threshold: int = 1,
    committee_breadth_vote_threshold: int = DEFAULT_FIXED_BREADTH_VOTE_THRESHOLD,
    committee_confirmation_mode: str = DEFAULT_FIXED_CONFIRMATION_MODE,
    committee_reduced_exposure_ratio: float = DEFAULT_FIXED_REDUCED_EXPOSURE_RATIO,
    min_constituents_per_day: int = 50,
    validation_ratio: float = DEFAULT_VALIDATION_RATIO,
) -> Topix100Top1OpenToOpen5dDuplicatePolicyAnalysisResult:
    if fallback_candidate_top_k <= 0:
        raise ValueError("fallback_candidate_top_k must be positive")
    if sleeve_count <= 0:
        raise ValueError("sleeve_count must be positive")
    if holding_session_count <= 0:
        raise ValueError("holding_session_count must be positive")
    if committee_trend_vote_threshold <= 0:
        raise ValueError("committee_trend_vote_threshold must be positive")
    if committee_breadth_vote_threshold <= 0:
        raise ValueError("committee_breadth_vote_threshold must be positive")
    if not 0.0 <= committee_reduced_exposure_ratio <= 1.0:
        raise ValueError("committee_reduced_exposure_ratio must stay within 0.0 .. 1.0")
    if not 0.0 <= validation_ratio < 1.0:
        raise ValueError("validation_ratio must satisfy 0.0 <= validation_ratio < 1.0")

    resolved_policies = _normalize_duplicate_policies(duplicate_policies)
    resolved_mean_window_days = _normalize_positive_int_sequence(
        committee_mean_window_days,
        default=DEFAULT_COMMITTEE_MEAN_WINDOW_DAYS,
        name="committee_mean_window_days",
    )
    resolved_high_thresholds = _normalize_non_negative_float_sequence(
        committee_high_thresholds,
        default=DEFAULT_COMMITTEE_HIGH_THRESHOLDS,
        name="committee_high_thresholds",
    )

    resolved_top1_bundle_path = _resolve_top1_bundle_path(top1_bundle_path)
    top1_bundle_info = load_research_bundle_info(resolved_top1_bundle_path)
    top1_result = (
        load_topix100_streak_353_next_session_open_to_open_5d_lightgbm_walkforward_research_bundle(
            resolved_top1_bundle_path
        )
    )
    candidate_pick_df = _select_source_candidate_pick_df(
        top1_result.walkforward_topk_pick_df,
        model_name=model_name,
        fallback_candidate_top_k=fallback_candidate_top_k,
        start_date=start_date,
        end_date=end_date,
    )

    resolved_committee_bundle_path = _resolve_optional_committee_bundle_path(
        committee_bundle_path
    )
    committee_bundle_run_id: str | None = None
    committee_candidate_id = _build_committee_id(
        low_threshold=committee_low_threshold,
        trend_vote_threshold=committee_trend_vote_threshold,
        breadth_vote_threshold=committee_breadth_vote_threshold,
        confirmation_mode=committee_confirmation_mode,
        reduced_exposure_ratio=committee_reduced_exposure_ratio,
        mean_window_days=resolved_mean_window_days,
        high_thresholds=resolved_high_thresholds,
    )
    if resolved_committee_bundle_path is not None:
        committee_bundle_info = load_research_bundle_info(resolved_committee_bundle_path)
        committee_bundle_run_id = committee_bundle_info.run_id
        committee_result = (
            load_topix_downside_return_standard_deviation_shock_confirmation_committee_overlay_research_bundle(
                resolved_committee_bundle_path
            )
        )
        if committee_result.committee_candidate_metrics_df[
            committee_result.committee_candidate_metrics_df["candidate_id"].astype(str)
            == committee_candidate_id
        ].empty:
            raise ValueError(
                "Fixed committee candidate was not present in the source committee bundle: "
                f"{committee_candidate_id}"
            )

    all_selected_trades: list[pd.DataFrame] = []
    all_selection_events: list[pd.DataFrame] = []
    for policy in resolved_policies:
        selection_event_df, selected_trade_df = _build_selected_trade_df_for_policy(
            candidate_pick_df,
            policy=policy,
        )
        selected_trade_df = _assign_trade_sleeves(selected_trade_df, sleeve_count=sleeve_count)
        selected_trade_df["policy_name"] = policy
        selection_event_df["policy_name"] = policy
        all_selected_trades.append(selected_trade_df)
        all_selection_events.append(selection_event_df)

    policy_selection_event_df = pd.concat(all_selection_events, ignore_index=True)
    policy_trade_schedule_df = pd.concat(all_selected_trades, ignore_index=True)
    max_exit_date = str(policy_trade_schedule_df["exit_date"].max())

    with _open_analysis_connection(db_path) as ctx:
        source_mode = ctx.source_mode
        source_detail = ctx.source_detail
        available_start_date, available_end_date = _fetch_date_range(
            ctx.connection,
            table_name="topix_data",
        )
        from src.domains.analytics.topix_close_return_streaks import _query_topix_daily_frame
        from src.domains.analytics.topix_rank_future_close_core import _query_topix100_stock_history

        topix_daily_df = _query_topix_daily_frame(
            ctx.connection,
            start_date=None,
            end_date=max_exit_date,
            future_horizons=(1,),
        )
        topix100_history_df = _query_topix100_stock_history(
            ctx.connection,
            end_date=max_exit_date,
        )

    market_interval = _build_market_interval_df(topix_daily_df)
    stock_interval_df = _build_stock_interval_df(
        topix100_history_df.assign(code=topix100_history_df["code"].astype(str).str.zfill(4)),
        market_interval_df=market_interval["interval_df"],
    )
    committee_daily_df = _build_fixed_committee_daily_df(
        topix_daily_df=topix_daily_df,
        breadth_history_df=topix100_history_df.assign(
            code=topix100_history_df["code"].astype(str).str.zfill(4),
            date=topix100_history_df["date"].astype(str),
        ),
        downside_return_standard_deviation_window_days=(
            downside_return_standard_deviation_window_days
        ),
        committee_mean_window_days=resolved_mean_window_days,
        committee_high_thresholds=resolved_high_thresholds,
        committee_low_threshold=committee_low_threshold,
        committee_trend_vote_threshold=committee_trend_vote_threshold,
        committee_breadth_vote_threshold=committee_breadth_vote_threshold,
        committee_confirmation_mode=committee_confirmation_mode,
        committee_reduced_exposure_ratio=committee_reduced_exposure_ratio,
        min_constituents_per_day=min_constituents_per_day,
        validation_ratio=validation_ratio,
    )

    trade_integrity_frames: list[pd.DataFrame] = []
    portfolio_daily_frames: list[pd.DataFrame] = []
    portfolio_stats_frames: list[pd.DataFrame] = []
    relative_performance_frames: list[pd.DataFrame] = []
    exposure_summary_frames: list[pd.DataFrame] = []
    concentration_summary_frames: list[pd.DataFrame] = []

    for policy in resolved_policies:
        trade_schedule_df = policy_trade_schedule_df[
            policy_trade_schedule_df["policy_name"] == policy
        ].copy()
        if trade_schedule_df.empty:
            continue
        trade_schedule_df = _attach_calendar_indices_per_policy(
            trade_schedule_df,
            calendar_dates=market_interval["calendar_dates"],
            holding_session_count=holding_session_count,
        )
        trade_interval_schedule_df = _expand_trade_intervals(
            trade_schedule_df,
            calendar_dates=market_interval["calendar_dates"],
        )
        trade_interval_df = trade_interval_schedule_df.merge(
            stock_interval_df,
            on=["code", "open_date", "next_open_date"],
            how="left",
            validate="many_to_one",
        )
        trade_integrity_df = _build_trade_integrity_df(
            trade_schedule_df,
            trade_interval_df=trade_interval_df,
        )
        if not trade_integrity_df["complete"].all():
            raise ValueError(f"Incomplete trade intervals were found for policy {policy}")
        analysis_interval_df = _subset_analysis_interval_df(
            market_interval["interval_df"],
            start_open_date=str(trade_schedule_df["entry_date"].min()),
            max_exit_date=str(trade_schedule_df["exit_date"].max()),
        )
        scoped_committee_df = committee_daily_df[
            committee_daily_df["realized_date"].astype(str).isin(
                analysis_interval_df["open_date"].astype(str)
            )
        ].copy()
        portfolio_daily_df = _build_portfolio_daily_df(
            trade_interval_df=trade_interval_df,
            analysis_interval_df=analysis_interval_df,
            committee_daily_df=scoped_committee_df,
            sleeve_count=sleeve_count,
        )
        portfolio_stats_df = _build_portfolio_stats_df(portfolio_daily_df)
        relative_performance_df = _build_relative_performance_df(portfolio_stats_df)
        exposure_summary_df = _build_exposure_summary_df(
            trade_integrity_df=trade_integrity_df,
            portfolio_daily_df=portfolio_daily_df,
        )
        concentration_summary_df = _build_policy_concentration_summary_df(
            trade_interval_df=trade_interval_df,
            selection_event_df=policy_selection_event_df[
                policy_selection_event_df["policy_name"] == policy
            ].copy(),
        )

        for df in (
            trade_integrity_df,
            portfolio_daily_df,
            portfolio_stats_df,
            relative_performance_df,
            exposure_summary_df,
            concentration_summary_df,
        ):
            df["policy_name"] = policy

        trade_integrity_frames.append(trade_integrity_df)
        portfolio_daily_frames.append(portfolio_daily_df)
        portfolio_stats_frames.append(portfolio_stats_df)
        relative_performance_frames.append(relative_performance_df)
        exposure_summary_frames.append(exposure_summary_df)
        concentration_summary_frames.append(concentration_summary_df)

    policy_trade_integrity_df = pd.concat(trade_integrity_frames, ignore_index=True)
    policy_portfolio_daily_df = pd.concat(portfolio_daily_frames, ignore_index=True)
    policy_portfolio_stats_df = pd.concat(portfolio_stats_frames, ignore_index=True)
    policy_relative_performance_df = pd.concat(relative_performance_frames, ignore_index=True)
    policy_exposure_summary_df = pd.concat(exposure_summary_frames, ignore_index=True)
    policy_concentration_summary_df = pd.concat(
        concentration_summary_frames,
        ignore_index=True,
    )

    analysis_start_date = (
        str(policy_portfolio_daily_df["open_date"].min())
        if not policy_portfolio_daily_df.empty
        else None
    )
    analysis_end_date = (
        str(policy_portfolio_daily_df["next_open_date"].max())
        if not policy_portfolio_daily_df.empty
        else None
    )
    return Topix100Top1OpenToOpen5dDuplicatePolicyAnalysisResult(
        db_path=db_path,
        source_mode=cast(SourceMode, source_mode),
        source_detail=str(source_detail),
        available_start_date=available_start_date,
        available_end_date=available_end_date,
        analysis_start_date=analysis_start_date,
        analysis_end_date=analysis_end_date,
        top1_bundle_path=str(resolved_top1_bundle_path),
        top1_bundle_run_id=top1_bundle_info.run_id,
        committee_bundle_path=(
            str(resolved_committee_bundle_path)
            if resolved_committee_bundle_path is not None
            else None
        ),
        committee_bundle_run_id=committee_bundle_run_id,
        model_name=model_name,
        fallback_candidate_top_k=fallback_candidate_top_k,
        sleeve_count=sleeve_count,
        holding_session_count=holding_session_count,
        duplicate_policies=tuple(resolved_policies),
        downside_return_standard_deviation_window_days=(
            downside_return_standard_deviation_window_days
        ),
        committee_mean_window_days=resolved_mean_window_days,
        committee_high_thresholds=resolved_high_thresholds,
        committee_low_threshold=committee_low_threshold,
        committee_trend_vote_threshold=committee_trend_vote_threshold,
        committee_breadth_vote_threshold=committee_breadth_vote_threshold,
        committee_confirmation_mode=committee_confirmation_mode,
        committee_reduced_exposure_ratio=committee_reduced_exposure_ratio,
        min_constituents_per_day=min_constituents_per_day,
        validation_ratio=validation_ratio,
        committee_candidate_id=committee_candidate_id,
        policy_selection_event_df=policy_selection_event_df,
        policy_trade_schedule_df=policy_trade_schedule_df,
        policy_trade_integrity_df=policy_trade_integrity_df,
        policy_portfolio_daily_df=policy_portfolio_daily_df,
        policy_portfolio_stats_df=policy_portfolio_stats_df,
        policy_relative_performance_df=policy_relative_performance_df,
        policy_exposure_summary_df=policy_exposure_summary_df,
        policy_concentration_summary_df=policy_concentration_summary_df,
    )


def write_topix100_top1_open_to_open_5d_duplicate_policy_analysis_bundle(
    result: Topix100Top1OpenToOpen5dDuplicatePolicyAnalysisResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=TOPIX100_TOP1_OPEN_TO_OPEN_5D_DUPLICATE_POLICY_ANALYSIS_EXPERIMENT_ID,
        module=__name__,
        function="run_topix100_top1_open_to_open_5d_duplicate_policy_analysis",
        params={
            "start_date": result.analysis_start_date,
            "end_date": result.analysis_end_date,
            "top1_bundle_path": result.top1_bundle_path,
            "committee_bundle_path": result.committee_bundle_path,
            "model_name": result.model_name,
            "fallback_candidate_top_k": result.fallback_candidate_top_k,
            "sleeve_count": result.sleeve_count,
            "holding_session_count": result.holding_session_count,
            "duplicate_policies": list(result.duplicate_policies),
        },
        result=result,
        table_field_names=_RESULT_TABLE_NAMES,
        summary_markdown=_build_research_bundle_summary_markdown(result),
        published_summary=_build_published_summary_payload(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_topix100_top1_open_to_open_5d_duplicate_policy_analysis_bundle(
    bundle_path: str | Path,
) -> Topix100Top1OpenToOpen5dDuplicatePolicyAnalysisResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=Topix100Top1OpenToOpen5dDuplicatePolicyAnalysisResult,
        table_field_names=_RESULT_TABLE_NAMES,
    )


def get_topix100_top1_open_to_open_5d_duplicate_policy_analysis_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        TOPIX100_TOP1_OPEN_TO_OPEN_5D_DUPLICATE_POLICY_ANALYSIS_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_topix100_top1_open_to_open_5d_duplicate_policy_analysis_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        TOPIX100_TOP1_OPEN_TO_OPEN_5D_DUPLICATE_POLICY_ANALYSIS_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


def _normalize_duplicate_policies(
    values: Sequence[DuplicatePolicy] | None,
) -> tuple[DuplicatePolicy, ...]:
    raw_values = tuple(DEFAULT_DUPLICATE_POLICIES if values is None else values)
    if not raw_values:
        raise ValueError("duplicate_policies must not be empty")
    valid_values = set(DEFAULT_DUPLICATE_POLICIES)
    normalized: list[DuplicatePolicy] = []
    for value in raw_values:
        if value not in valid_values:
            raise ValueError(f"Unsupported duplicate policy: {value}")
        if value not in normalized:
            normalized.append(value)
    return tuple(normalized)


def _select_source_candidate_pick_df(
    pick_df: pd.DataFrame,
    *,
    model_name: str,
    fallback_candidate_top_k: int,
    start_date: str | None,
    end_date: str | None,
) -> pd.DataFrame:
    selected_df = pick_df[
        (pick_df["model_name"].astype(str) == model_name)
        & (pick_df["top_k"] == fallback_candidate_top_k)
    ].copy()
    if start_date is not None:
        selected_df = selected_df[selected_df["date"].astype(str) >= start_date].copy()
    if end_date is not None:
        selected_df = selected_df[selected_df["date"].astype(str) <= end_date].copy()
    if selected_df.empty:
        raise ValueError("No candidate picks remained for the duplicate-policy analysis")
    selected_df["signal_date"] = selected_df["date"].astype(str)
    selected_df["code"] = selected_df["code"].astype(str).str.zfill(4)
    selected_df["entry_date"] = selected_df["swing_entry_date"].astype(str)
    selected_df["exit_date"] = selected_df["swing_exit_date"].astype(str)
    selected_df["source_realized_return"] = selected_df["realized_return"].astype(float)
    return selected_df.sort_values(
        ["signal_date", "selection_rank", "code"],
        kind="stable",
    ).reset_index(drop=True)


def _build_selected_trade_df_for_policy(
    candidate_pick_df: pd.DataFrame,
    *,
    policy: DuplicatePolicy,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    active_trades: list[dict[str, str]] = []
    selection_rows: list[dict[str, Any]] = []
    selected_rows: list[dict[str, Any]] = []
    grouped = candidate_pick_df.groupby("signal_date", observed=True, sort=True)
    for signal_date, group in grouped:
        daily_df = group.sort_values(["selection_rank", "code"], kind="stable").reset_index(drop=True)
        entry_date = str(daily_df["entry_date"].iloc[0])
        active_trades = [
            trade for trade in active_trades if str(trade["exit_date"]) > entry_date
        ]
        active_codes = {str(trade["code"]) for trade in active_trades}
        top1_row = daily_df.iloc[0]
        top1_is_duplicate = str(top1_row["code"]) in active_codes

        chosen_row: pd.Series | None = None
        action = "selected_top1"
        if policy == "allow_stack":
            chosen_row = top1_row
            action = "selected_top1_duplicate" if top1_is_duplicate else "selected_top1"
        elif policy == "skip_if_held":
            if top1_is_duplicate:
                action = "skipped_duplicate_top1"
            else:
                chosen_row = top1_row
        elif policy == "next_unique_within_top5":
            for _, candidate_row in daily_df.iterrows():
                if str(candidate_row["code"]) not in active_codes:
                    chosen_row = candidate_row
                    break
            if chosen_row is None:
                action = "skipped_no_unique_candidate"
            elif int(chosen_row["selection_rank"]) == 1:
                action = "selected_top1"
            else:
                action = "selected_alternative"
        else:
            raise ValueError(f"Unsupported duplicate policy: {policy}")

        selection_payload = {
            "signal_date": str(signal_date),
            "entry_date": entry_date,
            "exit_date": str(top1_row["exit_date"]),
            "top1_code": str(top1_row["code"]),
            "top1_company_name": str(top1_row["company_name"]),
            "top1_selection_rank": int(top1_row["selection_rank"]),
            "active_code_count_at_entry": len(active_codes),
            "top1_duplicate_at_entry": top1_is_duplicate,
            "candidate_count": int(len(daily_df)),
            "action": action,
            "selected_code": None,
            "selected_company_name": None,
            "selected_rank": None,
            "selected_realized_return": None,
        }
        if chosen_row is not None:
            chosen_payload = chosen_row.to_dict()
            selected_rows.append(chosen_payload)
            active_trades.append(
                {
                    "code": str(chosen_row["code"]),
                    "exit_date": str(chosen_row["exit_date"]),
                }
            )
            selection_payload.update(
                {
                    "selected_code": str(chosen_row["code"]),
                    "selected_company_name": str(chosen_row["company_name"]),
                    "selected_rank": int(chosen_row["selection_rank"]),
                    "selected_realized_return": float(chosen_row["source_realized_return"]),
                }
            )
        selection_rows.append(selection_payload)

    selection_event_df = pd.DataFrame.from_records(selection_rows)
    selected_trade_df = pd.DataFrame.from_records(selected_rows)
    if selected_trade_df.empty:
        raise ValueError(f"Policy {policy} produced no trades")
    ordered_columns = [
        "signal_date",
        "entry_date",
        "exit_date",
        "code",
        "company_name",
        "score",
        "source_realized_return",
        "selection_rank",
        "split_index",
        "train_start",
        "train_end",
        "test_start",
        "test_end",
    ]
    existing_columns = [column for column in ordered_columns if column in selected_trade_df.columns]
    return (
        selection_event_df.sort_values(["signal_date"], kind="stable").reset_index(drop=True),
        selected_trade_df[existing_columns]
        .sort_values(["entry_date", "signal_date", "selection_rank"], kind="stable")
        .reset_index(drop=True),
    )


def _attach_calendar_indices_per_policy(
    trade_schedule_df: pd.DataFrame,
    *,
    calendar_dates: Sequence[str],
    holding_session_count: int,
) -> pd.DataFrame:
    from src.domains.analytics.topix100_top1_open_to_open_5d_fixed_committee_overlay import (
        _attach_market_calendar_indices,
    )

    return _attach_market_calendar_indices(
        trade_schedule_df,
        calendar_dates=calendar_dates,
        holding_session_count=holding_session_count,
    )


def _build_policy_concentration_summary_df(
    *,
    trade_interval_df: pd.DataFrame,
    selection_event_df: pd.DataFrame,
) -> pd.DataFrame:
    active_code_df = (
        trade_interval_df.groupby(["open_date", "code"], observed=True, sort=False)
        .size()
        .rename("same_code_sleeves")
        .reset_index()
    )
    if active_code_df.empty:
        raise ValueError("trade_interval_df must not be empty")
    daily_summary_df = (
        active_code_df.groupby("open_date", observed=True, sort=False)
        .agg(
            active_trade_count=("same_code_sleeves", "sum"),
            distinct_code_count=("code", "nunique"),
            max_same_code_sleeves=("same_code_sleeves", "max"),
        )
        .reset_index()
    )
    return pd.DataFrame(
        [
            {
                "signal_count": int(len(selection_event_df)),
                "selected_trade_count": int(selection_event_df["selected_code"].notna().sum()),
                "top1_duplicate_signal_count": int(
                    selection_event_df["top1_duplicate_at_entry"].astype(bool).sum()
                ),
                "top1_duplicate_signal_rate": float(
                    selection_event_df["top1_duplicate_at_entry"].astype(bool).mean()
                ),
                "selected_alternative_count": int(
                    (selection_event_df["action"].astype(str) == "selected_alternative").sum()
                ),
                "skipped_signal_count": int(selection_event_df["selected_code"].isna().sum()),
                "mean_active_trade_count": float(daily_summary_df["active_trade_count"].mean()),
                "mean_distinct_code_count": float(daily_summary_df["distinct_code_count"].mean()),
                "mean_overlap_count": float(
                    (
                        daily_summary_df["active_trade_count"]
                        - daily_summary_df["distinct_code_count"]
                    ).mean()
                ),
                "max_same_code_sleeves": int(
                    daily_summary_df["max_same_code_sleeves"].max()
                ),
            }
        ]
    )


def _build_research_bundle_summary_markdown(
    result: Topix100Top1OpenToOpen5dDuplicatePolicyAnalysisResult,
) -> str:
    lines = [
        "# TOPIX100 Top1 Open-to-Open 5D Duplicate Policy Analysis",
        "",
        f"- source bundle: `{result.top1_bundle_run_id or 'unknown'}`",
        f"- committee bundle: `{result.committee_bundle_run_id or 'unknown'}`",
        f"- fixed committee: `{result.committee_candidate_id}`",
        "",
    ]
    for policy in result.duplicate_policies:
        stats_df = result.policy_portfolio_stats_df[
            result.policy_portfolio_stats_df["policy_name"] == policy
        ].copy()
        concentration_row = result.policy_concentration_summary_df[
            result.policy_concentration_summary_df["policy_name"] == policy
        ].iloc[0]
        raw_row = _lookup_series_row(stats_df, "top1_raw")
        overlay_row = _lookup_series_row(stats_df, "top1_fixed_committee_overlay")
        lines.extend(
            [
                f"## {policy}",
                "",
                (
                    f"- raw: CAGR `{_fmt_pct(raw_row['cagr'])}`, Sharpe "
                    f"`{_fmt_ratio(raw_row['sharpe_ratio'])}`, MaxDD `{_fmt_pct(raw_row['max_drawdown'])}`"
                ),
                (
                    f"- overlay: CAGR `{_fmt_pct(overlay_row['cagr'])}`, Sharpe "
                    f"`{_fmt_ratio(overlay_row['sharpe_ratio'])}`, MaxDD `{_fmt_pct(overlay_row['max_drawdown'])}`"
                ),
                (
                    f"- top1 duplicate signal rate: `{_fmt_pct(concentration_row['top1_duplicate_signal_rate'])}`"
                ),
                (
                    f"- mean overlap count: `{_fmt_ratio(concentration_row['mean_overlap_count'])}`"
                ),
                "",
            ]
        )
    return "\n".join(lines)


def _build_published_summary_payload(
    result: Topix100Top1OpenToOpen5dDuplicatePolicyAnalysisResult,
) -> dict[str, Any]:
    policies: dict[str, Any] = {}
    for policy in result.duplicate_policies:
        stats_df = result.policy_portfolio_stats_df[
            result.policy_portfolio_stats_df["policy_name"] == policy
        ].copy()
        concentration_row = result.policy_concentration_summary_df[
            result.policy_concentration_summary_df["policy_name"] == policy
        ].iloc[0]
        raw_row = _lookup_series_row(stats_df, "top1_raw")
        overlay_row = _lookup_series_row(stats_df, "top1_fixed_committee_overlay")
        policies[policy] = {
            "raw": {
                "cagr": _to_float(raw_row["cagr"]),
                "sharpeRatio": _to_float(raw_row["sharpe_ratio"]),
                "sortinoRatio": _to_float(raw_row["sortino_ratio"]),
                "maxDrawdown": _to_float(raw_row["max_drawdown"]),
            },
            "overlay": {
                "cagr": _to_float(overlay_row["cagr"]),
                "sharpeRatio": _to_float(overlay_row["sharpe_ratio"]),
                "sortinoRatio": _to_float(overlay_row["sortino_ratio"]),
                "maxDrawdown": _to_float(overlay_row["max_drawdown"]),
            },
            "top1DuplicateSignalRate": _to_float(
                concentration_row["top1_duplicate_signal_rate"]
            ),
            "meanOverlapCount": _to_float(concentration_row["mean_overlap_count"]),
        }
    return {"title": "TOPIX100 Top1 duplicate policy analysis", "policies": policies}


def _fmt_pct(value: object) -> str:
    return f"{_to_float(value) * 100:.2f}%"


def _fmt_ratio(value: object) -> str:
    return f"{_to_float(value):.2f}"


def _to_float(value: object) -> float:
    return float(cast(Any, value))
