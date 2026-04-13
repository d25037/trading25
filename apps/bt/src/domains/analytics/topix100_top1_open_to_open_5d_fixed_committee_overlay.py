"""
TOPIX100 Top1 open-to-open 5D with fixed TOPIX committee overlay.

This study composes two already-defined research primitives:

- stock book: TOPIX100 streak 3/53 next-session open-to-open 5D LightGBM Top1
- overlay: fixed 4-member TOPIX downside-return-standard-deviation committee

Execution convention:

- stock ranking signal uses information available at date X
- stock entry happens at X+1 open
- stock exit happens at X+6 open
- committee signal uses information available at date X close
- committee target exposure applies from X+1 open onward
- portfolio is implemented as 5 independent sleeves, one new Top1 entry per day
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

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
    get_topix100_streak_353_next_session_open_to_open_5d_lightgbm_walkforward_latest_bundle_path,
    load_topix100_streak_353_next_session_open_to_open_5d_lightgbm_walkforward_research_bundle,
)
from src.domains.analytics.topix_close_return_streaks import (
    DEFAULT_VALIDATION_RATIO,
    _normalize_positive_int_sequence,
    _query_topix_daily_frame,
)
from src.domains.analytics.topix_close_stock_overnight_distribution import (
    SourceMode,
    _fetch_date_range,
    _open_analysis_connection,
)
from src.domains.analytics.topix_downside_return_standard_deviation_exposure_timing import (
    _build_drawdown_series,
    _compute_return_series_stats,
    _normalize_non_negative_float_sequence,
    _prepare_topix_market_frame,
)
from src.domains.analytics.topix_downside_return_standard_deviation_shock_confirmation_committee_overlay import (
    DEFAULT_COMMITTEE_HIGH_THRESHOLDS,
    DEFAULT_COMMITTEE_MEAN_WINDOW_DAYS,
    DEFAULT_DOWNSIDE_RETURN_STANDARD_DEVIATION_WINDOW_DAYS,
    DEFAULT_FIXED_BREADTH_VOTE_THRESHOLD,
    DEFAULT_FIXED_CONFIRMATION_MODE,
    DEFAULT_FIXED_REDUCED_EXPOSURE_RATIO,
    _build_committee_daily_df,
    _build_committee_id,
    get_topix_downside_return_standard_deviation_shock_confirmation_committee_overlay_latest_bundle_path,
    load_topix_downside_return_standard_deviation_shock_confirmation_committee_overlay_research_bundle,
)
from src.domains.analytics.topix_downside_return_standard_deviation_shock_confirmation_vote_overlay import (
    DEFAULT_BREADTH_FAMILY_RULES,
    DEFAULT_TREND_FAMILY_RULES,
    _build_candidate_id,
    _build_candidate_signal_frame_on_common_base,
    _simulate_candidate_daily_df_with_family_votes,
)
from src.domains.analytics.topix_downside_return_standard_deviation_trend_breadth_overlay import (
    DEFAULT_MIN_CONSTITUENTS_PER_DAY,
    _build_common_signal_frame_with_regimes,
    _build_topix100_breadth_daily_df,
    _build_topix_trend_feature_df,
)
from src.domains.analytics.topix_rank_future_close_core import (
    _query_topix100_stock_history,
)

TOPIX100_TOP1_OPEN_TO_OPEN_5D_FIXED_COMMITTEE_OVERLAY_EXPERIMENT_ID = (
    "market-behavior/topix100-top1-open-to-open-5d-fixed-committee-overlay"
)
DEFAULT_TOP1_MODEL_NAME = "lightgbm"
DEFAULT_TOP_K = 1
DEFAULT_SLEEVE_COUNT = 5
DEFAULT_HOLDING_SESSION_COUNT = 5
DEFAULT_FIXED_COMMITTEE_LOW_THRESHOLD = 0.22
_RESULT_TABLE_NAMES: tuple[str, ...] = (
    "source_top1_pick_df",
    "trade_schedule_df",
    "trade_interval_df",
    "trade_integrity_df",
    "committee_daily_df",
    "portfolio_daily_df",
    "portfolio_stats_df",
    "relative_performance_df",
    "exposure_summary_df",
)


@dataclass(frozen=True)
class Topix100Top1OpenToOpen5dFixedCommitteeOverlayResearchResult:
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
    top1_model_name: str
    top_k: int
    sleeve_count: int
    holding_session_count: int
    require_complete_trades: bool
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
    source_top1_pick_df: pd.DataFrame
    trade_schedule_df: pd.DataFrame
    trade_interval_df: pd.DataFrame
    trade_integrity_df: pd.DataFrame
    committee_daily_df: pd.DataFrame
    portfolio_daily_df: pd.DataFrame
    portfolio_stats_df: pd.DataFrame
    relative_performance_df: pd.DataFrame
    exposure_summary_df: pd.DataFrame


def run_topix100_top1_open_to_open_5d_fixed_committee_overlay_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    top1_bundle_path: str | Path | None = None,
    committee_bundle_path: str | Path | None = None,
    top1_model_name: str = DEFAULT_TOP1_MODEL_NAME,
    top_k: int = DEFAULT_TOP_K,
    sleeve_count: int = DEFAULT_SLEEVE_COUNT,
    holding_session_count: int = DEFAULT_HOLDING_SESSION_COUNT,
    require_complete_trades: bool = True,
    downside_return_standard_deviation_window_days: int = DEFAULT_DOWNSIDE_RETURN_STANDARD_DEVIATION_WINDOW_DAYS,
    committee_mean_window_days: Sequence[int] | None = None,
    committee_high_thresholds: Sequence[float] | None = None,
    committee_low_threshold: float = DEFAULT_FIXED_COMMITTEE_LOW_THRESHOLD,
    committee_trend_vote_threshold: int = 1,
    committee_breadth_vote_threshold: int = DEFAULT_FIXED_BREADTH_VOTE_THRESHOLD,
    committee_confirmation_mode: str = DEFAULT_FIXED_CONFIRMATION_MODE,
    committee_reduced_exposure_ratio: float = DEFAULT_FIXED_REDUCED_EXPOSURE_RATIO,
    min_constituents_per_day: int = DEFAULT_MIN_CONSTITUENTS_PER_DAY,
    validation_ratio: float = DEFAULT_VALIDATION_RATIO,
) -> Topix100Top1OpenToOpen5dFixedCommitteeOverlayResearchResult:
    if top_k <= 0:
        raise ValueError("top_k must be positive")
    if sleeve_count <= 0:
        raise ValueError("sleeve_count must be positive")
    if holding_session_count <= 0:
        raise ValueError("holding_session_count must be positive")
    if downside_return_standard_deviation_window_days <= 0:
        raise ValueError("downside_return_standard_deviation_window_days must be positive")
    if committee_trend_vote_threshold <= 0:
        raise ValueError("committee_trend_vote_threshold must be positive")
    if committee_breadth_vote_threshold <= 0:
        raise ValueError("committee_breadth_vote_threshold must be positive")
    if not 0.0 <= committee_reduced_exposure_ratio <= 1.0:
        raise ValueError("committee_reduced_exposure_ratio must stay within 0.0 .. 1.0")
    if not 0.0 <= validation_ratio < 1.0:
        raise ValueError("validation_ratio must satisfy 0.0 <= validation_ratio < 1.0")

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
    source_top1_pick_df = _select_source_top1_pick_df(
        top1_result.walkforward_topk_pick_df,
        model_name=top1_model_name,
        top_k=top_k,
        start_date=start_date,
        end_date=end_date,
    )
    trade_schedule_df = _assign_trade_sleeves(
        source_top1_pick_df,
        sleeve_count=sleeve_count,
    )
    if trade_schedule_df.empty:
        raise ValueError("No Top1 trades remained after sleeve assignment")

    resolved_committee_bundle_path = _resolve_optional_committee_bundle_path(
        committee_bundle_path
    )
    committee_candidate_id = _build_committee_id(
        low_threshold=committee_low_threshold,
        trend_vote_threshold=committee_trend_vote_threshold,
        breadth_vote_threshold=committee_breadth_vote_threshold,
        confirmation_mode=committee_confirmation_mode,
        reduced_exposure_ratio=committee_reduced_exposure_ratio,
        mean_window_days=resolved_mean_window_days,
        high_thresholds=resolved_high_thresholds,
    )
    committee_bundle_run_id: str | None = None
    if resolved_committee_bundle_path is not None:
        committee_bundle_info = load_research_bundle_info(resolved_committee_bundle_path)
        committee_bundle_run_id = committee_bundle_info.run_id
        committee_result = (
            load_topix_downside_return_standard_deviation_shock_confirmation_committee_overlay_research_bundle(
                resolved_committee_bundle_path
            )
        )
        candidate_rows = committee_result.committee_candidate_metrics_df[
            committee_result.committee_candidate_metrics_df["candidate_id"].astype(str)
            == committee_candidate_id
        ].copy()
        if candidate_rows.empty:
            raise ValueError(
                "Fixed committee candidate was not present in the source committee bundle: "
                f"{committee_candidate_id}"
            )

    max_exit_date = str(trade_schedule_df["exit_date"].max())
    with _open_analysis_connection(db_path) as ctx:
        source_mode = ctx.source_mode
        source_detail = ctx.source_detail
        available_start_date, available_end_date = _fetch_date_range(
            ctx.connection,
            table_name="topix_data",
        )
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

    if topix100_history_df.empty:
        raise ValueError("No TOPIX100 history was available for the overlay study")
    topix100_history_df = topix100_history_df.copy()
    topix100_history_df["date"] = topix100_history_df["date"].astype(str)
    topix100_history_df["code"] = topix100_history_df["code"].astype(str).str.zfill(4)

    market_interval_df = _build_market_interval_df(topix_daily_df)
    trade_schedule_df = _attach_market_calendar_indices(
        trade_schedule_df,
        calendar_dates=market_interval_df["calendar_dates"],
        holding_session_count=holding_session_count,
    )
    trade_interval_schedule_df = _expand_trade_intervals(
        trade_schedule_df,
        calendar_dates=market_interval_df["calendar_dates"],
    )
    stock_interval_df = _build_stock_interval_df(
        topix100_history_df,
        market_interval_df=market_interval_df["interval_df"],
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
    incomplete_trade_df = trade_integrity_df[~trade_integrity_df["complete"]].copy()
    if require_complete_trades and not incomplete_trade_df.empty:
        raise ValueError(
            "Trade path reconstruction found incomplete open-to-open intervals for "
            f"{len(incomplete_trade_df)} trades"
        )
    if not incomplete_trade_df.empty:
        complete_trade_ids = set(
            trade_integrity_df.loc[trade_integrity_df["complete"], "trade_id"].tolist()
        )
        trade_schedule_df = trade_schedule_df[
            trade_schedule_df["trade_id"].isin(complete_trade_ids)
        ].copy()
        trade_interval_df = trade_interval_df[
            trade_interval_df["trade_id"].isin(complete_trade_ids)
        ].copy()
        trade_integrity_df = trade_integrity_df[
            trade_integrity_df["trade_id"].isin(complete_trade_ids)
        ].copy()

    committee_daily_df = _build_fixed_committee_daily_df(
        topix_daily_df=topix_daily_df,
        breadth_history_df=topix100_history_df,
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
    if str(committee_daily_df["candidate_id"].iloc[0]) != committee_candidate_id:
        raise ValueError("Unexpected fixed committee candidate_id mismatch")

    analysis_interval_df = _subset_analysis_interval_df(
        market_interval_df["interval_df"],
        start_open_date=str(trade_schedule_df["entry_date"].min()),
        max_exit_date=max_exit_date,
    )
    committee_daily_df = committee_daily_df[
        committee_daily_df["realized_date"].astype(str).isin(
            analysis_interval_df["open_date"].astype(str)
        )
    ].copy()
    portfolio_daily_df = _build_portfolio_daily_df(
        trade_interval_df=trade_interval_df,
        analysis_interval_df=analysis_interval_df,
        committee_daily_df=committee_daily_df,
        sleeve_count=sleeve_count,
    )
    portfolio_stats_df = _build_portfolio_stats_df(portfolio_daily_df)
    relative_performance_df = _build_relative_performance_df(portfolio_stats_df)
    exposure_summary_df = _build_exposure_summary_df(
        trade_integrity_df=trade_integrity_df,
        portfolio_daily_df=portfolio_daily_df,
    )

    analysis_start_date = (
        str(portfolio_daily_df["open_date"].iloc[0]) if not portfolio_daily_df.empty else None
    )
    analysis_end_date = (
        str(portfolio_daily_df["next_open_date"].iloc[-1])
        if not portfolio_daily_df.empty
        else None
    )

    return Topix100Top1OpenToOpen5dFixedCommitteeOverlayResearchResult(
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
        top1_model_name=top1_model_name,
        top_k=top_k,
        sleeve_count=sleeve_count,
        holding_session_count=holding_session_count,
        require_complete_trades=require_complete_trades,
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
        source_top1_pick_df=source_top1_pick_df,
        trade_schedule_df=trade_schedule_df,
        trade_interval_df=trade_interval_df,
        trade_integrity_df=trade_integrity_df,
        committee_daily_df=committee_daily_df,
        portfolio_daily_df=portfolio_daily_df,
        portfolio_stats_df=portfolio_stats_df,
        relative_performance_df=relative_performance_df,
        exposure_summary_df=exposure_summary_df,
    )


def write_topix100_top1_open_to_open_5d_fixed_committee_overlay_research_bundle(
    result: Topix100Top1OpenToOpen5dFixedCommitteeOverlayResearchResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=TOPIX100_TOP1_OPEN_TO_OPEN_5D_FIXED_COMMITTEE_OVERLAY_EXPERIMENT_ID,
        module=__name__,
        function="run_topix100_top1_open_to_open_5d_fixed_committee_overlay_research",
        params={
            "start_date": result.analysis_start_date,
            "end_date": result.analysis_end_date,
            "top1_bundle_path": result.top1_bundle_path,
            "committee_bundle_path": result.committee_bundle_path,
            "top1_model_name": result.top1_model_name,
            "top_k": result.top_k,
            "sleeve_count": result.sleeve_count,
            "holding_session_count": result.holding_session_count,
            "require_complete_trades": result.require_complete_trades,
            "downside_return_standard_deviation_window_days": (
                result.downside_return_standard_deviation_window_days
            ),
            "committee_mean_window_days": list(result.committee_mean_window_days),
            "committee_high_thresholds": list(result.committee_high_thresholds),
            "committee_low_threshold": result.committee_low_threshold,
            "committee_trend_vote_threshold": result.committee_trend_vote_threshold,
            "committee_breadth_vote_threshold": result.committee_breadth_vote_threshold,
            "committee_confirmation_mode": result.committee_confirmation_mode,
            "committee_reduced_exposure_ratio": result.committee_reduced_exposure_ratio,
            "min_constituents_per_day": result.min_constituents_per_day,
            "validation_ratio": result.validation_ratio,
        },
        result=result,
        table_field_names=_RESULT_TABLE_NAMES,
        summary_markdown=_build_research_bundle_summary_markdown(result),
        published_summary=_build_published_summary_payload(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_topix100_top1_open_to_open_5d_fixed_committee_overlay_research_bundle(
    bundle_path: str | Path,
) -> Topix100Top1OpenToOpen5dFixedCommitteeOverlayResearchResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=Topix100Top1OpenToOpen5dFixedCommitteeOverlayResearchResult,
        table_field_names=_RESULT_TABLE_NAMES,
    )


def get_topix100_top1_open_to_open_5d_fixed_committee_overlay_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        TOPIX100_TOP1_OPEN_TO_OPEN_5D_FIXED_COMMITTEE_OVERLAY_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_topix100_top1_open_to_open_5d_fixed_committee_overlay_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        TOPIX100_TOP1_OPEN_TO_OPEN_5D_FIXED_COMMITTEE_OVERLAY_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


def _resolve_top1_bundle_path(bundle_path: str | Path | None) -> Path:
    if bundle_path is not None:
        return Path(bundle_path).expanduser()
    latest_bundle_path = (
        get_topix100_streak_353_next_session_open_to_open_5d_lightgbm_walkforward_latest_bundle_path()
    )
    if latest_bundle_path is None:
        raise FileNotFoundError("No TOPIX100 open-to-open 5D source bundle was found")
    return latest_bundle_path


def _resolve_optional_committee_bundle_path(
    bundle_path: str | Path | None,
) -> Path | None:
    if bundle_path is not None:
        return Path(bundle_path).expanduser()
    return get_topix_downside_return_standard_deviation_shock_confirmation_committee_overlay_latest_bundle_path()


def _select_source_top1_pick_df(
    pick_df: pd.DataFrame,
    *,
    model_name: str,
    top_k: int,
    start_date: str | None,
    end_date: str | None,
) -> pd.DataFrame:
    selected_df = pick_df[
        (pick_df["model_name"].astype(str) == model_name) & (pick_df["top_k"] == top_k)
    ].copy()
    if top_k == 1 and "selection_rank" in selected_df.columns:
        selected_df = selected_df[selected_df["selection_rank"] == 1].copy()
    if start_date is not None:
        selected_df = selected_df[selected_df["date"].astype(str) >= start_date].copy()
    if end_date is not None:
        selected_df = selected_df[selected_df["date"].astype(str) <= end_date].copy()
    if selected_df.empty:
        raise ValueError(
            "No source Top1 picks remained after filtering by model/top_k/date range"
        )
    selected_df["date"] = selected_df["date"].astype(str)
    selected_df["code"] = selected_df["code"].astype(str).str.zfill(4)
    selected_df["swing_entry_date"] = selected_df["swing_entry_date"].astype(str)
    selected_df["swing_exit_date"] = selected_df["swing_exit_date"].astype(str)
    selected_df = selected_df.rename(
        columns={
            "date": "signal_date",
            "swing_entry_date": "entry_date",
            "swing_exit_date": "exit_date",
            "realized_return": "source_realized_return",
        }
    )
    if selected_df["signal_date"].duplicated().any():
        duplicate_dates = (
            selected_df.loc[selected_df["signal_date"].duplicated(), "signal_date"]
            .astype(str)
            .tolist()
        )
        raise ValueError(f"Source Top1 picks contained duplicate signal_date rows: {duplicate_dates[:5]}")
    ordered_columns = [
        "signal_date",
        "entry_date",
        "exit_date",
        "code",
        "company_name",
        "score",
        "source_realized_return",
        "split_index",
        "train_start",
        "train_end",
        "test_start",
        "test_end",
    ]
    existing_columns = [column for column in ordered_columns if column in selected_df.columns]
    return selected_df[existing_columns].sort_values(
        ["entry_date", "signal_date", "code"],
        kind="stable",
    ).reset_index(drop=True)


def _assign_trade_sleeves(
    source_top1_pick_df: pd.DataFrame,
    *,
    sleeve_count: int,
) -> pd.DataFrame:
    sleeve_available_dates: list[str | None] = [None] * sleeve_count
    rows: list[dict[str, Any]] = []
    for trade_id, row in enumerate(
        source_top1_pick_df.sort_values(["entry_date", "signal_date"], kind="stable")
        .itertuples(index=False),
        start=1,
    ):
        assigned_sleeve_id: int | None = None
        for sleeve_index, available_date in enumerate(sleeve_available_dates):
            if available_date is None or available_date <= str(row.entry_date):
                assigned_sleeve_id = sleeve_index + 1
                sleeve_available_dates[sleeve_index] = str(row.exit_date)
                break
        if assigned_sleeve_id is None:
            raise ValueError(
                "No free sleeve was available; sleeve_count is too small for the trade overlap"
            )
        payload = dict(row._asdict())
        payload["trade_id"] = trade_id
        payload["sleeve_id"] = assigned_sleeve_id
        rows.append(payload)
    return pd.DataFrame.from_records(rows).sort_values(
        ["entry_date", "signal_date", "trade_id"],
        kind="stable",
    ).reset_index(drop=True)


def _build_market_interval_df(topix_daily_df: pd.DataFrame) -> dict[str, Any]:
    calendar_df = topix_daily_df[["date", "open"]].copy().reset_index(drop=True)
    calendar_df["date"] = calendar_df["date"].astype(str)
    calendar_df["next_open_date"] = calendar_df["date"].shift(-1)
    calendar_df["next_open"] = calendar_df["open"].shift(-1)
    calendar_df["topix_open_to_open_return"] = calendar_df["next_open"].div(
        calendar_df["open"].replace(0, pd.NA)
    ).sub(1.0)
    interval_df = calendar_df.dropna(subset=["next_open_date", "topix_open_to_open_return"]).copy()
    interval_df = interval_df.rename(columns={"date": "open_date"})
    interval_df = interval_df[
        ["open_date", "next_open_date", "topix_open_to_open_return"]
    ].reset_index(drop=True)
    return {
        "calendar_dates": topix_daily_df["date"].astype(str).tolist(),
        "interval_df": interval_df,
    }


def _attach_market_calendar_indices(
    trade_schedule_df: pd.DataFrame,
    *,
    calendar_dates: Sequence[str],
    holding_session_count: int,
) -> pd.DataFrame:
    date_to_index = {date: index for index, date in enumerate(calendar_dates)}
    attached_df = trade_schedule_df.copy()
    attached_df["entry_calendar_index"] = attached_df["entry_date"].map(date_to_index)
    attached_df["exit_calendar_index"] = attached_df["exit_date"].map(date_to_index)
    if attached_df["entry_calendar_index"].isna().any() or attached_df["exit_calendar_index"].isna().any():
        raise ValueError("Trade schedule contained entry/exit dates outside the market calendar")
    attached_df["entry_calendar_index"] = attached_df["entry_calendar_index"].astype(int)
    attached_df["exit_calendar_index"] = attached_df["exit_calendar_index"].astype(int)
    attached_df["expected_interval_count"] = (
        attached_df["exit_calendar_index"] - attached_df["entry_calendar_index"]
    )
    unexpected_df = attached_df[
        attached_df["expected_interval_count"] != holding_session_count
    ].copy()
    if not unexpected_df.empty:
        raise ValueError(
            "Source Top1 trades did not match the expected holding session count of "
            f"{holding_session_count}"
        )
    return attached_df


def _expand_trade_intervals(
    trade_schedule_df: pd.DataFrame,
    *,
    calendar_dates: Sequence[str],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for row in trade_schedule_df.itertuples(index=False):
        entry_calendar_index = _to_int(row.entry_calendar_index)
        exit_calendar_index = _to_int(row.exit_calendar_index)
        for calendar_index in range(
            entry_calendar_index,
            exit_calendar_index,
        ):
            payload = dict(row._asdict())
            payload["interval_number"] = calendar_index - entry_calendar_index + 1
            payload["open_date"] = str(calendar_dates[calendar_index])
            payload["next_open_date"] = str(calendar_dates[calendar_index + 1])
            rows.append(payload)
    if not rows:
        raise ValueError("Trade interval expansion produced no rows")
    return pd.DataFrame.from_records(rows).sort_values(
        ["open_date", "sleeve_id", "trade_id"],
        kind="stable",
    ).reset_index(drop=True)


def _build_stock_interval_df(
    topix100_history_df: pd.DataFrame,
    *,
    market_interval_df: pd.DataFrame,
) -> pd.DataFrame:
    open_lookup_df = topix100_history_df[["code", "date", "open"]].copy()
    open_lookup_df["code"] = open_lookup_df["code"].astype(str).str.zfill(4)
    open_lookup_df["date"] = open_lookup_df["date"].astype(str)
    stock_interval_df = open_lookup_df.rename(
        columns={"date": "open_date", "open": "stock_open"}
    ).merge(
        market_interval_df,
        on="open_date",
        how="inner",
        validate="many_to_one",
    )
    stock_interval_df = stock_interval_df.merge(
        open_lookup_df.rename(
            columns={"date": "next_open_date", "open": "stock_next_open"}
        ),
        on=["code", "next_open_date"],
        how="left",
        validate="many_to_one",
    )
    stock_interval_df["raw_open_to_open_return"] = stock_interval_df["stock_next_open"].div(
        stock_interval_df["stock_open"].replace(0, pd.NA)
    ).sub(1.0)
    return stock_interval_df[
        [
            "code",
            "open_date",
            "next_open_date",
            "stock_open",
            "stock_next_open",
            "raw_open_to_open_return",
        ]
    ].copy()


def _build_trade_integrity_df(
    trade_schedule_df: pd.DataFrame,
    *,
    trade_interval_df: pd.DataFrame,
) -> pd.DataFrame:
    grouped_df = (
        trade_interval_df.groupby("trade_id", observed=True, sort=False)
        .agg(
            observed_interval_count=(
                "raw_open_to_open_return",
                lambda values: int(values.notna().sum()),
            ),
            reconstructed_realized_return=(
                "raw_open_to_open_return",
                lambda values: (
                    float((1.0 + values.astype(float)).prod() - 1.0)
                    if values.notna().all()
                    else float("nan")
                ),
            ),
        )
        .reset_index()
    )
    integrity_df = trade_schedule_df.merge(
        grouped_df,
        on="trade_id",
        how="left",
        validate="one_to_one",
    )
    integrity_df["complete"] = (
        integrity_df["observed_interval_count"].fillna(0).astype(int)
        == integrity_df["expected_interval_count"].astype(int)
    )
    integrity_df["return_reconstruction_gap"] = (
        integrity_df["reconstructed_realized_return"].astype(float)
        - integrity_df["source_realized_return"].astype(float)
    )
    return integrity_df[
        [
            "trade_id",
            "signal_date",
            "entry_date",
            "exit_date",
            "code",
            "company_name",
            "sleeve_id",
            "expected_interval_count",
            "observed_interval_count",
            "complete",
            "source_realized_return",
            "reconstructed_realized_return",
            "return_reconstruction_gap",
        ]
    ].sort_values(["trade_id"], kind="stable").reset_index(drop=True)


def _build_fixed_committee_daily_df(
    *,
    topix_daily_df: pd.DataFrame,
    breadth_history_df: pd.DataFrame,
    downside_return_standard_deviation_window_days: int,
    committee_mean_window_days: Sequence[int],
    committee_high_thresholds: Sequence[float],
    committee_low_threshold: float,
    committee_trend_vote_threshold: int,
    committee_breadth_vote_threshold: int,
    committee_confirmation_mode: str,
    committee_reduced_exposure_ratio: float,
    min_constituents_per_day: int,
    validation_ratio: float,
) -> pd.DataFrame:
    market_frame_df = _build_topix_trend_feature_df(_prepare_topix_market_frame(topix_daily_df))
    breadth_daily_df = _build_topix100_breadth_daily_df(
        breadth_history_df,
        min_constituents_per_day=min_constituents_per_day,
    )
    signal_base_df = _build_common_signal_frame_with_regimes(
        market_frame_df,
        breadth_daily_df=breadth_daily_df,
        max_downside_return_standard_deviation_window_days=(
            downside_return_standard_deviation_window_days
        ),
        max_downside_return_standard_deviation_mean_window_days=max(committee_mean_window_days),
        validation_ratio=validation_ratio,
    )
    member_daily_dfs: list[pd.DataFrame] = []
    for mean_window_days in committee_mean_window_days:
        candidate_signal_df = _build_candidate_signal_frame_on_common_base(
            market_frame_df,
            signal_base_df=signal_base_df,
            stddev_window_days=downside_return_standard_deviation_window_days,
            mean_window_days=mean_window_days,
        )
        for high_threshold in committee_high_thresholds:
            candidate_id = _build_candidate_id(
                stddev_window_days=downside_return_standard_deviation_window_days,
                mean_window_days=mean_window_days,
                high_threshold=high_threshold,
                low_threshold=committee_low_threshold,
                reduced_exposure_ratio=committee_reduced_exposure_ratio,
                trend_vote_threshold=committee_trend_vote_threshold,
                breadth_vote_threshold=committee_breadth_vote_threshold,
                confirmation_mode=committee_confirmation_mode,
            )
            member_daily_dfs.append(
                _simulate_candidate_daily_df_with_family_votes(
                    candidate_id=candidate_id,
                    candidate_signal_df=candidate_signal_df,
                    high_annualized_downside_return_standard_deviation_threshold=(
                        high_threshold
                    ),
                    low_annualized_downside_return_standard_deviation_threshold=(
                        committee_low_threshold
                    ),
                    reduced_exposure_ratio=committee_reduced_exposure_ratio,
                    trend_family_rules=DEFAULT_TREND_FAMILY_RULES,
                    breadth_family_rules=DEFAULT_BREADTH_FAMILY_RULES,
                    trend_vote_threshold=committee_trend_vote_threshold,
                    breadth_vote_threshold=committee_breadth_vote_threshold,
                    confirmation_mode=committee_confirmation_mode,
                )
            )
    committee_id = _build_committee_id(
        low_threshold=committee_low_threshold,
        trend_vote_threshold=committee_trend_vote_threshold,
        breadth_vote_threshold=committee_breadth_vote_threshold,
        confirmation_mode=committee_confirmation_mode,
        reduced_exposure_ratio=committee_reduced_exposure_ratio,
        mean_window_days=committee_mean_window_days,
        high_thresholds=committee_high_thresholds,
    )
    return _build_committee_daily_df(
        committee_id=committee_id,
        member_daily_dfs=member_daily_dfs,
    )


def _subset_analysis_interval_df(
    market_interval_df: pd.DataFrame,
    *,
    start_open_date: str,
    max_exit_date: str,
) -> pd.DataFrame:
    scoped_df = market_interval_df[
        (market_interval_df["open_date"].astype(str) >= start_open_date)
        & (market_interval_df["next_open_date"].astype(str) <= max_exit_date)
    ].copy()
    if scoped_df.empty:
        raise ValueError("No analysis intervals remained after applying the trade date range")
    return scoped_df.reset_index(drop=True)


def _build_portfolio_daily_df(
    *,
    trade_interval_df: pd.DataFrame,
    analysis_interval_df: pd.DataFrame,
    committee_daily_df: pd.DataFrame,
    sleeve_count: int,
) -> pd.DataFrame:
    if trade_interval_df.empty:
        raise ValueError("trade_interval_df must not be empty")
    committee_lookup_df = committee_daily_df[
        [
            "realized_date",
            "sample_split",
            "target_exposure_ratio",
            "member_reduced_count",
            "member_reduced_rate",
            "signal_state",
        ]
    ].copy()
    committee_lookup_df["realized_date"] = committee_lookup_df["realized_date"].astype(str)
    committee_lookup_df = committee_lookup_df.rename(columns={"realized_date": "open_date"})
    portfolio_df = analysis_interval_df.merge(
        committee_lookup_df,
        on="open_date",
        how="left",
        validate="one_to_one",
    )
    if portfolio_df["target_exposure_ratio"].isna().any():
        missing_dates = (
            portfolio_df.loc[
                portfolio_df["target_exposure_ratio"].isna(),
                "open_date",
            ]
            .astype(str)
            .tolist()
        )
        raise ValueError(
            "Committee exposure was missing for portfolio dates: "
            f"{missing_dates[:5]}"
        )

    active_groups = {
        str(open_date): group.sort_values(["sleeve_id", "trade_id"], kind="stable").copy()
        for open_date, group in trade_interval_df.groupby("open_date", observed=True, sort=False)
    }
    raw_sleeve_nav = {sleeve_id: 1.0 / sleeve_count for sleeve_id in range(1, sleeve_count + 1)}
    overlay_sleeve_nav = {
        sleeve_id: 1.0 / sleeve_count for sleeve_id in range(1, sleeve_count + 1)
    }
    rows: list[dict[str, Any]] = []

    for row in portfolio_df.itertuples(index=False):
        open_date = str(row.open_date)
        active_df = active_groups.get(open_date)
        committee_target_exposure_ratio = _to_float(row.target_exposure_ratio)
        raw_total_before = float(sum(raw_sleeve_nav.values()))
        overlay_total_before = float(sum(overlay_sleeve_nav.values()))
        raw_active_capital = 0.0
        overlay_active_capital = 0.0
        active_trade_count = 0
        active_sleeve_count = 0
        raw_interval_return_mean = float("nan")

        if active_df is not None and not active_df.empty:
            if active_df["sleeve_id"].duplicated().any():
                raise ValueError(f"Found multiple active trades in one sleeve on {open_date}")
            active_trade_count = int(len(active_df))
            active_sleeve_count = int(active_df["sleeve_id"].nunique())
            raw_interval_return_mean = float(active_df["raw_open_to_open_return"].mean())
            for active_row in active_df.itertuples(index=False):
                sleeve_id = _to_int(active_row.sleeve_id)
                raw_active_capital += raw_sleeve_nav[sleeve_id]
                overlay_active_capital += overlay_sleeve_nav[sleeve_id]
            for active_row in active_df.itertuples(index=False):
                sleeve_id = _to_int(active_row.sleeve_id)
                raw_return = _to_float(active_row.raw_open_to_open_return)
                raw_sleeve_nav[sleeve_id] *= 1.0 + raw_return
                overlay_sleeve_nav[sleeve_id] *= 1.0 + committee_target_exposure_ratio * raw_return

        raw_total_after = float(sum(raw_sleeve_nav.values()))
        overlay_total_after = float(sum(overlay_sleeve_nav.values()))
        raw_portfolio_return = (
            raw_total_after / raw_total_before - 1.0 if raw_total_before > 0.0 else float("nan")
        )
        overlay_portfolio_return = (
            overlay_total_after / overlay_total_before - 1.0
            if overlay_total_before > 0.0
            else float("nan")
        )
        rows.append(
            {
                "open_date": open_date,
                "next_open_date": str(row.next_open_date),
                "committee_sample_split": str(row.sample_split),
                "committee_signal_state": str(row.signal_state),
                "committee_target_exposure_ratio": committee_target_exposure_ratio,
                "committee_member_reduced_count": _to_int(row.member_reduced_count),
                "committee_member_reduced_rate": _to_float(row.member_reduced_rate),
                "active_trade_count": active_trade_count,
                "active_sleeve_count": active_sleeve_count,
                "raw_active_capital_ratio": (
                    raw_active_capital / raw_total_before if raw_total_before > 0.0 else float("nan")
                ),
                "overlay_deployed_capital_ratio": (
                    committee_target_exposure_ratio * overlay_active_capital / overlay_total_before
                    if overlay_total_before > 0.0
                    else float("nan")
                ),
                "raw_interval_return_mean": raw_interval_return_mean,
                "topix_open_to_open_return": _to_float(row.topix_open_to_open_return),
                "raw_portfolio_return": float(raw_portfolio_return),
                "overlay_portfolio_return": float(overlay_portfolio_return),
            }
        )

    result_df = pd.DataFrame.from_records(rows)
    result_df["raw_excess_vs_topix"] = (
        result_df["raw_portfolio_return"] - result_df["topix_open_to_open_return"]
    )
    result_df["overlay_excess_vs_topix"] = (
        result_df["overlay_portfolio_return"] - result_df["topix_open_to_open_return"]
    )
    result_df["raw_equity_curve"] = (1.0 + result_df["raw_portfolio_return"]).cumprod()
    result_df["overlay_equity_curve"] = (1.0 + result_df["overlay_portfolio_return"]).cumprod()
    result_df["topix_equity_curve"] = (1.0 + result_df["topix_open_to_open_return"]).cumprod()
    result_df["raw_drawdown"] = _build_drawdown_series(result_df["raw_equity_curve"])
    result_df["overlay_drawdown"] = _build_drawdown_series(result_df["overlay_equity_curve"])
    result_df["topix_drawdown"] = _build_drawdown_series(result_df["topix_equity_curve"])
    return result_df


def _build_portfolio_stats_df(portfolio_daily_df: pd.DataFrame) -> pd.DataFrame:
    series_map = {
        "top1_raw": portfolio_daily_df["raw_portfolio_return"],
        "top1_fixed_committee_overlay": portfolio_daily_df["overlay_portfolio_return"],
        "topix_open_to_open_hold": portfolio_daily_df["topix_open_to_open_return"],
    }
    rows: list[dict[str, Any]] = []
    for series_name, series in series_map.items():
        rows.append(
            {
                "series_name": series_name,
                **_compute_return_series_stats(series),
            }
        )
    return pd.DataFrame(rows).sort_values(["series_name"], kind="stable").reset_index(drop=True)


def _build_relative_performance_df(portfolio_stats_df: pd.DataFrame) -> pd.DataFrame:
    stats_by_name = {
        str(row["series_name"]): row for row in portfolio_stats_df.to_dict(orient="records")
    }
    benchmark = stats_by_name["topix_open_to_open_hold"]
    rows: list[dict[str, Any]] = []
    for series_name in ("top1_raw", "top1_fixed_committee_overlay"):
        series_row = stats_by_name[series_name]
        rows.append(
            {
                "series_name": series_name,
                "benchmark_series_name": "topix_open_to_open_hold",
                "total_return_gap_vs_topix": (
                    float(series_row["total_return"]) - float(benchmark["total_return"])
                ),
                "cagr_gap_vs_topix": float(series_row["cagr"]) - float(benchmark["cagr"]),
                "sharpe_gap_vs_topix": (
                    float(series_row["sharpe_ratio"]) - float(benchmark["sharpe_ratio"])
                ),
                "sortino_gap_vs_topix": (
                    float(series_row["sortino_ratio"]) - float(benchmark["sortino_ratio"])
                ),
                "max_drawdown_gap_vs_topix": (
                    float(series_row["max_drawdown"]) - float(benchmark["max_drawdown"])
                ),
            }
        )
    return pd.DataFrame(rows).sort_values(["series_name"], kind="stable").reset_index(drop=True)


def _build_exposure_summary_df(
    *,
    trade_integrity_df: pd.DataFrame,
    portfolio_daily_df: pd.DataFrame,
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "trade_count": int(len(trade_integrity_df)),
                "complete_trade_count": int(trade_integrity_df["complete"].sum()),
                "incomplete_trade_count": int((~trade_integrity_df["complete"]).sum()),
                "max_abs_trade_reconstruction_gap": float(
                    trade_integrity_df["return_reconstruction_gap"].abs().max()
                ),
                "mean_committee_target_exposure_ratio": float(
                    portfolio_daily_df["committee_target_exposure_ratio"].astype(float).mean()
                ),
                "committee_reduced_open_count": int(
                    portfolio_daily_df["committee_target_exposure_ratio"]
                    .astype(float)
                    .lt(1.0 - 1e-12)
                    .sum()
                ),
                "committee_reduced_open_rate": float(
                    portfolio_daily_df["committee_target_exposure_ratio"]
                    .astype(float)
                    .lt(1.0 - 1e-12)
                    .mean()
                ),
                "mean_active_sleeve_count": float(
                    portfolio_daily_df["active_sleeve_count"].astype(float).mean()
                ),
                "mean_raw_active_capital_ratio": float(
                    portfolio_daily_df["raw_active_capital_ratio"].astype(float).mean()
                ),
                "mean_overlay_deployed_capital_ratio": float(
                    portfolio_daily_df["overlay_deployed_capital_ratio"].astype(float).mean()
                ),
            }
        ]
    )


def _build_research_bundle_summary_markdown(
    result: Topix100Top1OpenToOpen5dFixedCommitteeOverlayResearchResult,
) -> str:
    raw_row = _lookup_series_row(result.portfolio_stats_df, "top1_raw")
    overlay_row = _lookup_series_row(
        result.portfolio_stats_df,
        "top1_fixed_committee_overlay",
    )
    topix_row = _lookup_series_row(result.portfolio_stats_df, "topix_open_to_open_hold")
    integrity_row = result.exposure_summary_df.iloc[0]
    lines = [
        "# TOPIX100 Top1 Open-to-Open 5D with Fixed Committee Overlay",
        "",
        "## Scope",
        "",
        f"- stock book source bundle: `{result.top1_bundle_run_id or 'unknown'}`",
        f"- committee source bundle: `{result.committee_bundle_run_id or 'not_recorded'}`",
        f"- fixed committee candidate: `{result.committee_candidate_id}`",
        f"- sleeves: `{result.sleeve_count}`",
        "",
        "## Integrity",
        "",
        f"- trades: `{_to_int(integrity_row['trade_count'])}`",
        f"- complete trades: `{_to_int(integrity_row['complete_trade_count'])}`",
        f"- incomplete trades: `{_to_int(integrity_row['incomplete_trade_count'])}`",
        (
            "- max absolute trade reconstruction gap: "
            f"`{_format_percent(_to_float(integrity_row['max_abs_trade_reconstruction_gap']))}`"
        ),
        "",
        "## Full Sample",
        "",
        (
            f"- Raw Top1 sleeve{result.sleeve_count}: CAGR "
            f"`{_format_percent(_to_float(raw_row['cagr']))}`, Sharpe "
            f"`{_format_ratio(_to_float(raw_row['sharpe_ratio']))}`, Sortino "
            f"`{_format_ratio(_to_float(raw_row['sortino_ratio']))}`, MaxDD "
            f"`{_format_percent(_to_float(raw_row['max_drawdown']))}`"
        ),
        (
            f"- Overlay Top1 sleeve{result.sleeve_count}: CAGR "
            f"`{_format_percent(_to_float(overlay_row['cagr']))}`, Sharpe "
            f"`{_format_ratio(_to_float(overlay_row['sharpe_ratio']))}`, Sortino "
            f"`{_format_ratio(_to_float(overlay_row['sortino_ratio']))}`, MaxDD "
            f"`{_format_percent(_to_float(overlay_row['max_drawdown']))}`"
        ),
        (
            f"- TOPIX hold: CAGR `{_format_percent(_to_float(topix_row['cagr']))}`, Sharpe "
            f"`{_format_ratio(_to_float(topix_row['sharpe_ratio']))}`, Sortino "
            f"`{_format_ratio(_to_float(topix_row['sortino_ratio']))}`, MaxDD "
            f"`{_format_percent(_to_float(topix_row['max_drawdown']))}`"
        ),
        "",
        "## Exposure",
        "",
        (
            "- mean overlay deployed capital ratio: "
            f"`{_format_percent(_to_float(integrity_row['mean_overlay_deployed_capital_ratio']))}`"
        ),
        (
            "- mean raw active capital ratio: "
            f"`{_format_percent(_to_float(integrity_row['mean_raw_active_capital_ratio']))}`"
        ),
        (
            "- committee reduced-open rate: "
            f"`{_format_percent(_to_float(integrity_row['committee_reduced_open_rate']))}`"
        ),
    ]
    return "\n".join(lines)


def _build_published_summary_payload(
    result: Topix100Top1OpenToOpen5dFixedCommitteeOverlayResearchResult,
) -> dict[str, Any]:
    raw_row = _lookup_series_row(result.portfolio_stats_df, "top1_raw")
    overlay_row = _lookup_series_row(
        result.portfolio_stats_df,
        "top1_fixed_committee_overlay",
    )
    topix_row = _lookup_series_row(result.portfolio_stats_df, "topix_open_to_open_hold")
    return {
        "title": "TOPIX100 Top1 Open-to-Open 5D with Fixed Committee Overlay",
        "committeeCandidateId": result.committee_candidate_id,
        "top1SourceRunId": result.top1_bundle_run_id,
        "committeeSourceRunId": result.committee_bundle_run_id,
        "metrics": {
            "top1Raw": {
                "cagr": _to_float(raw_row["cagr"]),
                "sharpeRatio": _to_float(raw_row["sharpe_ratio"]),
                "sortinoRatio": _to_float(raw_row["sortino_ratio"]),
                "maxDrawdown": _to_float(raw_row["max_drawdown"]),
            },
            "top1FixedCommitteeOverlay": {
                "cagr": _to_float(overlay_row["cagr"]),
                "sharpeRatio": _to_float(overlay_row["sharpe_ratio"]),
                "sortinoRatio": _to_float(overlay_row["sortino_ratio"]),
                "maxDrawdown": _to_float(overlay_row["max_drawdown"]),
            },
            "topixHold": {
                "cagr": _to_float(topix_row["cagr"]),
                "sharpeRatio": _to_float(topix_row["sharpe_ratio"]),
                "sortinoRatio": _to_float(topix_row["sortino_ratio"]),
                "maxDrawdown": _to_float(topix_row["max_drawdown"]),
            },
        },
    }


def _lookup_series_row(stats_df: pd.DataFrame, series_name: str) -> pd.Series:
    scoped_df = stats_df[stats_df["series_name"].astype(str) == series_name].copy()
    if scoped_df.empty:
        raise ValueError(f"Series was not found in portfolio_stats_df: {series_name}")
    return scoped_df.iloc[0]


def _to_int(value: object) -> int:
    return int(cast(Any, value))


def _to_float(value: object) -> float:
    return float(cast(Any, value))


def _format_percent(value: float) -> str:
    if not pd.notna(value):
        return "n/a"
    return f"{value * 100:.2f}%"


def _format_ratio(value: float) -> str:
    if not pd.notna(value):
        return "n/a"
    return f"{value:.2f}"
