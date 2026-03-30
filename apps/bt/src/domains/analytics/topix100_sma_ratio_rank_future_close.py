"""
TOPIX SMA-ratio rank / future close research analytics.

The market.duckdb file is the source of truth. This module keeps the public
result dataclass and entrypoints while delegating bucket-specific and composite
research helpers to neighboring internal modules.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from src.domains.analytics.topix_close_stock_overnight_distribution import (
    SourceMode,
    _open_analysis_connection,
)
from src.domains.analytics.topix_rank_future_close_core import (
    _assign_feature_deciles as _core_assign_feature_deciles,
    _default_start_date as _core_default_start_date,
    _query_universe_date_range as _core_query_universe_date_range,
    _query_universe_stock_history as _core_query_universe_stock_history,
    _rolling_mean as _core_rolling_mean,
    _safe_ratio as _core_safe_ratio,
)
from src.domains.analytics.topix_sma_ratio_rank_future_close_buckets import (
    _build_nested_volume_daily_means,
    _build_nested_volume_global_significance,
    _build_nested_volume_horizon_panel,
    _build_nested_volume_interaction,
    _build_nested_volume_pairwise_significance,
    _build_nested_volume_split_panel,
    _build_q10_low_hypothesis,
    _build_q10_middle_volume_daily_means,
    _build_q10_middle_volume_horizon_panel,
    _build_q10_middle_volume_pairwise_significance,
    _build_q10_middle_volume_split_panel,
    _build_q1_q10_volume_daily_means,
    _build_q1_q10_volume_global_significance,
    _build_q1_q10_volume_horizon_panel,
    _build_q1_q10_volume_interaction,
    _build_q1_q10_volume_pairwise_significance,
    _build_q1_q10_volume_split_panel,
    _summarize_nested_volume_split,
    _summarize_q10_middle_volume_split,
    _summarize_q1_q10_volume_split,
)
from src.domains.analytics.topix_sma_ratio_rank_future_close_selection import (
    _analyze_ranked_panel,
    _build_composite_candidates,
    _build_feature_selection,
    _collect_selected_composite_tables,
    _filter_df_by_date_split,
)
from src.domains.analytics.topix_sma_ratio_rank_future_close_support import (
    DECILE_ORDER,
    DEFAULT_LOOKBACK_YEARS as _DEFAULT_LOOKBACK_YEARS,
    DEFAULT_PRIME_EX_TOPIX500_MIN_CONSTITUENTS_PER_DAY as _DEFAULT_PRIME_EX_TOPIX500_MIN_CONSTITUENTS_PER_DAY,
    DEFAULT_TOPIX100_MIN_CONSTITUENTS_PER_DAY as _DEFAULT_TOPIX100_MIN_CONSTITUENTS_PER_DAY,
    DISCOVERY_END_DATE,
    HORIZON_DAY_MAP,
    HORIZON_ORDER,
    METRIC_ORDER,
    PRICE_SMA_WINDOWS,
    RANKING_FEATURE_LABEL_MAP,
    RANKING_FEATURE_ORDER,
    UNIVERSE_LABEL_MAP,
    VALIDATION_START_DATE,
    VOLUME_SMA_WINDOWS,
    UniverseKey,
)

__all__ = [
    "DECILE_ORDER",
    "HORIZON_ORDER",
    "METRIC_ORDER",
    "RANKING_FEATURE_ORDER",
    "Topix100SmaRatioRankFutureCloseResearchResult",
    "get_topix100_sma_ratio_rank_future_close_available_date_range",
    "get_prime_ex_topix500_sma_ratio_rank_future_close_available_date_range",
    "run_topix100_sma_ratio_rank_future_close_research",
    "run_prime_ex_topix500_sma_ratio_rank_future_close_research",
]


@dataclass(frozen=True)
class Topix100SmaRatioRankFutureCloseResearchResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    universe_key: UniverseKey
    universe_label: str
    available_start_date: str | None
    available_end_date: str | None
    default_start_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    lookback_years: int
    min_constituents_per_day: int
    universe_constituent_count: int
    topix100_constituent_count: int
    stock_day_count: int
    ranked_event_count: int
    valid_date_count: int
    discovery_end_date: str
    validation_start_date: str
    event_panel_df: pd.DataFrame
    ranked_panel_df: pd.DataFrame
    ranking_feature_summary_df: pd.DataFrame
    decile_future_summary_df: pd.DataFrame
    daily_group_means_df: pd.DataFrame
    global_significance_df: pd.DataFrame
    pairwise_significance_df: pd.DataFrame
    extreme_vs_middle_summary_df: pd.DataFrame
    extreme_vs_middle_daily_means_df: pd.DataFrame
    extreme_vs_middle_significance_df: pd.DataFrame
    nested_volume_split_panel_df: pd.DataFrame
    nested_volume_split_summary_df: pd.DataFrame
    nested_volume_split_daily_means_df: pd.DataFrame
    nested_volume_split_global_significance_df: pd.DataFrame
    nested_volume_split_pairwise_significance_df: pd.DataFrame
    nested_volume_split_interaction_df: pd.DataFrame
    q1_q10_volume_split_panel_df: pd.DataFrame
    q1_q10_volume_split_summary_df: pd.DataFrame
    q1_q10_volume_split_daily_means_df: pd.DataFrame
    q1_q10_volume_split_global_significance_df: pd.DataFrame
    q1_q10_volume_split_pairwise_significance_df: pd.DataFrame
    q1_q10_volume_split_interaction_df: pd.DataFrame
    q10_middle_volume_split_panel_df: pd.DataFrame
    q10_middle_volume_split_summary_df: pd.DataFrame
    q10_middle_volume_split_daily_means_df: pd.DataFrame
    q10_middle_volume_split_pairwise_significance_df: pd.DataFrame
    q10_low_hypothesis_df: pd.DataFrame
    feature_selection_df: pd.DataFrame
    selected_feature_df: pd.DataFrame
    composite_candidate_df: pd.DataFrame
    selected_composite_df: pd.DataFrame
    selected_composite_ranking_summary_df: pd.DataFrame
    selected_composite_future_summary_df: pd.DataFrame
    selected_composite_daily_group_means_df: pd.DataFrame
    selected_composite_global_significance_df: pd.DataFrame
    selected_composite_pairwise_significance_df: pd.DataFrame


def _query_universe_stock_history(
    conn,
    *,
    universe_key: UniverseKey,
    end_date: str | None,
) -> pd.DataFrame:
    return _core_query_universe_stock_history(
        conn,
        universe_key=universe_key,
        end_date=end_date,
    )


def _query_universe_date_range(
    conn,
    *,
    universe_key: UniverseKey,
) -> tuple[str | None, str | None]:
    return _core_query_universe_date_range(
        conn,
        universe_key=universe_key,
    )


def _default_start_date(
    *,
    available_start_date: str | None,
    available_end_date: str | None,
    lookback_years: int,
) -> str | None:
    return _core_default_start_date(
        available_start_date=available_start_date,
        available_end_date=available_end_date,
        lookback_years=lookback_years,
    )


def _rolling_mean(
    df: pd.DataFrame,
    *,
    column_name: str,
    window: int,
) -> pd.Series:
    return _core_rolling_mean(
        df,
        column_name=column_name,
        window=window,
    )


def _safe_ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return _core_safe_ratio(numerator, denominator)


def _enrich_event_panel(
    history_df: pd.DataFrame,
    *,
    analysis_start_date: str | None,
    analysis_end_date: str | None,
    min_constituents_per_day: int,
) -> pd.DataFrame:
    if history_df.empty:
        return pd.DataFrame()

    panel = history_df.copy()
    panel["date"] = panel["date"].astype(str)
    panel = panel.sort_values(["code", "date"]).reset_index(drop=True)

    close_sma_cache: dict[int, pd.Series] = {}
    volume_sma_cache: dict[int, pd.Series] = {}

    for short_window, long_window in PRICE_SMA_WINDOWS:
        close_sma_cache[short_window] = close_sma_cache.get(
            short_window,
            _rolling_mean(panel, column_name="close", window=short_window),
        )
        close_sma_cache[long_window] = close_sma_cache.get(
            long_window,
            _rolling_mean(panel, column_name="close", window=long_window),
        )
        panel[f"price_sma_{short_window}_{long_window}"] = _safe_ratio(
            close_sma_cache[short_window],
            close_sma_cache[long_window],
        )

    for short_window, long_window in VOLUME_SMA_WINDOWS:
        volume_sma_cache[short_window] = volume_sma_cache.get(
            short_window,
            _rolling_mean(panel, column_name="volume", window=short_window),
        )
        volume_sma_cache[long_window] = volume_sma_cache.get(
            long_window,
            _rolling_mean(panel, column_name="volume", window=long_window),
        )
        panel[f"volume_sma_{short_window}_{long_window}"] = _safe_ratio(
            volume_sma_cache[short_window],
            volume_sma_cache[long_window],
        )

    for horizon_key, horizon_days in HORIZON_DAY_MAP.items():
        future_close = (
            panel.groupby("code", sort=False)["close"].shift(-horizon_days).astype(float)
        )
        panel[f"{horizon_key}_close"] = future_close
        panel[f"{horizon_key}_return"] = _safe_ratio(future_close, panel["close"]) - 1.0

    required_mask = panel["close"].gt(0) & panel[list(RANKING_FEATURE_ORDER)].notna().all(
        axis=1
    )
    if analysis_start_date is not None:
        required_mask &= panel["date"] >= analysis_start_date
    if analysis_end_date is not None:
        required_mask &= panel["date"] <= analysis_end_date
    panel = panel.loc[required_mask].copy()
    if panel.empty:
        return panel

    panel["date_constituent_count"] = panel.groupby("date")["code"].transform("size")
    panel = panel.loc[panel["date_constituent_count"] >= min_constituents_per_day].copy()
    if panel.empty:
        return panel

    return panel.reset_index(drop=True)


def _build_ranked_panel(event_panel_df: pd.DataFrame) -> pd.DataFrame:
    if event_panel_df.empty:
        return pd.DataFrame()

    base_columns = [
        "date",
        "code",
        "company_name",
        "close",
        "volume",
        "date_constituent_count",
        *[f"{horizon_key}_close" for horizon_key in HORIZON_ORDER],
        *[f"{horizon_key}_return" for horizon_key in HORIZON_ORDER],
    ]
    ranked_panel_df = event_panel_df.melt(
        id_vars=base_columns,
        value_vars=list(RANKING_FEATURE_ORDER),
        var_name="ranking_feature",
        value_name="ranking_value",
    )
    ranked_panel_df["ranking_feature_label"] = ranked_panel_df["ranking_feature"].map(
        RANKING_FEATURE_LABEL_MAP
    )
    return _core_assign_feature_deciles(
        ranked_panel_df,
        known_feature_order=RANKING_FEATURE_ORDER,
    )


def get_topix100_sma_ratio_rank_future_close_available_date_range(
    db_path: str,
) -> tuple[str | None, str | None]:
    """Return the available date range for latest TOPIX100 constituents."""
    with _open_analysis_connection(db_path) as ctx:
        return _query_universe_date_range(ctx.connection, universe_key="topix100")


def get_prime_ex_topix500_sma_ratio_rank_future_close_available_date_range(
    db_path: str,
) -> tuple[str | None, str | None]:
    """Return the available date range for latest PRIME ex TOPIX500 constituents."""
    with _open_analysis_connection(db_path) as ctx:
        return _query_universe_date_range(
            ctx.connection,
            universe_key="prime_ex_topix500",
        )


def _run_sma_ratio_rank_future_close_research(
    db_path: str,
    *,
    universe_key: UniverseKey,
    start_date: str | None = None,
    end_date: str | None = None,
    lookback_years: int = _DEFAULT_LOOKBACK_YEARS,
    min_constituents_per_day: int,
) -> Topix100SmaRatioRankFutureCloseResearchResult:
    if lookback_years <= 0:
        raise ValueError("lookback_years must be positive")
    if min_constituents_per_day <= 0:
        raise ValueError("min_constituents_per_day must be positive")
    if start_date and end_date and start_date > end_date:
        raise ValueError("start_date must be less than or equal to end_date")

    with _open_analysis_connection(db_path) as ctx:
        history_df = _query_universe_stock_history(
            ctx.connection,
            universe_key=universe_key,
            end_date=end_date,
        )

    if history_df.empty:
        raise ValueError(
            f"No latest {UNIVERSE_LABEL_MAP[universe_key]} stock_data rows were found"
        )

    available_start_date = str(history_df["date"].min())
    available_end_date = str(history_df["date"].max())
    default_start = _default_start_date(
        available_start_date=available_start_date,
        available_end_date=available_end_date,
        lookback_years=lookback_years,
    )
    resolved_start_date = start_date or default_start
    resolved_end_date = end_date or available_end_date

    event_panel_df = _enrich_event_panel(
        history_df,
        analysis_start_date=resolved_start_date,
        analysis_end_date=resolved_end_date,
        min_constituents_per_day=min_constituents_per_day,
    )
    ranked_panel_df = _build_ranked_panel(event_panel_df)
    full_analysis = _analyze_ranked_panel(ranked_panel_df)

    nested_volume_split_panel_df = _build_nested_volume_split_panel(event_panel_df)
    nested_volume_horizon_panel_df = _build_nested_volume_horizon_panel(
        nested_volume_split_panel_df
    )
    nested_volume_split_daily_means_df = _build_nested_volume_daily_means(
        nested_volume_horizon_panel_df
    )
    nested_volume_split_summary_df = _summarize_nested_volume_split(
        nested_volume_split_daily_means_df
    )
    nested_volume_split_global_significance_df = (
        _build_nested_volume_global_significance(nested_volume_split_daily_means_df)
    )
    nested_volume_split_pairwise_significance_df = (
        _build_nested_volume_pairwise_significance(nested_volume_split_daily_means_df)
    )
    nested_volume_split_interaction_df = _build_nested_volume_interaction(
        nested_volume_split_daily_means_df
    )

    q1_q10_volume_split_panel_df = _build_q1_q10_volume_split_panel(event_panel_df)
    q1_q10_volume_horizon_panel_df = _build_q1_q10_volume_horizon_panel(
        q1_q10_volume_split_panel_df
    )
    q1_q10_volume_split_daily_means_df = _build_q1_q10_volume_daily_means(
        q1_q10_volume_horizon_panel_df
    )
    q1_q10_volume_split_summary_df = _summarize_q1_q10_volume_split(
        q1_q10_volume_split_daily_means_df
    )
    q1_q10_volume_split_global_significance_df = (
        _build_q1_q10_volume_global_significance(q1_q10_volume_split_daily_means_df)
    )
    q1_q10_volume_split_pairwise_significance_df = (
        _build_q1_q10_volume_pairwise_significance(q1_q10_volume_split_daily_means_df)
    )
    q1_q10_volume_split_interaction_df = _build_q1_q10_volume_interaction(
        q1_q10_volume_split_daily_means_df
    )

    q10_middle_volume_split_panel_df = _build_q10_middle_volume_split_panel(
        event_panel_df
    )
    q10_middle_volume_horizon_panel_df = _build_q10_middle_volume_horizon_panel(
        q10_middle_volume_split_panel_df
    )
    q10_middle_volume_split_daily_means_df = _build_q10_middle_volume_daily_means(
        q10_middle_volume_horizon_panel_df
    )
    q10_middle_volume_split_summary_df = _summarize_q10_middle_volume_split(
        q10_middle_volume_split_daily_means_df
    )
    q10_middle_volume_split_pairwise_significance_df = (
        _build_q10_middle_volume_pairwise_significance(
            q10_middle_volume_split_daily_means_df
        )
    )
    q10_low_hypothesis_df = _build_q10_low_hypothesis(
        q10_middle_volume_split_pairwise_significance_df
    )

    discovery_analysis = _analyze_ranked_panel(
        _filter_df_by_date_split(ranked_panel_df, split_name="discovery")
    )
    validation_analysis = _analyze_ranked_panel(
        _filter_df_by_date_split(ranked_panel_df, split_name="validation")
    )
    feature_selection_df, selected_feature_df = _build_feature_selection(
        discovery_analysis=discovery_analysis,
        validation_analysis=validation_analysis,
    )
    composite_candidate_df, selected_composite_df, selected_combo_analyses = (
        _build_composite_candidates(
            event_panel_df,
            selected_feature_df=selected_feature_df,
        )
    )
    selected_composite_tables = _collect_selected_composite_tables(
        selected_composite_df=selected_composite_df,
        selected_combo_analyses=selected_combo_analyses,
    )

    analysis_start = str(event_panel_df["date"].min()) if not event_panel_df.empty else None
    analysis_end = str(event_panel_df["date"].max()) if not event_panel_df.empty else None

    return Topix100SmaRatioRankFutureCloseResearchResult(
        db_path=db_path,
        source_mode=ctx.source_mode,
        source_detail=ctx.source_detail,
        universe_key=universe_key,
        universe_label=UNIVERSE_LABEL_MAP[universe_key],
        available_start_date=available_start_date,
        available_end_date=available_end_date,
        default_start_date=default_start,
        analysis_start_date=analysis_start,
        analysis_end_date=analysis_end,
        lookback_years=lookback_years,
        min_constituents_per_day=min_constituents_per_day,
        universe_constituent_count=int(history_df["code"].nunique()),
        topix100_constituent_count=int(history_df["code"].nunique()),
        stock_day_count=int(len(event_panel_df)),
        ranked_event_count=int(len(ranked_panel_df)),
        valid_date_count=int(event_panel_df["date"].nunique())
        if not event_panel_df.empty
        else 0,
        discovery_end_date=DISCOVERY_END_DATE,
        validation_start_date=VALIDATION_START_DATE,
        event_panel_df=event_panel_df,
        ranked_panel_df=ranked_panel_df,
        ranking_feature_summary_df=full_analysis["ranking_feature_summary_df"],
        decile_future_summary_df=full_analysis["decile_future_summary_df"],
        daily_group_means_df=full_analysis["daily_group_means_df"],
        global_significance_df=full_analysis["global_significance_df"],
        pairwise_significance_df=full_analysis["pairwise_significance_df"],
        extreme_vs_middle_summary_df=full_analysis["extreme_vs_middle_summary_df"],
        extreme_vs_middle_daily_means_df=full_analysis[
            "extreme_vs_middle_daily_means_df"
        ],
        extreme_vs_middle_significance_df=full_analysis[
            "extreme_vs_middle_significance_df"
        ],
        nested_volume_split_panel_df=nested_volume_split_panel_df,
        nested_volume_split_summary_df=nested_volume_split_summary_df,
        nested_volume_split_daily_means_df=nested_volume_split_daily_means_df,
        nested_volume_split_global_significance_df=nested_volume_split_global_significance_df,
        nested_volume_split_pairwise_significance_df=nested_volume_split_pairwise_significance_df,
        nested_volume_split_interaction_df=nested_volume_split_interaction_df,
        q1_q10_volume_split_panel_df=q1_q10_volume_split_panel_df,
        q1_q10_volume_split_summary_df=q1_q10_volume_split_summary_df,
        q1_q10_volume_split_daily_means_df=q1_q10_volume_split_daily_means_df,
        q1_q10_volume_split_global_significance_df=q1_q10_volume_split_global_significance_df,
        q1_q10_volume_split_pairwise_significance_df=q1_q10_volume_split_pairwise_significance_df,
        q1_q10_volume_split_interaction_df=q1_q10_volume_split_interaction_df,
        q10_middle_volume_split_panel_df=q10_middle_volume_split_panel_df,
        q10_middle_volume_split_summary_df=q10_middle_volume_split_summary_df,
        q10_middle_volume_split_daily_means_df=q10_middle_volume_split_daily_means_df,
        q10_middle_volume_split_pairwise_significance_df=q10_middle_volume_split_pairwise_significance_df,
        q10_low_hypothesis_df=q10_low_hypothesis_df,
        feature_selection_df=feature_selection_df,
        selected_feature_df=selected_feature_df,
        composite_candidate_df=composite_candidate_df,
        selected_composite_df=selected_composite_df,
        selected_composite_ranking_summary_df=selected_composite_tables[
            "ranking_feature_summary_df"
        ],
        selected_composite_future_summary_df=selected_composite_tables[
            "decile_future_summary_df"
        ],
        selected_composite_daily_group_means_df=selected_composite_tables[
            "daily_group_means_df"
        ],
        selected_composite_global_significance_df=selected_composite_tables[
            "global_significance_df"
        ],
        selected_composite_pairwise_significance_df=selected_composite_tables[
            "pairwise_significance_df"
        ],
    )


def run_topix100_sma_ratio_rank_future_close_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    lookback_years: int = _DEFAULT_LOOKBACK_YEARS,
    min_constituents_per_day: int = _DEFAULT_TOPIX100_MIN_CONSTITUENTS_PER_DAY,
) -> Topix100SmaRatioRankFutureCloseResearchResult:
    """
    Run TOPIX100 SMA-ratio rank vs future close research from market.duckdb.

    Each SMA ratio is ranked independently within the same-day TOPIX100
    universe. Significance is computed on date-level decile means.
    """

    return _run_sma_ratio_rank_future_close_research(
        db_path,
        universe_key="topix100",
        start_date=start_date,
        end_date=end_date,
        lookback_years=lookback_years,
        min_constituents_per_day=min_constituents_per_day,
    )


def run_prime_ex_topix500_sma_ratio_rank_future_close_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    lookback_years: int = _DEFAULT_LOOKBACK_YEARS,
    min_constituents_per_day: int = _DEFAULT_PRIME_EX_TOPIX500_MIN_CONSTITUENTS_PER_DAY,
) -> Topix100SmaRatioRankFutureCloseResearchResult:
    """
    Run PRIME ex TOPIX500 SMA-ratio rank vs future close research from market.duckdb.

    Each SMA ratio is ranked independently within the same-day PRIME ex TOPIX500
    universe. Significance is computed on date-level decile means.
    """

    return _run_sma_ratio_rank_future_close_research(
        db_path,
        universe_key="prime_ex_topix500",
        start_date=start_date,
        end_date=end_date,
        lookback_years=lookback_years,
        min_constituents_per_day=min_constituents_per_day,
    )
