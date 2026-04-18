"""
TOPIX100 13:30 entry to next-session 10:45 exit conditioning research.

This follow-up study segments the 13:30 -> next-session 10:45 pattern by:

- market regime at the 13:30 entry snapshot
- sector_33_name
- previous-session 10:45 winner/loser state
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pandas as pd

from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    find_latest_research_bundle_path,
    get_research_bundle_dir,
    load_research_bundle_info,
    load_research_bundle_tables,
    write_bundle_artifact,
    write_research_bundle,
)
from src.domains.analytics.topix100_1330_entry_next_1045_exit import (
    DEFAULT_ENTRY_TIME,
    DEFAULT_EXIT_TIME,
    DEFAULT_INTERVAL_MINUTES,
    DEFAULT_TAIL_FRACTION,
    Topix1001330EntryNext1045ExitResult,
    run_topix100_1330_entry_next_1045_exit_research,
)
from src.domains.analytics.topix100_open_relative_intraday_path import (
    SourceMode,
    TOPIX100_SCALE_CATEGORIES,
    _import_matplotlib_pyplot,
    _normalize_code_sql,
    _open_analysis_connection,
)
from src.domains.analytics.topix100_peak_winner_loser_intraday_path import (
    run_topix100_peak_winner_loser_intraday_path_research,
)
from src.domains.analytics.topix100_second_bar_volume_drop_performance import (
    _safe_welch_t_test,
)

TOPIX100_1330_ENTRY_NEXT_1045_EXIT_CONDITIONING_EXPERIMENT_ID = (
    "market-behavior/topix100-1330-entry-next-1045-exit-conditioning"
)
TOPIX100_1330_ENTRY_NEXT_1045_EXIT_CONDITIONING_PLOT_FILENAME = (
    "topix100_1330_entry_next_1045_exit_conditioning_overview.png"
)
DEFAULT_PREV_DAY_PEAK_TIME = "10:45"

_ENRICHED_SESSION_LEVEL_COLUMNS: tuple[str, ...] = (
    "interval_minutes",
    "entry_date",
    "previous_entry_date",
    "code",
    "company_name",
    "scale_category",
    "sector_17_name",
    "sector_33_name",
    "previous_close",
    "entry_time",
    "entry_bucket_minute",
    "entry_close",
    "same_day_close_time",
    "same_day_close_bucket_minute",
    "same_day_close",
    "next_date",
    "next_day_open_time",
    "next_day_open_bucket_minute",
    "next_day_open",
    "exit_time",
    "exit_bucket_minute",
    "next_day_exit_close",
    "prev_close_to_entry_return",
    "entry_to_close_return",
    "close_to_next_open_return",
    "next_open_to_exit_return",
    "entry_to_next_exit_return",
    "prev_close_to_next_exit_return",
    "entry_split_rank",
    "entry_split_group",
    "entry_split_session_count",
    "current_entry_bucket_key",
    "current_entry_bucket_label",
    "market_regime_return",
    "market_regime_bucket_key",
    "market_regime_bucket_label",
    "prev_day_peak_date",
    "prev_day_peak_return",
    "prev_day_peak_group_key",
    "prev_day_peak_group_label",
)
_REGIME_MARKET_COLUMNS: tuple[str, ...] = (
    "entry_date",
    "market_regime_return",
    "sample_count",
    "stock_count",
    "market_regime_rank",
    "market_regime_bucket_key",
    "market_regime_bucket_label",
)
_SEGMENT_GROUP_SUMMARY_COLUMNS: tuple[str, ...] = (
    "segment_key",
    "segment_label",
    "group_label",
    "segment_sample_count",
    "sample_count",
    "sample_share_within_segment",
    "date_count",
    "stock_count",
    "mean_prev_close_to_entry_return",
    "median_prev_close_to_entry_return",
    "mean_entry_to_close_return",
    "median_entry_to_close_return",
    "entry_to_close_hit_positive",
    "mean_close_to_next_open_return",
    "median_close_to_next_open_return",
    "close_to_next_open_hit_positive",
    "mean_next_open_to_exit_return",
    "median_next_open_to_exit_return",
    "next_open_to_exit_hit_positive",
    "mean_entry_to_next_exit_return",
    "median_entry_to_next_exit_return",
    "entry_to_next_exit_hit_positive",
)
_SEGMENT_COMPARISON_COLUMNS: tuple[str, ...] = (
    "segment_key",
    "segment_label",
    "segment_sample_count",
    "winners_count",
    "losers_count",
    "winners_mean_prev_close_to_entry_return",
    "losers_mean_prev_close_to_entry_return",
    "prev_close_to_entry_mean_spread",
    "winners_entry_to_close_mean",
    "losers_entry_to_close_mean",
    "entry_to_close_mean_spread",
    "entry_to_close_welch_t_stat",
    "entry_to_close_welch_p_value",
    "winners_close_to_next_open_mean",
    "losers_close_to_next_open_mean",
    "close_to_next_open_mean_spread",
    "close_to_next_open_welch_t_stat",
    "close_to_next_open_welch_p_value",
    "winners_next_open_to_exit_mean",
    "losers_next_open_to_exit_mean",
    "next_open_to_exit_mean_spread",
    "next_open_to_exit_welch_t_stat",
    "next_open_to_exit_welch_p_value",
    "winners_entry_to_next_exit_mean",
    "losers_entry_to_next_exit_mean",
    "entry_to_next_exit_mean_spread",
    "entry_to_next_exit_welch_t_stat",
    "entry_to_next_exit_welch_p_value",
)
_PREV_DAY_PEAK_TRANSITION_COLUMNS: tuple[str, ...] = (
    "prev_day_peak_group_key",
    "prev_day_peak_group_label",
    "current_entry_bucket_key",
    "current_entry_bucket_label",
    "sample_count",
    "share_within_prev_day_peak_group",
    "share_within_current_entry_bucket",
    "mean_prev_close_to_entry_return",
    "mean_entry_to_next_exit_return",
)

_CURRENT_ENTRY_BUCKET_ORDER: tuple[str, ...] = ("winners", "middle", "losers")
_GROUP_LABEL_ORDER: tuple[str, ...] = ("all", "winners", "losers")
_PREV_DAY_PEAK_ORDER: tuple[str, ...] = ("winners", "middle", "losers", "unclassified")


def _build_current_entry_bucket_label_map(tail_fraction: float) -> dict[str, str]:
    tail_pct = int(round(tail_fraction * 100))
    middle_pct = max(0, 100 - (tail_pct * 2))
    return {
        "winners": f"Current 13:30 top {tail_pct}%",
        "middle": f"Current 13:30 middle {middle_pct}%",
        "losers": f"Current 13:30 bottom {tail_pct}%",
    }


def _build_prev_day_peak_label_map(
    *,
    tail_fraction: float,
    prev_day_peak_time: str,
) -> dict[str, str]:
    tail_pct = int(round(tail_fraction * 100))
    return {
        "winners": f"Prev-day {prev_day_peak_time} top {tail_pct}%",
        "middle": f"Prev-day {prev_day_peak_time} middle",
        "losers": f"Prev-day {prev_day_peak_time} bottom {tail_pct}%",
        "unclassified": f"Prev-day {prev_day_peak_time} unclassified",
    }


@dataclass(frozen=True)
class Topix1001330EntryNext1045ExitConditioningResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    interval_minutes: int
    entry_time: str
    exit_time: str
    tail_fraction: float
    prev_day_peak_time: str
    topix100_constituent_count: int
    total_entry_session_count: int
    eligible_session_count: int
    regime_day_count: int
    enriched_session_level_df: pd.DataFrame
    regime_market_df: pd.DataFrame
    regime_group_summary_df: pd.DataFrame
    regime_comparison_df: pd.DataFrame
    sector_group_summary_df: pd.DataFrame
    sector_comparison_df: pd.DataFrame
    prev_day_peak_group_summary_df: pd.DataFrame
    prev_day_peak_comparison_df: pd.DataFrame
    prev_day_peak_transition_df: pd.DataFrame


def _empty_enriched_session_level_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_ENRICHED_SESSION_LEVEL_COLUMNS))


def _empty_regime_market_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_REGIME_MARKET_COLUMNS))


def _empty_segment_group_summary_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_SEGMENT_GROUP_SUMMARY_COLUMNS))


def _empty_segment_comparison_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_SEGMENT_COMPARISON_COLUMNS))


def _empty_prev_day_peak_transition_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_PREV_DAY_PEAK_TRANSITION_COLUMNS))


def _validate_time_label(value: str, *, argument_name: str) -> str:
    normalized = str(value).strip()
    if len(normalized) != 5 or normalized[2] != ":":
        raise ValueError(f"{argument_name} must be formatted as HH:MM")
    return normalized


def _fetch_topix100_sector_metadata_df(db_path: str) -> pd.DataFrame:
    normalized_code_sql = _normalize_code_sql("code")
    with _open_analysis_connection(db_path) as ctx:
        metadata_df = cast(
            pd.DataFrame,
            ctx.connection.execute(
                f"""
                WITH topix100_stocks AS (
                    SELECT
                        normalized_code,
                        company_name,
                        coalesce(scale_category, '') AS scale_category,
                        coalesce(sector_17_name, '') AS sector_17_name,
                        coalesce(sector_33_name, '') AS sector_33_name
                    FROM (
                        SELECT
                            {normalized_code_sql} AS normalized_code,
                            company_name,
                            scale_category,
                            sector_17_name,
                            sector_33_name,
                            ROW_NUMBER() OVER (
                                PARTITION BY {normalized_code_sql}
                                ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END, code
                            ) AS row_priority
                        FROM stocks
                        WHERE coalesce(scale_category, '') IN {cast(Any, TOPIX100_SCALE_CATEGORIES)}
                    ) stock_candidates
                    WHERE row_priority = 1
                )
                SELECT
                    normalized_code AS code,
                    company_name,
                    scale_category,
                    nullif(sector_17_name, '') AS sector_17_name,
                    nullif(sector_33_name, '') AS sector_33_name
                FROM topix100_stocks
                ORDER BY normalized_code
                """
            ).fetchdf(),
        )
    if metadata_df.empty:
        return pd.DataFrame(
            columns=["code", "company_name", "scale_category", "sector_17_name", "sector_33_name"]
        )

    metadata_df = metadata_df.copy()
    metadata_df["code"] = metadata_df["code"].astype(str)
    metadata_df["company_name"] = metadata_df["company_name"].astype(str)
    metadata_df["scale_category"] = metadata_df["scale_category"].astype(str)
    metadata_df["sector_17_name"] = metadata_df["sector_17_name"].fillna("Unknown").astype(str)
    metadata_df["sector_33_name"] = metadata_df["sector_33_name"].fillna("Unknown").astype(str)
    return metadata_df


def _normalize_current_entry_bucket(entry_split_group: pd.Series) -> pd.Series:
    bucket_key = (
        entry_split_group.fillna("middle")
        .astype(str)
        .replace({"<NA>": "middle", "nan": "middle"})
    )
    return bucket_key.astype(str)


def _assign_regime_buckets(regime_market_df: pd.DataFrame) -> pd.DataFrame:
    if regime_market_df.empty:
        return _empty_regime_market_df()

    ordered_df = regime_market_df.sort_values(
        ["market_regime_return", "entry_date"],
        ascending=[True, True],
        kind="stable",
    ).reset_index(drop=True)
    day_count = len(ordered_df)
    bucket_count = min(3, day_count)
    if bucket_count == 1:
        keys = ["neutral"]
        labels = {"neutral": "Neutral"}
    elif bucket_count == 2:
        keys = ["weak", "strong"]
        labels = {
            "weak": "Weak regime",
            "strong": "Strong regime",
        }
    else:
        keys = ["weak", "neutral", "strong"]
        labels = {
            "weak": "Weak regime (bottom tercile)",
            "neutral": "Neutral regime (middle tercile)",
            "strong": "Strong regime (top tercile)",
        }

    bucket_key_values: list[str] = []
    for rank_index in range(day_count):
        bucket_index = min(
            bucket_count - 1,
            int(math.floor(rank_index * bucket_count / day_count)),
        )
        bucket_key_values.append(keys[bucket_index])

    ordered_df["market_regime_rank"] = range(1, day_count + 1)
    ordered_df["market_regime_bucket_key"] = bucket_key_values
    ordered_df["market_regime_bucket_label"] = ordered_df["market_regime_bucket_key"].map(labels)
    return ordered_df.loc[:, list(_REGIME_MARKET_COLUMNS)].copy()


def _build_regime_market_df(eligible_df: pd.DataFrame) -> pd.DataFrame:
    if eligible_df.empty:
        return _empty_regime_market_df()

    regime_market_df = (
        eligible_df.groupby("entry_date", as_index=False)
        .agg(
            market_regime_return=("prev_close_to_entry_return", "mean"),
            sample_count=("code", "size"),
            stock_count=("code", "nunique"),
        )
        .sort_values(["entry_date"], kind="stable")
        .reset_index(drop=True)
    )
    return _assign_regime_buckets(regime_market_df)


def _build_enriched_session_level_df(
    overnight_result: Topix1001330EntryNext1045ExitResult,
    peak_result_session_df: pd.DataFrame,
    sector_metadata_df: pd.DataFrame,
    *,
    prev_day_peak_time: str,
) -> pd.DataFrame:
    session_df = overnight_result.session_level_df.copy()
    if session_df.empty:
        return _empty_enriched_session_level_df()

    session_df["entry_date_ts"] = pd.to_datetime(session_df["entry_date"])
    session_df = session_df.sort_values(
        ["code", "entry_date_ts"],
        kind="stable",
    ).reset_index(drop=True)
    session_df["previous_entry_date"] = session_df.groupby("code")["entry_date"].shift(1)
    session_df = session_df.drop(columns=["entry_date_ts"])
    session_df = session_df.merge(sector_metadata_df, how="left", on="code")
    session_df["company_name"] = session_df["company_name"].fillna(session_df["code"]).astype(str)
    session_df["scale_category"] = session_df["scale_category"].fillna("Unknown").astype(str)
    session_df["sector_17_name"] = session_df["sector_17_name"].fillna("Unknown").astype(str)
    session_df["sector_33_name"] = session_df["sector_33_name"].fillna("Unknown").astype(str)

    current_entry_bucket_key = _normalize_current_entry_bucket(session_df["entry_split_group"])
    current_entry_label_map = _build_current_entry_bucket_label_map(
        overnight_result.tail_fraction
    )
    session_df["current_entry_bucket_key"] = current_entry_bucket_key
    session_df["current_entry_bucket_label"] = session_df["current_entry_bucket_key"].map(
        current_entry_label_map
    )

    peak_join_df = peak_result_session_df.loc[
        :,
        ["date", "code", "prev_close_to_anchor_return", "prev_close_split_group"],
    ].rename(
        columns={
            "date": "prev_day_peak_date",
            "prev_close_to_anchor_return": "prev_day_peak_return",
            "prev_close_split_group": "prev_day_peak_split_group",
        }
    )
    session_df = session_df.merge(
        peak_join_df,
        how="left",
        left_on=["previous_entry_date", "code"],
        right_on=["prev_day_peak_date", "code"],
    )
    session_df["prev_day_peak_group_key"] = "unclassified"
    session_df.loc[
        session_df["prev_day_peak_return"].notna(),
        "prev_day_peak_group_key",
    ] = "middle"
    session_df.loc[
        session_df["prev_day_peak_split_group"] == "winners",
        "prev_day_peak_group_key",
    ] = "winners"
    session_df.loc[
        session_df["prev_day_peak_split_group"] == "losers",
        "prev_day_peak_group_key",
    ] = "losers"
    prev_day_peak_label_map = _build_prev_day_peak_label_map(
        tail_fraction=overnight_result.tail_fraction,
        prev_day_peak_time=prev_day_peak_time,
    )
    session_df["prev_day_peak_group_label"] = session_df["prev_day_peak_group_key"].map(
        prev_day_peak_label_map
    )

    eligible_mask = (
        session_df["prev_close_to_entry_return"].notna()
        & session_df["entry_to_next_exit_return"].notna()
    )
    eligible_df = session_df.loc[eligible_mask].copy()
    regime_market_df = _build_regime_market_df(eligible_df)
    session_df = session_df.merge(
        regime_market_df.loc[
            :,
            ["entry_date", "market_regime_return", "market_regime_bucket_key", "market_regime_bucket_label"],
        ],
        how="left",
        on="entry_date",
    )
    return session_df.loc[:, list(_ENRICHED_SESSION_LEVEL_COLUMNS)].copy()


def _summarize_returns(
    df: pd.DataFrame,
    *,
    value_column: str,
) -> tuple[float | None, float | None, float | None]:
    if df.empty:
        return None, None, None
    values = df[value_column].dropna()
    if values.empty:
        return None, None, None
    return (
        float(values.mean()),
        float(values.median()),
        float((values > 0).mean()),
    )


def _segment_iter(
    eligible_df: pd.DataFrame,
    *,
    segment_key_column: str,
    segment_label_column: str,
):
    if eligible_df.empty:
        return []
    grouped = eligible_df.groupby(
        [segment_key_column, segment_label_column],
        as_index=False,
        dropna=False,
        sort=False,
    )
    return grouped


def _build_segment_group_summary_df(
    eligible_df: pd.DataFrame,
    *,
    segment_key_column: str,
    segment_label_column: str,
) -> pd.DataFrame:
    if eligible_df.empty:
        return _empty_segment_group_summary_df()

    rows: list[dict[str, Any]] = []
    for (segment_key, segment_label), segment_df in _segment_iter(
        eligible_df,
        segment_key_column=segment_key_column,
        segment_label_column=segment_label_column,
    ):
        tail_df = segment_df.loc[segment_df["entry_split_group"].notna()].copy()
        group_frames = (
            ("all", segment_df),
            ("winners", tail_df.loc[tail_df["entry_split_group"] == "winners"].copy()),
            ("losers", tail_df.loc[tail_df["entry_split_group"] == "losers"].copy()),
        )
        segment_sample_count = len(segment_df)
        for group_label, group_df in group_frames:
            if group_df.empty:
                continue
            (
                mean_prev_close_to_entry,
                median_prev_close_to_entry,
                _,
            ) = _summarize_returns(group_df, value_column="prev_close_to_entry_return")
            (
                mean_entry_to_close,
                median_entry_to_close,
                hit_entry_to_close,
            ) = _summarize_returns(group_df, value_column="entry_to_close_return")
            (
                mean_close_to_next_open,
                median_close_to_next_open,
                hit_close_to_next_open,
            ) = _summarize_returns(group_df, value_column="close_to_next_open_return")
            (
                mean_next_open_to_exit,
                median_next_open_to_exit,
                hit_next_open_to_exit,
            ) = _summarize_returns(group_df, value_column="next_open_to_exit_return")
            (
                mean_entry_to_next_exit,
                median_entry_to_next_exit,
                hit_entry_to_next_exit,
            ) = _summarize_returns(group_df, value_column="entry_to_next_exit_return")
            rows.append(
                {
                    "segment_key": str(segment_key),
                    "segment_label": str(segment_label),
                    "group_label": group_label,
                    "segment_sample_count": int(segment_sample_count),
                    "sample_count": int(len(group_df)),
                    "sample_share_within_segment": float(len(group_df) / segment_sample_count),
                    "date_count": int(group_df["entry_date"].nunique()),
                    "stock_count": int(group_df["code"].nunique()),
                    "mean_prev_close_to_entry_return": mean_prev_close_to_entry,
                    "median_prev_close_to_entry_return": median_prev_close_to_entry,
                    "mean_entry_to_close_return": mean_entry_to_close,
                    "median_entry_to_close_return": median_entry_to_close,
                    "entry_to_close_hit_positive": hit_entry_to_close,
                    "mean_close_to_next_open_return": mean_close_to_next_open,
                    "median_close_to_next_open_return": median_close_to_next_open,
                    "close_to_next_open_hit_positive": hit_close_to_next_open,
                    "mean_next_open_to_exit_return": mean_next_open_to_exit,
                    "median_next_open_to_exit_return": median_next_open_to_exit,
                    "next_open_to_exit_hit_positive": hit_next_open_to_exit,
                    "mean_entry_to_next_exit_return": mean_entry_to_next_exit,
                    "median_entry_to_next_exit_return": median_entry_to_next_exit,
                    "entry_to_next_exit_hit_positive": hit_entry_to_next_exit,
                }
            )
    return pd.DataFrame.from_records(rows, columns=_SEGMENT_GROUP_SUMMARY_COLUMNS)


def _build_segment_comparison_df(
    eligible_df: pd.DataFrame,
    *,
    segment_key_column: str,
    segment_label_column: str,
) -> pd.DataFrame:
    if eligible_df.empty:
        return _empty_segment_comparison_df()

    rows: list[dict[str, Any]] = []
    for (segment_key, segment_label), segment_df in _segment_iter(
        eligible_df,
        segment_key_column=segment_key_column,
        segment_label_column=segment_label_column,
    ):
        winners_df = segment_df.loc[segment_df["entry_split_group"] == "winners"].copy()
        losers_df = segment_df.loc[segment_df["entry_split_group"] == "losers"].copy()
        if winners_df.empty or losers_df.empty:
            continue
        entry_to_close_t_stat, entry_to_close_p_value = _safe_welch_t_test(
            winners_df["entry_to_close_return"],
            losers_df["entry_to_close_return"],
        )
        close_to_next_open_t_stat, close_to_next_open_p_value = _safe_welch_t_test(
            winners_df["close_to_next_open_return"],
            losers_df["close_to_next_open_return"],
        )
        next_open_to_exit_t_stat, next_open_to_exit_p_value = _safe_welch_t_test(
            winners_df["next_open_to_exit_return"],
            losers_df["next_open_to_exit_return"],
        )
        entry_to_next_exit_t_stat, entry_to_next_exit_p_value = _safe_welch_t_test(
            winners_df["entry_to_next_exit_return"],
            losers_df["entry_to_next_exit_return"],
        )
        rows.append(
            {
                "segment_key": str(segment_key),
                "segment_label": str(segment_label),
                "segment_sample_count": int(len(segment_df)),
                "winners_count": int(len(winners_df)),
                "losers_count": int(len(losers_df)),
                "winners_mean_prev_close_to_entry_return": float(
                    winners_df["prev_close_to_entry_return"].mean()
                ),
                "losers_mean_prev_close_to_entry_return": float(
                    losers_df["prev_close_to_entry_return"].mean()
                ),
                "prev_close_to_entry_mean_spread": float(
                    winners_df["prev_close_to_entry_return"].mean()
                    - losers_df["prev_close_to_entry_return"].mean()
                ),
                "winners_entry_to_close_mean": float(
                    winners_df["entry_to_close_return"].mean()
                ),
                "losers_entry_to_close_mean": float(
                    losers_df["entry_to_close_return"].mean()
                ),
                "entry_to_close_mean_spread": float(
                    winners_df["entry_to_close_return"].mean()
                    - losers_df["entry_to_close_return"].mean()
                ),
                "entry_to_close_welch_t_stat": entry_to_close_t_stat,
                "entry_to_close_welch_p_value": entry_to_close_p_value,
                "winners_close_to_next_open_mean": float(
                    winners_df["close_to_next_open_return"].mean()
                ),
                "losers_close_to_next_open_mean": float(
                    losers_df["close_to_next_open_return"].mean()
                ),
                "close_to_next_open_mean_spread": float(
                    winners_df["close_to_next_open_return"].mean()
                    - losers_df["close_to_next_open_return"].mean()
                ),
                "close_to_next_open_welch_t_stat": close_to_next_open_t_stat,
                "close_to_next_open_welch_p_value": close_to_next_open_p_value,
                "winners_next_open_to_exit_mean": float(
                    winners_df["next_open_to_exit_return"].mean()
                ),
                "losers_next_open_to_exit_mean": float(
                    losers_df["next_open_to_exit_return"].mean()
                ),
                "next_open_to_exit_mean_spread": float(
                    winners_df["next_open_to_exit_return"].mean()
                    - losers_df["next_open_to_exit_return"].mean()
                ),
                "next_open_to_exit_welch_t_stat": next_open_to_exit_t_stat,
                "next_open_to_exit_welch_p_value": next_open_to_exit_p_value,
                "winners_entry_to_next_exit_mean": float(
                    winners_df["entry_to_next_exit_return"].mean()
                ),
                "losers_entry_to_next_exit_mean": float(
                    losers_df["entry_to_next_exit_return"].mean()
                ),
                "entry_to_next_exit_mean_spread": float(
                    winners_df["entry_to_next_exit_return"].mean()
                    - losers_df["entry_to_next_exit_return"].mean()
                ),
                "entry_to_next_exit_welch_t_stat": entry_to_next_exit_t_stat,
                "entry_to_next_exit_welch_p_value": entry_to_next_exit_p_value,
            }
        )
    return pd.DataFrame.from_records(rows, columns=_SEGMENT_COMPARISON_COLUMNS)


def _build_prev_day_peak_transition_df(eligible_df: pd.DataFrame) -> pd.DataFrame:
    if eligible_df.empty:
        return _empty_prev_day_peak_transition_df()

    counts_by_prev_day = eligible_df.groupby("prev_day_peak_group_key")["code"].transform("size")
    counts_by_current = eligible_df.groupby("current_entry_bucket_key")["code"].transform("size")
    working_df = eligible_df.assign(
        prev_day_group_size=counts_by_prev_day,
        current_group_size=counts_by_current,
    )
    transition_df = (
        working_df.groupby(
            [
                "prev_day_peak_group_key",
                "prev_day_peak_group_label",
                "current_entry_bucket_key",
                "current_entry_bucket_label",
            ],
            as_index=False,
        )
        .agg(
            sample_count=("code", "size"),
            prev_day_group_size=("prev_day_group_size", "first"),
            current_group_size=("current_group_size", "first"),
            mean_prev_close_to_entry_return=("prev_close_to_entry_return", "mean"),
            mean_entry_to_next_exit_return=("entry_to_next_exit_return", "mean"),
        )
    )
    transition_df["share_within_prev_day_peak_group"] = (
        transition_df["sample_count"] / transition_df["prev_day_group_size"]
    )
    transition_df["share_within_current_entry_bucket"] = (
        transition_df["sample_count"] / transition_df["current_group_size"]
    )
    transition_df = transition_df.drop(columns=["prev_day_group_size", "current_group_size"])
    return transition_df.loc[:, list(_PREV_DAY_PEAK_TRANSITION_COLUMNS)].copy()


def _sort_segment_group_summary_df(
    df: pd.DataFrame,
    *,
    segment_order: dict[str, int] | None = None,
) -> pd.DataFrame:
    if df.empty:
        return df
    ordered = df.copy()
    if segment_order is not None:
        ordered["_segment_order"] = ordered["segment_key"].map(segment_order).fillna(9999)
    else:
        ordered["_segment_order"] = 0
    ordered["_group_order"] = ordered["group_label"].map(
        {value: index for index, value in enumerate(_GROUP_LABEL_ORDER)}
    ).fillna(9999)
    ordered = ordered.sort_values(
        ["_segment_order", "segment_label", "_group_order"],
        kind="stable",
    ).drop(columns=["_segment_order", "_group_order"])
    return ordered.reset_index(drop=True)


def _sort_segment_comparison_df(
    df: pd.DataFrame,
    *,
    segment_order: dict[str, int] | None = None,
) -> pd.DataFrame:
    if df.empty:
        return df
    ordered = df.copy()
    if segment_order is not None:
        ordered["_segment_order"] = ordered["segment_key"].map(segment_order).fillna(9999)
    else:
        ordered["_segment_order"] = 0
    ordered = ordered.sort_values(
        ["_segment_order", "segment_label"],
        kind="stable",
    ).drop(columns=["_segment_order"])
    return ordered.reset_index(drop=True)


def _sort_prev_day_peak_transition_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    ordered = df.copy()
    ordered["_prev_day_order"] = ordered["prev_day_peak_group_key"].map(
        {value: index for index, value in enumerate(_PREV_DAY_PEAK_ORDER)}
    ).fillna(9999)
    ordered["_current_order"] = ordered["current_entry_bucket_key"].map(
        {value: index for index, value in enumerate(_CURRENT_ENTRY_BUCKET_ORDER)}
    ).fillna(9999)
    ordered = ordered.sort_values(
        ["_prev_day_order", "_current_order"],
        kind="stable",
    ).drop(columns=["_prev_day_order", "_current_order"])
    return ordered.reset_index(drop=True)


def run_topix100_1330_entry_next_1045_exit_conditioning_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    interval_minutes: int = DEFAULT_INTERVAL_MINUTES,
    entry_time: str = DEFAULT_ENTRY_TIME,
    exit_time: str = DEFAULT_EXIT_TIME,
    tail_fraction: float = DEFAULT_TAIL_FRACTION,
    prev_day_peak_time: str = DEFAULT_PREV_DAY_PEAK_TIME,
) -> Topix1001330EntryNext1045ExitConditioningResult:
    validated_prev_day_peak_time = _validate_time_label(
        prev_day_peak_time,
        argument_name="prev_day_peak_time",
    )

    overnight_result = run_topix100_1330_entry_next_1045_exit_research(
        db_path,
        start_date=start_date,
        end_date=end_date,
        interval_minutes=interval_minutes,
        entry_time=entry_time,
        exit_time=exit_time,
        tail_fraction=tail_fraction,
    )
    peak_result = run_topix100_peak_winner_loser_intraday_path_research(
        db_path,
        start_date=start_date,
        end_date=end_date,
        interval_minutes=interval_minutes,
        anchor_candidate_times=(validated_prev_day_peak_time,),
        midday_reference_time="13:30",
        tail_fraction=tail_fraction,
    )
    sector_metadata_df = _fetch_topix100_sector_metadata_df(db_path)

    enriched_session_level_df = _build_enriched_session_level_df(
        overnight_result,
        peak_result.session_level_df,
        sector_metadata_df,
        prev_day_peak_time=validated_prev_day_peak_time,
    )
    eligible_df = enriched_session_level_df.loc[
        enriched_session_level_df["prev_close_to_entry_return"].notna()
        & enriched_session_level_df["entry_to_next_exit_return"].notna()
    ].copy()
    if eligible_df.empty:
        raise ValueError("No eligible TOPIX100 13:30 -> next 10:45 sessions were available.")

    regime_market_df = _build_regime_market_df(eligible_df)
    regime_order = {value: index for index, value in enumerate(("weak", "neutral", "strong"))}
    regime_group_summary_df = _sort_segment_group_summary_df(
        _build_segment_group_summary_df(
            eligible_df,
            segment_key_column="market_regime_bucket_key",
            segment_label_column="market_regime_bucket_label",
        ),
        segment_order=regime_order,
    )
    regime_comparison_df = _sort_segment_comparison_df(
        _build_segment_comparison_df(
            eligible_df,
            segment_key_column="market_regime_bucket_key",
            segment_label_column="market_regime_bucket_label",
        ),
        segment_order=regime_order,
    )

    sector_group_summary_df = _build_segment_group_summary_df(
        eligible_df,
        segment_key_column="sector_33_name",
        segment_label_column="sector_33_name",
    )
    if not sector_group_summary_df.empty:
        sector_all_counts = sector_group_summary_df.loc[
            sector_group_summary_df["group_label"] == "all",
            ["segment_key", "segment_sample_count", "mean_entry_to_next_exit_return"],
        ].copy()
        sector_all_counts = sector_all_counts.sort_values(
            ["segment_sample_count", "mean_entry_to_next_exit_return", "segment_key"],
            ascending=[False, False, True],
            kind="stable",
        ).reset_index(drop=True)
        sector_order = {
            str(row.segment_key): index
            for index, row in enumerate(sector_all_counts.itertuples(index=False))
        }
        sector_group_summary_df = _sort_segment_group_summary_df(
            sector_group_summary_df,
            segment_order=sector_order,
        )
        sector_comparison_df = _sort_segment_comparison_df(
            _build_segment_comparison_df(
                eligible_df,
                segment_key_column="sector_33_name",
                segment_label_column="sector_33_name",
            ),
            segment_order=sector_order,
        )
    else:
        sector_comparison_df = _empty_segment_comparison_df()

    prev_day_peak_group_summary_df = _sort_segment_group_summary_df(
        _build_segment_group_summary_df(
            eligible_df,
            segment_key_column="prev_day_peak_group_key",
            segment_label_column="prev_day_peak_group_label",
        ),
        segment_order={value: index for index, value in enumerate(_PREV_DAY_PEAK_ORDER)},
    )
    prev_day_peak_comparison_df = _sort_segment_comparison_df(
        _build_segment_comparison_df(
            eligible_df,
            segment_key_column="prev_day_peak_group_key",
            segment_label_column="prev_day_peak_group_label",
        ),
        segment_order={value: index for index, value in enumerate(_PREV_DAY_PEAK_ORDER)},
    )
    prev_day_peak_transition_df = _sort_prev_day_peak_transition_df(
        _build_prev_day_peak_transition_df(eligible_df)
    )

    return Topix1001330EntryNext1045ExitConditioningResult(
        db_path=overnight_result.db_path,
        source_mode=overnight_result.source_mode,
        source_detail=overnight_result.source_detail,
        available_start_date=overnight_result.available_start_date,
        available_end_date=overnight_result.available_end_date,
        analysis_start_date=overnight_result.analysis_start_date,
        analysis_end_date=overnight_result.analysis_end_date,
        interval_minutes=overnight_result.interval_minutes,
        entry_time=overnight_result.entry_time,
        exit_time=overnight_result.exit_time,
        tail_fraction=overnight_result.tail_fraction,
        prev_day_peak_time=validated_prev_day_peak_time,
        topix100_constituent_count=overnight_result.topix100_constituent_count,
        total_entry_session_count=overnight_result.total_entry_session_count,
        eligible_session_count=overnight_result.eligible_session_count,
        regime_day_count=int(regime_market_df["entry_date"].nunique()),
        enriched_session_level_df=enriched_session_level_df,
        regime_market_df=regime_market_df,
        regime_group_summary_df=regime_group_summary_df,
        regime_comparison_df=regime_comparison_df,
        sector_group_summary_df=sector_group_summary_df,
        sector_comparison_df=sector_comparison_df,
        prev_day_peak_group_summary_df=prev_day_peak_group_summary_df,
        prev_day_peak_comparison_df=prev_day_peak_comparison_df,
        prev_day_peak_transition_df=prev_day_peak_transition_df,
    )


def _split_result_payload(
    result: Topix1001330EntryNext1045ExitConditioningResult,
) -> tuple[dict[str, Any], dict[str, pd.DataFrame]]:
    return (
        {
            "db_path": result.db_path,
            "source_mode": result.source_mode,
            "source_detail": result.source_detail,
            "available_start_date": result.available_start_date,
            "available_end_date": result.available_end_date,
            "analysis_start_date": result.analysis_start_date,
            "analysis_end_date": result.analysis_end_date,
            "interval_minutes": result.interval_minutes,
            "entry_time": result.entry_time,
            "exit_time": result.exit_time,
            "tail_fraction": result.tail_fraction,
            "prev_day_peak_time": result.prev_day_peak_time,
            "topix100_constituent_count": result.topix100_constituent_count,
            "total_entry_session_count": result.total_entry_session_count,
            "eligible_session_count": result.eligible_session_count,
            "regime_day_count": result.regime_day_count,
        },
        {
            "enriched_session_level_df": result.enriched_session_level_df,
            "regime_market_df": result.regime_market_df,
            "regime_group_summary_df": result.regime_group_summary_df,
            "regime_comparison_df": result.regime_comparison_df,
            "sector_group_summary_df": result.sector_group_summary_df,
            "sector_comparison_df": result.sector_comparison_df,
            "prev_day_peak_group_summary_df": result.prev_day_peak_group_summary_df,
            "prev_day_peak_comparison_df": result.prev_day_peak_comparison_df,
            "prev_day_peak_transition_df": result.prev_day_peak_transition_df,
        },
    )


def _build_result_from_payload(
    metadata: dict[str, Any],
    tables: dict[str, pd.DataFrame],
) -> Topix1001330EntryNext1045ExitConditioningResult:
    return Topix1001330EntryNext1045ExitConditioningResult(
        db_path=str(metadata["db_path"]),
        source_mode=cast(SourceMode, metadata["source_mode"]),
        source_detail=str(metadata["source_detail"]),
        available_start_date=cast(str | None, metadata.get("available_start_date")),
        available_end_date=cast(str | None, metadata.get("available_end_date")),
        analysis_start_date=cast(str | None, metadata.get("analysis_start_date")),
        analysis_end_date=cast(str | None, metadata.get("analysis_end_date")),
        interval_minutes=int(metadata["interval_minutes"]),
        entry_time=str(metadata["entry_time"]),
        exit_time=str(metadata["exit_time"]),
        tail_fraction=float(metadata["tail_fraction"]),
        prev_day_peak_time=str(metadata["prev_day_peak_time"]),
        topix100_constituent_count=int(metadata["topix100_constituent_count"]),
        total_entry_session_count=int(metadata["total_entry_session_count"]),
        eligible_session_count=int(metadata["eligible_session_count"]),
        regime_day_count=int(metadata["regime_day_count"]),
        enriched_session_level_df=tables["enriched_session_level_df"],
        regime_market_df=tables["regime_market_df"],
        regime_group_summary_df=tables["regime_group_summary_df"],
        regime_comparison_df=tables["regime_comparison_df"],
        sector_group_summary_df=tables["sector_group_summary_df"],
        sector_comparison_df=tables["sector_comparison_df"],
        prev_day_peak_group_summary_df=tables["prev_day_peak_group_summary_df"],
        prev_day_peak_comparison_df=tables["prev_day_peak_comparison_df"],
        prev_day_peak_transition_df=tables["prev_day_peak_transition_df"],
    )


def _build_published_summary(
    result: Topix1001330EntryNext1045ExitConditioningResult,
) -> dict[str, Any]:
    return {
        "intervalMinutes": result.interval_minutes,
        "entryTime": result.entry_time,
        "exitTime": result.exit_time,
        "tailFraction": result.tail_fraction,
        "prevDayPeakTime": result.prev_day_peak_time,
        "analysisStartDate": result.analysis_start_date,
        "analysisEndDate": result.analysis_end_date,
        "regimeSummary": result.regime_comparison_df.to_dict(orient="records"),
        "sectorSummary": result.sector_comparison_df.to_dict(orient="records"),
        "prevDayPeakSummary": result.prev_day_peak_comparison_df.to_dict(orient="records"),
    }


def _format_pct(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"{float(value) * 100:+.4f}%"


def _build_research_bundle_summary_markdown(
    result: Topix1001330EntryNext1045ExitConditioningResult,
) -> str:
    summary_lines = [
        "# TOPIX100 13:30 Entry -> Next 10:45 Exit Conditioning",
        "",
        "## Snapshot",
        "",
        f"- Source mode: `{result.source_mode}`",
        f"- Available range: `{result.available_start_date} -> {result.available_end_date}`",
        f"- Analysis range: `{result.analysis_start_date} -> {result.analysis_end_date}`",
        f"- Interval minutes: `{result.interval_minutes}`",
        f"- Entry time: `{result.entry_time}`",
        f"- Exit time: `{result.exit_time}`",
        f"- Previous-day peak anchor: `{result.prev_day_peak_time}`",
        f"- Tail fraction per side: `{result.tail_fraction * 100:.1f}%`",
        f"- Current TOPIX100 constituents: `{result.topix100_constituent_count}`",
        f"- Eligible entry sessions: `{result.eligible_session_count}`",
        f"- Regime day count: `{result.regime_day_count}`",
        "",
        "## Current Read",
        "",
    ]

    if result.regime_comparison_df.empty:
        summary_lines.append("- Regime comparison rows were empty.")
    else:
        for row in result.regime_comparison_df.itertuples(index=False):
            summary_lines.append(
                f"- `{row.segment_label}`: winners-minus-losers "
                f"`{result.entry_time} -> D+1 {result.exit_time}` spread "
                f"`{_format_pct(cast(float | None, row.entry_to_next_exit_mean_spread))}` "
                f"(p=`{row.entry_to_next_exit_welch_p_value}`)."
            )

    if not result.sector_comparison_df.empty:
        best_sector = result.sector_comparison_df.sort_values(
            ["entry_to_next_exit_mean_spread", "segment_label"],
            ascending=[False, True],
            kind="stable",
        ).iloc[0]
        weakest_sector = result.sector_comparison_df.sort_values(
            ["entry_to_next_exit_mean_spread", "segment_label"],
            ascending=[True, True],
            kind="stable",
        ).iloc[0]
        summary_lines.extend(
            [
                "",
                "## Sector Read",
                "",
                (
                    f"- Largest positive winners-minus-losers sector spread: "
                    f"`{best_sector['segment_label']}` "
                    f"`{_format_pct(float(best_sector['entry_to_next_exit_mean_spread']))}`."
                ),
                (
                    f"- Largest negative winners-minus-losers sector spread: "
                    f"`{weakest_sector['segment_label']}` "
                    f"`{_format_pct(float(weakest_sector['entry_to_next_exit_mean_spread']))}`."
                ),
            ]
        )

    if not result.prev_day_peak_transition_df.empty:
        strongest_transition = result.prev_day_peak_transition_df.sort_values(
            ["mean_entry_to_next_exit_return", "sample_count"],
            ascending=[False, False],
            kind="stable",
        ).iloc[0]
        weakest_transition = result.prev_day_peak_transition_df.sort_values(
            ["mean_entry_to_next_exit_return", "sample_count"],
            ascending=[True, False],
            kind="stable",
        ).iloc[0]
        summary_lines.extend(
            [
                "",
                "## Previous-Day 10:45 Cross",
                "",
                (
                    f"- Strongest cell: `{strongest_transition['prev_day_peak_group_label']}` x "
                    f"`{strongest_transition['current_entry_bucket_label']}` "
                    f"`{_format_pct(float(strongest_transition['mean_entry_to_next_exit_return']))}` "
                    f"({int(strongest_transition['sample_count'])} sessions)."
                ),
                (
                    f"- Weakest cell: `{weakest_transition['prev_day_peak_group_label']}` x "
                    f"`{weakest_transition['current_entry_bucket_label']}` "
                    f"`{_format_pct(float(weakest_transition['mean_entry_to_next_exit_return']))}` "
                    f"({int(weakest_transition['sample_count'])} sessions)."
                ),
            ]
        )

    summary_lines.extend(
        [
            "",
            "## Artifact Plots",
            "",
            f"- `{TOPIX100_1330_ENTRY_NEXT_1045_EXIT_CONDITIONING_PLOT_FILENAME}`",
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


def _plot_grouped_regime_bars(axis: Any, regime_group_summary_df: pd.DataFrame) -> None:
    if regime_group_summary_df.empty:
        axis.text(0.5, 0.5, "No regime data", ha="center", va="center", transform=axis.transAxes)
        return
    color_map = {
        "all": "#6b7280",
        "winners": "#2563eb",
        "losers": "#dc2626",
    }
    group_order = ("all", "winners", "losers")
    regime_rows = regime_group_summary_df.loc[
        regime_group_summary_df["group_label"] == "all",
        ["segment_key", "segment_label"],
    ].copy()
    regime_rows = regime_rows.drop_duplicates().reset_index(drop=True)
    x_positions = list(range(len(regime_rows)))
    width = 0.24
    for group_index, group_label in enumerate(group_order):
        group_df = regime_group_summary_df.loc[
            regime_group_summary_df["group_label"] == group_label
        ].copy()
        heights = []
        for row in regime_rows.itertuples(index=False):
            scoped = group_df.loc[group_df["segment_key"] == row.segment_key]
            heights.append(
                float(scoped["mean_entry_to_next_exit_return"].iloc[0]) * 100.0
                if not scoped.empty
                else 0.0
            )
        shifted_positions = [
            position + (group_index - 1) * width for position in x_positions
        ]
        axis.bar(
            shifted_positions,
            heights,
            width=width,
            color=color_map[group_label],
            label=group_label.title(),
        )
    axis.axhline(0.0, color="#111827", linewidth=0.8, alpha=0.8)
    axis.set_xticks(x_positions)
    axis.set_xticklabels(regime_rows["segment_label"], fontsize=8)
    axis.set_ylabel("Mean return (%)")
    axis.set_title("Market regime split", fontsize=10)
    axis.grid(axis="y", alpha=0.2, linewidth=0.6)
    axis.legend(loc="best", frameon=False, fontsize=8)


def _plot_heatmap(
    axis: Any,
    *,
    pivot_df: pd.DataFrame,
    title: str,
    x_label: str,
    y_label: str,
    figure: Any,
) -> None:
    if pivot_df.empty:
        axis.text(0.5, 0.5, "No data", ha="center", va="center", transform=axis.transAxes)
        axis.set_title(title, fontsize=10)
        return
    image = axis.imshow(pivot_df.to_numpy(dtype=float), aspect="auto", cmap="coolwarm")
    axis.set_xticks(range(len(pivot_df.columns)))
    axis.set_xticklabels(list(pivot_df.columns), fontsize=8)
    axis.set_yticks(range(len(pivot_df.index)))
    axis.set_yticklabels(list(pivot_df.index), fontsize=7)
    axis.set_xlabel(x_label)
    axis.set_ylabel(y_label)
    axis.set_title(title, fontsize=10)
    figure.colorbar(image, ax=axis, fraction=0.025, pad=0.02, label="Mean return (%)")


def write_topix100_1330_entry_next_1045_exit_conditioning_overview_plot(
    result: Topix1001330EntryNext1045ExitConditioningResult,
    *,
    output_path: str | Path,
) -> Path:
    plt = _import_matplotlib_pyplot()
    try:
        from matplotlib import font_manager

        preferred_fonts = (
            "Hiragino Sans",
            "Yu Gothic",
            "Meiryo",
            "Noto Sans CJK JP",
        )
        available_fonts = {entry.name for entry in font_manager.fontManager.ttflist}
        configured_fonts = [
            font_name for font_name in preferred_fonts if font_name in available_fonts
        ]
        plt.rcParams["font.family"] = configured_fonts or ["sans-serif"]
    except Exception:
        plt.rcParams["font.family"] = ["sans-serif"]
    output_path = Path(output_path).expanduser()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fig, axes = plt.subplots(3, 1, figsize=(14, 14), constrained_layout=True)
    _plot_grouped_regime_bars(axes[0], result.regime_group_summary_df)

    sector_pivot_df = pd.DataFrame()
    if not result.sector_group_summary_df.empty:
        sector_plot_df = result.sector_group_summary_df.loc[
            result.sector_group_summary_df["group_label"].isin(("all", "winners", "losers"))
        ].copy()
        sector_pivot_df = (
            sector_plot_df.pivot(
                index="segment_label",
                columns="group_label",
                values="mean_entry_to_next_exit_return",
            )
            * 100.0
        )
        desired_columns = [value for value in ("all", "winners", "losers") if value in sector_pivot_df.columns]
        sector_pivot_df = sector_pivot_df.loc[:, desired_columns]
    _plot_heatmap(
        axes[1],
        pivot_df=sector_pivot_df,
        title="Sector split",
        x_label="Current 13:30 group",
        y_label="Sector 33",
        figure=fig,
    )

    transition_pivot_df = pd.DataFrame()
    if not result.prev_day_peak_transition_df.empty:
        current_entry_label_map = _build_current_entry_bucket_label_map(
            result.tail_fraction
        )
        prev_day_peak_label_map = _build_prev_day_peak_label_map(
            tail_fraction=result.tail_fraction,
            prev_day_peak_time=result.prev_day_peak_time,
        )
        transition_pivot_df = (
            result.prev_day_peak_transition_df.pivot(
                index="prev_day_peak_group_label",
                columns="current_entry_bucket_label",
                values="mean_entry_to_next_exit_return",
            )
            * 100.0
        )
        desired_columns = [
            current_entry_label_map[key]
            for key in _CURRENT_ENTRY_BUCKET_ORDER
            if current_entry_label_map[key] in transition_pivot_df.columns
        ]
        desired_index = [
            prev_day_peak_label_map[key]
            for key in _PREV_DAY_PEAK_ORDER
            if prev_day_peak_label_map[key] in transition_pivot_df.index
        ]
        transition_pivot_df = transition_pivot_df.loc[desired_index, desired_columns]
    _plot_heatmap(
        axes[2],
        pivot_df=transition_pivot_df,
        title="Previous-day 10:45 cross",
        x_label="Current 13:30 group",
        y_label="Previous-day 10:45 group",
        figure=fig,
    )

    fig.suptitle(
        f"TOPIX100 {result.entry_time} -> next-session {result.exit_time} conditioning",
        fontsize=12,
    )
    fig.savefig(output_path, dpi=160, bbox_inches="tight")
    plt.close(fig)
    return output_path


def write_topix100_1330_entry_next_1045_exit_conditioning_research_bundle(
    result: Topix1001330EntryNext1045ExitConditioningResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    metadata, tables = _split_result_payload(result)
    bundle = write_research_bundle(
        experiment_id=TOPIX100_1330_ENTRY_NEXT_1045_EXIT_CONDITIONING_EXPERIMENT_ID,
        module=__name__,
        function="run_topix100_1330_entry_next_1045_exit_conditioning_research",
        params={
            "start_date": result.analysis_start_date,
            "end_date": result.analysis_end_date,
            "interval_minutes": result.interval_minutes,
            "entry_time": result.entry_time,
            "exit_time": result.exit_time,
            "tail_fraction": result.tail_fraction,
            "prev_day_peak_time": result.prev_day_peak_time,
        },
        db_path=result.db_path,
        analysis_start_date=result.analysis_start_date,
        analysis_end_date=result.analysis_end_date,
        result_metadata=metadata,
        result_tables=tables,
        summary_markdown=_build_research_bundle_summary_markdown(result),
        published_summary=_build_published_summary(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )
    write_bundle_artifact(
        bundle,
        TOPIX100_1330_ENTRY_NEXT_1045_EXIT_CONDITIONING_PLOT_FILENAME,
        lambda output_path: write_topix100_1330_entry_next_1045_exit_conditioning_overview_plot(
            result,
            output_path=output_path,
        ),
    )
    return bundle


def load_topix100_1330_entry_next_1045_exit_conditioning_research_bundle(
    bundle_path: str | Path,
) -> Topix1001330EntryNext1045ExitConditioningResult:
    info = load_research_bundle_info(bundle_path)
    tables = load_research_bundle_tables(bundle_path)
    return _build_result_from_payload(dict(info.result_metadata), tables)


def get_topix100_1330_entry_next_1045_exit_conditioning_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        TOPIX100_1330_ENTRY_NEXT_1045_EXIT_CONDITIONING_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_topix100_1330_entry_next_1045_exit_conditioning_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        TOPIX100_1330_ENTRY_NEXT_1045_EXIT_CONDITIONING_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )
