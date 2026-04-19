# pyright: reportUnusedFunction=false
"""Support helpers for TOPIX100 13:30 to next-session 10:45 conditioning."""

from __future__ import annotations

import math
from typing import Any

import pandas as pd

from src.domains.analytics.topix100_1330_entry_next_1045_exit import (
    Topix1001330EntryNext1045ExitResult,
)
from src.domains.analytics.topix100_second_bar_volume_drop_performance import (
    _safe_welch_t_test,
)

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
            [
                "entry_date",
                "market_regime_return",
                "market_regime_bucket_key",
                "market_regime_bucket_label",
            ],
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
