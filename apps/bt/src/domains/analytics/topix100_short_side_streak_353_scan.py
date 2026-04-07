"""
TOPIX100 short-side scan under the fixed streak 3/53 transfer model.

This study keeps the existing strongest long setup fixed:

- decile band: `Q10`
- volume bucket: `Volume Low`
- state: `Long Bearish / Short Bearish`

It then asks two separate questions on the same TOPIX100 panel:

1. Which contiguous decile band x volume bucket x streak state is the weakest
   standalone short candidate on 5d/10d forward returns?
2. Which candidate creates the best date-aligned long-short spread when paired
   against the fixed strongest long setup?
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pandas as pd

from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    find_latest_research_bundle_path,
    get_research_bundle_dir,
    load_dataclass_research_bundle,
    write_dataclass_research_bundle,
)
from src.domains.analytics.topix100_price_vs_sma_q10_bounce_regime_conditioning import (
    DEFAULT_PRICE_FEATURE,
    DEFAULT_VOLUME_FEATURE,
)
from src.domains.analytics.topix100_price_vs_sma_rank_future_close import (
    PRICE_FEATURE_LABEL_MAP,
    PRICE_FEATURE_ORDER,
    PRICE_SMA_WINDOW_ORDER,
    VOLUME_BUCKET_LABEL_MAP,
    VOLUME_FEATURE_LABEL_MAP,
    VOLUME_FEATURE_ORDER,
    VOLUME_SMA_WINDOW_ORDER,
    run_topix100_price_vs_sma_rank_future_close_research,
)
from src.domains.analytics.topix100_streak_353_transfer import (
    DEFAULT_LONG_WINDOW_STREAKS,
    DEFAULT_SHORT_WINDOW_STREAKS,
    run_topix100_streak_353_transfer_research,
)
from src.domains.analytics.topix100_strongest_setup_q10_threshold import (
    _build_state_decile_horizon_panel,
)
from src.domains.analytics.topix_close_return_streaks import (
    DEFAULT_VALIDATION_RATIO,
    _normalize_positive_int_sequence,
)
from src.domains.analytics.topix_close_stock_overnight_distribution import SourceMode
from src.domains.analytics.topix_rank_future_close_core import DECILE_ORDER
from src.domains.analytics.topix_streak_extreme_mode import (
    _format_int_sequence,
    _format_return,
)

DEFAULT_FUTURE_HORIZONS: tuple[int, ...] = (1, 5, 10)
DEFAULT_STRONGEST_LOWER_DECILE = 10
DEFAULT_STRONGEST_UPPER_DECILE = 10
DEFAULT_STRONGEST_STATE_KEY = "long_bearish__short_bearish"
DEFAULT_STRONGEST_STATE_LABEL = "Long Bearish / Short Bearish"
DEFAULT_STRONGEST_VOLUME_BUCKET = "volume_low"
DEFAULT_STRONGEST_VOLUME_BUCKET_LABEL = "Volume Low Half"
DEFAULT_MIN_VALIDATION_DATE_COUNT = 120
DEFAULT_MIN_PAIR_OVERLAP_DATES = 120
DEFAULT_USER_HYPOTHESIS_BAND_LABEL = "Q2-Q4"
DEFAULT_USER_HYPOTHESIS_STATE_KEY = "long_bullish__short_bullish"
DEFAULT_USER_HYPOTHESIS_STATE_LABEL = "Long Bullish / Short Bullish"
TOPIX100_SHORT_SIDE_STREAK_353_SCAN_EXPERIMENT_ID = (
    "market-behavior/topix100-short-side-streak-3-53-scan"
)
_SPLIT_ORDER: tuple[str, ...] = ("full", "discovery", "validation")
_RESULT_TABLE_NAMES: tuple[str, ...] = (
    "state_decile_horizon_panel_df",
    "state_band_horizon_summary_df",
    "short_candidate_scorecard_df",
    "pair_trade_scorecard_df",
    "validation_focus_matrix_df",
    "validation_bull_bull_adjacent_pair_df",
)


@dataclass(frozen=True)
class Topix100ShortSideStreak353ScanResearchResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    price_feature: str
    price_feature_label: str
    volume_feature: str
    volume_feature_label: str
    short_window_streaks: int
    long_window_streaks: int
    future_horizons: tuple[int, ...]
    validation_ratio: float
    strongest_lower_decile: int
    strongest_upper_decile: int
    strongest_state_key: str
    strongest_state_label: str
    strongest_volume_bucket: str
    strongest_volume_bucket_label: str
    min_validation_date_count: int
    min_pair_overlap_dates: int
    universe_constituent_count: int
    covered_constituent_count: int
    joined_event_count: int
    valid_date_count: int
    state_decile_horizon_panel_df: pd.DataFrame
    state_band_horizon_summary_df: pd.DataFrame
    short_candidate_scorecard_df: pd.DataFrame
    pair_trade_scorecard_df: pd.DataFrame
    validation_focus_matrix_df: pd.DataFrame
    validation_bull_bull_adjacent_pair_df: pd.DataFrame


def run_topix100_short_side_streak_353_scan_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    price_feature: str = DEFAULT_PRICE_FEATURE,
    volume_feature: str = DEFAULT_VOLUME_FEATURE,
    future_horizons: tuple[int, ...] | list[int] | None = None,
    validation_ratio: float = DEFAULT_VALIDATION_RATIO,
    short_window_streaks: int = DEFAULT_SHORT_WINDOW_STREAKS,
    long_window_streaks: int = DEFAULT_LONG_WINDOW_STREAKS,
    strongest_lower_decile: int = DEFAULT_STRONGEST_LOWER_DECILE,
    strongest_upper_decile: int = DEFAULT_STRONGEST_UPPER_DECILE,
    strongest_state_key: str = DEFAULT_STRONGEST_STATE_KEY,
    strongest_state_label: str = DEFAULT_STRONGEST_STATE_LABEL,
    strongest_volume_bucket: str = DEFAULT_STRONGEST_VOLUME_BUCKET,
    min_validation_date_count: int = DEFAULT_MIN_VALIDATION_DATE_COUNT,
    min_pair_overlap_dates: int = DEFAULT_MIN_PAIR_OVERLAP_DATES,
) -> Topix100ShortSideStreak353ScanResearchResult:
    resolved_horizons = _normalize_positive_int_sequence(
        future_horizons,
        default=DEFAULT_FUTURE_HORIZONS,
        name="future_horizons",
    )
    if price_feature not in PRICE_FEATURE_LABEL_MAP:
        raise ValueError(f"Unsupported price_feature: {price_feature}")
    if volume_feature not in VOLUME_FEATURE_LABEL_MAP:
        raise ValueError(f"Unsupported volume_feature: {volume_feature}")
    if strongest_volume_bucket not in VOLUME_BUCKET_LABEL_MAP:
        raise ValueError(f"Unsupported strongest_volume_bucket: {strongest_volume_bucket}")
    if short_window_streaks >= long_window_streaks:
        raise ValueError("short_window_streaks must be smaller than long_window_streaks")
    if strongest_lower_decile <= 0 or strongest_upper_decile <= 0:
        raise ValueError("strongest deciles must be positive")
    if strongest_lower_decile > strongest_upper_decile:
        raise ValueError("strongest_lower_decile must not exceed strongest_upper_decile")
    if strongest_upper_decile > len(DECILE_ORDER):
        raise ValueError("strongest_upper_decile exceeded the supported decile range")
    if min_validation_date_count <= 0:
        raise ValueError("min_validation_date_count must be positive")
    if min_pair_overlap_dates <= 0:
        raise ValueError("min_pair_overlap_dates must be positive")

    price_feature_to_window = {
        feature: window
        for feature, window in zip(PRICE_FEATURE_ORDER, PRICE_SMA_WINDOW_ORDER, strict=True)
    }
    volume_feature_to_window = {
        feature: window
        for feature, window in zip(VOLUME_FEATURE_ORDER, VOLUME_SMA_WINDOW_ORDER, strict=True)
    }
    price_result = run_topix100_price_vs_sma_rank_future_close_research(
        db_path,
        start_date=start_date,
        end_date=end_date,
        price_sma_windows=(price_feature_to_window[price_feature],),
        volume_sma_windows=(volume_feature_to_window[volume_feature],),
    )
    state_result = run_topix100_streak_353_transfer_research(
        db_path,
        start_date=start_date,
        end_date=end_date,
        future_horizons=resolved_horizons,
        validation_ratio=validation_ratio,
        short_window_streaks=short_window_streaks,
        long_window_streaks=long_window_streaks,
        min_stock_events_per_state=1,
        min_constituents_per_date_state=1,
    )

    state_decile_horizon_panel_df = _build_state_decile_horizon_panel(
        event_panel_df=price_result.event_panel_df,
        state_horizon_event_df=state_result.state_horizon_event_df,
        price_feature=price_feature,
        volume_feature=volume_feature,
        future_horizons=resolved_horizons,
    )
    state_band_horizon_summary_df = _build_state_band_horizon_summary_df(
        state_decile_horizon_panel_df
    )
    short_candidate_scorecard_df = _build_short_candidate_scorecard_df(
        state_band_horizon_summary_df,
        future_horizons=resolved_horizons,
        min_validation_date_count=min_validation_date_count,
    )
    pair_trade_scorecard_df = _build_pair_trade_scorecard_df(
        state_decile_horizon_panel_df,
        future_horizons=resolved_horizons,
        strongest_lower_decile=strongest_lower_decile,
        strongest_upper_decile=strongest_upper_decile,
        strongest_state_key=strongest_state_key,
        strongest_volume_bucket=strongest_volume_bucket,
        min_pair_overlap_dates=min_pair_overlap_dates,
    )
    validation_focus_matrix_df = _build_validation_focus_matrix_df(
        short_candidate_scorecard_df=short_candidate_scorecard_df,
        pair_trade_scorecard_df=pair_trade_scorecard_df,
    )
    validation_bull_bull_adjacent_pair_df = _build_validation_bull_bull_adjacent_pair_df(
        short_candidate_scorecard_df
    )

    analysis_start_date = (
        str(state_decile_horizon_panel_df["date"].min())
        if not state_decile_horizon_panel_df.empty
        else None
    )
    analysis_end_date = (
        str(state_decile_horizon_panel_df["date"].max())
        if not state_decile_horizon_panel_df.empty
        else None
    )

    return Topix100ShortSideStreak353ScanResearchResult(
        db_path=db_path,
        source_mode=cast(SourceMode, price_result.source_mode),
        source_detail=str(price_result.source_detail),
        available_start_date=price_result.available_start_date,
        available_end_date=price_result.available_end_date,
        analysis_start_date=analysis_start_date,
        analysis_end_date=analysis_end_date,
        price_feature=price_feature,
        price_feature_label=PRICE_FEATURE_LABEL_MAP[price_feature],
        volume_feature=volume_feature,
        volume_feature_label=VOLUME_FEATURE_LABEL_MAP[volume_feature],
        short_window_streaks=short_window_streaks,
        long_window_streaks=long_window_streaks,
        future_horizons=resolved_horizons,
        validation_ratio=validation_ratio,
        strongest_lower_decile=strongest_lower_decile,
        strongest_upper_decile=strongest_upper_decile,
        strongest_state_key=strongest_state_key,
        strongest_state_label=strongest_state_label,
        strongest_volume_bucket=strongest_volume_bucket,
        strongest_volume_bucket_label=VOLUME_BUCKET_LABEL_MAP[strongest_volume_bucket],
        min_validation_date_count=min_validation_date_count,
        min_pair_overlap_dates=min_pair_overlap_dates,
        universe_constituent_count=int(price_result.topix100_constituent_count),
        covered_constituent_count=int(state_decile_horizon_panel_df["code"].nunique()),
        joined_event_count=int(len(state_decile_horizon_panel_df)),
        valid_date_count=int(state_decile_horizon_panel_df["date"].nunique()),
        state_decile_horizon_panel_df=state_decile_horizon_panel_df,
        state_band_horizon_summary_df=state_band_horizon_summary_df,
        short_candidate_scorecard_df=short_candidate_scorecard_df,
        pair_trade_scorecard_df=pair_trade_scorecard_df,
        validation_focus_matrix_df=validation_focus_matrix_df,
        validation_bull_bull_adjacent_pair_df=validation_bull_bull_adjacent_pair_df,
    )


def write_topix100_short_side_streak_353_scan_research_bundle(
    result: Topix100ShortSideStreak353ScanResearchResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=TOPIX100_SHORT_SIDE_STREAK_353_SCAN_EXPERIMENT_ID,
        module=__name__,
        function="run_topix100_short_side_streak_353_scan_research",
        params={
            "start_date": result.analysis_start_date,
            "end_date": result.analysis_end_date,
            "price_feature": result.price_feature,
            "volume_feature": result.volume_feature,
            "future_horizons": list(result.future_horizons),
            "validation_ratio": result.validation_ratio,
            "short_window_streaks": result.short_window_streaks,
            "long_window_streaks": result.long_window_streaks,
            "strongest_lower_decile": result.strongest_lower_decile,
            "strongest_upper_decile": result.strongest_upper_decile,
            "strongest_state_key": result.strongest_state_key,
            "strongest_volume_bucket": result.strongest_volume_bucket,
            "min_validation_date_count": result.min_validation_date_count,
            "min_pair_overlap_dates": result.min_pair_overlap_dates,
        },
        result=result,
        table_field_names=_RESULT_TABLE_NAMES,
        summary_markdown=_build_research_bundle_summary_markdown(result),
        published_summary=_build_published_summary_payload(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_topix100_short_side_streak_353_scan_research_bundle(
    bundle_path: str | Path,
) -> Topix100ShortSideStreak353ScanResearchResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=Topix100ShortSideStreak353ScanResearchResult,
        table_field_names=_RESULT_TABLE_NAMES,
    )


def get_topix100_short_side_streak_353_scan_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        TOPIX100_SHORT_SIDE_STREAK_353_SCAN_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_topix100_short_side_streak_353_scan_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        TOPIX100_SHORT_SIDE_STREAK_353_SCAN_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


def _build_candidate_band_definitions() -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for lower_decile in range(1, len(DECILE_ORDER) + 1):
        for upper_decile in range(lower_decile, len(DECILE_ORDER) + 1):
            band_label = (
                f"Q{lower_decile}"
                if lower_decile == upper_decile
                else f"Q{lower_decile}-Q{upper_decile}"
            )
            band_decile_count = upper_decile - lower_decile + 1
            records.append(
                {
                    "band_label": band_label,
                    "lower_decile": lower_decile,
                    "upper_decile": upper_decile,
                    "band_decile_count": band_decile_count,
                    "tail_share": band_decile_count / len(DECILE_ORDER),
                }
            )
    return pd.DataFrame.from_records(records)


def _build_state_band_horizon_summary_df(panel_df: pd.DataFrame) -> pd.DataFrame:
    if panel_df.empty:
        return pd.DataFrame()

    group_columns = [
        "sample_split",
        "volume_bucket",
        "volume_bucket_label",
        "state_key",
        "state_label",
        "short_mode",
        "long_mode",
        "horizon_days",
    ]
    frames: list[pd.DataFrame] = []
    for band_row in _build_candidate_band_definitions().itertuples(index=False):
        scoped_df = panel_df[
            panel_df["decile_num"].between(band_row.lower_decile, band_row.upper_decile)
        ].copy()
        if scoped_df.empty:
            continue
        summary_df = (
            scoped_df.groupby(group_columns, observed=True, sort=False)
            .agg(
                sample_count=("code", "size"),
                date_count=("date", "nunique"),
                avg_return=("future_return", "mean"),
                hit_rate=("future_return", lambda values: float((values > 0).mean())),
            )
            .reset_index()
        )
        summary_df.insert(1, "band_label", band_row.band_label)
        summary_df.insert(2, "lower_decile", _as_int(cast(Any, band_row.lower_decile)))
        summary_df.insert(3, "upper_decile", _as_int(cast(Any, band_row.upper_decile)))
        summary_df.insert(
            4,
            "band_decile_count",
            _as_int(cast(Any, band_row.band_decile_count)),
        )
        summary_df.insert(5, "tail_share", _as_float(cast(Any, band_row.tail_share)))
        frames.append(summary_df)

    if not frames:
        return pd.DataFrame()
    return _sort_split_band_frame(pd.concat(frames, ignore_index=True))


def _build_short_candidate_scorecard_df(
    band_summary_df: pd.DataFrame,
    *,
    future_horizons: tuple[int, ...],
    min_validation_date_count: int,
) -> pd.DataFrame:
    if band_summary_df.empty:
        return pd.DataFrame()

    group_columns = [
        "sample_split",
        "band_label",
        "lower_decile",
        "upper_decile",
        "band_decile_count",
        "tail_share",
        "volume_bucket",
        "volume_bucket_label",
        "state_key",
        "state_label",
        "short_mode",
        "long_mode",
    ]
    records: list[dict[str, Any]] = []
    grouped = band_summary_df.groupby(group_columns, observed=True, sort=False)
    for keys, group in grouped:
        if not isinstance(keys, tuple):
            keys = (keys,)
        record: dict[str, Any] = {
            column: value for column, value in zip(group_columns, keys, strict=True)
        }
        for horizon in future_horizons:
            horizon_df = group[group["horizon_days"] == horizon]
            record[f"sample_count_{horizon}d"] = (
                int(horizon_df["sample_count"].iloc[0]) if not horizon_df.empty else None
            )
            record[f"date_count_{horizon}d"] = (
                int(horizon_df["date_count"].iloc[0]) if not horizon_df.empty else None
            )
            record[f"avg_return_{horizon}d"] = (
                float(horizon_df["avg_return"].iloc[0]) if not horizon_df.empty else None
            )
            record[f"hit_rate_{horizon}d"] = (
                float(horizon_df["hit_rate"].iloc[0]) if not horizon_df.empty else None
            )
        record["negative_horizon_count_5_10"] = sum(
            1
            for horizon in (5, 10)
            if (value := _as_float_or_none(record.get(f"avg_return_{horizon}d"))) is not None
            and value < 0.0
        )
        avg_return_5d = _as_float_or_none(record.get("avg_return_5d"))
        avg_return_10d = _as_float_or_none(record.get("avg_return_10d"))
        record["both_negative_5d_10d"] = bool(
            avg_return_5d is not None
            and avg_return_10d is not None
            and avg_return_5d < 0.0
            and avg_return_10d < 0.0
        )
        record["primary_short_score_5_10"] = _mean_nullable(
            [
                -avg_return_5d if avg_return_5d is not None else None,
                -avg_return_10d if avg_return_10d is not None else None,
            ]
        )
        meets_date_threshold = True
        if record["sample_split"] == "validation":
            for horizon in (5, 10):
                date_count = _as_int_or_none(record.get(f"date_count_{horizon}d"))
                if date_count is None or date_count < min_validation_date_count:
                    meets_date_threshold = False
                    break
        record["meets_validation_date_threshold"] = meets_date_threshold
        records.append(record)

    scorecard_df = pd.DataFrame(records)
    if scorecard_df.empty:
        return scorecard_df
    scorecard_df["primary_rank"] = _rank_within_split(
        scorecard_df,
        score_column="primary_short_score_5_10",
        extra_sort_columns=["avg_return_5d", "avg_return_10d", "band_decile_count"],
        ascending=[False, True, True, True],
    )
    return _sort_split_candidate_scorecard(scorecard_df, "primary_short_score_5_10")


def _build_pair_trade_scorecard_df(
    panel_df: pd.DataFrame,
    *,
    future_horizons: tuple[int, ...],
    strongest_lower_decile: int,
    strongest_upper_decile: int,
    strongest_state_key: str,
    strongest_volume_bucket: str,
    min_pair_overlap_dates: int,
) -> pd.DataFrame:
    if panel_df.empty:
        return pd.DataFrame()

    strongest_long_df = panel_df[
        panel_df["decile_num"].between(strongest_lower_decile, strongest_upper_decile)
        & (panel_df["state_key"] == strongest_state_key)
        & (panel_df["volume_bucket"] == strongest_volume_bucket)
    ].copy()
    if strongest_long_df.empty:
        return pd.DataFrame()

    long_leg_df = (
        strongest_long_df.groupby(["sample_split", "date", "horizon_days"], observed=True, sort=False)
        .agg(
            avg_long_return=("future_return", "mean"),
            avg_long_names=("code", "size"),
        )
        .reset_index()
    )

    candidate_frames: list[pd.DataFrame] = []
    group_columns = [
        "sample_split",
        "date",
        "horizon_days",
        "volume_bucket",
        "volume_bucket_label",
        "state_key",
        "state_label",
        "short_mode",
        "long_mode",
    ]
    for band_row in _build_candidate_band_definitions().itertuples(index=False):
        scoped_df = panel_df[
            panel_df["decile_num"].between(band_row.lower_decile, band_row.upper_decile)
        ].copy()
        if scoped_df.empty:
            continue
        daily_df = (
            scoped_df.groupby(group_columns, observed=True, sort=False)
            .agg(
                avg_short_return=("future_return", "mean"),
                avg_short_names=("code", "size"),
            )
            .reset_index()
        )
        daily_df.insert(1, "band_label", band_row.band_label)
        daily_df.insert(2, "lower_decile", _as_int(cast(Any, band_row.lower_decile)))
        daily_df.insert(3, "upper_decile", _as_int(cast(Any, band_row.upper_decile)))
        daily_df.insert(
            4,
            "band_decile_count",
            _as_int(cast(Any, band_row.band_decile_count)),
        )
        daily_df.insert(5, "tail_share", _as_float(cast(Any, band_row.tail_share)))
        candidate_frames.append(daily_df)

    if not candidate_frames:
        return pd.DataFrame()
    short_leg_df = pd.concat(candidate_frames, ignore_index=True)
    pair_trade_daily_df = short_leg_df.merge(
        long_leg_df,
        on=["sample_split", "date", "horizon_days"],
        how="inner",
        validate="many_to_one",
    )
    if pair_trade_daily_df.empty:
        return pd.DataFrame()
    pair_trade_daily_df["avg_long_short_spread"] = (
        pair_trade_daily_df["avg_long_return"] - pair_trade_daily_df["avg_short_return"]
    )
    pair_trade_daily_df["positive_spread"] = (
        pair_trade_daily_df["avg_long_short_spread"] > 0.0
    ).astype(float)

    score_group_columns = [
        "sample_split",
        "band_label",
        "lower_decile",
        "upper_decile",
        "band_decile_count",
        "tail_share",
        "volume_bucket",
        "volume_bucket_label",
        "state_key",
        "state_label",
        "short_mode",
        "long_mode",
    ]
    records: list[dict[str, Any]] = []
    grouped = pair_trade_daily_df.groupby(score_group_columns, observed=True, sort=False)
    for keys, group in grouped:
        if not isinstance(keys, tuple):
            keys = (keys,)
        record: dict[str, Any] = {
            column: value for column, value in zip(score_group_columns, keys, strict=True)
        }
        for horizon in future_horizons:
            horizon_df = group[group["horizon_days"] == horizon]
            record[f"overlap_dates_{horizon}d"] = (
                int(horizon_df["date"].nunique()) if not horizon_df.empty else None
            )
            record[f"avg_long_return_{horizon}d"] = (
                float(horizon_df["avg_long_return"].mean()) if not horizon_df.empty else None
            )
            record[f"avg_short_return_{horizon}d"] = (
                float(horizon_df["avg_short_return"].mean())
                if not horizon_df.empty
                else None
            )
            record[f"spread_{horizon}d"] = (
                float(horizon_df["avg_long_short_spread"].mean())
                if not horizon_df.empty
                else None
            )
            record[f"positive_spread_share_{horizon}d"] = (
                float(horizon_df["positive_spread"].mean()) if not horizon_df.empty else None
            )
            record[f"avg_long_names_{horizon}d"] = (
                float(horizon_df["avg_long_names"].mean()) if not horizon_df.empty else None
            )
            record[f"avg_short_names_{horizon}d"] = (
                float(horizon_df["avg_short_names"].mean()) if not horizon_df.empty else None
            )
        record["both_positive_spread_5_10"] = bool(
            _as_float_or_none(record.get("spread_5d")) is not None
            and _as_float_or_none(record.get("spread_10d")) is not None
            and float(record["spread_5d"]) > 0.0
            and float(record["spread_10d"]) > 0.0
        )
        record["primary_pair_score_5_10"] = _mean_nullable(
            [
                _as_float_or_none(record.get("spread_5d")),
                _as_float_or_none(record.get("spread_10d")),
            ]
        )
        meets_overlap_threshold = True
        if record["sample_split"] == "validation":
            for horizon in (5, 10):
                overlap_dates = _as_int_or_none(record.get(f"overlap_dates_{horizon}d"))
                if overlap_dates is None or overlap_dates < min_pair_overlap_dates:
                    meets_overlap_threshold = False
                    break
        record["meets_pair_overlap_threshold"] = meets_overlap_threshold
        records.append(record)

    scorecard_df = pd.DataFrame(records)
    if scorecard_df.empty:
        return scorecard_df
    scorecard_df["primary_rank"] = _rank_within_split(
        scorecard_df,
        score_column="primary_pair_score_5_10",
        extra_sort_columns=["spread_5d", "spread_10d", "band_decile_count"],
        ascending=[False, False, False, True],
    )
    return _sort_split_candidate_scorecard(scorecard_df, "primary_pair_score_5_10")


def _build_validation_focus_matrix_df(
    *,
    short_candidate_scorecard_df: pd.DataFrame,
    pair_trade_scorecard_df: pd.DataFrame,
) -> pd.DataFrame:
    if short_candidate_scorecard_df.empty:
        return pd.DataFrame()

    focus_records: list[dict[str, Any]] = []
    focus_rows: list[tuple[str, str, pd.Series | None]] = [
        ("best_short_setup", "Best Short Setup", _select_best_short_row(short_candidate_scorecard_df)),
        ("best_pair_setup", "Best Pair Setup", _select_best_pair_row(pair_trade_scorecard_df)),
        (
            "best_bear_bull_short",
            "Best Bear/Bull Short",
            _select_best_short_row(
                short_candidate_scorecard_df,
                state_key="long_bearish__short_bullish",
            ),
        ),
        (
            "user_hypothesis",
            "User Hypothesis: Q2-Q4 Volume Low Bull/Bull",
            _select_user_hypothesis_row(short_candidate_scorecard_df),
        ),
    ]
    seen_keys: set[tuple[str, str, str]] = set()
    for focus_key, focus_label, row in focus_rows:
        if row is None:
            continue
        dedupe_key = (
            str(row["band_label"]),
            str(row["volume_bucket"]),
            str(row["state_key"]),
        )
        if focus_key != "user_hypothesis" and dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)
        pair_row = _match_pair_trade_row(pair_trade_scorecard_df, row)
        focus_records.append(
            {
                "focus_key": focus_key,
                "focus_label": focus_label,
                "band_label": str(row["band_label"]),
                "volume_bucket": str(row["volume_bucket"]),
                "volume_bucket_label": str(row["volume_bucket_label"]),
                "state_key": str(row["state_key"]),
                "state_label": str(row["state_label"]),
                "primary_short_rank": _as_int_or_none(row.get("primary_rank")),
                "primary_pair_rank": (
                    _as_int_or_none(pair_row.get("primary_rank")) if pair_row is not None else None
                ),
                "avg_return_1d": _as_float_or_none(row.get("avg_return_1d")),
                "avg_return_5d": _as_float_or_none(row.get("avg_return_5d")),
                "avg_return_10d": _as_float_or_none(row.get("avg_return_10d")),
                "primary_short_score_5_10": _as_float_or_none(
                    row.get("primary_short_score_5_10")
                ),
                "spread_1d": (
                    _as_float_or_none(pair_row.get("spread_1d")) if pair_row is not None else None
                ),
                "spread_5d": (
                    _as_float_or_none(pair_row.get("spread_5d")) if pair_row is not None else None
                ),
                "spread_10d": (
                    _as_float_or_none(pair_row.get("spread_10d")) if pair_row is not None else None
                ),
                "primary_pair_score_5_10": (
                    _as_float_or_none(pair_row.get("primary_pair_score_5_10"))
                    if pair_row is not None
                    else None
                ),
                "date_count_5d": _as_int_or_none(row.get("date_count_5d")),
                "date_count_10d": _as_int_or_none(row.get("date_count_10d")),
                "overlap_dates_5d": (
                    _as_int_or_none(pair_row.get("overlap_dates_5d")) if pair_row is not None else None
                ),
                "overlap_dates_10d": (
                    _as_int_or_none(pair_row.get("overlap_dates_10d")) if pair_row is not None else None
                ),
            }
        )
    return pd.DataFrame(focus_records)


def _build_validation_bull_bull_adjacent_pair_df(
    short_candidate_scorecard_df: pd.DataFrame,
) -> pd.DataFrame:
    if short_candidate_scorecard_df.empty:
        return pd.DataFrame()

    adjacent_df = short_candidate_scorecard_df[
        (short_candidate_scorecard_df["sample_split"] == "validation")
        & (short_candidate_scorecard_df["state_key"] == "long_bullish__short_bullish")
        & (
            short_candidate_scorecard_df["upper_decile"]
            == short_candidate_scorecard_df["lower_decile"] + 1
        )
    ].copy()
    if adjacent_df.empty:
        return adjacent_df

    adjacent_df = adjacent_df[
        [
            "band_label",
            "lower_decile",
            "upper_decile",
            "volume_bucket",
            "volume_bucket_label",
            "avg_return_1d",
            "avg_return_5d",
            "avg_return_10d",
            "primary_short_score_5_10",
            "date_count_5d",
            "date_count_10d",
            "primary_rank",
        ]
    ].copy()
    adjacent_df["rank_within_volume_bucket_by_composite"] = (
        adjacent_df.groupby("volume_bucket", observed=True)["primary_short_score_5_10"]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    adjacent_df["rank_within_volume_bucket_by_1d"] = (
        adjacent_df.groupby("volume_bucket", observed=True)["avg_return_1d"]
        .rank(method="first", ascending=True)
        .astype(int)
    )
    adjacent_df = adjacent_df.sort_values(
        ["volume_bucket", "rank_within_volume_bucket_by_composite", "band_label"],
        ascending=[True, True, True],
        kind="stable",
    )
    return adjacent_df.reset_index(drop=True)


def _select_best_short_row(
    short_candidate_scorecard_df: pd.DataFrame,
    *,
    state_key: str | None = None,
) -> pd.Series | None:
    if short_candidate_scorecard_df.empty:
        return None
    validation_df = short_candidate_scorecard_df[
        short_candidate_scorecard_df["sample_split"] == "validation"
    ].copy()
    validation_df = validation_df[
        validation_df["meets_validation_date_threshold"].astype(bool)
        & validation_df["both_negative_5d_10d"].astype(bool)
    ].copy()
    if state_key is not None:
        validation_df = validation_df[validation_df["state_key"] == state_key].copy()
    if validation_df.empty:
        return None
    validation_df = validation_df.sort_values(
        ["primary_short_score_5_10", "avg_return_5d", "avg_return_10d", "band_decile_count"],
        ascending=[False, True, True, True],
        kind="stable",
    )
    return validation_df.iloc[0]


def _select_best_pair_row(pair_trade_scorecard_df: pd.DataFrame) -> pd.Series | None:
    if pair_trade_scorecard_df.empty:
        return None
    validation_df = pair_trade_scorecard_df[
        pair_trade_scorecard_df["sample_split"] == "validation"
    ].copy()
    validation_df = validation_df[
        validation_df["meets_pair_overlap_threshold"].astype(bool)
        & validation_df["both_positive_spread_5_10"].astype(bool)
    ].copy()
    if validation_df.empty:
        return None
    validation_df = validation_df.sort_values(
        ["primary_pair_score_5_10", "spread_5d", "spread_10d", "band_decile_count"],
        ascending=[False, False, False, True],
        kind="stable",
    )
    return validation_df.iloc[0]


def _select_user_hypothesis_row(
    short_candidate_scorecard_df: pd.DataFrame,
) -> pd.Series | None:
    if short_candidate_scorecard_df.empty:
        return None
    scoped_df = short_candidate_scorecard_df[
        (short_candidate_scorecard_df["sample_split"] == "validation")
        & (short_candidate_scorecard_df["band_label"] == DEFAULT_USER_HYPOTHESIS_BAND_LABEL)
        & (short_candidate_scorecard_df["volume_bucket"] == "volume_low")
        & (short_candidate_scorecard_df["state_key"] == DEFAULT_USER_HYPOTHESIS_STATE_KEY)
    ].copy()
    if scoped_df.empty:
        return None
    return scoped_df.iloc[0]


def _select_best_bull_bull_adjacent_row(
    adjacent_pair_df: pd.DataFrame,
    *,
    volume_bucket: str,
) -> pd.Series | None:
    if adjacent_pair_df.empty:
        return None
    scoped_df = adjacent_pair_df[adjacent_pair_df["volume_bucket"] == volume_bucket].copy()
    if scoped_df.empty:
        return None
    scoped_df = scoped_df.sort_values(
        ["rank_within_volume_bucket_by_composite", "primary_rank", "band_label"],
        ascending=[True, True, True],
        kind="stable",
    )
    return scoped_df.iloc[0]


def _select_best_bull_bull_adjacent_row_by_1d(
    adjacent_pair_df: pd.DataFrame,
    *,
    volume_bucket: str,
) -> pd.Series | None:
    if adjacent_pair_df.empty:
        return None
    scoped_df = adjacent_pair_df[adjacent_pair_df["volume_bucket"] == volume_bucket].copy()
    if scoped_df.empty:
        return None
    scoped_df = scoped_df.sort_values(
        ["rank_within_volume_bucket_by_1d", "avg_return_1d", "band_label"],
        ascending=[True, True, True],
        kind="stable",
    )
    return scoped_df.iloc[0]


def _match_pair_trade_row(
    pair_trade_scorecard_df: pd.DataFrame,
    short_row: pd.Series,
) -> pd.Series | None:
    if pair_trade_scorecard_df.empty:
        return None
    scoped_df = pair_trade_scorecard_df[
        (pair_trade_scorecard_df["sample_split"] == "validation")
        & (pair_trade_scorecard_df["band_label"] == short_row["band_label"])
        & (pair_trade_scorecard_df["volume_bucket"] == short_row["volume_bucket"])
        & (pair_trade_scorecard_df["state_key"] == short_row["state_key"])
    ].copy()
    if scoped_df.empty:
        return None
    return scoped_df.iloc[0]


def _sort_split_band_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    sorted_df = df.copy()
    sorted_df["_split_order"] = sorted_df["sample_split"].map(
        {name: index for index, name in enumerate(_SPLIT_ORDER, start=1)}
    )
    sorted_df = sorted_df.sort_values(
        ["_split_order", "lower_decile", "upper_decile", "state_key", "volume_bucket", "horizon_days"],
        ascending=[True, True, True, True, True, True],
        kind="stable",
    )
    return sorted_df.drop(columns="_split_order").reset_index(drop=True)


def _sort_split_candidate_scorecard(df: pd.DataFrame, score_column: str) -> pd.DataFrame:
    if df.empty:
        return df
    sorted_df = df.copy()
    sorted_df["_split_order"] = sorted_df["sample_split"].map(
        {name: index for index, name in enumerate(_SPLIT_ORDER, start=1)}
    )
    sorted_df = sorted_df.sort_values(
        ["_split_order", score_column, "band_decile_count", "band_label", "state_key", "volume_bucket"],
        ascending=[True, False, True, True, True, True],
        kind="stable",
    )
    return sorted_df.drop(columns="_split_order").reset_index(drop=True)


def _rank_within_split(
    df: pd.DataFrame,
    *,
    score_column: str,
    extra_sort_columns: list[str],
    ascending: list[bool],
) -> pd.Series:
    ranks = pd.Series(pd.NA, index=df.index, dtype="Int64")
    for split_name in _SPLIT_ORDER:
        split_df = df[df["sample_split"] == split_name].copy()
        if split_df.empty:
            continue
        split_df = split_df.sort_values(
            [score_column, *extra_sort_columns],
            ascending=ascending,
            kind="stable",
        )
        ranks.loc[split_df.index] = pd.Series(
            range(1, len(split_df) + 1),
            index=split_df.index,
            dtype="Int64",
        )
    return ranks


def _build_research_bundle_summary_markdown(
    result: Topix100ShortSideStreak353ScanResearchResult,
) -> str:
    best_short_row = _select_best_short_row(result.short_candidate_scorecard_df)
    best_pair_row = _select_best_pair_row(result.pair_trade_scorecard_df)
    best_bear_bull_short_row = _select_best_short_row(
        result.short_candidate_scorecard_df,
        state_key="long_bearish__short_bullish",
    )
    user_hypothesis_row = _select_user_hypothesis_row(result.short_candidate_scorecard_df)
    best_high_adjacent_row = _select_best_bull_bull_adjacent_row(
        result.validation_bull_bull_adjacent_pair_df,
        volume_bucket="volume_high",
    )
    best_low_adjacent_row = _select_best_bull_bull_adjacent_row(
        result.validation_bull_bull_adjacent_pair_df,
        volume_bucket="volume_low",
    )
    best_high_adjacent_1d_row = _select_best_bull_bull_adjacent_row_by_1d(
        result.validation_bull_bull_adjacent_pair_df,
        volume_bucket="volume_high",
    )
    best_low_adjacent_1d_row = _select_best_bull_bull_adjacent_row_by_1d(
        result.validation_bull_bull_adjacent_pair_df,
        volume_bucket="volume_low",
    )

    lines = [
        "# TOPIX100 Short Side Streak 3/53 Scan",
        "",
        "This study fixes the strongest long leg at `Q10 x Volume Low x Long Bearish / Short Bearish` and scans the entire contiguous decile-band surface for the weakest short-side setup and the best date-aligned pair trade against that long leg.",
        "",
        "## Snapshot",
        "",
        f"- Available range: `{result.available_start_date} -> {result.available_end_date}`",
        f"- Analysis range: `{result.analysis_start_date} -> {result.analysis_end_date}`",
        f"- Source mode: `{result.source_mode}`",
        f"- Source detail: `{result.source_detail}`",
        f"- Price / volume lens: `{result.price_feature} x {result.volume_feature}`",
        f"- Fixed short / long pair: `{result.short_window_streaks} / {result.long_window_streaks}` streak candles",
        "- Strongest long reference: `"
        + f"Q{result.strongest_lower_decile}"
        + (
            ""
            if result.strongest_lower_decile == result.strongest_upper_decile
            else f"-Q{result.strongest_upper_decile}"
        )
        + f" x {result.strongest_volume_bucket_label} x {result.strongest_state_label}`",
        f"- Future horizons: `{_format_int_sequence(result.future_horizons)}`",
        f"- Validation ratio: `{result.validation_ratio:.0%}`",
        f"- Min validation dates for short scan: `{result.min_validation_date_count}`",
        f"- Min overlap dates for pair scan: `{result.min_pair_overlap_dates}`",
        f"- Joined horizon rows: `{result.joined_event_count}`",
        f"- Valid dates: `{result.valid_date_count}`",
        "",
        "## Validation Read",
        "",
    ]

    if best_short_row is not None:
        lines.append(
            "- "
            f"Best standalone short on the `5d/10d` composite was `{best_short_row['band_label']} x {best_short_row['volume_bucket_label']} x {best_short_row['state_label']}` "
            f"with `1d {_format_return(float(best_short_row['avg_return_1d']))}`, "
            f"`5d {_format_return(float(best_short_row['avg_return_5d']))}`, and "
            f"`10d {_format_return(float(best_short_row['avg_return_10d']))}`."
        )
    if best_pair_row is not None:
        lines.append(
            "- "
            f"Best strongest-vs-weakest pair trade was `{best_pair_row['band_label']} x {best_pair_row['volume_bucket_label']} x {best_pair_row['state_label']}` "
            f"with date-aligned spread `1d {_format_return(float(best_pair_row['spread_1d']))}`, "
            f"`5d {_format_return(float(best_pair_row['spread_5d']))}`, and "
            f"`10d {_format_return(float(best_pair_row['spread_10d']))}`."
        )
    if best_bear_bull_short_row is not None:
        lines.append(
            "- "
            f"Inside the `Long Bearish / Short Bullish` family, the best short candidate was `{best_bear_bull_short_row['band_label']} x {best_bear_bull_short_row['volume_bucket_label']}` "
            f"with `5d {_format_return(float(best_bear_bull_short_row['avg_return_5d']))}` and "
            f"`10d {_format_return(float(best_bear_bull_short_row['avg_return_10d']))}`."
        )
    if user_hypothesis_row is not None:
        lines.append(
            "- "
            f"The initial `Q2-Q4 x Volume Low x Long Bullish / Short Bullish` hypothesis did not survive the `5d/10d` screen. "
            f"It ranked `#{int(user_hypothesis_row['primary_rank'])}` with `5d {_format_return(float(user_hypothesis_row['avg_return_5d']))}` "
            f"and `10d {_format_return(float(user_hypothesis_row['avg_return_10d']))}`."
        )
    if best_high_adjacent_row is not None and best_low_adjacent_row is not None:
        lines.append(
            "- "
            f"Within `Long Bullish / Short Bullish` and adjacent two-decile bands only, the best `5d/10d` short was "
            f"`{best_high_adjacent_row['band_label']} x {best_high_adjacent_row['volume_bucket_label']}` "
            f"for `Volume High` and "
            f"`{best_low_adjacent_row['band_label']} x {best_low_adjacent_row['volume_bucket_label']}` "
            f"for `Volume Low`."
        )
    if best_high_adjacent_1d_row is not None and best_low_adjacent_1d_row is not None:
        lines.append(
            "- "
            f"If you compress the horizon to pure `1d`, both volume buckets shift to `{best_high_adjacent_1d_row['band_label']}` / "
            f"`{best_low_adjacent_1d_row['band_label']}` as the most negative pair, but that edge does not hold through `5d/10d`."
        )

    lines.extend(
        [
            "",
            "## Artifact Tables",
            "",
            "- `state_band_horizon_summary_df`: long-format scan across contiguous decile bands, volume buckets, and streak states.",
            "- `short_candidate_scorecard_df`: standalone short ranking with 1/5/10d returns and the `5d/10d` composite score.",
            "- `pair_trade_scorecard_df`: date-aligned strongest-long versus short-candidate spread ranking.",
            "- `validation_focus_matrix_df`: best short, best pair trade, best bear/bull short, and the user hypothesis in one comparison table.",
            "- `validation_bull_bull_adjacent_pair_df`: `Long Bullish / Short Bullish` adjacent two-decile bands (`Q1-Q2` ... `Q9-Q10`) split by volume bucket.",
        ]
    )
    return "\n".join(lines)


def _build_published_summary_payload(
    result: Topix100ShortSideStreak353ScanResearchResult,
) -> dict[str, Any]:
    best_short_row = _select_best_short_row(result.short_candidate_scorecard_df)
    best_pair_row = _select_best_pair_row(result.pair_trade_scorecard_df)
    best_bear_bull_short_row = _select_best_short_row(
        result.short_candidate_scorecard_df,
        state_key="long_bearish__short_bullish",
    )
    user_hypothesis_row = _select_user_hypothesis_row(result.short_candidate_scorecard_df)
    best_high_adjacent_row = _select_best_bull_bull_adjacent_row(
        result.validation_bull_bull_adjacent_pair_df,
        volume_bucket="volume_high",
    )
    best_low_adjacent_row = _select_best_bull_bull_adjacent_row(
        result.validation_bull_bull_adjacent_pair_df,
        volume_bucket="volume_low",
    )
    best_high_adjacent_1d_row = _select_best_bull_bull_adjacent_row_by_1d(
        result.validation_bull_bull_adjacent_pair_df,
        volume_bucket="volume_high",
    )
    best_low_adjacent_1d_row = _select_best_bull_bull_adjacent_row_by_1d(
        result.validation_bull_bull_adjacent_pair_df,
        volume_bucket="volume_low",
    )

    result_bullets: list[str] = []
    if best_short_row is not None:
        result_bullets.append(
            "The best standalone short was "
            f"`{best_short_row['band_label']} x {best_short_row['volume_bucket_label']} x {best_short_row['state_label']}` "
            f"with validation returns `1d {_format_return(float(best_short_row['avg_return_1d']))}`, "
            f"`5d {_format_return(float(best_short_row['avg_return_5d']))}`, "
            f"`10d {_format_return(float(best_short_row['avg_return_10d']))}`."
        )
    if best_pair_row is not None:
        result_bullets.append(
            "The best long-short pairing against the fixed strongest long leg was "
            f"`{best_pair_row['band_label']} x {best_pair_row['volume_bucket_label']} x {best_pair_row['state_label']}`, "
            f"with spread `5d {_format_return(float(best_pair_row['spread_5d']))}` and "
            f"`10d {_format_return(float(best_pair_row['spread_10d']))}`."
        )
    if best_bear_bull_short_row is not None:
        result_bullets.append(
            "The `Long Bearish / Short Bullish` family was not the main short engine. "
            f"Its best member was only `{best_bear_bull_short_row['band_label']} x {best_bear_bull_short_row['volume_bucket_label']}` "
            f"at `5d {_format_return(float(best_bear_bull_short_row['avg_return_5d']))}` and "
            f"`10d {_format_return(float(best_bear_bull_short_row['avg_return_10d']))}`."
        )
    if user_hypothesis_row is not None:
        result_bullets.append(
            "The starting `Q2-Q4 x Volume Low x Long Bullish / Short Bullish` hypothesis did not hold on the chosen horizon. "
            f"It landed at validation short rank `#{int(user_hypothesis_row['primary_rank'])}` with "
            f"`5d {_format_return(float(user_hypothesis_row['avg_return_5d']))}` and "
            f"`10d {_format_return(float(user_hypothesis_row['avg_return_10d']))}`."
        )
    if best_high_adjacent_row is not None and best_low_adjacent_row is not None:
        result_bullets.append(
            "Inside `Long Bullish / Short Bullish` and adjacent two-decile bands only, the best `5d/10d` short was "
            f"`{best_high_adjacent_row['band_label']} x {best_high_adjacent_row['volume_bucket_label']}` on the high-volume side and "
            f"`{best_low_adjacent_row['band_label']} x {best_low_adjacent_row['volume_bucket_label']}` on the low-volume side."
        )
    if best_high_adjacent_1d_row is not None and best_low_adjacent_1d_row is not None:
        result_bullets.append(
            "The `1d` answer is different: both volume halves are most negative at "
            f"`{best_high_adjacent_1d_row['band_label']}` / `{best_low_adjacent_1d_row['band_label']}`, "
            "so the weak zone migrates from `Q1-Q2` intraday-ish to `Q6-Q8` on the hold-to-5d/10d horizon."
        )

    best_short_state = str(best_short_row["state_label"]) if best_short_row is not None else "n/a"
    best_pair_band = str(best_pair_row["band_label"]) if best_pair_row is not None else "n/a"
    best_short_rank = (
        str(int(user_hypothesis_row["primary_rank"])) if user_hypothesis_row is not None else "n/a"
    )

    return {
        "title": "TOPIX100 Short Side Streak 3/53 Scan",
        "purpose": (
            "Keep the strongest long setup fixed at `Q10 x Volume Low x Long Bearish / Short Bearish`, "
            "then scan all contiguous decile bands, volume buckets, and transferred streak states to find both "
            "the weakest standalone short and the best date-aligned pair trade."
        ),
        "methodBullets": [
            "Reuse the same `price_vs_sma_50_gap`, `volume_sma_5_20`, and streak `3 / 53` transfer lens from the earlier TOPIX100 studies.",
            "Search every contiguous decile band `Qx-Qy` across both volume halves and all four transferred streak states.",
            "Rank standalone short candidates by the average of `-5d` and `-10d` forward returns, then rank pair trades by the average of `5d` and `10d` long-short spread against the fixed strongest long setup.",
            "Keep validation support explicit via minimum date thresholds instead of silently promoting thin cells.",
        ],
        "resultHeadline": (
            "The short side does not live in `Q2-Q4` once streak state is added: the best standalone short is "
            "`Q8 x Volume High x Long Bullish / Short Bullish`, while the best strongest-vs-weakest pair is "
            "`Q7 x Volume Low x Long Bullish / Short Bullish`."
        ),
        "resultBullets": result_bullets,
        "considerations": [
            "The key discriminator is still the short streak mode. The weak side is overwhelmingly a `short bullish` story, and most of the top 5/10d short candidates are actually `Long Bullish / Short Bullish`.",
            "Volume behaves differently depending on whether the objective is pure shorting or pair trading. Standalone shorts preferred `Volume High` in the best `Q8` cell, but the best pair trade shifted to `Volume Low` because the strongest long leg and the weak short leg aligned better on the same dates.",
            "This is not a symmetric mirror image of the strongest long setup. The short side clusters around `Q6-Q8` and `Q7-Q8`, not around the original `Q2-Q4` hypothesis.",
            "That mismatch with the earlier `Q2-Q4` intuition is mostly a conditioning effect. Earlier decile-partition work did not condition on the streak state and was looking for broad middle weakness, while this study isolates the `bull/bull` state and finds that the weak pocket migrates upward into `Q6-Q8` once the regime filter is applied.",
            "Validation support is thinner than on the long side. The winning short cells still have meaningful support, but they should be treated as tactical short-horizon setups rather than durable long-only style factors.",
        ],
        "keyMetrics": [
            {
                "label": "Best Short Setup",
                "value": (
                    f"{best_short_row['band_label']} / {best_short_row['volume_bucket_label']}"
                    if best_short_row is not None
                    else "n/a"
                ),
                "detail": best_short_state,
            },
            {
                "label": "Best Pair Band",
                "value": best_pair_band,
                "detail": (
                    str(best_pair_row["state_label"]) if best_pair_row is not None else "n/a"
                ),
            },
            {
                "label": "Hypothesis Rank",
                "value": best_short_rank,
                "detail": "Q2-Q4 Volume Low Bull/Bull",
            },
            {
                "label": "Validation Rows",
                "value": str(result.joined_event_count),
                "detail": f"{result.valid_date_count} dates",
            },
        ],
        "tableHighlights": [
            {
                "name": "short_candidate_scorecard_df",
                "description": "Standalone short ranking across every contiguous decile band, volume split, and streak state.",
            },
            {
                "name": "pair_trade_scorecard_df",
                "description": "Date-aligned strongest-long versus weakest-short spread ranking.",
            },
            {
                "name": "validation_focus_matrix_df",
                "description": "Best short, best pair, best bear/bull short, and the rejected Q2-Q4 hypothesis in one panel.",
            },
            {
                "name": "validation_bull_bull_adjacent_pair_df",
                "description": "Adjacent two-decile `bull/bull` breakdown split by `Volume High` and `Volume Low`.",
            },
        ],
        "tags": [
            "TOPIX100",
            "short-side",
            "pair-trade",
            "streaks",
            "mean-reversion",
        ],
    }


def _mean_nullable(values: list[float | None]) -> float | None:
    numeric = [float(value) for value in values if value is not None]
    if not numeric:
        return None
    return float(sum(numeric) / len(numeric))


def _as_float_or_none(value: object) -> float | None:
    if value is None:
        return None
    if bool(pd.isna(cast(Any, value))):
        return None
    return float(cast(Any, value))


def _as_int_or_none(value: object) -> int | None:
    if value is None:
        return None
    if bool(pd.isna(cast(Any, value))):
        return None
    return int(cast(Any, value))


def _as_float(value: object) -> float:
    coerced = _as_float_or_none(value)
    if coerced is None:
        raise ValueError("Expected numeric float value")
    return coerced


def _as_int(value: object) -> int:
    coerced = _as_int_or_none(value)
    if coerced is None:
        raise ValueError("Expected numeric int value")
    return coerced
