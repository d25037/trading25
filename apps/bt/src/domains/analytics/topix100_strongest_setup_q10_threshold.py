"""
TOPIX100 strongest-setup vs Q10 threshold research.

The strongest setup is fixed from the prior fusion study:

- state: `Long Bearish / Short Bearish`
- volume split: `Volume Low`

This study asks a narrower execution question:

- How much more important is being in the strongest setup than merely being in
  `Q10` without that setup?
- If the strongest setup is not in `Q10`, how wide can the lower-tail decile
  band become before it stops being a usable secondary buy zone?
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
    load_research_bundle_info,
    load_research_bundle_tables,
    write_research_bundle,
)
from src.domains.analytics.topix100_price_vs_sma_q10_bounce_regime_conditioning import (
    DEFAULT_PRICE_FEATURE,
    DEFAULT_VOLUME_FEATURE,
)
from src.domains.analytics.topix100_price_vs_sma_rank_future_close import (
    PRICE_FEATURE_LABEL_MAP,
    PRICE_FEATURE_ORDER,
    PRICE_SMA_WINDOW_ORDER,
    VOLUME_FEATURE_LABEL_MAP,
    VOLUME_FEATURE_ORDER,
    VOLUME_BUCKET_LABEL_MAP,
    VOLUME_SMA_WINDOW_ORDER,
    run_topix100_price_vs_sma_rank_future_close_research,
)
from src.domains.analytics.topix100_streak_353_transfer import (
    DEFAULT_LONG_WINDOW_STREAKS,
    DEFAULT_SHORT_WINDOW_STREAKS,
    run_topix100_streak_353_transfer_research,
)
from src.domains.analytics.topix_close_return_streaks import (
    DEFAULT_VALIDATION_RATIO,
    _normalize_positive_int_sequence,
)
from src.domains.analytics.topix_close_stock_overnight_distribution import SourceMode
from src.domains.analytics.topix_rank_future_close_core import DECILE_ORDER
from src.domains.analytics.topix_streak_extreme_mode import _format_int_sequence, _format_return

DEFAULT_FUTURE_HORIZONS: tuple[int, ...] = (1, 5, 10)
DEFAULT_STRONGEST_STATE_KEY = "long_bearish__short_bearish"
DEFAULT_STRONGEST_STATE_LABEL = "Long Bearish / Short Bearish"
DEFAULT_STRONGEST_VOLUME_BUCKET = "volume_low"
DEFAULT_STRONGEST_VOLUME_BUCKET_LABEL = "Volume Low Half"
DEFAULT_BROAD_TAIL_SHARE_CEILING = 0.4
DEFAULT_LOOSE_TAIL_SHARE_CEILING = 0.5
TOPIX100_STRONGEST_SETUP_Q10_THRESHOLD_EXPERIMENT_ID = (
    "market-behavior/topix100-strongest-setup-q10-threshold"
)
_SPLIT_ORDER: tuple[str, ...] = ("full", "discovery", "validation")


@dataclass(frozen=True)
class Topix100StrongestSetupQ10ThresholdResearchResult:
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
    strongest_state_key: str
    strongest_state_label: str
    strongest_volume_bucket: str
    strongest_volume_bucket_label: str
    universe_constituent_count: int
    covered_constituent_count: int
    joined_event_count: int
    valid_date_count: int
    state_decile_horizon_panel_df: pd.DataFrame
    strongest_setup_decile_summary_df: pd.DataFrame
    strongest_setup_lower_tail_band_summary_df: pd.DataFrame
    q10_non_strong_reference_df: pd.DataFrame
    band_vs_q10_reference_scorecard_df: pd.DataFrame


def run_topix100_strongest_setup_q10_threshold_research(
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
    strongest_state_key: str = DEFAULT_STRONGEST_STATE_KEY,
    strongest_state_label: str = DEFAULT_STRONGEST_STATE_LABEL,
    strongest_volume_bucket: str = DEFAULT_STRONGEST_VOLUME_BUCKET,
) -> Topix100StrongestSetupQ10ThresholdResearchResult:
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
    strongest_setup_decile_summary_df = _build_strongest_setup_decile_summary_df(
        state_decile_horizon_panel_df,
        strongest_state_key=strongest_state_key,
        strongest_volume_bucket=strongest_volume_bucket,
        future_horizons=resolved_horizons,
    )
    strongest_setup_lower_tail_band_summary_df = (
        _build_strongest_setup_lower_tail_band_summary_df(
            state_decile_horizon_panel_df,
            strongest_state_key=strongest_state_key,
            strongest_volume_bucket=strongest_volume_bucket,
            future_horizons=resolved_horizons,
        )
    )
    q10_non_strong_reference_df = _build_q10_non_strong_reference_df(
        state_decile_horizon_panel_df,
        strongest_state_key=strongest_state_key,
        strongest_volume_bucket=strongest_volume_bucket,
        future_horizons=resolved_horizons,
    )
    band_vs_q10_reference_scorecard_df = _build_band_vs_q10_reference_scorecard_df(
        strongest_setup_lower_tail_band_summary_df,
        q10_non_strong_reference_df,
        future_horizons=resolved_horizons,
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

    return Topix100StrongestSetupQ10ThresholdResearchResult(
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
        strongest_state_key=strongest_state_key,
        strongest_state_label=strongest_state_label,
        strongest_volume_bucket=strongest_volume_bucket,
        strongest_volume_bucket_label=VOLUME_BUCKET_LABEL_MAP[strongest_volume_bucket],
        universe_constituent_count=int(price_result.topix100_constituent_count),
        covered_constituent_count=int(state_decile_horizon_panel_df["code"].nunique()),
        joined_event_count=int(len(state_decile_horizon_panel_df)),
        valid_date_count=int(state_decile_horizon_panel_df["date"].nunique()),
        state_decile_horizon_panel_df=state_decile_horizon_panel_df,
        strongest_setup_decile_summary_df=strongest_setup_decile_summary_df,
        strongest_setup_lower_tail_band_summary_df=strongest_setup_lower_tail_band_summary_df,
        q10_non_strong_reference_df=q10_non_strong_reference_df,
        band_vs_q10_reference_scorecard_df=band_vs_q10_reference_scorecard_df,
    )


def write_topix100_strongest_setup_q10_threshold_research_bundle(
    result: Topix100StrongestSetupQ10ThresholdResearchResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=TOPIX100_STRONGEST_SETUP_Q10_THRESHOLD_EXPERIMENT_ID,
        module=__name__,
        function="run_topix100_strongest_setup_q10_threshold_research",
        params={
            "start_date": result.analysis_start_date,
            "end_date": result.analysis_end_date,
            "price_feature": result.price_feature,
            "volume_feature": result.volume_feature,
            "future_horizons": list(result.future_horizons),
            "validation_ratio": result.validation_ratio,
            "short_window_streaks": result.short_window_streaks,
            "long_window_streaks": result.long_window_streaks,
            "strongest_state_key": result.strongest_state_key,
            "strongest_volume_bucket": result.strongest_volume_bucket,
        },
        db_path=result.db_path,
        analysis_start_date=result.analysis_start_date,
        analysis_end_date=result.analysis_end_date,
        result_metadata={
            "db_path": result.db_path,
            "source_mode": result.source_mode,
            "source_detail": result.source_detail,
            "available_start_date": result.available_start_date,
            "available_end_date": result.available_end_date,
            "analysis_start_date": result.analysis_start_date,
            "analysis_end_date": result.analysis_end_date,
            "price_feature": result.price_feature,
            "price_feature_label": result.price_feature_label,
            "volume_feature": result.volume_feature,
            "volume_feature_label": result.volume_feature_label,
            "short_window_streaks": result.short_window_streaks,
            "long_window_streaks": result.long_window_streaks,
            "future_horizons": list(result.future_horizons),
            "validation_ratio": result.validation_ratio,
            "strongest_state_key": result.strongest_state_key,
            "strongest_state_label": result.strongest_state_label,
            "strongest_volume_bucket": result.strongest_volume_bucket,
            "strongest_volume_bucket_label": result.strongest_volume_bucket_label,
            "universe_constituent_count": result.universe_constituent_count,
            "covered_constituent_count": result.covered_constituent_count,
            "joined_event_count": result.joined_event_count,
            "valid_date_count": result.valid_date_count,
        },
        result_tables={
            "state_decile_horizon_panel_df": result.state_decile_horizon_panel_df,
            "strongest_setup_decile_summary_df": result.strongest_setup_decile_summary_df,
            "strongest_setup_lower_tail_band_summary_df": result.strongest_setup_lower_tail_band_summary_df,
            "q10_non_strong_reference_df": result.q10_non_strong_reference_df,
            "band_vs_q10_reference_scorecard_df": result.band_vs_q10_reference_scorecard_df,
        },
        summary_markdown=_build_research_bundle_summary_markdown(result),
        published_summary=_build_published_summary_payload(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_topix100_strongest_setup_q10_threshold_research_bundle(
    bundle_path: str | Path,
) -> Topix100StrongestSetupQ10ThresholdResearchResult:
    info = load_research_bundle_info(bundle_path)
    tables = load_research_bundle_tables(bundle_path)
    metadata = dict(info.result_metadata)
    return Topix100StrongestSetupQ10ThresholdResearchResult(
        db_path=str(metadata["db_path"]),
        source_mode=cast(SourceMode, metadata["source_mode"]),
        source_detail=str(metadata["source_detail"]),
        available_start_date=metadata.get("available_start_date"),
        available_end_date=metadata.get("available_end_date"),
        analysis_start_date=metadata.get("analysis_start_date"),
        analysis_end_date=metadata.get("analysis_end_date"),
        price_feature=str(metadata["price_feature"]),
        price_feature_label=str(metadata["price_feature_label"]),
        volume_feature=str(metadata["volume_feature"]),
        volume_feature_label=str(metadata["volume_feature_label"]),
        short_window_streaks=int(metadata["short_window_streaks"]),
        long_window_streaks=int(metadata["long_window_streaks"]),
        future_horizons=tuple(int(value) for value in metadata["future_horizons"]),
        validation_ratio=float(metadata["validation_ratio"]),
        strongest_state_key=str(metadata["strongest_state_key"]),
        strongest_state_label=str(metadata["strongest_state_label"]),
        strongest_volume_bucket=str(metadata["strongest_volume_bucket"]),
        strongest_volume_bucket_label=str(metadata["strongest_volume_bucket_label"]),
        universe_constituent_count=int(metadata["universe_constituent_count"]),
        covered_constituent_count=int(metadata["covered_constituent_count"]),
        joined_event_count=int(metadata["joined_event_count"]),
        valid_date_count=int(metadata["valid_date_count"]),
        state_decile_horizon_panel_df=tables["state_decile_horizon_panel_df"],
        strongest_setup_decile_summary_df=tables["strongest_setup_decile_summary_df"],
        strongest_setup_lower_tail_band_summary_df=tables[
            "strongest_setup_lower_tail_band_summary_df"
        ],
        q10_non_strong_reference_df=tables["q10_non_strong_reference_df"],
        band_vs_q10_reference_scorecard_df=tables[
            "band_vs_q10_reference_scorecard_df"
        ],
    )


def get_topix100_strongest_setup_q10_threshold_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        TOPIX100_STRONGEST_SETUP_Q10_THRESHOLD_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_topix100_strongest_setup_q10_threshold_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        TOPIX100_STRONGEST_SETUP_Q10_THRESHOLD_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


def _build_state_decile_horizon_panel(
    *,
    event_panel_df: pd.DataFrame,
    state_horizon_event_df: pd.DataFrame,
    price_feature: str,
    volume_feature: str,
    future_horizons: tuple[int, ...],
) -> pd.DataFrame:
    if event_panel_df.empty or state_horizon_event_df.empty:
        raise ValueError("Base event/state inputs were empty")
    missing_event_columns = [
        column
        for column in (price_feature, volume_feature, "date_constituent_count")
        if column not in event_panel_df.columns
    ]
    if missing_event_columns:
        raise ValueError(f"Missing event panel columns: {missing_event_columns}")

    price_df = event_panel_df.copy()
    price_df["date"] = price_df["date"].astype(str)
    price_df["code"] = price_df["code"].astype(str).str.zfill(4)
    return_columns = [f"t_plus_{horizon}_return" for horizon in future_horizons]
    missing_return_columns = [
        column for column in return_columns if column not in price_df.columns
    ]
    if missing_return_columns:
        raise ValueError(f"Missing future return columns: {missing_return_columns}")

    price_df = price_df[
        [
            "date",
            "code",
            "company_name",
            "date_constituent_count",
            price_feature,
            volume_feature,
            *return_columns,
        ]
    ].copy()
    price_df = price_df.dropna(subset=[price_feature, volume_feature]).copy()
    if price_df.empty:
        raise ValueError("No event rows remained after dropping missing feature values")

    price_df["price_rank_desc"] = (
        price_df.groupby("date", observed=True)[price_feature]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    price_df["decile_num"] = (
        ((price_df["price_rank_desc"] - 1) * len(DECILE_ORDER))
        // price_df["date_constituent_count"]
    ) + 1
    price_df["decile_num"] = price_df["decile_num"].clip(1, len(DECILE_ORDER))
    price_df["decile"] = price_df["decile_num"].map(
        {index: f"Q{index}" for index in range(1, len(DECILE_ORDER) + 1)}
    )
    price_df["decile_size"] = price_df.groupby(["date", "decile"], observed=True)[
        "code"
    ].transform("size")
    price_df["volume_rank_desc_within_decile"] = (
        price_df.groupby(["date", "decile"], observed=True)[volume_feature]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    price_df["volume_bucket_index"] = (
        ((price_df["volume_rank_desc_within_decile"] - 1) * 2)
        // price_df["decile_size"]
    ) + 1
    price_df["volume_bucket_index"] = price_df["volume_bucket_index"].clip(1, 2)
    price_df["volume_bucket"] = price_df["volume_bucket_index"].map(
        {1: "volume_high", 2: "volume_low"}
    )
    price_df["volume_bucket_label"] = price_df["volume_bucket"].map(
        VOLUME_BUCKET_LABEL_MAP
    )

    frames: list[pd.DataFrame] = []
    for horizon in future_horizons:
        future_return_column = f"t_plus_{horizon}_return"
        frame = price_df[
            [
                "date",
                "code",
                "company_name",
                "decile_num",
                "decile",
                "volume_bucket",
                "volume_bucket_label",
                future_return_column,
            ]
        ].copy()
        frame = frame.rename(columns={future_return_column: "future_return"})
        frame["horizon_days"] = horizon
        frame = frame.dropna(subset=["future_return"]).copy()
        frames.append(frame)
    event_long_df = pd.concat(frames, ignore_index=True)

    state_df = state_horizon_event_df[
        state_horizon_event_df["horizon_days"].isin(future_horizons)
    ].copy()
    if state_df.empty:
        raise ValueError("No state horizon rows were available for the selected horizons")
    state_df["date"] = state_df["date"].astype(str)
    state_df["code"] = state_df["code"].astype(str).str.zfill(4)
    merged_df = event_long_df.merge(
        state_df[
            [
                "date",
                "code",
                "company_name",
                "sample_split",
                "state_key",
                "state_label",
                "short_mode",
                "long_mode",
                "horizon_days",
            ]
        ],
        on=["date", "code", "company_name", "horizon_days"],
        how="inner",
        validate="one_to_one",
    )
    if merged_df.empty:
        raise ValueError("Joining decile rows with streak-state rows produced no overlap")

    merged_df["price_feature"] = price_feature
    merged_df["price_feature_label"] = PRICE_FEATURE_LABEL_MAP[price_feature]
    merged_df["volume_feature"] = volume_feature
    merged_df["volume_feature_label"] = VOLUME_FEATURE_LABEL_MAP[volume_feature]

    full_df = merged_df.copy()
    full_df["sample_split"] = "full"
    combined_df = pd.concat([full_df, merged_df], ignore_index=True)
    return _sort_split_decile_frame(combined_df)


def _build_wide_horizon_summary(
    df: pd.DataFrame,
    *,
    group_columns: list[str],
    future_horizons: tuple[int, ...],
) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    grouped = df.groupby(group_columns, observed=True, sort=False)
    records: list[dict[str, Any]] = []
    for keys, group in grouped:
        if not isinstance(keys, tuple):
            keys = (keys,)
        record = {column: value for column, value in zip(group_columns, keys, strict=True)}
        base_horizon = future_horizons[0]
        record["setup_count"] = int((group["horizon_days"] == base_horizon).sum())
        record["date_count"] = int(group["date"].nunique())
        for horizon in future_horizons:
            scoped = group[group["horizon_days"] == horizon]
            record[f"avg_return_{horizon}d"] = (
                float(scoped["future_return"].mean()) if not scoped.empty else None
            )
            record[f"hit_rate_{horizon}d"] = (
                float((scoped["future_return"] > 0).mean()) if not scoped.empty else None
            )
        if {5, 10}.issubset(set(future_horizons)):
            values = [record.get("avg_return_5d"), record.get("avg_return_10d")]
            numeric = [float(value) for value in values if value is not None]
            record["primary_score_5_10"] = (
                float(sum(numeric) / len(numeric)) if numeric else None
            )
        records.append(record)
    return pd.DataFrame(records)


def _build_strongest_setup_decile_summary_df(
    panel_df: pd.DataFrame,
    *,
    strongest_state_key: str,
    strongest_volume_bucket: str,
    future_horizons: tuple[int, ...],
) -> pd.DataFrame:
    strongest_df = panel_df[
        (panel_df["state_key"] == strongest_state_key)
        & (panel_df["volume_bucket"] == strongest_volume_bucket)
    ].copy()
    summary_df = _build_wide_horizon_summary(
        strongest_df,
        group_columns=["sample_split", "decile_num", "decile"],
        future_horizons=future_horizons,
    )
    return _sort_split_decile_frame(summary_df)


def _build_strongest_setup_lower_tail_band_summary_df(
    panel_df: pd.DataFrame,
    *,
    strongest_state_key: str,
    strongest_volume_bucket: str,
    future_horizons: tuple[int, ...],
) -> pd.DataFrame:
    strongest_df = panel_df[
        (panel_df["state_key"] == strongest_state_key)
        & (panel_df["volume_bucket"] == strongest_volume_bucket)
    ].copy()
    if strongest_df.empty:
        return pd.DataFrame()

    records: list[dict[str, Any]] = []
    for sample_split in _SPLIT_ORDER:
        split_df = strongest_df[strongest_df["sample_split"] == sample_split].copy()
        if split_df.empty:
            continue
        for lower_decile in range(10, 0, -1):
            scoped_df = split_df[split_df["decile_num"].between(lower_decile, 10)].copy()
            if scoped_df.empty:
                continue
            band_label = "Q10" if lower_decile == 10 else f"Q{lower_decile}-Q10"
            record: dict[str, Any] = {
                "sample_split": sample_split,
                "band_label": band_label,
                "lower_decile": lower_decile,
                "upper_decile": 10,
                "band_decile_count": 11 - lower_decile,
                "tail_share": (11 - lower_decile) / len(DECILE_ORDER),
                "setup_count": int((scoped_df["horizon_days"] == future_horizons[0]).sum()),
                "date_count": int(scoped_df["date"].nunique()),
            }
            for horizon in future_horizons:
                horizon_df = scoped_df[scoped_df["horizon_days"] == horizon]
                record[f"avg_return_{horizon}d"] = (
                    float(horizon_df["future_return"].mean())
                    if not horizon_df.empty
                    else None
                )
                record[f"hit_rate_{horizon}d"] = (
                    float((horizon_df["future_return"] > 0).mean())
                    if not horizon_df.empty
                    else None
                )
            if {5, 10}.issubset(set(future_horizons)):
                values = [record.get("avg_return_5d"), record.get("avg_return_10d")]
                numeric = [float(value) for value in values if value is not None]
                record["primary_score_5_10"] = (
                    float(sum(numeric) / len(numeric)) if numeric else None
                )
            records.append(record)
    return _sort_split_band_frame(pd.DataFrame(records))


def _build_q10_non_strong_reference_df(
    panel_df: pd.DataFrame,
    *,
    strongest_state_key: str,
    strongest_volume_bucket: str,
    future_horizons: tuple[int, ...],
) -> pd.DataFrame:
    q10_non_strong_df = panel_df[
        (panel_df["decile"] == "Q10")
        & ~(
            (panel_df["state_key"] == strongest_state_key)
            & (panel_df["volume_bucket"] == strongest_volume_bucket)
        )
    ].copy()
    summary_df = _build_wide_horizon_summary(
        q10_non_strong_df,
        group_columns=[
            "sample_split",
            "volume_bucket",
            "state_key",
            "state_label",
            "short_mode",
            "long_mode",
        ],
        future_horizons=future_horizons,
    )
    return _sort_split_reference_frame(summary_df)


def _build_band_vs_q10_reference_scorecard_df(
    band_summary_df: pd.DataFrame,
    reference_df: pd.DataFrame,
    *,
    future_horizons: tuple[int, ...],
) -> pd.DataFrame:
    if band_summary_df.empty or reference_df.empty:
        return pd.DataFrame()

    records: list[dict[str, Any]] = []
    for sample_split in _SPLIT_ORDER:
        split_reference_df = reference_df[reference_df["sample_split"] == sample_split].copy()
        split_band_df = band_summary_df[band_summary_df["sample_split"] == sample_split].copy()
        if split_reference_df.empty or split_band_df.empty:
            continue
        reference_row = _select_reference_row(split_reference_df)
        if reference_row is None:
            continue
        q10_row = _select_band_row(split_band_df, band_label="Q10")
        for _, band_row in split_band_df.iterrows():
            record = {
                "sample_split": sample_split,
                "band_label": band_row["band_label"],
                "lower_decile": int(band_row["lower_decile"]),
                "upper_decile": int(band_row["upper_decile"]),
                "band_decile_count": int(band_row["band_decile_count"]),
                "tail_share": float(band_row["tail_share"]),
                "setup_count": int(band_row["setup_count"]),
                "date_count": int(band_row["date_count"]),
                "band_primary_score_5_10": band_row.get("primary_score_5_10"),
                "reference_volume_bucket": str(reference_row["volume_bucket"]),
                "reference_state_key": str(reference_row["state_key"]),
                "reference_state_label": str(reference_row["state_label"]),
                "reference_primary_score_5_10": reference_row.get("primary_score_5_10"),
            }
            for horizon in future_horizons:
                band_value = band_row.get(f"avg_return_{horizon}d")
                reference_value = reference_row.get(f"avg_return_{horizon}d")
                record[f"band_avg_return_{horizon}d"] = band_value
                record[f"reference_avg_return_{horizon}d"] = reference_value
                record[f"edge_vs_reference_{horizon}d"] = _subtract_nullable(
                    band_value,
                    reference_value,
                )
            edge_5d = record.get("edge_vs_reference_5d")
            edge_10d = record.get("edge_vs_reference_10d")
            record["beats_reference_5d"] = bool(edge_5d is not None and edge_5d > 0.0)
            record["beats_reference_10d"] = bool(
                edge_10d is not None and edge_10d > 0.0
            )
            record["beats_reference_5d_10d"] = bool(
                record["beats_reference_5d"] and record["beats_reference_10d"]
            )
            if q10_row is not None:
                record["coverage_multiple_vs_q10"] = _safe_scalar_ratio(
                    _as_float_or_none(band_row.get("setup_count")),
                    _as_float_or_none(q10_row.get("setup_count")),
                )
                record["return_retention_vs_q10_5d"] = _safe_scalar_ratio(
                    _as_float_or_none(band_row.get("avg_return_5d")),
                    _as_float_or_none(q10_row.get("avg_return_5d")),
                )
                record["return_retention_vs_q10_10d"] = _safe_scalar_ratio(
                    _as_float_or_none(band_row.get("avg_return_10d")),
                    _as_float_or_none(q10_row.get("avg_return_10d")),
                )
            else:
                record["coverage_multiple_vs_q10"] = None
                record["return_retention_vs_q10_5d"] = None
                record["return_retention_vs_q10_10d"] = None
            records.append(record)
    return _sort_split_band_frame(pd.DataFrame(records))


def _sort_split_decile_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    sorted_df = df.copy()
    sort_columns: list[str] = []
    if "sample_split" in sorted_df.columns:
        sorted_df["_split_order"] = sorted_df["sample_split"].map(
            {name: index for index, name in enumerate(_SPLIT_ORDER, start=1)}
        )
        sort_columns.append("_split_order")
    if "decile_num" in sorted_df.columns:
        sort_columns.append("decile_num")
    if "primary_score_5_10" in sorted_df.columns:
        sort_columns.append("primary_score_5_10")
    sorted_df = sorted_df.sort_values(
        sort_columns,
        ascending=[True, False, False][: len(sort_columns)],
        kind="stable",
    )
    return sorted_df.drop(
        columns=[column for column in ("_split_order",) if column in sorted_df.columns]
    ).reset_index(drop=True)


def _sort_split_band_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    sorted_df = df.copy()
    sorted_df["_split_order"] = sorted_df["sample_split"].map(
        {name: index for index, name in enumerate(_SPLIT_ORDER, start=1)}
    )
    sort_columns = ["_split_order"]
    ascending = [True]
    if "lower_decile" in sorted_df.columns:
        sort_columns.append("lower_decile")
        ascending.append(False)
    if "primary_score_5_10" in sorted_df.columns:
        sort_columns.append("primary_score_5_10")
        ascending.append(False)
    sorted_df = sorted_df.sort_values(sort_columns, ascending=ascending, kind="stable")
    return sorted_df.drop(columns="_split_order").reset_index(drop=True)


def _sort_split_reference_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    sorted_df = df.copy()
    sorted_df["_split_order"] = sorted_df["sample_split"].map(
        {name: index for index, name in enumerate(_SPLIT_ORDER, start=1)}
    )
    sorted_df = sorted_df.sort_values(
        ["_split_order", "primary_score_5_10", "avg_return_5d", "avg_return_10d"],
        ascending=[True, False, False, False],
        kind="stable",
    )
    return sorted_df.drop(columns="_split_order").reset_index(drop=True)


def _select_reference_row(reference_df: pd.DataFrame) -> pd.Series | None:
    if reference_df.empty:
        return None
    sorted_df = reference_df.sort_values(
        ["primary_score_5_10", "avg_return_5d", "avg_return_10d", "state_key"],
        ascending=[False, False, False, True],
        kind="stable",
    )
    return sorted_df.iloc[0]


def _select_band_row(
    band_df: pd.DataFrame,
    *,
    band_label: str,
) -> pd.Series | None:
    scoped_df = band_df[band_df["band_label"] == band_label].copy()
    if scoped_df.empty:
        return None
    return scoped_df.iloc[0]


def _select_validation_reference_row(
    result: Topix100StrongestSetupQ10ThresholdResearchResult,
) -> pd.Series | None:
    validation_df = result.q10_non_strong_reference_df[
        result.q10_non_strong_reference_df["sample_split"] == "validation"
    ].copy()
    return _select_reference_row(validation_df)


def _select_validation_q10_row(
    result: Topix100StrongestSetupQ10ThresholdResearchResult,
) -> pd.Series | None:
    validation_df = result.strongest_setup_lower_tail_band_summary_df[
        result.strongest_setup_lower_tail_band_summary_df["sample_split"] == "validation"
    ].copy()
    return _select_band_row(validation_df, band_label="Q10")


def _select_strict_secondary_band_row(
    result: Topix100StrongestSetupQ10ThresholdResearchResult,
) -> pd.Series | None:
    validation_df = result.band_vs_q10_reference_scorecard_df[
        result.band_vs_q10_reference_scorecard_df["sample_split"] == "validation"
    ].copy()
    validation_df = validation_df[
        (validation_df["band_label"] != "Q10")
        & validation_df["beats_reference_5d_10d"].astype(bool)
    ].copy()
    if validation_df.empty:
        return None
    validation_df = validation_df.sort_values(
        ["lower_decile", "band_primary_score_5_10"],
        ascending=[False, False],
        kind="stable",
    )
    return validation_df.iloc[0]


def _select_broad_secondary_band_row(
    result: Topix100StrongestSetupQ10ThresholdResearchResult,
) -> pd.Series | None:
    validation_df = result.band_vs_q10_reference_scorecard_df[
        result.band_vs_q10_reference_scorecard_df["sample_split"] == "validation"
    ].copy()
    validation_df = validation_df[
        (validation_df["band_label"] != "Q10")
        & validation_df["beats_reference_5d_10d"].astype(bool)
        & (validation_df["tail_share"] <= DEFAULT_BROAD_TAIL_SHARE_CEILING)
    ].copy()
    if validation_df.empty:
        return None
    validation_df = validation_df.sort_values(
        ["tail_share", "band_primary_score_5_10", "lower_decile"],
        ascending=[False, False, False],
        kind="stable",
    )
    return validation_df.iloc[0]


def _select_loose_secondary_band_row(
    result: Topix100StrongestSetupQ10ThresholdResearchResult,
) -> pd.Series | None:
    validation_df = result.band_vs_q10_reference_scorecard_df[
        result.band_vs_q10_reference_scorecard_df["sample_split"] == "validation"
    ].copy()
    validation_df = validation_df[
        (validation_df["band_label"] != "Q10")
        & validation_df["beats_reference_5d_10d"].astype(bool)
        & (validation_df["tail_share"] <= DEFAULT_LOOSE_TAIL_SHARE_CEILING)
    ].copy()
    if validation_df.empty:
        return None
    validation_df = validation_df.sort_values(
        ["tail_share", "band_primary_score_5_10", "lower_decile"],
        ascending=[False, False, False],
        kind="stable",
    )
    return validation_df.iloc[0]


def _select_best_decile_row(
    result: Topix100StrongestSetupQ10ThresholdResearchResult,
) -> pd.Series | None:
    validation_df = result.strongest_setup_decile_summary_df[
        result.strongest_setup_decile_summary_df["sample_split"] == "validation"
    ].copy()
    if validation_df.empty:
        return None
    validation_df = validation_df.sort_values(
        ["primary_score_5_10", "avg_return_5d", "avg_return_10d", "decile_num"],
        ascending=[False, False, False, False],
        kind="stable",
    )
    return validation_df.iloc[0]


def _build_research_bundle_summary_markdown(
    result: Topix100StrongestSetupQ10ThresholdResearchResult,
) -> str:
    reference_row = _select_validation_reference_row(result)
    q10_row = _select_validation_q10_row(result)
    best_decile_row = _select_best_decile_row(result)
    strict_secondary_row = _select_strict_secondary_band_row(result)
    broad_secondary_row = _select_broad_secondary_band_row(result)
    loose_secondary_row = _select_loose_secondary_band_row(result)
    validation_band_df = result.band_vs_q10_reference_scorecard_df[
        result.band_vs_q10_reference_scorecard_df["sample_split"] == "validation"
    ].copy()
    q8_to_q10_row = _select_band_row(validation_band_df, band_label="Q8-Q10")

    lines = [
        "# TOPIX100 Strongest Setup vs Q10 Threshold",
        "",
        "This study keeps the execution setup fixed at `Volume Low x Long Bearish / Short Bearish` and asks whether that setup matters more than raw `Q10` membership by itself.",
        "",
        "## Snapshot",
        "",
        f"- Available range: `{result.available_start_date} -> {result.available_end_date}`",
        f"- Analysis range: `{result.analysis_start_date} -> {result.analysis_end_date}`",
        f"- Source mode: `{result.source_mode}`",
        f"- Source detail: `{result.source_detail}`",
        f"- Price / volume lens: `{result.price_feature}` x `{result.volume_feature}`",
        f"- Strongest setup: `{result.strongest_volume_bucket_label} x {result.strongest_state_label}`",
        f"- Fixed short / long pair: `{result.short_window_streaks} / {result.long_window_streaks}` streak candles",
        f"- Future horizons: `{_format_int_sequence(result.future_horizons)}`",
        f"- Validation ratio: `{result.validation_ratio:.0%}`",
        f"- Joined horizon rows: `{result.joined_event_count}`",
        f"- Valid dates: `{result.valid_date_count}`",
        "",
        "## Validation Read",
        "",
    ]

    if reference_row is None:
        lines.append("- No validation `Q10` reference rows were available.")
    else:
        lines.append(
            "- "
            f"Best non-strong `Q10` reference: `{reference_row['state_label']} x {VOLUME_BUCKET_LABEL_MAP[str(reference_row['volume_bucket'])]}` "
            f"with `5d {_format_return(float(reference_row['avg_return_5d']))}` and "
            f"`10d {_format_return(float(reference_row['avg_return_10d']))}`."
        )
    if q10_row is not None and reference_row is not None:
        lines.append(
            "- "
            f"Strict best entry stays `Q10` strongest setup: `5d {_format_return(float(q10_row['avg_return_5d']))}`, "
            f"`10d {_format_return(float(q10_row['avg_return_10d']))}`, versus the best non-strong `Q10` reference by "
            f"`{_format_return(float(q10_row['avg_return_5d'] - reference_row['avg_return_5d']))}` at 5d and "
            f"`{_format_return(float(q10_row['avg_return_10d'] - reference_row['avg_return_10d']))}` at 10d."
        )
    if strict_secondary_row is not None:
        lines.append(
            "- "
            f"Strict second-best band: `{strict_secondary_row['band_label']}`. This is the first relaxation away from pure `Q10` that still beats the best non-strong `Q10` reference on both `5d` and `10d`."
        )
    if broad_secondary_row is not None:
        lines.append(
            "- "
            f"Practical broad buy zone: `{broad_secondary_row['band_label']}`. That is still a true lower-tail read "
            f"(`bottom {int(float(broad_secondary_row['tail_share']) * 100)}%`) and keeps positive edges of "
            f"`{_format_return(float(broad_secondary_row['edge_vs_reference_5d']))}` at 5d and "
            f"`{_format_return(float(broad_secondary_row['edge_vs_reference_10d']))}` at 10d."
        )
    if q8_to_q10_row is not None and not bool(q8_to_q10_row["beats_reference_5d_10d"]):
        lines.append(
            "- "
            f"`Q8-Q10` is the unstable threshold: it still clears the reference at `5d`, but slips to "
            f"`{_format_return(float(q8_to_q10_row['edge_vs_reference_10d']))}` versus the reference at `10d`."
        )
    if loose_secondary_row is not None:
        lines.append(
            "- "
            f"Loose half-tail filter: `{loose_secondary_row['band_label']}`. This shows the cliff is losing the setup itself, not leaving `Q10`."
        )
    if best_decile_row is not None:
        lines.append(
            "- "
            f"Best single decile inside the strongest setup was `{best_decile_row['decile']}`, but the decile-by-decile path is not monotonic. Use bands, not single-decile precision, for execution."
        )

    lines.extend(
        [
            "",
            "## Artifact Tables",
            "",
            "- `state_decile_horizon_panel_df`",
            "- `strongest_setup_decile_summary_df`",
            "- `strongest_setup_lower_tail_band_summary_df`",
            "- `q10_non_strong_reference_df`",
            "- `band_vs_q10_reference_scorecard_df`",
        ]
    )
    return "\n".join(lines)


def _build_published_summary_payload(
    result: Topix100StrongestSetupQ10ThresholdResearchResult,
) -> dict[str, Any]:
    reference_row = _select_validation_reference_row(result)
    q10_row = _select_validation_q10_row(result)
    best_decile_row = _select_best_decile_row(result)
    strict_secondary_row = _select_strict_secondary_band_row(result)
    broad_secondary_row = _select_broad_secondary_band_row(result)
    loose_secondary_row = _select_loose_secondary_band_row(result)
    validation_band_df = result.band_vs_q10_reference_scorecard_df[
        result.band_vs_q10_reference_scorecard_df["sample_split"] == "validation"
    ].copy()
    q8_to_q10_row = _select_band_row(validation_band_df, band_label="Q8-Q10")

    highlights: list[dict[str, Any]] = [
        {
            "label": "Strongest setup",
            "value": result.strongest_state_label,
            "tone": "success",
            "detail": result.strongest_volume_bucket_label,
        },
        {
            "label": "Price / volume lens",
            "value": "Price vs SMA50",
            "tone": "neutral",
            "detail": "volume_sma_5_20",
        },
    ]
    result_bullets = [
        "This is a threshold study, not a new factor search. The setup is fixed to `Volume Low x Long Bearish / Short Bearish`, and the only question is how much raw `Q10` membership still matters once that setup is already true.",
    ]

    if reference_row is not None:
        result_bullets.append(
            f"The best non-strong `Q10` reference was `{reference_row['state_label']} x {VOLUME_BUCKET_LABEL_MAP[str(reference_row['volume_bucket'])]}`, with `5d {_format_return(float(reference_row['avg_return_5d']))}` and `10d {_format_return(float(reference_row['avg_return_10d']))}`."
        )
        highlights.append(
            {
                "label": "Best non-strong Q10",
                "value": str(reference_row["state_label"]),
                "tone": "accent",
                "detail": f"5d {_format_return(float(reference_row['avg_return_5d']))}",
            }
        )

    if q10_row is not None and reference_row is not None:
        edge_5d = float(q10_row["avg_return_5d"] - reference_row["avg_return_5d"])
        edge_10d = float(q10_row["avg_return_10d"] - reference_row["avg_return_10d"])
        result_bullets.append(
            f"`Q10` strongest setup is still the cleanest strict entry, but the important point is why: it beats the best non-strong `Q10` reference by `{_format_return(edge_5d)}` at 5d and `{_format_return(edge_10d)}` at 10d."
        )
        highlights.append(
            {
                "label": "Strict best entry",
                "value": "Q10 strongest",
                "tone": "success",
                "detail": f"5d {_format_return(float(q10_row['avg_return_5d']))}",
            }
        )

    if strict_secondary_row is not None:
        result_bullets.append(
            f"If the rule is 'next-best after losing pure Q10', the strict answer is `{strict_secondary_row['band_label']}`. It is the first band away from `Q10` that still outruns the best non-strong `Q10` reference at both `5d` and `10d`."
        )
        highlights.append(
            {
                "label": "Strict second-best",
                "value": str(strict_secondary_row["band_label"]),
                "tone": "neutral",
                "detail": f"5d edge {_format_return(float(strict_secondary_row['edge_vs_reference_5d']))}",
            }
        )

    if broad_secondary_row is not None:
        result_bullets.append(
            f"The practical broad buy zone is `{broad_secondary_row['band_label']}`. That keeps the setup inside the bottom {int(float(broad_secondary_row['tail_share']) * 100)}% of the price decile range while still beating the best non-strong `Q10` reference by `{_format_return(float(broad_secondary_row['edge_vs_reference_5d']))}` at 5d and `{_format_return(float(broad_secondary_row['edge_vs_reference_10d']))}` at 10d."
        )
        highlights.append(
            {
                "label": "Practical broad zone",
                "value": str(broad_secondary_row["band_label"]),
                "tone": "success",
                "detail": f"{float(broad_secondary_row['coverage_multiple_vs_q10']):.1f}x Q10 coverage",
            }
        )
    if q8_to_q10_row is not None and not bool(q8_to_q10_row["beats_reference_5d_10d"]):
        result_bullets.append(
            f"`Q8-Q10` is the deceptive band. It still looks fine at 5d, but it drops to `{_format_return(float(q8_to_q10_row['edge_vs_reference_10d']))}` versus the best non-strong `Q10` reference at 10d, which is why the practical band is `Q7-Q10`, not `Q8-Q10`."
        )

    if loose_secondary_row is not None:
        result_bullets.append(
            f"Even the loose half-tail band `{loose_secondary_row['band_label']}` stays above the best non-strong `Q10` reference. That is the clearest sign that the setup itself matters more than raw `Q10` membership."
        )

    if best_decile_row is not None:
        result_bullets.append(
            f"The single-decile path is not monotonic: the strongest setup has usable pockets outside `Q10`, but the cleaner read is the lower-tail band, not exact decile slicing. The best single decile in validation was `{best_decile_row['decile']}`."
        )

    headline = (
        "Being in `Volume Low x Long Bearish / Short Bearish` matters more than merely being `Q10`: even a broader lower-tail strongest-setup band still beats the best non-strong `Q10` alternative."
    )

    return {
        "title": "TOPIX100 Strongest Setup vs Q10 Threshold",
        "tags": ["TOPIX100", "streaks", "buckets", "mean-reversion", "threshold"],
        "purpose": (
            "Measure whether the strongest execution setup from the prior TOPIX100 fusion study matters more than raw Q10 membership, and identify how wide the lower-tail decile band can be before that edge stops being buyable."
        ),
        "method": [
            "Keep the setup fixed to `Volume Low x Long Bearish / Short Bearish` using the same `price_vs_sma_50_gap`, `volume_sma_5_20`, and streak `3 / 53` definitions from the prior studies.",
            "Rebuild full daily deciles for `price_vs_sma_50_gap`, split each decile into volume high / low halves, then join that panel to each stock's own streak-state labels at the same date and horizon.",
            "Compare the strongest setup across single deciles and cumulative lower-tail bands (`Q10`, `Q9-Q10`, `Q8-Q10`, ...) against the best non-strong `Q10` reference, using `5d/10d` as the primary execution read.",
        ],
        "resultHeadline": headline,
        "resultBullets": result_bullets,
        "considerations": [
            "The main result is structural: setup beats location. The big cliff is losing `Volume Low x Long Bearish / Short Bearish`, not losing pure `Q10`.",
            "Use the cumulative band table as the real decision surface. Single-decile results are informative, but they are not monotonic enough to treat `Q8` vs `Q7` as a precise hard boundary.",
            "A practical execution reading is: `Q10` strongest is best, `Q9-Q10` strongest is the strict next-best fallback, and `Q7-Q10` strongest is the broader but still credible buy zone if you need more names.",
        ],
        "selectedParameters": [
            {"label": "Strongest state", "value": result.strongest_state_label},
            {"label": "Volume split", "value": result.strongest_volume_bucket_label},
            {"label": "Short / Long", "value": f"{result.short_window_streaks} / {result.long_window_streaks} streaks"},
            {"label": "Future horizons", "value": _format_int_sequence(result.future_horizons)},
            {"label": "Validation split", "value": f"{result.validation_ratio:.0%}"},
        ],
        "highlights": highlights,
        "tableHighlights": [
            {
                "name": "strongest_setup_lower_tail_band_summary_df",
                "label": "Lower-tail band summary",
                "description": "Strongest setup performance by cumulative lower-tail bands from Q10 outward.",
            },
            {
                "name": "q10_non_strong_reference_df",
                "label": "Q10 non-strong reference",
                "description": "The best non-strong Q10 alternatives that the strongest setup must beat.",
            },
            {
                "name": "band_vs_q10_reference_scorecard_df",
                "label": "Band vs reference scorecard",
                "description": "Direct edge and coverage comparison versus the best non-strong Q10 reference.",
            },
            {
                "name": "strongest_setup_decile_summary_df",
                "label": "Single-decile summary",
                "description": "How the strongest setup behaves inside each exact price decile.",
            },
        ],
    }


def _subtract_nullable(left: Any, right: Any) -> float | None:
    left_value = _as_float_or_none(left)
    right_value = _as_float_or_none(right)
    if left_value is None or right_value is None:
        return None
    return float(left_value - right_value)


def _as_float_or_none(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    return float(value)


def _safe_scalar_ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator == 0.0:
        return None
    return float(numerator / denominator)
