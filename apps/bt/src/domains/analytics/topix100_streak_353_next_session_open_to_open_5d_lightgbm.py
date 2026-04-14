"""
LightGBM runtime snapshot for TOPIX100 streak 3/53 swing ranking.

This keeps the leak-free point-in-time snapshot discipline:

- features are built using information available up to day X
- entry is at X+1 open
- exit is at X+6 open
- target is X+6 open / X+1 open - 1
"""

from __future__ import annotations

from functools import lru_cache
from typing import Any

import pandas as pd

from src.domains.analytics.research_bundle import (
    find_latest_research_bundle_path,
    load_research_bundle_info,
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
    VOLUME_SMA_WINDOW_ORDER,
    _enrich_event_panel,
)
from src.domains.analytics.topix100_streak_353_next_session_intraday_lightgbm import (
    DEFAULT_RUNTIME_TRAIN_LOOKBACK_DAYS,
    _slice_feature_panel_to_recent_dates,
)
from src.domains.analytics.topix100_streak_353_signal_score_lightgbm import (
    DEFAULT_CATEGORICAL_FEATURE_COLUMNS,
    DEFAULT_CONTINUOUS_FEATURE_SUFFIXES,
    DEFAULT_LONG_TARGET_HORIZON_DAYS,
    DEFAULT_SHORT_TARGET_HORIZON_DAYS,
    Topix100Streak353SignalScoreLightgbmSnapshot,
    Topix100Streak353SignalScoreLightgbmSnapshotRow,
    _build_category_lookup,
    _build_price_feature_frame,
    _build_scoring_snapshot_df,
    _coerce_signal_state_panel_df,
    _load_lightgbm_regressor_cls,
    _predict_lightgbm_snapshot_scores,
    get_topix100_streak_353_signal_score_lightgbm_latest_bundle_path,
)
from src.domains.analytics.topix100_streak_353_transfer import (
    DEFAULT_LONG_WINDOW_STREAKS,
    DEFAULT_SHORT_WINDOW_STREAKS,
    build_topix100_streak_daily_state_panel_df,
)
from src.domains.analytics.topix_close_stock_overnight_distribution import (
    _open_analysis_connection,
)
from src.domains.analytics.topix_rank_future_close_core import (
    _query_topix100_stock_history,
)

TOPIX100_STREAK_353_NEXT_SESSION_OPEN_TO_OPEN_5D_LIGHTGBM_WALKFORWARD_EXPERIMENT_ID = (
    "market-behavior/topix100-streak-3-53-next-session-open-to-open-5d-lightgbm-walkforward"
)


def score_topix100_streak_353_next_session_open_to_open_5d_lightgbm_snapshot(
    db_path: str,
    *,
    target_date: str,
    price_feature: str = DEFAULT_PRICE_FEATURE,
    volume_feature: str = DEFAULT_VOLUME_FEATURE,
    short_window_streaks: int = DEFAULT_SHORT_WINDOW_STREAKS,
    long_window_streaks: int = DEFAULT_LONG_WINDOW_STREAKS,
    train_lookback_days: int | None = None,
    connection: Any | None = None,
) -> Topix100Streak353SignalScoreLightgbmSnapshot:
    if connection is not None:
        return _score_topix100_streak_353_next_session_open_to_open_5d_lightgbm_snapshot(
            db_path=db_path,
            target_date=target_date,
            price_feature=price_feature,
            volume_feature=volume_feature,
            short_window_streaks=short_window_streaks,
            long_window_streaks=long_window_streaks,
            train_lookback_days=train_lookback_days,
            connection=connection,
        )
    return _score_topix100_streak_353_next_session_open_to_open_5d_lightgbm_snapshot_cached(
        db_path=db_path,
        target_date=target_date,
        price_feature=price_feature,
        volume_feature=volume_feature,
        short_window_streaks=short_window_streaks,
        long_window_streaks=long_window_streaks,
        train_lookback_days=train_lookback_days,
    )


@lru_cache(maxsize=8)
def _score_topix100_streak_353_next_session_open_to_open_5d_lightgbm_snapshot_cached(
    db_path: str,
    target_date: str,
    price_feature: str,
    volume_feature: str,
    short_window_streaks: int,
    long_window_streaks: int,
    train_lookback_days: int | None,
) -> Topix100Streak353SignalScoreLightgbmSnapshot:
    return _score_topix100_streak_353_next_session_open_to_open_5d_lightgbm_snapshot(
        db_path=db_path,
        target_date=target_date,
        price_feature=price_feature,
        volume_feature=volume_feature,
        short_window_streaks=short_window_streaks,
        long_window_streaks=long_window_streaks,
        train_lookback_days=train_lookback_days,
    )


def _score_topix100_streak_353_next_session_open_to_open_5d_lightgbm_snapshot(
    *,
    db_path: str,
    target_date: str,
    price_feature: str,
    volume_feature: str,
    short_window_streaks: int,
    long_window_streaks: int,
    train_lookback_days: int | None,
    connection: Any | None = None,
) -> Topix100Streak353SignalScoreLightgbmSnapshot:
    if price_feature not in PRICE_FEATURE_LABEL_MAP:
        raise ValueError(f"Unsupported price_feature: {price_feature}")
    if volume_feature not in VOLUME_FEATURE_LABEL_MAP:
        raise ValueError(f"Unsupported volume_feature: {volume_feature}")
    if short_window_streaks >= long_window_streaks:
        raise ValueError("short_window_streaks must be smaller than long_window_streaks")
    if train_lookback_days is not None and train_lookback_days < 1:
        raise ValueError("train_lookback_days must be >= 1 when provided")

    bundle_path = find_latest_research_bundle_path(
        TOPIX100_STREAK_353_NEXT_SESSION_OPEN_TO_OPEN_5D_LIGHTGBM_WALKFORWARD_EXPERIMENT_ID
    )
    if bundle_path is None:
        bundle_path = get_topix100_streak_353_signal_score_lightgbm_latest_bundle_path()
    score_source_run_id = (
        load_research_bundle_info(bundle_path).run_id if bundle_path is not None else None
    )
    train_window_days = train_lookback_days or DEFAULT_RUNTIME_TRAIN_LOOKBACK_DAYS
    price_feature_to_window = {
        feature: window
        for feature, window in zip(PRICE_FEATURE_ORDER, PRICE_SMA_WINDOW_ORDER, strict=True)
    }
    volume_feature_to_window = {
        feature: window
        for feature, window in zip(VOLUME_FEATURE_ORDER, VOLUME_SMA_WINDOW_ORDER, strict=True)
    }

    if connection is None:
        with _open_analysis_connection(db_path) as ctx:
            history_df = _query_topix100_stock_history(
                ctx.connection,
                end_date=target_date,
            )
    else:
        history_df = _query_topix100_stock_history(
            connection,
            end_date=target_date,
        )
    if history_df.empty:
        return _empty_snapshot(
            score_source_run_id=score_source_run_id,
            price_feature=price_feature,
            volume_feature=volume_feature,
            short_window_streaks=short_window_streaks,
            long_window_streaks=long_window_streaks,
        )

    event_panel_df = _enrich_event_panel(
        history_df,
        analysis_start_date=None,
        analysis_end_date=target_date,
        min_constituents_per_day=1,
        price_sma_windows=(price_feature_to_window[price_feature],),
        volume_sma_windows=(volume_feature_to_window[volume_feature],),
    )
    if event_panel_df.empty:
        return _empty_snapshot(
            score_source_run_id=score_source_run_id,
            price_feature=price_feature,
            volume_feature=volume_feature,
            short_window_streaks=short_window_streaks,
            long_window_streaks=long_window_streaks,
        )

    try:
        state_panel_df = build_topix100_streak_daily_state_panel_df(
            history_df,
            analysis_end_date=target_date,
            validation_ratio=None,
            short_window_streaks=short_window_streaks,
            long_window_streaks=long_window_streaks,
        )
        full_feature_panel_df = _build_feature_panel_from_state_event_df(
            event_panel_df=event_panel_df,
            state_event_df=state_panel_df,
            price_feature=price_feature,
            volume_feature=volume_feature,
        )
    except ValueError:
        return _empty_snapshot(
            score_source_run_id=score_source_run_id,
            price_feature=price_feature,
            volume_feature=volume_feature,
            short_window_streaks=short_window_streaks,
            long_window_streaks=long_window_streaks,
        )

    training_source_df = (
        full_feature_panel_df[full_feature_panel_df["date"].astype(str) < target_date]
        .copy()
        .reset_index(drop=True)
    )
    training_feature_panel_df, _train_start, _train_end = _slice_feature_panel_to_recent_dates(
        training_source_df,
        max_date_count=train_window_days,
    )
    if training_feature_panel_df.empty:
        return _empty_snapshot(
            score_source_run_id=score_source_run_id,
            price_feature=price_feature,
            volume_feature=volume_feature,
            short_window_streaks=short_window_streaks,
            long_window_streaks=long_window_streaks,
        )

    snapshot_feature_df = (
        full_feature_panel_df[full_feature_panel_df["date"].astype(str) == target_date]
        .copy()
        .reset_index(drop=True)
    )
    if snapshot_feature_df.empty:
        snapshot_feature_df = _build_scoring_snapshot_df(
            event_panel_df=event_panel_df,
            history_df=history_df,
            target_date=target_date,
            price_feature=price_feature,
            volume_feature=volume_feature,
            short_window_streaks=short_window_streaks,
            long_window_streaks=long_window_streaks,
        )
    if snapshot_feature_df.empty:
        return _empty_snapshot(
            score_source_run_id=score_source_run_id,
            price_feature=price_feature,
            volume_feature=volume_feature,
            short_window_streaks=short_window_streaks,
            long_window_streaks=long_window_streaks,
        )

    regressor_cls = _load_lightgbm_regressor_cls()
    category_source_df = pd.concat(
        [training_feature_panel_df, snapshot_feature_df],
        ignore_index=True,
    )
    categories = _build_category_lookup(category_source_df)
    feature_columns = [
        *DEFAULT_CATEGORICAL_FEATURE_COLUMNS,
        price_feature,
        volume_feature,
        *DEFAULT_CONTINUOUS_FEATURE_SUFFIXES,
    ]
    long_scores = _predict_lightgbm_snapshot_scores(
        training_feature_panel_df=training_feature_panel_df,
        snapshot_df=snapshot_feature_df,
        target_column="next_session_open_to_open_5d_return",
        regressor_cls=regressor_cls,
        feature_columns=feature_columns,
        categorical_feature_columns=DEFAULT_CATEGORICAL_FEATURE_COLUMNS,
        categories=categories,
    )

    rows_by_code: dict[str, Topix100Streak353SignalScoreLightgbmSnapshotRow] = {}
    for row in snapshot_feature_df.to_dict(orient="records"):
        normalized_row = {str(key): value for key, value in row.items()}
        code = str(normalized_row["code"])
        long_score_value = long_scores.get(code)
        rows_by_code[code] = Topix100Streak353SignalScoreLightgbmSnapshotRow(
            code=code,
            company_name=str(normalized_row["company_name"]),
            date=str(normalized_row["date"]),
            long_score_5d=(
                float(long_score_value)
                if long_score_value is not None and pd.notna(long_score_value)
                else None
            ),
            short_score_1d=None,
        )

    return Topix100Streak353SignalScoreLightgbmSnapshot(
        score_source_run_id=score_source_run_id,
        price_feature=price_feature,
        volume_feature=volume_feature,
        short_window_streaks=short_window_streaks,
        long_window_streaks=long_window_streaks,
        long_target_horizon_days=DEFAULT_LONG_TARGET_HORIZON_DAYS,
        short_target_horizon_days=DEFAULT_SHORT_TARGET_HORIZON_DAYS,
        rows_by_code=rows_by_code,
    )


def _empty_snapshot(
    *,
    score_source_run_id: str | None,
    price_feature: str,
    volume_feature: str,
    short_window_streaks: int,
    long_window_streaks: int,
) -> Topix100Streak353SignalScoreLightgbmSnapshot:
    return Topix100Streak353SignalScoreLightgbmSnapshot(
        score_source_run_id=score_source_run_id,
        price_feature=price_feature,
        volume_feature=volume_feature,
        short_window_streaks=short_window_streaks,
        long_window_streaks=long_window_streaks,
        long_target_horizon_days=DEFAULT_LONG_TARGET_HORIZON_DAYS,
        short_target_horizon_days=DEFAULT_SHORT_TARGET_HORIZON_DAYS,
        rows_by_code={},
    )


def _build_feature_panel_from_state_event_df(
    *,
    event_panel_df: pd.DataFrame,
    state_event_df: pd.DataFrame,
    price_feature: str,
    volume_feature: str,
) -> pd.DataFrame:
    if event_panel_df.empty or state_event_df.empty:
        raise ValueError("Base price/state inputs were empty")

    price_df = _build_price_feature_frame(
        event_panel_df,
        price_feature=price_feature,
        volume_feature=volume_feature,
    )
    price_df["date"] = price_df["date"].astype(str)
    price_df = price_df.sort_values(["code", "date"], kind="stable").reset_index(drop=True)
    market_calendar_df = pd.DataFrame(
        {
            "date": sorted(price_df["date"].drop_duplicates().tolist()),
        }
    )
    market_calendar_df["swing_entry_date"] = market_calendar_df["date"].shift(-1)
    market_calendar_df["swing_exit_date"] = market_calendar_df["date"].shift(-6)
    price_df = price_df.merge(
        market_calendar_df,
        on="date",
        how="left",
        validate="many_to_one",
    )
    open_lookup_df = price_df[["code", "date", "open"]].copy()
    open_lookup_df["code"] = open_lookup_df["code"].astype(str).str.zfill(4)
    price_df["code"] = price_df["code"].astype(str).str.zfill(4)
    price_df = price_df.merge(
        open_lookup_df.rename(
            columns={
                "date": "swing_entry_date",
                "open": "swing_entry_open",
            }
        ),
        on=["code", "swing_entry_date"],
        how="left",
        validate="many_to_one",
    ).merge(
        open_lookup_df.rename(
            columns={
                "date": "swing_exit_date",
                "open": "swing_exit_open",
            }
        ),
        on=["code", "swing_exit_date"],
        how="left",
        validate="many_to_one",
    )
    price_df["next_session_open_to_open_5d_return"] = price_df["swing_exit_open"].div(
        price_df["swing_entry_open"].replace(0, pd.NA)
    ).sub(1.0)

    state_df = _coerce_signal_state_panel_df(state_event_df)
    state_columns = [
        "state_event_id",
        "code",
        "company_name",
        "sample_split",
        "segment_id",
        "date",
        "segment_return",
        "segment_day_count",
    ]
    missing_state_columns = [
        column for column in state_columns if column not in state_df.columns
    ]
    if missing_state_columns:
        raise ValueError(f"Missing state event columns: {missing_state_columns}")

    state_df = state_df[state_columns].copy()
    state_df["date"] = state_df["date"].astype(str)
    state_df["code"] = state_df["code"].astype(str).str.zfill(4)

    merged_df = price_df.merge(
        state_df,
        on=["date", "code", "company_name"],
        how="inner",
        validate="one_to_one",
    )
    merged_df = merged_df.dropna(subset=["next_session_open_to_open_5d_return"]).copy()
    if merged_df.empty:
        raise ValueError("No swing targets remained after joining price and state rows")

    merged_df["segment_abs_return"] = merged_df["segment_return"].astype(float).abs()
    ordered_columns = [
        "date",
        "code",
        "company_name",
        "sample_split",
        "state_event_id",
        "segment_id",
        "decile_num",
        "decile",
        price_feature,
        volume_feature,
        "recent_return_1d",
        "recent_return_3d",
        "recent_return_5d",
        "intraday_return",
        "range_pct",
        "segment_return",
        "segment_abs_return",
        "segment_day_count",
        "swing_entry_date",
        "swing_exit_date",
        "next_session_open_to_open_5d_return",
    ]
    return merged_df[ordered_columns].sort_values(
        ["sample_split", "date", "code"],
        kind="stable",
    ).reset_index(drop=True)
