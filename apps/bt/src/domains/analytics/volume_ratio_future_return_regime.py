"""
Multi-universe volume-ratio / future-return research.

This study evaluates whether strong volume-ratio expansion contributes to
subsequent returns, and whether the relationship differs across:

- TOPIX500
- PRIME ex TOPIX500
- Standard
- Growth

The research keeps the main inference on the full cross-sectional panel, then
adds deterministic samples for quick manual inspection.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any, Literal, cast

import numpy as np
import pandas as pd
from scipy import stats

from src.domains.analytics.deterministic_sampling import select_deterministic_samples
from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    find_latest_research_bundle_path,
    get_research_bundle_dir,
    load_research_bundle_info,
    load_research_bundle_tables,
    write_research_bundle,
)
from src.domains.analytics.topix_close_stock_overnight_distribution import (
    SourceMode,
    _normalize_code_sql,
    _open_analysis_connection,
)
from src.domains.analytics.topix_rank_future_close_core import _default_start_date

UniverseKey = Literal["topix500", "prime_ex_topix500", "standard", "growth"]
SplitKey = Literal["full", "discovery", "validation"]
EntryModeKey = Literal["close_to_close", "next_open_to_close"]
ConditionFamilyKey = Literal[
    "trend_state",
    "momentum_state",
    "liquidity_state",
    "volatility_state",
]
DecileKey = Literal[
    "Q1",
    "Q2",
    "Q3",
    "Q4",
    "Q5",
    "Q6",
    "Q7",
    "Q8",
    "Q9",
    "Q10",
]

VOLUME_RATIO_FUTURE_RETURN_REGIME_EXPERIMENT_ID = (
    "market-behavior/volume-ratio-future-return-regime"
)
UNIVERSE_ORDER: tuple[UniverseKey, ...] = (
    "topix500",
    "prime_ex_topix500",
    "standard",
    "growth",
)
UNIVERSE_LABEL_MAP: dict[UniverseKey, str] = {
    "topix500": "TOPIX500",
    "prime_ex_topix500": "PRIME ex TOPIX500",
    "standard": "Standard",
    "growth": "Growth",
}
ENTRY_MODE_ORDER: tuple[EntryModeKey, ...] = (
    "close_to_close",
    "next_open_to_close",
)
ENTRY_MODE_LABEL_MAP: dict[EntryModeKey, str] = {
    "close_to_close": "Close(t) -> Close(t+h)",
    "next_open_to_close": "Open(t+1) -> Close(t+h)",
}
SPLIT_ORDER: tuple[SplitKey, ...] = ("full", "discovery", "validation")
DECILE_ORDER: tuple[DecileKey, ...] = (
    "Q1",
    "Q2",
    "Q3",
    "Q4",
    "Q5",
    "Q6",
    "Q7",
    "Q8",
    "Q9",
    "Q10",
)
CONDITION_FAMILY_ORDER: tuple[ConditionFamilyKey, ...] = (
    "trend_state",
    "momentum_state",
    "liquidity_state",
    "volatility_state",
)
CONDITION_LABEL_MAP: dict[ConditionFamilyKey, str] = {
    "trend_state": "Trend state",
    "momentum_state": "20d momentum state",
    "liquidity_state": "ADV20 state",
    "volatility_state": "20d volatility state",
}
CONDITION_VALUE_ORDER: dict[ConditionFamilyKey, tuple[str, ...]] = {
    "trend_state": ("above_sma150", "below_or_at_sma150"),
    "momentum_state": ("positive_20d", "non_positive_20d"),
    "liquidity_state": ("high_adv20", "low_adv20"),
    "volatility_state": ("high_vol20", "low_vol20"),
}
CONDITION_VALUE_LABEL_MAP: dict[str, str] = {
    "above_sma150": "Close > SMA150",
    "below_or_at_sma150": "Close <= SMA150",
    "positive_20d": "Past 20d return > 0",
    "non_positive_20d": "Past 20d return <= 0",
    "high_adv20": "ADV20 >= median",
    "low_adv20": "ADV20 < median",
    "high_vol20": "Vol20 >= median",
    "low_vol20": "Vol20 < median",
}
DEFAULT_SHORT_WINDOWS: tuple[int, ...] = (5, 10, 20, 30, 50, 70)
DEFAULT_LONG_WINDOWS: tuple[int, ...] = (20, 50, 100, 150, 200, 250)
DEFAULT_THRESHOLDS: tuple[float, ...] = tuple(
    round(value, 1) for value in np.arange(1.0, 2.01, 0.1)
)
DEFAULT_HORIZONS: tuple[int, ...] = (5, 10, 20)
DEFAULT_LOOKBACK_YEARS = 10
DEFAULT_VALIDATION_RATIO = 0.3
DEFAULT_SAMPLE_SEED = 42
DEFAULT_SAMPLE_SIZE_PER_UNIVERSE = 100
DEFAULT_SAMPLE_EVENT_SIZE_PER_UNIVERSE = 40
DEFAULT_MIN_SIGNAL_EVENTS = 200
DEFAULT_MIN_UNIQUE_CODES = 30
DEFAULT_TOP_K = 3
REFERENCE_SHORT_PERIOD = 50
REFERENCE_LONG_PERIOD = 150
REFERENCE_THRESHOLD = 1.7
TREND_SMA_WINDOW = 150
MOMENTUM_WINDOW = 20
LIQUIDITY_WINDOW = 20
VOLATILITY_WINDOW = 20
TOPIX500_SCALE_CATEGORIES: tuple[str, ...] = (
    "TOPIX Core30",
    "TOPIX Large70",
    "TOPIX Mid400",
)
PRIME_MARKET_CODES: tuple[str, ...] = ("0111", "prime")
STANDARD_MARKET_CODES: tuple[str, ...] = ("0112", "standard")
GROWTH_MARKET_CODES: tuple[str, ...] = ("0113", "growth")
MEMBERSHIP_MODE = "latest_market_code_scale_category_proxy"

TABLE_FIELD_NAMES: tuple[str, ...] = (
    "parameter_grid_df",
    "universe_summary_df",
    "sampled_codes_df",
    "sampled_reference_events_df",
    "decile_summary_df",
    "decile_spread_summary_df",
    "threshold_grid_summary_df",
    "best_thresholds_df",
    "reference_condition_summary_df",
)
SAMPLED_REFERENCE_EVENT_COLUMNS: tuple[str, ...] = (
    "universe_key",
    "universe_label",
    "date",
    "code",
    "company_name",
    "reference_volume_ratio",
    "trend_state",
    "momentum_state",
    "liquidity_state",
    "volatility_state",
    "close_to_close_5d",
    "next_open_to_close_5d",
    "close_to_close_10d",
    "next_open_to_close_10d",
    "close_to_close_20d",
    "next_open_to_close_20d",
    "sample_rank",
)


@dataclass(frozen=True)
class VolumeRatioFutureReturnRegimeResearchResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    default_start_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    lookback_years: int
    validation_ratio: float
    analysis_use_sampled_codes: bool
    sample_seed: int
    sample_size_per_universe: int
    sample_event_size_per_universe: int
    short_windows: tuple[int, ...]
    long_windows: tuple[int, ...]
    threshold_values: tuple[float, ...]
    horizons: tuple[int, ...]
    min_signal_events: int
    min_unique_codes: int
    top_k: int
    reference_short_period: int
    reference_long_period: int
    reference_threshold: float
    membership_mode: str
    parameter_grid_df: pd.DataFrame
    universe_summary_df: pd.DataFrame
    sampled_codes_df: pd.DataFrame
    sampled_reference_events_df: pd.DataFrame
    decile_summary_df: pd.DataFrame
    decile_spread_summary_df: pd.DataFrame
    threshold_grid_summary_df: pd.DataFrame
    best_thresholds_df: pd.DataFrame
    reference_condition_summary_df: pd.DataFrame


def _normalize_int_sequence(
    values: tuple[int, ...] | list[int] | None,
    *,
    fallback: tuple[int, ...],
    name: str,
) -> tuple[int, ...]:
    if values is None:
        return fallback
    normalized = tuple(
        sorted(dict.fromkeys(int(value) for value in values if int(value) > 0))
    )
    if not normalized:
        raise ValueError(f"{name} must contain at least one positive integer")
    return normalized


def _normalize_float_sequence(
    values: tuple[float, ...] | list[float] | None,
    *,
    fallback: tuple[float, ...],
    name: str,
) -> tuple[float, ...]:
    if values is None:
        return fallback
    normalized = tuple(
        sorted(dict.fromkeys(round(float(value), 6) for value in values if float(value) > 0))
    )
    if not normalized:
        raise ValueError(f"{name} must contain at least one positive float")
    return normalized


def _coerce_int(value: Any) -> int:
    return int(np.asarray(value).item())


def _coerce_float(value: Any) -> float:
    return float(np.asarray(value).item())


def _coerce_optional_float(value: Any) -> float | None:
    array = np.asarray(value)
    if array.size != 1:
        return None
    scalar = array.item()
    if pd.isna(scalar):
        return None
    return float(scalar)


def _build_parameter_grid(
    *,
    short_windows: tuple[int, ...],
    long_windows: tuple[int, ...],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for short_period in short_windows:
        for long_period in long_windows:
            if short_period >= long_period:
                continue
            rows.append(
                {
                    "short_period": short_period,
                    "long_period": long_period,
                    "parameter_key": f"short{short_period}_long{long_period}",
                }
            )
    if not rows:
        raise ValueError("short_windows and long_windows must produce at least one short < long pair")
    return pd.DataFrame(rows)


def _sort_table(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    sorted_df = df.copy()
    if "universe_key" in sorted_df.columns:
        sorted_df["_universe_order"] = sorted_df["universe_key"].map(
            {key: index for index, key in enumerate(UNIVERSE_ORDER, start=1)}
        )
    if "split_key" in sorted_df.columns:
        sorted_df["_split_order"] = sorted_df["split_key"].map(
            {key: index for index, key in enumerate(SPLIT_ORDER, start=1)}
        )
    if "entry_mode" in sorted_df.columns:
        sorted_df["_entry_mode_order"] = sorted_df["entry_mode"].map(
            {key: index for index, key in enumerate(ENTRY_MODE_ORDER, start=1)}
        )
    if "feature_decile" in sorted_df.columns:
        sorted_df["_decile_order"] = sorted_df["feature_decile"].map(
            {key: index for index, key in enumerate(DECILE_ORDER, start=1)}
        )
    if "condition_family" in sorted_df.columns:
        sorted_df["_condition_family_order"] = sorted_df["condition_family"].map(
            {key: index for index, key in enumerate(CONDITION_FAMILY_ORDER, start=1)}
        )
    if "condition_value" in sorted_df.columns and "condition_family" in sorted_df.columns:
        sorted_df["_condition_value_order"] = sorted_df.apply(
            lambda row: (
                CONDITION_VALUE_ORDER.get(cast(ConditionFamilyKey, row["condition_family"]), ())
                .index(str(row["condition_value"]))
                + 1
                if str(row["condition_value"])
                in CONDITION_VALUE_ORDER.get(
                    cast(ConditionFamilyKey, row["condition_family"]),
                    (),
                )
                else 999
            ),
            axis=1,
        )

    sort_columns = [
        column
        for column in [
            "_universe_order",
            "_split_order",
            "_entry_mode_order",
            "horizon_days",
            "short_period",
            "long_period",
            "threshold",
            "_condition_family_order",
            "_condition_value_order",
            "_decile_order",
            "sample_rank",
            "date",
            "code",
        ]
        if column in sorted_df.columns
    ]
    if sort_columns:
        sorted_df = sorted_df.sort_values(sort_columns).reset_index(drop=True)

    return sorted_df.drop(
        columns=[
            column
            for column in [
                "_universe_order",
                "_split_order",
                "_entry_mode_order",
                "_condition_family_order",
                "_condition_value_order",
                "_decile_order",
            ]
            if column in sorted_df.columns
        ]
    )


def _universe_case_sql() -> str:
    return """
        CASE
            WHEN coalesce(scale_category, '') IN (?, ?, ?) THEN 'topix500'
            WHEN lower(coalesce(market_code, '')) IN (?, ?) THEN 'prime_ex_topix500'
            WHEN lower(coalesce(market_code, '')) IN (?, ?) THEN 'standard'
            WHEN lower(coalesce(market_code, '')) IN (?, ?) THEN 'growth'
            ELSE NULL
        END
    """


def _universe_case_params() -> list[str]:
    return [
        *TOPIX500_SCALE_CATEGORIES,
        *PRIME_MARKET_CODES,
        *STANDARD_MARKET_CODES,
        *GROWTH_MARKET_CODES,
    ]


def _query_universe_codes(
    conn: Any,
    *,
    universe_key: UniverseKey,
) -> pd.DataFrame:
    normalized_code_sql = _normalize_code_sql("code")
    sql = f"""
        WITH latest_universe_raw AS (
            SELECT
                {normalized_code_sql} AS normalized_code,
                coalesce(company_name, code) AS company_name,
                lower(coalesce(market_code, '')) AS market_code,
                coalesce(scale_category, '') AS scale_category,
                ROW_NUMBER() OVER (
                    PARTITION BY {normalized_code_sql}
                    ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END, code
                ) AS row_priority
            FROM stocks
        ),
        classified_universe AS (
            SELECT
                normalized_code AS code,
                company_name,
                {_universe_case_sql()} AS universe_key
            FROM latest_universe_raw
            WHERE row_priority = 1
        )
        SELECT
            code,
            company_name,
            universe_key
        FROM classified_universe
        WHERE universe_key = ?
        ORDER BY code
    """
    return cast(
        pd.DataFrame,
        conn.execute(sql, [*_universe_case_params(), universe_key]).fetchdf(),
    )


def _query_universe_stock_history(
    conn: Any,
    *,
    universe_key: UniverseKey,
    end_date: str | None,
) -> pd.DataFrame:
    normalized_code_sql = _normalize_code_sql("code")
    params: list[Any] = [*_universe_case_params(), universe_key]
    date_filter_sql = ""
    if end_date is not None:
        date_filter_sql = " AND date <= ?"
        params.append(end_date)

    sql = f"""
        WITH latest_universe_raw AS (
            SELECT
                {normalized_code_sql} AS normalized_code,
                coalesce(company_name, code) AS company_name,
                lower(coalesce(market_code, '')) AS market_code,
                coalesce(scale_category, '') AS scale_category,
                ROW_NUMBER() OVER (
                    PARTITION BY {normalized_code_sql}
                    ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END, code
                ) AS row_priority
            FROM stocks
        ),
        classified_universe AS (
            SELECT
                normalized_code,
                company_name,
                {_universe_case_sql()} AS universe_key
            FROM latest_universe_raw
            WHERE row_priority = 1
        ),
        stock_rows_raw AS (
            SELECT
                date,
                {normalized_code_sql} AS normalized_code,
                CAST(open AS DOUBLE) AS open,
                CAST(close AS DOUBLE) AS close,
                CAST(volume AS DOUBLE) AS volume,
                ROW_NUMBER() OVER (
                    PARTITION BY {normalized_code_sql}, date
                    ORDER BY CASE WHEN length(code) = 4 THEN 0 ELSE 1 END, code
                ) AS row_priority
            FROM stock_data
            WHERE close IS NOT NULL
              AND close > 0
              AND open IS NOT NULL
              AND open > 0
              AND volume IS NOT NULL
              AND volume > 0
              {date_filter_sql}
        ),
        stock_rows AS (
            SELECT
                date,
                normalized_code,
                open,
                close,
                volume
            FROM stock_rows_raw
            WHERE row_priority = 1
        )
        SELECT
            u.universe_key,
            u.normalized_code AS code,
            u.company_name,
            s.date,
            s.open,
            s.close,
            s.volume
        FROM stock_rows s
        JOIN classified_universe u
          ON u.normalized_code = s.normalized_code
        WHERE u.universe_key = ?
        ORDER BY s.date, u.normalized_code
    """
    return cast(pd.DataFrame, conn.execute(sql, params).fetchdf())


def _safe_optional_date(value: pd.Timestamp | str | None) -> str | None:
    if value is None:
        return None
    if pd.isna(value):
        return None
    return pd.Timestamp(value).strftime("%Y-%m-%d")


def _iter_split_frames(
    df: pd.DataFrame,
    *,
    validation_start_ts: pd.Timestamp | None,
) -> list[tuple[SplitKey, pd.DataFrame]]:
    frames: list[tuple[SplitKey, pd.DataFrame]] = [("full", df)]
    if validation_start_ts is None:
        return frames

    discovery_df = df.loc[df["date"] < validation_start_ts].copy()
    validation_df = df.loc[df["date"] >= validation_start_ts].copy()
    if not discovery_df.empty:
        frames.append(("discovery", discovery_df))
    if not validation_df.empty:
        frames.append(("validation", validation_df))
    return frames


def _resolve_validation_split_dates(
    panel_df: pd.DataFrame,
    *,
    validation_ratio: float,
) -> tuple[str | None, str | None, pd.Timestamp | None]:
    unique_dates = (
        panel_df["date"].dropna().sort_values().drop_duplicates().reset_index(drop=True)
    )
    if unique_dates.empty:
        return None, None, None
    if validation_ratio <= 0:
        return _safe_optional_date(unique_dates.iloc[-1]), None, None

    date_count = len(unique_dates)
    validation_count = int(np.ceil(date_count * validation_ratio))
    validation_count = max(1, validation_count)
    if date_count > 1:
        validation_count = min(validation_count, date_count - 1)
    validation_start_index = max(0, date_count - validation_count)
    validation_start_ts = pd.Timestamp(unique_dates.iloc[validation_start_index])
    discovery_end_ts = (
        pd.Timestamp(unique_dates.iloc[validation_start_index - 1])
        if validation_start_index > 0
        else None
    )
    return (
        _safe_optional_date(discovery_end_ts),
        _safe_optional_date(validation_start_ts),
        validation_start_ts,
    )


def _safe_welch_t_test(
    left: pd.Series,
    right: pd.Series,
) -> tuple[float | None, float | None]:
    if len(left) < 2 or len(right) < 2:
        return None, None
    statistic, p_value = stats.ttest_ind(
        left.to_numpy(dtype=float),
        right.to_numpy(dtype=float),
        equal_var=False,
        nan_policy="omit",
    )
    statistic_value = _coerce_optional_float(statistic)
    p_value_value = _coerce_optional_float(p_value)
    if statistic_value is None or p_value_value is None:
        return None, None
    return statistic_value, p_value_value


def _safe_one_sample_t_test(
    values: pd.Series,
) -> tuple[float | None, float | None]:
    if len(values) < 2:
        return None, None
    statistic, p_value = stats.ttest_1samp(values.to_numpy(dtype=float), 0.0)
    statistic_value = _coerce_optional_float(statistic)
    p_value_value = _coerce_optional_float(p_value)
    if statistic_value is None or p_value_value is None:
        return None, None
    return statistic_value, p_value_value


def _prepare_universe_panel(
    raw_df: pd.DataFrame,
    *,
    analysis_start_date: str | None,
    analysis_end_date: str | None,
    unique_volume_windows: tuple[int, ...],
    horizons: tuple[int, ...],
) -> pd.DataFrame:
    if raw_df.empty:
        return raw_df.copy()

    panel_df = raw_df.copy()
    panel_df["date"] = pd.to_datetime(panel_df["date"])
    panel_df["open"] = pd.to_numeric(panel_df["open"], errors="coerce")
    panel_df["close"] = pd.to_numeric(panel_df["close"], errors="coerce")
    panel_df["volume"] = pd.to_numeric(panel_df["volume"], errors="coerce")
    panel_df = panel_df.dropna(subset=["open", "close", "volume"]).copy()
    panel_df = panel_df.sort_values(["code", "date"]).reset_index(drop=True)
    panel_df["trading_value"] = panel_df["close"] * panel_df["volume"]

    grouped = panel_df.groupby("code", sort=False)
    for window in unique_volume_windows:
        panel_df[f"volume_ma_{window}"] = (
            grouped["volume"]
            .rolling(window=window, min_periods=window)
            .mean()
            .reset_index(level=0, drop=True)
        )

    panel_df["close_sma_150"] = (
        grouped["close"]
        .rolling(window=TREND_SMA_WINDOW, min_periods=TREND_SMA_WINDOW)
        .mean()
        .reset_index(level=0, drop=True)
    )
    panel_df["past_return_20"] = grouped["close"].pct_change(MOMENTUM_WINDOW)
    panel_df["daily_return"] = grouped["close"].pct_change()
    panel_df["adv_20"] = (
        grouped["trading_value"]
        .rolling(window=LIQUIDITY_WINDOW, min_periods=LIQUIDITY_WINDOW)
        .mean()
        .reset_index(level=0, drop=True)
    )
    panel_df["volatility_20"] = (
        panel_df.groupby("code", sort=False)["daily_return"]
        .rolling(window=VOLATILITY_WINDOW, min_periods=VOLATILITY_WINDOW)
        .std()
        .reset_index(level=0, drop=True)
    )

    for horizon_days in horizons:
        future_close = grouped["close"].shift(-horizon_days)
        next_open = grouped["open"].shift(-1)
        panel_df[f"close_to_close_{horizon_days}d"] = future_close / panel_df["close"] - 1.0
        panel_df[f"next_open_to_close_{horizon_days}d"] = future_close / next_open - 1.0

    for column_name in panel_df.columns:
        if column_name.endswith("d") and column_name.startswith(
            ("close_to_close_", "next_open_to_close_")
        ):
            panel_df[column_name] = panel_df[column_name].where(
                np.isfinite(panel_df[column_name])
            )

    if analysis_start_date is not None:
        panel_df = panel_df.loc[
            panel_df["date"] >= pd.Timestamp(analysis_start_date)
        ].copy()
    if analysis_end_date is not None:
        panel_df = panel_df.loc[
            panel_df["date"] <= pd.Timestamp(analysis_end_date)
        ].copy()

    if panel_df.empty:
        return panel_df

    liquidity_median = panel_df.groupby("date")["adv_20"].transform("median")
    volatility_median = panel_df.groupby("date")["volatility_20"].transform("median")

    panel_df["trend_state"] = pd.Series(pd.NA, index=panel_df.index, dtype="object")
    trend_mask = panel_df["close_sma_150"].notna()
    panel_df.loc[trend_mask, "trend_state"] = np.where(
        panel_df.loc[trend_mask, "close"] > panel_df.loc[trend_mask, "close_sma_150"],
        "above_sma150",
        "below_or_at_sma150",
    )

    panel_df["momentum_state"] = pd.Series(pd.NA, index=panel_df.index, dtype="object")
    momentum_mask = panel_df["past_return_20"].notna()
    panel_df.loc[momentum_mask, "momentum_state"] = np.where(
        panel_df.loc[momentum_mask, "past_return_20"] > 0.0,
        "positive_20d",
        "non_positive_20d",
    )

    panel_df["liquidity_state"] = pd.Series(pd.NA, index=panel_df.index, dtype="object")
    liquidity_mask = panel_df["adv_20"].notna() & liquidity_median.notna()
    panel_df.loc[liquidity_mask, "liquidity_state"] = np.where(
        panel_df.loc[liquidity_mask, "adv_20"] >= liquidity_median.loc[liquidity_mask],
        "high_adv20",
        "low_adv20",
    )

    panel_df["volatility_state"] = pd.Series(pd.NA, index=panel_df.index, dtype="object")
    volatility_mask = panel_df["volatility_20"].notna() & volatility_median.notna()
    panel_df.loc[volatility_mask, "volatility_state"] = np.where(
        panel_df.loc[volatility_mask, "volatility_20"]
        >= volatility_median.loc[volatility_mask],
        "high_vol20",
        "low_vol20",
    )
    return panel_df


def _build_universe_summary_row(
    *,
    universe_key: UniverseKey,
    codes_df: pd.DataFrame,
    analysis_panel_df: pd.DataFrame,
    analysis_start_date: str | None,
    analysis_end_date: str | None,
    discovery_end_date: str | None,
    validation_start_date: str | None,
    analysis_panel_mode: str,
) -> dict[str, Any]:
    reference_ratio = (
        analysis_panel_df[f"volume_ma_{REFERENCE_SHORT_PERIOD}"]
        / analysis_panel_df[f"volume_ma_{REFERENCE_LONG_PERIOD}"]
    ).where(
        np.isfinite(
            analysis_panel_df[f"volume_ma_{REFERENCE_SHORT_PERIOD}"]
            / analysis_panel_df[f"volume_ma_{REFERENCE_LONG_PERIOD}"]
        )
    )
    reference_signal_mask = reference_ratio > REFERENCE_THRESHOLD
    return {
        "universe_key": universe_key,
        "universe_label": UNIVERSE_LABEL_MAP[universe_key],
        "membership_mode": MEMBERSHIP_MODE,
        "analysis_panel_mode": analysis_panel_mode,
        "stock_count": int(codes_df["code"].nunique()),
        "analysis_stock_count": int(analysis_panel_df["code"].nunique()),
        "stock_day_count": int(len(analysis_panel_df)),
        "analysis_date_count": int(analysis_panel_df["date"].nunique()) if not analysis_panel_df.empty else 0,
        "available_start_date": _safe_optional_date(analysis_panel_df["date"].min()),
        "available_end_date": _safe_optional_date(analysis_panel_df["date"].max()),
        "analysis_start_date": analysis_start_date,
        "analysis_end_date": analysis_end_date,
        "discovery_end_date": discovery_end_date,
        "validation_start_date": validation_start_date,
        "reference_signal_event_count": int(reference_signal_mask.fillna(False).sum()),
        "reference_signal_unique_code_count": int(
            analysis_panel_df.loc[reference_signal_mask.fillna(False), "code"].nunique()
        ),
    }


def _assign_cross_section_deciles(feature_df: pd.DataFrame) -> pd.DataFrame:
    if feature_df.empty:
        return feature_df
    ranked_df = feature_df.copy()
    ranked_df["date_constituent_count"] = ranked_df.groupby("date")["code"].transform("size")
    ranked_df = ranked_df.loc[ranked_df["date_constituent_count"] >= len(DECILE_ORDER)].copy()
    if ranked_df.empty:
        return ranked_df
    ranked_df["feature_rank_desc"] = (
        ranked_df.groupby("date")["volume_ratio"]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    ranked_df["feature_decile_index"] = (
        ((ranked_df["feature_rank_desc"] - 1) * len(DECILE_ORDER))
        // ranked_df["date_constituent_count"]
    ) + 1
    ranked_df["feature_decile_index"] = ranked_df["feature_decile_index"].clip(1, len(DECILE_ORDER))
    ranked_df["feature_decile"] = ranked_df["feature_decile_index"].map(
        {index: f"Q{index}" for index in range(1, len(DECILE_ORDER) + 1)}
    )
    return ranked_df


def _build_decile_tables_for_universe(
    *,
    universe_key: UniverseKey,
    panel_df: pd.DataFrame,
    parameter_grid_df: pd.DataFrame,
    horizons: tuple[int, ...],
    validation_start_ts: pd.Timestamp | None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    summary_rows: list[dict[str, Any]] = []
    spread_rows: list[dict[str, Any]] = []
    base_columns = ["date", "code", "company_name"]

    for parameter_row in parameter_grid_df.itertuples(index=False):
        short_period = _coerce_int(parameter_row.short_period)
        long_period = _coerce_int(parameter_row.long_period)
        ratio = panel_df[f"volume_ma_{short_period}"] / panel_df[f"volume_ma_{long_period}"]
        feature_df = panel_df.loc[np.isfinite(ratio), base_columns].copy()
        if feature_df.empty:
            continue
        feature_df["volume_ratio"] = ratio.loc[feature_df.index]
        for horizon_days in horizons:
            feature_df[f"close_to_close_{horizon_days}d"] = panel_df.loc[
                feature_df.index, f"close_to_close_{horizon_days}d"
            ]
            feature_df[f"next_open_to_close_{horizon_days}d"] = panel_df.loc[
                feature_df.index, f"next_open_to_close_{horizon_days}d"
            ]

        ranked_df = _assign_cross_section_deciles(feature_df)
        if ranked_df.empty:
            continue

        for split_key, split_df in _iter_split_frames(
            ranked_df,
            validation_start_ts=validation_start_ts,
        ):
            for entry_mode in ENTRY_MODE_ORDER:
                for horizon_days in horizons:
                    future_return_column = f"{entry_mode}_{horizon_days}d"
                    scoped_df = split_df.dropna(subset=[future_return_column]).copy()
                    if scoped_df.empty:
                        continue

                    baseline_mean_return = _coerce_float(
                        scoped_df[future_return_column].mean()
                    )
                    summary_df = (
                        scoped_df.groupby("feature_decile", as_index=False)
                        .agg(
                            sample_count=("code", "size"),
                            unique_code_count=("code", "nunique"),
                            date_count=("date", "nunique"),
                            mean_volume_ratio=("volume_ratio", "mean"),
                            median_volume_ratio=("volume_ratio", "median"),
                            mean_future_return=(future_return_column, "mean"),
                            median_future_return=(future_return_column, "median"),
                            positive_ratio=(
                                future_return_column,
                                lambda values: float((values > 0).mean()),
                            ),
                        )
                    )
                    for row in summary_df.itertuples(index=False):
                        summary_rows.append(
                            {
                                "universe_key": universe_key,
                                "universe_label": UNIVERSE_LABEL_MAP[universe_key],
                                "split_key": split_key,
                                "entry_mode": entry_mode,
                                "entry_mode_label": ENTRY_MODE_LABEL_MAP[entry_mode],
                                "horizon_days": horizon_days,
                                "short_period": short_period,
                                "long_period": long_period,
                                "feature_decile": row.feature_decile,
                                "sample_count": _coerce_int(row.sample_count),
                                "unique_code_count": _coerce_int(row.unique_code_count),
                                "date_count": _coerce_int(row.date_count),
                                "mean_volume_ratio": _coerce_float(row.mean_volume_ratio),
                                "median_volume_ratio": _coerce_float(row.median_volume_ratio),
                                "mean_future_return": _coerce_float(row.mean_future_return),
                                "median_future_return": _coerce_float(row.median_future_return),
                                "positive_ratio": _coerce_float(row.positive_ratio),
                                "mean_return_lift_vs_all": _coerce_float(
                                    _coerce_float(row.mean_future_return) - baseline_mean_return
                                ),
                            }
                        )

                    daily_decile_df = (
                        scoped_df.groupby(["date", "feature_decile"], as_index=False)
                        .agg(mean_future_return=(future_return_column, "mean"))
                    )
                    spread_pivot_df = (
                        daily_decile_df.pivot(
                            index="date",
                            columns="feature_decile",
                            values="mean_future_return",
                        )
                        .reindex(columns=list(DECILE_ORDER))
                        .dropna(subset=["Q1", "Q10"])
                    )
                    if spread_pivot_df.empty:
                        continue
                    spread_series = spread_pivot_df["Q1"] - spread_pivot_df["Q10"]
                    spread_t_stat, spread_p_value = _safe_one_sample_t_test(spread_series)
                    spread_rows.append(
                        {
                            "universe_key": universe_key,
                            "universe_label": UNIVERSE_LABEL_MAP[universe_key],
                            "split_key": split_key,
                            "entry_mode": entry_mode,
                            "entry_mode_label": ENTRY_MODE_LABEL_MAP[entry_mode],
                            "horizon_days": horizon_days,
                            "short_period": short_period,
                            "long_period": long_period,
                            "date_count": int(len(spread_pivot_df)),
                            "mean_q1_future_return": float(spread_pivot_df["Q1"].mean()),
                            "mean_q10_future_return": float(spread_pivot_df["Q10"].mean()),
                            "mean_q1_q10_return_spread": float(spread_series.mean()),
                            "median_q1_q10_return_spread": float(spread_series.median()),
                            "spread_t_statistic": spread_t_stat,
                            "spread_p_value": spread_p_value,
                        }
                    )

    return _sort_table(pd.DataFrame(summary_rows)), _sort_table(pd.DataFrame(spread_rows))


def _build_threshold_grid_for_universe(
    *,
    universe_key: UniverseKey,
    panel_df: pd.DataFrame,
    parameter_grid_df: pd.DataFrame,
    thresholds: tuple[float, ...],
    horizons: tuple[int, ...],
    validation_start_ts: pd.Timestamp | None,
    min_signal_events: int,
    min_unique_codes: int,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    base_columns = ["date", "code", "company_name"]

    for parameter_row in parameter_grid_df.itertuples(index=False):
        short_period = _coerce_int(parameter_row.short_period)
        long_period = _coerce_int(parameter_row.long_period)
        ratio = panel_df[f"volume_ma_{short_period}"] / panel_df[f"volume_ma_{long_period}"]
        feature_df = panel_df.loc[np.isfinite(ratio), base_columns].copy()
        if feature_df.empty:
            continue
        feature_df["volume_ratio"] = ratio.loc[feature_df.index]
        for horizon_days in horizons:
            feature_df[f"close_to_close_{horizon_days}d"] = panel_df.loc[
                feature_df.index, f"close_to_close_{horizon_days}d"
            ]
            feature_df[f"next_open_to_close_{horizon_days}d"] = panel_df.loc[
                feature_df.index, f"next_open_to_close_{horizon_days}d"
            ]

        for split_key, split_df in _iter_split_frames(
            feature_df,
            validation_start_ts=validation_start_ts,
        ):
            for entry_mode in ENTRY_MODE_ORDER:
                for horizon_days in horizons:
                    future_return_column = f"{entry_mode}_{horizon_days}d"
                    scoped_df = split_df.dropna(subset=[future_return_column]).copy()
                    if scoped_df.empty:
                        continue

                    baseline_values = scoped_df[future_return_column]
                    baseline_mean = _coerce_float(baseline_values.mean())
                    baseline_positive_ratio = _coerce_float((baseline_values > 0).mean())
                    total_event_count = int(len(scoped_df))
                    total_unique_codes = int(scoped_df["code"].nunique())
                    total_date_count = int(scoped_df["date"].nunique())

                    for threshold in thresholds:
                        signal_mask = scoped_df["volume_ratio"] > threshold
                        signal_df = scoped_df.loc[signal_mask].copy()
                        if signal_df.empty:
                            rows.append(
                                {
                                    "universe_key": universe_key,
                                    "universe_label": UNIVERSE_LABEL_MAP[universe_key],
                                    "split_key": split_key,
                                    "entry_mode": entry_mode,
                                    "entry_mode_label": ENTRY_MODE_LABEL_MAP[entry_mode],
                                    "horizon_days": horizon_days,
                                    "short_period": short_period,
                                    "long_period": long_period,
                                    "threshold": threshold,
                                    "total_event_count": total_event_count,
                                    "total_unique_code_count": total_unique_codes,
                                    "total_date_count": total_date_count,
                                    "signal_event_count": 0,
                                    "signal_unique_code_count": 0,
                                    "signal_date_count": 0,
                                    "signal_rate": 0.0,
                                    "mean_signal_volume_ratio": None,
                                    "mean_future_return": None,
                                    "median_future_return": None,
                                    "positive_ratio": None,
                                    "mean_return_lift_vs_all": None,
                                    "mean_return_lift_vs_non_signal": None,
                                    "positive_ratio_lift_vs_all": None,
                                    "welch_t_statistic": None,
                                    "welch_p_value": None,
                                    "meets_min_counts": False,
                                }
                            )
                            continue

                        signal_values = signal_df[future_return_column]
                        non_signal_values = scoped_df.loc[~signal_mask, future_return_column]
                        non_signal_mean = (
                            _coerce_float(non_signal_values.mean())
                            if not non_signal_values.empty
                            else None
                        )
                        non_signal_positive_ratio = (
                            _coerce_float((non_signal_values > 0).mean())
                            if not non_signal_values.empty
                            else None
                        )
                        t_stat, p_value = _safe_welch_t_test(signal_values, non_signal_values)
                        signal_event_count = int(len(signal_df))
                        signal_unique_codes = int(signal_df["code"].nunique())
                        rows.append(
                            {
                                "universe_key": universe_key,
                                "universe_label": UNIVERSE_LABEL_MAP[universe_key],
                                "split_key": split_key,
                                "entry_mode": entry_mode,
                                "entry_mode_label": ENTRY_MODE_LABEL_MAP[entry_mode],
                                "horizon_days": horizon_days,
                                "short_period": short_period,
                                "long_period": long_period,
                                "threshold": threshold,
                                "total_event_count": total_event_count,
                                "total_unique_code_count": total_unique_codes,
                                "total_date_count": total_date_count,
                                "signal_event_count": signal_event_count,
                                "signal_unique_code_count": signal_unique_codes,
                                "signal_date_count": int(signal_df["date"].nunique()),
                                "signal_rate": _coerce_float(signal_event_count / total_event_count),
                                "mean_signal_volume_ratio": _coerce_float(
                                    signal_df["volume_ratio"].mean()
                                ),
                                "mean_future_return": _coerce_float(signal_values.mean()),
                                "median_future_return": _coerce_float(signal_values.median()),
                                "positive_ratio": _coerce_float((signal_values > 0).mean()),
                                "mean_return_lift_vs_all": _coerce_float(
                                    signal_values.mean() - baseline_mean
                                ),
                                "mean_return_lift_vs_non_signal": (
                                    _coerce_float(signal_values.mean() - non_signal_mean)
                                    if non_signal_mean is not None
                                    else None
                                ),
                                "positive_ratio_lift_vs_all": _coerce_float(
                                    (signal_values > 0).mean() - baseline_positive_ratio
                                ),
                                "positive_ratio_lift_vs_non_signal": (
                                    _coerce_float(
                                        (signal_values > 0).mean() - non_signal_positive_ratio
                                    )
                                    if non_signal_positive_ratio is not None
                                    else None
                                ),
                                "welch_t_statistic": t_stat,
                                "welch_p_value": p_value,
                                "meets_min_counts": bool(
                                    signal_event_count >= min_signal_events
                                    and signal_unique_codes >= min_unique_codes
                                ),
                            }
                        )

    return _sort_table(pd.DataFrame(rows))


def _build_reference_condition_table_for_universe(
    *,
    universe_key: UniverseKey,
    panel_df: pd.DataFrame,
    horizons: tuple[int, ...],
    validation_start_ts: pd.Timestamp | None,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    reference_ratio = (
        panel_df[f"volume_ma_{REFERENCE_SHORT_PERIOD}"]
        / panel_df[f"volume_ma_{REFERENCE_LONG_PERIOD}"]
    )
    feature_df = panel_df.loc[np.isfinite(reference_ratio), ["date", "code", "company_name"]].copy()
    if feature_df.empty:
        return pd.DataFrame()

    feature_df["reference_volume_ratio"] = reference_ratio.loc[feature_df.index]
    for horizon_days in horizons:
        feature_df[f"close_to_close_{horizon_days}d"] = panel_df.loc[
            feature_df.index, f"close_to_close_{horizon_days}d"
        ]
        feature_df[f"next_open_to_close_{horizon_days}d"] = panel_df.loc[
            feature_df.index, f"next_open_to_close_{horizon_days}d"
        ]
    for family in CONDITION_FAMILY_ORDER:
        feature_df[family] = panel_df.loc[feature_df.index, family]

    for split_key, split_df in _iter_split_frames(
        feature_df,
        validation_start_ts=validation_start_ts,
    ):
        for entry_mode in ENTRY_MODE_ORDER:
            for horizon_days in horizons:
                future_return_column = f"{entry_mode}_{horizon_days}d"
                scoped_df = split_df.dropna(subset=[future_return_column]).copy()
                if scoped_df.empty:
                    continue

                for family in CONDITION_FAMILY_ORDER:
                    family_df = scoped_df.dropna(subset=[family]).copy()
                    if family_df.empty:
                        continue
                    for condition_value in CONDITION_VALUE_ORDER[family]:
                        condition_df = family_df.loc[family_df[family] == condition_value].copy()
                        if condition_df.empty:
                            continue
                        signal_mask = condition_df["reference_volume_ratio"] > REFERENCE_THRESHOLD
                        signal_df = condition_df.loc[signal_mask].copy()
                        signal_values = signal_df[future_return_column]
                        non_signal_values = condition_df.loc[~signal_mask, future_return_column]
                        non_signal_mean = (
                            _coerce_float(non_signal_values.mean())
                            if not non_signal_values.empty
                            else None
                        )
                        non_signal_positive_ratio = (
                            _coerce_float((non_signal_values > 0).mean())
                            if not non_signal_values.empty
                            else None
                        )
                        t_stat, p_value = _safe_welch_t_test(signal_values, non_signal_values)
                        rows.append(
                            {
                                "universe_key": universe_key,
                                "universe_label": UNIVERSE_LABEL_MAP[universe_key],
                                "split_key": split_key,
                                "entry_mode": entry_mode,
                                "entry_mode_label": ENTRY_MODE_LABEL_MAP[entry_mode],
                                "horizon_days": horizon_days,
                                "condition_family": family,
                                "condition_family_label": CONDITION_LABEL_MAP[family],
                                "condition_value": condition_value,
                                "condition_value_label": CONDITION_VALUE_LABEL_MAP[condition_value],
                                "condition_total_event_count": int(len(condition_df)),
                                "condition_unique_code_count": int(condition_df["code"].nunique()),
                                "condition_date_count": int(condition_df["date"].nunique()),
                                "signal_event_count": int(len(signal_df)),
                                "signal_unique_code_count": int(signal_df["code"].nunique()),
                                "signal_date_count": int(signal_df["date"].nunique()),
                                "signal_rate": _coerce_float(len(signal_df) / len(condition_df)),
                                "mean_signal_volume_ratio": (
                                    _coerce_float(signal_df["reference_volume_ratio"].mean())
                                    if not signal_df.empty
                                    else None
                                ),
                                "mean_future_return": (
                                    _coerce_float(signal_values.mean())
                                    if not signal_values.empty
                                    else None
                                ),
                                "median_future_return": (
                                    _coerce_float(signal_values.median())
                                    if not signal_values.empty
                                    else None
                                ),
                                "positive_ratio": (
                                    _coerce_float((signal_values > 0).mean())
                                    if not signal_values.empty
                                    else None
                                ),
                                "mean_return_lift_vs_non_signal": (
                                    _coerce_float(signal_values.mean() - non_signal_mean)
                                    if non_signal_mean is not None and not signal_values.empty
                                    else None
                                ),
                                "positive_ratio_lift_vs_non_signal": (
                                    _coerce_float(
                                        (signal_values > 0).mean() - non_signal_positive_ratio
                                    )
                                    if non_signal_positive_ratio is not None and not signal_values.empty
                                    else None
                                ),
                                "welch_t_statistic": t_stat,
                                "welch_p_value": p_value,
                            }
                        )
    return _sort_table(pd.DataFrame(rows))


def _sample_codes_for_universe(
    codes_df: pd.DataFrame,
    *,
    universe_key: UniverseKey,
    sample_seed: int,
    sample_size: int,
) -> pd.DataFrame:
    if codes_df.empty:
        return pd.DataFrame(
            columns=["universe_key", "universe_label", "code", "company_name", "sample_rank"]
        )
    sample_df = codes_df.copy()
    sample_df["universe_key"] = universe_key
    sample_df["universe_label"] = UNIVERSE_LABEL_MAP[universe_key]
    sample_df["seed_token"] = f"seed-{sample_seed}"
    sampled_df = select_deterministic_samples(
        sample_df,
        sample_size=sample_size,
        partition_columns=["universe_key"],
        hash_columns=["seed_token", "universe_key", "code"],
        final_order_columns=["universe_key", "sample_rank"],
    )
    return sampled_df.drop(columns=["seed_token"])


def _sample_reference_events_for_universe(
    panel_df: pd.DataFrame,
    *,
    universe_key: UniverseKey,
    sampled_codes_df: pd.DataFrame,
    sample_seed: int,
    sample_event_size: int,
) -> pd.DataFrame:
    if panel_df.empty or sampled_codes_df.empty or sample_event_size <= 0:
        return pd.DataFrame(columns=list(SAMPLED_REFERENCE_EVENT_COLUMNS))

    reference_ratio = (
        panel_df[f"volume_ma_{REFERENCE_SHORT_PERIOD}"]
        / panel_df[f"volume_ma_{REFERENCE_LONG_PERIOD}"]
    ).where(
        np.isfinite(
            panel_df[f"volume_ma_{REFERENCE_SHORT_PERIOD}"]
            / panel_df[f"volume_ma_{REFERENCE_LONG_PERIOD}"]
        )
    )
    reference_df = panel_df.loc[reference_ratio > REFERENCE_THRESHOLD].copy()
    if reference_df.empty:
        return pd.DataFrame(columns=list(SAMPLED_REFERENCE_EVENT_COLUMNS))

    reference_df["reference_volume_ratio"] = reference_ratio.loc[reference_df.index]
    reference_df["universe_key"] = universe_key
    reference_df["universe_label"] = UNIVERSE_LABEL_MAP[universe_key]
    reference_df = reference_df.merge(
        sampled_codes_df[["code"]],
        on="code",
        how="inner",
    )
    if reference_df.empty:
        return pd.DataFrame(columns=list(SAMPLED_REFERENCE_EVENT_COLUMNS))

    keep_columns = [
        "universe_key",
        "universe_label",
        "date",
        "code",
        "company_name",
        "reference_volume_ratio",
        "trend_state",
        "momentum_state",
        "liquidity_state",
        "volatility_state",
    ]
    for horizon_days in DEFAULT_HORIZONS:
        for entry_mode in ENTRY_MODE_ORDER:
            column_name = f"{entry_mode}_{horizon_days}d"
            if column_name in reference_df.columns:
                keep_columns.append(column_name)
    sampled_events_df = reference_df[keep_columns].copy()
    sampled_events_df["date"] = sampled_events_df["date"].dt.strftime("%Y-%m-%d")
    sampled_events_df["seed_token"] = f"seed-{sample_seed}"
    sampled_events_df = select_deterministic_samples(
        sampled_events_df,
        sample_size=sample_event_size,
        partition_columns=["universe_key"],
        hash_columns=["seed_token", "universe_key", "code", "date"],
        final_order_columns=["universe_key", "sample_rank"],
    )
    sampled_events_df = sampled_events_df.drop(columns=["seed_token"])
    return sampled_events_df.reindex(columns=list(SAMPLED_REFERENCE_EVENT_COLUMNS))


def _build_best_thresholds_table(
    threshold_grid_summary_df: pd.DataFrame,
    *,
    top_k: int,
) -> pd.DataFrame:
    if threshold_grid_summary_df.empty:
        return pd.DataFrame()

    merge_keys = [
        "universe_key",
        "universe_label",
        "entry_mode",
        "entry_mode_label",
        "horizon_days",
        "short_period",
        "long_period",
        "threshold",
    ]
    metric_columns = [
        "signal_event_count",
        "signal_unique_code_count",
        "signal_date_count",
        "signal_rate",
        "mean_signal_volume_ratio",
        "mean_future_return",
        "median_future_return",
        "positive_ratio",
        "mean_return_lift_vs_all",
        "mean_return_lift_vs_non_signal",
        "positive_ratio_lift_vs_all",
        "positive_ratio_lift_vs_non_signal",
        "welch_t_statistic",
        "welch_p_value",
        "meets_min_counts",
    ]

    split_tables: dict[str, pd.DataFrame] = {}
    for split_key in SPLIT_ORDER:
        scoped_df = threshold_grid_summary_df.loc[
            threshold_grid_summary_df["split_key"] == split_key,
            [*merge_keys, *metric_columns],
        ].copy()
        if scoped_df.empty:
            continue
        split_tables[split_key] = scoped_df.rename(
            columns={column: f"{split_key}_{column}" for column in metric_columns}
        )

    if "validation" not in split_tables:
        return pd.DataFrame()

    best_df = split_tables["validation"]
    if "discovery" in split_tables:
        best_df = best_df.merge(split_tables["discovery"], how="left", on=merge_keys)
    if "full" in split_tables:
        best_df = best_df.merge(split_tables["full"], how="left", on=merge_keys)

    best_df["passes_stability_gate"] = (
        best_df["validation_meets_min_counts"].fillna(False)
        & best_df.get("discovery_meets_min_counts", pd.Series(False, index=best_df.index)).fillna(False)
        & best_df.get(
            "discovery_mean_return_lift_vs_non_signal",
            pd.Series(np.nan, index=best_df.index),
        ).fillna(-np.inf).gt(0.0)
    )
    best_df["is_reference_config"] = (
        (best_df["short_period"] == REFERENCE_SHORT_PERIOD)
        & (best_df["long_period"] == REFERENCE_LONG_PERIOD)
        & best_df["threshold"].round(6).eq(round(REFERENCE_THRESHOLD, 6))
    )

    sort_columns = [
        "passes_stability_gate",
        "validation_mean_return_lift_vs_non_signal",
        "validation_mean_future_return",
        "validation_signal_event_count",
    ]
    ascending = [False, False, False, False]
    best_df = best_df.sort_values(sort_columns, ascending=ascending).reset_index(drop=True)
    best_df["selection_rank"] = (
        best_df.groupby(["universe_key", "entry_mode", "horizon_days"]).cumcount() + 1
    )
    best_df = best_df.loc[best_df["selection_rank"] <= top_k].copy()
    return _sort_table(best_df)


def _build_research_bundle_summary_markdown(
    result: VolumeRatioFutureReturnRegimeResearchResult,
) -> str:
    lines: list[str] = [
        "# Volume Ratio Future Return Regime",
        "",
        "## Snapshot",
        "",
        f"- Analysis range: `{result.analysis_start_date} -> {result.analysis_end_date}`",
        f"- Available range: `{result.available_start_date} -> {result.available_end_date}`",
        f"- Universes: `{', '.join(UNIVERSE_LABEL_MAP[key] for key in UNIVERSE_ORDER)}`",
        f"- Membership mode: `{result.membership_mode}`",
        f"- Validation ratio: `{result.validation_ratio:.2f}`",
        (
            f"- Analysis panel: `sampled {result.sample_size_per_universe} codes per universe`"
            if result.analysis_use_sampled_codes
            else "- Analysis panel: `full panel`"
        ),
        f"- Volume grid pairs: `{len(result.parameter_grid_df)}`",
        f"- Thresholds: `{', '.join(f'{value:.1f}' for value in result.threshold_values)}`",
        f"- Horizons: `{', '.join(str(value) for value in result.horizons)}` trading days",
        f"- Reference config: `short={result.reference_short_period}, long={result.reference_long_period}, threshold={result.reference_threshold:.1f}`",
        "",
        "## Universe Coverage",
        "",
    ]

    for row in result.universe_summary_df.itertuples(index=False):
        lines.append(
            f"- `{row.universe_label}`: universe stocks=`{row.stock_count}`, analysis stocks=`{row.analysis_stock_count}`, "
            f"stock-days=`{row.stock_day_count}`, panel=`{row.analysis_panel_mode}`, "
            f"analysis dates=`{row.analysis_date_count}`, reference signal events=`{row.reference_signal_event_count}`"
        )

    lines.extend(["", "## Reference Config Quick Read", ""])
    reference_rows = result.threshold_grid_summary_df.loc[
        (result.threshold_grid_summary_df["split_key"] == "full")
        & (result.threshold_grid_summary_df["entry_mode"] == "next_open_to_close")
        & (result.threshold_grid_summary_df["horizon_days"] == 5)
        & (result.threshold_grid_summary_df["short_period"] == result.reference_short_period)
        & (result.threshold_grid_summary_df["long_period"] == result.reference_long_period)
        & result.threshold_grid_summary_df["threshold"].round(6).eq(
            round(result.reference_threshold, 6)
        )
    ].copy()
    if reference_rows.empty:
        lines.append("- No full-split reference rows were generated.")
    else:
        for row in _sort_table(reference_rows).itertuples(index=False):
            lift = (
                "N/A"
                if row.mean_return_lift_vs_non_signal is None
                else f"{_coerce_float(row.mean_return_lift_vs_non_signal) * 10000:.1f} bps"
            )
            mean_ret = (
                "N/A"
                if row.mean_future_return is None
                else f"{_coerce_float(row.mean_future_return) * 100:.2f}%"
            )
            lines.append(
                f"- `{row.universe_label}` next-open 5d: mean return=`{mean_ret}`, "
                f"lift vs non-signal=`{lift}`, signal events=`{_coerce_int(row.signal_event_count)}`"
            )

    lines.extend(["", "## Best Validated Configs", ""])
    best_rows = result.best_thresholds_df.loc[
        (result.best_thresholds_df["selection_rank"] == 1)
        & (result.best_thresholds_df["entry_mode"] == "next_open_to_close")
        & (result.best_thresholds_df["horizon_days"] == 5)
    ].copy()
    if best_rows.empty:
        lines.append("- No validated winners satisfied the stability gate.")
    else:
        for row in _sort_table(best_rows).itertuples(index=False):
            validation_lift = (
                "N/A"
                if row.validation_mean_return_lift_vs_non_signal is None
                else f"{_coerce_float(row.validation_mean_return_lift_vs_non_signal) * 10000:.1f} bps"
            )
            discovery_lift = (
                "N/A"
                if getattr(row, "discovery_mean_return_lift_vs_non_signal", None) is None
                else f"{_coerce_float(row.discovery_mean_return_lift_vs_non_signal) * 10000:.1f} bps"
            )
            lines.append(
                f"- `{row.universe_label}`: best=`short={_coerce_int(row.short_period)}, long={_coerce_int(row.long_period)}, threshold={_coerce_float(row.threshold):.1f}`, "
                f"validation lift=`{validation_lift}`, discovery lift=`{discovery_lift}`, "
                f"stability gate=`{bool(row.passes_stability_gate)}`"
            )

    lines.extend(["", "## Trend Conditioning On Reference Config", ""])
    trend_rows = result.reference_condition_summary_df.loc[
        (result.reference_condition_summary_df["split_key"] == "full")
        & (result.reference_condition_summary_df["entry_mode"] == "next_open_to_close")
        & (result.reference_condition_summary_df["horizon_days"] == 5)
        & (result.reference_condition_summary_df["condition_family"] == "trend_state")
    ].copy()
    if trend_rows.empty:
        lines.append("- No trend-conditioned reference rows were generated.")
    else:
        for universe_key in UNIVERSE_ORDER:
            scoped_df = trend_rows.loc[trend_rows["universe_key"] == universe_key].copy()
            if scoped_df.empty:
                continue
            best_row = scoped_df.sort_values(
                by=["mean_return_lift_vs_non_signal", "signal_event_count"],
                ascending=[False, False],
            ).iloc[0]
            lift = (
                "N/A"
                if pd.isna(best_row["mean_return_lift_vs_non_signal"])
                else f"{float(best_row['mean_return_lift_vs_non_signal']) * 10000:.1f} bps"
            )
            lines.append(
                f"- `{UNIVERSE_LABEL_MAP[universe_key]}` strongest trend bucket: "
                f"`{best_row['condition_value_label']}` with lift vs non-signal `{lift}`"
            )

    lines.extend(["", "## Artifact Tables", ""])
    for table_name in TABLE_FIELD_NAMES:
        lines.append(f"- `{table_name}`")
    return "\n".join(lines)


def _split_result_payload(
    result: VolumeRatioFutureReturnRegimeResearchResult,
) -> tuple[dict[str, Any], dict[str, pd.DataFrame]]:
    metadata_fields = {field.name for field in fields(result)} - set(TABLE_FIELD_NAMES)
    result_metadata = {
        field_name: getattr(result, field_name) for field_name in sorted(metadata_fields)
    }
    result_tables = {
        field_name: cast(pd.DataFrame, getattr(result, field_name))
        for field_name in TABLE_FIELD_NAMES
    }
    return result_metadata, result_tables


def run_volume_ratio_future_return_regime_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    lookback_years: int = DEFAULT_LOOKBACK_YEARS,
    validation_ratio: float = DEFAULT_VALIDATION_RATIO,
    analysis_use_sampled_codes: bool = False,
    short_windows: tuple[int, ...] | list[int] | None = None,
    long_windows: tuple[int, ...] | list[int] | None = None,
    threshold_values: tuple[float, ...] | list[float] | None = None,
    horizons: tuple[int, ...] | list[int] | None = None,
    sample_seed: int = DEFAULT_SAMPLE_SEED,
    sample_size_per_universe: int = DEFAULT_SAMPLE_SIZE_PER_UNIVERSE,
    sample_event_size_per_universe: int = DEFAULT_SAMPLE_EVENT_SIZE_PER_UNIVERSE,
    min_signal_events: int = DEFAULT_MIN_SIGNAL_EVENTS,
    min_unique_codes: int = DEFAULT_MIN_UNIQUE_CODES,
    top_k: int = DEFAULT_TOP_K,
) -> VolumeRatioFutureReturnRegimeResearchResult:
    if lookback_years <= 0:
        raise ValueError("lookback_years must be positive")
    if not 0.0 <= validation_ratio < 1.0:
        raise ValueError("validation_ratio must satisfy 0.0 <= ratio < 1.0")
    if sample_size_per_universe < 0:
        raise ValueError("sample_size_per_universe must be non-negative")
    if sample_event_size_per_universe < 0:
        raise ValueError("sample_event_size_per_universe must be non-negative")
    if min_signal_events < 0:
        raise ValueError("min_signal_events must be non-negative")
    if min_unique_codes < 0:
        raise ValueError("min_unique_codes must be non-negative")
    if top_k <= 0:
        raise ValueError("top_k must be positive")

    normalized_short_windows = _normalize_int_sequence(
        short_windows,
        fallback=DEFAULT_SHORT_WINDOWS,
        name="short_windows",
    )
    normalized_long_windows = _normalize_int_sequence(
        long_windows,
        fallback=DEFAULT_LONG_WINDOWS,
        name="long_windows",
    )
    normalized_threshold_values = _normalize_float_sequence(
        threshold_values,
        fallback=DEFAULT_THRESHOLDS,
        name="threshold_values",
    )
    normalized_horizons = _normalize_int_sequence(
        horizons,
        fallback=DEFAULT_HORIZONS,
        name="horizons",
    )

    parameter_grid_df = _build_parameter_grid(
        short_windows=normalized_short_windows,
        long_windows=normalized_long_windows,
    )
    unique_volume_windows = tuple(
        sorted(
            {
                *normalized_short_windows,
                *normalized_long_windows,
                REFERENCE_SHORT_PERIOD,
                REFERENCE_LONG_PERIOD,
            }
        )
    )

    universe_summary_rows: list[dict[str, Any]] = []
    sampled_code_tables: list[pd.DataFrame] = []
    sampled_reference_event_tables: list[pd.DataFrame] = []
    decile_summary_tables: list[pd.DataFrame] = []
    decile_spread_tables: list[pd.DataFrame] = []
    threshold_grid_tables: list[pd.DataFrame] = []
    reference_condition_tables: list[pd.DataFrame] = []
    available_start_dates: list[str] = []
    available_end_dates: list[str] = []
    analysis_start_dates: list[str] = []
    analysis_end_dates: list[str] = []
    source_mode: SourceMode = "live"
    source_detail = ""

    with _open_analysis_connection(db_path) as ctx:
        conn = ctx.connection
        source_mode = cast(SourceMode, ctx.source_mode)
        source_detail = str(ctx.source_detail)

        for universe_key in UNIVERSE_ORDER:
            universe_codes_df = _query_universe_codes(conn, universe_key=universe_key)
            history_df = _query_universe_stock_history(
                conn,
                universe_key=universe_key,
                end_date=end_date,
            )
            if history_df.empty:
                continue

            available_start_date = _safe_optional_date(history_df["date"].min())
            available_end_date = _safe_optional_date(history_df["date"].max())
            effective_start_date = start_date or _default_start_date(
                available_start_date=available_start_date,
                available_end_date=available_end_date,
                lookback_years=lookback_years,
            )
            effective_end_date = end_date or available_end_date

            universe_panel_df = _prepare_universe_panel(
                history_df,
                analysis_start_date=effective_start_date,
                analysis_end_date=effective_end_date,
                unique_volume_windows=unique_volume_windows,
                horizons=normalized_horizons,
            )
            if universe_panel_df.empty:
                continue

            discovery_end_date, validation_start_date, validation_start_ts = (
                _resolve_validation_split_dates(
                    universe_panel_df,
                    validation_ratio=validation_ratio,
                )
            )

            sampled_codes_df = _sample_codes_for_universe(
                universe_codes_df,
                universe_key=universe_key,
                sample_seed=sample_seed,
                sample_size=sample_size_per_universe,
            )
            sampled_code_tables.append(sampled_codes_df)

            analysis_panel_df = universe_panel_df
            analysis_panel_mode = "full_panel"
            if analysis_use_sampled_codes and not sampled_codes_df.empty:
                analysis_panel_df = universe_panel_df.merge(
                    sampled_codes_df[["code"]],
                    on="code",
                    how="inner",
                )
                analysis_panel_mode = "sampled_codes_only"

            universe_summary_rows.append(
                _build_universe_summary_row(
                    universe_key=universe_key,
                    codes_df=universe_codes_df,
                    analysis_panel_df=analysis_panel_df,
                    analysis_start_date=effective_start_date,
                    analysis_end_date=effective_end_date,
                    discovery_end_date=discovery_end_date,
                    validation_start_date=validation_start_date,
                    analysis_panel_mode=analysis_panel_mode,
                )
            )
            sampled_reference_event_tables.append(
                _sample_reference_events_for_universe(
                    analysis_panel_df,
                    universe_key=universe_key,
                    sampled_codes_df=sampled_codes_df,
                    sample_seed=sample_seed,
                    sample_event_size=sample_event_size_per_universe,
                )
            )

            decile_summary_df, decile_spread_summary_df = _build_decile_tables_for_universe(
                universe_key=universe_key,
                panel_df=analysis_panel_df,
                parameter_grid_df=parameter_grid_df,
                horizons=normalized_horizons,
                validation_start_ts=validation_start_ts,
            )
            decile_summary_tables.append(decile_summary_df)
            decile_spread_tables.append(decile_spread_summary_df)

            threshold_grid_tables.append(
                _build_threshold_grid_for_universe(
                    universe_key=universe_key,
                    panel_df=analysis_panel_df,
                    parameter_grid_df=parameter_grid_df,
                    thresholds=normalized_threshold_values,
                    horizons=normalized_horizons,
                    validation_start_ts=validation_start_ts,
                    min_signal_events=min_signal_events,
                    min_unique_codes=min_unique_codes,
                )
            )
            reference_condition_tables.append(
                _build_reference_condition_table_for_universe(
                    universe_key=universe_key,
                    panel_df=analysis_panel_df,
                    horizons=normalized_horizons,
                    validation_start_ts=validation_start_ts,
                )
            )

            if available_start_date is not None:
                available_start_dates.append(available_start_date)
            if available_end_date is not None:
                available_end_dates.append(available_end_date)
            if effective_start_date is not None:
                analysis_start_dates.append(effective_start_date)
            if effective_end_date is not None:
                analysis_end_dates.append(effective_end_date)

    threshold_grid_summary_df = _sort_table(
        pd.concat(threshold_grid_tables, ignore_index=True)
        if threshold_grid_tables
        else pd.DataFrame()
    )
    result = VolumeRatioFutureReturnRegimeResearchResult(
        db_path=db_path,
        source_mode=source_mode,
        source_detail=source_detail,
        available_start_date=min(available_start_dates) if available_start_dates else None,
        available_end_date=max(available_end_dates) if available_end_dates else None,
        default_start_date=min(analysis_start_dates) if analysis_start_dates else None,
        analysis_start_date=min(analysis_start_dates) if analysis_start_dates else None,
        analysis_end_date=max(analysis_end_dates) if analysis_end_dates else None,
        lookback_years=lookback_years,
        validation_ratio=validation_ratio,
        analysis_use_sampled_codes=analysis_use_sampled_codes,
        sample_seed=sample_seed,
        sample_size_per_universe=sample_size_per_universe,
        sample_event_size_per_universe=sample_event_size_per_universe,
        short_windows=normalized_short_windows,
        long_windows=normalized_long_windows,
        threshold_values=normalized_threshold_values,
        horizons=normalized_horizons,
        min_signal_events=min_signal_events,
        min_unique_codes=min_unique_codes,
        top_k=top_k,
        reference_short_period=REFERENCE_SHORT_PERIOD,
        reference_long_period=REFERENCE_LONG_PERIOD,
        reference_threshold=REFERENCE_THRESHOLD,
        membership_mode=MEMBERSHIP_MODE,
        parameter_grid_df=_sort_table(parameter_grid_df),
        universe_summary_df=_sort_table(pd.DataFrame(universe_summary_rows)),
        sampled_codes_df=_sort_table(
            pd.concat(sampled_code_tables, ignore_index=True)
            if sampled_code_tables
            else pd.DataFrame()
        ),
        sampled_reference_events_df=_sort_table(
            pd.concat(sampled_reference_event_tables, ignore_index=True)
            if sampled_reference_event_tables
            else pd.DataFrame(columns=list(SAMPLED_REFERENCE_EVENT_COLUMNS))
        ),
        decile_summary_df=_sort_table(
            pd.concat(decile_summary_tables, ignore_index=True)
            if decile_summary_tables
            else pd.DataFrame()
        ),
        decile_spread_summary_df=_sort_table(
            pd.concat(decile_spread_tables, ignore_index=True)
            if decile_spread_tables
            else pd.DataFrame()
        ),
        threshold_grid_summary_df=threshold_grid_summary_df,
        best_thresholds_df=_build_best_thresholds_table(
            threshold_grid_summary_df,
            top_k=top_k,
        ),
        reference_condition_summary_df=_sort_table(
            pd.concat(reference_condition_tables, ignore_index=True)
            if reference_condition_tables
            else pd.DataFrame()
        ),
    )
    return result


def write_volume_ratio_future_return_regime_research_bundle(
    result: VolumeRatioFutureReturnRegimeResearchResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    result_metadata, result_tables = _split_result_payload(result)
    return write_research_bundle(
        experiment_id=VOLUME_RATIO_FUTURE_RETURN_REGIME_EXPERIMENT_ID,
        module=__name__,
        function="run_volume_ratio_future_return_regime_research",
        params={
            "lookback_years": result.lookback_years,
            "validation_ratio": result.validation_ratio,
            "analysis_use_sampled_codes": result.analysis_use_sampled_codes,
            "sample_seed": result.sample_seed,
            "sample_size_per_universe": result.sample_size_per_universe,
            "sample_event_size_per_universe": result.sample_event_size_per_universe,
            "short_windows": list(result.short_windows),
            "long_windows": list(result.long_windows),
            "threshold_values": list(result.threshold_values),
            "horizons": list(result.horizons),
            "min_signal_events": result.min_signal_events,
            "min_unique_codes": result.min_unique_codes,
            "top_k": result.top_k,
            "reference_short_period": result.reference_short_period,
            "reference_long_period": result.reference_long_period,
            "reference_threshold": result.reference_threshold,
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


def load_volume_ratio_future_return_regime_research_bundle(
    bundle_path: str | Path,
) -> VolumeRatioFutureReturnRegimeResearchResult:
    info = load_research_bundle_info(bundle_path)
    tables = load_research_bundle_tables(bundle_path)
    metadata = dict(info.result_metadata)
    return VolumeRatioFutureReturnRegimeResearchResult(
        db_path=str(metadata["db_path"]),
        source_mode=cast(SourceMode, metadata["source_mode"]),
        source_detail=str(metadata["source_detail"]),
        available_start_date=cast(str | None, metadata.get("available_start_date")),
        available_end_date=cast(str | None, metadata.get("available_end_date")),
        default_start_date=cast(str | None, metadata.get("default_start_date")),
        analysis_start_date=cast(str | None, metadata.get("analysis_start_date")),
        analysis_end_date=cast(str | None, metadata.get("analysis_end_date")),
        lookback_years=int(metadata["lookback_years"]),
        validation_ratio=float(metadata["validation_ratio"]),
        analysis_use_sampled_codes=bool(metadata["analysis_use_sampled_codes"]),
        sample_seed=int(metadata["sample_seed"]),
        sample_size_per_universe=int(metadata["sample_size_per_universe"]),
        sample_event_size_per_universe=int(metadata["sample_event_size_per_universe"]),
        short_windows=tuple(int(value) for value in metadata["short_windows"]),
        long_windows=tuple(int(value) for value in metadata["long_windows"]),
        threshold_values=tuple(float(value) for value in metadata["threshold_values"]),
        horizons=tuple(int(value) for value in metadata["horizons"]),
        min_signal_events=int(metadata["min_signal_events"]),
        min_unique_codes=int(metadata["min_unique_codes"]),
        top_k=int(metadata["top_k"]),
        reference_short_period=int(metadata["reference_short_period"]),
        reference_long_period=int(metadata["reference_long_period"]),
        reference_threshold=float(metadata["reference_threshold"]),
        membership_mode=str(metadata["membership_mode"]),
        parameter_grid_df=tables["parameter_grid_df"],
        universe_summary_df=tables["universe_summary_df"],
        sampled_codes_df=tables["sampled_codes_df"],
        sampled_reference_events_df=tables["sampled_reference_events_df"],
        decile_summary_df=tables["decile_summary_df"],
        decile_spread_summary_df=tables["decile_spread_summary_df"],
        threshold_grid_summary_df=tables["threshold_grid_summary_df"],
        best_thresholds_df=tables["best_thresholds_df"],
        reference_condition_summary_df=tables["reference_condition_summary_df"],
    )


def get_volume_ratio_future_return_regime_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        VOLUME_RATIO_FUTURE_RETURN_REGIME_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_volume_ratio_future_return_regime_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        VOLUME_RATIO_FUTURE_RETURN_REGIME_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )
