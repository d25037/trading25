"""High-zone bearish price-action event study.

The study tests whether bearish candlestick patterns near recent highs predict
weaker forward returns, and separates strict engulfing from broader large-red
body / volume-expansion patterns.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any, NamedTuple, cast

import pandas as pd

from src.domains.analytics.deterministic_sampling import select_deterministic_samples
from src.domains.analytics.readonly_duckdb_support import (
    SourceMode,
    fetch_date_range,
    normalize_code_sql,
    open_readonly_analysis_connection,
)
from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    load_research_bundle_info,
    load_research_bundle_tables,
    write_research_bundle,
)
from src.domains.analytics.topix_rank_future_close_core import _default_start_date
from src.shared.utils.market_code_alias import expand_market_codes

HIGH_ZONE_BEARISH_PRICE_ACTION_EXPERIMENT_ID = (
    "market-behavior/high-zone-bearish-price-action"
)

DEFAULT_LOOKBACK_YEARS = 10
DEFAULT_HORIZONS: tuple[int, ...] = (1, 3, 5, 10, 20)
DEFAULT_SAMPLE_EVENT_SIZE = 30
DEFAULT_MIN_EVENTS_FOR_SELECTION = 100
ATR_WINDOW = 14
ROLLING_HIGH_WINDOW = 252
SMA_WINDOW = 250
RUNUP_WINDOW = 20
VOLUME_WINDOW = 20
MARKET_ORDER: tuple[str, ...] = ("prime", "standard")
MARKET_LABELS: dict[str, str] = {
    "prime": "Prime",
    "standard": "Standard",
}
MARKET_CODES: dict[str, tuple[str, ...]] = {
    "prime": tuple(expand_market_codes(["prime"])),
    "standard": tuple(expand_market_codes(["standard"])),
}
MEMBERSHIP_MODE = "stock_master_daily_as_of_price_date"


class FilterDefinition(NamedTuple):
    key: str
    label: str
    expression: str


HIGH_ZONE_DEFINITIONS: tuple[FilterDefinition, ...] = (
    FilterDefinition(
        "any_high_zone",
        "Any high-zone definition",
        "(near_52w_high OR above_sma250_105 OR runup_20d_10)",
    ),
    FilterDefinition("near_52w_high", "Close >= 95% of prior 252d high", "near_52w_high"),
    FilterDefinition("above_sma250_105", "Close / SMA250 >= 1.05", "above_sma250_105"),
    FilterDefinition("runup_20d_10", "Past 20d return >= 10%", "runup_20d_10"),
)

PATTERN_DEFINITIONS: tuple[FilterDefinition, ...] = (
    FilterDefinition("high_zone_all", "High-zone baseline", "TRUE"),
    FilterDefinition("bearish_day", "Bearish candle", "bearish_day"),
    FilterDefinition("strict_bearish_engulfing", "Strict bearish engulfing", "strict_bearish_engulfing"),
    FilterDefinition("relaxed_body_engulfing", "Relaxed body engulfing", "relaxed_body_engulfing"),
    FilterDefinition("bearish_outside_day", "Bearish outside day", "bearish_outside_day"),
    FilterDefinition("large_red_atr_0_8", "Red body / prior ATR >= 0.8", "large_red_atr_0_8"),
    FilterDefinition("large_red_atr_1_2", "Red body / prior ATR >= 1.2", "large_red_atr_1_2"),
    FilterDefinition("large_red_atr_1_6", "Red body / prior ATR >= 1.6", "large_red_atr_1_6"),
    FilterDefinition(
        "large_red_atr_1_2_close_low",
        "Red body / prior ATR >= 1.2 and close near low",
        "(large_red_atr_1_2 AND close_near_low)",
    ),
    FilterDefinition(
        "strict_engulfing_close_low",
        "Strict engulfing and close near low",
        "(strict_bearish_engulfing AND close_near_low)",
    ),
)

VOLUME_DEFINITIONS: tuple[FilterDefinition, ...] = (
    FilterDefinition("all", "All volume regimes", "TRUE"),
    FilterDefinition("volume_ratio_20d_ge_1_5", "Volume / prior 20d average >= 1.5", "volume_ratio_20d >= 1.5"),
    FilterDefinition("volume_ratio_20d_ge_2", "Volume / prior 20d average >= 2.0", "volume_ratio_20d >= 2.0"),
    FilterDefinition(
        "value_ratio_20d_ge_2",
        "Trading value / prior 20d average >= 2.0",
        "trading_value_ratio_20d >= 2.0",
    ),
    FilterDefinition(
        "no_volume_or_value_surge",
        "No 1.5x volume/value expansion",
        "(coalesce(volume_ratio_20d, 0) < 1.5 AND coalesce(trading_value_ratio_20d, 0) < 1.5)",
    ),
)

TABLE_FIELD_NAMES: tuple[str, ...] = (
    "universe_summary_df",
    "pattern_summary_df",
    "top_negative_patterns_df",
    "sampled_events_df",
)


@dataclass(frozen=True)
class HighZoneBearishPriceActionResearchResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    default_start_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    lookback_years: int
    horizons: tuple[int, ...]
    sample_event_size: int
    min_events_for_selection: int
    membership_mode: str
    universe_summary_df: pd.DataFrame
    pattern_summary_df: pd.DataFrame
    top_negative_patterns_df: pd.DataFrame
    sampled_events_df: pd.DataFrame


def _normalize_horizons(horizons: tuple[int, ...] | list[int] | None) -> tuple[int, ...]:
    if horizons is None:
        return DEFAULT_HORIZONS
    normalized = tuple(sorted(dict.fromkeys(int(value) for value in horizons if int(value) > 0)))
    if not normalized:
        raise ValueError("horizons must contain at least one positive integer")
    return normalized


def _warmup_start_date(analysis_start_date: str | None, available_start_date: str | None) -> str | None:
    if analysis_start_date is None:
        return available_start_date
    candidate = (pd.Timestamp(analysis_start_date) - pd.Timedelta(days=420)).strftime("%Y-%m-%d")
    if available_start_date is None:
        return candidate
    return max(available_start_date, candidate)


def _sql_string_list(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{value}'" for value in values)


def _sort_table(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    result = df.copy()
    if "market_key" in result.columns:
        result["_market_order"] = result["market_key"].map(
            {key: index for index, key in enumerate(MARKET_ORDER)}
        )
    if "high_zone_key" in result.columns:
        result["_high_zone_order"] = result["high_zone_key"].map(
            {item.key: index for index, item in enumerate(HIGH_ZONE_DEFINITIONS)}
        )
    if "pattern_key" in result.columns:
        result["_pattern_order"] = result["pattern_key"].map(
            {item.key: index for index, item in enumerate(PATTERN_DEFINITIONS)}
        )
    if "volume_key" in result.columns:
        result["_volume_order"] = result["volume_key"].map(
            {item.key: index for index, item in enumerate(VOLUME_DEFINITIONS)}
        )
    sort_columns = [
        column
        for column in (
            "_market_order",
            "_high_zone_order",
            "_pattern_order",
            "_volume_order",
            "horizon_days",
            "mean_excess_return_vs_topix",
            "event_count",
            "date",
            "code",
        )
        if column in result.columns
    ]
    if sort_columns:
        result = result.sort_values(by=sort_columns, kind="stable").reset_index(drop=True)
    return result.drop(columns=[column for column in result.columns if column.startswith("_")])


def _create_feature_panel(
    conn: Any,
    *,
    raw_start_date: str | None,
    analysis_start_date: str | None,
    analysis_end_date: str | None,
    horizons: tuple[int, ...],
) -> None:
    price_code = normalize_code_sql("sd.code")
    master_code = normalize_code_sql("smd.code")
    market_code_values = tuple(dict.fromkeys([*MARKET_CODES["prime"], *MARKET_CODES["standard"]]))
    market_code_sql = _sql_string_list(market_code_values)
    raw_date_filter = ""
    raw_params: list[str] = []
    if raw_start_date is not None:
        raw_date_filter = "WHERE sd.date >= ?"
        raw_params.append(raw_start_date)
    final_conditions: list[str] = []
    final_params: list[str] = []
    if analysis_start_date is not None:
        final_conditions.append("date >= ?")
        final_params.append(analysis_start_date)
    if analysis_end_date is not None:
        final_conditions.append("date <= ?")
        final_params.append(analysis_end_date)
    final_where = "" if not final_conditions else "WHERE " + " AND ".join(final_conditions)
    lead_close_exprs = ",\n                ".join(
        f"lead(close, {horizon}) over (partition by code order by date) as future_close_{horizon}d"
        for horizon in horizons
    )
    return_exprs = ",\n            ".join(
        [
            f"case when next_open > 0 and future_close_{horizon}d > 0 "
            f"then future_close_{horizon}d / next_open - 1 end as return_next_open_to_close_{horizon}d"
            for horizon in horizons
        ]
    )
    topix_lead_exprs = ",\n                ".join(
        f"lead(close, {horizon}) over (order by date) as topix_future_close_{horizon}d"
        for horizon in horizons
    )
    topix_return_exprs = ",\n            ".join(
        [
            f"case when topix_next_open > 0 and topix_future_close_{horizon}d > 0 "
            f"then topix_future_close_{horizon}d / topix_next_open - 1 end as topix_return_{horizon}d"
            for horizon in horizons
        ]
    )
    excess_exprs = ",\n            ".join(
        [
            f"return_next_open_to_close_{horizon}d - topix_return_{horizon}d "
            f"as excess_return_vs_topix_{horizon}d"
            for horizon in horizons
        ]
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE high_zone_bearish_price_action_panel AS
        WITH raw_prices AS (
            SELECT
                {price_code} AS code,
                sd.date,
                sd.open,
                sd.high,
                sd.low,
                sd.close,
                sd.volume,
                row_number() OVER (
                    PARTITION BY {price_code}, sd.date
                    ORDER BY CASE WHEN length(sd.code) = 4 THEN 0 ELSE 1 END, sd.code
                ) AS row_rank
            FROM stock_data sd
            {raw_date_filter}
        ),
        prices AS (
            SELECT code, date, open, high, low, close, volume
            FROM raw_prices
            WHERE row_rank = 1
              AND open > 0 AND high > 0 AND low > 0 AND close > 0
        ),
        master AS (
            SELECT
                {master_code} AS code,
                smd.date,
                CASE
                    WHEN smd.market_code IN ({_sql_string_list(MARKET_CODES["prime"])}) THEN 'prime'
                    WHEN smd.market_code IN ({_sql_string_list(MARKET_CODES["standard"])}) THEN 'standard'
                END AS market_key,
                smd.company_name,
                row_number() OVER (
                    PARTITION BY {master_code}, smd.date
                    ORDER BY CASE WHEN length(smd.code) = 4 THEN 0 ELSE 1 END, smd.code
                ) AS row_rank
            FROM stock_master_daily smd
            WHERE smd.market_code IN ({market_code_sql})
        ),
        scoped AS (
            SELECT p.*, m.market_key, m.company_name
            FROM prices p
            JOIN master m ON m.code = p.code AND m.date = p.date AND m.row_rank = 1
            WHERE m.market_key IS NOT NULL
        ),
        lagged AS (
            SELECT
                *,
                lag(open) OVER (PARTITION BY code ORDER BY date) AS prev_open,
                lag(high) OVER (PARTITION BY code ORDER BY date) AS prev_high,
                lag(low) OVER (PARTITION BY code ORDER BY date) AS prev_low,
                lag(close) OVER (PARTITION BY code ORDER BY date) AS prev_close,
                lag(close, {RUNUP_WINDOW}) OVER (PARTITION BY code ORDER BY date) AS close_lag_{RUNUP_WINDOW},
                lead(open, 1) OVER (PARTITION BY code ORDER BY date) AS next_open,
                {lead_close_exprs}
            FROM scoped
        ),
        ranged AS (
            SELECT
                *,
                greatest(high - low, abs(high - prev_close), abs(low - prev_close)) AS true_range,
                abs(close - open) AS body_size,
                abs(prev_close - prev_open) AS prev_body_size,
                volume * close AS trading_value
            FROM lagged
        ),
        featured AS (
            SELECT
                *,
                avg(true_range) OVER (
                    PARTITION BY code ORDER BY date
                    ROWS BETWEEN {ATR_WINDOW} PRECEDING AND 1 PRECEDING
                ) AS atr_{ATR_WINDOW}_prev,
                max(high) OVER (
                    PARTITION BY code ORDER BY date
                    ROWS BETWEEN {ROLLING_HIGH_WINDOW} PRECEDING AND 1 PRECEDING
                ) AS rolling_high_{ROLLING_HIGH_WINDOW}_prev,
                avg(close) OVER (
                    PARTITION BY code ORDER BY date
                    ROWS BETWEEN {SMA_WINDOW} PRECEDING AND 1 PRECEDING
                ) AS sma_{SMA_WINDOW}_prev,
                avg(volume) OVER (
                    PARTITION BY code ORDER BY date
                    ROWS BETWEEN {VOLUME_WINDOW} PRECEDING AND 1 PRECEDING
                ) AS avg_volume_{VOLUME_WINDOW}_prev,
                avg(trading_value) OVER (
                    PARTITION BY code ORDER BY date
                    ROWS BETWEEN {VOLUME_WINDOW} PRECEDING AND 1 PRECEDING
                ) AS avg_trading_value_{VOLUME_WINDOW}_prev
            FROM ranged
        ),
        topix_lagged AS (
            SELECT
                date,
                lead(open, 1) OVER (ORDER BY date) AS topix_next_open,
                {topix_lead_exprs}
            FROM topix_data
            WHERE open > 0 AND close > 0
        ),
        topix_returns AS (
            SELECT
                date,
                {topix_return_exprs}
            FROM topix_lagged
        ),
        stock_returns AS (
            SELECT
                *,
                {return_exprs}
            FROM featured
        ),
        joined AS (
            SELECT
                sr.*,
                tr.* EXCLUDE(date)
            FROM stock_returns sr
            LEFT JOIN topix_returns tr USING (date)
        ),
        final_panel AS (
            SELECT
                *,
                close >= rolling_high_{ROLLING_HIGH_WINDOW}_prev * 0.95 AS near_52w_high,
                close / nullif(sma_{SMA_WINDOW}_prev, 0) >= 1.05 AS above_sma250_105,
                close / nullif(close_lag_{RUNUP_WINDOW}, 0) - 1 >= 0.10 AS runup_20d_10,
                close < open AS bearish_day,
                (prev_close > prev_open AND close < open AND open >= prev_close AND close <= prev_open)
                    AS strict_bearish_engulfing,
                (
                    prev_close > prev_open
                    AND close < open
                    AND body_size >= prev_body_size * 0.8
                    AND open >= prev_open
                    AND close <= prev_close
                ) AS relaxed_body_engulfing,
                (close < open AND high >= prev_high AND low <= prev_low) AS bearish_outside_day,
                CASE
                    WHEN close < open AND atr_{ATR_WINDOW}_prev > 0 THEN body_size / atr_{ATR_WINDOW}_prev
                END AS red_body_atr_ratio,
                close < open AND atr_{ATR_WINDOW}_prev > 0 AND body_size / atr_{ATR_WINDOW}_prev >= 0.8
                    AS large_red_atr_0_8,
                close < open AND atr_{ATR_WINDOW}_prev > 0 AND body_size / atr_{ATR_WINDOW}_prev >= 1.2
                    AS large_red_atr_1_2,
                close < open AND atr_{ATR_WINDOW}_prev > 0 AND body_size / atr_{ATR_WINDOW}_prev >= 1.6
                    AS large_red_atr_1_6,
                high > low AND (close - low) / nullif(high - low, 0) <= 0.25 AS close_near_low,
                volume / nullif(avg_volume_{VOLUME_WINDOW}_prev, 0) AS volume_ratio_20d,
                trading_value / nullif(avg_trading_value_{VOLUME_WINDOW}_prev, 0)
                    AS trading_value_ratio_20d,
                {excess_exprs}
            FROM joined
        )
        SELECT *
        FROM final_panel
        {final_where}
        """,
        [*raw_params, *final_params],
    )


def _aggregate_pattern_summary(conn: Any, *, horizons: tuple[int, ...]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for high_zone in HIGH_ZONE_DEFINITIONS:
        for pattern in PATTERN_DEFINITIONS:
            for volume in VOLUME_DEFINITIONS:
                filter_sql = f"({high_zone.expression}) AND ({pattern.expression}) AND ({volume.expression})"
                for horizon in horizons:
                    return_column = f"return_next_open_to_close_{horizon}d"
                    excess_column = f"excess_return_vs_topix_{horizon}d"
                    topix_column = f"topix_return_{horizon}d"
                    frame = conn.execute(
                        f"""
                        WITH baseline AS (
                            SELECT
                                market_key,
                                date,
                                avg({return_column}) AS baseline_return,
                                avg({excess_column}) AS baseline_excess_return
                            FROM high_zone_bearish_price_action_panel
                            WHERE ({high_zone.expression})
                              AND {return_column} IS NOT NULL
                            GROUP BY market_key, date
                        )
                        SELECT
                            p.market_key,
                            ? AS market_label,
                            ? AS high_zone_key,
                            ? AS high_zone_label,
                            ? AS pattern_key,
                            ? AS pattern_label,
                            ? AS volume_key,
                            ? AS volume_label,
                            ? AS horizon_days,
                            count(*) AS event_count,
                            count(DISTINCT p.code) AS unique_code_count,
                            count(DISTINCT p.date) AS active_date_count,
                            avg(p.{return_column}) AS mean_forward_return,
                            median(p.{return_column}) AS median_forward_return,
                            quantile_cont(p.{return_column}, 0.1) AS p10_forward_return,
                            quantile_cont(p.{return_column}, 0.9) AS p90_forward_return,
                            avg(CASE WHEN p.{return_column} < 0 THEN 1.0 ELSE 0.0 END) AS negative_return_rate,
                            avg(CASE WHEN p.{return_column} <= -0.05 THEN 1.0 ELSE 0.0 END) AS loss_5pct_rate,
                            avg(p.{excess_column}) AS mean_excess_return_vs_topix,
                            avg(p.{topix_column}) AS mean_topix_return,
                            avg(p.{return_column} - b.baseline_return)
                                AS mean_lift_vs_same_market_high_zone_day,
                            avg(p.{excess_column} - b.baseline_excess_return)
                                AS mean_excess_lift_vs_same_market_high_zone_day,
                            avg(p.red_body_atr_ratio) AS mean_red_body_atr_ratio,
                            avg(p.volume_ratio_20d) AS mean_volume_ratio_20d,
                            avg(p.trading_value_ratio_20d) AS mean_trading_value_ratio_20d
                        FROM high_zone_bearish_price_action_panel p
                        JOIN baseline b ON b.market_key = p.market_key AND b.date = p.date
                        WHERE {filter_sql}
                          AND p.{return_column} IS NOT NULL
                        GROUP BY p.market_key
                        """,
                        [
                            "",
                            high_zone.key,
                            high_zone.label,
                            pattern.key,
                            pattern.label,
                            volume.key,
                            volume.label,
                            horizon,
                        ],
                    ).fetchdf()
                    if frame.empty:
                        continue
                    frame["market_label"] = frame["market_key"].map(MARKET_LABELS)
                    frames.append(frame)
    if not frames:
        return pd.DataFrame()
    summary = pd.concat(frames, ignore_index=True)
    baseline = summary.loc[
        summary["pattern_key"].eq("high_zone_all"),
        [
            "market_key",
            "high_zone_key",
            "volume_key",
            "horizon_days",
            "event_count",
            "mean_forward_return",
            "mean_excess_return_vs_topix",
        ],
    ].rename(
        columns={
            "event_count": "baseline_event_count",
            "mean_forward_return": "baseline_mean_forward_return",
            "mean_excess_return_vs_topix": "baseline_mean_excess_return_vs_topix",
        }
    )
    summary = summary.merge(
        baseline,
        on=["market_key", "high_zone_key", "volume_key", "horizon_days"],
        how="left",
    )
    baseline_event_count = summary["baseline_event_count"].where(
        summary["baseline_event_count"] != 0
    )
    summary["event_share_of_high_zone_baseline"] = (
        summary["event_count"] / baseline_event_count
    )
    summary["mean_lift_vs_high_zone_baseline"] = (
        summary["mean_forward_return"] - summary["baseline_mean_forward_return"]
    )
    summary["mean_excess_lift_vs_high_zone_baseline"] = (
        summary["mean_excess_return_vs_topix"]
        - summary["baseline_mean_excess_return_vs_topix"]
    )
    return _sort_table(summary)


def _build_universe_summary(conn: Any) -> pd.DataFrame:
    frame = conn.execute(
        """
        SELECT
            market_key,
            count(DISTINCT code) AS stock_count,
            count(*) AS stock_day_count,
            count(DISTINCT date) AS analysis_date_count,
            sum(CASE WHEN near_52w_high THEN 1 ELSE 0 END) AS near_52w_high_count,
            sum(CASE WHEN above_sma250_105 THEN 1 ELSE 0 END) AS above_sma250_105_count,
            sum(CASE WHEN runup_20d_10 THEN 1 ELSE 0 END) AS runup_20d_10_count,
            sum(CASE WHEN near_52w_high OR above_sma250_105 OR runup_20d_10 THEN 1 ELSE 0 END)
                AS any_high_zone_count,
            sum(CASE WHEN strict_bearish_engulfing THEN 1 ELSE 0 END) AS strict_bearish_engulfing_count,
            sum(CASE WHEN large_red_atr_1_2 THEN 1 ELSE 0 END) AS large_red_atr_1_2_count
        FROM high_zone_bearish_price_action_panel
        GROUP BY market_key
        """
    ).fetchdf()
    if frame.empty:
        return frame
    frame["market_label"] = frame["market_key"].map(MARKET_LABELS)
    for column in (
        "near_52w_high_count",
        "above_sma250_105_count",
        "runup_20d_10_count",
        "any_high_zone_count",
        "strict_bearish_engulfing_count",
        "large_red_atr_1_2_count",
    ):
        frame[column.replace("_count", "_rate")] = (
            frame[column] / frame["stock_day_count"].replace({0: pd.NA})
        )
    return _sort_table(frame)


def _build_top_negative_patterns(
    pattern_summary_df: pd.DataFrame,
    *,
    min_events_for_selection: int,
) -> pd.DataFrame:
    if pattern_summary_df.empty:
        return pattern_summary_df.copy()
    scoped = pattern_summary_df.loc[
        pattern_summary_df["high_zone_key"].eq("any_high_zone")
        & pattern_summary_df["volume_key"].eq("all")
        & pattern_summary_df["horizon_days"].eq(5)
        & ~pattern_summary_df["pattern_key"].isin(["high_zone_all", "bearish_day"])
        & (pattern_summary_df["event_count"] >= min_events_for_selection)
    ].copy()
    if scoped.empty:
        return scoped
    scoped = scoped.sort_values(
        by=[
            "market_key",
            "mean_excess_lift_vs_same_market_high_zone_day",
            "mean_excess_return_vs_topix",
            "event_count",
        ],
        ascending=[True, True, True, False],
        kind="stable",
    )
    scoped["selection_rank"] = scoped.groupby("market_key").cumcount() + 1
    return scoped.loc[scoped["selection_rank"] <= 5].sort_values(
        by=["market_key", "selection_rank"],
        kind="stable",
    ).reset_index(drop=True)


def _build_sampled_events(
    conn: Any,
    *,
    sample_event_size: int,
    horizons: tuple[int, ...],
) -> pd.DataFrame:
    sample_horizon = 5 if 5 in horizons else horizons[0]
    frame = conn.execute(
        f"""
        WITH events AS (
            SELECT
                market_key,
                {sample_horizon} AS sample_horizon_days,
                CASE
                    WHEN strict_bearish_engulfing THEN 'strict_bearish_engulfing'
                    WHEN large_red_atr_1_6 THEN 'large_red_atr_1_6'
                    WHEN large_red_atr_1_2 THEN 'large_red_atr_1_2'
                    WHEN relaxed_body_engulfing THEN 'relaxed_body_engulfing'
                    WHEN bearish_outside_day THEN 'bearish_outside_day'
                    ELSE 'bearish_day'
                END AS primary_pattern_key,
                date,
                code,
                company_name,
                close,
                red_body_atr_ratio,
                volume_ratio_20d,
                trading_value_ratio_20d,
                return_next_open_to_close_{sample_horizon}d AS sample_forward_return,
                excess_return_vs_topix_{sample_horizon}d AS sample_excess_return_vs_topix
            FROM high_zone_bearish_price_action_panel
            WHERE (near_52w_high OR above_sma250_105 OR runup_20d_10)
              AND bearish_day
              AND return_next_open_to_close_{sample_horizon}d IS NOT NULL
        )
        SELECT *
        FROM events
        """
    ).fetchdf()
    if frame.empty:
        frame["sample_rank"] = pd.Series(dtype="int64")
        return frame
    frame["market_label"] = frame["market_key"].map(MARKET_LABELS)
    return select_deterministic_samples(
        frame,
        sample_size=sample_event_size,
        partition_columns=("market_key", "primary_pattern_key"),
        hash_columns=("market_key", "primary_pattern_key", "date", "code"),
        final_order_columns=("market_key", "primary_pattern_key", "sample_rank", "date", "code"),
    )


def _build_research_bundle_summary_markdown(
    result: HighZoneBearishPriceActionResearchResult,
) -> str:
    lines: list[str] = [
        "# High-Zone Bearish Price Action",
        "",
        "## Snapshot",
        "",
        f"- Analysis range: `{result.analysis_start_date} -> {result.analysis_end_date}`",
        f"- Available range: `{result.available_start_date} -> {result.available_end_date}`",
        f"- Markets: `{', '.join(MARKET_LABELS[key] for key in MARKET_ORDER)}`",
        f"- Membership mode: `{result.membership_mode}`",
        f"- Horizons: `{', '.join(str(value) for value in result.horizons)}` trading days",
        "",
        "## Universe Coverage",
        "",
    ]
    if result.universe_summary_df.empty:
        lines.append("- No universe rows were generated.")
    else:
        for row in result.universe_summary_df.itertuples(index=False):
            lines.append(
                f"- `{row.market_label}`: stocks=`{row.stock_count}`, stock-days=`{row.stock_day_count}`, "
                f"high-zone days=`{row.any_high_zone_count}`, strict engulfing days=`{row.strict_bearish_engulfing_count}`, "
                f"large red ATR>=1.2 days=`{row.large_red_atr_1_2_count}`"
            )
    lines.extend(["", "## Top Negative Patterns", ""])
    if result.top_negative_patterns_df.empty:
        lines.append("- No pattern satisfied the minimum event gate.")
    else:
        for row in result.top_negative_patterns_df.itertuples(index=False):
            excess_return = float(cast(Any, row.mean_excess_return_vs_topix))
            same_day_lift = float(
                cast(Any, row.mean_excess_lift_vs_same_market_high_zone_day)
            )
            lines.append(
                f"- `{row.market_label}` rank `{row.selection_rank}`: `{row.pattern_key}` "
                f"events=`{row.event_count}`, 5d excess=`{excess_return * 100:.2f}%`, "
                f"same-day high-zone lift=`{same_day_lift * 100:.2f}%`"
            )
    lines.extend(["", "## Artifact Tables", ""])
    for table_name in TABLE_FIELD_NAMES:
        lines.append(f"- `{table_name}`")
    return "\n".join(lines)


def _split_result_payload(
    result: HighZoneBearishPriceActionResearchResult,
) -> tuple[dict[str, Any], dict[str, pd.DataFrame]]:
    metadata_fields = {field.name for field in fields(result)} - set(TABLE_FIELD_NAMES)
    metadata = {field_name: getattr(result, field_name) for field_name in sorted(metadata_fields)}
    tables = {
        field_name: cast(pd.DataFrame, getattr(result, field_name))
        for field_name in TABLE_FIELD_NAMES
    }
    return metadata, tables


def run_high_zone_bearish_price_action_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    lookback_years: int = DEFAULT_LOOKBACK_YEARS,
    horizons: tuple[int, ...] | list[int] | None = None,
    sample_event_size: int = DEFAULT_SAMPLE_EVENT_SIZE,
    min_events_for_selection: int = DEFAULT_MIN_EVENTS_FOR_SELECTION,
) -> HighZoneBearishPriceActionResearchResult:
    normalized_horizons = _normalize_horizons(horizons)
    with open_readonly_analysis_connection(
        db_path,
        snapshot_prefix="high-zone-bearish-price-action-",
    ) as context:
        conn = context.connection
        available_start_date, available_end_date = fetch_date_range(conn, table_name="stock_data")
        default_start_date = _default_start_date(
            available_start_date=available_start_date,
            available_end_date=available_end_date,
            lookback_years=lookback_years,
        )
        analysis_start_date = start_date or default_start_date
        analysis_end_date = end_date or available_end_date
        raw_start_date = _warmup_start_date(analysis_start_date, available_start_date)
        _create_feature_panel(
            conn,
            raw_start_date=raw_start_date,
            analysis_start_date=analysis_start_date,
            analysis_end_date=analysis_end_date,
            horizons=normalized_horizons,
        )
        universe_summary_df = _build_universe_summary(conn)
        pattern_summary_df = _aggregate_pattern_summary(conn, horizons=normalized_horizons)
        top_negative_patterns_df = _build_top_negative_patterns(
            pattern_summary_df,
            min_events_for_selection=min_events_for_selection,
        )
        sampled_events_df = _build_sampled_events(
            conn,
            sample_event_size=sample_event_size,
            horizons=normalized_horizons,
        )
        return HighZoneBearishPriceActionResearchResult(
            db_path=db_path,
            source_mode=context.source_mode,
            source_detail=context.source_detail,
            available_start_date=available_start_date,
            available_end_date=available_end_date,
            default_start_date=default_start_date,
            analysis_start_date=analysis_start_date,
            analysis_end_date=analysis_end_date,
            lookback_years=lookback_years,
            horizons=normalized_horizons,
            sample_event_size=sample_event_size,
            min_events_for_selection=min_events_for_selection,
            membership_mode=MEMBERSHIP_MODE,
            universe_summary_df=universe_summary_df,
            pattern_summary_df=pattern_summary_df,
            top_negative_patterns_df=top_negative_patterns_df,
            sampled_events_df=sampled_events_df,
        )


def write_high_zone_bearish_price_action_research_bundle(
    result: HighZoneBearishPriceActionResearchResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    result_metadata, result_tables = _split_result_payload(result)
    return write_research_bundle(
        experiment_id=HIGH_ZONE_BEARISH_PRICE_ACTION_EXPERIMENT_ID,
        module=__name__,
        function="run_high_zone_bearish_price_action_research",
        params={
            "lookback_years": result.lookback_years,
            "horizons": list(result.horizons),
            "sample_event_size": result.sample_event_size,
            "min_events_for_selection": result.min_events_for_selection,
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


def load_high_zone_bearish_price_action_research_bundle(
    bundle_path: str | Path,
) -> HighZoneBearishPriceActionResearchResult:
    info = load_research_bundle_info(bundle_path)
    tables = load_research_bundle_tables(bundle_path)
    metadata = dict(info.result_metadata)
    return HighZoneBearishPriceActionResearchResult(
        db_path=str(metadata["db_path"]),
        source_mode=cast(SourceMode, metadata["source_mode"]),
        source_detail=str(metadata["source_detail"]),
        available_start_date=cast(str | None, metadata.get("available_start_date")),
        available_end_date=cast(str | None, metadata.get("available_end_date")),
        default_start_date=cast(str | None, metadata.get("default_start_date")),
        analysis_start_date=cast(str | None, info.analysis_start_date),
        analysis_end_date=cast(str | None, info.analysis_end_date),
        lookback_years=int(metadata["lookback_years"]),
        horizons=tuple(int(value) for value in metadata["horizons"]),
        sample_event_size=int(metadata["sample_event_size"]),
        min_events_for_selection=int(metadata["min_events_for_selection"]),
        membership_mode=str(metadata["membership_mode"]),
        universe_summary_df=tables["universe_summary_df"],
        pattern_summary_df=tables["pattern_summary_df"],
        top_negative_patterns_df=tables["top_negative_patterns_df"],
        sampled_events_df=tables["sampled_events_df"],
    )
