"""
Speculative price-and-volume surge follow-on research.

This study defines a primary episode as a daily stock event where:

- close >= previous close * (1 + price threshold)
- volume >= trailing average volume * (volume-ratio threshold)

It then asks whether the initial leg extension from the day-before base close
helps explain whether a later "follow-on" breakout appears after a gap.

The main outputs intentionally separate:

- descriptive amplitude summaries
- tradeable-proxy summaries that only use the first 0/1/3/5 sessions
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

import pandas as pd

from src.domains.analytics.deterministic_sampling import select_deterministic_samples
from src.domains.analytics.jpx_daily_price_limits import (
    JPX_DAILY_PRICE_LIMITS_REFERENCE_LABEL,
    build_standard_daily_limit_width_case_sql,
)
from src.domains.analytics.readonly_duckdb_support import (
    SourceMode,
    _connect_duckdb as _shared_connect_duckdb,
    fetch_date_range,
    normalize_code_sql,
    open_readonly_analysis_connection,
)
from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    find_latest_research_bundle_path,
    get_research_bundle_dir,
    load_dataclass_research_bundle,
    write_dataclass_research_bundle,
)
from src.domains.analytics.topix_rank_future_close_core import _default_start_date

CohortKey = Literal[
    "surge_price_and_volume",
    "surge_price_only",
    "surge_volume_only",
]

SPECULATIVE_VOLUME_SURGE_FOLLOW_ON_EXPERIMENT_ID = (
    "market-behavior/speculative-volume-surge-follow-on"
)
COHORT_ORDER: tuple[CohortKey, ...] = (
    "surge_price_and_volume",
    "surge_price_only",
    "surge_volume_only",
)
COHORT_LABEL_MAP: dict[CohortKey, str] = {
    "surge_price_and_volume": "Price >= threshold and volume >= threshold",
    "surge_price_only": "Price >= threshold only",
    "surge_volume_only": "Volume >= threshold only",
}
PRIMARY_COHORT_KEY: CohortKey = "surge_price_and_volume"
DEFAULT_LOOKBACK_YEARS = 10
DEFAULT_PRICE_JUMP_THRESHOLD = 0.10
DEFAULT_VOLUME_RATIO_THRESHOLD = 10.0
DEFAULT_VOLUME_WINDOW = 20
DEFAULT_ADV_WINDOW = 20
DEFAULT_EXTENSION_WINDOWS: tuple[int, ...] = (0, 1, 3, 5)
DEFAULT_FULL_EXTENSION_WINDOWS: tuple[int, ...] = (20, 60)
DEFAULT_FOLLOW_ON_GAPS: tuple[int, ...] = (5, 10, 20)
DEFAULT_FOLLOW_ON_WINDOWS: tuple[int, ...] = (20, 40, 60)
DEFAULT_COOLDOWN_SESSIONS = 20
DEFAULT_SAMPLE_SIZE = 8
DEFAULT_PRIMARY_EXTENSION_WINDOW = 5
DEFAULT_PRIMARY_GAP = 10
DEFAULT_PRIMARY_FOLLOW_ON_WINDOW = 40
EXTENSION_BUCKET_ORDER: tuple[str, ...] = (
    "10-20%",
    "20-35%",
    "35-50%",
    "50-100%",
    "100%+",
    "missing",
)
ADV_BUCKET_ORDER: tuple[str, ...] = (
    "<50m",
    "50m-200m",
    "200m-1000m",
    ">=1000m",
    "missing",
)
PRICE_BUCKET_ORDER: tuple[str, ...] = (
    "<100",
    "100-300",
    "300-1000",
    ">=1000",
    "missing",
)
_PREFER_4DIGIT_ORDER_SQL = "CASE WHEN length(code) = 4 THEN 0 ELSE 1 END"
_EPSILON = 1e-6
TABLE_FIELD_NAMES: tuple[str, ...] = (
    "event_ledger_df",
    "bucket_definition_df",
    "follow_on_summary_df",
    "size_liquidity_summary_df",
    "control_cohort_summary_df",
    "top_examples_df",
)
FOLLOW_ON_SUMMARY_COLUMNS: tuple[str, ...] = (
    "cohort_key",
    "cohort_label",
    "extension_window_days",
    "gap_days",
    "follow_on_window_days",
    "follow_on_start_days",
    "follow_on_end_days",
    "extension_bucket",
    "extension_bucket_order",
    "episode_count",
    "breakout_follow_on_count",
    "breakout_follow_on_rate",
    "secondary_surge_count",
    "secondary_surge_rate",
    "mean_initial_extension_pct",
    "median_initial_extension_pct",
    "mean_full_extension_60d_pct",
    "mean_event_close_return_pct",
    "mean_volume_ratio",
    "mean_trading_value_ratio",
    "mean_adv_window_jpy",
    "stop_high_exact_rate",
    "outside_standard_upper_rate",
)
SIZE_LIQUIDITY_SUMMARY_COLUMNS: tuple[str, ...] = (
    "dimension_name",
    "dimension_label",
    "dimension_value",
    "extension_bucket",
    "extension_bucket_order",
    "episode_count",
    "breakout_follow_on_count",
    "breakout_follow_on_rate",
    "secondary_surge_count",
    "secondary_surge_rate",
    "mean_initial_extension_pct",
    "median_full_extension_60d_pct",
)
CONTROL_COHORT_SUMMARY_COLUMNS: tuple[str, ...] = (
    "cohort_key",
    "cohort_label",
    "scope",
    "extension_bucket",
    "extension_bucket_order",
    "episode_count",
    "breakout_follow_on_count",
    "breakout_follow_on_rate",
    "secondary_surge_count",
    "secondary_surge_rate",
    "mean_initial_extension_pct",
    "median_full_extension_60d_pct",
)
TOP_EXAMPLES_COLUMNS: tuple[str, ...] = (
    "example_group",
    "sample_rank",
    "event_date",
    "code",
    "company_name",
    "market_name",
    "scale_category",
    "adv20_bucket",
    "price_bucket",
    "event_close_return_pct",
    "event_high_return_pct",
    "volume_ratio_window",
    "trading_value_ratio_window",
    "initial_extension_5d_pct",
    "full_extension_60d_pct",
    "primary_breakout_pct",
    "primary_breakout_follow_on",
    "primary_secondary_surge",
    "hit_stop_high_exact",
)


@dataclass(frozen=True)
class SpeculativeVolumeSurgeFollowOnResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    default_start_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    lookback_years: int
    price_jump_threshold: float
    volume_ratio_threshold: float
    volume_window: int
    adv_window: int
    cooldown_sessions: int
    extension_windows: tuple[int, ...]
    full_extension_windows: tuple[int, ...]
    follow_on_gaps: tuple[int, ...]
    follow_on_windows: tuple[int, ...]
    primary_extension_window: int
    primary_gap: int
    primary_follow_on_window: int
    sample_size: int
    total_primary_candidate_count: int
    total_primary_episode_count: int
    event_ledger_df: pd.DataFrame
    bucket_definition_df: pd.DataFrame
    follow_on_summary_df: pd.DataFrame
    size_liquidity_summary_df: pd.DataFrame
    control_cohort_summary_df: pd.DataFrame
    top_examples_df: pd.DataFrame


def _connect_duckdb(db_path: str, *, read_only: bool = True) -> Any:
    return _shared_connect_duckdb(db_path, read_only=read_only)


def _open_analysis_connection(db_path: str):
    return open_readonly_analysis_connection(
        db_path,
        snapshot_prefix="speculative-volume-surge-follow-on-",
        connect_fn=_connect_duckdb,
    )


def _empty_df(columns: Sequence[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=list(columns))


def _normalize_int_sequence(
    values: tuple[int, ...] | list[int] | None,
    *,
    fallback: tuple[int, ...],
    name: str,
    allow_zero: bool = False,
) -> tuple[int, ...]:
    if values is None:
        return fallback
    normalized_raw = [int(value) for value in values]
    if allow_zero:
        normalized = tuple(sorted(dict.fromkeys(value for value in normalized_raw if value >= 0)))
    else:
        normalized = tuple(sorted(dict.fromkeys(value for value in normalized_raw if value > 0)))
    if not normalized:
        comparator = "non-negative" if allow_zero else "positive"
        raise ValueError(f"{name} must contain at least one {comparator} integer")
    return normalized


def _safe_optional_date(value: object) -> str | None:
    if value is None or bool(pd.isna(cast(Any, value))):
        return None
    return str(value)


def _coerce_float(value: object) -> float | None:
    if value is None or bool(pd.isna(cast(Any, value))):
        return None
    return float(cast(Any, value))


def _range_alias(start: int, end: int) -> str:
    return f"max_high_{start}_{end}d"


def _future_event_alias(start: int, end: int) -> str:
    return f"future_primary_event_{start}_{end}d"


def _obs_alias(end: int) -> str:
    return f"has_obs_{end}d"


def _breakout_alias(extension_window: int, gap: int, follow_on_window: int) -> str:
    return (
        f"breakout_follow_on_extension{extension_window}d_gap{gap}d_"
        f"window{follow_on_window}d"
    )


def _secondary_alias(gap: int, follow_on_window: int) -> str:
    return f"secondary_surge_gap{gap}d_window{follow_on_window}d"


def _breakout_pct_alias(extension_window: int, gap: int, follow_on_window: int) -> str:
    return (
        f"breakout_pct_extension{extension_window}d_gap{gap}d_"
        f"window{follow_on_window}d"
    )


def _event_ledger_columns(
    *,
    extension_windows: Sequence[int],
    full_extension_windows: Sequence[int],
    follow_on_gaps: Sequence[int],
    follow_on_windows: Sequence[int],
) -> tuple[str, ...]:
    columns: list[str] = [
        "episode_id",
        "event_date",
        "code",
        "company_name",
        "market_code",
        "market_name",
        "sector_33_name",
        "scale_category",
        "session_index",
        "base_close",
        "event_open",
        "event_high",
        "event_low",
        "event_close",
        "event_volume",
        "event_trading_value_jpy",
        "event_close_return_pct",
        "event_high_return_pct",
        "volume_ratio_window",
        "trading_value_ratio_window",
        "adv_window_jpy",
        "prior_20d_return_pct",
        "prior_60d_return_pct",
        "prior_252d_return_pct",
        "price_jump_flag",
        "volume_jump_flag",
        "cohort_key",
        "cohort_label",
        "limit_width",
        "upper_limit",
        "hit_stop_high_exact",
        "above_standard_upper",
        "adv20_bucket",
        "price_bucket",
    ]
    unique_obs = set(full_extension_windows)
    unique_obs.update(end for end in extension_windows if end > 0)
    for extension_window in extension_windows:
        columns.extend(
            [
                _range_alias(0, extension_window),
                f"initial_extension_{extension_window}d_pct",
                f"initial_extension_{extension_window}d_bucket",
            ]
        )
        for gap in follow_on_gaps:
            for follow_on_window in follow_on_windows:
                start = max(extension_window + 1, gap)
                end = start + follow_on_window
                unique_obs.add(end)
                columns.extend(
                    [
                        _range_alias(start, end),
                        _breakout_alias(extension_window, gap, follow_on_window),
                        _breakout_pct_alias(extension_window, gap, follow_on_window),
                    ]
                )
    for full_window in full_extension_windows:
        columns.append(f"full_extension_{full_window}d_pct")
    for gap in follow_on_gaps:
        for follow_on_window in follow_on_windows:
            start = gap
            end = gap + follow_on_window
            columns.extend(
                [
                    _future_event_alias(start, end),
                    _secondary_alias(gap, follow_on_window),
                ]
            )
    for end in sorted(unique_obs):
        if end <= 0:
            continue
        columns.append(_obs_alias(end))
    return tuple(dict.fromkeys(columns))


def _bucket_extension_pct(value: float | None) -> str:
    numeric = _coerce_float(value)
    if numeric is None:
        return "missing"
    if numeric < 0.20:
        return "10-20%"
    if numeric < 0.35:
        return "20-35%"
    if numeric < 0.50:
        return "35-50%"
    if numeric < 1.00:
        return "50-100%"
    return "100%+"


def _bucket_adv20_jpy(value: float | None) -> str:
    numeric = _coerce_float(value)
    if numeric is None or numeric <= 0:
        return "missing"
    if numeric < 50_000_000:
        return "<50m"
    if numeric < 200_000_000:
        return "50m-200m"
    if numeric < 1_000_000_000:
        return "200m-1000m"
    return ">=1000m"


def _bucket_price(value: float | None) -> str:
    numeric = _coerce_float(value)
    if numeric is None or numeric <= 0:
        return "missing"
    if numeric < 100:
        return "<100"
    if numeric < 300:
        return "100-300"
    if numeric < 1000:
        return "300-1000"
    return ">=1000"


def _bool_rate(series: pd.Series) -> float | None:
    scoped = series.dropna()
    if scoped.empty:
        return None
    return float(scoped.astype(bool).mean())


def _mean(series: pd.Series) -> float | None:
    scoped = pd.to_numeric(series, errors="coerce").dropna()
    if scoped.empty:
        return None
    return float(scoped.mean())


def _median(series: pd.Series) -> float | None:
    scoped = pd.to_numeric(series, errors="coerce").dropna()
    if scoped.empty:
        return None
    return float(scoped.median())


def _build_bucket_definition_df() -> pd.DataFrame:
    rows = [
        {
            "bucket_family": "initial_extension_pct",
            "bucket_label": "10-20%",
            "rule": "0.10 <= extension < 0.20",
        },
        {
            "bucket_family": "initial_extension_pct",
            "bucket_label": "20-35%",
            "rule": "0.20 <= extension < 0.35",
        },
        {
            "bucket_family": "initial_extension_pct",
            "bucket_label": "35-50%",
            "rule": "0.35 <= extension < 0.50",
        },
        {
            "bucket_family": "initial_extension_pct",
            "bucket_label": "50-100%",
            "rule": "0.50 <= extension < 1.00",
        },
        {
            "bucket_family": "initial_extension_pct",
            "bucket_label": "100%+",
            "rule": "extension >= 1.00",
        },
        {
            "bucket_family": "adv20_jpy",
            "bucket_label": "<50m",
            "rule": "ADV20 < 50,000,000 JPY",
        },
        {
            "bucket_family": "adv20_jpy",
            "bucket_label": "50m-200m",
            "rule": "50,000,000 <= ADV20 < 200,000,000 JPY",
        },
        {
            "bucket_family": "adv20_jpy",
            "bucket_label": "200m-1000m",
            "rule": "200,000,000 <= ADV20 < 1,000,000,000 JPY",
        },
        {
            "bucket_family": "adv20_jpy",
            "bucket_label": ">=1000m",
            "rule": "ADV20 >= 1,000,000,000 JPY",
        },
        {
            "bucket_family": "event_close_price",
            "bucket_label": "<100",
            "rule": "event close < 100 JPY",
        },
        {
            "bucket_family": "event_close_price",
            "bucket_label": "100-300",
            "rule": "100 <= event close < 300 JPY",
        },
        {
            "bucket_family": "event_close_price",
            "bucket_label": "300-1000",
            "rule": "300 <= event close < 1000 JPY",
        },
        {
            "bucket_family": "event_close_price",
            "bucket_label": ">=1000",
            "rule": "event close >= 1000 JPY",
        },
    ]
    return pd.DataFrame(rows)


def _query_candidate_event_rows(
    conn: Any,
    *,
    analysis_start_date: str | None,
    analysis_end_date: str | None,
    price_jump_threshold: float,
    volume_ratio_threshold: float,
    volume_window: int,
    adv_window: int,
    extension_windows: Sequence[int],
    full_extension_windows: Sequence[int],
    follow_on_gaps: Sequence[int],
    follow_on_windows: Sequence[int],
) -> pd.DataFrame:
    normalized_code_sql = normalize_code_sql("code")
    limit_width_sql = build_standard_daily_limit_width_case_sql("prev_close")
    unique_initial_ends = sorted(set(extension_windows).union(full_extension_windows))
    follow_on_ranges = sorted(
        {
            (max(extension_window + 1, gap), max(extension_window + 1, gap) + follow_on_window)
            for extension_window in extension_windows
            for gap in follow_on_gaps
            for follow_on_window in follow_on_windows
        }
    )
    secondary_ranges = sorted(
        {
            (gap, gap + follow_on_window)
            for gap in follow_on_gaps
            for follow_on_window in follow_on_windows
        }
    )
    observation_ends = sorted(
        {
            *[end for end in unique_initial_ends if end > 0],
            *[end for _, end in follow_on_ranges],
        }
    )
    initial_max_clauses = [
        (
            f"MAX(sd.high) OVER (PARTITION BY sd.code ORDER BY sd.date "
            f"ROWS BETWEEN CURRENT ROW AND {end} FOLLOWING) AS {_range_alias(0, end)}"
        )
        for end in unique_initial_ends
    ]
    follow_on_max_clauses = [
        (
            f"MAX(sd.high) OVER (PARTITION BY sd.code ORDER BY sd.date "
            f"ROWS BETWEEN {start} FOLLOWING AND {end} FOLLOWING) AS {_range_alias(start, end)}"
        )
        for start, end in follow_on_ranges
    ]
    observation_clauses = [
        (
            "CASE WHEN LEAD(sd.date, {end}) OVER (PARTITION BY sd.code ORDER BY sd.date) "
            "IS NOT NULL THEN TRUE ELSE FALSE END AS {alias}"
        ).format(end=end, alias=_obs_alias(end))
        for end in observation_ends
    ]
    future_event_clauses = [
        (
            f"MAX(CASE WHEN flagged.primary_event_flag THEN 1 ELSE 0 END) OVER "
            f"(PARTITION BY flagged.code ORDER BY flagged.date "
            f"ROWS BETWEEN {start} FOLLOWING AND {end} FOLLOWING) AS "
            f"{_future_event_alias(start, end)}"
        )
        for start, end in secondary_ranges
    ]
    analysis_conditions = [
        "cohort_key IS NOT NULL",
        f"volume_obs_window >= {volume_window}",
        f"adv_obs_window >= {adv_window}",
    ]
    analysis_params: list[str] = []
    if analysis_start_date is not None:
        analysis_conditions.append("flagged.date >= ?")
        analysis_params.append(analysis_start_date)
    if analysis_end_date is not None:
        analysis_conditions.append("flagged.date <= ?")
        analysis_params.append(analysis_end_date)
    analysis_where_sql = " WHERE " + " AND ".join(analysis_conditions)
    sql = f"""
        WITH stocks_snapshot AS (
            SELECT
                normalized_code,
                company_name,
                market_code,
                market_name,
                sector_33_name,
                scale_category
            FROM (
                SELECT
                    {normalized_code_sql} AS normalized_code,
                    COALESCE(NULLIF(trim(company_name), ''), {normalized_code_sql}) AS company_name,
                    NULLIF(trim(market_code), '') AS market_code,
                    NULLIF(trim(market_name), '') AS market_name,
                    NULLIF(trim(sector_33_name), '') AS sector_33_name,
                    NULLIF(trim(scale_category), '') AS scale_category,
                    ROW_NUMBER() OVER (
                        PARTITION BY {normalized_code_sql}
                        ORDER BY {_PREFER_4DIGIT_ORDER_SQL}, code
                    ) AS row_priority
                FROM stocks
            )
            WHERE row_priority = 1
        ),
        stock_daily AS (
            SELECT
                date,
                normalized_code AS code,
                open,
                high,
                low,
                close,
                volume
            FROM (
                SELECT
                    date,
                    {normalized_code_sql} AS normalized_code,
                    CAST(open AS DOUBLE) AS open,
                    CAST(high AS DOUBLE) AS high,
                    CAST(low AS DOUBLE) AS low,
                    CAST(close AS DOUBLE) AS close,
                    CAST(volume AS DOUBLE) AS volume,
                    ROW_NUMBER() OVER (
                        PARTITION BY {normalized_code_sql}, date
                        ORDER BY {_PREFER_4DIGIT_ORDER_SQL}, code
                    ) AS row_priority
                FROM stock_data
            )
            WHERE row_priority = 1
              AND open IS NOT NULL
              AND high IS NOT NULL
              AND low IS NOT NULL
              AND close IS NOT NULL
              AND volume IS NOT NULL
        ),
        feature_base AS (
            SELECT
                sd.date,
                sd.code,
                COALESCE(snapshot.company_name, sd.code) AS company_name,
                COALESCE(snapshot.market_code, 'unknown') AS market_code,
                COALESCE(snapshot.market_name, 'UNKNOWN') AS market_name,
                COALESCE(snapshot.sector_33_name, 'UNKNOWN') AS sector_33_name,
                COALESCE(snapshot.scale_category, 'unknown') AS scale_category,
                ROW_NUMBER() OVER (PARTITION BY sd.code ORDER BY sd.date) - 1 AS session_index,
                sd.open,
                sd.high,
                sd.low,
                sd.close,
                sd.volume,
                LAG(sd.close) OVER (PARTITION BY sd.code ORDER BY sd.date) AS prev_close,
                AVG(sd.volume) OVER (
                    PARTITION BY sd.code ORDER BY sd.date
                    ROWS BETWEEN {volume_window} PRECEDING AND 1 PRECEDING
                ) AS volume_avg_window,
                COUNT(sd.volume) OVER (
                    PARTITION BY sd.code ORDER BY sd.date
                    ROWS BETWEEN {volume_window} PRECEDING AND 1 PRECEDING
                ) AS volume_obs_window,
                AVG(sd.close * sd.volume) OVER (
                    PARTITION BY sd.code ORDER BY sd.date
                    ROWS BETWEEN {volume_window} PRECEDING AND 1 PRECEDING
                ) AS trading_value_avg_window,
                AVG(sd.close * sd.volume) OVER (
                    PARTITION BY sd.code ORDER BY sd.date
                    ROWS BETWEEN {adv_window} PRECEDING AND 1 PRECEDING
                ) AS adv_window_jpy_raw,
                COUNT(sd.close * sd.volume) OVER (
                    PARTITION BY sd.code ORDER BY sd.date
                    ROWS BETWEEN {adv_window} PRECEDING AND 1 PRECEDING
                ) AS adv_obs_window,
                LAG(sd.close, 20) OVER (PARTITION BY sd.code ORDER BY sd.date) AS close_lag_20d,
                LAG(sd.close, 60) OVER (PARTITION BY sd.code ORDER BY sd.date) AS close_lag_60d,
                LAG(sd.close, 252) OVER (PARTITION BY sd.code ORDER BY sd.date) AS close_lag_252d,
                {", ".join([*initial_max_clauses, *follow_on_max_clauses, *observation_clauses])}
            FROM stock_daily sd
            LEFT JOIN stocks_snapshot snapshot
              ON snapshot.normalized_code = sd.code
        ),
        metric_rows AS (
            SELECT
                *,
                {limit_width_sql} AS limit_width,
                CASE
                    WHEN prev_close IS NULL OR prev_close <= 0 THEN NULL
                    ELSE close / prev_close - 1.0
                END AS event_close_return_pct,
                CASE
                    WHEN prev_close IS NULL OR prev_close <= 0 THEN NULL
                    ELSE high / prev_close - 1.0
                END AS event_high_return_pct,
                CASE
                    WHEN volume_obs_window < {volume_window}
                         OR volume_avg_window IS NULL
                         OR volume_avg_window <= 0
                    THEN NULL
                    ELSE volume / volume_avg_window
                END AS volume_ratio_window,
                CASE
                    WHEN volume_obs_window < {volume_window}
                         OR trading_value_avg_window IS NULL
                         OR trading_value_avg_window <= 0
                    THEN NULL
                    ELSE (close * volume) / trading_value_avg_window
                END AS trading_value_ratio_window,
                CASE
                    WHEN adv_obs_window < {adv_window}
                         OR adv_window_jpy_raw IS NULL
                         OR adv_window_jpy_raw <= 0
                    THEN NULL
                    ELSE adv_window_jpy_raw
                END AS adv_window_jpy_effective,
                CASE
                    WHEN close_lag_20d IS NULL OR close_lag_20d <= 0 THEN NULL
                    ELSE close / close_lag_20d - 1.0
                END AS prior_20d_return_pct,
                CASE
                    WHEN close_lag_60d IS NULL OR close_lag_60d <= 0 THEN NULL
                    ELSE close / close_lag_60d - 1.0
                END AS prior_60d_return_pct,
                CASE
                    WHEN close_lag_252d IS NULL OR close_lag_252d <= 0 THEN NULL
                    ELSE close / close_lag_252d - 1.0
                END AS prior_252d_return_pct
            FROM feature_base
        ),
        flagged AS (
            SELECT
                *,
                prev_close + limit_width AS upper_limit,
                CASE
                    WHEN limit_width IS NULL THEN FALSE
                    ELSE ABS(high - (prev_close + limit_width)) <= {_EPSILON}
                END AS hit_stop_high_exact,
                CASE
                    WHEN limit_width IS NULL THEN FALSE
                    ELSE high > prev_close + limit_width + {_EPSILON}
                END AS above_standard_upper,
                CASE WHEN event_close_return_pct >= {price_jump_threshold} THEN TRUE ELSE FALSE END AS price_jump_flag,
                CASE WHEN volume_ratio_window >= {volume_ratio_threshold} THEN TRUE ELSE FALSE END AS volume_jump_flag,
                CASE
                    WHEN event_close_return_pct >= {price_jump_threshold}
                         AND volume_ratio_window >= {volume_ratio_threshold}
                    THEN TRUE
                    ELSE FALSE
                END AS primary_event_flag,
                CASE
                    WHEN event_close_return_pct >= {price_jump_threshold}
                         AND volume_ratio_window >= {volume_ratio_threshold}
                    THEN '{PRIMARY_COHORT_KEY}'
                    WHEN event_close_return_pct >= {price_jump_threshold}
                    THEN 'surge_price_only'
                    WHEN volume_ratio_window >= {volume_ratio_threshold}
                    THEN 'surge_volume_only'
                    ELSE NULL
                END AS cohort_key
            FROM metric_rows
        ),
        final_rows AS (
            SELECT
                flagged.*,
                CASE
                    WHEN cohort_key = '{PRIMARY_COHORT_KEY}'
                    THEN '{COHORT_LABEL_MAP[PRIMARY_COHORT_KEY]}'
                    WHEN cohort_key = 'surge_price_only'
                    THEN '{COHORT_LABEL_MAP["surge_price_only"]}'
                    WHEN cohort_key = 'surge_volume_only'
                    THEN '{COHORT_LABEL_MAP["surge_volume_only"]}'
                    ELSE NULL
                END AS cohort_label,
                {", ".join(future_event_clauses)}
            FROM flagged
        )
        SELECT
            *
        FROM final_rows flagged
        {analysis_where_sql}
        ORDER BY code, date
    """
    return conn.execute(sql, analysis_params).fetchdf()


def _dedupe_candidate_events(
    candidate_df: pd.DataFrame,
    *,
    cooldown_sessions: int,
) -> pd.DataFrame:
    if candidate_df.empty:
        return candidate_df.copy()
    keep_indices: list[int] = []
    ordered = candidate_df.sort_values(by=["code", "session_index", "date"], kind="stable")
    for _, group_df in ordered.groupby("code", sort=False):
        last_kept_session: int | None = None
        for row in group_df.itertuples(index=True):
            session_index = int(cast(Any, row.session_index))
            if last_kept_session is None or session_index - last_kept_session > cooldown_sessions:
                keep_indices.append(cast(int, row.Index))
                last_kept_session = session_index
    deduped = ordered.loc[keep_indices].copy()
    return deduped.sort_values(by=["date", "code"], kind="stable").reset_index(drop=True)


def _enrich_episode_df(
    event_df: pd.DataFrame,
    *,
    extension_windows: Sequence[int],
    full_extension_windows: Sequence[int],
    follow_on_gaps: Sequence[int],
    follow_on_windows: Sequence[int],
) -> pd.DataFrame:
    if event_df.empty:
        return _empty_df(
            _event_ledger_columns(
                extension_windows=extension_windows,
                full_extension_windows=full_extension_windows,
                follow_on_gaps=follow_on_gaps,
                follow_on_windows=follow_on_windows,
            )
        )
    result = event_df.copy()
    result["event_date"] = result["date"].astype(str)
    result["episode_id"] = result["code"].astype(str) + "::" + result["event_date"]
    result = result.rename(
        columns={
            "open": "event_open",
            "high": "event_high",
            "low": "event_low",
            "close": "event_close",
            "volume": "event_volume",
            "adv_window_jpy_effective": "adv_window_jpy",
        }
    )
    result["base_close"] = pd.to_numeric(result["prev_close"], errors="coerce")
    result["event_trading_value_jpy"] = (
        pd.to_numeric(result["event_close"], errors="coerce")
        * pd.to_numeric(result["event_volume"], errors="coerce")
    )
    result["adv20_bucket"] = result["adv_window_jpy"].map(_bucket_adv20_jpy)
    result["price_bucket"] = result["event_close"].map(_bucket_price)
    result["cohort_label"] = result["cohort_key"].map(COHORT_LABEL_MAP).fillna(
        result["cohort_label"]
    )
    for extension_window in extension_windows:
        range_col = _range_alias(0, extension_window)
        extension_col = f"initial_extension_{extension_window}d_pct"
        bucket_col = f"initial_extension_{extension_window}d_bucket"
        result[extension_col] = (
            pd.to_numeric(result[range_col], errors="coerce")
            / result["base_close"]
            - 1.0
        )
        result.loc[result["base_close"].isna() | (result["base_close"] <= 0), extension_col] = pd.NA
        result[bucket_col] = result[extension_col].map(_bucket_extension_pct)
    for full_window in full_extension_windows:
        full_col = f"full_extension_{full_window}d_pct"
        result[full_col] = (
            pd.to_numeric(result[_range_alias(0, full_window)], errors="coerce")
            / result["base_close"]
            - 1.0
        )
        result.loc[result["base_close"].isna() | (result["base_close"] <= 0), full_col] = pd.NA
    for gap in follow_on_gaps:
        for follow_on_window in follow_on_windows:
            secondary_col = _secondary_alias(gap, follow_on_window)
            future_flag_col = _future_event_alias(gap, gap + follow_on_window)
            obs_col = _obs_alias(gap + follow_on_window)
            secondary_flag = pd.Series(pd.NA, index=result.index, dtype="boolean")
            secondary_flag.loc[result[obs_col].fillna(False)] = (
                pd.to_numeric(result[future_flag_col], errors="coerce")
                .fillna(0)
                .gt(0)
                .loc[result[obs_col].fillna(False)]
                .astype("boolean")
            )
            result[secondary_col] = secondary_flag
    for extension_window in extension_windows:
        initial_max_col = _range_alias(0, extension_window)
        for gap in follow_on_gaps:
            for follow_on_window in follow_on_windows:
                start = max(extension_window + 1, gap)
                end = start + follow_on_window
                obs_col = _obs_alias(end)
                future_max_col = _range_alias(start, end)
                breakout_col = _breakout_alias(extension_window, gap, follow_on_window)
                breakout_pct_col = _breakout_pct_alias(extension_window, gap, follow_on_window)
                breakout_flag = pd.Series(pd.NA, index=result.index, dtype="boolean")
                valid_mask = result[obs_col].fillna(False) & result[initial_max_col].notna()
                breakout_flag.loc[valid_mask] = (
                    pd.to_numeric(result[future_max_col], errors="coerce")
                    .gt(pd.to_numeric(result[initial_max_col], errors="coerce"))
                    .loc[valid_mask]
                    .astype("boolean")
                )
                result[breakout_col] = breakout_flag
                result[breakout_pct_col] = (
                    pd.to_numeric(result[future_max_col], errors="coerce")
                    / pd.to_numeric(result[initial_max_col], errors="coerce")
                    - 1.0
                )
                result.loc[~valid_mask, breakout_pct_col] = pd.NA
    ordered_columns = _event_ledger_columns(
        extension_windows=extension_windows,
        full_extension_windows=full_extension_windows,
        follow_on_gaps=follow_on_gaps,
        follow_on_windows=follow_on_windows,
    )
    return result.reindex(columns=ordered_columns)


def _build_follow_on_summary_df(
    event_df: pd.DataFrame,
    *,
    extension_windows: Sequence[int],
    follow_on_gaps: Sequence[int],
    follow_on_windows: Sequence[int],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if event_df.empty:
        return _empty_df(FOLLOW_ON_SUMMARY_COLUMNS)
    for extension_window in extension_windows:
        bucket_col = f"initial_extension_{extension_window}d_bucket"
        initial_extension_col = f"initial_extension_{extension_window}d_pct"
        for gap in follow_on_gaps:
            for follow_on_window in follow_on_windows:
                start = max(extension_window + 1, gap)
                end = start + follow_on_window
                obs_col = _obs_alias(end)
                breakout_col = _breakout_alias(extension_window, gap, follow_on_window)
                secondary_col = _secondary_alias(gap, follow_on_window)
                scoped = event_df.loc[event_df[obs_col].fillna(False)].copy()
                if scoped.empty:
                    continue
                for bucket_label, bucket_df in scoped.groupby(bucket_col, sort=False, dropna=False):
                    bucket_name = "missing" if pd.isna(bucket_label) else str(bucket_label)
                    rows.append(
                        {
                            "cohort_key": PRIMARY_COHORT_KEY,
                            "cohort_label": COHORT_LABEL_MAP[PRIMARY_COHORT_KEY],
                            "extension_window_days": extension_window,
                            "gap_days": gap,
                            "follow_on_window_days": follow_on_window,
                            "follow_on_start_days": start,
                            "follow_on_end_days": end,
                            "extension_bucket": bucket_name,
                            "extension_bucket_order": EXTENSION_BUCKET_ORDER.index(bucket_name)
                            if bucket_name in EXTENSION_BUCKET_ORDER
                            else len(EXTENSION_BUCKET_ORDER),
                            "episode_count": int(len(bucket_df)),
                            "breakout_follow_on_count": int(
                                bucket_df[breakout_col].fillna(False).astype(bool).sum()
                            ),
                            "breakout_follow_on_rate": _bool_rate(bucket_df[breakout_col]),
                            "secondary_surge_count": int(
                                bucket_df[secondary_col].fillna(False).astype(bool).sum()
                            ),
                            "secondary_surge_rate": _bool_rate(bucket_df[secondary_col]),
                            "mean_initial_extension_pct": _mean(bucket_df[initial_extension_col]),
                            "median_initial_extension_pct": _median(bucket_df[initial_extension_col]),
                            "mean_full_extension_60d_pct": _mean(bucket_df["full_extension_60d_pct"]),
                            "mean_event_close_return_pct": _mean(bucket_df["event_close_return_pct"]),
                            "mean_volume_ratio": _mean(bucket_df["volume_ratio_window"]),
                            "mean_trading_value_ratio": _mean(bucket_df["trading_value_ratio_window"]),
                            "mean_adv_window_jpy": _mean(bucket_df["adv_window_jpy"]),
                            "stop_high_exact_rate": _bool_rate(bucket_df["hit_stop_high_exact"]),
                            "outside_standard_upper_rate": _bool_rate(
                                bucket_df["above_standard_upper"]
                            ),
                        }
                    )
    if not rows:
        return _empty_df(FOLLOW_ON_SUMMARY_COLUMNS)
    return pd.DataFrame(rows).sort_values(
        by=[
            "extension_window_days",
            "gap_days",
            "follow_on_window_days",
            "extension_bucket_order",
        ],
        kind="stable",
    ).reset_index(drop=True)


def _build_size_liquidity_summary_df(
    event_df: pd.DataFrame,
    *,
    primary_extension_window: int,
    primary_gap: int,
    primary_follow_on_window: int,
) -> pd.DataFrame:
    if event_df.empty:
        return _empty_df(SIZE_LIQUIDITY_SUMMARY_COLUMNS)
    start = max(primary_extension_window + 1, primary_gap)
    end = start + primary_follow_on_window
    obs_col = _obs_alias(end)
    breakout_col = _breakout_alias(
        primary_extension_window,
        primary_gap,
        primary_follow_on_window,
    )
    secondary_col = _secondary_alias(primary_gap, primary_follow_on_window)
    bucket_col = f"initial_extension_{primary_extension_window}d_bucket"
    scoped = event_df.loc[event_df[obs_col].fillna(False)].copy()
    if scoped.empty:
        return _empty_df(SIZE_LIQUIDITY_SUMMARY_COLUMNS)
    dimension_specs: tuple[tuple[str, str], ...] = (
        ("adv20_bucket", "ADV20 bucket"),
        ("price_bucket", "Event close price bucket"),
        ("market_name", "Latest market"),
        ("scale_category", "Latest scale category"),
        ("hit_stop_high_exact", "Exact stop_high on event day"),
    )
    rows: list[dict[str, Any]] = []
    for dimension_name, dimension_label in dimension_specs:
        dimension_series = scoped[dimension_name].copy()
        if dimension_name == "hit_stop_high_exact":
            dimension_series = dimension_series.map(
                lambda value: "exact_stop_high" if bool(value) else "not_exact_stop_high"
            )
        scoped_dimension = scoped.assign(_dimension_value=dimension_series)
        for (dimension_value, extension_bucket), group_df in scoped_dimension.groupby(
            ["_dimension_value", bucket_col],
            dropna=False,
            sort=False,
        ):
            extension_bucket_name = (
                "missing" if pd.isna(extension_bucket) else str(extension_bucket)
            )
            rows.append(
                {
                    "dimension_name": dimension_name,
                    "dimension_label": dimension_label,
                    "dimension_value": "missing" if pd.isna(dimension_value) else str(dimension_value),
                    "extension_bucket": extension_bucket_name,
                    "extension_bucket_order": EXTENSION_BUCKET_ORDER.index(extension_bucket_name)
                    if extension_bucket_name in EXTENSION_BUCKET_ORDER
                    else len(EXTENSION_BUCKET_ORDER),
                    "episode_count": int(len(group_df)),
                    "breakout_follow_on_count": int(
                        group_df[breakout_col].fillna(False).astype(bool).sum()
                    ),
                    "breakout_follow_on_rate": _bool_rate(group_df[breakout_col]),
                    "secondary_surge_count": int(
                        group_df[secondary_col].fillna(False).astype(bool).sum()
                    ),
                    "secondary_surge_rate": _bool_rate(group_df[secondary_col]),
                    "mean_initial_extension_pct": _mean(
                        group_df[f"initial_extension_{primary_extension_window}d_pct"]
                    ),
                    "median_full_extension_60d_pct": _median(group_df["full_extension_60d_pct"]),
                }
            )
    return pd.DataFrame(rows).sort_values(
        by=["dimension_name", "extension_bucket_order", "dimension_value"],
        kind="stable",
    ).reset_index(drop=True)


def _build_control_cohort_summary_df(
    candidate_df: pd.DataFrame,
    *,
    cooldown_sessions: int,
    extension_windows: Sequence[int],
    full_extension_windows: Sequence[int],
    follow_on_gaps: Sequence[int],
    follow_on_windows: Sequence[int],
    primary_extension_window: int,
    primary_gap: int,
    primary_follow_on_window: int,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    start = max(primary_extension_window + 1, primary_gap)
    end = start + primary_follow_on_window
    obs_col = _obs_alias(end)
    breakout_col = _breakout_alias(
        primary_extension_window,
        primary_gap,
        primary_follow_on_window,
    )
    secondary_col = _secondary_alias(primary_gap, primary_follow_on_window)
    bucket_col = f"initial_extension_{primary_extension_window}d_bucket"
    for cohort_key in COHORT_ORDER:
        cohort_candidates = candidate_df.loc[candidate_df["cohort_key"] == cohort_key].copy()
        cohort_episodes = _dedupe_candidate_events(
            cohort_candidates,
            cooldown_sessions=cooldown_sessions,
        )
        cohort_ledger = _enrich_episode_df(
            cohort_episodes,
            extension_windows=extension_windows,
            full_extension_windows=full_extension_windows,
            follow_on_gaps=follow_on_gaps,
            follow_on_windows=follow_on_windows,
        )
        scoped = cohort_ledger.loc[cohort_ledger[obs_col].fillna(False)].copy()
        if scoped.empty:
            continue
        rows.append(
            {
                "cohort_key": cohort_key,
                "cohort_label": COHORT_LABEL_MAP[cohort_key],
                "scope": "all",
                "extension_bucket": "all",
                "extension_bucket_order": -1,
                "episode_count": int(len(scoped)),
                "breakout_follow_on_count": int(scoped[breakout_col].fillna(False).astype(bool).sum()),
                "breakout_follow_on_rate": _bool_rate(scoped[breakout_col]),
                "secondary_surge_count": int(scoped[secondary_col].fillna(False).astype(bool).sum()),
                "secondary_surge_rate": _bool_rate(scoped[secondary_col]),
                "mean_initial_extension_pct": _mean(
                    scoped[f"initial_extension_{primary_extension_window}d_pct"]
                ),
                "median_full_extension_60d_pct": _median(scoped["full_extension_60d_pct"]),
            }
        )
        for bucket_label, bucket_df in scoped.groupby(bucket_col, sort=False, dropna=False):
            bucket_name = "missing" if pd.isna(bucket_label) else str(bucket_label)
            rows.append(
                {
                    "cohort_key": cohort_key,
                    "cohort_label": COHORT_LABEL_MAP[cohort_key],
                    "scope": "extension_bucket",
                    "extension_bucket": bucket_name,
                    "extension_bucket_order": EXTENSION_BUCKET_ORDER.index(bucket_name)
                    if bucket_name in EXTENSION_BUCKET_ORDER
                    else len(EXTENSION_BUCKET_ORDER),
                    "episode_count": int(len(bucket_df)),
                    "breakout_follow_on_count": int(
                        bucket_df[breakout_col].fillna(False).astype(bool).sum()
                    ),
                    "breakout_follow_on_rate": _bool_rate(bucket_df[breakout_col]),
                    "secondary_surge_count": int(
                        bucket_df[secondary_col].fillna(False).astype(bool).sum()
                    ),
                    "secondary_surge_rate": _bool_rate(bucket_df[secondary_col]),
                    "mean_initial_extension_pct": _mean(
                        bucket_df[f"initial_extension_{primary_extension_window}d_pct"]
                    ),
                    "median_full_extension_60d_pct": _median(
                        bucket_df["full_extension_60d_pct"]
                    ),
                }
            )
    if not rows:
        return _empty_df(CONTROL_COHORT_SUMMARY_COLUMNS)
    return pd.DataFrame(rows).sort_values(
        by=["scope", "extension_bucket_order", "cohort_key"],
        kind="stable",
    ).reset_index(drop=True)


def _build_top_examples_df(
    event_df: pd.DataFrame,
    *,
    primary_extension_window: int,
    primary_gap: int,
    primary_follow_on_window: int,
    sample_size: int,
) -> pd.DataFrame:
    if event_df.empty:
        return _empty_df(TOP_EXAMPLES_COLUMNS)
    start = max(primary_extension_window + 1, primary_gap)
    end = start + primary_follow_on_window
    obs_col = _obs_alias(end)
    breakout_col = _breakout_alias(
        primary_extension_window,
        primary_gap,
        primary_follow_on_window,
    )
    breakout_pct_col = _breakout_pct_alias(
        primary_extension_window,
        primary_gap,
        primary_follow_on_window,
    )
    secondary_col = _secondary_alias(primary_gap, primary_follow_on_window)
    scoped = event_df.loc[event_df[obs_col].fillna(False)].copy()
    if scoped.empty:
        return _empty_df(TOP_EXAMPLES_COLUMNS)
    scoped["example_group"] = scoped[breakout_col].fillna(False).map(
        lambda value: "follow_on_breakout" if bool(value) else "no_follow_on_breakout"
    )
    sampled = select_deterministic_samples(
        scoped,
        sample_size=sample_size,
        partition_columns=["example_group"],
        hash_columns=["code", "event_date"],
        final_order_columns=["example_group", "sample_rank", "event_date", "code"],
    )
    sampled["primary_breakout_follow_on"] = sampled[breakout_col]
    sampled["primary_breakout_pct"] = sampled[breakout_pct_col]
    sampled["primary_secondary_surge"] = sampled[secondary_col]
    initial_extension_col = f"initial_extension_{primary_extension_window}d_pct"
    if initial_extension_col != "initial_extension_5d_pct":
        sampled["initial_extension_5d_pct"] = sampled.get(initial_extension_col)
    if "full_extension_60d_pct" not in sampled.columns:
        sampled["full_extension_60d_pct"] = pd.NA
    return sampled.reindex(columns=TOP_EXAMPLES_COLUMNS)


def _build_research_bundle_summary_markdown(
    result: SpeculativeVolumeSurgeFollowOnResult,
) -> str:
    lines = [
        "# Speculative Volume-Surge Follow-On",
        "",
        f"- Analysis range: `{result.analysis_start_date}` -> `{result.analysis_end_date}`",
        f"- Primary trigger: close jump `>= {result.price_jump_threshold * 100:.1f}%` and volume ratio `>= {result.volume_ratio_threshold:.1f}x`",
        f"- Windows: extension `{','.join(str(v) for v in result.extension_windows)}` / "
        f"full `{','.join(str(v) for v in result.full_extension_windows)}` / "
        f"gap `{','.join(str(v) for v in result.follow_on_gaps)}` / "
        f"follow-on `{','.join(str(v) for v in result.follow_on_windows)}`",
        f"- JPX stop-high reference: `{JPX_DAILY_PRICE_LIMITS_REFERENCE_LABEL}`",
        f"- Primary candidates before cooldown merge: `{result.total_primary_candidate_count}`",
        f"- Primary episodes after `{result.cooldown_sessions}`-session cooldown: `{result.total_primary_episode_count}`",
        "",
        "## Main Read",
    ]
    primary_rows = result.follow_on_summary_df.loc[
        (result.follow_on_summary_df["extension_window_days"] == result.primary_extension_window)
        & (result.follow_on_summary_df["gap_days"] == result.primary_gap)
        & (result.follow_on_summary_df["follow_on_window_days"] == result.primary_follow_on_window)
    ].sort_values(
        by=["breakout_follow_on_rate", "episode_count"],
        ascending=[False, False],
        kind="stable",
    )
    if primary_rows.empty:
        lines.append("- No primary follow-on rows were produced.")
    else:
        for row in primary_rows.head(5).itertuples(index=False):
            breakout_rate = (
                "n/a"
                if row.breakout_follow_on_rate is None
                else f"{float(cast(Any, row.breakout_follow_on_rate)) * 100:.1f}%"
            )
            secondary_rate = (
                "n/a"
                if row.secondary_surge_rate is None
                else f"{float(cast(Any, row.secondary_surge_rate)) * 100:.1f}%"
            )
            lines.append(
                f"- `{row.extension_bucket}`: episodes `{int(cast(Any, row.episode_count))}`, "
                f"breakout follow-on `{breakout_rate}`, secondary-surge `{secondary_rate}`"
            )
    lines.extend(["", "## Control Cohorts"])
    control_rows = result.control_cohort_summary_df.loc[
        result.control_cohort_summary_df["scope"] == "all"
    ]
    if control_rows.empty:
        lines.append("- No control-cohort rows were produced.")
    else:
        for row in control_rows.itertuples(index=False):
            breakout_rate = (
                "n/a"
                if row.breakout_follow_on_rate is None
                else f"{float(cast(Any, row.breakout_follow_on_rate)) * 100:.1f}%"
            )
            lines.append(
                f"- `{row.cohort_key}`: episodes `{int(cast(Any, row.episode_count))}`, "
                f"breakout follow-on `{breakout_rate}`"
            )
    lines.extend(["", "## Caveat"])
    lines.append(
        "- `full_extension_20d/60d` rows are descriptive only. "
        "The more practical read is the breakout rate conditioned on the first 0/1/3/5 sessions."
    )
    return "\n".join(lines) + "\n"


def _build_published_summary(
    result: SpeculativeVolumeSurgeFollowOnResult,
) -> dict[str, Any]:
    primary_rows = result.follow_on_summary_df.loc[
        (result.follow_on_summary_df["extension_window_days"] == result.primary_extension_window)
        & (result.follow_on_summary_df["gap_days"] == result.primary_gap)
        & (result.follow_on_summary_df["follow_on_window_days"] == result.primary_follow_on_window)
    ].sort_values(
        by=["breakout_follow_on_rate", "episode_count"],
        ascending=[False, False],
        kind="stable",
    )
    return {
        "primaryCandidateCount": result.total_primary_candidate_count,
        "primaryEpisodeCount": result.total_primary_episode_count,
        "analysisStartDate": result.analysis_start_date,
        "analysisEndDate": result.analysis_end_date,
        "priceJumpThresholdPct": result.price_jump_threshold * 100.0,
        "volumeRatioThreshold": result.volume_ratio_threshold,
        "primaryConfig": {
            "extensionWindowDays": result.primary_extension_window,
            "gapDays": result.primary_gap,
            "followOnWindowDays": result.primary_follow_on_window,
        },
        "topPrimaryRows": primary_rows.head(5).to_dict(orient="records"),
        "controlRows": result.control_cohort_summary_df.loc[
            result.control_cohort_summary_df["scope"] == "all"
        ].to_dict(orient="records"),
    }


def run_speculative_volume_surge_follow_on_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    lookback_years: int = DEFAULT_LOOKBACK_YEARS,
    price_jump_threshold: float = DEFAULT_PRICE_JUMP_THRESHOLD,
    volume_ratio_threshold: float = DEFAULT_VOLUME_RATIO_THRESHOLD,
    volume_window: int = DEFAULT_VOLUME_WINDOW,
    adv_window: int = DEFAULT_ADV_WINDOW,
    extension_windows: tuple[int, ...] | list[int] | None = None,
    full_extension_windows: tuple[int, ...] | list[int] | None = None,
    follow_on_gaps: tuple[int, ...] | list[int] | None = None,
    follow_on_windows: tuple[int, ...] | list[int] | None = None,
    cooldown_sessions: int = DEFAULT_COOLDOWN_SESSIONS,
    sample_size: int = DEFAULT_SAMPLE_SIZE,
    primary_extension_window: int = DEFAULT_PRIMARY_EXTENSION_WINDOW,
    primary_gap: int = DEFAULT_PRIMARY_GAP,
    primary_follow_on_window: int = DEFAULT_PRIMARY_FOLLOW_ON_WINDOW,
) -> SpeculativeVolumeSurgeFollowOnResult:
    if lookback_years <= 0:
        raise ValueError("lookback_years must be positive")
    if price_jump_threshold <= 0:
        raise ValueError("price_jump_threshold must be positive")
    if volume_ratio_threshold <= 0:
        raise ValueError("volume_ratio_threshold must be positive")
    if volume_window <= 0:
        raise ValueError("volume_window must be positive")
    if adv_window <= 0:
        raise ValueError("adv_window must be positive")
    if cooldown_sessions < 0:
        raise ValueError("cooldown_sessions must be non-negative")
    if sample_size <= 0:
        raise ValueError("sample_size must be positive")

    normalized_extension_windows = _normalize_int_sequence(
        extension_windows,
        fallback=DEFAULT_EXTENSION_WINDOWS,
        name="extension_windows",
        allow_zero=True,
    )
    normalized_full_extension_windows = _normalize_int_sequence(
        full_extension_windows,
        fallback=DEFAULT_FULL_EXTENSION_WINDOWS,
        name="full_extension_windows",
    )
    normalized_follow_on_gaps = _normalize_int_sequence(
        follow_on_gaps,
        fallback=DEFAULT_FOLLOW_ON_GAPS,
        name="follow_on_gaps",
    )
    normalized_follow_on_windows = _normalize_int_sequence(
        follow_on_windows,
        fallback=DEFAULT_FOLLOW_ON_WINDOWS,
        name="follow_on_windows",
    )
    if primary_extension_window not in normalized_extension_windows:
        raise ValueError("primary_extension_window must be included in extension_windows")
    if primary_gap not in normalized_follow_on_gaps:
        raise ValueError("primary_gap must be included in follow_on_gaps")
    if primary_follow_on_window not in normalized_follow_on_windows:
        raise ValueError("primary_follow_on_window must be included in follow_on_windows")

    with _open_analysis_connection(db_path) as ctx:
        conn = ctx.connection
        available_start_date, available_end_date = fetch_date_range(conn, table_name="stock_data")
        default_start_date = _default_start_date(
            available_start_date=available_start_date,
            available_end_date=available_end_date,
            lookback_years=lookback_years,
        )
        analysis_start_date = start_date or default_start_date
        analysis_end_date = end_date or available_end_date
        candidate_df = _query_candidate_event_rows(
            conn,
            analysis_start_date=analysis_start_date,
            analysis_end_date=analysis_end_date,
            price_jump_threshold=price_jump_threshold,
            volume_ratio_threshold=volume_ratio_threshold,
            volume_window=volume_window,
            adv_window=adv_window,
            extension_windows=normalized_extension_windows,
            full_extension_windows=normalized_full_extension_windows,
            follow_on_gaps=normalized_follow_on_gaps,
            follow_on_windows=normalized_follow_on_windows,
        )
        primary_candidates = candidate_df.loc[
            candidate_df["cohort_key"] == PRIMARY_COHORT_KEY
        ].copy()
        primary_episodes = _dedupe_candidate_events(
            primary_candidates,
            cooldown_sessions=cooldown_sessions,
        )
        event_ledger_df = _enrich_episode_df(
            primary_episodes,
            extension_windows=normalized_extension_windows,
            full_extension_windows=normalized_full_extension_windows,
            follow_on_gaps=normalized_follow_on_gaps,
            follow_on_windows=normalized_follow_on_windows,
        )
        bucket_definition_df = _build_bucket_definition_df()
        follow_on_summary_df = _build_follow_on_summary_df(
            event_ledger_df,
            extension_windows=normalized_extension_windows,
            follow_on_gaps=normalized_follow_on_gaps,
            follow_on_windows=normalized_follow_on_windows,
        )
        size_liquidity_summary_df = _build_size_liquidity_summary_df(
            event_ledger_df,
            primary_extension_window=primary_extension_window,
            primary_gap=primary_gap,
            primary_follow_on_window=primary_follow_on_window,
        )
        control_cohort_summary_df = _build_control_cohort_summary_df(
            candidate_df,
            cooldown_sessions=cooldown_sessions,
            extension_windows=normalized_extension_windows,
            full_extension_windows=normalized_full_extension_windows,
            follow_on_gaps=normalized_follow_on_gaps,
            follow_on_windows=normalized_follow_on_windows,
            primary_extension_window=primary_extension_window,
            primary_gap=primary_gap,
            primary_follow_on_window=primary_follow_on_window,
        )
        top_examples_df = _build_top_examples_df(
            event_ledger_df,
            primary_extension_window=primary_extension_window,
            primary_gap=primary_gap,
            primary_follow_on_window=primary_follow_on_window,
            sample_size=sample_size,
        )
        return SpeculativeVolumeSurgeFollowOnResult(
            db_path=db_path,
            source_mode=cast(SourceMode, ctx.source_mode),
            source_detail=str(ctx.source_detail),
            available_start_date=_safe_optional_date(available_start_date),
            available_end_date=_safe_optional_date(available_end_date),
            default_start_date=_safe_optional_date(default_start_date),
            analysis_start_date=_safe_optional_date(analysis_start_date),
            analysis_end_date=_safe_optional_date(analysis_end_date),
            lookback_years=lookback_years,
            price_jump_threshold=price_jump_threshold,
            volume_ratio_threshold=volume_ratio_threshold,
            volume_window=volume_window,
            adv_window=adv_window,
            cooldown_sessions=cooldown_sessions,
            extension_windows=normalized_extension_windows,
            full_extension_windows=normalized_full_extension_windows,
            follow_on_gaps=normalized_follow_on_gaps,
            follow_on_windows=normalized_follow_on_windows,
            primary_extension_window=primary_extension_window,
            primary_gap=primary_gap,
            primary_follow_on_window=primary_follow_on_window,
            sample_size=sample_size,
            total_primary_candidate_count=int(len(primary_candidates)),
            total_primary_episode_count=int(len(event_ledger_df)),
            event_ledger_df=event_ledger_df,
            bucket_definition_df=bucket_definition_df,
            follow_on_summary_df=follow_on_summary_df,
            size_liquidity_summary_df=size_liquidity_summary_df,
            control_cohort_summary_df=control_cohort_summary_df,
            top_examples_df=top_examples_df,
        )


def write_speculative_volume_surge_follow_on_research_bundle(
    result: SpeculativeVolumeSurgeFollowOnResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=SPECULATIVE_VOLUME_SURGE_FOLLOW_ON_EXPERIMENT_ID,
        module=__name__,
        function="run_speculative_volume_surge_follow_on_research",
        params={
            "lookback_years": result.lookback_years,
            "price_jump_threshold": result.price_jump_threshold,
            "volume_ratio_threshold": result.volume_ratio_threshold,
            "volume_window": result.volume_window,
            "adv_window": result.adv_window,
            "cooldown_sessions": result.cooldown_sessions,
            "extension_windows": result.extension_windows,
            "full_extension_windows": result.full_extension_windows,
            "follow_on_gaps": result.follow_on_gaps,
            "follow_on_windows": result.follow_on_windows,
            "primary_extension_window": result.primary_extension_window,
            "primary_gap": result.primary_gap,
            "primary_follow_on_window": result.primary_follow_on_window,
            "sample_size": result.sample_size,
        },
        result=result,
        table_field_names=TABLE_FIELD_NAMES,
        summary_markdown=_build_research_bundle_summary_markdown(result),
        published_summary=_build_published_summary(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_speculative_volume_surge_follow_on_research_bundle(
    bundle_path: str | Path,
) -> SpeculativeVolumeSurgeFollowOnResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=SpeculativeVolumeSurgeFollowOnResult,
        table_field_names=TABLE_FIELD_NAMES,
    )


def get_speculative_volume_surge_follow_on_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        SPECULATIVE_VOLUME_SURGE_FOLLOW_ON_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_speculative_volume_surge_follow_on_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        SPECULATIVE_VOLUME_SURGE_FOLLOW_ON_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )
