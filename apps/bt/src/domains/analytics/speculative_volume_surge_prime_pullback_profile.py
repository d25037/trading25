"""
Prime-only speculative volume-surge pullback profile research.

This study treats prime-market surge episodes as a separate family from the
more speculative standard/growth names. It asks a narrower question:

- Within 20 sessions after the initial surge, does the episode make only a
  shallow pullback or a deep pullback before reclaiming the initial peak?
- When one deepest-pullback label is assigned per episode, which pullback
  family keeps the better close return and upside/downside asymmetry?

The pullback label is descriptive, not tradeable as-is: it is the deepest close
observed in the search window before any reclaim above the initial peak.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pandas as pd

from src.domains.analytics.readonly_duckdb_support import (
    SourceMode,
    _connect_duckdb as _shared_connect_duckdb,
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
from src.domains.analytics.speculative_volume_surge_follow_on import (
    DEFAULT_ADV_WINDOW,
    DEFAULT_COOLDOWN_SESSIONS,
    DEFAULT_LOOKBACK_YEARS,
    DEFAULT_PRICE_JUMP_THRESHOLD,
    DEFAULT_VOLUME_RATIO_THRESHOLD,
    DEFAULT_VOLUME_WINDOW,
    SpeculativeVolumeSurgeFollowOnResult,
    run_speculative_volume_surge_follow_on_research,
)

SPECULATIVE_VOLUME_SURGE_PRIME_PULLBACK_PROFILE_EXPERIMENT_ID = (
    "market-behavior/speculative-volume-surge-prime-pullback-profile"
)
PRIME_MARKET_NAME = "プライム"
DEFAULT_INITIAL_PEAK_WINDOW = 5
DEFAULT_PULLBACK_SEARCH_WINDOW = 20
DEFAULT_FUTURE_HORIZONS: tuple[int, ...] = (20, 40, 60)
DEFAULT_PRIMARY_HORIZON = 20
DEFAULT_SAMPLE_SIZE = 8
PULLBACK_BUCKET_ORDER: tuple[str, ...] = (
    "<0%",
    "0-10%",
    "10-20%",
    "20-35%",
    "35%+",
    "missing",
)
PRIME_EPISODE_COLUMNS: tuple[str, ...] = (
    "episode_id",
    "code",
    "company_name",
    "event_date",
    "base_close",
    "market_name",
    "scale_category",
    "adv20_bucket",
    "price_bucket",
)
PULLBACK_BUCKET_SUMMARY_COLUMNS: tuple[str, ...] = (
    "future_horizon_days",
    "pullback_bucket",
    "pullback_bucket_order",
    "observation_count",
    "mean_deepest_pullback_position_pct",
    "median_deepest_pullback_position_pct",
    "mean_peak_to_pullback_drawdown_pct",
    "mean_future_max_upside_pct",
    "median_future_max_upside_pct",
    "mean_future_max_downside_pct",
    "median_future_max_downside_pct",
    "mean_future_close_return_pct",
    "median_future_close_return_pct",
    "mean_asymmetry_pct",
    "median_asymmetry_pct",
    "mean_upside_to_downside_ratio",
    "positive_close_return_rate",
    "upside_gt_downside_rate",
    "peak_reclaim_rate",
)
ADV_BUCKET_SUMMARY_COLUMNS: tuple[str, ...] = (
    "adv20_bucket",
    "pullback_bucket",
    "pullback_bucket_order",
    "observation_count",
    "median_future_close_return_pct",
    "median_asymmetry_pct",
    "positive_close_return_rate",
    "upside_gt_downside_rate",
    "peak_reclaim_rate",
)
TOP_EXAMPLES_COLUMNS: tuple[str, ...] = (
    "example_group",
    "episode_id",
    "code",
    "company_name",
    "event_date",
    "adv20_bucket",
    "price_bucket",
    "initial_peak_offset",
    "initial_peak_price",
    "deepest_pullback_offset",
    "deepest_pullback_date",
    "deepest_pullback_bucket",
    "deepest_pullback_position_pct",
    "peak_to_pullback_drawdown_pct",
    "future_close_return_primary_pct",
    "asymmetry_primary_pct",
    "future_max_upside_primary_pct",
    "future_max_downside_primary_pct",
    "peak_reclaim_primary",
)
TABLE_FIELD_NAMES: tuple[str, ...] = (
    "prime_episode_df",
    "bucket_definition_df",
    "prime_pullback_profile_df",
    "pullback_bucket_summary_df",
    "adv_bucket_summary_df",
    "top_examples_df",
)
_PREFER_4DIGIT_ORDER_SQL = "CASE WHEN length(code) = 4 THEN 0 ELSE 1 END"
_EPSILON = 1e-9


@dataclass(frozen=True)
class PullbackBucketSpec:
    label: str
    lower_bound: float | None
    upper_bound: float | None


PULLBACK_BUCKET_SPECS: tuple[PullbackBucketSpec, ...] = (
    PullbackBucketSpec(label="<0%", lower_bound=None, upper_bound=0.0),
    PullbackBucketSpec(label="0-10%", lower_bound=0.0, upper_bound=0.10),
    PullbackBucketSpec(label="10-20%", lower_bound=0.10, upper_bound=0.20),
    PullbackBucketSpec(label="20-35%", lower_bound=0.20, upper_bound=0.35),
    PullbackBucketSpec(label="35%+", lower_bound=0.35, upper_bound=None),
)


@dataclass(frozen=True)
class SpeculativeVolumeSurgePrimePullbackProfileResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    market_name: str
    lookback_years: int
    price_jump_threshold: float
    volume_ratio_threshold: float
    volume_window: int
    adv_window: int
    cooldown_sessions: int
    initial_peak_window: int
    pullback_search_window: int
    future_horizons: tuple[int, ...]
    primary_horizon: int
    sample_size: int
    total_prime_episode_count: int
    prime_pullback_profile_count: int
    prime_episode_df: pd.DataFrame
    bucket_definition_df: pd.DataFrame
    prime_pullback_profile_df: pd.DataFrame
    pullback_bucket_summary_df: pd.DataFrame
    adv_bucket_summary_df: pd.DataFrame
    top_examples_df: pd.DataFrame


def _connect_duckdb(db_path: str, *, read_only: bool = True) -> Any:
    return _shared_connect_duckdb(db_path, read_only=read_only)


def _open_analysis_connection(db_path: str):
    return open_readonly_analysis_connection(
        db_path,
        snapshot_prefix="speculative-volume-surge-prime-pullback-profile-",
        connect_fn=_connect_duckdb,
    )


def _empty_df(columns: Sequence[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=list(columns))


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


def _coerce_float(value: object) -> float | None:
    if value is None or bool(pd.isna(cast(Any, value))):
        return None
    return float(cast(Any, value))


def _bucket_pullback_position(position_pct: float | None) -> str:
    value = _coerce_float(position_pct)
    if value is None:
        return "missing"
    for spec in PULLBACK_BUCKET_SPECS:
        lower_ok = spec.lower_bound is None or value >= spec.lower_bound
        upper_ok = spec.upper_bound is None or value < spec.upper_bound
        if lower_ok and upper_ok:
            return spec.label
    return "missing"


def _build_bucket_definition_df() -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for spec in PULLBACK_BUCKET_SPECS:
        lower_bound = spec.lower_bound
        upper_bound = spec.upper_bound
        if lower_bound is None:
            if upper_bound is None:
                raise ValueError(f"Bucket {spec.label} must have at least one bound")
            rule = f"position < {upper_bound * 100:.1f}%"
        elif upper_bound is None:
            rule = f"position >= {lower_bound * 100:.1f}%"
        else:
            rule = (
                f"{lower_bound * 100:.1f}% <= position < "
                f"{upper_bound * 100:.1f}%"
            )
        rows.append({"bucket_label": spec.label, "rule": rule})
    return pd.DataFrame(rows)


def _profile_columns(future_horizons: Sequence[int]) -> tuple[str, ...]:
    columns: list[str] = [
        "episode_id",
        "code",
        "company_name",
        "event_date",
        "adv20_bucket",
        "price_bucket",
        "base_close",
        "initial_peak_offset",
        "initial_peak_price",
        "deepest_pullback_offset",
        "deepest_pullback_date",
        "deepest_pullback_close",
        "deepest_pullback_position_pct",
        "peak_to_pullback_drawdown_pct",
        "deepest_pullback_bucket",
        "deepest_pullback_bucket_order",
    ]
    for horizon in future_horizons:
        columns.extend(
            [
                f"has_full_horizon_{horizon}d",
                f"future_max_upside_pct_{horizon}d",
                f"future_max_downside_pct_{horizon}d",
                f"future_close_return_pct_{horizon}d",
                f"asymmetry_pct_{horizon}d",
                f"upside_to_downside_ratio_{horizon}d",
                f"peak_reclaim_{horizon}d",
                f"positive_close_return_{horizon}d",
            ]
        )
    return tuple(columns)


def _query_episode_price_paths(
    conn: Any,
    episode_df: pd.DataFrame,
    *,
    max_session_offset: int,
) -> pd.DataFrame:
    if episode_df.empty:
        return _empty_df(
            (
                "episode_id",
                "code",
                "company_name",
                "event_date",
                "base_close",
                "adv20_bucket",
                "price_bucket",
                "date",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "session_offset",
            )
        )
    normalized_code_sql = normalize_code_sql("code")
    max_calendar_days = max_session_offset * 2 + 30
    episode_input_df = episode_df[list(PRIME_EPISODE_COLUMNS)].copy()
    conn.register("_prime_pullback_episode_input_df", episode_input_df)
    try:
        sql = f"""
            WITH episode_scope AS (
                SELECT
                    MIN(CAST(event_date AS DATE)) AS min_event_date,
                    MAX(CAST(event_date AS DATE)) AS max_event_date
                FROM _prime_pullback_episode_input_df
            ),
            episode_codes AS (
                SELECT DISTINCT code
                FROM _prime_pullback_episode_input_df
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
                    WHERE {normalized_code_sql} IN (SELECT code FROM episode_codes)
                      AND CAST(date AS DATE)
                            >= (SELECT min_event_date FROM episode_scope)
                      AND CAST(date AS DATE)
                            <= (
                                SELECT max_event_date + INTERVAL '{max_calendar_days} day'
                                FROM episode_scope
                            )
                )
                WHERE row_priority = 1
                  AND open IS NOT NULL
                  AND high IS NOT NULL
                  AND low IS NOT NULL
                  AND close IS NOT NULL
            ),
            joined AS (
                SELECT
                    e.episode_id,
                    e.code,
                    e.company_name,
                    e.event_date,
                    e.base_close,
                    e.adv20_bucket,
                    e.price_bucket,
                    sd.date,
                    sd.open,
                    sd.high,
                    sd.low,
                    sd.close,
                    sd.volume,
                    ROW_NUMBER() OVER (
                        PARTITION BY e.episode_id
                        ORDER BY sd.date
                    ) - 1 AS session_offset
                FROM _prime_pullback_episode_input_df e
                JOIN stock_daily sd
                  ON sd.code = e.code
                 AND sd.date >= e.event_date
                 AND CAST(sd.date AS DATE)
                        <= CAST(e.event_date AS DATE) + INTERVAL '{max_calendar_days} day'
            )
            SELECT *
            FROM joined
            WHERE session_offset <= {max_session_offset}
            ORDER BY episode_id, session_offset
        """
        return conn.execute(sql).fetchdf()
    finally:
        conn.unregister("_prime_pullback_episode_input_df")


def _build_prime_pullback_profile_df(
    episode_df: pd.DataFrame,
    path_df: pd.DataFrame,
    *,
    initial_peak_window: int,
    pullback_search_window: int,
    future_horizons: Sequence[int],
) -> pd.DataFrame:
    empty_profile_df = _empty_df(_profile_columns(future_horizons))
    if episode_df.empty or path_df.empty:
        return empty_profile_df
    path_groups: dict[Any, pd.DataFrame] = {
        episode_id: group.sort_values(by="session_offset", kind="stable").reset_index(drop=True)
        for episode_id, group in path_df.groupby("episode_id", sort=False, dropna=False)
    }
    rows: list[dict[str, Any]] = []
    for episode in episode_df.itertuples(index=False):
        episode_path = path_groups.get(episode.episode_id)
        if episode_path is None or episode_path.empty:
            continue
        base_close = _coerce_float(episode.base_close)
        if base_close is None or base_close <= 0:
            continue
        peak_rows = episode_path.loc[episode_path["session_offset"] <= initial_peak_window].copy()
        if peak_rows.empty:
            continue
        peak_idx = peak_rows["high"].astype(float).idxmax()
        peak_row = episode_path.loc[peak_idx]
        initial_peak_offset = int(cast(Any, peak_row["session_offset"]))
        initial_peak_price = float(cast(Any, peak_row["high"]))

        search_rows = episode_path.loc[
            (episode_path["session_offset"] > initial_peak_offset)
            & (episode_path["session_offset"] <= pullback_search_window)
        ].copy()
        if search_rows.empty:
            continue
        reclaim_rows = search_rows.loc[
            pd.to_numeric(search_rows["high"], errors="coerce") > initial_peak_price + _EPSILON
        ]
        if not reclaim_rows.empty:
            first_reclaim_offset = int(cast(Any, reclaim_rows.iloc[0]["session_offset"]))
            search_rows = search_rows.loc[search_rows["session_offset"] < first_reclaim_offset].copy()
        search_rows = search_rows.loc[
            pd.to_numeric(search_rows["close"], errors="coerce") < initial_peak_price - _EPSILON
        ].copy()
        if search_rows.empty:
            continue

        deepest_idx = pd.to_numeric(search_rows["close"], errors="coerce").idxmin()
        deepest_row = search_rows.loc[deepest_idx]
        deepest_offset = int(cast(Any, deepest_row["session_offset"]))
        deepest_close = float(cast(Any, deepest_row["close"]))
        deepest_position_pct = deepest_close / base_close - 1.0
        peak_to_pullback_drawdown_pct = 1.0 - deepest_close / initial_peak_price
        deepest_bucket = _bucket_pullback_position(deepest_position_pct)
        max_available_offset = int(cast(Any, episode_path["session_offset"].max()))

        state_row: dict[str, Any] = {
            "episode_id": episode.episode_id,
            "code": episode.code,
            "company_name": episode.company_name,
            "event_date": episode.event_date,
            "adv20_bucket": episode.adv20_bucket,
            "price_bucket": episode.price_bucket,
            "base_close": base_close,
            "initial_peak_offset": initial_peak_offset,
            "initial_peak_price": initial_peak_price,
            "deepest_pullback_offset": deepest_offset,
            "deepest_pullback_date": deepest_row["date"],
            "deepest_pullback_close": deepest_close,
            "deepest_pullback_position_pct": deepest_position_pct,
            "peak_to_pullback_drawdown_pct": peak_to_pullback_drawdown_pct,
            "deepest_pullback_bucket": deepest_bucket,
            "deepest_pullback_bucket_order": PULLBACK_BUCKET_ORDER.index(deepest_bucket)
            if deepest_bucket in PULLBACK_BUCKET_ORDER
            else len(PULLBACK_BUCKET_ORDER),
        }
        for horizon in future_horizons:
            valid_horizon = max_available_offset >= deepest_offset + horizon
            future_upside_col = f"future_max_upside_pct_{horizon}d"
            future_downside_col = f"future_max_downside_pct_{horizon}d"
            future_close_col = f"future_close_return_pct_{horizon}d"
            asymmetry_col = f"asymmetry_pct_{horizon}d"
            ratio_col = f"upside_to_downside_ratio_{horizon}d"
            reclaim_col = f"peak_reclaim_{horizon}d"
            positive_close_col = f"positive_close_return_{horizon}d"
            state_row[f"has_full_horizon_{horizon}d"] = valid_horizon
            if not valid_horizon:
                state_row[future_upside_col] = None
                state_row[future_downside_col] = None
                state_row[future_close_col] = None
                state_row[asymmetry_col] = None
                state_row[ratio_col] = None
                state_row[reclaim_col] = None
                state_row[positive_close_col] = None
                continue
            future_rows = episode_path.loc[
                (episode_path["session_offset"] > deepest_offset)
                & (episode_path["session_offset"] <= deepest_offset + horizon)
            ].copy()
            if future_rows.empty:
                state_row[future_upside_col] = None
                state_row[future_downside_col] = None
                state_row[future_close_col] = None
                state_row[asymmetry_col] = None
                state_row[ratio_col] = None
                state_row[reclaim_col] = None
                state_row[positive_close_col] = None
                continue
            future_max_high = float(pd.to_numeric(future_rows["high"], errors="coerce").max())
            future_min_low = float(pd.to_numeric(future_rows["low"], errors="coerce").min())
            future_last_close = float(pd.to_numeric(future_rows["close"], errors="coerce").iloc[-1])
            upside_pct = future_max_high / deepest_close - 1.0
            downside_pct = 1.0 - future_min_low / deepest_close
            future_close_return_pct = future_last_close / deepest_close - 1.0
            state_row[future_upside_col] = upside_pct
            state_row[future_downside_col] = downside_pct
            state_row[future_close_col] = future_close_return_pct
            state_row[asymmetry_col] = upside_pct - downside_pct
            state_row[ratio_col] = upside_pct / downside_pct if downside_pct > 0 else None
            state_row[reclaim_col] = future_max_high > initial_peak_price + _EPSILON
            state_row[positive_close_col] = future_close_return_pct > 0
        rows.append(state_row)
    if not rows:
        return empty_profile_df
    return pd.DataFrame(rows).reindex(columns=_profile_columns(future_horizons)).sort_values(
        by=["deepest_pullback_bucket_order", "event_date", "code"],
        kind="stable",
    ).reset_index(drop=True)


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


def _bool_rate(series: pd.Series) -> float | None:
    scoped = series.dropna()
    if scoped.empty:
        return None
    return float(scoped.astype(bool).mean())


def _build_pullback_bucket_summary_df(
    profile_df: pd.DataFrame,
    *,
    future_horizons: Sequence[int],
) -> pd.DataFrame:
    if profile_df.empty:
        return _empty_df(PULLBACK_BUCKET_SUMMARY_COLUMNS)
    rows: list[dict[str, Any]] = []
    for horizon in future_horizons:
        valid_mask = profile_df[f"has_full_horizon_{horizon}d"].fillna(False)
        scoped = profile_df.loc[valid_mask].copy()
        if scoped.empty:
            continue
        upside_col = f"future_max_upside_pct_{horizon}d"
        downside_col = f"future_max_downside_pct_{horizon}d"
        close_col = f"future_close_return_pct_{horizon}d"
        asymmetry_col = f"asymmetry_pct_{horizon}d"
        ratio_col = f"upside_to_downside_ratio_{horizon}d"
        reclaim_col = f"peak_reclaim_{horizon}d"
        positive_close_col = f"positive_close_return_{horizon}d"
        for bucket_label, bucket_df in scoped.groupby(
            "deepest_pullback_bucket",
            sort=False,
            dropna=False,
        ):
            bucket_name = "missing" if pd.isna(bucket_label) else str(bucket_label)
            rows.append(
                {
                    "future_horizon_days": horizon,
                    "pullback_bucket": bucket_name,
                    "pullback_bucket_order": PULLBACK_BUCKET_ORDER.index(bucket_name)
                    if bucket_name in PULLBACK_BUCKET_ORDER
                    else len(PULLBACK_BUCKET_ORDER),
                    "observation_count": int(len(bucket_df)),
                    "mean_deepest_pullback_position_pct": _mean(
                        bucket_df["deepest_pullback_position_pct"]
                    ),
                    "median_deepest_pullback_position_pct": _median(
                        bucket_df["deepest_pullback_position_pct"]
                    ),
                    "mean_peak_to_pullback_drawdown_pct": _mean(
                        bucket_df["peak_to_pullback_drawdown_pct"]
                    ),
                    "mean_future_max_upside_pct": _mean(bucket_df[upside_col]),
                    "median_future_max_upside_pct": _median(bucket_df[upside_col]),
                    "mean_future_max_downside_pct": _mean(bucket_df[downside_col]),
                    "median_future_max_downside_pct": _median(bucket_df[downside_col]),
                    "mean_future_close_return_pct": _mean(bucket_df[close_col]),
                    "median_future_close_return_pct": _median(bucket_df[close_col]),
                    "mean_asymmetry_pct": _mean(bucket_df[asymmetry_col]),
                    "median_asymmetry_pct": _median(bucket_df[asymmetry_col]),
                    "mean_upside_to_downside_ratio": _mean(bucket_df[ratio_col]),
                    "positive_close_return_rate": _bool_rate(bucket_df[positive_close_col]),
                    "upside_gt_downside_rate": _bool_rate(
                        pd.to_numeric(bucket_df[upside_col], errors="coerce")
                        > pd.to_numeric(bucket_df[downside_col], errors="coerce")
                    ),
                    "peak_reclaim_rate": _bool_rate(bucket_df[reclaim_col]),
                }
            )
    if not rows:
        return _empty_df(PULLBACK_BUCKET_SUMMARY_COLUMNS)
    return pd.DataFrame(rows).sort_values(
        by=["future_horizon_days", "pullback_bucket_order"],
        kind="stable",
    ).reset_index(drop=True)


def _build_adv_bucket_summary_df(
    profile_df: pd.DataFrame,
    *,
    primary_horizon: int,
) -> pd.DataFrame:
    if profile_df.empty:
        return _empty_df(ADV_BUCKET_SUMMARY_COLUMNS)
    valid_mask = profile_df[f"has_full_horizon_{primary_horizon}d"].fillna(False)
    scoped = profile_df.loc[valid_mask].copy()
    if scoped.empty:
        return _empty_df(ADV_BUCKET_SUMMARY_COLUMNS)
    close_col = f"future_close_return_pct_{primary_horizon}d"
    asymmetry_col = f"asymmetry_pct_{primary_horizon}d"
    upside_col = f"future_max_upside_pct_{primary_horizon}d"
    downside_col = f"future_max_downside_pct_{primary_horizon}d"
    reclaim_col = f"peak_reclaim_{primary_horizon}d"
    positive_close_col = f"positive_close_return_{primary_horizon}d"
    rows: list[dict[str, Any]] = []
    for (adv20_bucket, bucket_label), group_df in scoped.groupby(
        ["adv20_bucket", "deepest_pullback_bucket"],
        sort=False,
        dropna=False,
    ):
        bucket_name = "missing" if pd.isna(bucket_label) else str(bucket_label)
        rows.append(
            {
                "adv20_bucket": "missing" if pd.isna(adv20_bucket) else str(adv20_bucket),
                "pullback_bucket": bucket_name,
                "pullback_bucket_order": PULLBACK_BUCKET_ORDER.index(bucket_name)
                if bucket_name in PULLBACK_BUCKET_ORDER
                else len(PULLBACK_BUCKET_ORDER),
                "observation_count": int(len(group_df)),
                "median_future_close_return_pct": _median(group_df[close_col]),
                "median_asymmetry_pct": _median(group_df[asymmetry_col]),
                "positive_close_return_rate": _bool_rate(group_df[positive_close_col]),
                "upside_gt_downside_rate": _bool_rate(
                    pd.to_numeric(group_df[upside_col], errors="coerce")
                    > pd.to_numeric(group_df[downside_col], errors="coerce")
                ),
                "peak_reclaim_rate": _bool_rate(group_df[reclaim_col]),
            }
        )
    return pd.DataFrame(rows).sort_values(
        by=["adv20_bucket", "pullback_bucket_order", "observation_count"],
        ascending=[True, True, False],
        kind="stable",
    ).reset_index(drop=True)


def _build_top_examples_df(
    profile_df: pd.DataFrame,
    *,
    primary_horizon: int,
    sample_size: int,
) -> pd.DataFrame:
    if profile_df.empty:
        return _empty_df(TOP_EXAMPLES_COLUMNS)
    valid_mask = profile_df[f"has_full_horizon_{primary_horizon}d"].fillna(False)
    scoped = profile_df.loc[valid_mask].copy()
    if scoped.empty:
        return _empty_df(TOP_EXAMPLES_COLUMNS)
    close_col = f"future_close_return_pct_{primary_horizon}d"
    asymmetry_col = f"asymmetry_pct_{primary_horizon}d"
    upside_col = f"future_max_upside_pct_{primary_horizon}d"
    downside_col = f"future_max_downside_pct_{primary_horizon}d"
    reclaim_col = f"peak_reclaim_{primary_horizon}d"
    scoped = scoped.sort_values(by=[close_col, "event_date", "code"], kind="stable")
    negative_df = scoped.head(sample_size).copy()
    negative_df["example_group"] = "negative_close"
    positive_df = scoped.tail(sample_size).copy().sort_values(
        by=[close_col, "event_date", "code"],
        ascending=[False, True, True],
        kind="stable",
    )
    positive_df["example_group"] = "positive_close"
    combined = pd.concat([positive_df, negative_df], ignore_index=True)
    combined["future_close_return_primary_pct"] = combined[close_col]
    combined["asymmetry_primary_pct"] = combined[asymmetry_col]
    combined["future_max_upside_primary_pct"] = combined[upside_col]
    combined["future_max_downside_primary_pct"] = combined[downside_col]
    combined["peak_reclaim_primary"] = combined[reclaim_col]
    combined = combined.rename(
        columns={
            "deepest_pullback_offset": "deepest_pullback_offset",
            "deepest_pullback_date": "deepest_pullback_date",
            "deepest_pullback_bucket": "deepest_pullback_bucket",
            "deepest_pullback_position_pct": "deepest_pullback_position_pct",
        }
    )
    return combined.reindex(columns=TOP_EXAMPLES_COLUMNS)


def _build_research_bundle_summary_markdown(
    result: SpeculativeVolumeSurgePrimePullbackProfileResult,
) -> str:
    lines = [
        "# Speculative Volume-Surge Prime Pullback Profile",
        "",
        f"- Analysis range: `{result.analysis_start_date}` -> `{result.analysis_end_date}`",
        f"- Market: `{result.market_name}`",
        f"- Primary trigger: close jump `>= {result.price_jump_threshold * 100:.1f}%` and volume ratio `>= {result.volume_ratio_threshold:.1f}x`",
        f"- Initial peak window: `{result.initial_peak_window}` sessions",
        f"- Pullback search window: `{result.pullback_search_window}` sessions",
        f"- Future horizons: `{','.join(str(v) for v in result.future_horizons)}`",
        f"- Prime episodes: `{result.total_prime_episode_count}`",
        f"- Exclusive deepest-pullback profiles: `{result.prime_pullback_profile_count}`",
        "",
        "## Primary Horizon Read",
    ]
    primary_rows = result.pullback_bucket_summary_df.loc[
        result.pullback_bucket_summary_df["future_horizon_days"] == result.primary_horizon
    ]
    if primary_rows.empty:
        lines.append("- No pullback-profile rows were produced.")
    else:
        for row in primary_rows.itertuples(index=False):
            lines.append(
                f"- `{row.pullback_bucket}`: observations `{int(cast(Any, row.observation_count))}`, "
                f"close return `{float(cast(Any, row.median_future_close_return_pct)) * 100:.1f}%`, "
                f"asymmetry `{float(cast(Any, row.median_asymmetry_pct)) * 100:.1f}%`, "
                f"positive close `{float(cast(Any, row.positive_close_return_rate)) * 100:.1f}%`, "
                f"reclaim `{float(cast(Any, row.peak_reclaim_rate)) * 100:.1f}%`"
            )
    lines.extend(["", "## Note"])
    lines.append(
        f"- Each episode receives one exclusive label: the deepest close observed within {result.pullback_search_window} sessions after the surge and before any reclaim above the initial {result.initial_peak_window}-session peak."
    )
    return "\n".join(lines) + "\n"


def _build_published_summary(
    result: SpeculativeVolumeSurgePrimePullbackProfileResult,
) -> dict[str, Any]:
    primary_rows = result.pullback_bucket_summary_df.loc[
        result.pullback_bucket_summary_df["future_horizon_days"] == result.primary_horizon
    ]
    return {
        "analysisStartDate": result.analysis_start_date,
        "analysisEndDate": result.analysis_end_date,
        "marketName": result.market_name,
        "primeEpisodes": result.total_prime_episode_count,
        "primePullbackProfiles": result.prime_pullback_profile_count,
        "primaryHorizon": result.primary_horizon,
        "topRows": primary_rows.to_dict(orient="records"),
    }


def run_speculative_volume_surge_prime_pullback_profile_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    lookback_years: int = DEFAULT_LOOKBACK_YEARS,
    price_jump_threshold: float = DEFAULT_PRICE_JUMP_THRESHOLD,
    volume_ratio_threshold: float = DEFAULT_VOLUME_RATIO_THRESHOLD,
    volume_window: int = DEFAULT_VOLUME_WINDOW,
    adv_window: int = DEFAULT_ADV_WINDOW,
    cooldown_sessions: int = DEFAULT_COOLDOWN_SESSIONS,
    initial_peak_window: int = DEFAULT_INITIAL_PEAK_WINDOW,
    pullback_search_window: int = DEFAULT_PULLBACK_SEARCH_WINDOW,
    future_horizons: tuple[int, ...] | list[int] | None = None,
    primary_horizon: int = DEFAULT_PRIMARY_HORIZON,
    sample_size: int = DEFAULT_SAMPLE_SIZE,
) -> SpeculativeVolumeSurgePrimePullbackProfileResult:
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
    if initial_peak_window <= 0:
        raise ValueError("initial_peak_window must be positive")
    if pullback_search_window <= initial_peak_window:
        raise ValueError("pullback_search_window must be greater than initial_peak_window")
    if sample_size <= 0:
        raise ValueError("sample_size must be positive")
    normalized_future_horizons = _normalize_int_sequence(
        future_horizons,
        fallback=DEFAULT_FUTURE_HORIZONS,
        name="future_horizons",
    )
    if primary_horizon not in normalized_future_horizons:
        raise ValueError("primary_horizon must be included in future_horizons")

    base_result: SpeculativeVolumeSurgeFollowOnResult = (
        run_speculative_volume_surge_follow_on_research(
            db_path,
            start_date=start_date,
            end_date=end_date,
            lookback_years=lookback_years,
            price_jump_threshold=price_jump_threshold,
            volume_ratio_threshold=volume_ratio_threshold,
            volume_window=volume_window,
            adv_window=adv_window,
            cooldown_sessions=cooldown_sessions,
        )
    )
    prime_episode_df = base_result.event_ledger_df.loc[
        base_result.event_ledger_df["market_name"] == PRIME_MARKET_NAME,
        list(PRIME_EPISODE_COLUMNS),
    ].copy()
    bucket_definition_df = _build_bucket_definition_df()
    if prime_episode_df.empty:
        empty_profile_df = _empty_df(_profile_columns(normalized_future_horizons))
        return SpeculativeVolumeSurgePrimePullbackProfileResult(
            db_path=db_path,
            source_mode=base_result.source_mode,
            source_detail=base_result.source_detail,
            available_start_date=base_result.available_start_date,
            available_end_date=base_result.available_end_date,
            analysis_start_date=base_result.analysis_start_date,
            analysis_end_date=base_result.analysis_end_date,
            market_name=PRIME_MARKET_NAME,
            lookback_years=lookback_years,
            price_jump_threshold=price_jump_threshold,
            volume_ratio_threshold=volume_ratio_threshold,
            volume_window=volume_window,
            adv_window=adv_window,
            cooldown_sessions=cooldown_sessions,
            initial_peak_window=initial_peak_window,
            pullback_search_window=pullback_search_window,
            future_horizons=normalized_future_horizons,
            primary_horizon=primary_horizon,
            sample_size=sample_size,
            total_prime_episode_count=0,
            prime_pullback_profile_count=0,
            prime_episode_df=prime_episode_df,
            bucket_definition_df=bucket_definition_df,
            prime_pullback_profile_df=empty_profile_df,
            pullback_bucket_summary_df=_empty_df(PULLBACK_BUCKET_SUMMARY_COLUMNS),
            adv_bucket_summary_df=_empty_df(ADV_BUCKET_SUMMARY_COLUMNS),
            top_examples_df=_empty_df(TOP_EXAMPLES_COLUMNS),
        )

    max_session_offset = pullback_search_window + max(normalized_future_horizons)
    with _open_analysis_connection(db_path) as ctx:
        path_df = _query_episode_price_paths(
            ctx.connection,
            prime_episode_df,
            max_session_offset=max_session_offset,
        )
        prime_pullback_profile_df = _build_prime_pullback_profile_df(
            prime_episode_df,
            path_df,
            initial_peak_window=initial_peak_window,
            pullback_search_window=pullback_search_window,
            future_horizons=normalized_future_horizons,
        )

    pullback_bucket_summary_df = _build_pullback_bucket_summary_df(
        prime_pullback_profile_df,
        future_horizons=normalized_future_horizons,
    )
    adv_bucket_summary_df = _build_adv_bucket_summary_df(
        prime_pullback_profile_df,
        primary_horizon=primary_horizon,
    )
    top_examples_df = _build_top_examples_df(
        prime_pullback_profile_df,
        primary_horizon=primary_horizon,
        sample_size=sample_size,
    )
    return SpeculativeVolumeSurgePrimePullbackProfileResult(
        db_path=db_path,
        source_mode=base_result.source_mode,
        source_detail=base_result.source_detail,
        available_start_date=base_result.available_start_date,
        available_end_date=base_result.available_end_date,
        analysis_start_date=base_result.analysis_start_date,
        analysis_end_date=base_result.analysis_end_date,
        market_name=PRIME_MARKET_NAME,
        lookback_years=lookback_years,
        price_jump_threshold=price_jump_threshold,
        volume_ratio_threshold=volume_ratio_threshold,
        volume_window=volume_window,
        adv_window=adv_window,
        cooldown_sessions=cooldown_sessions,
        initial_peak_window=initial_peak_window,
        pullback_search_window=pullback_search_window,
        future_horizons=normalized_future_horizons,
        primary_horizon=primary_horizon,
        sample_size=sample_size,
        total_prime_episode_count=int(len(prime_episode_df)),
        prime_pullback_profile_count=int(len(prime_pullback_profile_df)),
        prime_episode_df=prime_episode_df,
        bucket_definition_df=bucket_definition_df,
        prime_pullback_profile_df=prime_pullback_profile_df,
        pullback_bucket_summary_df=pullback_bucket_summary_df,
        adv_bucket_summary_df=adv_bucket_summary_df,
        top_examples_df=top_examples_df,
    )


def write_speculative_volume_surge_prime_pullback_profile_research_bundle(
    result: SpeculativeVolumeSurgePrimePullbackProfileResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=SPECULATIVE_VOLUME_SURGE_PRIME_PULLBACK_PROFILE_EXPERIMENT_ID,
        module=__name__,
        function="run_speculative_volume_surge_prime_pullback_profile_research",
        params={
            "lookback_years": result.lookback_years,
            "price_jump_threshold": result.price_jump_threshold,
            "volume_ratio_threshold": result.volume_ratio_threshold,
            "volume_window": result.volume_window,
            "adv_window": result.adv_window,
            "cooldown_sessions": result.cooldown_sessions,
            "initial_peak_window": result.initial_peak_window,
            "pullback_search_window": result.pullback_search_window,
            "future_horizons": result.future_horizons,
            "primary_horizon": result.primary_horizon,
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


def load_speculative_volume_surge_prime_pullback_profile_research_bundle(
    bundle_path: str | Path,
) -> SpeculativeVolumeSurgePrimePullbackProfileResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=SpeculativeVolumeSurgePrimePullbackProfileResult,
        table_field_names=TABLE_FIELD_NAMES,
    )


def get_speculative_volume_surge_prime_pullback_profile_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        SPECULATIVE_VOLUME_SURGE_PRIME_PULLBACK_PROFILE_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_speculative_volume_surge_prime_pullback_profile_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        SPECULATIVE_VOLUME_SURGE_PRIME_PULLBACK_PROFILE_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )
