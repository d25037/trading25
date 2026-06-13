"""Index and Nikkei 225 option behavior around monthly SQ weeks."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, cast
import math

import numpy as np
import pandas as pd

from src.domains.analytics.readonly_duckdb_support import (
    open_readonly_analysis_connection,
)
from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    find_latest_research_bundle_path,
    get_research_bundle_dir,
    load_dataclass_research_bundle,
    write_dataclass_research_bundle,
)
from src.shared.utils.pandas_type_guards import finite_float_or_none

INDEX_SQ_WEEK_EFFECT_RESEARCH_EXPERIMENT_ID = (
    "market-behavior/index-sq-week-effect-research"
)
DEFAULT_MIN_WEEK_OBSERVATIONS = 20

_RESULT_TABLE_NAMES: tuple[str, ...] = (
    "sq_calendar_df",
    "index_daily_panel_df",
    "index_weekly_panel_df",
    "index_weekly_summary_df",
    "index_daily_summary_df",
    "options_front_daily_df",
    "options_weekly_panel_df",
    "options_weekly_summary_df",
    "options_days_to_sq_summary_df",
)
_COMPARISON_COLUMNS: tuple[str, ...] = (
    "group_key",
    "group_label",
    "metric",
    "metric_family",
    "sq_observation_count",
    "other_observation_count",
    "sq_mean",
    "other_mean",
    "difference",
    "relative_difference_pct",
    "effect_size_vs_other_std",
    "cohens_d",
    "sq_median",
    "other_median",
    "sq_p10",
    "other_p10",
    "sq_p90",
    "other_p90",
    "welch_t_stat",
    "normal_approx_p_value",
)
_OPTIONS_DAYS_TO_SQ_COLUMNS: tuple[str, ...] = (
    "calendar_days_to_sq",
    "metric",
    "observation_count",
    "mean",
    "median",
    "p10",
    "p90",
)


@dataclass(frozen=True)
class IndexSqWeekEffectResearchResult:
    db_path: str
    source_mode: str
    source_detail: str
    analysis_start_date: str | None
    analysis_end_date: str | None
    index_observation_count: int
    index_week_count: int
    sq_week_count: int
    option_observation_count: int
    min_week_observations: int
    research_policy: str
    sq_calendar_df: pd.DataFrame
    index_daily_panel_df: pd.DataFrame
    index_weekly_panel_df: pd.DataFrame
    index_weekly_summary_df: pd.DataFrame
    index_daily_summary_df: pd.DataFrame
    options_front_daily_df: pd.DataFrame
    options_weekly_panel_df: pd.DataFrame
    options_weekly_summary_df: pd.DataFrame
    options_days_to_sq_summary_df: pd.DataFrame


def run_index_sq_week_effect_research(
    db_path: str | Path,
    *,
    min_week_observations: int = DEFAULT_MIN_WEEK_OBSERVATIONS,
) -> IndexSqWeekEffectResearchResult:
    if min_week_observations < 2:
        raise ValueError("min_week_observations must be >= 2")

    resolved_db_path = str(Path(db_path).expanduser())
    with open_readonly_analysis_connection(
        resolved_db_path,
        snapshot_prefix="index-sq-week-effect-",
    ) as ctx:
        index_price_df = _query_index_price_rows(ctx.connection)
        options_front_daily_df = _query_front_options_daily_rows(ctx.connection)
        source_mode = ctx.source_mode
        source_detail = ctx.source_detail

    if index_price_df.empty:
        raise RuntimeError("No TOPIX/Nikkei 225 index rows were found.")

    index_daily_panel_df = _build_index_daily_panel(index_price_df)
    index_weekly_panel_df = _build_index_weekly_panel(index_daily_panel_df)
    index_weekly_summary_df = _build_index_weekly_summary_df(
        index_weekly_panel_df,
        min_observations=min_week_observations,
    )
    index_daily_summary_df = _build_index_daily_summary_df(index_daily_panel_df)
    options_front_daily_df = _build_options_front_daily_panel(options_front_daily_df)
    options_weekly_panel_df = _build_options_weekly_panel(options_front_daily_df)
    options_weekly_summary_df = _build_options_weekly_summary_df(options_weekly_panel_df)
    options_days_to_sq_summary_df = _build_options_days_to_sq_summary_df(
        options_front_daily_df
    )
    sq_calendar_df = _build_sq_calendar_df(index_daily_panel_df, options_front_daily_df)

    date_values = pd.to_datetime(index_daily_panel_df["date"], errors="coerce").dropna()
    return IndexSqWeekEffectResearchResult(
        db_path=resolved_db_path,
        source_mode=source_mode,
        source_detail=source_detail,
        analysis_start_date=date_values.min().strftime("%Y-%m-%d")
        if not date_values.empty
        else None,
        analysis_end_date=date_values.max().strftime("%Y-%m-%d")
        if not date_values.empty
        else None,
        index_observation_count=int(len(index_daily_panel_df)),
        index_week_count=int(index_weekly_panel_df["week_start"].nunique())
        if not index_weekly_panel_df.empty
        else 0,
        sq_week_count=int(
            index_weekly_panel_df[index_weekly_panel_df["is_sq_week"]][
                "week_start"
            ].nunique()
        )
        if not index_weekly_panel_df.empty
        else 0,
        option_observation_count=int(len(options_front_daily_df)),
        min_week_observations=min_week_observations,
        research_policy=(
            "Monthly SQ week is the Monday-Friday week containing the second Friday "
            "of each calendar month. Index comparisons use only TOPIX daily OHLC and "
            "Nikkei 225 UnderPx-derived OHLC available in market.duckdb. Option "
            "comparisons aggregate the front Nikkei 225 option SQ contract for each "
            "date, using local options_225_data only. Buckets are calendar-defined, "
            "not fitted from future return outcomes."
        ),
        sq_calendar_df=sq_calendar_df,
        index_daily_panel_df=index_daily_panel_df,
        index_weekly_panel_df=index_weekly_panel_df,
        index_weekly_summary_df=index_weekly_summary_df,
        index_daily_summary_df=index_daily_summary_df,
        options_front_daily_df=options_front_daily_df,
        options_weekly_panel_df=options_weekly_panel_df,
        options_weekly_summary_df=options_weekly_summary_df,
        options_days_to_sq_summary_df=options_days_to_sq_summary_df,
    )


def _query_index_price_rows(conn: Any) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    topix = conn.execute(
        """
        SELECT
            'TOPIX' AS index_key,
            'TOPIX' AS index_name,
            date,
            open,
            high,
            low,
            close
        FROM topix_data
        WHERE open IS NOT NULL
          AND high IS NOT NULL
          AND low IS NOT NULL
          AND close IS NOT NULL
          AND close > 0
        ORDER BY date
        """
    ).fetchdf()
    if not topix.empty:
        frames.append(topix)

    n225 = conn.execute(
        """
        SELECT
            'N225_UNDERPX' AS index_key,
            'Nikkei 225 UnderPx' AS index_name,
            date,
            open,
            high,
            low,
            close
        FROM indices_data
        WHERE upper(code) = 'N225_UNDERPX'
          AND close IS NOT NULL
          AND close > 0
        ORDER BY date
        """
    ).fetchdf()
    if not n225.empty:
        frames.append(n225)

    if not frames:
        return pd.DataFrame(
            columns=["index_key", "index_name", "date", "open", "high", "low", "close"]
        )
    rows = pd.concat(frames, ignore_index=True)
    rows["date"] = pd.to_datetime(rows["date"], errors="coerce")
    for col in ("open", "high", "low", "close"):
        rows[col] = pd.to_numeric(rows[col], errors="coerce")
    return (
        rows.dropna(subset=["index_key", "date", "close"])
        .sort_values(["index_key", "date"])
        .reset_index(drop=True)
    )


def _query_front_options_daily_rows(conn: Any) -> pd.DataFrame:
    if not _table_exists(conn, "options_225_data"):
        return _empty_options_front_daily_df()

    return conn.execute(
        """
        WITH valid_options AS (
            SELECT
                date,
                special_quotation_day,
                CAST(date AS DATE) AS trade_date,
                CAST(special_quotation_day AS DATE) AS sq_date,
                volume,
                open_interest,
                turnover_value,
                strike_price,
                underlying_price,
                implied_volatility,
                base_volatility
            FROM options_225_data
            WHERE special_quotation_day IS NOT NULL
              AND CAST(special_quotation_day AS DATE) >= CAST(date AS DATE)
        ),
        front_expiry AS (
            SELECT date, MIN(sq_date) AS front_sq_date
            FROM valid_options
            GROUP BY date
        ),
        front_rows AS (
            SELECT v.*, f.front_sq_date
            FROM valid_options v
            JOIN front_expiry f
              ON v.date = f.date
             AND v.sq_date = f.front_sq_date
        ),
        daily_agg AS (
            SELECT
                date,
                MIN(CAST(front_sq_date AS VARCHAR)) AS front_sq_date,
                COUNT(*) AS option_row_count,
                SUM(COALESCE(volume, 0)) AS front_volume,
                SUM(COALESCE(open_interest, 0)) AS front_open_interest,
                SUM(COALESCE(turnover_value, 0)) AS front_turnover_value,
                AVG(CASE WHEN implied_volatility > 0 THEN implied_volatility END)
                    AS mean_implied_volatility,
                AVG(CASE WHEN base_volatility > 0 THEN base_volatility END)
                    AS mean_base_volatility,
                AVG(CASE WHEN underlying_price > 0 THEN underlying_price END)
                    AS mean_underlying_price
            FROM front_rows
            GROUP BY date
        ),
        atm_ranked AS (
            SELECT
                date,
                strike_price,
                underlying_price,
                implied_volatility,
                base_volatility,
                open_interest,
                volume,
                ABS(strike_price - underlying_price) AS atm_distance,
                ROW_NUMBER() OVER (
                    PARTITION BY date
                    ORDER BY
                        ABS(strike_price - underlying_price) ASC,
                        COALESCE(open_interest, 0) DESC,
                        COALESCE(volume, 0) DESC
                ) AS rn
            FROM front_rows
            WHERE strike_price IS NOT NULL
              AND underlying_price IS NOT NULL
              AND underlying_price > 0
        )
        SELECT
            d.*,
            DATE_DIFF('day', CAST(d.date AS DATE), CAST(d.front_sq_date AS DATE))
                AS calendar_days_to_sq,
            a.strike_price AS atm_strike_price,
            a.underlying_price AS atm_underlying_price,
            a.atm_distance,
            a.implied_volatility AS atm_implied_volatility,
            a.base_volatility AS atm_base_volatility,
            a.open_interest AS atm_open_interest,
            a.volume AS atm_volume
        FROM daily_agg d
        LEFT JOIN atm_ranked a
          ON d.date = a.date
         AND a.rn = 1
        ORDER BY d.date
        """
    ).fetchdf()


def _table_exists(conn: Any, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(row and int(row[0]) > 0)


def _build_index_daily_panel(index_price_df: pd.DataFrame) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for _, group in index_price_df.groupby("index_key", sort=True):
        g = group.sort_values("date").copy()
        g["prev_close"] = g["close"].shift(1)
        g["close_to_close_return_pct"] = (g["close"] / g["prev_close"] - 1.0) * 100.0
        g["open_to_close_return_pct"] = (g["close"] / g["open"] - 1.0) * 100.0
        g["overnight_return_pct"] = (g["open"] / g["prev_close"] - 1.0) * 100.0
        g["intraday_range_pct"] = (g["high"] / g["low"] - 1.0) * 100.0
        frames.append(g)
    result = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if result.empty:
        return result
    return _add_sq_calendar_columns(result)


def _build_index_weekly_panel(index_daily_panel_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    if index_daily_panel_df.empty:
        return pd.DataFrame()
    for (index_key, week_start), group in index_daily_panel_df.groupby(
        ["index_key", "week_start"],
        sort=True,
    ):
        g = group.sort_values("date").copy()
        if g.empty:
            continue
        first = g.iloc[0]
        last = g.iloc[-1]
        week_open = finite_float_or_none(first["open"])
        week_close = finite_float_or_none(last["close"])
        week_high = finite_float_or_none(pd.to_numeric(g["high"], errors="coerce").max())
        week_low = finite_float_or_none(pd.to_numeric(g["low"], errors="coerce").min())
        daily_returns = pd.to_numeric(g["close_to_close_return_pct"], errors="coerce")
        week_return = (
            (week_close / week_open - 1.0) * 100.0
            if week_open and week_close and week_open > 0
            else np.nan
        )
        week_range = (
            (week_high / week_low - 1.0) * 100.0
            if week_high and week_low and week_low > 0
            else np.nan
        )
        rows.append(
            {
                "index_key": str(index_key),
                "index_name": str(first["index_name"]),
                "week_start": str(week_start),
                "week_end": str(first["week_end"]),
                "sq_date": str(first["sq_date"]),
                "is_sq_week": bool(first["is_sq_week"]),
                "trading_day_count": int(len(g)),
                "week_open": week_open,
                "week_close": week_close,
                "week_return_pct": week_return,
                "abs_week_return_pct": abs(week_return)
                if np.isfinite(week_return)
                else np.nan,
                "week_high_low_range_pct": week_range,
                "max_abs_daily_return_pct": float(daily_returns.abs().max())
                if not daily_returns.dropna().empty
                else np.nan,
                "daily_return_std_pct": float(daily_returns.std(ddof=1))
                if daily_returns.dropna().shape[0] > 1
                else np.nan,
                "mean_intraday_range_pct": float(
                    pd.to_numeric(g["intraday_range_pct"], errors="coerce").mean()
                ),
                "mean_open_to_close_return_pct": float(
                    pd.to_numeric(g["open_to_close_return_pct"], errors="coerce").mean()
                ),
                "mean_overnight_return_pct": float(
                    pd.to_numeric(g["overnight_return_pct"], errors="coerce").mean()
                ),
            }
        )
    return pd.DataFrame(rows)


def _build_index_weekly_summary_df(
    index_weekly_panel_df: pd.DataFrame,
    *,
    min_observations: int,
) -> pd.DataFrame:
    metric_specs = (
        ("week_return_pct", "direction"),
        ("abs_week_return_pct", "absolute_move"),
        ("week_high_low_range_pct", "range"),
        ("max_abs_daily_return_pct", "max_daily_shock"),
        ("daily_return_std_pct", "daily_realized_vol"),
        ("mean_intraday_range_pct", "intraday_range"),
        ("mean_open_to_close_return_pct", "intraday_direction"),
        ("mean_overnight_return_pct", "overnight_direction"),
    )
    rows: list[dict[str, object]] = []
    for index_key, index_df in index_weekly_panel_df.groupby("index_key", sort=True):
        for metric, metric_family in metric_specs:
            sq_values = _numeric_values(index_df[index_df["is_sq_week"]][metric])
            other_values = _numeric_values(index_df[~index_df["is_sq_week"]][metric])
            if len(sq_values) < min_observations or len(other_values) < min_observations:
                continue
            rows.append(
                _comparison_row(
                    group_key=str(index_key),
                    group_label=str(index_df["index_name"].dropna().iloc[0]),
                    metric=metric,
                    metric_family=metric_family,
                    sq_values=sq_values,
                    other_values=other_values,
                )
            )
    return pd.DataFrame(rows, columns=_COMPARISON_COLUMNS)


def _build_index_daily_summary_df(index_daily_panel_df: pd.DataFrame) -> pd.DataFrame:
    metric_specs = (
        ("close_to_close_return_pct", "daily_direction"),
        ("open_to_close_return_pct", "daily_intraday"),
        ("overnight_return_pct", "daily_overnight"),
        ("intraday_range_pct", "daily_range"),
    )
    rows: list[dict[str, object]] = []
    for (index_key, day_of_week), day_df in index_daily_panel_df.groupby(
        ["index_key", "day_of_week"],
        sort=True,
    ):
        for metric, metric_family in metric_specs:
            sq_values = _numeric_values(day_df[day_df["is_sq_week"]][metric])
            other_values = _numeric_values(day_df[~day_df["is_sq_week"]][metric])
            if len(sq_values) < 10 or len(other_values) < 10:
                continue
            rows.append(
                _comparison_row(
                    group_key=f"{index_key}/{day_of_week}",
                    group_label=f"{index_key} {day_of_week}",
                    metric=metric,
                    metric_family=metric_family,
                    sq_values=sq_values,
                    other_values=other_values,
                )
            )
    return pd.DataFrame(rows, columns=_COMPARISON_COLUMNS)


def _build_options_front_daily_panel(options_front_daily_df: pd.DataFrame) -> pd.DataFrame:
    if options_front_daily_df.empty:
        return options_front_daily_df
    result = options_front_daily_df.copy()
    result["date"] = pd.to_datetime(result["date"], errors="coerce")
    result = result.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    for col in (
        "front_volume",
        "front_open_interest",
        "front_turnover_value",
        "mean_implied_volatility",
        "mean_base_volatility",
        "mean_underlying_price",
        "calendar_days_to_sq",
        "atm_strike_price",
        "atm_underlying_price",
        "atm_distance",
        "atm_implied_volatility",
        "atm_base_volatility",
        "atm_open_interest",
        "atm_volume",
    ):
        if col in result.columns:
            result[col] = pd.to_numeric(result[col], errors="coerce")
    result["front_open_interest_change_pct"] = (
        result["front_open_interest"] / result["front_open_interest"].shift(1) - 1.0
    ) * 100.0
    result["atm_implied_volatility_change"] = result["atm_implied_volatility"].diff()
    return _add_sq_calendar_columns(result)


def _build_options_weekly_panel(options_front_daily_df: pd.DataFrame) -> pd.DataFrame:
    if options_front_daily_df.empty:
        return pd.DataFrame()
    rows: list[dict[str, object]] = []
    for week_start, group in options_front_daily_df.groupby("week_start", sort=True):
        g = group.sort_values("date").copy()
        if g.empty:
            continue
        first = g.iloc[0]
        last = g.iloc[-1]
        first_oi = finite_float_or_none(first["front_open_interest"])
        last_oi = finite_float_or_none(last["front_open_interest"])
        first_iv = finite_float_or_none(first["atm_implied_volatility"])
        last_iv = finite_float_or_none(last["atm_implied_volatility"])
        rows.append(
            {
                "week_start": str(week_start),
                "week_end": str(first["week_end"]),
                "sq_date": str(first["sq_date"]),
                "is_sq_week": bool(first["is_sq_week"]),
                "trading_day_count": int(len(g)),
                "front_volume_sum": float(
                    pd.to_numeric(g["front_volume"], errors="coerce").sum()
                ),
                "front_turnover_value_sum": float(
                    pd.to_numeric(g["front_turnover_value"], errors="coerce").sum()
                ),
                "front_open_interest_mean": float(
                    pd.to_numeric(g["front_open_interest"], errors="coerce").mean()
                ),
                "front_open_interest_change_pct": (
                    (last_oi / first_oi - 1.0) * 100.0
                    if first_oi and last_oi and first_oi > 0
                    else np.nan
                ),
                "atm_implied_volatility_mean": float(
                    pd.to_numeric(g["atm_implied_volatility"], errors="coerce").mean()
                ),
                "atm_implied_volatility_change": (
                    last_iv - first_iv
                    if first_iv is not None and last_iv is not None
                    else np.nan
                ),
                "min_calendar_days_to_sq": float(
                    pd.to_numeric(g["calendar_days_to_sq"], errors="coerce").min()
                ),
                "mean_calendar_days_to_sq": float(
                    pd.to_numeric(g["calendar_days_to_sq"], errors="coerce").mean()
                ),
            }
        )
    return pd.DataFrame(rows)


def _build_options_weekly_summary_df(options_weekly_panel_df: pd.DataFrame) -> pd.DataFrame:
    if options_weekly_panel_df.empty:
        return pd.DataFrame(columns=_COMPARISON_COLUMNS)
    metric_specs = (
        ("front_volume_sum", "option_activity"),
        ("front_turnover_value_sum", "option_activity"),
        ("front_open_interest_mean", "option_positioning"),
        ("front_open_interest_change_pct", "option_positioning"),
        ("atm_implied_volatility_mean", "option_iv"),
        ("atm_implied_volatility_change", "option_iv"),
    )
    rows: list[dict[str, object]] = []
    for metric, metric_family in metric_specs:
        sq_values = _numeric_values(options_weekly_panel_df[options_weekly_panel_df["is_sq_week"]][metric])
        other_values = _numeric_values(options_weekly_panel_df[~options_weekly_panel_df["is_sq_week"]][metric])
        if len(sq_values) < 5 or len(other_values) < 5:
            continue
        rows.append(
            _comparison_row(
                group_key="front_n225_options",
                group_label="Front Nikkei 225 options",
                metric=metric,
                metric_family=metric_family,
                sq_values=sq_values,
                other_values=other_values,
            )
        )
    return pd.DataFrame(rows, columns=_COMPARISON_COLUMNS)


def _build_options_days_to_sq_summary_df(
    options_front_daily_df: pd.DataFrame,
) -> pd.DataFrame:
    if options_front_daily_df.empty:
        return pd.DataFrame(columns=_OPTIONS_DAYS_TO_SQ_COLUMNS)
    rows: list[dict[str, object]] = []
    metrics = (
        "front_volume",
        "front_open_interest",
        "front_turnover_value",
        "atm_implied_volatility",
        "atm_implied_volatility_change",
    )
    frame = options_front_daily_df[
        options_front_daily_df["calendar_days_to_sq"].between(0, 30, inclusive="both")
    ].copy()
    for days_to_sq, group in frame.groupby("calendar_days_to_sq", sort=True):
        for metric in metrics:
            values = _numeric_values(group[metric])
            if values.empty:
                continue
            rows.append(
                {
                    "calendar_days_to_sq": int(cast(Any, days_to_sq)),
                    "metric": metric,
                    "observation_count": int(len(values)),
                    "mean": float(values.mean()),
                    "median": float(values.median()),
                    "p10": float(values.quantile(0.10)),
                    "p90": float(values.quantile(0.90)),
                }
            )
    return pd.DataFrame(rows, columns=_OPTIONS_DAYS_TO_SQ_COLUMNS)


def _build_sq_calendar_df(
    index_daily_panel_df: pd.DataFrame,
    options_front_daily_df: pd.DataFrame,
) -> pd.DataFrame:
    frames = []
    if not index_daily_panel_df.empty:
        frames.append(index_daily_panel_df[["date", "week_start", "week_end", "sq_date", "is_sq_week"]])
    if not options_front_daily_df.empty:
        frames.append(options_front_daily_df[["date", "week_start", "week_end", "sq_date", "is_sq_week"]])
    if not frames:
        return pd.DataFrame(columns=["date", "week_start", "week_end", "sq_date", "is_sq_week"])
    result = pd.concat(frames, ignore_index=True).drop_duplicates()
    return result.sort_values(["date", "week_start"]).reset_index(drop=True)


def _add_sq_calendar_columns(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    dates = pd.to_datetime(result["date"], errors="coerce")
    week_starts = dates - pd.to_timedelta(dates.dt.weekday, unit="D")
    week_ends = week_starts + pd.Timedelta(days=4)
    sq_dates = dates.map(lambda value: pd.Timestamp(_second_friday(value.date())))
    result["date"] = dates.dt.strftime("%Y-%m-%d")
    result["week_start"] = week_starts.dt.strftime("%Y-%m-%d")
    result["week_end"] = week_ends.dt.strftime("%Y-%m-%d")
    result["sq_date"] = sq_dates.dt.strftime("%Y-%m-%d")
    result["is_sq_week"] = (sq_dates >= week_starts) & (sq_dates <= week_ends)
    result["day_of_week"] = dates.dt.day_name()
    result["days_to_monthly_sq"] = (sq_dates - dates).dt.days
    return result


def _second_friday(value: date) -> date:
    first_day = value.replace(day=1)
    days_until_friday = (4 - first_day.weekday()) % 7
    return first_day + timedelta(days=days_until_friday + 7)


def _comparison_row(
    *,
    group_key: str,
    group_label: str,
    metric: str,
    metric_family: str,
    sq_values: pd.Series,
    other_values: pd.Series,
) -> dict[str, object]:
    sq_mean = float(sq_values.mean())
    other_mean = float(other_values.mean())
    diff = sq_mean - other_mean
    other_std = float(other_values.std(ddof=1)) if len(other_values) > 1 else np.nan
    pooled_std = _pooled_std(sq_values, other_values)
    t_stat, p_value = _welch_t_normal_approx(sq_values, other_values)
    return {
        "group_key": group_key,
        "group_label": group_label,
        "metric": metric,
        "metric_family": metric_family,
        "sq_observation_count": int(len(sq_values)),
        "other_observation_count": int(len(other_values)),
        "sq_mean": sq_mean,
        "other_mean": other_mean,
        "difference": diff,
        "relative_difference_pct": diff / abs(other_mean) * 100.0
        if other_mean != 0
        else np.nan,
        "effect_size_vs_other_std": diff / other_std
        if np.isfinite(other_std) and other_std > 0
        else np.nan,
        "cohens_d": diff / pooled_std
        if np.isfinite(pooled_std) and pooled_std > 0
        else np.nan,
        "sq_median": float(sq_values.median()),
        "other_median": float(other_values.median()),
        "sq_p10": float(sq_values.quantile(0.10)),
        "other_p10": float(other_values.quantile(0.10)),
        "sq_p90": float(sq_values.quantile(0.90)),
        "other_p90": float(other_values.quantile(0.90)),
        "welch_t_stat": t_stat,
        "normal_approx_p_value": p_value,
    }


def _numeric_values(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").dropna()


def _pooled_std(left: pd.Series, right: pd.Series) -> float:
    if len(left) < 2 or len(right) < 2:
        return float("nan")
    left_var = float(left.var(ddof=1))
    right_var = float(right.var(ddof=1))
    denom = len(left) + len(right) - 2
    if denom <= 0:
        return float("nan")
    return math.sqrt(((len(left) - 1) * left_var + (len(right) - 1) * right_var) / denom)


def _welch_t_normal_approx(left: pd.Series, right: pd.Series) -> tuple[float, float]:
    if len(left) < 2 or len(right) < 2:
        return float("nan"), float("nan")
    left_var = float(left.var(ddof=1))
    right_var = float(right.var(ddof=1))
    standard_error = math.sqrt(left_var / len(left) + right_var / len(right))
    if standard_error <= 0:
        return float("nan"), float("nan")
    t_stat = (float(left.mean()) - float(right.mean())) / standard_error
    p_value = math.erfc(abs(t_stat) / math.sqrt(2.0))
    return float(t_stat), float(p_value)


def _empty_options_front_daily_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "date",
            "front_sq_date",
            "option_row_count",
            "front_volume",
            "front_open_interest",
            "front_turnover_value",
            "mean_implied_volatility",
            "mean_base_volatility",
            "mean_underlying_price",
            "calendar_days_to_sq",
            "atm_strike_price",
            "atm_underlying_price",
            "atm_distance",
            "atm_implied_volatility",
            "atm_base_volatility",
            "atm_open_interest",
            "atm_volume",
        ]
    )


def _build_summary_markdown(result: IndexSqWeekEffectResearchResult) -> str:
    lines = [
        "# Index SQ Week Effect Research",
        "",
        f"- DB: `{result.db_path}`",
        f"- Source: `{result.source_mode}` ({result.source_detail})",
        f"- Analysis range: `{result.analysis_start_date}` to `{result.analysis_end_date}`",
        f"- Index observations: `{result.index_observation_count}`",
        f"- Index weeks: `{result.index_week_count}`",
        f"- SQ weeks: `{result.sq_week_count}`",
        f"- Option daily observations: `{result.option_observation_count}`",
        "",
        "## Policy",
        "",
        result.research_policy,
        "",
        "## Index Weekly Differences",
        "",
        _format_top_comparison_rows(result.index_weekly_summary_df, top_n=16),
        "",
        "## Option Weekly Differences",
        "",
        _format_top_comparison_rows(result.options_weekly_summary_df, top_n=12),
        "",
        "## Output Tables",
        "",
    ]
    for table_name in _RESULT_TABLE_NAMES:
        table = getattr(result, table_name)
        lines.append(f"- `{table_name}`: `{len(table)}` rows")
    return "\n".join(lines) + "\n"


def _format_top_comparison_rows(summary_df: pd.DataFrame, *, top_n: int) -> str:
    if summary_df.empty:
        return "No comparison rows."
    rows = summary_df.copy()
    rows["_abs_effect"] = pd.to_numeric(
        rows["effect_size_vs_other_std"],
        errors="coerce",
    ).abs()
    rows = rows.sort_values("_abs_effect", ascending=False).head(top_n)
    lines = [
        "| Group | Metric | SQ n | Other n | SQ mean | Other mean | Diff | Effect | p |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in rows.to_dict(orient="records"):
        lines.append(
            "| "
            f"`{row['group_label']}` | "
            f"`{row['metric']}` | "
            f"`{int(row['sq_observation_count'])}` | "
            f"`{int(row['other_observation_count'])}` | "
            f"`{_fmt(row['sq_mean'])}` | "
            f"`{_fmt(row['other_mean'])}` | "
            f"`{_fmt(row['difference'])}` | "
            f"`{_fmt(row['effect_size_vs_other_std'])}` | "
            f"`{_fmt(row['normal_approx_p_value'], digits=4)}` |"
        )
    return "\n".join(lines)


def _fmt(value: object, digits: int = 2) -> str:
    number = finite_float_or_none(value)
    if number is None:
        return "nan"
    return f"{number:.{digits}f}"


def write_index_sq_week_effect_research_bundle(
    result: IndexSqWeekEffectResearchResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=INDEX_SQ_WEEK_EFFECT_RESEARCH_EXPERIMENT_ID,
        module=__name__,
        function="run_index_sq_week_effect_research",
        params={
            "db_path": result.db_path,
            "min_week_observations": result.min_week_observations,
        },
        result=result,
        table_field_names=_RESULT_TABLE_NAMES,
        summary_markdown=_build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_index_sq_week_effect_research_bundle(
    bundle_path: str | Path,
) -> IndexSqWeekEffectResearchResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=IndexSqWeekEffectResearchResult,
        table_field_names=_RESULT_TABLE_NAMES,
    )


def get_index_sq_week_effect_research_latest_bundle_path(
    *,
    output_root: str | Path | None = None,
) -> Path | None:
    return find_latest_research_bundle_path(
        INDEX_SQ_WEEK_EFFECT_RESEARCH_EXPERIMENT_ID,
        output_root=output_root,
    )


def get_index_sq_week_effect_research_bundle_path_for_run_id(
    run_id: str,
    *,
    output_root: str | Path | None = None,
) -> Path:
    return get_research_bundle_dir(
        INDEX_SQ_WEEK_EFFECT_RESEARCH_EXPERIMENT_ID,
        run_id,
        output_root=output_root,
    )


__all__: Sequence[str] = (
    "DEFAULT_MIN_WEEK_OBSERVATIONS",
    "INDEX_SQ_WEEK_EFFECT_RESEARCH_EXPERIMENT_ID",
    "IndexSqWeekEffectResearchResult",
    "get_index_sq_week_effect_research_bundle_path_for_run_id",
    "get_index_sq_week_effect_research_latest_bundle_path",
    "load_index_sq_week_effect_research_bundle",
    "run_index_sq_week_effect_research",
    "write_index_sq_week_effect_research_bundle",
)
