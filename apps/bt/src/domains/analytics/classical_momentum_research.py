"""Classical cross-sectional momentum portfolio research."""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from src.domains.analytics.annual_value_composite_selection import _daily_stats, _series_mean
from src.domains.analytics.readonly_duckdb_support import (
    SourceMode,
    fetch_date_range,
    normalize_code_sql,
    open_readonly_analysis_connection,
)
from src.domains.analytics.research_core import (
    UNIVERSE_LABELS,
    build_market_universe_case_sql,
    normalize_positive_int_sequence,
    research_universe_market_codes,
    sort_research_table,
    sql_string_list,
    warmup_start_date,
)
from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    load_dataclass_research_bundle,
    write_dataclass_research_bundle,
)
from src.domains.analytics.topix_rank_future_close_core import _default_start_date
from src.shared.utils.pandas_type_guards import (
    finite_float_or_none,
    int_or_none,
    required_float,
    required_int,
)

CLASSICAL_MOMENTUM_RESEARCH_EXPERIMENT_ID = "market-behavior/classical-momentum-research"

DEFAULT_LOOKBACK_YEARS = 10
DEFAULT_LOOKBACK_SPECS: tuple[tuple[int, int], ...] = (
    (63, 5),
    (126, 20),
    (252, 20),
)
DEFAULT_HOLD_SESSIONS: tuple[int, ...] = (20, 60)
DEFAULT_SELECTION_FRACTIONS: tuple[float, ...] = (0.05, 0.10)
DEFAULT_REBALANCE_INTERVAL_SESSIONS = 20
DEFAULT_MIN_AVG_TRADING_VALUE_MIL_JPY = 10.0

TABLE_FIELD_NAMES: tuple[str, ...] = (
    "universe_summary_df",
    "selected_event_df",
    "portfolio_daily_df",
    "portfolio_summary_df",
)


@dataclass(frozen=True)
class ClassicalMomentumResearchResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    default_start_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    lookback_years: int
    lookback_specs: tuple[tuple[int, int], ...]
    hold_sessions: tuple[int, ...]
    selection_fractions: tuple[float, ...]
    rebalance_interval_sessions: int
    min_avg_trading_value_mil_jpy: float
    universe_summary_df: pd.DataFrame
    selected_event_df: pd.DataFrame
    portfolio_daily_df: pd.DataFrame
    portfolio_summary_df: pd.DataFrame


def _sort_table(df: pd.DataFrame) -> pd.DataFrame:
    return sort_research_table(
        df,
        sort_columns=(
            "lookback_sessions",
            "skip_sessions",
            "hold_sessions",
            "selection_fraction",
            "signal_date",
            "date",
            "selection_rank",
            "code",
        ),
    )


def _normalize_lookback_specs(
    values: tuple[tuple[int, int], ...] | list[tuple[int, int]] | None,
) -> tuple[tuple[int, int], ...]:
    raw_values = DEFAULT_LOOKBACK_SPECS if values is None else tuple(values)
    normalized: list[tuple[int, int]] = []
    for raw_lookback, raw_skip in raw_values:
        lookback = int(raw_lookback)
        skip = int(raw_skip)
        if lookback <= 1:
            raise ValueError("lookback sessions must be greater than 1")
        if skip < 0:
            raise ValueError("skip sessions must be non-negative")
        if skip >= lookback:
            raise ValueError("skip sessions must be smaller than lookback sessions")
        spec = (lookback, skip)
        if spec not in normalized:
            normalized.append(spec)
    if not normalized:
        raise ValueError("at least one lookback spec is required")
    return tuple(sorted(normalized))


def _normalize_selection_fractions(
    values: tuple[float, ...] | list[float] | None,
) -> tuple[float, ...]:
    raw_values = DEFAULT_SELECTION_FRACTIONS if values is None else tuple(values)
    normalized: list[float] = []
    for raw_value in raw_values:
        value = float(raw_value)
        if not (0.0 < value <= 1.0):
            raise ValueError("selection fractions must satisfy 0 < fraction <= 1")
        if value not in normalized:
            normalized.append(value)
    if not normalized:
        raise ValueError("at least one selection fraction is required")
    return tuple(sorted(normalized))


def _warmup_start_date(
    analysis_start_date: str | None,
    available_start_date: str | None,
    *,
    lookback_specs: tuple[tuple[int, int], ...],
) -> str | None:
    return warmup_start_date(
        analysis_start_date,
        available_start_date,
        warmup_sessions=max(lookback for lookback, _ in lookback_specs),
        session_to_calendar_multiplier=2.1,
    )


def _create_panel_table(
    conn: Any,
    *,
    raw_start_date: str | None,
    analysis_start_date: str | None,
    analysis_end_date: str | None,
    lookback_specs: tuple[tuple[int, int], ...],
    hold_sessions: tuple[int, ...],
) -> None:
    price_code = normalize_code_sql("sd.code")
    master_code = normalize_code_sql("smd.code")
    all_market_codes = research_universe_market_codes()
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
    lag_close_exprs = ",\n                ".join(
        [
            *[
                f"lag(close, {lookback}) over (partition by code order by date) "
                f"as close_lag_{lookback}d"
                for lookback, _ in lookback_specs
            ],
            *[
                f"lag(close, {skip}) over (partition by code order by date) "
                f"as close_lag_{skip}d"
                for skip in sorted({skip for _, skip in lookback_specs if skip > 0})
            ],
        ]
    )
    momentum_exprs = ",\n            ".join(
        f"case when close_lag_{lookback}d > 0 then "
        f"{('close' if skip == 0 else f'close_lag_{skip}d')} / close_lag_{lookback}d - 1 end "
        f"as momentum_return_{lookback}_{skip}"
        for lookback, skip in lookback_specs
    )
    future_exprs = ",\n                ".join(
        [
            *[
                f"lead(date, {hold}) over (partition by code order by date) as future_date_{hold}d"
                for hold in hold_sessions
            ],
            *[
                f"lead(close, {hold}) over (partition by code order by date) "
                f"as future_close_{hold}d"
                for hold in hold_sessions
            ],
        ]
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE classical_momentum_panel AS
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
                smd.company_name,
                smd.market_code,
                smd.scale_category,
                {build_market_universe_case_sql(market_code_column="smd.market_code", scale_category_column="smd.scale_category")}
                    AS universe_key,
                row_number() OVER (
                    PARTITION BY {master_code}, smd.date
                    ORDER BY CASE WHEN length(smd.code) = 4 THEN 0 ELSE 1 END, smd.code
                ) AS row_rank
            FROM stock_master_daily smd
            WHERE smd.market_code IN ({sql_string_list(all_market_codes)})
        ),
        scoped AS (
            SELECT p.*, m.company_name, m.market_code, m.scale_category, m.universe_key
            FROM prices p
            JOIN master m ON m.code = p.code AND m.date = p.date AND m.row_rank = 1
            WHERE m.universe_key IS NOT NULL
        ),
        featured AS (
            SELECT
                *,
                lead(date, 1) over (partition by code order by date) as next_date,
                lead(open, 1) over (partition by code order by date) as next_open,
                avg(volume * close) over (
                    partition by code order by date
                    rows between 60 preceding and 1 preceding
                ) / 1000000.0 as avg_trading_value_60d_mil_jpy,
                {lag_close_exprs},
                {future_exprs}
            FROM scoped
        )
        SELECT
            *,
            {momentum_exprs}
        FROM featured
        {final_where}
        """,
        [*raw_params, *final_params],
    )


def _build_universe_summary(conn: Any) -> pd.DataFrame:
    frame = conn.execute(
        """
        SELECT
            universe_key,
            count(*) AS stock_day_count,
            count(DISTINCT code) AS unique_code_count,
            count(DISTINCT date) AS analysis_date_count
        FROM classical_momentum_panel
        GROUP BY universe_key
        """
    ).fetchdf()
    if frame.empty:
        return frame
    frame["universe_label"] = frame["universe_key"].map(UNIVERSE_LABELS)
    return _sort_table(frame)


def _build_selected_event_df(
    conn: Any,
    *,
    lookback_specs: tuple[tuple[int, int], ...],
    hold_sessions: tuple[int, ...],
    selection_fractions: tuple[float, ...],
    rebalance_interval_sessions: int,
    min_avg_trading_value_mil_jpy: float,
) -> pd.DataFrame:
    columns = [
        "universe_key",
        "universe_label",
        "lookback_sessions",
        "skip_sessions",
        "momentum_label",
        "hold_sessions",
        "selection_fraction",
        "signal_date",
        "entry_date",
        "exit_date",
        "code",
        "company_name",
        "selection_rank",
        "universe_count",
        "selection_count_target",
        "momentum_return",
        "avg_trading_value_60d_mil_jpy",
        "entry_open",
        "exit_close",
        "forward_return",
    ]
    frames: list[pd.DataFrame] = []
    for lookback, skip in lookback_specs:
        score_column = f"momentum_return_{lookback}_{skip}"
        label = f"{lookback}d_skip_{skip}d"
        for hold in hold_sessions:
            for fraction in selection_fractions:
                frame = conn.execute(
                    f"""
                    WITH rebalance_dates AS (
                        SELECT date
                        FROM (
                            SELECT
                                date,
                                row_number() OVER (ORDER BY date) AS row_num
                            FROM (
                                SELECT DISTINCT date
                                FROM classical_momentum_panel
                            )
                        )
                        WHERE ((row_num - 1) % ?) = 0
                    ),
                    eligible AS (
                        SELECT
                            p.*,
                            count(*) OVER (PARTITION BY p.universe_key, p.date) AS universe_count,
                            row_number() OVER (
                                PARTITION BY p.universe_key, p.date
                                ORDER BY p.{score_column} DESC NULLS LAST, p.code
                            ) AS selection_rank
                        FROM classical_momentum_panel p
                        JOIN rebalance_dates rd ON rd.date = p.date
                        WHERE p.{score_column} IS NOT NULL
                          AND p.next_date IS NOT NULL
                          AND p.next_open > 0
                          AND p.future_date_{hold}d IS NOT NULL
                          AND p.future_close_{hold}d > 0
                          AND coalesce(p.avg_trading_value_60d_mil_jpy, 0) >= ?
                    ),
                    selected AS (
                        SELECT
                            *,
                            greatest(1, ceil(universe_count * ?))::INTEGER AS selection_count_target
                        FROM eligible
                    )
                    SELECT
                        universe_key,
                        '{UNIVERSE_LABELS.get("topix500", "TOPIX500")}' AS unused_label,
                        {lookback} AS lookback_sessions,
                        {skip} AS skip_sessions,
                        '{label}' AS momentum_label,
                        {hold} AS hold_sessions,
                        {fraction} AS selection_fraction,
                        date AS signal_date,
                        next_date AS entry_date,
                        future_date_{hold}d AS exit_date,
                        code,
                        company_name,
                        selection_rank,
                        universe_count,
                        selection_count_target,
                        {score_column} AS momentum_return,
                        avg_trading_value_60d_mil_jpy,
                        next_open AS entry_open,
                        future_close_{hold}d AS exit_close,
                        future_close_{hold}d / next_open - 1 AS forward_return
                    FROM selected
                    WHERE selection_rank <= selection_count_target
                    """,
                    [int(rebalance_interval_sessions), float(min_avg_trading_value_mil_jpy), float(fraction)],
                ).fetchdf()
                if frame.empty:
                    continue
                frame = frame.drop(columns=["unused_label"])
                frame["universe_label"] = frame["universe_key"].map(UNIVERSE_LABELS)
                frames.append(frame)
    if not frames:
        return pd.DataFrame(columns=columns)
    result = pd.concat(frames, ignore_index=True, sort=False)
    for column in columns:
        if column not in result.columns:
            result[column] = None
    return _sort_table(result[columns])


def _build_portfolio_daily_df(
    conn: Any,
    selected_event_df: pd.DataFrame,
    *,
    analysis_start_date: str | None,
    analysis_end_date: str | None,
) -> pd.DataFrame:
    columns = [
        "universe_key",
        "universe_label",
        "lookback_sessions",
        "skip_sessions",
        "momentum_label",
        "hold_sessions",
        "selection_fraction",
        "date",
        "active_positions",
        "mean_daily_return",
        "mean_daily_return_pct",
        "portfolio_value",
        "drawdown_pct",
    ]
    if selected_event_df.empty:
        return pd.DataFrame(columns=columns)
    event_df = selected_event_df.copy()
    conn.register("classical_momentum_selected_events_input", event_df)
    start_date = analysis_start_date or str(event_df["entry_date"].min())
    end_date = analysis_end_date or str(event_df["exit_date"].max())
    price_code = normalize_code_sql("sd.code")
    daily = conn.execute(
        f"""
        WITH raw_prices AS (
            SELECT
                {price_code} AS code,
                sd.date,
                sd.close,
                row_number() OVER (
                    PARTITION BY {price_code}, sd.date
                    ORDER BY CASE WHEN length(sd.code) = 4 THEN 0 ELSE 1 END, sd.code
                ) AS row_rank
            FROM stock_data sd
            WHERE sd.date >= ?
              AND sd.date <= (SELECT max(exit_date) FROM classical_momentum_selected_events_input)
              AND sd.close > 0
        ),
        prices AS (
            SELECT
                code,
                date,
                close,
                lag(close) OVER (PARTITION BY code ORDER BY date) AS prev_close
            FROM raw_prices
            WHERE row_rank = 1
        ),
        active_daily AS (
            SELECT
                e.universe_key,
                e.universe_label,
                e.lookback_sessions,
                e.skip_sessions,
                e.momentum_label,
                e.hold_sessions,
                e.selection_fraction,
                p.date,
                count(*) AS active_positions,
                avg(
                    CASE
                        WHEN p.date = e.entry_date THEN p.close / nullif(e.entry_open, 0) - 1
                        ELSE p.close / nullif(p.prev_close, 0) - 1
                    END
                ) AS mean_daily_return
            FROM classical_momentum_selected_events_input e
            JOIN prices p
              ON p.code = e.code
             AND p.date >= e.entry_date
             AND p.date <= e.exit_date
            GROUP BY
                e.universe_key,
                e.universe_label,
                e.lookback_sessions,
                e.skip_sessions,
                e.momentum_label,
                e.hold_sessions,
                e.selection_fraction,
                p.date
        ),
        configs AS (
            SELECT DISTINCT
                universe_key,
                universe_label,
                lookback_sessions,
                skip_sessions,
                momentum_label,
                hold_sessions,
                selection_fraction
            FROM classical_momentum_selected_events_input
        ),
        calendar AS (
            SELECT date
            FROM topix_data
            WHERE date >= ?
              AND date <= ?
        ),
        dense_daily AS (
            SELECT
                c.universe_key,
                c.universe_label,
                c.lookback_sessions,
                c.skip_sessions,
                c.momentum_label,
                c.hold_sessions,
                c.selection_fraction,
                cal.date,
                coalesce(a.active_positions, 0) AS active_positions,
                coalesce(a.mean_daily_return, 0.0) AS mean_daily_return
            FROM configs c
            CROSS JOIN calendar cal
            LEFT JOIN active_daily a
              ON a.universe_key = c.universe_key
             AND a.lookback_sessions = c.lookback_sessions
             AND a.skip_sessions = c.skip_sessions
             AND a.hold_sessions = c.hold_sessions
             AND abs(a.selection_fraction - c.selection_fraction) < 0.0000001
             AND a.date = cal.date
        )
        SELECT
            *,
            mean_daily_return * 100.0 AS mean_daily_return_pct
        FROM dense_daily
        ORDER BY
            universe_key,
            lookback_sessions,
            skip_sessions,
            hold_sessions,
            selection_fraction,
            date
        """,
        [start_date, start_date, end_date],
    ).fetchdf()
    conn.unregister("classical_momentum_selected_events_input")
    if daily.empty:
        return pd.DataFrame(columns=columns)
    daily["portfolio_value"] = pd.NA
    daily["drawdown_pct"] = pd.NA
    group_columns = [
        "universe_key",
        "lookback_sessions",
        "skip_sessions",
        "hold_sessions",
        "selection_fraction",
    ]
    for _, group in daily.groupby(group_columns, sort=False):
        idx = list(group.index)
        values = (1.0 + pd.to_numeric(daily.loc[idx, "mean_daily_return"])).cumprod()
        peaks = values.cummax()
        daily.loc[idx, "portfolio_value"] = values.to_numpy()
        daily.loc[idx, "drawdown_pct"] = ((values / peaks - 1.0) * 100.0).to_numpy()
    for column in columns:
        if column not in daily.columns:
            daily[column] = None
    return _sort_table(daily[columns])


def _build_portfolio_summary_df(
    portfolio_daily_df: pd.DataFrame,
    selected_event_df: pd.DataFrame,
) -> pd.DataFrame:
    columns = [
        "universe_key",
        "universe_label",
        "lookback_sessions",
        "skip_sessions",
        "momentum_label",
        "hold_sessions",
        "selection_fraction",
        "event_count",
        "unique_code_count",
        "active_days",
        "avg_active_positions",
        "max_active_positions",
        "mean_momentum_return_pct",
        "mean_forward_return_pct",
        "win_rate_pct",
        "total_return_pct",
        "cagr_pct",
        "max_drawdown_pct",
        "annualized_volatility_pct",
        "sharpe_ratio",
        "sortino_ratio",
        "calmar_ratio",
    ]
    if portfolio_daily_df.empty:
        return pd.DataFrame(columns=columns)
    group_columns = [
        "universe_key",
        "lookback_sessions",
        "skip_sessions",
        "hold_sessions",
        "selection_fraction",
    ]
    event_stats = {
        tuple(keys): group
        for keys, group in selected_event_df.groupby(group_columns, sort=False)
    }
    records: list[dict[str, Any]] = []
    for keys, group in portfolio_daily_df.groupby(group_columns, sort=False):
        event_group = event_stats.get(tuple(keys), pd.DataFrame())
        total_return = float(group["portfolio_value"].iloc[-1] - 1.0)
        start_date = str(group["date"].iloc[0])
        end_date = str(group["date"].iloc[-1])
        period_days = (pd.Timestamp(end_date) - pd.Timestamp(start_date)).days
        cagr = None
        if period_days > 0 and total_return > -1.0:
            cagr_value = (1.0 + total_return) ** (365.25 / period_days) - 1.0
            cagr = float(cagr_value) if math.isfinite(cagr_value) else None
        drawdown = pd.to_numeric(group["drawdown_pct"], errors="coerce").min()
        max_drawdown_pct = float(drawdown) if pd.notna(drawdown) else None
        momentum_returns = (
            pd.to_numeric(event_group["momentum_return"], errors="coerce").dropna()
            if "momentum_return" in event_group
            else pd.Series(dtype="float64")
        )
        forward_returns = (
            pd.to_numeric(event_group["forward_return"], errors="coerce").dropna()
            if "forward_return" in event_group
            else pd.Series(dtype="float64")
        )
        records.append(
            {
                "universe_key": keys[0],
                "universe_label": str(group["universe_label"].iloc[0]),
                "lookback_sessions": required_int(keys[1], field="lookback_sessions"),
                "skip_sessions": required_int(keys[2], field="skip_sessions"),
                "momentum_label": str(group["momentum_label"].iloc[0]),
                "hold_sessions": required_int(keys[3], field="hold_sessions"),
                "selection_fraction": required_float(
                    keys[4], field="selection_fraction"
                ),
                "event_count": int(len(event_group)),
                "unique_code_count": int(event_group["code"].nunique()) if not event_group.empty else 0,
                "active_days": int((pd.to_numeric(group["active_positions"]) > 0).sum()),
                "avg_active_positions": _series_mean(group["active_positions"]),
                "max_active_positions": int(pd.to_numeric(group["active_positions"]).max()),
                "mean_momentum_return_pct": (
                    float(momentum_returns.mean() * 100.0) if not momentum_returns.empty else None
                ),
                "mean_forward_return_pct": (
                    float(forward_returns.mean() * 100.0) if not forward_returns.empty else None
                ),
                "win_rate_pct": (
                    float((forward_returns > 0.0).mean() * 100.0)
                    if not forward_returns.empty
                    else None
                ),
                "total_return_pct": total_return * 100.0,
                "cagr_pct": cagr * 100.0 if cagr is not None else None,
                "max_drawdown_pct": max_drawdown_pct,
                **_daily_stats(group["mean_daily_return"]),
                "calmar_ratio": (
                    cagr / abs(max_drawdown_pct / 100.0)
                    if cagr is not None and max_drawdown_pct is not None and max_drawdown_pct < -1e-12
                    else None
                ),
            }
        )
    return _sort_table(pd.DataFrame(records)[columns])


def run_classical_momentum_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    lookback_years: int = DEFAULT_LOOKBACK_YEARS,
    lookback_specs: tuple[tuple[int, int], ...] | list[tuple[int, int]] | None = None,
    hold_sessions: tuple[int, ...] | list[int] | None = None,
    selection_fractions: tuple[float, ...] | list[float] | None = None,
    rebalance_interval_sessions: int = DEFAULT_REBALANCE_INTERVAL_SESSIONS,
    min_avg_trading_value_mil_jpy: float = DEFAULT_MIN_AVG_TRADING_VALUE_MIL_JPY,
) -> ClassicalMomentumResearchResult:
    normalized_specs = _normalize_lookback_specs(lookback_specs)
    normalized_holds = normalize_positive_int_sequence(
        hold_sessions,
        fallback=DEFAULT_HOLD_SESSIONS,
        name="hold_sessions",
    )
    normalized_fractions = _normalize_selection_fractions(selection_fractions)
    if rebalance_interval_sessions <= 0:
        raise ValueError("rebalance_interval_sessions must be positive")
    if min_avg_trading_value_mil_jpy < 0:
        raise ValueError("min_avg_trading_value_mil_jpy must be non-negative")
    with open_readonly_analysis_connection(
        db_path,
        snapshot_prefix="classical-momentum-",
    ) as ctx:
        conn = ctx.connection
        available_start_date, available_end_date = fetch_date_range(
            conn,
            table_name="stock_data",
        )
        default_start_date = _default_start_date(
            available_start_date=available_start_date,
            available_end_date=available_end_date,
            lookback_years=lookback_years,
        )
        analysis_start_date = start_date or default_start_date
        analysis_end_date = end_date or available_end_date
        raw_start_date = _warmup_start_date(
            analysis_start_date,
            available_start_date,
            lookback_specs=normalized_specs,
        )
        _create_panel_table(
            conn,
            raw_start_date=raw_start_date,
            analysis_start_date=analysis_start_date,
            analysis_end_date=analysis_end_date,
            lookback_specs=normalized_specs,
            hold_sessions=normalized_holds,
        )
        universe_summary_df = _build_universe_summary(conn)
        selected_event_df = _build_selected_event_df(
            conn,
            lookback_specs=normalized_specs,
            hold_sessions=normalized_holds,
            selection_fractions=normalized_fractions,
            rebalance_interval_sessions=int(rebalance_interval_sessions),
            min_avg_trading_value_mil_jpy=float(min_avg_trading_value_mil_jpy),
        )
        portfolio_daily_df = _build_portfolio_daily_df(
            conn,
            selected_event_df,
            analysis_start_date=analysis_start_date,
            analysis_end_date=analysis_end_date,
        )
        portfolio_summary_df = _build_portfolio_summary_df(
            portfolio_daily_df,
            selected_event_df,
        )
    return ClassicalMomentumResearchResult(
        db_path=str(db_path),
        source_mode=ctx.source_mode,
        source_detail=ctx.source_detail,
        available_start_date=available_start_date,
        available_end_date=available_end_date,
        default_start_date=default_start_date,
        analysis_start_date=analysis_start_date,
        analysis_end_date=analysis_end_date,
        lookback_years=lookback_years,
        lookback_specs=normalized_specs,
        hold_sessions=normalized_holds,
        selection_fractions=normalized_fractions,
        rebalance_interval_sessions=int(rebalance_interval_sessions),
        min_avg_trading_value_mil_jpy=float(min_avg_trading_value_mil_jpy),
        universe_summary_df=universe_summary_df,
        selected_event_df=selected_event_df,
        portfolio_daily_df=portfolio_daily_df,
        portfolio_summary_df=portfolio_summary_df,
    )


def _format_int(value: object) -> str:
    number = int_or_none(value)
    if number is None:
        return "-"
    return f"{number:,}"


def _format_number(value: object, *, digits: int = 2, suffix: str = "") -> str:
    number = finite_float_or_none(value)
    if number is None:
        return "-"
    return f"{number:.{digits}f}{suffix}"


def _format_fraction_pct(value: object) -> str:
    number = finite_float_or_none(value)
    if number is None:
        return "-"
    return _format_number(number * 100.0, digits=1, suffix="%")


def _build_research_bundle_summary_markdown(result: ClassicalMomentumResearchResult) -> str:
    lines = [
        "# Classical Momentum Research",
        "",
        "## Parameters",
        "",
        f"- Analysis period: `{result.analysis_start_date}` to `{result.analysis_end_date}`",
        f"- Lookback specs: `{', '.join(f'{lookback}:{skip}' for lookback, skip in result.lookback_specs)}`",
        f"- Hold sessions: `{', '.join(str(value) for value in result.hold_sessions)}`",
        f"- Selection fractions: `{', '.join(f'{value:.2f}' for value in result.selection_fractions)}`",
        f"- Rebalance interval sessions: `{result.rebalance_interval_sessions}`",
        f"- Minimum ADV60: `{result.min_avg_trading_value_mil_jpy:.1f}mn JPY`",
        f"- Source: `{result.source_detail}`",
        "",
        "## Universe Summary",
        "",
    ]
    if result.universe_summary_df.empty:
        lines.append("_No universe rows._")
    else:
        lines.extend(
            [
                "| Universe | Stock-days | Unique codes | Dates |",
                "| --- | ---: | ---: | ---: |",
            ]
        )
        for row in result.universe_summary_df.itertuples(index=False):
            lines.append(
                "| "
                + " | ".join(
                    [
                        f"`{row.universe_key}`",
                        _format_int(row.stock_day_count),
                        _format_int(row.unique_code_count),
                        _format_int(row.analysis_date_count),
                    ]
                )
                + " |"
            )
    lines.extend(["", "## Top Portfolio Rows", ""])
    if result.portfolio_summary_df.empty:
        lines.append("_No portfolio rows._")
    else:
        rows = result.portfolio_summary_df.sort_values(
            ["sharpe_ratio", "cagr_pct", "event_count"],
            ascending=[False, False, False],
            na_position="last",
            kind="stable",
        ).head(24)
        lines.extend(
            [
                "| Universe | Spec | Hold | Top | Events | Avg active | CAGR | Sharpe | MaxDD | Mean fwd |",
                "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for row in rows.itertuples(index=False):
            lines.append(
                "| "
                + " | ".join(
                    [
                        f"`{row.universe_key}`",
                        f"`{row.momentum_label}`",
                        _format_int(row.hold_sessions),
                        _format_fraction_pct(row.selection_fraction),
                        _format_int(row.event_count),
                        _format_number(row.avg_active_positions, digits=1),
                        _format_number(row.cagr_pct, suffix="%"),
                        _format_number(row.sharpe_ratio),
                        _format_number(row.max_drawdown_pct, suffix="%"),
                        _format_number(row.mean_forward_return_pct, suffix="%"),
                    ]
                )
                + " |"
            )
    return "\n".join(lines) + "\n"


def write_classical_momentum_research_bundle(
    result: ClassicalMomentumResearchResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=CLASSICAL_MOMENTUM_RESEARCH_EXPERIMENT_ID,
        module="src.domains.analytics.classical_momentum_research",
        function="run_classical_momentum_research",
        params={
            "lookback_years": result.lookback_years,
            "lookback_specs": [list(spec) for spec in result.lookback_specs],
            "hold_sessions": list(result.hold_sessions),
            "selection_fractions": list(result.selection_fractions),
            "rebalance_interval_sessions": result.rebalance_interval_sessions,
            "min_avg_trading_value_mil_jpy": result.min_avg_trading_value_mil_jpy,
        },
        result=result,
        table_field_names=TABLE_FIELD_NAMES,
        summary_markdown=_build_research_bundle_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_classical_momentum_research_bundle(
    bundle_path: str | Path,
) -> ClassicalMomentumResearchResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=ClassicalMomentumResearchResult,
        table_field_names=TABLE_FIELD_NAMES,
    )
