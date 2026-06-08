"""N-day new-high momentum event study.

The study tests whether stocks that print fresh N-day highs have better
forward returns, and whether volume/price-action and PIT fundamentals improve
the breakout candidate set.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, NamedTuple

import pandas as pd

from src.domains.analytics.annual_value_composite_selection import _daily_stats, _series_mean
from src.domains.analytics.deterministic_sampling import select_deterministic_samples
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
from src.shared.utils.pandas_type_guards import required_int, required_str

NEW_HIGH_MOMENTUM_EXPERIMENT_ID = "market-behavior/new-high-momentum-research"

DEFAULT_LOOKBACK_YEARS = 10
DEFAULT_HIGH_WINDOWS: tuple[int, ...] = (20, 60, 120, 252)
DEFAULT_HORIZONS: tuple[int, ...] = (5, 10, 20, 60)
DEFAULT_SAMPLE_EVENT_SIZE = 40
TECHNICAL_BASELINE_WINDOW = 20
RANGE_WINDOW = 252
MEMBERSHIP_MODE = "stock_master_daily_as_of_price_date"

TABLE_FIELD_NAMES: tuple[str, ...] = (
    "universe_summary_df",
    "new_high_summary_df",
    "top_candidates_df",
    "portfolio_event_df",
    "portfolio_daily_df",
    "portfolio_summary_df",
    "sampled_events_df",
)
PORTFOLIO_CONDITION_KEYS: tuple[str, ...] = (
    "all",
    "low_forward_per_le_10",
    "low_pbr_forward_per_15",
    "annual_value_score_ge_2",
    "annual_value_score_3",
)


class FilterDefinition(NamedTuple):
    family: str
    key: str
    label: str
    expression: str


CONDITION_DEFINITIONS: tuple[FilterDefinition, ...] = (
    FilterDefinition("baseline", "all", "All new-high events", "TRUE"),
    FilterDefinition(
        "technical",
        "close_position_ge_70",
        "Close in upper 30% of daily range",
        "close_position >= 0.70",
    ),
    FilterDefinition(
        "technical",
        "volume_ratio_20d_ge_1_5",
        "Volume / prior 20d average >= 1.5",
        "volume_ratio_20d >= 1.5",
    ),
    FilterDefinition(
        "technical",
        "volume_ratio_20d_ge_2",
        "Volume / prior 20d average >= 2.0",
        "volume_ratio_20d >= 2.0",
    ),
    FilterDefinition(
        "technical",
        "trading_value_ratio_20d_ge_2",
        "Trading value / prior 20d average >= 2.0",
        "trading_value_ratio_20d >= 2.0",
    ),
    FilterDefinition(
        "technical",
        "prior_return_20d_positive",
        "Prior 20d return > 0",
        "prior_return_20d > 0",
    ),
    FilterDefinition(
        "technical",
        "prior_return_20d_ge_10",
        "Prior 20d return >= 10%",
        "prior_return_20d >= 0.10",
    ),
    FilterDefinition(
        "technical",
        "range_position_252d_ge_80",
        "Close in top 20% of prior 252d range",
        "range_position_252d >= 0.80",
    ),
    FilterDefinition(
        "fundamental",
        "statement_available",
        "Latest statement available",
        "statement_available",
    ),
    FilterDefinition(
        "fundamental",
        "forecast_eps_positive",
        "Forecast EPS positive",
        "forecast_eps > 0",
    ),
    FilterDefinition(
        "fundamental",
        "profit_positive",
        "Profit positive",
        "profit > 0",
    ),
    FilterDefinition(
        "fundamental",
        "cfo_positive",
        "Operating cash flow positive",
        "operating_cash_flow > 0",
    ),
    FilterDefinition(
        "fundamental",
        "quality_score_ge_3",
        "Quality score >= 3",
        "quality_score >= 3",
    ),
    FilterDefinition(
        "fundamental",
        "low_pbr_le_1",
        "PBR <= 1.0",
        "pbr > 0 AND pbr <= 1.0",
    ),
    FilterDefinition(
        "annual_value",
        "low_forward_per_le_10",
        "Forward PER <= 10",
        "forward_per > 0 AND forward_per <= 10",
    ),
    FilterDefinition(
        "annual_value",
        "low_forward_per_le_15",
        "Forward PER <= 15",
        "forward_per > 0 AND forward_per <= 15",
    ),
    FilterDefinition(
        "annual_value",
        "small_market_cap_bottom_30",
        "Bottom 30% market cap among same-day new-high events",
        "market_cap_event_percentile <= 0.30",
    ),
    FilterDefinition(
        "annual_value",
        "low_pbr_forward_per_15",
        "PBR <= 1.0 and forward PER <= 15",
        "pbr > 0 AND pbr <= 1.0 AND forward_per > 0 AND forward_per <= 15",
    ),
    FilterDefinition(
        "annual_value",
        "annual_value_score_ge_2",
        "At least 2 of low PBR / low forward PER / small cap",
        "annual_value_score >= 2",
    ),
    FilterDefinition(
        "annual_value",
        "annual_value_score_3",
        "Low PBR, low forward PER, and small cap",
        "annual_value_score = 3",
    ),
    FilterDefinition(
        "fundamental",
        "forward_eps_growth_positive",
        "Forecast EPS > actual EPS",
        "forecast_eps > eps",
    ),
    FilterDefinition(
        "interaction",
        "volume_2_quality_3",
        "Volume >= 2.0x and quality score >= 3",
        "volume_ratio_20d >= 2.0 AND quality_score >= 3",
    ),
    FilterDefinition(
        "interaction",
        "close_high_volume_1_5",
        "Upper-range close and volume >= 1.5x",
        "close_position >= 0.70 AND volume_ratio_20d >= 1.5",
    ),
    FilterDefinition(
        "interaction",
        "volume_2_without_quality_3",
        "Volume >= 2.0x without quality score >= 3",
        "volume_ratio_20d >= 2.0 AND coalesce(quality_score, 0) < 3",
    ),
)


@dataclass(frozen=True)
class NewHighMomentumResearchResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    available_start_date: str | None
    available_end_date: str | None
    default_start_date: str | None
    analysis_start_date: str | None
    analysis_end_date: str | None
    lookback_years: int
    high_windows: tuple[int, ...]
    horizons: tuple[int, ...]
    sample_event_size: int
    membership_mode: str
    universe_summary_df: pd.DataFrame
    new_high_summary_df: pd.DataFrame
    top_candidates_df: pd.DataFrame
    portfolio_event_df: pd.DataFrame
    portfolio_daily_df: pd.DataFrame
    portfolio_summary_df: pd.DataFrame
    sampled_events_df: pd.DataFrame


def _warmup_start_date(
    analysis_start_date: str | None,
    available_start_date: str | None,
    *,
    high_windows: tuple[int, ...],
) -> str | None:
    return warmup_start_date(
        analysis_start_date,
        available_start_date,
        warmup_sessions=max(high_windows),
        session_to_calendar_multiplier=2.1,
    )


def _table_exists(conn: Any, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT count(*)
        FROM information_schema.tables
        WHERE table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(row and row[0])


def _sort_table(df: pd.DataFrame) -> pd.DataFrame:
    return sort_research_table(
        df,
        sort_columns=(
            "new_high_window",
            "horizon_days",
            "selection_rank",
            "date",
            "code",
        ),
        extra_order_columns={
            "condition_family": {
                "baseline": 0,
                "technical": 1,
                "fundamental": 2,
                "annual_value": 3,
                "interaction": 4,
            },
            "condition_key": {item.key: index for index, item in enumerate(CONDITION_DEFINITIONS)},
        },
    )


def _create_panel_tables(
    conn: Any,
    *,
    raw_start_date: str | None,
    analysis_start_date: str | None,
    analysis_end_date: str | None,
    high_windows: tuple[int, ...],
    horizons: tuple[int, ...],
) -> None:
    price_code = normalize_code_sql("sd.code")
    master_code = normalize_code_sql("smd.code")
    statement_code = normalize_code_sql("s.code")
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
    lead_close_exprs = ",\n                ".join(
        f"lead(close, {horizon}) over (partition by code order by date) as future_close_{horizon}d"
        for horizon in horizons
    )
    lead_date_exprs = ",\n                ".join(
        f"lead(date, {horizon}) over (partition by code order by date) as future_date_{horizon}d"
        for horizon in horizons
    )
    return_exprs = ",\n            ".join(
        f"case when next_open > 0 and future_close_{horizon}d > 0 "
        f"then future_close_{horizon}d / next_open - 1 end as return_next_open_to_close_{horizon}d"
        for horizon in horizons
    )
    topix_lead_exprs = ",\n                ".join(
        f"lead(close, {horizon}) over (order by date) as topix_future_close_{horizon}d"
        for horizon in horizons
    )
    topix_return_exprs = ",\n            ".join(
        f"case when topix_next_open > 0 and topix_future_close_{horizon}d > 0 "
        f"then topix_future_close_{horizon}d / topix_next_open - 1 end as topix_return_{horizon}d"
        for horizon in horizons
    )
    excess_exprs = ",\n            ".join(
        f"return_next_open_to_close_{horizon}d - topix_return_{horizon}d "
        f"as excess_return_vs_topix_{horizon}d"
        for horizon in horizons
    )
    rolling_high_windows = tuple(sorted({*high_windows, RANGE_WINDOW}))
    rolling_high_exprs = ",\n                ".join(
        f"max(high) over (partition by code order by date rows between {window} preceding and 1 preceding) "
        f"as prior_high_{window}d"
        for window in rolling_high_windows
    )
    new_high_exprs = ",\n                ".join(
        f"high > prior_high_{window}d as new_high_{window}d"
        for window in high_windows
    )
    any_new_high_expr = " OR ".join(f"new_high_{window}d" for window in high_windows)
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE new_high_momentum_panel AS
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
        lagged AS (
            SELECT
                *,
                lag(close) OVER (PARTITION BY code ORDER BY date) AS prev_close,
                lag(close, 20) OVER (PARTITION BY code ORDER BY date) AS close_lag_20d,
                lag(close, 60) OVER (PARTITION BY code ORDER BY date) AS close_lag_60d,
                lead(date, 1) OVER (PARTITION BY code ORDER BY date) AS next_date,
                lead(open, 1) OVER (PARTITION BY code ORDER BY date) AS next_open,
                {lead_close_exprs},
                {lead_date_exprs},
                {rolling_high_exprs},
                min(low) OVER (
                    PARTITION BY code ORDER BY date
                    ROWS BETWEEN {RANGE_WINDOW} PRECEDING AND 1 PRECEDING
                ) AS prior_low_{RANGE_WINDOW}d
            FROM scoped
        ),
        featured AS (
            SELECT
                *,
                volume * close AS trading_value,
                avg(volume) OVER (
                    PARTITION BY code ORDER BY date
                    ROWS BETWEEN {TECHNICAL_BASELINE_WINDOW} PRECEDING AND 1 PRECEDING
                ) AS avg_volume_20d_prev,
                avg(volume * close) OVER (
                    PARTITION BY code ORDER BY date
                    ROWS BETWEEN {TECHNICAL_BASELINE_WINDOW} PRECEDING AND 1 PRECEDING
                ) AS avg_trading_value_20d_prev,
                {new_high_exprs}
            FROM lagged
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
        )
        SELECT
            *,
            row_number() OVER () AS panel_id,
            CASE WHEN high > low THEN (close - low) / nullif(high - low, 0) END AS close_position,
            volume / nullif(avg_volume_20d_prev, 0) AS volume_ratio_20d,
            trading_value / nullif(avg_trading_value_20d_prev, 0) AS trading_value_ratio_20d,
            close / nullif(close_lag_20d, 0) - 1 AS prior_return_20d,
            close / nullif(close_lag_60d, 0) - 1 AS prior_return_60d,
            (close - prior_low_{RANGE_WINDOW}d)
                / nullif(prior_high_{RANGE_WINDOW}d - prior_low_{RANGE_WINDOW}d, 0)
                AS range_position_252d,
            {excess_exprs}
        FROM joined
        {final_where}
        """,
        [*raw_params, *final_params],
    )

    event_columns = ", ".join(f"new_high_{window}d" for window in high_windows)
    return_columns = ", ".join(
        [
            "next_date",
            "next_open",
            *[f"future_date_{horizon}d" for horizon in horizons],
            *[f"future_close_{horizon}d" for horizon in horizons],
            *[f"return_next_open_to_close_{horizon}d" for horizon in horizons],
            *[f"topix_return_{horizon}d" for horizon in horizons],
            *[f"excess_return_vs_topix_{horizon}d" for horizon in horizons],
        ]
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE new_high_momentum_events_base AS
        SELECT
            panel_id,
            code,
            date,
            company_name,
            universe_key,
            market_code,
            scale_category,
            high,
            close,
            volume,
            {event_columns},
            close_position,
            volume_ratio_20d,
            trading_value_ratio_20d,
            prior_return_20d,
            prior_return_60d,
            range_position_252d,
            {return_columns}
        FROM new_high_momentum_panel
        WHERE {any_new_high_expr}
        """
    )

    if _table_exists(conn, "statements"):
        conn.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE new_high_momentum_events AS
            WITH statement_candidates AS (
                SELECT
                    e.panel_id,
                    s.disclosed_date,
                    s.type_of_current_period AS period_type,
                    s.type_of_document AS document_type,
                    CAST(s.earnings_per_share AS DOUBLE) AS eps,
                    CAST(COALESCE(
                        s.next_year_forecast_earnings_per_share,
                        s.forecast_eps
                    ) AS DOUBLE) AS forecast_eps,
                    CAST(s.profit AS DOUBLE) AS profit,
                    CAST(s.sales AS DOUBLE) AS sales,
                    CAST(s.operating_cash_flow AS DOUBLE) AS operating_cash_flow,
                    CAST(s.equity AS DOUBLE) AS equity,
                    CAST(s.total_assets AS DOUBLE) AS total_assets,
                    CAST(s.bps AS DOUBLE) AS bps,
                    CAST(s.shares_outstanding AS DOUBLE) AS shares_outstanding,
                    ROW_NUMBER() OVER (
                        PARTITION BY e.panel_id
                        ORDER BY s.disclosed_date DESC, s.type_of_current_period DESC
                    ) AS row_priority
                FROM new_high_momentum_events_base e
                JOIN statements s
                    ON {statement_code} = e.code
                   AND s.disclosed_date <= e.date
            ),
            latest_statement AS (
                SELECT *
                FROM statement_candidates
                WHERE row_priority = 1
            ),
            enriched AS (
                SELECT
                    e.*,
                    ls.disclosed_date,
                    ls.period_type,
                    ls.document_type,
                    ls.eps,
                    ls.forecast_eps,
                    ls.profit,
                    ls.sales,
                    ls.operating_cash_flow,
                    ls.equity,
                    ls.total_assets,
                    ls.bps,
                    ls.shares_outstanding,
                    ls.disclosed_date IS NOT NULL AS statement_available,
                    CASE WHEN ls.bps > 0 THEN e.close / ls.bps END AS pbr,
                    CASE WHEN ls.eps > 0 THEN e.close / ls.eps END AS per,
                    CASE WHEN ls.forecast_eps > 0 THEN e.close / ls.forecast_eps END
                        AS forward_per,
                    CASE WHEN ls.shares_outstanding > 0 THEN e.close * ls.shares_outstanding / 1e9 END
                        AS market_cap_bil_jpy,
                    CASE WHEN ls.total_assets > 0 THEN ls.equity / ls.total_assets * 100 END
                        AS equity_ratio_pct,
                    (
                        CASE WHEN ls.profit > 0 THEN 1 ELSE 0 END
                        + CASE WHEN ls.forecast_eps > 0 THEN 1 ELSE 0 END
                        + CASE WHEN ls.operating_cash_flow > 0 THEN 1 ELSE 0 END
                        + CASE WHEN ls.total_assets > 0 AND ls.equity / ls.total_assets >= 0.30
                            THEN 1 ELSE 0 END
                    ) AS quality_score
                FROM new_high_momentum_events_base e
                LEFT JOIN latest_statement ls ON ls.panel_id = e.panel_id
            ),
            ranked AS (
                SELECT
                    *,
                    CASE
                        WHEN market_cap_bil_jpy > 0 THEN percent_rank() OVER (
                            PARTITION BY universe_key, date
                            ORDER BY market_cap_bil_jpy ASC NULLS LAST
                        )
                    END AS market_cap_event_percentile
                FROM enriched
            )
            SELECT
                *,
                (
                    CASE WHEN pbr > 0 AND pbr <= 1.0 THEN 1 ELSE 0 END
                    + CASE WHEN forward_per > 0 AND forward_per <= 15.0 THEN 1 ELSE 0 END
                    + CASE WHEN market_cap_event_percentile <= 0.30 THEN 1 ELSE 0 END
                ) AS annual_value_score
            FROM ranked
            """
        )
    else:
        conn.execute(
            """
            CREATE OR REPLACE TEMP TABLE new_high_momentum_events AS
            SELECT
                *,
                NULL::VARCHAR AS disclosed_date,
                NULL::VARCHAR AS period_type,
                NULL::VARCHAR AS document_type,
                NULL::DOUBLE AS eps,
                NULL::DOUBLE AS forecast_eps,
                NULL::DOUBLE AS profit,
                NULL::DOUBLE AS sales,
                NULL::DOUBLE AS operating_cash_flow,
                NULL::DOUBLE AS equity,
                NULL::DOUBLE AS total_assets,
                NULL::DOUBLE AS bps,
                NULL::DOUBLE AS shares_outstanding,
                FALSE AS statement_available,
                NULL::DOUBLE AS pbr,
                NULL::DOUBLE AS per,
                NULL::DOUBLE AS forward_per,
                NULL::DOUBLE AS market_cap_bil_jpy,
                NULL::DOUBLE AS market_cap_event_percentile,
                NULL::DOUBLE AS equity_ratio_pct,
                NULL::INTEGER AS quality_score,
                NULL::INTEGER AS annual_value_score
            FROM new_high_momentum_events_base
            """
        )


def _build_universe_summary(conn: Any, *, high_windows: tuple[int, ...]) -> pd.DataFrame:
    count_exprs = ",\n            ".join(
        f"sum(CASE WHEN new_high_{window}d THEN 1 ELSE 0 END) AS new_high_{window}d_count"
        for window in high_windows
    )
    frame = conn.execute(
        f"""
        SELECT
            universe_key,
            count(*) AS stock_day_count,
            count(DISTINCT code) AS unique_code_count,
            count(DISTINCT date) AS analysis_date_count,
            {count_exprs}
        FROM new_high_momentum_panel
        GROUP BY universe_key
        """
    ).fetchdf()
    if frame.empty:
        return frame
    frame["universe_label"] = frame["universe_key"].map(UNIVERSE_LABELS)
    for window in high_windows:
        column = f"new_high_{window}d_count"
        frame[f"new_high_{window}d_rate"] = frame[column] / frame["stock_day_count"].replace(
            {0: pd.NA}
        )
    return _sort_table(frame)


def _aggregate_condition_summary(
    conn: Any,
    *,
    high_windows: tuple[int, ...],
    horizons: tuple[int, ...],
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for window in high_windows:
        event_expression = f"new_high_{window}d"
        for condition in CONDITION_DEFINITIONS:
            filter_sql = f"({event_expression}) AND ({condition.expression})"
            for horizon in horizons:
                return_column = f"return_next_open_to_close_{horizon}d"
                topix_column = f"topix_return_{horizon}d"
                excess_column = f"excess_return_vs_topix_{horizon}d"
                frame = conn.execute(
                    f"""
                    WITH universe_day_baseline AS (
                        SELECT
                            universe_key,
                            date,
                            avg({return_column}) AS universe_day_return,
                            avg({excess_column}) AS universe_day_excess_return
                        FROM new_high_momentum_panel
                        WHERE {return_column} IS NOT NULL
                        GROUP BY universe_key, date
                    ),
                    filtered AS (
                        SELECT
                            e.*,
                            b.universe_day_return,
                            b.universe_day_excess_return
                        FROM new_high_momentum_events e
                        JOIN universe_day_baseline b
                            ON b.universe_key = e.universe_key AND b.date = e.date
                        WHERE {filter_sql}
                          AND e.{return_column} IS NOT NULL
                    ),
                    event_summary AS (
                        SELECT
                            universe_key,
                            count(*) AS event_count,
                            count(DISTINCT code) AS unique_code_count,
                            count(DISTINCT date) AS active_date_count,
                            avg({return_column}) AS mean_forward_return,
                            median({return_column}) AS median_forward_return,
                            quantile_cont({return_column}, 0.1) AS p10_forward_return,
                            quantile_cont({return_column}, 0.9) AS p90_forward_return,
                            avg(CASE WHEN {return_column} > 0 THEN 1.0 ELSE 0.0 END)
                                AS positive_return_rate,
                            avg(CASE WHEN {return_column} <= -0.05 THEN 1.0 ELSE 0.0 END)
                                AS loss_5pct_rate,
                            avg({excess_column}) AS mean_excess_return_vs_topix,
                            avg({topix_column}) AS mean_topix_return,
                            avg({return_column} - universe_day_return)
                                AS mean_lift_vs_same_universe_day,
                            avg({excess_column} - universe_day_excess_return)
                                AS mean_excess_lift_vs_same_universe_day,
                            avg(volume_ratio_20d) AS mean_volume_ratio_20d,
                            avg(trading_value_ratio_20d) AS mean_trading_value_ratio_20d,
                            avg(prior_return_20d) AS mean_prior_return_20d,
                            avg(range_position_252d) AS mean_range_position_252d,
                            avg(pbr) AS mean_pbr,
                            avg(forward_per) AS mean_forward_per,
                            median(forward_per) AS median_forward_per,
                            avg(market_cap_bil_jpy) AS mean_market_cap_bil_jpy,
                            avg(quality_score) AS mean_quality_score,
                            avg(annual_value_score) AS mean_annual_value_score,
                            avg(CASE WHEN statement_available THEN 1.0 ELSE 0.0 END)
                                AS statement_coverage_rate
                        FROM filtered
                        GROUP BY universe_key
                    ),
                    daily_summary AS (
                        SELECT
                            universe_key,
                            avg(daily_return) AS daily_equal_weight_forward_return,
                            avg(daily_excess_return) AS daily_equal_weight_excess_return
                        FROM (
                            SELECT
                                universe_key,
                                date,
                                avg({return_column}) AS daily_return,
                                avg({excess_column}) AS daily_excess_return
                            FROM filtered
                            GROUP BY universe_key, date
                        )
                        GROUP BY universe_key
                    )
                    SELECT
                        es.*,
                        ds.daily_equal_weight_forward_return,
                        ds.daily_equal_weight_excess_return
                    FROM event_summary es
                    LEFT JOIN daily_summary ds USING (universe_key)
                    """,
                ).fetchdf()
                if frame.empty:
                    continue
                frame["universe_label"] = frame["universe_key"].map(UNIVERSE_LABELS)
                frame["new_high_window"] = window
                frame["condition_family"] = condition.family
                frame["condition_key"] = condition.key
                frame["condition_label"] = condition.label
                frame["horizon_days"] = horizon
                frames.append(frame)
    if not frames:
        return pd.DataFrame()
    summary = pd.concat(frames, ignore_index=True)
    baseline = summary.loc[
        summary["condition_key"].eq("all"),
        [
            "universe_key",
            "new_high_window",
            "horizon_days",
            "event_count",
            "mean_forward_return",
            "mean_excess_return_vs_topix",
        ],
    ].rename(
        columns={
            "event_count": "new_high_baseline_event_count",
            "mean_forward_return": "new_high_baseline_mean_forward_return",
            "mean_excess_return_vs_topix": "new_high_baseline_mean_excess_return_vs_topix",
        }
    )
    summary = summary.merge(
        baseline,
        on=["universe_key", "new_high_window", "horizon_days"],
        how="left",
    )
    baseline_event_count = summary["new_high_baseline_event_count"].where(
        summary["new_high_baseline_event_count"] != 0
    )
    summary["event_share_of_new_high_baseline"] = (
        summary["event_count"] / baseline_event_count
    )
    summary["mean_lift_vs_new_high_baseline"] = (
        summary["mean_forward_return"] - summary["new_high_baseline_mean_forward_return"]
    )
    summary["mean_excess_lift_vs_new_high_baseline"] = (
        summary["mean_excess_return_vs_topix"]
        - summary["new_high_baseline_mean_excess_return_vs_topix"]
    )
    return _sort_table(summary)


def _build_top_candidates(summary_df: pd.DataFrame) -> pd.DataFrame:
    if summary_df.empty:
        return summary_df.copy()
    scoped = summary_df.loc[
        summary_df["condition_family"].isin(
            ["technical", "fundamental", "annual_value", "interaction"]
        )
        & summary_df["horizon_days"].eq(20)
        & (summary_df["event_count"] >= 100)
        & (summary_df["unique_code_count"] >= 30)
    ].copy()
    if scoped.empty:
        return scoped
    scoped = scoped.sort_values(
        by=[
            "universe_key",
            "new_high_window",
            "mean_excess_lift_vs_new_high_baseline",
            "mean_lift_vs_same_universe_day",
            "event_count",
        ],
        ascending=[True, True, False, False, False],
        kind="stable",
    )
    scoped["selection_rank"] = scoped.groupby(["universe_key", "new_high_window"]).cumcount() + 1
    return _sort_table(scoped.loc[scoped["selection_rank"] <= 5].reset_index(drop=True))


def _build_sampled_events(
    conn: Any,
    *,
    high_windows: tuple[int, ...],
    horizons: tuple[int, ...],
    sample_event_size: int,
) -> pd.DataFrame:
    sample_window = 252 if 252 in high_windows else max(high_windows)
    sample_horizon = 20 if 20 in horizons else horizons[0]
    frame = conn.execute(
        f"""
        SELECT
            universe_key,
            {sample_window} AS sample_new_high_window,
            {sample_horizon} AS sample_horizon_days,
            date,
            code,
            company_name,
            close,
            volume_ratio_20d,
            trading_value_ratio_20d,
            prior_return_20d,
            range_position_252d,
            quality_score,
            annual_value_score,
            pbr,
            forward_per,
            market_cap_bil_jpy,
            market_cap_event_percentile,
            forecast_eps,
            eps,
            return_next_open_to_close_{sample_horizon}d AS sample_forward_return,
            excess_return_vs_topix_{sample_horizon}d AS sample_excess_return
        FROM new_high_momentum_events
        WHERE new_high_{sample_window}d
          AND return_next_open_to_close_{sample_horizon}d IS NOT NULL
        ORDER BY universe_key, date, code
        """
    ).fetchdf()
    if frame.empty:
        return frame
    frame["universe_label"] = frame["universe_key"].map(UNIVERSE_LABELS)
    return select_deterministic_samples(
        frame,
        sample_size=sample_event_size,
        partition_columns=("universe_key",),
        hash_columns=("universe_key", "date", "code"),
        final_order_columns=("universe_key", "sample_rank", "date", "code"),
    )


def _portfolio_condition_definitions() -> tuple[FilterDefinition, ...]:
    by_key = {condition.key: condition for condition in CONDITION_DEFINITIONS}
    return tuple(by_key[key] for key in PORTFOLIO_CONDITION_KEYS if key in by_key)


def _build_portfolio_event_df(
    conn: Any,
    *,
    high_windows: tuple[int, ...],
    horizons: tuple[int, ...],
) -> pd.DataFrame:
    columns = [
        "universe_key",
        "universe_label",
        "new_high_window",
        "condition_family",
        "condition_key",
        "condition_label",
        "horizon_days",
        "signal_date",
        "entry_date",
        "exit_date",
        "code",
        "company_name",
        "entry_open",
        "exit_close",
        "forward_return",
        "topix_return",
        "excess_return_vs_topix",
        "annual_value_score",
        "pbr",
        "forward_per",
        "market_cap_bil_jpy",
        "market_cap_event_percentile",
    ]
    frames: list[pd.DataFrame] = []
    for window in high_windows:
        event_expression = f"new_high_{window}d"
        for horizon in horizons:
            return_column = f"return_next_open_to_close_{horizon}d"
            topix_column = f"topix_return_{horizon}d"
            excess_column = f"excess_return_vs_topix_{horizon}d"
            exit_date_column = f"future_date_{horizon}d"
            for condition in _portfolio_condition_definitions():
                frame = conn.execute(
                    f"""
                    SELECT
                        universe_key,
                        {window} AS new_high_window,
                        '{condition.family}' AS condition_family,
                        '{condition.key}' AS condition_key,
                        '{condition.label}' AS condition_label,
                        {horizon} AS horizon_days,
                        date AS signal_date,
                        next_date AS entry_date,
                        {exit_date_column} AS exit_date,
                        code,
                        company_name,
                        next_open AS entry_open,
                        future_close_{horizon}d AS exit_close,
                        {return_column} AS forward_return,
                        {topix_column} AS topix_return,
                        {excess_column} AS excess_return_vs_topix,
                        annual_value_score,
                        pbr,
                        forward_per,
                        market_cap_bil_jpy,
                        market_cap_event_percentile
                    FROM new_high_momentum_events
                    WHERE {event_expression}
                      AND ({condition.expression})
                      AND next_date IS NOT NULL
                      AND {exit_date_column} IS NOT NULL
                      AND next_open > 0
                      AND future_close_{horizon}d > 0
                      AND {return_column} IS NOT NULL
                    """
                ).fetchdf()
                if frame.empty:
                    continue
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
    portfolio_event_df: pd.DataFrame,
    *,
    analysis_start_date: str | None,
    analysis_end_date: str | None,
) -> pd.DataFrame:
    columns = [
        "universe_key",
        "universe_label",
        "new_high_window",
        "condition_key",
        "condition_label",
        "horizon_days",
        "date",
        "active_positions",
        "mean_daily_return",
        "mean_daily_return_pct",
        "portfolio_value",
        "drawdown_pct",
    ]
    if portfolio_event_df.empty:
        return pd.DataFrame(columns=columns)
    event_df = portfolio_event_df.copy()
    conn.register("new_high_value_portfolio_events_input", event_df)
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
              AND sd.date <= (SELECT max(exit_date) FROM new_high_value_portfolio_events_input)
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
                e.new_high_window,
                e.condition_key,
                e.condition_label,
                e.horizon_days,
                p.date,
                count(*) AS active_positions,
                avg(
                    CASE
                        WHEN p.date = e.entry_date THEN p.close / nullif(e.entry_open, 0) - 1
                        ELSE p.close / nullif(p.prev_close, 0) - 1
                    END
                ) AS mean_daily_return
            FROM new_high_value_portfolio_events_input e
            JOIN prices p
              ON p.code = e.code
             AND p.date >= e.entry_date
             AND p.date <= e.exit_date
            GROUP BY
                e.universe_key,
                e.universe_label,
                e.new_high_window,
                e.condition_key,
                e.condition_label,
                e.horizon_days,
                p.date
        ),
        configs AS (
            SELECT DISTINCT
                universe_key,
                universe_label,
                new_high_window,
                condition_key,
                condition_label,
                horizon_days
            FROM new_high_value_portfolio_events_input
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
                c.new_high_window,
                c.condition_key,
                c.condition_label,
                c.horizon_days,
                cal.date,
                coalesce(a.active_positions, 0) AS active_positions,
                coalesce(a.mean_daily_return, 0.0) AS mean_daily_return
            FROM configs c
            CROSS JOIN calendar cal
            LEFT JOIN active_daily a
              ON a.universe_key = c.universe_key
             AND a.new_high_window = c.new_high_window
             AND a.condition_key = c.condition_key
             AND a.horizon_days = c.horizon_days
             AND a.date = cal.date
        )
        SELECT
            *,
            mean_daily_return * 100.0 AS mean_daily_return_pct
        FROM dense_daily
        ORDER BY universe_key, new_high_window, condition_key, horizon_days, date
        """,
        [start_date, start_date, end_date],
    ).fetchdf()
    conn.unregister("new_high_value_portfolio_events_input")
    if daily.empty:
        return pd.DataFrame(columns=columns)
    daily["portfolio_value"] = pd.NA
    daily["drawdown_pct"] = pd.NA
    group_columns = ["universe_key", "new_high_window", "condition_key", "horizon_days"]
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
    portfolio_event_df: pd.DataFrame,
) -> pd.DataFrame:
    columns = [
        "universe_key",
        "universe_label",
        "new_high_window",
        "condition_key",
        "condition_label",
        "horizon_days",
        "event_count",
        "unique_code_count",
        "active_days",
        "avg_active_positions",
        "max_active_positions",
        "total_return_pct",
        "cagr_pct",
        "max_drawdown_pct",
        "annualized_volatility_pct",
        "sharpe_ratio",
        "sortino_ratio",
        "calmar_ratio",
        "mean_forward_return_pct",
        "mean_excess_return_vs_topix_pct",
        "win_rate_pct",
    ]
    if portfolio_daily_df.empty:
        return pd.DataFrame(columns=columns)
    group_columns = ["universe_key", "new_high_window", "condition_key", "horizon_days"]
    event_stats = {
        tuple(keys): group
        for keys, group in portfolio_event_df.groupby(group_columns, sort=False)
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
            cagr = (1.0 + total_return) ** (365.25 / period_days) - 1.0
        drawdown = pd.to_numeric(group["drawdown_pct"], errors="coerce").min()
        max_drawdown_pct = float(drawdown) if pd.notna(drawdown) else None
        returns = (
            pd.to_numeric(event_group["forward_return"], errors="coerce").dropna()
            if "forward_return" in event_group
            else pd.Series(dtype="float64")
        )
        excess = (
            pd.to_numeric(event_group["excess_return_vs_topix"], errors="coerce").dropna()
            if "excess_return_vs_topix" in event_group
            else pd.Series(dtype="float64")
        )
        records.append(
            {
                "universe_key": keys[0],
                "universe_label": str(group["universe_label"].iloc[0]),
                "new_high_window": required_int(keys[1], field="new_high_window"),
                "condition_key": required_str(keys[2], field="condition_key"),
                "condition_label": str(group["condition_label"].iloc[0]),
                "horizon_days": required_int(keys[3], field="horizon_days"),
                "event_count": int(len(event_group)),
                "unique_code_count": int(event_group["code"].nunique()) if not event_group.empty else 0,
                "active_days": int((pd.to_numeric(group["active_positions"]) > 0).sum()),
                "avg_active_positions": _series_mean(group["active_positions"]),
                "max_active_positions": int(pd.to_numeric(group["active_positions"]).max()),
                "total_return_pct": total_return * 100.0,
                "cagr_pct": cagr * 100.0 if cagr is not None else None,
                "max_drawdown_pct": max_drawdown_pct,
                **_daily_stats(group["mean_daily_return"]),
                "calmar_ratio": (
                    cagr / abs(max_drawdown_pct / 100.0)
                    if cagr is not None and max_drawdown_pct is not None and max_drawdown_pct < -1e-12
                    else None
                ),
                "mean_forward_return_pct": float(returns.mean() * 100.0) if not returns.empty else None,
                "mean_excess_return_vs_topix_pct": (
                    float(excess.mean() * 100.0) if not excess.empty else None
                ),
                "win_rate_pct": float((returns > 0.0).mean() * 100.0) if not returns.empty else None,
            }
        )
    return _sort_table(pd.DataFrame(records)[columns])


def run_new_high_momentum_research(
    db_path: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    lookback_years: int = DEFAULT_LOOKBACK_YEARS,
    high_windows: tuple[int, ...] | list[int] | None = None,
    horizons: tuple[int, ...] | list[int] | None = None,
    sample_event_size: int = DEFAULT_SAMPLE_EVENT_SIZE,
) -> NewHighMomentumResearchResult:
    normalized_windows = normalize_positive_int_sequence(
        high_windows,
        fallback=DEFAULT_HIGH_WINDOWS,
        name="high_windows",
        non_positive="filter",
    )
    normalized_horizons = normalize_positive_int_sequence(
        horizons,
        fallback=DEFAULT_HORIZONS,
        name="horizons",
        non_positive="filter",
    )
    if sample_event_size < 1:
        raise ValueError("sample_event_size must be positive")
    with open_readonly_analysis_connection(
        db_path,
        snapshot_prefix="new-high-momentum-",
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
            high_windows=normalized_windows,
        )
        _create_panel_tables(
            conn,
            raw_start_date=raw_start_date,
            analysis_start_date=analysis_start_date,
            analysis_end_date=analysis_end_date,
            high_windows=normalized_windows,
            horizons=normalized_horizons,
        )
        universe_summary_df = _build_universe_summary(
            conn,
            high_windows=normalized_windows,
        )
        new_high_summary_df = _aggregate_condition_summary(
            conn,
            high_windows=normalized_windows,
            horizons=normalized_horizons,
        )
        top_candidates_df = _build_top_candidates(new_high_summary_df)
        portfolio_event_df = _build_portfolio_event_df(
            conn,
            high_windows=normalized_windows,
            horizons=normalized_horizons,
        )
        portfolio_daily_df = _build_portfolio_daily_df(
            conn,
            portfolio_event_df,
            analysis_start_date=analysis_start_date,
            analysis_end_date=analysis_end_date,
        )
        portfolio_summary_df = _build_portfolio_summary_df(
            portfolio_daily_df,
            portfolio_event_df,
        )
        sampled_events_df = _build_sampled_events(
            conn,
            high_windows=normalized_windows,
            horizons=normalized_horizons,
            sample_event_size=sample_event_size,
        )
        return NewHighMomentumResearchResult(
            db_path=str(db_path),
            source_mode=ctx.source_mode,
            source_detail=ctx.source_detail,
            available_start_date=available_start_date,
            available_end_date=available_end_date,
            default_start_date=default_start_date,
            analysis_start_date=analysis_start_date,
            analysis_end_date=analysis_end_date,
            lookback_years=lookback_years,
            high_windows=normalized_windows,
            horizons=normalized_horizons,
            sample_event_size=sample_event_size,
            membership_mode=MEMBERSHIP_MODE,
            universe_summary_df=universe_summary_df,
            new_high_summary_df=new_high_summary_df,
            top_candidates_df=top_candidates_df,
            portfolio_event_df=portfolio_event_df,
            portfolio_daily_df=portfolio_daily_df,
            portfolio_summary_df=portfolio_summary_df,
            sampled_events_df=sampled_events_df,
        )


def _format_pct(value: object, digits: int = 2) -> str:
    try:
        number = float(value) * 100.0  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return "-"
    if pd.isna(number):
        return "-"
    return f"{number:.{digits}f}%"


def _format_int(value: object) -> str:
    try:
        number = int(float(value))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return "-"
    return f"{number:,}"


def _format_number(value: object, *, digits: int = 2, suffix: str = "") -> str:
    try:
        number = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return "-"
    if pd.isna(number):
        return "-"
    return f"{number:.{digits}f}{suffix}"


def _build_research_bundle_summary_markdown(result: NewHighMomentumResearchResult) -> str:
    lines = [
        "# New-High Momentum Research",
        "",
        "## Parameters",
        "",
        f"- Analysis period: `{result.analysis_start_date}` to `{result.analysis_end_date}`",
        f"- High windows: `{', '.join(str(value) for value in result.high_windows)}`",
        f"- Horizons: `{', '.join(str(value) for value in result.horizons)}`",
        f"- Membership mode: `{result.membership_mode}`",
        f"- Source: `{result.source_detail}`",
        "",
        "## Universe Summary",
        "",
    ]
    if result.universe_summary_df.empty:
        lines.append("_No universe rows._")
    else:
        header = "| Universe | Stock-days | Unique codes | 20d high rate | 252d high rate |"
        lines.extend([header, "| --- | ---: | ---: | ---: | ---: |"])
        for row in result.universe_summary_df.itertuples(index=False):
            lines.append(
                "| "
                + " | ".join(
                    [
                        f"`{row.universe_key}`",
                        _format_int(row.stock_day_count),
                        _format_int(row.unique_code_count),
                        _format_pct(getattr(row, "new_high_20d_rate", None)),
                        _format_pct(getattr(row, "new_high_252d_rate", None)),
                    ]
                )
                + " |"
            )
    lines.extend(["", "## 252d New-High Baseline (20d Horizon)", ""])
    baseline = result.new_high_summary_df.loc[
        result.new_high_summary_df["new_high_window"].eq(252)
        & result.new_high_summary_df["condition_key"].eq("all")
        & result.new_high_summary_df["horizon_days"].eq(20)
    ]
    if baseline.empty:
        lines.append("_No 252d baseline rows._")
    else:
        lines.extend(
            [
                "| Universe | Events | 20d Return | 20d Excess | Same-Day Lift | Loss >=5% |",
                "| --- | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for row in baseline.itertuples(index=False):
            lines.append(
                "| "
                + " | ".join(
                    [
                        f"`{row.universe_key}`",
                        _format_int(row.event_count),
                        _format_pct(row.mean_forward_return),
                        _format_pct(row.mean_excess_return_vs_topix),
                        _format_pct(row.mean_lift_vs_same_universe_day),
                        _format_pct(row.loss_5pct_rate),
                    ]
                )
                + " |"
            )
    lines.extend(["", "## Top Candidate Conditions", ""])
    if result.top_candidates_df.empty:
        lines.append("_No candidate rows met minimum event requirements._")
    else:
        lines.extend(
            [
                "| Universe | Window | Condition | Events | 20d Excess | Lift vs New-High | Same-Day Lift |",
                "| --- | ---: | --- | ---: | ---: | ---: | ---: |",
            ]
        )
        for row in result.top_candidates_df.head(20).itertuples(index=False):
            lines.append(
                "| "
                + " | ".join(
                    [
                        f"`{row.universe_key}`",
                        _format_int(row.new_high_window),
                        f"`{row.condition_key}`",
                        _format_int(row.event_count),
                        _format_pct(row.mean_excess_return_vs_topix),
                        _format_pct(row.mean_excess_lift_vs_new_high_baseline),
                        _format_pct(row.mean_lift_vs_same_universe_day),
                    ]
                )
                + " |"
            )
    lines.extend(["", "## Event-Driven Portfolio (252d New High + Value Conditions)", ""])
    portfolio = result.portfolio_summary_df.loc[
        result.portfolio_summary_df["new_high_window"].eq(252)
        & result.portfolio_summary_df["horizon_days"].isin([20, 60])
        & result.portfolio_summary_df["universe_key"].isin(["prime_ex_topix500", "standard"])
    ]
    if portfolio.empty:
        lines.append("_No portfolio rows._")
    else:
        lines.extend(
            [
                "| Universe | Condition | Hold | Events | Avg active | CAGR | Sharpe | MaxDD | Event mean |",
                "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for row in portfolio.sort_values(
            ["universe_key", "horizon_days", "sharpe_ratio"],
            ascending=[True, True, False],
            kind="stable",
        ).itertuples(index=False):
            lines.append(
                "| "
                + " | ".join(
                    [
                        f"`{row.universe_key}`",
                        f"`{row.condition_key}`",
                        _format_int(row.horizon_days),
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


def write_new_high_momentum_research_bundle(
    result: NewHighMomentumResearchResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_dataclass_research_bundle(
        experiment_id=NEW_HIGH_MOMENTUM_EXPERIMENT_ID,
        module="src.domains.analytics.new_high_momentum_research",
        function="run_new_high_momentum_research",
        params={
            "lookback_years": result.lookback_years,
            "high_windows": list(result.high_windows),
            "horizons": list(result.horizons),
            "sample_event_size": result.sample_event_size,
        },
        result=result,
        table_field_names=TABLE_FIELD_NAMES,
        summary_markdown=_build_research_bundle_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def load_new_high_momentum_research_bundle(
    bundle_path: str | Path,
) -> NewHighMomentumResearchResult:
    return load_dataclass_research_bundle(
        bundle_path,
        result_type=NewHighMomentumResearchResult,
        table_field_names=TABLE_FIELD_NAMES,
    )
