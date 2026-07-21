"""Market bubble footprint monitor and rerating regime research."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Literal, Sequence, cast

import pandas as pd

from src.domains.analytics.daily_ranking_consumer_support import (
    RERATING_VALUE_CONDITIONS,
    aggregate_metric_columns,
    sql_string_list,
    table_exists,
)
from src.domains.analytics.daily_ranking_research_base import (
    DailyRankingPanelRequest,
    MarketScope,
    SignalExpression,
    assert_daily_ranking_research_tables,
    attach_daily_ranking_outcomes,
    build_daily_ranking_research_base,
    materialize_daily_ranking_signal_cohort,
    normalize_daily_ranking_market_scopes,
)
from src.domains.analytics.earnings_holdthrough_expectancy_report import (
    _top_rows_for_markdown,
)
from src.domains.analytics.readonly_duckdb_support import (
    SourceMode,
    normalize_code_sql,
    open_readonly_analysis_connection,
)
from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    write_research_bundle,
)
from src.shared.utils.market_code_alias import MARKET_CODES_BY_SCOPE

BUBBLE_FOOTPRINT_ID = "market-behavior/market-bubble-footprint"
RERATING_BUBBLE_REGIME_ID = (
    "market-behavior/rerating-bubble-regime-forward-response"
)
DEFAULT_FOOTPRINT_HORIZONS: tuple[int, ...] = (20, 60, 120, 252)
DEFAULT_RERATING_SIGNAL_HORIZONS: tuple[int, ...] = (20, 60, 120)
DEFAULT_MARKET_SCOPES: tuple[str, ...] = ("prime", "standard", "growth")
DEFAULT_MIN_OBSERVATIONS = 100
DEFAULT_SEVERE_LOSS_THRESHOLD_PCT = -10.0
DEFAULT_OBSERVATION_SAMPLE_LIMIT = 10_000
Frequency = Literal["monthly", "weekly"]

_REQUIRED_TABLES: tuple[str, ...] = ("stock_data", "topix_data", "daily_valuation")
_BUBBLE_REGIMES: tuple[str, ...] = (
    "normal",
    "narrowing",
    "crowded",
    "blowoff_watch",
)


def _market_scope_case_sql(column_sql: str) -> str:
    return f"""
                CASE
                    WHEN lower(trim({column_sql})) IN ({_sql_string_list(MARKET_CODES_BY_SCOPE["prime"])})
                        THEN 'prime'
                    WHEN lower(trim({column_sql})) IN ({_sql_string_list(MARKET_CODES_BY_SCOPE["standard"])})
                        THEN 'standard'
                    WHEN lower(trim({column_sql})) IN ({_sql_string_list(MARKET_CODES_BY_SCOPE["growth"])})
                        THEN 'growth'
                    ELSE 'unknown'
                END
    """.strip()


@dataclass(frozen=True)
class BubbleFootprintResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    market_source: str
    analysis_start_date: str | None
    analysis_end_date: str | None
    return_horizons: tuple[int, ...]
    market_scopes: tuple[str, ...]
    frequency: Frequency
    required_tables: tuple[str, ...]
    latest_snapshot_date: str | None
    footprint_df: pd.DataFrame
    latest_snapshot_df: pd.DataFrame
    top_contributors_df: pd.DataFrame
    sector_contributors_df: pd.DataFrame


@dataclass(frozen=True)
class ReratingBubbleRegimeResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    market_source: str
    analysis_start_date: str | None
    analysis_end_date: str | None
    signal_horizons: tuple[int, ...]
    footprint_horizons: tuple[int, ...]
    market_scopes: tuple[str, ...]
    frequency: Frequency
    min_observations: int
    severe_loss_threshold_pct: float
    required_tables: tuple[str, ...]
    observation_count: int
    footprint_df: pd.DataFrame
    regime_transition_df: pd.DataFrame
    rerating_bubble_regime_df: pd.DataFrame
    observation_sample_df: pd.DataFrame


def run_market_bubble_footprint_research(
    db_path: str | Path,
    *,
    start_date: str | None = "2018-01-01",
    end_date: str | None = None,
    return_horizons: Iterable[int] = DEFAULT_FOOTPRINT_HORIZONS,
    market_scopes: Sequence[str] = DEFAULT_MARKET_SCOPES,
    frequency: Frequency = "monthly",
) -> BubbleFootprintResult:
    resolved_horizons = _normalize_positive_ints(return_horizons, name="return_horizons")
    resolved_market_scopes = normalize_daily_ranking_market_scopes(market_scopes)
    _validate_frequency(frequency)
    db_path_obj = Path(db_path).expanduser().resolve()
    if not db_path_obj.is_file():
        raise FileNotFoundError(f"market.duckdb was not found: {db_path_obj}")

    with open_readonly_analysis_connection(
        str(db_path_obj),
        snapshot_prefix="market-bubble-footprint-",
    ) as ctx:
        _assert_footprint_required_tables(ctx.connection)
        market_source = _market_source(ctx.connection)
        footprint_df = _build_footprint_table(
            ctx.connection,
            start_date=start_date,
            end_date=end_date,
            return_horizons=resolved_horizons,
            market_scopes=resolved_market_scopes,
            frequency=frequency,
            table_name="bubble_footprint_with_regimes",
        )
        latest_snapshot_date = _latest_snapshot_date(footprint_df)
        latest_snapshot_df = (
            footprint_df.loc[footprint_df["snapshot_date"] == latest_snapshot_date]
            .copy()
            .reset_index(drop=True)
            if latest_snapshot_date is not None
            else footprint_df.head(0).copy()
        )
        top_contributors_df = _query_top_contributors_df(
            ctx.connection,
            latest_snapshot_date=latest_snapshot_date,
            limit=30,
        )
        sector_contributors_df = _query_sector_contributors_df(
            ctx.connection,
            latest_snapshot_date=latest_snapshot_date,
            limit=30,
        )

    return BubbleFootprintResult(
        db_path=str(db_path_obj),
        source_mode=ctx.source_mode,
        source_detail=ctx.source_detail,
        market_source=market_source,
        analysis_start_date=start_date,
        analysis_end_date=end_date,
        return_horizons=resolved_horizons,
        market_scopes=resolved_market_scopes,
        frequency=frequency,
        required_tables=_REQUIRED_TABLES,
        latest_snapshot_date=latest_snapshot_date,
        footprint_df=footprint_df,
        latest_snapshot_df=latest_snapshot_df,
        top_contributors_df=top_contributors_df,
        sector_contributors_df=sector_contributors_df,
    )


def run_rerating_bubble_regime_forward_response_research(
    db_path: str | Path,
    *,
    start_date: str | None = "2018-01-01",
    end_date: str | None = None,
    signal_horizons: Iterable[int] = DEFAULT_RERATING_SIGNAL_HORIZONS,
    footprint_horizons: Iterable[int] = (60, 120, 252),
    market_scopes: Sequence[str] = DEFAULT_MARKET_SCOPES,
    frequency: Frequency = "monthly",
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
    severe_loss_threshold_pct: float = DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    observation_sample_limit: int = DEFAULT_OBSERVATION_SAMPLE_LIMIT,
) -> ReratingBubbleRegimeResult:
    resolved_signal_horizons = _normalize_positive_ints(
        signal_horizons,
        name="signal_horizons",
    )
    resolved_footprint_horizons = _normalize_positive_ints(
        footprint_horizons,
        name="footprint_horizons",
    )
    resolved_market_scopes = normalize_daily_ranking_market_scopes(market_scopes)
    _validate_frequency(frequency)
    if min_observations <= 0:
        raise ValueError("min_observations must be positive")
    if severe_loss_threshold_pct >= 0:
        raise ValueError("severe_loss_threshold_pct must be negative")
    if observation_sample_limit <= 0:
        raise ValueError("observation_sample_limit must be positive")

    db_path_obj = Path(db_path).expanduser().resolve()
    if not db_path_obj.is_file():
        raise FileNotFoundError(f"market.duckdb was not found: {db_path_obj}")

    with open_readonly_analysis_connection(
        str(db_path_obj),
        snapshot_prefix="rerating-bubble-regime-",
    ) as ctx:
        _assert_footprint_required_tables(ctx.connection)
        assert_daily_ranking_research_tables(ctx.connection)
        market_source = _market_source(ctx.connection)
        ranking_relations = build_daily_ranking_research_base(
            ctx.connection,
            DailyRankingPanelRequest(
                namespace="rerating_bubble_regime",
                analysis_start_date=(
                    None if start_date is None else date.fromisoformat(start_date)
                ),
                analysis_end_date=(
                    None if end_date is None else date.fromisoformat(end_date)
                ),
                horizons=resolved_signal_horizons,
                market_scopes=cast(
                    tuple[MarketScope, ...], resolved_market_scopes
                ),
                include_liquidity=True,
                percentile_features=(
                    "forecast_per_to_per_ratio",
                    "forecast_p_op_to_per_ratio",
                ),
            ),
        )
        footprint_df = _build_footprint_table(
            ctx.connection,
            start_date=start_date,
            end_date=end_date,
            return_horizons=resolved_footprint_horizons,
            market_scopes=resolved_market_scopes,
            frequency=frequency,
            table_name="bubble_footprint_with_regimes",
            price_history_name=ranking_relations.price_history.name,
            signal_basis_name=ranking_relations.signal_prices.name,
        )
        signal_source = ranking_relations.liquidity_ranked_signals
        if signal_source is None:
            raise RuntimeError("rerating bubble research requires liquidity ranking")
        ranking_cohort = materialize_daily_ranking_signal_cohort(
            ctx.connection,
            ranking_relations,
            source=signal_source,
            name="rerating_bubble_signals",
            predicate=SignalExpression(sql="TRUE", referenced_columns=()),
        )
        evaluated_ranking = attach_daily_ranking_outcomes(
            ctx.connection,
            ranking_cohort,
            ranking_relations,
            name="rerating_bubble_outcomes",
        )
        _create_typed_rerating_bubble_observation_table(
            ctx.connection,
            source_name=evaluated_ranking.name,
        )
        observation_count = int(
            ctx.connection.execute(
                "SELECT count(*) FROM rerating_bubble_observations"
            ).fetchone()[0]
        )
        regime_transition_df = _query_regime_transition_df(ctx.connection)
        rerating_bubble_regime_df = _build_rerating_bubble_regime_df(
            ctx.connection,
            horizons=resolved_signal_horizons,
            min_observations=min_observations,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
        )
        observation_sample_df = _query_rerating_observation_sample_df(
            ctx.connection,
            limit=observation_sample_limit,
        )

    return ReratingBubbleRegimeResult(
        db_path=str(db_path_obj),
        source_mode=ctx.source_mode,
        source_detail=ctx.source_detail,
        market_source=market_source,
        analysis_start_date=start_date,
        analysis_end_date=end_date,
        signal_horizons=resolved_signal_horizons,
        footprint_horizons=resolved_footprint_horizons,
        market_scopes=resolved_market_scopes,
        frequency=frequency,
        min_observations=int(min_observations),
        severe_loss_threshold_pct=float(severe_loss_threshold_pct),
        required_tables=tuple(
            dict.fromkeys(
                (
                    *_REQUIRED_TABLES,
                    "stock_data_raw",
                    "stock_adjustment_bases",
                    "stock_adjustment_basis_segments",
                    "indices_data",
                )
            )
        ),
        observation_count=observation_count,
        footprint_df=footprint_df,
        regime_transition_df=regime_transition_df,
        rerating_bubble_regime_df=rerating_bubble_regime_df,
        observation_sample_df=observation_sample_df,
    )


def write_bubble_footprint_bundle(
    result: BubbleFootprintResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=BUBBLE_FOOTPRINT_ID,
        module="src.domains.analytics.market_bubble_footprint",
        function="run_market_bubble_footprint_research",
        params={
            "return_horizons": list(result.return_horizons),
            "market_scopes": list(result.market_scopes),
            "frequency": result.frequency,
            "required_tables": list(result.required_tables),
        },
        db_path=result.db_path,
        analysis_start_date=result.analysis_start_date,
        analysis_end_date=result.analysis_end_date,
        result_metadata={
            "source_mode": str(result.source_mode),
            "source_detail": result.source_detail,
            "market_source": result.market_source,
            "latest_snapshot_date": result.latest_snapshot_date,
        },
        result_tables={
            "footprint_df": result.footprint_df,
            "latest_snapshot_df": result.latest_snapshot_df,
            "top_contributors_df": result.top_contributors_df,
            "sector_contributors_df": result.sector_contributors_df,
        },
        summary_markdown=build_bubble_footprint_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def write_rerating_bubble_regime_bundle(
    result: ReratingBubbleRegimeResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=RERATING_BUBBLE_REGIME_ID,
        module="src.domains.analytics.market_bubble_footprint",
        function="run_rerating_bubble_regime_forward_response_research",
        params={
            "signal_horizons": list(result.signal_horizons),
            "footprint_horizons": list(result.footprint_horizons),
            "market_scopes": list(result.market_scopes),
            "frequency": result.frequency,
            "min_observations": result.min_observations,
            "severe_loss_threshold_pct": result.severe_loss_threshold_pct,
            "required_tables": list(result.required_tables),
        },
        db_path=result.db_path,
        analysis_start_date=result.analysis_start_date,
        analysis_end_date=result.analysis_end_date,
        result_metadata={
            "source_mode": str(result.source_mode),
            "source_detail": result.source_detail,
            "market_source": result.market_source,
            "observation_count": result.observation_count,
        },
        result_tables={
            "footprint_df": result.footprint_df,
            "regime_transition_df": result.regime_transition_df,
            "rerating_bubble_regime_df": result.rerating_bubble_regime_df,
            "observation_sample_df": result.observation_sample_df,
        },
        summary_markdown=build_rerating_bubble_regime_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def build_bubble_footprint_summary_markdown(result: BubbleFootprintResult) -> str:
    parts = [
        "# Market Bubble Footprint",
        "",
        "## Metadata",
        "",
        f"- db_path: `{result.db_path}`",
        f"- source_mode: `{result.source_mode}`",
        f"- source_detail: `{result.source_detail}`",
        f"- market_source: `{result.market_source}`",
        f"- analysis_start_date: `{result.analysis_start_date}`",
        f"- analysis_end_date: `{result.analysis_end_date}`",
        f"- return_horizons: `{', '.join(str(item) for item in result.return_horizons)}`",
        f"- market_scopes: `{', '.join(result.market_scopes)}`",
        f"- frequency: `{result.frequency}`",
        f"- latest_snapshot_date: `{result.latest_snapshot_date}`",
        "",
        "## Latest Snapshot",
        "",
        _top_rows_for_markdown(result.latest_snapshot_df, limit=20),
        "",
        "## Footprint History",
        "",
        _top_rows_for_markdown(
            result.footprint_df.sort_values(["snapshot_date", "horizon"], ascending=[False, True]),
            limit=80,
        ),
        "",
        "## Top Contributors",
        "",
        _top_rows_for_markdown(result.top_contributors_df, limit=40),
        "",
        "## Sector Contributors",
        "",
        _top_rows_for_markdown(result.sector_contributors_df, limit=40),
    ]
    return "\n".join(parts).rstrip() + "\n"


def build_rerating_bubble_regime_summary_markdown(
    result: ReratingBubbleRegimeResult,
) -> str:
    parts = [
        "# Rerating x Bubble Regime Forward Response",
        "",
        "## Metadata",
        "",
        f"- db_path: `{result.db_path}`",
        f"- source_mode: `{result.source_mode}`",
        f"- source_detail: `{result.source_detail}`",
        f"- market_source: `{result.market_source}`",
        f"- analysis_start_date: `{result.analysis_start_date}`",
        f"- analysis_end_date: `{result.analysis_end_date}`",
        f"- signal_horizons: `{', '.join(str(item) for item in result.signal_horizons)}`",
        f"- footprint_horizons: `{', '.join(str(item) for item in result.footprint_horizons)}`",
        f"- market_scopes: `{', '.join(result.market_scopes)}`",
        f"- frequency: `{result.frequency}`",
        f"- observation_count: `{result.observation_count}`",
        "",
        "## Regime Transition",
        "",
        _top_rows_for_markdown(result.regime_transition_df, limit=80),
        "",
        "## Rerating Bubble Regime Evidence",
        "",
        _top_rows_for_markdown(result.rerating_bubble_regime_df, limit=160),
        "",
        "## Observation Sample",
        "",
        _top_rows_for_markdown(result.observation_sample_df, limit=40),
    ]
    return "\n".join(parts).rstrip() + "\n"


def _assert_footprint_required_tables(conn: Any) -> None:
    missing = [table for table in _REQUIRED_TABLES if not table_exists(conn, table)]
    if not table_exists(conn, "stock_master_daily"):
        missing.append("stock_master_daily")
    if missing:
        raise ValueError(f"market.duckdb is missing required tables: {', '.join(missing)}")


def _build_footprint_table(
    conn: Any,
    *,
    start_date: str | None,
    end_date: str | None,
    return_horizons: Sequence[int],
    market_scopes: Sequence[str],
    frequency: Frequency,
    table_name: str,
    price_history_name: str | None = None,
    signal_basis_name: str | None = None,
) -> pd.DataFrame:
    if (price_history_name is None) != (signal_basis_name is None):
        raise ValueError(
            "event-time footprint requires both price history and signal basis"
        )
    _create_footprint_base_tables(
        conn,
        market_scopes=market_scopes,
        frequency=frequency,
        start_date=start_date,
        end_date=end_date,
        price_history_name=price_history_name,
    )
    snapshot_basis_join_sql = (
        ""
        if signal_basis_name is None
        else f"""
            JOIN {signal_basis_name} snapshot_basis
              ON snapshot_basis.code = l.code
             AND CAST(snapshot_basis.date AS DATE) = CAST(l.date AS DATE)
             AND CAST(snapshot_basis.price_basis_id AS VARCHAR) = l.price_basis_id
        """
    )
    horizons_sql = ", ".join(f"({int(horizon)})" for horizon in return_horizons)
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE bubble_footprint_raw AS
        WITH horizons(horizon) AS (VALUES {horizons_sql}),
        pairs AS (
            SELECT
                s.snapshot_date,
                h.horizon,
                ca.date AS anchor_date
            FROM bubble_snapshot_dates s
            JOIN bubble_calendar cs ON cs.date = s.snapshot_date
            JOIN horizons h ON TRUE
            JOIN bubble_calendar ca ON ca.rn = cs.rn - h.horizon
        ),
        observations AS (
            SELECT
                p.snapshot_date,
                p.anchor_date,
                p.horizon,
                l.code,
                l.company_name,
                l.market,
                l.sector_33_name,
                l.close,
                l.close / nullif(a.close, 0) - 1.0 AS return_decimal,
                va.market_cap AS anchor_market_cap,
                vl.market_cap AS latest_market_cap,
                vl.forward_per,
                vl.pbr,
                vl.eps,
                vl.forward_eps,
                l.sma50,
                l.sma200,
                l.sma200_count,
                l.trading_value_20d,
                l.trading_value_prev232d,
                t0.close / nullif(ta.close, 0) - 1.0 AS topix_return_decimal
            FROM pairs p
            JOIN bubble_stock_features l
              ON l.date = p.snapshot_date
            {snapshot_basis_join_sql}
            JOIN bubble_stock_features a
              ON a.code = l.code
             AND a.date = p.anchor_date
             AND a.price_basis_id = l.price_basis_id
            JOIN bubble_market_master m
              ON m.code = l.code
             AND m.date = l.date
            LEFT JOIN bubble_daily_valuation va
              ON va.code = l.code
             AND va.date = a.date
             AND va.price_basis_id = l.price_basis_id
            LEFT JOIN bubble_daily_valuation vl
              ON vl.code = l.code
             AND vl.date = l.date
             AND vl.price_basis_id = l.price_basis_id
            JOIN topix_data t0
              ON t0.date = p.snapshot_date
            JOIN topix_data ta
              ON ta.date = p.anchor_date
            WHERE l.close > 0
              AND a.close > 0
        ),
        contribution_base AS (
            SELECT
                *,
                anchor_market_cap * return_decimal AS cap_return_contribution
            FROM observations
            WHERE return_decimal IS NOT NULL
              AND anchor_market_cap IS NOT NULL
              AND anchor_market_cap > 0
        ),
        ranked_positive AS (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY snapshot_date, horizon
                    ORDER BY cap_return_contribution DESC NULLS LAST
                ) AS positive_contribution_rank
            FROM contribution_base
            WHERE cap_return_contribution > 0
        ),
        ranked_mcap AS (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY snapshot_date, horizon
                    ORDER BY latest_market_cap DESC NULLS LAST
                ) AS mcap_rank
            FROM contribution_base
            WHERE latest_market_cap > 0
        ),
        metric_base AS (
            SELECT
                c.*,
                coalesce(rp.positive_contribution_rank, 999999) AS positive_contribution_rank,
                coalesce(rm.mcap_rank, 999999) AS mcap_rank
            FROM contribution_base c
            LEFT JOIN ranked_positive rp USING (
                snapshot_date, anchor_date, horizon, code, company_name, market,
                sector_33_name, return_decimal, anchor_market_cap, latest_market_cap,
                forward_per, pbr, eps, forward_eps, sma50, sma200, sma200_count,
                trading_value_20d, trading_value_prev232d, topix_return_decimal,
                cap_return_contribution
            )
            LEFT JOIN ranked_mcap rm USING (
                snapshot_date, anchor_date, horizon, code, company_name, market,
                sector_33_name, return_decimal, anchor_market_cap, latest_market_cap,
                forward_per, pbr, eps, forward_eps, sma50, sma200, sma200_count,
                trading_value_20d, trading_value_prev232d, topix_return_decimal,
                cap_return_contribution
            )
        )
        SELECT
            snapshot_date,
            anchor_date,
            horizon,
            count(*) AS observation_count,
            count(DISTINCT code) AS code_count,
            avg(CASE WHEN return_decimal > 0 THEN 1.0 ELSE 0.0 END) * 100.0
                AS breadth_up_pct,
            avg(return_decimal) * 100.0 AS equal_weight_return_pct,
            sum(anchor_market_cap * return_decimal) / nullif(sum(anchor_market_cap), 0)
                * 100.0 AS cap_weight_return_pct,
            (quantile_cont(return_decimal, 0.90) - quantile_cont(return_decimal, 0.10))
                * 100.0 AS return_p90_p10_spread_pct,
            sum(CASE WHEN positive_contribution_rank <= 5 THEN cap_return_contribution ELSE 0 END)
                / nullif(sum(CASE WHEN cap_return_contribution > 0 THEN cap_return_contribution ELSE 0 END), 0)
                * 100.0 AS top5_positive_contribution_share_pct,
            sum(CASE WHEN positive_contribution_rank <= 10 THEN cap_return_contribution ELSE 0 END)
                / nullif(sum(CASE WHEN cap_return_contribution > 0 THEN cap_return_contribution ELSE 0 END), 0)
                * 100.0 AS top10_positive_contribution_share_pct,
            sum(CASE WHEN mcap_rank <= 5 THEN latest_market_cap ELSE 0 END)
                / nullif(sum(latest_market_cap), 0) * 100.0 AS top5_mcap_share_pct,
            sum(CASE WHEN mcap_rank <= 10 THEN latest_market_cap ELSE 0 END)
                / nullif(sum(latest_market_cap), 0) * 100.0 AS top10_mcap_share_pct,
            sum(CASE WHEN forward_per > 40 OR pbr > 5 THEN latest_market_cap ELSE 0 END)
                / nullif(sum(latest_market_cap), 0) * 100.0 AS expensive_mcap_share_pct,
            avg(CASE WHEN forward_per > 40 OR pbr > 5 THEN 1.0 ELSE 0.0 END)
                * 100.0 AS expensive_count_share_pct,
            avg(CASE
                    WHEN coalesce(forward_eps, eps) <= 0
                      OR coalesce(forward_eps, eps) IS NULL
                    THEN 1.0 ELSE 0.0
                END) * 100.0 AS no_positive_earnings_count_share_pct,
            avg(CASE WHEN close > sma50 THEN 1.0 ELSE 0.0 END) * 100.0
                AS pct_above_sma50,
            avg(CASE WHEN sma200_count >= 200 AND close > sma200 THEN 1.0 ELSE 0.0 END)
                * 100.0 AS pct_above_sma200,
            median(trading_value_20d / nullif(trading_value_prev232d, 0))
                AS median_trading_value_ratio_20v232,
            quantile_cont(trading_value_20d / nullif(trading_value_prev232d, 0), 0.90)
                AS p90_trading_value_ratio_20v232,
            median(topix_return_decimal) * 100.0 AS topix_return_pct
        FROM metric_base
        GROUP BY snapshot_date, anchor_date, horizon
        ORDER BY snapshot_date, horizon
        """
    )
    raw = conn.execute("SELECT * FROM bubble_footprint_raw").fetchdf()
    footprint = _classify_footprint(raw)
    conn.register("__bubble_footprint_with_regimes_df", footprint)
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {table_name} AS
        SELECT * FROM __bubble_footprint_with_regimes_df
        """
    )
    conn.unregister("__bubble_footprint_with_regimes_df")
    return footprint


def _create_footprint_base_tables(
    conn: Any,
    *,
    market_scopes: Sequence[str],
    frequency: Frequency,
    start_date: str | None,
    end_date: str | None,
    price_history_name: str | None,
) -> None:
    stock_code = normalize_code_sql("sd.code")
    valuation_code = normalize_code_sql("dv.code")
    stock_source_sql = (
        "stock_data sd" if price_history_name is None else f"{price_history_name} sd"
    )
    stock_basis_sql = (
        "'__convenience__'"
        if price_history_name is None
        else "CAST(sd.price_basis_id AS VARCHAR)"
    )
    valuation_basis_sql = (
        "'__convenience__'"
        if price_history_name is None
        else "CAST(price_basis.price_basis_id AS VARCHAR)"
    )
    valuation_basis_join_sql = (
        ""
        if price_history_name is None
        else f"""
            JOIN {price_history_name} price_basis
              ON {normalize_code_sql("price_basis.code")} = {valuation_code}
             AND CAST(price_basis.date AS DATE) = CAST(dv.date AS DATE)
             AND CAST(price_basis.price_basis_id AS VARCHAR)
                 = CAST(dv.basis_version AS VARCHAR)
        """
    )
    query_start = (
        (pd.Timestamp(start_date) - pd.Timedelta(days=900)).strftime("%Y-%m-%d")
        if start_date is not None
        else None
    )
    query_end = (
        (pd.Timestamp(end_date) + pd.Timedelta(days=500)).strftime("%Y-%m-%d")
        if end_date is not None
        else None
    )
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE bubble_calendar AS
        SELECT date, row_number() OVER (ORDER BY date) AS rn
        FROM (SELECT DISTINCT date FROM topix_data WHERE close > 0)
        ORDER BY date
        """
    )
    frequency_expr = (
        "substr(date, 1, 7)"
        if frequency == "monthly"
        else "strftime(CAST(date AS DATE), '%Y-%W')"
    )
    filters: list[str] = []
    params: list[str] = []
    if start_date is not None:
        filters.append("date >= ?")
        params.append(start_date)
    if end_date is not None:
        filters.append("date <= ?")
        params.append(end_date)
    where_sql = "" if not filters else "WHERE " + " AND ".join(filters)
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE bubble_snapshot_dates AS
        WITH ranged AS (
            SELECT date, {frequency_expr} AS period_key
            FROM bubble_calendar
            {where_sql}
        ),
        periodic AS (
            SELECT max(date) AS snapshot_date
            FROM ranged
            GROUP BY period_key
        ),
        latest AS (
            SELECT max(date) AS snapshot_date
            FROM ranged
        )
        SELECT DISTINCT snapshot_date
        FROM (
            SELECT snapshot_date FROM periodic
            UNION ALL
            SELECT snapshot_date FROM latest
        )
        WHERE snapshot_date IS NOT NULL
        ORDER BY snapshot_date
        """,
        params,
    )
    market_filter = (
        "TRUE"
        if "all" in market_scopes
        else f"market IN ({_sql_string_list(market_scopes)})"
    )
    raw_filters = ["sd.close > 0"]
    valuation_filters: list[str] = []
    if query_start is not None:
        raw_filters.append(f"sd.date >= '{query_start}'")
        valuation_filters.append(f"dv.date >= '{query_start}'")
    if query_end is not None:
        raw_filters.append(f"sd.date <= '{query_end}'")
        valuation_filters.append(f"dv.date <= '{query_end}'")
    raw_where_sql = " AND ".join(raw_filters)
    valuation_where_sql = (
        ""
        if not valuation_filters
        else "WHERE " + " AND ".join(valuation_filters)
    )
    _create_market_master_source(
        conn,
        query_start=query_start,
        query_end=query_end,
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE bubble_stock_norm AS
        SELECT code, date, price_basis_id, close, volume,
               company_name, market, sector_33_name
        FROM (
            SELECT
                {stock_code} AS code,
                sd.date,
                {stock_basis_sql} AS price_basis_id,
                CAST(sd.close AS DOUBLE) AS close,
                CAST(sd.volume AS DOUBLE) AS volume,
                mm.company_name,
                mm.market,
                mm.sector_33_name,
                row_number() OVER (
                    PARTITION BY {stock_code}, sd.date, {stock_basis_sql}
                    ORDER BY CASE WHEN length(sd.code) = 4 THEN 0 ELSE 1 END, sd.code
                ) AS row_rank
            FROM {stock_source_sql}
            JOIN bubble_market_master_source mm
              ON mm.code = {stock_code}
             AND mm.date = sd.date
            WHERE {raw_where_sql}
        )
        WHERE row_rank = 1
          AND {market_filter}
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE bubble_market_master AS
        SELECT DISTINCT code, date, company_name, market, sector_33_name
        FROM bubble_stock_norm
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE bubble_stock_features AS
        SELECT
            *,
            avg(close) OVER (
                PARTITION BY code, price_basis_id
                ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
            ) AS sma50,
            avg(close) OVER (
                PARTITION BY code, price_basis_id
                ORDER BY date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
            ) AS sma200,
            count(close) OVER (
                PARTITION BY code, price_basis_id
                ORDER BY date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
            ) AS sma200_count,
            avg(close * volume) OVER (
                PARTITION BY code, price_basis_id
                ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) AS trading_value_20d,
            median(close * volume) OVER (
                PARTITION BY code, price_basis_id
                ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
            ) AS med_adv60_jpy,
            count(close * volume) OVER (
                PARTITION BY code, price_basis_id
                ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
            ) AS med_adv60_sessions,
            avg(close * volume) OVER (
                PARTITION BY code, price_basis_id
                ORDER BY date ROWS BETWEEN 251 PRECEDING AND 20 PRECEDING
            ) AS trading_value_prev232d
        FROM bubble_stock_norm
        """
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE bubble_daily_valuation AS
        SELECT
            code,
            date,
            price_basis_id,
            market_cap,
            free_float_market_cap,
            per,
            forward_per,
            pbr,
            p_op,
            forward_p_op,
            eps,
            forward_eps
        FROM (
            SELECT
                {valuation_code} AS code,
                dv.date,
                {valuation_basis_sql} AS price_basis_id,
                CAST(dv.market_cap AS DOUBLE) AS market_cap,
                {_optional_double_expr(conn, "daily_valuation", "dv", "free_float_market_cap")}
                    AS free_float_market_cap,
                {_optional_double_expr(conn, "daily_valuation", "dv", "per")} AS per,
                {_optional_double_expr(conn, "daily_valuation", "dv", "forward_per")}
                    AS forward_per,
                {_optional_double_expr(conn, "daily_valuation", "dv", "pbr")} AS pbr,
                {_optional_double_expr(conn, "daily_valuation", "dv", "p_op")} AS p_op,
                {_optional_double_expr(conn, "daily_valuation", "dv", "forward_p_op")}
                    AS forward_p_op,
                {_optional_double_expr(conn, "daily_valuation", "dv", "eps")} AS eps,
                {_optional_double_expr(conn, "daily_valuation", "dv", "forward_eps")}
                    AS forward_eps,
                row_number() OVER (
                    PARTITION BY {valuation_code}, dv.date, {valuation_basis_sql}
                    ORDER BY dv.price_basis_date DESC NULLS LAST,
                             dv.basis_version DESC NULLS LAST,
                             CASE WHEN length(dv.code) = 4 THEN 0 ELSE 1 END,
                             dv.code
                ) AS row_rank
            FROM daily_valuation dv
            {valuation_basis_join_sql}
            {valuation_where_sql}
        )
        WHERE row_rank = 1
        """
    )


def _classify_footprint(raw: pd.DataFrame) -> pd.DataFrame:
    if raw.empty:
        return raw.reindex(columns=[*raw.columns, "bubble_score", "bubble_regime"])
    frame = raw.copy()
    percentile_metrics = (
        "top10_mcap_share_pct",
        "expensive_mcap_share_pct",
        "return_p90_p10_spread_pct",
        "top10_positive_contribution_share_pct",
    )
    for metric in percentile_metrics:
        frame[f"{metric}_hist_pct"] = (
            frame.groupby("horizon", group_keys=False)[metric]
            .apply(_historical_percentile)
            .astype(float)
        )
    score = pd.Series(0, index=frame.index, dtype="int64")
    score += (
        (frame["breadth_up_pct"] < 45.0) | (frame["pct_above_sma50"] < 45.0)
    ).astype("int64")
    score += (
        (frame["top10_mcap_share_pct_hist_pct"] >= 0.90)
        | (frame["top10_mcap_share_pct"] >= 20.0)
    ).astype("int64")
    score += (
        (frame["expensive_mcap_share_pct_hist_pct"] >= 0.80)
        | (frame["expensive_mcap_share_pct"] >= 22.0)
    ).astype("int64")
    score += (frame["return_p90_p10_spread_pct_hist_pct"] >= 0.90).astype("int64")
    score += (
        (frame["cap_weight_return_pct"] - frame["equal_weight_return_pct"]) >= 5.0
    ).astype("int64")
    frame["bubble_score"] = score
    frame["bubble_regime"] = pd.cut(
        score,
        bins=[-1, 1, 2, 3, 99],
        labels=list(_BUBBLE_REGIMES),
    ).astype(str)
    flag_columns = []
    checks = {
        "breadth_narrowing": (
            (frame["breadth_up_pct"] < 45.0) | (frame["pct_above_sma50"] < 45.0)
        ),
        "market_cap_concentration": (
            (frame["top10_mcap_share_pct_hist_pct"] >= 0.90)
            | (frame["top10_mcap_share_pct"] >= 20.0)
        ),
        "valuation_pressure": (
            (frame["expensive_mcap_share_pct_hist_pct"] >= 0.80)
            | (frame["expensive_mcap_share_pct"] >= 22.0)
        ),
        "return_dispersion": frame["return_p90_p10_spread_pct_hist_pct"] >= 0.90,
        "cap_weight_leadership": (
            (frame["cap_weight_return_pct"] - frame["equal_weight_return_pct"]) >= 5.0
        ),
    }
    for column, values in checks.items():
        frame[column] = values.astype(bool)
        flag_columns.append(column)
    frame["active_flags"] = frame[flag_columns].apply(
        lambda row: ",".join(column for column in flag_columns if bool(row[column])),
        axis=1,
    )
    return frame.sort_values(["snapshot_date", "horizon"]).reset_index(drop=True)


def _historical_percentile(series: pd.Series) -> pd.Series:
    valid = series.notna()
    result = pd.Series(pd.NA, index=series.index, dtype="Float64")
    count = int(valid.sum())
    if count <= 1:
        result.loc[valid] = 1.0
        return result
    ranks = series.loc[valid].rank(method="max")
    result.loc[valid] = (ranks - 1.0) / (count - 1.0)
    return result


def _query_top_contributors_df(
    conn: Any,
    *,
    latest_snapshot_date: str | None,
    limit: int,
) -> pd.DataFrame:
    if latest_snapshot_date is None:
        return pd.DataFrame()
    return conn.execute(
        """
        WITH latest_horizons AS (
            SELECT snapshot_date, anchor_date, horizon
            FROM bubble_footprint_with_regimes
            WHERE snapshot_date = ?
        ),
        rows AS (
            SELECT
                lh.snapshot_date,
                lh.horizon,
                l.code,
                l.company_name,
                l.market,
                l.sector_33_name,
                l.close / nullif(a.close, 0) - 1.0 AS return_decimal,
                vl.market_cap AS latest_market_cap,
                vl.forward_per,
                vl.pbr,
                va.market_cap * (l.close / nullif(a.close, 0) - 1.0)
                    AS contribution
            FROM latest_horizons lh
            JOIN bubble_stock_features l ON l.date = lh.snapshot_date
            JOIN bubble_stock_features a ON a.code = l.code AND a.date = lh.anchor_date
            LEFT JOIN bubble_daily_valuation va ON va.code = l.code AND va.date = a.date
            LEFT JOIN bubble_daily_valuation vl ON vl.code = l.code AND vl.date = l.date
            WHERE va.market_cap > 0
        ),
        denom AS (
            SELECT snapshot_date, horizon, sum(contribution) AS positive_contribution
            FROM rows
            WHERE contribution > 0
            GROUP BY 1, 2
        ),
        ranked AS (
            SELECT
                r.*,
                contribution / nullif(d.positive_contribution, 0) * 100.0
                    AS positive_contribution_share_pct,
                row_number() OVER (
                    PARTITION BY r.snapshot_date, r.horizon
                    ORDER BY r.contribution DESC NULLS LAST
                ) AS contribution_rank
            FROM rows r
            JOIN denom d USING (snapshot_date, horizon)
            WHERE r.contribution > 0
        )
        SELECT
            snapshot_date,
            horizon,
            contribution_rank,
            code,
            company_name,
            market,
            sector_33_name,
            return_decimal * 100.0 AS return_pct,
            latest_market_cap / 1000000000000.0 AS latest_market_cap_jpy_trn,
            positive_contribution_share_pct,
            forward_per,
            pbr,
            contribution / 1000000000000.0 AS contribution_jpy_trn
        FROM ranked
        WHERE contribution_rank <= ?
        ORDER BY horizon, contribution_rank
        """,
        [latest_snapshot_date, int(limit)],
    ).fetchdf()


def _query_sector_contributors_df(
    conn: Any,
    *,
    latest_snapshot_date: str | None,
    limit: int,
) -> pd.DataFrame:
    if latest_snapshot_date is None:
        return pd.DataFrame()
    return conn.execute(
        """
        WITH latest_horizons AS (
            SELECT snapshot_date, anchor_date, horizon
            FROM bubble_footprint_with_regimes
            WHERE snapshot_date = ?
        ),
        rows AS (
            SELECT
                lh.snapshot_date,
                lh.horizon,
                l.sector_33_name,
                l.close / nullif(a.close, 0) - 1.0 AS return_decimal,
                va.market_cap,
                va.market_cap * (l.close / nullif(a.close, 0) - 1.0) AS contribution
            FROM latest_horizons lh
            JOIN bubble_stock_features l ON l.date = lh.snapshot_date
            JOIN bubble_stock_features a ON a.code = l.code AND a.date = lh.anchor_date
            LEFT JOIN bubble_daily_valuation va ON va.code = l.code AND va.date = a.date
            WHERE va.market_cap > 0
        ),
        denom AS (
            SELECT snapshot_date, horizon, sum(contribution) AS positive_contribution
            FROM rows
            WHERE contribution > 0
            GROUP BY 1, 2
        )
        , aggregated AS (
            SELECT
                r.snapshot_date,
                r.horizon,
                r.sector_33_name,
                count(*) AS observation_count,
                avg(r.return_decimal) * 100.0 AS equal_weight_return_pct,
                sum(r.market_cap * r.return_decimal) / nullif(sum(r.market_cap), 0)
                    * 100.0 AS cap_weight_return_pct,
                sum(r.contribution) / 1000000000000.0 AS contribution_jpy_trn,
                sum(CASE WHEN r.contribution > 0 THEN r.contribution ELSE 0 END)
                    / nullif(d.positive_contribution, 0) * 100.0
                    AS positive_contribution_share_pct
            FROM rows r
            JOIN denom d USING (snapshot_date, horizon)
            GROUP BY r.snapshot_date, r.horizon, r.sector_33_name, d.positive_contribution
        ),
        ranked AS (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY snapshot_date, horizon
                    ORDER BY contribution_jpy_trn DESC NULLS LAST
                ) AS sector_contribution_rank
            FROM aggregated
        )
        SELECT *
        FROM ranked
        WHERE sector_contribution_rank <= ?
        ORDER BY horizon, sector_contribution_rank
        """,
        [latest_snapshot_date, int(limit)],
    ).fetchdf()


def _create_typed_rerating_bubble_observation_table(
    conn: Any,
    *,
    source_name: str,
) -> None:
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE rerating_anchor_ranked AS
        SELECT
            r.* EXCLUDE (
                forecast_per,
                forecast_p_op,
                forecast_per_to_per_ratio,
                forecast_p_op_to_per_ratio,
                forecast_per_percentile,
                forecast_p_op_percentile,
                forecast_per_to_per_ratio_percentile,
                forecast_p_op_to_per_ratio_percentile,
                liquidity_scope
            ),
            r.forecast_per AS forward_per,
            r.forecast_p_op AS forward_p_op,
            r.forecast_per_to_per_ratio AS forward_per_to_per_ratio,
            r.forecast_p_op_to_per_ratio AS forward_p_op_to_per_ratio,
            r.forecast_per_percentile AS forward_per_percentile,
            r.forecast_p_op_percentile AS forward_p_op_percentile,
            r.forecast_per_to_per_ratio_percentile
                AS forward_per_to_per_ratio_percentile,
            r.forecast_p_op_to_per_ratio_percentile
                AS forward_p_op_to_per_ratio_percentile,
            r.liquidity_scope
        FROM {source_name} r
        JOIN bubble_snapshot_dates snapshots
          ON snapshots.snapshot_date = r.date
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE rerating_bubble_observations AS
        SELECT
            r.*,
            bf.horizon AS footprint_horizon,
            bf.bubble_score,
            bf.bubble_regime,
            bf.active_flags,
            bf.breadth_up_pct,
            bf.pct_above_sma50,
            bf.top10_mcap_share_pct,
            bf.expensive_mcap_share_pct,
            bf.return_p90_p10_spread_pct
        FROM rerating_anchor_ranked r
        JOIN bubble_footprint_with_regimes bf
          ON bf.snapshot_date = r.date
        WHERE r.liquidity_scope IN ('neutral_rerating', 'crowded_rerating')
        """
    )


def _build_rerating_bubble_regime_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    value_union = "\nUNION ALL\n".join(
        f"""
        SELECT
            *,
            '{value_condition}' AS value_condition,
            {value_order} AS value_condition_order
        FROM rerating_bubble_observations
        WHERE {value_sql}
        """
        for value_order, (value_condition, value_sql) in enumerate(
            RERATING_VALUE_CONDITIONS
        )
    )
    return_union = "\nUNION ALL\n".join(
        f"""
        SELECT
            *,
            {int(horizon)} AS horizon,
            forward_close_excess_return_{int(horizon)}d_pct AS forward_return_pct
        FROM value_labeled
        WHERE forward_close_excess_return_{int(horizon)}d_pct IS NOT NULL
        """
        for horizon in horizons
    )
    frame = conn.execute(
        f"""
        WITH value_labeled AS (
            {value_union}
        ),
        return_labeled AS (
            {return_union}
        )
        SELECT
            'rerating_bubble_regime' AS condition_family,
            footprint_horizon,
            bubble_regime,
            CASE bubble_regime
                WHEN 'normal' THEN 0
                WHEN 'narrowing' THEN 1
                WHEN 'crowded' THEN 2
                WHEN 'blowoff_watch' THEN 3
                ELSE 99
            END AS bubble_regime_order,
            liquidity_scope AS liquidity_regime,
            CASE liquidity_scope
                WHEN 'neutral_rerating' THEN 0
                WHEN 'crowded_rerating' THEN 1
                ELSE 99
            END AS liquidity_regime_order,
            value_condition,
            value_condition_order,
            horizon,
            market_scope,
            count(*) AS observation_count,
            count(DISTINCT code) AS code_count,
            count(DISTINCT date) AS date_count,
            avg(forward_return_pct) AS mean_forward_excess_return_pct,
            median(forward_return_pct) AS median_forward_excess_return_pct,
            quantile_cont(forward_return_pct, 0.10) AS p10_forward_excess_return_pct,
            quantile_cont(forward_return_pct, 0.25) AS p25_forward_excess_return_pct,
            quantile_cont(forward_return_pct, 0.75) AS p75_forward_excess_return_pct,
            quantile_cont(forward_return_pct, 0.90) AS p90_forward_excess_return_pct,
            avg(CASE WHEN forward_return_pct > 0 THEN 1.0 ELSE 0.0 END) * 100.0
                AS win_rate_pct,
            avg(CASE WHEN forward_return_pct <= ? THEN 1.0 ELSE 0.0 END) * 100.0
                AS severe_loss_rate_pct,
            median(recent_return_20d_pct) AS median_recent_return_20d_pct,
            median(recent_return_60d_pct) AS median_recent_return_60d_pct,
            median(recent_return_120d_pct) AS median_recent_return_120d_pct,
            median(recent_return_150d_pct) AS median_recent_return_150d_pct,
            median(topix_recent_return_20d_pct) AS median_topix_recent_return_20d_pct,
            median(topix_recent_return_60d_pct) AS median_topix_recent_return_60d_pct,
            median(med_adv60_jpy) / 1000000.0 AS median_med_adv60_mil_jpy,
            median(market_cap_bil_jpy) AS median_market_cap_bil_jpy,
            median(free_float_market_cap_jpy) / 1000000000.0
                AS median_free_float_market_cap_bil_jpy,
            median(liquidity_residual_z) AS median_liquidity_residual_z,
            median(per) AS median_per,
            median(forward_per) AS median_forward_per,
            median(pbr) AS median_pbr,
            median(p_op) AS median_p_op,
            median(forward_p_op) AS median_forward_p_op,
            median(forward_per_to_per_ratio) AS median_forward_per_to_per_ratio,
            median(forward_p_op_to_per_ratio) AS median_forward_p_op_to_per_ratio,
            median(per_percentile) AS median_per_percentile,
            median(forward_per_percentile) AS median_forward_per_percentile,
            median(forward_p_op_percentile) AS median_forward_p_op_percentile,
            median(pbr_percentile) AS median_pbr_percentile,
            median(forward_per_to_per_ratio_percentile)
                AS median_forward_per_to_per_ratio_percentile,
            median(forward_p_op_to_per_ratio_percentile)
                AS median_forward_p_op_to_per_ratio_percentile,
            median(bubble_score) AS median_bubble_score,
            median(breadth_up_pct) AS median_breadth_up_pct,
            median(pct_above_sma50) AS median_pct_above_sma50,
            median(top10_mcap_share_pct) AS median_top10_mcap_share_pct,
            median(expensive_mcap_share_pct) AS median_expensive_mcap_share_pct,
            median(return_p90_p10_spread_pct) AS median_return_p90_p10_spread_pct
        FROM return_labeled
        GROUP BY
            footprint_horizon,
            bubble_regime,
            liquidity_scope,
            value_condition,
            value_condition_order,
            horizon,
            market_scope
        HAVING count(*) >= ?
        ORDER BY
            footprint_horizon,
            bubble_regime_order,
            liquidity_regime_order,
            value_condition_order,
            horizon,
            market_scope
        """,
        [float(severe_loss_threshold_pct), int(min_observations)],
    ).fetchdf()
    return frame.reindex(columns=_rerating_bubble_regime_columns())


def _query_regime_transition_df(conn: Any) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            horizon AS footprint_horizon,
            snapshot_date,
            bubble_regime,
            bubble_score,
            active_flags,
            breadth_up_pct,
            pct_above_sma50,
            top10_mcap_share_pct,
            expensive_mcap_share_pct,
            return_p90_p10_spread_pct,
            lag(bubble_regime) OVER (
                PARTITION BY horizon ORDER BY snapshot_date
            ) AS previous_bubble_regime
        FROM bubble_footprint_with_regimes
        ORDER BY snapshot_date DESC, horizon
        """
    ).fetchdf()


def _query_rerating_observation_sample_df(conn: Any, *, limit: int) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            date,
            footprint_horizon,
            bubble_regime,
            bubble_score,
            active_flags,
            code,
            company_name,
            market_scope,
            liquidity_scope AS liquidity_regime,
            close,
            recent_return_20d_pct,
            recent_return_60d_pct,
            topix_recent_return_20d_pct,
            topix_recent_return_60d_pct,
            liquidity_residual_z,
            per,
            forward_per,
            pbr,
            per_percentile,
            forward_per_percentile,
            pbr_percentile,
            forward_close_excess_return_20d_pct
        FROM rerating_bubble_observations
        ORDER BY date, footprint_horizon, code
        LIMIT ?
        """,
        [int(limit)],
    ).fetchdf()


def _create_market_master_source(
    conn: Any,
    *,
    query_start: str | None,
    query_end: str | None,
) -> None:
    filters: list[str] = []
    if query_start is not None:
        filters.append(f"date >= '{query_start}'")
    if query_end is not None:
        filters.append(f"date <= '{query_end}'")
    date_where = "" if not filters else "WHERE " + " AND ".join(filters)
    if not table_exists(conn, "stock_master_daily"):
        raise RuntimeError("market.duckdb requires stock_master_daily for PIT market footprint")
    code = normalize_code_sql("smd.code")
    sector_expr = (
        "smd.sector_33_name"
        if _column_exists(conn, "stock_master_daily", "sector_33_name")
        else "'unknown'"
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE bubble_market_master_source AS
        SELECT
            {code} AS code,
            smd.date,
            smd.company_name,
            {_market_scope_case_sql("smd.market_code")} AS market,
            {sector_expr} AS sector_33_name
        FROM stock_master_daily smd
        {date_where}
        """
    )


def _market_source(conn: Any) -> str:
    return "stock_master_daily_exact_date"


def _optional_double_expr(conn: Any, table: str, alias: str, column: str) -> str:
    if _column_exists(conn, table, column):
        return f"CAST({alias}.{column} AS DOUBLE)"
    return "CAST(NULL AS DOUBLE)"


def _column_exists(conn: Any, table: str, column: str) -> bool:
    return bool(
        conn.execute(
            f"SELECT count(*) FROM pragma_table_info('{table}') WHERE name = ?",
            [column],
        ).fetchone()[0]
    )


def _latest_snapshot_date(frame: pd.DataFrame) -> str | None:
    if frame.empty or "snapshot_date" not in frame.columns:
        return None
    value = frame["snapshot_date"].max()
    return str(value) if pd.notna(value) else None


def _normalize_positive_ints(values: Iterable[int], *, name: str) -> tuple[int, ...]:
    normalized = tuple(sorted({int(value) for value in values}))
    if not normalized or any(value <= 0 for value in normalized):
        raise ValueError(f"{name} must contain positive integers")
    return normalized


def _validate_frequency(frequency: str) -> None:
    if frequency not in ("monthly", "weekly"):
        raise ValueError("frequency must be monthly or weekly")


def _sql_string_list(values: Sequence[str]) -> str:
    return sql_string_list(values)


def assert_footprint_required_tables(conn: Any) -> None:
    """Validate the shared market-footprint input contract."""

    _assert_footprint_required_tables(conn)


def build_footprint_table(
    conn: Any,
    *,
    start_date: str | None,
    end_date: str | None,
    return_horizons: Sequence[int],
    market_scopes: Sequence[str],
    frequency: Frequency,
    table_name: str,
) -> pd.DataFrame:
    """Build the reusable footprint table for research and runtime monitoring."""

    return _build_footprint_table(
        conn,
        start_date=start_date,
        end_date=end_date,
        return_horizons=return_horizons,
        market_scopes=market_scopes,
        frequency=frequency,
        table_name=table_name,
    )


def classify_footprint(raw: pd.DataFrame) -> pd.DataFrame:
    """Classify raw footprint observations into stable market regimes."""

    return _classify_footprint(raw)


def column_exists(conn: Any, table: str, column: str) -> bool:
    return _column_exists(conn, table, column)


def market_scope_case_sql(column_sql: str) -> str:
    return _market_scope_case_sql(column_sql)


def optional_double_expr(conn: Any, table: str, alias: str, column: str) -> str:
    return _optional_double_expr(conn, table, alias, column)


def _rerating_bubble_regime_columns() -> list[str]:
    return [
        "condition_family",
        "footprint_horizon",
        "bubble_regime",
        "bubble_regime_order",
        "liquidity_regime",
        "liquidity_regime_order",
        "value_condition",
        "value_condition_order",
        "horizon",
        "market_scope",
        *aggregate_metric_columns(),
        "median_bubble_score",
        "median_breadth_up_pct",
        "median_pct_above_sma50",
        "median_top10_mcap_share_pct",
        "median_expensive_mcap_share_pct",
        "median_return_p90_p10_spread_pct",
    ]
