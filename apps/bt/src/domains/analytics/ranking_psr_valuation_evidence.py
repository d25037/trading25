"""PSR valuation evidence for Daily Ranking."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

import pandas as pd

from src.domains.analytics.atr_expansion_forward_response import (
    _create_observation_panel as _create_atr_observation_panel,
)
from src.domains.analytics.daily_ranking_research_base import (
    DAILY_RANKING_RESEARCH_RANKED_TABLE,
    create_daily_ranking_research_panel,
    daily_ranking_query_end_date,
    daily_ranking_query_start_date,
    normalize_daily_ranking_market_scopes,
)
from src.domains.analytics.earnings_holdthrough_expectancy import _table_exists
from src.domains.analytics.earnings_holdthrough_expectancy_report import (
    _top_rows_for_markdown,
)
from src.domains.analytics.ranking_forecast_operating_profit_growth_evidence import (
    _aggregate_lateral_conditions,
    _aggregate_metric_columns,
    _concat_sorted,
    _condition_values_sql,
    _deep_dive_metric_columns,
    _deep_dive_metric_sql,
    _sql_literal,
)
from src.domains.analytics.ranking_long_sector_leadership_horizon_decomposition import (
    _create_long_sector_leadership_tables,
    _create_long_signal_tables,
)
from src.domains.analytics.ranking_sector_strength_evidence import (
    _create_sector_strength_tables,
)
from src.domains.analytics.ranking_short_red_evidence import (
    _create_feature_panel as _create_short_red_feature_panel,
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

RANKING_PSR_VALUATION_EXPERIMENT_ID = (
    "market-behavior/ranking-psr-valuation-evidence"
)
DEFAULT_HORIZONS: tuple[int, ...] = (5, 10, 20, 60)
DEFAULT_MARKET_SCOPES: tuple[str, ...] = ("prime",)
DEFAULT_MIN_OBSERVATIONS = 500
DEFAULT_SEVERE_LOSS_THRESHOLD_PCT = -10.0
DEFAULT_OBSERVATION_SAMPLE_LIMIT = 10_000
_REQUIRED_TABLES: tuple[str, ...] = (
    "stock_data",
    "topix_data",
    "daily_valuation",
    "stock_master_daily",
    "statements",
    "indices_data",
    "index_master",
)
_LEADERSHIP_WINDOWS: tuple[int, ...] = (120, 252, 504)
_REQUIRED_ATR_WINDOWS: tuple[int, ...] = (20, 60)
_REQUIRED_RETURN_WINDOWS: tuple[int, ...] = (20, 60)
_PSR_BUCKETS: tuple[tuple[str, str], ...] = (
    ("missing_psr", "psr IS NULL"),
    ("low_psr_10pct", "psr_percentile <= 0.1"),
    ("low_psr_20pct", "psr_percentile <= 0.2"),
    ("middle_psr_60pct", "psr_percentile > 0.2 AND psr_percentile < 0.8"),
    ("high_psr_20pct", "psr_percentile >= 0.8"),
    ("high_psr_10pct", "psr_percentile >= 0.9"),
)
_PSR_CONDITIONS: tuple[tuple[str, str], ...] = (
    ("all", "TRUE"),
    ("psr_undervalued", "psr_percentile <= 0.2"),
    ("psr_overvalued", "psr_percentile >= 0.8"),
    ("psr_very_overvalued", "psr_percentile >= 0.9"),
    ("missing_psr", "psr IS NULL"),
)
_DECISION_SCOPES: tuple[tuple[str, str], ...] = (
    ("all_market", "TRUE"),
    ("deep_value", "valuation_signal = 'strong_value_confirmation'"),
    ("undervalued", "valuation_signal = 'medium_value_confirmation'"),
    ("overvalued", "overvalued_warning"),
    ("very_overvalued", "very_overvalued_warning"),
    ("psr_undervalued", "psr_percentile <= 0.2"),
    ("psr_overvalued", "psr_percentile >= 0.8"),
    ("psr_very_overvalued", "psr_percentile >= 0.9"),
    (
        "deep_value_or_psr_undervalued",
        "valuation_signal = 'strong_value_confirmation' OR psr_percentile <= 0.2",
    ),
    (
        "overvalued_or_psr_overvalued",
        "overvalued_warning OR psr_percentile >= 0.8",
    ),
)
_LONG_DEEP_SCOPES: tuple[tuple[str, str], ...] = (
    ("all_market", "TRUE"),
    ("deep_value", "valuation_signal = 'strong_value_confirmation'"),
    ("psr_undervalued", "psr_percentile <= 0.2"),
    (
        "long_hybrid_leadership_strong",
        "long_hybrid_leadership_score >= 0.799999",
    ),
    (
        "long_hybrid_leadership_strong_atr20_accel",
        "long_hybrid_leadership_score >= 0.799999 "
        "AND atr20_acceleration_ex_overheat_flag",
    ),
    (
        "deep_value_long_hybrid_atr20_accel",
        "valuation_signal = 'strong_value_confirmation' "
        "AND long_hybrid_leadership_score >= 0.799999 "
        "AND atr20_acceleration_ex_overheat_flag",
    ),
    (
        "psr_undervalued_long_hybrid_atr20_accel",
        "psr_percentile <= 0.2 "
        "AND long_hybrid_leadership_score >= 0.799999 "
        "AND atr20_acceleration_ex_overheat_flag",
    ),
    (
        "deep_value_or_psr_undervalued_long_hybrid_atr20_accel",
        "(valuation_signal = 'strong_value_confirmation' OR psr_percentile <= 0.2) "
        "AND long_hybrid_leadership_score >= 0.799999 "
        "AND atr20_acceleration_ex_overheat_flag",
    ),
    (
        "psr_overvalued_long_hybrid_atr20_accel",
        "psr_percentile >= 0.8 "
        "AND long_hybrid_leadership_score >= 0.799999 "
        "AND atr20_acceleration_ex_overheat_flag",
    ),
)
_SHORT_DEEP_SCOPES: tuple[tuple[str, str], ...] = (
    ("all_market", "TRUE"),
    ("sector_weak", "sector_strength_bucket = 'sector_weak'"),
    ("overvalued", "overvalued_warning"),
    ("very_overvalued", "very_overvalued_warning"),
    ("psr_overvalued", "psr_percentile >= 0.8"),
    ("psr_very_overvalued", "psr_percentile >= 0.9"),
    (
        "overvalued_sector_weak",
        "overvalued_warning AND sector_strength_bucket = 'sector_weak'",
    ),
    (
        "psr_overvalued_sector_weak",
        "psr_percentile >= 0.8 AND sector_strength_bucket = 'sector_weak'",
    ),
    (
        "overvalued_or_psr_overvalued_sector_weak",
        "(overvalued_warning OR psr_percentile >= 0.8) "
        "AND sector_strength_bucket = 'sector_weak'",
    ),
    (
        "crowded_overvalued_sector_weak",
        "crowded_overvalued_flag AND sector_strength_bucket = 'sector_weak'",
    ),
    (
        "crowded_psr_overvalued_sector_weak",
        "liquidity_regime = 'crowded_rerating' "
        "AND psr_percentile >= 0.8 "
        "AND sector_strength_bucket = 'sector_weak'",
    ),
    (
        "distribution_stress_psr_overvalued_sector_weak",
        "liquidity_regime = 'distribution_stress' "
        "AND psr_percentile >= 0.8 "
        "AND sector_strength_bucket = 'sector_weak'",
    ),
)


@dataclass(frozen=True)
class RankingPsrValuationEvidenceResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    market_source: str
    analysis_start_date: str | None
    analysis_end_date: str | None
    horizons: tuple[int, ...]
    market_scopes: tuple[str, ...]
    min_observations: int
    severe_loss_threshold_pct: float
    required_tables: tuple[str, ...]
    observation_count: int
    fwd_psr_data_plane_status: str
    observation_sample_df: pd.DataFrame
    coverage_diagnostics_df: pd.DataFrame
    psr_bucket_evidence_df: pd.DataFrame
    decision_scope_psr_evidence_df: pd.DataFrame
    long_deep_dive_psr_evidence_df: pd.DataFrame
    short_deep_dive_psr_evidence_df: pd.DataFrame


def run_ranking_psr_valuation_evidence_research(
    db_path: str | Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    market_scopes: Sequence[str] = DEFAULT_MARKET_SCOPES,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
    severe_loss_threshold_pct: float = DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    observation_sample_limit: int = DEFAULT_OBSERVATION_SAMPLE_LIMIT,
) -> RankingPsrValuationEvidenceResult:
    resolved_horizons = tuple(sorted({int(horizon) for horizon in horizons}))
    resolved_market_scopes = normalize_daily_ranking_market_scopes(market_scopes)
    _validate_params(
        horizons=resolved_horizons,
        min_observations=min_observations,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
        observation_sample_limit=observation_sample_limit,
    )
    db_path_obj = Path(db_path).expanduser().resolve()
    if not db_path_obj.is_file():
        raise FileNotFoundError(f"market.duckdb was not found: {db_path_obj}")

    query_start = daily_ranking_query_start_date(
        start_date,
        warmup_calendar_days=max(720, max(_LEADERSHIP_WINDOWS) * 3),
    )
    query_end = daily_ranking_query_end_date(
        end_date,
        max_horizon=max(resolved_horizons),
    )

    with open_readonly_analysis_connection(
        str(db_path_obj),
        snapshot_prefix="ranking-psr-valuation-",
    ) as ctx:
        _assert_required_tables(ctx.connection)
        market_source = "stock_master_daily_exact_date"
        create_daily_ranking_research_panel(
            ctx.connection,
            query_start=query_start,
            query_end=query_end,
            analysis_start_date=start_date,
            analysis_end_date=end_date,
            horizons=resolved_horizons,
            market_scopes=resolved_market_scopes,
            market_source=market_source,
            include_liquidity_ranked=True,
        )
        _create_atr_observation_panel(
            ctx.connection,
            query_start=query_start,
            query_end=query_end,
            analysis_start_date=start_date,
            analysis_end_date=end_date,
            atr_windows=_REQUIRED_ATR_WINDOWS,
            return_windows=_REQUIRED_RETURN_WINDOWS,
            horizons=resolved_horizons,
            market_source=market_source,
            market_scopes=resolved_market_scopes,
        )
        _create_psr_valuation_panel(ctx.connection)
        _create_short_red_feature_panel(ctx.connection)
        _create_sector_strength_tables(ctx.connection, horizons=resolved_horizons)
        _create_long_sector_leadership_tables(
            ctx.connection,
            leadership_windows=_LEADERSHIP_WINDOWS,
        )
        _create_long_signal_tables(
            ctx.connection,
            leadership_windows=_LEADERSHIP_WINDOWS,
        )
        _create_deep_dive_panel(ctx.connection)
        observation_count = int(
            ctx.connection.execute(
                "SELECT count(*) FROM ranking_psr_valuation_panel"
            ).fetchone()[0]
        )
        result = RankingPsrValuationEvidenceResult(
            db_path=str(db_path_obj),
            source_mode=ctx.source_mode,
            source_detail=ctx.source_detail,
            market_source=market_source,
            analysis_start_date=start_date,
            analysis_end_date=end_date,
            horizons=resolved_horizons,
            market_scopes=resolved_market_scopes,
            min_observations=int(min_observations),
            severe_loss_threshold_pct=float(severe_loss_threshold_pct),
            required_tables=_REQUIRED_TABLES,
            observation_count=observation_count,
            fwd_psr_data_plane_status=(
                "J-Quants statement contracts expose FSales/NxFSales candidates, "
                "but current bt market.duckdb statements and daily_valuation do not "
                "persist forecast sales; this run evaluates actual FY PSR only."
            ),
            observation_sample_df=_query_observation_sample_df(
                ctx.connection,
                limit=observation_sample_limit,
            ),
            coverage_diagnostics_df=_build_coverage_diagnostics_df(ctx.connection),
            psr_bucket_evidence_df=_build_psr_bucket_evidence_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            decision_scope_psr_evidence_df=_build_decision_scope_psr_evidence_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            long_deep_dive_psr_evidence_df=_build_deep_dive_psr_evidence_df(
                ctx.connection,
                condition_family="long_hybrid_sector_atr_psr",
                conditions=_LONG_DEEP_SCOPES,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            short_deep_dive_psr_evidence_df=_build_deep_dive_psr_evidence_df(
                ctx.connection,
                condition_family="short_sector_crowded_psr",
                conditions=_SHORT_DEEP_SCOPES,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
        )
    return result


def write_ranking_psr_valuation_evidence_bundle(
    result: RankingPsrValuationEvidenceResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=RANKING_PSR_VALUATION_EXPERIMENT_ID,
        module="src.domains.analytics.ranking_psr_valuation_evidence",
        function="run_ranking_psr_valuation_evidence_research",
        params={
            "horizons": list(result.horizons),
            "market_scopes": list(result.market_scopes),
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
            "fwd_psr_data_plane_status": result.fwd_psr_data_plane_status,
        },
        result_tables={
            "observation_sample_df": result.observation_sample_df,
            "coverage_diagnostics_df": result.coverage_diagnostics_df,
            "psr_bucket_evidence_df": result.psr_bucket_evidence_df,
            "decision_scope_psr_evidence_df": result.decision_scope_psr_evidence_df,
            "long_deep_dive_psr_evidence_df": result.long_deep_dive_psr_evidence_df,
            "short_deep_dive_psr_evidence_df": result.short_deep_dive_psr_evidence_df,
        },
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def build_summary_markdown(result: RankingPsrValuationEvidenceResult) -> str:
    parts = [
        "# Ranking PSR Valuation Evidence",
        "",
        "## Metadata",
        "",
        f"- db_path: `{result.db_path}`",
        f"- source_mode: `{result.source_mode}`",
        f"- source_detail: `{result.source_detail}`",
        f"- market_source: `{result.market_source}`",
        f"- analysis_start_date: `{result.analysis_start_date}`",
        f"- analysis_end_date: `{result.analysis_end_date}`",
        f"- horizons: `{', '.join(str(item) for item in result.horizons)}`",
        f"- market_scopes: `{', '.join(result.market_scopes)}`",
        f"- observation_count: `{result.observation_count}`",
        f"- fwd_psr_data_plane_status: {result.fwd_psr_data_plane_status}",
        "",
        "## Coverage Diagnostics",
        "",
        _top_rows_for_markdown(result.coverage_diagnostics_df, limit=30),
        "",
        "## PSR Bucket Evidence",
        "",
        _top_rows_for_markdown(result.psr_bucket_evidence_df, limit=120),
        "",
        "## Daily Ranking Decision Scope x PSR Evidence",
        "",
        _top_rows_for_markdown(result.decision_scope_psr_evidence_df, limit=180),
        "",
        "## Explicit Long Deep Dive: Long Hybrid Leadership x ATR x PSR",
        "",
        _top_rows_for_markdown(result.long_deep_dive_psr_evidence_df, limit=220),
        "",
        "## Explicit Short Deep Dive: Balanced Sector Strength x Crowded/PSR",
        "",
        _top_rows_for_markdown(result.short_deep_dive_psr_evidence_df, limit=220),
    ]
    return "\n".join(parts).rstrip() + "\n"


def _assert_required_tables(conn: Any) -> None:
    missing = [table for table in _REQUIRED_TABLES if not _table_exists(conn, table)]
    if missing:
        raise ValueError(f"market.duckdb is missing required tables: {', '.join(missing)}")


def _create_psr_valuation_panel(conn: Any) -> None:
    statement_code = normalize_code_sql("st.code")
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranking_psr_valuation_panel AS
        WITH actual_fy_sales AS (
            SELECT
                {statement_code} AS code,
                st.disclosed_date,
                st.sales AS actual_sales,
                lead(st.disclosed_date) OVER (
                    PARTITION BY {statement_code}
                    ORDER BY st.disclosed_date
                ) AS valid_to
            FROM statements st
            WHERE st.sales > 0
              AND upper(st.type_of_current_period) = 'FY'
              AND (
                  st.type_of_document LIKE '%FinancialStatements%'
                  OR coalesce(st.type_of_document, '') = ''
              )
        ),
        joined AS (
            SELECT
                r.*,
                s.actual_sales,
                s.disclosed_date AS actual_sales_disclosed_date,
                CASE
                    WHEN r.market_cap_bil_jpy > 0 AND s.actual_sales > 0
                    THEN (r.market_cap_bil_jpy * 1000000000.0) / s.actual_sales
                END AS psr
            FROM {DAILY_RANKING_RESEARCH_RANKED_TABLE} r
            LEFT JOIN actual_fy_sales s
              ON s.code = r.code
             AND s.disclosed_date <= r.date
             AND (s.valid_to IS NULL OR r.date < s.valid_to)
        ),
        ranked AS (
            SELECT
                *,
                count(*) FILTER (WHERE psr > 0) OVER (
                    PARTITION BY market_scope, date
                ) AS psr_valid_count,
                rank() OVER (
                    PARTITION BY market_scope, date
                    ORDER BY CASE WHEN psr > 0 THEN psr END NULLS LAST
                ) AS psr_rank
            FROM joined
        )
        SELECT
            * EXCLUDE (psr_valid_count, psr_rank),
            CASE
                WHEN psr > 0 AND psr_valid_count <= 1 THEN 0.0
                WHEN psr > 0 THEN (psr_rank - 1.0) / (psr_valid_count - 1.0)
            END AS psr_percentile,
            CASE
                WHEN psr IS NULL THEN 'missing_psr'
                WHEN psr_valid_count <= 1 OR (psr_rank - 1.0) / (psr_valid_count - 1.0) <= 0.2
                    THEN 'psr_undervalued'
                WHEN (psr_rank - 1.0) / (psr_valid_count - 1.0) >= 0.9
                    THEN 'psr_very_overvalued'
                WHEN (psr_rank - 1.0) / (psr_valid_count - 1.0) >= 0.8
                    THEN 'psr_overvalued'
            END AS psr_signal
        FROM ranked
        """
    )


def _create_deep_dive_panel(conn: Any) -> None:
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE ranking_psr_valuation_deep_panel AS
        SELECT
            p.*,
            l.sector_33_code,
            l.sector_33_name,
            l.sector_strength_bucket,
            l.sector_strength_score,
            l.sector_index_strength_score,
            l.sector_constituent_strength_score,
            l.long_index_leadership_score,
            l.long_constituent_breadth_leadership_score,
            l.long_hybrid_leadership_score,
            l.long_hybrid_bucket_label,
            coalesce(l.momentum_20_60_top20_flag, FALSE)
                AS momentum_20_60_top20_flag,
            s.atr20_pct,
            s.atr60_pct,
            s.atr20_to_atr60,
            s.atr20_change_20d_pct,
            coalesce(s.atr20_acceleration, FALSE) AS atr20_acceleration_flag,
            coalesce(
                s.atr20_acceleration
                AND coalesce(p.recent_return_20d_pct, 0.0) < 30.0,
                FALSE
            ) AS atr20_acceleration_ex_overheat_flag,
            coalesce(s.atr20_to_atr60_overheat, FALSE)
                AS atr20_to_atr60_overheat,
            coalesce(s.weak_trend, FALSE) AS weak_trend,
            (
                p.liquidity_regime = 'crowded_rerating'
                AND p.no_value_confirmation
            ) AS crowded_no_value_flag,
            (
                p.liquidity_regime = 'crowded_rerating'
                AND (
                    p.overvalued_warning
                    OR p.no_positive_earnings_valuation
                )
            ) AS crowded_overvalued_flag
        FROM ranking_psr_valuation_panel p
        LEFT JOIN long_sector_leadership_base_panel l
          ON l.code = p.code
         AND l.date = p.date
         AND l.market_scope = p.market_scope
        LEFT JOIN ranking_short_red_feature_panel s
          ON s.code = p.code
         AND s.date = p.date
         AND s.market_scope = p.market_scope
        """
    )


def _build_coverage_diagnostics_df(conn: Any) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            market_scope,
            count(*) AS observation_count,
            count(DISTINCT code) AS code_count,
            count(DISTINCT date) AS date_count,
            avg(CASE WHEN actual_sales > 0 THEN 1.0 ELSE 0.0 END)
                * 100.0 AS actual_sales_coverage_pct,
            avg(CASE WHEN psr > 0 THEN 1.0 ELSE 0.0 END)
                * 100.0 AS psr_coverage_pct,
            median(actual_sales) AS median_actual_sales,
            median(psr) AS median_psr,
            median(psr_percentile) AS median_psr_percentile,
            avg(CASE WHEN psr_percentile <= 0.2 THEN 1.0 ELSE 0.0 END)
                * 100.0 AS psr_undervalued_rate_pct,
            avg(CASE WHEN psr_percentile >= 0.8 THEN 1.0 ELSE 0.0 END)
                * 100.0 AS psr_overvalued_rate_pct
        FROM ranking_psr_valuation_panel
        GROUP BY market_scope
        ORDER BY market_scope
        """
    ).fetchdf()


def _build_psr_bucket_evidence_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    psr_lateral_sql = f"""
        CROSS JOIN LATERAL (
            VALUES {_condition_values_sql(_PSR_BUCKETS)}
        ) AS psr_bucket(psr_bucket, psr_bucket_order, condition_matches)
    """
    for horizon in horizons:
        frames.append(
            _aggregate_lateral_conditions(
                conn,
                source_name="ranking_psr_valuation_panel",
                lateral_sql=psr_lateral_sql,
                match_condition="psr_bucket.condition_matches",
                group_select_sql=(
                    "'psr_bucket' AS condition_family,\n"
                    "            psr_bucket.psr_bucket,\n"
                    "            psr_bucket.psr_bucket_order,\n"
                    f"            {int(horizon)} AS horizon"
                ),
                group_by_sql="psr_bucket.psr_bucket, psr_bucket.psr_bucket_order, market_scope",
                return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
                extra_metric_sql=_psr_metric_sql(),
            )
        )
    return _concat_sorted(frames, columns=_psr_bucket_columns())


def _build_decision_scope_psr_evidence_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    decision_lateral_sql = f"""
        CROSS JOIN LATERAL (
            VALUES {_condition_values_sql(_DECISION_SCOPES)}
        ) AS decision_scope(decision_scope, decision_scope_order, decision_matches)
        CROSS JOIN LATERAL (
            VALUES {_condition_values_sql(_PSR_CONDITIONS)}
        ) AS psr_condition(psr_condition, psr_condition_order, psr_matches)
    """
    for horizon in horizons:
        frames.append(
            _aggregate_lateral_conditions(
                conn,
                source_name="ranking_psr_valuation_panel",
                lateral_sql=decision_lateral_sql,
                match_condition=(
                    "decision_scope.decision_matches AND psr_condition.psr_matches"
                ),
                group_select_sql=(
                    "'decision_scope_psr' AS condition_family,\n"
                    "            decision_scope.decision_scope,\n"
                    "            decision_scope.decision_scope_order,\n"
                    "            psr_condition.psr_condition,\n"
                    "            psr_condition.psr_condition_order,\n"
                    f"            {int(horizon)} AS horizon"
                ),
                group_by_sql=(
                    "decision_scope.decision_scope, "
                    "decision_scope.decision_scope_order, "
                    "psr_condition.psr_condition, "
                    "psr_condition.psr_condition_order, "
                    "market_scope"
                ),
                return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
                extra_metric_sql=_psr_metric_sql(),
            )
        )
    return _concat_sorted(frames, columns=_decision_scope_psr_columns())


def _build_deep_dive_psr_evidence_df(
    conn: Any,
    *,
    condition_family: str,
    conditions: Sequence[tuple[str, str]],
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    deep_lateral_sql = f"""
        CROSS JOIN LATERAL (
            VALUES {_condition_values_sql(conditions)}
        ) AS deep_scope(deep_scope, deep_scope_order, deep_scope_matches)
    """
    for horizon in horizons:
        frames.append(
            _aggregate_lateral_conditions(
                conn,
                source_name="ranking_psr_valuation_deep_panel",
                lateral_sql=deep_lateral_sql,
                match_condition="deep_scope.deep_scope_matches",
                group_select_sql=(
                    f"{_sql_literal(condition_family)} AS condition_family,\n"
                    "            deep_scope.deep_scope,\n"
                    "            deep_scope.deep_scope_order,\n"
                    f"            {int(horizon)} AS horizon"
                ),
                group_by_sql="deep_scope.deep_scope, deep_scope.deep_scope_order, market_scope",
                return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
                extra_metric_sql=_deep_dive_metric_sql() + _psr_metric_sql(),
            )
        )
    return _concat_sorted(frames, columns=_deep_dive_psr_columns())


def _psr_metric_sql() -> str:
    return """,
            median(actual_sales) AS median_actual_sales,
            median(psr) AS median_psr,
            median(psr_percentile) AS median_psr_percentile,
            avg(CASE WHEN psr_percentile <= 0.2 THEN 1.0 ELSE 0.0 END)
                * 100.0 AS psr_undervalued_rate_pct,
            avg(CASE WHEN psr_percentile >= 0.8 THEN 1.0 ELSE 0.0 END)
                * 100.0 AS psr_overvalued_rate_pct"""


def _query_observation_sample_df(conn: Any, *, limit: int) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            date,
            code,
            company_name,
            market,
            market_code,
            liquidity_regime,
            close,
            recent_return_20d_pct,
            recent_return_60d_pct,
            liquidity_residual_z,
            market_cap_bil_jpy,
            actual_sales,
            actual_sales_disclosed_date,
            psr,
            psr_percentile,
            psr_signal,
            per,
            per_percentile,
            forward_per,
            forward_per_percentile,
            pbr,
            pbr_percentile,
            valuation_signal,
            strong_value_confirmation,
            medium_value_confirmation,
            overvalued_warning,
            very_overvalued_warning,
            no_positive_earnings_valuation,
            no_value_confirmation,
            forward_close_excess_return_20d_pct
        FROM ranking_psr_valuation_panel
        ORDER BY date, code
        LIMIT ?
        """,
        [int(limit)],
    ).fetchdf()


def _validate_params(
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
    observation_sample_limit: int,
) -> None:
    if not horizons or any(int(horizon) <= 0 for horizon in horizons):
        raise ValueError("horizons must contain positive integers")
    if min_observations <= 0:
        raise ValueError("min_observations must be positive")
    if severe_loss_threshold_pct >= 0:
        raise ValueError("severe_loss_threshold_pct must be negative")
    if observation_sample_limit <= 0:
        raise ValueError("observation_sample_limit must be positive")


def _psr_bucket_columns() -> list[str]:
    return [
        "condition_family",
        "psr_bucket",
        "psr_bucket_order",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
        *_psr_metric_columns(),
    ]


def _decision_scope_psr_columns() -> list[str]:
    return [
        "condition_family",
        "decision_scope",
        "decision_scope_order",
        "psr_condition",
        "psr_condition_order",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
        *_psr_metric_columns(),
    ]


def _deep_dive_psr_columns() -> list[str]:
    return [
        "condition_family",
        "deep_scope",
        "deep_scope_order",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
        *_deep_dive_metric_columns(),
        *_psr_metric_columns(),
    ]


def _psr_metric_columns() -> list[str]:
    return [
        "median_actual_sales",
        "median_psr",
        "median_psr_percentile",
        "psr_undervalued_rate_pct",
        "psr_overvalued_rate_pct",
    ]
