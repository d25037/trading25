"""Crowded long left-tail evidence for Daily Ranking."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

import pandas as pd

from src.domains.analytics.atr_expansion_forward_response import (
    _create_observation_panel as _create_atr_observation_panel,
)
from src.domains.analytics.daily_ranking_research_base import (
    create_daily_ranking_research_panel,
    daily_ranking_query_end_date,
    daily_ranking_query_start_date,
    normalize_daily_ranking_market_scopes,
)
from src.domains.analytics.earnings_holdthrough_expectancy import _table_exists
from src.domains.analytics.earnings_holdthrough_expectancy_report import (
    _top_rows_for_markdown,
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
from src.domains.analytics.research_bundle import ResearchBundleInfo, write_research_bundle

RANKING_CROWDED_LONG_TAIL_EXPERIMENT_ID = (
    "market-behavior/ranking-crowded-long-tail-evidence"
)
DEFAULT_HORIZONS: tuple[int, ...] = (5, 10, 20, 40, 60)
DEFAULT_MARKET_SCOPES: tuple[str, ...] = ("prime",)
DEFAULT_START_DATE = "2023-01-01"
DEFAULT_MIN_OBSERVATIONS = 30
DEFAULT_SEVERE_LOSS_THRESHOLD_PCT = -10.0
DEFAULT_OBSERVATION_SAMPLE_LIMIT = 10_000
DEFAULT_LONG_HYBRID_THRESHOLD = 0.799999
_LEADERSHIP_WINDOWS: tuple[int, ...] = (120, 252, 504)
_REQUIRED_ATR_WINDOWS: tuple[int, ...] = (20, 60)
_REQUIRED_RETURN_WINDOWS: tuple[int, ...] = (20, 60)
_LONG_WARMUP_CALENDAR_DAYS = 820
_REQUIRED_TABLES: tuple[str, ...] = (
    "stock_data",
    "topix_data",
    "daily_valuation",
    "stock_master_daily",
    "indices_data",
    "index_master",
)
_VALUATION_OVERLAP_CONDITIONS: tuple[tuple[str, str], ...] = (
    ("all_crowded_long_hybrid", "TRUE"),
    ("low10_pbr", "pbr_percentile <= 0.1"),
    ("low10_per", "per_percentile <= 0.1"),
    ("low10_forward_per", "forward_per_percentile <= 0.1"),
    ("low10_psr", "psr_percentile <= 0.1"),
    ("low10_forward_psr", "forward_psr_percentile <= 0.1"),
    ("low10_forward_p_op", "forward_p_op_percentile <= 0.1"),
    (
        "low10_pbr_and_forward_per",
        "pbr_percentile <= 0.1 AND forward_per_percentile <= 0.1",
    ),
    ("low10_pbr_and_psr", "pbr_percentile <= 0.1 AND psr_percentile <= 0.1"),
    (
        "low10_pbr_and_forward_psr",
        "pbr_percentile <= 0.1 AND forward_psr_percentile <= 0.1",
    ),
    (
        "low10_psr_and_forward_psr",
        "psr_percentile <= 0.1 AND forward_psr_percentile <= 0.1",
    ),
    (
        "low10_pbr_psr_forward_psr",
        "pbr_percentile <= 0.1 "
        "AND psr_percentile <= 0.1 "
        "AND forward_psr_percentile <= 0.1",
    ),
)
_ATR_BUCKETS: tuple[tuple[str, str], ...] = (
    ("all", "TRUE"),
    ("atr_accel_ex_overheat", "atr20_acceleration_ex_overheat_flag"),
    ("no_atr_accel_ex_overheat", "NOT atr20_acceleration_ex_overheat_flag"),
    ("atr20_to_atr60_overheat", "atr20_to_atr60_overheat_flag"),
    ("no_atr20_to_atr60_overheat", "NOT atr20_to_atr60_overheat_flag"),
    ("recent20_overheat_ge30", "recent_return_20d_pct >= 30.0"),
    (
        "no_recent20_overheat_ge30",
        "recent_return_20d_pct IS NULL OR recent_return_20d_pct < 30.0",
    ),
)


@dataclass(frozen=True)
class RankingCrowdedLongTailEvidenceResult:
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
    long_hybrid_threshold: float
    required_tables: tuple[str, ...]
    observation_count: int
    coverage_diagnostics_df: pd.DataFrame
    valuation_overlap_tail_df: pd.DataFrame
    atr_overheat_tail_df: pd.DataFrame
    sector_bucket_tail_df: pd.DataFrame
    horizon_path_tail_df: pd.DataFrame
    observation_sample_df: pd.DataFrame


def run_ranking_crowded_long_tail_evidence_research(
    db_path: str | Path,
    *,
    start_date: str | None = DEFAULT_START_DATE,
    end_date: str | None = None,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    market_scopes: Sequence[str] = DEFAULT_MARKET_SCOPES,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
    severe_loss_threshold_pct: float = DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    long_hybrid_threshold: float = DEFAULT_LONG_HYBRID_THRESHOLD,
    observation_sample_limit: int = DEFAULT_OBSERVATION_SAMPLE_LIMIT,
) -> RankingCrowdedLongTailEvidenceResult:
    resolved_horizons = tuple(sorted({int(horizon) for horizon in horizons}))
    resolved_market_scopes = normalize_daily_ranking_market_scopes(market_scopes)
    _validate_params(
        horizons=resolved_horizons,
        min_observations=min_observations,
        observation_sample_limit=observation_sample_limit,
    )
    db_path_obj = Path(db_path).expanduser().resolve()
    if not db_path_obj.is_file():
        raise FileNotFoundError(f"market.duckdb was not found: {db_path_obj}")

    query_start = daily_ranking_query_start_date(
        start_date,
        warmup_calendar_days=_LONG_WARMUP_CALENDAR_DAYS,
    )
    query_end = daily_ranking_query_end_date(
        end_date,
        max_horizon=max(resolved_horizons),
    )

    with open_readonly_analysis_connection(
        str(db_path_obj),
        snapshot_prefix="ranking-crowded-long-tail-",
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
        _create_crowded_long_tail_panel(
            ctx.connection,
            long_hybrid_threshold=long_hybrid_threshold,
        )
        observation_count = int(
            ctx.connection.execute(
                "SELECT count(*) FROM ranking_crowded_long_tail_panel"
            ).fetchone()[0]
        )
        result = RankingCrowdedLongTailEvidenceResult(
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
            long_hybrid_threshold=float(long_hybrid_threshold),
            required_tables=_REQUIRED_TABLES,
            observation_count=observation_count,
            coverage_diagnostics_df=_build_coverage_diagnostics_df(ctx.connection),
            valuation_overlap_tail_df=_build_valuation_overlap_tail_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            atr_overheat_tail_df=_build_atr_overheat_tail_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            sector_bucket_tail_df=_build_sector_bucket_tail_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            horizon_path_tail_df=_build_horizon_path_tail_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            observation_sample_df=_query_observation_sample_df(
                ctx.connection,
                limit=observation_sample_limit,
            ),
        )
    return result


def write_ranking_crowded_long_tail_evidence_bundle(
    result: RankingCrowdedLongTailEvidenceResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=RANKING_CROWDED_LONG_TAIL_EXPERIMENT_ID,
        module="src.domains.analytics.ranking_crowded_long_tail_evidence",
        function="run_ranking_crowded_long_tail_evidence_research",
        params={
            "horizons": list(result.horizons),
            "market_scopes": list(result.market_scopes),
            "min_observations": result.min_observations,
            "severe_loss_threshold_pct": result.severe_loss_threshold_pct,
            "long_hybrid_threshold": result.long_hybrid_threshold,
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
            "scaffold": (
                "Crowded Rerating + Long Hybrid Leadership >= "
                f"{result.long_hybrid_threshold:.6g}"
            ),
        },
        result_tables={
            "coverage_diagnostics_df": result.coverage_diagnostics_df,
            "valuation_overlap_tail_df": result.valuation_overlap_tail_df,
            "atr_overheat_tail_df": result.atr_overheat_tail_df,
            "sector_bucket_tail_df": result.sector_bucket_tail_df,
            "horizon_path_tail_df": result.horizon_path_tail_df,
            "observation_sample_df": result.observation_sample_df,
        },
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def build_summary_markdown(result: RankingCrowdedLongTailEvidenceResult) -> str:
    parts = [
        "# Ranking Crowded Long Tail Evidence",
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
        f"- long_hybrid_threshold: `{result.long_hybrid_threshold}`",
        "",
        "## Coverage Diagnostics",
        "",
        _top_rows_for_markdown(result.coverage_diagnostics_df, limit=30),
        "",
        "## Valuation Low10 Overlap Tail",
        "",
        _top_rows_for_markdown(result.valuation_overlap_tail_df, limit=220),
        "",
        "## ATR / Overheat Tail",
        "",
        _top_rows_for_markdown(result.atr_overheat_tail_df, limit=260),
        "",
        "## Sector Bucket Tail",
        "",
        _top_rows_for_markdown(result.sector_bucket_tail_df, limit=260),
        "",
        "## Horizon Path Tail",
        "",
        _top_rows_for_markdown(result.horizon_path_tail_df, limit=260),
    ]
    return "\n".join(parts).rstrip() + "\n"


def _assert_required_tables(conn: Any) -> None:
    missing = [table for table in _REQUIRED_TABLES if not _table_exists(conn, table)]
    if missing:
        raise ValueError(f"market.duckdb is missing required tables: {', '.join(missing)}")


def _create_crowded_long_tail_panel(
    conn: Any,
    *,
    long_hybrid_threshold: float,
) -> None:
    valuation_code = normalize_code_sql("dv.code")
    psr_expr = (
        "dv.psr"
        if _daily_valuation_column_exists(conn, "psr")
        else "CAST(NULL AS DOUBLE)"
    )
    forward_psr_expr = (
        "dv.forward_psr"
        if _daily_valuation_column_exists(conn, "forward_psr")
        else "CAST(NULL AS DOUBLE)"
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranking_crowded_long_tail_panel AS
        WITH valuation AS (
            SELECT
                {valuation_code} AS code,
                date,
                {psr_expr} AS psr,
                {forward_psr_expr} AS forward_psr
            FROM daily_valuation dv
        ),
        joined AS (
            SELECT
                l.*,
                v.psr,
                v.forward_psr,
                coalesce(s.atr20_acceleration, FALSE)
                    AS atr20_acceleration_flag,
                coalesce(
                    s.atr20_acceleration
                    AND coalesce(l.recent_return_20d_pct, 0.0) < 30.0,
                    FALSE
                ) AS atr20_acceleration_ex_overheat_flag,
                coalesce(s.atr20_to_atr60_overheat, FALSE)
                    AS atr20_to_atr60_overheat_flag,
                coalesce(s.weak_trend, FALSE) AS weak_trend_flag
            FROM long_sector_leadership_base_panel l
            LEFT JOIN valuation v
              ON v.code = l.code
             AND v.date = l.date
            LEFT JOIN ranking_short_red_feature_panel s
              ON s.code = l.code
             AND s.date = l.date
             AND s.market_scope = l.market_scope
            WHERE l.liquidity_regime = 'crowded_rerating'
              AND l.long_hybrid_leadership_score >= ?
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
                ) AS psr_rank,
                count(*) FILTER (WHERE forward_psr > 0) OVER (
                    PARTITION BY market_scope, date
                ) AS forward_psr_valid_count,
                rank() OVER (
                    PARTITION BY market_scope, date
                    ORDER BY CASE WHEN forward_psr > 0 THEN forward_psr END NULLS LAST
                ) AS forward_psr_rank
            FROM joined
        )
        SELECT
            * EXCLUDE (
                psr_valid_count,
                psr_rank,
                forward_psr_valid_count,
                forward_psr_rank
            ),
            CASE
                WHEN psr > 0 AND psr_valid_count <= 1 THEN 0.0
                WHEN psr > 0 THEN (psr_rank - 1.0) / (psr_valid_count - 1.0)
            END AS psr_percentile,
            CASE
                WHEN forward_psr > 0 AND forward_psr_valid_count <= 1 THEN 0.0
                WHEN forward_psr > 0
                    THEN (forward_psr_rank - 1.0) / (forward_psr_valid_count - 1.0)
            END AS forward_psr_percentile
        FROM ranked
        """,
        [float(long_hybrid_threshold)],
    )


def _build_coverage_diagnostics_df(conn: Any) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            market_scope,
            count(*) AS observation_count,
            count(DISTINCT code) AS code_count,
            count(DISTINCT date) AS date_count,
            avg(CASE WHEN pbr_percentile <= 0.1 THEN 1.0 ELSE 0.0 END) * 100.0
                AS low10_pbr_rate_pct,
            avg(CASE WHEN psr_percentile <= 0.1 THEN 1.0 ELSE 0.0 END) * 100.0
                AS low10_psr_rate_pct,
            avg(CASE WHEN forward_psr_percentile <= 0.1 THEN 1.0 ELSE 0.0 END)
                * 100.0 AS low10_forward_psr_rate_pct,
            avg(CASE WHEN atr20_acceleration_ex_overheat_flag THEN 1.0 ELSE 0.0 END)
                * 100.0 AS atr20_acceleration_ex_overheat_rate_pct,
            avg(CASE WHEN atr20_to_atr60_overheat_flag THEN 1.0 ELSE 0.0 END)
                * 100.0 AS atr20_to_atr60_overheat_rate_pct,
            avg(CASE WHEN sector_strength_bucket = 'sector_weak' THEN 1.0 ELSE 0.0 END)
                * 100.0 AS sector_weak_rate_pct,
            avg(CASE WHEN sector_strength_bucket = 'sector_strong' THEN 1.0 ELSE 0.0 END)
                * 100.0 AS sector_strong_rate_pct
        FROM ranking_crowded_long_tail_panel
        GROUP BY market_scope
        ORDER BY market_scope
        """
    ).fetchdf()


def _build_valuation_overlap_tail_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    return _concat_frames(
        _tail_df_for_conditions(
            conn,
            horizons=horizons,
            conditions=_VALUATION_OVERLAP_CONDITIONS,
            select_sql=(
                "'valuation_overlap' AS dimension, "
                "condition_name AS bucket, "
                "NULL AS secondary_bucket"
            ),
            min_observations=min_observations,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
        )
    )


def _build_atr_overheat_tail_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    valuation_values = _condition_values_sql(_VALUATION_OVERLAP_CONDITIONS)
    atr_values = _condition_values_sql(_ATR_BUCKETS)
    for horizon in horizons:
        return_column = f"forward_close_excess_return_{int(horizon)}d_pct"
        frames.append(
            _tail_aggregate_query(
                conn,
                horizon=int(horizon),
                return_column=return_column,
                lateral_sql=(
                    f"CROSS JOIN LATERAL (VALUES {valuation_values}) "
                    "AS v(valuation_bucket, valuation_matches)\n"
                    f"CROSS JOIN LATERAL (VALUES {atr_values}) "
                    "AS a(atr_bucket, atr_matches)"
                ),
                match_condition=(
                    "v.valuation_matches AND a.atr_matches "
                    f"AND {return_column} IS NOT NULL"
                ),
                select_sql=(
                    "'atr_overheat' AS dimension, "
                    "v.valuation_bucket AS bucket, "
                    "a.atr_bucket AS secondary_bucket"
                ),
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            )
        )
    return _concat_frames(frames)


def _build_sector_bucket_tail_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    valuation_values = _condition_values_sql(_VALUATION_OVERLAP_CONDITIONS)
    for horizon in horizons:
        return_column = f"forward_close_excess_return_{int(horizon)}d_pct"
        frames.append(
            _tail_aggregate_query(
                conn,
                horizon=int(horizon),
                return_column=return_column,
                lateral_sql=(
                    f"CROSS JOIN LATERAL (VALUES {valuation_values}) "
                    "AS v(valuation_bucket, valuation_matches)"
                ),
                match_condition=(
                    "v.valuation_matches "
                    "AND sector_strength_bucket IS NOT NULL "
                    f"AND {return_column} IS NOT NULL"
                ),
                select_sql=(
                    "'sector_bucket' AS dimension, "
                    "v.valuation_bucket AS bucket, "
                    "sector_strength_bucket AS secondary_bucket"
                ),
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            )
        )
    return _concat_frames(frames)


def _build_horizon_path_tail_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    return _concat_frames(
        _tail_df_for_conditions(
            conn,
            horizons=horizons,
            conditions=_VALUATION_OVERLAP_CONDITIONS,
            select_sql=(
                "'horizon_path' AS dimension, "
                "condition_name AS bucket, "
                "NULL AS secondary_bucket"
            ),
            min_observations=min_observations,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
        )
    )


def _tail_df_for_conditions(
    conn: Any,
    *,
    horizons: Sequence[int],
    conditions: Sequence[tuple[str, str]],
    select_sql: str,
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> list[pd.DataFrame]:
    frames: list[pd.DataFrame] = []
    condition_values = _condition_values_sql(conditions)
    for horizon in horizons:
        return_column = f"forward_close_excess_return_{int(horizon)}d_pct"
        frames.append(
            _tail_aggregate_query(
                conn,
                horizon=int(horizon),
                return_column=return_column,
                lateral_sql=(
                    f"CROSS JOIN LATERAL (VALUES {condition_values}) "
                    "AS c(condition_name, condition_matches)"
                ),
                match_condition=f"c.condition_matches AND {return_column} IS NOT NULL",
                select_sql=select_sql,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            )
        )
    return frames


def _tail_aggregate_query(
    conn: Any,
    *,
    horizon: int,
    return_column: str,
    lateral_sql: str,
    match_condition: str,
    select_sql: str,
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    return conn.execute(
        f"""
        WITH labeled AS (
            SELECT
                {select_sql},
                market_scope,
                code,
                date,
                {return_column} AS ret
            FROM ranking_crowded_long_tail_panel
            {lateral_sql}
            WHERE {match_condition}
        ),
        quantiles AS (
            SELECT
                dimension,
                bucket,
                coalesce(secondary_bucket, '') AS secondary_bucket,
                market_scope,
                count(*) AS observation_count,
                count(DISTINCT code) AS code_count,
                count(DISTINCT date) AS date_count,
                avg(ret) AS mean_forward_excess_return_pct,
                median(ret) AS median_forward_excess_return_pct,
                quantile_cont(ret, 0.01) AS p01_forward_excess_return_pct,
                quantile_cont(ret, 0.05) AS p05_forward_excess_return_pct,
                quantile_cont(ret, 0.10) AS p10_forward_excess_return_pct,
                quantile_cont(ret, 0.25) AS p25_forward_excess_return_pct,
                quantile_cont(ret, 0.75) AS p75_forward_excess_return_pct,
                quantile_cont(ret, 0.90) AS p90_forward_excess_return_pct,
                avg(CASE WHEN ret > 0 THEN 1.0 ELSE 0.0 END) * 100.0
                    AS excess_win_rate_pct,
                avg(CASE WHEN ret <= ? THEN 1.0 ELSE 0.0 END) * 100.0
                    AS severe_loss_rate_pct
            FROM labeled
            GROUP BY dimension, bucket, secondary_bucket, market_scope
            HAVING count(*) >= ?
        )
        SELECT
            q.dimension,
            q.bucket,
            nullif(q.secondary_bucket, '') AS secondary_bucket,
            {int(horizon)} AS horizon,
            q.market_scope,
            q.observation_count,
            q.code_count,
            q.date_count,
            q.mean_forward_excess_return_pct,
            q.median_forward_excess_return_pct,
            q.p01_forward_excess_return_pct,
            q.p05_forward_excess_return_pct,
            avg(l.ret) FILTER (WHERE l.ret <= q.p05_forward_excess_return_pct)
                AS cvar05_forward_excess_return_pct,
            q.p10_forward_excess_return_pct,
            q.p25_forward_excess_return_pct,
            q.p75_forward_excess_return_pct,
            q.p90_forward_excess_return_pct,
            q.excess_win_rate_pct,
            q.severe_loss_rate_pct,
            q.median_forward_excess_return_pct - q.p10_forward_excess_return_pct
                AS median_to_p10_tail_spread_pct,
            q.mean_forward_excess_return_pct - q.median_forward_excess_return_pct
                AS mean_median_gap_pct
        FROM quantiles q
        JOIN labeled l
          ON l.dimension = q.dimension
         AND l.bucket = q.bucket
         AND coalesce(l.secondary_bucket, '') = q.secondary_bucket
         AND l.market_scope = q.market_scope
        GROUP BY
            q.dimension,
            q.bucket,
            q.secondary_bucket,
            q.market_scope,
            q.observation_count,
            q.code_count,
            q.date_count,
            q.mean_forward_excess_return_pct,
            q.median_forward_excess_return_pct,
            q.p01_forward_excess_return_pct,
            q.p05_forward_excess_return_pct,
            q.p10_forward_excess_return_pct,
            q.p25_forward_excess_return_pct,
            q.p75_forward_excess_return_pct,
            q.p90_forward_excess_return_pct,
            q.excess_win_rate_pct,
            q.severe_loss_rate_pct
        ORDER BY
            q.market_scope,
            q.dimension,
            q.bucket,
            q.secondary_bucket
        """,
        [float(severe_loss_threshold_pct), int(min_observations)],
    ).fetchdf()


def _query_observation_sample_df(conn: Any, *, limit: int) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            date,
            code,
            company_name,
            market_scope,
            liquidity_regime,
            sector_33_name,
            sector_strength_bucket,
            long_hybrid_leadership_score,
            atr20_acceleration_ex_overheat_flag,
            atr20_to_atr60_overheat_flag,
            pbr_percentile,
            per_percentile,
            forward_per_percentile,
            psr_percentile,
            forward_psr_percentile,
            forward_p_op_percentile
        FROM ranking_crowded_long_tail_panel
        ORDER BY date, code
        LIMIT ?
        """,
        [int(limit)],
    ).fetchdf()


def _condition_values_sql(conditions: Sequence[tuple[str, str]]) -> str:
    return ", ".join(f"('{_sql_literal(name)}', {sql})" for name, sql in conditions)


def _sql_literal(value: str) -> str:
    return str(value).replace("'", "''")


def _daily_valuation_column_exists(conn: Any, column: str) -> bool:
    row = conn.execute(
        "SELECT count(*) FROM pragma_table_info('daily_valuation') WHERE name = ?",
        [column],
    ).fetchone()
    return bool(row and int(row[0] or 0) > 0)


def _concat_frames(frames: Sequence[pd.DataFrame]) -> pd.DataFrame:
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _validate_params(
    *,
    horizons: Sequence[int],
    min_observations: int,
    observation_sample_limit: int,
) -> None:
    if not horizons or any(int(horizon) <= 0 for horizon in horizons):
        raise ValueError("horizons must contain positive integers")
    if min_observations <= 0:
        raise ValueError("min_observations must be positive")
    if observation_sample_limit <= 0:
        raise ValueError("observation_sample_limit must be positive")
