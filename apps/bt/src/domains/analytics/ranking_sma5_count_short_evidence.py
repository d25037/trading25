"""SMA5 above-count evidence for Daily Ranking short-side candidates."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

import pandas as pd

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
from src.domains.analytics.ranking_forecast_operating_profit_growth_evidence import (
    _aggregate_lateral_conditions,
    _aggregate_metric_columns,
    _condition_values_sql,
)
from src.domains.analytics.ranking_liquidity_price_action_recomposition import (
    _PRICE_ACTION_BUCKETS,
    _SHORT_OVERLAYS,
    _build_short_overlay_evidence_df,
    _liquidity_band_labels,
    _normalize_liquidity_bands,
    _recomposition_metric_columns,
    _recomposition_metric_sql,
    _sql_string_list,
)
from src.domains.analytics.ranking_psr_valuation_evidence import (
    _create_psr_valuation_panel,
    _psr_metric_columns,
)
from src.domains.analytics.ranking_sector_strength_evidence import (
    _create_sector_strength_tables,
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

RANKING_SMA5_COUNT_SHORT_EVIDENCE_EXPERIMENT_ID = (
    "market-behavior/ranking-sma5-count-short-evidence"
)
DEFAULT_HORIZONS: tuple[int, ...] = (5, 20, 60)
DEFAULT_MARKET_SCOPES: tuple[str, ...] = ("prime",)
DEFAULT_LIQUIDITY_BANDS: tuple[str, ...] = ("high", "mid", "low")
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
_SMA5_COUNT_BUCKETS: tuple[tuple[str, str], ...] = tuple(
    (f"sma5_above_count_{count}", f"sma5_above_count_5d = {count}")
    for count in range(6)
)
_SMA5_COUNT_GROUP_BUCKETS: tuple[tuple[str, str], ...] = (
    ("sma5_above_count_0_1", "sma5_above_count_5d IN (0, 1)"),
    ("sma5_above_count_2_3", "sma5_above_count_5d IN (2, 3)"),
    ("sma5_above_count_4_5", "sma5_above_count_5d IN (4, 5)"),
)


@dataclass(frozen=True)
class RankingSma5CountShortEvidenceResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    market_source: str
    analysis_start_date: str | None
    analysis_end_date: str | None
    horizons: tuple[int, ...]
    market_scopes: tuple[str, ...]
    liquidity_bands: tuple[str, ...]
    min_observations: int
    severe_loss_threshold_pct: float
    required_tables: tuple[str, ...]
    observation_count: int
    coverage_diagnostics_df: pd.DataFrame
    short_overlay_evidence_df: pd.DataFrame
    sma5_count_evidence_df: pd.DataFrame
    sma5_count_group_evidence_df: pd.DataFrame
    short_overlay_sma5_count_evidence_df: pd.DataFrame
    short_overlay_sma5_count_group_evidence_df: pd.DataFrame
    observation_sample_df: pd.DataFrame


def run_ranking_sma5_count_short_evidence_research(
    db_path: str | Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    market_scopes: Sequence[str] = DEFAULT_MARKET_SCOPES,
    liquidity_bands: Sequence[str] = DEFAULT_LIQUIDITY_BANDS,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
    severe_loss_threshold_pct: float = DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    observation_sample_limit: int = DEFAULT_OBSERVATION_SAMPLE_LIMIT,
) -> RankingSma5CountShortEvidenceResult:
    resolved_horizons = tuple(sorted({int(horizon) for horizon in horizons}))
    resolved_market_scopes = normalize_daily_ranking_market_scopes(market_scopes)
    resolved_liquidity_bands = _normalize_liquidity_bands(liquidity_bands)
    _validate_params(
        horizons=resolved_horizons,
        liquidity_bands=resolved_liquidity_bands,
        min_observations=min_observations,
        severe_loss_threshold_pct=severe_loss_threshold_pct,
        observation_sample_limit=observation_sample_limit,
    )
    db_path_obj = Path(db_path).expanduser().resolve()
    if not db_path_obj.is_file():
        raise FileNotFoundError(f"market.duckdb was not found: {db_path_obj}")

    query_start = daily_ranking_query_start_date(start_date, warmup_calendar_days=720)
    query_end = daily_ranking_query_end_date(end_date, max_horizon=max(resolved_horizons))
    market_source = "stock_master_daily_exact_date"

    with open_readonly_analysis_connection(
        str(db_path_obj),
        snapshot_prefix="ranking-sma5-count-short-evidence-",
    ) as ctx:
        _assert_required_tables(ctx.connection)
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
            include_relation_percentiles=False,
        )
        _create_psr_valuation_panel(ctx.connection)
        _create_sector_strength_tables(ctx.connection, horizons=resolved_horizons)
        _create_sma5_count_short_panel(
            ctx.connection,
            liquidity_bands=resolved_liquidity_bands,
        )
        observation_count = int(
            ctx.connection.execute(
                "SELECT count(*) FROM ranking_sma5_count_short_panel"
            ).fetchone()[0]
        )
        result = RankingSma5CountShortEvidenceResult(
            db_path=str(db_path_obj),
            source_mode=ctx.source_mode,
            source_detail=ctx.source_detail,
            market_source=market_source,
            analysis_start_date=start_date,
            analysis_end_date=end_date,
            horizons=resolved_horizons,
            market_scopes=resolved_market_scopes,
            liquidity_bands=resolved_liquidity_bands,
            min_observations=int(min_observations),
            severe_loss_threshold_pct=float(severe_loss_threshold_pct),
            required_tables=_REQUIRED_TABLES,
            observation_count=observation_count,
            coverage_diagnostics_df=_build_coverage_diagnostics_df(ctx.connection),
            short_overlay_evidence_df=_build_short_overlay_evidence_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            sma5_count_evidence_df=_build_sma5_count_evidence_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            sma5_count_group_evidence_df=_build_sma5_count_group_evidence_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            short_overlay_sma5_count_evidence_df=(
                _build_short_overlay_sma5_count_evidence_df(
                    ctx.connection,
                    horizons=resolved_horizons,
                    min_observations=min_observations,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
            ),
            short_overlay_sma5_count_group_evidence_df=(
                _build_short_overlay_sma5_count_group_evidence_df(
                    ctx.connection,
                    horizons=resolved_horizons,
                    min_observations=min_observations,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
            ),
            observation_sample_df=_query_observation_sample_df(
                ctx.connection,
                horizons=resolved_horizons,
                limit=observation_sample_limit,
            ),
        )
    return result


def write_ranking_sma5_count_short_evidence_bundle(
    result: RankingSma5CountShortEvidenceResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=RANKING_SMA5_COUNT_SHORT_EVIDENCE_EXPERIMENT_ID,
        module="src.domains.analytics.ranking_sma5_count_short_evidence",
        function="run_ranking_sma5_count_short_evidence_research",
        params={
            "horizons": list(result.horizons),
            "market_scopes": list(result.market_scopes),
            "liquidity_bands": list(result.liquidity_bands),
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
            "primary_outcome": "forward_close_excess_return_{horizon}d_pct",
            "sma5_parameter": "sma5_above_count_5d",
        },
        result_tables={
            "coverage_diagnostics_df": result.coverage_diagnostics_df,
            "short_overlay_evidence_df": result.short_overlay_evidence_df,
            "sma5_count_evidence_df": result.sma5_count_evidence_df,
            "sma5_count_group_evidence_df": result.sma5_count_group_evidence_df,
            "short_overlay_sma5_count_evidence_df": (
                result.short_overlay_sma5_count_evidence_df
            ),
            "short_overlay_sma5_count_group_evidence_df": (
                result.short_overlay_sma5_count_group_evidence_df
            ),
            "observation_sample_df": result.observation_sample_df,
        },
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def build_summary_markdown(result: RankingSma5CountShortEvidenceResult) -> str:
    parts = [
        "# Ranking SMA5 Count Short Evidence",
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
        f"- liquidity_bands: `{', '.join(result.liquidity_bands)}`",
        f"- observation_count: `{result.observation_count}`",
        "",
        "## Coverage Diagnostics",
        "",
        _top_rows_for_markdown(result.coverage_diagnostics_df, limit=80),
        "",
        "## Short Overlay Evidence",
        "",
        _top_rows_for_markdown(result.short_overlay_evidence_df, limit=120),
        "",
        "## SMA5 Count Evidence",
        "",
        _top_rows_for_markdown(result.sma5_count_evidence_df, limit=120),
        "",
        "## SMA5 Count Group Evidence",
        "",
        _top_rows_for_markdown(result.sma5_count_group_evidence_df, limit=120),
        "",
        "## Short Overlay x SMA5 Count Evidence",
        "",
        _top_rows_for_markdown(result.short_overlay_sma5_count_evidence_df, limit=240),
        "",
        "## Short Overlay x SMA5 Count Group Evidence",
        "",
        _top_rows_for_markdown(
            result.short_overlay_sma5_count_group_evidence_df,
            limit=240,
        ),
    ]
    return "\n".join(parts).rstrip() + "\n"


def _assert_required_tables(conn: Any) -> None:
    missing = [table for table in _REQUIRED_TABLES if not _table_exists(conn, table)]
    if missing:
        raise ValueError(f"market.duckdb is missing required tables: {', '.join(missing)}")


def _create_sma5_count_short_panel(
    conn: Any,
    *,
    liquidity_bands: Sequence[str],
) -> None:
    liquidity_band_list = _sql_string_list(_liquidity_band_labels(liquidity_bands))
    stock_code = normalize_code_sql("sd.code")
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranking_sma5_count_short_panel AS
        WITH normalized_prices AS (
            SELECT
                {stock_code} AS code,
                sd.date,
                arg_min(
                    sd.close,
                    CASE WHEN length(sd.code) = 4 THEN '0:' ELSE '1:' END || sd.code
                ) AS close
            FROM stock_data sd
            WHERE EXISTS (
                SELECT 1
                FROM ranking_psr_valuation_panel p
                WHERE p.code = {stock_code}
            )
            GROUP BY {stock_code}, sd.date
        ),
        sma5_base AS (
            SELECT
                code,
                date,
                close,
                avg(close) OVER (
                    PARTITION BY code ORDER BY date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                ) AS sma5,
                count(close) OVER (
                    PARTITION BY code ORDER BY date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                ) AS sma5_sessions
            FROM normalized_prices
            WHERE close > 0
        ),
        sma5_flags AS (
            SELECT
                code,
                date,
                CASE
                    WHEN sma5_sessions = 5 AND close > sma5 THEN 1
                    WHEN sma5_sessions = 5 THEN 0
                END AS close_above_sma5_flag
            FROM sma5_base
        ),
        sma5_counts AS (
            SELECT
                code,
                date,
                sum(close_above_sma5_flag) OVER (
                    PARTITION BY code ORDER BY date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                ) AS sma5_above_count_5d,
                count(close_above_sma5_flag) OVER (
                    PARTITION BY code ORDER BY date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                ) AS sma5_above_count_sessions
            FROM sma5_flags
        ),
        classified AS (
            SELECT
                p.*,
                s.sector_strength_score,
                s.sector_strength_bucket,
                CAST(sc.sma5_above_count_5d AS INTEGER) AS sma5_above_count_5d,
                CASE
                    WHEN p.liquidity_residual_z >= 1 THEN 'high_liquidity_z_ge_1'
                    WHEN p.liquidity_residual_z > -1
                     AND p.liquidity_residual_z < 1
                        THEN 'mid_liquidity_z_minus1_to_1'
                    WHEN p.liquidity_residual_z < -1 THEN 'low_liquidity_z_lt_minus1'
                    ELSE 'liquidity_boundary_unclassified'
                END AS liquidity_band,
                CASE
                    WHEN p.recent_return_20d_pct > 0
                     AND p.recent_return_60d_pct > 0
                        THEN 'dual_positive_crowded'
                    WHEN p.recent_return_20d_pct > 0
                     AND p.recent_return_60d_pct < 0
                        THEN 'recent20_positive_60d_negative'
                    WHEN p.recent_return_20d_pct < 0
                     AND p.recent_return_60d_pct > 0
                        THEN 'recent20_negative_60d_positive'
                    WHEN p.recent_return_20d_pct < 0
                     AND p.recent_return_60d_pct < 0
                        THEN 'dual_negative_stress'
                    ELSE 'price_action_unclassified'
                END AS price_action_bucket
            FROM ranking_psr_valuation_panel p
            LEFT JOIN ranking_sector_master sm
              ON sm.code = p.code
             AND sm.date = p.date
            LEFT JOIN ranking_sector_daily_state s
              ON s.market_scope = p.market_scope
             AND s.date = p.date
             AND s.sector_33_code = sm.sector_33_code
             AND s.sector_33_name = sm.sector_33_name
            LEFT JOIN sma5_counts sc
              ON sc.code = p.code
             AND sc.date = p.date
             AND sc.sma5_above_count_sessions = 5
            WHERE p.liquidity_residual_z IS NOT NULL
              AND p.recent_return_20d_pct IS NOT NULL
              AND p.recent_return_60d_pct IS NOT NULL
        )
        SELECT *
        FROM classified
        WHERE liquidity_band IN ({liquidity_band_list})
          AND price_action_bucket <> 'price_action_unclassified'
          AND sma5_above_count_5d IS NOT NULL
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE TEMP VIEW ranking_liquidity_price_action_recomposition_panel AS
        SELECT * FROM ranking_sma5_count_short_panel
        """
    )


def _build_coverage_diagnostics_df(conn: Any) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            market_scope,
            liquidity_band,
            sma5_above_count_5d,
            count(*) AS observation_count,
            count(DISTINCT code) AS code_count,
            count(DISTINCT date) AS date_count,
            avg(CASE WHEN psr_percentile >= 0.8 THEN 1.0 ELSE 0.0 END)
                * 100.0 AS high_psr_rate_pct,
            avg(CASE WHEN sector_strength_bucket = 'sector_weak' THEN 1.0 ELSE 0.0 END)
                * 100.0 AS sector_weak_rate_pct,
            avg(CASE WHEN price_action_bucket = 'dual_positive_crowded' THEN 1.0 ELSE 0.0 END)
                * 100.0 AS dual_positive_rate_pct,
            avg(CASE WHEN price_action_bucket = 'dual_negative_stress' THEN 1.0 ELSE 0.0 END)
                * 100.0 AS dual_negative_rate_pct,
            median(liquidity_residual_z) AS median_liquidity_residual_z,
            median(recent_return_20d_pct) AS median_recent_return_20d_pct,
            median(recent_return_60d_pct) AS median_recent_return_60d_pct,
            median(psr_percentile) AS median_psr_percentile
        FROM ranking_sma5_count_short_panel
        GROUP BY market_scope, liquidity_band, sma5_above_count_5d
        ORDER BY market_scope, liquidity_band, sma5_above_count_5d
        """
    ).fetchdf()


def _build_sma5_count_evidence_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    sma5_lateral_sql = f"""
        CROSS JOIN LATERAL (
            VALUES {_condition_values_sql(_SMA5_COUNT_BUCKETS)}
        ) AS sma5_count(
            sma5_count_bucket,
            sma5_count_order,
            sma5_count_matches
        )
    """
    for horizon in horizons:
        frames.append(
            _aggregate_lateral_conditions(
                conn,
                source_name="ranking_sma5_count_short_panel",
                lateral_sql=sma5_lateral_sql,
                match_condition="sma5_count.sma5_count_matches",
                group_select_sql=(
                    "'sma5_count' AS condition_family,\n"
                    "            liquidity_band,\n"
                    "            sma5_count.sma5_count_bucket,\n"
                    "            sma5_count.sma5_count_order,\n"
                    "            sma5_count.sma5_count_order AS sma5_above_count_5d,\n"
                    f"            {int(horizon)} AS horizon"
                ),
                group_by_sql=(
                    "liquidity_band, "
                    "sma5_count.sma5_count_bucket, "
                    "sma5_count.sma5_count_order, "
                    "market_scope"
                ),
                return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
                extra_metric_sql=_sma5_metric_sql(),
            )
        )
    return _concat_sorted(frames, columns=_sma5_count_columns())


def _build_sma5_count_group_evidence_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    sma5_group_lateral_sql = f"""
        CROSS JOIN LATERAL (
            VALUES {_condition_values_sql(_SMA5_COUNT_GROUP_BUCKETS)}
        ) AS sma5_count_group(
            sma5_count_group,
            sma5_count_group_order,
            sma5_count_group_matches
        )
    """
    for horizon in horizons:
        frames.append(
            _aggregate_lateral_conditions(
                conn,
                source_name="ranking_sma5_count_short_panel",
                lateral_sql=sma5_group_lateral_sql,
                match_condition="sma5_count_group.sma5_count_group_matches",
                group_select_sql=(
                    "'sma5_count_group' AS condition_family,\n"
                    "            liquidity_band,\n"
                    "            sma5_count_group.sma5_count_group,\n"
                    "            sma5_count_group.sma5_count_group_order,\n"
                    f"            {int(horizon)} AS horizon"
                ),
                group_by_sql=(
                    "liquidity_band, "
                    "sma5_count_group.sma5_count_group, "
                    "sma5_count_group.sma5_count_group_order, "
                    "market_scope"
                ),
                return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
                extra_metric_sql=_sma5_metric_sql(),
            )
        )
    return _concat_sorted(frames, columns=_sma5_count_group_columns())


def _build_short_overlay_sma5_count_evidence_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    lateral_sql = f"""
        CROSS JOIN LATERAL (
            VALUES {_condition_values_sql(_PRICE_ACTION_BUCKETS)}
        ) AS price_action_bucket(
            price_action_bucket,
            price_action_bucket_order,
            price_action_bucket_matches
        )
        CROSS JOIN LATERAL (
            VALUES {_condition_values_sql(_SHORT_OVERLAYS)}
        ) AS short_overlay(short_overlay, short_overlay_order, short_overlay_matches)
        CROSS JOIN LATERAL (
            VALUES {_condition_values_sql(_SMA5_COUNT_BUCKETS)}
        ) AS sma5_count(
            sma5_count_bucket,
            sma5_count_order,
            sma5_count_matches
        )
    """
    for horizon in horizons:
        frames.append(
            _aggregate_lateral_conditions(
                conn,
                source_name="ranking_sma5_count_short_panel",
                lateral_sql=lateral_sql,
                match_condition=(
                    "price_action_bucket.price_action_bucket_matches "
                    "AND short_overlay.short_overlay_matches "
                    "AND sma5_count.sma5_count_matches"
                ),
                group_select_sql=(
                    "'short_overlay_sma5_count' AS condition_family,\n"
                    "            liquidity_band,\n"
                    "            price_action_bucket.price_action_bucket,\n"
                    "            price_action_bucket.price_action_bucket_order,\n"
                    "            short_overlay.short_overlay,\n"
                    "            short_overlay.short_overlay_order,\n"
                    "            sma5_count.sma5_count_bucket,\n"
                    "            sma5_count.sma5_count_order,\n"
                    "            sma5_count.sma5_count_order AS sma5_above_count_5d,\n"
                    f"            {int(horizon)} AS horizon"
                ),
                group_by_sql=(
                    "liquidity_band, "
                    "price_action_bucket.price_action_bucket, "
                    "price_action_bucket.price_action_bucket_order, "
                    "short_overlay.short_overlay, "
                    "short_overlay.short_overlay_order, "
                    "sma5_count.sma5_count_bucket, "
                    "sma5_count.sma5_count_order, "
                    "market_scope"
                ),
                return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
                extra_metric_sql=_sma5_metric_sql(),
            )
        )
    return _concat_sorted(frames, columns=_short_overlay_sma5_count_columns())


def _build_short_overlay_sma5_count_group_evidence_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    lateral_sql = f"""
        CROSS JOIN LATERAL (
            VALUES {_condition_values_sql(_PRICE_ACTION_BUCKETS)}
        ) AS price_action_bucket(
            price_action_bucket,
            price_action_bucket_order,
            price_action_bucket_matches
        )
        CROSS JOIN LATERAL (
            VALUES {_condition_values_sql(_SHORT_OVERLAYS)}
        ) AS short_overlay(short_overlay, short_overlay_order, short_overlay_matches)
        CROSS JOIN LATERAL (
            VALUES {_condition_values_sql(_SMA5_COUNT_GROUP_BUCKETS)}
        ) AS sma5_count_group(
            sma5_count_group,
            sma5_count_group_order,
            sma5_count_group_matches
        )
    """
    for horizon in horizons:
        frames.append(
            _aggregate_lateral_conditions(
                conn,
                source_name="ranking_sma5_count_short_panel",
                lateral_sql=lateral_sql,
                match_condition=(
                    "price_action_bucket.price_action_bucket_matches "
                    "AND short_overlay.short_overlay_matches "
                    "AND sma5_count_group.sma5_count_group_matches"
                ),
                group_select_sql=(
                    "'short_overlay_sma5_count_group' AS condition_family,\n"
                    "            liquidity_band,\n"
                    "            price_action_bucket.price_action_bucket,\n"
                    "            price_action_bucket.price_action_bucket_order,\n"
                    "            short_overlay.short_overlay,\n"
                    "            short_overlay.short_overlay_order,\n"
                    "            sma5_count_group.sma5_count_group,\n"
                    "            sma5_count_group.sma5_count_group_order,\n"
                    f"            {int(horizon)} AS horizon"
                ),
                group_by_sql=(
                    "liquidity_band, "
                    "price_action_bucket.price_action_bucket, "
                    "price_action_bucket.price_action_bucket_order, "
                    "short_overlay.short_overlay, "
                    "short_overlay.short_overlay_order, "
                    "sma5_count_group.sma5_count_group, "
                    "sma5_count_group.sma5_count_group_order, "
                    "market_scope"
                ),
                return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
                extra_metric_sql=_sma5_metric_sql(),
            )
        )
    return _concat_sorted(frames, columns=_short_overlay_sma5_count_group_columns())


def _sma5_metric_sql() -> str:
    return _recomposition_metric_sql() + """,
            median(sma5_above_count_5d) AS median_sma5_above_count_5d,
            avg(CASE WHEN sma5_above_count_5d = 0 THEN 1.0 ELSE 0.0 END)
                * 100.0 AS sma5_count_0_rate_pct,
            avg(CASE WHEN sma5_above_count_5d = 5 THEN 1.0 ELSE 0.0 END)
                * 100.0 AS sma5_count_5_rate_pct"""


def _query_observation_sample_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    limit: int,
) -> pd.DataFrame:
    forward_columns = ",\n            ".join(
        column
        for horizon in horizons
        for column in (
            f"forward_close_return_{int(horizon)}d_pct",
            f"topix_close_return_{int(horizon)}d_pct",
            f"forward_close_excess_return_{int(horizon)}d_pct",
        )
    )
    return conn.execute(
        f"""
        SELECT
            date,
            code,
            company_name,
            market,
            market_code,
            liquidity_regime,
            liquidity_band,
            price_action_bucket,
            sma5_above_count_5d,
            close,
            recent_return_20d_pct,
            recent_return_60d_pct,
            liquidity_residual_z,
            sector_strength_score,
            sector_strength_bucket,
            psr,
            psr_percentile,
            psr_signal,
            overvalued_warning,
            very_overvalued_warning,
            {forward_columns}
        FROM ranking_sma5_count_short_panel
        ORDER BY date, code
        LIMIT ?
        """,
        [int(limit)],
    ).fetchdf()


def _validate_params(
    *,
    horizons: Sequence[int],
    liquidity_bands: Sequence[str],
    min_observations: int,
    severe_loss_threshold_pct: float,
    observation_sample_limit: int,
) -> None:
    if not horizons or any(int(horizon) <= 0 for horizon in horizons):
        raise ValueError("horizons must contain positive integers")
    if not liquidity_bands:
        raise ValueError("liquidity_bands must contain at least one item")
    if min_observations <= 0:
        raise ValueError("min_observations must be positive")
    if severe_loss_threshold_pct >= 0:
        raise ValueError("severe_loss_threshold_pct must be negative")
    if observation_sample_limit <= 0:
        raise ValueError("observation_sample_limit must be positive")


def _sma5_count_columns() -> list[str]:
    return [
        "condition_family",
        "liquidity_band",
        "sma5_count_bucket",
        "sma5_count_order",
        "sma5_above_count_5d",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
        *_psr_metric_columns(),
        *_sma5_metric_columns(),
    ]


def _sma5_count_group_columns() -> list[str]:
    return [
        "condition_family",
        "liquidity_band",
        "sma5_count_group",
        "sma5_count_group_order",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
        *_psr_metric_columns(),
        *_sma5_metric_columns(),
    ]


def _short_overlay_sma5_count_columns() -> list[str]:
    return [
        "condition_family",
        "liquidity_band",
        "price_action_bucket",
        "price_action_bucket_order",
        "short_overlay",
        "short_overlay_order",
        "sma5_count_bucket",
        "sma5_count_order",
        "sma5_above_count_5d",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
        *_psr_metric_columns(),
        *_sma5_metric_columns(),
    ]


def _short_overlay_sma5_count_group_columns() -> list[str]:
    return [
        "condition_family",
        "liquidity_band",
        "price_action_bucket",
        "price_action_bucket_order",
        "short_overlay",
        "short_overlay_order",
        "sma5_count_group",
        "sma5_count_group_order",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
        *_psr_metric_columns(),
        *_sma5_metric_columns(),
    ]


def _sma5_metric_columns() -> list[str]:
    return [
        *_recomposition_metric_columns(),
        "median_sma5_above_count_5d",
        "sma5_count_0_rate_pct",
        "sma5_count_5_rate_pct",
    ]


def _concat_sorted(
    frames: Sequence[pd.DataFrame],
    *,
    columns: Sequence[str],
) -> pd.DataFrame:
    non_empty = [frame for frame in frames if not frame.empty]
    if not non_empty:
        return pd.DataFrame(columns=list(columns))
    frame = pd.concat(non_empty, ignore_index=True)
    order_columns = [
        column
        for column in (
            "condition_family",
            "market_scope",
            "horizon",
            "liquidity_band",
            "price_action_bucket_order",
            "short_overlay_order",
            "sma5_count_group_order",
            "sma5_count_order",
        )
        if column in frame.columns
    ]
    return frame.reindex(columns=list(columns)).sort_values(
        order_columns,
        kind="stable",
    )
