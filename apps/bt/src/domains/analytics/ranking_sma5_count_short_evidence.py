"""SMA5 above-count evidence for Daily Ranking short-side candidates."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Sequence, cast

import pandas as pd

from src.domains.analytics.daily_ranking_consumer_support import (
    PRICE_ACTION_BUCKETS as _PRICE_ACTION_BUCKETS,
    SHORT_OVERLAYS as _SHORT_OVERLAYS,
    aggregate_lateral_conditions as _aggregate_lateral_conditions,
    aggregate_metric_columns as _aggregate_metric_columns,
    compose_daily_ranking_signal_features,
    condition_values_sql as _condition_values_sql,
    liquidity_band_labels as _liquidity_band_labels,
    normalize_liquidity_bands as _normalize_liquidity_bands,
    psr_metric_columns as _psr_metric_columns,
    recomposition_metric_columns as _recomposition_metric_columns,
    recomposition_metric_sql as _recomposition_metric_sql,
    sql_string_list as _sql_string_list,
    table_exists,
)
from src.domains.analytics.daily_ranking_feature_builders import (
    PsrFeaturesRequest,
    SectorStrengthFeaturesRequest,
    SmaFeaturesRequest,
    build_psr_features,
    build_sector_strength_features,
    build_sma_features,
)
from src.domains.analytics.daily_ranking_research_base import (
    DailyRankingPanelRequest,
    MarketScope,
    SignalDerivedColumn,
    SignalExpression,
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
    open_readonly_analysis_connection,
)
from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    write_research_bundle,
)

PUBLIC_FEATURE_BUILDER = build_sma_features
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
    "stock_data_raw",
    "stock_provider_windows",
    "stock_adjustment_events",
    "current_basis_recompute_pending",
    "current_basis_fundamentals_state",
    "topix_data",
    "daily_valuation",
    "stock_master_daily",
    "statements",
    "statement_metrics_adjusted",
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

    market_source = "stock_master_daily_exact_date"

    with open_readonly_analysis_connection(
        str(db_path_obj),
        snapshot_prefix="ranking-sma5-count-short-evidence-",
    ) as ctx:
        _assert_required_tables(ctx.connection)
        relations = build_daily_ranking_research_base(
            ctx.connection,
            DailyRankingPanelRequest(
                namespace="sma5_count_short",
                analysis_start_date=_parse_optional_date(start_date),
                analysis_end_date=_parse_optional_date(end_date),
                horizons=resolved_horizons,
                market_scopes=cast(tuple[MarketScope, ...], resolved_market_scopes),
                include_liquidity=True,
                percentile_features=(),
            ),
        )
        signal_source = relations.ranked_signals
        psr_features = build_psr_features(
            ctx.connection,
            PsrFeaturesRequest(
                source=signal_source,
                namespace="sma5_count_short_psr",
            ),
        )
        sector_features = build_sector_strength_features(
            ctx.connection,
            SectorStrengthFeaturesRequest(
                source=signal_source,
                population_source=signal_source,
                namespace="sma5_count_short_sector",
            ),
        )
        sma_features = build_sma_features(
            ctx.connection,
            SmaFeaturesRequest(
                source=signal_source,
                price_history=relations.price_history,
                namespace="sma5_count_short_sma",
            ),
        )
        composed = compose_daily_ranking_signal_features(
            ctx.connection,
            source=signal_source,
            features=(psr_features, sector_features, sma_features),
            namespace="sma5_count_short",
        )
        liquidity_labels = _liquidity_band_labels(resolved_liquidity_bands)
        cohort = materialize_daily_ranking_signal_cohort(
            ctx.connection,
            relations,
            source=composed,
            name="sma5_count_short_signals",
            predicate=SignalExpression(
                sql=(
                    "liquidity_residual_z IS NOT NULL "
                    "AND recent_return_20d_pct IS NOT NULL "
                    "AND recent_return_60d_pct IS NOT NULL "
                    "AND sma5_above_count_5d IS NOT NULL "
                    f"AND (CASE WHEN liquidity_residual_z >= 1 THEN "
                    "'high_liquidity_z_ge_1' WHEN liquidity_residual_z > -1 "
                    "AND liquidity_residual_z < 1 THEN "
                    "'mid_liquidity_z_minus1_to_1' WHEN liquidity_residual_z < -1 "
                    "THEN 'low_liquidity_z_lt_minus1' ELSE "
                    "'liquidity_boundary_unclassified' END) IN "
                    f"({_sql_string_list(liquidity_labels)}) "
                    "AND recent_return_20d_pct <> 0 "
                    "AND recent_return_60d_pct <> 0"
                ),
                referenced_columns=(
                    "liquidity_residual_z",
                    "recent_return_20d_pct",
                    "recent_return_60d_pct",
                    "sma5_above_count_5d",
                ),
            ),
            derived_columns=(
                SignalDerivedColumn(
                    name="liquidity_band",
                    expression=SignalExpression(
                        sql=(
                            "CASE WHEN liquidity_residual_z >= 1 THEN "
                            "'high_liquidity_z_ge_1' "
                            "WHEN liquidity_residual_z > -1 "
                            "AND liquidity_residual_z < 1 THEN "
                            "'mid_liquidity_z_minus1_to_1' "
                            "WHEN liquidity_residual_z < -1 THEN "
                            "'low_liquidity_z_lt_minus1' "
                            "ELSE 'liquidity_boundary_unclassified' END"
                        ),
                        referenced_columns=("liquidity_residual_z",),
                    ),
                    sql_type="VARCHAR",
                ),
                SignalDerivedColumn(
                    name="price_action_bucket",
                    expression=SignalExpression(
                        sql=(
                            "CASE WHEN recent_return_20d_pct > 0 "
                            "AND recent_return_60d_pct > 0 THEN "
                            "'dual_positive_crowded' "
                            "WHEN recent_return_20d_pct > 0 "
                            "AND recent_return_60d_pct < 0 THEN "
                            "'recent20_positive_60d_negative' "
                            "WHEN recent_return_20d_pct < 0 "
                            "AND recent_return_60d_pct > 0 THEN "
                            "'recent20_negative_60d_positive' "
                            "WHEN recent_return_20d_pct < 0 "
                            "AND recent_return_60d_pct < 0 THEN "
                            "'dual_negative_stress' "
                            "ELSE 'price_action_unclassified' END"
                        ),
                        referenced_columns=(
                            "recent_return_20d_pct",
                            "recent_return_60d_pct",
                        ),
                    ),
                    sql_type="VARCHAR",
                ),
            ),
        )
        evaluated = attach_daily_ranking_outcomes(
            ctx.connection,
            cohort,
            relations,
            name="sma5_count_short",
        )
        _create_sma5_count_short_panel(
            ctx.connection,
            source_name=evaluated.name,
            horizons=resolved_horizons,
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
    missing = [table for table in _REQUIRED_TABLES if not table_exists(conn, table)]
    if missing:
        raise ValueError(
            f"market.duckdb is missing required tables: {', '.join(missing)}"
        )


def _create_sma5_count_short_panel(
    conn: Any,
    *,
    source_name: str,
    horizons: Sequence[int],
) -> None:
    benchmark_columns = ",\n            ".join(
        expression
        for horizon in horizons
        for expression in (
            f"forward_close_return_{int(horizon)}d_pct "
            f"- forward_close_excess_return_{int(horizon)}d_pct "
            f"AS topix_close_return_{int(horizon)}d_pct",
            f"forward_close_return_{int(horizon)}d_pct "
            f"- forward_close_n225_excess_return_{int(horizon)}d_pct "
            f"AS n225_close_return_{int(horizon)}d_pct",
        )
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranking_sma5_count_short_panel AS
        SELECT *,
            {benchmark_columns}
        FROM {source_name}
        """
    )


def _build_short_overlay_evidence_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    overlay_lateral_sql = f"""
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
    """
    for horizon in horizons:
        frames.append(
            _aggregate_lateral_conditions(
                conn,
                source_name="ranking_sma5_count_short_panel",
                lateral_sql=overlay_lateral_sql,
                match_condition=(
                    "price_action_bucket.price_action_bucket_matches "
                    "AND short_overlay.short_overlay_matches"
                ),
                group_select_sql=(
                    "'price_action_short_overlay' AS condition_family,\n"
                    "            liquidity_band,\n"
                    "            price_action_bucket.price_action_bucket,\n"
                    "            price_action_bucket.price_action_bucket_order,\n"
                    "            short_overlay.short_overlay,\n"
                    "            short_overlay.short_overlay_order,\n"
                    f"            {int(horizon)} AS horizon"
                ),
                group_by_sql=(
                    "liquidity_band, "
                    "price_action_bucket.price_action_bucket, "
                    "price_action_bucket.price_action_bucket_order, "
                    "short_overlay.short_overlay, "
                    "short_overlay.short_overlay_order, "
                    "market_scope"
                ),
                return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
                extra_metric_sql=_recomposition_metric_sql(),
            )
        )
    return _concat_sorted(frames, columns=_short_overlay_columns())


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
    return (
        _recomposition_metric_sql()
        + """,
            median(sma5_above_count_5d) AS median_sma5_above_count_5d,
            avg(CASE WHEN sma5_above_count_5d = 0 THEN 1.0 ELSE 0.0 END)
                * 100.0 AS sma5_count_0_rate_pct,
            avg(CASE WHEN sma5_above_count_5d = 5 THEN 1.0 ELSE 0.0 END)
                * 100.0 AS sma5_count_5_rate_pct"""
    )


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


def _short_overlay_columns() -> list[str]:
    return [
        "condition_family",
        "liquidity_band",
        "price_action_bucket",
        "price_action_bucket_order",
        "short_overlay",
        "short_overlay_order",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
        *_psr_metric_columns(),
        *_recomposition_metric_columns(),
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


def _parse_optional_date(value: str | None) -> date | None:
    return date.fromisoformat(value) if value is not None else None
