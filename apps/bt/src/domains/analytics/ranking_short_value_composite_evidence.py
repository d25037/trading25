"""Short-side evidence for high fwd PER/PBR composite vs PSR axes."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Sequence, cast

import pandas as pd

from src.domains.analytics.daily_ranking_consumer_support import (
    compose_daily_ranking_signal_features,
    condition_values_sql,
    table_exists,
)
from src.domains.analytics.daily_ranking_feature_builders import (
    AtrFeaturesRequest,
    PsrFeaturesRequest,
    SectorStrengthFeaturesRequest,
    ShortScaffoldFeaturesRequest,
    SmaFeaturesRequest,
    build_atr_features,
    build_psr_features,
    build_sector_strength_features,
    build_short_scaffold_features,
    build_sma_features,
)
from src.domains.analytics.daily_ranking_research_base import (
    DailyRankingPanelRequest,
    MarketScope,
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

RANKING_SHORT_VALUE_COMPOSITE_EXPERIMENT_ID = (
    "market-behavior/ranking-short-value-composite-evidence"
)
DEFAULT_HORIZONS: tuple[int, ...] = (20, 60)
DEFAULT_MARKET_SCOPES: tuple[str, ...] = ("prime",)
DEFAULT_MIN_OBSERVATIONS = 100
DEFAULT_TAIL_RETURN_THRESHOLD_PCT = 10.0
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
_VALUATION_AXIS_CONDITIONS: tuple[tuple[str, str], ...] = (
    ("all_market", "TRUE"),
    ("high_fwd_per_pbr_composite_80", "high_fwd_per_pbr_composite_score >= 0.8"),
    ("high_fwd_per_pbr_composite_90", "high_fwd_per_pbr_composite_score >= 0.9"),
    ("high_psr_80", "psr_percentile >= 0.8"),
    ("high_psr_90", "psr_percentile >= 0.9"),
    ("overvalued_warning", "overvalued_warning"),
    ("overvalued_or_high_psr", "overvalued_warning OR psr_percentile >= 0.8"),
)
_SHORT_SEARCH_CONDITIONS: tuple[tuple[str, str], ...] = (
    ("sector_soft_weak", "sector_strength_score <= 0.4"),
    (
        "overvalued_breakdown_core",
        "(overvalued_warning OR psr_percentile >= 0.8 "
        "OR high_fwd_per_pbr_composite_score >= 0.8) "
        "AND sector_strength_score <= 0.4 "
        "AND sma5_above_count_5d IN (0, 1)",
    ),
    (
        "overvalued_breakdown_without_psr",
        "(overvalued_warning OR high_fwd_per_pbr_composite_score >= 0.8) "
        "AND sector_strength_score <= 0.4 "
        "AND sma5_above_count_5d IN (0, 1)",
    ),
    (
        "high_fpbr_breakdown",
        "high_fwd_per_pbr_composite_score >= 0.8 "
        "AND sector_strength_score <= 0.4 "
        "AND sma5_above_count_5d IN (0, 1)",
    ),
    (
        "high_psr_breakdown",
        "psr_percentile >= 0.8 "
        "AND sector_strength_score <= 0.4 "
        "AND sma5_above_count_5d IN (0, 1)",
    ),
    (
        "crowded_overvalued_sector_weak",
        "liquidity_regime = 'crowded_rerating' "
        "AND overvalued_warning "
        "AND sector_strength_bucket = 'sector_weak'",
    ),
    (
        "crowded_high_fpbr_sector_weak",
        "liquidity_regime = 'crowded_rerating' "
        "AND high_fwd_per_pbr_composite_score >= 0.8 "
        "AND sector_strength_bucket = 'sector_weak'",
    ),
    (
        "crowded_high_psr_sector_weak",
        "liquidity_regime = 'crowded_rerating' "
        "AND psr_percentile >= 0.8 "
        "AND sector_strength_bucket = 'sector_weak'",
    ),
    (
        "crowded_overvalued_overheat_sector_weak",
        "liquidity_regime = 'crowded_rerating' "
        "AND overvalued_warning "
        "AND atr20_to_atr60_overheat "
        "AND sector_strength_bucket = 'sector_weak'",
    ),
    (
        "crowded_high_fpbr_overheat_sector_weak",
        "liquidity_regime = 'crowded_rerating' "
        "AND high_fwd_per_pbr_composite_score >= 0.8 "
        "AND atr20_to_atr60_overheat "
        "AND sector_strength_bucket = 'sector_weak'",
    ),
    (
        "crowded_high_psr_overheat_sector_weak",
        "liquidity_regime = 'crowded_rerating' "
        "AND psr_percentile >= 0.8 "
        "AND atr20_to_atr60_overheat "
        "AND sector_strength_bucket = 'sector_weak'",
    ),
    (
        "distribution_stress_high_fpbr_sector_weak",
        "liquidity_regime = 'distribution_stress' "
        "AND high_fwd_per_pbr_composite_score >= 0.8 "
        "AND sector_strength_bucket = 'sector_weak'",
    ),
    (
        "distribution_stress_high_psr_sector_weak",
        "liquidity_regime = 'distribution_stress' "
        "AND psr_percentile >= 0.8 "
        "AND sector_strength_bucket = 'sector_weak'",
    ),
    (
        "stale_high_fpbr_rally_fade",
        "liquidity_regime = 'stale_liquidity' "
        "AND high_fwd_per_pbr_composite_score >= 0.8 "
        "AND recent_return_20d_pct > 0 "
        "AND recent_return_60d_pct > 0",
    ),
    (
        "stale_high_psr_rally_fade",
        "liquidity_regime = 'stale_liquidity' "
        "AND psr_percentile >= 0.8 "
        "AND recent_return_20d_pct > 0 "
        "AND recent_return_60d_pct > 0",
    ),
)


@dataclass(frozen=True)
class RankingShortValueCompositeEvidenceResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    market_source: str
    analysis_start_date: str | None
    analysis_end_date: str | None
    horizons: tuple[int, ...]
    market_scopes: tuple[str, ...]
    min_observations: int
    tail_return_threshold_pct: float
    required_tables: tuple[str, ...]
    observation_count: int
    observation_sample_df: pd.DataFrame
    coverage_diagnostics_df: pd.DataFrame
    valuation_axis_evidence_df: pd.DataFrame
    short_search_condition_evidence_df: pd.DataFrame


def run_ranking_short_value_composite_evidence_research(
    db_path: str | Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    market_scopes: Sequence[str] = DEFAULT_MARKET_SCOPES,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
    tail_return_threshold_pct: float = DEFAULT_TAIL_RETURN_THRESHOLD_PCT,
    observation_sample_limit: int = DEFAULT_OBSERVATION_SAMPLE_LIMIT,
) -> RankingShortValueCompositeEvidenceResult:
    resolved_horizons = tuple(sorted({int(horizon) for horizon in horizons}))
    resolved_market_scopes = normalize_daily_ranking_market_scopes(market_scopes)
    _validate_params(
        horizons=resolved_horizons,
        min_observations=min_observations,
        tail_return_threshold_pct=tail_return_threshold_pct,
        observation_sample_limit=observation_sample_limit,
    )
    db_path_obj = Path(db_path).expanduser().resolve()
    if not db_path_obj.is_file():
        raise FileNotFoundError(f"market.duckdb was not found: {db_path_obj}")

    with open_readonly_analysis_connection(
        str(db_path_obj),
        snapshot_prefix="ranking-short-value-composite-",
    ) as ctx:
        _assert_required_tables(ctx.connection)
        market_source = "stock_master_daily_exact_date"
        relations = build_daily_ranking_research_base(
            ctx.connection,
            DailyRankingPanelRequest(
                namespace="short_value_composite",
                analysis_start_date=_parse_optional_date(start_date),
                analysis_end_date=_parse_optional_date(end_date),
                horizons=resolved_horizons,
                market_scopes=cast(
                    tuple[MarketScope, ...],
                    resolved_market_scopes,
                ),
                include_liquidity=True,
                percentile_features=(),
            ),
        )
        signal_source = relations.liquidity_ranked_signals
        if signal_source is None:
            raise RuntimeError("short value research requires liquidity-ranked signals")
        atr_features = build_atr_features(
            ctx.connection,
            AtrFeaturesRequest(source=signal_source, namespace="short_value_atr"),
        )
        psr_features = build_psr_features(
            ctx.connection,
            PsrFeaturesRequest(source=signal_source, namespace="short_value_psr"),
        )
        short_features = build_short_scaffold_features(
            ctx.connection,
            ShortScaffoldFeaturesRequest(
                source=signal_source,
                atr_features=atr_features,
                namespace="short_value_short",
            ),
        )
        sector_features = build_sector_strength_features(
            ctx.connection,
            SectorStrengthFeaturesRequest(
                source=signal_source,
                population_source=signal_source,
                namespace="short_value_sector",
            ),
        )
        sma_features = build_sma_features(
            ctx.connection,
            SmaFeaturesRequest(
                source=signal_source,
                price_history=relations.price_history,
                namespace="short_value_sma",
            ),
        )
        composed = compose_daily_ranking_signal_features(
            ctx.connection,
            source=signal_source,
            features=(psr_features, short_features, sector_features, sma_features),
            namespace="short_value_composite",
        )
        cohort = materialize_daily_ranking_signal_cohort(
            ctx.connection,
            relations,
            source=composed,
            name="short_value_signals",
        )
        evaluated = attach_daily_ranking_outcomes(
            ctx.connection,
            cohort,
            relations,
            name="short_value_outcomes",
        )
        _create_short_value_composite_panel(
            ctx.connection,
            source_name=evaluated.name,
        )
        observation_count = int(
            ctx.connection.execute(
                "SELECT count(*) FROM ranking_short_value_composite_panel"
            ).fetchone()[0]
        )
        result = RankingShortValueCompositeEvidenceResult(
            db_path=str(db_path_obj),
            source_mode=ctx.source_mode,
            source_detail=ctx.source_detail,
            market_source=market_source,
            analysis_start_date=start_date,
            analysis_end_date=end_date,
            horizons=resolved_horizons,
            market_scopes=resolved_market_scopes,
            min_observations=int(min_observations),
            tail_return_threshold_pct=float(tail_return_threshold_pct),
            required_tables=_REQUIRED_TABLES,
            observation_count=observation_count,
            observation_sample_df=_query_observation_sample_df(
                ctx.connection,
                horizons=resolved_horizons,
                limit=observation_sample_limit,
            ),
            coverage_diagnostics_df=_build_coverage_diagnostics_df(ctx.connection),
            valuation_axis_evidence_df=_build_condition_evidence_df(
                ctx.connection,
                condition_family="valuation_axis",
                conditions=_VALUATION_AXIS_CONDITIONS,
                horizons=resolved_horizons,
                min_observations=min_observations,
                tail_return_threshold_pct=tail_return_threshold_pct,
            ),
            short_search_condition_evidence_df=_build_condition_evidence_df(
                ctx.connection,
                condition_family="short_search_condition",
                conditions=_SHORT_SEARCH_CONDITIONS,
                horizons=resolved_horizons,
                min_observations=min_observations,
                tail_return_threshold_pct=tail_return_threshold_pct,
            ),
        )
    return result


def write_ranking_short_value_composite_evidence_bundle(
    result: RankingShortValueCompositeEvidenceResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=RANKING_SHORT_VALUE_COMPOSITE_EXPERIMENT_ID,
        module="src.domains.analytics.ranking_short_value_composite_evidence",
        function="run_ranking_short_value_composite_evidence_research",
        params={
            "horizons": list(result.horizons),
            "market_scopes": list(result.market_scopes),
            "min_observations": result.min_observations,
            "tail_return_threshold_pct": result.tail_return_threshold_pct,
            "required_tables": list(result.required_tables),
            "high_composite": "equal-weight high forward PER percentile + high PBR percentile",
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
            "observation_sample_df": result.observation_sample_df,
            "coverage_diagnostics_df": result.coverage_diagnostics_df,
            "valuation_axis_evidence_df": result.valuation_axis_evidence_df,
            "short_search_condition_evidence_df": (
                result.short_search_condition_evidence_df
            ),
        },
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def build_summary_markdown(result: RankingShortValueCompositeEvidenceResult) -> str:
    parts = [
        "# Ranking Short Value Composite Evidence",
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
        "",
        "## Coverage Diagnostics",
        "",
        _top_rows_for_markdown(result.coverage_diagnostics_df, limit=30),
        "",
        "## Valuation Axis Evidence",
        "",
        _top_rows_for_markdown(result.valuation_axis_evidence_df, limit=120),
        "",
        "## Short Search Condition Evidence",
        "",
        _top_rows_for_markdown(result.short_search_condition_evidence_df, limit=180),
    ]
    return "\n".join(parts).rstrip() + "\n"


def _assert_required_tables(conn: Any) -> None:
    missing = [table for table in _REQUIRED_TABLES if not table_exists(conn, table)]
    if missing:
        raise ValueError(
            f"market.duckdb is missing required tables: {', '.join(missing)}"
        )


def _create_short_value_composite_panel(conn: Any, *, source_name: str) -> None:
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranking_short_value_composite_panel AS
        SELECT
            p.*,
            p.forecast_per AS forward_per,
            p.forecast_per_percentile AS forward_per_percentile,
            CASE
                WHEN p.forecast_per_percentile IS NOT NULL
                 AND p.pbr_percentile IS NOT NULL
                    THEN (p.forecast_per_percentile + p.pbr_percentile) / 2.0
            END AS high_fwd_per_pbr_composite_score,
            CASE
                WHEN p.overvalued_warning OR p.psr_percentile >= 0.8
                    THEN TRUE
                ELSE FALSE
            END AS overvalued_or_high_psr_flag,
            CASE
                WHEN p.overvalued_warning
                  OR p.psr_percentile >= 0.8
                  OR (
                      p.forecast_per_percentile IS NOT NULL
                      AND p.pbr_percentile IS NOT NULL
                      AND (p.forecast_per_percentile + p.pbr_percentile) / 2.0 >= 0.8
                  )
                    THEN TRUE
                ELSE FALSE
            END AS overvalued_or_high_psr_or_high_fpbr_flag
        FROM {source_name} p
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
            avg(CASE WHEN high_fwd_per_pbr_composite_score IS NOT NULL
                THEN 1.0 ELSE 0.0 END) * 100.0 AS high_fpbr_composite_coverage_pct,
            avg(CASE WHEN psr_percentile IS NOT NULL THEN 1.0 ELSE 0.0 END)
                * 100.0 AS psr_coverage_pct,
            avg(CASE WHEN sma5_above_count_5d IS NOT NULL THEN 1.0 ELSE 0.0 END)
                * 100.0 AS sma5_count_coverage_pct,
            avg(CASE WHEN sector_strength_score IS NOT NULL THEN 1.0 ELSE 0.0 END)
                * 100.0 AS sector_strength_coverage_pct,
            median(high_fwd_per_pbr_composite_score)
                AS median_high_fwd_per_pbr_composite_score,
            median(psr_percentile) AS median_psr_percentile,
            avg(CASE WHEN high_fwd_per_pbr_composite_score >= 0.8
                THEN 1.0 ELSE 0.0 END) * 100.0 AS high_fpbr_80_rate_pct,
            avg(CASE WHEN psr_percentile >= 0.8 THEN 1.0 ELSE 0.0 END)
                * 100.0 AS high_psr_80_rate_pct
        FROM ranking_short_value_composite_panel
        GROUP BY market_scope
        ORDER BY market_scope
        """
    ).fetchdf()


def _build_condition_evidence_df(
    conn: Any,
    *,
    condition_family: str,
    conditions: Sequence[tuple[str, str]],
    horizons: Sequence[int],
    min_observations: int,
    tail_return_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    condition_lateral_sql = f"""
        CROSS JOIN LATERAL (
            VALUES {condition_values_sql(conditions)}
        ) AS condition_item(
            condition_name,
            condition_order,
            condition_matches
        )
    """
    for horizon in horizons:
        frames.append(
            _aggregate_short_conditions(
                conn,
                condition_family=condition_family,
                lateral_sql=condition_lateral_sql,
                match_condition="condition_item.condition_matches",
                group_select_sql=(
                    "condition_item.condition_name,\n"
                    "            condition_item.condition_order,\n"
                    f"            {int(horizon)} AS horizon"
                ),
                group_by_sql=(
                    "condition_item.condition_name, "
                    "condition_item.condition_order, market_scope"
                ),
                return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                min_observations=min_observations,
                tail_return_threshold_pct=tail_return_threshold_pct,
            )
        )
    columns = _condition_evidence_columns()
    non_empty = [frame for frame in frames if not frame.empty]
    if not non_empty:
        return pd.DataFrame(columns=columns)
    return (
        pd.concat(non_empty, ignore_index=True)
        .reindex(columns=columns)
        .sort_values(["condition_family", "market_scope", "horizon", "condition_order"])
    )


def _aggregate_short_conditions(
    conn: Any,
    *,
    condition_family: str,
    lateral_sql: str,
    match_condition: str,
    group_select_sql: str,
    group_by_sql: str,
    return_column: str,
    min_observations: int,
    tail_return_threshold_pct: float,
) -> pd.DataFrame:
    horizon_prefix = return_column.replace("forward_close_excess_return_", "")
    raw_return_column = f"forward_close_return_{horizon_prefix}"
    topix_return_expression = f"({raw_return_column} - {return_column})"
    return conn.execute(
        f"""
        SELECT
            ? AS condition_family,
            {group_select_sql},
            market_scope,
            count(*) AS observation_count,
            count(DISTINCT code) AS code_count,
            count(DISTINCT date) AS date_count,
            avg({raw_return_column}) AS mean_forward_return_pct,
            median({raw_return_column}) AS median_forward_return_pct,
            avg({topix_return_expression}) AS mean_topix_return_pct,
            median({topix_return_expression}) AS median_topix_return_pct,
            avg({return_column}) AS mean_forward_excess_return_pct,
            median({return_column}) AS median_forward_excess_return_pct,
            quantile_cont({return_column}, 0.10) AS p10_forward_excess_return_pct,
            quantile_cont({return_column}, 0.90) AS p90_forward_excess_return_pct,
            avg(CASE WHEN {return_column} < 0 THEN 1.0 ELSE 0.0 END) * 100.0
                AS negative_excess_return_rate_pct,
            avg(CASE WHEN {return_column} <= -? THEN 1.0 ELSE 0.0 END) * 100.0
                AS downside_excess_tail_rate_pct,
            avg(CASE WHEN {return_column} >= ? THEN 1.0 ELSE 0.0 END) * 100.0
                AS upside_excess_tail_rate_pct,
            median(high_fwd_per_pbr_composite_score)
                AS median_high_fwd_per_pbr_composite_score,
            median(forward_per_percentile) AS median_forward_per_percentile,
            median(pbr_percentile) AS median_pbr_percentile,
            median(psr_percentile) AS median_psr_percentile,
            median(sector_strength_score) AS median_sector_strength_score,
            median(sma5_above_count_5d) AS median_sma5_above_count_5d,
            avg(CASE WHEN sector_strength_score <= 0.4 THEN 1.0 ELSE 0.0 END)
                * 100.0 AS sector_soft_weak_rate_pct,
            avg(CASE WHEN sector_strength_bucket = 'sector_weak' THEN 1.0 ELSE 0.0 END)
                * 100.0 AS sector_weak_rate_pct,
            avg(CASE WHEN sma5_above_count_5d IN (0, 1) THEN 1.0 ELSE 0.0 END)
                * 100.0 AS sma5_weak_0_1_rate_pct,
            avg(CASE WHEN atr20_to_atr60_overheat THEN 1.0 ELSE 0.0 END)
                * 100.0 AS atr20_to_atr60_overheat_rate_pct,
            avg(CASE WHEN liquidity_regime = 'crowded_rerating' THEN 1.0 ELSE 0.0 END)
                * 100.0 AS crowded_rerating_rate_pct,
            avg(CASE WHEN liquidity_regime = 'distribution_stress' THEN 1.0 ELSE 0.0 END)
                * 100.0 AS distribution_stress_rate_pct,
            avg(CASE WHEN liquidity_regime = 'stale_liquidity' THEN 1.0 ELSE 0.0 END)
                * 100.0 AS stale_liquidity_rate_pct
        FROM ranking_short_value_composite_panel
        {lateral_sql}
        WHERE {match_condition}
          AND {return_column} IS NOT NULL
        GROUP BY {group_by_sql}
        HAVING count(*) >= ?
        """,
        [
            condition_family,
            float(tail_return_threshold_pct),
            float(tail_return_threshold_pct),
            int(min_observations),
        ],
    ).fetchdf()


def _query_observation_sample_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    limit: int,
) -> pd.DataFrame:
    horizon_columns = ",\n".join(
        f"            forward_close_excess_return_{int(horizon)}d_pct"
        for horizon in horizons
    )
    horizon_select = f",\n{horizon_columns}" if horizon_columns else ""
    return conn.execute(
        f"""
        SELECT
            date,
            code,
            company_name,
            market,
            market_code,
            liquidity_regime,
            recent_return_20d_pct,
            recent_return_60d_pct,
            forward_per,
            forward_per_percentile,
            pbr,
            pbr_percentile,
            high_fwd_per_pbr_composite_score,
            psr,
            psr_percentile,
            overvalued_warning,
            very_overvalued_warning,
            sector_strength_score,
            sector_strength_bucket,
            sma5_above_count_5d,
            atr20_to_atr60,
            atr20_change_20d_pct,
            atr20_to_atr60_overheat
            {horizon_select}
        FROM ranking_short_value_composite_panel
        ORDER BY date, code
        LIMIT ?
        """,
        [int(limit)],
    ).fetchdf()


def _validate_params(
    *,
    horizons: Sequence[int],
    min_observations: int,
    tail_return_threshold_pct: float,
    observation_sample_limit: int,
) -> None:
    if not horizons or any(int(horizon) <= 0 for horizon in horizons):
        raise ValueError("horizons must contain positive integers")
    if min_observations <= 0:
        raise ValueError("min_observations must be positive")
    if not pd.notna(tail_return_threshold_pct) or tail_return_threshold_pct <= 0:
        raise ValueError("tail_return_threshold_pct must be positive and finite")
    if observation_sample_limit <= 0:
        raise ValueError("observation_sample_limit must be positive")


def _parse_optional_date(value: str | None) -> date | None:
    return None if value is None else date.fromisoformat(value)


def _condition_evidence_columns() -> list[str]:
    return [
        "condition_family",
        "condition_name",
        "condition_order",
        "market_scope",
        "horizon",
        "observation_count",
        "code_count",
        "date_count",
        "mean_forward_return_pct",
        "median_forward_return_pct",
        "mean_topix_return_pct",
        "median_topix_return_pct",
        "mean_forward_excess_return_pct",
        "median_forward_excess_return_pct",
        "p10_forward_excess_return_pct",
        "p90_forward_excess_return_pct",
        "negative_excess_return_rate_pct",
        "downside_excess_tail_rate_pct",
        "upside_excess_tail_rate_pct",
        "median_high_fwd_per_pbr_composite_score",
        "median_forward_per_percentile",
        "median_pbr_percentile",
        "median_psr_percentile",
        "median_sector_strength_score",
        "median_sma5_above_count_5d",
        "sector_soft_weak_rate_pct",
        "sector_weak_rate_pct",
        "sma5_weak_0_1_rate_pct",
        "atr20_to_atr60_overheat_rate_pct",
        "crowded_rerating_rate_pct",
        "distribution_stress_rate_pct",
        "stale_liquidity_rate_pct",
    ]
