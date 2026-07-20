"""PSR valuation evidence for Daily Ranking."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Sequence, cast

import pandas as pd

from src.domains.analytics.daily_ranking_consumer_support import (
    aggregate_lateral_conditions,
    aggregate_metric_columns,
    compose_daily_ranking_signal_features,
    condition_values_sql,
    concat_sorted,
    deep_dive_metric_columns,
    deep_dive_metric_sql,
    sql_literal,
    table_exists,
)
from src.domains.analytics.daily_ranking_feature_builders import (
    AtrFeaturesRequest,
    LongLeadershipFeaturesRequest,
    PsrFeaturesRequest,
    SectorStrengthFeaturesRequest,
    ShortScaffoldFeaturesRequest,
    build_atr_features,
    build_long_leadership_features,
    build_psr_features,
    build_sector_strength_features,
    build_short_scaffold_features,
    publish_legacy_psr_features,
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

    with open_readonly_analysis_connection(
        str(db_path_obj),
        snapshot_prefix="ranking-psr-valuation-",
    ) as ctx:
        _assert_required_tables(ctx.connection)
        market_source = "stock_master_daily_exact_date"
        relations = build_daily_ranking_research_base(
            ctx.connection,
            DailyRankingPanelRequest(
                namespace="psr_valuation",
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
            raise RuntimeError("PSR research requires liquidity-ranked signals")
        psr_features = build_psr_features(
            ctx.connection,
            PsrFeaturesRequest(source=signal_source, namespace="psr_valuation_psr"),
        )
        atr_features = build_atr_features(
            ctx.connection,
            AtrFeaturesRequest(source=signal_source, namespace="psr_valuation_atr"),
        )
        short_features = build_short_scaffold_features(
            ctx.connection,
            ShortScaffoldFeaturesRequest(
                source=signal_source,
                atr_features=atr_features,
                namespace="psr_valuation_short",
            ),
        )
        sector_features = build_sector_strength_features(
            ctx.connection,
            SectorStrengthFeaturesRequest(
                source=signal_source,
                population_source=signal_source,
                namespace="psr_valuation_sector",
            ),
        )
        leadership_features = build_long_leadership_features(
            ctx.connection,
            LongLeadershipFeaturesRequest(
                source=signal_source,
                sector_features=sector_features,
                namespace="psr_valuation_leadership",
                leadership_windows=_LEADERSHIP_WINDOWS,
            ),
        )
        composed = compose_daily_ranking_signal_features(
            ctx.connection,
            source=signal_source,
            features=(psr_features, leadership_features, short_features),
            namespace="psr_valuation",
        )
        cohort = materialize_daily_ranking_signal_cohort(
            ctx.connection,
            relations,
            source=composed,
            name="psr_valuation_signals",
        )
        evaluated = attach_daily_ranking_outcomes(
            ctx.connection,
            cohort,
            relations,
            name="psr_valuation_outcomes",
        )
        _create_evaluated_psr_valuation_panel(
            ctx.connection,
            source_name=evaluated.name,
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
                "bt market.duckdb persists forecast sales and materializes "
                "daily_valuation.forward_psr. This run keeps the established "
                "actual FY PSR evidence axis and prefers daily_valuation.psr "
                "when available."
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
    missing = [table for table in _REQUIRED_TABLES if not table_exists(conn, table)]
    if missing:
        raise ValueError(f"market.duckdb is missing required tables: {', '.join(missing)}")


def _create_psr_valuation_panel(  # pyright: ignore[reportUnusedFunction]
    conn: Any,
) -> None:
    """Compatibility bridge for remaining Task 8 and Task 10 consumers."""

    publish_legacy_psr_features(conn)


PUBLIC_FEATURE_BUILDER = build_psr_features


def _create_evaluated_psr_valuation_panel(conn: Any, *, source_name: str) -> None:
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranking_psr_valuation_panel AS
        SELECT
            source.*,
            source.forecast_per AS forward_per,
            source.forecast_per_percentile AS forward_per_percentile,
            source.forecast_p_op AS forward_p_op,
            source.forecast_p_op_percentile AS forward_p_op_percentile
        FROM {source_name} source
        """
    )


def _create_deep_dive_panel(conn: Any) -> None:
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE ranking_psr_valuation_deep_panel AS
        SELECT
            p.*,
            coalesce(p.atr20_acceleration, FALSE) AS atr20_acceleration_flag,
            coalesce(
                p.atr20_acceleration
                AND coalesce(p.recent_return_20d_pct, 0.0) < 30.0,
                FALSE
            ) AS atr20_acceleration_ex_overheat_flag,
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
            VALUES {condition_values_sql(_PSR_BUCKETS)}
        ) AS psr_bucket(psr_bucket, psr_bucket_order, condition_matches)
    """
    for horizon in horizons:
        frames.append(
            aggregate_lateral_conditions(
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
    return concat_sorted(frames, columns=_psr_bucket_columns())


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
            VALUES {condition_values_sql(_DECISION_SCOPES)}
        ) AS decision_scope(decision_scope, decision_scope_order, decision_matches)
        CROSS JOIN LATERAL (
            VALUES {condition_values_sql(_PSR_CONDITIONS)}
        ) AS psr_condition(psr_condition, psr_condition_order, psr_matches)
    """
    for horizon in horizons:
        frames.append(
            aggregate_lateral_conditions(
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
    return concat_sorted(frames, columns=_decision_scope_psr_columns())


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
            VALUES {condition_values_sql(conditions)}
        ) AS deep_scope(deep_scope, deep_scope_order, deep_scope_matches)
    """
    for horizon in horizons:
        frames.append(
            aggregate_lateral_conditions(
                conn,
                source_name="ranking_psr_valuation_deep_panel",
                lateral_sql=deep_lateral_sql,
                match_condition="deep_scope.deep_scope_matches",
                group_select_sql=(
                    f"{sql_literal(condition_family)} AS condition_family,\n"
                    "            deep_scope.deep_scope,\n"
                    "            deep_scope.deep_scope_order,\n"
                    f"            {int(horizon)} AS horizon"
                ),
                group_by_sql="deep_scope.deep_scope, deep_scope.deep_scope_order, market_scope",
                return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
                extra_metric_sql=deep_dive_metric_sql() + _psr_metric_sql(),
            )
        )
    return concat_sorted(frames, columns=_deep_dive_psr_columns())


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


def _parse_optional_date(value: str | None) -> date | None:
    return None if value is None else date.fromisoformat(value)


def _psr_bucket_columns() -> list[str]:
    return [
        "condition_family",
        "psr_bucket",
        "psr_bucket_order",
        "horizon",
        "market_scope",
        *aggregate_metric_columns(),
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
        *aggregate_metric_columns(),
        *_psr_metric_columns(),
    ]


def _deep_dive_psr_columns() -> list[str]:
    return [
        "condition_family",
        "deep_scope",
        "deep_scope_order",
        "horizon",
        "market_scope",
        *aggregate_metric_columns(),
        *deep_dive_metric_columns(),
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
