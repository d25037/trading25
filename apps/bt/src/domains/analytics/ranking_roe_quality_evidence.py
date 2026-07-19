"""ROE and FwdROE quality evidence for Daily Ranking."""

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
    RoeFeaturesRequest,
    SectorStrengthFeaturesRequest,
    ShortScaffoldFeaturesRequest,
    build_atr_features,
    build_long_leadership_features,
    build_roe_features,
    build_sector_strength_features,
    build_short_scaffold_features,
    publish_legacy_roe_features,
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

RANKING_ROE_QUALITY_EXPERIMENT_ID = "market-behavior/ranking-roe-quality-evidence"
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
    "statement_metrics_adjusted",
    "indices_data",
    "index_master",
)
_LEADERSHIP_WINDOWS: tuple[int, ...] = (120, 252, 504)
_ROE_BUCKETS: tuple[tuple[str, str], ...] = (
    ("missing_roe", "roe IS NULL"),
    ("low_roe_20pct", "roe_percentile <= 0.2"),
    ("middle_roe_60pct", "roe_percentile > 0.2 AND roe_percentile < 0.8"),
    ("high_roe_20pct", "roe_percentile >= 0.8"),
    ("high_roe_10pct", "roe_percentile >= 0.9"),
)
_FORWARD_ROE_BUCKETS: tuple[tuple[str, str], ...] = (
    ("missing_forward_roe", "forward_roe IS NULL"),
    ("low_forward_roe_20pct", "forward_roe_percentile <= 0.2"),
    (
        "middle_forward_roe_60pct",
        "forward_roe_percentile > 0.2 AND forward_roe_percentile < 0.8",
    ),
    ("high_forward_roe_20pct", "forward_roe_percentile >= 0.8"),
    ("high_forward_roe_10pct", "forward_roe_percentile >= 0.9"),
)
_QUALITY_CONDITIONS: tuple[tuple[str, str], ...] = (
    ("all", "TRUE"),
    ("roe_high", "roe_percentile >= 0.8"),
    ("forward_roe_high", "forward_roe_percentile >= 0.8"),
    (
        "roe_and_forward_roe_high",
        "roe_percentile >= 0.8 AND forward_roe_percentile >= 0.8",
    ),
    (
        "roe_or_forward_roe_low",
        "roe_percentile <= 0.2 OR forward_roe_percentile <= 0.2",
    ),
)
_DECISION_SCOPES: tuple[tuple[str, str], ...] = (
    ("all_market", "TRUE"),
    ("deep_value", "valuation_signal = 'strong_value_confirmation'"),
    ("overvalued", "overvalued_warning"),
    (
        "deep_value_or_high_quality",
        "valuation_signal = 'strong_value_confirmation' "
        "OR roe_percentile >= 0.8 "
        "OR forward_roe_percentile >= 0.8",
    ),
    (
        "overvalued_low_quality",
        "overvalued_warning "
        "AND (roe_percentile <= 0.2 OR forward_roe_percentile <= 0.2)",
    ),
)
_LONG_DEEP_SCOPES: tuple[tuple[str, str], ...] = (
    ("all_market", "TRUE"),
    ("deep_value", "valuation_signal = 'strong_value_confirmation'"),
    ("high_roe", "roe_percentile >= 0.8"),
    ("high_forward_roe", "forward_roe_percentile >= 0.8"),
    (
        "high_roe_and_forward_roe",
        "roe_percentile >= 0.8 AND forward_roe_percentile >= 0.8",
    ),
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
        "high_roe_long_hybrid_atr20_accel",
        "roe_percentile >= 0.8 "
        "AND long_hybrid_leadership_score >= 0.799999 "
        "AND atr20_acceleration_ex_overheat_flag",
    ),
    (
        "high_forward_roe_long_hybrid_atr20_accel",
        "forward_roe_percentile >= 0.8 "
        "AND long_hybrid_leadership_score >= 0.799999 "
        "AND atr20_acceleration_ex_overheat_flag",
    ),
    (
        "deep_value_high_forward_roe_long_hybrid_atr20_accel",
        "valuation_signal = 'strong_value_confirmation' "
        "AND forward_roe_percentile >= 0.8 "
        "AND long_hybrid_leadership_score >= 0.799999 "
        "AND atr20_acceleration_ex_overheat_flag",
    ),
    (
        "low_roe_long_hybrid_atr20_accel",
        "roe_percentile <= 0.2 "
        "AND long_hybrid_leadership_score >= 0.799999 "
        "AND atr20_acceleration_ex_overheat_flag",
    ),
)
_SHORT_DEEP_SCOPES: tuple[tuple[str, str], ...] = (
    ("all_market", "TRUE"),
    ("sector_weak", "sector_strength_bucket = 'sector_weak'"),
    ("overvalued", "overvalued_warning"),
    (
        "overvalued_sector_weak",
        "overvalued_warning AND sector_strength_bucket = 'sector_weak'",
    ),
    ("low_roe", "roe_percentile <= 0.2"),
    ("low_forward_roe", "forward_roe_percentile <= 0.2"),
    (
        "low_quality",
        "roe_percentile <= 0.2 OR forward_roe_percentile <= 0.2",
    ),
    (
        "low_roe_sector_weak",
        "roe_percentile <= 0.2 AND sector_strength_bucket = 'sector_weak'",
    ),
    (
        "low_forward_roe_sector_weak",
        "forward_roe_percentile <= 0.2 AND sector_strength_bucket = 'sector_weak'",
    ),
    (
        "low_quality_sector_weak",
        "(roe_percentile <= 0.2 OR forward_roe_percentile <= 0.2) "
        "AND sector_strength_bucket = 'sector_weak'",
    ),
    (
        "overvalued_low_quality_sector_weak",
        "overvalued_warning "
        "AND (roe_percentile <= 0.2 OR forward_roe_percentile <= 0.2) "
        "AND sector_strength_bucket = 'sector_weak'",
    ),
    (
        "crowded_low_quality_sector_weak",
        "liquidity_regime = 'crowded_rerating' "
        "AND (roe_percentile <= 0.2 OR forward_roe_percentile <= 0.2) "
        "AND sector_strength_bucket = 'sector_weak'",
    ),
)


@dataclass(frozen=True)
class RankingRoeQualityEvidenceResult:
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
    roe_definition: str
    observation_sample_df: pd.DataFrame
    coverage_diagnostics_df: pd.DataFrame
    roe_bucket_evidence_df: pd.DataFrame
    forward_roe_bucket_evidence_df: pd.DataFrame
    decision_scope_quality_evidence_df: pd.DataFrame
    long_deep_dive_quality_evidence_df: pd.DataFrame
    short_deep_dive_quality_evidence_df: pd.DataFrame


def run_ranking_roe_quality_evidence_research(
    db_path: str | Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    market_scopes: Sequence[str] = DEFAULT_MARKET_SCOPES,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
    severe_loss_threshold_pct: float = DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    observation_sample_limit: int = DEFAULT_OBSERVATION_SAMPLE_LIMIT,
) -> RankingRoeQualityEvidenceResult:
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
        snapshot_prefix="ranking-roe-quality-",
    ) as ctx:
        _assert_required_tables(ctx.connection)
        market_source = "stock_master_daily_exact_date"
        relations = build_daily_ranking_research_base(
            ctx.connection,
            DailyRankingPanelRequest(
                namespace="roe_quality",
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
            raise RuntimeError("ROE research requires liquidity-ranked signals")
        roe_features = build_roe_features(
            ctx.connection,
            RoeFeaturesRequest(source=signal_source, namespace="roe_quality_roe"),
        )
        atr_features = build_atr_features(
            ctx.connection,
            AtrFeaturesRequest(source=signal_source, namespace="roe_quality_atr"),
        )
        short_features = build_short_scaffold_features(
            ctx.connection,
            ShortScaffoldFeaturesRequest(
                source=signal_source,
                atr_features=atr_features,
                namespace="roe_quality_short",
            ),
        )
        sector_features = build_sector_strength_features(
            ctx.connection,
            SectorStrengthFeaturesRequest(
                source=signal_source,
                population_source=signal_source,
                namespace="roe_quality_sector",
            ),
        )
        leadership_features = build_long_leadership_features(
            ctx.connection,
            LongLeadershipFeaturesRequest(
                source=signal_source,
                sector_features=sector_features,
                namespace="roe_quality_leadership",
                leadership_windows=_LEADERSHIP_WINDOWS,
            ),
        )
        composed = compose_daily_ranking_signal_features(
            ctx.connection,
            source=signal_source,
            features=(roe_features, leadership_features, short_features),
            namespace="roe_quality",
        )
        cohort = materialize_daily_ranking_signal_cohort(
            ctx.connection,
            relations,
            source=composed,
            name="roe_quality_signals",
        )
        evaluated = attach_daily_ranking_outcomes(
            ctx.connection,
            cohort,
            relations,
            name="roe_quality_outcomes",
        )
        _create_evaluated_roe_quality_panel(
            ctx.connection,
            source_name=evaluated.name,
        )
        _create_deep_dive_panel(ctx.connection)
        observation_count = int(
            ctx.connection.execute(
                "SELECT count(*) FROM ranking_roe_quality_panel"
            ).fetchone()[0]
        )
        result = RankingRoeQualityEvidenceResult(
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
            roe_definition=(
                "ROE = adjusted_eps / adjusted_bps * 100; "
                "FwdROE = adjusted_forecast_eps / adjusted_bps * 100, "
                "using latest statement_metrics_adjusted row as-of Ranking date."
            ),
            observation_sample_df=_query_observation_sample_df(
                ctx.connection,
                limit=observation_sample_limit,
            ),
            coverage_diagnostics_df=_build_coverage_diagnostics_df(ctx.connection),
            roe_bucket_evidence_df=_build_roe_bucket_evidence_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            forward_roe_bucket_evidence_df=_build_forward_roe_bucket_evidence_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            decision_scope_quality_evidence_df=(
                _build_decision_scope_quality_evidence_df(
                    ctx.connection,
                    horizons=resolved_horizons,
                    min_observations=min_observations,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
            ),
            long_deep_dive_quality_evidence_df=(
                _build_deep_dive_quality_evidence_df(
                    ctx.connection,
                    condition_family="long_hybrid_sector_atr_roe_quality",
                    conditions=_LONG_DEEP_SCOPES,
                    horizons=resolved_horizons,
                    min_observations=min_observations,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
            ),
            short_deep_dive_quality_evidence_df=(
                _build_deep_dive_quality_evidence_df(
                    ctx.connection,
                    condition_family="short_sector_crowded_roe_quality",
                    conditions=_SHORT_DEEP_SCOPES,
                    horizons=resolved_horizons,
                    min_observations=min_observations,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
            ),
        )
    return result


def write_ranking_roe_quality_evidence_bundle(
    result: RankingRoeQualityEvidenceResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=RANKING_ROE_QUALITY_EXPERIMENT_ID,
        module="src.domains.analytics.ranking_roe_quality_evidence",
        function="run_ranking_roe_quality_evidence_research",
        params={
            "horizons": list(result.horizons),
            "market_scopes": list(result.market_scopes),
            "min_observations": result.min_observations,
            "severe_loss_threshold_pct": result.severe_loss_threshold_pct,
            "required_tables": list(result.required_tables),
            "roe_definition": result.roe_definition,
        },
        db_path=result.db_path,
        analysis_start_date=result.analysis_start_date,
        analysis_end_date=result.analysis_end_date,
        result_metadata={
            "source_mode": str(result.source_mode),
            "source_detail": result.source_detail,
            "market_source": result.market_source,
            "observation_count": result.observation_count,
            "roe_definition": result.roe_definition,
        },
        result_tables={
            "observation_sample_df": result.observation_sample_df,
            "coverage_diagnostics_df": result.coverage_diagnostics_df,
            "roe_bucket_evidence_df": result.roe_bucket_evidence_df,
            "forward_roe_bucket_evidence_df": result.forward_roe_bucket_evidence_df,
            "decision_scope_quality_evidence_df": (
                result.decision_scope_quality_evidence_df
            ),
            "long_deep_dive_quality_evidence_df": (
                result.long_deep_dive_quality_evidence_df
            ),
            "short_deep_dive_quality_evidence_df": (
                result.short_deep_dive_quality_evidence_df
            ),
        },
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def build_summary_markdown(result: RankingRoeQualityEvidenceResult) -> str:
    parts = [
        "# Ranking ROE Quality Evidence",
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
        f"- roe_definition: {result.roe_definition}",
        "",
        "## Coverage Diagnostics",
        "",
        _top_rows_for_markdown(result.coverage_diagnostics_df, limit=30),
        "",
        "## ROE Bucket Evidence",
        "",
        _top_rows_for_markdown(result.roe_bucket_evidence_df, limit=120),
        "",
        "## FwdROE Bucket Evidence",
        "",
        _top_rows_for_markdown(result.forward_roe_bucket_evidence_df, limit=120),
        "",
        "## Daily Ranking Decision Scope x ROE/FwdROE Quality Evidence",
        "",
        _top_rows_for_markdown(result.decision_scope_quality_evidence_df, limit=180),
        "",
        "## Explicit Long Deep Dive: Long Hybrid Leadership x ATR x ROE/FwdROE",
        "",
        _top_rows_for_markdown(result.long_deep_dive_quality_evidence_df, limit=220),
        "",
        "## Explicit Short Deep Dive: Balanced Sector Strength x Crowded/ROE",
        "",
        _top_rows_for_markdown(result.short_deep_dive_quality_evidence_df, limit=220),
    ]
    return "\n".join(parts).rstrip() + "\n"


def _assert_required_tables(conn: Any) -> None:
    missing = [table for table in _REQUIRED_TABLES if not table_exists(conn, table)]
    if missing:
        raise ValueError(f"market.duckdb is missing required tables: {', '.join(missing)}")


def _create_roe_quality_panel(  # pyright: ignore[reportUnusedFunction]
    conn: Any,
) -> None:
    """Compatibility bridge for direct legacy callers."""

    publish_legacy_roe_features(conn)


PUBLIC_FEATURE_BUILDER = build_roe_features


def _create_evaluated_roe_quality_panel(conn: Any, *, source_name: str) -> None:
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranking_roe_quality_panel AS
        SELECT
            source.*,
            source.forecast_per AS forward_per,
            source.forecast_per_percentile AS forward_per_percentile,
            source.forecast_roe AS forward_roe,
            source.forecast_roe_percentile AS forward_roe_percentile,
            source.forecast_roe_signal AS forward_roe_signal
        FROM {source_name} source
        """
    )


def _create_deep_dive_panel(conn: Any) -> None:
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE ranking_roe_quality_deep_panel AS
        SELECT
            q.*,
            coalesce(q.atr20_acceleration, FALSE) AS atr20_acceleration_flag,
            coalesce(
                q.atr20_acceleration
                AND coalesce(q.recent_return_20d_pct, 0.0) < 30.0,
                FALSE
            ) AS atr20_acceleration_ex_overheat_flag,
            (
                q.liquidity_regime = 'crowded_rerating'
                AND q.no_value_confirmation
            ) AS crowded_no_value_flag,
            (
                q.liquidity_regime = 'crowded_rerating'
                AND (
                    q.overvalued_warning
                    OR q.no_positive_earnings_valuation
                )
            ) AS crowded_overvalued_flag
        FROM ranking_roe_quality_panel q
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
            avg(CASE WHEN adjusted_eps IS NOT NULL THEN 1.0 ELSE 0.0 END)
                * 100.0 AS adjusted_eps_coverage_pct,
            avg(CASE WHEN adjusted_bps > 0 THEN 1.0 ELSE 0.0 END)
                * 100.0 AS adjusted_bps_coverage_pct,
            avg(CASE WHEN adjusted_forecast_eps IS NOT NULL THEN 1.0 ELSE 0.0 END)
                * 100.0 AS adjusted_forecast_eps_coverage_pct,
            avg(CASE WHEN roe IS NOT NULL THEN 1.0 ELSE 0.0 END)
                * 100.0 AS roe_coverage_pct,
            avg(CASE WHEN forward_roe IS NOT NULL THEN 1.0 ELSE 0.0 END)
                * 100.0 AS forward_roe_coverage_pct,
            median(roe) AS median_roe,
            median(forward_roe) AS median_forward_roe,
            median(roe_percentile) AS median_roe_percentile,
            median(forward_roe_percentile) AS median_forward_roe_percentile,
            avg(CASE WHEN roe_percentile >= 0.8 THEN 1.0 ELSE 0.0 END)
                * 100.0 AS high_roe_rate_pct,
            avg(CASE WHEN forward_roe_percentile >= 0.8 THEN 1.0 ELSE 0.0 END)
                * 100.0 AS high_forward_roe_rate_pct
        FROM ranking_roe_quality_panel
        GROUP BY market_scope
        ORDER BY market_scope
        """
    ).fetchdf()


def _build_roe_bucket_evidence_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    roe_lateral_sql = f"""
        CROSS JOIN LATERAL (
            VALUES {condition_values_sql(_ROE_BUCKETS)}
        ) AS roe_bucket(roe_bucket, roe_bucket_order, condition_matches)
    """
    for horizon in horizons:
        frames.append(
            aggregate_lateral_conditions(
                conn,
                source_name="ranking_roe_quality_panel",
                lateral_sql=roe_lateral_sql,
                match_condition="roe_bucket.condition_matches",
                group_select_sql=(
                    "'roe_bucket' AS condition_family,\n"
                    "            roe_bucket.roe_bucket,\n"
                    "            roe_bucket.roe_bucket_order,\n"
                    f"            {int(horizon)} AS horizon"
                ),
                group_by_sql="roe_bucket.roe_bucket, roe_bucket.roe_bucket_order, market_scope",
                return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
                extra_metric_sql=_quality_metric_sql(),
            )
        )
    return concat_sorted(frames, columns=_roe_bucket_columns())


def _build_forward_roe_bucket_evidence_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    forward_roe_lateral_sql = f"""
        CROSS JOIN LATERAL (
            VALUES {condition_values_sql(_FORWARD_ROE_BUCKETS)}
        ) AS forward_roe_bucket(
            forward_roe_bucket,
            forward_roe_bucket_order,
            condition_matches
        )
    """
    for horizon in horizons:
        frames.append(
            aggregate_lateral_conditions(
                conn,
                source_name="ranking_roe_quality_panel",
                lateral_sql=forward_roe_lateral_sql,
                match_condition="forward_roe_bucket.condition_matches",
                group_select_sql=(
                    "'forward_roe_bucket' AS condition_family,\n"
                    "            forward_roe_bucket.forward_roe_bucket,\n"
                    "            forward_roe_bucket.forward_roe_bucket_order,\n"
                    f"            {int(horizon)} AS horizon"
                ),
                group_by_sql=(
                    "forward_roe_bucket.forward_roe_bucket, "
                    "forward_roe_bucket.forward_roe_bucket_order, "
                    "market_scope"
                ),
                return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
                extra_metric_sql=_quality_metric_sql(),
            )
        )
    return concat_sorted(frames, columns=_forward_roe_bucket_columns())


def _build_decision_scope_quality_evidence_df(
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
            VALUES {condition_values_sql(_QUALITY_CONDITIONS)}
        ) AS quality_condition(
            quality_condition,
            quality_condition_order,
            quality_matches
        )
    """
    for horizon in horizons:
        frames.append(
            aggregate_lateral_conditions(
                conn,
                source_name="ranking_roe_quality_panel",
                lateral_sql=decision_lateral_sql,
                match_condition=(
                    "decision_scope.decision_matches "
                    "AND quality_condition.quality_matches"
                ),
                group_select_sql=(
                    "'decision_scope_roe_quality' AS condition_family,\n"
                    "            decision_scope.decision_scope,\n"
                    "            decision_scope.decision_scope_order,\n"
                    "            quality_condition.quality_condition,\n"
                    "            quality_condition.quality_condition_order,\n"
                    f"            {int(horizon)} AS horizon"
                ),
                group_by_sql=(
                    "decision_scope.decision_scope, "
                    "decision_scope.decision_scope_order, "
                    "quality_condition.quality_condition, "
                    "quality_condition.quality_condition_order, "
                    "market_scope"
                ),
                return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
                extra_metric_sql=_quality_metric_sql(),
            )
        )
    return concat_sorted(frames, columns=_decision_scope_quality_columns())


def _build_deep_dive_quality_evidence_df(
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
                source_name="ranking_roe_quality_deep_panel",
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
                extra_metric_sql=deep_dive_metric_sql() + _quality_metric_sql(),
            )
        )
    return concat_sorted(frames, columns=_deep_dive_quality_columns())


def _quality_metric_sql() -> str:
    return """,
            median(roe) AS median_roe,
            median(forward_roe) AS median_forward_roe,
            median(roe_percentile) AS median_roe_percentile,
            median(forward_roe_percentile) AS median_forward_roe_percentile,
            avg(CASE WHEN roe_percentile >= 0.8 THEN 1.0 ELSE 0.0 END)
                * 100.0 AS high_roe_rate_pct,
            avg(CASE WHEN forward_roe_percentile >= 0.8 THEN 1.0 ELSE 0.0 END)
                * 100.0 AS high_forward_roe_rate_pct,
            avg(CASE WHEN roe_percentile <= 0.2 THEN 1.0 ELSE 0.0 END)
                * 100.0 AS low_roe_rate_pct,
            avg(CASE WHEN forward_roe_percentile <= 0.2 THEN 1.0 ELSE 0.0 END)
                * 100.0 AS low_forward_roe_rate_pct"""


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
            quality_disclosed_date,
            quality_period_end,
            adjusted_eps,
            adjusted_bps,
            adjusted_forecast_eps,
            roe,
            roe_percentile,
            roe_signal,
            forward_roe,
            forward_roe_percentile,
            forward_roe_signal,
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
        FROM ranking_roe_quality_panel
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


def _roe_bucket_columns() -> list[str]:
    return [
        "condition_family",
        "roe_bucket",
        "roe_bucket_order",
        "horizon",
        "market_scope",
        *aggregate_metric_columns(),
        *_quality_metric_columns(),
    ]


def _forward_roe_bucket_columns() -> list[str]:
    return [
        "condition_family",
        "forward_roe_bucket",
        "forward_roe_bucket_order",
        "horizon",
        "market_scope",
        *aggregate_metric_columns(),
        *_quality_metric_columns(),
    ]


def _decision_scope_quality_columns() -> list[str]:
    return [
        "condition_family",
        "decision_scope",
        "decision_scope_order",
        "quality_condition",
        "quality_condition_order",
        "horizon",
        "market_scope",
        *aggregate_metric_columns(),
        *_quality_metric_columns(),
    ]


def _deep_dive_quality_columns() -> list[str]:
    return [
        "condition_family",
        "deep_scope",
        "deep_scope_order",
        "horizon",
        "market_scope",
        *aggregate_metric_columns(),
        *deep_dive_metric_columns(),
        *_quality_metric_columns(),
    ]


def _quality_metric_columns() -> list[str]:
    return [
        "median_roe",
        "median_forward_roe",
        "median_roe_percentile",
        "median_forward_roe_percentile",
        "high_roe_rate_pct",
        "high_forward_roe_rate_pct",
        "low_roe_rate_pct",
        "low_forward_roe_rate_pct",
    ]
