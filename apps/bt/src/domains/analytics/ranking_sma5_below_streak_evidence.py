"""SMA5 below-streak evidence for Daily Ranking long-side timing diagnostics."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Sequence, cast

import pandas as pd

from src.domains.analytics.daily_ranking_consumer_support import (
    aggregate_lateral_conditions as _aggregate_lateral_conditions,
    aggregate_metric_columns as _aggregate_metric_columns,
    compose_daily_ranking_signal_features,
    condition_values_sql as _condition_values_sql,
    deep_dive_metric_columns as _deep_dive_metric_columns,
    deep_dive_metric_sql as _deep_dive_metric_sql,
    table_exists,
)
from src.domains.analytics.daily_ranking_feature_builders import (
    AtrFeaturesRequest,
    LongLeadershipFeaturesRequest,
    LongScaffoldFeaturesRequest,
    SectorStrengthFeaturesRequest,
    ShortScaffoldFeaturesRequest,
    SmaFeaturesRequest,
    build_atr_features,
    build_long_leadership_features,
    build_long_scaffold_features,
    build_sector_strength_features,
    build_short_scaffold_features,
    build_sma_features,
)
from src.domains.analytics.daily_ranking_research_base import (
    DailyRankingPanelRequest,
    MarketScope,
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
RANKING_SMA5_BELOW_STREAK_EVIDENCE_EXPERIMENT_ID = (
    "market-behavior/ranking-sma5-below-streak-evidence"
)
DEFAULT_HORIZONS: tuple[int, ...] = (5, 20, 60)
DEFAULT_MARKET_SCOPES: tuple[str, ...] = ("prime",)
DEFAULT_MIN_OBSERVATIONS = 300
DEFAULT_SEVERE_LOSS_THRESHOLD_PCT = -10.0
DEFAULT_OBSERVATION_SAMPLE_LIMIT = 10_000
_LEADERSHIP_WINDOWS: tuple[int, ...] = (120, 252, 504)
_REQUIRED_TABLES: tuple[str, ...] = (
    "stock_data_raw",
    "stock_data",
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
_SMA5_BELOW_STREAK_BUCKETS: tuple[tuple[str, str], ...] = (
    ("below_sma5_streak_other", "NOT below_sma5_streak_ge3_flag"),
    ("below_sma5_streak_ge3", "below_sma5_streak_ge3_flag"),
)
_SMA5_COUNT_GROUP_BUCKETS: tuple[tuple[str, str], ...] = (
    ("sma5_above_count_0_1", "sma5_above_count_5d IN (0, 1)"),
    ("sma5_above_count_2_3", "sma5_above_count_5d IN (2, 3)"),
    ("sma5_above_count_4_5", "sma5_above_count_5d IN (4, 5)"),
)
_LONG_SCAFFOLDS: tuple[tuple[str, str], ...] = (
    ("all_market", "TRUE"),
    ("deep_value", "valuation_signal = 'strong_value_confirmation'"),
    (
        "deep_value_long_hybrid_atr20_accel",
        "valuation_signal = 'strong_value_confirmation' "
        "AND long_hybrid_leadership_score >= 0.799999 "
        "AND atr20_acceleration_ex_overheat_flag",
    ),
    (
        "neutral_deep_value",
        "liquidity_regime = 'neutral_rerating' "
        "AND valuation_signal = 'strong_value_confirmation'",
    ),
    (
        "neutral_long_hybrid_atr20_accel",
        "liquidity_regime = 'neutral_rerating' "
        "AND long_hybrid_leadership_score >= 0.799999 "
        "AND atr20_acceleration_ex_overheat_flag",
    ),
    (
        "neutral_deep_value_long_hybrid_atr20_accel",
        "liquidity_regime = 'neutral_rerating' "
        "AND valuation_signal = 'strong_value_confirmation' "
        "AND long_hybrid_leadership_score >= 0.799999 "
        "AND atr20_acceleration_ex_overheat_flag",
    ),
    (
        "neutral_deep_value_sector_strong_atr20_accel",
        "liquidity_regime = 'neutral_rerating' "
        "AND valuation_signal = 'strong_value_confirmation' "
        "AND sector_strength_bucket = 'sector_strong' "
        "AND atr20_acceleration_ex_overheat_flag",
    ),
    (
        "crowded_long_hybrid",
        "liquidity_regime = 'crowded_rerating' "
        "AND long_hybrid_leadership_score >= 0.799999",
    ),
    (
        "crowded_low10_pbr",
        "liquidity_regime = 'crowded_rerating' "
        "AND long_hybrid_leadership_score >= 0.799999 "
        "AND pbr_percentile <= 0.1",
    ),
    (
        "crowded_low10_pbr_forward_per",
        "liquidity_regime = 'crowded_rerating' "
        "AND long_hybrid_leadership_score >= 0.799999 "
        "AND pbr_percentile <= 0.1 "
        "AND forecast_per_percentile <= 0.1",
    ),
    (
        "crowded_low10_pbr_forward_per_atr20_accel",
        "liquidity_regime = 'crowded_rerating' "
        "AND long_hybrid_leadership_score >= 0.799999 "
        "AND pbr_percentile <= 0.1 "
        "AND forecast_per_percentile <= 0.1 "
        "AND atr20_acceleration_ex_overheat_flag",
    ),
)


@dataclass(frozen=True)
class RankingSma5BelowStreakEvidenceResult:
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
    observation_sample_df: pd.DataFrame
    coverage_diagnostics_df: pd.DataFrame
    sma5_below_streak_evidence_df: pd.DataFrame
    long_scaffold_sma5_below_streak_evidence_df: pd.DataFrame
    long_scaffold_sma5_below_streak_count_cross_df: pd.DataFrame
    same_day_sma5_below_streak_spread_df: pd.DataFrame
    long_scaffold_same_day_sma5_below_streak_spread_df: pd.DataFrame
    long_scaffold_same_day_sma5_below_streak_count_cross_spread_df: pd.DataFrame


def run_ranking_sma5_below_streak_evidence_research(
    db_path: str | Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    market_scopes: Sequence[str] = DEFAULT_MARKET_SCOPES,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
    severe_loss_threshold_pct: float = DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    observation_sample_limit: int = DEFAULT_OBSERVATION_SAMPLE_LIMIT,
) -> RankingSma5BelowStreakEvidenceResult:
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

    market_source = "stock_master_daily_exact_date"

    with open_readonly_analysis_connection(
        str(db_path_obj),
        snapshot_prefix="ranking-sma5-below-streak-evidence-",
    ) as ctx:
        _assert_required_tables(ctx.connection)
        relations = build_daily_ranking_research_base(
            ctx.connection,
            DailyRankingPanelRequest(
                namespace="sma5_below_streak",
                analysis_start_date=_parse_optional_date(start_date),
                analysis_end_date=_parse_optional_date(end_date),
                horizons=resolved_horizons,
                market_scopes=cast(tuple[MarketScope, ...], resolved_market_scopes),
                include_liquidity=True,
                percentile_features=(),
            ),
        )
        signal_source = relations.ranked_signals
        atr_features = build_atr_features(
            ctx.connection,
            AtrFeaturesRequest(source=signal_source, namespace="sma5_below_streak_atr"),
        )
        short_features = build_short_scaffold_features(
            ctx.connection,
            ShortScaffoldFeaturesRequest(
                source=signal_source,
                atr_features=atr_features,
                namespace="sma5_below_streak_short",
            ),
        )
        sector_features = build_sector_strength_features(
            ctx.connection,
            SectorStrengthFeaturesRequest(
                source=signal_source,
                population_source=signal_source,
                namespace="sma5_below_streak_sector",
            ),
        )
        leadership_features = build_long_leadership_features(
            ctx.connection,
            LongLeadershipFeaturesRequest(
                source=signal_source,
                sector_features=sector_features,
                namespace="sma5_below_streak_leadership",
                leadership_windows=_LEADERSHIP_WINDOWS,
            ),
        )
        long_features = build_long_scaffold_features(
            ctx.connection,
            LongScaffoldFeaturesRequest(
                source=signal_source,
                leadership_features=leadership_features,
                short_scaffold_features=short_features,
                namespace="sma5_below_streak_scaffold",
            ),
        )
        sma_features = build_sma_features(
            ctx.connection,
            SmaFeaturesRequest(
                source=signal_source,
                price_history=relations.price_history,
                namespace="sma5_below_streak_sma",
            ),
        )
        composed = compose_daily_ranking_signal_features(
            ctx.connection,
            source=signal_source,
            features=(long_features, sma_features),
            namespace="sma5_below_streak",
        )
        cohort = materialize_daily_ranking_signal_cohort(
            ctx.connection,
            relations,
            source=composed,
            name="sma5_below_streak_signals",
            predicate=SignalExpression(
                sql="close_below_sma5_count_3d IS NOT NULL",
                referenced_columns=("close_below_sma5_count_3d",),
            ),
        )
        evaluated = attach_daily_ranking_outcomes(
            ctx.connection,
            cohort,
            relations,
            name="sma5_below_streak_outcomes",
        )
        _create_sma5_below_streak_panel(ctx.connection, source_name=evaluated.name)
        observation_count = int(
            ctx.connection.execute(
                "SELECT count(*) FROM ranking_sma5_below_streak_panel"
            ).fetchone()[0]
        )
        result = RankingSma5BelowStreakEvidenceResult(
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
            observation_sample_df=_query_observation_sample_df(
                ctx.connection,
                horizons=resolved_horizons,
                limit=observation_sample_limit,
            ),
            coverage_diagnostics_df=_build_coverage_diagnostics_df(ctx.connection),
            sma5_below_streak_evidence_df=_build_sma5_below_streak_evidence_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            long_scaffold_sma5_below_streak_evidence_df=(
                _build_long_scaffold_sma5_below_streak_evidence_df(
                    ctx.connection,
                    horizons=resolved_horizons,
                    min_observations=min_observations,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
            ),
            long_scaffold_sma5_below_streak_count_cross_df=(
                _build_long_scaffold_sma5_below_streak_count_cross_df(
                    ctx.connection,
                    horizons=resolved_horizons,
                    min_observations=min_observations,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
            ),
            same_day_sma5_below_streak_spread_df=(
                _build_same_day_sma5_below_streak_spread_df(
                    ctx.connection,
                    horizons=resolved_horizons,
                )
            ),
            long_scaffold_same_day_sma5_below_streak_spread_df=(
                _build_long_scaffold_same_day_sma5_below_streak_spread_df(
                    ctx.connection,
                    horizons=resolved_horizons,
                )
            ),
            long_scaffold_same_day_sma5_below_streak_count_cross_spread_df=(
                _build_long_scaffold_same_day_sma5_below_streak_count_cross_spread_df(
                    ctx.connection,
                    horizons=resolved_horizons,
                )
            ),
        )
    return result


def write_ranking_sma5_below_streak_evidence_bundle(
    result: RankingSma5BelowStreakEvidenceResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=RANKING_SMA5_BELOW_STREAK_EVIDENCE_EXPERIMENT_ID,
        module="src.domains.analytics.ranking_sma5_below_streak_evidence",
        function="run_ranking_sma5_below_streak_evidence_research",
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
            "primary_outcome": "forward_close_excess_return_{horizon}d_pct",
            "sma5_parameter": "close_below_sma5 for at least 3 consecutive sessions",
            "same_day_spread": "below_sma5_streak_ge3 - below_sma5_streak_other",
            "same_day_cross_spread": "weak_condition - comparison_condition",
        },
        result_tables={
            "observation_sample_df": result.observation_sample_df,
            "coverage_diagnostics_df": result.coverage_diagnostics_df,
            "sma5_below_streak_evidence_df": (result.sma5_below_streak_evidence_df),
            "long_scaffold_sma5_below_streak_evidence_df": (
                result.long_scaffold_sma5_below_streak_evidence_df
            ),
            "long_scaffold_sma5_below_streak_count_cross_df": (
                result.long_scaffold_sma5_below_streak_count_cross_df
            ),
            "same_day_sma5_below_streak_spread_df": (
                result.same_day_sma5_below_streak_spread_df
            ),
            "long_scaffold_same_day_sma5_below_streak_spread_df": (
                result.long_scaffold_same_day_sma5_below_streak_spread_df
            ),
            "long_scaffold_same_day_sma5_below_streak_count_cross_spread_df": (
                result.long_scaffold_same_day_sma5_below_streak_count_cross_spread_df
            ),
        },
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def build_summary_markdown(result: RankingSma5BelowStreakEvidenceResult) -> str:
    parts = [
        "# Ranking SMA5 Below-Streak Evidence",
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
        _top_rows_for_markdown(result.coverage_diagnostics_df, limit=80),
        "",
        "## SMA5 Below-Streak Evidence",
        "",
        _top_rows_for_markdown(result.sma5_below_streak_evidence_df, limit=80),
        "",
        "## Long Scaffold x SMA5 Below-Streak Evidence",
        "",
        _top_rows_for_markdown(
            result.long_scaffold_sma5_below_streak_evidence_df,
            limit=260,
        ),
        "",
        "## Long Scaffold x SMA5 Below-Streak x SMA5 Count Cross Evidence",
        "",
        _top_rows_for_markdown(
            result.long_scaffold_sma5_below_streak_count_cross_df,
            limit=360,
        ),
        "",
        "## Same-Day SMA5 Below-Streak Spread",
        "",
        _top_rows_for_markdown(
            result.same_day_sma5_below_streak_spread_df,
            limit=80,
        ),
        "",
        "## Long Scaffold Same-Day SMA5 Below-Streak Spread",
        "",
        _top_rows_for_markdown(
            result.long_scaffold_same_day_sma5_below_streak_spread_df,
            limit=260,
        ),
        "",
        "## Long Scaffold Same-Day SMA5 Below-Streak x SMA5 Count Cross Spread",
        "",
        _top_rows_for_markdown(
            result.long_scaffold_same_day_sma5_below_streak_count_cross_spread_df,
            limit=360,
        ),
    ]
    return "\n".join(parts).rstrip() + "\n"


def _assert_required_tables(conn: Any) -> None:
    missing = [table for table in _REQUIRED_TABLES if not table_exists(conn, table)]
    if missing:
        raise ValueError(
            f"market.duckdb is missing required tables: {', '.join(missing)}"
        )


def _create_sma5_below_streak_panel(conn: Any, *, source_name: str) -> None:
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranking_sma5_below_streak_panel AS
        SELECT
            source.*,
            source.forecast_per AS forward_per,
            source.forecast_per_percentile AS forward_per_percentile
        FROM {source_name} source
        """
    )


def _build_coverage_diagnostics_df(conn: Any) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            market_scope,
            sma5_below_streak_bucket,
            count(*) AS observation_count,
            count(DISTINCT code) AS code_count,
            count(DISTINCT date) AS date_count,
            avg(CASE WHEN below_sma5_streak_ge3_flag THEN 1.0 ELSE 0.0 END)
                * 100.0 AS below_sma5_streak_ge3_rate_pct,
            median(sma5_deviation_pct) AS median_sma5_deviation_pct,
            median(close_below_sma5_count_3d) AS median_close_below_sma5_count_3d,
            avg(CASE WHEN valuation_signal = 'strong_value_confirmation'
                THEN 1.0 ELSE 0.0 END) * 100.0 AS deep_value_rate_pct,
            avg(CASE WHEN liquidity_regime = 'neutral_rerating'
                THEN 1.0 ELSE 0.0 END) * 100.0 AS neutral_rerating_rate_pct,
            avg(CASE WHEN liquidity_regime = 'crowded_rerating'
                THEN 1.0 ELSE 0.0 END) * 100.0 AS crowded_rerating_rate_pct,
            avg(CASE WHEN sector_strength_bucket = 'sector_strong'
                THEN 1.0 ELSE 0.0 END) * 100.0 AS sector_strong_rate_pct,
            avg(CASE WHEN long_hybrid_leadership_score >= 0.799999
                THEN 1.0 ELSE 0.0 END) * 100.0 AS long_hybrid_strong_rate_pct,
            avg(CASE WHEN atr20_acceleration_ex_overheat_flag
                THEN 1.0 ELSE 0.0 END) * 100.0
                AS atr20_acceleration_ex_overheat_rate_pct,
            median(recent_return_20d_pct) AS median_recent_return_20d_pct,
            median(recent_return_60d_pct) AS median_recent_return_60d_pct,
            median(liquidity_residual_z) AS median_liquidity_residual_z,
            median(long_hybrid_leadership_score)
                AS median_long_hybrid_leadership_score
        FROM ranking_sma5_below_streak_panel
        GROUP BY market_scope, sma5_below_streak_bucket
        ORDER BY market_scope, sma5_below_streak_bucket
        """
    ).fetchdf()


def _build_sma5_below_streak_evidence_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    lateral_sql = f"""
        CROSS JOIN LATERAL (
            VALUES {_condition_values_sql(_SMA5_BELOW_STREAK_BUCKETS)}
        ) AS sma5_below_streak_bucket(
            sma5_below_streak_bucket,
            sma5_below_streak_bucket_order,
            sma5_below_streak_bucket_matches
        )
    """
    for horizon in horizons:
        frames.append(
            _aggregate_lateral_conditions(
                conn,
                source_name="ranking_sma5_below_streak_panel",
                lateral_sql=lateral_sql,
                match_condition=(
                    "sma5_below_streak_bucket.sma5_below_streak_bucket_matches"
                ),
                group_select_sql=(
                    "'sma5_below_streak' AS condition_family,\n"
                    "            sma5_below_streak_bucket.sma5_below_streak_bucket,\n"
                    "            sma5_below_streak_bucket.sma5_below_streak_bucket_order,\n"
                    f"            {int(horizon)} AS horizon"
                ),
                group_by_sql=(
                    "sma5_below_streak_bucket.sma5_below_streak_bucket, "
                    "sma5_below_streak_bucket.sma5_below_streak_bucket_order, "
                    "market_scope"
                ),
                return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
                extra_metric_sql=_sma5_below_streak_metric_sql(),
            )
        )
    return _concat_sorted(frames, columns=_sma5_below_streak_columns())


def _build_long_scaffold_sma5_below_streak_evidence_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    lateral_sql = f"""
        CROSS JOIN LATERAL (
            VALUES {_condition_values_sql(_LONG_SCAFFOLDS)}
        ) AS long_scaffold(
            long_scaffold,
            long_scaffold_order,
            long_scaffold_matches
        )
        CROSS JOIN LATERAL (
            VALUES {_condition_values_sql(_SMA5_BELOW_STREAK_BUCKETS)}
        ) AS sma5_below_streak_bucket(
            sma5_below_streak_bucket,
            sma5_below_streak_bucket_order,
            sma5_below_streak_bucket_matches
        )
    """
    for horizon in horizons:
        frames.append(
            _aggregate_lateral_conditions(
                conn,
                source_name="ranking_sma5_below_streak_panel",
                lateral_sql=lateral_sql,
                match_condition=(
                    "long_scaffold.long_scaffold_matches "
                    "AND sma5_below_streak_bucket.sma5_below_streak_bucket_matches"
                ),
                group_select_sql=(
                    "'long_scaffold_sma5_below_streak' AS condition_family,\n"
                    "            long_scaffold.long_scaffold,\n"
                    "            long_scaffold.long_scaffold_order,\n"
                    "            sma5_below_streak_bucket.sma5_below_streak_bucket,\n"
                    "            sma5_below_streak_bucket.sma5_below_streak_bucket_order,\n"
                    f"            {int(horizon)} AS horizon"
                ),
                group_by_sql=(
                    "long_scaffold.long_scaffold, "
                    "long_scaffold.long_scaffold_order, "
                    "sma5_below_streak_bucket.sma5_below_streak_bucket, "
                    "sma5_below_streak_bucket.sma5_below_streak_bucket_order, "
                    "market_scope"
                ),
                return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
                extra_metric_sql=_deep_dive_metric_sql()
                + _sma5_below_streak_metric_sql(),
            )
        )
    return _concat_sorted(
        frames,
        columns=_long_scaffold_sma5_below_streak_columns(),
    )


def _build_long_scaffold_sma5_below_streak_count_cross_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    lateral_sql = f"""
        CROSS JOIN LATERAL (
            VALUES {_condition_values_sql(_LONG_SCAFFOLDS)}
        ) AS long_scaffold(
            long_scaffold,
            long_scaffold_order,
            long_scaffold_matches
        )
        CROSS JOIN LATERAL (
            VALUES {_condition_values_sql(_SMA5_BELOW_STREAK_BUCKETS)}
        ) AS sma5_below_streak_bucket(
            sma5_below_streak_bucket,
            sma5_below_streak_bucket_order,
            sma5_below_streak_bucket_matches
        )
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
                source_name="ranking_sma5_below_streak_panel",
                lateral_sql=lateral_sql,
                match_condition=(
                    "long_scaffold.long_scaffold_matches "
                    "AND sma5_below_streak_bucket.sma5_below_streak_bucket_matches "
                    "AND sma5_count_group.sma5_count_group_matches"
                ),
                group_select_sql=(
                    "'long_scaffold_sma5_below_streak_count_cross' "
                    "AS condition_family,\n"
                    "            long_scaffold.long_scaffold,\n"
                    "            long_scaffold.long_scaffold_order,\n"
                    "            sma5_below_streak_bucket.sma5_below_streak_bucket,\n"
                    "            sma5_below_streak_bucket.sma5_below_streak_bucket_order,\n"
                    "            sma5_count_group.sma5_count_group,\n"
                    "            sma5_count_group.sma5_count_group_order,\n"
                    f"            {int(horizon)} AS horizon"
                ),
                group_by_sql=(
                    "long_scaffold.long_scaffold, "
                    "long_scaffold.long_scaffold_order, "
                    "sma5_below_streak_bucket.sma5_below_streak_bucket, "
                    "sma5_below_streak_bucket.sma5_below_streak_bucket_order, "
                    "sma5_count_group.sma5_count_group, "
                    "sma5_count_group.sma5_count_group_order, "
                    "market_scope"
                ),
                return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
                extra_metric_sql=_deep_dive_metric_sql()
                + _sma5_below_streak_metric_sql(),
            )
        )
    return _concat_sorted(
        frames,
        columns=_long_scaffold_sma5_below_streak_count_cross_columns(),
    )


def _build_same_day_sma5_below_streak_spread_df(
    conn: Any,
    *,
    horizons: Sequence[int],
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for horizon in horizons:
        frames.append(
            _query_same_day_spread_df(
                conn,
                return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                horizon=int(horizon),
                condition_family="same_day_sma5_below_streak_spread",
                scaffold_lateral_sql="",
                scaffold_select_sql="'all_market' AS long_scaffold,\n"
                "            0 AS long_scaffold_order,",
                scaffold_group_sql="",
                scaffold_join_sql="",
                match_condition="TRUE",
            )
        )
    return _concat_sorted(frames, columns=_same_day_spread_columns())


def _build_long_scaffold_same_day_sma5_below_streak_spread_df(
    conn: Any,
    *,
    horizons: Sequence[int],
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    scaffold_lateral_sql = f"""
        CROSS JOIN LATERAL (
            VALUES {_condition_values_sql(_LONG_SCAFFOLDS)}
        ) AS long_scaffold(
            long_scaffold,
            long_scaffold_order,
            long_scaffold_matches
        )
    """
    for horizon in horizons:
        frames.append(
            _query_same_day_spread_df(
                conn,
                return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                horizon=int(horizon),
                condition_family="long_scaffold_same_day_sma5_below_streak_spread",
                scaffold_lateral_sql=scaffold_lateral_sql,
                scaffold_select_sql="long_scaffold.long_scaffold,\n"
                "            long_scaffold.long_scaffold_order,",
                scaffold_group_sql=(
                    "long_scaffold.long_scaffold, long_scaffold.long_scaffold_order, "
                ),
                scaffold_join_sql=(
                    "AND comparison.long_scaffold = base.long_scaffold "
                    "AND comparison.long_scaffold_order = base.long_scaffold_order"
                ),
                match_condition="long_scaffold.long_scaffold_matches",
            )
        )
    return _concat_sorted(frames, columns=_same_day_spread_columns())


def _build_long_scaffold_same_day_sma5_below_streak_count_cross_spread_df(
    conn: Any,
    *,
    horizons: Sequence[int],
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    scaffold_lateral_sql = f"""
        CROSS JOIN LATERAL (
            VALUES {_condition_values_sql(_LONG_SCAFFOLDS)}
        ) AS long_scaffold(
            long_scaffold,
            long_scaffold_order,
            long_scaffold_matches
        )
    """
    for horizon in horizons:
        frames.append(
            _query_same_day_count_cross_weak_spread_df(
                conn,
                return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                horizon=int(horizon),
                scaffold_lateral_sql=scaffold_lateral_sql,
                scaffold_select_sql="long_scaffold.long_scaffold,\n"
                "            long_scaffold.long_scaffold_order,",
                scaffold_group_sql=(
                    "long_scaffold.long_scaffold, long_scaffold.long_scaffold_order, "
                ),
                scaffold_join_sql=(
                    "AND comparison.long_scaffold = weak.long_scaffold "
                    "AND comparison.long_scaffold_order = weak.long_scaffold_order"
                ),
                match_condition="long_scaffold.long_scaffold_matches",
            )
        )
    return _concat_sorted(frames, columns=_same_day_count_cross_spread_columns())


def _query_same_day_count_cross_weak_spread_df(
    conn: Any,
    *,
    return_column: str,
    horizon: int,
    scaffold_lateral_sql: str,
    scaffold_select_sql: str,
    scaffold_group_sql: str,
    scaffold_join_sql: str,
    match_condition: str,
) -> pd.DataFrame:
    return conn.execute(
        f"""
        WITH daily_group AS (
            SELECT
                {scaffold_select_sql}
                market_scope,
                date,
                sma5_below_streak_bucket,
                CASE
                    WHEN sma5_below_streak_bucket = 'below_sma5_streak_other'
                        THEN 0
                    WHEN sma5_below_streak_bucket = 'below_sma5_streak_ge3'
                        THEN 1
                END AS sma5_below_streak_bucket_order,
                sma5_count_group,
                CASE
                    WHEN sma5_count_group = 'sma5_above_count_0_1' THEN 0
                    WHEN sma5_count_group = 'sma5_above_count_2_3' THEN 1
                    WHEN sma5_count_group = 'sma5_above_count_4_5' THEN 2
                END AS sma5_count_group_order,
                count(*) AS observation_count,
                median({return_column}) AS median_excess_return_pct,
                avg({return_column}) AS mean_excess_return_pct
            FROM ranking_sma5_below_streak_panel
            {scaffold_lateral_sql}
            WHERE {match_condition}
              AND {return_column} IS NOT NULL
              AND sma5_below_streak_bucket IS NOT NULL
              AND sma5_count_group IS NOT NULL
            GROUP BY
                {scaffold_group_sql}
                market_scope,
                date,
                sma5_below_streak_bucket,
                sma5_below_streak_bucket_order,
                sma5_count_group,
                sma5_count_group_order
        ),
        pair_values AS (
            SELECT
                weak.long_scaffold,
                weak.long_scaffold_order,
                weak.market_scope,
                weak.date,
                weak.sma5_below_streak_bucket
                    AS weak_sma5_below_streak_bucket,
                weak.sma5_count_group AS weak_sma5_count_group,
                comparison.sma5_below_streak_bucket
                    AS comparison_sma5_below_streak_bucket,
                comparison.sma5_count_group AS comparison_sma5_count_group,
                weak.observation_count AS weak_observation_count,
                comparison.observation_count AS comparison_observation_count,
                weak.median_excess_return_pct
                    AS weak_daily_median_excess_return_pct,
                comparison.median_excess_return_pct
                    AS comparison_daily_median_excess_return_pct,
                weak.median_excess_return_pct
                    - comparison.median_excess_return_pct
                    AS daily_median_excess_weak_minus_comparison_pct,
                weak.mean_excess_return_pct
                    - comparison.mean_excess_return_pct
                    AS daily_mean_excess_weak_minus_comparison_pct
            FROM daily_group weak
            JOIN daily_group comparison
              ON comparison.market_scope = weak.market_scope
             AND comparison.date = weak.date
             {scaffold_join_sql}
             AND (
                weak.sma5_below_streak_bucket = 'below_sma5_streak_ge3'
                OR weak.sma5_count_group = 'sma5_above_count_0_1'
             )
             AND NOT (
                comparison.sma5_below_streak_bucket = weak.sma5_below_streak_bucket
                AND comparison.sma5_count_group = weak.sma5_count_group
             )
             AND NOT (
                comparison.sma5_below_streak_bucket = 'below_sma5_streak_ge3'
                OR comparison.sma5_count_group = 'sma5_above_count_0_1'
             )
        )
        SELECT
            'long_scaffold_same_day_sma5_below_streak_count_cross_spread'
                AS condition_family,
            long_scaffold,
            long_scaffold_order,
            weak_sma5_below_streak_bucket,
            weak_sma5_count_group,
            comparison_sma5_below_streak_bucket,
            comparison_sma5_count_group,
            {int(horizon)} AS horizon,
            market_scope,
            count(*) AS matched_date_count,
            sum(weak_observation_count) AS weak_observation_count,
            sum(comparison_observation_count) AS comparison_observation_count,
            avg(weak_observation_count) AS mean_weak_observations_per_date,
            avg(comparison_observation_count)
                AS mean_comparison_observations_per_date,
            median(weak_daily_median_excess_return_pct)
                AS median_weak_daily_median_excess_return_pct,
            median(comparison_daily_median_excess_return_pct)
                AS median_comparison_daily_median_excess_return_pct,
            median(daily_median_excess_weak_minus_comparison_pct)
                AS median_daily_median_excess_weak_minus_comparison_pct,
            avg(daily_median_excess_weak_minus_comparison_pct)
                AS mean_daily_median_excess_weak_minus_comparison_pct,
            quantile_cont(daily_median_excess_weak_minus_comparison_pct, 0.10)
                AS p10_daily_median_excess_weak_minus_comparison_pct,
            quantile_cont(daily_median_excess_weak_minus_comparison_pct, 0.25)
                AS p25_daily_median_excess_weak_minus_comparison_pct,
            quantile_cont(daily_median_excess_weak_minus_comparison_pct, 0.75)
                AS p75_daily_median_excess_weak_minus_comparison_pct,
            quantile_cont(daily_median_excess_weak_minus_comparison_pct, 0.90)
                AS p90_daily_median_excess_weak_minus_comparison_pct,
            avg(
                CASE
                    WHEN daily_median_excess_weak_minus_comparison_pct < 0
                        THEN 1.0
                    ELSE 0.0
                END
            ) * 100.0 AS weak_underperform_date_rate_pct,
            median(daily_mean_excess_weak_minus_comparison_pct)
                AS median_daily_mean_excess_weak_minus_comparison_pct,
            avg(daily_mean_excess_weak_minus_comparison_pct)
                AS mean_daily_mean_excess_weak_minus_comparison_pct
        FROM pair_values
        GROUP BY
            long_scaffold,
            long_scaffold_order,
            weak_sma5_below_streak_bucket,
            weak_sma5_count_group,
            comparison_sma5_below_streak_bucket,
            comparison_sma5_count_group,
            market_scope
        """
    ).fetchdf()


def _query_same_day_spread_df(
    conn: Any,
    *,
    return_column: str,
    horizon: int,
    condition_family: str,
    scaffold_lateral_sql: str,
    scaffold_select_sql: str,
    scaffold_group_sql: str,
    scaffold_join_sql: str,
    match_condition: str,
) -> pd.DataFrame:
    return conn.execute(
        f"""
        WITH daily_group AS (
            SELECT
                {scaffold_select_sql}
                market_scope,
                date,
                sma5_below_streak_bucket,
                CASE
                    WHEN sma5_below_streak_bucket = 'below_sma5_streak_other'
                        THEN 0
                    WHEN sma5_below_streak_bucket = 'below_sma5_streak_ge3'
                        THEN 1
                END AS sma5_below_streak_bucket_order,
                count(*) AS observation_count,
                median({return_column}) AS median_excess_return_pct,
                avg({return_column}) AS mean_excess_return_pct
            FROM ranking_sma5_below_streak_panel
            {scaffold_lateral_sql}
            WHERE {match_condition}
              AND {return_column} IS NOT NULL
              AND sma5_below_streak_bucket IS NOT NULL
            GROUP BY
                {scaffold_group_sql}
                market_scope,
                date,
                sma5_below_streak_bucket,
                sma5_below_streak_bucket_order
        ),
        pair_values AS (
            SELECT
                base.long_scaffold,
                base.long_scaffold_order,
                base.market_scope,
                base.date,
                base.sma5_below_streak_bucket AS base_sma5_below_streak_bucket,
                comparison.sma5_below_streak_bucket
                    AS comparison_sma5_below_streak_bucket,
                base.observation_count AS base_observation_count,
                comparison.observation_count AS comparison_observation_count,
                base.median_excess_return_pct AS base_daily_median_excess_return_pct,
                comparison.median_excess_return_pct
                    AS comparison_daily_median_excess_return_pct,
                comparison.median_excess_return_pct
                    - base.median_excess_return_pct
                    AS daily_median_excess_spread_pct,
                comparison.mean_excess_return_pct
                    - base.mean_excess_return_pct
                    AS daily_mean_excess_spread_pct
            FROM daily_group base
            JOIN daily_group comparison
              ON comparison.market_scope = base.market_scope
             AND comparison.date = base.date
             {scaffold_join_sql}
             AND base.sma5_below_streak_bucket_order = 0
             AND comparison.sma5_below_streak_bucket_order = 1
        )
        SELECT
            {condition_family!r} AS condition_family,
            long_scaffold,
            long_scaffold_order,
            base_sma5_below_streak_bucket,
            comparison_sma5_below_streak_bucket,
            {int(horizon)} AS horizon,
            market_scope,
            count(*) AS matched_date_count,
            sum(base_observation_count) AS base_observation_count,
            sum(comparison_observation_count) AS comparison_observation_count,
            avg(base_observation_count) AS mean_base_observations_per_date,
            avg(comparison_observation_count)
                AS mean_comparison_observations_per_date,
            median(base_daily_median_excess_return_pct)
                AS median_base_daily_median_excess_return_pct,
            median(comparison_daily_median_excess_return_pct)
                AS median_comparison_daily_median_excess_return_pct,
            median(daily_median_excess_spread_pct)
                AS median_daily_median_excess_spread_pct,
            avg(daily_median_excess_spread_pct)
                AS mean_daily_median_excess_spread_pct,
            quantile_cont(daily_median_excess_spread_pct, 0.10)
                AS p10_daily_median_excess_spread_pct,
            quantile_cont(daily_median_excess_spread_pct, 0.25)
                AS p25_daily_median_excess_spread_pct,
            quantile_cont(daily_median_excess_spread_pct, 0.75)
                AS p75_daily_median_excess_spread_pct,
            quantile_cont(daily_median_excess_spread_pct, 0.90)
                AS p90_daily_median_excess_spread_pct,
            avg(CASE WHEN daily_median_excess_spread_pct > 0 THEN 1.0 ELSE 0.0 END)
                * 100.0 AS comparison_outperform_date_rate_pct,
            median(daily_mean_excess_spread_pct)
                AS median_daily_mean_excess_spread_pct,
            avg(daily_mean_excess_spread_pct)
                AS mean_daily_mean_excess_spread_pct
        FROM pair_values
        GROUP BY
            long_scaffold,
            long_scaffold_order,
            base_sma5_below_streak_bucket,
            comparison_sma5_below_streak_bucket,
            market_scope
        """
    ).fetchdf()


def _sma5_below_streak_metric_sql() -> str:
    return """,
            avg(CASE WHEN below_sma5_streak_ge3_flag THEN 1.0 ELSE 0.0 END)
                * 100.0 AS below_sma5_streak_ge3_rate_pct,
            median(sma5_deviation_pct) AS median_sma5_deviation_pct,
            median(close_below_sma5_count_3d) AS median_close_below_sma5_count_3d"""


def _query_observation_sample_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    limit: int,
) -> pd.DataFrame:
    horizon_columns = ",\n            ".join(
        f"forward_close_excess_return_{int(horizon)}d_pct" for horizon in horizons
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
            close,
            sma5,
            sma5_deviation_pct,
            close_below_sma5_flag,
            close_below_sma5_count_3d,
            sma5_above_count_5d,
            below_sma5_streak_ge3_flag,
            sma5_below_streak_bucket,
            sma5_count_group,
            recent_return_20d_pct,
            recent_return_60d_pct,
            liquidity_residual_z,
            valuation_signal,
            pbr,
            pbr_percentile,
            forward_per,
            forward_per_percentile,
            sector_strength_bucket,
            long_hybrid_leadership_score,
            atr20_change_20d_pct,
            atr20_acceleration_ex_overheat_flag,
            {horizon_columns}
        FROM ranking_sma5_below_streak_panel
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


def _sma5_below_streak_metric_columns() -> list[str]:
    return [
        "below_sma5_streak_ge3_rate_pct",
        "median_sma5_deviation_pct",
        "median_close_below_sma5_count_3d",
    ]


def _sma5_below_streak_columns() -> list[str]:
    return [
        "condition_family",
        "sma5_below_streak_bucket",
        "sma5_below_streak_bucket_order",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
        *_sma5_below_streak_metric_columns(),
    ]


def _long_scaffold_sma5_below_streak_columns() -> list[str]:
    return [
        "condition_family",
        "long_scaffold",
        "long_scaffold_order",
        "sma5_below_streak_bucket",
        "sma5_below_streak_bucket_order",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
        *_deep_dive_metric_columns(),
        *_sma5_below_streak_metric_columns(),
    ]


def _long_scaffold_sma5_below_streak_count_cross_columns() -> list[str]:
    return [
        "condition_family",
        "long_scaffold",
        "long_scaffold_order",
        "sma5_below_streak_bucket",
        "sma5_below_streak_bucket_order",
        "sma5_count_group",
        "sma5_count_group_order",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
        *_deep_dive_metric_columns(),
        *_sma5_below_streak_metric_columns(),
    ]


def _same_day_spread_columns() -> list[str]:
    return [
        "condition_family",
        "long_scaffold",
        "long_scaffold_order",
        "base_sma5_below_streak_bucket",
        "comparison_sma5_below_streak_bucket",
        "horizon",
        "market_scope",
        "matched_date_count",
        "base_observation_count",
        "comparison_observation_count",
        "mean_base_observations_per_date",
        "mean_comparison_observations_per_date",
        "median_base_daily_median_excess_return_pct",
        "median_comparison_daily_median_excess_return_pct",
        "median_daily_median_excess_spread_pct",
        "mean_daily_median_excess_spread_pct",
        "p10_daily_median_excess_spread_pct",
        "p25_daily_median_excess_spread_pct",
        "p75_daily_median_excess_spread_pct",
        "p90_daily_median_excess_spread_pct",
        "comparison_outperform_date_rate_pct",
        "median_daily_mean_excess_spread_pct",
        "mean_daily_mean_excess_spread_pct",
    ]


def _same_day_count_cross_spread_columns() -> list[str]:
    return [
        "condition_family",
        "long_scaffold",
        "long_scaffold_order",
        "weak_sma5_below_streak_bucket",
        "weak_sma5_count_group",
        "comparison_sma5_below_streak_bucket",
        "comparison_sma5_count_group",
        "horizon",
        "market_scope",
        "matched_date_count",
        "weak_observation_count",
        "comparison_observation_count",
        "mean_weak_observations_per_date",
        "mean_comparison_observations_per_date",
        "median_weak_daily_median_excess_return_pct",
        "median_comparison_daily_median_excess_return_pct",
        "median_daily_median_excess_weak_minus_comparison_pct",
        "mean_daily_median_excess_weak_minus_comparison_pct",
        "p10_daily_median_excess_weak_minus_comparison_pct",
        "p25_daily_median_excess_weak_minus_comparison_pct",
        "p75_daily_median_excess_weak_minus_comparison_pct",
        "p90_daily_median_excess_weak_minus_comparison_pct",
        "weak_underperform_date_rate_pct",
        "median_daily_mean_excess_weak_minus_comparison_pct",
        "mean_daily_mean_excess_weak_minus_comparison_pct",
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
            "long_scaffold_order",
            "sma5_below_streak_bucket_order",
            "sma5_count_group_order",
        )
        if column in frame.columns
    ]
    return frame.reindex(columns=list(columns)).sort_values(
        order_columns,
        kind="stable",
    )


def _parse_optional_date(value: str | None) -> date | None:
    return date.fromisoformat(value) if value is not None else None
