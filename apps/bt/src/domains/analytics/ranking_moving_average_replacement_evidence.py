"""Moving-average replacements for Daily Ranking fixed-return technical states."""

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
    RollingTrendFeaturesRequest,
    SectorStrengthFeaturesRequest,
    ShortScaffoldFeaturesRequest,
    build_atr_features,
    build_long_leadership_features,
    build_long_scaffold_features,
    build_rolling_trend_features,
    build_sector_strength_features,
    build_short_scaffold_features,
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
RANKING_MOVING_AVERAGE_REPLACEMENT_EVIDENCE_EXPERIMENT_ID = (
    "market-behavior/ranking-moving-average-replacement-evidence"
)
DEFAULT_HORIZONS: tuple[int, ...] = (5, 20, 60)
DEFAULT_MARKET_SCOPES: tuple[str, ...] = ("prime",)
DEFAULT_MIN_OBSERVATIONS = 300
DEFAULT_SEVERE_LOSS_THRESHOLD_PCT = -10.0
DEFAULT_OBSERVATION_SAMPLE_LIMIT = 10_000
OVERHEAT_RETURN_20D_THRESHOLD_PCT = 30.0
_REQUIRED_TABLES: tuple[str, ...] = (
    "stock_data_raw",
    "stock_adjustment_bases",
    "stock_adjustment_basis_segments",
    "topix_data",
    "daily_valuation",
    "stock_master_daily",
    "indices_data",
    "index_master",
)
_LEADERSHIP_WINDOWS: tuple[int, ...] = (120, 252, 504)
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
_TECHNICAL_CONDITIONS: tuple[tuple[str, str], ...] = (
    ("all_market", "TRUE"),
    ("fixed_overheat_20d_return_ge_30", "fixed_overheat_flag"),
    ("sma20_deviation_ge_30", "sma20_literal_overheat_flag"),
    ("sma20_qmatched_overheat", "sma20_qmatched_overheat_flag"),
    ("ema20_deviation_ge_30", "ema20_literal_overheat_flag"),
    ("ema20_qmatched_overheat", "ema20_qmatched_overheat_flag"),
    ("fixed_20d_pos_60d_pos", "fixed_price_action_bucket = 'fixed_20d_pos_60d_pos'"),
    ("sma20_pos_sma60_pos", "sma_price_action_bucket = 'sma20_pos_sma60_pos'"),
    ("ema20_pos_ema60_pos", "ema_price_action_bucket = 'ema20_pos_ema60_pos'"),
    ("fixed_20d_pos_60d_neg", "fixed_price_action_bucket = 'fixed_20d_pos_60d_neg'"),
    ("sma20_pos_sma60_neg", "sma_price_action_bucket = 'sma20_pos_sma60_neg'"),
    ("ema20_pos_ema60_neg", "ema_price_action_bucket = 'ema20_pos_ema60_neg'"),
    ("fixed_20d_neg_60d_pos", "fixed_price_action_bucket = 'fixed_20d_neg_60d_pos'"),
    ("sma20_neg_sma60_pos", "sma_price_action_bucket = 'sma20_neg_sma60_pos'"),
    ("ema20_neg_ema60_pos", "ema_price_action_bucket = 'ema20_neg_ema60_pos'"),
    ("fixed_20d_neg_60d_neg", "fixed_price_action_bucket = 'fixed_20d_neg_60d_neg'"),
    ("sma20_neg_sma60_neg", "sma_price_action_bucket = 'sma20_neg_sma60_neg'"),
    ("ema20_neg_ema60_neg", "ema_price_action_bucket = 'ema20_neg_ema60_neg'"),
    ("fixed_stale_rally_fade_candidate", "fixed_stale_rally_fade_candidate"),
    ("sma_stale_rally_fade_candidate", "sma_stale_rally_fade_candidate"),
    ("ema_stale_rally_fade_candidate", "ema_stale_rally_fade_candidate"),
)
_REPLACEMENT_PAIRS: tuple[tuple[str, str, str], ...] = (
    (
        "sma_overheat_qmatched",
        "fixed_overheat_20d_return_ge_30",
        "sma20_qmatched_overheat",
    ),
    (
        "ema_overheat_qmatched",
        "fixed_overheat_20d_return_ge_30",
        "ema20_qmatched_overheat",
    ),
    (
        "sma_overheat_literal",
        "fixed_overheat_20d_return_ge_30",
        "sma20_deviation_ge_30",
    ),
    (
        "ema_overheat_literal",
        "fixed_overheat_20d_return_ge_30",
        "ema20_deviation_ge_30",
    ),
    ("sma_dual_positive", "fixed_20d_pos_60d_pos", "sma20_pos_sma60_pos"),
    ("ema_dual_positive", "fixed_20d_pos_60d_pos", "ema20_pos_ema60_pos"),
    (
        "sma_recent20_positive_60d_negative",
        "fixed_20d_pos_60d_neg",
        "sma20_pos_sma60_neg",
    ),
    (
        "ema_recent20_positive_60d_negative",
        "fixed_20d_pos_60d_neg",
        "ema20_pos_ema60_neg",
    ),
    (
        "sma_recent20_negative_60d_positive",
        "fixed_20d_neg_60d_pos",
        "sma20_neg_sma60_pos",
    ),
    (
        "ema_recent20_negative_60d_positive",
        "fixed_20d_neg_60d_pos",
        "ema20_neg_ema60_pos",
    ),
    ("sma_dual_negative", "fixed_20d_neg_60d_neg", "sma20_neg_sma60_neg"),
    ("ema_dual_negative", "fixed_20d_neg_60d_neg", "ema20_neg_ema60_neg"),
    (
        "sma_stale_rally_fade_candidate",
        "fixed_stale_rally_fade_candidate",
        "sma_stale_rally_fade_candidate",
    ),
    (
        "ema_stale_rally_fade_candidate",
        "fixed_stale_rally_fade_candidate",
        "ema_stale_rally_fade_candidate",
    ),
)


@dataclass(frozen=True)
class RankingMovingAverageReplacementEvidenceResult:
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
    fixed_overheat_observation_count: int
    sma20_qmatched_overheat_threshold_pct: float | None
    ema20_qmatched_overheat_threshold_pct: float | None
    coverage_diagnostics_df: pd.DataFrame
    technical_condition_evidence_df: pd.DataFrame
    replacement_delta_df: pd.DataFrame
    long_candidate_moving_average_evidence_df: pd.DataFrame
    price_action_migration_df: pd.DataFrame
    overheat_overlap_df: pd.DataFrame
    observation_sample_df: pd.DataFrame


def run_ranking_moving_average_replacement_evidence_research(
    db_path: str | Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    market_scopes: Sequence[str] = DEFAULT_MARKET_SCOPES,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
    severe_loss_threshold_pct: float = DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    observation_sample_limit: int = DEFAULT_OBSERVATION_SAMPLE_LIMIT,
) -> RankingMovingAverageReplacementEvidenceResult:
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
        snapshot_prefix="ranking-moving-average-replacement-evidence-",
    ) as ctx:
        _assert_required_tables(ctx.connection)
        relations = build_daily_ranking_research_base(
            ctx.connection,
            DailyRankingPanelRequest(
                namespace="moving_average_replacement",
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
            AtrFeaturesRequest(
                source=signal_source,
                namespace="moving_average_replacement_atr",
            ),
        )
        short_features = build_short_scaffold_features(
            ctx.connection,
            ShortScaffoldFeaturesRequest(
                source=signal_source,
                atr_features=atr_features,
                namespace="moving_average_replacement_short",
            ),
        )
        sector_features = build_sector_strength_features(
            ctx.connection,
            SectorStrengthFeaturesRequest(
                source=signal_source,
                population_source=signal_source,
                namespace="moving_average_replacement_sector",
            ),
        )
        leadership_features = build_long_leadership_features(
            ctx.connection,
            LongLeadershipFeaturesRequest(
                source=signal_source,
                sector_features=sector_features,
                namespace="moving_average_replacement_leadership",
                leadership_windows=_LEADERSHIP_WINDOWS,
            ),
        )
        long_features = build_long_scaffold_features(
            ctx.connection,
            LongScaffoldFeaturesRequest(
                source=signal_source,
                leadership_features=leadership_features,
                short_scaffold_features=short_features,
                namespace="moving_average_replacement_long",
            ),
        )
        trend_features = build_rolling_trend_features(
            ctx.connection,
            RollingTrendFeaturesRequest(
                source=signal_source,
                price_history=relations.price_history,
                namespace="moving_average_replacement_trend",
            ),
        )
        composed = compose_daily_ranking_signal_features(
            ctx.connection,
            source=signal_source,
            features=(long_features, trend_features),
            namespace="moving_average_replacement",
        )
        eligible_sql = (
            "sma20 > 0 AND sma60 > 0 AND ema20 > 0 AND ema60 > 0 "
            "AND recent_return_20d_pct IS NOT NULL "
            "AND recent_return_60d_pct IS NOT NULL"
        )
        fixed_overheat_count = int(
            ctx.connection.execute(
                f"""
                SELECT count(*)
                FROM {composed.name}
                WHERE {eligible_sql}
                  AND recent_return_20d_pct >= ?
                """,
                [OVERHEAT_RETURN_20D_THRESHOLD_PCT],
            ).fetchone()[0]
        )
        sma_threshold = _quantile_matched_threshold(
            ctx.connection,
            source_name=composed.name,
            deviation_sql="((close / sma20) - 1.0) * 100.0",
            eligible_sql=eligible_sql,
            target_count=fixed_overheat_count,
        )
        ema_threshold = _quantile_matched_threshold(
            ctx.connection,
            source_name=composed.name,
            deviation_sql="((close / ema20) - 1.0) * 100.0",
            eligible_sql=eligible_sql,
            target_count=fixed_overheat_count,
        )
        cohort = materialize_daily_ranking_signal_cohort(
            ctx.connection,
            relations,
            source=composed,
            name="moving_average_replacement_signals",
            predicate=SignalExpression(
                sql=eligible_sql,
                referenced_columns=(
                    "sma20",
                    "sma60",
                    "ema20",
                    "ema60",
                    "recent_return_20d_pct",
                    "recent_return_60d_pct",
                ),
            ),
            derived_columns=_replacement_derived_columns(
                sma_threshold=sma_threshold,
                ema_threshold=ema_threshold,
            ),
        )
        evaluated = attach_daily_ranking_outcomes(
            ctx.connection,
            cohort,
            relations,
            name="moving_average_replacement",
        )
        _create_replacement_panel(
            ctx.connection,
            source_name=evaluated.name,
        )
        observation_count = int(
            ctx.connection.execute(
                "SELECT count(*) FROM ranking_moving_average_replacement_panel"
            ).fetchone()[0]
        )
        technical_condition_evidence_df = _build_technical_condition_evidence_df(
            ctx.connection,
            horizons=resolved_horizons,
            min_observations=min_observations,
            severe_loss_threshold_pct=severe_loss_threshold_pct,
        )
        result = RankingMovingAverageReplacementEvidenceResult(
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
            fixed_overheat_observation_count=fixed_overheat_count,
            sma20_qmatched_overheat_threshold_pct=sma_threshold,
            ema20_qmatched_overheat_threshold_pct=ema_threshold,
            coverage_diagnostics_df=_build_coverage_diagnostics_df(ctx.connection),
            technical_condition_evidence_df=technical_condition_evidence_df,
            replacement_delta_df=_build_replacement_delta_df(
                technical_condition_evidence_df,
            ),
            long_candidate_moving_average_evidence_df=(
                _build_long_candidate_moving_average_evidence_df(
                    ctx.connection,
                    horizons=resolved_horizons,
                    min_observations=min_observations,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
            ),
            price_action_migration_df=_build_price_action_migration_df(
                ctx.connection,
                horizons=resolved_horizons,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            overheat_overlap_df=_build_overheat_overlap_df(
                ctx.connection,
                horizons=resolved_horizons,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            observation_sample_df=_query_observation_sample_df(
                ctx.connection,
                horizons=resolved_horizons,
                limit=observation_sample_limit,
            ),
        )
    return result


def write_ranking_moving_average_replacement_evidence_bundle(
    result: RankingMovingAverageReplacementEvidenceResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=RANKING_MOVING_AVERAGE_REPLACEMENT_EVIDENCE_EXPERIMENT_ID,
        module="src.domains.analytics.ranking_moving_average_replacement_evidence",
        function="run_ranking_moving_average_replacement_evidence_research",
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
            "fixed_overheat_observation_count": (
                result.fixed_overheat_observation_count
            ),
            "sma20_qmatched_overheat_threshold_pct": (
                result.sma20_qmatched_overheat_threshold_pct
            ),
            "ema20_qmatched_overheat_threshold_pct": (
                result.ema20_qmatched_overheat_threshold_pct
            ),
            "fixed_return_definition": "close / close_lag_N - 1",
            "moving_average_definition": "close / SMA_N - 1 and close / EMA_N - 1",
            "primary_outcome": "forward_close_excess_return_{horizon}d_pct",
        },
        result_tables={
            "coverage_diagnostics_df": result.coverage_diagnostics_df,
            "technical_condition_evidence_df": result.technical_condition_evidence_df,
            "replacement_delta_df": result.replacement_delta_df,
            "long_candidate_moving_average_evidence_df": (
                result.long_candidate_moving_average_evidence_df
            ),
            "price_action_migration_df": result.price_action_migration_df,
            "overheat_overlap_df": result.overheat_overlap_df,
            "observation_sample_df": result.observation_sample_df,
        },
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def build_summary_markdown(
    result: RankingMovingAverageReplacementEvidenceResult,
) -> str:
    parts = [
        "# Ranking Moving Average Replacement Evidence",
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
        f"- fixed_overheat_observation_count: `{result.fixed_overheat_observation_count}`",
        "- sma20_qmatched_overheat_threshold_pct: "
        f"`{result.sma20_qmatched_overheat_threshold_pct}`",
        "- ema20_qmatched_overheat_threshold_pct: "
        f"`{result.ema20_qmatched_overheat_threshold_pct}`",
        "",
        "## Coverage Diagnostics",
        "",
        _top_rows_for_markdown(result.coverage_diagnostics_df, limit=80),
        "",
        "## Technical Condition Evidence",
        "",
        _top_rows_for_markdown(result.technical_condition_evidence_df, limit=240),
        "",
        "## Replacement Delta",
        "",
        _top_rows_for_markdown(result.replacement_delta_df, limit=120),
        "",
        "## Long Candidate Moving Average Evidence",
        "",
        _top_rows_for_markdown(
            result.long_candidate_moving_average_evidence_df,
            limit=260,
        ),
        "",
        "## Price Action Migration",
        "",
        _top_rows_for_markdown(result.price_action_migration_df, limit=160),
        "",
        "## Overheat Overlap",
        "",
        _top_rows_for_markdown(result.overheat_overlap_df, limit=120),
    ]
    return "\n".join(parts).rstrip() + "\n"


def _assert_required_tables(conn: Any) -> None:
    missing = [table for table in _REQUIRED_TABLES if not table_exists(conn, table)]
    if missing:
        raise ValueError(
            f"market.duckdb is missing required tables: {', '.join(missing)}"
        )


def _create_replacement_panel(conn: Any, *, source_name: str) -> None:
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranking_moving_average_replacement_panel AS
        SELECT * FROM {source_name}
        """
    )


def _quantile_matched_threshold(
    conn: Any,
    *,
    source_name: str,
    deviation_sql: str,
    eligible_sql: str,
    target_count: int,
) -> float | None:
    if target_count <= 0:
        return None
    row = conn.execute(
        f"""
        SELECT {deviation_sql}
        FROM {source_name}
        WHERE {eligible_sql}
        ORDER BY {deviation_sql} DESC
        LIMIT 1 OFFSET ?
        """,
        [target_count - 1],
    ).fetchone()
    return None if row is None else float(row[0])


def _replacement_derived_columns(
    *,
    sma_threshold: float | None,
    ema_threshold: float | None,
) -> tuple[SignalDerivedColumn, ...]:
    sma20_deviation = "((close / sma20) - 1.0) * 100.0"
    sma60_deviation = "((close / sma60) - 1.0) * 100.0"
    ema20_deviation = "((close / ema20) - 1.0) * 100.0"
    ema60_deviation = "((close / ema60) - 1.0) * 100.0"
    sma_threshold_sql = "NULL" if sma_threshold is None else f"{sma_threshold:.12f}"
    ema_threshold_sql = "NULL" if ema_threshold is None else f"{ema_threshold:.12f}"
    valuation_warning = "(overvalued_warning OR no_positive_earnings_valuation)"
    return (
        _derived("sma20_deviation_pct", sma20_deviation, ("close", "sma20"), "DOUBLE"),
        _derived("sma60_deviation_pct", sma60_deviation, ("close", "sma60"), "DOUBLE"),
        _derived("ema20_deviation_pct", ema20_deviation, ("close", "ema20"), "DOUBLE"),
        _derived("ema60_deviation_pct", ema60_deviation, ("close", "ema60"), "DOUBLE"),
        _derived(
            "fixed_overheat_flag",
            f"recent_return_20d_pct >= {OVERHEAT_RETURN_20D_THRESHOLD_PCT}",
            ("recent_return_20d_pct",),
            "BOOLEAN",
        ),
        _derived(
            "sma20_literal_overheat_flag",
            f"{sma20_deviation} >= {OVERHEAT_RETURN_20D_THRESHOLD_PCT}",
            ("close", "sma20"),
            "BOOLEAN",
        ),
        _derived(
            "ema20_literal_overheat_flag",
            f"{ema20_deviation} >= {OVERHEAT_RETURN_20D_THRESHOLD_PCT}",
            ("close", "ema20"),
            "BOOLEAN",
        ),
        _derived(
            "fixed_price_action_bucket",
            _price_action_case(
                "recent_return_20d_pct", "recent_return_60d_pct", "fixed"
            ),
            ("recent_return_20d_pct", "recent_return_60d_pct"),
            "VARCHAR",
        ),
        _derived(
            "sma_price_action_bucket",
            _price_action_case(sma20_deviation, sma60_deviation, "sma"),
            ("close", "sma20", "sma60"),
            "VARCHAR",
        ),
        _derived(
            "ema_price_action_bucket",
            _price_action_case(ema20_deviation, ema60_deviation, "ema"),
            ("close", "ema20", "ema60"),
            "VARCHAR",
        ),
        _derived(
            "fixed_stale_rally_fade_candidate",
            "liquidity_regime = 'stale_liquidity' "
            f"AND {valuation_warning} "
            "AND recent_return_20d_pct > 0 AND recent_return_60d_pct > 0",
            (
                "liquidity_regime",
                "overvalued_warning",
                "no_positive_earnings_valuation",
                "recent_return_20d_pct",
                "recent_return_60d_pct",
            ),
            "BOOLEAN",
        ),
        _derived(
            "sma_stale_rally_fade_candidate",
            "liquidity_regime = 'stale_liquidity' "
            f"AND {valuation_warning} "
            f"AND {sma20_deviation} > 0 AND {sma60_deviation} > 0",
            (
                "liquidity_regime",
                "overvalued_warning",
                "no_positive_earnings_valuation",
                "close",
                "sma20",
                "sma60",
            ),
            "BOOLEAN",
        ),
        _derived(
            "ema_stale_rally_fade_candidate",
            "liquidity_regime = 'stale_liquidity' "
            f"AND {valuation_warning} "
            f"AND {ema20_deviation} > 0 AND {ema60_deviation} > 0",
            (
                "liquidity_regime",
                "overvalued_warning",
                "no_positive_earnings_valuation",
                "close",
                "ema20",
                "ema60",
            ),
            "BOOLEAN",
        ),
        _derived(
            "sma20_qmatched_overheat_threshold_pct",
            sma_threshold_sql,
            (),
            "DOUBLE",
        ),
        _derived(
            "ema20_qmatched_overheat_threshold_pct",
            ema_threshold_sql,
            (),
            "DOUBLE",
        ),
        _derived(
            "sma20_qmatched_overheat_flag",
            "FALSE"
            if sma_threshold is None
            else f"{sma20_deviation} >= {sma_threshold_sql}",
            () if sma_threshold is None else ("close", "sma20"),
            "BOOLEAN",
        ),
        _derived(
            "ema20_qmatched_overheat_flag",
            "FALSE"
            if ema_threshold is None
            else f"{ema20_deviation} >= {ema_threshold_sql}",
            () if ema_threshold is None else ("close", "ema20"),
            "BOOLEAN",
        ),
    )


def _derived(
    name: str,
    sql: str,
    referenced_columns: tuple[str, ...],
    sql_type: str,
) -> SignalDerivedColumn:
    return SignalDerivedColumn(
        name=name,
        expression=SignalExpression(sql=sql, referenced_columns=referenced_columns),
        sql_type=sql_type,
    )


def _price_action_case(short_sql: str, long_sql: str, prefix: str) -> str:
    short_label = "fixed_20d" if prefix == "fixed" else f"{prefix}20"
    long_label = "60d" if prefix == "fixed" else f"{prefix}60"
    return (
        f"CASE WHEN {short_sql} > 0 AND {long_sql} > 0 "
        f"THEN '{short_label}_pos_{long_label}_pos' "
        f"WHEN {short_sql} > 0 AND {long_sql} < 0 "
        f"THEN '{short_label}_pos_{long_label}_neg' "
        f"WHEN {short_sql} < 0 AND {long_sql} > 0 "
        f"THEN '{short_label}_neg_{long_label}_pos' "
        f"WHEN {short_sql} < 0 AND {long_sql} < 0 "
        f"THEN '{short_label}_neg_{long_label}_neg' "
        f"ELSE '{prefix}_price_action_unclassified' END"
    )


def _build_coverage_diagnostics_df(conn: Any) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            market_scope,
            count(*) AS observation_count,
            count(DISTINCT code) AS code_count,
            count(DISTINCT date) AS date_count,
            median(recent_return_20d_pct) AS median_recent_return_20d_pct,
            median(recent_return_60d_pct) AS median_recent_return_60d_pct,
            median(sma20_deviation_pct) AS median_sma20_deviation_pct,
            median(sma60_deviation_pct) AS median_sma60_deviation_pct,
            median(ema20_deviation_pct) AS median_ema20_deviation_pct,
            median(ema60_deviation_pct) AS median_ema60_deviation_pct,
            quantile_cont(sma20_deviation_pct, 0.9) AS p90_sma20_deviation_pct,
            quantile_cont(sma60_deviation_pct, 0.9) AS p90_sma60_deviation_pct,
            quantile_cont(ema20_deviation_pct, 0.9) AS p90_ema20_deviation_pct,
            quantile_cont(ema60_deviation_pct, 0.9) AS p90_ema60_deviation_pct,
            avg(CASE WHEN fixed_overheat_flag THEN 1.0 ELSE 0.0 END) * 100.0
                AS fixed_overheat_rate_pct,
            avg(CASE WHEN sma20_literal_overheat_flag THEN 1.0 ELSE 0.0 END) * 100.0
                AS sma20_literal_overheat_rate_pct,
            avg(CASE WHEN sma20_qmatched_overheat_flag THEN 1.0 ELSE 0.0 END) * 100.0
                AS sma20_qmatched_overheat_rate_pct,
            avg(CASE WHEN ema20_literal_overheat_flag THEN 1.0 ELSE 0.0 END) * 100.0
                AS ema20_literal_overheat_rate_pct,
            avg(CASE WHEN ema20_qmatched_overheat_flag THEN 1.0 ELSE 0.0 END) * 100.0
                AS ema20_qmatched_overheat_rate_pct,
            avg(CASE
                WHEN fixed_price_action_bucket = 'fixed_20d_pos_60d_pos'
                 AND sma_price_action_bucket = 'sma20_pos_sma60_pos' THEN 1.0
                WHEN fixed_price_action_bucket = 'fixed_20d_pos_60d_neg'
                 AND sma_price_action_bucket = 'sma20_pos_sma60_neg' THEN 1.0
                WHEN fixed_price_action_bucket = 'fixed_20d_neg_60d_pos'
                 AND sma_price_action_bucket = 'sma20_neg_sma60_pos' THEN 1.0
                WHEN fixed_price_action_bucket = 'fixed_20d_neg_60d_neg'
                 AND sma_price_action_bucket = 'sma20_neg_sma60_neg' THEN 1.0
                ELSE 0.0
            END) * 100.0 AS sma_price_action_sign_match_rate_pct,
            avg(CASE
                WHEN fixed_price_action_bucket = 'fixed_20d_pos_60d_pos'
                 AND ema_price_action_bucket = 'ema20_pos_ema60_pos' THEN 1.0
                WHEN fixed_price_action_bucket = 'fixed_20d_pos_60d_neg'
                 AND ema_price_action_bucket = 'ema20_pos_ema60_neg' THEN 1.0
                WHEN fixed_price_action_bucket = 'fixed_20d_neg_60d_pos'
                 AND ema_price_action_bucket = 'ema20_neg_ema60_pos' THEN 1.0
                WHEN fixed_price_action_bucket = 'fixed_20d_neg_60d_neg'
                 AND ema_price_action_bucket = 'ema20_neg_ema60_neg' THEN 1.0
                ELSE 0.0
            END) * 100.0 AS ema_price_action_sign_match_rate_pct
        FROM ranking_moving_average_replacement_panel
        GROUP BY market_scope
        ORDER BY market_scope
        """
    ).fetchdf()


def _build_technical_condition_evidence_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    lateral_sql = f"""
        CROSS JOIN LATERAL (
            VALUES {_condition_values_sql(_TECHNICAL_CONDITIONS)}
        ) AS technical_condition(
            technical_condition,
            technical_condition_order,
            technical_condition_matches
        )
    """
    for horizon in horizons:
        frames.append(
            _aggregate_lateral_conditions(
                conn,
                source_name="ranking_moving_average_replacement_panel",
                lateral_sql=lateral_sql,
                match_condition="technical_condition.technical_condition_matches",
                group_select_sql=(
                    "'technical_condition' AS condition_family,\n"
                    "            technical_condition.technical_condition,\n"
                    "            technical_condition.technical_condition_order,\n"
                    f"            {int(horizon)} AS horizon"
                ),
                group_by_sql=(
                    "technical_condition.technical_condition, "
                    "technical_condition.technical_condition_order, "
                    "market_scope"
                ),
                return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
                extra_metric_sql=_replacement_metric_sql(),
            )
        )
    return _concat_sorted(frames, columns=_technical_condition_columns())


def _build_replacement_delta_df(evidence_df: pd.DataFrame) -> pd.DataFrame:
    if evidence_df.empty:
        return pd.DataFrame(columns=_replacement_delta_columns())
    rows: list[dict[str, Any]] = []
    for pair_name, fixed_condition, sma_condition in _REPLACEMENT_PAIRS:
        fixed_rows = evidence_df[evidence_df["technical_condition"] == fixed_condition]
        sma_rows = evidence_df[evidence_df["technical_condition"] == sma_condition]
        merged = fixed_rows.merge(
            sma_rows,
            on=["market_scope", "horizon"],
            suffixes=("_fixed", "_sma"),
        )
        for item in merged.to_dict("records"):
            fixed_count = float(item["observation_count_fixed"])
            sma_count = float(item["observation_count_sma"])
            rows.append(
                {
                    "replacement_pair": pair_name,
                    "fixed_condition": fixed_condition,
                    "sma_condition": sma_condition,
                    "market_scope": item["market_scope"],
                    "horizon": item["horizon"],
                    "fixed_observation_count": item["observation_count_fixed"],
                    "sma_observation_count": item["observation_count_sma"],
                    "observation_count_delta": sma_count - fixed_count,
                    "fixed_median_forward_excess_return_pct": item[
                        "median_forward_excess_return_pct_fixed"
                    ],
                    "sma_median_forward_excess_return_pct": item[
                        "median_forward_excess_return_pct_sma"
                    ],
                    "median_forward_excess_return_delta_pct": item[
                        "median_forward_excess_return_pct_sma"
                    ]
                    - item["median_forward_excess_return_pct_fixed"],
                    "fixed_mean_forward_excess_return_pct": item[
                        "mean_forward_excess_return_pct_fixed"
                    ],
                    "sma_mean_forward_excess_return_pct": item[
                        "mean_forward_excess_return_pct_sma"
                    ],
                    "mean_forward_excess_return_delta_pct": item[
                        "mean_forward_excess_return_pct_sma"
                    ]
                    - item["mean_forward_excess_return_pct_fixed"],
                    "fixed_severe_loss_rate_pct": item["severe_loss_rate_pct_fixed"],
                    "sma_severe_loss_rate_pct": item["severe_loss_rate_pct_sma"],
                    "severe_loss_rate_delta_pct": item["severe_loss_rate_pct_sma"]
                    - item["severe_loss_rate_pct_fixed"],
                    "fixed_excess_win_rate_pct": item["excess_win_rate_pct_fixed"],
                    "sma_excess_win_rate_pct": item["excess_win_rate_pct_sma"],
                    "excess_win_rate_delta_pct": item["excess_win_rate_pct_sma"]
                    - item["excess_win_rate_pct_fixed"],
                }
            )
    return pd.DataFrame(rows, columns=_replacement_delta_columns()).sort_values(
        ["market_scope", "horizon", "replacement_pair"],
        kind="stable",
    )


def _build_long_candidate_moving_average_evidence_df(
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
            VALUES {_condition_values_sql(_TECHNICAL_CONDITIONS)}
        ) AS technical_condition(
            technical_condition,
            technical_condition_order,
            technical_condition_matches
        )
    """
    for horizon in horizons:
        frames.append(
            _aggregate_lateral_conditions(
                conn,
                source_name="ranking_moving_average_replacement_panel",
                lateral_sql=lateral_sql,
                match_condition=(
                    "long_scaffold.long_scaffold_matches "
                    "AND technical_condition.technical_condition_matches"
                ),
                group_select_sql=(
                    "'long_candidate_moving_average' AS condition_family,\n"
                    "            long_scaffold.long_scaffold,\n"
                    "            long_scaffold.long_scaffold_order,\n"
                    "            technical_condition.technical_condition,\n"
                    "            technical_condition.technical_condition_order,\n"
                    f"            {int(horizon)} AS horizon"
                ),
                group_by_sql=(
                    "long_scaffold.long_scaffold, "
                    "long_scaffold.long_scaffold_order, "
                    "technical_condition.technical_condition, "
                    "technical_condition.technical_condition_order, "
                    "market_scope"
                ),
                return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
                extra_metric_sql=_deep_dive_metric_sql() + _replacement_metric_sql(),
            )
        )
    return _concat_sorted(
        frames,
        columns=_long_candidate_moving_average_columns(),
    )


def _build_price_action_migration_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for horizon in horizons:
        frames.append(
            conn.execute(
                f"""
                SELECT
                    market_scope,
                    {int(horizon)} AS horizon,
                    fixed_price_action_bucket,
                    sma_price_action_bucket,
                    count(*) AS observation_count,
                    count(DISTINCT code) AS code_count,
                    count(DISTINCT date) AS date_count,
                    avg(forward_close_excess_return_{int(horizon)}d_pct)
                        AS mean_forward_excess_return_pct,
                    median(forward_close_excess_return_{int(horizon)}d_pct)
                        AS median_forward_excess_return_pct,
                    avg(CASE
                        WHEN forward_close_excess_return_{int(horizon)}d_pct > 0
                        THEN 1.0 ELSE 0.0 END) * 100.0 AS excess_win_rate_pct,
                    avg(CASE
                        WHEN forward_close_excess_return_{int(horizon)}d_pct <= ?
                        THEN 1.0 ELSE 0.0 END) * 100.0 AS severe_loss_rate_pct,
                    median(recent_return_20d_pct) AS median_recent_return_20d_pct,
                    median(recent_return_60d_pct) AS median_recent_return_60d_pct,
                    median(sma20_deviation_pct) AS median_sma20_deviation_pct,
                    median(sma60_deviation_pct) AS median_sma60_deviation_pct
                FROM ranking_moving_average_replacement_panel
                WHERE forward_close_excess_return_{int(horizon)}d_pct IS NOT NULL
                GROUP BY market_scope, fixed_price_action_bucket, sma_price_action_bucket
                ORDER BY market_scope, fixed_price_action_bucket, sma_price_action_bucket
                """,
                [float(severe_loss_threshold_pct)],
            ).fetchdf()
        )
    return _concat_sorted(frames, columns=_price_action_migration_columns())


def _build_overheat_overlap_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for horizon in horizons:
        frames.append(
            conn.execute(
                f"""
                SELECT
                    market_scope,
                    {int(horizon)} AS horizon,
                    fixed_overheat_flag,
                    sma20_literal_overheat_flag,
                    sma20_qmatched_overheat_flag,
                    ema20_literal_overheat_flag,
                    ema20_qmatched_overheat_flag,
                    count(*) AS observation_count,
                    count(DISTINCT code) AS code_count,
                    count(DISTINCT date) AS date_count,
                    median(recent_return_20d_pct) AS median_recent_return_20d_pct,
                    median(sma20_deviation_pct) AS median_sma20_deviation_pct,
                    median(ema20_deviation_pct) AS median_ema20_deviation_pct,
                    avg(forward_close_excess_return_{int(horizon)}d_pct)
                        AS mean_forward_excess_return_pct,
                    median(forward_close_excess_return_{int(horizon)}d_pct)
                        AS median_forward_excess_return_pct,
                    avg(CASE
                        WHEN forward_close_excess_return_{int(horizon)}d_pct > 0
                        THEN 1.0 ELSE 0.0 END) * 100.0 AS excess_win_rate_pct,
                    avg(CASE
                        WHEN forward_close_excess_return_{int(horizon)}d_pct <= ?
                        THEN 1.0 ELSE 0.0 END) * 100.0 AS severe_loss_rate_pct
                FROM ranking_moving_average_replacement_panel
                WHERE forward_close_excess_return_{int(horizon)}d_pct IS NOT NULL
                GROUP BY
                    market_scope,
                    fixed_overheat_flag,
                    sma20_literal_overheat_flag,
                    sma20_qmatched_overheat_flag,
                    ema20_literal_overheat_flag,
                    ema20_qmatched_overheat_flag
                ORDER BY
                    market_scope,
                    fixed_overheat_flag DESC,
                    sma20_literal_overheat_flag DESC,
                    sma20_qmatched_overheat_flag DESC,
                    ema20_literal_overheat_flag DESC,
                    ema20_qmatched_overheat_flag DESC
                """,
                [float(severe_loss_threshold_pct)],
            ).fetchdf()
        )
    return _concat_sorted(frames, columns=_overheat_overlap_columns())


def _replacement_metric_sql() -> str:
    return """,
            median(sma20_deviation_pct) AS median_sma20_deviation_pct,
            median(sma60_deviation_pct) AS median_sma60_deviation_pct,
            median(ema20_deviation_pct) AS median_ema20_deviation_pct,
            median(ema60_deviation_pct) AS median_ema60_deviation_pct,
            avg(CASE WHEN fixed_overheat_flag THEN 1.0 ELSE 0.0 END) * 100.0
                AS fixed_overheat_rate_pct,
            avg(CASE WHEN sma20_literal_overheat_flag THEN 1.0 ELSE 0.0 END) * 100.0
                AS sma20_literal_overheat_rate_pct,
            avg(CASE WHEN sma20_qmatched_overheat_flag THEN 1.0 ELSE 0.0 END) * 100.0
                AS sma20_qmatched_overheat_rate_pct,
            avg(CASE WHEN ema20_literal_overheat_flag THEN 1.0 ELSE 0.0 END) * 100.0
                AS ema20_literal_overheat_rate_pct,
            avg(CASE WHEN ema20_qmatched_overheat_flag THEN 1.0 ELSE 0.0 END) * 100.0
                AS ema20_qmatched_overheat_rate_pct,
            avg(CASE WHEN fixed_stale_rally_fade_candidate THEN 1.0 ELSE 0.0 END)
                * 100.0 AS fixed_stale_rally_fade_rate_pct,
            avg(CASE WHEN sma_stale_rally_fade_candidate THEN 1.0 ELSE 0.0 END)
                * 100.0 AS sma_stale_rally_fade_rate_pct,
            avg(CASE WHEN ema_stale_rally_fade_candidate THEN 1.0 ELSE 0.0 END)
                * 100.0 AS ema_stale_rally_fade_rate_pct"""


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
            fixed_price_action_bucket,
            sma_price_action_bucket,
            recent_return_20d_pct,
            recent_return_60d_pct,
            sma20,
            sma60,
            sma20_deviation_pct,
            sma60_deviation_pct,
            ema20,
            ema60,
            ema20_deviation_pct,
            ema60_deviation_pct,
            fixed_overheat_flag,
            sma20_literal_overheat_flag,
            sma20_qmatched_overheat_flag,
            ema20_literal_overheat_flag,
            ema20_qmatched_overheat_flag,
            fixed_stale_rally_fade_candidate,
            sma_stale_rally_fade_candidate,
            ema_stale_rally_fade_candidate,
            liquidity_residual_z,
            valuation_signal,
            {horizon_columns}
        FROM ranking_moving_average_replacement_panel
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


def _technical_condition_columns() -> list[str]:
    return [
        "condition_family",
        "technical_condition",
        "technical_condition_order",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
        *_replacement_metric_columns(),
    ]


def _replacement_metric_columns() -> list[str]:
    return [
        "median_sma20_deviation_pct",
        "median_sma60_deviation_pct",
        "median_ema20_deviation_pct",
        "median_ema60_deviation_pct",
        "fixed_overheat_rate_pct",
        "sma20_literal_overheat_rate_pct",
        "sma20_qmatched_overheat_rate_pct",
        "ema20_literal_overheat_rate_pct",
        "ema20_qmatched_overheat_rate_pct",
        "fixed_stale_rally_fade_rate_pct",
        "sma_stale_rally_fade_rate_pct",
        "ema_stale_rally_fade_rate_pct",
    ]


def _replacement_delta_columns() -> list[str]:
    return [
        "replacement_pair",
        "fixed_condition",
        "sma_condition",
        "market_scope",
        "horizon",
        "fixed_observation_count",
        "sma_observation_count",
        "observation_count_delta",
        "fixed_median_forward_excess_return_pct",
        "sma_median_forward_excess_return_pct",
        "median_forward_excess_return_delta_pct",
        "fixed_mean_forward_excess_return_pct",
        "sma_mean_forward_excess_return_pct",
        "mean_forward_excess_return_delta_pct",
        "fixed_severe_loss_rate_pct",
        "sma_severe_loss_rate_pct",
        "severe_loss_rate_delta_pct",
        "fixed_excess_win_rate_pct",
        "sma_excess_win_rate_pct",
        "excess_win_rate_delta_pct",
    ]


def _long_candidate_moving_average_columns() -> list[str]:
    return [
        "condition_family",
        "long_scaffold",
        "long_scaffold_order",
        "technical_condition",
        "technical_condition_order",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
        *_deep_dive_metric_columns(),
        *_replacement_metric_columns(),
    ]


def _price_action_migration_columns() -> list[str]:
    return [
        "market_scope",
        "horizon",
        "fixed_price_action_bucket",
        "sma_price_action_bucket",
        "observation_count",
        "code_count",
        "date_count",
        "mean_forward_excess_return_pct",
        "median_forward_excess_return_pct",
        "excess_win_rate_pct",
        "severe_loss_rate_pct",
        "median_recent_return_20d_pct",
        "median_recent_return_60d_pct",
        "median_sma20_deviation_pct",
        "median_sma60_deviation_pct",
    ]


def _overheat_overlap_columns() -> list[str]:
    return [
        "market_scope",
        "horizon",
        "fixed_overheat_flag",
        "sma20_literal_overheat_flag",
        "sma20_qmatched_overheat_flag",
        "ema20_literal_overheat_flag",
        "ema20_qmatched_overheat_flag",
        "observation_count",
        "code_count",
        "date_count",
        "median_recent_return_20d_pct",
        "median_sma20_deviation_pct",
        "median_ema20_deviation_pct",
        "mean_forward_excess_return_pct",
        "median_forward_excess_return_pct",
        "excess_win_rate_pct",
        "severe_loss_rate_pct",
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
            "technical_condition_order",
            "long_scaffold_order",
            "fixed_price_action_bucket",
            "sma_price_action_bucket",
        )
        if column in frame.columns
    ]
    return frame.reindex(columns=list(columns)).sort_values(
        order_columns,
        kind="stable",
    )


def _parse_optional_date(value: str | None) -> date | None:
    return date.fromisoformat(value) if value is not None else None
