"""Trend-slope evidence for Daily Ranking fixed-return technical states."""

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
    concat_sorted,
    condition_values_sql,
    deep_dive_metric_columns,
    deep_dive_metric_sql,
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

RANKING_TREND_SLOPE_EVIDENCE_EXPERIMENT_ID = (
    "market-behavior/ranking-trend-slope-evidence"
)
DEFAULT_HORIZONS: tuple[int, ...] = (5, 20, 60)
DEFAULT_MARKET_SCOPES: tuple[str, ...] = ("prime",)
DEFAULT_MIN_OBSERVATIONS = 300
DEFAULT_SEVERE_LOSS_THRESHOLD_PCT = -10.0
DEFAULT_OBSERVATION_SAMPLE_LIMIT = 10_000
_LEADERSHIP_WINDOWS: tuple[int, ...] = (120, 252, 504)
_SLOPE_WINDOWS: tuple[int, ...] = (20, 60)
_MA_SLOPE_LAGS: tuple[int, ...] = (5, 20)
_HIGH_R2_THRESHOLD = 0.5
_REQUIRED_TABLES: tuple[str, ...] = (
    "stock_data",
    "topix_data",
    "daily_valuation",
    "stock_master_daily",
    "indices_data",
    "index_master",
)
_TECHNICAL_CONDITIONS: tuple[tuple[str, str], ...] = (
    ("all_market", "TRUE"),
    ("fixed_20d_pos_60d_pos", "fixed_price_action_bucket = 'fixed_20d_pos_60d_pos'"),
    ("fixed_20d_pos_60d_neg", "fixed_price_action_bucket = 'fixed_20d_pos_60d_neg'"),
    ("fixed_20d_neg_60d_pos", "fixed_price_action_bucket = 'fixed_20d_neg_60d_pos'"),
    ("fixed_20d_neg_60d_neg", "fixed_price_action_bucket = 'fixed_20d_neg_60d_neg'"),
    ("lr20_pos_lr60_pos", "lr_price_action_bucket = 'lr20_pos_lr60_pos'"),
    ("lr20_pos_lr60_neg", "lr_price_action_bucket = 'lr20_pos_lr60_neg'"),
    ("lr20_neg_lr60_pos", "lr_price_action_bucket = 'lr20_neg_lr60_pos'"),
    ("lr20_neg_lr60_neg", "lr_price_action_bucket = 'lr20_neg_lr60_neg'"),
    (
        "lr20_pos_lr60_pos_r2_high",
        "lr_price_action_bucket = 'lr20_pos_lr60_pos' "
        f"AND price_lr_r2_20 >= {_HIGH_R2_THRESHOLD} "
        f"AND price_lr_r2_60 >= {_HIGH_R2_THRESHOLD}",
    ),
    (
        "sma20_slope_pos_sma60_slope_pos",
        "sma_slope_bucket = 'sma20_slope_pos_sma60_slope_pos'",
    ),
    (
        "sma20_slope_neg_sma60_slope_pos",
        "sma_slope_bucket = 'sma20_slope_neg_sma60_slope_pos'",
    ),
    (
        "ema20_slope_pos_ema60_slope_pos",
        "ema_slope_bucket = 'ema20_slope_pos_ema60_slope_pos'",
    ),
    (
        "ema20_slope_neg_ema60_slope_pos",
        "ema_slope_bucket = 'ema20_slope_neg_ema60_slope_pos'",
    ),
    (
        "lr20_accel_over_lr60",
        "price_lr_slope_20_pct > price_lr_slope_60_pct",
    ),
    (
        "lr20_decel_below_lr60",
        "price_lr_slope_20_pct <= price_lr_slope_60_pct",
    ),
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
        "AND pbr_percentile <= 0.1 AND forward_per_percentile <= 0.1",
    ),
    (
        "crowded_low10_pbr_forward_per_atr20_accel",
        "liquidity_regime = 'crowded_rerating' "
        "AND long_hybrid_leadership_score >= 0.799999 "
        "AND pbr_percentile <= 0.1 AND forward_per_percentile <= 0.1 "
        "AND atr20_acceleration_ex_overheat_flag",
    ),
)


@dataclass(frozen=True)
class RankingTrendSlopeEvidenceResult:
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
    coverage_diagnostics_df: pd.DataFrame
    technical_condition_evidence_df: pd.DataFrame
    fixed_vs_slope_conflict_df: pd.DataFrame
    long_candidate_trend_slope_evidence_df: pd.DataFrame
    observation_sample_df: pd.DataFrame


def run_ranking_trend_slope_evidence_research(
    db_path: str | Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    market_scopes: Sequence[str] = DEFAULT_MARKET_SCOPES,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
    severe_loss_threshold_pct: float = DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    observation_sample_limit: int = DEFAULT_OBSERVATION_SAMPLE_LIMIT,
) -> RankingTrendSlopeEvidenceResult:
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
        snapshot_prefix="ranking-trend-slope-evidence-",
    ) as ctx:
        _assert_required_tables(ctx.connection)
        relations = build_daily_ranking_research_base(
            ctx.connection,
            DailyRankingPanelRequest(
                namespace="trend_slope",
                analysis_start_date=_parse_optional_date(start_date),
                analysis_end_date=_parse_optional_date(end_date),
                horizons=resolved_horizons,
                market_scopes=cast(tuple[MarketScope, ...], resolved_market_scopes),
                include_liquidity=True,
                percentile_features=(),
            ),
        )
        signal_source = relations.liquidity_ranked_signals
        if signal_source is None:
            raise RuntimeError("trend slope research requires liquidity-ranked signals")
        atr_features = build_atr_features(
            ctx.connection,
            AtrFeaturesRequest(source=signal_source, namespace="trend_slope_atr"),
        )
        short_features = build_short_scaffold_features(
            ctx.connection,
            ShortScaffoldFeaturesRequest(
                source=signal_source,
                atr_features=atr_features,
                namespace="trend_slope_short",
            ),
        )
        sector_features = build_sector_strength_features(
            ctx.connection,
            SectorStrengthFeaturesRequest(
                source=signal_source,
                population_source=signal_source,
                namespace="trend_slope_sector",
            ),
        )
        leadership_features = build_long_leadership_features(
            ctx.connection,
            LongLeadershipFeaturesRequest(
                source=signal_source,
                sector_features=sector_features,
                namespace="trend_slope_leadership",
                leadership_windows=_LEADERSHIP_WINDOWS,
            ),
        )
        long_features = build_long_scaffold_features(
            ctx.connection,
            LongScaffoldFeaturesRequest(
                source=signal_source,
                leadership_features=leadership_features,
                short_scaffold_features=short_features,
                namespace="trend_slope_features",
            ),
        )
        rolling_features = build_rolling_trend_features(
            ctx.connection,
            RollingTrendFeaturesRequest(
                source=signal_source,
                price_history=relations.price_history,
                namespace="trend_slope_rolling",
            ),
        )
        composed = compose_daily_ranking_signal_features(
            ctx.connection,
            source=signal_source,
            features=(long_features, rolling_features),
            namespace="trend_slope",
        )
        cohort = materialize_daily_ranking_signal_cohort(
            ctx.connection,
            relations,
            source=composed,
            name="trend_slope_signals",
        )
        evaluated = attach_daily_ranking_outcomes(
            ctx.connection,
            cohort,
            relations,
            name="trend_slope_outcomes",
        )
        _create_trend_slope_panel(
            ctx.connection,
            source_name=evaluated.name,
        )
        observation_count = int(
            ctx.connection.execute(
                "SELECT count(*) FROM ranking_trend_slope_panel"
            ).fetchone()[0]
        )
        result = RankingTrendSlopeEvidenceResult(
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
            coverage_diagnostics_df=_build_coverage_diagnostics_df(ctx.connection),
            technical_condition_evidence_df=_build_technical_condition_evidence_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            fixed_vs_slope_conflict_df=_build_fixed_vs_slope_conflict_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            long_candidate_trend_slope_evidence_df=(
                _build_long_candidate_trend_slope_evidence_df(
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


def write_ranking_trend_slope_evidence_bundle(
    result: RankingTrendSlopeEvidenceResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=RANKING_TREND_SLOPE_EVIDENCE_EXPERIMENT_ID,
        module="src.domains.analytics.ranking_trend_slope_evidence",
        function="run_ranking_trend_slope_evidence_research",
        params={
            "horizons": list(result.horizons),
            "market_scopes": list(result.market_scopes),
            "min_observations": result.min_observations,
            "severe_loss_threshold_pct": result.severe_loss_threshold_pct,
            "required_tables": list(result.required_tables),
            "slope_windows": list(_SLOPE_WINDOWS),
            "ma_slope_lags": list(_MA_SLOPE_LAGS),
            "high_r2_threshold": _HIGH_R2_THRESHOLD,
        },
        db_path=result.db_path,
        analysis_start_date=result.analysis_start_date,
        analysis_end_date=result.analysis_end_date,
        result_metadata={
            "source_mode": str(result.source_mode),
            "source_detail": result.source_detail,
            "market_source": result.market_source,
            "observation_count": result.observation_count,
            "price_lr_definition": (
                "rolling OLS on log(close); slope pct is exp(slope*(window-1))-1"
            ),
            "ma_slope_definition": "moving_average(today) / moving_average(lag) - 1",
            "primary_outcome": "forward_close_excess_return_{horizon}d_pct",
        },
        result_tables={
            "coverage_diagnostics_df": result.coverage_diagnostics_df,
            "technical_condition_evidence_df": result.technical_condition_evidence_df,
            "fixed_vs_slope_conflict_df": result.fixed_vs_slope_conflict_df,
            "long_candidate_trend_slope_evidence_df": (
                result.long_candidate_trend_slope_evidence_df
            ),
            "observation_sample_df": result.observation_sample_df,
        },
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def build_summary_markdown(result: RankingTrendSlopeEvidenceResult) -> str:
    parts = [
        "# Ranking Trend Slope Evidence",
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
        f"- high_r2_threshold: `{_HIGH_R2_THRESHOLD}`",
        "",
        "## Coverage Diagnostics",
        "",
        _top_rows_for_markdown(result.coverage_diagnostics_df, limit=80),
        "",
        "## Technical Condition Evidence",
        "",
        _top_rows_for_markdown(result.technical_condition_evidence_df, limit=220),
        "",
        "## Fixed vs Slope Conflict",
        "",
        _top_rows_for_markdown(result.fixed_vs_slope_conflict_df, limit=180),
        "",
        "## Long Candidate Trend Slope Evidence",
        "",
        _top_rows_for_markdown(
            result.long_candidate_trend_slope_evidence_df,
            limit=260,
        ),
    ]
    return "\n".join(parts).rstrip() + "\n"


def _assert_required_tables(conn: Any) -> None:
    missing = [table for table in _REQUIRED_TABLES if not table_exists(conn, table)]
    if missing:
        raise ValueError(
            f"market.duckdb is missing required tables: {', '.join(missing)}"
        )


def _create_trend_slope_panel(
    conn: Any,
    *,
    source_name: str,
) -> None:
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranking_trend_slope_panel AS
        SELECT
            r.*,
            r.forecast_per_percentile AS forward_per_percentile,
            CASE
                WHEN r.recent_return_20d_pct > 0 THEN 'fixed20_pos'
                WHEN r.recent_return_20d_pct < 0 THEN 'fixed20_neg'
                ELSE 'fixed20_flat'
            END AS fixed_20d_sign_bucket,
            CASE
                WHEN r.recent_return_60d_pct > 0 THEN 'fixed60_pos'
                WHEN r.recent_return_60d_pct < 0 THEN 'fixed60_neg'
                ELSE 'fixed60_flat'
            END AS fixed_60d_sign_bucket,
            CASE
                WHEN r.price_lr_slope_20_pct > 0 THEN 'lr20_pos'
                WHEN r.price_lr_slope_20_pct < 0 THEN 'lr20_neg'
                ELSE 'lr20_flat'
            END AS lr20_sign_bucket,
            CASE
                WHEN r.price_lr_slope_60_pct > 0 THEN 'lr60_pos'
                WHEN r.price_lr_slope_60_pct < 0 THEN 'lr60_neg'
                ELSE 'lr60_flat'
            END AS lr60_sign_bucket,
            CASE
                WHEN r.recent_return_20d_pct > 0
                 AND r.recent_return_60d_pct > 0
                    THEN 'fixed_20d_pos_60d_pos'
                WHEN r.recent_return_20d_pct > 0
                 AND r.recent_return_60d_pct < 0
                    THEN 'fixed_20d_pos_60d_neg'
                WHEN r.recent_return_20d_pct < 0
                 AND r.recent_return_60d_pct > 0
                    THEN 'fixed_20d_neg_60d_pos'
                WHEN r.recent_return_20d_pct < 0
                 AND r.recent_return_60d_pct < 0
                    THEN 'fixed_20d_neg_60d_neg'
                ELSE 'fixed_price_action_unclassified'
            END AS fixed_price_action_bucket,
            CASE
                WHEN r.price_lr_slope_20_pct > 0
                 AND r.price_lr_slope_60_pct > 0
                    THEN 'lr20_pos_lr60_pos'
                WHEN r.price_lr_slope_20_pct > 0
                 AND r.price_lr_slope_60_pct < 0
                    THEN 'lr20_pos_lr60_neg'
                WHEN r.price_lr_slope_20_pct < 0
                 AND r.price_lr_slope_60_pct > 0
                    THEN 'lr20_neg_lr60_pos'
                WHEN r.price_lr_slope_20_pct < 0
                 AND r.price_lr_slope_60_pct < 0
                    THEN 'lr20_neg_lr60_neg'
                ELSE 'lr_price_action_unclassified'
            END AS lr_price_action_bucket,
            CASE
                WHEN r.sma20_slope_5d_pct > 0
                 AND r.sma60_slope_20d_pct > 0
                    THEN 'sma20_slope_pos_sma60_slope_pos'
                WHEN r.sma20_slope_5d_pct < 0
                 AND r.sma60_slope_20d_pct > 0
                    THEN 'sma20_slope_neg_sma60_slope_pos'
                WHEN r.sma20_slope_5d_pct > 0
                 AND r.sma60_slope_20d_pct < 0
                    THEN 'sma20_slope_pos_sma60_slope_neg'
                WHEN r.sma20_slope_5d_pct < 0
                 AND r.sma60_slope_20d_pct < 0
                    THEN 'sma20_slope_neg_sma60_slope_neg'
                ELSE 'sma_slope_unclassified'
            END AS sma_slope_bucket,
            CASE
                WHEN r.ema20_slope_5d_pct > 0
                 AND r.ema60_slope_20d_pct > 0
                    THEN 'ema20_slope_pos_ema60_slope_pos'
                WHEN r.ema20_slope_5d_pct < 0
                 AND r.ema60_slope_20d_pct > 0
                    THEN 'ema20_slope_neg_ema60_slope_pos'
                WHEN r.ema20_slope_5d_pct > 0
                 AND r.ema60_slope_20d_pct < 0
                    THEN 'ema20_slope_pos_ema60_slope_neg'
                WHEN r.ema20_slope_5d_pct < 0
                 AND r.ema60_slope_20d_pct < 0
                    THEN 'ema20_slope_neg_ema60_slope_neg'
                ELSE 'ema_slope_unclassified'
            END AS ema_slope_bucket,
            CASE
                WHEN r.recent_return_20d_pct > 0 AND r.price_lr_slope_20_pct > 0
                    THEN 'fixed20_pos_lr20_pos'
                WHEN r.recent_return_20d_pct > 0 AND r.price_lr_slope_20_pct <= 0
                    THEN 'fixed20_pos_lr20_neg'
                WHEN r.recent_return_20d_pct <= 0 AND r.price_lr_slope_20_pct > 0
                    THEN 'fixed20_neg_lr20_pos'
                ELSE 'fixed20_neg_lr20_neg'
            END AS fixed20_lr20_conflict_bucket,
            CASE
                WHEN r.recent_return_60d_pct > 0 AND r.price_lr_slope_60_pct > 0
                    THEN 'fixed60_pos_lr60_pos'
                WHEN r.recent_return_60d_pct > 0 AND r.price_lr_slope_60_pct <= 0
                    THEN 'fixed60_pos_lr60_neg'
                WHEN r.recent_return_60d_pct <= 0 AND r.price_lr_slope_60_pct > 0
                    THEN 'fixed60_neg_lr60_pos'
                ELSE 'fixed60_neg_lr60_neg'
            END AS fixed60_lr60_conflict_bucket
        FROM {source_name} r
        WHERE r.recent_return_20d_pct IS NOT NULL
          AND r.recent_return_60d_pct IS NOT NULL
          AND r.price_lr_slope_20_pct IS NOT NULL
          AND r.price_lr_slope_60_pct IS NOT NULL
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
            median(recent_return_20d_pct) AS median_recent_return_20d_pct,
            median(recent_return_60d_pct) AS median_recent_return_60d_pct,
            median(price_lr_slope_20_pct) AS median_price_lr_slope_20_pct,
            median(price_lr_slope_60_pct) AS median_price_lr_slope_60_pct,
            median(price_lr_r2_20) AS median_price_lr_r2_20,
            median(price_lr_r2_60) AS median_price_lr_r2_60,
            median(sma20_slope_5d_pct) AS median_sma20_slope_5d_pct,
            median(sma60_slope_20d_pct) AS median_sma60_slope_20d_pct,
            median(ema20_slope_5d_pct) AS median_ema20_slope_5d_pct,
            median(ema60_slope_20d_pct) AS median_ema60_slope_20d_pct,
            avg(CASE
                WHEN fixed_price_action_bucket = 'fixed_20d_pos_60d_pos'
                 AND lr_price_action_bucket = 'lr20_pos_lr60_pos' THEN 1.0
                WHEN fixed_price_action_bucket = 'fixed_20d_pos_60d_neg'
                 AND lr_price_action_bucket = 'lr20_pos_lr60_neg' THEN 1.0
                WHEN fixed_price_action_bucket = 'fixed_20d_neg_60d_pos'
                 AND lr_price_action_bucket = 'lr20_neg_lr60_pos' THEN 1.0
                WHEN fixed_price_action_bucket = 'fixed_20d_neg_60d_neg'
                 AND lr_price_action_bucket = 'lr20_neg_lr60_neg' THEN 1.0
                ELSE 0.0
            END) * 100.0 AS fixed_lr_exact_label_match_rate_pct,
            avg(CASE WHEN fixed20_lr20_conflict_bucket = 'fixed20_pos_lr20_pos'
                THEN 1.0 ELSE 0.0 END) * 100.0
                AS fixed20_pos_lr20_pos_rate_pct,
            avg(CASE WHEN fixed20_lr20_conflict_bucket = 'fixed20_pos_lr20_neg'
                THEN 1.0 ELSE 0.0 END) * 100.0
                AS fixed20_pos_lr20_neg_rate_pct,
            avg(CASE WHEN fixed60_lr60_conflict_bucket = 'fixed60_pos_lr60_pos'
                THEN 1.0 ELSE 0.0 END) * 100.0
                AS fixed60_pos_lr60_pos_rate_pct,
            avg(CASE WHEN fixed60_lr60_conflict_bucket = 'fixed60_pos_lr60_neg'
                THEN 1.0 ELSE 0.0 END) * 100.0
                AS fixed60_pos_lr60_neg_rate_pct
        FROM ranking_trend_slope_panel
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
            VALUES {condition_values_sql(_TECHNICAL_CONDITIONS)}
        ) AS technical_condition(
            technical_condition,
            technical_condition_order,
            technical_condition_matches
        )
    """
    for horizon in horizons:
        frames.append(
            aggregate_lateral_conditions(
                conn,
                source_name="ranking_trend_slope_panel",
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
                extra_metric_sql=_trend_metric_sql(),
            )
        )
    return concat_sorted(frames, columns=_technical_condition_columns())


def _build_fixed_vs_slope_conflict_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for horizon in horizons:
        for conflict_window, bucket_column in (
            (20, "fixed20_lr20_conflict_bucket"),
            (60, "fixed60_lr60_conflict_bucket"),
        ):
            frame = conn.execute(
                f"""
                SELECT
                    market_scope,
                    {int(horizon)} AS horizon,
                    {int(conflict_window)} AS conflict_window,
                    {bucket_column} AS conflict_bucket,
                    count(*) AS observation_count,
                    count(DISTINCT code) AS code_count,
                    count(DISTINCT date) AS date_count,
                    avg(forward_close_excess_return_{int(horizon)}d_pct)
                        AS mean_forward_excess_return_pct,
                    median(forward_close_excess_return_{int(horizon)}d_pct)
                        AS median_forward_excess_return_pct,
                    quantile_cont(
                        forward_close_excess_return_{int(horizon)}d_pct,
                        0.1
                    ) AS p10_forward_excess_return_pct,
                    avg(CASE
                        WHEN forward_close_excess_return_{int(horizon)}d_pct > 0
                        THEN 1.0 ELSE 0.0 END) * 100.0 AS excess_win_rate_pct,
                    avg(CASE
                        WHEN forward_close_excess_return_{int(horizon)}d_pct <= ?
                        THEN 1.0 ELSE 0.0 END) * 100.0 AS severe_loss_rate_pct,
                    median(recent_return_{int(conflict_window)}d_pct)
                        AS median_recent_return_pct,
                    median(price_lr_slope_{int(conflict_window)}_pct)
                        AS median_price_lr_slope_pct,
                    median(price_lr_r2_{int(conflict_window)})
                        AS median_price_lr_r2
                FROM ranking_trend_slope_panel
                WHERE forward_close_excess_return_{int(horizon)}d_pct IS NOT NULL
                GROUP BY market_scope, {bucket_column}
                HAVING count(*) >= ?
                ORDER BY market_scope, conflict_window, conflict_bucket
                """,
                [float(severe_loss_threshold_pct), int(min_observations)],
            ).fetchdf()
            frames.append(frame)
    return concat_sorted(frames, columns=_fixed_vs_slope_conflict_columns())


def _build_long_candidate_trend_slope_evidence_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    lateral_sql = f"""
        CROSS JOIN LATERAL (
            VALUES {condition_values_sql(_LONG_SCAFFOLDS)}
        ) AS long_scaffold(
            long_scaffold,
            long_scaffold_order,
            long_scaffold_matches
        )
        CROSS JOIN LATERAL (
            VALUES {condition_values_sql(_TECHNICAL_CONDITIONS)}
        ) AS technical_condition(
            technical_condition,
            technical_condition_order,
            technical_condition_matches
        )
    """
    for horizon in horizons:
        frames.append(
            aggregate_lateral_conditions(
                conn,
                source_name="ranking_trend_slope_panel",
                lateral_sql=lateral_sql,
                match_condition=(
                    "long_scaffold.long_scaffold_matches "
                    "AND technical_condition.technical_condition_matches"
                ),
                group_select_sql=(
                    "'long_candidate_trend_slope' AS condition_family,\n"
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
                extra_metric_sql=deep_dive_metric_sql() + _trend_metric_sql(),
            )
        )
    return concat_sorted(
        frames,
        columns=_long_candidate_trend_slope_columns(),
    )


def _trend_metric_sql() -> str:
    return """,
            median(recent_return_20d_pct) AS median_recent_return_20d_pct,
            median(recent_return_60d_pct) AS median_recent_return_60d_pct,
            median(price_lr_slope_20_pct) AS median_price_lr_slope_20_pct,
            median(price_lr_slope_60_pct) AS median_price_lr_slope_60_pct,
            median(price_lr_r2_20) AS median_price_lr_r2_20,
            median(price_lr_r2_60) AS median_price_lr_r2_60,
            median(sma20_slope_5d_pct) AS median_sma20_slope_5d_pct,
            median(sma60_slope_20d_pct) AS median_sma60_slope_20d_pct,
            median(ema20_slope_5d_pct) AS median_ema20_slope_5d_pct,
            median(ema60_slope_20d_pct) AS median_ema60_slope_20d_pct"""


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
            fixed_20d_sign_bucket,
            fixed_60d_sign_bucket,
            lr20_sign_bucket,
            lr60_sign_bucket,
            fixed_price_action_bucket,
            lr_price_action_bucket,
            sma_slope_bucket,
            ema_slope_bucket,
            fixed20_lr20_conflict_bucket,
            fixed60_lr60_conflict_bucket,
            recent_return_20d_pct,
            recent_return_60d_pct,
            price_lr_slope_20_pct,
            price_lr_slope_60_pct,
            price_lr_r2_20,
            price_lr_r2_60,
            sma20_slope_5d_pct,
            sma20_slope_20d_pct,
            sma60_slope_5d_pct,
            sma60_slope_20d_pct,
            ema20_slope_5d_pct,
            ema20_slope_20d_pct,
            ema60_slope_5d_pct,
            ema60_slope_20d_pct,
            liquidity_residual_z,
            valuation_signal,
            {horizon_columns}
        FROM ranking_trend_slope_panel
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


def _technical_condition_columns() -> list[str]:
    return [
        "condition_family",
        "technical_condition",
        "technical_condition_order",
        "horizon",
        "market_scope",
        *aggregate_metric_columns(),
        *_trend_metric_columns(),
    ]


def _trend_metric_columns() -> list[str]:
    return [
        "median_recent_return_20d_pct",
        "median_recent_return_60d_pct",
        "median_price_lr_slope_20_pct",
        "median_price_lr_slope_60_pct",
        "median_price_lr_r2_20",
        "median_price_lr_r2_60",
        "median_sma20_slope_5d_pct",
        "median_sma60_slope_20d_pct",
        "median_ema20_slope_5d_pct",
        "median_ema60_slope_20d_pct",
    ]


def _fixed_vs_slope_conflict_columns() -> list[str]:
    return [
        "market_scope",
        "horizon",
        "conflict_window",
        "conflict_bucket",
        "observation_count",
        "code_count",
        "date_count",
        "mean_forward_excess_return_pct",
        "median_forward_excess_return_pct",
        "p10_forward_excess_return_pct",
        "excess_win_rate_pct",
        "severe_loss_rate_pct",
        "median_recent_return_pct",
        "median_price_lr_slope_pct",
        "median_price_lr_r2",
    ]


def _long_candidate_trend_slope_columns() -> list[str]:
    return [
        "condition_family",
        "long_scaffold",
        "long_scaffold_order",
        "technical_condition",
        "technical_condition_order",
        "horizon",
        "market_scope",
        *aggregate_metric_columns(),
        *deep_dive_metric_columns(),
        *_trend_metric_columns(),
    ]
