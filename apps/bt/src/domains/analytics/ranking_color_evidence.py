"""Fast research for Ranking valuation/liquidity color evidence."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
import re
from typing import Any, Iterable, Sequence, cast

import pandas as pd

from src.domains.analytics.daily_ranking_research_base import (
    DailyRankingPanelRequest,
    DailyRankingResearchRelations,
    MarketScope,
    RelationRef,
    SignalDerivedColumn,
    SignalExpression,
    assert_daily_ranking_research_tables,
    attach_daily_ranking_outcomes,
    build_daily_ranking_research_base,
    deprecated_create_daily_ranking_observation_panel,
    deprecated_offset_daily_ranking_calendar_date,
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

# Deprecated cross-experiment compatibility exports. Ranking Color itself does not
# call these; Tasks 8-10 remove them after the remaining consumers migrate.
_create_observation_panel = deprecated_create_daily_ranking_observation_panel
_assert_required_tables = assert_daily_ranking_research_tables
_normalize_market_scopes = normalize_daily_ranking_market_scopes
_offset_calendar_date = deprecated_offset_daily_ranking_calendar_date

RANKING_COLOR_EVIDENCE_EXPERIMENT_ID = "market-behavior/ranking-color-evidence"
DEFAULT_HORIZONS: tuple[int, ...] = (20,)
DEFAULT_MARKET_SCOPES: tuple[str, ...] = ("prime",)
DEFAULT_MIN_OBSERVATIONS = 500
DEFAULT_SEVERE_LOSS_THRESHOLD_PCT = -10.0
DEFAULT_OBSERVATION_SAMPLE_LIMIT = 10_000
PANEL_FEATURE_WARMUP_CALENDAR_DAYS = 365
_REQUIRED_TABLES: tuple[str, ...] = (
    "stock_data_raw",
    "stock_adjustment_bases",
    "stock_adjustment_basis_segments",
    "stock_master_daily",
    "topix_data",
    "indices_data",
    "daily_valuation",
)
_NIKKEI_SYNTHETIC_INDEX_CODE = "N225_UNDERPX"
_VALUATION_FEATURES: tuple[str, ...] = (
    "per",
    "forward_per",
    "forward_p_op",
    "pbr",
)
_VALUATION_BUCKETS: tuple[str, ...] = (
    "cheapest_10pct",
    "cheapest_20pct",
    "middle_60pct",
    "expensive_20pct",
    "expensive_10pct",
)
_FORWARD_PER_POP_BUCKETS: tuple[str, ...] = (
    "low_forward_per_low_forward_p_op",
    "low_forward_per_high_forward_p_op",
    "low_forward_per_only",
    "low_forward_p_op_only",
    "neither_extreme",
)
_PER_RELATION_FEATURES: tuple[str, ...] = (
    "forward_per_to_per_ratio",
    "forward_p_op_to_per_ratio",
)
_LOW_PER_SCOPES: tuple[tuple[str, float], ...] = (
    ("low_per_10pct", 0.10),
    ("low_per_20pct", 0.20),
)
_RELATION_RATIO_BUCKETS: tuple[tuple[str, str], ...] = (
    ("ratio_lte_0_8", "{column} <= 0.8"),
    ("ratio_0_8_to_1_0", "{column} > 0.8 AND {column} <= 1.0"),
    ("ratio_1_0_to_1_25", "{column} > 1.0 AND {column} <= 1.25"),
    ("ratio_gt_1_25", "{column} > 1.25"),
)
_LIQUIDITY_REGIMES: tuple[str, ...] = (
    "neutral_rerating",
    "crowded_rerating",
    "distribution_stress",
    "stale_liquidity",
    "neutral",
)
_TOPIX_REGIMES: tuple[tuple[str, str], ...] = (
    (
        "all_topix",
        "topix_recent_return_20d_pct IS NOT NULL AND topix_recent_return_60d_pct IS NOT NULL",
    ),
    (
        "topix_20d_ge_0_60d_ge_0",
        "topix_recent_return_20d_pct >= 0 AND topix_recent_return_60d_pct >= 0",
    ),
    (
        "topix_20d_lt_0_60d_gt_0",
        "topix_recent_return_20d_pct < 0 AND topix_recent_return_60d_pct > 0",
    ),
    (
        "topix_20d_lt_0",
        "topix_recent_return_20d_pct < 0",
    ),
    (
        "topix_60d_lt_0",
        "topix_recent_return_60d_pct < 0",
    ),
)
_RERATING_VALUE_CONDITIONS: tuple[tuple[str, str], ...] = (
    ("all_value", "TRUE"),
    (
        "no_value_confirmation",
        "NOT (pbr_percentile <= 0.2 OR "
        "(per_percentile <= 0.2 AND forward_per_to_per_ratio <= 1.0))",
    ),
    ("low_pbr20", "pbr_percentile <= 0.2"),
    ("low_fwd_per20", "forward_per_percentile <= 0.2"),
    (
        "low_pbr20_low_fwd_per20",
        "pbr_percentile <= 0.2 AND forward_per_percentile <= 0.2",
    ),
    (
        "low_per20_fwdper_per_lte_0_8",
        "per_percentile <= 0.2 AND forward_per_to_per_ratio <= 0.8",
    ),
    (
        "medium_value_confirmation",
        "pbr_percentile <= 0.2 OR "
        "(per_percentile <= 0.2 AND forward_per_to_per_ratio <= 1.0)",
    ),
    (
        "strong_value_confirmation",
        "(pbr_percentile <= 0.2 AND forward_per_percentile <= 0.2) OR "
        "(per_percentile <= 0.2 AND forward_per_to_per_ratio <= 0.8)",
    ),
)
_OVERVALUED_CONDITIONS: tuple[tuple[str, str], ...] = (
    (
        "all_positive_per_pbr",
        "per_percentile IS NOT NULL AND pbr_percentile IS NOT NULL",
    ),
    (
        "high_per20_high_pbr20",
        "per_percentile >= 0.8 AND pbr_percentile >= 0.8",
    ),
    (
        "high_forward_per20_high_pbr20",
        "forward_per_percentile >= 0.8 AND pbr_percentile >= 0.8",
    ),
    (
        "high_per_or_pbr20",
        "per_percentile >= 0.8 OR pbr_percentile >= 0.8",
    ),
    (
        "not_high_per_pbr20",
        "per_percentile < 0.8 AND pbr_percentile < 0.8",
    ),
)
_MARKET_CAP_ABS_BUCKETS: tuple[tuple[str, str], ...] = (
    ("cap_lt_10bn", "market_cap_bil_jpy > 0 AND market_cap_bil_jpy < 10"),
    ("cap_10_50bn", "market_cap_bil_jpy >= 10 AND market_cap_bil_jpy < 50"),
    ("cap_50_200bn", "market_cap_bil_jpy >= 50 AND market_cap_bil_jpy < 200"),
    ("cap_200bn_1tn", "market_cap_bil_jpy >= 200 AND market_cap_bil_jpy < 1000"),
    ("cap_ge_1tn", "market_cap_bil_jpy >= 1000"),
)
_ADV60_ABS_BUCKETS: tuple[tuple[str, str], ...] = (
    (
        "adv_lt_10mn",
        "med_adv60_sessions >= 60 AND med_adv60_jpy > 0 AND med_adv60_jpy < 10000000",
    ),
    (
        "adv_10_50mn",
        "med_adv60_sessions >= 60 AND med_adv60_jpy >= 10000000 "
        "AND med_adv60_jpy < 50000000",
    ),
    (
        "adv_50_300mn",
        "med_adv60_sessions >= 60 AND med_adv60_jpy >= 50000000 "
        "AND med_adv60_jpy < 300000000",
    ),
    (
        "adv_300mn_1bn",
        "med_adv60_sessions >= 60 AND med_adv60_jpy >= 300000000 "
        "AND med_adv60_jpy < 1000000000",
    ),
    (
        "adv_ge_1bn",
        "med_adv60_sessions >= 60 AND med_adv60_jpy >= 1000000000",
    ),
)
_RANKING_COLOR_OUTPUT_STRING_COLUMNS = frozenset(
    {
        "code",
        "company_name",
        "market",
        "market_code",
        "scale_category",
        "liquidity_regime",
        "market_scope",
        "condition_family",
        "valuation_feature",
        "ranking_color_bucket",
        "evidence_tier",
        "relation_feature",
        "relation_bucket",
        "per_scope",
        "relation_level_bucket",
        "interaction_bucket",
        "topix_regime",
        "value_condition",
        "good_scope",
        "chain_condition",
        "ui_color",
        "trend_condition",
        "valuation_condition",
        "market_cap_abs_bucket",
        "adv60_abs_bucket",
    }
)
_RANKING_COLOR_OUTPUT_INTEGER_COLUMNS = frozenset(
    {"horizon", "observation_count", "code_count", "date_count", "trend_window"}
)


@dataclass(frozen=True)
class RankingColorEvidenceResult:
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
    ranking_color_evidence_df: pd.DataFrame
    per_relation_evidence_df: pd.DataFrame
    low_per_relation_evidence_df: pd.DataFrame
    low_per_relation_level_evidence_df: pd.DataFrame
    forward_per_pop_interaction_df: pd.DataFrame
    liquidity_regime_evidence_df: pd.DataFrame
    topix_regime_liquidity_value_evidence_df: pd.DataFrame
    rerating_good_valuation_chain_df: pd.DataFrame
    liquidity_color_long_trend_evidence_df: pd.DataFrame
    overvalued_size_liquidity_interaction_df: pd.DataFrame


def run_ranking_color_evidence_research(
    db_path: str | Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    market_scopes: Sequence[str] = DEFAULT_MARKET_SCOPES,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
    severe_loss_threshold_pct: float = DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    observation_sample_limit: int = DEFAULT_OBSERVATION_SAMPLE_LIMIT,
) -> RankingColorEvidenceResult:
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
        snapshot_prefix="ranking-color-evidence-",
    ) as ctx:
        assert_daily_ranking_research_tables(ctx.connection)
        market_source = "stock_master_daily_exact_date"
        relations = build_daily_ranking_research_base(
            ctx.connection,
            DailyRankingPanelRequest(
                namespace="ranking_color",
                analysis_start_date=(
                    None if start_date is None else date.fromisoformat(start_date)
                ),
                analysis_end_date=(
                    None if end_date is None else date.fromisoformat(end_date)
                ),
                horizons=resolved_horizons,
                market_scopes=cast(tuple[MarketScope, ...], resolved_market_scopes),
                include_liquidity=True,
                percentile_features=(
                    "forecast_per_to_per_ratio",
                    "forecast_p_op_to_per_ratio",
                    "forecast_operating_profit_growth_ratio",
                    "per_to_fop_growth_ratio",
                    "forecast_per_to_fop_growth_ratio",
                ),
            ),
        )
        panel_cohort = _freeze_full_signal_relation(
            ctx.connection,
            relations,
            source=relations.signal_panel,
            name="panel",
        )
        evaluated_panel = attach_daily_ranking_outcomes(
            ctx.connection,
            panel_cohort,
            relations,
            name="panel",
        )
        ranked_cohort = _freeze_full_signal_relation(
            ctx.connection,
            relations,
            source=relations.ranked_signals,
            name="ranked",
        )
        evaluated_ranked = attach_daily_ranking_outcomes(
            ctx.connection,
            ranked_cohort,
            relations,
            name="ranked",
        )
        valuation_bucket_sources = _freeze_valuation_bucket_sources(
            ctx.connection,
            relations,
        )
        if relations.liquidity_ranked_signals is None:
            raise RuntimeError(
                "Ranking Color requires the liquidity-ranked signal relation"
            )
        liquidity_cohort = _freeze_full_signal_relation(
            ctx.connection,
            relations,
            source=relations.liquidity_ranked_signals,
            name="liquidity_ranked",
        )
        evaluated_liquidity = attach_daily_ranking_outcomes(
            ctx.connection,
            liquidity_cohort,
            relations,
            name="liquidity_ranked",
        )
        panel_source = _create_ranking_color_evaluated_view(
            ctx.connection,
            evaluated_panel,
            name="ranking_color_evaluated_panel",
        )
        ranked_source = _create_ranking_color_evaluated_view(
            ctx.connection,
            evaluated_ranked,
            name="ranking_color_evaluated_ranked",
        )
        liquidity_source = _create_ranking_color_evaluated_view(
            ctx.connection,
            evaluated_liquidity,
            name="ranking_color_evaluated_liquidity_ranked",
        )
        observation_count = evaluated_panel.row_count
        result = RankingColorEvidenceResult(
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
                source_name=ranked_source,
                limit=observation_sample_limit,
            ),
            coverage_diagnostics_df=_build_coverage_diagnostics_df(
                ctx.connection,
                source_name=panel_source,
            ),
            ranking_color_evidence_df=_build_ranking_color_evidence_df(
                ctx.connection,
                source_name=ranked_source,
                bucket_sources=valuation_bucket_sources,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            per_relation_evidence_df=_build_per_relation_evidence_df(
                ctx.connection,
                source_name=ranked_source,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            low_per_relation_evidence_df=_build_low_per_relation_evidence_df(
                ctx.connection,
                source_name=ranked_source,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            low_per_relation_level_evidence_df=(
                _build_low_per_relation_level_evidence_df(
                    ctx.connection,
                    source_name=ranked_source,
                    horizons=resolved_horizons,
                    min_observations=min_observations,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
            ),
            forward_per_pop_interaction_df=_build_forward_per_pop_interaction_df(
                ctx.connection,
                source_name=ranked_source,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            liquidity_regime_evidence_df=_build_liquidity_regime_evidence_df(
                ctx.connection,
                source_name=liquidity_source,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            topix_regime_liquidity_value_evidence_df=(
                _build_topix_regime_liquidity_value_evidence_df(
                    ctx.connection,
                    source_name=liquidity_source,
                    horizons=resolved_horizons,
                    min_observations=min_observations,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
            ),
            rerating_good_valuation_chain_df=(
                _build_rerating_good_valuation_chain_df(
                    ctx.connection,
                    source_name=ranked_source,
                    horizons=resolved_horizons,
                    min_observations=min_observations,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
            ),
            liquidity_color_long_trend_evidence_df=(
                _build_liquidity_color_long_trend_evidence_df(
                    ctx.connection,
                    source_name=liquidity_source,
                    horizons=resolved_horizons,
                    min_observations=min_observations,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
            ),
            overvalued_size_liquidity_interaction_df=(
                _build_overvalued_size_liquidity_interaction_df(
                    ctx.connection,
                    source_name=ranked_source,
                    horizons=resolved_horizons,
                    min_observations=min_observations,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
            ),
        )
    return result


def _freeze_full_signal_relation(
    conn: Any,
    relations: DailyRankingResearchRelations,
    *,
    source: RelationRef,
    name: str,
) -> RelationRef:
    """Freeze Ranking Color's complete signal-time population before evaluation."""

    return materialize_daily_ranking_signal_cohort(
        conn,
        relations,
        source=source,
        name=f"ranking_color_{name}",
    )


def _freeze_valuation_bucket_sources(
    conn: Any,
    relations: DailyRankingResearchRelations,
) -> dict[tuple[str, str], str]:
    """Freeze each valuation membership before attaching any forward outcome."""

    sources: dict[tuple[str, str], str] = {}
    for feature in _VALUATION_FEATURES:
        canonical_feature = {
            "forward_per": "forecast_per",
            "forward_p_op": "forecast_p_op",
        }.get(feature, feature)
        percentile_column = f"{canonical_feature}_percentile"
        for bucket in _VALUATION_BUCKETS:
            cohort = materialize_daily_ranking_signal_cohort(
                conn,
                relations,
                source=relations.ranked_signals,
                name=f"ranking_color_valuation_{feature}_{bucket}",
                predicate=SignalExpression(
                    _valuation_bucket_condition(percentile_column, bucket),
                    referenced_columns=(percentile_column,),
                ),
                derived_columns=(
                    SignalDerivedColumn(
                        "valuation_feature",
                        SignalExpression(f"'{feature}'"),
                        "VARCHAR",
                    ),
                    SignalDerivedColumn(
                        "ranking_color_bucket",
                        SignalExpression(f"'{bucket}'"),
                        "VARCHAR",
                    ),
                ),
            )
            evaluated = attach_daily_ranking_outcomes(
                conn,
                cohort,
                relations,
                name=f"ranking_color_valuation_{feature}_{bucket}",
            )
            sources[(feature, bucket)] = _create_ranking_color_evaluated_view(
                conn,
                evaluated,
                name=f"ranking_color_evaluated_valuation_{feature}_{bucket}",
            )
    return sources


_RANKING_COLOR_LEGACY_NAMES: dict[str, str] = {
    "forecast_per": "forward_per",
    "forecast_p_op": "forward_p_op",
    "forecast_per_to_per_ratio": "forward_per_to_per_ratio",
    "forecast_p_op_to_per_ratio": "forward_p_op_to_per_ratio",
    "forecast_per_to_fop_growth_ratio": "forward_per_to_fop_growth_ratio",
    "forecast_per_percentile": "forward_per_percentile",
    "forecast_p_op_percentile": "forward_p_op_percentile",
    "forecast_per_to_per_ratio_percentile": "forward_per_to_per_ratio_percentile",
    "forecast_p_op_to_per_ratio_percentile": "forward_p_op_to_per_ratio_percentile",
    "forecast_per_to_fop_growth_ratio_percentile": (
        "forward_per_to_fop_growth_ratio_percentile"
    ),
}


def _create_ranking_color_evaluated_view(
    conn: Any,
    relation: RelationRef,
    *,
    name: str,
) -> str:
    """Expose Ranking Color's existing internal aliases after outcomes attach."""

    select_columns = [
        f"{column} AS {_RANKING_COLOR_LEGACY_NAMES[column]}"
        if column in _RANKING_COLOR_LEGACY_NAMES
        else column
        for column in relation.columns
    ]
    horizons = sorted(
        {
            int(match.group(1))
            for column in relation.columns
            if (match := re.fullmatch(r"forward_close_return_(\d+)d_pct", column))
        }
    )
    for horizon in horizons:
        select_columns.extend(
            (
                f"forward_close_return_{horizon}d_pct "
                f"- forward_close_excess_return_{horizon}d_pct "
                f"AS topix_close_return_{horizon}d_pct",
                f"forward_close_return_{horizon}d_pct "
                f"- forward_close_n225_excess_return_{horizon}d_pct "
                f"AS n225_close_return_{horizon}d_pct",
            )
        )
    conn.execute(
        f"CREATE OR REPLACE TEMP VIEW {name} AS "
        f"SELECT {', '.join(select_columns)} FROM {relation.name}"
    )
    return name


def write_ranking_color_evidence_bundle(
    result: RankingColorEvidenceResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    result_tables = {
        "observation_sample_df": result.observation_sample_df,
        "coverage_diagnostics_df": result.coverage_diagnostics_df,
        "ranking_color_evidence_df": result.ranking_color_evidence_df,
        "per_relation_evidence_df": result.per_relation_evidence_df,
        "low_per_relation_evidence_df": result.low_per_relation_evidence_df,
        "low_per_relation_level_evidence_df": (
            result.low_per_relation_level_evidence_df
        ),
        "forward_per_pop_interaction_df": result.forward_per_pop_interaction_df,
        "liquidity_regime_evidence_df": result.liquidity_regime_evidence_df,
        "topix_regime_liquidity_value_evidence_df": (
            result.topix_regime_liquidity_value_evidence_df
        ),
        "rerating_good_valuation_chain_df": result.rerating_good_valuation_chain_df,
        "liquidity_color_long_trend_evidence_df": (
            result.liquidity_color_long_trend_evidence_df
        ),
        "overvalued_size_liquidity_interaction_df": (
            result.overvalued_size_liquidity_interaction_df
        ),
    }
    return write_research_bundle(
        experiment_id=RANKING_COLOR_EVIDENCE_EXPERIMENT_ID,
        module="src.domains.analytics.ranking_color_evidence",
        function="run_ranking_color_evidence_research",
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
        },
        result_tables={
            name: _ranking_color_bundle_frame(frame)
            for name, frame in result_tables.items()
        },
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def _ranking_color_output_schema(frame: pd.DataFrame) -> tuple[tuple[str, str], ...]:
    """Return the exact DuckDB bundle schema for a Ranking Color frame."""

    return tuple(
        (
            column,
            "DATE"
            if column == "date"
            else "VARCHAR"
            if column in _RANKING_COLOR_OUTPUT_STRING_COLUMNS
            else "BIGINT"
            if column in _RANKING_COLOR_OUTPUT_INTEGER_COLUMNS
            or column.endswith("_order")
            else "DOUBLE",
        )
        for column in frame.columns
    )


def _ranking_color_bundle_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Cast even empty/all-null columns to the declared bundle SQL types."""

    typed = frame.copy()
    for column, sql_type in _ranking_color_output_schema(frame):
        if sql_type == "DATE":
            typed[column] = pd.to_datetime(typed[column]).dt.date
        elif sql_type == "VARCHAR":
            typed[column] = typed[column].astype("string")
        elif sql_type == "BIGINT":
            typed[column] = pd.to_numeric(typed[column]).astype("Int64")
        else:
            typed[column] = pd.to_numeric(typed[column]).astype("float64")
    return typed


def build_summary_markdown(result: RankingColorEvidenceResult) -> str:
    parts = [
        "# Ranking Color Evidence",
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
        "## Ranking Color Evidence",
        "",
        _top_rows_for_markdown(result.ranking_color_evidence_df, limit=60),
        "",
        "## Forward Valuation vs PER Relation Evidence",
        "",
        _top_rows_for_markdown(result.per_relation_evidence_df, limit=40),
        "",
        "## Low PER x Forward Valuation Relation Evidence",
        "",
        _top_rows_for_markdown(result.low_per_relation_evidence_df, limit=80),
        "",
        "## Low PER x Forward Valuation Relation Level Evidence",
        "",
        _top_rows_for_markdown(
            result.low_per_relation_level_evidence_df,
            limit=80,
        ),
        "",
        "## Forward PER x Forward P/OP Interaction",
        "",
        _top_rows_for_markdown(result.forward_per_pop_interaction_df, limit=40),
        "",
        "## Liquidity Regime Evidence",
        "",
        _top_rows_for_markdown(result.liquidity_regime_evidence_df, limit=40),
        "",
        "## TOPIX Regime x Liquidity x Value Evidence",
        "",
        _top_rows_for_markdown(
            result.topix_regime_liquidity_value_evidence_df,
            limit=120,
        ),
        "",
        "## Rerating Good x PER > Fwd PER > Fwd P/OP",
        "",
        _top_rows_for_markdown(
            result.rerating_good_valuation_chain_df,
            limit=80,
        ),
        "",
        "## Liquidity Color x Long Trend Evidence",
        "",
        _top_rows_for_markdown(
            result.liquidity_color_long_trend_evidence_df,
            limit=120,
        ),
        "",
        "## Overvalued x Size x Liquidity Interaction",
        "",
        _top_rows_for_markdown(
            result.overvalued_size_liquidity_interaction_df,
            limit=160,
        ),
    ]
    return "\n".join(parts).rstrip() + "\n"


def _build_ranking_color_evidence_df(
    conn: Any,
    *,
    source_name: str,
    bucket_sources: dict[tuple[str, str], str] | None = None,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for feature in _VALUATION_FEATURES:
        percentile_column = f"{feature}_percentile"
        for bucket in _VALUATION_BUCKETS:
            bucket_source = (
                source_name
                if bucket_sources is None
                else bucket_sources[(feature, bucket)]
            )
            condition = (
                _valuation_bucket_condition(percentile_column, bucket)
                if bucket_sources is None
                else "TRUE"
            )
            for horizon in horizons:
                frames.append(
                    _aggregate_condition(
                        conn,
                        source_name=bucket_source,
                        condition=condition,
                        condition_fields={
                            "condition_family": "ranking_color_percentile_evidence",
                            "valuation_feature": feature,
                            "ranking_color_bucket": bucket,
                            "ranking_color_bucket_order": _VALUATION_BUCKETS.index(
                                bucket
                            ),
                            "evidence_tier": _evidence_tier(bucket),
                            "horizon": int(horizon),
                        },
                        return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                        min_observations=min_observations,
                        severe_loss_threshold_pct=severe_loss_threshold_pct,
                    )
                )
    return _concat_sorted(frames, columns=_ranking_color_evidence_columns())


def _build_forward_per_pop_interaction_df(
    conn: Any,
    *,
    source_name: str,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for bucket in _FORWARD_PER_POP_BUCKETS:
        for horizon in horizons:
            frames.append(
                _aggregate_condition(
                    conn,
                    source_name=source_name,
                    condition=_forward_per_pop_condition(bucket),
                    condition_fields={
                        "condition_family": "forward_per_forward_p_op_relative",
                        "interaction_bucket": bucket,
                        "interaction_bucket_order": _FORWARD_PER_POP_BUCKETS.index(
                            bucket
                        ),
                        "horizon": int(horizon),
                    },
                    return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                    min_observations=min_observations,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
            )
    return _concat_sorted(frames, columns=_forward_per_pop_columns())


def _build_per_relation_evidence_df(
    conn: Any,
    *,
    source_name: str,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for feature in _PER_RELATION_FEATURES:
        percentile_column = f"{feature}_percentile"
        for bucket in _VALUATION_BUCKETS:
            for horizon in horizons:
                frames.append(
                    _aggregate_condition(
                        conn,
                        source_name=source_name,
                        condition=_valuation_bucket_condition(
                            percentile_column, bucket
                        ),
                        condition_fields={
                            "condition_family": "forward_valuation_per_relation",
                            "relation_feature": feature,
                            "relation_bucket": bucket,
                            "relation_bucket_order": _VALUATION_BUCKETS.index(bucket),
                            "evidence_tier": _evidence_tier(bucket),
                            "horizon": int(horizon),
                        },
                        return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                        min_observations=min_observations,
                        severe_loss_threshold_pct=severe_loss_threshold_pct,
                    )
                )
    return _concat_sorted(frames, columns=_per_relation_columns())


def _build_low_per_relation_evidence_df(
    conn: Any,
    *,
    source_name: str,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for per_scope, per_threshold in _LOW_PER_SCOPES:
        for feature in _PER_RELATION_FEATURES:
            percentile_column = f"{feature}_percentile"
            for bucket in _VALUATION_BUCKETS:
                relation_condition = _valuation_bucket_condition(
                    percentile_column,
                    bucket,
                )
                for horizon in horizons:
                    frames.append(
                        _aggregate_condition(
                            conn,
                            source_name=source_name,
                            condition=(
                                f"per_percentile <= {per_threshold} "
                                f"AND {relation_condition}"
                            ),
                            condition_fields={
                                "condition_family": "low_per_forward_relation",
                                "per_scope": per_scope,
                                "relation_feature": feature,
                                "relation_bucket": bucket,
                                "relation_bucket_order": _VALUATION_BUCKETS.index(
                                    bucket
                                ),
                                "evidence_tier": _evidence_tier(bucket),
                                "horizon": int(horizon),
                            },
                            return_column=(
                                f"forward_close_excess_return_{int(horizon)}d_pct"
                            ),
                            min_observations=min_observations,
                            severe_loss_threshold_pct=severe_loss_threshold_pct,
                        )
                    )
    return _concat_sorted(frames, columns=_low_per_relation_columns())


def _build_low_per_relation_level_evidence_df(
    conn: Any,
    *,
    source_name: str,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for per_scope, per_threshold in _LOW_PER_SCOPES:
        for feature in _PER_RELATION_FEATURES:
            for bucket_order, (bucket, condition_template) in enumerate(
                _RELATION_RATIO_BUCKETS
            ):
                relation_condition = condition_template.format(column=feature)
                for horizon in horizons:
                    frames.append(
                        _aggregate_condition(
                            conn,
                            source_name=source_name,
                            condition=(
                                f"per_percentile <= {per_threshold} "
                                f"AND {relation_condition}"
                            ),
                            condition_fields={
                                "condition_family": "low_per_forward_relation_level",
                                "per_scope": per_scope,
                                "relation_feature": feature,
                                "relation_level_bucket": bucket,
                                "relation_level_bucket_order": bucket_order,
                                "horizon": int(horizon),
                            },
                            return_column=(
                                f"forward_close_excess_return_{int(horizon)}d_pct"
                            ),
                            min_observations=min_observations,
                            severe_loss_threshold_pct=severe_loss_threshold_pct,
                        )
                    )
    return _concat_sorted(frames, columns=_low_per_relation_level_columns())


def _build_liquidity_regime_evidence_df(
    conn: Any,
    *,
    source_name: str,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for regime in _LIQUIDITY_REGIMES:
        for horizon in horizons:
            frames.append(
                _aggregate_condition(
                    conn,
                    source_name=source_name,
                    condition=f"liquidity_scope = '{regime}'",
                    condition_fields={
                        "condition_family": "liquidity_regime",
                        "liquidity_regime": regime,
                        "horizon": int(horizon),
                    },
                    return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                    min_observations=min_observations,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
            )
    return _concat_sorted(frames, columns=_liquidity_regime_columns())


def _build_topix_regime_liquidity_value_evidence_df(
    conn: Any,
    *,
    source_name: str,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for topix_order, (topix_regime, topix_condition) in enumerate(_TOPIX_REGIMES):
        for regime in _LIQUIDITY_REGIMES:
            for value_order, (value_condition, value_sql) in enumerate(
                _RERATING_VALUE_CONDITIONS
            ):
                for horizon in horizons:
                    frames.append(
                        _aggregate_condition(
                            conn,
                            source_name=source_name,
                            condition=(
                                f"({topix_condition}) "
                                f"AND liquidity_scope = '{regime}' "
                                f"AND ({value_sql})"
                            ),
                            condition_fields={
                                "condition_family": "topix_regime_liquidity_value",
                                "topix_regime": topix_regime,
                                "topix_regime_order": topix_order,
                                "liquidity_regime": regime,
                                "value_condition": value_condition,
                                "value_condition_order": value_order,
                                "horizon": int(horizon),
                            },
                            return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                            min_observations=min_observations,
                            severe_loss_threshold_pct=severe_loss_threshold_pct,
                        )
                    )
    return _concat_sorted(frames, columns=_topix_regime_liquidity_value_columns())


def _build_liquidity_color_long_trend_evidence_df(
    conn: Any,
    *,
    source_name: str,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    trend_conditions: tuple[tuple[int, str, str], ...] = (
        (120, "trend_positive", "recent_return_120d_pct > 0"),
        (120, "trend_non_positive", "recent_return_120d_pct <= 0"),
        (150, "trend_positive", "recent_return_150d_pct > 0"),
        (150, "trend_non_positive", "recent_return_150d_pct <= 0"),
    )
    for regime_order, (regime, ui_colors) in enumerate(_liquidity_color_sql().items()):
        for color_order, (ui_color, color_sql) in enumerate(ui_colors.items()):
            for trend_order, (trend_window, trend_condition, trend_sql) in enumerate(
                trend_conditions
            ):
                for horizon in horizons:
                    frames.append(
                        _aggregate_condition(
                            conn,
                            source_name=source_name,
                            condition=(
                                f"liquidity_scope = '{regime}' "
                                f"AND ({color_sql}) "
                                f"AND ({trend_sql})"
                            ),
                            condition_fields={
                                "condition_family": "liquidity_color_long_trend",
                                "liquidity_regime": regime,
                                "liquidity_regime_order": regime_order,
                                "ui_color": ui_color,
                                "ui_color_order": color_order,
                                "trend_window": int(trend_window),
                                "trend_condition": trend_condition,
                                "trend_condition_order": trend_order,
                                "horizon": int(horizon),
                            },
                            return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                            min_observations=min_observations,
                            severe_loss_threshold_pct=severe_loss_threshold_pct,
                        )
                    )
    return _concat_sorted(frames, columns=_liquidity_color_long_trend_columns())


def _build_rerating_good_valuation_chain_df(
    conn: Any,
    *,
    source_name: str,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    neutral_good = _neutral_rerating_good_condition()
    crowded_good = _crowded_rerating_good_condition()
    all_good = f"(({neutral_good}) OR ({crowded_good}))"
    chain = _per_forward_per_forward_p_op_chain_condition()
    scopes = (
        ("all_rerating_good", all_good, 0),
        ("neutral_rerating_good", neutral_good, 1),
        ("crowded_rerating_good", crowded_good, 2),
    )
    chain_conditions = (
        ("all_good", "TRUE", 0),
        ("per_gt_fwdper_gt_fwdpop", chain, 1),
        ("good_without_chain", f"NOT coalesce(({chain}), FALSE)", 2),
    )
    for good_scope, good_condition, good_order in scopes:
        for chain_condition, chain_sql, chain_order in chain_conditions:
            for horizon in horizons:
                frames.append(
                    _aggregate_condition(
                        conn,
                        source_name=source_name,
                        condition=f"({good_condition}) AND ({chain_sql})",
                        condition_fields={
                            "condition_family": "rerating_good_forward_valuation_chain",
                            "good_scope": good_scope,
                            "good_scope_order": good_order,
                            "chain_condition": chain_condition,
                            "chain_condition_order": chain_order,
                            "horizon": int(horizon),
                        },
                        return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                        min_observations=min_observations,
                        severe_loss_threshold_pct=severe_loss_threshold_pct,
                    )
                )
    return _concat_sorted(frames, columns=_rerating_good_valuation_chain_columns())


def _build_overvalued_size_liquidity_interaction_df(
    conn: Any,
    *,
    source_name: str,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for value_order, (valuation_condition, valuation_sql) in enumerate(
        _OVERVALUED_CONDITIONS
    ):
        for cap_order, (market_cap_bucket, market_cap_sql) in enumerate(
            _MARKET_CAP_ABS_BUCKETS
        ):
            for adv_order, (adv60_bucket, adv60_sql) in enumerate(_ADV60_ABS_BUCKETS):
                for horizon in horizons:
                    frames.append(
                        _aggregate_condition(
                            conn,
                            source_name=source_name,
                            condition=(
                                f"({valuation_sql}) "
                                f"AND ({market_cap_sql}) "
                                f"AND ({adv60_sql})"
                            ),
                            condition_fields={
                                "condition_family": (
                                    "overvalued_size_liquidity_interaction"
                                ),
                                "valuation_condition": valuation_condition,
                                "valuation_condition_order": value_order,
                                "market_cap_abs_bucket": market_cap_bucket,
                                "market_cap_abs_bucket_order": cap_order,
                                "adv60_abs_bucket": adv60_bucket,
                                "adv60_abs_bucket_order": adv_order,
                                "horizon": int(horizon),
                            },
                            return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                            min_observations=min_observations,
                            severe_loss_threshold_pct=severe_loss_threshold_pct,
                        )
                    )
    return _concat_sorted(
        frames,
        columns=_overvalued_size_liquidity_interaction_columns(),
    )


def _aggregate_condition(
    conn: Any,
    *,
    source_name: str,
    condition: str,
    condition_fields: dict[str, Any],
    return_column: str,
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    frame = conn.execute(
        f"""
        SELECT
            market_scope,
            count(*) AS observation_count,
            count(DISTINCT code) AS code_count,
            count(DISTINCT date) AS date_count,
            avg({return_column}) AS mean_forward_excess_return_pct,
            median({return_column}) AS median_forward_excess_return_pct,
            quantile_cont({return_column}, 0.10) AS p10_forward_excess_return_pct,
            quantile_cont({return_column}, 0.25) AS p25_forward_excess_return_pct,
            quantile_cont({return_column}, 0.75) AS p75_forward_excess_return_pct,
            quantile_cont({return_column}, 0.90) AS p90_forward_excess_return_pct,
            avg(CASE WHEN {return_column} > 0 THEN 1.0 ELSE 0.0 END) * 100.0
                AS win_rate_pct,
            avg(CASE WHEN {return_column} <= ? THEN 1.0 ELSE 0.0 END) * 100.0
                AS severe_loss_rate_pct,
            median(recent_return_20d_pct) AS median_recent_return_20d_pct,
            median(recent_return_60d_pct) AS median_recent_return_60d_pct,
            median(recent_return_120d_pct) AS median_recent_return_120d_pct,
            median(recent_return_150d_pct) AS median_recent_return_150d_pct,
            median(topix_recent_return_20d_pct) AS median_topix_recent_return_20d_pct,
            median(topix_recent_return_60d_pct) AS median_topix_recent_return_60d_pct,
            median(med_adv60_jpy) / 1000000.0 AS median_med_adv60_mil_jpy,
            median(market_cap_bil_jpy) AS median_market_cap_bil_jpy,
            median(free_float_market_cap_jpy) / 1000000000.0
                AS median_free_float_market_cap_bil_jpy,
            median(liquidity_residual_z) AS median_liquidity_residual_z,
            median(per) AS median_per,
            median(forward_per) AS median_forward_per,
            median(pbr) AS median_pbr,
            median(p_op) AS median_p_op,
            median(forward_p_op) AS median_forward_p_op,
            median(forward_per_to_per_ratio) AS median_forward_per_to_per_ratio,
            median(forward_p_op_to_per_ratio) AS median_forward_p_op_to_per_ratio,
            median(per_percentile) AS median_per_percentile,
            median(forward_per_percentile) AS median_forward_per_percentile,
            median(forward_p_op_percentile) AS median_forward_p_op_percentile,
            median(pbr_percentile) AS median_pbr_percentile,
            median(forward_per_to_per_ratio_percentile)
                AS median_forward_per_to_per_ratio_percentile,
            median(forward_p_op_to_per_ratio_percentile)
                AS median_forward_p_op_to_per_ratio_percentile
        FROM {source_name}
        WHERE {condition}
          AND {return_column} IS NOT NULL
        GROUP BY market_scope
        HAVING count(*) >= ?
        """,
        [float(severe_loss_threshold_pct), int(min_observations)],
    ).fetchdf()
    if frame.empty:
        return frame
    for column, value in condition_fields.items():
        frame[column] = value
    ordered = [*condition_fields.keys(), "market_scope"]
    ordered.extend(_aggregate_metric_columns())
    return frame.reindex(columns=ordered)


def _build_coverage_diagnostics_df(
    conn: Any,
    *,
    source_name: str,
) -> pd.DataFrame:
    return conn.execute(
        f"""
        SELECT
            market,
            count(*) AS observation_count,
            count(DISTINCT code) AS code_count,
            count(DISTINCT date) AS date_count,
            avg(CASE WHEN per > 0 THEN 1.0 ELSE 0.0 END) * 100.0 AS per_coverage_pct,
            avg(CASE WHEN forward_per > 0 THEN 1.0 ELSE 0.0 END) * 100.0
                AS forward_per_coverage_pct,
            avg(CASE WHEN forward_p_op > 0 THEN 1.0 ELSE 0.0 END) * 100.0
                AS forward_p_op_coverage_pct,
            avg(CASE WHEN pbr > 0 THEN 1.0 ELSE 0.0 END) * 100.0 AS pbr_coverage_pct,
            avg(CASE WHEN liquidity_residual_z IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100.0
                AS liquidity_residual_z_coverage_pct
        FROM {source_name}
        GROUP BY market
        ORDER BY market
        """
    ).fetchdf()


def _query_observation_sample_df(
    conn: Any,
    *,
    source_name: str,
    limit: int,
) -> pd.DataFrame:
    return conn.execute(
        f"""
        SELECT
            date,
            code,
            company_name,
            market,
            market_code,
            scale_category,
            close,
            recent_return_20d_pct,
            recent_return_60d_pct,
            recent_return_120d_pct,
            recent_return_150d_pct,
            topix_recent_return_20d_pct,
            topix_recent_return_60d_pct,
            n225_recent_return_20d_pct,
            n225_recent_return_60d_pct,
            n225_close_return_20d_pct,
            med_adv60_jpy / 1000000.0 AS med_adv60_mil_jpy,
            free_float_market_cap_jpy / 1000000000.0 AS free_float_market_cap_bil_jpy,
            liquidity_residual_z,
            liquidity_regime,
            per,
            per_percentile,
            forward_per,
            forward_per_percentile,
            forward_per_to_per_ratio,
            forward_per_to_per_ratio_percentile,
            pbr,
            pbr_percentile,
            p_op,
            forward_p_op,
            forward_p_op_percentile,
            forward_p_op_to_per_ratio,
            forward_p_op_to_per_ratio_percentile,
            forecast_operating_profit_growth_ratio,
            forecast_operating_profit_growth_ratio_percentile,
            forecast_operating_profit_growth_pct,
            per_to_fop_growth_ratio,
            per_to_fop_growth_ratio_percentile,
            forward_per_to_fop_growth_ratio,
            forward_per_to_fop_growth_ratio_percentile,
            market_cap_bil_jpy,
            forward_close_excess_return_20d_pct,
            forward_close_n225_excess_return_20d_pct
        FROM {source_name}
        ORDER BY date, code
        LIMIT ?
        """,
        [int(limit)],
    ).fetchdf()


def _valuation_bucket_condition(percentile_column: str, bucket: str) -> str:
    if bucket == "cheapest_10pct":
        return f"{percentile_column} <= 0.1"
    if bucket == "cheapest_20pct":
        return f"{percentile_column} > 0.1 AND {percentile_column} <= 0.2"
    if bucket == "middle_60pct":
        return f"{percentile_column} > 0.2 AND {percentile_column} < 0.8"
    if bucket == "expensive_20pct":
        return f"{percentile_column} >= 0.8 AND {percentile_column} < 0.9"
    if bucket == "expensive_10pct":
        return f"{percentile_column} >= 0.9"
    raise ValueError(f"unsupported valuation bucket: {bucket}")


def _forward_per_pop_condition(bucket: str) -> str:
    low_forward_per = "forward_per_percentile <= 0.2"
    low_forward_p_op = "forward_p_op_percentile <= 0.2"
    high_forward_p_op = "forward_p_op_percentile >= 0.8"
    if bucket == "low_forward_per_low_forward_p_op":
        return f"{low_forward_per} AND {low_forward_p_op}"
    if bucket == "low_forward_per_high_forward_p_op":
        return f"{low_forward_per} AND {high_forward_p_op}"
    if bucket == "low_forward_per_only":
        return f"{low_forward_per} AND NOT ({low_forward_p_op}) AND NOT ({high_forward_p_op})"
    if bucket == "low_forward_p_op_only":
        return f"NOT ({low_forward_per}) AND {low_forward_p_op}"
    if bucket == "neither_extreme":
        return f"NOT ({low_forward_per}) AND NOT ({low_forward_p_op}) AND NOT ({high_forward_p_op})"
    raise ValueError(f"unsupported forward PER/P-OP bucket: {bucket}")


def _liquidity_color_sql() -> dict[str, dict[str, str]]:
    strong_value = (
        "(pbr_percentile <= 0.2 AND forward_per_percentile <= 0.2) "
        "OR (per_percentile <= 0.2 AND forward_per_to_per_ratio <= 0.8)"
    )
    neutral_green = "per_percentile <= 0.2 AND forward_per_to_per_ratio <= 0.8"
    crowded_green = strong_value
    medium_value = (
        "pbr_percentile <= 0.2 "
        "OR (per_percentile <= 0.2 AND forward_per_to_per_ratio <= 1.0)"
    )
    return {
        "crowded_rerating": {
            "green": f"({crowded_green})",
            "blue": f"({medium_value}) AND NOT ({crowded_green})",
        },
        "neutral_rerating": {
            "green": f"({neutral_green})",
            "blue": f"NOT ({neutral_green})",
        },
    }


def _neutral_rerating_good_condition() -> str:
    return (
        "liquidity_regime = 'neutral_rerating' "
        "AND ("
        "(pbr_percentile <= 0.2 AND forward_per_percentile <= 0.2) "
        "OR (per_percentile <= 0.2 AND forward_per_to_per_ratio <= 0.8)"
        ")"
    )


def _crowded_rerating_good_condition() -> str:
    return (
        "liquidity_regime = 'crowded_rerating' "
        "AND ("
        "(pbr_percentile <= 0.2 AND forward_per_percentile <= 0.2) "
        "OR (per_percentile <= 0.2 AND forward_per_to_per_ratio <= 0.8) "
        "OR pbr_percentile <= 0.2 "
        "OR (per_percentile <= 0.2 AND forward_per_to_per_ratio <= 1.0)"
        ")"
    )


def _per_forward_per_forward_p_op_chain_condition() -> str:
    return (
        "per > 0 "
        "AND forward_per > 0 "
        "AND forward_p_op > 0 "
        "AND per > forward_per "
        "AND forward_per > forward_p_op"
    )


def _evidence_tier(bucket: str) -> str:
    return {
        "cheapest_10pct": "excellent",
        "cheapest_20pct": "good",
        "middle_60pct": "neutral",
        "expensive_20pct": "bad",
        "expensive_10pct": "very_bad",
    }[bucket]


def _validate_params(
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
    observation_sample_limit: int,
) -> None:
    if not horizons or any(horizon <= 0 for horizon in horizons):
        raise ValueError("horizons must be positive")
    if min_observations <= 0:
        raise ValueError("min_observations must be positive")
    if severe_loss_threshold_pct >= 0:
        raise ValueError("severe_loss_threshold_pct must be negative")
    if observation_sample_limit <= 0:
        raise ValueError("observation_sample_limit must be positive")


def _concat_sorted(
    frames: Sequence[pd.DataFrame], *, columns: Sequence[str]
) -> pd.DataFrame:
    non_empty = [frame for frame in frames if not frame.empty]
    if not non_empty:
        return pd.DataFrame(columns=list(columns))
    frame = pd.concat(non_empty, ignore_index=True)
    return frame.reindex(columns=list(columns))


def _aggregate_metric_columns() -> list[str]:
    return [
        "observation_count",
        "code_count",
        "date_count",
        "mean_forward_excess_return_pct",
        "median_forward_excess_return_pct",
        "p10_forward_excess_return_pct",
        "p25_forward_excess_return_pct",
        "p75_forward_excess_return_pct",
        "p90_forward_excess_return_pct",
        "win_rate_pct",
        "severe_loss_rate_pct",
        "median_recent_return_20d_pct",
        "median_recent_return_60d_pct",
        "median_recent_return_120d_pct",
        "median_recent_return_150d_pct",
        "median_topix_recent_return_20d_pct",
        "median_topix_recent_return_60d_pct",
        "median_med_adv60_mil_jpy",
        "median_market_cap_bil_jpy",
        "median_free_float_market_cap_bil_jpy",
        "median_liquidity_residual_z",
        "median_per",
        "median_forward_per",
        "median_pbr",
        "median_p_op",
        "median_forward_p_op",
        "median_forward_per_to_per_ratio",
        "median_forward_p_op_to_per_ratio",
        "median_per_percentile",
        "median_forward_per_percentile",
        "median_forward_p_op_percentile",
        "median_pbr_percentile",
        "median_forward_per_to_per_ratio_percentile",
        "median_forward_p_op_to_per_ratio_percentile",
    ]


def _ranking_color_evidence_columns() -> list[str]:
    return [
        "condition_family",
        "valuation_feature",
        "ranking_color_bucket",
        "ranking_color_bucket_order",
        "evidence_tier",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
    ]


def _forward_per_pop_columns() -> list[str]:
    return [
        "condition_family",
        "interaction_bucket",
        "interaction_bucket_order",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
    ]


def _per_relation_columns() -> list[str]:
    return [
        "condition_family",
        "relation_feature",
        "relation_bucket",
        "relation_bucket_order",
        "evidence_tier",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
    ]


def _low_per_relation_columns() -> list[str]:
    return [
        "condition_family",
        "per_scope",
        "relation_feature",
        "relation_bucket",
        "relation_bucket_order",
        "evidence_tier",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
    ]


def _low_per_relation_level_columns() -> list[str]:
    return [
        "condition_family",
        "per_scope",
        "relation_feature",
        "relation_level_bucket",
        "relation_level_bucket_order",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
    ]


def _liquidity_regime_columns() -> list[str]:
    return [
        "condition_family",
        "liquidity_regime",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
    ]


def _topix_regime_liquidity_value_columns() -> list[str]:
    return [
        "condition_family",
        "topix_regime",
        "topix_regime_order",
        "liquidity_regime",
        "value_condition",
        "value_condition_order",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
    ]


def _liquidity_color_long_trend_columns() -> list[str]:
    return [
        "condition_family",
        "liquidity_regime",
        "liquidity_regime_order",
        "ui_color",
        "ui_color_order",
        "trend_window",
        "trend_condition",
        "trend_condition_order",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
    ]


def _rerating_good_valuation_chain_columns() -> list[str]:
    return [
        "condition_family",
        "good_scope",
        "good_scope_order",
        "chain_condition",
        "chain_condition_order",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
    ]


def _overvalued_size_liquidity_interaction_columns() -> list[str]:
    return [
        "condition_family",
        "valuation_condition",
        "valuation_condition_order",
        "market_cap_abs_bucket",
        "market_cap_abs_bucket_order",
        "adv60_abs_bucket",
        "adv60_abs_bucket_order",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
    ]
