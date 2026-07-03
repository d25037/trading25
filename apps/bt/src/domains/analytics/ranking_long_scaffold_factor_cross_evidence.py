"""Factor-cross evidence for Daily Ranking long-side scaffolds."""

from __future__ import annotations

from dataclasses import dataclass
from itertools import combinations
from pathlib import Path
from typing import Any, Iterable, Sequence

import pandas as pd

from src.domains.analytics.atr_expansion_forward_response import (
    _create_observation_panel as _create_atr_observation_panel,
)
from src.domains.analytics.daily_ranking_research_base import (
    DAILY_RANKING_RESEARCH_RANKED_TABLE,
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
    _concat_sorted,
    _deep_dive_metric_columns,
    _deep_dive_metric_sql,
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
    open_readonly_analysis_connection,
)
from src.domains.analytics.research_bundle import ResearchBundleInfo, write_research_bundle

RANKING_LONG_SCAFFOLD_FACTOR_CROSS_EXPERIMENT_ID = (
    "market-behavior/ranking-long-scaffold-factor-cross-evidence"
)
DEFAULT_HORIZONS: tuple[int, ...] = (5, 20, 60)
DEFAULT_MARKET_SCOPES: tuple[str, ...] = ("prime",)
DEFAULT_MIN_OBSERVATIONS = 100
DEFAULT_SEVERE_LOSS_THRESHOLD_PCT = -10.0
DEFAULT_OBSERVATION_SAMPLE_LIMIT = 10_000
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
_LIQUIDITY_Z_0_TO_2_RERATING_SQL = (
    "liquidity_residual_z > 0.0 AND liquidity_residual_z < 2.0 "
    "AND recent_return_20d_pct >= 0.0 AND recent_return_60d_pct >= 0.0"
)
_LIQUIDITY_Z_MINUS1_TO_2_RERATING_SQL = (
    "liquidity_residual_z > -1.0 AND liquidity_residual_z < 2.0 "
    "AND recent_return_20d_pct >= 0.0 AND recent_return_60d_pct >= 0.0"
)
_FACTOR_CONDITIONS: tuple[tuple[str, str], ...] = (
    ("liquidity_z_0_to_2_rerating", _LIQUIDITY_Z_0_TO_2_RERATING_SQL),
    (
        "liquidity_z_minus1_to_2_rerating",
        _LIQUIDITY_Z_MINUS1_TO_2_RERATING_SQL,
    ),
    ("fwd_op_op_gt_1_2", "forecast_operating_profit_growth_ratio > 1.2"),
    ("good_fwd_per", "forward_per_to_per_ratio <= 0.8"),
)
_LIQUIDITY_FACTOR_CONDITION_NAMES = {
    "liquidity_z_0_to_2_rerating",
    "liquidity_z_minus1_to_2_rerating",
}
_FACTOR_COMBOS: tuple[tuple[str, str], ...] = tuple(
    (
        "__".join(name for name, _condition in combo),
        " AND ".join(f"({condition})" for _name, condition in combo),
    )
    for size in range(2, len(_FACTOR_CONDITIONS) + 1)
    for combo in combinations(_FACTOR_CONDITIONS, size)
    if len(_LIQUIDITY_FACTOR_CONDITION_NAMES.intersection(name for name, _ in combo))
    <= 1
)
_LIQUIDITY_Z_0_TO_2_RERATING_PANEL_SQL = (
    "r.liquidity_residual_z > 0.0 AND r.liquidity_residual_z < 2.0 "
    "AND r.recent_return_20d_pct >= 0.0 AND r.recent_return_60d_pct >= 0.0"
)
_LIQUIDITY_Z_MINUS1_TO_2_RERATING_PANEL_SQL = (
    "r.liquidity_residual_z > -1.0 AND r.liquidity_residual_z < 2.0 "
    "AND r.recent_return_20d_pct >= 0.0 AND r.recent_return_60d_pct >= 0.0"
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
        "neutral_deep_value_long_hybrid_atr20_accel",
        "liquidity_regime = 'neutral_rerating' "
        "AND valuation_signal = 'strong_value_confirmation' "
        "AND long_hybrid_leadership_score >= 0.799999 "
        "AND atr20_acceleration_ex_overheat_flag",
    ),
    (
        "z_0_to_2_deep_value",
        f"({_LIQUIDITY_Z_0_TO_2_RERATING_SQL}) "
        "AND valuation_signal = 'strong_value_confirmation'",
    ),
    (
        "z_0_to_2_deep_value_long_hybrid_atr20_accel",
        f"({_LIQUIDITY_Z_0_TO_2_RERATING_SQL}) "
        "AND valuation_signal = 'strong_value_confirmation' "
        "AND long_hybrid_leadership_score >= 0.799999 "
        "AND atr20_acceleration_ex_overheat_flag",
    ),
    (
        "z_minus1_to_2_deep_value",
        f"({_LIQUIDITY_Z_MINUS1_TO_2_RERATING_SQL}) "
        "AND valuation_signal = 'strong_value_confirmation'",
    ),
    (
        "z_minus1_to_2_deep_value_long_hybrid_atr20_accel",
        f"({_LIQUIDITY_Z_MINUS1_TO_2_RERATING_SQL}) "
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
        "z_0_to_2_deep_value_sector_strong_atr20_accel",
        f"({_LIQUIDITY_Z_0_TO_2_RERATING_SQL}) "
        "AND valuation_signal = 'strong_value_confirmation' "
        "AND sector_strength_bucket = 'sector_strong' "
        "AND atr20_acceleration_ex_overheat_flag",
    ),
    (
        "z_minus1_to_2_deep_value_sector_strong_atr20_accel",
        f"({_LIQUIDITY_Z_MINUS1_TO_2_RERATING_SQL}) "
        "AND valuation_signal = 'strong_value_confirmation' "
        "AND sector_strength_bucket = 'sector_strong' "
        "AND atr20_acceleration_ex_overheat_flag",
    ),
)


@dataclass(frozen=True)
class RankingLongScaffoldFactorCrossEvidenceResult:
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
    long_scaffold_evidence_df: pd.DataFrame
    factor_condition_evidence_df: pd.DataFrame
    long_scaffold_factor_evidence_df: pd.DataFrame
    long_scaffold_factor_combo_evidence_df: pd.DataFrame


def run_ranking_long_scaffold_factor_cross_evidence_research(
    db_path: str | Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    market_scopes: Sequence[str] = DEFAULT_MARKET_SCOPES,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
    severe_loss_threshold_pct: float = DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    observation_sample_limit: int = DEFAULT_OBSERVATION_SAMPLE_LIMIT,
) -> RankingLongScaffoldFactorCrossEvidenceResult:
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
        snapshot_prefix="ranking-long-scaffold-factor-cross-",
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
        _create_factor_panel(ctx.connection)
        observation_count = int(
            ctx.connection.execute(
                "SELECT count(*) FROM ranking_long_scaffold_factor_cross_panel"
            ).fetchone()[0]
        )
        result = RankingLongScaffoldFactorCrossEvidenceResult(
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
                limit=observation_sample_limit,
            ),
            coverage_diagnostics_df=_build_coverage_diagnostics_df(ctx.connection),
            long_scaffold_evidence_df=_build_long_scaffold_evidence_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            factor_condition_evidence_df=_build_factor_condition_evidence_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            long_scaffold_factor_evidence_df=_build_long_scaffold_factor_evidence_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
            ),
            long_scaffold_factor_combo_evidence_df=(
                _build_long_scaffold_factor_combo_evidence_df(
                    ctx.connection,
                    horizons=resolved_horizons,
                    min_observations=min_observations,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
            ),
        )
    return result


def write_ranking_long_scaffold_factor_cross_evidence_bundle(
    result: RankingLongScaffoldFactorCrossEvidenceResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=RANKING_LONG_SCAFFOLD_FACTOR_CROSS_EXPERIMENT_ID,
        module="src.domains.analytics.ranking_long_scaffold_factor_cross_evidence",
        function="run_ranking_long_scaffold_factor_cross_evidence_research",
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
            "factor_conditions": [name for name, _condition in _FACTOR_CONDITIONS],
        },
        result_tables={
            "observation_sample_df": result.observation_sample_df,
            "coverage_diagnostics_df": result.coverage_diagnostics_df,
            "long_scaffold_evidence_df": result.long_scaffold_evidence_df,
            "factor_condition_evidence_df": result.factor_condition_evidence_df,
            "long_scaffold_factor_evidence_df": (
                result.long_scaffold_factor_evidence_df
            ),
            "long_scaffold_factor_combo_evidence_df": (
                result.long_scaffold_factor_combo_evidence_df
            ),
        },
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def build_summary_markdown(
    result: RankingLongScaffoldFactorCrossEvidenceResult,
) -> str:
    parts = [
        "# Ranking Long Scaffold Factor Cross Evidence",
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
        "## Long Scaffold Evidence",
        "",
        _top_rows_for_markdown(result.long_scaffold_evidence_df, limit=120),
        "",
        "## Factor Condition Evidence",
        "",
        _top_rows_for_markdown(result.factor_condition_evidence_df, limit=80),
        "",
        "## Long Scaffold x Factor Condition Evidence",
        "",
        _top_rows_for_markdown(result.long_scaffold_factor_evidence_df, limit=220),
        "",
        "## Long Scaffold x Factor Combo Evidence",
        "",
        _top_rows_for_markdown(result.long_scaffold_factor_combo_evidence_df, limit=260),
    ]
    return "\n".join(parts).rstrip() + "\n"


def _assert_required_tables(conn: Any) -> None:
    missing = [table for table in _REQUIRED_TABLES if not _table_exists(conn, table)]
    if missing:
        raise ValueError(f"market.duckdb is missing required tables: {', '.join(missing)}")


def _create_factor_panel(conn: Any) -> None:
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranking_long_scaffold_factor_cross_panel AS
        SELECT
            r.*,
            l.sector_33_code,
            l.sector_33_name,
            l.sector_strength_bucket,
            l.sector_strength_score,
            l.sector_index_strength_score,
            l.sector_constituent_strength_score,
            l.long_index_leadership_score,
            l.long_constituent_breadth_leadership_score,
            l.long_hybrid_leadership_score,
            l.balanced_sector_strength_bucket_label,
            l.long_hybrid_bucket_label,
            coalesce(l.momentum_20_60_top20_flag, FALSE)
                AS momentum_20_60_top20_flag,
            s.atr20_pct,
            s.atr60_pct,
            s.atr20_to_atr60,
            s.atr20_change_20d_pct,
            coalesce(s.atr20_acceleration, FALSE) AS atr20_acceleration_flag,
            coalesce(
                s.atr20_acceleration
                AND coalesce(r.recent_return_20d_pct, 0.0) < 30.0,
                FALSE
            ) AS atr20_acceleration_ex_overheat_flag,
            coalesce(s.atr20_to_atr60_overheat, FALSE)
                AS atr20_to_atr60_overheat,
            coalesce(s.weak_trend, FALSE) AS weak_trend,
            ({_LIQUIDITY_Z_0_TO_2_RERATING_PANEL_SQL})
                AS liquidity_z_0_to_2_rerating_flag,
            ({_LIQUIDITY_Z_MINUS1_TO_2_RERATING_PANEL_SQL})
                AS liquidity_z_minus1_to_2_rerating_flag,
            r.forecast_operating_profit_growth_ratio > 1.2
                AS fwd_op_op_gt_1_2_flag,
            r.forward_per_to_per_ratio <= 0.8 AS good_fwd_per_flag
        FROM {DAILY_RANKING_RESEARCH_RANKED_TABLE} r
        LEFT JOIN long_sector_leadership_base_panel l
          ON l.code = r.code
         AND l.date = r.date
         AND l.market_scope = r.market_scope
        LEFT JOIN ranking_short_red_feature_panel s
          ON s.code = r.code
         AND s.date = r.date
         AND s.market_scope = r.market_scope
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
            avg(CASE WHEN liquidity_residual_z IS NOT NULL THEN 1.0 ELSE 0.0 END)
                * 100.0 AS liquidity_z_coverage_pct,
            avg(CASE WHEN liquidity_z_0_to_2_rerating_flag THEN 1.0 ELSE 0.0 END)
                * 100.0 AS liquidity_z_0_to_2_rerating_rate_pct,
            avg(CASE WHEN liquidity_z_minus1_to_2_rerating_flag
                THEN 1.0 ELSE 0.0 END) * 100.0
                AS liquidity_z_minus1_to_2_rerating_rate_pct,
            avg(CASE WHEN forecast_operating_profit_growth_ratio IS NOT NULL
                THEN 1.0 ELSE 0.0 END) * 100.0 AS fwd_op_op_coverage_pct,
            avg(CASE WHEN fwd_op_op_gt_1_2_flag THEN 1.0 ELSE 0.0 END)
                * 100.0 AS fwd_op_op_gt_1_2_rate_pct,
            avg(CASE WHEN forward_per_to_per_ratio IS NOT NULL THEN 1.0 ELSE 0.0 END)
                * 100.0 AS fwd_per_per_coverage_pct,
            avg(CASE WHEN good_fwd_per_flag THEN 1.0 ELSE 0.0 END)
                * 100.0 AS good_fwd_per_rate_pct,
            median(liquidity_residual_z) AS median_liquidity_residual_z,
            median(forecast_operating_profit_growth_ratio)
                AS median_forecast_operating_profit_growth_ratio,
            median(forward_per_to_per_ratio) AS median_forward_per_to_per_ratio
        FROM ranking_long_scaffold_factor_cross_panel
        GROUP BY market_scope
        ORDER BY market_scope
        """
    ).fetchdf()


def _build_long_scaffold_evidence_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    scaffold_lateral_sql = f"""
        CROSS JOIN LATERAL (
            VALUES {_condition_values_sql(_LONG_SCAFFOLDS)}
        ) AS long_scaffold(
            long_scaffold,
            long_scaffold_order,
            long_scaffold_matches
        )
    """
    frames: list[pd.DataFrame] = []
    for horizon in horizons:
        frames.append(
            _aggregate_lateral_conditions(
                conn,
                source_name="ranking_long_scaffold_factor_cross_panel",
                lateral_sql=scaffold_lateral_sql,
                match_condition="long_scaffold.long_scaffold_matches",
                group_select_sql=(
                    "'long_scaffold' AS condition_family,\n"
                    "            long_scaffold.long_scaffold,\n"
                    "            long_scaffold.long_scaffold_order,\n"
                    f"            {int(horizon)} AS horizon"
                ),
                group_by_sql=(
                    "long_scaffold.long_scaffold, "
                    "long_scaffold.long_scaffold_order, market_scope"
                ),
                return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
                extra_metric_sql=_deep_dive_metric_sql(),
            )
        )
    return _concat_sorted(frames, columns=_long_scaffold_columns())


def _build_factor_condition_evidence_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    factor_lateral_sql = f"""
        CROSS JOIN LATERAL (
            VALUES {_condition_values_sql(_FACTOR_CONDITIONS)}
        ) AS factor_condition(
            factor_condition,
            factor_condition_order,
            factor_condition_matches
        )
    """
    frames: list[pd.DataFrame] = []
    for horizon in horizons:
        frames.append(
            _aggregate_lateral_conditions(
                conn,
                source_name="ranking_long_scaffold_factor_cross_panel",
                lateral_sql=factor_lateral_sql,
                match_condition="factor_condition.factor_condition_matches",
                group_select_sql=(
                    "'factor_condition' AS condition_family,\n"
                    "            factor_condition.factor_condition,\n"
                    "            factor_condition.factor_condition_order,\n"
                    f"            {int(horizon)} AS horizon"
                ),
                group_by_sql=(
                    "factor_condition.factor_condition, "
                    "factor_condition.factor_condition_order, market_scope"
                ),
                return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
                extra_metric_sql=_deep_dive_metric_sql(),
            )
        )
    return _concat_sorted(frames, columns=_factor_condition_columns())


def _build_long_scaffold_factor_evidence_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    lateral_sql = f"""
        CROSS JOIN LATERAL (
            VALUES {_condition_values_sql(_LONG_SCAFFOLDS)}
        ) AS long_scaffold(
            long_scaffold,
            long_scaffold_order,
            long_scaffold_matches
        )
        CROSS JOIN LATERAL (
            VALUES {_condition_values_sql(_FACTOR_CONDITIONS)}
        ) AS factor_condition(
            factor_condition,
            factor_condition_order,
            factor_condition_matches
        )
    """
    frames: list[pd.DataFrame] = []
    for horizon in horizons:
        frames.append(
            _aggregate_lateral_conditions(
                conn,
                source_name="ranking_long_scaffold_factor_cross_panel",
                lateral_sql=lateral_sql,
                match_condition=(
                    "long_scaffold.long_scaffold_matches "
                    "AND factor_condition.factor_condition_matches"
                ),
                group_select_sql=(
                    "'long_scaffold_factor' AS condition_family,\n"
                    "            long_scaffold.long_scaffold,\n"
                    "            long_scaffold.long_scaffold_order,\n"
                    "            factor_condition.factor_condition,\n"
                    "            factor_condition.factor_condition_order,\n"
                    f"            {int(horizon)} AS horizon"
                ),
                group_by_sql=(
                    "long_scaffold.long_scaffold, "
                    "long_scaffold.long_scaffold_order, "
                    "factor_condition.factor_condition, "
                    "factor_condition.factor_condition_order, market_scope"
                ),
                return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
                extra_metric_sql=_deep_dive_metric_sql(),
            )
        )
    return _concat_sorted(frames, columns=_long_scaffold_factor_columns())


def _build_long_scaffold_factor_combo_evidence_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    lateral_sql = f"""
        CROSS JOIN LATERAL (
            VALUES {_condition_values_sql(_LONG_SCAFFOLDS)}
        ) AS long_scaffold(
            long_scaffold,
            long_scaffold_order,
            long_scaffold_matches
        )
        CROSS JOIN LATERAL (
            VALUES {_condition_values_sql(_FACTOR_COMBOS)}
        ) AS factor_combo(
            factor_combo,
            factor_combo_order,
            factor_combo_matches
        )
    """
    frames: list[pd.DataFrame] = []
    for horizon in horizons:
        frames.append(
            _aggregate_lateral_conditions(
                conn,
                source_name="ranking_long_scaffold_factor_cross_panel",
                lateral_sql=lateral_sql,
                match_condition=(
                    "long_scaffold.long_scaffold_matches "
                    "AND factor_combo.factor_combo_matches"
                ),
                group_select_sql=(
                    "'long_scaffold_factor_combo' AS condition_family,\n"
                    "            long_scaffold.long_scaffold,\n"
                    "            long_scaffold.long_scaffold_order,\n"
                    "            factor_combo.factor_combo,\n"
                    "            factor_combo.factor_combo_order,\n"
                    f"            {int(horizon)} AS horizon"
                ),
                group_by_sql=(
                    "long_scaffold.long_scaffold, "
                    "long_scaffold.long_scaffold_order, "
                    "factor_combo.factor_combo, "
                    "factor_combo.factor_combo_order, market_scope"
                ),
                return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
                extra_metric_sql=_deep_dive_metric_sql(),
            )
        )
    return _concat_sorted(frames, columns=_long_scaffold_factor_combo_columns())


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
            liquidity_z_0_to_2_rerating_flag,
            liquidity_z_minus1_to_2_rerating_flag,
            per,
            per_percentile,
            forward_per,
            forward_per_percentile,
            forward_per_to_per_ratio,
            good_fwd_per_flag,
            pbr,
            pbr_percentile,
            p_op,
            forward_p_op,
            forecast_operating_profit_growth_ratio,
            forecast_operating_profit_growth_pct,
            fwd_op_op_gt_1_2_flag,
            valuation_signal,
            strong_value_confirmation,
            medium_value_confirmation,
            long_hybrid_leadership_score,
            atr20_change_20d_pct,
            atr20_to_atr60,
            atr20_acceleration_ex_overheat_flag,
            sector_strength_bucket,
            forward_close_excess_return_5d_pct,
            forward_close_excess_return_20d_pct
        FROM ranking_long_scaffold_factor_cross_panel
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


def _long_scaffold_columns() -> list[str]:
    return [
        "condition_family",
        "long_scaffold",
        "long_scaffold_order",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
        *_deep_dive_metric_columns(),
    ]


def _factor_condition_columns() -> list[str]:
    return [
        "condition_family",
        "factor_condition",
        "factor_condition_order",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
        *_deep_dive_metric_columns(),
    ]


def _long_scaffold_factor_columns() -> list[str]:
    return [
        "condition_family",
        "long_scaffold",
        "long_scaffold_order",
        "factor_condition",
        "factor_condition_order",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
        *_deep_dive_metric_columns(),
    ]


def _long_scaffold_factor_combo_columns() -> list[str]:
    return [
        "condition_family",
        "long_scaffold",
        "long_scaffold_order",
        "factor_combo",
        "factor_combo_order",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
        *_deep_dive_metric_columns(),
    ]
