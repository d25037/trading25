"""SMA5 above-count evidence for Daily Ranking long-side candidates."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

import pandas as pd

from src.domains.analytics.daily_ranking_feature_builders import (
    build_sma_features,
)
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
    normalize_code_sql,
    open_readonly_analysis_connection,
)
from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    write_research_bundle,
)

PUBLIC_FEATURE_BUILDER = build_sma_features
RANKING_SMA5_COUNT_LONG_EVIDENCE_EXPERIMENT_ID = (
    "market-behavior/ranking-sma5-count-long-evidence"
)
DEFAULT_HORIZONS: tuple[int, ...] = (5, 20, 60)
DEFAULT_MARKET_SCOPES: tuple[str, ...] = ("prime",)
DEFAULT_MIN_OBSERVATIONS = 300
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
        "AND forward_per_percentile <= 0.1",
    ),
    (
        "crowded_low10_pbr_forward_per_atr20_accel",
        "liquidity_regime = 'crowded_rerating' "
        "AND long_hybrid_leadership_score >= 0.799999 "
        "AND pbr_percentile <= 0.1 "
        "AND forward_per_percentile <= 0.1 "
        "AND atr20_acceleration_ex_overheat_flag",
    ),
)


@dataclass(frozen=True)
class RankingSma5CountLongEvidenceResult:
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
    sma5_count_group_evidence_df: pd.DataFrame
    long_scaffold_sma5_count_group_evidence_df: pd.DataFrame
    same_day_sma5_group_spread_df: pd.DataFrame
    long_scaffold_same_day_sma5_group_spread_df: pd.DataFrame


def run_ranking_sma5_count_long_evidence_research(
    db_path: str | Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    market_scopes: Sequence[str] = DEFAULT_MARKET_SCOPES,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
    severe_loss_threshold_pct: float = DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
    observation_sample_limit: int = DEFAULT_OBSERVATION_SAMPLE_LIMIT,
) -> RankingSma5CountLongEvidenceResult:
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
        warmup_calendar_days=max(_LONG_WARMUP_CALENDAR_DAYS, max(_LEADERSHIP_WINDOWS) * 3),
    )
    query_end = daily_ranking_query_end_date(
        end_date,
        max_horizon=max(resolved_horizons),
    )

    with open_readonly_analysis_connection(
        str(db_path_obj),
        snapshot_prefix="ranking-sma5-count-long-evidence-",
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
            include_relation_percentiles=False,
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
        _create_sma5_count_long_panel(ctx.connection)
        observation_count = int(
            ctx.connection.execute(
                "SELECT count(*) FROM ranking_sma5_count_long_panel"
            ).fetchone()[0]
        )
        result = RankingSma5CountLongEvidenceResult(
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
            long_scaffold_evidence_df=_build_long_scaffold_evidence_df(
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
            long_scaffold_sma5_count_group_evidence_df=(
                _build_long_scaffold_sma5_count_group_evidence_df(
                    ctx.connection,
                    horizons=resolved_horizons,
                    min_observations=min_observations,
                    severe_loss_threshold_pct=severe_loss_threshold_pct,
                )
            ),
            same_day_sma5_group_spread_df=_build_same_day_sma5_group_spread_df(
                ctx.connection,
                horizons=resolved_horizons,
            ),
            long_scaffold_same_day_sma5_group_spread_df=(
                _build_long_scaffold_same_day_sma5_group_spread_df(
                    ctx.connection,
                    horizons=resolved_horizons,
                )
            ),
        )
    return result


def write_ranking_sma5_count_long_evidence_bundle(
    result: RankingSma5CountLongEvidenceResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=RANKING_SMA5_COUNT_LONG_EVIDENCE_EXPERIMENT_ID,
        module="src.domains.analytics.ranking_sma5_count_long_evidence",
        function="run_ranking_sma5_count_long_evidence_research",
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
            "sma5_parameter": "sma5_above_count_5d",
            "same_day_spread": "base_sma5_count_group - comparison_sma5_count_group",
        },
        result_tables={
            "observation_sample_df": result.observation_sample_df,
            "coverage_diagnostics_df": result.coverage_diagnostics_df,
            "long_scaffold_evidence_df": result.long_scaffold_evidence_df,
            "sma5_count_group_evidence_df": result.sma5_count_group_evidence_df,
            "long_scaffold_sma5_count_group_evidence_df": (
                result.long_scaffold_sma5_count_group_evidence_df
            ),
            "same_day_sma5_group_spread_df": result.same_day_sma5_group_spread_df,
            "long_scaffold_same_day_sma5_group_spread_df": (
                result.long_scaffold_same_day_sma5_group_spread_df
            ),
        },
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def build_summary_markdown(result: RankingSma5CountLongEvidenceResult) -> str:
    parts = [
        "# Ranking SMA5 Count Long Evidence",
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
        "## Long Scaffold Evidence",
        "",
        _top_rows_for_markdown(result.long_scaffold_evidence_df, limit=140),
        "",
        "## SMA5 Count Group Evidence",
        "",
        _top_rows_for_markdown(result.sma5_count_group_evidence_df, limit=120),
        "",
        "## Long Scaffold x SMA5 Count Group Evidence",
        "",
        _top_rows_for_markdown(
            result.long_scaffold_sma5_count_group_evidence_df,
            limit=260,
        ),
        "",
        "## Same-Day SMA5 Count Group Spread",
        "",
        _top_rows_for_markdown(
            result.same_day_sma5_group_spread_df,
            limit=120,
        ),
        "",
        "## Long Scaffold Same-Day SMA5 Count Group Spread",
        "",
        _top_rows_for_markdown(
            result.long_scaffold_same_day_sma5_group_spread_df,
            limit=260,
        ),
    ]
    return "\n".join(parts).rstrip() + "\n"


def _assert_required_tables(conn: Any) -> None:
    missing = [table for table in _REQUIRED_TABLES if not _table_exists(conn, table)]
    if missing:
        raise ValueError(f"market.duckdb is missing required tables: {', '.join(missing)}")


def _create_sma5_count_long_panel(conn: Any) -> None:
    stock_code = normalize_code_sql("sd.code")
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranking_sma5_count_long_panel AS
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
                FROM {DAILY_RANKING_RESEARCH_RANKED_TABLE} p
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
        )
        SELECT
            g.*,
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
                AND coalesce(g.recent_return_20d_pct, 0.0) < 30.0,
                FALSE
            ) AS atr20_acceleration_ex_overheat_flag,
            coalesce(s.atr20_to_atr60_overheat, FALSE)
                AS atr20_to_atr60_overheat,
            coalesce(s.weak_trend, FALSE) AS weak_trend,
            CAST(sc.sma5_above_count_5d AS INTEGER) AS sma5_above_count_5d,
            CASE
                WHEN sc.sma5_above_count_5d IN (0, 1) THEN 'sma5_above_count_0_1'
                WHEN sc.sma5_above_count_5d IN (2, 3) THEN 'sma5_above_count_2_3'
                WHEN sc.sma5_above_count_5d IN (4, 5) THEN 'sma5_above_count_4_5'
            END AS sma5_count_group
        FROM {DAILY_RANKING_RESEARCH_RANKED_TABLE} g
        LEFT JOIN long_sector_leadership_base_panel l
          ON l.code = g.code
         AND l.date = g.date
         AND l.market_scope = g.market_scope
        LEFT JOIN ranking_short_red_feature_panel s
          ON s.code = g.code
         AND s.date = g.date
         AND s.market_scope = g.market_scope
        LEFT JOIN sma5_counts sc
          ON sc.code = g.code
         AND sc.date = g.date
         AND sc.sma5_above_count_sessions = 5
        WHERE sc.sma5_above_count_5d IS NOT NULL
        """
    )


def _build_coverage_diagnostics_df(conn: Any) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            market_scope,
            sma5_count_group,
            count(*) AS observation_count,
            count(DISTINCT code) AS code_count,
            count(DISTINCT date) AS date_count,
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
                AS median_long_hybrid_leadership_score,
            median(pbr_percentile) AS median_pbr_percentile,
            median(forward_per_percentile) AS median_forward_per_percentile
        FROM ranking_sma5_count_long_panel
        GROUP BY market_scope, sma5_count_group
        ORDER BY market_scope, sma5_count_group
        """
    ).fetchdf()


def _build_long_scaffold_evidence_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    severe_loss_threshold_pct: float,
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
            _aggregate_lateral_conditions(
                conn,
                source_name="ranking_sma5_count_long_panel",
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
                    "long_scaffold.long_scaffold_order, "
                    "market_scope"
                ),
                return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
                extra_metric_sql=_deep_dive_metric_sql(),
            )
        )
    return _concat_sorted(frames, columns=_long_scaffold_columns())


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
                source_name="ranking_sma5_count_long_panel",
                lateral_sql=sma5_group_lateral_sql,
                match_condition="sma5_count_group.sma5_count_group_matches",
                group_select_sql=(
                    "'sma5_count_group' AS condition_family,\n"
                    "            sma5_count_group.sma5_count_group,\n"
                    "            sma5_count_group.sma5_count_group_order,\n"
                    f"            {int(horizon)} AS horizon"
                ),
                group_by_sql=(
                    "sma5_count_group.sma5_count_group, "
                    "sma5_count_group.sma5_count_group_order, "
                    "market_scope"
                ),
                return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
                extra_metric_sql=_deep_dive_metric_sql(),
            )
        )
    return _concat_sorted(frames, columns=_sma5_count_group_columns())


def _build_long_scaffold_sma5_count_group_evidence_df(
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
                source_name="ranking_sma5_count_long_panel",
                lateral_sql=lateral_sql,
                match_condition=(
                    "long_scaffold.long_scaffold_matches "
                    "AND sma5_count_group.sma5_count_group_matches"
                ),
                group_select_sql=(
                    "'long_scaffold_sma5_count_group' AS condition_family,\n"
                    "            long_scaffold.long_scaffold,\n"
                    "            long_scaffold.long_scaffold_order,\n"
                    "            sma5_count_group.sma5_count_group,\n"
                    "            sma5_count_group.sma5_count_group_order,\n"
                    f"            {int(horizon)} AS horizon"
                ),
                group_by_sql=(
                    "long_scaffold.long_scaffold, "
                    "long_scaffold.long_scaffold_order, "
                    "sma5_count_group.sma5_count_group, "
                    "sma5_count_group.sma5_count_group_order, "
                    "market_scope"
                ),
                return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                min_observations=min_observations,
                severe_loss_threshold_pct=severe_loss_threshold_pct,
                extra_metric_sql=_deep_dive_metric_sql(),
            )
        )
    return _concat_sorted(frames, columns=_long_scaffold_sma5_count_group_columns())


def _build_same_day_sma5_group_spread_df(
    conn: Any,
    *,
    horizons: Sequence[int],
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for horizon in horizons:
        frames.append(
            _query_same_day_spread_df(
                conn,
                source_name="ranking_sma5_count_long_panel",
                return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                horizon=int(horizon),
                condition_family="same_day_sma5_count_group_spread",
                scaffold_lateral_sql="",
                scaffold_select_sql="'all_market' AS long_scaffold,\n"
                "            0 AS long_scaffold_order,",
                scaffold_group_sql="",
                scaffold_join_sql="",
                match_condition="TRUE",
            )
        )
    return _concat_sorted(frames, columns=_same_day_spread_columns())


def _build_long_scaffold_same_day_sma5_group_spread_df(
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
                source_name="ranking_sma5_count_long_panel",
                return_column=f"forward_close_excess_return_{int(horizon)}d_pct",
                horizon=int(horizon),
                condition_family="long_scaffold_same_day_sma5_count_group_spread",
                scaffold_lateral_sql=scaffold_lateral_sql,
                scaffold_select_sql="long_scaffold.long_scaffold,\n"
                "            long_scaffold.long_scaffold_order,",
                scaffold_group_sql=(
                    "long_scaffold.long_scaffold, "
                    "long_scaffold.long_scaffold_order, "
                ),
                scaffold_join_sql=(
                    "AND comparison.long_scaffold = base.long_scaffold "
                    "AND comparison.long_scaffold_order = base.long_scaffold_order"
                ),
                match_condition="long_scaffold.long_scaffold_matches",
            )
        )
    return _concat_sorted(
        frames,
        columns=_long_scaffold_same_day_spread_columns(),
    )


def _query_same_day_spread_df(
    conn: Any,
    *,
    source_name: str,
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
                sma5_count_group,
                CASE
                    WHEN sma5_count_group = 'sma5_above_count_0_1' THEN 0
                    WHEN sma5_count_group = 'sma5_above_count_2_3' THEN 1
                    WHEN sma5_count_group = 'sma5_above_count_4_5' THEN 2
                END AS sma5_count_group_order,
                count(*) AS observation_count,
                median({return_column}) AS median_excess_return_pct,
                avg({return_column}) AS mean_excess_return_pct
            FROM {source_name}
            {scaffold_lateral_sql}
            WHERE {match_condition}
              AND {return_column} IS NOT NULL
              AND sma5_count_group IS NOT NULL
            GROUP BY
                {scaffold_group_sql}
                market_scope,
                date,
                sma5_count_group,
                sma5_count_group_order
        ),
        pair_values AS (
            SELECT
                base.long_scaffold,
                base.long_scaffold_order,
                base.market_scope,
                base.date,
                base.sma5_count_group AS base_sma5_count_group,
                base.sma5_count_group_order AS base_sma5_count_group_order,
                comparison.sma5_count_group AS comparison_sma5_count_group,
                comparison.sma5_count_group_order
                    AS comparison_sma5_count_group_order,
                base.observation_count AS base_observation_count,
                comparison.observation_count AS comparison_observation_count,
                base.median_excess_return_pct AS base_daily_median_excess_return_pct,
                comparison.median_excess_return_pct
                    AS comparison_daily_median_excess_return_pct,
                base.median_excess_return_pct
                    - comparison.median_excess_return_pct
                    AS daily_median_excess_spread_pct,
                base.mean_excess_return_pct
                    - comparison.mean_excess_return_pct
                    AS daily_mean_excess_spread_pct
            FROM daily_group base
            JOIN daily_group comparison
              ON comparison.market_scope = base.market_scope
             AND comparison.date = base.date
             {scaffold_join_sql}
             AND comparison.sma5_count_group_order > base.sma5_count_group_order
        )
        SELECT
            {condition_family!r} AS condition_family,
            long_scaffold,
            long_scaffold_order,
            base_sma5_count_group,
            base_sma5_count_group_order,
            comparison_sma5_count_group,
            comparison_sma5_count_group_order,
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
            avg(CASE WHEN daily_median_excess_spread_pct < 0 THEN 1.0 ELSE 0.0 END)
                * 100.0 AS comparison_outperform_date_rate_pct,
            median(daily_mean_excess_spread_pct)
                AS median_daily_mean_excess_spread_pct,
            avg(daily_mean_excess_spread_pct)
                AS mean_daily_mean_excess_spread_pct
        FROM pair_values
        GROUP BY
            long_scaffold,
            long_scaffold_order,
            base_sma5_count_group,
            base_sma5_count_group_order,
            comparison_sma5_count_group,
            comparison_sma5_count_group_order,
            market_scope
        """
    ).fetchdf()


def _query_observation_sample_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    limit: int,
) -> pd.DataFrame:
    horizon_columns = ",\n            ".join(
        f"forward_close_excess_return_{int(horizon)}d_pct"
        for horizon in horizons
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
            recent_return_20d_pct,
            recent_return_60d_pct,
            liquidity_residual_z,
            valuation_signal,
            pbr,
            pbr_percentile,
            forward_per,
            forward_per_percentile,
            sector_strength_bucket,
            sector_strength_score,
            long_hybrid_leadership_score,
            atr20_change_20d_pct,
            atr20_acceleration_ex_overheat_flag,
            sma5_above_count_5d,
            sma5_count_group,
            {horizon_columns}
        FROM ranking_sma5_count_long_panel
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


def _sma5_count_group_columns() -> list[str]:
    return [
        "condition_family",
        "sma5_count_group",
        "sma5_count_group_order",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
        *_deep_dive_metric_columns(),
    ]


def _long_scaffold_sma5_count_group_columns() -> list[str]:
    return [
        "condition_family",
        "long_scaffold",
        "long_scaffold_order",
        "sma5_count_group",
        "sma5_count_group_order",
        "horizon",
        "market_scope",
        *_aggregate_metric_columns(),
        *_deep_dive_metric_columns(),
    ]


def _same_day_spread_columns() -> list[str]:
    return [
        "condition_family",
        "long_scaffold",
        "long_scaffold_order",
        "base_sma5_count_group",
        "base_sma5_count_group_order",
        "comparison_sma5_count_group",
        "comparison_sma5_count_group_order",
        "horizon",
        "market_scope",
        *_same_day_spread_metric_columns(),
    ]


def _long_scaffold_same_day_spread_columns() -> list[str]:
    return _same_day_spread_columns()


def _same_day_spread_metric_columns() -> list[str]:
    return [
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


def _concat_sorted(frames: Sequence[pd.DataFrame], *, columns: Sequence[str]) -> pd.DataFrame:
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
            "sma5_count_group_order",
            "base_sma5_count_group_order",
            "comparison_sma5_count_group_order",
        )
        if column in frame.columns
    ]
    return frame.reindex(columns=list(columns)).sort_values(
        order_columns,
        kind="stable",
    )
