"""PIT sector-strength evidence for Daily Ranking short/red candidates."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

import pandas as pd

from src.domains.analytics.atr_expansion_forward_response import (
    _create_observation_panel as _create_atr_observation_panel,
)
from src.domains.analytics.earnings_holdthrough_expectancy import _table_exists
from src.domains.analytics.earnings_holdthrough_expectancy_report import (
    _top_rows_for_markdown,
)
from src.domains.analytics.daily_ranking_research_base import (
    assert_daily_ranking_research_tables,
    create_daily_ranking_research_panel,
    daily_ranking_query_end_date,
    daily_ranking_query_start_date,
    normalize_daily_ranking_market_scopes,
)
from src.domains.analytics.ranking_sector_strength_evidence import (
    _create_sector_strength_tables,
)
from src.domains.analytics.ranking_short_red_evidence import (
    DEFAULT_HORIZONS,
    DEFAULT_MARKET_SCOPES,
    DEFAULT_MIN_OBSERVATIONS,
    DEFAULT_OBSERVATION_SAMPLE_LIMIT,
    DEFAULT_TAIL_RETURN_THRESHOLD_PCT,
    _CANDIDATE_BUCKETS,
    _REQUIRED_ATR_WINDOWS,
    _REQUIRED_RETURN_WINDOWS,
    _STALE_OVERVALUED_TREND_SPLITS,
    _TECHNICAL_STATES,
    _VALUATION_STATES,
    _create_feature_panel,
)
from src.domains.analytics.readonly_duckdb_support import (
    SourceMode,
    open_readonly_analysis_connection,
)
from src.domains.analytics.research_bundle import ResearchBundleInfo, write_research_bundle

RANKING_SHORT_SECTOR_STRENGTH_EVIDENCE_EXPERIMENT_ID = (
    "market-behavior/ranking-short-sector-strength-evidence"
)
_REQUIRED_SECTOR_TABLES: tuple[str, ...] = ("stock_master_daily",)
_PRIORITY_SHORT_SECTOR_CONDITIONS: tuple[tuple[str, str], ...] = (
    (
        "stale_overvalued_sector_weak",
        "liquidity_regime = 'stale_liquidity' "
        "AND overvalued_or_no_earnings_warning "
        "AND sector_strength_bucket = 'sector_weak'",
    ),
    (
        "stale_rally_fade_sector_weak",
        "liquidity_regime = 'stale_liquidity' "
        "AND overvalued_or_no_earnings_warning "
        "AND recent_return_20d_pct > 0 "
        "AND recent_return_60d_pct > 0 "
        "AND sector_strength_bucket = 'sector_weak'",
    ),
    (
        "distribution_stress_overvalued_sector_weak",
        "liquidity_regime = 'distribution_stress' "
        "AND overvalued_or_no_earnings_warning "
        "AND sector_strength_bucket = 'sector_weak'",
    ),
    (
        "crowded_overvalued_overheat_sector_weak",
        "liquidity_regime = 'crowded_rerating' "
        "AND overvalued_or_no_earnings_warning "
        "AND atr20_to_atr60_overheat "
        "AND sector_strength_bucket = 'sector_weak'",
    ),
    (
        "crowded_no_value_overheat_sector_weak",
        "liquidity_regime = 'crowded_rerating' "
        "AND no_value_confirmation "
        "AND atr20_to_atr60_overheat "
        "AND sector_strength_bucket = 'sector_weak'",
    ),
    (
        "stale_overvalued_sector_strong",
        "liquidity_regime = 'stale_liquidity' "
        "AND overvalued_or_no_earnings_warning "
        "AND sector_strength_bucket = 'sector_strong'",
    ),
    (
        "distribution_stress_overvalued_sector_strong",
        "liquidity_regime = 'distribution_stress' "
        "AND overvalued_or_no_earnings_warning "
        "AND sector_strength_bucket = 'sector_strong'",
    ),
    (
        "strong_low_value_sector_strong_short_prohibit",
        "strong_value_confirmation "
        "AND sector_strength_bucket = 'sector_strong'",
    ),
)
_SECTOR_BUCKETS: tuple[str, ...] = ("sector_weak", "sector_neutral", "sector_strong")


@dataclass(frozen=True)
class RankingShortSectorStrengthEvidenceResult:
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
    observation_count: int
    coverage_diagnostics_df: pd.DataFrame
    short_candidate_sector_interaction_df: pd.DataFrame
    short_value_sector_interaction_df: pd.DataFrame
    stale_rally_sector_interaction_df: pd.DataFrame
    technical_sector_short_interaction_df: pd.DataFrame
    priority_short_sector_readout_df: pd.DataFrame
    observation_sample_df: pd.DataFrame


def run_ranking_short_sector_strength_evidence_research(
    db_path: str | Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    market_scopes: Sequence[str] = DEFAULT_MARKET_SCOPES,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
    tail_return_threshold_pct: float = DEFAULT_TAIL_RETURN_THRESHOLD_PCT,
    observation_sample_limit: int = DEFAULT_OBSERVATION_SAMPLE_LIMIT,
) -> RankingShortSectorStrengthEvidenceResult:
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

    query_start = daily_ranking_query_start_date(start_date, warmup_calendar_days=720)
    query_end = daily_ranking_query_end_date(
        end_date,
        max_horizon=max(resolved_horizons),
    )

    with open_readonly_analysis_connection(
        str(db_path_obj),
        snapshot_prefix="ranking-short-sector-strength-evidence-",
    ) as ctx:
        _assert_sector_tables(ctx.connection)
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
        assert_daily_ranking_research_tables(ctx.connection)
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
        _create_feature_panel(ctx.connection)
        _create_sector_strength_tables(ctx.connection, horizons=resolved_horizons)
        _create_short_sector_feature_panel(ctx.connection)
        _create_short_sector_candidate_work(ctx.connection)
        observation_count = int(
            ctx.connection.execute(
                "SELECT count(*) FROM ranking_short_sector_feature_panel"
            ).fetchone()[0]
        )
        result = RankingShortSectorStrengthEvidenceResult(
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
            observation_count=observation_count,
            coverage_diagnostics_df=_build_coverage_diagnostics_df(ctx.connection),
            short_candidate_sector_interaction_df=(
                _build_short_candidate_sector_interaction_df(
                    ctx.connection,
                    horizons=resolved_horizons,
                    min_observations=min_observations,
                    tail_return_threshold_pct=tail_return_threshold_pct,
                )
            ),
            short_value_sector_interaction_df=_build_short_value_sector_interaction_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                tail_return_threshold_pct=tail_return_threshold_pct,
            ),
            stale_rally_sector_interaction_df=_build_stale_rally_sector_interaction_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                tail_return_threshold_pct=tail_return_threshold_pct,
            ),
            technical_sector_short_interaction_df=(
                _build_technical_sector_short_interaction_df(
                    ctx.connection,
                    horizons=resolved_horizons,
                    min_observations=min_observations,
                    tail_return_threshold_pct=tail_return_threshold_pct,
                )
            ),
            priority_short_sector_readout_df=_build_priority_short_sector_readout_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                tail_return_threshold_pct=tail_return_threshold_pct,
            ),
            observation_sample_df=_query_observation_sample_df(
                ctx.connection,
                limit=observation_sample_limit,
            ),
        )
    return result


def write_ranking_short_sector_strength_evidence_bundle(
    result: RankingShortSectorStrengthEvidenceResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=RANKING_SHORT_SECTOR_STRENGTH_EVIDENCE_EXPERIMENT_ID,
        module=__name__,
        function="run_ranking_short_sector_strength_evidence_research",
        params={
            "horizons": list(result.horizons),
            "market_scopes": list(result.market_scopes),
            "min_observations": result.min_observations,
            "tail_return_threshold_pct": result.tail_return_threshold_pct,
            "sector_strength_score_definition": (
                "average of official sector-index score "
                "(0.20*5d + 0.45*20d + 0.25*60d TOPIX-excess ranks + "
                "0.10*constituent breadth rank) and constituent score "
                "(20d TOPIX-excess rank + 60d TOPIX-excess rank + breadth rank)/3"
            ),
            "sector_strength_bucket_definition": "score>=0.8 strong, score<=0.2 weak",
        },
        db_path=result.db_path,
        analysis_start_date=result.analysis_start_date,
        analysis_end_date=result.analysis_end_date,
        result_metadata={
            "source_mode": result.source_mode,
            "source_detail": result.source_detail,
            "market_source": result.market_source,
            "observation_count": result.observation_count,
        },
        result_tables={
            "coverage_diagnostics_df": result.coverage_diagnostics_df,
            "short_candidate_sector_interaction_df": (
                result.short_candidate_sector_interaction_df
            ),
            "short_value_sector_interaction_df": (
                result.short_value_sector_interaction_df
            ),
            "stale_rally_sector_interaction_df": result.stale_rally_sector_interaction_df,
            "technical_sector_short_interaction_df": (
                result.technical_sector_short_interaction_df
            ),
            "priority_short_sector_readout_df": result.priority_short_sector_readout_df,
            "observation_sample_df": result.observation_sample_df,
        },
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def build_summary_markdown(result: RankingShortSectorStrengthEvidenceResult) -> str:
    return "\n".join(
        [
            "# Ranking Short Sector Strength Evidence",
            "",
            f"- DB: `{result.db_path}`",
            f"- Source: `{result.source_mode}` / `{result.source_detail}`",
            f"- Market source: `{result.market_source}`",
            f"- Analysis window: `{result.analysis_start_date}` to `{result.analysis_end_date}`",
            f"- Observation count: `{result.observation_count}`",
            f"- Forward horizons: `{list(result.horizons)}`",
            f"- Market scopes: `{list(result.market_scopes)}`",
            f"- Min observations: `{result.min_observations}`",
            "",
            "## Coverage Diagnostics",
            "",
            _top_rows_for_markdown(result.coverage_diagnostics_df, limit=30),
            "",
            "## Short Candidate x Sector Strength",
            "",
            _top_rows_for_markdown(
                result.short_candidate_sector_interaction_df,
                sort_columns=[
                    "market_scope",
                    "candidate_bucket_order",
                    "horizon",
                    "sector_strength_bucket_order",
                ],
                limit=180,
            ),
            "",
            "## Short Value x Sector Strength",
            "",
            _top_rows_for_markdown(
                result.short_value_sector_interaction_df,
                sort_columns=[
                    "market_scope",
                    "liquidity_regime_order",
                    "valuation_state_order",
                    "horizon",
                    "sector_strength_bucket_order",
                ],
                limit=180,
            ),
            "",
            "## Stale Rally x Sector Strength",
            "",
            _top_rows_for_markdown(
                result.stale_rally_sector_interaction_df,
                sort_columns=[
                    "market_scope",
                    "trend_split_order",
                    "horizon",
                    "sector_strength_bucket_order",
                ],
                limit=180,
            ),
            "",
            "## Technical x Sector Strength",
            "",
            _top_rows_for_markdown(
                result.technical_sector_short_interaction_df,
                sort_columns=[
                    "market_scope",
                    "candidate_bucket_order",
                    "technical_state_order",
                    "horizon",
                    "sector_strength_bucket_order",
                ],
                limit=220,
            ),
            "",
            "## Priority Short Sector Readout",
            "",
            _top_rows_for_markdown(
                result.priority_short_sector_readout_df,
                sort_columns=["market_scope", "priority_condition_order", "horizon"],
                limit=120,
            ),
            "",
            "## Observation Sample",
            "",
            _top_rows_for_markdown(result.observation_sample_df, limit=80),
            "",
        ]
    )


def _validate_params(
    *,
    horizons: Sequence[int],
    min_observations: int,
    tail_return_threshold_pct: float,
    observation_sample_limit: int,
) -> None:
    if not horizons or any(horizon <= 0 for horizon in horizons):
        raise ValueError("horizons must be positive")
    if min_observations <= 0:
        raise ValueError("min_observations must be positive")
    if tail_return_threshold_pct >= 0.0:
        raise ValueError("tail_return_threshold_pct must be negative")
    if observation_sample_limit <= 0:
        raise ValueError("observation_sample_limit must be positive")


def _assert_sector_tables(conn: Any) -> None:
    missing = [table for table in _REQUIRED_SECTOR_TABLES if not _table_exists(conn, table)]
    if missing:
        raise ValueError(f"market.duckdb is missing required tables: {', '.join(missing)}")


def _create_short_sector_feature_panel(conn: Any) -> None:
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE ranking_short_sector_feature_panel AS
        SELECT
            f.*,
            sm.sector_33_code,
            sm.sector_33_name,
            s.sector_observation_count,
            s.sector_code_count,
            s.sector_20d_topix_excess_pct,
            s.sector_60d_topix_excess_pct,
            s.sector_breadth_20d_pct,
            s.sector_20d_strength_rank,
            s.sector_60d_strength_rank,
            s.sector_breadth_strength_rank,
            s.sector_strength_score,
            s.sector_strength_bucket,
            CASE
                WHEN s.sector_strength_bucket = 'sector_weak' THEN 0
                WHEN s.sector_strength_bucket = 'sector_neutral' THEN 1
                WHEN s.sector_strength_bucket = 'sector_strong' THEN 2
                ELSE 99
            END AS sector_strength_bucket_order,
            s.sector_consistency_bucket
        FROM ranking_short_red_feature_panel f
        JOIN ranking_sector_master sm
          ON sm.code = f.code
         AND sm.date = f.date
        JOIN ranking_sector_daily_state s
          ON s.market_scope = f.market_scope
         AND s.date = f.date
         AND s.sector_33_code = sm.sector_33_code
         AND s.sector_33_name = sm.sector_33_name
        """
    )


def _create_short_sector_candidate_work(conn: Any) -> None:
    selects = []
    for order, (bucket, condition) in enumerate(_CANDIDATE_BUCKETS):
        selects.append(
            f"""
            SELECT
                *,
                '{bucket}' AS candidate_bucket,
                {order} AS candidate_bucket_order
            FROM ranking_short_sector_feature_panel
            WHERE {condition}
            """
        )
    conn.execute(
        "CREATE OR REPLACE TEMP TABLE ranking_short_sector_candidate_work AS\n"
        + "\nUNION ALL\n".join(selects)
    )


def _build_coverage_diagnostics_df(conn: Any) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            market_scope,
            count(*) AS observation_count,
            count(DISTINCT code) AS code_count,
            count(DISTINCT date) AS date_count,
            count(DISTINCT sector_33_name) AS sector_count,
            min(date) AS min_date,
            max(date) AS max_date,
            avg(CASE WHEN sector_strength_score IS NOT NULL THEN 1.0 ELSE 0.0 END)
                * 100.0 AS sector_strength_coverage_pct,
            avg(CASE WHEN overvalued_or_no_earnings_warning THEN 1.0 ELSE 0.0 END)
                * 100.0 AS overvalued_or_no_earnings_warning_rate_pct,
            avg(CASE WHEN no_value_confirmation THEN 1.0 ELSE 0.0 END) * 100.0
                AS no_value_confirmation_rate_pct,
            avg(CASE WHEN strong_value_confirmation THEN 1.0 ELSE 0.0 END) * 100.0
                AS strong_value_confirmation_rate_pct
        FROM ranking_short_sector_feature_panel
        GROUP BY market_scope
        ORDER BY market_scope
        """
    ).fetchdf()


def _build_short_candidate_sector_interaction_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    tail_return_threshold_pct: float,
) -> pd.DataFrame:
    frames = [
        _aggregate_condition(
            conn,
            source_name="ranking_short_sector_candidate_work",
            condition="TRUE",
            condition_fields={"horizon": int(horizon)},
            horizon=int(horizon),
            group_columns=[
                "market_scope",
                "candidate_bucket",
                "candidate_bucket_order",
                "sector_strength_bucket",
                "sector_strength_bucket_order",
            ],
            min_observations=min_observations,
            tail_return_threshold_pct=tail_return_threshold_pct,
        )
        for horizon in horizons
    ]
    return _concat_sorted(frames, columns=_candidate_sector_columns())


def _build_short_value_sector_interaction_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    tail_return_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for regime_order, regime in enumerate(
        ("crowded_rerating", "distribution_stress", "stale_liquidity", "neutral")
    ):
        for valuation_order, (valuation_state, condition) in enumerate(_VALUATION_STATES):
            for sector_order, sector_bucket in enumerate(_SECTOR_BUCKETS):
                for horizon in horizons:
                    frames.append(
                        _aggregate_condition(
                            conn,
                            source_name="ranking_short_sector_feature_panel",
                            condition=(
                                f"liquidity_regime = '{regime}' "
                                f"AND ({condition}) "
                                f"AND sector_strength_bucket = '{sector_bucket}'"
                            ),
                            condition_fields={
                                "liquidity_regime_order": regime_order,
                                "valuation_state": valuation_state,
                                "valuation_state_order": valuation_order,
                                "horizon": int(horizon),
                                "sector_strength_bucket_order": sector_order,
                            },
                            horizon=int(horizon),
                            group_columns=[
                                "market_scope",
                                "liquidity_regime",
                                "sector_strength_bucket",
                            ],
                            min_observations=min_observations,
                            tail_return_threshold_pct=tail_return_threshold_pct,
                        )
                    )
    return _concat_sorted(frames, columns=_value_sector_columns())


def _build_stale_rally_sector_interaction_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    tail_return_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    base_condition = (
        "liquidity_regime = 'stale_liquidity' "
        "AND overvalued_or_no_earnings_warning"
    )
    for trend_order, (trend_split, condition) in enumerate(
        _STALE_OVERVALUED_TREND_SPLITS
    ):
        for sector_order, sector_bucket in enumerate(_SECTOR_BUCKETS):
            for horizon in horizons:
                frames.append(
                    _aggregate_condition(
                        conn,
                        source_name="ranking_short_sector_feature_panel",
                        condition=(
                            f"{base_condition} "
                            f"AND ({condition}) "
                            f"AND sector_strength_bucket = '{sector_bucket}'"
                        ),
                        condition_fields={
                            "trend_split": trend_split,
                            "trend_split_order": trend_order,
                            "horizon": int(horizon),
                            "sector_strength_bucket_order": sector_order,
                        },
                        horizon=int(horizon),
                        group_columns=[
                            "market_scope",
                            "sector_strength_bucket",
                        ],
                        min_observations=min_observations,
                        tail_return_threshold_pct=tail_return_threshold_pct,
                    )
                )
    return _concat_sorted(frames, columns=_stale_rally_sector_columns())


def _build_technical_sector_short_interaction_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    tail_return_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for technical_order, (technical_state, condition) in enumerate(_TECHNICAL_STATES):
        for sector_order, sector_bucket in enumerate(_SECTOR_BUCKETS):
            for horizon in horizons:
                frames.append(
                    _aggregate_condition(
                        conn,
                        source_name="ranking_short_sector_candidate_work",
                        condition=(
                            f"({condition}) "
                            f"AND sector_strength_bucket = '{sector_bucket}'"
                        ),
                        condition_fields={
                            "technical_state": technical_state,
                            "technical_state_order": technical_order,
                            "horizon": int(horizon),
                            "sector_strength_bucket_order": sector_order,
                        },
                        horizon=int(horizon),
                        group_columns=[
                            "market_scope",
                            "candidate_bucket",
                            "candidate_bucket_order",
                            "sector_strength_bucket",
                        ],
                        min_observations=min_observations,
                        tail_return_threshold_pct=tail_return_threshold_pct,
                    )
                )
    return _concat_sorted(frames, columns=_technical_sector_columns())


def _build_priority_short_sector_readout_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    tail_return_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for condition_order, (priority_condition, condition) in enumerate(
        _PRIORITY_SHORT_SECTOR_CONDITIONS
    ):
        for horizon in horizons:
            frames.append(
                _aggregate_condition(
                    conn,
                    source_name="ranking_short_sector_feature_panel",
                    condition=condition,
                    condition_fields={
                        "priority_condition": priority_condition,
                        "priority_condition_order": condition_order,
                        "horizon": int(horizon),
                    },
                    horizon=int(horizon),
                    group_columns=["market_scope"],
                    min_observations=min_observations,
                    tail_return_threshold_pct=tail_return_threshold_pct,
                )
            )
    return _concat_sorted(frames, columns=_priority_columns())


def _aggregate_condition(
    conn: Any,
    *,
    source_name: str,
    condition: str,
    condition_fields: dict[str, Any],
    horizon: int,
    group_columns: Sequence[str],
    min_observations: int,
    tail_return_threshold_pct: float,
) -> pd.DataFrame:
    group_select = ",\n            ".join(group_columns)
    group_by = ", ".join(group_columns)
    raw_return_column = f"forward_close_return_{horizon}d_pct"
    topix_return_column = f"topix_close_return_{horizon}d_pct"
    excess_return_column = f"forward_close_excess_return_{horizon}d_pct"
    frame = conn.execute(
        f"""
        SELECT
            {group_select},
            count(*) AS observation_count,
            count(DISTINCT code) AS code_count,
            count(DISTINCT date) AS date_count,
            count(DISTINCT sector_33_name) AS sector_count,
            avg({raw_return_column}) AS mean_forward_raw_return_pct,
            median({raw_return_column}) AS median_forward_raw_return_pct,
            quantile_cont({raw_return_column}, 0.10) AS p10_forward_raw_return_pct,
            quantile_cont({raw_return_column}, 0.25) AS p25_forward_raw_return_pct,
            quantile_cont({raw_return_column}, 0.75) AS p75_forward_raw_return_pct,
            quantile_cont({raw_return_column}, 0.90) AS p90_forward_raw_return_pct,
            avg(CASE WHEN {raw_return_column} > 0 THEN 1.0 ELSE 0.0 END) * 100.0
                AS positive_raw_return_rate_pct,
            avg(CASE WHEN {raw_return_column} < 0 THEN 1.0 ELSE 0.0 END) * 100.0
                AS negative_raw_return_rate_pct,
            avg(CASE WHEN {raw_return_column} <= ? THEN 1.0 ELSE 0.0 END) * 100.0
                AS downside_raw_tail_rate_pct,
            avg(CASE WHEN {raw_return_column} >= ? THEN 1.0 ELSE 0.0 END) * 100.0
                AS upside_raw_tail_rate_pct,
            avg({topix_return_column}) AS mean_topix_return_pct,
            median({topix_return_column}) AS median_topix_return_pct,
            avg({excess_return_column}) AS mean_forward_excess_return_pct,
            median({excess_return_column}) AS median_forward_excess_return_pct,
            quantile_cont({excess_return_column}, 0.10) AS p10_forward_excess_return_pct,
            quantile_cont({excess_return_column}, 0.25) AS p25_forward_excess_return_pct,
            quantile_cont({excess_return_column}, 0.75) AS p75_forward_excess_return_pct,
            quantile_cont({excess_return_column}, 0.90) AS p90_forward_excess_return_pct,
            avg(CASE WHEN {excess_return_column} > 0 THEN 1.0 ELSE 0.0 END) * 100.0
                AS positive_excess_return_rate_pct,
            avg(CASE WHEN {excess_return_column} < 0 THEN 1.0 ELSE 0.0 END) * 100.0
                AS negative_excess_return_rate_pct,
            avg(CASE WHEN {excess_return_column} <= ? THEN 1.0 ELSE 0.0 END) * 100.0
                AS downside_excess_tail_rate_pct,
            avg(CASE WHEN {excess_return_column} >= ? THEN 1.0 ELSE 0.0 END) * 100.0
                AS upside_excess_tail_rate_pct,
            median(sector_strength_score) AS median_sector_strength_score,
            median(sector_20d_topix_excess_pct) AS median_sector_20d_topix_excess_pct,
            median(sector_60d_topix_excess_pct) AS median_sector_60d_topix_excess_pct,
            median(sector_breadth_20d_pct) AS median_sector_breadth_20d_pct,
            median(recent_return_20d_pct) AS median_recent_return_20d_pct,
            median(recent_return_60d_pct) AS median_recent_return_60d_pct,
            median(topix_recent_return_20d_pct) AS median_topix_recent_return_20d_pct,
            median(topix_recent_return_60d_pct) AS median_topix_recent_return_60d_pct,
            median(med_adv60_jpy) / 1000000.0 AS median_med_adv60_mil_jpy,
            median(market_cap_bil_jpy) AS median_market_cap_bil_jpy,
            median(liquidity_residual_z) AS median_liquidity_residual_z,
            median(per_percentile) AS median_per_percentile,
            median(forward_per_percentile) AS median_forward_per_percentile,
            median(forward_p_op_percentile) AS median_forward_p_op_percentile,
            median(pbr_percentile) AS median_pbr_percentile,
            avg(CASE WHEN strong_value_confirmation THEN 1.0 ELSE 0.0 END) * 100.0
                AS strong_value_confirmation_rate_pct,
            avg(CASE WHEN no_value_confirmation THEN 1.0 ELSE 0.0 END) * 100.0
                AS no_value_confirmation_rate_pct,
            median(atr20_pct) AS median_atr20_pct,
            median(atr60_pct) AS median_atr60_pct,
            median(atr20_to_atr60) AS median_atr20_to_atr60,
            median(atr20_change_20d_pct) AS median_atr20_change_20d_pct,
            avg(CASE WHEN atr20_acceleration THEN 1.0 ELSE 0.0 END) * 100.0
                AS atr20_acceleration_rate_pct,
            avg(CASE WHEN atr20_to_atr60_overheat THEN 1.0 ELSE 0.0 END) * 100.0
                AS atr20_to_atr60_overheat_rate_pct
        FROM {source_name}
        WHERE {condition}
          AND {raw_return_column} IS NOT NULL
          AND {topix_return_column} IS NOT NULL
          AND {excess_return_column} IS NOT NULL
        GROUP BY {group_by}
        HAVING count(*) >= ?
        """,
        [
            float(tail_return_threshold_pct),
            abs(float(tail_return_threshold_pct)),
            float(tail_return_threshold_pct),
            abs(float(tail_return_threshold_pct)),
            int(min_observations),
        ],
    ).fetchdf()
    if frame.empty:
        return frame
    for column, value in condition_fields.items():
        frame[column] = value
    ordered = [*condition_fields.keys(), *group_columns]
    ordered.extend(_metric_columns())
    return frame.reindex(columns=ordered)


def _query_observation_sample_df(conn: Any, *, limit: int) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            date,
            market_scope,
            code,
            company_name,
            sector_33_name,
            sector_strength_score,
            sector_strength_bucket,
            liquidity_regime,
            recent_return_20d_pct,
            recent_return_60d_pct,
            overvalued_or_no_earnings_warning,
            strong_value_confirmation,
            no_value_confirmation,
            per_percentile,
            forward_per_percentile,
            pbr_percentile,
            atr20_to_atr60,
            atr20_change_20d_pct,
            forward_close_return_20d_pct,
            topix_close_return_20d_pct,
            forward_close_excess_return_20d_pct
        FROM ranking_short_sector_feature_panel
        ORDER BY date, code
        LIMIT ?
        """,
        [int(limit)],
    ).fetchdf()


def _metric_columns() -> list[str]:
    return [
        "observation_count",
        "code_count",
        "date_count",
        "sector_count",
        "mean_forward_raw_return_pct",
        "median_forward_raw_return_pct",
        "p10_forward_raw_return_pct",
        "p25_forward_raw_return_pct",
        "p75_forward_raw_return_pct",
        "p90_forward_raw_return_pct",
        "positive_raw_return_rate_pct",
        "negative_raw_return_rate_pct",
        "downside_raw_tail_rate_pct",
        "upside_raw_tail_rate_pct",
        "mean_topix_return_pct",
        "median_topix_return_pct",
        "mean_forward_excess_return_pct",
        "median_forward_excess_return_pct",
        "p10_forward_excess_return_pct",
        "p25_forward_excess_return_pct",
        "p75_forward_excess_return_pct",
        "p90_forward_excess_return_pct",
        "positive_excess_return_rate_pct",
        "negative_excess_return_rate_pct",
        "downside_excess_tail_rate_pct",
        "upside_excess_tail_rate_pct",
        "median_sector_strength_score",
        "median_sector_20d_topix_excess_pct",
        "median_sector_60d_topix_excess_pct",
        "median_sector_breadth_20d_pct",
        "median_recent_return_20d_pct",
        "median_recent_return_60d_pct",
        "median_topix_recent_return_20d_pct",
        "median_topix_recent_return_60d_pct",
        "median_med_adv60_mil_jpy",
        "median_market_cap_bil_jpy",
        "median_liquidity_residual_z",
        "median_per_percentile",
        "median_forward_per_percentile",
        "median_forward_p_op_percentile",
        "median_pbr_percentile",
        "strong_value_confirmation_rate_pct",
        "no_value_confirmation_rate_pct",
        "median_atr20_pct",
        "median_atr60_pct",
        "median_atr20_to_atr60",
        "median_atr20_change_20d_pct",
        "atr20_acceleration_rate_pct",
        "atr20_to_atr60_overheat_rate_pct",
    ]


def _candidate_sector_columns() -> list[str]:
    return [
        "horizon",
        "market_scope",
        "candidate_bucket",
        "candidate_bucket_order",
        "sector_strength_bucket",
        "sector_strength_bucket_order",
        *_metric_columns(),
    ]


def _value_sector_columns() -> list[str]:
    return [
        "liquidity_regime_order",
        "valuation_state",
        "valuation_state_order",
        "horizon",
        "sector_strength_bucket_order",
        "market_scope",
        "liquidity_regime",
        "sector_strength_bucket",
        *_metric_columns(),
    ]


def _stale_rally_sector_columns() -> list[str]:
    return [
        "trend_split",
        "trend_split_order",
        "horizon",
        "sector_strength_bucket_order",
        "market_scope",
        "sector_strength_bucket",
        *_metric_columns(),
    ]


def _technical_sector_columns() -> list[str]:
    return [
        "technical_state",
        "technical_state_order",
        "horizon",
        "sector_strength_bucket_order",
        "market_scope",
        "candidate_bucket",
        "candidate_bucket_order",
        "sector_strength_bucket",
        *_metric_columns(),
    ]


def _priority_columns() -> list[str]:
    return [
        "priority_condition",
        "priority_condition_order",
        "horizon",
        "market_scope",
        *_metric_columns(),
    ]


def _concat_sorted(frames: Sequence[pd.DataFrame], *, columns: Sequence[str]) -> pd.DataFrame:
    non_empty = [frame for frame in frames if not frame.empty]
    if not non_empty:
        return pd.DataFrame(columns=list(columns))
    frame = pd.concat(non_empty, ignore_index=True)
    return frame.reindex(columns=list(columns))
