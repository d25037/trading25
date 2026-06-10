"""Independent short/red evidence for Daily Ranking regimes."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

import pandas as pd

from src.domains.analytics.atr_expansion_forward_response import (
    _create_observation_panel as _create_atr_observation_panel,
)
from src.domains.analytics.earnings_holdthrough_expectancy import (
    _sort_summary_df,
)
from src.domains.analytics.earnings_holdthrough_expectancy_report import (
    _top_rows_for_markdown,
)
from src.domains.analytics.daily_ranking_research_base import (
    DAILY_RANKING_RESEARCH_LIQUIDITY_RANKED_TABLE,
    assert_daily_ranking_research_tables,
    create_daily_ranking_research_panel,
    daily_ranking_query_end_date,
    daily_ranking_query_start_date,
    normalize_daily_ranking_market_scopes,
)
from src.domains.analytics.readonly_duckdb_support import (
    SourceMode,
    open_readonly_analysis_connection,
)
from src.domains.analytics.research_bundle import ResearchBundleInfo, write_research_bundle

RANKING_SHORT_RED_EVIDENCE_EXPERIMENT_ID = (
    "market-behavior/ranking-short-red-evidence"
)
DEFAULT_HORIZONS: tuple[int, ...] = (5, 10, 20, 60)
DEFAULT_MARKET_SCOPES: tuple[str, ...] = ("prime",)
DEFAULT_MIN_OBSERVATIONS = 500
DEFAULT_TAIL_RETURN_THRESHOLD_PCT = -10.0
DEFAULT_OBSERVATION_SAMPLE_LIMIT = 10_000
_REQUIRED_ATR_WINDOWS: tuple[int, ...] = (20, 60)
_REQUIRED_RETURN_WINDOWS: tuple[int, ...] = (20, 60)
_CANDIDATE_BUCKETS: tuple[tuple[str, str], ...] = (
    (
        "crowded_no_value",
        "liquidity_regime = 'crowded_rerating' AND no_value_confirmation",
    ),
    (
        "crowded_high_valuation",
        "liquidity_regime = 'crowded_rerating' AND high_valuation_warning",
    ),
    (
        "crowded_high_valuation_weak_trend",
        "liquidity_regime = 'crowded_rerating' "
        "AND high_valuation_warning AND weak_trend",
    ),
    (
        "distribution_stress_weak_trend",
        "liquidity_regime = 'distribution_stress' AND weak_trend",
    ),
    (
        "distribution_stress_high_valuation",
        "liquidity_regime = 'distribution_stress' AND high_valuation_warning",
    ),
    (
        "stale_high_valuation_weak_trend",
        "liquidity_regime = 'stale_liquidity' "
        "AND high_valuation_warning AND weak_trend",
    ),
)
_TECHNICAL_STATES: tuple[tuple[str, str], ...] = (
    ("all_technical", "TRUE"),
    ("recent_20d_negative", "recent_return_20d_pct <= 0"),
    ("recent_60d_negative", "recent_return_60d_pct <= 0"),
    (
        "recent_20d_60d_negative",
        "recent_return_20d_pct <= 0 AND recent_return_60d_pct <= 0",
    ),
    (
        "atr20_acceleration",
        "atr20_change_20d_pct >= 25.0 AND atr20_to_atr60 < 1.25",
    ),
    (
        "atr20_to_atr60_overheat",
        "atr20_change_20d_pct >= 25.0 AND atr20_to_atr60 >= 1.25",
    ),
)
_VALUATION_STATES: tuple[tuple[str, str], ...] = (
    ("all_valuation", "TRUE"),
    ("high_valuation", "high_valuation_warning"),
    ("missing_earnings", "missing_earnings_warning"),
    ("no_value_confirmation", "no_value_confirmation"),
    ("strong_low_value", "strong_value_confirmation"),
)
_STALE_CONDITIONS: tuple[tuple[str, str], ...] = (
    ("all_stale", "liquidity_regime = 'stale_liquidity'"),
    (
        "stale_high_valuation",
        "liquidity_regime = 'stale_liquidity' AND high_valuation_warning",
    ),
    (
        "stale_weak_trend",
        "liquidity_regime = 'stale_liquidity' AND weak_trend",
    ),
    (
        "stale_high_valuation_weak_trend",
        "liquidity_regime = 'stale_liquidity' "
        "AND high_valuation_warning AND weak_trend",
    ),
)
_STALE_HIGH_VALUATION_TREND_SPLITS: tuple[tuple[str, str], ...] = (
    ("all_stale_high_valuation", "TRUE"),
    ("recent_20d_nonpositive", "recent_return_20d_pct <= 0"),
    ("recent_60d_nonpositive", "recent_return_60d_pct <= 0"),
    (
        "recent_20d_or_60d_nonpositive",
        "recent_return_20d_pct <= 0 OR recent_return_60d_pct <= 0",
    ),
    (
        "recent_20d_and_60d_nonpositive",
        "recent_return_20d_pct <= 0 AND recent_return_60d_pct <= 0",
    ),
    (
        "recent_20d_and_60d_positive",
        "recent_return_20d_pct > 0 AND recent_return_60d_pct > 0",
    ),
)


@dataclass(frozen=True)
class RankingShortRedEvidenceResult:
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
    short_red_candidate_df: pd.DataFrame
    regime_valuation_interaction_df: pd.DataFrame
    technical_atr_short_interaction_df: pd.DataFrame
    stale_liquidity_short_diagnostics_df: pd.DataFrame
    stale_high_valuation_trend_split_df: pd.DataFrame
    live_ranking_replay_df: pd.DataFrame
    observation_sample_df: pd.DataFrame


def run_ranking_short_red_evidence_research(
    db_path: str | Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    market_scopes: Sequence[str] = DEFAULT_MARKET_SCOPES,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
    tail_return_threshold_pct: float = DEFAULT_TAIL_RETURN_THRESHOLD_PCT,
    observation_sample_limit: int = DEFAULT_OBSERVATION_SAMPLE_LIMIT,
) -> RankingShortRedEvidenceResult:
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
        snapshot_prefix="ranking-short-red-evidence-",
    ) as ctx:
        assert_daily_ranking_research_tables(ctx.connection)
        market_source = "stock_master_daily_exact_date"
        create_daily_ranking_research_panel(
            ctx.connection,
            query_start=query_start,
            query_end=query_end,
            analysis_start_date=start_date,
            analysis_end_date=end_date,
            horizons=resolved_horizons,
            market_source=market_source,
            market_scopes=resolved_market_scopes,
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
        _create_feature_panel(ctx.connection)
        _create_candidate_work(ctx.connection)
        observation_count = int(
            ctx.connection.execute(
                "SELECT count(*) FROM ranking_short_red_feature_panel"
            ).fetchone()[0]
        )
        coverage_diagnostics_df = _build_coverage_diagnostics_df(ctx.connection)
        short_red_candidate_df = _build_short_red_candidate_df(
            ctx.connection,
            horizons=resolved_horizons,
            min_observations=min_observations,
            tail_return_threshold_pct=tail_return_threshold_pct,
        )
        regime_valuation_interaction_df = _build_regime_valuation_interaction_df(
            ctx.connection,
            horizons=resolved_horizons,
            min_observations=min_observations,
            tail_return_threshold_pct=tail_return_threshold_pct,
        )
        technical_atr_short_interaction_df = _build_technical_atr_short_interaction_df(
            ctx.connection,
            horizons=resolved_horizons,
            min_observations=min_observations,
            tail_return_threshold_pct=tail_return_threshold_pct,
        )
        stale_liquidity_short_diagnostics_df = _build_stale_liquidity_short_diagnostics_df(
            ctx.connection,
            horizons=resolved_horizons,
            min_observations=min_observations,
            tail_return_threshold_pct=tail_return_threshold_pct,
        )
        stale_high_valuation_trend_split_df = (
            _build_stale_high_valuation_trend_split_df(
                ctx.connection,
                horizons=resolved_horizons,
                min_observations=min_observations,
                tail_return_threshold_pct=tail_return_threshold_pct,
            )
        )
        live_ranking_replay_df = _query_live_ranking_replay_df(
            ctx.connection,
            limit=observation_sample_limit,
        )
        observation_sample_df = _query_observation_sample_df(
            ctx.connection,
            limit=observation_sample_limit,
        )
        source_mode = ctx.source_mode
        source_detail = ctx.source_detail

    return RankingShortRedEvidenceResult(
        db_path=str(db_path_obj),
        source_mode=source_mode,
        source_detail=source_detail,
        market_source=market_source,
        analysis_start_date=_analysis_start_from_sample(observation_sample_df, start_date),
        analysis_end_date=end_date,
        horizons=resolved_horizons,
        market_scopes=resolved_market_scopes,
        min_observations=min_observations,
        tail_return_threshold_pct=tail_return_threshold_pct,
        observation_count=observation_count,
        coverage_diagnostics_df=coverage_diagnostics_df,
        short_red_candidate_df=short_red_candidate_df,
        regime_valuation_interaction_df=regime_valuation_interaction_df,
        technical_atr_short_interaction_df=technical_atr_short_interaction_df,
        stale_liquidity_short_diagnostics_df=stale_liquidity_short_diagnostics_df,
        stale_high_valuation_trend_split_df=stale_high_valuation_trend_split_df,
        live_ranking_replay_df=live_ranking_replay_df,
        observation_sample_df=observation_sample_df,
    )


def write_ranking_short_red_evidence_bundle(
    result: RankingShortRedEvidenceResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    return write_research_bundle(
        experiment_id=RANKING_SHORT_RED_EVIDENCE_EXPERIMENT_ID,
        module=__name__,
        function="run_ranking_short_red_evidence_research",
        params={
            "horizons": list(result.horizons),
            "market_scopes": list(result.market_scopes),
            "min_observations": result.min_observations,
            "tail_return_threshold_pct": result.tail_return_threshold_pct,
        },
        db_path=result.db_path,
        analysis_start_date=result.analysis_start_date,
        analysis_end_date=result.analysis_end_date,
        result_metadata={
            "source_mode": result.source_mode,
            "source_detail": result.source_detail,
            "market_source": result.market_source,
            "observation_count": result.observation_count,
            "observation_sample_count": int(len(result.observation_sample_df)),
        },
        result_tables={
            "coverage_diagnostics_df": result.coverage_diagnostics_df,
            "short_red_candidate_df": result.short_red_candidate_df,
            "regime_valuation_interaction_df": result.regime_valuation_interaction_df,
            "technical_atr_short_interaction_df": (
                result.technical_atr_short_interaction_df
            ),
            "stale_liquidity_short_diagnostics_df": (
                result.stale_liquidity_short_diagnostics_df
            ),
            "stale_high_valuation_trend_split_df": (
                result.stale_high_valuation_trend_split_df
            ),
            "live_ranking_replay_df": result.live_ranking_replay_df,
            "observation_sample_df": result.observation_sample_df,
        },
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def build_summary_markdown(result: RankingShortRedEvidenceResult) -> str:
    coverage = _top_rows_for_markdown(result.coverage_diagnostics_df, limit=24)
    candidates = _top_rows_for_markdown(
        result.short_red_candidate_df,
        sort_columns=["market_scope", "candidate_bucket_order", "horizon"],
        limit=80,
    )
    regime_valuation = _top_rows_for_markdown(
        result.regime_valuation_interaction_df,
        sort_columns=[
            "market_scope",
            "liquidity_regime_order",
            "valuation_state_order",
            "horizon",
        ],
        limit=100,
    )
    technical = _top_rows_for_markdown(
        result.technical_atr_short_interaction_df,
        sort_columns=[
            "market_scope",
            "candidate_bucket_order",
            "technical_state_order",
            "horizon",
        ],
        limit=120,
    )
    stale = _top_rows_for_markdown(
        result.stale_liquidity_short_diagnostics_df,
        sort_columns=["market_scope", "stale_condition_order", "horizon"],
        limit=80,
    )
    stale_trend_split = _top_rows_for_markdown(
        result.stale_high_valuation_trend_split_df,
        sort_columns=["market_scope", "trend_split_order", "horizon"],
        limit=80,
    )
    replay = _top_rows_for_markdown(result.live_ranking_replay_df, limit=60)
    return "\n".join(
        [
            "# Ranking Short Red Evidence",
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
            coverage,
            "",
            "## Short Red Candidates",
            "",
            candidates,
            "",
            "## Regime x Valuation Interaction",
            "",
            regime_valuation,
            "",
            "## Technical ATR Short Interaction",
            "",
            technical,
            "",
            "## Stale Liquidity Short Diagnostics",
            "",
            stale,
            "",
            "## Stale High Valuation Trend Split",
            "",
            stale_trend_split,
            "",
            "## Live Ranking Replay",
            "",
            replay,
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


def _create_feature_panel(conn: Any) -> None:
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranking_short_red_feature_panel AS
        SELECT
            r.*,
            r.liquidity_scope AS liquidity_regime,
            a.atr20_pct,
            a.atr60_pct,
            a.atr20_to_atr60,
            a.atr20_change_20d_pct,
            CASE
                WHEN r.pbr_percentile <= 0.2 AND r.forward_per_percentile <= 0.2
                    THEN TRUE
                WHEN r.per_percentile <= 0.2 AND r.forward_per_to_per_ratio <= 0.8
                    THEN TRUE
                ELSE FALSE
            END AS strong_value_confirmation,
            CASE
                WHEN r.pbr_percentile <= 0.2
                    THEN TRUE
                WHEN r.per_percentile <= 0.2 AND r.forward_per_to_per_ratio <= 1.0
                    THEN TRUE
                ELSE FALSE
            END AS medium_value_confirmation,
            CASE
                WHEN r.per_percentile >= 0.8 THEN TRUE
                WHEN r.forward_per_percentile >= 0.8 THEN TRUE
                WHEN r.forward_p_op_percentile >= 0.8 THEN TRUE
                WHEN r.pbr_percentile >= 0.8 THEN TRUE
                ELSE FALSE
            END AS high_valuation_percentile,
            CASE
                WHEN r.per_percentile IS NULL AND r.forward_per_percentile IS NULL
                    THEN TRUE
                ELSE FALSE
            END AS missing_earnings_warning,
            CASE
                WHEN r.recent_return_20d_pct <= 0 OR r.recent_return_60d_pct <= 0
                    THEN TRUE
                ELSE FALSE
            END AS weak_trend
        FROM {DAILY_RANKING_RESEARCH_LIQUIDITY_RANKED_TABLE} r
        JOIN atr_expansion_scoped a
          ON a.code = r.code
         AND a.date = r.date
         AND a.market_scope = r.market_scope
        WHERE r.liquidity_scope IN (
            'crowded_rerating',
            'distribution_stress',
            'stale_liquidity',
            'neutral_rerating',
            'neutral'
        )
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE ranking_short_red_feature_panel AS
        SELECT
            *,
            high_valuation_percentile OR missing_earnings_warning
                AS high_valuation_warning,
            NOT medium_value_confirmation AS no_value_confirmation,
            atr20_change_20d_pct >= 25.0 AND atr20_to_atr60 < 1.25
                AS atr20_acceleration,
            atr20_change_20d_pct >= 25.0 AND atr20_to_atr60 >= 1.25
                AS atr20_to_atr60_overheat
        FROM ranking_short_red_feature_panel
        """
    )


def _create_candidate_work(conn: Any) -> None:
    selects = []
    for order, (bucket, condition) in enumerate(_CANDIDATE_BUCKETS):
        selects.append(
            f"""
            SELECT
                *,
                '{bucket}' AS candidate_bucket,
                {order} AS candidate_bucket_order
            FROM ranking_short_red_feature_panel
            WHERE {condition}
            """
        )
    conn.execute(
        "CREATE OR REPLACE TEMP TABLE ranking_short_red_candidate_work AS\n"
        + "\nUNION ALL\n".join(selects)
    )


def _build_coverage_diagnostics_df(conn: Any) -> pd.DataFrame:
    frame = conn.execute(
        """
        SELECT
            market_scope,
            count(*) AS observation_count,
            count(DISTINCT code) AS code_count,
            count(DISTINCT date) AS date_count,
            min(date) AS min_date,
            max(date) AS max_date,
            avg(CASE WHEN high_valuation_warning THEN 1.0 ELSE 0.0 END) * 100.0
                AS high_valuation_warning_rate_pct,
            avg(CASE WHEN missing_earnings_warning THEN 1.0 ELSE 0.0 END) * 100.0
                AS missing_earnings_warning_rate_pct,
            avg(CASE WHEN weak_trend THEN 1.0 ELSE 0.0 END) * 100.0
                AS weak_trend_rate_pct,
            avg(CASE WHEN atr20_pct IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100.0
                AS atr20_coverage_pct,
            avg(CASE WHEN atr20_change_20d_pct IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100.0
                AS atr20_change_20d_coverage_pct
        FROM ranking_short_red_feature_panel
        GROUP BY market_scope
        """
    ).fetchdf()
    return _sort_summary_df(frame, columns=list(frame.columns))


def _build_short_red_candidate_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    tail_return_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for horizon in horizons:
        frames.append(
            _aggregate_condition(
                conn,
                source_name="ranking_short_red_candidate_work",
                condition="TRUE",
                condition_fields={"horizon": int(horizon)},
                horizon=int(horizon),
                group_columns=[
                    "market_scope",
                    "candidate_bucket",
                    "candidate_bucket_order",
                ],
                min_observations=min_observations,
                tail_return_threshold_pct=tail_return_threshold_pct,
            )
        )
    return _concat_sorted(frames, columns=_short_red_candidate_columns())


def _build_regime_valuation_interaction_df(
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
        for value_order, (valuation_state, condition) in enumerate(_VALUATION_STATES):
            for horizon in horizons:
                frames.append(
                    _aggregate_condition(
                        conn,
                        source_name="ranking_short_red_feature_panel",
                        condition=f"liquidity_regime = '{regime}' AND ({condition})",
                        condition_fields={
                            "liquidity_regime_order": regime_order,
                            "valuation_state": valuation_state,
                            "valuation_state_order": value_order,
                            "horizon": int(horizon),
                        },
                        horizon=int(horizon),
                        group_columns=["market_scope", "liquidity_regime"],
                        min_observations=min_observations,
                        tail_return_threshold_pct=tail_return_threshold_pct,
                    )
                )
    return _concat_sorted(frames, columns=_regime_valuation_columns())


def _build_technical_atr_short_interaction_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    tail_return_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for technical_order, (technical_state, condition) in enumerate(_TECHNICAL_STATES):
        for horizon in horizons:
            frames.append(
                _aggregate_condition(
                    conn,
                    source_name="ranking_short_red_candidate_work",
                    condition=condition,
                    condition_fields={
                        "technical_state": technical_state,
                        "technical_state_order": technical_order,
                        "horizon": int(horizon),
                    },
                    horizon=int(horizon),
                    group_columns=[
                        "market_scope",
                        "candidate_bucket",
                        "candidate_bucket_order",
                    ],
                    min_observations=min_observations,
                    tail_return_threshold_pct=tail_return_threshold_pct,
                )
            )
    return _concat_sorted(frames, columns=_technical_atr_columns())


def _build_stale_liquidity_short_diagnostics_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    tail_return_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for condition_order, (stale_condition, condition) in enumerate(_STALE_CONDITIONS):
        for horizon in horizons:
            frames.append(
                _aggregate_condition(
                    conn,
                    source_name="ranking_short_red_feature_panel",
                    condition=condition,
                    condition_fields={
                        "stale_condition": stale_condition,
                        "stale_condition_order": condition_order,
                        "horizon": int(horizon),
                    },
                    horizon=int(horizon),
                    group_columns=["market_scope"],
                    min_observations=min_observations,
                    tail_return_threshold_pct=tail_return_threshold_pct,
                )
            )
    return _concat_sorted(frames, columns=_stale_diagnostics_columns())


def _build_stale_high_valuation_trend_split_df(
    conn: Any,
    *,
    horizons: Sequence[int],
    min_observations: int,
    tail_return_threshold_pct: float,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    base_condition = "liquidity_regime = 'stale_liquidity' AND high_valuation_warning"
    for split_order, (trend_split, condition) in enumerate(
        _STALE_HIGH_VALUATION_TREND_SPLITS
    ):
        for horizon in horizons:
            frames.append(
                _aggregate_condition(
                    conn,
                    source_name="ranking_short_red_feature_panel",
                    condition=f"{base_condition} AND ({condition})",
                    condition_fields={
                        "trend_split": trend_split,
                        "trend_split_order": split_order,
                        "horizon": int(horizon),
                    },
                    horizon=int(horizon),
                    group_columns=["market_scope"],
                    min_observations=min_observations,
                    tail_return_threshold_pct=tail_return_threshold_pct,
                )
            )
    return _concat_sorted(frames, columns=_stale_high_valuation_trend_split_columns())


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
    ordered.extend(_short_red_metric_columns())
    return frame.reindex(columns=ordered)


def _query_live_ranking_replay_df(conn: Any, *, limit: int) -> pd.DataFrame:
    return conn.execute(
        """
        WITH latest_dates AS (
            SELECT market_scope, max(date) AS max_date
            FROM ranking_short_red_candidate_work
            GROUP BY market_scope
        )
        SELECT
            c.date,
            c.market_scope,
            c.code,
            c.company_name,
            c.candidate_bucket,
            c.candidate_bucket_order,
            c.liquidity_regime,
            c.recent_return_20d_pct,
            c.recent_return_60d_pct,
            c.liquidity_residual_z,
            c.per_percentile,
            c.forward_per_percentile,
            c.forward_p_op_percentile,
            c.pbr_percentile,
            c.atr20_pct,
            c.atr60_pct,
            c.atr20_to_atr60,
            c.atr20_change_20d_pct,
            c.forward_close_return_20d_pct,
            c.topix_close_return_20d_pct,
            c.forward_close_excess_return_20d_pct
        FROM ranking_short_red_candidate_work c
        JOIN latest_dates d
          ON d.market_scope = c.market_scope
         AND d.max_date = c.date
        ORDER BY c.market_scope, c.candidate_bucket_order, c.code
        LIMIT ?
        """,
        [int(limit)],
    ).fetchdf()


def _query_observation_sample_df(conn: Any, *, limit: int) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT
            date,
            market_scope,
            code,
            company_name,
            liquidity_regime,
            recent_return_20d_pct,
            recent_return_60d_pct,
            topix_recent_return_20d_pct,
            topix_recent_return_60d_pct,
            med_adv60_jpy / 1000000.0 AS med_adv60_mil_jpy,
            liquidity_residual_z,
            per_percentile,
            forward_per_percentile,
            forward_p_op_percentile,
            pbr_percentile,
            high_valuation_warning,
            missing_earnings_warning,
            weak_trend,
            atr20_pct,
            atr60_pct,
            atr20_to_atr60,
            atr20_change_20d_pct,
            forward_close_return_20d_pct,
            topix_close_return_20d_pct,
            forward_close_excess_return_20d_pct
        FROM ranking_short_red_feature_panel
        ORDER BY date, code, liquidity_regime
        LIMIT ?
        """,
        [int(limit)],
    ).fetchdf()


def _analysis_start_from_sample(frame: pd.DataFrame, fallback: str | None) -> str | None:
    if frame.empty or "date" not in frame:
        return fallback
    value = frame["date"].min()
    if pd.isna(value):
        return fallback
    return str(value)


def _short_red_metric_columns() -> list[str]:
    return [
        "observation_count",
        "code_count",
        "date_count",
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
        "median_atr20_pct",
        "median_atr60_pct",
        "median_atr20_to_atr60",
        "median_atr20_change_20d_pct",
        "atr20_acceleration_rate_pct",
        "atr20_to_atr60_overheat_rate_pct",
    ]


def _short_red_candidate_columns() -> list[str]:
    return [
        "horizon",
        "market_scope",
        "candidate_bucket",
        "candidate_bucket_order",
        *_short_red_metric_columns(),
    ]


def _regime_valuation_columns() -> list[str]:
    return [
        "liquidity_regime_order",
        "valuation_state",
        "valuation_state_order",
        "horizon",
        "market_scope",
        "liquidity_regime",
        *_short_red_metric_columns(),
    ]


def _technical_atr_columns() -> list[str]:
    return [
        "technical_state",
        "technical_state_order",
        "horizon",
        "market_scope",
        "candidate_bucket",
        "candidate_bucket_order",
        *_short_red_metric_columns(),
    ]


def _stale_diagnostics_columns() -> list[str]:
    return [
        "stale_condition",
        "stale_condition_order",
        "horizon",
        "market_scope",
        *_short_red_metric_columns(),
    ]


def _stale_high_valuation_trend_split_columns() -> list[str]:
    return [
        "trend_split",
        "trend_split_order",
        "horizon",
        "market_scope",
        *_short_red_metric_columns(),
    ]


def _concat_sorted(frames: Sequence[pd.DataFrame], *, columns: Sequence[str]) -> pd.DataFrame:
    non_empty = [frame for frame in frames if not frame.empty]
    if not non_empty:
        return pd.DataFrame(columns=list(columns))
    frame = pd.concat(non_empty, ignore_index=True)
    return frame.reindex(columns=list(columns))
