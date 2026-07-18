"""Conditional lift of trend acceleration inside existing Ranking candidates."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Literal, Sequence, cast

import numpy as np
import pandas as pd

from src.domains.analytics.atr_expansion_forward_response import (
    _create_observation_panel as _create_atr_observation_panel,
)
from src.domains.analytics.daily_ranking_research_base import (
    DAILY_RANKING_RESEARCH_RANKED_TABLE,
    create_daily_ranking_research_panel,
    daily_ranking_query_end_date,
    daily_ranking_query_start_date,
)
from src.domains.analytics.ranking_long_sector_leadership_horizon_decomposition import (
    _create_long_sector_leadership_tables,
    _create_long_signal_tables,
)
from src.domains.analytics.earnings_holdthrough_expectancy_report import (
    _top_rows_for_markdown,
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
    require_market_v4_compatibility,
)
from src.domains.analytics.research_bundle import ResearchBundleInfo, write_research_bundle
from src.domains.analytics.ranking_technical_fit_price_projection import (
    EventTimePriceAudit,
    create_event_time_price_relations,
)
from src.shared.utils.market_code_alias import MARKET_CODES_BY_SCOPE

CandidateRole = Literal[
    "primary",
    "broad_sensitivity",
    "nested_sensitivity",
    "secondary_20d",
]


@dataclass(frozen=True)
class CandidateDefinition:
    name: str
    predicate: str
    role: CandidateRole


_DEEP_VALUE = "valuation_signal = 'strong_value_confirmation'"
_NEUTRAL = "liquidity_regime = 'neutral_rerating'"
_CROWDED = "liquidity_regime = 'crowded_rerating'"
# The candidate panel exposes this semantic alias so the frozen registry itself
# contains no accidental ``r2`` token from the production name ``atr20``.
_ATR_ACCEL_EX_OVERHEAT = "atr_acceleration_ex_overheat_flag"
_MOMENTUM = "momentum_20_60_top20_flag"
_Z_MINUS1_TO_2 = "liquidity_residual_z >= -1.0 AND liquidity_residual_z <= 2.0"
_Z_1_TO_2 = "liquidity_residual_z >= 1.0 AND liquidity_residual_z <= 2.0"

CANDIDATE_REGISTRY: tuple[CandidateDefinition, ...] = (
    CandidateDefinition(
        "core_long",
        f"{_NEUTRAL} AND {_DEEP_VALUE} AND {_ATR_ACCEL_EX_OVERHEAT} "
        f"AND {_Z_MINUS1_TO_2}",
        "primary",
    ),
    CandidateDefinition(
        "momentum_value",
        f"{_NEUTRAL} AND {_DEEP_VALUE} AND {_MOMENTUM} AND {_Z_MINUS1_TO_2}",
        "primary",
    ),
    CandidateDefinition(
        "neutral_rerating_good",
        f"{_NEUTRAL} AND {_DEEP_VALUE}",
        "broad_sensitivity",
    ),
    CandidateDefinition(
        "earnings_priority",
        f"{_NEUTRAL} AND {_DEEP_VALUE} AND {_ATR_ACCEL_EX_OVERHEAT} "
        f"AND {_Z_MINUS1_TO_2} AND forecast_operating_profit_growth_ratio >= 1.2",
        "nested_sensitivity",
    ),
    CandidateDefinition(
        "aggressive_rerating",
        f"{_CROWDED} AND {_DEEP_VALUE} AND {_ATR_ACCEL_EX_OVERHEAT} "
        f"AND {_Z_1_TO_2}",
        "secondary_20d",
    ),
)

SEGMENTS: tuple[tuple[str, date, date | None], ...] = (
    ("historical_pre_reorg", date(2017, 1, 1), date(2021, 12, 31)),
    ("historical_post_reorg", date(2022, 1, 1), date(2023, 12, 31)),
    ("recent_hypothesis_origin", date(2024, 1, 1), None),
)

PRIME_EQUIVALENT_MARKET_CODES: tuple[str, ...] = tuple(
    code for code in MARKET_CODES_BY_SCOPE["prime"] if code.isdigit()
)
if set(PRIME_EQUIVALENT_MARKET_CODES) != {"0101", "0111"}:
    raise RuntimeError("prime research must resolve to exact-date 0101/0111 membership")

RANKING_TREND_ACCELERATION_CONDITIONAL_LIFT_EXPERIMENT_ID = (
    "market-behavior/ranking-trend-acceleration-conditional-lift"
)
DEFAULT_HORIZONS: tuple[int, ...] = (5, 20, 60)
DEFAULT_MIN_OBSERVATIONS = 300
DEFAULT_BOOTSTRAP_RESAMPLES = 2_000
DEFAULT_BOOTSTRAP_SEED = 17
DEFAULT_OBSERVATION_SAMPLE_LIMIT = 10_000
DEFAULT_SEVERE_LOSS_THRESHOLD_PCT = -10.0
_WARMUP_CALENDAR_DAYS = 820
_LEADERSHIP_WINDOWS: tuple[int, ...] = (120, 252, 504)
_REQUIRED_ATR_WINDOWS: tuple[int, ...] = (20, 60)
_REQUIRED_RETURN_WINDOWS: tuple[int, ...] = (20, 60)
_REQUIRED_MARKET_TABLES = {
    "stock_data_raw",
    "topix_data",
    "daily_valuation",
    "stock_master_daily",
    "indices_data",
    "index_master",
    "stock_adjustment_bases",
    "stock_adjustment_basis_segments",
}


@dataclass(frozen=True)
class RankingTrendAccelerationConditionalLiftResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    market_source: str
    analysis_start_date: str | None
    analysis_end_date: str | None
    horizons: tuple[int, ...]
    min_observations: int
    bootstrap_resamples: int
    bootstrap_seed: int
    severe_loss_threshold_pct: float
    observation_count: int
    price_projection: EventTimePriceAudit
    coverage_diagnostics_df: pd.DataFrame
    candidate_registry_df: pd.DataFrame
    conditional_binary_lift_df: pd.DataFrame
    fixed_incremental_2x2_df: pd.DataFrame
    continuous_rank_lift_df: pd.DataFrame
    topk_priority_lift_df: pd.DataFrame
    segment_stability_df: pd.DataFrame
    bootstrap_effect_ci_df: pd.DataFrame
    decision_gate_df: pd.DataFrame
    observation_sample_df: pd.DataFrame


def classify_trend_acceleration_triple(
    slope_20_pct: float | None,
    slope_60_pct: float | None,
) -> bool:
    """Return the frozen positive-positive-and-accelerating binary feature."""

    if slope_20_pct is None or slope_60_pct is None:
        return False
    return slope_20_pct > 0.0 and slope_60_pct > 0.0 and slope_20_pct > slope_60_pct


def run_ranking_trend_acceleration_conditional_lift_research(
    db_path: str | Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    min_observations: int = DEFAULT_MIN_OBSERVATIONS,
    bootstrap_resamples: int = DEFAULT_BOOTSTRAP_RESAMPLES,
    bootstrap_seed: int = DEFAULT_BOOTSTRAP_SEED,
    observation_sample_limit: int = DEFAULT_OBSERVATION_SAMPLE_LIMIT,
) -> RankingTrendAccelerationConditionalLiftResult:
    """Run the fixed, Prime-only conditional-lift experiment."""

    resolved_horizons = tuple(sorted({int(horizon) for horizon in horizons}))
    _validate_params(
        start_date=start_date,
        end_date=end_date,
        horizons=resolved_horizons,
        min_observations=min_observations,
        bootstrap_resamples=bootstrap_resamples,
        observation_sample_limit=observation_sample_limit,
    )
    db_path_obj = Path(db_path).expanduser().resolve()
    if not db_path_obj.is_file():
        raise FileNotFoundError(f"market.duckdb was not found: {db_path_obj}")

    query_start = daily_ranking_query_start_date(
        start_date,
        warmup_calendar_days=max(_WARMUP_CALENDAR_DAYS, max(_LEADERSHIP_WINDOWS) * 3),
    )
    query_end = daily_ranking_query_end_date(
        end_date,
        max_horizon=max(resolved_horizons),
    )
    market_source = "stock_master_daily_exact_date"
    with open_readonly_analysis_connection(
        str(db_path_obj),
        snapshot_prefix="ranking-trend-acceleration-conditional-lift-",
    ) as ctx:
        require_market_v4_compatibility(
            ctx.connection,
            required_tables=_REQUIRED_MARKET_TABLES,
        )
        price_relations, price_projection = create_event_time_price_relations(
            ctx.connection,
            query_start=query_start,
            query_end=query_end,
            analysis_start_date=start_date,
            analysis_end_date=end_date,
            horizons=resolved_horizons,
        )
        create_daily_ranking_research_panel(
            ctx.connection,
            query_start=query_start,
            query_end=query_end,
            analysis_start_date=start_date,
            analysis_end_date=end_date,
            horizons=resolved_horizons,
            market_scopes=("prime",),
            market_source=market_source,
            include_liquidity_ranked=True,
            include_relation_percentiles=True,
            event_time_basis_only=True,
            price_feature_relation=price_relations.signal_features,
            price_outcome_relation=price_relations.forward_outcomes,
        )
        _create_sector_strength_tables(ctx.connection, horizons=resolved_horizons)
        _create_long_sector_leadership_tables(
            ctx.connection,
            leadership_windows=_LEADERSHIP_WINDOWS,
            stock_return_relation=price_relations.signal_features,
        )
        _create_long_signal_tables(
            ctx.connection,
            leadership_windows=_LEADERSHIP_WINDOWS,
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
            market_scopes=("prime",),
            price_feature_relation=price_relations.signal_features,
            price_outcome_relation=price_relations.forward_outcomes,
        )
        _create_short_red_feature_panel(ctx.connection)
        _create_candidate_base_panel(ctx.connection)
        _create_trend_feature_table(
            ctx.connection,
            price_feature_relation=price_relations.signal_features,
        )
        observations = _build_candidate_observations(
            ctx.connection,
            horizons=resolved_horizons,
        )

        coverage_df = _build_coverage_diagnostics(observations)
        registry_df = _build_candidate_registry_df(observations)
        conditional_binary_df = _build_conditional_binary_lift_df(
            observations,
            horizons=resolved_horizons,
            severe_loss_threshold_pct=DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
        )
        fixed_2x2_df = _build_fixed_incremental_2x2_df(
            observations,
            horizons=resolved_horizons,
            severe_loss_threshold_pct=DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
        )
        continuous_df = _build_continuous_rank_lift_df(
            observations,
            horizons=resolved_horizons,
            severe_loss_threshold_pct=DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
        )
        topk_df = _build_topk_priority_lift_df(
            observations,
            horizons=resolved_horizons,
            severe_loss_threshold_pct=DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
        )
        segment_df = _build_segment_stability_df(
            conditional_binary_df,
            fixed_2x2_df,
            continuous_df,
            min_observations=min_observations,
        )
        bootstrap_df = _build_bootstrap_effect_ci_df(
            conditional_binary_df,
            fixed_2x2_df,
            continuous_df,
            topk_df,
            resamples=bootstrap_resamples,
            seed=bootstrap_seed,
        )
        decision_df = _build_decision_gate_df(
            coverage_df,
            segment_df,
            bootstrap_df,
            conditional_binary_df,
        )
        sample = observations.head(int(observation_sample_limit)).copy()
        result = RankingTrendAccelerationConditionalLiftResult(
            db_path=str(db_path_obj),
            source_mode=ctx.source_mode,
            source_detail=ctx.source_detail,
            market_source=market_source,
            analysis_start_date=start_date,
            analysis_end_date=end_date,
            horizons=resolved_horizons,
            min_observations=int(min_observations),
            bootstrap_resamples=int(bootstrap_resamples),
            bootstrap_seed=int(bootstrap_seed),
            severe_loss_threshold_pct=DEFAULT_SEVERE_LOSS_THRESHOLD_PCT,
            observation_count=len(observations),
            price_projection=price_projection,
            coverage_diagnostics_df=coverage_df,
            candidate_registry_df=registry_df,
            conditional_binary_lift_df=conditional_binary_df,
            fixed_incremental_2x2_df=fixed_2x2_df,
            continuous_rank_lift_df=continuous_df,
            topk_priority_lift_df=topk_df,
            segment_stability_df=segment_df,
            bootstrap_effect_ci_df=bootstrap_df,
            decision_gate_df=decision_df,
            observation_sample_df=sample,
        )
    return result


def write_ranking_trend_acceleration_conditional_lift_bundle(
    result: RankingTrendAccelerationConditionalLiftResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    """Write the canonical manifest/results/summary research bundle."""

    return write_research_bundle(
        experiment_id=RANKING_TREND_ACCELERATION_CONDITIONAL_LIFT_EXPERIMENT_ID,
        module=(
            "src.domains.analytics."
            "ranking_trend_acceleration_conditional_lift"
        ),
        function="run_ranking_trend_acceleration_conditional_lift_research",
        params={
            "horizons": list(result.horizons),
            "market_scope": "prime",
            "market_codes": list(PRIME_EQUIVALENT_MARKET_CODES),
            "min_observations": result.min_observations,
            "bootstrap_resamples": result.bootstrap_resamples,
            "bootstrap_seed": result.bootstrap_seed,
            "severe_loss_threshold_pct": result.severe_loss_threshold_pct,
            "candidate_groups": [candidate.name for candidate in CANDIDATE_REGISTRY],
        },
        db_path=result.db_path,
        analysis_start_date=result.analysis_start_date,
        analysis_end_date=result.analysis_end_date,
        result_metadata={
            "source_mode": str(result.source_mode),
            "source_detail": result.source_detail,
            "market_source": result.market_source,
            "observation_count": result.observation_count,
            "feature_timing": "after_close",
            "primary_horizon": 20,
            "slope_definition": (
                "rolling OLS on adjusted log(close); "
                "exp(beta * (window - 1)) - 1"
            ),
            "price_projection": result.price_projection.to_manifest_payload(),
        },
        result_tables={
            "coverage_diagnostics_df": result.coverage_diagnostics_df,
            "candidate_registry_df": result.candidate_registry_df,
            "conditional_binary_lift_df": result.conditional_binary_lift_df,
            "fixed_incremental_2x2_df": result.fixed_incremental_2x2_df,
            "continuous_rank_lift_df": result.continuous_rank_lift_df,
            "topk_priority_lift_df": result.topk_priority_lift_df,
            "segment_stability_df": result.segment_stability_df,
            "bootstrap_effect_ci_df": result.bootstrap_effect_ci_df,
            "decision_gate_df": result.decision_gate_df,
            "observation_sample_df": result.observation_sample_df,
        },
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def build_summary_markdown(
    result: RankingTrendAccelerationConditionalLiftResult,
) -> str:
    parts = [
        "# Ranking Trend Acceleration Conditional Lift",
        "",
        "## Metadata",
        "",
        f"- db_path: `{result.db_path}`",
        f"- source_mode: `{result.source_mode}`",
        f"- source_detail: `{result.source_detail}`",
        f"- market_source: `{result.market_source}`",
        "- market_scope: `prime` (`0101`, `0111`, exact signal date)",
        f"- analysis_start_date: `{result.analysis_start_date}`",
        f"- analysis_end_date: `{result.analysis_end_date}`",
        f"- horizons: `{', '.join(str(item) for item in result.horizons)}`",
        f"- observation_count: `{result.observation_count}`",
        "- timing: `after_close`",
        "- Physical price source: `stock_data_raw`",
        "- `stock_data` fallback: `false`",
        f"- Signal price policy: `{result.price_projection.signal_basis_policy}`",
        f"- Outcome price policy: `{result.price_projection.completion_basis_policy}`",
        "- Price projection rows: canonical raw "
        f"`{result.price_projection.canonical_raw_row_count}` / signal features "
        f"`{result.price_projection.signal_feature_row_count}` / completed outcomes "
        f"`{result.price_projection.completed_outcome_row_count}` / signal basis "
        f"`{result.price_projection.signal_basis_row_count}` / signal segments "
        f"`{result.price_projection.signal_segment_row_count}` / completion basis "
        f"`{result.price_projection.completion_basis_row_count}` / completion segments "
        f"`{result.price_projection.completion_segment_row_count}`",
        f"- Signal basis SHA-256: `{result.price_projection.signal_basis_sha256}`",
        f"- Signal segment SHA-256: `{result.price_projection.signal_segment_sha256}`",
        f"- Completion basis SHA-256: `{result.price_projection.completion_basis_sha256}`",
        f"- Completion segment SHA-256: `{result.price_projection.completion_segment_sha256}`",
        f"- Forward outcome SHA-256: `{result.price_projection.forward_outcome_sha256}`",
        f"- Price projection SHA-256: `{result.price_projection.price_projection_sha256}`",
    ]
    sections = (
        ("Coverage Diagnostics", result.coverage_diagnostics_df, 80),
        ("Candidate Registry", result.candidate_registry_df, 40),
        ("Conditional Binary Lift", result.conditional_binary_lift_df, 160),
        ("Fixed Incremental 2x2", result.fixed_incremental_2x2_df, 160),
        ("Continuous Rank Lift", result.continuous_rank_lift_df, 160),
        ("Top-K Priority Lift", result.topk_priority_lift_df, 160),
        ("Segment Stability", result.segment_stability_df, 160),
        ("Bootstrap Effect CI", result.bootstrap_effect_ci_df, 160),
        ("Decision Gate", result.decision_gate_df, 80),
        ("Observation Sample", result.observation_sample_df, 40),
    )
    for title, frame, limit in sections:
        parts.extend(
            [
                "",
                f"## {title}",
                "",
                _top_rows_for_markdown(frame, limit=limit),
            ]
        )
    return "\n".join(parts).rstrip() + "\n"


def _create_candidate_base_panel(conn: Any) -> None:
    prime_codes_sql = ", ".join(f"'{code}'" for code in PRIME_EQUIVALENT_MARKET_CODES)
    candidate_columns = ",\n            ".join(
        f"coalesce(({candidate.predicate}), FALSE) AS {candidate.name}_flag"
        for candidate in CANDIDATE_REGISTRY
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranking_trend_acceleration_technical_base AS
        SELECT
            r.*,
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
            ) AS atr_acceleration_ex_overheat_flag
        FROM {DAILY_RANKING_RESEARCH_RANKED_TABLE} r
        LEFT JOIN long_sector_leadership_base_panel l
          ON l.code = r.code
         AND l.date = r.date
         AND l.market_scope = r.market_scope
        LEFT JOIN ranking_short_red_feature_panel s
          ON s.code = r.code
         AND s.date = r.date
         AND s.market_scope = r.market_scope
        WHERE r.market_scope = 'prime'
          AND r.market_code IN ({prime_codes_sql})
        """
    )
    # Candidate membership is frozen in this table before any OLS feature joins.
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranking_trend_acceleration_candidate_base AS
        SELECT
            *,
            {candidate_columns}
        FROM ranking_trend_acceleration_technical_base
        """
    )


def _create_trend_feature_table(
    conn: Any,
    *,
    price_feature_relation: str,
) -> None:
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranking_trend_acceleration_features AS
        SELECT
            code,
            date,
            ols_move_20d_pct AS price_lr_slope_20_pct,
            ols_move_60d_pct AS price_lr_slope_60_pct,
            ols_r2_20 AS price_lr_r2_20,
            ols_r2_60 AS price_lr_r2_60
        FROM {price_feature_relation}
        """
    )


def _build_candidate_observations(
    conn: Any,
    *,
    horizons: Sequence[int],
) -> pd.DataFrame:
    horizon_columns = ",\n                ".join(
        f"b.forward_close_excess_return_{int(horizon)}d_pct"
        for horizon in horizons
    )
    named_values = ",\n                    ".join(
        f"('{candidate.name}', "
        f"'{('exclusive_slice' if candidate.name == 'aggressive_rerating' else 'named_group')}', "
        f"f.{candidate.name}_flag)"
        for candidate in CANDIDATE_REGISTRY
    )
    frame = conn.execute(
        f"""
        WITH feature_base AS (
            SELECT
                b.date,
                b.code,
                b.company_name,
                b.market,
                b.market_code,
                b.liquidity_regime,
                b.valuation_signal,
                b.liquidity_residual_z,
                b.recent_return_20d_pct,
                b.recent_return_60d_pct,
                b.forecast_operating_profit_growth_ratio,
                b.core_long_flag,
                b.momentum_value_flag,
                b.neutral_rerating_good_flag,
                b.earnings_priority_flag,
                b.aggressive_rerating_flag,
                f.price_lr_slope_20_pct,
                f.price_lr_slope_60_pct,
                f.price_lr_r2_20,
                f.price_lr_r2_60,
                CASE
                    WHEN f.price_lr_slope_20_pct IS NOT NULL
                     AND f.price_lr_slope_60_pct IS NOT NULL
                        THEN f.price_lr_slope_20_pct - f.price_lr_slope_60_pct
                END AS trend_acceleration_margin_pct,
                coalesce(
                    f.price_lr_slope_20_pct > 0.0
                    AND f.price_lr_slope_60_pct > 0.0
                    AND f.price_lr_slope_20_pct > f.price_lr_slope_60_pct,
                    FALSE
                ) AS trend_acceleration_triple,
                CASE
                    WHEN b.recent_return_20d_pct IS NULL
                      OR b.recent_return_60d_pct IS NULL
                        THEN NULL
                    ELSE b.recent_return_20d_pct > 0.0
                     AND b.recent_return_60d_pct > 0.0
                END AS fixed_dual_positive,
                CASE
                    WHEN b.core_long_flag AND NOT b.momentum_value_flag
                        THEN 'core_long_only'
                    WHEN b.momentum_value_flag AND NOT b.core_long_flag
                        THEN 'momentum_value_only'
                    WHEN b.core_long_flag AND b.momentum_value_flag
                        THEN 'core_momentum_overlap'
                    WHEN b.aggressive_rerating_flag THEN 'aggressive_rerating'
                    WHEN b.neutral_rerating_good_flag
                        THEN 'neutral_good_remainder'
                END AS exclusive_slice,
                {horizon_columns}
            FROM ranking_trend_acceleration_candidate_base b
            LEFT JOIN ranking_trend_acceleration_features f
              ON f.code = b.code
             AND f.date = b.date
        ),
        named_rows AS (
            SELECT f.*, v.candidate_group, v.candidate_kind
            FROM feature_base f
            CROSS JOIN LATERAL (
                VALUES {named_values}
            ) AS v(candidate_group, candidate_kind, matches)
            WHERE v.matches
        ),
        exclusive_rows AS (
            SELECT
                f.*,
                f.exclusive_slice AS candidate_group,
                'exclusive_slice' AS candidate_kind
            FROM feature_base f
            WHERE f.exclusive_slice IS NOT NULL
              AND f.exclusive_slice <> 'aggressive_rerating'
        )
        SELECT * FROM named_rows
        UNION ALL BY NAME
        SELECT * FROM exclusive_rows
        ORDER BY date, code, candidate_kind, candidate_group
        """
    ).fetchdf()
    if frame.empty:
        frame["acceleration_percentile"] = pd.Series(dtype=float)
        frame["acceleration_bucket"] = pd.Series(dtype=str)
        return frame
    frame["date"] = pd.to_datetime(frame["date"]).dt.strftime("%Y-%m-%d")
    frame = _add_candidate_local_percentiles(frame)
    if frame.duplicated(["code", "date", "candidate_group"]).any():
        raise RuntimeError("candidate observations must be unique by code/date/group")
    return frame.reset_index(drop=True)


def _add_candidate_local_percentiles(observations: pd.DataFrame) -> pd.DataFrame:
    """Rank the acceleration margin only within candidate group and signal date."""

    frame = observations.copy()
    eligible = frame["trend_acceleration_margin_pct"].notna()
    frame["acceleration_percentile"] = np.nan
    frame.loc[eligible, "acceleration_percentile"] = (
        frame.loc[eligible]
        .groupby(["candidate_group", "date"])["trend_acceleration_margin_pct"]
        .rank(method="max", pct=True)
    )
    frame["acceleration_bucket"] = np.select(
        [
            frame["acceleration_percentile"] <= 0.2,
            frame["acceleration_percentile"] > 0.8,
        ],
        ["bottom_20", "top_20"],
        default="middle_60",
    )
    frame.loc[~eligible, "acceleration_bucket"] = None
    return frame


def _build_conditional_binary_lift_df(
    observations: pd.DataFrame,
    *,
    horizons: Sequence[int],
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    """Build valid same-day triple/control pairs with at least two symbols per side."""

    columns = [
        "candidate_group",
        "candidate_kind",
        "horizon",
        "paired_date",
        "triple_observation_count",
        "control_observation_count",
        "triple_mean_excess_return_pct",
        "control_mean_excess_return_pct",
        "mean_lift_pct",
        "triple_median_excess_return_pct",
        "control_median_excess_return_pct",
        "median_lift_pct",
        "triple_win_rate_pct",
        "control_win_rate_pct",
        "triple_p10_excess_return_pct",
        "control_p10_excess_return_pct",
        "triple_p25_excess_return_pct",
        "control_p25_excess_return_pct",
        "triple_severe_loss_rate_pct",
        "control_severe_loss_rate_pct",
        "severe_loss_rate_difference_pct",
    ]
    rows: list[dict[str, object]] = []
    if observations.empty:
        return pd.DataFrame(columns=columns)
    for horizon in horizons:
        outcome_column = f"forward_close_excess_return_{int(horizon)}d_pct"
        if outcome_column not in observations:
            continue
        eligible = observations.loc[
            observations["trend_acceleration_margin_pct"].notna()
            & observations[outcome_column].notna()
        ]
        for (candidate_group, candidate_kind, paired_date), group in eligible.groupby(
            ["candidate_group", "candidate_kind", "date"],
            sort=True,
        ):
            triple = group.loc[group["trend_acceleration_triple"], outcome_column].astype(float)
            control = group.loc[~group["trend_acceleration_triple"], outcome_column].astype(float)
            if len(triple) < 2 or len(control) < 2:
                continue
            triple_severe = float((triple <= severe_loss_threshold_pct).mean() * 100.0)
            control_severe = float((control <= severe_loss_threshold_pct).mean() * 100.0)
            rows.append(
                {
                    "candidate_group": candidate_group,
                    "candidate_kind": candidate_kind,
                    "horizon": int(horizon),
                    "paired_date": str(paired_date),
                    "triple_observation_count": len(triple),
                    "control_observation_count": len(control),
                    "triple_mean_excess_return_pct": float(triple.mean()),
                    "control_mean_excess_return_pct": float(control.mean()),
                    "mean_lift_pct": float(triple.mean() - control.mean()),
                    "triple_median_excess_return_pct": float(triple.median()),
                    "control_median_excess_return_pct": float(control.median()),
                    "median_lift_pct": float(triple.median() - control.median()),
                    "triple_win_rate_pct": float((triple > 0).mean() * 100.0),
                    "control_win_rate_pct": float((control > 0).mean() * 100.0),
                    "triple_p10_excess_return_pct": float(triple.quantile(0.1)),
                    "control_p10_excess_return_pct": float(control.quantile(0.1)),
                    "triple_p25_excess_return_pct": float(triple.quantile(0.25)),
                    "control_p25_excess_return_pct": float(control.quantile(0.25)),
                    "triple_severe_loss_rate_pct": triple_severe,
                    "control_severe_loss_rate_pct": control_severe,
                    "severe_loss_rate_difference_pct": triple_severe - control_severe,
                }
            )
    return pd.DataFrame(rows, columns=columns)


def _build_fixed_incremental_2x2_df(
    observations: pd.DataFrame,
    *,
    horizons: Sequence[int],
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    columns = [
        "row_type",
        "comparison",
        "candidate_group",
        "candidate_kind",
        "horizon",
        "date",
        "fixed_dual_positive",
        "trend_acceleration_triple",
        "observation_count",
        "symbol_count",
        "mean_excess_return_pct",
        "median_excess_return_pct",
        "win_rate_pct",
        "p10_excess_return_pct",
        "p25_excess_return_pct",
        "severe_loss_rate_pct",
        "meets_two_symbol_minimum",
        "triple_observation_count",
        "control_observation_count",
        "triple_mean_excess_return_pct",
        "control_mean_excess_return_pct",
        "mean_lift_pct",
        "triple_median_excess_return_pct",
        "control_median_excess_return_pct",
        "median_lift_pct",
        "severe_loss_rate_difference_pct",
    ]
    rows: list[dict[str, object]] = []
    for horizon in horizons:
        outcome = f"forward_close_excess_return_{int(horizon)}d_pct"
        if outcome not in observations:
            continue
        eligible = observations.loc[
            observations[outcome].notna()
            & observations["trend_acceleration_margin_pct"].notna()
            & observations["fixed_dual_positive"].notna()
        ]
        keys = [
            "candidate_group",
            "candidate_kind",
            "date",
            "fixed_dual_positive",
            "trend_acceleration_triple",
        ]
        for key, group in eligible.groupby(keys, sort=True):
            values = group[outcome].astype(float)
            rows.append(
                {
                    "row_type": "cell_2x2",
                    "comparison": "fixed_by_triple_cell",
                    "candidate_group": key[0],
                    "candidate_kind": key[1],
                    "horizon": int(horizon),
                    "date": str(key[2]),
                    "fixed_dual_positive": bool(key[3]),
                    "trend_acceleration_triple": bool(key[4]),
                    "observation_count": len(values),
                    "symbol_count": group["code"].nunique(),
                    "mean_excess_return_pct": float(values.mean()),
                    "median_excess_return_pct": float(values.median()),
                    "win_rate_pct": float((values > 0).mean() * 100.0),
                    "p10_excess_return_pct": float(values.quantile(0.1)),
                    "p25_excess_return_pct": float(values.quantile(0.25)),
                    "severe_loss_rate_pct": float(
                        (values <= severe_loss_threshold_pct).mean() * 100.0
                    ),
                    "meets_two_symbol_minimum": len(values) >= 2,
                }
            )
        fixed_positive = eligible.loc[eligible["fixed_dual_positive"].eq(True)]
        for (candidate_group, candidate_kind, signal_date), group in fixed_positive.groupby(
            ["candidate_group", "candidate_kind", "date"],
            sort=True,
        ):
            triple = group.loc[group["trend_acceleration_triple"], outcome].astype(float)
            control = group.loc[~group["trend_acceleration_triple"], outcome].astype(float)
            if len(triple) < 2 or len(control) < 2:
                continue
            rows.append(
                {
                    "row_type": "fixed_dual_positive_lift",
                    "comparison": "fixed_dual_positive_triple_minus_control",
                    "candidate_group": candidate_group,
                    "candidate_kind": candidate_kind,
                    "horizon": int(horizon),
                    "date": str(signal_date),
                    "fixed_dual_positive": True,
                    "trend_acceleration_triple": None,
                    "observation_count": len(triple) + len(control),
                    "symbol_count": group["code"].nunique(),
                    "meets_two_symbol_minimum": True,
                    "triple_observation_count": len(triple),
                    "control_observation_count": len(control),
                    "triple_mean_excess_return_pct": float(triple.mean()),
                    "control_mean_excess_return_pct": float(control.mean()),
                    "mean_lift_pct": float(triple.mean() - control.mean()),
                    "triple_median_excess_return_pct": float(triple.median()),
                    "control_median_excess_return_pct": float(control.median()),
                    "median_lift_pct": float(triple.median() - control.median()),
                    "severe_loss_rate_difference_pct": float(
                        (triple <= severe_loss_threshold_pct).mean() * 100.0
                        - (control <= severe_loss_threshold_pct).mean() * 100.0
                    ),
                }
            )
    return pd.DataFrame(rows, columns=columns)


def _build_continuous_rank_lift_df(
    observations: pd.DataFrame,
    *,
    horizons: Sequence[int],
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    columns = [
        "candidate_group",
        "candidate_kind",
        "horizon",
        "paired_date",
        "observation_count",
        "symbol_count",
        "bottom_observation_count",
        "middle_observation_count",
        "top_observation_count",
        "bottom_mean_excess_return_pct",
        "middle_mean_excess_return_pct",
        "top_mean_excess_return_pct",
        "top_minus_bottom_lift_pct",
        "bottom_median_excess_return_pct",
        "top_median_excess_return_pct",
        "top_minus_bottom_median_lift_pct",
        "bottom_win_rate_pct",
        "top_win_rate_pct",
        "bottom_p10_excess_return_pct",
        "top_p10_excess_return_pct",
        "bottom_p25_excess_return_pct",
        "top_p25_excess_return_pct",
        "bottom_severe_loss_rate_pct",
        "top_severe_loss_rate_pct",
        "severe_loss_rate_difference_pct",
        "spearman_ic",
    ]
    rows: list[dict[str, object]] = []
    for horizon in horizons:
        outcome = f"forward_close_excess_return_{int(horizon)}d_pct"
        if outcome not in observations:
            continue
        eligible = observations.loc[
            observations["trend_acceleration_margin_pct"].notna()
            & observations[outcome].notna()
        ]
        for (candidate_group, candidate_kind, paired_date), group in eligible.groupby(
            ["candidate_group", "candidate_kind", "date"],
            sort=True,
        ):
            if len(group) < 20:
                continue
            ranked = group.copy()
            ranked["_percentile"] = ranked["trend_acceleration_margin_pct"].rank(
                method="max",
                pct=True,
            )
            bottom = ranked.loc[ranked["_percentile"] <= 0.2, outcome].astype(float)
            middle = ranked.loc[
                (ranked["_percentile"] > 0.2) & (ranked["_percentile"] <= 0.8),
                outcome,
            ].astype(float)
            top = ranked.loc[ranked["_percentile"] > 0.8, outcome].astype(float)
            if bottom.empty or top.empty:
                continue
            top_severe = float((top <= severe_loss_threshold_pct).mean() * 100.0)
            bottom_severe = float((bottom <= severe_loss_threshold_pct).mean() * 100.0)
            rows.append(
                {
                    "candidate_group": candidate_group,
                    "candidate_kind": candidate_kind,
                    "horizon": int(horizon),
                    "paired_date": str(paired_date),
                    "observation_count": len(ranked),
                    "symbol_count": ranked["code"].nunique(),
                    "bottom_observation_count": len(bottom),
                    "middle_observation_count": len(middle),
                    "top_observation_count": len(top),
                    "bottom_mean_excess_return_pct": float(bottom.mean()),
                    "middle_mean_excess_return_pct": float(middle.mean()),
                    "top_mean_excess_return_pct": float(top.mean()),
                    "top_minus_bottom_lift_pct": float(top.mean() - bottom.mean()),
                    "bottom_median_excess_return_pct": float(bottom.median()),
                    "top_median_excess_return_pct": float(top.median()),
                    "top_minus_bottom_median_lift_pct": float(
                        top.median() - bottom.median()
                    ),
                    "bottom_win_rate_pct": float((bottom > 0).mean() * 100.0),
                    "top_win_rate_pct": float((top > 0).mean() * 100.0),
                    "bottom_p10_excess_return_pct": float(bottom.quantile(0.1)),
                    "top_p10_excess_return_pct": float(top.quantile(0.1)),
                    "bottom_p25_excess_return_pct": float(bottom.quantile(0.25)),
                    "top_p25_excess_return_pct": float(top.quantile(0.25)),
                    "bottom_severe_loss_rate_pct": bottom_severe,
                    "top_severe_loss_rate_pct": top_severe,
                    "severe_loss_rate_difference_pct": top_severe - bottom_severe,
                    "spearman_ic": float(
                        ranked["trend_acceleration_margin_pct"].corr(
                            ranked[outcome].astype(float),
                            method="spearman",
                        )
                    ),
                }
            )
    return pd.DataFrame(rows, columns=columns)


def _build_topk_priority_lift_df(
    observations: pd.DataFrame,
    *,
    horizons: Sequence[int],
    severe_loss_threshold_pct: float,
) -> pd.DataFrame:
    columns = [
        "candidate_group",
        "candidate_kind",
        "horizon",
        "date",
        "k",
        "candidate_count",
        "basket_mean_excess_return_pct",
        "basket_median_excess_return_pct",
        "basket_win_rate_pct",
        "basket_p10_excess_return_pct",
        "basket_p25_excess_return_pct",
        "basket_severe_loss_rate_pct",
        "priority_mean_excess_return_pct",
        "priority_median_excess_return_pct",
        "priority_win_rate_pct",
        "priority_p10_excess_return_pct",
        "priority_p25_excess_return_pct",
        "priority_severe_loss_rate_pct",
        "priority_lift_pct",
        "symbol_turnover_pct",
        "rank_stability_spearman",
    ]
    rows: list[dict[str, object]] = []
    for horizon in horizons:
        outcome = f"forward_close_excess_return_{int(horizon)}d_pct"
        if outcome not in observations:
            continue
        eligible = observations.loc[
            observations["trend_acceleration_margin_pct"].notna()
            & observations[outcome].notna()
        ]
        for (candidate_group, candidate_kind, signal_date), group in eligible.groupby(
            ["candidate_group", "candidate_kind", "date"],
            sort=True,
        ):
            ranked = group.sort_values(
                ["trend_acceleration_triple", "trend_acceleration_margin_pct", "code"],
                ascending=[False, False, True],
            ).copy()
            ranked["_priority_rank"] = np.arange(1, len(ranked) + 1)
            for k in (5, 10):
                if len(ranked) < 2 * k:
                    continue
                priority = ranked.head(k)
                basket_values = ranked[outcome].astype(float)
                priority_values = priority[outcome].astype(float)
                rows.append(
                    {
                        "candidate_group": candidate_group,
                        "candidate_kind": candidate_kind,
                        "horizon": int(horizon),
                        "date": str(signal_date),
                        "k": k,
                        "candidate_count": len(ranked),
                        "basket_mean_excess_return_pct": float(basket_values.mean()),
                        "basket_median_excess_return_pct": float(
                            basket_values.median()
                        ),
                        "basket_win_rate_pct": float(
                            (basket_values > 0).mean() * 100.0
                        ),
                        "basket_p10_excess_return_pct": float(
                            basket_values.quantile(0.1)
                        ),
                        "basket_p25_excess_return_pct": float(
                            basket_values.quantile(0.25)
                        ),
                        "basket_severe_loss_rate_pct": float(
                            (basket_values <= severe_loss_threshold_pct).mean() * 100.0
                        ),
                        "priority_mean_excess_return_pct": float(
                            priority_values.mean()
                        ),
                        "priority_median_excess_return_pct": float(
                            priority_values.median()
                        ),
                        "priority_win_rate_pct": float(
                            (priority_values > 0).mean() * 100.0
                        ),
                        "priority_p10_excess_return_pct": float(
                            priority_values.quantile(0.1)
                        ),
                        "priority_p25_excess_return_pct": float(
                            priority_values.quantile(0.25)
                        ),
                        "priority_severe_loss_rate_pct": float(
                            (priority_values <= severe_loss_threshold_pct).mean()
                            * 100.0
                        ),
                        "priority_lift_pct": float(
                            priority_values.mean() - basket_values.mean()
                        ),
                        "symbol_turnover_pct": np.nan,
                        "rank_stability_spearman": np.nan,
                        "_selected_codes": tuple(priority["code"].astype(str)),
                        "_ranks": dict(
                            zip(
                                ranked["code"].astype(str),
                                ranked["_priority_rank"].astype(float),
                                strict=True,
                            )
                        ),
                    }
                )
    if not rows:
        return pd.DataFrame(columns=columns)
    frame = pd.DataFrame(rows)
    for _, indices in frame.groupby(
        ["candidate_group", "candidate_kind", "horizon", "k"],
        sort=False,
    ).groups.items():
        prior_codes: set[str] | None = None
        prior_ranks: dict[str, float] | None = None
        ordered_indices = sorted(
            cast(Iterable[Any], indices),
            key=lambda item: str(frame.at[item, "date"]),
        )
        for index in ordered_indices:
            current_codes = set(
                cast(tuple[str, ...], frame.at[index, "_selected_codes"])
            )
            current_ranks = cast(dict[str, float], frame.at[index, "_ranks"])
            if prior_codes is not None and prior_ranks is not None:
                frame.at[index, "symbol_turnover_pct"] = (
                    1.0
                    - len(current_codes & prior_codes)
                    / int(str(frame.at[index, "k"]))
                ) * 100.0
                overlap = sorted(current_codes & prior_codes)
                if len(overlap) >= 2:
                    frame.at[index, "rank_stability_spearman"] = pd.Series(
                        [prior_ranks[code] for code in overlap],
                        dtype=float,
                    ).corr(
                        pd.Series(
                            [current_ranks[code] for code in overlap],
                            dtype=float,
                        ),
                        method="spearman",
                    )
            prior_codes = current_codes
            prior_ranks = current_ranks
    return frame[columns]


def moving_block_bootstrap_ci(
    values: np.ndarray,
    *,
    block_length: int,
    resamples: int,
    seed: int,
) -> tuple[float, float, float]:
    """Return mean and percentile CI from circular contiguous block resamples."""

    clean = np.asarray(values, dtype=float)
    clean = clean[np.isfinite(clean)]
    if clean.size == 0:
        return (float("nan"), float("nan"), float("nan"))
    if int(block_length) <= 0:
        raise ValueError("block_length must be positive")
    if int(resamples) <= 0:
        raise ValueError("resamples must be positive")
    rng = np.random.default_rng(int(seed))
    length = clean.size
    blocks_needed = int(np.ceil(length / int(block_length)))
    offsets = np.arange(int(block_length), dtype=int)
    estimates = np.empty(int(resamples), dtype=float)
    for index in range(int(resamples)):
        starts = rng.integers(0, length, size=blocks_needed)
        sample_indices = ((starts[:, None] + offsets) % length).reshape(-1)[:length]
        estimates[index] = float(clean[sample_indices].mean())
    lower, upper = np.quantile(estimates, [0.025, 0.975])
    return (float(clean.mean()), float(lower), float(upper))


def _period_masks(dates: pd.Series) -> list[tuple[str, str, pd.Series]]:
    parsed = pd.to_datetime(dates)
    masks: list[tuple[str, str, pd.Series]] = []
    for name, start, end in SEGMENTS:
        mask = parsed.ge(pd.Timestamp(start))
        if end is not None:
            mask &= parsed.le(pd.Timestamp(end))
        masks.append(("segment", name, mask))
    masks.append(
        (
            "combined_historical",
            "combined_historical_2017_2023",
            parsed.between(pd.Timestamp("2017-01-01"), pd.Timestamp("2023-12-31")),
        )
    )
    for year in sorted(parsed.dt.year.dropna().unique()):
        masks.append(("year", str(int(year)), parsed.dt.year.eq(year)))
    return masks


def _build_segment_stability_df(
    binary: pd.DataFrame,
    fixed_2x2: pd.DataFrame,
    continuous: pd.DataFrame,
    *,
    min_observations: int,
) -> pd.DataFrame:
    columns = [
        "comparison",
        "candidate_group",
        "candidate_kind",
        "horizon",
        "period_type",
        "period_label",
        "date_count",
        "observation_count",
        "mean_daily_lift_pct",
        "median_daily_lift_pct",
        "positive_date_rate_pct",
        "median_daily_spearman_ic",
        "mean_daily_spearman_ic",
        "ic_positive_date_rate_pct",
        "mean_severe_loss_rate_difference_pct",
        "median_focus_candidates_per_date",
        "meets_min_observations",
    ]
    sources: list[tuple[str, pd.DataFrame, str, str | None, str, str]] = [
        (
            "binary_triple",
            binary,
            "mean_lift_pct",
            None,
            "triple_observation_count",
            "paired_date",
        ),
        (
            "fixed_dual_incremental",
            fixed_2x2.loc[
                fixed_2x2.get("row_type", pd.Series(dtype=str))
                == "fixed_dual_positive_lift"
            ],
            "mean_lift_pct",
            None,
            "triple_observation_count",
            "date",
        ),
        (
            "continuous_margin",
            continuous,
            "top_minus_bottom_lift_pct",
            "spearman_ic",
            "top_observation_count",
            "paired_date",
        ),
    ]
    rows: list[dict[str, object]] = []
    for (
        comparison,
        source,
        lift_column,
        ic_column,
        focus_count_column,
        date_column,
    ) in sources:
        if source.empty:
            continue
        for (candidate_group, candidate_kind, horizon), group in source.groupby(
            ["candidate_group", "candidate_kind", "horizon"],
            sort=True,
        ):
            for period_type, period_label, mask in _period_masks(group[date_column]):
                period = group.loc[mask.to_numpy()]
                if period.empty:
                    continue
                lift = period[lift_column].astype(float)
                ic = (
                    period[ic_column].astype(float).dropna()
                    if ic_column is not None
                    else pd.Series(dtype=float)
                )
                rows.append(
                    {
                        "comparison": comparison,
                        "candidate_group": candidate_group,
                        "candidate_kind": candidate_kind,
                        "horizon": int(str(horizon)),
                        "period_type": period_type,
                        "period_label": period_label,
                        "date_count": len(period),
                        "observation_count": int(
                            (
                                period["triple_observation_count"].astype(int)
                                + period["control_observation_count"].astype(int)
                            ).sum()
                            if comparison in {"binary_triple", "fixed_dual_incremental"}
                            else period["observation_count"].astype(int).sum()
                        ),
                        "mean_daily_lift_pct": float(lift.mean()),
                        "median_daily_lift_pct": float(lift.median()),
                        "positive_date_rate_pct": float((lift > 0).mean() * 100.0),
                        "median_daily_spearman_ic": (
                            float(ic.median()) if not ic.empty else np.nan
                        ),
                        "mean_daily_spearman_ic": (
                            float(ic.mean()) if not ic.empty else np.nan
                        ),
                        "ic_positive_date_rate_pct": (
                            float((ic > 0).mean() * 100.0) if not ic.empty else np.nan
                        ),
                        "mean_severe_loss_rate_difference_pct": float(
                            period["severe_loss_rate_difference_pct"].astype(float).mean()
                        ),
                        "median_focus_candidates_per_date": float(
                            period[focus_count_column].astype(float).median()
                        ),
                        "meets_min_observations": (
                            (
                                period["triple_observation_count"].astype(int)
                                + period["control_observation_count"].astype(int)
                            ).sum()
                            if comparison in {"binary_triple", "fixed_dual_incremental"}
                            else period["observation_count"].astype(int).sum()
                        )
                        >= int(min_observations),
                    }
                )
    return pd.DataFrame(rows, columns=columns)


def _build_bootstrap_effect_ci_df(
    binary: pd.DataFrame,
    fixed_2x2: pd.DataFrame,
    continuous: pd.DataFrame,
    topk: pd.DataFrame,
    *,
    resamples: int,
    seed: int,
) -> pd.DataFrame:
    columns = [
        "comparison",
        "candidate_group",
        "candidate_kind",
        "horizon",
        "k",
        "period_type",
        "period_label",
        "date_count",
        "block_length",
        "resamples",
        "seed",
        "point_estimate_pct",
        "ci_lower_95_pct",
        "ci_upper_95_pct",
    ]
    rows: list[dict[str, object]] = []
    sources: tuple[tuple[str, pd.DataFrame, str, str, str | None], ...] = (
        ("binary_triple", binary, "mean_lift_pct", "paired_date", None),
        (
            "fixed_dual_incremental",
            fixed_2x2.loc[
                fixed_2x2.get("row_type", pd.Series(dtype=str))
                == "fixed_dual_positive_lift"
            ],
            "mean_lift_pct",
            "date",
            None,
        ),
        (
            "continuous_margin",
            continuous,
            "top_minus_bottom_lift_pct",
            "paired_date",
            None,
        ),
        ("topk_priority", topk, "priority_lift_pct", "date", "k"),
    )
    for comparison, source, lift_column, date_column, detail_column in sources:
        if source.empty:
            continue
        group_columns = ["candidate_group", "candidate_kind", "horizon"]
        if detail_column is not None:
            group_columns.append(detail_column)
        for group_key, group in source.groupby(
            group_columns,
            sort=True,
        ):
            keys = cast(tuple[object, ...], group_key)
            candidate_group, candidate_kind, horizon = keys[:3]
            detail_value = keys[3] if detail_column is not None else pd.NA
            period_masks = _period_masks(group[date_column])
            period_masks.append(
                ("all_period", "all_available", pd.Series(True, index=group.index))
            )
            for period_type, period_label, mask in period_masks:
                # Segment masks have a fresh positional index; all-period retains source index.
                selected = (
                    group.loc[mask]
                    if mask.index.equals(group.index)
                    else group.loc[mask.to_numpy()]
                )
                if selected.empty:
                    continue
                point, lower, upper = moving_block_bootstrap_ci(
                    selected[lift_column].to_numpy(dtype=float),
                    block_length=int(str(horizon)),
                    resamples=int(resamples),
                    seed=int(seed),
                )
                rows.append(
                    {
                        "comparison": comparison,
                        "candidate_group": candidate_group,
                        "candidate_kind": candidate_kind,
                        "horizon": int(str(horizon)),
                        "k": (
                            int(str(detail_value))
                            if detail_column == "k"
                            else pd.NA
                        ),
                        "period_type": period_type,
                        "period_label": period_label,
                        "date_count": len(selected),
                        "block_length": int(str(horizon)),
                        "resamples": int(resamples),
                        "seed": int(seed),
                        "point_estimate_pct": point,
                        "ci_lower_95_pct": lower,
                        "ci_upper_95_pct": upper,
                    }
                )
    frame = pd.DataFrame(rows, columns=columns)
    if not frame.empty:
        frame["k"] = frame["k"].astype("Int64")
    return frame


def _build_decision_gate_df(
    coverage: pd.DataFrame,
    stability: pd.DataFrame,
    bootstrap: pd.DataFrame,
    _binary: pd.DataFrame,
) -> pd.DataFrame:
    """Evaluate the frozen gates without treating nested groups as replication."""

    independent_families = {
        "core_long_only",
        "momentum_value_only",
        "aggressive_rerating",
    }
    primary_families = {"core_long_only", "momentum_value_only"}

    def _coverage_gate() -> bool:
        rows = coverage.loc[coverage["candidate_group"].isin(primary_families)]
        return len(rows) == len(primary_families) and bool(
            rows["trend_feature_coverage_pct"].ge(95.0).all()
        )

    eligible_stability = stability.loc[stability["meets_min_observations"]]
    continuous_20 = eligible_stability.loc[
        (eligible_stability["comparison"] == "continuous_margin")
        & (eligible_stability["horizon"] == 20)
        & eligible_stability["candidate_group"].isin(independent_families)
    ]
    historical_ci = bootstrap.loc[
        (bootstrap["comparison"] == "continuous_margin")
        & (bootstrap["horizon"] == 20)
        & (bootstrap["period_label"] == "combined_historical_2017_2023")
        & bootstrap["candidate_group"].isin(independent_families)
    ]
    segment_rows = continuous_20.loc[continuous_20["period_type"] == "segment"]
    combined_rows = continuous_20.loc[
        continuous_20["period_label"] == "combined_historical_2017_2023"
    ]
    continuous_segment_positive = {
        family
        for family, rows in segment_rows.groupby("candidate_group")
        if rows["period_label"].nunique() == len(SEGMENTS)
        and rows.groupby("period_label")["mean_daily_lift_pct"].mean().gt(0.0).all()
    }
    continuous_ic_families = set(
        combined_rows.loc[
            combined_rows["median_daily_spearman_ic"].ge(0.02),
            "candidate_group",
        ]
    )
    continuous_ic_rate_families = set(
        combined_rows.loc[
            combined_rows["ic_positive_date_rate_pct"].ge(52.0),
            "candidate_group",
        ]
    )
    continuous_historical_families = set(
        combined_rows.loc[
            combined_rows["mean_daily_lift_pct"].ge(0.25),
            "candidate_group",
        ]
    ) & set(
        historical_ci.loc[historical_ci["ci_lower_95_pct"].gt(0.0), "candidate_group"]
    )
    continuous_full_families = (
        continuous_ic_families
        & continuous_ic_rate_families
        & continuous_historical_families
        & continuous_segment_positive
    )
    continuous_primary = continuous_20.loc[
        continuous_20["candidate_group"].isin(primary_families)
    ]
    continuous_gates = {
        "coverage_ge_95_every_primary_family": _coverage_gate(),
        "median_ic_ge_0_02": len(continuous_ic_families) >= 2,
        "ic_positive_rate_ge_52": len(continuous_ic_rate_families) >= 2,
        "historical_lift_ge_0_25_and_ci_positive": (
            len(continuous_historical_families) >= 2
        ),
        "all_three_segments_positive": len(continuous_segment_positive) >= 2,
        "two_independent_families_positive": len(continuous_full_families) >= 2,
        "severe_loss_not_worse_by_gt_1": bool(
            not continuous_primary.empty
            and continuous_primary["mean_severe_loss_rate_difference_pct"].le(1.0).all()
        ),
    }

    binary_20 = eligible_stability.loc[
        (eligible_stability["comparison"] == "binary_triple")
        & (eligible_stability["horizon"] == 20)
        & eligible_stability["candidate_group"].isin(independent_families)
    ]
    binary_historical = binary_20.loc[
        binary_20["period_label"] == "combined_historical_2017_2023"
    ]
    binary_ci = bootstrap.loc[
        (bootstrap["comparison"] == "binary_triple")
        & (bootstrap["horizon"] == 20)
        & (bootstrap["period_label"] == "combined_historical_2017_2023")
        & bootstrap["candidate_group"].isin(independent_families)
    ]
    binary_segments = binary_20.loc[binary_20["period_type"] == "segment"]
    binary_lift_families = set(
        binary_historical.loc[
            binary_historical["median_daily_lift_pct"].ge(0.25), "candidate_group"
        ]
    )
    binary_win_families = set(
        binary_historical.loc[
            binary_historical["positive_date_rate_pct"].ge(52.0), "candidate_group"
        ]
    )
    binary_ci_families = set(
        binary_ci.loc[binary_ci["ci_lower_95_pct"].gt(0.0), "candidate_group"]
    )
    binary_segment_positive = {
        family
        for family, rows in binary_segments.groupby("candidate_group")
        if rows["period_label"].nunique() == len(SEGMENTS)
        and rows.groupby("period_label")["mean_daily_lift_pct"].mean().gt(0.0).all()
    }
    binary_severe_families = {
        family
        for family, rows in binary_20.groupby("candidate_group")
        if rows["mean_severe_loss_rate_difference_pct"].le(1.0).all()
    }
    binary_candidate_count_families = set(
        binary_historical.loc[
            binary_historical["median_focus_candidates_per_date"].ge(5.0),
            "candidate_group",
        ]
    )
    binary_full_families = (
        binary_lift_families
        & binary_win_families
        & binary_ci_families
        & binary_segment_positive
        & binary_severe_families
        & binary_candidate_count_families
    )
    binary_gates = {
        "median_lift_ge_0_25": len(binary_lift_families) >= 2,
        "paired_date_win_rate_ge_52": len(binary_win_families) >= 2,
        "historical_ci_positive": len(binary_ci_families) >= 2,
        "all_three_segments_positive": len(binary_segment_positive) >= 2,
        "two_independent_families_positive": len(binary_full_families) >= 2,
        "severe_loss_not_worse_by_gt_1": len(binary_severe_families) >= 2,
        "median_triple_candidates_ge_5": (
            len(binary_candidate_count_families) >= 2
        ),
    }
    continuous_passed = all(continuous_gates.values())
    binary_passed = len(binary_full_families) >= 2
    rows = [
        {
            "recommendation": "add_continuous_columns",
            "gate": name,
            "passed": passed,
        }
        for name, passed in continuous_gates.items()
    ]
    rows.extend(
        {
            "recommendation": "add_binary_badge_only",
            "gate": name,
            "passed": passed,
        }
        for name, passed in binary_gates.items()
    )
    final = (
        "add_continuous_columns"
        if continuous_passed
        else "add_binary_badge_only"
        if binary_passed
        else "reject_introduction"
    )
    rows.append(
        {
            "recommendation": final,
            "gate": "final_decision",
            "passed": True,
        }
    )
    return pd.DataFrame(rows)


def _build_coverage_diagnostics(observations: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "candidate_group",
        "candidate_kind",
        "observation_count",
        "symbol_count",
        "date_count",
        "trend_feature_count",
        "trend_feature_coverage_pct",
    ]
    if observations.empty:
        return pd.DataFrame(columns=columns)
    rows: list[dict[str, object]] = []
    for (candidate_group, candidate_kind), group in observations.groupby(
        ["candidate_group", "candidate_kind"],
        sort=True,
    ):
        feature_count = int(group["trend_acceleration_margin_pct"].notna().sum())
        rows.append(
            {
                "candidate_group": candidate_group,
                "candidate_kind": candidate_kind,
                "observation_count": len(group),
                "symbol_count": group["code"].nunique(),
                "date_count": group["date"].nunique(),
                "trend_feature_count": feature_count,
                "trend_feature_coverage_pct": feature_count / len(group) * 100.0,
            }
        )
    return pd.DataFrame(rows, columns=columns)


def _build_candidate_registry_df(observations: pd.DataFrame) -> pd.DataFrame:
    counts = (
        observations.groupby("candidate_group").size().to_dict()
        if not observations.empty
        else {}
    )
    rows = [
        {
            "candidate_group": candidate.name,
            "candidate_kind": (
                "exclusive_slice"
                if candidate.name == "aggressive_rerating"
                else "named_group"
            ),
            "predicate": candidate.predicate,
            "role": candidate.role,
            "independent_evidence": candidate.role in {"primary", "secondary_20d"},
            "observation_count": int(counts.get(candidate.name, 0)),
        }
        for candidate in CANDIDATE_REGISTRY
    ]
    exclusive = (
        ("core_long_only", "primary", True),
        ("momentum_value_only", "primary", True),
        ("core_momentum_overlap", "nested_sensitivity", False),
        ("neutral_good_remainder", "broad_sensitivity", False),
    )
    rows.extend(
        {
            "candidate_group": name,
            "candidate_kind": "exclusive_slice",
            "predicate": f"exclusive_slice = '{name}'",
            "role": role,
            "independent_evidence": independent,
            "observation_count": int(counts.get(name, 0)),
        }
        for name, role, independent in exclusive
    )
    return pd.DataFrame(rows)


def _validate_params(
    *,
    start_date: str | None,
    end_date: str | None,
    horizons: Sequence[int],
    min_observations: int,
    bootstrap_resamples: int,
    observation_sample_limit: int,
) -> None:
    if not horizons or any(int(horizon) <= 0 for horizon in horizons):
        raise ValueError("horizons must contain positive integers")
    if int(min_observations) <= 0:
        raise ValueError("min_observations must be positive")
    if int(bootstrap_resamples) <= 0:
        raise ValueError("bootstrap_resamples must be positive")
    if int(observation_sample_limit) < 0:
        raise ValueError("observation_sample_limit must be non-negative")
    if start_date is not None and end_date is not None:
        if date.fromisoformat(start_date) > date.fromisoformat(end_date):
            raise ValueError("start_date must be on or before end_date")
