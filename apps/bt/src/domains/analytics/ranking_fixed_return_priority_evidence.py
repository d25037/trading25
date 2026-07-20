"""Fixed 20D/60D Ranking priority evidence inside fixed-free long scaffolds."""

# pyright: reportArgumentType=false, reportAssignmentType=false, reportUnusedFunction=false

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Literal, Sequence

import numpy as np
import pandas as pd

from src.domains.analytics.daily_ranking_consumer_support import (
    compose_daily_ranking_signal_features,
)
from src.domains.analytics.daily_ranking_event_time_prices import (
    DailyRankingPriceLineage,
)
from src.domains.analytics.daily_ranking_feature_builders import (
    AtrFeaturesRequest,
    LongLeadershipFeaturesRequest,
    LongScaffoldFeaturesRequest,
    SectorStrengthFeaturesRequest,
    ShortScaffoldFeaturesRequest,
    build_atr_features,
    build_long_leadership_features,
    build_long_scaffold_features,
    build_sector_strength_features,
    build_short_scaffold_features,
)
from src.domains.analytics.daily_ranking_research_base import (
    DailyRankingPanelRequest,
    SignalDerivedColumn,
    SignalExpression,
    attach_daily_ranking_outcomes,
    build_daily_ranking_research_base,
    materialize_daily_ranking_signal_cohort,
)
from src.domains.analytics.earnings_holdthrough_expectancy_report import (
    _top_rows_for_markdown,
)
from src.domains.analytics.ranking_research_selection_contract import (
    EvaluatedSignalSelection,
    evaluate_frozen_selection,
    freeze_signal_tails,
    select_frozen_topk,
)
from src.domains.analytics.readonly_duckdb_support import (
    SourceMode,
    open_readonly_analysis_connection,
    require_market_v4_compatibility,
)
from src.domains.analytics.research_bundle import (
    ResearchBundleInfo,
    write_research_bundle,
)
from src.shared.utils.market_code_alias import MARKET_CODES_BY_SCOPE

FixedReturnQuadrant = Literal["++", "+-", "-+", "--", "zero", "missing"]


@dataclass(frozen=True)
class ScaffoldDefinition:
    name: str
    predicate: str


SCAFFOLD_REGISTRY: tuple[ScaffoldDefinition, ...] = (
    ScaffoldDefinition(
        "strict_value_long_only",
        "deep_value_flag AND long_hybrid_leadership_flag AND raw_atr_acceleration_flag",
    ),
    ScaffoldDefinition(
        "value_extension_long_only",
        "value_composite_equal_score >= 0.8 AND NOT deep_value_flag "
        "AND long_hybrid_leadership_flag AND raw_atr_acceleration_flag",
    ),
)

PRIME_EQUIVALENT_MARKET_CODES: tuple[str, ...] = tuple(
    code for code in MARKET_CODES_BY_SCOPE["prime"] if code.isdigit()
)
if set(PRIME_EQUIVALENT_MARKET_CODES) != {"0101", "0111"}:
    raise RuntimeError("Prime research must resolve to exact-date 0101/0111 membership")

PRIMARY_SCAFFOLD_FAMILIES = frozenset(item.name for item in SCAFFOLD_REGISTRY)
PRIORITY_VARIANTS = ("fixed20_priority", "fixed60_priority", "fixed_equal_priority")
RANKING_FIXED_RETURN_PRIORITY_EXPERIMENT_ID = (
    "market-behavior/ranking-fixed-return-priority-evidence"
)
DEFAULT_HORIZONS = (5, 20, 60)
DEFAULT_BOOTSTRAP_RESAMPLES = 2_000
DEFAULT_BOOTSTRAP_SEED = 31
DEFAULT_OBSERVATION_SAMPLE_LIMIT = 10_000
DEFAULT_SEVERE_LOSS_THRESHOLD_PCT = -10.0
_LEADERSHIP_WINDOWS = (120, 252, 504)
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
SEGMENTS: tuple[tuple[str, date, date | None], ...] = (
    ("historical_pre_reorg", date(2017, 1, 1), date(2021, 12, 31)),
    ("historical_post_reorg", date(2022, 1, 1), date(2023, 12, 31)),
    ("recent_hypothesis_origin", date(2024, 1, 1), None),
)
REQUIRED_BUNDLE_TABLES = {
    "coverage_attrition",
    "scaffold_registry",
    "continuous_priority_lift",
    "fixed_2x2_daily",
    "fixed_incremental_contrast",
    "topk_priority_lift",
    "segment_stability",
    "bootstrap_effect_ci",
    "regression_sensitivity",
    "decision_gate",
    "observation_sample",
}


def classify_fixed_return_quadrant(
    return_20d_pct: float | None,
    return_60d_pct: float | None,
) -> FixedReturnQuadrant:
    """Classify strict fixed-return signs without folding zero into positive."""

    if return_20d_pct is None or return_60d_pct is None:
        return "missing"
    if not np.isfinite(return_20d_pct) or not np.isfinite(return_60d_pct):
        return "missing"
    if return_20d_pct == 0.0 or return_60d_pct == 0.0:
        return "zero"
    return ("+" if return_20d_pct > 0.0 else "-") + (
        "+" if return_60d_pct > 0.0 else "-"
    )  # type: ignore[return-value]


def _add_prime_date_percentiles(frame: pd.DataFrame) -> pd.DataFrame:
    """Rank fixed returns against every Prime member on each signal date."""

    ranked = frame.copy()
    ranked["fixed20_priority"] = ranked.groupby("date", observed=True)[
        "recent_return_20d_pct"
    ].rank(method="average", pct=True)
    ranked["fixed60_priority"] = ranked.groupby("date", observed=True)[
        "recent_return_60d_pct"
    ].rank(method="average", pct=True)
    ranked["fixed_equal_priority"] = ranked[
        ["fixed20_priority", "fixed60_priority"]
    ].mean(axis=1, skipna=False)
    return ranked


def moving_block_bootstrap_ci(
    values: np.ndarray,
    *,
    block_length: int,
    resamples: int,
    seed: int,
) -> tuple[float, float, float]:
    """Return mean and fixed-seed moving-block 95% interval."""

    clean = np.asarray(values, dtype=float)
    clean = clean[np.isfinite(clean)]
    if clean.size == 0:
        return (float("nan"), float("nan"), float("nan"))
    if block_length <= 0 or resamples <= 0:
        raise ValueError("block_length and resamples must be positive")
    rng = np.random.default_rng(seed)
    size = clean.size
    block = min(block_length, size)
    starts = np.arange(size)
    estimates = np.empty(resamples, dtype=float)
    for index in range(resamples):
        sample_parts: list[np.ndarray] = []
        while sum(part.size for part in sample_parts) < size:
            start = int(rng.choice(starts))
            positions = (start + np.arange(block)) % size
            sample_parts.append(clean[positions])
        estimates[index] = np.concatenate(sample_parts)[:size].mean()
    return (
        float(clean.mean()),
        float(np.quantile(estimates, 0.025)),
        float(np.quantile(estimates, 0.975)),
    )


def _build_decision_gate_df(
    continuous_evidence: pd.DataFrame,
    badge_evidence: pd.DataFrame,
    topk_evidence: pd.DataFrame,
) -> pd.DataFrame:
    """Apply frozen family replication gates to every priority variant."""

    del badge_evidence, topk_evidence
    rows: list[dict[str, object]] = []
    for variant in PRIORITY_VARIANTS:
        subset = continuous_evidence.loc[
            continuous_evidence.get("priority_variant", pd.Series(dtype="object")).eq(
                variant
            )
        ]
        families = set(subset.get("scaffold_family", pd.Series(dtype="object")))
        if families != PRIMARY_SCAFFOLD_FAMILIES:
            rows.append(
                {
                    "decision_key": variant,
                    "passed": False,
                    "reason": "requires_both_primary_families",
                }
            )
            continue
        sufficient_sample = bool(
            subset["observation_count"].ge(300).all()
            and subset["paired_date_count"].ge(50).all()
            and subset["median_focus_candidates"].ge(5.0).all()
        )
        if not sufficient_sample:
            rows.append(
                {
                    "decision_key": variant,
                    "passed": False,
                    "reason": "insufficient_sample",
                }
            )
            continue
        passed = bool(
            subset["mean_lift_pct"].ge(0.25).all()
            and subset["ci_lower_pct"].gt(0.0).all()
            and subset["median_spearman_ic"].ge(0.02).all()
            and subset["ic_positive_date_rate_pct"].ge(52.0).all()
            and subset["all_segments_positive"].astype(bool).all()
            and subset["severe_loss_rate_difference_pct"].le(1.0).all()
        )
        rows.append(
            {
                "decision_key": variant,
                "passed": passed,
                "reason": "all_frozen_gates_pass"
                if passed
                else "one_or_more_gates_failed",
            }
        )
    return pd.DataFrame(rows)


@dataclass(frozen=True)
class RankingFixedReturnPriorityEvidenceResult:
    db_path: str
    source_mode: SourceMode
    source_detail: str
    market_source: str
    analysis_start_date: str | None
    analysis_end_date: str | None
    horizons: tuple[int, ...]
    bootstrap_resamples: int
    bootstrap_seed: int
    observation_count: int
    price_projection: DailyRankingPriceLineage
    coverage_attrition_df: pd.DataFrame
    scaffold_registry_df: pd.DataFrame
    continuous_priority_lift_df: pd.DataFrame
    fixed_2x2_daily_df: pd.DataFrame
    fixed_incremental_contrast_df: pd.DataFrame
    topk_priority_lift_df: pd.DataFrame
    segment_stability_df: pd.DataFrame
    bootstrap_effect_ci_df: pd.DataFrame
    regression_sensitivity_df: pd.DataFrame
    decision_gate_df: pd.DataFrame
    observation_sample_df: pd.DataFrame


def run_ranking_fixed_return_priority_evidence_research(
    db_path: str | Path,
    *,
    start_date: str | None = "2017-01-01",
    end_date: str | None = None,
    horizons: Iterable[int] = DEFAULT_HORIZONS,
    bootstrap_resamples: int = DEFAULT_BOOTSTRAP_RESAMPLES,
    bootstrap_seed: int = DEFAULT_BOOTSTRAP_SEED,
    observation_sample_limit: int = DEFAULT_OBSERVATION_SAMPLE_LIMIT,
) -> RankingFixedReturnPriorityEvidenceResult:
    """Run the frozen PIT Prime-only fixed-return priority experiment."""

    resolved_horizons = tuple(sorted({int(item) for item in horizons}))
    if not resolved_horizons or any(item <= 0 for item in resolved_horizons):
        raise ValueError("horizons must contain positive integers")
    if bootstrap_resamples <= 0 or observation_sample_limit <= 0:
        raise ValueError(
            "bootstrap_resamples and observation_sample_limit must be positive"
        )
    db_path_obj = Path(db_path).expanduser().resolve()
    if not db_path_obj.is_file():
        raise FileNotFoundError(f"market.duckdb was not found: {db_path_obj}")

    market_source = "stock_master_daily_exact_date"
    with open_readonly_analysis_connection(
        str(db_path_obj),
        snapshot_prefix="ranking-fixed-return-priority-",
    ) as ctx:
        require_market_v4_compatibility(
            ctx.connection,
            required_tables=_REQUIRED_MARKET_TABLES,
        )
        relations = build_daily_ranking_research_base(
            ctx.connection,
            DailyRankingPanelRequest(
                namespace="fixed_return_priority",
                analysis_start_date=(
                    None if start_date is None else date.fromisoformat(start_date)
                ),
                analysis_end_date=(
                    None if end_date is None else date.fromisoformat(end_date)
                ),
                horizons=resolved_horizons,
                market_scopes=("prime",),
                include_liquidity=True,
                percentile_features=(),
            ),
        )
        signal_source = relations.liquidity_ranked_signals
        if signal_source is None:
            raise RuntimeError(
                "fixed return priority requires liquidity-ranked signals"
            )
        atr_features = build_atr_features(
            ctx.connection,
            AtrFeaturesRequest(source=signal_source, namespace="fixed_return_atr"),
        )
        short_features = build_short_scaffold_features(
            ctx.connection,
            ShortScaffoldFeaturesRequest(
                source=signal_source,
                atr_features=atr_features,
                namespace="fixed_return_short",
            ),
        )
        sector_features = build_sector_strength_features(
            ctx.connection,
            SectorStrengthFeaturesRequest(
                source=signal_source,
                population_source=signal_source,
                namespace="fixed_return_sector",
            ),
        )
        leadership_features = build_long_leadership_features(
            ctx.connection,
            LongLeadershipFeaturesRequest(
                source=signal_source,
                sector_features=sector_features,
                namespace="fixed_return_leadership",
                leadership_windows=_LEADERSHIP_WINDOWS,
            ),
        )
        long_features = build_long_scaffold_features(
            ctx.connection,
            LongScaffoldFeaturesRequest(
                source=signal_source,
                leadership_features=leadership_features,
                short_scaffold_features=short_features,
                namespace="fixed_return_long",
            ),
        )
        composed = compose_daily_ranking_signal_features(
            ctx.connection,
            source=signal_source,
            features=(long_features,),
            namespace="fixed_return_priority",
        )
        cohort = materialize_daily_ranking_signal_cohort(
            ctx.connection,
            relations,
            source=composed,
            name="fixed_return_candidates",
            predicate=SignalExpression(
                sql="market_scope = 'prime' AND market_code IN ('0101', '0111')",
                referenced_columns=("market_scope", "market_code"),
            ),
            derived_columns=_fixed_return_candidate_columns(),
        )
        evaluated = attach_daily_ranking_outcomes(
            ctx.connection,
            cohort,
            relations,
            name="fixed_return_outcomes",
        )
        observations = _query_fixed_free_observations(
            ctx.connection,
            horizons=resolved_horizons,
            source_name=evaluated.name,
        )

        continuous = _build_continuous_priority_lift_df(
            observations,
            horizons=resolved_horizons,
        )
        fixed_2x2 = _build_fixed_2x2_daily_df(
            observations,
            horizons=resolved_horizons,
        )
        contrasts = _build_fixed_incremental_contrast_df(
            observations,
            horizons=resolved_horizons,
        )
        topk = _build_topk_priority_lift_df(
            observations,
            horizons=resolved_horizons,
        )
        complete_continuous = continuous.loc[
            continuous["outcome_status"].eq("complete")
        ].copy()
        complete_topk = topk.loc[topk["outcome_status"].eq("complete")].copy()
        segments = _build_segment_stability_df(
            complete_continuous,
            contrasts,
            complete_topk,
        )
        bootstrap = _build_bootstrap_effect_ci_df(
            complete_continuous,
            contrasts,
            complete_topk,
            resamples=bootstrap_resamples,
            seed=bootstrap_seed,
        )
        continuous_gate = _build_continuous_gate_evidence(
            observations,
            complete_continuous,
            segments,
            bootstrap,
        )
        badge_gate = _build_badge_gate_evidence(
            fixed_2x2,
            contrasts,
            segments,
            bootstrap,
        )
        topk_gate = _build_topk_gate_evidence(complete_topk, bootstrap)
        decisions = _build_decision_gate_df(continuous_gate, badge_gate, topk_gate)
        decisions = _append_badge_topk_and_recommendation(
            decisions,
            badge_gate,
            topk_gate,
        )
        coverage = _build_coverage_attrition_df(observations)
        regression = _build_regression_sensitivity_df(
            observations,
            horizons=resolved_horizons,
        )
        registry = pd.DataFrame(
            [
                {
                    "scaffold_family": item.name,
                    "predicate": item.predicate,
                    "candidate_timing": "before_fixed_return_features",
                    "market_scope": "prime_exact_date_0101_0111",
                }
                for item in SCAFFOLD_REGISTRY
            ]
        )
        result = RankingFixedReturnPriorityEvidenceResult(
            db_path=str(db_path_obj),
            source_mode=ctx.source_mode,
            source_detail=ctx.source_detail,
            market_source=market_source,
            analysis_start_date=start_date,
            analysis_end_date=end_date,
            horizons=resolved_horizons,
            bootstrap_resamples=int(bootstrap_resamples),
            bootstrap_seed=int(bootstrap_seed),
            observation_count=len(observations),
            price_projection=relations.lineage.price,
            coverage_attrition_df=coverage,
            scaffold_registry_df=registry,
            continuous_priority_lift_df=continuous,
            fixed_2x2_daily_df=fixed_2x2,
            fixed_incremental_contrast_df=contrasts,
            topk_priority_lift_df=topk,
            segment_stability_df=segments,
            bootstrap_effect_ci_df=bootstrap,
            regression_sensitivity_df=regression,
            decision_gate_df=decisions,
            observation_sample_df=observations.head(observation_sample_limit).copy(),
        )
    return result


def _fixed_return_candidate_columns() -> tuple[SignalDerivedColumn, ...]:
    return (
        SignalDerivedColumn(
            name="strict_value_long_only_flag",
            expression=SignalExpression(
                sql=(
                    "coalesce(valuation_signal = 'strong_value_confirmation' "
                    "AND long_hybrid_leadership_score >= 0.799999 "
                    "AND atr20_acceleration_flag, FALSE)"
                ),
                referenced_columns=(
                    "valuation_signal",
                    "long_hybrid_leadership_score",
                    "atr20_acceleration_flag",
                ),
            ),
            sql_type="BOOLEAN",
        ),
        SignalDerivedColumn(
            name="value_extension_long_only_flag",
            expression=SignalExpression(
                sql=(
                    "coalesce(value_composite_equal_score >= 0.8 "
                    "AND valuation_signal <> 'strong_value_confirmation' "
                    "AND long_hybrid_leadership_score >= 0.799999 "
                    "AND atr20_acceleration_flag, FALSE)"
                ),
                referenced_columns=(
                    "value_composite_equal_score",
                    "valuation_signal",
                    "long_hybrid_leadership_score",
                    "atr20_acceleration_flag",
                ),
            ),
            sql_type="BOOLEAN",
        ),
    )


def _query_fixed_free_observations(
    conn: Any,
    *,
    horizons: Sequence[int],
    source_name: str,
) -> pd.DataFrame:
    forward_columns = ",\n            ".join(
        expression
        for horizon in horizons
        for expression in (
            f"forward_outcome_completion_date_{int(horizon)}d",
            f"forward_close_return_{int(horizon)}d_pct",
            f"forward_close_excess_return_{int(horizon)}d_pct",
            f"forward_close_n225_excess_return_{int(horizon)}d_pct",
        )
    )
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ranking_fixed_return_prime_ranked AS
        SELECT
            p.*,
            CASE WHEN recent_return_20d_pct IS NOT NULL THEN
                rank() OVER (
                    PARTITION BY p.date ORDER BY recent_return_20d_pct NULLS LAST
                )::DOUBLE
                / count(recent_return_20d_pct) OVER (PARTITION BY p.date)
            END AS fixed20_priority,
            CASE WHEN recent_return_60d_pct IS NOT NULL THEN
                rank() OVER (
                    PARTITION BY p.date ORDER BY recent_return_60d_pct NULLS LAST
                )::DOUBLE
                / count(recent_return_60d_pct) OVER (PARTITION BY p.date)
            END AS fixed60_priority
        FROM {source_name} p
        """
    )
    frame = conn.execute(
        f"""
        WITH candidates AS (
            SELECT 'strict_value_long_only' AS scaffold_family, *
            FROM ranking_fixed_return_prime_ranked
            WHERE strict_value_long_only_flag
            UNION ALL
            SELECT 'value_extension_long_only' AS scaffold_family, *
            FROM ranking_fixed_return_prime_ranked
            WHERE value_extension_long_only_flag
        )
        SELECT
            scaffold_family,
            date,
            code,
            market_code,
            sector_33_code,
            sector_33_name,
            valuation_signal,
            value_composite_equal_score,
            long_hybrid_leadership_score,
            atr20_pct,
            atr20_change_20d_pct,
            liquidity_residual_z,
            recent_return_20d_pct,
            recent_return_60d_pct,
            fixed20_priority,
            fixed60_priority,
            (fixed20_priority + fixed60_priority) / 2.0 AS fixed_equal_priority,
            {forward_columns}
        FROM candidates
        ORDER BY date, scaffold_family, code
        """
    ).fetchdf()
    if frame.empty:
        frame["fixed_quadrant"] = pd.Series(dtype="object")
        return frame
    frame["date"] = pd.to_datetime(frame["date"])
    frame["fixed_quadrant"] = [
        classify_fixed_return_quadrant(r20, r60)
        for r20, r60 in zip(
            frame["recent_return_20d_pct"],
            frame["recent_return_60d_pct"],
            strict=True,
        )
    ]
    return frame


def _build_continuous_priority_lift_df(
    observations: pd.DataFrame,
    *,
    horizons: Sequence[int],
) -> pd.DataFrame:
    columns = [
        "scaffold_family",
        "date",
        "horizon",
        "priority_variant",
        "observation_count",
        "candidate_outcome_count",
        "candidate_outcome_coverage_pct",
        "focus_candidate_count",
        "selected_outcome_count",
        "selected_outcome_coverage_pct",
        "outcome_status",
        "bottom_mean_excess_return_pct",
        "top_mean_excess_return_pct",
        "mean_lift_pct",
        "bottom_median_excess_return_pct",
        "top_median_excess_return_pct",
        "median_lift_pct",
        "bottom_severe_loss_rate_pct",
        "top_severe_loss_rate_pct",
        "severe_loss_rate_difference_pct",
        "spearman_ic",
    ]
    rows: list[dict[str, object]] = []
    if observations.empty:
        return pd.DataFrame(rows, columns=columns)
    for (family, signal_date), group in observations.groupby(
        ["scaffold_family", "date"], observed=True
    ):
        for variant in PRIORITY_VARIANTS:
            for horizon in horizons:
                outcome = f"forward_close_excess_return_{horizon}d_pct"
                evaluated = _evaluate_priority_tails(
                    group,
                    score_column=variant,
                    outcome_column=outcome,
                )
                if evaluated is None or len(evaluated.top) < 2:
                    continue
                bottom = evaluated.bottom[outcome]
                top = evaluated.top[outcome]
                candidate_values = evaluated.candidates[outcome]
                outcome_complete = evaluated.outcome_status == "complete"
                outcome_metrics = (
                    {
                        "bottom_mean_excess_return_pct": bottom.mean(),
                        "top_mean_excess_return_pct": top.mean(),
                        "mean_lift_pct": top.mean() - bottom.mean(),
                        "bottom_median_excess_return_pct": bottom.median(),
                        "top_median_excess_return_pct": top.median(),
                        "median_lift_pct": top.median() - bottom.median(),
                        "bottom_severe_loss_rate_pct": bottom.le(
                            DEFAULT_SEVERE_LOSS_THRESHOLD_PCT
                        ).mean()
                        * 100.0,
                        "top_severe_loss_rate_pct": top.le(
                            DEFAULT_SEVERE_LOSS_THRESHOLD_PCT
                        ).mean()
                        * 100.0,
                        "severe_loss_rate_difference_pct": (
                            top.le(DEFAULT_SEVERE_LOSS_THRESHOLD_PCT).mean()
                            - bottom.le(DEFAULT_SEVERE_LOSS_THRESHOLD_PCT).mean()
                        )
                        * 100.0,
                        "spearman_ic": evaluated.candidates[variant].corr(
                            candidate_values,
                            method="spearman",
                        ),
                    }
                    if outcome_complete
                    else {
                        metric: float("nan")
                        for metric in (
                            "bottom_mean_excess_return_pct",
                            "top_mean_excess_return_pct",
                            "mean_lift_pct",
                            "bottom_median_excess_return_pct",
                            "top_median_excess_return_pct",
                            "median_lift_pct",
                            "bottom_severe_loss_rate_pct",
                            "top_severe_loss_rate_pct",
                            "severe_loss_rate_difference_pct",
                            "spearman_ic",
                        )
                    }
                )
                rows.append(
                    {
                        "scaffold_family": family,
                        "date": signal_date,
                        "horizon": int(horizon),
                        "priority_variant": variant,
                        "observation_count": evaluated.candidate_count,
                        "candidate_outcome_count": evaluated.candidate_outcome_count,
                        "candidate_outcome_coverage_pct": (
                            evaluated.candidate_outcome_coverage_pct
                        ),
                        "focus_candidate_count": len(evaluated.top),
                        "selected_outcome_count": evaluated.selected_outcome_count,
                        "selected_outcome_coverage_pct": (
                            evaluated.selected_outcome_coverage_pct
                        ),
                        "outcome_status": evaluated.outcome_status,
                        **outcome_metrics,
                    }
                )
    return pd.DataFrame(rows, columns=columns)


def _evaluate_priority_tails(
    frame: pd.DataFrame,
    *,
    score_column: str,
    outcome_column: str,
    extra_signal_columns: Sequence[str] = (),
) -> EvaluatedSignalSelection | None:
    signal_columns = ["date", "code", score_column, *extra_signal_columns]
    candidates = (
        frame.dropna(subset=["date", "code", score_column])
        .drop_duplicates(["date", "code"])
        .copy()
    )
    if candidates.empty:
        return None
    frozen = freeze_signal_tails(
        candidates.loc[:, signal_columns],
        group_columns=("date",),
        score_columns=(score_column,),
        fraction=0.2,
        ascending=(False,),
    )
    return evaluate_frozen_selection(
        frozen,
        candidates.loc[:, ["date", "code", outcome_column]],
        outcome_column=outcome_column,
    )


def _build_fixed_2x2_daily_df(
    observations: pd.DataFrame,
    *,
    horizons: Sequence[int],
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    strict = observations.loc[
        observations.get("fixed_quadrant", "").isin(["++", "+-", "-+", "--"])
    ]
    for (family, signal_date, quadrant), group in strict.groupby(
        ["scaffold_family", "date", "fixed_quadrant"], observed=True
    ):
        for horizon in horizons:
            outcome = f"forward_close_excess_return_{horizon}d_pct"
            values = group[outcome].dropna()
            rows.append(
                {
                    "scaffold_family": family,
                    "date": signal_date,
                    "horizon": int(horizon),
                    "quadrant": quadrant,
                    "observation_count": len(values),
                    "mean_excess_return_pct": values.mean(),
                    "median_excess_return_pct": values.median(),
                    "win_rate_pct": values.gt(0).mean() * 100.0,
                    "severe_loss_rate_pct": values.le(
                        DEFAULT_SEVERE_LOSS_THRESHOLD_PCT
                    ).mean()
                    * 100.0,
                }
            )
    return pd.DataFrame(rows)


def _build_fixed_incremental_contrast_df(
    observations: pd.DataFrame,
    *,
    horizons: Sequence[int],
) -> pd.DataFrame:
    definitions = {
        "plusplus_minus_plusminus": ({"++"}, {"+-"}),
        "plusplus_minus_minusplus": ({"++"}, {"-+"}),
        "plusplus_minus_nonplusplus": ({"++"}, {"+-", "-+", "--"}),
    }
    rows: list[dict[str, object]] = []
    for (family, signal_date), group in observations.groupby(
        ["scaffold_family", "date"], observed=True
    ):
        for horizon in horizons:
            outcome = f"forward_close_excess_return_{horizon}d_pct"
            for name, (focus_cells, control_cells) in definitions.items():
                focus = group.loc[
                    group["fixed_quadrant"].isin(focus_cells), outcome
                ].dropna()
                control = group.loc[
                    group["fixed_quadrant"].isin(control_cells), outcome
                ].dropna()
                if len(focus) < 2 or len(control) < 2:
                    continue
                rows.append(
                    {
                        "scaffold_family": family,
                        "date": signal_date,
                        "horizon": int(horizon),
                        "contrast": name,
                        "focus_count": len(focus),
                        "control_count": len(control),
                        "mean_lift_pct": focus.mean() - control.mean(),
                        "median_lift_pct": focus.median() - control.median(),
                        "positive": focus.mean() > control.mean(),
                        "severe_loss_rate_difference_pct": focus.le(
                            DEFAULT_SEVERE_LOSS_THRESHOLD_PCT
                        ).mean()
                        * 100.0
                        - control.le(DEFAULT_SEVERE_LOSS_THRESHOLD_PCT).mean() * 100.0,
                    }
                )
            cells: dict[str, pd.Series] = {
                key: group.loc[group["fixed_quadrant"].eq(key), outcome].dropna()
                for key in ("++", "+-", "-+", "--")
            }
            if all(len(values) >= 2 for values in cells.values()):
                interaction = (
                    cells["++"].mean()
                    - cells["+-"].mean()
                    - cells["-+"].mean()
                    + cells["--"].mean()
                )
                rows.append(
                    {
                        "scaffold_family": family,
                        "date": signal_date,
                        "horizon": int(horizon),
                        "contrast": "sign_interaction",
                        "focus_count": min(len(cells["++"]), len(cells["-+"])),
                        "control_count": min(len(cells["+-"]), len(cells["--"])),
                        "mean_lift_pct": interaction,
                        "median_lift_pct": (
                            cells["++"].median()
                            - cells["+-"].median()
                            - cells["-+"].median()
                            + cells["--"].median()
                        ),
                        "positive": interaction > 0.0,
                        "severe_loss_rate_difference_pct": float("nan"),
                    }
                )
    return pd.DataFrame(rows)


def _build_topk_priority_lift_df(
    observations: pd.DataFrame,
    *,
    horizons: Sequence[int],
) -> pd.DataFrame:
    columns = [
        "scope",
        "date",
        "horizon",
        "priority_variant",
        "k",
        "candidate_count",
        "candidate_outcome_count",
        "candidate_outcome_coverage_pct",
        "selected_outcome_count",
        "selected_outcome_coverage_pct",
        "outcome_status",
        "basket_mean_excess_return_pct",
        "priority_mean_excess_return_pct",
        "priority_lift_pct",
        "basket_severe_loss_rate_pct",
        "priority_severe_loss_rate_pct",
        "severe_loss_rate_difference_pct",
        "basket_sector_hhi",
        "priority_sector_hhi",
    ]
    rows: list[dict[str, object]] = []
    scopes: list[tuple[str, pd.DataFrame]] = [
        (family, group)
        for family, group in observations.groupby("scaffold_family", observed=True)
    ]
    scopes.append(("combined_primary", observations))
    for excluded in sorted(PRIMARY_SCAFFOLD_FAMILIES):
        scopes.append(
            (
                f"leave_out_{excluded}",
                observations.loc[observations["scaffold_family"].ne(excluded)],
            )
        )
    for scope, scope_frame in scopes:
        for signal_date, group in scope_frame.groupby("date", observed=True):
            group = group.drop_duplicates(["date", "code"])
            for variant in PRIORITY_VARIANTS:
                for horizon in horizons:
                    outcome = f"forward_close_excess_return_{horizon}d_pct"
                    candidates = group.dropna(subset=[variant]).drop_duplicates(
                        ["date", "code"]
                    )
                    for k in (5, 10):
                        if len(candidates) < 2 * k:
                            continue
                        selection = select_frozen_topk(
                            candidates,
                            score_columns=(variant,),
                            outcome_column=outcome,
                            k=k,
                            ascending=(False,),
                        )
                        eligible = selection.candidates
                        selected = selection.selected
                        outcome_complete = selection.outcome_status == "complete"
                        basket_values = selection.candidate_outcomes
                        selected_values = selection.selected_outcomes
                        outcome_metrics = (
                            {
                                "basket_mean_excess_return_pct": basket_values.mean(),
                                "priority_mean_excess_return_pct": selected_values.mean(),
                                "priority_lift_pct": selected_values.mean()
                                - basket_values.mean(),
                                "basket_severe_loss_rate_pct": basket_values.le(
                                    DEFAULT_SEVERE_LOSS_THRESHOLD_PCT
                                ).mean()
                                * 100.0,
                                "priority_severe_loss_rate_pct": selected_values.le(
                                    DEFAULT_SEVERE_LOSS_THRESHOLD_PCT
                                ).mean()
                                * 100.0,
                                "severe_loss_rate_difference_pct": (
                                    selected_values.le(
                                        DEFAULT_SEVERE_LOSS_THRESHOLD_PCT
                                    ).mean()
                                    - basket_values.le(
                                        DEFAULT_SEVERE_LOSS_THRESHOLD_PCT
                                    ).mean()
                                )
                                * 100.0,
                            }
                            if outcome_complete
                            else {
                                "basket_mean_excess_return_pct": float("nan"),
                                "priority_mean_excess_return_pct": float("nan"),
                                "priority_lift_pct": float("nan"),
                                "basket_severe_loss_rate_pct": float("nan"),
                                "priority_severe_loss_rate_pct": float("nan"),
                                "severe_loss_rate_difference_pct": float("nan"),
                            }
                        )
                        rows.append(
                            {
                                "scope": scope,
                                "date": signal_date,
                                "horizon": int(horizon),
                                "priority_variant": variant,
                                "k": k,
                                "candidate_count": selection.candidate_count,
                                "candidate_outcome_count": selection.candidate_outcome_count,
                                "candidate_outcome_coverage_pct": selection.candidate_outcome_coverage_pct,
                                "selected_outcome_count": selection.selected_outcome_count,
                                "selected_outcome_coverage_pct": selection.selected_outcome_coverage_pct,
                                "outcome_status": selection.outcome_status,
                                **outcome_metrics,
                                "basket_sector_hhi": _sector_hhi(eligible),
                                "priority_sector_hhi": _sector_hhi(selected),
                            }
                        )
    return pd.DataFrame(rows, columns=columns)


def _sector_hhi(frame: pd.DataFrame) -> float:
    shares = frame["sector_33_code"].value_counts(normalize=True)
    return float((shares**2).sum()) if not shares.empty else float("nan")


def _segment_label(value: Any) -> str:
    stamp = pd.Timestamp(value)
    if stamp.year <= 2021:
        return "historical_pre_reorg"
    if stamp.year <= 2023:
        return "historical_post_reorg"
    return "recent_hypothesis_origin"


def _build_segment_stability_df(*frames: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    configs = (
        (frames[0], "continuous", "priority_variant", "mean_lift_pct"),
        (frames[1], "sign_contrast", "contrast", "mean_lift_pct"),
        (frames[2], "topk", "priority_variant", "priority_lift_pct"),
    )
    for frame, analysis, key_column, effect_column in configs:
        if frame.empty:
            continue
        working = frame.copy()
        working["segment"] = working["date"].map(_segment_label)
        working["year"] = pd.to_datetime(working["date"]).dt.year.astype(str)
        group_columns = [
            column
            for column in (
                "scaffold_family",
                "scope",
                key_column,
                "horizon",
                "k",
                "segment",
            )
            if column in working
        ]
        for keys, group in working.groupby(group_columns, observed=True, dropna=False):
            key_values = keys if isinstance(keys, tuple) else (keys,)
            row = dict(zip(group_columns, key_values, strict=True))
            row.update(
                analysis=analysis,
                period_type="segment",
                period_label=row["segment"],
                date_count=group["date"].nunique(),
                mean_effect_pct=group[effect_column].mean(),
                median_effect_pct=group[effect_column].median(),
                positive_date_rate_pct=group[effect_column].gt(0).mean() * 100.0,
            )
            rows.append(row)
        annual_columns = [
            column
            for column in (
                "scaffold_family",
                "scope",
                key_column,
                "horizon",
                "k",
                "year",
            )
            if column in working
        ]
        for keys, group in working.groupby(annual_columns, observed=True, dropna=False):
            key_values = keys if isinstance(keys, tuple) else (keys,)
            row = dict(zip(annual_columns, key_values, strict=True))
            row.update(
                analysis=analysis,
                segment=None,
                period_type="year",
                period_label=row["year"],
                date_count=group["date"].nunique(),
                mean_effect_pct=group[effect_column].mean(),
                median_effect_pct=group[effect_column].median(),
                positive_date_rate_pct=group[effect_column].gt(0).mean() * 100.0,
            )
            rows.append(row)
    return pd.DataFrame(rows)


def _build_bootstrap_effect_ci_df(
    continuous: pd.DataFrame,
    contrasts: pd.DataFrame,
    topk: pd.DataFrame,
    *,
    resamples: int,
    seed: int,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    configs = (
        (
            continuous,
            "continuous",
            ["scaffold_family", "priority_variant", "horizon"],
            "mean_lift_pct",
        ),
        (
            contrasts,
            "sign_contrast",
            ["scaffold_family", "contrast", "horizon"],
            "mean_lift_pct",
        ),
        (
            topk,
            "topk",
            ["scope", "priority_variant", "horizon", "k"],
            "priority_lift_pct",
        ),
    )
    for frame, analysis, keys, effect in configs:
        if frame.empty:
            continue
        for values, group in frame.groupby(keys, observed=True):
            values_tuple = values if isinstance(values, tuple) else (values,)
            point, lower, upper = moving_block_bootstrap_ci(
                group.sort_values("date")[effect].to_numpy(),
                block_length=int(values_tuple[keys.index("horizon")]),
                resamples=resamples,
                seed=seed,
            )
            row = dict(zip(keys, values_tuple, strict=True))
            row.update(
                analysis=analysis,
                date_count=group["date"].nunique(),
                point_estimate_pct=point,
                ci_lower_pct=lower,
                ci_upper_pct=upper,
            )
            rows.append(row)
    return pd.DataFrame(rows)


def _build_continuous_gate_evidence(
    observations: pd.DataFrame,
    continuous: pd.DataFrame,
    segments: pd.DataFrame,
    bootstrap: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    if continuous.empty:
        return pd.DataFrame(rows)
    primary = continuous.loc[continuous.get("horizon", pd.Series(dtype=int)).eq(20)]
    for (family, variant), group in primary.groupby(
        ["scaffold_family", "priority_variant"], observed=True
    ):
        ci = bootstrap.loc[
            bootstrap["analysis"].eq("continuous")
            & bootstrap["scaffold_family"].eq(family)
            & bootstrap["priority_variant"].eq(variant)
            & bootstrap["horizon"].eq(20)
        ]
        segment_rows = segments.loc[
            segments["analysis"].eq("continuous")
            & segments["period_type"].eq("segment")
            & segments["scaffold_family"].eq(family)
            & segments["priority_variant"].eq(variant)
            & segments["horizon"].eq(20)
        ]
        rows.append(
            {
                "scaffold_family": family,
                "priority_variant": variant,
                "mean_lift_pct": group["mean_lift_pct"].mean(),
                "ci_lower_pct": ci["ci_lower_pct"].iloc[0] if not ci.empty else np.nan,
                "median_spearman_ic": group["spearman_ic"].median(),
                "ic_positive_date_rate_pct": group["spearman_ic"].gt(0).mean() * 100.0,
                "all_segments_positive": len(segment_rows) == 3
                and segment_rows["mean_effect_pct"].gt(0).all(),
                "severe_loss_rate_difference_pct": group[
                    "severe_loss_rate_difference_pct"
                ].mean(),
                "observation_count": int(group["observation_count"].sum()),
                "paired_date_count": group["date"].nunique(),
                "median_focus_candidates": group["focus_candidate_count"].median(),
            }
        )
    return pd.DataFrame(rows)


def _build_badge_gate_evidence(
    fixed_2x2: pd.DataFrame,
    contrasts: pd.DataFrame,
    segments: pd.DataFrame,
    bootstrap: pd.DataFrame,
) -> pd.DataFrame:
    del fixed_2x2
    required = {"plusplus_minus_plusminus", "plusplus_minus_minusplus"}
    rows: list[dict[str, object]] = []
    if contrasts.empty:
        return pd.DataFrame(rows)
    primary = contrasts.loc[
        contrasts.get("horizon", pd.Series(dtype=int)).eq(20)
        & contrasts.get("contrast", pd.Series(dtype=str)).isin(required)
    ]
    for (family, contrast), group in primary.groupby(
        ["scaffold_family", "contrast"], observed=True
    ):
        ci = bootstrap.loc[
            bootstrap["analysis"].eq("sign_contrast")
            & bootstrap["scaffold_family"].eq(family)
            & bootstrap["contrast"].eq(contrast)
            & bootstrap["horizon"].eq(20)
        ]
        segment_rows = segments.loc[
            segments["analysis"].eq("sign_contrast")
            & segments["period_type"].eq("segment")
            & segments["scaffold_family"].eq(family)
            & segments["contrast"].eq(contrast)
            & segments["horizon"].eq(20)
        ]
        sufficient_sample = bool(
            group["focus_count"].median() >= 5 and group["control_count"].median() >= 5
        )
        passed = bool(
            sufficient_sample
            and group["median_lift_pct"].median() >= 0.25
            and group["positive"].mean() * 100.0 >= 52.0
            and not ci.empty
            and ci["ci_lower_pct"].iloc[0] > 0.0
            and len(segment_rows) == 3
            and segment_rows["mean_effect_pct"].gt(0).all()
            and group["severe_loss_rate_difference_pct"].mean() <= 1.0
        )
        rows.append(
            {
                "scaffold_family": family,
                "contrast": contrast,
                "passed": passed,
                "reason": (
                    "all_frozen_gates_pass"
                    if passed
                    else "insufficient_sample"
                    if not sufficient_sample
                    else "one_or_more_gates_failed"
                ),
            }
        )
    return pd.DataFrame(rows)


def _build_topk_gate_evidence(
    topk: pd.DataFrame,
    bootstrap: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    if topk.empty:
        return pd.DataFrame(rows)
    primary = topk.loc[
        topk.get("horizon", pd.Series(dtype=int)).eq(20)
        & topk.get("scope", pd.Series(dtype=str)).eq("combined_primary")
    ]
    for variant, group in primary.groupby("priority_variant", observed=True):
        ci = bootstrap.loc[
            bootstrap["analysis"].eq("topk")
            & bootstrap["scope"].eq("combined_primary")
            & bootstrap["priority_variant"].eq(variant)
            & bootstrap["horizon"].eq(20)
        ]
        by_k = group.groupby("k", observed=True)["priority_lift_pct"].mean()
        leave_one_out = topk.loc[
            topk["horizon"].eq(20)
            & topk["priority_variant"].eq(variant)
            & topk["scope"].isin(
                [
                    "leave_out_strict_value_long_only",
                    "leave_out_value_extension_long_only",
                ]
            )
        ]
        leave_one_out_by_scope_k = leave_one_out.groupby(["scope", "k"], observed=True)[
            "priority_lift_pct"
        ].mean()
        sufficient_sample = bool(
            {5, 10}.issubset(set(by_k.index)) and len(leave_one_out_by_scope_k) == 4
        )
        severe_by_k = group.groupby("k", observed=True)[
            "severe_loss_rate_difference_pct"
        ].mean()
        hhi_by_k = (
            group.assign(
                sector_hhi_difference=(
                    group["priority_sector_hhi"] - group["basket_sector_hhi"]
                )
            )
            .groupby("k", observed=True)["sector_hhi_difference"]
            .mean()
        )
        passed = bool(
            sufficient_sample
            and {5, 10}.issubset(set(by_k.index))
            and by_k.loc[[5, 10]].gt(0).all()
            and ci["ci_lower_pct"].gt(0).any()
            and leave_one_out_by_scope_k.ge(0.0).all()
            and severe_by_k.loc[[5, 10]].le(0.0).all()
            and hhi_by_k.loc[[5, 10]].le(0.0).all()
        )
        rows.append(
            {
                "priority_variant": variant,
                "passed": passed,
                "reason": (
                    "all_frozen_gates_pass"
                    if passed
                    else "insufficient_sample"
                    if not sufficient_sample
                    else "one_or_more_gates_failed"
                ),
            }
        )
    return pd.DataFrame(rows)


def _append_badge_topk_and_recommendation(
    decisions: pd.DataFrame,
    badge: pd.DataFrame,
    topk: pd.DataFrame,
) -> pd.DataFrame:
    badge_pass = bool(
        not badge.empty
        and set(badge["scaffold_family"]) == PRIMARY_SCAFFOLD_FAMILIES
        and badge.groupby("scaffold_family")["contrast"].nunique().eq(2).all()
        and badge["passed"].all()
    )
    badge_insufficient = bool(
        badge.empty
        or set(badge["scaffold_family"]) != PRIMARY_SCAFFOLD_FAMILIES
        or not badge.groupby("scaffold_family")["contrast"].nunique().eq(2).all()
        or badge["reason"].eq("insufficient_sample").any()
    )
    rows = decisions.to_dict("records")
    rows.append(
        {
            "decision_key": "plusplus_badge",
            "passed": badge_pass,
            "reason": (
                "all_frozen_gates_pass"
                if badge_pass
                else "insufficient_sample"
                if badge_insufficient
                else "one_or_more_gates_failed"
            ),
        }
    )
    pass_map = dict(zip(decisions["decision_key"], decisions["passed"], strict=True))
    topk_pass = (
        set(topk.loc[topk["passed"].astype(bool), "priority_variant"])
        if not topk.empty
        else set()
    )
    topk_insufficient = bool(
        topk.empty
        or topk.get("reason", pd.Series(dtype=str)).eq("insufficient_sample").any()
    )
    eligible = {
        key for key, passed in pass_map.items() if bool(passed) and key in topk_pass
    }
    any_insufficient = bool(
        badge_insufficient
        or topk_insufficient
        or decisions["reason"]
        .isin(["requires_both_primary_families", "insufficient_sample"])
        .any()
    )
    if any_insufficient:
        recommendation = "insufficient_evidence"
    elif {"fixed20_priority", "fixed60_priority"}.issubset(eligible):
        recommendation = "keep_both_fixed_20d_60d_priority"
    elif "fixed20_priority" in eligible:
        recommendation = "keep_fixed_20d_priority_only"
    elif "fixed60_priority" in eligible:
        recommendation = "keep_fixed_60d_priority_only"
    elif "fixed_equal_priority" in eligible:
        recommendation = "equal_weight_composite_priority_raw_columns_informational"
    elif badge_pass:
        recommendation = "plusplus_badge_only"
    else:
        recommendation = "remove_fixed_returns_from_priority_keep_raw_informational"
    rows.append(
        {
            "decision_key": "final_recommendation",
            "passed": recommendation not in {"insufficient_evidence"},
            "reason": recommendation,
        }
    )
    return pd.DataFrame(rows)


def _build_coverage_attrition_df(observations: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for family, group in observations.groupby("scaffold_family", observed=True):
        rows.append(
            {
                "scaffold_family": family,
                "observation_count": len(group),
                "symbol_count": group["code"].nunique(),
                "date_count": group["date"].nunique(),
                "first_date": group["date"].min(),
                "last_date": group["date"].max(),
                "median_candidates_per_date": group.groupby("date").size().median(),
                "fixed20_coverage_pct": group["fixed20_priority"].notna().mean()
                * 100.0,
                "fixed60_coverage_pct": group["fixed60_priority"].notna().mean()
                * 100.0,
                "market_codes": ",".join(sorted(set(group["market_code"].astype(str)))),
            }
        )
    return pd.DataFrame(rows)


def _build_regression_sensitivity_df(
    observations: pd.DataFrame,
    *,
    horizons: Sequence[int],
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    controls = [
        "value_composite_equal_score",
        "liquidity_residual_z",
        "atr20_change_20d_pct",
        "long_hybrid_leadership_score",
    ]
    for family, family_frame in observations.groupby("scaffold_family", observed=True):
        for variant in PRIORITY_VARIANTS:
            for horizon in horizons:
                outcome = f"forward_close_excess_return_{horizon}d_pct"
                columns = [outcome, variant, "date", *controls]
                frame = family_frame[columns].dropna().copy()
                if len(frame) < 20:
                    continue
                numeric = [outcome, variant, *controls]
                demeaned = frame[numeric] - frame.groupby("date")[numeric].transform(
                    "mean"
                )
                x = demeaned[[variant, *controls]].to_numpy(dtype=float)
                y = demeaned[outcome].to_numpy(dtype=float)
                if np.linalg.matrix_rank(x) < x.shape[1]:
                    continue
                beta = np.linalg.lstsq(x, y, rcond=None)[0]
                rows.append(
                    {
                        "sensitivity_type": "date_fixed_effect_regression",
                        "sensitivity_bucket": "all_candidates",
                        "scaffold_family": family,
                        "horizon": int(horizon),
                        "priority_variant": variant,
                        "observation_count": len(frame),
                        "date_count": frame["date"].nunique(),
                        "date_fixed_effect_priority_coefficient": beta[0],
                        "mean_priority_lift_pct": np.nan,
                        "controls": ",".join(controls),
                        "role": "sensitivity_only",
                    }
                )
    sensitivity_specs: list[tuple[str, str, pd.DataFrame]] = []
    z = observations["liquidity_residual_z"]
    sensitivity_specs.extend(
        [
            ("liquidity_z_band", "z_lt_minus1", observations.loc[z.lt(-1.0)]),
            (
                "liquidity_z_band",
                "z_minus1_to_1",
                observations.loc[z.ge(-1.0) & z.lt(1.0)],
            ),
            (
                "liquidity_z_band",
                "z_1_to_2",
                observations.loc[z.ge(1.0) & z.lt(2.0)],
            ),
            ("liquidity_z_band", "z_ge_2", observations.loc[z.ge(2.0)]),
            (
                "bank_exclusion",
                "exclude_banks",
                observations.loc[observations["sector_33_name"].ne("銀行業")],
            ),
        ]
    )
    for sensitivity_type, bucket, frame in sensitivity_specs:
        rows.extend(
            _continuous_sensitivity_rows(
                frame,
                horizons=horizons,
                sensitivity_type=sensitivity_type,
                sensitivity_bucket=bucket,
                benchmark="topix",
            )
        )
    rows.extend(
        _continuous_sensitivity_rows(
            observations,
            horizons=horizons,
            sensitivity_type="benchmark",
            sensitivity_bucket="n225_excess",
            benchmark="n225",
        )
    )
    rows.extend(_sector_equal_sensitivity_rows(observations, horizons=horizons))
    rows.extend(_nonnegative_boundary_rows(observations, horizons=horizons))
    for family, family_frame in observations.groupby("scaffold_family", observed=True):
        negative_buckets = (
            (
                "deep_pullback_20d_le_minus10",
                family_frame["recent_return_20d_pct"].le(-10.0),
            ),
            (
                "shallow_negative_20d_minus10_to_0",
                family_frame["recent_return_20d_pct"].gt(-10.0)
                & family_frame["recent_return_20d_pct"].lt(0.0),
            ),
            ("nonnegative_20d", family_frame["recent_return_20d_pct"].ge(0.0)),
        )
        for bucket, mask in negative_buckets:
            subset = family_frame.loc[mask]
            for horizon in horizons:
                outcome = f"forward_close_excess_return_{horizon}d_pct"
                values = subset[outcome].dropna()
                rows.append(
                    {
                        "sensitivity_type": "negative_20d_path",
                        "sensitivity_bucket": bucket,
                        "scaffold_family": family,
                        "horizon": int(horizon),
                        "priority_variant": "not_applicable",
                        "observation_count": len(values),
                        "date_count": subset.loc[values.index, "date"].nunique(),
                        "date_fixed_effect_priority_coefficient": np.nan,
                        "mean_priority_lift_pct": np.nan,
                        "mean_excess_return_pct": values.mean(),
                        "controls": "none",
                        "role": "sensitivity_only",
                    }
                )
    required_types = {
        "date_fixed_effect_regression",
        "liquidity_z_band",
        "bank_exclusion",
        "benchmark",
        "negative_20d_path",
        "sector_equal_weight",
        "nonnegative_boundary",
    }
    present_types = {str(row["sensitivity_type"]) for row in rows}
    for missing_type in sorted(required_types - present_types):
        rows.append(
            {
                "sensitivity_type": missing_type,
                "sensitivity_bucket": "insufficient_observations",
                "scaffold_family": "all_primary",
                "horizon": 20,
                "priority_variant": "not_applicable",
                "observation_count": 0,
                "date_count": 0,
                "date_fixed_effect_priority_coefficient": np.nan,
                "mean_priority_lift_pct": np.nan,
                "mean_excess_return_pct": np.nan,
                "controls": "none",
                "role": "sensitivity_only",
            }
        )
    return pd.DataFrame(rows)


def _continuous_sensitivity_rows(
    observations: pd.DataFrame,
    *,
    horizons: Sequence[int],
    sensitivity_type: str,
    sensitivity_bucket: str,
    benchmark: Literal["topix", "n225"],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for family, family_frame in observations.groupby("scaffold_family", observed=True):
        for variant in PRIORITY_VARIANTS:
            for horizon in horizons:
                outcome = (
                    f"forward_close_excess_return_{horizon}d_pct"
                    if benchmark == "topix"
                    else f"forward_close_n225_excess_return_{horizon}d_pct"
                )
                daily_lifts: list[float] = []
                observation_count = 0
                candidate_outcome_count = 0
                selected_count = 0
                selected_outcome_count = 0
                evaluated_date_count = 0
                incomplete_date_count = 0
                for _, date_frame in family_frame.groupby("date", observed=True):
                    evaluated = _evaluate_priority_tails(
                        date_frame,
                        score_column=variant,
                        outcome_column=outcome,
                    )
                    if evaluated is None or len(evaluated.top) < 2:
                        continue
                    evaluated_date_count += 1
                    observation_count += evaluated.candidate_count
                    candidate_outcome_count += evaluated.candidate_outcome_count
                    selected_count += len(evaluated.selected)
                    selected_outcome_count += evaluated.selected_outcome_count
                    if evaluated.outcome_status != "complete":
                        incomplete_date_count += 1
                        continue
                    daily_lifts.append(
                        float(
                            evaluated.top[outcome].mean()
                            - evaluated.bottom[outcome].mean()
                        )
                    )
                rows.append(
                    {
                        "sensitivity_type": sensitivity_type,
                        "sensitivity_bucket": sensitivity_bucket,
                        "scaffold_family": family,
                        "horizon": int(horizon),
                        "priority_variant": variant,
                        "observation_count": observation_count,
                        "candidate_outcome_count": candidate_outcome_count,
                        "candidate_outcome_coverage_pct": (
                            candidate_outcome_count / observation_count * 100.0
                            if observation_count
                            else np.nan
                        ),
                        "selected_count": selected_count,
                        "selected_outcome_count": selected_outcome_count,
                        "selected_outcome_coverage_pct": (
                            selected_outcome_count / selected_count * 100.0
                            if selected_count
                            else np.nan
                        ),
                        "evaluated_date_count": evaluated_date_count,
                        "incomplete_date_count": incomplete_date_count,
                        "outcome_status": (
                            "complete"
                            if evaluated_date_count and not incomplete_date_count
                            else "incomplete"
                        ),
                        "date_count": len(daily_lifts),
                        "date_fixed_effect_priority_coefficient": np.nan,
                        "mean_priority_lift_pct": (
                            float(np.mean(daily_lifts)) if daily_lifts else np.nan
                        ),
                        "mean_excess_return_pct": np.nan,
                        "controls": "none",
                        "role": "sensitivity_only",
                    }
                )
    return rows


def _sector_equal_sensitivity_rows(
    observations: pd.DataFrame,
    *,
    horizons: Sequence[int],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for family, family_frame in observations.groupby("scaffold_family", observed=True):
        for variant in PRIORITY_VARIANTS:
            for horizon in horizons:
                outcome = f"forward_close_excess_return_{horizon}d_pct"
                daily_lifts: list[float] = []
                observation_count = 0
                candidate_outcome_count = 0
                selected_count = 0
                selected_outcome_count = 0
                evaluated_date_count = 0
                incomplete_date_count = 0
                for _, date_frame in family_frame.groupby("date", observed=True):
                    evaluated = _evaluate_priority_tails(
                        date_frame.dropna(subset=["sector_33_code"]),
                        score_column=variant,
                        outcome_column=outcome,
                        extra_signal_columns=("sector_33_code",),
                    )
                    if evaluated is None or len(evaluated.top) < 2:
                        continue
                    evaluated_date_count += 1
                    observation_count += evaluated.candidate_count
                    candidate_outcome_count += evaluated.candidate_outcome_count
                    selected_count += len(evaluated.selected)
                    selected_outcome_count += evaluated.selected_outcome_count
                    if evaluated.outcome_status != "complete":
                        incomplete_date_count += 1
                        continue
                    bottom = evaluated.bottom
                    top = evaluated.top
                    daily_lifts.append(
                        float(
                            top.groupby("sector_33_code", observed=True)[outcome]
                            .mean()
                            .mean()
                            - bottom.groupby("sector_33_code", observed=True)[outcome]
                            .mean()
                            .mean()
                        )
                    )
                rows.append(
                    {
                        "sensitivity_type": "sector_equal_weight",
                        "sensitivity_bucket": "date_top_bottom_sector_equal",
                        "scaffold_family": family,
                        "horizon": int(horizon),
                        "priority_variant": variant,
                        "observation_count": observation_count,
                        "candidate_outcome_count": candidate_outcome_count,
                        "candidate_outcome_coverage_pct": (
                            candidate_outcome_count / observation_count * 100.0
                            if observation_count
                            else np.nan
                        ),
                        "selected_count": selected_count,
                        "selected_outcome_count": selected_outcome_count,
                        "selected_outcome_coverage_pct": (
                            selected_outcome_count / selected_count * 100.0
                            if selected_count
                            else np.nan
                        ),
                        "evaluated_date_count": evaluated_date_count,
                        "incomplete_date_count": incomplete_date_count,
                        "outcome_status": (
                            "complete"
                            if evaluated_date_count and not incomplete_date_count
                            else "incomplete"
                        ),
                        "date_count": len(daily_lifts),
                        "date_fixed_effect_priority_coefficient": np.nan,
                        "mean_priority_lift_pct": (
                            float(np.mean(daily_lifts)) if daily_lifts else np.nan
                        ),
                        "mean_excess_return_pct": np.nan,
                        "controls": "none",
                        "role": "sensitivity_only",
                    }
                )
    return rows


def _nonnegative_boundary_rows(
    observations: pd.DataFrame,
    *,
    horizons: Sequence[int],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for family, family_frame in observations.groupby("scaffold_family", observed=True):
        for horizon in horizons:
            outcome = f"forward_close_excess_return_{horizon}d_pct"
            daily_lifts: list[float] = []
            observation_count = 0
            for _, date_frame in family_frame.groupby("date", observed=True):
                eligible = date_frame.dropna(
                    subset=["recent_return_20d_pct", "recent_return_60d_pct", outcome]
                )
                focus_mask = eligible["recent_return_20d_pct"].ge(0.0) & eligible[
                    "recent_return_60d_pct"
                ].ge(0.0)
                focus = eligible.loc[focus_mask, outcome]
                control = eligible.loc[~focus_mask, outcome]
                if len(focus) < 2 or len(control) < 2:
                    continue
                observation_count += len(focus) + len(control)
                daily_lifts.append(float(focus.mean() - control.mean()))
            rows.append(
                {
                    "sensitivity_type": "nonnegative_boundary",
                    "sensitivity_bucket": "both_ge_zero_minus_other",
                    "scaffold_family": family,
                    "horizon": int(horizon),
                    "priority_variant": "plusplus_badge_nonnegative_boundary",
                    "observation_count": observation_count,
                    "date_count": len(daily_lifts),
                    "date_fixed_effect_priority_coefficient": np.nan,
                    "mean_priority_lift_pct": (
                        float(np.mean(daily_lifts)) if daily_lifts else np.nan
                    ),
                    "mean_excess_return_pct": np.nan,
                    "controls": "none",
                    "role": "sensitivity_only",
                }
            )
    return rows


def write_ranking_fixed_return_priority_evidence_bundle(
    result: RankingFixedReturnPriorityEvidenceResult,
    *,
    output_root: str | Path | None = None,
    run_id: str | None = None,
    notes: str | None = None,
) -> ResearchBundleInfo:
    tables = {
        "coverage_attrition": _bundle_safe_frame(result.coverage_attrition_df),
        "scaffold_registry": _bundle_safe_frame(result.scaffold_registry_df),
        "continuous_priority_lift": _bundle_safe_frame(
            result.continuous_priority_lift_df
        ),
        "fixed_2x2_daily": _bundle_safe_frame(result.fixed_2x2_daily_df),
        "fixed_incremental_contrast": _bundle_safe_frame(
            result.fixed_incremental_contrast_df
        ),
        "topk_priority_lift": _bundle_safe_frame(result.topk_priority_lift_df),
        "segment_stability": _bundle_safe_frame(result.segment_stability_df),
        "bootstrap_effect_ci": _bundle_safe_frame(result.bootstrap_effect_ci_df),
        "regression_sensitivity": _bundle_safe_frame(result.regression_sensitivity_df),
        "decision_gate": _bundle_safe_frame(result.decision_gate_df),
        "observation_sample": _bundle_safe_frame(result.observation_sample_df),
    }
    if set(tables) != REQUIRED_BUNDLE_TABLES:
        raise RuntimeError("fixed return priority bundle table contract drift")
    return write_research_bundle(
        experiment_id=RANKING_FIXED_RETURN_PRIORITY_EXPERIMENT_ID,
        module="src.domains.analytics.ranking_fixed_return_priority_evidence",
        function="run_ranking_fixed_return_priority_evidence_research",
        params={
            "horizons": list(result.horizons),
            "market_scope": "prime",
            "market_codes": list(PRIME_EQUIVALENT_MARKET_CODES),
            "bootstrap_resamples": result.bootstrap_resamples,
            "bootstrap_seed": result.bootstrap_seed,
            "scaffold_families": sorted(PRIMARY_SCAFFOLD_FAMILIES),
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
            "candidate_selection": "fixed_return_free",
            "price_projection": result.price_projection.to_manifest_payload(),
        },
        result_tables=tables,
        summary_markdown=build_summary_markdown(result),
        output_root=output_root,
        run_id=run_id,
        notes=notes,
    )


def _bundle_safe_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if len(frame.columns) > 0:
        return frame
    return pd.DataFrame({"status": pd.Series(dtype="object")})


def build_summary_markdown(result: RankingFixedReturnPriorityEvidenceResult) -> str:
    parts = [
        "# Ranking Fixed Return Priority Evidence",
        "",
        "## Metadata",
        "",
        f"- db_path: `{result.db_path}`",
        f"- source_mode: `{result.source_mode}`",
        f"- source_detail: `{result.source_detail}`",
        "- market_scope: `prime` (`0101`, `0111`, exact signal date)",
        f"- analysis_start_date: `{result.analysis_start_date}`",
        f"- analysis_end_date: `{result.analysis_end_date}`",
        f"- observation_count: `{result.observation_count}`",
        "- candidate_selection: `fixed_return_free`",
        "- Physical price source: `stock_data_raw`",
        "- `stock_data` fallback: `false`",
        f"- Signal price policy: `{result.price_projection.signal_basis_policy}`",
        f"- Outcome price policy: `{result.price_projection.completion_basis_policy}`",
        "- Price projection rows: canonical raw "
        f"`{result.price_projection.canonical_raw_row_count}` / signal features "
        f"`{result.price_projection.signal_feature_row_count}` / outcome requests "
        f"`{result.price_projection.outcome_request_row_count}` / completed outcomes "
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
        ("Decision Gate", result.decision_gate_df, 40),
        ("Coverage Attrition", result.coverage_attrition_df, 40),
        ("Scaffold Registry", result.scaffold_registry_df, 20),
        ("Continuous Priority Lift", result.continuous_priority_lift_df, 100),
        ("Fixed 2x2 Daily", result.fixed_2x2_daily_df, 100),
        ("Fixed Incremental Contrast", result.fixed_incremental_contrast_df, 100),
        ("Top-K Priority Lift", result.topk_priority_lift_df, 100),
        ("Segment Stability", result.segment_stability_df, 100),
        ("Bootstrap Effect CI", result.bootstrap_effect_ci_df, 100),
        ("Regression Sensitivity", result.regression_sensitivity_df, 100),
        ("Observation Sample", result.observation_sample_df, 30),
    )
    for title, frame, limit in sections:
        parts.extend(
            ["", f"## {title}", "", _top_rows_for_markdown(frame, limit=limit)]
        )
    return "\n".join(parts).rstrip() + "\n"
