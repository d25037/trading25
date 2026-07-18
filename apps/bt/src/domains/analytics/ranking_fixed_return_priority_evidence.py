"""Fixed 20D/60D Ranking priority evidence inside fixed-free long scaffolds."""

# pyright: reportArgumentType=false, reportAssignmentType=false, reportUnusedFunction=false

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Literal, Sequence

import numpy as np
import pandas as pd

from src.domains.analytics.atr_expansion_forward_response import (
    _create_observation_panel as _create_atr_observation_panel,
)
from src.domains.analytics.daily_ranking_research_base import (
    assert_daily_ranking_research_tables,
    create_daily_ranking_research_panel,
    daily_ranking_query_end_date,
    daily_ranking_query_start_date,
)
from src.domains.analytics.earnings_holdthrough_expectancy_report import (
    _top_rows_for_markdown,
)
from src.domains.analytics.ranking_long_scaffold_value_composite_evidence import (
    _create_value_composite_panel,
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
_REQUIRED_ATR_WINDOWS = (20, 60)
_REQUIRED_RETURN_WINDOWS = (20, 60)
_WARMUP_CALENDAR_DAYS = 820
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
            continuous_evidence.get(
                "priority_variant", pd.Series(dtype="object")
            ).eq(variant)
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
        passed = bool(
            subset["mean_lift_pct"].ge(0.25).all()
            and subset["ci_lower_pct"].gt(0.0).all()
            and subset["median_spearman_ic"].ge(0.02).all()
            and subset["ic_positive_date_rate_pct"].ge(52.0).all()
            and subset["all_segments_positive"].astype(bool).all()
            and subset["severe_loss_rate_difference_pct"].le(1.0).all()
            and subset["observation_count"].ge(300).all()
            and subset["paired_date_count"].ge(50).all()
            and subset["median_focus_candidates"].ge(5.0).all()
        )
        rows.append(
            {
                "decision_key": variant,
                "passed": passed,
                "reason": "all_frozen_gates_pass" if passed else "one_or_more_gates_failed",
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
        raise ValueError("bootstrap_resamples and observation_sample_limit must be positive")
    db_path_obj = Path(db_path).expanduser().resolve()
    if not db_path_obj.is_file():
        raise FileNotFoundError(f"market.duckdb was not found: {db_path_obj}")

    query_start = daily_ranking_query_start_date(
        start_date,
        warmup_calendar_days=_WARMUP_CALENDAR_DAYS,
    )
    query_end = daily_ranking_query_end_date(end_date, max_horizon=max(resolved_horizons))
    market_source = "stock_master_daily_exact_date"
    with open_readonly_analysis_connection(
        str(db_path_obj),
        snapshot_prefix="ranking-fixed-return-priority-",
    ) as ctx:
        assert_daily_ranking_research_tables(ctx.connection)
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
        _create_value_composite_panel(ctx.connection)
        observations = _query_fixed_free_observations(
            ctx.connection,
            horizons=resolved_horizons,
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
        segments = _build_segment_stability_df(continuous, contrasts, topk)
        bootstrap = _build_bootstrap_effect_ci_df(
            continuous,
            contrasts,
            topk,
            resamples=bootstrap_resamples,
            seed=bootstrap_seed,
        )
        continuous_gate = _build_continuous_gate_evidence(
            observations,
            continuous,
            segments,
            bootstrap,
        )
        badge_gate = _build_badge_gate_evidence(
            fixed_2x2,
            contrasts,
            segments,
            bootstrap,
        )
        topk_gate = _build_topk_gate_evidence(topk, bootstrap)
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


def _query_fixed_free_observations(
    conn: Any,
    *,
    horizons: Sequence[int],
) -> pd.DataFrame:
    prime_codes_sql = ", ".join(f"'{item}'" for item in PRIME_EQUIVALENT_MARKET_CODES)
    forward_columns = ",\n            ".join(
        f"forward_close_excess_return_{int(horizon)}d_pct" for horizon in horizons
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
        FROM ranking_long_scaffold_value_composite_panel p
        WHERE p.market_scope = 'prime'
          AND p.market_code IN ({prime_codes_sql})
        """
    )
    frame = conn.execute(
        f"""
        WITH candidates AS (
            SELECT 'strict_value_long_only' AS scaffold_family, *
            FROM ranking_fixed_return_prime_ranked
            WHERE valuation_signal = 'strong_value_confirmation'
              AND long_hybrid_leadership_score >= 0.799999
              AND atr20_acceleration_flag
            UNION ALL
            SELECT 'value_extension_long_only' AS scaffold_family, *
            FROM ranking_fixed_return_prime_ranked
            WHERE value_composite_equal_score >= 0.8
              AND valuation_signal <> 'strong_value_confirmation'
              AND long_hybrid_leadership_score >= 0.799999
              AND atr20_acceleration_flag
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
    rows: list[dict[str, object]] = []
    if observations.empty:
        return pd.DataFrame(rows)
    for (family, signal_date), group in observations.groupby(
        ["scaffold_family", "date"], observed=True
    ):
        for variant in PRIORITY_VARIANTS:
            for horizon in horizons:
                outcome = f"forward_close_excess_return_{horizon}d_pct"
                eligible = group.dropna(subset=[variant, outcome]).sort_values(variant)
                focus_count = int(np.floor(len(eligible) * 0.2))
                if focus_count < 2:
                    continue
                bottom = eligible.head(focus_count)
                top = eligible.tail(focus_count)
                rows.append(
                    {
                        "scaffold_family": family,
                        "date": signal_date,
                        "horizon": int(horizon),
                        "priority_variant": variant,
                        "observation_count": len(eligible),
                        "focus_candidate_count": focus_count,
                        "bottom_mean_excess_return_pct": bottom[outcome].mean(),
                        "top_mean_excess_return_pct": top[outcome].mean(),
                        "mean_lift_pct": top[outcome].mean() - bottom[outcome].mean(),
                        "bottom_median_excess_return_pct": bottom[outcome].median(),
                        "top_median_excess_return_pct": top[outcome].median(),
                        "median_lift_pct": top[outcome].median() - bottom[outcome].median(),
                        "bottom_severe_loss_rate_pct": bottom[outcome].le(
                            DEFAULT_SEVERE_LOSS_THRESHOLD_PCT
                        ).mean()
                        * 100.0,
                        "top_severe_loss_rate_pct": top[outcome].le(
                            DEFAULT_SEVERE_LOSS_THRESHOLD_PCT
                        ).mean()
                        * 100.0,
                        "severe_loss_rate_difference_pct": top[outcome].le(
                            DEFAULT_SEVERE_LOSS_THRESHOLD_PCT
                        ).mean()
                        * 100.0
                        - bottom[outcome].le(DEFAULT_SEVERE_LOSS_THRESHOLD_PCT).mean()
                        * 100.0,
                        "spearman_ic": eligible[variant].corr(
                            eligible[outcome], method="spearman"
                        ),
                    }
                )
    return pd.DataFrame(rows)


def _build_fixed_2x2_daily_df(
    observations: pd.DataFrame,
    *,
    horizons: Sequence[int],
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    strict = observations.loc[observations.get("fixed_quadrant", "").isin(["++", "+-", "-+", "--"])]
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
                focus = group.loc[group["fixed_quadrant"].isin(focus_cells), outcome].dropna()
                control = group.loc[group["fixed_quadrant"].isin(control_cells), outcome].dropna()
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
                        - control.le(DEFAULT_SEVERE_LOSS_THRESHOLD_PCT).mean()
                        * 100.0,
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
                    eligible = group.dropna(subset=[variant, outcome])
                    for k in (5, 10):
                        if len(eligible) < 2 * k:
                            continue
                        selected = eligible.nlargest(k, variant)
                        rows.append(
                            {
                                "scope": scope,
                                "date": signal_date,
                                "horizon": int(horizon),
                                "priority_variant": variant,
                                "k": k,
                                "candidate_count": len(eligible),
                                "basket_mean_excess_return_pct": eligible[outcome].mean(),
                                "priority_mean_excess_return_pct": selected[outcome].mean(),
                                "priority_lift_pct": selected[outcome].mean()
                                - eligible[outcome].mean(),
                                "basket_severe_loss_rate_pct": eligible[outcome].le(
                                    DEFAULT_SEVERE_LOSS_THRESHOLD_PCT
                                ).mean()
                                * 100.0,
                                "priority_severe_loss_rate_pct": selected[outcome].le(
                                    DEFAULT_SEVERE_LOSS_THRESHOLD_PCT
                                ).mean()
                                * 100.0,
                                "severe_loss_rate_difference_pct": selected[outcome].le(
                                    DEFAULT_SEVERE_LOSS_THRESHOLD_PCT
                                ).mean()
                                * 100.0
                                - eligible[outcome].le(
                                    DEFAULT_SEVERE_LOSS_THRESHOLD_PCT
                                ).mean()
                                * 100.0,
                                "basket_sector_hhi": _sector_hhi(eligible),
                                "priority_sector_hhi": _sector_hhi(selected),
                            }
                        )
    return pd.DataFrame(rows)


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
        group_columns = [column for column in ("scaffold_family", "scope", key_column, "horizon", "k", "segment") if column in working]
        for keys, group in working.groupby(group_columns, observed=True, dropna=False):
            key_values = keys if isinstance(keys, tuple) else (keys,)
            row = dict(zip(group_columns, key_values, strict=True))
            row.update(
                analysis=analysis,
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
        (continuous, "continuous", ["scaffold_family", "priority_variant", "horizon"], "mean_lift_pct"),
        (contrasts, "sign_contrast", ["scaffold_family", "contrast", "horizon"], "mean_lift_pct"),
        (topk, "topk", ["scope", "priority_variant", "horizon", "k"], "priority_lift_pct"),
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
            & segments["scaffold_family"].eq(family)
            & segments["priority_variant"].eq(variant)
            & segments["horizon"].eq(20)
        ]
        family_obs = observations.loc[observations["scaffold_family"].eq(family)]
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
                "observation_count": len(family_obs),
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
            & segments["scaffold_family"].eq(family)
            & segments["contrast"].eq(contrast)
            & segments["horizon"].eq(20)
        ]
        rows.append(
            {
                "scaffold_family": family,
                "contrast": contrast,
                "passed": bool(
                    group["median_lift_pct"].median() >= 0.25
                    and group["positive"].mean() * 100.0 >= 52.0
                    and not ci.empty
                    and ci["ci_lower_pct"].iloc[0] > 0.0
                    and len(segment_rows) == 3
                    and segment_rows["mean_effect_pct"].gt(0).all()
                    and group["focus_count"].median() >= 5
                    and group["control_count"].median() >= 5
                    and group["severe_loss_rate_difference_pct"].mean() <= 1.0
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
        rows.append(
            {
                "priority_variant": variant,
                "passed": bool(
                    {5, 10}.issubset(set(by_k.index))
                    and by_k.loc[[5, 10]].gt(0).all()
                    and ci["ci_lower_pct"].gt(0).any()
                    and group["severe_loss_rate_difference_pct"].mean() <= 0.0
                    and (
                        group["priority_sector_hhi"] - group["basket_sector_hhi"]
                    ).mean()
                    <= 0.0
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
    rows = decisions.to_dict("records")
    rows.append(
        {
            "decision_key": "plusplus_badge",
            "passed": badge_pass,
            "reason": "all_frozen_gates_pass" if badge_pass else "one_or_more_gates_failed",
        }
    )
    pass_map = dict(zip(decisions["decision_key"], decisions["passed"], strict=True))
    topk_pass = (
        set(topk.loc[topk["passed"].astype(bool), "priority_variant"])
        if not topk.empty
        else set()
    )
    eligible = {key for key, passed in pass_map.items() if bool(passed) and key in topk_pass}
    if {"fixed20_priority", "fixed60_priority"}.issubset(eligible):
        recommendation = "keep_both_fixed_20d_60d_priority"
    elif "fixed20_priority" in eligible:
        recommendation = "keep_fixed_20d_priority_only"
    elif "fixed60_priority" in eligible:
        recommendation = "keep_fixed_60d_priority_only"
    elif "fixed_equal_priority" in eligible:
        recommendation = "equal_weight_composite_priority_raw_columns_informational"
    elif badge_pass:
        recommendation = "plusplus_badge_only"
    elif decisions["reason"].eq("requires_both_primary_families").all():
        recommendation = "insufficient_evidence"
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
                "fixed20_coverage_pct": group["fixed20_priority"].notna().mean() * 100.0,
                "fixed60_coverage_pct": group["fixed60_priority"].notna().mean() * 100.0,
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
                demeaned = frame[numeric] - frame.groupby("date")[numeric].transform("mean")
                x = demeaned[[variant, *controls]].to_numpy(dtype=float)
                y = demeaned[outcome].to_numpy(dtype=float)
                if np.linalg.matrix_rank(x) < x.shape[1]:
                    continue
                beta = np.linalg.lstsq(x, y, rcond=None)[0]
                rows.append(
                    {
                        "scaffold_family": family,
                        "horizon": int(horizon),
                        "priority_variant": variant,
                        "observation_count": len(frame),
                        "date_count": frame["date"].nunique(),
                        "date_fixed_effect_priority_coefficient": beta[0],
                        "controls": ",".join(controls),
                        "role": "sensitivity_only",
                    }
                )
    return pd.DataFrame(rows)


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
        "regression_sensitivity": _bundle_safe_frame(
            result.regression_sensitivity_df
        ),
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
        parts.extend(["", f"## {title}", "", _top_rows_for_markdown(frame, limit=limit)])
    return "\n".join(parts).rstrip() + "\n"
