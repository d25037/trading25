"""Fixed 20D/60D Ranking priority evidence inside fixed-free long scaffolds."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import numpy as np
import pandas as pd

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
