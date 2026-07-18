"""Pure contracts for the Ranking Technical Fit Score shape-evidence study.

This module deliberately contains no database or production-Ranking integration.
The frozen ring, raw-bin, walk-forward, and final-decision contracts are shared by
the later PIT panel, evaluation, and bundle tasks.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import numpy as np
import pandas as pd

from src.shared.utils.pandas_type_guards import finite_float_or_none


@dataclass(frozen=True)
class CandidateRingDefinition:
    """A mutually-exclusive, fixed-free candidate quality tier."""

    name: str
    predicate: str


@dataclass(frozen=True)
class RawScoreDefinition:
    """A Prime-wide raw technical percentile retained for the experiment."""

    name: str
    family: str
    is_primary: bool


RING_REGISTRY: tuple[CandidateRingDefinition, ...] = (
    CandidateRingDefinition(
        "core_high_high",
        "value_composite_equal_score >= 0.8 AND long_hybrid_leadership_score >= 0.8",
    ),
    CandidateRingDefinition(
        "near_high_high_1",
        "value_composite_equal_score >= 0.7 AND long_hybrid_leadership_score >= 0.7 "
        "AND NOT (value_composite_equal_score >= 0.8 "
        "AND long_hybrid_leadership_score >= 0.8)",
    ),
    CandidateRingDefinition(
        "near_high_high_2",
        "value_composite_equal_score >= 0.6 AND long_hybrid_leadership_score >= 0.6 "
        "AND NOT (value_composite_equal_score >= 0.7 "
        "AND long_hybrid_leadership_score >= 0.7)",
    ),
)

RAW_SCORE_REGISTRY: tuple[RawScoreDefinition, ...] = (
    RawScoreDefinition("fixed20_level", "fixed", False),
    RawScoreDefinition("fixed60_level", "fixed", False),
    RawScoreDefinition("fixed_equal_level", "fixed", True),
    RawScoreDefinition("ols20_level", "ols", False),
    RawScoreDefinition("ols60_level", "ols", False),
    RawScoreDefinition("ols_equal_level", "ols", True),
)

RAW_BIN_LABELS: tuple[str, ...] = ("q1", "q2", "q3", "q4", "q5")
RAW_BIN_BOUNDARIES: tuple[float, ...] = (0.0, 0.2, 0.4, 0.6, 0.8, 1.0)
RAW_BIN_CENTERS: tuple[float, ...] = (0.1, 0.3, 0.5, 0.7, 0.9)
DEFAULT_MIN_TRAINING_OBSERVATIONS = 200
DEFAULT_MIN_TRAINING_DATES = 50

REQUIRED_BUNDLE_TABLES = {
    "ring_registry",
    "raw_score_registry",
    "coverage_attrition",
    "raw_shape_daily",
    "raw_shape_summary",
    "walkforward_mapping",
    "oos_fit_score_lift",
    "fixed_vs_ols_paired",
    "topk_operational_lift",
    "overheat_negative_diagnostics",
    "segment_stability",
    "annual_stability",
    "bootstrap_effect_ci",
    "decision_gate",
    "observation_sample",
}

_MAPPING_COLUMNS = (
    "raw_score_name",
    "evaluation_year",
    "raw_bin",
    "bin_lower",
    "bin_upper",
    "bin_center",
    "observation_count",
    "signal_date_count",
    "expectancy_pct",
    "technical_fit_score",
    "mapping_status",
    "shape_classification",
    "training_start_date",
    "training_end_date",
)


def _as_finite_float(value: object) -> float | None:
    return finite_float_or_none(value)


def classify_candidate_ring(
    value_score: float | None, leadership_score: float | None
) -> str:
    """Return the one frozen Value/Long-Hybrid candidate ring for a row."""

    value = _as_finite_float(value_score)
    leadership = _as_finite_float(leadership_score)
    if value is None or leadership is None:
        return "missing"
    if value >= 0.8 and leadership >= 0.8:
        return "core_high_high"
    if value >= 0.7 and leadership >= 0.7:
        return "near_high_high_1"
    if value >= 0.6 and leadership >= 0.6:
        return "near_high_high_2"
    return "outside"


def classify_raw_level_bin(level: float | None) -> str:
    """Classify a closed-unit-interval percentile into one frozen raw bin."""

    numeric = _as_finite_float(level)
    if numeric is None or numeric < 0.0 or numeric > 1.0:
        return "missing"
    if numeric < 0.2:
        return "q1"
    if numeric < 0.4:
        return "q2"
    if numeric < 0.6:
        return "q3"
    if numeric < 0.8:
        return "q4"
    return "q5"


def classify_shape(
    expectancies: Sequence[float | None],
    *,
    reproduces_core_and_near: bool = False,
    positive_2022_2023: bool = False,
    positive_2024_plus: bool = False,
    severe_loss_not_worse: bool = False,
) -> str:
    """Classify a five-bin response without designating a preferred bin a priori."""

    values = [_as_finite_float(value) for value in expectancies]
    if len(values) != len(RAW_BIN_LABELS) or any(value is None for value in values):
        return "insufficient_evidence"
    finite_values = [float(value) for value in values if value is not None]
    if np.allclose(finite_values, finite_values[0]):
        return "flat"
    differences = np.diff(finite_values)
    if bool(np.all(differences >= 0.0) or np.all(differences <= 0.0)):
        return "monotonic"
    best_index = int(np.argmax(finite_values))
    if best_index in {0, len(finite_values) - 1}:
        return "unstable_shape"
    adjacent_values = (finite_values[best_index - 1], finite_values[best_index + 1])
    is_interior_winner = (
        finite_values[best_index] > max(adjacent_values)
        and finite_values[best_index] > finite_values[-1]
    )
    if (
        is_interior_winner
        and reproduces_core_and_near
        and positive_2022_2023
        and positive_2024_plus
        and severe_loss_not_worse
    ):
        return "interior_sweet_spot_confirmed"
    return "unstable_shape"


def build_walkforward_mapping(
    training: pd.DataFrame,
    evaluation_year: int,
    *,
    raw_level_column: str = "raw_level",
    outcome_column: str = "forward_topix_excess_20d_pct",
    date_column: str = "date",
    raw_score_name: str = "raw_level",
    min_observations: int = DEFAULT_MIN_TRAINING_OBSERVATIONS,
    min_signal_dates: int = DEFAULT_MIN_TRAINING_DATES,
) -> pd.DataFrame:
    """Learn a five-bin, strictly-prior-year Technical Fit mapping.

    Training expectancies are date-equal means.  Under-covered bins are represented
    with an explicit unavailable status and never produce an interpolation mapping.
    """

    required = {raw_level_column, outcome_column, date_column}
    missing = required.difference(training.columns)
    if missing:
        raise ValueError(f"training is missing required columns: {sorted(missing)}")
    if min_observations <= 0 or min_signal_dates <= 0:
        raise ValueError("training minimums must be positive")

    evaluation_start = pd.Timestamp(year=int(evaluation_year), month=1, day=1)
    source = training.loc[:, [date_column, raw_level_column, outcome_column]].copy()
    source[date_column] = pd.to_datetime(source[date_column], errors="coerce").dt.normalize()
    source[outcome_column] = pd.to_numeric(source[outcome_column], errors="coerce")
    source["raw_bin"] = source[raw_level_column].map(classify_raw_level_bin)
    usable = source.loc[
        source[date_column].notna()
        & source[date_column].lt(evaluation_start)
        & source[outcome_column].notna()
        & np.isfinite(source[outcome_column])
        & source["raw_bin"].ne("missing")
    ].copy()
    training_start = usable[date_column].min() if not usable.empty else pd.NaT
    training_end = usable[date_column].max() if not usable.empty else pd.NaT

    per_date = (
        usable.groupby(["raw_bin", date_column], observed=True)[outcome_column]
        .mean()
        .rename("date_equal_expectancy_pct")
        .reset_index()
    )
    counts_by_bin: dict[str, tuple[int, int]] = {
        str(raw_bin): (int(len(group)), int(group[date_column].nunique()))
        for raw_bin, group in usable.groupby("raw_bin", observed=True)
    }
    expectancies = per_date.groupby("raw_bin", observed=True)[
        "date_equal_expectancy_pct"
    ].mean()

    rows: list[dict[str, object]] = []
    for index, raw_bin in enumerate(RAW_BIN_LABELS):
        observation_count, signal_date_count = counts_by_bin.get(raw_bin, (0, 0))
        rows.append(
            {
                "raw_score_name": raw_score_name,
                "evaluation_year": int(evaluation_year),
                "raw_bin": raw_bin,
                "bin_lower": RAW_BIN_BOUNDARIES[index],
                "bin_upper": RAW_BIN_BOUNDARIES[index + 1],
                "bin_center": RAW_BIN_CENTERS[index],
                "observation_count": observation_count,
                "signal_date_count": signal_date_count,
                "expectancy_pct": (
                    float(expectancies.loc[raw_bin])
                    if raw_bin in expectancies.index
                    else float("nan")
                ),
                "technical_fit_score": float("nan"),
                "mapping_status": "insufficient_training_data",
                "shape_classification": "insufficient_evidence",
                "training_start_date": training_start,
                "training_end_date": training_end,
            }
        )
    mapping = pd.DataFrame(rows, columns=_MAPPING_COLUMNS)
    has_coverage = bool(
        mapping["observation_count"].ge(min_observations).all()
        and mapping["signal_date_count"].ge(min_signal_dates).all()
    )
    if not has_coverage:
        return mapping

    expectancy_values = mapping["expectancy_pct"].tolist()
    shape = classify_shape(expectancy_values)
    if shape == "flat":
        mapping["technical_fit_score"] = 0.5
        mapping["mapping_status"] = "flat"
    else:
        minimum = float(mapping["expectancy_pct"].min())
        maximum = float(mapping["expectancy_pct"].max())
        mapping["technical_fit_score"] = (
            mapping["expectancy_pct"] - minimum
        ) / (maximum - minimum)
        mapping["mapping_status"] = "ready"
    mapping["shape_classification"] = shape
    return mapping


def apply_walkforward_mapping(
    frame: pd.DataFrame,
    mapping: pd.DataFrame,
    *,
    raw_level_column: str = "raw_level",
    date_column: str = "date",
    raw_score_name: str = "raw_level",
    fit_score_column: str = "technical_fit_score",
) -> pd.DataFrame:
    """Apply only the mapping of each row's evaluation year, with no fallback."""

    if raw_level_column not in frame.columns or date_column not in frame.columns:
        raise ValueError("frame must contain raw-level and date columns")
    required_mapping = {
        "raw_score_name",
        "evaluation_year",
        "bin_center",
        "technical_fit_score",
        "mapping_status",
    }
    missing_mapping = required_mapping.difference(mapping.columns)
    if missing_mapping:
        raise ValueError(f"mapping is missing required columns: {sorted(missing_mapping)}")

    scored = frame.copy()
    scored[fit_score_column] = float("nan")
    scored["mapping_status"] = "missing_mapping"
    dates = pd.to_datetime(scored[date_column], errors="coerce")
    evaluation_years = dates.dt.year
    scored.loc[dates.isna(), "mapping_status"] = "missing_evaluation_year"

    selected = mapping.loc[mapping["raw_score_name"].eq(raw_score_name)].copy()
    for year in sorted(evaluation_years.dropna().unique()):
        row_mask = evaluation_years.eq(year)
        year_mapping = selected.loc[selected["evaluation_year"].eq(int(year))]
        if year_mapping.empty:
            continue
        if not year_mapping["mapping_status"].isin({"ready", "flat"}).all():
            scored.loc[row_mask, "mapping_status"] = "insufficient_training_data"
            continue
        ordered = year_mapping.sort_values("bin_center")
        centers = ordered["bin_center"].to_numpy(dtype=float)
        values = ordered["technical_fit_score"].to_numpy(dtype=float)
        if len(centers) != len(RAW_BIN_CENTERS) or not np.isfinite(values).all():
            scored.loc[row_mask, "mapping_status"] = "insufficient_training_data"
            continue
        raw_values = pd.to_numeric(scored.loc[row_mask, raw_level_column], errors="coerce")
        valid = raw_values.notna() & np.isfinite(raw_values) & raw_values.between(0.0, 1.0)
        if valid.any():
            scored.loc[raw_values.index[valid], fit_score_column] = np.interp(
                raw_values.loc[valid].to_numpy(dtype=float), centers, values
            )
            scored.loc[raw_values.index[valid], "mapping_status"] = "ready"
        if (~valid).any():
            scored.loc[raw_values.index[~valid], "mapping_status"] = "missing_raw_level"
    return scored


def build_decision_gate_df(
    family_evidence: pd.DataFrame,
    paired_evidence: pd.DataFrame,
) -> pd.DataFrame:
    """Apply the frozen equal-weight Fixed-versus-OLS decision precedence."""

    required_family = {"family", "passes_adoption_gate", "sufficient_sample"}
    missing_family = required_family.difference(family_evidence.columns)
    if missing_family:
        raise ValueError(
            f"family_evidence is missing required columns: {sorted(missing_family)}"
        )

    family_rows: dict[str, tuple[bool, bool]] = {}
    result_rows: list[dict[str, object]] = []
    for family in ("fixed", "ols"):
        subset = family_evidence.loc[family_evidence["family"].eq(family)]
        sufficient = bool(
            not subset.empty and subset["sufficient_sample"].astype(bool).all()
        )
        passed = bool(
            sufficient and subset["passes_adoption_gate"].astype(bool).all()
        )
        family_rows[family] = (sufficient, passed)
        result_rows.append(
            {
                "decision_key": family,
                "decision": (
                    "passes_adoption_gate"
                    if passed
                    else "fails_adoption_gate"
                    if sufficient
                    else "insufficient_evidence"
                ),
                "sufficient_sample": sufficient,
                "passed": passed,
            }
        )

    fixed_sufficient, fixed_passed = family_rows["fixed"]
    ols_sufficient, ols_passed = family_rows["ols"]
    if not fixed_sufficient or not ols_sufficient:
        decision = "insufficient_evidence"
    elif fixed_passed and not ols_passed:
        decision = "fixed_wins"
    elif ols_passed and not fixed_passed:
        decision = "ols_wins"
    elif not fixed_passed and not ols_passed:
        decision = "neither"
    else:
        required_paired = {"sufficient_sample", "ci_lower_pct", "ci_upper_pct"}
        if required_paired.difference(paired_evidence.columns) or paired_evidence.empty:
            decision = "insufficient_evidence"
        else:
            paired_sufficient = bool(paired_evidence["sufficient_sample"].astype(bool).all())
            lower = pd.to_numeric(paired_evidence["ci_lower_pct"], errors="coerce")
            upper = pd.to_numeric(paired_evidence["ci_upper_pct"], errors="coerce")
            if not paired_sufficient or not np.isfinite(lower).all() or not np.isfinite(upper).all():
                decision = "insufficient_evidence"
            elif bool(lower.gt(0.0).all()):
                decision = "fixed_wins"
            elif bool(upper.lt(0.0).all()):
                decision = "ols_wins"
            else:
                decision = "equivalent_fixed_preferred_operationally"
    result_rows.append(
        {
            "decision_key": "fixed_vs_ols",
            "decision": decision,
            "sufficient_sample": decision != "insufficient_evidence",
            "passed": decision in {"fixed_wins", "ols_wins"},
        }
    )
    return pd.DataFrame(
        result_rows,
        columns=("decision_key", "decision", "sufficient_sample", "passed"),
    )
