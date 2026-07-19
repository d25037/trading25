"""Selection-first contracts for ranking research outcome evaluation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import pandas as pd


@dataclass(frozen=True)
class FrozenTopKSelection:
    """A signal-time ranking whose outcome coverage is reported separately."""

    candidates: pd.DataFrame
    selected: pd.DataFrame
    candidate_outcomes: pd.Series
    selected_outcomes: pd.Series
    candidate_count: int
    candidate_outcome_count: int
    candidate_outcome_coverage_pct: float
    selected_outcome_count: int
    selected_outcome_coverage_pct: float
    outcome_status: str


def select_frozen_topk(
    frame: pd.DataFrame,
    *,
    score_columns: Sequence[str],
    outcome_column: str,
    k: int,
    ascending: Sequence[bool],
) -> FrozenTopKSelection:
    """Freeze a deterministic top-k before inspecting forward outcomes."""

    if k <= 0:
        raise ValueError("k must be positive")
    if not score_columns:
        raise ValueError("score_columns must not be empty")
    if len(score_columns) != len(ascending):
        raise ValueError("score_columns and ascending must have matching lengths")
    required_columns = {"code", *score_columns, outcome_column}
    missing = required_columns.difference(frame.columns)
    if missing:
        raise ValueError(f"frame is missing required columns: {sorted(missing)}")

    candidates = frame.sort_values(
        [*score_columns, "code"],
        ascending=[*ascending, True],
        kind="mergesort",
    ).copy()
    selected = candidates.head(k).copy()
    candidate_outcomes = pd.to_numeric(candidates[outcome_column], errors="coerce")
    selected_outcomes = pd.to_numeric(selected[outcome_column], errors="coerce")
    candidate_count = len(candidates)
    selected_count = len(selected)
    candidate_outcome_count = int(candidate_outcomes.notna().sum())
    selected_outcome_count = int(selected_outcomes.notna().sum())
    complete = bool(
        candidate_outcomes.notna().all() and selected_outcomes.notna().all()
    )
    return FrozenTopKSelection(
        candidates=candidates,
        selected=selected,
        candidate_outcomes=candidate_outcomes,
        selected_outcomes=selected_outcomes,
        candidate_count=candidate_count,
        candidate_outcome_count=candidate_outcome_count,
        candidate_outcome_coverage_pct=(
            candidate_outcome_count / candidate_count * 100.0
            if candidate_count
            else float("nan")
        ),
        selected_outcome_count=selected_outcome_count,
        selected_outcome_coverage_pct=(
            selected_outcome_count / selected_count * 100.0
            if selected_count
            else float("nan")
        ),
        outcome_status="complete" if complete else "incomplete_outcomes",
    )
