"""Shared factor scoring helpers for research modules."""

from __future__ import annotations

from collections.abc import Sequence

import numpy as np
import pandas as pd


def score_within_groups(
    frame: pd.DataFrame,
    source_column: str,
    *,
    group_columns: Sequence[str],
    prefer_low: bool = False,
    min_observations: int = 1,
    dropna: bool = True,
) -> pd.Series:
    values = pd.to_numeric(frame[source_column], errors="coerce")
    scores = pd.Series(np.nan, index=frame.index, dtype="float64")
    for _, group in frame.groupby(list(group_columns), dropna=dropna, sort=False):
        valid = values.loc[group.index].dropna()
        count = len(valid)
        if count < min_observations:
            continue
        if count == 1:
            ranked = pd.Series(0.5, index=valid.index, dtype="float64")
        else:
            ranked = (valid.rank(method="average") - 1.0) / float(count - 1)
        if prefer_low:
            ranked = 1.0 - ranked
        scores.loc[ranked.index] = ranked.astype(float)
    return scores


def assign_ordered_buckets(
    frame: pd.DataFrame,
    score_column: str,
    *,
    group_columns: Sequence[str],
    bucket_count: int = 5,
    min_observations: int | None = None,
    observed: bool = False,
    dropna: bool = True,
) -> pd.Series:
    values = pd.to_numeric(frame[score_column], errors="coerce")
    buckets = pd.Series(np.nan, index=frame.index, dtype="float64")
    required_observations = bucket_count if min_observations is None else min_observations
    for _, group in frame.groupby(
        list(group_columns),
        observed=observed,
        dropna=dropna,
        sort=False,
    ):
        valid = values.loc[group.index].dropna().sort_values(kind="stable")
        count = len(valid)
        if count < required_observations:
            continue
        ranks = np.arange(count, dtype=float)
        assigned = np.floor(ranks * bucket_count / count).astype(int) + 1
        buckets.loc[valid.index] = assigned.astype(float)
    return buckets
