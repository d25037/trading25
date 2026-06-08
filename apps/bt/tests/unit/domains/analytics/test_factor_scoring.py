from __future__ import annotations

import pandas as pd

from src.domains.analytics.research_core.factor_scoring import (
    assign_ordered_buckets,
    score_within_groups,
)


def test_score_within_groups_scores_valid_values_and_keeps_missing_nan() -> None:
    frame = pd.DataFrame(
        {
            "year": [2024, 2024, 2024, 2025],
            "market": ["prime", "prime", "prime", "prime"],
            "value": [10.0, 20.0, None, 30.0],
        },
        index=["a", "b", "c", "d"],
    )

    scores = score_within_groups(frame, "value", group_columns=("year", "market"))

    assert scores.loc["a"] == 0.0
    assert scores.loc["b"] == 1.0
    assert pd.isna(scores.loc["c"])
    assert scores.loc["d"] == 0.5


def test_score_within_groups_can_prefer_low_values() -> None:
    frame = pd.DataFrame({"group": ["x", "x"], "value": [10.0, 20.0]})

    scores = score_within_groups(frame, "value", group_columns=("group",), prefer_low=True)

    assert scores.tolist() == [1.0, 0.0]


def test_assign_ordered_buckets_requires_enough_valid_values_per_group() -> None:
    frame = pd.DataFrame(
        {
            "group": ["x", "x", "x", "x", "y"],
            "score": [0.4, 0.1, 0.2, 0.3, 0.9],
        },
        index=["a", "b", "c", "d", "e"],
    )

    buckets = assign_ordered_buckets(
        frame,
        "score",
        group_columns=("group",),
        bucket_count=4,
        min_observations=4,
    )

    assert buckets.loc["b"] == 1.0
    assert buckets.loc["c"] == 2.0
    assert buckets.loc["d"] == 3.0
    assert buckets.loc["a"] == 4.0
    assert pd.isna(buckets.loc["e"])


def test_score_within_groups_can_include_null_group_keys() -> None:
    frame = pd.DataFrame({"group": ["x", None, None], "value": [1.0, 2.0, 3.0]})

    scores = score_within_groups(
        frame,
        "value",
        group_columns=("group",),
        min_observations=2,
        dropna=False,
    )

    assert pd.isna(scores.iloc[0])
    assert scores.iloc[1:].tolist() == [0.0, 1.0]
