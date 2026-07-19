from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.domains.analytics.ranking_research_selection_contract import (
    evaluate_frozen_selection,
    freeze_signal_percentile_buckets,
    freeze_signal_tails,
    freeze_signal_topk,
    select_frozen_topk,
)


def test_select_frozen_topk_keeps_missing_outcome_ranked_member() -> None:
    frame = pd.DataFrame(
        {
            "code": [f"{index:02d}" for index in range(20)],
            "score": list(range(20, 0, -1)),
            "outcome_pct": [np.nan, *range(1, 20)],
        }
    )

    selection = select_frozen_topk(
        frame,
        score_columns=("score",),
        outcome_column="outcome_pct",
        k=5,
        ascending=(False,),
    )

    assert selection.candidate_count == 20
    assert selection.selected["code"].tolist() == ["00", "01", "02", "03", "04"]
    assert selection.candidate_outcome_coverage_pct == 95.0
    assert selection.selected_outcome_count == 4
    assert selection.selected_outcome_coverage_pct == 80.0
    assert selection.outcome_status == "incomplete_outcomes"


def test_frozen_tails_do_not_backfill_missing_best_outcome() -> None:
    frame = pd.DataFrame(
        {
            "date": ["2026-01-05"] * 10,
            "code": [f"{index:04d}" for index in range(1, 11)],
            "score": [*range(1, 9), 10, 9],
        }
    ).sample(frac=1.0, random_state=7)
    outcomes = pd.DataFrame(
        {
            "date": ["2026-01-05"] * 9,
            "code": [f"{index:04d}" for index in range(1, 10)],
            "outcome": list(range(1, 10)),
        }
    )

    frozen = freeze_signal_tails(
        frame,
        group_columns=("date",),
        score_columns=("score",),
        fraction=0.2,
        min_side=2,
    )
    evaluated = evaluate_frozen_selection(frozen, outcomes, outcome_column="outcome")

    assert tuple(frozen.top["code"]) == ("0009", "0010")
    assert tuple(evaluated.top["code"]) == ("0009", "0010")
    assert evaluated.outcome_status == "incomplete"
    assert evaluated.effect_metrics is None


def test_frozen_topk_breaks_all_score_ties_by_normalized_code() -> None:
    frame = pd.DataFrame(
        {
            "date": ["2026-01-05"] * 3,
            "code": ["0002", "0003", "0001"],
            "score": [1.0, 1.0, 1.0],
        }
    )

    frozen = freeze_signal_topk(
        frame,
        group_columns=("date",),
        score_columns=("score",),
        k=2,
    )

    assert tuple(frozen.top["code"]) == ("0001", "0002")


def test_frozen_percentile_buckets_keep_boundary_ties_together() -> None:
    frame = pd.DataFrame(
        {
            "date": ["2026-01-05"] * 5,
            "code": [f"{index:04d}" for index in range(1, 6)],
            "signal_percentile": [0.2, 0.2, 0.5, 0.8, 0.8],
        }
    )

    frozen = freeze_signal_percentile_buckets(
        frame,
        group_columns=("date",),
        percentile_column="signal_percentile",
        lower_max=0.2,
        upper_min=0.8,
    )

    assert tuple(frozen.bottom["code"]) == ("0001", "0002")
    assert tuple(frozen.middle["code"]) == ("0003",)
    assert tuple(frozen.top["code"]) == ("0004", "0005")


def test_signal_selection_rejects_duplicate_signal_keys() -> None:
    frame = pd.DataFrame(
        {
            "date": ["2026-01-05", "2026-01-05"],
            "code": ["0001", "0001"],
            "score": [2.0, 1.0],
        }
    )

    with pytest.raises(ValueError, match="duplicate signal keys"):
        freeze_signal_topk(
            frame,
            group_columns=("date",),
            score_columns=("score",),
            k=1,
        )


def test_signal_selection_rejects_outcome_derived_selection_fields() -> None:
    frame = pd.DataFrame(
        {
            "date": ["2026-01-05"],
            "code": ["0001"],
            "forward_return": [1.0],
        }
    )

    with pytest.raises(ValueError, match="outcome-derived"):
        freeze_signal_topk(
            frame,
            group_columns=("date",),
            score_columns=("forward_return",),
            k=1,
        )


def test_evaluation_rejects_duplicate_outcome_keys_and_outcome_score() -> None:
    frozen = freeze_signal_topk(
        pd.DataFrame(
            {
                "date": ["2026-01-05"],
                "code": ["0001"],
                "score": [1.0],
            }
        ),
        group_columns=("date",),
        score_columns=("score",),
        k=1,
    )
    duplicate_outcomes = pd.DataFrame(
        {
            "date": ["2026-01-05", "2026-01-05"],
            "code": ["0001", "0001"],
            "outcome": [1.0, 2.0],
        }
    )

    with pytest.raises(ValueError, match="duplicate outcome keys"):
        evaluate_frozen_selection(
            frozen,
            duplicate_outcomes,
            outcome_column="outcome",
        )

    with pytest.raises(ValueError, match="must not be used as a score or group"):
        evaluate_frozen_selection(
            frozen,
            pd.DataFrame(
                {"date": ["2026-01-05"], "code": ["0001"], "score": [1.0]}
            ),
            outcome_column="score",
        )
