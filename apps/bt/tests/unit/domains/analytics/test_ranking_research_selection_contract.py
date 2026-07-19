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

    with pytest.raises(ValueError, match="already carries declared outcome"):
        evaluate_frozen_selection(
            frozen,
            pd.DataFrame(
                {"date": ["2026-01-05"], "code": ["0001"], "score": [1.0]}
            ),
            outcome_column="score",
        )


@pytest.mark.parametrize(
    ("size", "fraction", "min_side", "top_codes", "bottom_codes"),
    [
        (4, 0.5, 1, ("0001", "0002"), ("0003", "0004")),
        (5, 0.4, 1, ("0001", "0002"), ("0004", "0005")),
        (3, 0.2, 1, ("0001",), ("0003",)),
    ],
)
def test_frozen_tails_are_disjoint_for_all_score_ties(
    size: int,
    fraction: float,
    min_side: int,
    top_codes: tuple[str, ...],
    bottom_codes: tuple[str, ...],
) -> None:
    frozen = freeze_signal_tails(
        pd.DataFrame(
            {
                "date": ["2026-01-05"] * size,
                "code": [f"{index:04d}" for index in range(1, size + 1)],
                "score": [1.0] * size,
            }
        ),
        group_columns=("date",),
        score_columns=("score",),
        fraction=fraction,
        min_side=min_side,
    )

    assert tuple(frozen.top["code"]) == top_codes
    assert tuple(frozen.bottom["code"]) == bottom_codes
    assert set(frozen.top["code"]).isdisjoint(frozen.bottom["code"])
    assert len(frozen.selected) == len(top_codes) + len(bottom_codes)


def test_legacy_topk_preserves_score_then_normalized_code_candidate_order() -> None:
    selection = select_frozen_topk(
        pd.DataFrame(
            {
                "code": ["0003", "0001", "0002"],
                "score": [3.0, 2.0, 2.0],
                "outcome": [1.0, 2.0, 3.0],
            }
        ),
        score_columns=("score",),
        outcome_column="outcome",
        k=2,
        ascending=(False,),
    )

    assert tuple(selection.candidates["code"]) == ("0003", "0001", "0002")


@pytest.mark.parametrize("invalid", [np.nan, None, np.inf, -np.inf, "not-a-number"])
def test_evaluation_treats_nonfinite_or_nonnumeric_outcomes_as_missing(
    invalid: object,
) -> None:
    frozen = freeze_signal_topk(
        pd.DataFrame(
            {"date": ["2026-01-05"], "code": ["0001"], "score": [1.0]}
        ),
        group_columns=("date",),
        score_columns=("score",),
        k=1,
    )
    evaluated = evaluate_frozen_selection(
        frozen,
        pd.DataFrame(
            {"date": ["2026-01-05"], "code": ["0001"], "outcome": [invalid]}
        ),
        outcome_column="outcome",
    )

    assert evaluated.outcome_status == "incomplete"
    assert evaluated.selected_outcome_count == 0
    assert evaluated.effect_metrics is None
    assert pd.isna(evaluated.selected.loc[0, "outcome"])


def test_evaluation_rejects_a_declared_outcome_already_carried_by_frozen_frames() -> None:
    frozen = freeze_signal_topk(
        pd.DataFrame(
            {
                "date": ["2026-01-05"],
                "code": ["0001"],
                "score": [1.0],
                "outcome": [999.0],
            }
        ),
        group_columns=("date",),
        score_columns=("score",),
        k=1,
    )

    with pytest.raises(ValueError, match="already carries declared outcome"):
        evaluate_frozen_selection(
            frozen,
            pd.DataFrame(
                {"date": ["2026-01-05"], "code": ["0001"], "outcome": [1.0]}
            ),
            outcome_column="outcome",
        )


@pytest.mark.parametrize("invalid", [np.nan, np.inf, -np.inf, "0.5"])
def test_selection_rejects_nonfinite_or_nonnumeric_signal_fields(invalid: object) -> None:
    topk_frame = pd.DataFrame(
        {"date": ["2026-01-05"], "code": ["0001"], "score": [invalid]}
    )
    tails_frame = pd.DataFrame(
        {
            "date": ["2026-01-05", "2026-01-05"],
            "code": ["0001", "0002"],
            "score": [1.0, invalid],
        }
    )
    percentile_frame = pd.DataFrame(
        {
            "date": ["2026-01-05"],
            "code": ["0001"],
            "percentile": [invalid],
        }
    )

    with pytest.raises(ValueError, match="finite numeric"):
        freeze_signal_topk(
            topk_frame,
            group_columns=("date",),
            score_columns=("score",),
            k=1,
        )
    with pytest.raises(ValueError, match="finite numeric"):
        freeze_signal_tails(
            tails_frame,
            group_columns=("date",),
            score_columns=("score",),
            fraction=0.5,
        )
    with pytest.raises(ValueError, match="finite numeric"):
        freeze_signal_percentile_buckets(
            percentile_frame,
            group_columns=("date",),
            percentile_column="percentile",
        )
