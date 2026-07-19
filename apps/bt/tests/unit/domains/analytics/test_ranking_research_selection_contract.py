from __future__ import annotations

import numpy as np
import pandas as pd

from src.domains.analytics.ranking_research_selection_contract import (
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
