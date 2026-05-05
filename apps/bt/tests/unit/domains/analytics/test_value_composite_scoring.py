from __future__ import annotations

import math

import pandas as pd
import pytest

from src.domains.analytics.value_composite_scoring import (
    FIXED_VALUE_COMPOSITE_SCORE_COLUMN,
    VALUE_COMPOSITE_SCORE_COLUMN,
    build_value_composite_score_frame,
    normalize_required_positive_columns,
)


def test_build_value_composite_score_frame_scores_within_groups() -> None:
    frame = pd.DataFrame(
        [
            {"market": "standard", "code": "1111", "pbr": 0.5, "forward_per": 5.0, "market_cap_bil_jpy": 3.0},
            {"market": "standard", "code": "2222", "pbr": 1.0, "forward_per": 10.0, "market_cap_bil_jpy": 9.0},
            {"market": "prime", "code": "3333", "pbr": 0.8, "forward_per": 8.0, "market_cap_bil_jpy": 30.0},
            {"market": "prime", "code": "4444", "pbr": 1.6, "forward_per": 16.0, "market_cap_bil_jpy": 90.0},
        ]
    )

    scored = build_value_composite_score_frame(frame, group_columns=("market",))

    score_by_code = scored.set_index("code")[FIXED_VALUE_COMPOSITE_SCORE_COLUMN].to_dict()
    assert score_by_code["1111"] == 1.0
    assert score_by_code["2222"] == 0.0
    assert score_by_code["3333"] == 1.0
    assert score_by_code["4444"] == 0.0


def test_build_value_composite_score_frame_filters_required_positive_columns() -> None:
    frame = pd.DataFrame(
        [
            {"code": "1111", "pbr": 0.5, "forward_per": 5.0, "market_cap_bil_jpy": 3.0},
            {"code": "2222", "pbr": 0.0, "forward_per": 4.0, "market_cap_bil_jpy": 1.0},
            {"code": "3333", "pbr": 0.4, "forward_per": -1.0, "market_cap_bil_jpy": 2.0},
        ]
    )

    scored = build_value_composite_score_frame(
        frame,
        group_columns=(),
        required_positive_columns=("pbr", "forward_per", "pbr"),
        score_column=VALUE_COMPOSITE_SCORE_COLUMN,
    )

    assert scored["code"].tolist() == ["1111"]
    assert scored[VALUE_COMPOSITE_SCORE_COLUMN].tolist() == [0.5]


def test_build_value_composite_score_frame_handles_missing_inputs_and_custom_weights() -> None:
    frame = pd.DataFrame(
        [
            {"code": "1111", "pbr": 0.5, "market_cap_bil_jpy": 3.0},
            {"code": "2222", "pbr": 1.0, "market_cap_bil_jpy": 9.0},
        ]
    )

    scored = build_value_composite_score_frame(
        frame,
        group_columns=(),
        weights={"low_pbr_score": 1.0},
    )

    assert scored["forward_per"].isna().all()
    assert scored.set_index("code")[FIXED_VALUE_COMPOSITE_SCORE_COLUMN].to_dict() == {
        "1111": 1.0,
        "2222": 0.0,
    }


def test_build_value_composite_score_frame_rejects_invalid_options() -> None:
    frame = pd.DataFrame([{"code": "1111", "pbr": 0.5, "forward_per": 5.0, "market_cap_bil_jpy": 3.0}])

    with pytest.raises(ValueError, match="Unsupported required positive column"):
        normalize_required_positive_columns(("eps",))

    with pytest.raises(ValueError, match="score group columns must not be empty"):
        build_value_composite_score_frame(frame, group_columns=("",))

    with pytest.raises(ValueError, match="Unsupported value composite score column"):
        build_value_composite_score_frame(frame, weights={"unknown_score": 1.0})

    with pytest.raises(ValueError, match="positive finite"):
        build_value_composite_score_frame(frame, weights={"low_pbr_score": math.nan})
