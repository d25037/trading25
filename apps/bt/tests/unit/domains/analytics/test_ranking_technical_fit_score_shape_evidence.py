from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import pytest

from src.domains.analytics.ranking_technical_fit_score_shape_evidence import (
    RAW_SCORE_REGISTRY,
    REQUIRED_BUNDLE_TABLES,
    RING_REGISTRY,
    apply_walkforward_mapping,
    build_decision_gate_df,
    build_walkforward_mapping,
    classify_candidate_ring,
    classify_raw_level_bin,
    classify_shape,
)


@pytest.mark.parametrize(
    ("value", "leadership", "expected"),
    [
        (0.8, 0.8, "core_high_high"),
        (0.7, 0.8, "near_high_high_1"),
        (0.7, 0.7, "near_high_high_1"),
        (0.6, 0.7, "near_high_high_2"),
        (0.59, 0.9, "outside"),
        (None, 0.9, "missing"),
    ],
)
def test_ring_boundaries(
    value: float | None, leadership: float | None, expected: str
) -> None:
    assert classify_candidate_ring(value, leadership) == expected


@pytest.mark.parametrize(
    ("level", "expected"),
    [
        (0.0, "q1"),
        (0.2, "q2"),
        (0.4, "q3"),
        (0.6, "q4"),
        (0.8, "q5"),
        (1.0, "q5"),
        (None, "missing"),
    ],
)
def test_raw_bin_boundaries(level: float | None, expected: str) -> None:
    assert classify_raw_level_bin(level) == expected


def test_ring_registry_is_fixed_free_and_raw_registry_is_frozen() -> None:
    forbidden_tokens = (
        "fixed",
        "ols",
        "atr",
        "liquidity",
        "sector",
        "outcome",
        "forward",
        "future",
        "overheat",
    )

    assert [item.name for item in RING_REGISTRY] == [
        "core_high_high",
        "near_high_high_1",
        "near_high_high_2",
    ]
    for ring in RING_REGISTRY:
        assert not any(token in ring.predicate.lower() for token in forbidden_tokens)
    assert {item.name for item in RAW_SCORE_REGISTRY} == {
        "fixed20_level",
        "fixed60_level",
        "fixed_equal_level",
        "ols20_level",
        "ols60_level",
        "ols_equal_level",
    }


def _complete_training(
    outcomes: list[float], *, year: int = 2021, rows_per_date: int = 4
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    centers = (0.1, 0.3, 0.5, 0.7, 0.9)
    start = date(year, 1, 1)
    for index, (center, outcome) in enumerate(zip(centers, outcomes, strict=True)):
        for day in range(50):
            for row in range(rows_per_date):
                rows.append(
                    {
                        "date": start + timedelta(days=day),
                        "raw_level": center,
                        "forward_topix_excess_20d_pct": outcome,
                        "code": f"{index}-{day}-{row}",
                    }
                )
    return pd.DataFrame(rows)


def test_walkforward_mapping_is_flat_at_neutral_half() -> None:
    mapping = build_walkforward_mapping(
        _complete_training([1.0] * 5), evaluation_year=2022
    )

    assert set(mapping["mapping_status"]) == {"flat"}
    assert mapping["technical_fit_score"].tolist() == [0.5] * 5
    assert classify_shape(mapping["expectancy_pct"].tolist()) == "flat"


def test_walkforward_mapping_uses_only_completed_prior_year_rows() -> None:
    training = _complete_training([0.0, 1.0, 2.0, 3.0, 4.0])
    future = _complete_training([100.0, 100.0, 100.0, 100.0, 100.0], year=2022)
    mapping = build_walkforward_mapping(
        pd.concat([training, future], ignore_index=True), evaluation_year=2022
    )

    assert mapping["training_end_date"].max() < pd.Timestamp("2022-01-01")
    assert mapping.loc[mapping["raw_bin"].eq("q1"), "expectancy_pct"].item() == 0.0
    assert mapping.loc[mapping["raw_bin"].eq("q5"), "expectancy_pct"].item() == 4.0


def test_walkforward_mapping_rejects_a_bin_without_200_rows_and_50_dates() -> None:
    training = _complete_training([0.0, 1.0, 2.0, 3.0, 4.0]).iloc[:-1]
    mapping = build_walkforward_mapping(training, evaluation_year=2022)

    assert set(mapping["mapping_status"]) == {"insufficient_training_data"}
    assert mapping["technical_fit_score"].isna().all()


def test_walkforward_mapping_counts_distinct_calendar_signal_dates() -> None:
    training = _complete_training([0.0, 1.0, 2.0, 3.0, 4.0])
    q5_rows = training.index[training["raw_level"].eq(0.9)]
    training.loc[q5_rows, "date"] = [
        pd.Timestamp("2021-01-01")
        + pd.Timedelta(days=index % 49, minutes=index)
        for index in range(len(q5_rows))
    ]

    mapping = build_walkforward_mapping(training, evaluation_year=2022)

    assert set(mapping["mapping_status"]) == {"insufficient_training_data"}
    assert mapping.loc[mapping["raw_bin"].eq("q5"), "signal_date_count"].item() == 49


def test_walkforward_mapping_interpolates_between_fixed_bin_centers() -> None:
    mapping = build_walkforward_mapping(
        _complete_training([0.0, 1.0, 2.0, 3.0, 4.0]), evaluation_year=2022
    )
    scored = apply_walkforward_mapping(
        pd.DataFrame(
            {
                "date": ["2022-03-01", "2022-03-01", "2022-03-01"],
                "raw_level": [0.0, 0.4, 1.0],
            }
        ),
        mapping,
    )

    assert scored["technical_fit_score"].tolist() == pytest.approx([0.0, 0.375, 1.0])
    assert set(scored["mapping_status"]) == {"ready"}


@pytest.mark.parametrize(
    ("ci_lower", "ci_upper", "fixed_passed", "ols_passed", "fixed_sufficient", "ols_sufficient", "expected"),
    [
        (0.01, 0.2, True, True, True, True, "fixed_wins"),
        (-0.2, -0.01, True, True, True, True, "ols_wins"),
        (-0.01, 0.01, True, True, True, True, "equivalent_fixed_preferred_operationally"),
        (-0.01, 0.01, False, False, True, True, "neither"),
        (0.01, 0.2, True, True, True, False, "insufficient_evidence"),
    ],
)
def test_decision_gate_applies_frozen_family_and_insufficiency_precedence(
    ci_lower: float,
    ci_upper: float,
    fixed_passed: bool,
    ols_passed: bool,
    fixed_sufficient: bool,
    ols_sufficient: bool,
    expected: str,
) -> None:
    family_evidence = pd.DataFrame(
        [
            {
                "family": "fixed",
                "passes_adoption_gate": fixed_passed,
                "sufficient_sample": fixed_sufficient,
            },
            {
                "family": "ols",
                "passes_adoption_gate": ols_passed,
                "sufficient_sample": ols_sufficient,
            },
        ]
    )
    paired_evidence = pd.DataFrame(
        [
            {
                "sufficient_sample": True,
                "ci_lower_pct": ci_lower,
                "ci_upper_pct": ci_upper,
            }
        ]
    )

    decision = build_decision_gate_df(family_evidence, paired_evidence)

    final = decision.loc[decision["decision_key"].eq("fixed_vs_ols")].iloc[0]
    assert final["decision"] == expected


def test_required_bundle_table_contract_contains_exactly_the_fifteen_published_tables() -> None:
    assert REQUIRED_BUNDLE_TABLES == {
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
