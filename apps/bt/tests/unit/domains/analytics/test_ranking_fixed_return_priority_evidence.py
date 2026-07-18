from __future__ import annotations

import pandas as pd
import pytest

from src.domains.analytics.ranking_fixed_return_priority_evidence import (
    REQUIRED_BUNDLE_TABLES,
    SCAFFOLD_REGISTRY,
    _add_prime_date_percentiles,
    _build_decision_gate_df,
    classify_fixed_return_quadrant,
    moving_block_bootstrap_ci,
    run_ranking_fixed_return_priority_evidence_research,
    write_ranking_fixed_return_priority_evidence_bundle,
)
from tests.unit.domains.analytics.test_ranking_trend_acceleration_conditional_lift import (
    _build_mixed_market_db,
)


@pytest.mark.parametrize(
    ("return_20d", "return_60d", "expected"),
    [
        (1.0, 2.0, "++"),
        (1.0, -2.0, "+-"),
        (-1.0, 2.0, "-+"),
        (-1.0, -2.0, "--"),
        (0.0, 1.0, "zero"),
        (1.0, 0.0, "zero"),
        (None, 1.0, "missing"),
        (1.0, float("nan"), "missing"),
    ],
)
def test_fixed_return_quadrant_boundaries(
    return_20d: float | None,
    return_60d: float | None,
    expected: str,
) -> None:
    assert classify_fixed_return_quadrant(return_20d, return_60d) == expected


def test_scaffold_predicates_are_fixed_and_future_free_and_mutually_exclusive() -> None:
    forbidden = (
        "return_20",
        "return_60",
        "momentum",
        "neutral_rerating",
        "crowded_rerating",
        "distribution_stress",
        "ex_overheat",
        "sector_strength",
        "forward_",
        "future_",
    )
    assert {item.name for item in SCAFFOLD_REGISTRY} == {
        "strict_value_long_only",
        "value_extension_long_only",
    }
    for scaffold in SCAFFOLD_REGISTRY:
        assert not any(token in scaffold.predicate.lower() for token in forbidden)
    extension = next(
        item for item in SCAFFOLD_REGISTRY if item.name == "value_extension_long_only"
    )
    assert "NOT deep_value_flag" in extension.predicate


def test_priority_percentiles_are_prime_date_wide_before_scaffold_filter() -> None:
    frame = pd.DataFrame(
        {
            "date": ["2024-01-04"] * 5,
            "code": ["1", "2", "3", "4", "5"],
            "recent_return_20d_pct": [1.0, 2.0, 3.0, 4.0, 5.0],
            "recent_return_60d_pct": [5.0, 4.0, 3.0, 2.0, 1.0],
            "is_scaffold_candidate": [False, True, True, False, False],
        }
    )

    ranked = _add_prime_date_percentiles(frame)

    assert ranked.loc[ranked["code"] == "2", "fixed20_priority"].item() == 0.4
    assert ranked.loc[ranked["code"] == "2", "fixed60_priority"].item() == 0.8
    assert ranked.loc[
        ranked["code"] == "2", "fixed_equal_priority"
    ].item() == pytest.approx(0.6)


def test_moving_block_bootstrap_is_reproducible() -> None:
    values = pd.Series([0.5, -0.25, 1.0, 0.75, -0.1]).to_numpy()
    first = moving_block_bootstrap_ci(values, block_length=3, resamples=100, seed=7)
    second = moving_block_bootstrap_ci(values, block_length=3, resamples=100, seed=7)
    assert first == second


def test_decision_gate_requires_both_primary_families() -> None:
    evidence = pd.DataFrame(
        [
            {
                "priority_variant": "fixed20_priority",
                "scaffold_family": "strict_value_long_only",
                "mean_lift_pct": 1.0,
                "ci_lower_pct": 0.2,
                "median_spearman_ic": 0.1,
                "ic_positive_date_rate_pct": 60.0,
                "all_segments_positive": True,
                "severe_loss_rate_difference_pct": 0.0,
                "observation_count": 500,
                "paired_date_count": 100,
                "median_focus_candidates": 10.0,
            }
        ]
    )
    decision = _build_decision_gate_df(evidence, pd.DataFrame(), pd.DataFrame())
    row = decision.loc[decision["decision_key"] == "fixed20_priority"].iloc[0]
    assert not bool(row["passed"])
    assert row["reason"] == "requires_both_primary_families"


def test_required_bundle_table_contract() -> None:
    assert REQUIRED_BUNDLE_TABLES == {
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


def test_runner_uses_exact_date_prime_membership_and_writes_all_tables(
    tmp_path,
) -> None:
    db_path = _build_mixed_market_db(tmp_path / "market.duckdb")

    result = run_ranking_fixed_return_priority_evidence_research(
        db_path,
        start_date="2023-01-03",
        end_date="2024-12-20",
        horizons=(20,),
        bootstrap_resamples=20,
        observation_sample_limit=1_000,
    )

    assert set(result.observation_sample_df["market_code"].astype(str)).issubset(
        {"0101", "0111"}
    )
    assert not set(result.observation_sample_df["market_code"].astype(str)).intersection(
        {"0112", "0113"}
    )
    assert not result.observation_sample_df.duplicated(
        ["date", "code", "scaffold_family"]
    ).any()

    bundle = write_ranking_fixed_return_priority_evidence_bundle(
        result,
        output_root=tmp_path / "research",
        run_id="fixture",
    )
    import duckdb

    conn = duckdb.connect(bundle.results_db_path, read_only=True)
    try:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables"
            ).fetchall()
        }
    finally:
        conn.close()
    assert tables == REQUIRED_BUNDLE_TABLES
