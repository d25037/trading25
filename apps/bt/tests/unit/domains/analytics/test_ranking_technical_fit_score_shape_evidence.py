from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import pytest

from src.domains.analytics.ranking_technical_fit_score_shape_evidence import (
    RAW_SCORE_REGISTRY,
    REQUIRED_BUNDLE_TABLES,
    RING_REGISTRY,
    _build_ols_feature_frame,
    _create_candidate_ring_flags_table,
    _create_prime_technical_rank_table,
    apply_walkforward_mapping,
    build_decision_gate_df,
    build_walkforward_mapping,
    classify_candidate_ring,
    classify_raw_level_bin,
    classify_shape,
    run_ranking_technical_fit_score_shape_evidence_research,
)
from src.domains.analytics.trend_slope_features import rolling_log_slope_features
from tests.unit.domains.analytics.test_ranking_fixed_return_priority_evidence import (
    _mark_fixture_market_v4,
)
from tests.unit.domains.analytics.test_ranking_trend_acceleration_conditional_lift import (
    _build_mixed_market_db,
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


def test_walkforward_mapping_requires_exact_expectancy_equality_for_flatness() -> None:
    mapping = build_walkforward_mapping(
        _complete_training([1.0, 1.0, 1.0, 1.0, 1.0 + 1e-12]), evaluation_year=2022
    )

    assert set(mapping["mapping_status"]) == {"ready"}
    assert mapping["technical_fit_score"].tolist() == pytest.approx(
        [0.0, 0.0, 0.0, 0.0, 1.0]
    )
    assert classify_shape(mapping["expectancy_pct"].tolist()) == "monotonic"


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
                "raw_score_name": "fixed_equal_level",
                "passes_adoption_gate": fixed_passed,
                "sufficient_sample": fixed_sufficient,
            },
            {
                "family": "ols",
                "raw_score_name": "ols_equal_level",
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


@pytest.mark.parametrize(
    ("family_evidence", "paired_evidence"),
    [
        (
            pd.DataFrame(
                [
                    {
                        "family": "fixed",
                        "raw_score_name": "fixed_equal_level",
                        "passes_adoption_gate": True,
                        "sufficient_sample": pd.NA,
                    },
                    {
                        "family": "ols",
                        "raw_score_name": "ols_equal_level",
                        "passes_adoption_gate": True,
                        "sufficient_sample": True,
                    },
                ]
            ),
            pd.DataFrame(
                [{"sufficient_sample": True, "ci_lower_pct": 0.1, "ci_upper_pct": 0.2}]
            ),
        ),
        (
            pd.DataFrame(
                [
                    {
                        "family": "fixed",
                        "raw_score_name": "fixed_equal_level",
                        "passes_adoption_gate": pd.NA,
                        "sufficient_sample": True,
                    },
                    {
                        "family": "ols",
                        "raw_score_name": "ols_equal_level",
                        "passes_adoption_gate": True,
                        "sufficient_sample": True,
                    },
                ]
            ),
            pd.DataFrame(
                [{"sufficient_sample": True, "ci_lower_pct": 0.1, "ci_upper_pct": 0.2}]
            ),
        ),
        (
            pd.DataFrame(
                [
                    {
                        "family": "fixed",
                        "raw_score_name": "fixed_equal_level",
                        "passes_adoption_gate": True,
                        "sufficient_sample": True,
                    },
                    {
                        "family": "ols",
                        "raw_score_name": "ols_equal_level",
                        "passes_adoption_gate": True,
                        "sufficient_sample": True,
                    },
                ]
            ),
            pd.DataFrame(
                [{"sufficient_sample": pd.NA, "ci_lower_pct": 0.1, "ci_upper_pct": 0.2}]
            ),
        ),
    ],
)
def test_decision_gate_fails_closed_for_missing_boolean_evidence(
    family_evidence: pd.DataFrame, paired_evidence: pd.DataFrame
) -> None:
    decision = build_decision_gate_df(family_evidence, paired_evidence)

    final = decision.loc[decision["decision_key"].eq("fixed_vs_ols")].iloc[0]
    assert final["decision"] == "insufficient_evidence"


def test_decision_gate_components_cannot_rescue_missing_primary_score() -> None:
    family_evidence = pd.DataFrame(
        [
            {
                "family": "fixed",
                "raw_score_name": "fixed20_level",
                "passes_adoption_gate": True,
                "sufficient_sample": True,
            },
            {
                "family": "fixed",
                "raw_score_name": "fixed60_level",
                "passes_adoption_gate": True,
                "sufficient_sample": True,
            },
            {
                "family": "ols",
                "raw_score_name": "ols_equal_level",
                "passes_adoption_gate": True,
                "sufficient_sample": True,
            },
        ]
    )
    paired_evidence = pd.DataFrame(
        [{"sufficient_sample": True, "ci_lower_pct": 0.1, "ci_upper_pct": 0.2}]
    )

    decision = build_decision_gate_df(family_evidence, paired_evidence)

    final = decision.loc[decision["decision_key"].eq("fixed_vs_ols")].iloc[0]
    assert final["decision"] == "insufficient_evidence"


def test_decision_gate_components_cannot_overturn_passing_primary_scores() -> None:
    family_evidence = pd.DataFrame(
        [
            {
                "family": "fixed",
                "raw_score_name": "fixed_equal_level",
                "passes_adoption_gate": True,
                "sufficient_sample": True,
            },
            {
                "family": "fixed",
                "raw_score_name": "fixed20_level",
                "passes_adoption_gate": False,
                "sufficient_sample": False,
            },
            {
                "family": "fixed",
                "raw_score_name": "fixed60_level",
                "passes_adoption_gate": False,
                "sufficient_sample": False,
            },
            {
                "family": "ols",
                "raw_score_name": "ols_equal_level",
                "passes_adoption_gate": True,
                "sufficient_sample": True,
            },
            {
                "family": "ols",
                "raw_score_name": "ols20_level",
                "passes_adoption_gate": False,
                "sufficient_sample": False,
            },
            {
                "family": "ols",
                "raw_score_name": "ols60_level",
                "passes_adoption_gate": False,
                "sufficient_sample": False,
            },
        ]
    )
    paired_evidence = pd.DataFrame(
        [{"sufficient_sample": True, "ci_lower_pct": 0.1, "ci_upper_pct": 0.2}]
    )

    decision = build_decision_gate_df(family_evidence, paired_evidence)

    final = decision.loc[decision["decision_key"].eq("fixed_vs_ols")].iloc[0]
    assert final["decision"] == "fixed_wins"


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


def test_candidate_ring_flags_are_materialized_as_keys_and_exclusive_flags() -> None:
    conn = duckdb.connect(":memory:")
    try:
        conn.execute(
            """
            CREATE TEMP TABLE ranking_long_scaffold_value_composite_panel AS
            SELECT * FROM (VALUES
                ('prime', '0101', DATE '2021-01-04', '1001', 0.8, 0.8, 99.0, 99.0),
                ('prime', '0111', DATE '2024-01-04', '1002', 0.7, 0.7, 99.0, 99.0),
                ('prime', '0111', DATE '2024-01-04', '1003', 0.6, 0.6, 99.0, 99.0),
                ('prime', '0111', DATE '2024-01-04', '1004', 0.5, 0.9, 99.0, 99.0),
                ('standard', '0112', DATE '2024-01-04', '2001', 1.0, 1.0, 99.0, 99.0),
                ('growth', '0113', DATE '2024-01-04', '3001', 1.0, 1.0, 99.0, 99.0)
            ) AS source(
                market_scope,
                market_code,
                date,
                code,
                value_composite_equal_score,
                long_hybrid_leadership_score,
                recent_return_20d_pct,
                forward_close_excess_return_20d_pct
            )
            """
        )

        _create_candidate_ring_flags_table(conn)

        columns = [
            row[0]
            for row in conn.execute(
                "DESCRIBE ranking_technical_fit_candidate_ring_flags"
            ).fetchall()
        ]
        flags = conn.execute(
            "SELECT * FROM ranking_technical_fit_candidate_ring_flags ORDER BY code"
        ).fetchdf()
    finally:
        conn.close()

    assert columns == [
        "market_scope",
        "market_code",
        "date",
        "code",
        "core_high_high_flag",
        "near_high_high_1_flag",
        "near_high_high_2_flag",
    ]
    assert flags["code"].tolist() == ["1001", "1002", "1003"]
    assert flags[
        [
            "core_high_high_flag",
            "near_high_high_1_flag",
            "near_high_high_2_flag",
        ]
    ].sum(axis=1).eq(1).all()


def test_fixed_and_ols_levels_are_ranked_prime_date_wide_before_ring_filter() -> None:
    conn = duckdb.connect(":memory:")
    try:
        conn.execute(
            """
            CREATE TEMP TABLE daily_ranking_research_ranked AS
            SELECT * FROM (VALUES
                ('prime', '0111', DATE '2024-01-04', '1', 1.0, 5.0),
                ('prime', '0111', DATE '2024-01-04', '2', 2.0, 4.0),
                ('prime', '0111', DATE '2024-01-04', '3', 3.0, 3.0),
                ('prime', '0111', DATE '2024-01-04', '4', 4.0, 2.0),
                ('prime', '0111', DATE '2024-01-04', '5', 5.0, 1.0),
                ('standard', '0112', DATE '2024-01-04', '9', 99.0, 99.0)
            ) AS source(
                market_scope,
                market_code,
                date,
                code,
                recent_return_20d_pct,
                recent_return_60d_pct
            )
            """
        )
        conn.execute(
            """
            CREATE TEMP TABLE ranking_technical_fit_ols_features AS
            SELECT * FROM (VALUES
                ('1', DATE '2024-01-04', 5.0, 1.0, 0.91, 0.81),
                ('2', DATE '2024-01-04', 4.0, 2.0, 0.92, 0.82),
                ('3', DATE '2024-01-04', 3.0, 3.0, 0.93, 0.83),
                ('4', DATE '2024-01-04', 2.0, 4.0, 0.94, 0.84),
                ('5', DATE '2024-01-04', 1.0, 5.0, 0.95, 0.85),
                ('9', DATE '2024-01-04', 99.0, 99.0, 0.99, 0.99)
            ) AS source(
                code,
                date,
                ols_move_20d_pct,
                ols_move_60d_pct,
                ols_r2_20,
                ols_r2_60
            )
            """
        )

        _create_prime_technical_rank_table(conn)

        row = conn.execute(
            """
            SELECT *
            FROM ranking_technical_fit_prime_ranked
            WHERE code = '2'
            """
        ).fetchdf().iloc[0]
    finally:
        conn.close()

    assert row["fixed20_level"] == pytest.approx(0.4)
    assert row["fixed60_level"] == pytest.approx(0.8)
    assert row["fixed_equal_level"] == pytest.approx(0.6)
    assert row["ols20_level"] == pytest.approx(0.8)
    assert row["ols60_level"] == pytest.approx(0.4)
    assert row["ols_equal_level"] == pytest.approx(0.6)


def test_ols_fitted_moves_reuse_shared_rolling_log_slope_features() -> None:
    dates = pd.bdate_range("2024-01-02", periods=60)
    closes = np.exp(np.linspace(np.log(100.0), np.log(140.0), len(dates)))
    prices = pd.DataFrame({"code": "1001", "date": dates, "close": closes})

    features = _build_ols_feature_frame(prices)
    expected_20, expected_r2_20 = rolling_log_slope_features(
        np.log(closes), window=20
    )
    expected_60, expected_r2_60 = rolling_log_slope_features(
        np.log(closes), window=60
    )

    final = features.iloc[-1]
    assert final["ols_move_20d_pct"] == pytest.approx(expected_20[-1])
    assert final["ols_move_60d_pct"] == pytest.approx(expected_60[-1])
    assert final["ols_r2_20"] == pytest.approx(expected_r2_20[-1])
    assert final["ols_r2_60"] == pytest.approx(expected_r2_60[-1])


def test_runner_builds_unique_prime_candidates_with_raw_levels_and_outcomes(
    tmp_path: Path,
) -> None:
    db_path = _build_mixed_market_db(tmp_path / "market.duckdb")
    _mark_fixture_market_v4(db_path)

    result = _run_fixture_research(db_path)
    sample = result.observation_sample_df

    assert not sample.empty
    assert set(sample["market_code"].astype(str)).issubset({"0101", "0111"})
    assert not set(sample["market_code"].astype(str)).intersection({"0112", "0113"})
    assert not sample.duplicated(["date", "code"]).any()
    assert set(sample["ring"]).issubset(
        {"core_high_high", "near_high_high_1", "near_high_high_2"}
    )
    assert {
        "fixed20_level",
        "fixed60_level",
        "fixed_equal_level",
        "ols20_level",
        "ols60_level",
        "ols_equal_level",
        "ols_r2_20",
        "ols_r2_60",
        "ols20_minus_ols60_move_pct",
        "fixed20_ols20_sign_conflict",
        "fixed60_ols60_sign_conflict",
        "fixed20_negative_flag",
        "fixed60_negative_flag",
        "fixed20_overheat_flag",
        "forward_close_excess_return_5d_pct",
        "forward_close_excess_return_20d_pct",
        "forward_close_excess_return_60d_pct",
    }.issubset(sample.columns)


@pytest.mark.parametrize(
    ("schema_version", "adjustment_mode", "message"),
    [
        (3, "local_projection_v2_event_time", "required schema version 4"),
        (4, "legacy_adjusted", "stock_price_adjustment_mode"),
    ],
)
def test_runner_rejects_incompatible_market_metadata(
    tmp_path: Path,
    schema_version: int,
    adjustment_mode: str,
    message: str,
) -> None:
    db_path = _build_mixed_market_db(tmp_path / "market.duckdb")
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute("CREATE TABLE market_schema_version(version INTEGER)")
        conn.execute("INSERT INTO market_schema_version VALUES (?)", [schema_version])
        conn.execute("CREATE TABLE sync_metadata(key VARCHAR, value VARCHAR)")
        conn.execute(
            "INSERT INTO sync_metadata VALUES ('stock_price_adjustment_mode', ?)",
            [adjustment_mode],
        )
    finally:
        conn.close()

    with pytest.raises(RuntimeError, match=message):
        _run_fixture_research(db_path)


def test_appending_future_rows_does_not_change_earlier_rings_or_raw_levels(
    tmp_path: Path,
) -> None:
    db_path = _build_mixed_market_db(tmp_path / "market.duckdb")
    _mark_fixture_market_v4(db_path)
    before = _run_fixture_research(db_path).observation_sample_df
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            "INSERT INTO stock_data VALUES "
            "('1111', '2025-01-06', 999, 1000, 998, 999, 10000)"
        )
        conn.execute(
            """
            INSERT INTO stock_master_daily (
                date,
                code,
                company_name,
                market_code,
                market_name,
                scale_category,
                sector_33_code,
                sector_33_name
            ) VALUES (
                '2025-01-06', '1111', 'Alpha', '0113', 'Growth', NULL,
                '3600', 'Machinery'
            )
            """
        )
    finally:
        conn.close()

    after = _run_fixture_research(db_path).observation_sample_df
    stable_columns = [
        "date",
        "code",
        "market_code",
        "ring",
        "value_composite_equal_score",
        "long_hybrid_leadership_score",
        "recent_return_20d_pct",
        "recent_return_60d_pct",
        "fixed20_level",
        "fixed60_level",
        "fixed_equal_level",
        "ols_move_20d_pct",
        "ols_move_60d_pct",
        "ols20_level",
        "ols60_level",
        "ols_equal_level",
    ]
    pd.testing.assert_frame_equal(
        before[stable_columns].reset_index(drop=True),
        after[stable_columns].reset_index(drop=True),
    )


def _run_fixture_research(db_path: Path):
    return run_ranking_technical_fit_score_shape_evidence_research(
        db_path,
        start_date="2024-06-19",
        end_date="2024-06-21",
        horizons=(5, 20, 60),
        observation_sample_limit=20_000,
    )
