from __future__ import annotations

import json
import os
import re
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import pytest

import src.domains.analytics.ranking_technical_fit_score_shape_evidence as technical_fit

from src.domains.analytics.ranking_technical_fit_score_shape_evidence import (
    RAW_SCORE_REGISTRY,
    REQUIRED_BUNDLE_TABLES,
    RING_REGISTRY,
    _build_ols_feature_frame,
    _create_candidate_ring_flags_table,
    _create_candidate_observation_table,
    _create_n225_forward_return_table,
    _create_prime_technical_rank_table,
    apply_walkforward_mapping,
    build_decision_gate_df,
    build_summary_markdown,
    build_technical_fit_evidence_tables,
    build_walkforward_mapping,
    classify_candidate_ring,
    classify_raw_level_bin,
    classify_shape,
    run_ranking_technical_fit_score_shape_evidence_research,
    write_ranking_technical_fit_score_shape_evidence_bundle,
)
from src.domains.analytics.atr_expansion_forward_response import (
    _create_observation_panel as _create_atr_observation_panel,
)
from src.domains.analytics.trend_slope_features import rolling_log_slope_features
from tests.unit.domains.analytics.test_ranking_fixed_return_priority_evidence import (
    _mark_fixture_market_v4 as _mark_base_fixture_market_v4,
)
from tests.unit.domains.analytics.test_ranking_trend_acceleration_conditional_lift import (
    _build_mixed_market_db,
)


def _create_fixture_basis_catalog_tables(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE OR REPLACE TABLE stock_adjustment_bases (
            code TEXT,
            basis_id TEXT,
            valid_from TEXT,
            valid_to_exclusive TEXT,
            adjustment_through_date TEXT,
            source_fingerprint TEXT,
            materialized_through_date TEXT,
            status TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE TABLE stock_adjustment_basis_segments (
            code TEXT,
            basis_id TEXT,
            source_date_from TEXT,
            source_date_to_exclusive TEXT,
            cumulative_factor DOUBLE
        )
        """
    )


def _mark_fixture_market_v4(db_path: Path) -> None:
    _mark_base_fixture_market_v4(db_path)
    conn = duckdb.connect(str(db_path))
    try:
        _extend_technical_fit_leadership_history(conn)
        _create_fixture_basis_catalog_tables(conn)
        codes = [
            str(row[0])
            for row in conn.execute(
                "SELECT DISTINCT code FROM daily_valuation ORDER BY code"
            ).fetchall()
        ]
        first_date, last_date = conn.execute(
            "SELECT min(date), max(date) FROM daily_valuation"
        ).fetchone()
        basis_rows = []
        segment_rows = []
        for code in codes:
            basis_id = f"event-pit-v1:{code}:{first_date}"
            basis_rows.append(
                (
                    code,
                    basis_id,
                    first_date,
                    None,
                    first_date,
                    f"fixture-{code}",
                    last_date,
                    "ready",
                )
            )
            segment_rows.append((code, basis_id, first_date, None, 1.0))
            conn.execute(
                "UPDATE daily_valuation SET basis_version = ? WHERE code = ?",
                [basis_id, code],
            )
        conn.executemany(
            "INSERT INTO stock_adjustment_bases VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            basis_rows,
        )
        conn.executemany(
            "INSERT INTO stock_adjustment_basis_segments VALUES (?, ?, ?, ?, ?)",
            segment_rows,
        )
        conn.execute(
            """
            CREATE OR REPLACE TABLE stock_data_raw AS
            SELECT *, 1.0::DOUBLE AS adjustment_factor
            FROM stock_data
            """
        )
    finally:
        conn.close()


def _extend_technical_fit_leadership_history(
    conn: duckdb.DuckDBPyConnection,
) -> None:
    """Add only the rows needed to cross the 504-session completeness boundary."""

    first_date = pd.Timestamp(
        conn.execute("SELECT min(date) FROM stock_data").fetchone()[0]
    )
    existing_sessions = int(
        conn.execute("SELECT count(DISTINCT date) FROM stock_data").fetchone()[0]
    )
    # The integration cutoff is five sessions before the fixture's final date,
    # so retain 505 sessions through that cutoff (504 prior + signal session).
    missing_sessions = max(0, 510 - existing_sessions)
    if missing_sessions == 0:
        return
    dates = (
        pd.bdate_range(
            end=first_date - pd.Timedelta(days=1),
            periods=missing_sessions,
        )
        .strftime("%Y-%m-%d")
        .tolist()
    )
    leadership_codes = ("1111", "2222", "3333", "4444", "5555", "6666")
    placeholders = ", ".join("?" for _ in leadership_codes)
    stock_seed = conn.execute(
        f"SELECT code, open, high, low, close, volume FROM stock_data "
        f"WHERE date = ? AND code IN ({placeholders}) ORDER BY code",
        [first_date.strftime("%Y-%m-%d"), *leadership_codes],
    ).fetchall()
    master_seed = conn.execute(
        f"SELECT code, company_name, market_code, market_name, scale_category, "
        f"sector_33_code, sector_33_name FROM stock_master_daily "
        f"WHERE date = ? AND code IN ({placeholders}) ORDER BY code",
        [first_date.strftime("%Y-%m-%d"), *leadership_codes],
    ).fetchall()
    valuation_seed = conn.execute(
        f"SELECT code, price_basis_date, per, forward_per, pbr, p_op, "
        f"forward_p_op, market_cap, free_float_market_cap, basis_version "
        f"FROM daily_valuation WHERE date = ? AND code IN ({placeholders}) "
        "ORDER BY code",
        [first_date.strftime("%Y-%m-%d"), *leadership_codes],
    ).fetchall()
    topix_seed = conn.execute(
        "SELECT open, high, low, close FROM topix_data WHERE date = ?",
        [first_date.strftime("%Y-%m-%d")],
    ).fetchone()
    index_seed = conn.execute(
        "SELECT code, open, high, low, close, sector_name FROM indices_data "
        "WHERE date = ? ORDER BY code",
        [first_date.strftime("%Y-%m-%d")],
    ).fetchall()
    conn.executemany(
        "INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            (code, day, open_, high, low, close, volume)
            for day in dates
            for code, open_, high, low, close, volume in stock_seed
        ],
    )
    conn.executemany(
        "INSERT INTO stock_master_daily VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [(day, *row) for day in dates for row in master_seed],
    )
    conn.executemany(
        "INSERT INTO daily_valuation VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [(code, day, *rest) for day in dates for code, *rest in valuation_seed],
    )
    conn.executemany(
        "INSERT INTO topix_data VALUES (?, ?, ?, ?, ?)",
        [(day, *topix_seed) for day in dates],
    )
    conn.executemany(
        "INSERT INTO indices_data VALUES (?, ?, ?, ?, ?, ?, ?)",
        [(code, day, *rest) for day in dates for code, *rest in index_seed],
    )


def test_technical_fit_fixture_crosses_504_prior_session_boundary(
    tmp_path: Path,
) -> None:
    db_path = _build_mixed_market_db(tmp_path / "market.duckdb")
    _mark_fixture_market_v4(db_path)
    conn = duckdb.connect(str(db_path))
    try:
        boundary = conn.execute(
            """
            WITH ordered AS (
                SELECT row_number() OVER (ORDER BY date) AS session_number,
                       lag(close, 504) OVER (ORDER BY date) AS close_lag_504d
                FROM stock_data
                WHERE code = '1111'
            )
            SELECT session_number, close_lag_504d
            FROM ordered
            WHERE session_number IN (504, 505)
            ORDER BY session_number
            """
        ).fetchall()
    finally:
        conn.close()

    assert boundary[0] == (504, None)
    assert boundary[1][0] == 505
    assert boundary[1][1] is not None


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
                        "forward_outcome_completion_date_20d": start
                        + timedelta(days=day + 28),
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


@pytest.mark.parametrize(
    "spread",
    [
        pytest.param(0.009, id="below_tolerance"),
        pytest.param(0.01, id="at_tolerance"),
    ],
)
def test_walkforward_mapping_treats_expectancy_spread_at_or_below_tolerance_as_flat(
    spread: float,
) -> None:
    mapping = build_walkforward_mapping(
        _complete_training([0.0, 0.0, 0.0, 0.0, spread]), evaluation_year=2022
    )

    assert set(mapping["mapping_status"]) == {"flat"}
    assert mapping["technical_fit_score"].tolist() == [0.5] * 5
    assert classify_shape(mapping["expectancy_pct"].tolist()) == "flat"


def test_walkforward_mapping_treats_nonzero_offset_exact_tolerance_as_flat() -> None:
    mapping = build_walkforward_mapping(
        _complete_training([0.13, 0.13, 0.13, 0.13, 0.14]),
        evaluation_year=2022,
    )

    assert set(mapping["mapping_status"]) == {"flat"}
    assert mapping["technical_fit_score"].tolist() == [0.5] * 5
    assert classify_shape(mapping["expectancy_pct"].tolist()) == "flat"


def test_walkforward_mapping_min_max_maps_expectancy_spread_above_tolerance() -> None:
    mapping = build_walkforward_mapping(
        _complete_training([0.0, 0.0, 0.0, 0.0, 0.011]), evaluation_year=2022
    )

    assert set(mapping["mapping_status"]) == {"ready"}
    assert mapping["technical_fit_score"].tolist() == pytest.approx(
        [0.0, 0.0, 0.0, 0.0, 1.0]
    )
    assert classify_shape(mapping["expectancy_pct"].tolist()) == "monotonic"


def test_classify_shape_rejects_negative_flat_tolerance_override() -> None:
    with pytest.raises(ValueError, match="flat_tolerance_pct must be non-negative"):
        classify_shape([0.0] * 5, flat_tolerance_pct=-0.01)


def test_walkforward_mapping_uses_only_completed_prior_year_rows() -> None:
    training = _complete_training([0.0, 1.0, 2.0, 3.0, 4.0])
    future = _complete_training([100.0, 100.0, 100.0, 100.0, 100.0], year=2022)
    mapping = build_walkforward_mapping(
        pd.concat([training, future], ignore_index=True), evaluation_year=2022
    )

    assert mapping["training_end_date"].max() < pd.Timestamp("2022-01-01")
    assert mapping.loc[mapping["raw_bin"].eq("q1"), "expectancy_pct"].item() == 0.0
    assert mapping.loc[mapping["raw_bin"].eq("q5"), "expectancy_pct"].item() == 4.0


def test_walkforward_mapping_excludes_prior_signal_completed_in_evaluation_year() -> (
    None
):
    training = _complete_training([0.0, 1.0, 2.0, 3.0, 4.0])
    crossing = training.loc[training["raw_level"].eq(0.9)].iloc[[0]].copy()
    crossing["date"] = pd.Timestamp("2021-12-31")
    crossing["forward_outcome_completion_date_20d"] = pd.Timestamp("2022-02-01")
    crossing["forward_topix_excess_20d_pct"] = 100.0

    mapping = build_walkforward_mapping(
        pd.concat([training, crossing], ignore_index=True), evaluation_year=2022
    )

    q5 = mapping.loc[mapping["raw_bin"].eq("q5")].iloc[0]
    assert q5["expectancy_pct"] == 4.0
    assert q5["observation_count"] == 200
    assert q5["training_completion_end_date"] < pd.Timestamp("2022-01-01")


def test_walkforward_mapping_rejects_a_bin_without_200_rows_and_50_dates() -> None:
    training = _complete_training([0.0, 1.0, 2.0, 3.0, 4.0]).iloc[:-1]
    mapping = build_walkforward_mapping(training, evaluation_year=2022)

    assert set(mapping["mapping_status"]) == {"insufficient_training_data"}
    assert mapping["technical_fit_score"].isna().all()


def test_walkforward_mapping_counts_distinct_calendar_signal_dates() -> None:
    training = _complete_training([0.0, 1.0, 2.0, 3.0, 4.0])
    q5_rows = training.index[training["raw_level"].eq(0.9)]
    training.loc[q5_rows, "date"] = [
        pd.Timestamp("2021-01-01") + pd.Timedelta(days=index % 49, minutes=index)
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
    ("expectancies", "kwargs", "expected"),
    [
        (
            [0.0, 1.0, 3.0, 1.0, 0.0],
            {
                "reproduces_core_and_near": True,
                "positive_2022_2023": True,
                "positive_2024_plus": True,
                "severe_loss_not_worse": True,
            },
            "interior_sweet_spot_confirmed",
        ),
        ([0.0, 1.0, 2.0, 3.0, 4.0], {}, "monotonic"),
        ([1.0] * 5, {}, "flat"),
        ([0.0, 2.0, 1.0, 3.0, 0.5], {}, "unstable_shape"),
        ([0.0, 1.0, None, 1.0, 0.0], {}, "insufficient_evidence"),
    ],
)
def test_shape_classification_covers_frozen_curve_states(
    expectancies: list[float | None], kwargs: dict[str, bool], expected: str
) -> None:
    assert classify_shape(expectancies, **kwargs) == expected


def _synthetic_walkforward_observations() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    rings = ("core_high_high", "near_high_high_1", "near_high_high_2")
    raw_centers = (0.1, 0.3, 0.5, 0.7, 0.9)
    for year in range(2017, 2022):
        for day_index in range(2):
            signal_date = pd.Timestamp(year=year, month=6, day=1 + day_index)
            for ring_index, ring in enumerate(rings):
                for bin_index, raw_level in enumerate(raw_centers):
                    rows.append(
                        {
                            "date": signal_date,
                            "code": f"T-{year}-{day_index}-{ring_index}-{bin_index}",
                            "ring": ring,
                            "sector_33_code": f"{bin_index:04d}",
                            "value_composite_equal_score": 0.9,
                            "long_hybrid_leadership_score": 0.9,
                            "liquidity_residual_z": 0.0,
                            "atr20_pct": 2.0,
                            "recent_return_20d_pct": raw_level * 10.0,
                            "recent_return_60d_pct": raw_level * 8.0,
                            "fixed_equal_level": raw_level,
                            "ols_equal_level": raw_level,
                            "fixed20_level": raw_level,
                            "fixed60_level": raw_level,
                            "ols20_level": raw_level,
                            "ols60_level": raw_level,
                            "ols_r2_20": 0.8,
                            "ols_r2_60": 0.7,
                            "ols20_minus_ols60_move_pct": 1.0,
                            "fixed20_ols20_sign_conflict": False,
                            "fixed60_ols60_sign_conflict": False,
                            "fixed20_negative_flag": False,
                            "fixed60_negative_flag": False,
                            "fixed20_overheat_flag": False,
                            "forward_close_excess_return_20d_pct": float(bin_index),
                            "forward_close_n225_excess_return_20d_pct": float(
                                bin_index
                            ),
                            "forward_outcome_completion_date_20d": signal_date
                            + pd.offsets.BDay(20),
                        }
                    )
    for day_index, candidate_count in enumerate((10, 9)):
        signal_date = pd.Timestamp(year=2022, month=6, day=1 + day_index)
        for ring_index, ring in enumerate(rings):
            for candidate_index in range(candidate_count):
                raw_level = candidate_index / max(candidate_count - 1, 1)
                rows.append(
                    {
                        "date": signal_date,
                        "code": f"E-{day_index}-{ring_index}-{candidate_index}",
                        "ring": ring,
                        "sector_33_code": f"{candidate_index % 4:04d}",
                        "value_composite_equal_score": 0.9,
                        "long_hybrid_leadership_score": 0.9,
                        "liquidity_residual_z": float(candidate_index % 4 - 1),
                        "atr20_pct": 2.0,
                        "recent_return_20d_pct": raw_level * 40.0 - 5.0,
                        "recent_return_60d_pct": raw_level * 20.0 - 2.0,
                        "fixed_equal_level": raw_level,
                        "ols_equal_level": 1.0 - raw_level,
                        "fixed20_level": raw_level,
                        "fixed60_level": raw_level,
                        "ols20_level": 1.0 - raw_level,
                        "ols60_level": 1.0 - raw_level,
                        "ols_r2_20": 0.5 + raw_level / 2.0,
                        "ols_r2_60": 0.4 + raw_level / 2.0,
                        "ols20_minus_ols60_move_pct": raw_level - 0.5,
                        "fixed20_ols20_sign_conflict": candidate_index % 2 == 0,
                        "fixed60_ols60_sign_conflict": candidate_index % 3 == 0,
                        "fixed20_negative_flag": candidate_index == 0,
                        "fixed60_negative_flag": candidate_index == 0,
                        "fixed20_overheat_flag": candidate_index == candidate_count - 1,
                        "forward_close_excess_return_20d_pct": float(candidate_index),
                        "forward_close_n225_excess_return_20d_pct": float(
                            candidate_index
                        ),
                        "forward_outcome_completion_date_20d": signal_date
                        + pd.offsets.BDay(20),
                    }
                )
    incomplete = dict(rows[-1])
    incomplete["code"] = "INCOMPLETE"
    incomplete["forward_close_excess_return_20d_pct"] = np.nan
    incomplete["forward_close_n225_excess_return_20d_pct"] = np.nan
    rows.append(incomplete)
    return pd.DataFrame(rows)


def test_walkforward_evidence_uses_expanding_prior_only_mapping() -> None:
    observations = _synthetic_walkforward_observations()
    evaluation_year_rows = observations.loc[
        observations["date"].dt.year.eq(2022)
    ].copy()
    evaluation_year_rows["forward_close_excess_return_20d_pct"] = 999.0
    tables = build_technical_fit_evidence_tables(
        pd.concat([observations, evaluation_year_rows], ignore_index=True),
        horizons=(20,),
        min_training_observations=1,
        min_training_dates=1,
    )

    mapping = tables.walkforward_mapping_df
    assert set(mapping["evaluation_year"]) == {2022}
    assert mapping["training_start_date"].min() == pd.Timestamp("2017-06-01")
    assert mapping["training_end_date"].max() < pd.Timestamp("2022-01-01")
    assert mapping["expectancy_pct"].max() == 4.0


def test_oos_daily_comparison_requires_ten_candidates_and_three_per_side() -> None:
    tables = build_technical_fit_evidence_tables(
        _synthetic_walkforward_observations(),
        horizons=(20,),
        min_training_observations=1,
        min_training_dates=1,
    )

    daily = tables.oos_fit_score_lift_df
    assert set(daily["date"]) == {
        pd.Timestamp("2022-06-01"),
        pd.Timestamp("2022-06-02"),
    }
    assert daily["candidate_count"].eq(10).all()
    assert daily["top_count"].eq(3).all()
    assert daily["bottom_count"].eq(3).all()
    incomplete = daily.loc[daily["date"].eq(pd.Timestamp("2022-06-02"))]
    assert incomplete["outcome_status"].eq("incomplete").all()
    assert incomplete["selected_outcome_count"].eq(5).all()
    assert incomplete["mean_lift_pct"].isna().all()


def test_fixed_and_ols_are_paired_on_identical_eligible_dates() -> None:
    tables = build_technical_fit_evidence_tables(
        _synthetic_walkforward_observations(),
        horizons=(20,),
        min_training_observations=1,
        min_training_dates=1,
    )

    paired = tables.fixed_vs_ols_paired_df
    assert not paired.empty
    assert paired["fixed_date"].equals(paired["ols_date"])
    assert paired["fixed_raw_score_name"].eq("fixed_equal_level").all()
    assert paired["ols_raw_score_name"].eq("ols_equal_level").all()


def test_oos_selection_missing_outcome_preserves_membership_and_fails_closed() -> None:
    scored = pd.DataFrame(
        [
            {
                "raw_score_name": "fixed_equal_level",
                "family": "fixed",
                "is_primary": True,
                "role": "primary",
                "ring": "core_high_high",
                "horizon": 20,
                "date": pd.Timestamp("2024-03-01"),
                "code": f"{index:02d}",
                "sector_33_code": f"{index % 2:04d}",
                "technical_fit_score": float(10 - index),
                "outcome_pct": float("nan") if index == 0 else float(index),
                "fixed20_negative_flag": False,
                "fixed20_overheat_flag": False,
            }
            for index in range(10)
        ]
    )

    oos = technical_fit._build_oos_fit_score_lift_df(scored)

    row = oos.iloc[0]
    assert row["candidate_count"] == 10
    assert row["candidate_outcome_count"] == 9
    assert row["top_count"] == 3
    assert row["bottom_count"] == 3
    assert row["selected_outcome_count"] == 5
    assert row["outcome_status"] == "incomplete"
    assert (
        row[
            [
                "top_mean_excess_return_pct",
                "bottom_mean_excess_return_pct",
                "mean_lift_pct",
                "median_lift_pct",
                "spearman_ic",
                "severe_loss_rate_difference_pct",
            ]
        ]
        .isna()
        .all()
    )


def test_topk_operational_lift_missing_outcome_does_not_backfill_ranked_selection() -> (
    None
):
    scored = pd.DataFrame(
        [
            {
                "family": "fixed",
                "raw_score_name": "fixed_equal_level",
                "is_primary": True,
                "horizon": 20,
                "date": pd.Timestamp("2024-03-01"),
                "code": f"{index:02d}",
                "ring": "core_high_high",
                "sector_33_code": "1000",
                "technical_fit_score": float(20 - index),
                "outcome_pct": float("nan") if index == 0 else float(index),
            }
            for index in range(20)
        ]
    )

    topk = technical_fit._build_topk_operational_lift_df(scored)

    row = topk.loc[topk["k"].eq(5)].iloc[0]
    assert row["candidate_count"] == 20
    assert row["candidate_outcome_count"] == 19
    assert row["candidate_outcome_coverage_pct"] == 95.0
    assert row["selected_outcome_count"] == 4
    assert row["selected_outcome_coverage_pct"] == 80.0
    assert row["outcome_status"] == "incomplete_outcomes"
    assert (
        row[
            [
                "eligible_mean_excess_return_pct",
                "selected_mean_excess_return_pct",
                "topk_lift_pct",
                "eligible_severe_loss_rate_pct",
                "selected_severe_loss_rate_pct",
                "severe_loss_rate_difference_pct",
            ]
        ]
        .isna()
        .all()
    )


def test_fixed_seed_2000_resample_bootstrap_is_exactly_reproducible() -> None:
    observations = _synthetic_walkforward_observations()
    first = build_technical_fit_evidence_tables(
        observations,
        horizons=(20,),
        min_training_observations=1,
        min_training_dates=1,
        bootstrap_resamples=2_000,
        bootstrap_seed=20260718,
    ).bootstrap_effect_ci_df
    second = build_technical_fit_evidence_tables(
        observations,
        horizons=(20,),
        min_training_observations=1,
        min_training_dates=1,
        bootstrap_resamples=2_000,
        bootstrap_seed=20260718,
    ).bootstrap_effect_ci_df

    pd.testing.assert_frame_equal(first, second)
    assert first["resamples"].eq(2_000).all()
    assert first["block_length"].eq(20).all()


def test_raw_shape_summary_is_date_equal_not_stock_day_pooled() -> None:
    rows = [
        {
            "date": "2020-01-02",
            "code": "A",
            "ring": "core_high_high",
            "fixed_equal_level": 0.5,
            "forward_close_excess_return_20d_pct": 10.0,
        }
    ]
    rows.extend(
        {
            "date": "2020-01-03",
            "code": f"B{index}",
            "ring": "core_high_high",
            "fixed_equal_level": 0.5,
            "forward_close_excess_return_20d_pct": 0.0,
        }
        for index in range(3)
    )

    tables = build_technical_fit_evidence_tables(
        pd.DataFrame(rows),
        horizons=(20,),
        min_training_observations=1,
        min_training_dates=1,
    )
    summary = tables.raw_shape_summary_df
    row = summary.loc[
        summary["raw_score_name"].eq("fixed_equal_level")
        & summary["raw_bin"].eq("q3")
        & summary["period_type"].eq("all_period")
    ].iloc[0]

    assert row["date_equal_mean_excess_return_pct"] == 5.0
    assert row["date_equal_mean_excess_return_pct"] != 2.5


def test_raw_shape_summary_carries_curve_classification() -> None:
    observations = _complete_training([0.0, 1.0, 2.0, 3.0, 4.0]).rename(
        columns={
            "raw_level": "fixed_equal_level",
            "forward_topix_excess_20d_pct": ("forward_close_excess_return_20d_pct"),
        }
    )
    observations["ring"] = "core_high_high"

    summary = build_technical_fit_evidence_tables(
        observations,
        horizons=(20,),
        min_training_observations=1,
        min_training_dates=1,
    ).raw_shape_summary_df
    selected = summary.loc[
        summary["raw_score_name"].eq("fixed_equal_level")
        & summary["period_type"].eq("all_period")
    ]

    assert set(selected["shape_classification"]) == {"monotonic"}


def test_frozen_sensitivities_are_labelled_and_cannot_be_primary() -> None:
    tables = build_technical_fit_evidence_tables(
        _synthetic_walkforward_observations(),
        horizons=(20,),
        min_training_observations=1,
        min_training_dates=1,
    )
    diagnostics = tables.overheat_negative_diagnostics_df

    assert {
        "sector_equal",
        "bank_exclusion",
        "benchmark",
        "liquidity_z_band",
        "negative_return",
        "overheat",
        "ex_overheat",
        "ols_r2",
        "ols_acceleration",
        "fixed_ols_conflict",
        "date_fixed_effect",
        "ols_spline_shape",
    }.issubset(set(diagnostics["sensitivity_type"]))
    assert diagnostics["role"].eq("sensitivity_only").all()
    assert (
        tables.oos_fit_score_lift_df.loc[
            tables.oos_fit_score_lift_df["raw_score_name"].isin(
                ["fixed20_level", "fixed60_level", "ols20_level", "ols60_level"]
            ),
            "role",
        ]
        .eq("attribution_only")
        .all()
    )


def test_ols_spline_sensitivity_is_a_continuous_cubic_spline_curve() -> None:
    diagnostics = build_technical_fit_evidence_tables(
        _synthetic_walkforward_observations(),
        horizons=(20,),
        min_training_observations=1,
        min_training_dates=1,
    ).overheat_negative_diagnostics_df
    spline = diagnostics.loc[
        diagnostics["sensitivity_type"].eq("ols_spline_shape")
        & diagnostics["diagnostic_status"].eq("ready")
    ]

    assert not spline.empty
    assert spline["spline_degree"].eq(3).all()
    assert spline["spline_raw_level"].between(0.0, 1.0).all()
    assert spline["spline_raw_level"].nunique() >= 21
    assert spline["spline_fitted_outcome_pct"].notna().all()
    assert spline["sensitivity_bucket"].eq("continuous_cubic_bspline").all()


def _mountain_walkforward_observations() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    rings = ("core_high_high", "near_high_high_1", "near_high_high_2")
    centers = (0.1, 0.3, 0.5, 0.7, 0.9)
    mountain = (0.0, 1.0, 4.0, 1.0, -1.0)
    for year in range(2017, 2022):
        for day in range(2):
            signal_date = pd.Timestamp(year=year, month=6, day=1 + day)
            for ring in rings:
                for bin_index, (center, outcome) in enumerate(
                    zip(centers, mountain, strict=True)
                ):
                    rows.append(
                        {
                            "date": signal_date,
                            "forward_outcome_completion_date_20d": signal_date
                            + pd.offsets.BDay(20),
                            "code": f"T-{year}-{day}-{ring}-{bin_index}",
                            "ring": ring,
                            "fixed_equal_level": center,
                            "forward_close_excess_return_20d_pct": outcome,
                        }
                    )
    for year in (2022, 2024):
        signal_date = pd.Timestamp(year=year, month=6, day=3)
        for ring in rings:
            for bin_index, (center, outcome) in enumerate(
                zip(centers, mountain, strict=True)
            ):
                rows.append(
                    {
                        "date": signal_date,
                        "forward_outcome_completion_date_20d": signal_date
                        + pd.offsets.BDay(20),
                        "code": f"E-{year}-{ring}-{bin_index}",
                        "ring": ring,
                        "fixed_equal_level": center,
                        "forward_close_excess_return_20d_pct": outcome,
                    }
                )
    return pd.DataFrame(rows)


def test_builder_confirms_mountain_only_from_frozen_oos_reproduction() -> None:
    tables = build_technical_fit_evidence_tables(
        _mountain_walkforward_observations(),
        horizons=(20,),
        min_training_observations=1,
        min_training_dates=1,
    )
    summary = tables.raw_shape_summary_df
    overall = summary.loc[
        summary["raw_score_name"].eq("fixed_equal_level")
        & summary["ring"].eq("core_high_high")
        & summary["period_type"].eq("all_period")
    ]

    assert set(overall["shape_classification"]) == {"interior_sweet_spot_confirmed"}
    assert not {
        "oos_reproduces_core_and_near",
        "oos_positive_2022_2023",
        "oos_positive_2024_plus",
        "oos_severe_loss_not_worse",
    }.intersection(summary.columns)
    gate = tables.segment_stability_df.loc[
        tables.segment_stability_df["analysis"].eq("raw_shape_pair_gate")
        & tables.segment_stability_df["raw_score_name"].eq("fixed_equal_level")
    ]
    assert set(gate["ring"]) == {"near_high_high_1", "near_high_high_2"}
    assert set(gate["period_label"]) == {
        "walkforward_2022_2023",
        "hypothesis_origin_2024_plus",
    }
    assert gate["positive_date_rate_pct"].eq(100.0).all()


def _shape_pair_gate_inputs(
    *,
    lifts: dict[tuple[str, int], float],
    severe_deterioration: dict[tuple[str, int], float] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    severe_deterioration = severe_deterioration or {}
    rows: list[dict[str, object]] = []
    mapping_rows: list[dict[str, object]] = []
    for year in (2022, 2024):
        signal_date = pd.Timestamp(year=year, month=6, day=1)
        for raw_bin in ("q1", "q2", "q3", "q4", "q5"):
            mapping_rows.append(
                {
                    "raw_score_name": "fixed_equal_level",
                    "evaluation_year": year,
                    "raw_bin": raw_bin,
                    "technical_fit_score": 1.0 if raw_bin == "q3" else 0.0,
                    "mapping_status": "ready",
                }
            )
        for ring in ("core_high_high", "near_high_high_1", "near_high_high_2"):
            lift = lifts[(ring, year)]
            severe = severe_deterioration.get((ring, year), 0.0)
            for raw_bin in ("q2", "q3", "q4", "q5"):
                rows.append(
                    {
                        "raw_score_name": "fixed_equal_level",
                        "ring": ring,
                        "date": signal_date,
                        "horizon": 20,
                        "raw_bin": raw_bin,
                        "mean_excess_return_pct": lift if raw_bin == "q3" else 0.0,
                        "severe_loss_rate_pct": 5.0 + severe
                        if raw_bin == "q3"
                        else 5.0,
                    }
                )
    return pd.DataFrame(rows), pd.DataFrame(mapping_rows)


def test_shape_gate_requires_the_same_near_ring_in_each_oos_period() -> None:
    daily, mapping = _shape_pair_gate_inputs(
        lifts={
            ("core_high_high", 2022): 2.0,
            ("near_high_high_1", 2022): 2.0,
            ("near_high_high_2", 2022): -1.0,
            ("core_high_high", 2024): 2.0,
            ("near_high_high_1", 2024): -1.0,
            ("near_high_high_2", 2024): 2.0,
        }
    )

    gate = technical_fit._build_oos_shape_pair_gate_rows(daily, mapping)

    assert not technical_fit._score_passes_oos_shape_pair_gate(
        gate, "fixed_equal_level"
    )
    assert set(
        gate.loc[
            gate["positive_date_rate_pct"].eq(100.0), ["ring", "period_label"]
        ].itertuples(index=False, name=None)
    ) == {
        ("near_high_high_1", "walkforward_2022_2023"),
        ("near_high_high_2", "hypothesis_origin_2024_plus"),
    }


def test_shape_gate_rejects_period_severe_loss_hidden_by_pooled_average() -> None:
    daily, mapping = _shape_pair_gate_inputs(
        lifts={
            (ring, year): 2.0
            for ring in ("core_high_high", "near_high_high_1", "near_high_high_2")
            for year in (2022, 2024)
        },
        severe_deterioration={
            ("core_high_high", 2022): 2.0,
            ("near_high_high_1", 2022): 2.0,
            ("core_high_high", 2024): -2.0,
            ("near_high_high_1", 2024): -2.0,
        },
    )

    gate = technical_fit._build_oos_shape_pair_gate_rows(daily, mapping)
    near1 = gate.loc[
        gate["ring"].eq("near_high_high_1")
        & gate["raw_score_name"].eq("fixed_equal_level")
    ]

    assert near1["median_effect_pct"].mean() == pytest.approx(0.0)
    assert (
        near1.loc[
            near1["period_label"].eq("walkforward_2022_2023"),
            "positive_date_rate_pct",
        ].item()
        == 0.0
    )
    assert not technical_fit._score_passes_oos_shape_pair_gate(
        gate, "fixed_equal_level"
    )


def test_shape_gate_keeps_explicit_failed_rows_when_no_interior_winner_exists() -> None:
    daily, mapping = _shape_pair_gate_inputs(
        lifts={
            (ring, year): 2.0
            for ring in ("core_high_high", "near_high_high_1", "near_high_high_2")
            for year in (2022, 2024)
        }
    )
    mapping["technical_fit_score"] = mapping["raw_bin"].eq("q5").astype(float)

    gate = technical_fit._build_oos_shape_pair_gate_rows(daily, mapping)
    primary = gate.loc[gate["raw_score_name"].eq("fixed_equal_level")]

    assert len(gate) == len(RAW_SCORE_REGISTRY) * 2 * 2
    assert len(primary) == 4
    assert primary["date_count"].eq(0).all()
    assert primary["mean_effect_pct"].isna().all()
    assert primary["median_effect_pct"].isna().all()
    assert primary["positive_date_rate_pct"].eq(0.0).all()
    assert not technical_fit._score_passes_oos_shape_pair_gate(
        gate, "fixed_equal_level"
    )


@pytest.mark.parametrize(
    (
        "ci_lower",
        "ci_upper",
        "fixed_passed",
        "ols_passed",
        "fixed_sufficient",
        "ols_sufficient",
        "expected",
    ),
    [
        (0.01, 0.2, True, True, True, True, "fixed_wins"),
        (-0.2, -0.01, True, True, True, True, "ols_wins"),
        (
            -0.01,
            0.01,
            True,
            True,
            True,
            True,
            "equivalent_fixed_preferred_operationally",
        ),
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
    if not fixed_sufficient:
        fixed = decision.loc[decision["decision_key"].eq("fixed")].iloc[0]
        assert fixed["decision"] == "insufficient_evidence"
    if not ols_sufficient:
        ols = decision.loc[decision["decision_key"].eq("ols")].iloc[0]
        assert ols["decision"] == "insufficient_evidence"


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


def test_required_bundle_table_contract_contains_exactly_the_fifteen_published_tables() -> (
    None
):
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


def test_pit_invalidation_disposition_closes_the_v11_publication_lineage() -> None:
    assert technical_fit._PIT_INVALIDATION_DISPOSITION == (
        "v1_v2_historical_archive_v3_superseded_by_v4_for_price_basis_gate_ci_"
        "hardening_v4_superseded_by_v5_for_explicit_failed_shape_slices_v5_"
        "superseded_by_v6_for_lineage_disposition_hardening_v6_superseded_by_v7_"
        "for_review_fixed_frontier_and_flat_mapping_v7_superseded_by_v8_for_"
        "lineage_disposition_hardening_v8_superseded_by_v9_for_completion_"
        "aligned_n225_endpoint_repair_v9_superseded_by_v10_for_missing_v8_v9_"
        "lineage_v10_superseded_by_v11_for_missing_v9_v10_lineage"
    )


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

        _create_candidate_ring_flags_table(
            conn,
            source_name="ranking_long_scaffold_value_composite_panel",
        )

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
    assert (
        flags[
            [
                "core_high_high_flag",
                "near_high_high_1_flag",
                "near_high_high_2_flag",
            ]
        ]
        .sum(axis=1)
        .eq(1)
        .all()
    )


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
        conn.execute(
            """
            CREATE TEMP VIEW technical_fit_rank_source AS
            SELECT r.*, f.ols_move_20d_pct, f.ols_move_60d_pct,
                   f.ols_r2_20, f.ols_r2_60
            FROM daily_ranking_research_ranked r
            LEFT JOIN ranking_technical_fit_ols_features f USING (code, date)
            """
        )

        _create_prime_technical_rank_table(
            conn,
            source_name="technical_fit_rank_source",
        )

        row = (
            conn.execute(
                """
            SELECT *
            FROM ranking_technical_fit_prime_ranked
            WHERE code = '2'
            """
            )
            .fetchdf()
            .iloc[0]
        )
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
    expected_20, expected_r2_20 = rolling_log_slope_features(np.log(closes), window=20)
    expected_60, expected_r2_60 = rolling_log_slope_features(np.log(closes), window=60)

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
        "forward_outcome_completion_date_5d",
        "forward_outcome_completion_date_20d",
        "forward_outcome_completion_date_60d",
    }.issubset(sample.columns)
    assert result.pit_lineage.data_plane_schema_version == 4
    assert (
        result.pit_lineage.stock_price_adjustment_mode
        == "local_projection_v2_event_time"
    )
    assert result.pit_lineage.universe_source == "stock_master_daily"
    assert result.pit_lineage.as_of_policy == "exact_signal_date_no_latest_fallback"
    assert result.pit_lineage.basis_dependent_sources == (
        "daily_valuation",
        "stock_data_raw",
    )
    assert result.pit_lineage.verification_status == "verified"
    assert result.pit_lineage.consumed_daily_valuation_row_count > 0
    assert result.pit_lineage.basis_ids
    assert result.pit_lineage.no_service_local_recomputation is True
    assert result.pit_lineage.no_basis_fallback is True
    assert result.pit_lineage.price_projection is not None
    assert result.pit_lineage.price_projection.no_stock_data_fallback is True
    assert result.pit_lineage.price_projection.signal_feature_row_count > 0
    assert result.pit_lineage.price_projection.signal_basis_sha256
    assert result.pit_lineage.price_projection.completion_basis_sha256


def test_runner_technical_features_and_outcomes_ignore_poisoned_stock_data(
    tmp_path: Path,
) -> None:
    db_path = _build_mixed_market_db(tmp_path / "market.duckdb")
    _mark_fixture_market_v4(db_path)
    before = _run_fixture_research(db_path).observation_sample_df
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            UPDATE stock_data
            SET open = 1.0, high = 1.0, low = 1.0, close = 1.0, volume = 1
            """
        )
    finally:
        conn.close()

    after = _run_fixture_research(db_path).observation_sample_df
    stable_columns = [
        "date",
        "code",
        "ring",
        "liquidity_residual_z",
        "atr20_pct",
        "atr20_change_20d_pct",
        "recent_return_20d_pct",
        "recent_return_60d_pct",
        "fixed_equal_level",
        "ols_equal_level",
        "forward_close_return_20d_pct",
        "forward_close_excess_return_20d_pct",
    ]
    pd.testing.assert_frame_equal(
        before[stable_columns].reset_index(drop=True),
        after[stable_columns].reset_index(drop=True),
    )


def _build_consumed_lineage_connection() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    conn.execute(
        """
        CREATE TABLE ranking_color_panel (
            code TEXT,
            date TEXT,
            valuation_basis_id TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE stock_adjustment_bases (
            code TEXT,
            basis_id TEXT,
            valid_from TEXT,
            valid_to_exclusive TEXT,
            adjustment_through_date TEXT,
            source_fingerprint TEXT,
            materialized_through_date TEXT,
            status TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE stock_adjustment_basis_segments (
            code TEXT,
            basis_id TEXT,
            source_date_from TEXT,
            source_date_to_exclusive TEXT,
            cumulative_factor DOUBLE
        )
        """
    )
    basis_id = "event-pit-v1:1111:2024-01-01"
    conn.execute(
        "INSERT INTO ranking_color_panel VALUES ('1111', '2024-06-20', ?)",
        [basis_id],
    )
    conn.execute(
        """
        INSERT INTO stock_adjustment_bases VALUES (
            '1111', ?, '2024-01-01', NULL, '2024-01-01',
            'fixture-fingerprint', '2024-12-31', 'ready'
        )
        """,
        [basis_id],
    )
    conn.execute(
        """
        INSERT INTO stock_adjustment_basis_segments
        VALUES ('1111', ?, '2024-01-01', NULL, 1.0)
        """,
        [basis_id],
    )
    return conn


def test_consumed_pit_lineage_audit_accepts_exact_ready_basis_and_segment() -> None:
    conn = _build_consumed_lineage_connection()
    try:
        audit = technical_fit._audit_consumed_pit_lineage(
            conn, source_name="ranking_color_panel"
        )
    finally:
        conn.close()

    assert audit.basis_ids == ("event-pit-v1:1111:2024-01-01",)
    assert audit.consumed_daily_valuation_row_count == 1
    assert audit.verified_basis_row_count == 1
    assert audit.verified_segment_row_count == 1
    assert len(audit.basis_id_sha256) == 64


def test_consumed_pit_lineage_audit_counts_invalid_overlapping_segment() -> None:
    conn = _build_consumed_lineage_connection()
    try:
        conn.execute(
            """
            INSERT INTO stock_adjustment_basis_segments
            VALUES (
                '1111', 'event-pit-v1:1111:2024-01-01',
                '2024-06-01', '2024-07-01', -1.0
            )
            """
        )
        with pytest.raises(RuntimeError, match="exactly one total covering"):
            technical_fit._audit_consumed_pit_lineage(
                conn, source_name="ranking_color_panel"
            )
    finally:
        conn.close()


def test_consumed_pit_lineage_audit_counts_multiple_invalid_segments() -> None:
    conn = _build_consumed_lineage_connection()
    try:
        conn.execute(
            "UPDATE stock_adjustment_basis_segments SET cumulative_factor = -1.0"
        )
        conn.execute(
            """
            INSERT INTO stock_adjustment_basis_segments
            VALUES (
                '1111', 'event-pit-v1:1111:2024-01-01',
                '2024-06-01', '2024-07-01', -2.0
            )
            """
        )
        with pytest.raises(RuntimeError, match="exactly one total covering"):
            technical_fit._audit_consumed_pit_lineage(
                conn, source_name="ranking_color_panel"
            )
    finally:
        conn.close()


@pytest.mark.parametrize("invalid_factor", [0.0, -1.0, float("inf")])
def test_consumed_pit_lineage_audit_rejects_invalid_single_segment_factor(
    invalid_factor: float,
) -> None:
    conn = _build_consumed_lineage_connection()
    try:
        conn.execute(
            "UPDATE stock_adjustment_basis_segments SET cumulative_factor = ?",
            [invalid_factor],
        )
        with pytest.raises(RuntimeError, match="finite and positive"):
            technical_fit._audit_consumed_pit_lineage(
                conn, source_name="ranking_color_panel"
            )
    finally:
        conn.close()


@pytest.mark.parametrize(
    ("mutation_sql", "message"),
    [
        (
            "UPDATE ranking_color_panel SET valuation_basis_id = NULL",
            "missing cutoff-valid daily_valuation basis",
        ),
        (
            "UPDATE ranking_color_panel SET valuation_basis_id = 'mismatched-basis'",
            "mismatched cutoff-valid basis",
        ),
        (
            "DELETE FROM stock_adjustment_bases",
            "mismatched cutoff-valid basis",
        ),
        (
            "DELETE FROM stock_adjustment_basis_segments",
            "exactly one total covering adjustment basis segment",
        ),
        (
            "UPDATE stock_adjustment_bases SET materialized_through_date = '2024-06-19'",
            "ready and materialized through signal date",
        ),
    ],
)
def test_consumed_pit_lineage_audit_fails_closed_on_missing_or_mismatched_lineage(
    mutation_sql: str,
    message: str,
) -> None:
    conn = _build_consumed_lineage_connection()
    try:
        conn.execute(mutation_sql)
        with pytest.raises(RuntimeError, match=message):
            technical_fit._audit_consumed_pit_lineage(
                conn, source_name="ranking_color_panel"
            )
    finally:
        conn.close()


def test_runner_rejects_market_v4_without_basis_catalog_tables(tmp_path: Path) -> None:
    db_path = _build_mixed_market_db(tmp_path / "market.duckdb")
    _mark_base_fixture_market_v4(db_path)
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute("DROP TABLE IF EXISTS stock_adjustment_basis_segments")
        conn.execute("DROP TABLE IF EXISTS stock_adjustment_bases")
    finally:
        conn.close()

    with pytest.raises(RuntimeError, match="stock_adjustment_bases"):
        _run_fixture_research(db_path)


def test_runner_filters_daily_valuation_to_cutoff_valid_basis_and_fails_closed(
    tmp_path: Path,
) -> None:
    db_path = _build_mixed_market_db(tmp_path / "market.duckdb")
    _mark_fixture_market_v4(db_path)
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            "UPDATE daily_valuation SET basis_version = 'future-or-latest-fallback' "
            "WHERE code = '1111'"
        )
    finally:
        conn.close()

    with pytest.raises(
        RuntimeError, match="missing cutoff-valid daily_valuation basis"
    ):
        _run_fixture_research(db_path)


def test_outcome_completion_date_is_exact_stock_twenty_session_lead(
    tmp_path: Path,
) -> None:
    db_path = _build_mixed_market_db(tmp_path / "market.duckdb")
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            UPDATE stock_master_daily
            SET market_code = '0112'
            WHERE code = '1111' AND CAST(date AS DATE) > DATE '2024-05-20'
            """
        )
        _create_atr_observation_panel(
            conn,
            query_start="2023-07-03",
            query_end="2024-06-28",
            analysis_start_date="2024-05-15",
            analysis_end_date="2024-05-17",
            atr_windows=(20, 60),
            return_windows=(20, 60),
            horizons=(20,),
            market_source="stock_master_daily_exact_date",
            market_scopes=("prime",),
        )
        complete = (
            conn.execute(
                """
            SELECT
                date,
                code,
                future_close_20d,
                forward_close_return_20d_pct,
                forward_outcome_completion_date_20d
            FROM atr_expansion_panel
            WHERE code = '1111' AND date = DATE '2024-05-15'
            """
            )
            .fetchdf()
            .iloc[0]
        )
        session_dates = conn.execute(
            """
            SELECT DISTINCT date
            FROM stock_data
            WHERE left(code, 4) = ?
            ORDER BY date
            """,
            [str(complete["code"])],
        ).fetchdf()["date"]
        signal_index = [str(value)[:10] for value in session_dates].index(
            str(complete["date"])[:10]
        )
        expected_future_close = conn.execute(
            """
            SELECT close
            FROM stock_data
            WHERE code = ? AND date = ?
            """,
            [str(complete["code"]), str(session_dates.iloc[signal_index + 20])[:10]],
        ).fetchone()[0]
        signal_close = conn.execute(
            "SELECT close FROM stock_data WHERE code = ? AND date = ?",
            [str(complete["code"]), str(complete["date"])[:10]],
        ).fetchone()[0]
    finally:
        conn.close()
    assert (
        str(complete["forward_outcome_completion_date_20d"])[:10]
        == str(session_dates.iloc[signal_index + 20])[:10]
    )
    assert complete["future_close_20d"] == expected_future_close
    assert complete["forward_close_return_20d_pct"] == pytest.approx(
        (expected_future_close / signal_close - 1.0) * 100.0
    )


def test_candidate_outcomes_share_full_stock_session_leg_and_independent_n225() -> None:
    conn = duckdb.connect(":memory:")
    try:
        signal_date = pd.Timestamp("2021-12-01")
        n225_dates = pd.bdate_range(signal_date, periods=22)
        conn.execute(
            """
            CREATE TABLE indices_data (
                code TEXT, date DATE, open DOUBLE, high DOUBLE,
                low DOUBLE, close DOUBLE, sector_name TEXT
            )
            """
        )
        conn.executemany(
            "INSERT INTO indices_data VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    "N225_UNDERPX",
                    value.date(),
                    100.0 + index,
                    100.0 + index,
                    100.0 + index,
                    100.0 + index,
                    "synthetic",
                )
                for index, value in enumerate(n225_dates)
            ],
        )
        conn.execute(
            """
            CREATE TEMP TABLE ranking_technical_fit_candidate_ring_flags AS
            SELECT
                'prime' AS market_scope, '0111' AS market_code,
                DATE '2021-12-01' AS date, '1111' AS code,
                TRUE AS core_high_high_flag,
                FALSE AS near_high_high_1_flag,
                FALSE AS near_high_high_2_flag
            """
        )
        conn.execute(
            """
            CREATE TEMP TABLE ranking_long_scaffold_value_composite_panel AS
            SELECT
                'prime' AS market_scope, '0111' AS market_code,
                DATE '2021-12-01' AS date, '1111' AS code,
                'Example' AS company_name, '3600' AS sector_33_code,
                'Machinery' AS sector_33_name,
                0.9 AS value_composite_equal_score,
                0.9 AS long_hybrid_leadership_score,
                0.0 AS liquidity_residual_z, 2.0 AS atr20_pct,
                0.0 AS atr20_change_20d_pct,
                1.0 AS recent_return_20d_pct,
                2.0 AS recent_return_60d_pct,
                DATE '2021-12-30' AS forward_outcome_completion_date_20d,
                10.0 AS forward_close_return_20d_pct,
                7.0 AS forward_close_excess_return_20d_pct,
                -11.0 AS forward_close_n225_excess_return_20d_pct
            """
        )
        conn.execute(
            """
            CREATE TEMP TABLE atr_expansion_panel AS
            SELECT
                DATE '2021-12-01' AS date, '1111' AS code,
                DATE '2021-12-30' AS forward_outcome_completion_date_20d,
                10.0 AS forward_close_return_20d_pct,
                7.0 AS forward_close_excess_return_20d_pct
            """
        )
        conn.execute(
            """
            CREATE TEMP TABLE ranking_technical_fit_prime_ranked AS
            SELECT
                'prime' AS market_scope, '0111' AS market_code,
                DATE '2021-12-01' AS date, '1111' AS code,
                1.0 AS recent_return_20d_pct, 2.0 AS recent_return_60d_pct,
                0.5 AS fixed20_level, 0.5 AS fixed60_level,
                0.5 AS fixed_equal_level,
                1.0 AS ols_move_20d_pct, 2.0 AS ols_move_60d_pct,
                0.5 AS ols20_level, 0.5 AS ols60_level,
                0.5 AS ols_equal_level, 0.8 AS ols_r2_20, 0.8 AS ols_r2_60
            """
        )

        _create_n225_forward_return_table(conn, horizons=(20,))
        _create_candidate_observation_table(
            conn,
            horizons=(20,),
            source_name="ranking_long_scaffold_value_composite_panel",
        )
        row = (
            conn.execute("SELECT * FROM ranking_technical_fit_candidate_observations")
            .fetchdf()
            .iloc[0]
        )
    finally:
        conn.close()

    assert row["forward_outcome_completion_date_20d"] == pd.Timestamp("2021-12-30")
    assert row["forward_close_return_20d_pct"] == 10.0
    assert row["forward_close_excess_return_20d_pct"] == 7.0
    assert row["forward_close_n225_excess_return_20d_pct"] == pytest.approx(-11.0)
    assert row["forward_close_n225_excess_return_20d_pct"] != pytest.approx(-10.0)


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
        conn.execute("CREATE OR REPLACE TABLE market_schema_version(version INTEGER)")
        conn.execute("INSERT INTO market_schema_version VALUES (?)", [schema_version])
        conn.execute(
            "CREATE OR REPLACE TABLE sync_metadata(key VARCHAR, value VARCHAR)"
        )
        conn.execute(
            "INSERT INTO sync_metadata VALUES ('stock_price_adjustment_mode', ?)",
            [adjustment_mode],
        )
        conn.execute(
            """
            CREATE OR REPLACE TABLE stock_data_raw AS
            SELECT *, 1.0::DOUBLE AS adjustment_factor
            FROM stock_data
            """
        )
        _create_fixture_basis_catalog_tables(conn)
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


def test_bundle_writes_exact_typed_table_contract_and_frozen_provenance(
    tmp_path: Path,
) -> None:
    db_path = _build_mixed_market_db(tmp_path / "market.duckdb")
    _mark_fixture_market_v4(db_path)
    result = _run_fixture_research(db_path)

    bundle = write_ranking_technical_fit_score_shape_evidence_bundle(
        result,
        output_root=tmp_path / "bundles",
        run_id="bundle-contract",
    )

    conn = duckdb.connect(str(bundle.results_db_path), read_only=True)
    try:
        table_names = {
            row[0]
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main'"
            ).fetchall()
        }
        assert table_names == REQUIRED_BUNDLE_TABLES
        for table_name in sorted(REQUIRED_BUNDLE_TABLES):
            assert conn.execute(f'DESCRIBE "{table_name}"').fetchall(), table_name
    finally:
        conn.close()

    manifest = json.loads(bundle.manifest_path.read_text(encoding="utf-8"))
    assert set(manifest["output_tables"]) == REQUIRED_BUNDLE_TABLES
    assert manifest["params"]["market_codes"] == ["0101", "0111"]
    assert manifest["params"]["candidate_rings"] == "fixed_return_free"
    assert manifest["params"]["min_training_observations"] == 200
    assert manifest["params"]["min_training_dates"] == 50
    assert manifest["params"]["bootstrap_resamples"] == 2_000
    assert manifest["params"]["bootstrap_seed"] == 20260718
    assert manifest["result_metadata"]["feature_timing"] == "after_close"
    assert (
        manifest["result_metadata"]["walkforward_training_timing"]
        == "completed_outcomes_strictly_before_evaluation_year"
    )
    lineage = manifest["result_metadata"]["pit_lineage"]
    assert lineage["data_plane"] == "physical_market.duckdb_schema_v4"
    assert lineage["stock_price_adjustment_mode"] == "local_projection_v2_event_time"
    assert lineage["universe_source"] == "stock_master_daily"
    assert lineage["as_of_policy"] == "exact_signal_date_no_latest_fallback"
    assert lineage["basis_dependent_sources"] == ["daily_valuation", "stock_data_raw"]
    assert lineage["basis_ids"] == list(result.pit_lineage.basis_ids)
    price_lineage = lineage["price_projection"]
    assert price_lineage["physical_price_source"] == "stock_data_raw"
    assert price_lineage["no_stock_data_fallback"] is True
    assert price_lineage["signal_feature_row_count"] > 0
    assert price_lineage["completion_basis_row_count"] > 0
    assert lineage["basis_id_count"] == len(result.pit_lineage.basis_ids)
    assert lineage["basis_id_sha256"] == result.pit_lineage.basis_id_sha256
    assert lineage["verification_status"] == "verified"
    assert lineage["no_service_local_recomputation"] is True
    assert lineage["no_basis_fallback"] is True
    assert lineage["invalidation_disposition"] == (
        "v1_v2_historical_archive_v3_superseded_by_v4_for_price_basis_gate_ci_"
        "hardening_v4_superseded_by_v5_for_explicit_failed_shape_slices_v5_"
        "superseded_by_v6_for_lineage_disposition_hardening_v6_superseded_by_v7_"
        "for_review_fixed_frontier_and_flat_mapping_v7_superseded_by_v8_for_"
        "lineage_disposition_hardening_v8_superseded_by_v9_for_completion_"
        "aligned_n225_endpoint_repair_v9_superseded_by_v10_for_missing_v8_v9_"
        "lineage_v10_superseded_by_v11_for_missing_v9_v10_lineage"
    )


def test_bundle_contract_rejects_column_drift_for_every_table() -> None:
    tables = {
        table_name: technical_fit._typed_empty_bundle_frame(table_name)
        for table_name in technical_fit.BUNDLE_TABLE_ORDER
    }

    technical_fit._validate_bundle_table_contract(tables, horizons=(5, 20, 60))
    for table_name, frame in tables.items():
        drifted = dict(tables)
        drifted[table_name] = frame.assign(unexpected_contract_column="drift")
        with pytest.raises(RuntimeError, match=table_name):
            technical_fit._validate_bundle_table_contract(
                drifted,
                horizons=(5, 20, 60),
            )

    reordered = dict(tables)
    decision_columns = list(reordered["decision_gate"].columns)
    reordered["decision_gate"] = reordered["decision_gate"][decision_columns[::-1]]
    with pytest.raises(RuntimeError, match="decision_gate"):
        technical_fit._validate_bundle_table_contract(
            reordered,
            horizons=(5, 20, 60),
        )


def test_bundle_writer_rejects_nonempty_frame_column_drift(tmp_path: Path) -> None:
    db_path = _build_mixed_market_db(tmp_path / "market.duckdb")
    _mark_fixture_market_v4(db_path)
    result = _run_fixture_research(db_path)
    drifted_result = replace(
        result,
        decision_gate_df=result.decision_gate_df.assign(unexpected="drift"),
    )

    with pytest.raises(RuntimeError, match="decision_gate"):
        write_ranking_technical_fit_score_shape_evidence_bundle(
            drifted_result,
            output_root=tmp_path / "bundles",
            run_id="must-not-write",
        )

    assert not (tmp_path / "bundles").exists()


def test_empty_observation_schema_tracks_custom_horizons() -> None:
    frame = technical_fit._typed_empty_bundle_frame(
        "observation_sample",
        horizons=(10, 30),
    )

    forward_columns = [
        column for column in frame.columns if column.startswith("forward_")
    ]
    assert forward_columns == [
        "forward_outcome_completion_date_10d",
        "forward_close_return_10d_pct",
        "forward_close_excess_return_10d_pct",
        "forward_close_n225_excess_return_10d_pct",
        "forward_outcome_completion_date_30d",
        "forward_close_return_30d_pct",
        "forward_close_excess_return_30d_pct",
        "forward_close_n225_excess_return_30d_pct",
    ]


def test_empty_custom_horizon_observation_bundle_uses_matching_duckdb_schema(
    tmp_path: Path,
) -> None:
    db_path = _build_mixed_market_db(tmp_path / "market.duckdb")
    _mark_fixture_market_v4(db_path)
    baseline = _run_fixture_research(db_path)
    result = replace(
        baseline,
        horizons=(10, 30),
        observation_sample_df=pd.DataFrame(),
    )

    bundle = write_ranking_technical_fit_score_shape_evidence_bundle(
        result,
        output_root=tmp_path / "bundles",
        run_id="custom-horizons",
    )

    conn = duckdb.connect(str(bundle.results_db_path), read_only=True)
    try:
        columns = [
            row[0] for row in conn.execute('DESCRIBE "observation_sample"').fetchall()
        ]
    finally:
        conn.close()
    assert "forward_outcome_completion_date_10d" in columns
    assert "forward_close_excess_return_30d_pct" in columns
    forward_columns = [column for column in columns if column.startswith("forward_")]
    assert not any(
        "_5d" in column or "_20d" in column or "_60d" in column
        for column in forward_columns
    )


def test_summary_is_japanese_decision_first_and_matches_decision_gate(
    tmp_path: Path,
) -> None:
    db_path = _build_mixed_market_db(tmp_path / "market.duckdb")
    _mark_fixture_market_v4(db_path)
    result = _run_fixture_research(db_path)
    final_decision = result.decision_gate_df.loc[
        result.decision_gate_df["decision_key"].eq("fixed_vs_ols"), "decision"
    ].item()

    summary = build_summary_markdown(result)

    assert summary.startswith(
        "# Ranking Technical Fit Score Shape Evidence\n\n"
        "## 結論\n\n"
        f"- 最終判断: `{final_decision}`"
    )
    assert "シグナル日の終値確定後にのみ利用可能" in summary
    assert "完了済み outcome のみ" in summary
    assert "family=`fixed` / ring=`core_high_high`" in summary
    assert "shape_classification=" in summary
    assert "## PIT Lineage" in summary
    assert "physical `market.duckdb` schema v4" in summary
    assert "stock_adjustment_bases" in summary
    assert "stock_adjustment_basis_segments" in summary
    assert "Physical price source: `stock_data_raw`" in summary
    assert "`stock_data` fallback なし" in summary
    assert result.pit_lineage.basis_id_sha256 in summary
    assert result.pit_lineage.price_projection is not None
    assert result.pit_lineage.price_projection.price_projection_sha256 in summary


def test_canonical_publication_is_decision_first_registered_and_gate_consistent() -> (
    None
):
    bt_root = Path(__file__).resolve().parents[4]
    digest_path = (
        bt_root / "tests/fixtures/research/"
        "ranking_technical_fit_score_shape_evidence_published_digest.json"
    )
    digest = json.loads(digest_path.read_text(encoding="utf-8"))
    experiment_id = str(digest["experiment_id"])
    readme = bt_root / f"docs/experiments/{experiment_id}/README.md"
    index = bt_root / "docs/experiments/README.md"
    catalog = bt_root / "docs/experiments/research-catalog-metadata.toml"

    assert readme.is_file()
    text = readme.read_text(encoding="utf-8")
    assert text.startswith(
        "# Ranking Technical Fit Score Shape Evidence\n\n"
        "## Published Readout\n\n"
        "### Decision\n\n"
        "**最終判断: `"
    )

    headline = re.search(r"\*\*最終判断: `([^`]+)`", text)
    assert headline is not None
    assert headline.group(1) in {
        "fixed_wins",
        "ols_wins",
        "equivalent_fixed_preferred_operationally",
        "neither",
        "insufficient_evidence",
    }

    published_run = re.search(r"- Published run: `([^`]+)`", text)
    assert published_run is not None
    assert published_run.group(1) == digest["published_run_id"]
    assert headline.group(1) == digest["decision"]
    for key, expected in digest["publication_metrics"].items():
        published_value = re.search(rf"\| `{key}` \| `([^`]+)` \|", text)
        assert published_value is not None, key
        assert float(published_value.group(1)) == pytest.approx(expected, abs=0.00005)

    assert "fixed" in text and "OLS" in text
    assert "shape_classification" in text
    assert "Technical Fit Score" in text and "Ranking" in text
    assert "20D<0" in text and "overheat" in text.lower()
    for ring in ("core_high_high", "near_high_high_1", "near_high_high_2"):
        assert ring in text
    assert "0101" in text and "0111" in text
    assert published_run.group(1) in text
    assert experiment_id in index.read_text(encoding="utf-8")
    assert experiment_id in catalog.read_text(encoding="utf-8")


@pytest.mark.integration
def test_live_canonical_publication_matches_committed_digest() -> None:
    research_root = os.environ.get("TRADING25_VERIFY_PUBLISHED_RESEARCH_ROOT")
    if research_root is None:
        pytest.skip(
            "set TRADING25_VERIFY_PUBLISHED_RESEARCH_ROOT for live verification"
        )

    bt_root = Path(__file__).resolve().parents[4]
    digest_path = (
        bt_root / "tests/fixtures/research/"
        "ranking_technical_fit_score_shape_evidence_published_digest.json"
    )
    digest = json.loads(digest_path.read_text(encoding="utf-8"))
    bundle_dir = (
        Path(research_root)
        / str(digest["experiment_id"])
        / str(digest["published_run_id"])
    )
    manifest_path = bundle_dir / "manifest.json"
    results_path = bundle_dir / "results.duckdb"
    assert manifest_path.is_file()
    assert results_path.is_file()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["run_id"] == digest["published_run_id"]

    conn = duckdb.connect(str(results_path), read_only=True)
    try:
        artifact_decision = conn.execute(
            "SELECT decision FROM decision_gate WHERE decision_key = 'fixed_vs_ols'"
        ).fetchone()[0]
        artifact_values = {
            "observation_count": float(
                conn.execute(
                    "SELECT sum(observation_count) FROM coverage_attrition"
                ).fetchone()[0]
            ),
            "fixed_core_oos_mean_lift_pct": float(
                conn.execute(
                    "SELECT avg(mean_lift_pct) FROM oos_fit_score_lift "
                    "WHERE is_primary AND horizon = 20 AND family = 'fixed' "
                    "AND ring = 'core_high_high'"
                ).fetchone()[0]
            ),
            "ols_core_oos_mean_lift_pct": float(
                conn.execute(
                    "SELECT avg(mean_lift_pct) FROM oos_fit_score_lift "
                    "WHERE is_primary AND horizon = 20 AND family = 'ols' "
                    "AND ring = 'core_high_high'"
                ).fetchone()[0]
            ),
            "near1_fixed_minus_ols_mean_lift_pct": float(
                conn.execute(
                    "SELECT avg(fixed_minus_ols_lift_pct) "
                    "FROM fixed_vs_ols_paired WHERE horizon = 20 "
                    "AND ring = 'near_high_high_1'"
                ).fetchone()[0]
            ),
            "fixed_top5_mean_lift_pct": float(
                conn.execute(
                    "SELECT avg(topk_lift_pct) FROM topk_operational_lift "
                    "WHERE horizon = 20 AND family = 'fixed' AND k = 5"
                ).fetchone()[0]
            ),
            "ols_top5_mean_lift_pct": float(
                conn.execute(
                    "SELECT avg(topk_lift_pct) FROM topk_operational_lift "
                    "WHERE horizon = 20 AND family = 'ols' AND k = 5"
                ).fetchone()[0]
            ),
        }
    finally:
        conn.close()

    assert digest["decision"] == artifact_decision
    for key, expected in artifact_values.items():
        assert float(digest["publication_metrics"][key]) == pytest.approx(
            expected, abs=0.00005
        )
