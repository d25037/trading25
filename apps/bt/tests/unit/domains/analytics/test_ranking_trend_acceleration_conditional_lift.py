from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from src.domains.analytics.ranking_trend_acceleration_conditional_lift import (
    CANDIDATE_REGISTRY,
    RankingTrendAccelerationConditionalLiftResult,
    SEGMENTS,
    _add_candidate_local_percentiles,
    _build_candidate_observations,
    _build_bootstrap_effect_ci_df,
    _build_conditional_binary_lift_df,
    _build_continuous_rank_lift_df,
    _build_decision_gate_df,
    _build_fixed_incremental_2x2_df,
    _build_topk_priority_lift_df,
    build_summary_markdown,
    classify_trend_acceleration_triple,
    moving_block_bootstrap_ci,
    run_ranking_trend_acceleration_conditional_lift_research,
    write_ranking_trend_acceleration_conditional_lift_bundle,
)
from tests.unit.domains.analytics.test_ranking_sma5_count_long_evidence import (
    _build_sma5_count_long_db,
)


@pytest.mark.parametrize(
    ("s20", "s60", "expected"),
    [
        (2.0, 1.0, True),
        (1.0, 1.0, False),
        (1.0, 0.0, False),
        (-1.0, -2.0, False),
        (None, 1.0, False),
    ],
)
def test_trend_acceleration_triple_boundaries(
    s20: float | None,
    s60: float | None,
    expected: bool,
) -> None:
    assert classify_trend_acceleration_triple(s20, s60) is expected


def test_candidate_predicates_do_not_reference_trend_or_future_columns() -> None:
    forbidden = ("slope", "r2", "forward_", "future_")
    for candidate in CANDIDATE_REGISTRY:
        assert not any(token in candidate.predicate.lower() for token in forbidden)


def test_panel_uses_exact_date_prime_equivalent_membership_and_exclusive_slices(
    tmp_path: Path,
) -> None:
    db_path = _build_mixed_market_db(tmp_path / "market.duckdb")

    result = _run_fixture_research(db_path)

    sample = result.observation_sample_df
    assert not sample.empty
    assert set(sample["market_code"].astype(str)).issubset({"0101", "0111"})
    assert {"0101", "0111"}.issubset(set(sample["market_code"].astype(str)))
    assert not set(sample["market_code"].astype(str)).intersection({"0112", "0113"})
    exclusive = sample.loc[sample["candidate_kind"] == "exclusive_slice"]
    assert not exclusive.empty
    assert exclusive.groupby(["code", "date"]).size().eq(1).all()
    assert not sample.duplicated(["code", "date", "candidate_group"]).any()


def test_future_append_does_not_change_earlier_features_or_candidates(
    tmp_path: Path,
) -> None:
    db_path = _build_mixed_market_db(tmp_path / "market.duckdb")
    before = _run_fixture_research(db_path).observation_sample_df
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            "INSERT INTO stock_data_raw VALUES "
            "('1111', '2025-01-06', 999, 1000, 998, 999, 10000, 1.0)"
        )
        conn.execute(
            "INSERT INTO stock_master_daily "
            "(date, code, company_name, market_code, market_name, scale_category, "
            "sector_33_code, sector_33_name) VALUES "
            "('2025-01-06', '1111', 'Alpha', '0113', 'Growth', NULL, '3600', 'Machinery')"
        )
    finally:
        conn.close()

    after = _run_fixture_research(db_path).observation_sample_df

    stable_columns = [
        "date",
        "code",
        "candidate_group",
        "candidate_kind",
        "price_lr_slope_20_pct",
        "price_lr_slope_60_pct",
        "trend_acceleration_triple",
        "exclusive_slice",
    ]
    pd.testing.assert_frame_equal(
        before[stable_columns].reset_index(drop=True),
        after[stable_columns].reset_index(drop=True),
    )


def test_poisoned_stock_data_cannot_change_price_pit_study(tmp_path: Path) -> None:
    db_path = _build_mixed_market_db(tmp_path / "market.duckdb")
    before = _run_fixture_research(db_path)
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            UPDATE stock_data
            SET open = 999999.0,
                high = 999999.0,
                low = 0.01,
                close = CASE WHEN date < '2024-03-01' THEN 0.01 ELSE 999999.0 END,
                volume = 1
            """
        )
    finally:
        conn.close()

    after = _run_fixture_research(db_path)

    pd.testing.assert_frame_equal(
        before.observation_sample_df.reset_index(drop=True),
        after.observation_sample_df.reset_index(drop=True),
    )
    assert before.price_projection == after.price_projection


@pytest.mark.parametrize(
    ("failure", "message"),
    [
        ("missing", "segment cardinality must be exactly one"),
        ("overlap", "segment cardinality must be exactly one"),
        ("invalid", "segment factor must be finite and positive"),
    ],
)
def test_price_pit_study_fails_closed_on_invalid_lineage(
    tmp_path: Path,
    failure: str,
    message: str,
) -> None:
    db_path = _build_mixed_market_db(tmp_path / "market.duckdb")
    _mark_fixture_market_v4(db_path)
    conn = duckdb.connect(str(db_path))
    try:
        basis_id = conn.execute(
            "SELECT basis_id FROM stock_adjustment_bases WHERE code = '1111'"
        ).fetchone()[0]
        if failure == "missing":
            conn.execute(
                "DELETE FROM stock_adjustment_basis_segments "
                "WHERE code = '1111' AND basis_id = ?",
                [basis_id],
            )
        elif failure == "overlap":
            conn.execute(
                "INSERT INTO stock_adjustment_basis_segments VALUES (?, ?, ?, ?, ?)",
                ["1111", basis_id, "2016-01-01", None, 1.0],
            )
        else:
            conn.execute(
                "UPDATE stock_adjustment_basis_segments SET cumulative_factor = 0.0 "
                "WHERE code = '1111' AND basis_id = ?",
                [basis_id],
            )
    finally:
        conn.close()

    with pytest.raises(RuntimeError, match=message):
        _run_fixture_research(db_path)


def test_binary_lift_requires_two_symbols_on_both_sides_same_day() -> None:
    rows: list[dict[str, object]] = []
    for paired_date, triple_values, control_values in (
        ("2024-03-04", (1.0, 2.0), (0.0,)),
        ("2024-03-05", (2.0, 4.0), (-1.0, 1.0)),
        ("2024-03-06", (3.0, None), (0.0, 1.0)),
    ):
        for index, value in enumerate(triple_values):
            rows.append(_observation(paired_date, f"T{index}", True, value))
        for index, value in enumerate(control_values):
            rows.append(_observation(paired_date, f"C{index}", False, value))

    result = _build_conditional_binary_lift_df(
        pd.DataFrame(rows),
        horizons=(20,),
        severe_loss_threshold_pct=-10.0,
    )

    assert set(result["paired_date"]) == {"2024-03-05"}
    row = result.iloc[0]
    assert row["triple_observation_count"] == 2
    assert row["control_observation_count"] == 2
    assert row["mean_lift_pct"] == pytest.approx(3.0)


def test_continuous_percentiles_are_candidate_date_local() -> None:
    frame = pd.DataFrame(
        [
            {
                "candidate_group": candidate,
                "date": "2024-03-05",
                "trend_acceleration_margin_pct": float(index),
            }
            for candidate in ("core_long_only", "momentum_value_only")
            for index in range(1, 6)
        ]
    )

    ranked = _add_candidate_local_percentiles(frame)

    assert (
        ranked.groupby(["candidate_group", "date"])["acceleration_percentile"]
        .max()
        .eq(1.0)
        .all()
    )
    assert ranked.groupby(["candidate_group", "date"])[
        "acceleration_percentile"
    ].min().eq(0.2).all()


def test_continuous_rank_lift_has_complete_top_bottom_distribution_contract() -> None:
    result = _build_continuous_rank_lift_df(
        pd.DataFrame(),
        horizons=(20,),
        severe_loss_threshold_pct=-10.0,
    )

    assert result.columns.tolist() == [
        "candidate_group",
        "candidate_kind",
        "horizon",
        "paired_date",
        "observation_count",
        "symbol_count",
        "bottom_observation_count",
        "middle_observation_count",
        "top_observation_count",
        "bottom_mean_excess_return_pct",
        "middle_mean_excess_return_pct",
        "top_mean_excess_return_pct",
        "top_minus_bottom_lift_pct",
        "bottom_median_excess_return_pct",
        "top_median_excess_return_pct",
        "top_minus_bottom_median_lift_pct",
        "bottom_win_rate_pct",
        "top_win_rate_pct",
        "bottom_p10_excess_return_pct",
        "top_p10_excess_return_pct",
        "bottom_p25_excess_return_pct",
        "top_p25_excess_return_pct",
        "bottom_severe_loss_rate_pct",
        "top_severe_loss_rate_pct",
        "severe_loss_rate_difference_pct",
        "spearman_ic",
    ]


def test_topk_priority_lift_has_complete_basket_priority_distribution_contract() -> None:
    result = _build_topk_priority_lift_df(
        pd.DataFrame(),
        horizons=(20,),
        severe_loss_threshold_pct=-10.0,
    )

    assert result.columns.tolist() == [
        "candidate_group",
        "candidate_kind",
        "horizon",
        "date",
        "k",
        "candidate_count",
        "candidate_outcome_count",
        "candidate_outcome_coverage_pct",
        "selected_outcome_count",
        "selected_outcome_coverage_pct",
        "outcome_status",
        "basket_mean_excess_return_pct",
        "basket_median_excess_return_pct",
        "basket_win_rate_pct",
        "basket_p10_excess_return_pct",
        "basket_p25_excess_return_pct",
        "basket_severe_loss_rate_pct",
        "priority_mean_excess_return_pct",
        "priority_median_excess_return_pct",
        "priority_win_rate_pct",
        "priority_p10_excess_return_pct",
        "priority_p25_excess_return_pct",
        "priority_severe_loss_rate_pct",
        "priority_lift_pct",
        "symbol_turnover_pct",
        "rank_stability_spearman",
    ]


def test_topk_priority_lift_missing_outcome_does_not_backfill_ranked_selection() -> None:
    rows = [
        {
            "date": "2024-03-01",
            "code": f"{index:02d}",
            "candidate_group": "core_long_only",
            "candidate_kind": "exclusive_slice",
            "trend_acceleration_triple": True,
            "trend_acceleration_margin_pct": float(20 - index),
            "forward_close_excess_return_20d_pct": (
                float("nan") if index == 0 else float(index)
            ),
        }
        for index in range(20)
    ]

    topk = _build_topk_priority_lift_df(
        pd.DataFrame(rows),
        horizons=(20,),
        severe_loss_threshold_pct=-10.0,
    )

    row = topk.loc[topk["k"].eq(5)].iloc[0]
    assert row["candidate_count"] == 20
    assert row["candidate_outcome_count"] == 19
    assert row["candidate_outcome_coverage_pct"] == 95.0
    assert row["selected_outcome_count"] == 4
    assert row["selected_outcome_coverage_pct"] == 80.0
    assert row["outcome_status"] == "incomplete_outcomes"
    assert row[
        [
            "basket_mean_excess_return_pct",
            "basket_median_excess_return_pct",
            "basket_win_rate_pct",
            "basket_p10_excess_return_pct",
            "basket_p25_excess_return_pct",
            "basket_severe_loss_rate_pct",
            "priority_mean_excess_return_pct",
            "priority_median_excess_return_pct",
            "priority_win_rate_pct",
            "priority_p10_excess_return_pct",
            "priority_p25_excess_return_pct",
            "priority_severe_loss_rate_pct",
            "priority_lift_pct",
        ]
    ].isna().all()


def test_topk_priority_lift_preserves_overlapping_candidate_groups() -> None:
    rows = []
    for candidate_group, reverse_ranking in (
        ("core_long_only", False),
        ("momentum_value_only", True),
    ):
        for index in range(10):
            rows.append(
                {
                    "date": "2024-03-01",
                    "code": f"{index:02d}",
                    "candidate_group": candidate_group,
                    "candidate_kind": "overlapping_slice",
                    "trend_acceleration_triple": True,
                    "trend_acceleration_margin_pct": float(
                        index if reverse_ranking else 10 - index
                    ),
                    "forward_close_excess_return_20d_pct": float(index),
                }
            )

    topk = _build_topk_priority_lift_df(
        pd.DataFrame(rows),
        horizons=(20,),
        severe_loss_threshold_pct=-10.0,
    )

    selected = topk.loc[topk["k"].eq(5)]
    assert set(selected["candidate_group"]) == {
        "core_long_only",
        "momentum_value_only",
    }
    assert selected["candidate_count"].eq(10).all()


def test_moving_block_bootstrap_is_fixed_seed_reproducible() -> None:
    values = pd.Series([1.0, -0.5, 2.0, 0.25, 1.25]).to_numpy()

    first = moving_block_bootstrap_ci(
        values,
        block_length=3,
        resamples=100,
        seed=42,
    )
    second = moving_block_bootstrap_ci(
        values,
        block_length=3,
        resamples=100,
        seed=42,
    )

    assert first == second
    assert first[0] == pytest.approx(values.mean())


def test_bootstrap_effect_ci_includes_every_topk_priority_comparison() -> None:
    topk_rows: list[dict[str, object]] = []
    for candidate_group in ("core_long_only", "momentum_value_only"):
        for horizon in (5, 20):
            for k in (5, 10):
                for day, lift in enumerate((0.5, 1.0, -0.25), start=1):
                    topk_rows.append(
                        {
                            "candidate_group": candidate_group,
                            "candidate_kind": "exclusive_slice",
                            "horizon": horizon,
                            "date": f"2024-01-{day:02d}",
                            "k": k,
                            "priority_lift_pct": lift,
                        }
                    )

    result = _build_bootstrap_effect_ci_df(
        pd.DataFrame(),
        pd.DataFrame(),
        pd.DataFrame(),
        pd.DataFrame(topk_rows),
        resamples=100,
        seed=42,
    )

    assert result.columns.tolist() == [
        "comparison",
        "candidate_group",
        "candidate_kind",
        "horizon",
        "k",
        "period_type",
        "period_label",
        "date_count",
        "block_length",
        "resamples",
        "seed",
        "point_estimate_pct",
        "ci_lower_95_pct",
        "ci_upper_95_pct",
    ]
    all_available = result.loc[
        (result["comparison"] == "topk_priority")
        & (result["period_label"] == "all_available")
    ]
    actual_comparisons = set(
        all_available[["candidate_group", "horizon", "k"]].itertuples(
            index=False,
            name=None,
        )
    )
    assert actual_comparisons == {
        (candidate_group, horizon, k)
        for candidate_group in ("core_long_only", "momentum_value_only")
        for horizon in (5, 20)
        for k in (5, 10)
    }
    assert all_available["date_count"].eq(3).all()
    assert all_available["resamples"].eq(100).all()
    assert all_available["seed"].eq(42).all()
    assert all_available["point_estimate_pct"].eq(5.0 / 12.0).all()


def test_fixed_dual_lift_excludes_missing_slopes_and_emits_paired_spread() -> None:
    rows = [
        {
            **_observation("2024-03-05", code, triple, outcome),
            "fixed_dual_positive": True,
        }
        for code, triple, outcome in (
            ("T1", True, 3.0),
            ("T2", True, 5.0),
            ("C1", False, 0.0),
            ("C2", False, 2.0),
        )
    ]
    rows.append(
        {
            **_observation("2024-03-05", "MISSING", False, -99.0),
            "fixed_dual_positive": True,
            "trend_acceleration_margin_pct": None,
        }
    )

    result = _build_fixed_incremental_2x2_df(
        pd.DataFrame(rows),
        horizons=(20,),
        severe_loss_threshold_pct=-10.0,
    )

    paired = result.loc[result["row_type"] == "fixed_dual_positive_lift"].iloc[0]
    assert paired["triple_observation_count"] == 2
    assert paired["control_observation_count"] == 2
    assert paired["mean_lift_pct"] == pytest.approx(3.0)


def test_fixed_dual_preserves_missing_returns_and_excludes_incomplete_2x2() -> None:
    conn = duckdb.connect()
    candidate_base = pd.DataFrame(
        [
            {
                "date": "2024-03-05",
                "code": "1111",
                "company_name": "Alpha",
                "market": "Prime",
                "market_code": "0111",
                "liquidity_regime": "neutral_rerating",
                "valuation_signal": "strong_value_confirmation",
                "liquidity_residual_z": 0.0,
                "recent_return_20d_pct": None,
                "recent_return_60d_pct": 2.0,
                "forecast_operating_profit_growth_ratio": 1.0,
                "core_long_flag": True,
                "momentum_value_flag": False,
                "neutral_rerating_good_flag": False,
                "earnings_priority_flag": False,
                "aggressive_rerating_flag": False,
                "forward_outcome_completion_date_20d": "2024-04-03",
                "forward_close_return_20d_pct": 4.0,
                "forward_close_excess_return_20d_pct": 3.0,
            }
        ]
    )
    features = pd.DataFrame(
        [
            {
                "date": "2024-03-05",
                "code": "1111",
                "price_lr_slope_20_pct": 2.0,
                "price_lr_slope_60_pct": 1.0,
                "price_lr_r2_20": 0.9,
                "price_lr_r2_60": 0.8,
            }
        ]
    )
    conn.register("candidate_base_df", candidate_base)
    conn.register("features_df", features)
    try:
        conn.execute(
            "CREATE TEMP TABLE ranking_trend_acceleration_candidate_base "
            "AS SELECT * FROM candidate_base_df"
        )
        conn.execute(
            "CREATE TEMP TABLE ranking_trend_acceleration_features "
            "AS SELECT * FROM features_df"
        )
        observations = _build_candidate_observations(conn, horizons=(20,))
    finally:
        conn.close()

    assert observations["fixed_dual_positive"].isna().all()
    result = _build_fixed_incremental_2x2_df(
        observations,
        horizons=(20,),
        severe_loss_threshold_pct=-10.0,
    )
    assert result.empty


def test_decision_gate_uses_eligible_observations_and_two_family_replication() -> None:
    coverage = pd.DataFrame(
        {
            "candidate_group": ["core_long_only", "momentum_value_only"],
            "trend_feature_coverage_pct": [100.0, 100.0],
        }
    )
    stability_rows: list[dict[str, object]] = []
    for family, lift in (
        ("core_long_only", 0.4),
        ("momentum_value_only", 0.5),
        ("aggressive_rerating", -0.5),
    ):
        stability_rows.append(
            _stability_row(family, "combined_historical_2017_2023", lift)
        )
        for segment, _start, _end in (
            ("historical_pre_reorg", None, None),
            ("historical_post_reorg", None, None),
            ("recent_hypothesis_origin", None, None),
        ):
            stability_rows.append(_stability_row(family, segment, lift, "segment"))
    stability = pd.DataFrame(stability_rows)
    bootstrap = pd.DataFrame(
        {
            "comparison": ["continuous_margin"] * 3,
            "horizon": [20] * 3,
            "period_label": ["combined_historical_2017_2023"] * 3,
            "candidate_group": [
                "core_long_only",
                "momentum_value_only",
                "aggressive_rerating",
            ],
            "ci_lower_95_pct": [0.1, 0.1, -1.0],
        }
    )

    decision = _build_decision_gate_df(
        coverage,
        stability,
        bootstrap,
        pd.DataFrame(),
    )
    assert decision.iloc[-1]["recommendation"] == "add_continuous_columns"

    stability["meets_min_observations"] = False
    rejected = _build_decision_gate_df(
        coverage,
        stability,
        bootstrap,
        pd.DataFrame(),
    )
    assert rejected.iloc[-1]["recommendation"] == "reject_introduction"


def test_binary_gate_rejects_rotated_family_sets() -> None:
    coverage = pd.DataFrame(
        {
            "candidate_group": ["core_long_only", "momentum_value_only"],
            "trend_feature_coverage_pct": [100.0, 100.0],
        }
    )
    stability_rows: list[dict[str, object]] = []
    for family, lift, median_candidates in (
        ("core_long_only", 0.4, 4.0),
        ("momentum_value_only", 0.5, 6.0),
        ("aggressive_rerating", -0.5, 6.0),
    ):
        historical = _stability_row(
            family,
            "combined_historical_2017_2023",
            lift,
            comparison="binary_triple",
        )
        historical["median_focus_candidates_per_date"] = median_candidates
        stability_rows.append(historical)
        for segment, _start, _end in SEGMENTS:
            stability_rows.append(
                _stability_row(
                    family,
                    segment,
                    lift,
                    period_type="segment",
                    comparison="binary_triple",
                )
            )
    bootstrap = pd.DataFrame(
        {
            "comparison": ["binary_triple"] * 3,
            "horizon": [20] * 3,
            "period_label": ["combined_historical_2017_2023"] * 3,
            "candidate_group": [
                "core_long_only",
                "momentum_value_only",
                "aggressive_rerating",
            ],
            "ci_lower_95_pct": [0.1, 0.1, -1.0],
        }
    )

    decision = _build_decision_gate_df(
        coverage,
        pd.DataFrame(stability_rows),
        bootstrap,
        pd.DataFrame(),
    )

    assert decision.iloc[-1]["recommendation"] == "reject_introduction"
    same_family_gate = decision.loc[
        (decision["recommendation"] == "add_binary_badge_only")
        & (decision["gate"] == "two_independent_families_positive"),
        "passed",
    ]
    assert same_family_gate.tolist() == [False]


def _stability_row(
    family: str,
    period_label: str,
    lift: float,
    period_type: str = "combined_historical",
    comparison: str = "continuous_margin",
) -> dict[str, object]:
    return {
        "comparison": comparison,
        "horizon": 20,
        "candidate_group": family,
        "period_type": period_type,
        "period_label": period_label,
        "meets_min_observations": True,
        "median_daily_spearman_ic": 0.03,
        "ic_positive_date_rate_pct": 55.0,
        "mean_daily_lift_pct": lift,
        "median_daily_lift_pct": lift,
        "positive_date_rate_pct": 55.0,
        "mean_severe_loss_rate_difference_pct": 0.0,
        "median_focus_candidates_per_date": 6.0,
    }


def test_bundle_contains_exactly_ten_tables_and_every_summary_section(
    tmp_path: Path,
) -> None:
    result = _run_fixture_research(
        _build_mixed_market_db(tmp_path / "market.duckdb")
    )

    summary = build_summary_markdown(result)
    expected_sections = {
        "Coverage Diagnostics",
        "Candidate Registry",
        "Conditional Binary Lift",
        "Fixed Incremental 2x2",
        "Continuous Rank Lift",
        "Top-K Priority Lift",
        "Segment Stability",
        "Bootstrap Effect CI",
        "Decision Gate",
        "Observation Sample",
    }
    assert all(f"## {section}" in summary for section in expected_sections)

    bundle = write_ranking_trend_acceleration_conditional_lift_bundle(
        result,
        output_root=tmp_path / "research",
        run_id="unit-test",
        notes="unit fixture",
    )
    conn = duckdb.connect(str(bundle.results_db_path), read_only=True)
    try:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main'"
            ).fetchall()
        }
    finally:
        conn.close()
    assert tables == {
        "coverage_diagnostics_df",
        "candidate_registry_df",
        "conditional_binary_lift_df",
        "fixed_incremental_2x2_df",
        "continuous_rank_lift_df",
        "topk_priority_lift_df",
        "segment_stability_df",
        "bootstrap_effect_ci_df",
        "decision_gate_df",
        "observation_sample_df",
    }
    manifest = json.loads(bundle.manifest_path.read_text())
    price_projection = manifest["result_metadata"]["price_projection"]
    assert price_projection["physical_price_source"] == "stock_data_raw"
    assert price_projection["no_stock_data_fallback"] is True
    assert price_projection["verification_status"] == "verified"
    assert price_projection["signal_feature_row_count"] > 0
    assert price_projection["completed_outcome_row_count"] > 0
    assert price_projection["signal_basis_sha256"]
    assert price_projection["completion_basis_sha256"]
    assert price_projection["price_projection_sha256"]
    assert "Physical price source: `stock_data_raw`" in summary
    assert "`stock_data` fallback: `false`" in summary
    assert (
        f"outcome requests `{result.price_projection.outcome_request_row_count}`"
        in summary
    )
    assert result.price_projection.price_projection_sha256 in summary


def test_trend_observation_bundle_preserves_sparse_session_authoritative_outcome(
    tmp_path: Path,
) -> None:
    db_path = _build_mixed_market_db(tmp_path / "market.duckdb")
    _mark_fixture_market_v4(db_path)
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            "DELETE FROM stock_data_raw "
            "WHERE code = '1111' AND date = '2024-03-08'"
        )
        signal_close = conn.execute(
            "SELECT close FROM stock_data_raw "
            "WHERE code = '1111' AND date = '2024-03-01'"
        ).fetchone()[0]
        completion_close = conn.execute(
            "SELECT close FROM stock_data_raw "
            "WHERE code = '1111' AND date = '2024-03-11'"
        ).fetchone()[0]
        signal_topix = conn.execute(
            "SELECT close FROM topix_data WHERE date = '2024-03-01'"
        ).fetchone()[0]
        nominal_topix = conn.execute(
            "SELECT close FROM topix_data WHERE date = '2024-03-08'"
        ).fetchone()[0]
        completion_topix = conn.execute(
            "SELECT close FROM topix_data WHERE date = '2024-03-11'"
        ).fetchone()[0]
    finally:
        conn.close()

    result = run_ranking_trend_acceleration_conditional_lift_research(
        db_path,
        start_date="2024-03-01",
        end_date="2024-03-08",
        horizons=(5,),
        min_observations=1,
        bootstrap_resamples=5,
        bootstrap_seed=17,
        observation_sample_limit=20_000,
    )
    expected_columns = {
        "forward_outcome_completion_date_5d",
        "forward_close_return_5d_pct",
        "forward_close_excess_return_5d_pct",
    }
    assert expected_columns.issubset(result.observation_sample_df.columns)
    row = result.observation_sample_df.loc[
        result.observation_sample_df["code"].astype(str).eq("1111")
        & result.observation_sample_df["date"].eq("2024-03-01")
    ].iloc[0]
    expected_return = (completion_close / signal_close - 1.0) * 100.0
    expected_aligned = expected_return - (completion_topix / signal_topix - 1.0) * 100.0
    nominal_excess = expected_return - (nominal_topix / signal_topix - 1.0) * 100.0
    assert row["forward_outcome_completion_date_5d"] == pd.Timestamp("2024-03-11")
    assert row["forward_close_return_5d_pct"] == pytest.approx(expected_return)
    assert row["forward_close_excess_return_5d_pct"] == pytest.approx(expected_aligned)
    assert row["forward_close_excess_return_5d_pct"] != pytest.approx(nominal_excess)

    bundle = write_ranking_trend_acceleration_conditional_lift_bundle(
        result,
        output_root=tmp_path / "research",
        run_id="sparse-session",
    )
    conn = duckdb.connect(str(bundle.results_db_path), read_only=True)
    try:
        persisted = conn.execute(
            "SELECT forward_outcome_completion_date_5d, "
            "forward_close_return_5d_pct, forward_close_excess_return_5d_pct "
            "FROM observation_sample_df WHERE code = '1111' "
            "AND CAST(date AS DATE) = DATE '2024-03-01' LIMIT 1"
        ).fetchone()
    finally:
        conn.close()
    assert persisted is not None
    assert pd.Timestamp(persisted[0]) == pd.Timestamp("2024-03-11")
    assert persisted[1] == pytest.approx(expected_return)
    assert persisted[2] == pytest.approx(expected_aligned)


def _observation(
    paired_date: str,
    code: str,
    triple: bool,
    outcome: float | None,
) -> dict[str, object]:
    return {
        "date": paired_date,
        "code": code,
        "candidate_group": "core_long_only",
        "candidate_kind": "exclusive_slice",
        "trend_acceleration_margin_pct": 1.0 if triple else -1.0,
        "trend_acceleration_triple": triple,
        "forward_close_excess_return_20d_pct": outcome,
    }


def _run_fixture_research(
    db_path: Path,
) -> RankingTrendAccelerationConditionalLiftResult:
    _mark_fixture_market_v4(db_path)
    return run_ranking_trend_acceleration_conditional_lift_research(
        db_path,
        start_date="2024-03-01",
        end_date="2024-03-08",
        horizons=(5, 20),
        min_observations=1,
        bootstrap_resamples=20,
        bootstrap_seed=17,
        observation_sample_limit=20_000,
    )


def _build_mixed_market_db(db_path: Path) -> Path:
    _build_sma5_count_long_db(db_path)
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            UPDATE stock_master_daily
            SET market_code = CASE
                WHEN code = '1111' AND date < '2024-03-05' THEN '0101'
                WHEN code = '1111' THEN '0111'
                WHEN code = '2222' THEN '0111'
                WHEN code = '3333' THEN '0112'
                WHEN code = '4444' THEN '0113'
                ELSE market_code
            END
            """
        )
        conn.execute(
            """
            UPDATE stock_data AS target
            SET
                open = source.open * 1.2,
                high = source.high * 1.2,
                low = source.low * 1.2,
                close = source.close * 1.2
            FROM stock_data AS source
            WHERE target.code = '2222'
              AND source.code = '1111'
              AND source.date = target.date
            """
        )
        conn.execute(
            """
            UPDATE daily_valuation AS target
            SET
                per = source.per,
                forward_per = source.forward_per,
                pbr = source.pbr,
                p_op = source.p_op,
                forward_p_op = source.forward_p_op
            FROM daily_valuation AS source
            WHERE target.code = '2222'
              AND source.code = '1111'
              AND source.date = target.date
            """
        )
    finally:
        conn.close()
    return db_path


def _mark_fixture_market_v4(db_path: Path) -> None:
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS market_schema_version(version INTEGER)")
        if (
            conn.execute("SELECT count(*) FROM market_schema_version").fetchone()[0]
            == 0
        ):
            conn.execute("INSERT INTO market_schema_version VALUES (4)")
        conn.execute("CREATE TABLE IF NOT EXISTS sync_metadata(key VARCHAR, value VARCHAR)")
        if (
            conn.execute(
                "SELECT count(*) FROM sync_metadata "
                "WHERE key = 'stock_price_adjustment_mode'"
            ).fetchone()[0]
            == 0
        ):
            conn.execute(
                "INSERT INTO sync_metadata VALUES "
                "('stock_price_adjustment_mode', 'local_projection_v2_event_time')"
            )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS stock_adjustment_bases (
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
            CREATE TABLE IF NOT EXISTS stock_adjustment_basis_segments (
                code TEXT,
                basis_id TEXT,
                source_date_from TEXT,
                source_date_to_exclusive TEXT,
                cumulative_factor DOUBLE
            )
            """
        )
        if (
            conn.execute("SELECT count(*) FROM stock_adjustment_bases").fetchone()[0]
            == 0
        ):
            first_date, last_date = conn.execute(
                "SELECT min(date), max(date) FROM stock_data"
            ).fetchone()
            codes = [
                str(row[0])
                for row in conn.execute(
                    "SELECT DISTINCT code FROM daily_valuation ORDER BY code"
                ).fetchall()
            ]
            for code in codes:
                basis_id = f"event-pit-v1:{code}:{first_date}"
                conn.execute(
                    "INSERT INTO stock_adjustment_bases VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        code,
                        basis_id,
                        first_date,
                        None,
                        first_date,
                        f"fixture-{code}",
                        last_date,
                        "ready",
                    ],
                )
                conn.execute(
                    "INSERT INTO stock_adjustment_basis_segments VALUES (?, ?, ?, ?, ?)",
                    [code, basis_id, first_date, None, 1.0],
                )
                conn.execute(
                    "UPDATE daily_valuation SET basis_version = ? WHERE code = ?",
                    [basis_id, code],
                )
        tables = {
            str(row[0])
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables"
            ).fetchall()
        }
        if "stock_data_raw" not in tables:
            conn.execute(
                "CREATE TABLE stock_data_raw AS "
                "SELECT *, 1.0::DOUBLE AS adjustment_factor FROM stock_data"
            )
    finally:
        conn.close()
