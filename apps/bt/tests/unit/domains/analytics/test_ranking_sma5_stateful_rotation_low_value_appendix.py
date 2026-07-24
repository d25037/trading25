from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pandas as pd
import pytest

from src.domains.analytics.ranking_sma5_score_ring_stateful_rotation_evidence import (
    RankingSma5ScoreRingStatefulRotationResult,
)
from src.domains.analytics.ranking_sma5_stateful_rotation_low_value_appendix import (
    build_low_value_appendix_evidence,
    transform_low_value_appendix_scores,
)


def _boundary_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "value_composite_equal_score": [0.2, 0.3, 0.4, 0.2, 0.2, None],
            "long_hybrid_leadership_score": [
                0.7,
                0.7,
                0.7,
                0.6999,
                None,
                0.7,
            ],
        },
        index=[
            "core_boundary",
            "near1_boundary",
            "near2_boundary",
            "long_0699",
            "long_nan",
            "value_nan",
        ],
    )


def _episode_frame() -> pd.DataFrame:
    dates = pd.date_range("2024-01-01", periods=7, freq="D")
    rows: list[dict[str, object]] = []
    definitions = {
        "source": (0.2, 0.7),
        "target": (0.2, 0.7),
        "bad_long": (0.2, 0.6999),
        "bad_value": (0.2001, 0.7),
    }
    for code, (value_score, long_score) in definitions.items():
        for position, date in enumerate(dates):
            count = 2
            deviation = 0.0
            if code == "source" and position >= 2:
                count = 1
            if code == "target" and position == 5:
                deviation = -1.0
            rows.append(
                {
                    "date": date,
                    "code": code,
                    "close": 100.0 + position,
                    "topix_close": 100.0,
                    "value_composite_equal_score": (
                        value_score if position >= 1 else 0.9
                    ),
                    "long_hybrid_leadership_score": (
                        long_score if position >= 1 else 0.0
                    ),
                    "sma5": 100.0,
                    "sma5_above_count_5d": count,
                    "sma5_below_streak": 0,
                    "sma5_atr20_deviation": deviation,
                }
            )
    return pd.DataFrame(rows)


def test_appendix_ring_transform_uses_inclusive_fixed_boundaries() -> None:
    transformed = transform_low_value_appendix_scores(_boundary_frame())

    assert transformed.loc[
        "core_boundary", "value_composite_equal_score"
    ] == pytest.approx(0.8)
    assert transformed.loc[
        "near1_boundary", "value_composite_equal_score"
    ] == pytest.approx(0.7)
    assert transformed.loc[
        "near2_boundary", "value_composite_equal_score"
    ] == pytest.approx(0.6)
    assert transformed.loc[
        "core_boundary", "long_hybrid_leadership_score"
    ] == 1.0
    assert transformed.loc["long_0699", "long_hybrid_leadership_score"] == 0.0


def test_appendix_ring_transform_preserves_value_nan_and_excludes_long_nan() -> None:
    transformed = transform_low_value_appendix_scores(_boundary_frame())

    assert pd.isna(
        transformed.loc["value_nan", "value_composite_equal_score"]
    )
    assert transformed.loc["long_nan", "long_hybrid_leadership_score"] == 0.0


def test_appendix_actual_episode_membership_uses_low_value_boundaries() -> None:
    result = build_low_value_appendix_evidence(_episode_frame())

    core_events = result.stateful_rotation_event_df.query(
        "ring_id == 'low_value_core'"
    )
    assert set(core_events["source_code"]) == {"source"}
    assert core_events.iloc[0]["target_count"] == 1


def test_appendix_reuses_stateful_result_and_does_not_mutate_input() -> None:
    feature_df = _episode_frame()
    original = feature_df.copy(deep=True)

    result = build_low_value_appendix_evidence(feature_df)

    pd.testing.assert_frame_equal(feature_df, original)
    assert set(result.stateful_rotation_summary_df["ring_id"]) == {
        "low_value_core",
        "low_value_near1",
        "low_value_near2",
    }
    for table_name in (
        "stateful_rotation_summary_df",
        "stateful_rotation_annual_df",
        "stateful_rotation_exit_reason_df",
        "stateful_rotation_decision_df",
        "stateful_rotation_event_df",
        "coverage_diagnostics_df",
    ):
        assert hasattr(result, table_name)


def test_appendix_remaps_all_six_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.domains.analytics import (
        ranking_sma5_stateful_rotation_low_value_appendix as appendix,
    )

    tables = {
        name: pd.DataFrame(
            {
                "ring_id": [
                    "core_high_high",
                    "near_high_high_1",
                    "near_high_high_2",
                ]
            }
        )
        for name in (
            "stateful_rotation_summary_df",
            "stateful_rotation_annual_df",
            "stateful_rotation_exit_reason_df",
            "stateful_rotation_decision_df",
            "stateful_rotation_event_df",
            "coverage_diagnostics_df",
        )
    }
    result = RankingSma5ScoreRingStatefulRotationResult(**tables)
    monkeypatch.setattr(
        appendix,
        "build_stateful_rotation_evidence",
        lambda _frame: replace(result),
    )

    remapped = appendix.build_low_value_appendix_evidence(_boundary_frame())

    expected = {"low_value_core", "low_value_near1", "low_value_near2"}
    for table_name in tables:
        assert set(getattr(remapped, table_name)["ring_id"]) == expected


def test_appendix_bundle_contract_contains_six_tables_and_manifest_metadata(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from scripts.research import (
        run_ranking_sma5_stateful_rotation_low_value_appendix as runner,
    )

    empty = pd.DataFrame()
    result = RankingSma5ScoreRingStatefulRotationResult(
        stateful_rotation_summary_df=empty,
        stateful_rotation_annual_df=empty,
        stateful_rotation_exit_reason_df=empty,
        stateful_rotation_decision_df=empty,
        stateful_rotation_event_df=empty,
        coverage_diagnostics_df=empty,
        db_path="/tmp/market.duckdb",
        analysis_start_date="2018-01-01",
        analysis_end_date="2026-07-21",
        market_schema_version=5,
        stock_price_adjustment_mode="provider_adjusted_v1",
    )
    captured: dict[str, object] = {}

    def fake_write_research_bundle(**kwargs: object) -> object:
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(runner, "write_research_bundle", fake_write_research_bundle)
    runner.write_ranking_sma5_stateful_rotation_low_value_appendix_bundle(
        result,
        output_root=tmp_path,
        run_id="test-run",
    )

    assert tuple(captured["result_tables"]) == (
        "stateful_rotation_summary_df",
        "stateful_rotation_annual_df",
        "stateful_rotation_exit_reason_df",
        "stateful_rotation_decision_df",
        "stateful_rotation_event_df",
        "coverage_diagnostics_df",
    )
    assert captured["result_metadata"] == {
        "execution_policy": "same_close_one_hop_stateful_rotation",
        "execution_is_optimistic": True,
        "research_status": "exploratory_appendix",
        "long_hybrid_min": 0.7,
        "value_max_by_ring": {
            "low_value_core": 0.2,
            "low_value_near1": 0.3,
            "low_value_near2": 0.4,
        },
        "holding_cap": 60,
        "cost_levels_bps": [0, 10, 20],
        "market_schema_version": 5,
        "stock_price_adjustment_mode": "provider_adjusted_v1",
    }
