from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.domains.analytics.ranking_sma5_score_ring_rotation_evidence import (
    DEFAULT_RING_IDS,
    build_rotation_evidence,
)


def _feature_fixture() -> pd.DataFrame:
    dates = pd.date_range("2024-01-01", periods=6, freq="D")
    rows: list[dict[str, object]] = []
    closes = {
        "1000": (100.0, 100.0, 100.0, 100.0, 100.0, 100.0),
        "2000": (100.0, 100.0, 100.0, 110.0, 110.0, 110.0),
        "3000": (100.0, 100.0, 100.0, 101.0, 101.0, 101.0),
        "4000": (100.0, 100.0, 100.0, 101.0, 101.0, 101.0),
        "5000": (100.0, 100.0, 100.0, 101.0, 101.0, 101.0),
    }
    trigger_features = {
        "1000": (1, 3, -1.0),
        "2000": (2, 0, 0.0),
        "3000": (1, 0, 0.0),
        "4000": (2, 3, 0.0),
        "5000": (2, 0, -1.0),
    }
    for code, code_closes in closes.items():
        for position, day in enumerate(dates):
            count, streak, deviation = (
                trigger_features[code] if position >= 2 else (2, 0, 0.0)
            )
            rows.append(
                {
                    "date": day,
                    "code": code,
                    "close": code_closes[position],
                    "topix_close": 100.0,
                    "value_composite_equal_score": (
                        0.9
                        if (code == "1000" and position >= 1) or position == 2
                        else 0.5
                    ),
                    "long_hybrid_leadership_score": (
                        0.9
                        if (code == "1000" and position >= 1) or position == 2
                        else 0.5
                    ),
                    "sma5": 100.0,
                    "sma5_above_count_5d": count,
                    "sma5_below_streak": streak,
                    "sma5_atr20_deviation": deviation,
                }
            )
    return pd.DataFrame(rows)


def test_rotation_uses_first_trigger_and_healthy_same_ring_target() -> None:
    result = build_rotation_evidence(
        _feature_fixture(),
        ring_ids=("core_high_high",),
    )

    events = result.rotation_event_df
    assert events["trigger_id"].tolist() == ["X4_atr20_below_le_neg1"]
    assert events["target_candidate_count"].tolist() == [1]
    assert events["target_codes"].tolist() == ["2000"]
    assert events["gross_paired_delta"].iloc[0] == pytest.approx(
        events["rotation_return"].iloc[0] - events["source_return"].iloc[0]
    )


def test_rotation_cost_is_charged_once_and_decision_uses_frozen_rules() -> None:
    result = build_rotation_evidence(_feature_fixture(), ring_ids=DEFAULT_RING_IDS)
    summary = result.rotation_summary_df
    x4_core = summary.loc[
        summary["ring_id"].eq("core_high_high")
        & summary["trigger_id"].eq("X4_atr20_below_le_neg1")
    ]

    gross = x4_core.loc[x4_core["fee_bps"].eq(0), "median_paired_delta"].iloc[0]
    net10 = x4_core.loc[x4_core["fee_bps"].eq(10), "median_paired_delta"].iloc[0]
    assert net10 == pytest.approx(gross - 0.001)

    decision = result.rotation_decision_df.set_index("trigger_id")
    assert decision.loc["X4_atr20_below_le_neg1", "decision"] == "rotation_candidate"
    assert decision.loc["X2_count_le_1", "decision"] == "insufficient_evidence"
    assert decision.loc["X3_below_streak_ge_3", "decision"] == "insufficient_evidence"


def test_missing_code_row_does_not_use_a_later_session_return() -> None:
    feature_df = _feature_fixture()
    feature_df = feature_df.loc[
        ~(
            feature_df["code"].eq("2000")
            & feature_df["date"].eq(pd.Timestamp("2024-01-04"))
        )
    ]

    result = build_rotation_evidence(feature_df, ring_ids=("core_high_high",))

    assert result.rotation_event_df.empty
    diagnostics = result.coverage_diagnostics_df
    x4 = diagnostics.loc[
        diagnostics["ring_id"].eq("core_high_high")
        & diagnostics["trigger_id"].eq("X4_atr20_below_le_neg1")
    ].iloc[0]
    assert x4["events_without_target"] == 1


def test_first_trigger_with_missing_outcome_does_not_advance_to_later_trigger() -> None:
    feature_df = _feature_fixture()
    feature_df = feature_df.loc[
        ~(
            feature_df["code"].eq("1000")
            & feature_df["date"].eq(pd.Timestamp("2024-01-04"))
        )
    ]

    result = build_rotation_evidence(feature_df, ring_ids=("core_high_high",))

    assert result.rotation_event_df.empty
    diagnostics = result.coverage_diagnostics_df
    x4 = diagnostics.loc[
        diagnostics["ring_id"].eq("core_high_high")
        & diagnostics["trigger_id"].eq("X4_atr20_below_le_neg1")
    ].iloc[0]
    assert x4["source_event_count"] == 1
    assert x4["source_outcome_count"] == 0


def test_bundle_contract_contains_exactly_five_tables(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from scripts.research import run_ranking_sma5_score_ring_rotation_evidence as runner

    result = build_rotation_evidence(_feature_fixture(), ring_ids=DEFAULT_RING_IDS)
    result.db_path = str(tmp_path / "market.duckdb")
    result.analysis_start_date = "2024-01-01"
    result.analysis_end_date = "2024-01-06"
    result.market_schema_version = 5
    result.stock_price_adjustment_mode = "provider_adjusted_v1"
    captured: dict[str, object] = {}

    def fake_write_research_bundle(**kwargs: object) -> object:
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(runner, "write_research_bundle", fake_write_research_bundle)

    runner.write_ranking_sma5_score_ring_rotation_bundle(
        result,
        output_root=tmp_path,
        run_id="test-run",
    )

    assert tuple(captured["result_tables"]) == (
        "rotation_summary_df",
        "rotation_annual_df",
        "rotation_decision_df",
        "coverage_diagnostics_df",
        "rotation_event_df",
    )
    metadata = captured["result_metadata"]
    assert metadata == {
        "execution_policy": "same_close_rotation_next_close_evaluation",
        "execution_is_optimistic": True,
        "primary_ring": "core_high_high",
        "holding_cap": 60,
        "cost_levels_bps": [0, 10, 20],
        "research_status": "exploratory",
        "market_schema_version": 5,
        "stock_price_adjustment_mode": "provider_adjusted_v1",
    }
