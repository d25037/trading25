from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.domains.analytics.ranking_sma5_score_ring_stateful_rotation_evidence import (
    _aggregate_evidence,
    build_stateful_rotation_evidence,
)


def _feature_fixture(
    *,
    periods: int = 8,
    target_exit: tuple[int, str] | None = (5, "x4"),
) -> pd.DataFrame:
    dates = pd.date_range("2024-01-01", periods=periods, freq="D")
    rows: list[dict[str, object]] = []
    closes = {
        "1000": [100.0 + position for position in range(periods)],
        "2000": [100.0 + 2 * position for position in range(periods)],
    }
    for code, code_closes in closes.items():
        for position, day in enumerate(dates):
            in_ring = position >= (1 if code == "1000" else 2)
            count = 2
            streak = 0
            deviation = 0.0
            if code == "1000" and position >= 2:
                count = 1
            if code == "2000" and target_exit and position == target_exit[0]:
                reason = target_exit[1]
                if reason == "x2":
                    count = 1
                elif reason == "x3":
                    streak = 3
                elif reason == "x4":
                    deviation = -1.0
                elif reason == "ring_exit":
                    in_ring = False
            rows.append(
                {
                    "date": day,
                    "code": code,
                    "close": code_closes[position],
                    "topix_close": 100.0,
                    "value_composite_equal_score": 0.9 if in_ring else 0.5,
                    "long_hybrid_leadership_score": 0.9 if in_ring else 0.5,
                    "sma5": 100.0,
                    "sma5_above_count_5d": count,
                    "sma5_below_streak": streak,
                    "sma5_atr20_deviation": deviation,
                }
            )
    return pd.DataFrame(rows)


def test_target_episode_stops_at_first_trigger_and_matches_source_horizon() -> None:
    result = build_stateful_rotation_evidence(
        _feature_fixture(),
        ring_ids=("core_high_high",),
    )

    event = result.stateful_rotation_event_df.iloc[0]
    assert event["source_trigger_id"] == "X2_count_le_1"
    assert event["median_holding_sessions"] == 3
    assert event["gross_event_paired_delta"] == pytest.approx(
        event["mean_target_cumulative_return"]
        - event["mean_matched_source_cumulative_return"]
    )
    reasons = result.stateful_rotation_exit_reason_df
    assert reasons["target_exit_reason"].tolist() == ["X4_atr20_below_le_neg1"]


@pytest.mark.parametrize(
    ("target_exit", "periods", "holding_cap", "expected_reason"),
    [
        ((4, "ring_exit"), 8, 60, "ring_exit"),
        (None, 8, 3, "holding_cap"),
        (None, 8, 60, "terminal_exit"),
    ],
)
def test_target_episode_uses_non_trigger_exit_boundaries(
    target_exit: tuple[int, str] | None,
    periods: int,
    holding_cap: int,
    expected_reason: str,
) -> None:
    result = build_stateful_rotation_evidence(
        _feature_fixture(periods=periods, target_exit=target_exit),
        ring_ids=("core_high_high",),
        holding_cap=holding_cap,
    )

    reasons = result.stateful_rotation_exit_reason_df
    assert reasons.iloc[0]["target_exit_reason"] == expected_reason


def test_missing_global_session_row_invalidates_target_episode() -> None:
    feature_df = _feature_fixture()
    feature_df = feature_df.loc[
        ~(
            feature_df["code"].eq("2000")
            & feature_df["date"].eq(pd.Timestamp("2024-01-04"))
        )
    ]

    result = build_stateful_rotation_evidence(
        feature_df,
        ring_ids=("core_high_high",),
    )

    assert result.stateful_rotation_event_df.empty
    diagnostics = result.coverage_diagnostics_df.iloc[0]
    assert diagnostics["invalid_target_episode_count"] == 1


def test_cost_is_applied_once_after_equal_weight_event_aggregation() -> None:
    result = build_stateful_rotation_evidence(
        _feature_fixture(),
        ring_ids=("core_high_high",),
    )
    summary = result.stateful_rotation_summary_df
    gross = summary.query("fee_bps == 0").iloc[0]
    net10 = summary.query("fee_bps == 10").iloc[0]

    assert net10["median_event_paired_delta"] == pytest.approx(
        gross["median_event_paired_delta"] - 0.001
    )
    event = result.stateful_rotation_event_df.iloc[0]
    assert event["gross_event_paired_delta"] == pytest.approx(
        event["mean_target_cumulative_return"]
        - event["mean_matched_source_cumulative_return"]
    )


def test_summary_weights_source_events_not_their_target_counts() -> None:
    events = pd.DataFrame(
        {
            "ring_id": ["core_high_high", "core_high_high"],
            "source_trigger_id": ["X2_count_le_1", "X2_count_le_1"],
            "year": [2024, 2024],
            "target_count": [1, 9],
            "median_holding_sessions": [2.0, 5.0],
            "mean_matched_source_cumulative_return": [0.0, 0.0],
            "mean_target_cumulative_return": [0.10, -0.02],
            "gross_event_paired_delta": [0.10, -0.02],
        }
    )

    summary, _ = _aggregate_evidence(events, (0,))

    assert summary.iloc[0]["mean_event_paired_delta"] == pytest.approx(0.04)


def test_trigger_precedence_is_x4_then_x3_then_x2() -> None:
    feature_df = _feature_fixture(target_exit=(5, "x4"))
    mask = feature_df["code"].eq("2000") & feature_df["date"].eq(
        pd.Timestamp("2024-01-06")
    )
    feature_df.loc[mask, ["sma5_above_count_5d", "sma5_below_streak"]] = [1, 3]

    result = build_stateful_rotation_evidence(
        feature_df,
        ring_ids=("core_high_high",),
    )

    assert result.stateful_rotation_exit_reason_df.iloc[0][
        "target_exit_reason"
    ] == "X4_atr20_below_le_neg1"


def test_bundle_contract_contains_exactly_six_tables(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from scripts.research import (
        run_ranking_sma5_score_ring_stateful_rotation_evidence as runner,
    )

    result = build_stateful_rotation_evidence(
        _feature_fixture(),
        ring_ids=("core_high_high",),
    )
    result.db_path = str(tmp_path / "market.duckdb")
    result.analysis_start_date = "2024-01-01"
    result.analysis_end_date = "2024-01-08"
    result.market_schema_version = 5
    result.stock_price_adjustment_mode = "provider_adjusted_v1"
    captured: dict[str, object] = {}

    def fake_write_research_bundle(**kwargs: object) -> object:
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(runner, "write_research_bundle", fake_write_research_bundle)
    runner.write_ranking_sma5_score_ring_stateful_rotation_bundle(
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
        "primary_ring": "core_high_high",
        "holding_cap": 60,
        "cost_levels_bps": [0, 10, 20],
        "research_status": "exploratory",
        "market_schema_version": 5,
        "stock_price_adjustment_mode": "provider_adjusted_v1",
    }
