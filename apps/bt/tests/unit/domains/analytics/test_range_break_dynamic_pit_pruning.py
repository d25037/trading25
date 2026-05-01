"""Tests for range-break dynamic PIT pruning research helpers."""

from __future__ import annotations

import pandas as pd

from src.domains.analytics.range_break_dynamic_pit_pruning import (
    _build_candidate_summary_df,
    _build_target_threshold_candidate,
    _normalize_trade_records,
)


def test_normalize_trade_records_maps_report_schema_to_research_schema() -> None:
    records = [
        {
            "Column": "1301",
            "Entry Timestamp": "2024-01-05T00:00:00",
            "Exit Timestamp": "2024-02-05T00:00:00",
            "Avg Entry Price": 1000.0,
            "Avg Exit Price": 1120.0,
            "PnL": 120.0,
            "Return": 0.12,
            "Direction": "Long",
            "Status": "Closed",
            "Position Id": 7,
        }
    ]

    frame = _normalize_trade_records(
        records,
        strategy_name="production/range_break_v15",
        dataset_name="primeExTopix500",
    )

    row = frame.iloc[0]
    assert row["symbol"] == "1301"
    assert row["trade_return_pct"] == 12.0
    assert row["holding_days"] == 31
    assert row["window_label"] == "full_dynamic_pit"


def test_build_target_threshold_candidate_uses_discovery_only_threshold() -> None:
    frame = pd.DataFrame(
        {
            "entry_date": pd.to_datetime(
                [
                    "2020-01-01",
                    "2020-01-02",
                    "2022-01-01",
                    "2022-01-02",
                ]
            ),
            "feature": [1.0, 2.0, 100.0, 200.0],
        }
    )

    candidate = _build_target_threshold_candidate(
        frame,
        feature_name="feature",
        direction="high",
        keep_ratio=0.5,
        discovery_end_date="2021-12-31",
    )

    assert candidate.threshold == 1.5
    assert candidate.mask.tolist() == [False, True, True, True]


def test_build_candidate_summary_marks_target_band_hit() -> None:
    frame = pd.DataFrame(
        {
            "candidate_a": [True, True, False, False],
            "candidate_b": [True, True, True, True],
            "trade_return_pct": [10.0, -5.0, 20.0, -10.0],
            "holding_days": [40.0, 40.0, 20.0, 20.0],
            "entry_date": pd.to_datetime(
                ["2020-01-01", "2020-01-02", "2022-01-01", "2022-01-02"]
            ),
            "dynamic_new_code": [False, True, False, True],
        }
    )

    summary = _build_candidate_summary_df(
        frame,
        candidates={
            "candidate_a": frame["candidate_a"],
            "candidate_b": frame["candidate_b"],
        },
        target_min_trades=2,
        target_max_trades=3,
        trading_day_count=100,
        severe_loss_threshold_pct=-10.0,
    )

    row_a = summary[summary["candidate_name"] == "candidate_a"].iloc[0]
    assert row_a["trade_count"] == 2
    assert bool(row_a["target_band_hit"]) is True
    assert row_a["approx_concurrent_positions"] == 0.8
