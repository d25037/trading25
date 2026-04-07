from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pandas.testing as pdt

from src.domains.analytics.topix100_short_side_streak_353_scan import (
    TOPIX100_SHORT_SIDE_STREAK_353_SCAN_EXPERIMENT_ID,
    _build_published_summary_payload,
    _select_best_pair_row,
    _select_best_short_row,
    _select_user_hypothesis_row,
    get_topix100_short_side_streak_353_scan_bundle_path_for_run_id,
    get_topix100_short_side_streak_353_scan_latest_bundle_path,
    load_topix100_short_side_streak_353_scan_research_bundle,
    run_topix100_short_side_streak_353_scan_research,
    write_topix100_short_side_streak_353_scan_research_bundle,
)


def _build_stub_inputs() -> tuple[SimpleNamespace, SimpleNamespace]:
    validation_dates = ["2024-01-02", "2024-01-03"]
    discovery_dates = ["2024-01-04"]
    split_by_date = {
        **{date: "validation" for date in validation_dates},
        **{date: "discovery" for date in discovery_dates},
    }

    default_returns = {
        "validation": (0.0010, 0.0030, 0.0050),
        "discovery": (0.0008, 0.0025, 0.0045),
    }
    strong_long_returns = {
        "2024-01-02": (0.0040, 0.0100, 0.0120),
        "2024-01-03": (0.0060, 0.0300, 0.0350),
        "2024-01-04": (0.0050, 0.0120, 0.0160),
    }
    q8_high_short_returns = (-0.0050, -0.0150, -0.0100)
    q7_low_pair_returns = (-0.0030, -0.0100, -0.0080)
    q10_high_bear_bull_returns = (-0.0040, -0.0090, -0.0020)
    user_hypothesis_returns = (-0.0020, 0.0010, 0.0020)

    event_rows: list[dict[str, object]] = []
    state_rows: list[dict[str, object]] = []
    for date in [*validation_dates, *discovery_dates]:
        sample_split = split_by_date[date]
        for decile in range(1, 11):
            for position_key, position_suffix, volume_value in (
                ("high", "01", 2.0),
                ("low", "02", 0.5),
            ):
                code = f"{decile:02d}{position_suffix}"
                company_name = f"Stock {code}"
                price_value = 2.0 - decile * 0.1 - (0.0 if position_key == "high" else 0.01)
                returns = default_returns[sample_split]
                state_key = "long_bullish__short_bearish"
                state_label = "Long Bullish / Short Bearish"
                short_mode = "bearish"
                long_mode = "bullish"

                if decile == 10 and position_key == "low":
                    returns = strong_long_returns[date]
                    state_key = "long_bearish__short_bearish"
                    state_label = "Long Bearish / Short Bearish"
                    short_mode = "bearish"
                    long_mode = "bearish"
                elif decile == 8 and position_key == "high" and date == "2024-01-02":
                    returns = q8_high_short_returns
                    state_key = "long_bullish__short_bullish"
                    state_label = "Long Bullish / Short Bullish"
                    short_mode = "bullish"
                    long_mode = "bullish"
                elif decile == 7 and position_key == "low" and date == "2024-01-03":
                    returns = q7_low_pair_returns
                    state_key = "long_bullish__short_bullish"
                    state_label = "Long Bullish / Short Bullish"
                    short_mode = "bullish"
                    long_mode = "bullish"
                elif decile == 10 and position_key == "high" and date in validation_dates:
                    returns = q10_high_bear_bull_returns
                    state_key = "long_bearish__short_bullish"
                    state_label = "Long Bearish / Short Bullish"
                    short_mode = "bullish"
                    long_mode = "bearish"
                elif decile in (2, 3, 4) and position_key == "low" and date in validation_dates:
                    returns = user_hypothesis_returns
                    state_key = "long_bullish__short_bullish"
                    state_label = "Long Bullish / Short Bullish"
                    short_mode = "bullish"
                    long_mode = "bullish"

                event_rows.append(
                    {
                        "code": code,
                        "company_name": company_name,
                        "date": date,
                        "price_vs_sma_50_gap": price_value,
                        "volume_sma_5_20": volume_value,
                        "t_plus_1_return": returns[0],
                        "t_plus_5_return": returns[1],
                        "t_plus_10_return": returns[2],
                        "date_constituent_count": 20,
                    }
                )
                for horizon_days, future_return in zip((1, 5, 10), returns, strict=True):
                    state_rows.append(
                        {
                            "state_event_id": f"{code}:{date}:{horizon_days}",
                            "code": code,
                            "company_name": company_name,
                            "sample_split": sample_split,
                            "segment_id": int(code),
                            "segment_start_date": date,
                            "date": date,
                            "segment_return": future_return / 2.0,
                            "segment_day_count": 2,
                            "base_streak_mode": "bullish" if short_mode == "bullish" else "bearish",
                            "short_mode": short_mode,
                            "long_mode": long_mode,
                            "state_key": state_key,
                            "state_label": state_label,
                            "short_window_streaks": 3,
                            "long_window_streaks": 53,
                            "future_return": future_return,
                            "future_diff": future_return * 100.0,
                            "horizon_days": horizon_days,
                        }
                    )

    price_result = SimpleNamespace(
        source_mode="snapshot",
        source_detail="test",
        available_start_date="2024-01-02",
        available_end_date="2024-01-04",
        topix100_constituent_count=20,
        event_panel_df=pd.DataFrame(event_rows),
    )
    state_result = SimpleNamespace(
        state_horizon_event_df=pd.DataFrame(state_rows),
    )
    return price_result, state_result


def test_short_side_scan_identifies_best_short_and_pair(monkeypatch) -> None:
    price_result, state_result = _build_stub_inputs()
    monkeypatch.setattr(
        "src.domains.analytics.topix100_short_side_streak_353_scan.run_topix100_price_vs_sma_rank_future_close_research",
        lambda *args, **kwargs: price_result,
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_short_side_streak_353_scan.run_topix100_streak_353_transfer_research",
        lambda *args, **kwargs: state_result,
    )

    result = run_topix100_short_side_streak_353_scan_research(
        "/tmp/unused.duckdb",
        min_validation_date_count=1,
        min_pair_overlap_dates=1,
    )

    assert not result.short_candidate_scorecard_df.empty
    assert not result.pair_trade_scorecard_df.empty
    assert not result.validation_focus_matrix_df.empty
    assert not result.validation_bull_bull_adjacent_pair_df.empty

    best_short = _select_best_short_row(result.short_candidate_scorecard_df)
    assert best_short is not None
    assert str(best_short["band_label"]) == "Q8"
    assert str(best_short["volume_bucket"]) == "volume_high"
    assert str(best_short["state_key"]) == "long_bullish__short_bullish"

    best_pair = _select_best_pair_row(result.pair_trade_scorecard_df)
    assert best_pair is not None
    assert str(best_pair["band_label"]) == "Q7"
    assert str(best_pair["volume_bucket"]) == "volume_low"
    assert str(best_pair["state_key"]) == "long_bullish__short_bullish"

    user_hypothesis = _select_user_hypothesis_row(result.short_candidate_scorecard_df)
    assert user_hypothesis is not None
    assert int(user_hypothesis["primary_rank"]) > int(best_short["primary_rank"])
    assert float(user_hypothesis["avg_return_5d"]) > float(best_short["avg_return_5d"])
    high_adjacent = result.validation_bull_bull_adjacent_pair_df[
        result.validation_bull_bull_adjacent_pair_df["volume_bucket"] == "volume_high"
    ].iloc[0]
    low_adjacent = result.validation_bull_bull_adjacent_pair_df[
        result.validation_bull_bull_adjacent_pair_df["volume_bucket"] == "volume_low"
    ].iloc[0]
    assert str(high_adjacent["band_label"]) == "Q7-Q8"
    assert str(low_adjacent["band_label"]) == "Q6-Q7"


def test_short_side_scan_bundle_roundtrip(monkeypatch, tmp_path: Path) -> None:
    price_result, state_result = _build_stub_inputs()
    monkeypatch.setattr(
        "src.domains.analytics.topix100_short_side_streak_353_scan.run_topix100_price_vs_sma_rank_future_close_research",
        lambda *args, **kwargs: price_result,
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_short_side_streak_353_scan.run_topix100_streak_353_transfer_research",
        lambda *args, **kwargs: state_result,
    )

    result = run_topix100_short_side_streak_353_scan_research(
        "/tmp/unused.duckdb",
        min_validation_date_count=1,
        min_pair_overlap_dates=1,
    )
    bundle = write_topix100_short_side_streak_353_scan_research_bundle(
        result,
        output_root=tmp_path,
        run_id="20260406_170000_testabcd",
    )
    reloaded = load_topix100_short_side_streak_353_scan_research_bundle(bundle.bundle_dir)

    assert bundle.experiment_id == TOPIX100_SHORT_SIDE_STREAK_353_SCAN_EXPERIMENT_ID
    assert (
        get_topix100_short_side_streak_353_scan_bundle_path_for_run_id(
            bundle.run_id,
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_topix100_short_side_streak_353_scan_latest_bundle_path(output_root=tmp_path)
        == bundle.bundle_dir
    )
    pdt.assert_frame_equal(
        reloaded.short_candidate_scorecard_df,
        result.short_candidate_scorecard_df,
        check_dtype=False,
    )
    pdt.assert_frame_equal(
        reloaded.pair_trade_scorecard_df,
        result.pair_trade_scorecard_df,
        check_dtype=False,
    )
    pdt.assert_frame_equal(
        reloaded.validation_bull_bull_adjacent_pair_df,
        result.validation_bull_bull_adjacent_pair_df,
        check_dtype=False,
    )


def test_published_summary_calls_out_rejected_q2_q4_hypothesis(monkeypatch) -> None:
    price_result, state_result = _build_stub_inputs()
    monkeypatch.setattr(
        "src.domains.analytics.topix100_short_side_streak_353_scan.run_topix100_price_vs_sma_rank_future_close_research",
        lambda *args, **kwargs: price_result,
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_short_side_streak_353_scan.run_topix100_streak_353_transfer_research",
        lambda *args, **kwargs: state_result,
    )

    result = run_topix100_short_side_streak_353_scan_research(
        "/tmp/unused.duckdb",
        min_validation_date_count=1,
        min_pair_overlap_dates=1,
    )
    summary = _build_published_summary_payload(result)

    assert summary["title"] == "TOPIX100 Short Side Streak 3/53 Scan"
    assert "Q8 x Volume High x Long Bullish / Short Bullish" in summary["resultHeadline"]
    assert "Q7 x Volume Low x Long Bullish / Short Bullish" in summary["resultHeadline"]
    assert "Q2-Q4" in " ".join(summary["resultBullets"])
    assert "adjacent two-decile bands" in " ".join(summary["resultBullets"])
    assert any(
        highlight["name"] == "pair_trade_scorecard_df"
        for highlight in summary["tableHighlights"]
    )
    assert any(
        highlight["name"] == "validation_bull_bull_adjacent_pair_df"
        for highlight in summary["tableHighlights"]
    )
