from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pandas.testing as pdt

from src.domains.analytics.topix100_strongest_setup_q10_threshold import (
    TOPIX100_STRONGEST_SETUP_Q10_THRESHOLD_EXPERIMENT_ID,
    _build_published_summary_payload,
    get_topix100_strongest_setup_q10_threshold_bundle_path_for_run_id,
    get_topix100_strongest_setup_q10_threshold_latest_bundle_path,
    load_topix100_strongest_setup_q10_threshold_research_bundle,
    run_topix100_strongest_setup_q10_threshold_research,
    write_topix100_strongest_setup_q10_threshold_research_bundle,
)


def _build_stub_inputs() -> tuple[SimpleNamespace, SimpleNamespace]:
    validation_dates = ["2024-01-02", "2024-01-03"]
    discovery_dates = ["2024-01-04"]
    split_by_date = {
        **{date: "validation" for date in validation_dates},
        **{date: "discovery" for date in discovery_dates},
    }
    strong_returns = {
        10: (0.0096, 0.0183, 0.0224),
        9: (0.0072, 0.0156, 0.0213),
        8: (0.0082, 0.0126, 0.0106),
        7: (0.0080, 0.0164, 0.0230),
        6: (0.0099, 0.0184, 0.0292),
        5: (0.0080, 0.0153, 0.0310),
        4: (0.0076, 0.0161, 0.0285),
        3: (0.0065, 0.0122, 0.0205),
        2: (0.0066, 0.0112, 0.0197),
        1: (0.0072, 0.0265, 0.0350),
    }
    reference_returns = (0.0090, 0.0122, 0.0182)
    weak_returns = (0.0010, 0.0020, 0.0040)

    event_rows: list[dict[str, object]] = []
    state_rows: list[dict[str, object]] = []
    for date in [*validation_dates, *discovery_dates]:
        sample_split = split_by_date[date]
        for decile in range(1, 11):
            for position in ("high", "low"):
                code = f"{decile:02d}{1 if position == 'high' else 2:02d}"
                company_name = f"Stock {code}"
                price_value = 2.0 - decile * 0.1 - (0.0 if position == "high" else 0.01)
                volume_value = 2.0 if position == "high" else 0.5
                if position == "low":
                    returns = strong_returns[decile]
                    state_key = "long_bearish__short_bearish"
                    state_label = "Long Bearish / Short Bearish"
                elif decile == 10:
                    returns = reference_returns
                    state_key = "long_bearish__short_bearish"
                    state_label = "Long Bearish / Short Bearish"
                else:
                    returns = weak_returns
                    state_key = "long_bullish__short_bearish"
                    state_label = "Long Bullish / Short Bearish"

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
                            "base_streak_mode": "bearish",
                            "short_mode": "bearish",
                            "long_mode": "bearish"
                            if state_key == "long_bearish__short_bearish"
                            else "bullish",
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


def test_strongest_setup_q10_threshold_returns_band_scorecard(monkeypatch) -> None:
    price_result, state_result = _build_stub_inputs()
    monkeypatch.setattr(
        "src.domains.analytics.topix100_strongest_setup_q10_threshold.run_topix100_price_vs_sma_rank_future_close_research",
        lambda *args, **kwargs: price_result,
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_strongest_setup_q10_threshold.run_topix100_streak_353_transfer_research",
        lambda *args, **kwargs: state_result,
    )

    result = run_topix100_strongest_setup_q10_threshold_research(
        "/tmp/unused.duckdb",
    )

    assert result.short_window_streaks == 3
    assert result.long_window_streaks == 53
    assert not result.strongest_setup_decile_summary_df.empty
    assert not result.strongest_setup_lower_tail_band_summary_df.empty
    assert not result.q10_non_strong_reference_df.empty
    assert not result.band_vs_q10_reference_scorecard_df.empty

    validation_reference = result.q10_non_strong_reference_df[
        result.q10_non_strong_reference_df["sample_split"] == "validation"
    ].iloc[0]
    assert str(validation_reference["volume_bucket"]) == "volume_high"
    assert str(validation_reference["state_key"]) == "long_bearish__short_bearish"

    q7_band = result.band_vs_q10_reference_scorecard_df[
        (result.band_vs_q10_reference_scorecard_df["sample_split"] == "validation")
        & (result.band_vs_q10_reference_scorecard_df["band_label"] == "Q7-Q10")
    ].iloc[0]
    assert bool(q7_band["beats_reference_5d_10d"]) is True


def test_strongest_setup_q10_threshold_bundle_roundtrip(
    monkeypatch,
    tmp_path: Path,
) -> None:
    price_result, state_result = _build_stub_inputs()
    monkeypatch.setattr(
        "src.domains.analytics.topix100_strongest_setup_q10_threshold.run_topix100_price_vs_sma_rank_future_close_research",
        lambda *args, **kwargs: price_result,
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_strongest_setup_q10_threshold.run_topix100_streak_353_transfer_research",
        lambda *args, **kwargs: state_result,
    )

    result = run_topix100_strongest_setup_q10_threshold_research(
        "/tmp/unused.duckdb",
    )
    bundle = write_topix100_strongest_setup_q10_threshold_research_bundle(
        result,
        output_root=tmp_path,
        run_id="20260406_160000_testabcd",
    )
    reloaded = load_topix100_strongest_setup_q10_threshold_research_bundle(bundle.bundle_dir)

    assert (
        bundle.experiment_id
        == TOPIX100_STRONGEST_SETUP_Q10_THRESHOLD_EXPERIMENT_ID
    )
    assert (
        get_topix100_strongest_setup_q10_threshold_bundle_path_for_run_id(
            bundle.run_id,
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_topix100_strongest_setup_q10_threshold_latest_bundle_path(
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    pdt.assert_frame_equal(
        reloaded.band_vs_q10_reference_scorecard_df,
        result.band_vs_q10_reference_scorecard_df,
        check_dtype=False,
    )


def test_published_summary_emphasizes_setup_over_raw_q10(monkeypatch) -> None:
    price_result, state_result = _build_stub_inputs()
    monkeypatch.setattr(
        "src.domains.analytics.topix100_strongest_setup_q10_threshold.run_topix100_price_vs_sma_rank_future_close_research",
        lambda *args, **kwargs: price_result,
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_strongest_setup_q10_threshold.run_topix100_streak_353_transfer_research",
        lambda *args, **kwargs: state_result,
    )

    result = run_topix100_strongest_setup_q10_threshold_research(
        "/tmp/unused.duckdb",
    )
    summary = _build_published_summary_payload(result)

    assert summary["title"] == "TOPIX100 Strongest Setup vs Q10 Threshold"
    assert "raw `Q10` membership" in summary["resultBullets"][0]
    assert "Q9-Q10" in " ".join(summary["resultBullets"])
    assert "Q7-Q10" in " ".join(summary["resultBullets"])
    assert any(
        highlight["name"] == "band_vs_q10_reference_scorecard_df"
        for highlight in summary["tableHighlights"]
    )
