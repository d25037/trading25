from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pandas.testing as pdt

from src.domains.analytics.topix100_q10_bounce_streak_353_conditioning import (
    TOPIX100_Q10_BOUNCE_STREAK_353_CONDITIONING_EXPERIMENT_ID,
    _build_published_summary_payload,
    get_topix100_q10_bounce_streak_353_conditioning_bundle_path_for_run_id,
    get_topix100_q10_bounce_streak_353_conditioning_latest_bundle_path,
    load_topix100_q10_bounce_streak_353_conditioning_research_bundle,
    run_topix100_q10_bounce_streak_353_conditioning_research,
    write_topix100_q10_bounce_streak_353_conditioning_research_bundle,
)


def _build_stub_inputs() -> tuple[SimpleNamespace, SimpleNamespace]:
    state_map = [
        ("2024-01-02", "long_bullish__short_bullish", "Long Bullish / Short Bullish"),
        ("2024-01-03", "long_bullish__short_bearish", "Long Bullish / Short Bearish"),
        ("2024-01-04", "long_bearish__short_bullish", "Long Bearish / Short Bullish"),
        ("2024-01-05", "long_bearish__short_bearish", "Long Bearish / Short Bearish"),
    ]
    bucket_specs = [
        ("middle_volume_high", "Middle Volume High", "middle", "Middle", "volume_high", "Volume High"),
        ("middle_volume_low", "Middle Volume Low", "middle", "Middle", "volume_low", "Volume Low"),
        ("q10_volume_high", "Q10 Volume High", "q10", "Q10", "volume_high", "Volume High"),
        ("q10_volume_low", "Q10 Volume Low", "q10", "Q10", "volume_low", "Volume Low"),
    ]
    state_bucket_returns = {
        "long_bullish__short_bullish": {
            "middle_volume_high": -0.01,
            "middle_volume_low": 0.00,
            "q10_volume_high": -0.005,
            "q10_volume_low": 0.01,
        },
        "long_bullish__short_bearish": {
            "middle_volume_high": 0.00,
            "middle_volume_low": 0.01,
            "q10_volume_high": 0.015,
            "q10_volume_low": 0.02,
        },
        "long_bearish__short_bullish": {
            "middle_volume_high": 0.01,
            "middle_volume_low": 0.015,
            "q10_volume_high": 0.02,
            "q10_volume_low": 0.03,
        },
        "long_bearish__short_bearish": {
            "middle_volume_high": 0.02,
            "middle_volume_low": 0.03,
            "q10_volume_high": 0.04,
            "q10_volume_low": 0.07,
        },
    }

    q10_rows: list[dict[str, object]] = []
    state_rows: list[dict[str, object]] = []
    code_index = 0
    for date, state_key, state_label in state_map:
        long_mode = "bearish" if "long_bearish" in state_key else "bullish"
        short_mode = "bearish" if "short_bearish" in state_key else "bullish"
        for (
            combined_bucket,
            combined_bucket_label,
            price_bucket,
            price_bucket_label,
            volume_bucket,
            volume_bucket_label,
        ) in bucket_specs:
            code_index += 1
            code = f"{code_index:04d}"
            bucket_return = state_bucket_returns[state_key][combined_bucket]
            q10_rows.append(
                {
                    "date": date,
                    "code": code,
                    "company_name": f"Stock {code}",
                    "close": 100.0,
                    "volume": 1000,
                    "date_constituent_count": 4,
                    "price_feature": "price_vs_sma_50_gap",
                    "price_feature_label": "Price vs SMA50 Gap",
                    "volume_feature": "volume_sma_5_20",
                    "volume_feature_label": "Volume SMA 5 / 20",
                    "price_bucket": price_bucket,
                    "price_bucket_label": price_bucket_label,
                    "volume_bucket": volume_bucket,
                    "volume_bucket_label": volume_bucket_label,
                    "combined_bucket": combined_bucket,
                    "combined_bucket_label": combined_bucket_label,
                    "t_plus_1_close": 100.0 * (1.0 + bucket_return),
                    "t_plus_1_return": bucket_return,
                    "t_plus_5_close": 100.0 * (1.0 + bucket_return + 0.01),
                    "t_plus_5_return": bucket_return + 0.01,
                    "t_plus_10_close": 100.0 * (1.0 + bucket_return + 0.02),
                    "t_plus_10_return": bucket_return + 0.02,
                }
            )
            for horizon_days, future_return in (
                (1, bucket_return),
                (5, bucket_return + 0.01),
                (10, bucket_return + 0.02),
            ):
                state_rows.append(
                    {
                        "state_event_id": f"{code}:{date}",
                        "code": code,
                        "company_name": f"Stock {code}",
                        "sample_split": "validation",
                        "segment_id": code_index,
                        "segment_start_date": date,
                        "date": date,
                        "segment_return": bucket_return / 2.0,
                        "segment_day_count": 2,
                        "base_streak_mode": short_mode,
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

    q10_result = SimpleNamespace(
        base_result=SimpleNamespace(
            source_mode="snapshot",
            source_detail="test",
            available_start_date="2024-01-02",
            available_end_date="2024-01-05",
            topix100_constituent_count=16,
        ),
        q10_middle_volume_split_panel_df=pd.DataFrame(q10_rows),
    )
    state_result = SimpleNamespace(
        state_horizon_event_df=pd.DataFrame(state_rows),
    )
    return q10_result, state_result


def test_q10_bounce_streak_353_conditioning_returns_scorecard(
    monkeypatch,
) -> None:
    q10_result, state_result = _build_stub_inputs()

    monkeypatch.setattr(
        "src.domains.analytics.topix100_q10_bounce_streak_353_conditioning.run_topix100_price_vs_sma_q10_bounce_research",
        lambda *args, **kwargs: q10_result,
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_q10_bounce_streak_353_conditioning.run_topix100_streak_353_transfer_research",
        lambda *args, **kwargs: state_result,
    )

    result = run_topix100_q10_bounce_streak_353_conditioning_research(
        "/tmp/unused.duckdb",
        min_constituents_per_bucket_state_date=1,
    )

    assert result.short_window_streaks == 3
    assert result.long_window_streaks == 53
    assert result.covered_constituent_count == 16
    assert not result.state_bucket_summary_df.empty
    assert not result.state_hypothesis_df.empty
    assert not result.state_scorecard_df.empty
    assert "q10_low_pairwise_edge_mean" in result.state_scorecard_df.columns
    assert "q10_low_is_best_bucket" in result.state_scorecard_df.columns
    best_validation_row = result.state_scorecard_df[
        (result.state_scorecard_df["sample_split"] == "validation")
        & (result.state_scorecard_df["horizon_days"] == 1)
    ].sort_values("q10_low_vs_middle_high", ascending=False, kind="stable").iloc[0]
    assert str(best_validation_row["state_key"]) == "long_bearish__short_bearish"


def test_q10_bounce_streak_353_conditioning_bundle_roundtrip(
    monkeypatch,
    tmp_path: Path,
) -> None:
    q10_result, state_result = _build_stub_inputs()
    monkeypatch.setattr(
        "src.domains.analytics.topix100_q10_bounce_streak_353_conditioning.run_topix100_price_vs_sma_q10_bounce_research",
        lambda *args, **kwargs: q10_result,
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_q10_bounce_streak_353_conditioning.run_topix100_streak_353_transfer_research",
        lambda *args, **kwargs: state_result,
    )

    result = run_topix100_q10_bounce_streak_353_conditioning_research(
        "/tmp/unused.duckdb",
        min_constituents_per_bucket_state_date=1,
    )
    bundle = write_topix100_q10_bounce_streak_353_conditioning_research_bundle(
        result,
        output_root=tmp_path,
        run_id="20260406_140000_testabcd",
    )
    reloaded = load_topix100_q10_bounce_streak_353_conditioning_research_bundle(
        bundle.bundle_dir
    )

    assert (
        bundle.experiment_id
        == TOPIX100_Q10_BOUNCE_STREAK_353_CONDITIONING_EXPERIMENT_ID
    )
    assert (
        get_topix100_q10_bounce_streak_353_conditioning_bundle_path_for_run_id(
            bundle.run_id,
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_topix100_q10_bounce_streak_353_conditioning_latest_bundle_path(
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    pdt.assert_frame_equal(
        reloaded.state_scorecard_df,
        result.state_scorecard_df,
        check_dtype=False,
    )


def test_published_summary_emphasizes_conditioned_read(monkeypatch) -> None:
    q10_result, state_result = _build_stub_inputs()
    monkeypatch.setattr(
        "src.domains.analytics.topix100_q10_bounce_streak_353_conditioning.run_topix100_price_vs_sma_q10_bounce_research",
        lambda *args, **kwargs: q10_result,
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_q10_bounce_streak_353_conditioning.run_topix100_streak_353_transfer_research",
        lambda *args, **kwargs: state_result,
    )
    result = run_topix100_q10_bounce_streak_353_conditioning_research(
        "/tmp/unused.duckdb",
        min_constituents_per_bucket_state_date=1,
    )

    summary = _build_published_summary_payload(result)

    assert summary["title"] == "TOPIX100 Q10 Bounce x Streak 3/53 Conditioning"
    assert "fusion study" in summary["resultBullets"][0].lower()
    assert summary["selectedParameters"][2]["value"] == "3 streaks"
    assert "Long Bearish / Short Bearish" in summary["resultHeadline"]
    assert any("clear avoid state" in bullet.lower() for bullet in summary["resultBullets"])
