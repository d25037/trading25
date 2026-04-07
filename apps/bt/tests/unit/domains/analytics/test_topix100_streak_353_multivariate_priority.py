from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pandas.testing as pdt

from src.domains.analytics.topix100_streak_353_multivariate_priority import (
    TOPIX100_STREAK_353_MULTIVARIATE_PRIORITY_EXPERIMENT_ID,
    _build_published_summary_payload,
    get_topix100_streak_353_multivariate_priority_bundle_path_for_run_id,
    get_topix100_streak_353_multivariate_priority_latest_bundle_path,
    load_topix100_streak_353_multivariate_priority_research_bundle,
    run_topix100_streak_353_multivariate_priority_research,
    write_topix100_streak_353_multivariate_priority_research_bundle,
)


def _build_stub_inputs() -> tuple[SimpleNamespace, SimpleNamespace]:
    discovery_dates = ["2024-01-02", "2024-01-03"]
    validation_dates = ["2024-01-04", "2024-01-05"]
    split_by_date = {
        **{date: "discovery" for date in discovery_dates},
        **{date: "validation" for date in validation_dates},
    }
    event_rows: list[dict[str, object]] = []
    state_rows: list[dict[str, object]] = []
    code_counter = 0
    for date in [*discovery_dates, *validation_dates]:
        sample_split = split_by_date[date]
        for decile_num in range(1, 11):
            base_price_value = 200.0 - decile_num * 10.0
            for position in range(1, 9):
                code_counter += 1
                code = f"{code_counter:04d}"
                company_name = f"Stock {code}"
                price_value = base_price_value - position * 0.1
                volume_bucket = "volume_high" if position <= 4 else "volume_low"
                volume_value = 100.0 - position if volume_bucket == "volume_high" else 10.0 - position
                if position in (1, 2):
                    long_mode = "bullish"
                    short_mode = "bullish"
                elif position in (3, 4):
                    long_mode = "bearish"
                    short_mode = "bullish"
                elif position in (5, 6):
                    long_mode = "bullish"
                    short_mode = "bearish"
                else:
                    long_mode = "bearish"
                    short_mode = "bearish"
                state_key = f"long_{long_mode}__short_{short_mode}"
                state_label = f"Long {long_mode.title()} / Short {short_mode.title()}"

                base_return = 0.002 * (decile_num - 5.5)
                base_return += 0.015 if volume_bucket == "volume_low" else -0.015
                base_return += 0.03 if short_mode == "bearish" else -0.03
                base_return += 0.008 if long_mode == "bearish" else -0.008

                horizon_returns = {
                    1: base_return,
                    5: base_return * 1.2,
                    10: base_return * 1.5,
                }

                event_rows.append(
                    {
                        "code": code,
                        "company_name": company_name,
                        "date": date,
                        "price_vs_sma_50_gap": price_value,
                        "volume_sma_5_20": volume_value,
                        "t_plus_1_return": horizon_returns[1],
                        "t_plus_5_return": horizon_returns[5],
                        "t_plus_10_return": horizon_returns[10],
                        "date_constituent_count": 80,
                    }
                )
                for horizon_days, future_return in horizon_returns.items():
                    state_rows.append(
                        {
                            "state_event_id": f"{code}:{date}:{horizon_days}",
                            "code": code,
                            "company_name": company_name,
                            "sample_split": sample_split,
                            "segment_id": code_counter,
                            "segment_start_date": date,
                            "date": date,
                            "segment_return": future_return / 2.0,
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

    price_result = SimpleNamespace(
        source_mode="snapshot",
        source_detail="test",
        available_start_date="2024-01-02",
        available_end_date="2024-01-05",
        topix100_constituent_count=80,
        event_panel_df=pd.DataFrame(event_rows),
    )
    state_result = SimpleNamespace(
        state_horizon_event_df=pd.DataFrame(state_rows),
    )
    return price_result, state_result


def test_multivariate_priority_returns_feature_rankings(monkeypatch) -> None:
    price_result, state_result = _build_stub_inputs()
    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_multivariate_priority.run_topix100_price_vs_sma_rank_future_close_research",
        lambda *args, **kwargs: price_result,
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_multivariate_priority.run_topix100_streak_353_transfer_research",
        lambda *args, **kwargs: state_result,
    )

    result = run_topix100_streak_353_multivariate_priority_research(
        "/tmp/unused.duckdb",
        min_discovery_date_count=1,
        min_validation_date_count=1,
    )

    assert result.short_window_streaks == 3
    assert result.long_window_streaks == 53
    assert not result.subset_rule_scorecard_df.empty
    assert not result.feature_priority_df.empty
    assert not result.feature_leave_one_out_df.empty
    assert not result.full_feature_setup_df.empty
    assert not result.validation_extreme_bucket_comparison_df.empty

    long_setup = result.full_feature_setup_df[
        result.full_feature_setup_df["side"] == "long"
    ].iloc[0]
    assert str(long_setup["bucket"]) == "Q10"
    assert str(long_setup["volume"]) == "volume_low"
    assert str(long_setup["short_mode"]) == "bearish"
    assert str(long_setup["long_mode"]) == "bearish"

    short_setup = result.full_feature_setup_df[
        result.full_feature_setup_df["side"] == "short"
    ].iloc[0]
    assert str(short_setup["bucket"]) == "Q1"
    assert str(short_setup["volume"]) == "volume_high"
    assert str(short_setup["short_mode"]) == "bullish"
    assert str(short_setup["long_mode"]) == "bullish"

    long_priority = result.feature_priority_df[
        result.feature_priority_df["side"] == "long"
    ].sort_values("priority_rank_primary", kind="stable")
    short_priority = result.feature_priority_df[
        result.feature_priority_df["side"] == "short"
    ].sort_values("priority_rank_primary", kind="stable")

    assert {
        str(value)
        for value in long_priority.head(2)["feature_name"].tolist()
    } == {"volume", "short_mode"}
    assert {
        str(value)
        for value in short_priority.head(2)["feature_name"].tolist()
    } == {"volume", "short_mode"}


def test_multivariate_priority_bundle_roundtrip(
    monkeypatch,
    tmp_path: Path,
) -> None:
    price_result, state_result = _build_stub_inputs()
    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_multivariate_priority.run_topix100_price_vs_sma_rank_future_close_research",
        lambda *args, **kwargs: price_result,
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_multivariate_priority.run_topix100_streak_353_transfer_research",
        lambda *args, **kwargs: state_result,
    )

    result = run_topix100_streak_353_multivariate_priority_research(
        "/tmp/unused.duckdb",
        min_discovery_date_count=1,
        min_validation_date_count=1,
    )
    bundle = write_topix100_streak_353_multivariate_priority_research_bundle(
        result,
        output_root=tmp_path,
        run_id="20260406_180000_testabcd",
    )
    reloaded = load_topix100_streak_353_multivariate_priority_research_bundle(
        bundle.bundle_dir
    )

    assert (
        bundle.experiment_id
        == TOPIX100_STREAK_353_MULTIVARIATE_PRIORITY_EXPERIMENT_ID
    )
    assert (
        get_topix100_streak_353_multivariate_priority_bundle_path_for_run_id(
            bundle.run_id,
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_topix100_streak_353_multivariate_priority_latest_bundle_path(
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    pdt.assert_frame_equal(
        reloaded.feature_priority_df,
        result.feature_priority_df,
        check_dtype=False,
    )
    pdt.assert_frame_equal(
        reloaded.full_feature_setup_df,
        result.full_feature_setup_df,
        check_dtype=False,
    )
    pdt.assert_frame_equal(
        reloaded.validation_extreme_bucket_comparison_df,
        result.validation_extreme_bucket_comparison_df,
        check_dtype=False,
    )


def test_published_summary_emphasizes_information_priority(monkeypatch) -> None:
    price_result, state_result = _build_stub_inputs()
    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_multivariate_priority.run_topix100_price_vs_sma_rank_future_close_research",
        lambda *args, **kwargs: price_result,
    )
    monkeypatch.setattr(
        "src.domains.analytics.topix100_streak_353_multivariate_priority.run_topix100_streak_353_transfer_research",
        lambda *args, **kwargs: state_result,
    )

    result = run_topix100_streak_353_multivariate_priority_research(
        "/tmp/unused.duckdb",
        min_discovery_date_count=1,
        min_validation_date_count=1,
    )
    summary = _build_published_summary_payload(result)

    assert summary["title"] == "TOPIX100 Streak 3/53 Multivariate Priority"
    assert "information-priority study" in summary["resultBullets"][0]
    assert any("Q10" in bullet and "Q1" in bullet for bullet in summary["resultBullets"])
    assert any(
        highlight["name"] == "feature_priority_df"
        for highlight in summary["tableHighlights"]
    )
    assert any(
        highlight["name"] == "validation_extreme_bucket_comparison_df"
        for highlight in summary["tableHighlights"]
    )
