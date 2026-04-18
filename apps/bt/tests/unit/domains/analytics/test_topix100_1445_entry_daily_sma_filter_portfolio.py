from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.domains.analytics.topix100_1445_entry_daily_sma_filter_comparison import (
    Topix1001445EntryDailySmaFilterComparisonResult,
)
from src.domains.analytics.topix100_1445_entry_daily_sma_filter_portfolio import (
    TOPIX100_1445_ENTRY_DAILY_SMA_FILTER_PORTFOLIO_EXPERIMENT_ID,
    _build_single_series_portfolio_daily_df,
    get_topix100_1445_entry_daily_sma_filter_portfolio_bundle_path_for_run_id,
    get_topix100_1445_entry_daily_sma_filter_portfolio_latest_bundle_path,
    load_topix100_1445_entry_daily_sma_filter_portfolio_research_bundle,
    run_topix100_1445_entry_daily_sma_filter_portfolio_research,
    write_topix100_1445_entry_daily_sma_filter_portfolio_research_bundle,
)


def _build_fake_comparison_result() -> Topix1001445EntryDailySmaFilterComparisonResult:
    selected_trade_level_df = pd.DataFrame(
        [
            {
                "interval_minutes": 15,
                "signal_family": "previous_open_vs_open",
                "signal_label": "Previous open vs current open",
                "target_bucket_side": "low",
                "expected_selected_bucket_label": "Q1",
                "exit_label": "next_open",
                "exit_time_target": "next open",
                "subgroup_key": "all",
                "subgroup_label": "All selected names",
                "period_index": 1,
                "period_label": "P1",
                "period_start_date": "2024-01-04",
                "period_end_date": "2024-01-05",
                "date": "2024-01-04",
                "code": "1111",
                "signal_ratio": 0.50,
                "signal_bucket_label": "Q1",
                "market_regime_return": -0.01,
                "market_regime_bucket_key": "weak",
                "market_regime_bucket_label": "Weak",
                "current_entry_bucket_key": "losers",
                "current_entry_bucket_label": "Losers",
                "entry_price": 100.0,
                "entry_actual_time": "14:45",
                "exit_price": 101.0,
                "exit_actual_time": "09:00",
                "trade_return": 0.01,
            },
            {
                "interval_minutes": 15,
                "signal_family": "previous_open_vs_open",
                "signal_label": "Previous open vs current open",
                "target_bucket_side": "low",
                "expected_selected_bucket_label": "Q1",
                "exit_label": "next_open",
                "exit_time_target": "next open",
                "subgroup_key": "all",
                "subgroup_label": "All selected names",
                "period_index": 1,
                "period_label": "P1",
                "period_start_date": "2024-01-04",
                "period_end_date": "2024-01-05",
                "date": "2024-01-05",
                "code": "1111",
                "signal_ratio": 0.40,
                "signal_bucket_label": "Q1",
                "market_regime_return": -0.02,
                "market_regime_bucket_key": "weak",
                "market_regime_bucket_label": "Weak",
                "current_entry_bucket_key": "losers",
                "current_entry_bucket_label": "Losers",
                "entry_price": 100.0,
                "entry_actual_time": "14:45",
                "exit_price": 102.0,
                "exit_actual_time": "09:00",
                "trade_return": 0.02,
            },
        ]
    )
    sma_trade_level_df = pd.DataFrame(
        [
            {
                **selected_trade_level_df.iloc[0].to_dict(),
                "subgroup_key": "losers",
                "subgroup_label": "Losers",
                "sma_window": 50,
                "sma_label": "SMA50",
                "daily_sma": 105.0,
                "entry_vs_sma_ratio": -0.0476,
                "sma_filter_state": "all",
                "sma_filter_label": "All",
            },
            {
                **selected_trade_level_df.iloc[1].to_dict(),
                "subgroup_key": "losers",
                "subgroup_label": "Losers",
                "sma_window": 50,
                "sma_label": "SMA50",
                "daily_sma": 104.0,
                "entry_vs_sma_ratio": -0.0385,
                "sma_filter_state": "all",
                "sma_filter_label": "All",
            },
            {
                **selected_trade_level_df.iloc[0].to_dict(),
                "subgroup_key": "losers",
                "subgroup_label": "Losers",
                "sma_window": 50,
                "sma_label": "SMA50",
                "daily_sma": 105.0,
                "entry_vs_sma_ratio": -0.0476,
                "sma_filter_state": "at_or_below",
                "sma_filter_label": "At or below daily SMA",
            },
        ]
    )
    empty_summary = pd.DataFrame()
    return Topix1001445EntryDailySmaFilterComparisonResult(
        db_path="/tmp/market.duckdb",
        source_mode="snapshot",
        source_detail="unit-test",
        available_start_date="2024-01-04",
        available_end_date="2024-01-05",
        analysis_start_date="2024-01-04",
        analysis_end_date="2024-01-05",
        interval_minutes=15,
        signal_family="previous_open_vs_open",
        exit_label="next_open",
        daily_sma_windows=(50,),
        bucket_count=4,
        period_months=6,
        entry_time="14:45",
        next_session_exit_time="10:30",
        tail_fraction=0.20,
        topix100_constituent_count=100,
        selected_trade_count=len(selected_trade_level_df),
        sma_trade_count=1,
        periods_df=pd.DataFrame(
            [
                {
                    "period_index": 1,
                    "period_label": "P1",
                    "period_start_date": "2024-01-04",
                    "period_end_date": "2024-01-05",
                }
            ]
        ),
        selected_trade_level_df=selected_trade_level_df,
        sma_trade_level_df=sma_trade_level_df,
        sma_filter_summary_df=empty_summary,
        sma_filter_comparison_df=empty_summary,
        period_sma_filter_summary_df=empty_summary,
    )


def test_build_single_series_portfolio_daily_df_includes_idle_days() -> None:
    calendar_df = pd.DataFrame(
        [
            {
                "date": "2024-01-04",
                "period_index": 1,
                "period_label": "P1",
                "period_start_date": "2024-01-04",
                "period_end_date": "2024-01-05",
            },
            {
                "date": "2024-01-05",
                "period_index": 1,
                "period_label": "P1",
                "period_start_date": "2024-01-04",
                "period_end_date": "2024-01-05",
            },
        ]
    )
    trade_df = pd.DataFrame(
        [
            {"date": "2024-01-04", "code": "1111", "trade_return": 0.02},
            {"date": "2024-01-04", "code": "2222", "trade_return": 0.00},
        ]
    )

    result_df = _build_single_series_portfolio_daily_df(
        calendar_df=calendar_df,
        trade_df=trade_df,
        series_name="branch_target",
        series_label="target",
    )

    assert list(result_df["active_trade_count"]) == [2, 0]
    assert list(result_df["portfolio_return"]) == [0.01, 0.0]
    assert list(result_df["active_day"]) == [True, False]
    assert result_df["equity_curve"].iloc[-1] == 1.01


def test_run_research_builds_portfolio_tables(monkeypatch) -> None:
    fake_result = _build_fake_comparison_result()
    monkeypatch.setattr(
        "src.domains.analytics.topix100_1445_entry_daily_sma_filter_portfolio.run_topix100_1445_entry_daily_sma_filter_comparison_research",
        lambda *args, **kwargs: fake_result,
    )

    result = run_topix100_1445_entry_daily_sma_filter_portfolio_research(
        "/tmp/market.duckdb",
        interval_minutes=15,
        signal_family="previous_open_vs_open",
        exit_label="next_open",
        market_regime_bucket_key="weak",
        subgroup_key="losers",
        sma_window=50,
        sma_filter_state="at_or_below",
        tail_fraction=0.20,
    )

    assert result.trading_day_count == 2
    assert result.all_branch_trade_count == 2
    assert result.target_branch_trade_count == 1
    assert result.all_active_day_count == 2
    assert result.target_active_day_count == 1
    assert set(result.portfolio_stats_df["series_name"]) == {
        "branch_all",
        "branch_target",
    }
    comparison_row = result.portfolio_comparison_df.iloc[0]
    assert comparison_row["target_minus_baseline_total_return"] < 0


def test_research_bundle_roundtrip(monkeypatch, tmp_path: Path) -> None:
    fake_result = _build_fake_comparison_result()
    monkeypatch.setattr(
        "src.domains.analytics.topix100_1445_entry_daily_sma_filter_portfolio.run_topix100_1445_entry_daily_sma_filter_comparison_research",
        lambda *args, **kwargs: fake_result,
    )

    result = run_topix100_1445_entry_daily_sma_filter_portfolio_research(
        "/tmp/market.duckdb",
        sma_window=50,
        sma_filter_state="at_or_below",
        tail_fraction=0.20,
    )
    bundle = write_topix100_1445_entry_daily_sma_filter_portfolio_research_bundle(
        result,
        output_root=tmp_path,
        run_id="20260419_130000_testabcd",
    )
    reloaded = load_topix100_1445_entry_daily_sma_filter_portfolio_research_bundle(
        bundle.bundle_dir
    )

    assert (
        bundle.experiment_id
        == TOPIX100_1445_ENTRY_DAILY_SMA_FILTER_PORTFOLIO_EXPERIMENT_ID
    )
    assert (
        get_topix100_1445_entry_daily_sma_filter_portfolio_bundle_path_for_run_id(
            "20260419_130000_testabcd",
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    assert (
        get_topix100_1445_entry_daily_sma_filter_portfolio_latest_bundle_path(
            output_root=tmp_path,
        )
        == bundle.bundle_dir
    )
    pd.testing.assert_frame_equal(
        reloaded.series_trade_level_df,
        result.series_trade_level_df,
        check_dtype=False,
    )
    pd.testing.assert_frame_equal(
        reloaded.portfolio_daily_df,
        result.portfolio_daily_df,
        check_dtype=False,
    )
    pd.testing.assert_frame_equal(
        reloaded.portfolio_stats_df,
        result.portfolio_stats_df,
        check_dtype=False,
    )
    pd.testing.assert_frame_equal(
        reloaded.period_portfolio_stats_df,
        result.period_portfolio_stats_df,
        check_dtype=False,
    )
    pd.testing.assert_frame_equal(
        reloaded.portfolio_comparison_df,
        result.portfolio_comparison_df,
        check_dtype=False,
    )
