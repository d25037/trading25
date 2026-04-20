from __future__ import annotations

import pandas as pd
import pytest

from src.domains.analytics.stop_limit_buy_only_next_close_followthrough import (
    run_stop_limit_buy_only_next_close_followthrough_research,
)
from src.domains.analytics.stop_limit_daily_classification import (
    StopLimitDailyClassificationResult,
)


def _build_classification_result(event_df: pd.DataFrame) -> StopLimitDailyClassificationResult:
    empty_df = pd.DataFrame()
    return StopLimitDailyClassificationResult(
        db_path="test.duckdb",
        source_mode="live",
        source_detail="test",
        available_start_date="2026-01-01",
        available_end_date="2026-01-10",
        analysis_start_date="2026-01-01",
        analysis_end_date="2026-01-10",
        total_event_count=int(len(event_df)),
        total_directional_event_count=int(len(event_df)),
        total_outside_standard_band_count=0,
        unmapped_latest_market_event_count=0,
        jpx_reference_label="test",
        classification_note="test",
        continuation_note="test",
        candidate_strategy_note="test",
        limit_table_df=empty_df.copy(),
        event_df=event_df,
        summary_df=empty_df.copy(),
        continuation_summary_df=empty_df.copy(),
        candidate_trade_summary_df=empty_df.copy(),
        outside_standard_band_df=empty_df.copy(),
        outside_standard_band_summary_df=empty_df.copy(),
    )


def test_run_stop_limit_buy_only_next_close_followthrough_research(monkeypatch) -> None:
    event_df = pd.DataFrame(
        [
            {
                "date": "2026-01-02",
                "code": "1111",
                "market_name": "スタンダード",
                "limit_side": "stop_low",
                "intraday_state": "intraday_range",
                "close_limit_state": "stop_low",
                "next_date": "2026-01-03",
                "next_close": 100.0,
                "close_3d": 110.0,
                "close_5d": 120.0,
                "next_close_return": 0.02,
            },
            {
                "date": "2026-01-05",
                "code": "2222",
                "market_name": "スタンダード",
                "limit_side": "stop_low",
                "intraday_state": "intraday_range",
                "close_limit_state": "off_limit_close",
                "next_date": "2026-01-06",
                "next_close": 100.0,
                "close_3d": 95.0,
                "close_5d": 90.0,
                "next_close_return": -0.03,
            },
            {
                "date": "2026-01-07",
                "code": "3333",
                "market_name": "グロース",
                "limit_side": "stop_low",
                "intraday_state": "intraday_range",
                "close_limit_state": "stop_low",
                "next_date": "2026-01-08",
                "next_close": 50.0,
                "close_3d": 60.0,
                "close_5d": 65.0,
                "next_close_return": 0.04,
            },
            {
                "date": "2026-01-08",
                "code": "4444",
                "market_name": "プライム",
                "limit_side": "stop_low",
                "intraday_state": "intraday_range",
                "close_limit_state": "off_limit_close",
                "next_date": "2026-01-09",
                "next_close": 80.0,
                "close_3d": 84.0,
                "close_5d": 88.0,
                "next_close_return": 0.01,
            },
            {
                "date": "2026-01-08",
                "code": "5555",
                "market_name": "スタンダード",
                "limit_side": "stop_high",
                "intraday_state": "intraday_range",
                "close_limit_state": "stop_high",
                "next_date": "2026-01-09",
                "next_close": 80.0,
                "close_3d": 84.0,
                "close_5d": 88.0,
                "next_close_return": 0.01,
            },
        ]
    )
    classification_result = _build_classification_result(event_df)

    import src.domains.analytics.stop_limit_buy_only_next_close_followthrough as module

    monkeypatch.setattr(
        module,
        "run_stop_limit_daily_classification_research",
        lambda db_path, start_date=None, end_date=None: classification_result,
    )

    result = run_stop_limit_buy_only_next_close_followthrough_research("test.duckdb")

    assert result.filtered_event_count == 4
    assert result.plus_signal_count == 3
    assert result.minus_signal_count == 1

    signal_summary_df = result.signal_summary_df
    standard_plus = signal_summary_df[
        (signal_summary_df["market_name"] == "スタンダード")
        & (signal_summary_df["close_limit_state"] == "stop_low")
        & (signal_summary_df["next_close_sign"] == "plus")
    ].iloc[0]
    assert int(standard_plus["event_count"]) == 1
    assert float(standard_plus["mean_next_close_to_close_5d_return"]) == pytest.approx(0.2)
    assert float(standard_plus["win_rate_5d"]) == 1.0

    standard_minus = signal_summary_df[
        (signal_summary_df["market_name"] == "スタンダード")
        & (signal_summary_df["close_limit_state"] == "off_limit_close")
        & (signal_summary_df["next_close_sign"] == "minus")
    ].iloc[0]
    assert float(standard_minus["mean_next_close_to_close_3d_return"]) == pytest.approx(-0.05)
    assert float(standard_minus["win_rate_3d"]) == 0.0

    yearly_summary_df = result.yearly_summary_df
    assert set(yearly_summary_df["calendar_year"]) == {2026}

    entry_cohort_df = result.entry_cohort_df
    assert set(entry_cohort_df["entry_date"]) == {"2026-01-03", "2026-01-06", "2026-01-08", "2026-01-09"}

    cohort_portfolio_summary_df = result.cohort_portfolio_summary_df
    growth_plus = cohort_portfolio_summary_df[
        (cohort_portfolio_summary_df["market_name"] == "グロース")
        & (cohort_portfolio_summary_df["close_limit_state"] == "stop_low")
        & (cohort_portfolio_summary_df["next_close_sign"] == "plus")
        & (cohort_portfolio_summary_df["horizon_key"] == "next_close_to_close_5d")
    ].iloc[0]
    assert int(growth_plus["date_count"]) == 1
    assert float(growth_plus["mean_cohort_return"]) == pytest.approx(0.3)
    assert float(growth_plus["win_rate"]) == 1.0

    prime_plus = cohort_portfolio_summary_df[
        (cohort_portfolio_summary_df["market_name"] == "プライム")
        & (cohort_portfolio_summary_df["next_close_sign"] == "plus")
        & (cohort_portfolio_summary_df["horizon_key"] == "next_close_to_close_3d")
    ].iloc[0]
    assert int(prime_plus["total_signal_count"]) == 1
    assert float(prime_plus["median_cohort_return"]) == pytest.approx(0.05)
