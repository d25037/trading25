from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.domains.analytics.standard_value_pump_fade_portfolio_filter import (
    _refill_to_top_n,
    run_standard_value_pump_fade_portfolio_filter_from_frames,
    write_standard_value_pump_fade_portfolio_filter_bundle,
)


def test_refill_to_top_n_applies_filter_per_rebalance_period() -> None:
    frame = pd.DataFrame(
        [
            {
                "year": year,
                "rebalance_period": period,
                "entry_date": entry_date,
                "exit_date": exit_date,
                "selection_count": 100,
                "selection_rank": rank,
                "speculative_risk_score": risk_score,
            }
            for year, period, entry_date, exit_date in [
                (2024, "2024-01", "2024-01-04", "2024-03-29"),
                (2024, "2024-04", "2024-04-01", "2024-06-28"),
            ]
            for rank, risk_score in [(1, 3), (2, 1), (3, 2), (4, 1)]
        ]
    )

    result = _refill_to_top_n(
        frame,
        policy="exclude_risk_ge3",
        base_selection_count=2,
    )

    assert len(result) == 4
    assert result.groupby("rebalance_period").size().to_dict() == {
        "2024-01": 2,
        "2024-04": 2,
    }
    assert result.groupby("rebalance_period")["selection_rank"].apply(list).to_dict() == {
        "2024-01": [1, 2],
        "2024-04": [1, 2],
    }
    assert result.groupby("rebalance_period")["speculative_risk_score"].apply(list).to_dict() == {
        "2024-01": [1, 2],
        "2024-04": [1, 2],
    }


def _price_rows(code: str, *, pump: bool) -> list[dict[str, object]]:
    dates = pd.bdate_range("2023-01-02", periods=430)
    rows: list[dict[str, object]] = []
    for idx, date in enumerate(dates):
        close = 100.0 + idx * 0.02
        high = close * 1.01
        low = close * 0.99
        open_price = close * 0.995
        volume = 1000
        if pump and 180 <= idx <= 200:
            close = 100.0 + (idx - 180) * 5.0
            high = close * 1.20
            low = close * 0.85
            open_price = close * 0.90
            volume = 10000
        if pump and idx > 200:
            close = 130.0 - (idx - 200) * 0.20
            high = close * 1.03
            low = close * 0.98
            open_price = close * 1.01
        rows.append(
            {
                "code": code,
                "date": date.strftime("%Y-%m-%d"),
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
            }
        )
    return rows


def _selected_event_rows(*, selection_count: int) -> list[dict[str, object]]:
    codes = ["1000", "2000", "3000", "4000"][:selection_count]
    rows: list[dict[str, object]] = []
    for rank, code in enumerate(codes, start=1):
        rows.append(
            {
                "market_scope": "standard",
                "score_method": "prime_size_tilt",
                "score_method_label": "Prime size tilt",
                "liquidity_scenario": "adv10m",
                "liquidity_scenario_label": "ADV60 >= 10mn JPY",
                "breakout_policy": "breakout_additive",
                "breakout_policy_label": "Value score + recent breakout boost",
                "breakout_window": 120,
                "breakout_lookback_sessions": 20,
                "selection_count": selection_count,
                "eligible_count": 4,
                "selection_rank": rank,
                "composite_score": 1.0 - rank * 0.01,
                "value_composite_score": 0.9 - rank * 0.01,
                "event_id": f"{selection_count}-{code}",
                "year": 2024,
                "rebalance_period": "2024-05",
                "rebalance_months": 3,
                "code": code,
                "company_name": f"Company {code}",
                "market": "standard",
                "market_code": "0112",
                "sector_33_name": "Services",
                "entry_date": "2024-05-02",
                "signal_date": "2024-05-01",
                "exit_date": "2024-06-28",
                "entry_open": 100.0,
                "exit_close": 110.0,
                "event_return_pct": 10.0,
                "event_return_winsor_pct": 10.0,
                "low_pbr_score": 0.5,
                "small_market_cap_score": 0.5,
                "low_forward_per_score": 0.5,
                "pbr": 0.6,
                "market_cap_bil_jpy": 2.0 if code == "1000" else 20.0,
                "forward_per": 6.0,
                "avg_trading_value_60d_mil_jpy": 20.0 if code == "1000" else 80.0,
                "signal_trading_value_mil_jpy": 30.0,
                "signal_trading_value_ratio_20d": 1.0,
                "signal_return_20d": 0.05,
                "new_high_120d": False,
                "days_since_new_high_120d": 10,
                "close_to_prior_high_120d_pct": -5.0,
            }
        )
    return rows


def test_portfolio_filter_builds_daily_summary_with_refill(tmp_path: Path) -> None:
    selected_events = pd.DataFrame(
        [*_selected_event_rows(selection_count=2), *_selected_event_rows(selection_count=4)]
    )
    price_history = pd.DataFrame(
        [
            *_price_rows("1000", pump=True),
            *_price_rows("2000", pump=False),
            *_price_rows("3000", pump=False),
            *_price_rows("4000", pump=False),
        ]
    )

    result = run_standard_value_pump_fade_portfolio_filter_from_frames(
        db_path="/tmp/market.duckdb",
        selected_event_df=selected_events,
        price_history_df=price_history,
        base_selection_count=2,
        refill_pool_selection_count=4,
        filter_policies=("base", "exclude_risk_ge3", "exclude_large_month_high_fade"),
    )

    assert not result.diagnostic_event_df.empty
    assert not result.filtered_selected_event_df.empty
    assert not result.portfolio_daily_df.empty
    assert set(result.portfolio_summary_df["pump_fade_policy"]) == {
        "base",
        "exclude_risk_ge3",
        "exclude_large_month_high_fade",
    }
    assert set(result.portfolio_summary_df["refill_mode"]) == {
        "base",
        "drop_only",
        "refill_to_top_n",
    }

    bundle = write_standard_value_pump_fade_portfolio_filter_bundle(
        result,
        output_root=tmp_path,
        run_id="20260508_portfolio_filter_test",
    )
    assert bundle.results_db_path.exists()
    assert "Portfolio Summary" in bundle.summary_path.read_text(encoding="utf-8")
