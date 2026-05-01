"""Tests for single-name trade-quality audit helpers."""

from __future__ import annotations

import pandas as pd

from src.domains.analytics import production_strategy_single_name_trade_quality as trade_quality
from src.domains.analytics.production_strategy_single_name_trade_quality import (
    _build_per_symbol_summary_df,
    _build_scenario_summary_row,
    _day_before_iso,
    _resolve_window_stock_codes,
    _subtract_months_iso,
)
from src.domains.analytics.window_warmup import (
    estimate_strategy_indicator_warmup_calendar_days,
    resolve_window_load_start_date,
)


def test_subtract_months_iso_keeps_calendar_day_when_possible() -> None:
    assert _subtract_months_iso("2026-03-24", 6) == "2025-09-24"


def test_day_before_iso_returns_previous_calendar_day() -> None:
    assert _day_before_iso("2025-09-24") == "2025-09-23"


def test_build_scenario_summary_row_computes_trade_quality_metrics() -> None:
    trade_rows = [
        {
            "symbol": "1111",
            "trade_return_pct": 10.0,
            "pnl": 1000.0,
            "holding_days": 5.0,
        },
        {
            "symbol": "2222",
            "trade_return_pct": -5.0,
            "pnl": -500.0,
            "holding_days": 3.0,
        },
        {
            "symbol": "1111",
            "trade_return_pct": 20.0,
            "pnl": 2000.0,
            "holding_days": 7.0,
        },
    ]

    row = _build_scenario_summary_row(
        strategy_name="production/demo",
        dataset_info={"dataset_name": "sample", "dataset_preset": "sample"},
        window_label="holdout_6m",
        window_start_date="2025-09-24",
        window_end_date="2026-03-24",
        runtime_seconds=12.5,
        trade_rows=trade_rows,
        error=None,
    )

    assert row["status"] == "ok"
    assert row["trade_count"] == 3
    assert row["traded_symbol_count"] == 2
    assert row["trades_per_symbol"] == 1.5
    assert row["win_rate_pct"] == 66.66666666666666
    assert row["avg_trade_return_pct"] == 8.333333333333334
    assert row["profit_factor"] == 6.0
    assert row["avg_holding_days"] == 5.0


def test_build_per_symbol_summary_df_aggregates_by_symbol() -> None:
    trade_ledger_df = pd.DataFrame(
        [
            {
                "strategy_name": "production/demo",
                "strategy_basename": "demo",
                "dataset_name": "sample",
                "dataset_preset": "sample",
                "window_label": "holdout_6m",
                "symbol": "1111",
                "trade_return_pct": 10.0,
                "pnl": 1000.0,
                "holding_days": 5.0,
                "entry_timestamp": "2025-10-01T00:00:00",
                "exit_timestamp": "2025-10-06T00:00:00",
            },
            {
                "strategy_name": "production/demo",
                "strategy_basename": "demo",
                "dataset_name": "sample",
                "dataset_preset": "sample",
                "window_label": "holdout_6m",
                "symbol": "1111",
                "trade_return_pct": -5.0,
                "pnl": -500.0,
                "holding_days": 2.0,
                "entry_timestamp": "2025-11-01T00:00:00",
                "exit_timestamp": "2025-11-03T00:00:00",
            },
            {
                "strategy_name": "production/demo",
                "strategy_basename": "demo",
                "dataset_name": "sample",
                "dataset_preset": "sample",
                "window_label": "holdout_6m",
                "symbol": "2222",
                "trade_return_pct": 3.0,
                "pnl": 300.0,
                "holding_days": 1.0,
                "entry_timestamp": "2025-12-01T00:00:00",
                "exit_timestamp": "2025-12-02T00:00:00",
            },
        ]
    )

    summary_df = _build_per_symbol_summary_df(trade_ledger_df)
    row_1111 = summary_df[summary_df["symbol"] == "1111"].iloc[0]

    assert row_1111["trade_count"] == 2
    assert row_1111["win_rate_pct"] == 50.0
    assert row_1111["avg_trade_return_pct"] == 2.5
    assert row_1111["total_pnl"] == 500.0


def test_indicator_warmup_uses_largest_lookback_parameter() -> None:
    warmup_days = estimate_strategy_indicator_warmup_calendar_days(
        {
            "entry_filter_params": {
                "volume_ratio_above": {
                    "short_period": 50,
                    "long_period": 150,
                },
                "risk_adjusted_return": {"lookback_period": 60},
            }
        }
    )

    assert warmup_days == 230


def test_resolve_window_load_start_date_clamps_to_dataset_start() -> None:
    assert resolve_window_load_start_date(
        dataset_start_date="2025-01-01",
        window_start_date="2025-02-01",
        warmup_calendar_days=230,
    ) == "2025-01-01"


def test_resolve_window_stock_codes_uses_window_range_for_market_universe(
    monkeypatch,
) -> None:
    captured: dict[str, object] = {}

    def _fake_resolve_backtest_universe_codes(shared_config: dict[str, object]) -> list[str]:
        captured.update(shared_config)
        return ["1301", "1400"]

    monkeypatch.setattr(
        trade_quality,
        "resolve_backtest_universe_codes",
        _fake_resolve_backtest_universe_codes,
    )

    codes = _resolve_window_stock_codes(
        dataset_info={"dataset_name": "prime", "dataset_preset": "prime"},
        window={
            "window_label": "holdout",
            "window_start_date": "2024-01-05",
            "window_end_date": "2024-01-06",
        },
        snapshot_stock_codes={},
    )

    assert codes == ["1301", "1400"]
    assert captured["start_date"] == "2024-01-05"
    assert captured["end_date"] == "2024-01-06"
