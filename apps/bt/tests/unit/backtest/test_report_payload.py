"""Tests for extracted backtest report payload helpers."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.domains.backtest.core.report_payload import (
    build_backtest_report_payload,
    load_backtest_report_inputs,
    load_backtest_report_payload,
    load_legacy_simulation_inputs,
    write_backtest_report_payload,
)
from src.domains.backtest.core.simulation import BacktestSimulationResult


class _Trades:
    def __init__(self) -> None:
        self.records_readable = pd.DataFrame(
            [{"Entry": "2024-01-04", "Exit": "2024-01-05", "PnL": 1.2}]
        )

    def stats(self) -> pd.Series:
        return pd.Series({"Total Trades": 1, "Win Rate [%]": 100.0})


class _Portfolio:
    def __init__(self) -> None:
        index = pd.to_datetime(["2024-01-04", "2024-01-05"])
        self._value = pd.Series([100.0, 101.2], index=index)
        self._drawdown = pd.Series([0.0, -0.5], index=index)
        self._returns = pd.Series([0.0, 0.012], index=index)
        self._stats = pd.Series({"Total Return [%]": 1.2, "Sharpe Ratio": 1.1})
        self.trades = _Trades()

    def value(self) -> pd.Series:
        return self._value

    def drawdown(self) -> pd.Series:
        return self._drawdown

    def returns(self) -> pd.Series:
        return self._returns

    def annualized_volatility(self) -> float:
        return 0.2

    def sharpe_ratio(self) -> float:
        return 1.1

    def sortino_ratio(self) -> float:
        return 1.3

    def calmar_ratio(self) -> float:
        return 0.9

    def omega_ratio(self) -> float:
        return 1.05

    def stats(self) -> pd.Series:
        return self._stats


def test_report_payload_roundtrip_restores_render_context(tmp_path: Path) -> None:
    payload_path = tmp_path / "result.report.json"
    simulation_result = BacktestSimulationResult(
        initial_portfolio=_Portfolio(),
        kelly_portfolio=_Portfolio(),
        allocation_info=0.42,
        all_entries=pd.DataFrame(
            {
                "7203": [True, False],
                "6758": [False, True],
            },
            index=pd.to_datetime(["2024-01-04", "2024-01-05"]),
        ),
        summary_metrics=None,
        metrics_payload={},
    )

    payload = build_backtest_report_payload(simulation_result)
    write_backtest_report_payload(path=payload_path, payload=payload)
    context = load_backtest_report_payload(payload_path)

    assert context.initial_portfolio is not None
    assert list(context.initial_portfolio.value()) == [100.0, 101.2]
    assert context.kelly_portfolio is not None
    assert list(context.kelly_portfolio.returns()) == [0.0, 0.012]
    assert context.kelly_portfolio.trades.records_readable.iloc[0]["PnL"] == 1.2
    assert context.allocation_info == 0.42
    assert context.all_entries is not None
    assert context.all_entries["signal_count"].tolist() == [1, 1]


def test_load_backtest_report_inputs_returns_empty_values_for_corrupt_json(
    tmp_path: Path,
) -> None:
    payload_path = tmp_path / "broken.report.json"
    payload_path.write_text("{", encoding="utf-8")

    result = load_backtest_report_inputs(payload_path)

    assert result == (None, None, None, None)


def test_load_legacy_simulation_inputs_returns_empty_values_for_corrupt_pickle(
    tmp_path: Path,
) -> None:
    payload_path = tmp_path / "broken.simulation.pkl"
    payload_path.write_bytes(b"not-a-pickle")

    result = load_legacy_simulation_inputs(payload_path)

    assert result == (None, None, None, None)
