"""Tests for backtest report payload serialization."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.domains.backtest.core import report_payload as report_payload_module
from src.domains.backtest.core.report_payload import (
    _coerce_float,
    _coerce_series_value,
    _deserialize_allocation_info,
    _deserialize_entry_counts,
    _deserialize_portfolio,
    _deserialize_series,
    _deserialize_stats,
    _serialize_allocation_info,
    _serialize_dataframe,
    _serialize_entry_counts,
    _serialize_metric_map,
    _serialize_portfolio,
    _serialize_series,
    _serialize_stats,
    build_backtest_report_payload,
    load_backtest_report_payload,
    write_backtest_report_payload,
)
from src.domains.backtest.core.simulation import BacktestSimulationResult
from src.shared.models.allocation import AllocationInfo


class _FakeTrades:
    def __init__(self) -> None:
        self.records_readable = pd.DataFrame(
            [
                {
                    "Column": "7203",
                    "Entry Timestamp": "2024-01-04",
                    "Exit Timestamp": "2024-01-05",
                    "Return": 1.2,
                    "PnL": 1200.0,
                }
            ]
        )

    def stats(self) -> pd.Series:
        return pd.Series({"Win Rate [%]": 55.0, "Total Trades": 1})


class _FakePortfolio:
    def __init__(self) -> None:
        index = pd.date_range("2024-01-01", periods=3, freq="D")
        self._value = pd.Series([100.0, 101.0, 103.0], index=index)
        self._drawdown = pd.Series([0.0, -0.5, -0.2], index=index)
        self._returns = pd.Series([0.0, 0.01, 0.02], index=index)
        self.trades = _FakeTrades()

    def value(self) -> pd.Series:
        return self._value

    def drawdown(self) -> pd.Series:
        return self._drawdown

    def returns(self) -> pd.Series:
        return self._returns

    def annualized_volatility(self) -> float:
        return 0.12

    def sharpe_ratio(self) -> float:
        return 1.1

    def sortino_ratio(self) -> float:
        return 1.4

    def calmar_ratio(self) -> float:
        return 0.8

    def omega_ratio(self) -> float:
        return 1.2

    def stats(self) -> pd.Series:
        return pd.Series({"Total Return [%]": 3.0, "Sharpe Ratio": 1.1})


def test_report_payload_round_trip(tmp_path: Path) -> None:
    allocation_info = AllocationInfo(
        allocation=0.25,
        win_rate=0.55,
        avg_win=1.2,
        avg_loss=0.8,
        total_trades=12,
        full_kelly=0.5,
        kelly_fraction=0.5,
    )
    all_entries = pd.DataFrame(
        {
            "7203": [True, False, True],
            "6758": [False, True, False],
        },
        index=pd.date_range("2024-01-01", periods=3, freq="D"),
    )
    simulation_result = BacktestSimulationResult(
        initial_portfolio=_FakePortfolio(),
        kelly_portfolio=_FakePortfolio(),
        allocation_info=allocation_info,
        all_entries=all_entries,
        summary_metrics=None,
        metrics_payload={"total_return": 3.0},
    )

    payload = build_backtest_report_payload(simulation_result)
    payload_path = write_backtest_report_payload(
        path=tmp_path / "result.report.json",
        payload=payload,
    )

    context = load_backtest_report_payload(payload_path)

    assert context.initial_portfolio is not None
    assert context.kelly_portfolio is not None
    assert context.all_entries is not None
    assert list(context.all_entries["signal_count"]) == [1, 1, 1]
    assert context.kelly_portfolio.value().iloc[-1] == 103.0
    assert context.kelly_portfolio.drawdown().iloc[-1] == -0.2
    assert context.kelly_portfolio.returns().iloc[-1] == 0.02
    assert context.kelly_portfolio.annualized_volatility() == 0.12
    assert context.kelly_portfolio.sharpe_ratio() == 1.1
    assert context.kelly_portfolio.sortino_ratio() == 1.4
    assert context.kelly_portfolio.calmar_ratio() == 0.8
    assert context.kelly_portfolio.omega_ratio() == 1.2
    assert context.kelly_portfolio.stats()["Sharpe Ratio"] == 1.1
    assert context.kelly_portfolio.trades.stats()["Total Trades"] == 1
    assert len(context.kelly_portfolio.trades.records_readable) == 1
    assert context.allocation_info is not None
    assert context.allocation_info.allocation == 0.25


def test_series_and_dataframe_helpers_cover_edge_cases() -> None:
    class _IsoformatError:
        def isoformat(self) -> str:
            raise RuntimeError("boom")

    ts = pd.Timestamp("2024-01-01")
    bad_iso = _IsoformatError()
    fallback_object = object()
    multi_col = pd.DataFrame({"a": [1], "b": [2]})

    assert _coerce_float(None) is None
    assert _coerce_series_value(ts) == ts.isoformat()
    assert _coerce_series_value(bad_iso) is bad_iso
    assert _coerce_series_value(fallback_object) == str(fallback_object)
    assert _serialize_series(None) is None
    assert _serialize_series(multi_col) is None
    assert _serialize_series(pd.DataFrame({"value": [1, 2]})) == {
        "index": [0, 1],
        "values": [1, 2],
    }
    assert _serialize_series([1, 2]) == {"index": [0, 1], "values": [1, 2]}
    assert _deserialize_series(None).empty
    assert _serialize_dataframe(pd.Series([1, 2], index=["a", "b"])) == [
        {"value": 1},
        {"value": 2},
    ]
    assert _serialize_dataframe(None) == []


def test_stats_and_entry_count_helpers_cover_fallback_paths() -> None:
    stats_series = pd.Series({"Sharpe Ratio": 1.2, "Total Return [%]": 5.0})
    generic_stats = [{"metric": "Win Rate [%]", "value": 55.0}]
    generic_records = [{"name": "Sharpe Ratio", "score": 1.2}]
    frame_stats = pd.DataFrame({"metric": ["Sharpe Ratio"], "value": [1.2]})

    serialized_stats = _serialize_stats(stats_series)
    assert serialized_stats[0]["metric"] == "Sharpe Ratio"
    assert _serialize_stats(None) == []
    assert _serialize_stats(frame_stats)[0]["metric"] == "Sharpe Ratio"
    assert _deserialize_stats(serialized_stats)["Sharpe Ratio"] == 1.2
    assert _deserialize_stats([]).empty
    assert _deserialize_stats(generic_stats)["Win Rate [%]"] == 55.0
    assert _deserialize_stats(generic_records)["Sharpe Ratio"] == 1.2

    assert _serialize_entry_counts(None) is None
    entry_counts = _serialize_entry_counts(pd.DataFrame({"a": [True, False], "b": [False, True]}))
    assert entry_counts is not None
    assert entry_counts == {"index": [0, 1], "values": [1, 1]}
    assert _serialize_entry_counts("bad") is None
    restored_entry_counts = _deserialize_entry_counts(entry_counts)
    assert restored_entry_counts is not None
    assert list(restored_entry_counts["signal_count"]) == [1, 1]
    assert _deserialize_entry_counts({}).empty
    assert _deserialize_entry_counts(None) is None


def test_metric_map_and_allocation_helpers_cover_non_happy_paths() -> None:
    class _BrokenMean:
        def mean(self) -> float:
            raise RuntimeError("boom")

    class _MetricPortfolio:
        annualized_volatility = 0.2
        omega_ratio = _BrokenMean()

        def sharpe_ratio(self) -> pd.Series:
            return pd.Series([1.0, 2.0])

        def sortino_ratio(self) -> float:
            raise RuntimeError("boom")

        def calmar_ratio(self) -> str:
            return "bad"

    metric_map = _serialize_metric_map(_MetricPortfolio())
    assert _serialize_metric_map(None)["sharpe_ratio"] is None
    assert metric_map["annualized_volatility"] == 0.2
    assert metric_map["sharpe_ratio"] == 1.5
    assert metric_map["sortino_ratio"] is None
    assert metric_map["calmar_ratio"] is None
    assert metric_map["omega_ratio"] is None

    allocation = AllocationInfo(
        allocation=0.3,
        win_rate=0.5,
        avg_win=1.1,
        avg_loss=0.9,
        total_trades=10,
        full_kelly=0.6,
        kelly_fraction=0.5,
    )
    class _TextAllocation:
        def __str__(self) -> str:
            return "allocation-text"

    class _BrokenModelDump:
        def model_dump(self) -> dict[str, object]:
            raise RuntimeError("boom")

    broken_model_dump = _BrokenModelDump()
    assert _serialize_allocation_info(None) is None
    assert _serialize_allocation_info(0.5) == {"kind": "scalar", "payload": 0.5}
    assert _serialize_allocation_info(allocation) == {
        "kind": "allocation_info",
        "payload": allocation.model_dump(),
    }
    assert _serialize_allocation_info(_TextAllocation()) == {
        "kind": "text",
        "payload": "allocation-text",
    }
    assert _serialize_allocation_info(broken_model_dump) == {
        "kind": "text",
        "payload": str(broken_model_dump),
    }
    assert _deserialize_allocation_info(None) is None
    assert _deserialize_allocation_info({"kind": "allocation_info", "payload": allocation.model_dump()}).allocation == 0.3
    assert _deserialize_allocation_info({"kind": "allocation_info", "payload": {"bad": "payload"}}) == {
        "bad": "payload"
    }
    assert _deserialize_allocation_info({"kind": "text", "payload": "fallback"}) == "fallback"


def test_private_helper_error_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    class _RaisingSeries:
        def __new__(cls, value):  # noqa: ANN001, ANN204
            raise TypeError(str(value))

    with monkeypatch.context() as m:
        m.setattr(report_payload_module.pd, "Series", _RaisingSeries)
        assert _serialize_series(object()) is None

    class _RaisingDataFrame:
        def __new__(cls, value):  # noqa: ANN001, ANN204
            raise TypeError(str(value))

    with monkeypatch.context() as m:
        m.setattr(report_payload_module.pd, "DataFrame", _RaisingDataFrame)
        assert _serialize_dataframe(object()) == []
        assert _serialize_stats(object()) == []

    class _BrokenTrades:
        records_readable = None

        def stats(self) -> pd.Series:
            raise RuntimeError("boom")

    class _BrokenPortfolio:
        trades = _BrokenTrades()

        def value(self) -> pd.Series:
            return pd.Series([1.0])

        def drawdown(self) -> pd.Series:
            return pd.Series([0.0])

        def returns(self) -> pd.Series:
            return pd.Series([0.0])

        def stats(self) -> pd.Series:
            raise RuntimeError("boom")

    payload = _serialize_portfolio(_BrokenPortfolio())
    assert payload is not None
    assert payload["trade_stats"] == []
    assert payload["final_stats"] == []
    assert _serialize_portfolio(None) is None
    assert _deserialize_portfolio(None) is None
