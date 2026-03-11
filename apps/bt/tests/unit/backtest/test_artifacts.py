"""Tests for extracted backtest artifact helpers."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from src.domains.backtest.core.artifacts import (
    BacktestArtifactWriter,
    build_metrics_payload,
)


class _MetricValue:
    def __init__(self, value: float) -> None:
        self._value = value

    def mean(self) -> float:
        return self._value


class _Portfolio:
    def stats(self):
        return {
            "Total Return [%]": 0.0,
            "Max Drawdown [%]": 0.0,
            "Sharpe Ratio": 0.0,
            "Sortino Ratio": 0.0,
            "Calmar Ratio": 0.0,
            "Win Rate [%]": 0.0,
            "Total Trades": 0.0,
        }

    def total_return(self):
        return _MetricValue(5.0)

    def sharpe_ratio(self):
        return _MetricValue(5.0)

    def sortino_ratio(self):
        return _MetricValue(5.0)

    def calmar_ratio(self):
        return _MetricValue(5.0)

    def max_drawdown(self):
        return _MetricValue(5.0)

    @property
    def trades(self):
        return SimpleNamespace(
            count=lambda: 7,
            win_rate=lambda: _MetricValue(5.0),
        )


def test_artifact_paths_for_html_include_report_payload() -> None:
    html_path = Path("/tmp/backtest/result.html")

    paths = BacktestArtifactWriter.artifact_paths_for_html(html_path)

    assert paths.metrics_path == html_path.with_suffix(".metrics.json")
    assert paths.manifest_path == html_path.with_suffix(".manifest.json")
    assert paths.simulation_payload_path == html_path.with_suffix(".simulation.pkl")
    assert paths.report_payload_path == html_path.with_suffix(".report.json")


def test_build_metrics_payload_preserves_stats_values_over_fallback_metrics() -> None:
    payload = build_metrics_payload(
        portfolio=_Portfolio(),
        allocation_info=0.0,
    )

    assert payload["total_return"] == 0.0
    assert payload["sharpe_ratio"] == 0.0
    assert payload["trade_count"] == 0
    assert payload["optimal_allocation"] == 0.0
