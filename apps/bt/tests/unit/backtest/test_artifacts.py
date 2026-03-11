"""Tests for backtest artifact helpers."""

from __future__ import annotations

import importlib.metadata as importlib_metadata
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from src.domains.backtest.contracts import CanonicalExecutionMetrics
from src.domains.backtest.core import artifacts as artifacts_module
from src.domains.backtest.core.artifacts import (
    BacktestArtifactWriter,
    _coerce_float,
    _extract_optimal_allocation,
    _extract_stat,
    build_metrics_payload,
)


def test_artifact_paths_and_write_metrics(tmp_path: Path) -> None:
    writer = BacktestArtifactWriter()
    html_path = tmp_path / "result.html"

    artifact_paths = writer.artifact_paths_for_html(html_path)
    metrics_path = writer.write_metrics(
        html_path=html_path,
        metrics_payload={"total_return": 1.2},
    )

    assert artifact_paths.metrics_path == html_path.with_suffix(".metrics.json")
    assert artifact_paths.manifest_path == html_path.with_suffix(".manifest.json")
    assert artifact_paths.report_data_path == html_path.with_suffix(".report.json")
    assert metrics_path.read_text(encoding="utf-8").strip().startswith("{")


def test_package_and_git_helpers(monkeypatch) -> None:
    monkeypatch.setattr(importlib_metadata, "version", lambda package: f"{package}-1.0.0")
    monkeypatch.setattr(
        artifacts_module.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(stdout="abc123\n"),
    )

    assert BacktestArtifactWriter._get_package_version("python")
    assert BacktestArtifactWriter._get_package_version("vectorbt") == "vectorbt-1.0.0"
    assert BacktestArtifactWriter._get_git_commit() == "abc123"


def test_package_and_git_helpers_return_none_on_failure(monkeypatch) -> None:
    def _raise_version(_package: str) -> str:
        raise importlib_metadata.PackageNotFoundError

    def _raise_run(*args, **kwargs):  # noqa: ANN002, ANN003
        raise RuntimeError("git unavailable")

    monkeypatch.setattr(importlib_metadata, "version", _raise_version)
    monkeypatch.setattr(artifacts_module.subprocess, "run", _raise_run)

    assert BacktestArtifactWriter._get_package_version("missing") is None
    assert BacktestArtifactWriter._get_git_commit() is None


def test_helper_coercions_cover_edge_cases() -> None:
    stats_series = pd.Series({"Profit Factor": 1.8})
    stats_frame = pd.DataFrame(
        {
            "strategy_a": [1.2],
            "strategy_b": [1.4],
        },
        index=["Profit Factor"],
    )

    assert _coerce_float(None) is None
    assert _coerce_float("nan") is None
    assert _coerce_float("3.5") == 3.5
    assert _extract_stat(stats_series, "Profit Factor") == 1.8
    assert _extract_stat(stats_frame, "Profit Factor") == pytest.approx(1.3)
    assert _extract_stat(None, "Profit Factor") is None
    assert _extract_optimal_allocation(SimpleNamespace(allocation=0.25)) == 0.25
    assert _extract_optimal_allocation("bad") is None


def test_build_metrics_payload_uses_summary_metrics_stats_and_allocation(monkeypatch) -> None:
    monkeypatch.setattr(
        artifacts_module,
        "canonical_metrics_from_portfolio",
        lambda _portfolio: CanonicalExecutionMetrics(
            total_return=10.0,
            sharpe_ratio=1.5,
            sortino_ratio=1.8,
            calmar_ratio=1.1,
            max_drawdown=-2.5,
            win_rate=55.0,
            trade_count=7,
        ),
    )

    class _Portfolio:
        def stats(self) -> pd.Series:
            return pd.Series({"Profit Factor": 2.2})

    payload = build_metrics_payload(
        portfolio=_Portfolio(),
        allocation_info=SimpleNamespace(allocation=0.4),
    )

    assert payload == {
        "total_return": 10.0,
        "max_drawdown": -2.5,
        "sharpe_ratio": 1.5,
        "sortino_ratio": 1.8,
        "calmar_ratio": 1.1,
        "win_rate": 55.0,
        "total_trades": 7,
        "profit_factor": 2.2,
        "optimal_allocation": 0.4,
    }


def test_build_metrics_payload_handles_missing_summary_and_stats_failure(monkeypatch) -> None:
    monkeypatch.setattr(
        artifacts_module,
        "canonical_metrics_from_portfolio",
        lambda _portfolio: None,
    )

    class _BrokenPortfolio:
        def stats(self) -> pd.Series:
            raise RuntimeError("stats failed")

    payload = build_metrics_payload(
        portfolio=_BrokenPortfolio(),
        allocation_info=float("nan"),
    )

    assert payload == {}
