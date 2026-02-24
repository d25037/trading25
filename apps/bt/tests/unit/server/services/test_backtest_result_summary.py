"""server/services/backtest_result_summary.py のテスト"""

import json
from pathlib import Path

from src.entrypoints.http.schemas.backtest import BacktestResultSummary
from src.application.services.backtest_result_summary import resolve_backtest_result_summary


def test_resolve_backtest_result_summary_prefers_artifact_set(tmp_path: Path):
    html_path = tmp_path / "result.html"
    html_path.write_text("<html></html>", encoding="utf-8")
    metrics_path = html_path.with_suffix(".metrics.json")
    metrics_path.write_text(
        json.dumps(
            {
                "total_return": 12.5,
                "sharpe_ratio": 1.7,
                "sortino_ratio": 2.1,
                "calmar_ratio": 2.9,
                "max_drawdown": -6.2,
                "win_rate": 58.0,
                "total_trades": 42,
            }
        ),
        encoding="utf-8",
    )

    fallback = {
        "total_return": 1.0,
        "sharpe_ratio": 1.0,
        "sortino_ratio": 1.0,
        "calmar_ratio": 1.0,
        "max_drawdown": -1.0,
        "win_rate": 50.0,
        "trade_count": 1,
    }

    summary = resolve_backtest_result_summary(html_path, fallback)

    assert summary is not None
    assert summary.total_return == 12.5
    assert summary.sharpe_ratio == 1.7
    assert summary.sortino_ratio == 2.1
    assert summary.calmar_ratio == 2.9
    assert summary.max_drawdown == -6.2
    assert summary.win_rate == 58.0
    assert summary.trade_count == 42


def test_resolve_backtest_result_summary_uses_fallback_for_missing_artifact_fields(tmp_path: Path):
    html_path = tmp_path / "result.html"
    html_path.write_text("<html></html>", encoding="utf-8")
    metrics_path = html_path.with_suffix(".metrics.json")
    metrics_path.write_text(
        json.dumps(
            {
                "total_return": 12.5,
                # intentionally missing sharpe/calmar/win_rate/trades
            }
        ),
        encoding="utf-8",
    )

    fallback = {
        "total_return": 1.0,
        "sharpe_ratio": 1.2,
        "sortino_ratio": 1.4,
        "calmar_ratio": 2.0,
        "max_drawdown": -4.0,
        "win_rate": 55.0,
        "trade_count": 11,
    }

    summary = resolve_backtest_result_summary(html_path, fallback)

    assert summary is not None
    assert summary.total_return == 12.5
    assert summary.sharpe_ratio == 1.2
    assert summary.sortino_ratio == 1.4
    assert summary.calmar_ratio == 2.0
    assert summary.max_drawdown == -4.0
    assert summary.win_rate == 55.0
    assert summary.trade_count == 11


def test_resolve_backtest_result_summary_falls_back_without_artifact():
    summary = resolve_backtest_result_summary(
        "/tmp/not-found.html",
        {
            "total_return": 7.0,
            "sharpe_ratio": 1.2,
            "sortino_ratio": 1.4,
            "calmar_ratio": 2.0,
            "max_drawdown": -4.0,
            "win_rate": 55.0,
            "trade_count": 11,
            "html_path": "fallback.html",
        },
    )

    assert summary is not None
    assert summary.total_return == 7.0
    assert summary.sharpe_ratio == 1.2
    assert summary.sortino_ratio == 1.4
    assert summary.calmar_ratio == 2.0
    assert summary.max_drawdown == -4.0
    assert summary.win_rate == 55.0
    assert summary.trade_count == 11
    assert summary.html_path == "/tmp/not-found.html"


def test_resolve_backtest_result_summary_accepts_summary_fallback():
    fallback_summary = BacktestResultSummary(
        total_return=3.0,
        sharpe_ratio=0.8,
        sortino_ratio=1.0,
        calmar_ratio=1.2,
        max_drawdown=-2.0,
        win_rate=52.0,
        trade_count=9,
        html_path=None,
    )

    summary = resolve_backtest_result_summary(None, fallback_summary)

    assert summary is not None
    assert summary.total_return == 3.0
    assert summary.sortino_ratio == 1.0
    assert summary.trade_count == 9


def test_resolve_backtest_result_summary_returns_none_when_no_sources():
    summary = resolve_backtest_result_summary(None, None)
    assert summary is None
