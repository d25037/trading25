from src.application.contracts.backtest import BacktestResultSummary


def _summary() -> BacktestResultSummary:
    return BacktestResultSummary(
        total_return=12.5,
        sharpe_ratio=1.2,
        sortino_ratio=1.4,
        calmar_ratio=0.8,
        max_drawdown=-9.5,
        win_rate=54.0,
        trade_count=42,
        html_path="/tmp/result.html",
    )


def test_backtest_result_summary_serialization_is_stable() -> None:
    assert _summary().model_dump(mode="json") == {
        "total_return": 12.5,
        "sharpe_ratio": 1.2,
        "sortino_ratio": 1.4,
        "calmar_ratio": 0.8,
        "max_drawdown": -9.5,
        "win_rate": 54.0,
        "trade_count": 42,
        "html_path": "/tmp/result.html",
    }


def test_backtest_result_summary_schema_is_stable() -> None:
    schema = BacktestResultSummary.model_json_schema()

    assert schema["title"] == "BacktestResultSummary"
    assert schema["required"] == [
        "total_return",
        "sharpe_ratio",
        "calmar_ratio",
        "max_drawdown",
        "win_rate",
        "trade_count",
    ]
    assert set(schema["properties"]) == {
        "total_return",
        "sharpe_ratio",
        "sortino_ratio",
        "calmar_ratio",
        "max_drawdown",
        "win_rate",
        "trade_count",
        "html_path",
    }


def test_backtest_result_summary_optional_fields_default_to_none() -> None:
    summary = BacktestResultSummary(
        total_return=0.0,
        sharpe_ratio=0.0,
        calmar_ratio=0.0,
        max_drawdown=0.0,
        win_rate=0.0,
        trade_count=0,
    )

    assert summary.sortino_ratio is None
    assert summary.html_path is None
