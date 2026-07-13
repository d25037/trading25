import pytest
from pydantic import ValidationError

from src.application.contracts.backtest import (
    BacktestResultSummary,
    SignalAttributionLooResult,
    SignalAttributionMetrics,
    SignalAttributionResult,
    SignalAttributionShapleyMeta,
    SignalAttributionShapleyResult,
    SignalAttributionSignalResult,
    SignalAttributionTiming,
    SignalAttributionTopNScore,
    SignalAttributionTopNSelection,
)


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


def test_signal_attribution_result_serialization_is_stable() -> None:
    result = SignalAttributionResult(
        baseline_metrics=SignalAttributionMetrics(total_return=10.0, sharpe_ratio=1.1),
        signals=[
            SignalAttributionSignalResult(
                signal_id="entry.range_break",
                scope="entry",
                param_key="range_break",
                signal_name="Range Break",
                loo=SignalAttributionLooResult(
                    status="ok",
                    variant_metrics=SignalAttributionMetrics(
                        total_return=8.0,
                        sharpe_ratio=0.9,
                    ),
                    delta_total_return=2.0,
                    delta_sharpe_ratio=0.2,
                ),
                shapley=SignalAttributionShapleyResult(
                    status="ok",
                    total_return=1.5,
                    sharpe_ratio=0.1,
                    method="exact",
                    sample_size=8,
                ),
            )
        ],
        top_n_selection=SignalAttributionTopNSelection(
            top_n_requested=5,
            top_n_effective=1,
            selected_signal_ids=["entry.range_break"],
            scores=[
                SignalAttributionTopNScore(
                    signal_id="entry.range_break",
                    score=1.0,
                )
            ],
        ),
        timing=SignalAttributionTiming(
            total_seconds=1.0,
            baseline_seconds=0.2,
            loo_seconds=0.3,
            shapley_seconds=0.5,
        ),
        shapley=SignalAttributionShapleyMeta(
            method="exact",
            sample_size=8,
            evaluations=8,
        ),
    )

    assert result.model_dump(mode="json") == {
        "baseline_metrics": {"total_return": 10.0, "sharpe_ratio": 1.1},
        "signals": [
            {
                "signal_id": "entry.range_break",
                "scope": "entry",
                "param_key": "range_break",
                "signal_name": "Range Break",
                "loo": {
                    "status": "ok",
                    "variant_metrics": {"total_return": 8.0, "sharpe_ratio": 0.9},
                    "delta_total_return": 2.0,
                    "delta_sharpe_ratio": 0.2,
                    "error": None,
                },
                "shapley": {
                    "status": "ok",
                    "total_return": 1.5,
                    "sharpe_ratio": 0.1,
                    "method": "exact",
                    "sample_size": 8,
                    "error": None,
                },
            }
        ],
        "top_n_selection": {
            "top_n_requested": 5,
            "top_n_effective": 1,
            "selected_signal_ids": ["entry.range_break"],
            "scores": [{"signal_id": "entry.range_break", "score": 1.0}],
        },
        "timing": {
            "total_seconds": 1.0,
            "baseline_seconds": 0.2,
            "loo_seconds": 0.3,
            "shapley_seconds": 0.5,
        },
        "shapley": {
            "method": "exact",
            "sample_size": 8,
            "error": None,
            "evaluations": 8,
        },
    }


def test_signal_attribution_result_optional_fields_and_scores_have_defaults() -> None:
    result = SignalAttributionResult.model_validate(
        {
            "baseline_metrics": {"total_return": 0.0, "sharpe_ratio": 0.0},
            "signals": [
                {
                    "signal_id": "exit.stop_loss",
                    "scope": "exit",
                    "param_key": "stop_loss",
                    "signal_name": "Stop Loss",
                    "loo": {"status": "error"},
                }
            ],
            "top_n_selection": {
                "top_n_requested": 0,
                "top_n_effective": 0,
                "selected_signal_ids": [],
            },
            "timing": {
                "total_seconds": 0.0,
                "baseline_seconds": 0.0,
                "loo_seconds": 0.0,
                "shapley_seconds": 0.0,
            },
            "shapley": {},
        }
    )

    assert result.top_n_selection.scores == []
    assert result.signals[0].loo.variant_metrics is None
    assert result.signals[0].loo.delta_total_return is None
    assert result.signals[0].loo.delta_sharpe_ratio is None
    assert result.signals[0].loo.error is None
    assert result.signals[0].shapley is None
    assert result.shapley.method is None
    assert result.shapley.sample_size is None
    assert result.shapley.error is None
    assert result.shapley.evaluations is None


@pytest.mark.parametrize("missing_field", ["signals", "selected_signal_ids"])
def test_signal_attribution_result_rejects_missing_required_lists(
    missing_field: str,
) -> None:
    payload = {
        "baseline_metrics": {"total_return": 0.0, "sharpe_ratio": 0.0},
        "signals": [],
        "top_n_selection": {
            "top_n_requested": 0,
            "top_n_effective": 0,
            "selected_signal_ids": [],
        },
        "timing": {
            "total_seconds": 0.0,
            "baseline_seconds": 0.0,
            "loo_seconds": 0.0,
            "shapley_seconds": 0.0,
        },
        "shapley": {},
    }
    if missing_field == "signals":
        del payload["signals"]
    else:
        del payload["top_n_selection"]["selected_signal_ids"]

    with pytest.raises(ValidationError):
        SignalAttributionResult.model_validate(payload)


def test_signal_attribution_results_reject_invalid_literal_values() -> None:
    with pytest.raises(ValidationError):
        SignalAttributionLooResult.model_validate({"status": "unknown"})

    with pytest.raises(ValidationError):
        SignalAttributionSignalResult.model_validate(
            {
                "signal_id": "entry.range_break",
                "scope": "both",
                "param_key": "range_break",
                "signal_name": "Range Break",
                "loo": {"status": "ok"},
            }
        )
