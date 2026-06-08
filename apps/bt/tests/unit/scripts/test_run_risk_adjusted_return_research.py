"""Tests for the synthetic risk-adjusted-return runner script."""

from __future__ import annotations


from tests.unit.scripts.research_runner_test_helpers import (
    assert_standard_bundle_payload,
    fake_research_bundle,
    load_research_runner_module,
    read_bundle_payload,
)


def _load_module():
    return load_research_runner_module("run_risk_adjusted_return_research.py")


def test_parse_args_accepts_risk_adjusted_return_runner_options() -> None:
    module = _load_module()

    args = module.parse_args(
        [
            "--lookback-period",
            "40",
            "--ratio-type",
            "sharpe",
            "--seed",
            "7",
            "--n-days",
            "300",
            "--output-root",
            "/tmp/research",
            "--run-id",
            "20260401_123500_testabcd",
        ]
    )

    assert args.lookback_period == 40
    assert args.ratio_type == "sharpe"
    assert args.seed == 7
    assert args.n_days == 300
    assert args.run_id == "20260401_123500_testabcd"


def test_main_runs_risk_adjusted_return_and_prints_bundle_payload(
    monkeypatch,
    capsys,
) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = fake_research_bundle(
        experiment_id="market-behavior/risk-adjusted-return-playground",
        run_id="20260401_123500_testabcd",
    )

    monkeypatch.setattr(
        module,
        "run_risk_adjusted_return_research",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_risk_adjusted_return_research_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--run-id",
            "20260401_123500_testabcd",
        ]
    )

    payload = read_bundle_payload(capsys)
    assert exit_code == 0
    assert_standard_bundle_payload(payload, run_id="20260401_123500_testabcd")
