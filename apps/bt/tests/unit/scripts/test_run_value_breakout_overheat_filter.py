"""Tests for the value-breakout overheat filter research runner."""

from __future__ import annotations


from tests.unit.scripts.research_runner_test_helpers import (
    assert_standard_bundle_payload,
    fake_research_bundle,
    load_research_runner_module,
    read_bundle_payload,
)


def _load_module():
    return load_research_runner_module("run_value_breakout_overheat_filter.py")


def test_parse_args_accepts_overheat_filter_options() -> None:
    module = _load_module()

    args = module.parse_args(
        [
            "--input-bundle",
            "/tmp/value-breakout/run",
            "--db-path",
            "/tmp/market.duckdb",
            "--market-scope",
            "standard",
            "--score-method",
            "prime_size_tilt",
            "--liquidity-scenario",
            "adv10m",
            "--breakout-policy",
            "breakout_additive",
            "--breakout-window",
            "120",
            "--breakout-lookback-sessions",
            "20",
            "--rebalance-months",
            "3",
            "--selection-count",
            "10",
            "--holdout-months",
            "0",
            "--output-root",
            "/tmp/research",
            "--run-id",
            "20260509_test",
        ]
    )

    assert args.input_bundle == "/tmp/value-breakout/run"
    assert args.db_path == "/tmp/market.duckdb"
    assert args.market_scope == "standard"
    assert args.breakout_window == 120
    assert args.holdout_months == 0
    assert args.run_id == "20260509_test"


def test_main_runs_overheat_filter_research_and_prints_bundle_payload(
    monkeypatch,
    capsys,
) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = fake_research_bundle(
        experiment_id="market-behavior/value-breakout-overheat-filter",
        run_id="20260509_test",
    )

    monkeypatch.setattr(
        module,
        "run_value_breakout_overheat_filter",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_value_breakout_overheat_filter_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--input-bundle",
            "/tmp/value-breakout/run",
            "--run-id",
            "20260509_test",
        ]
    )

    payload = read_bundle_payload(capsys)
    assert exit_code == 0
    assert_standard_bundle_payload(payload, run_id="20260509_test")
