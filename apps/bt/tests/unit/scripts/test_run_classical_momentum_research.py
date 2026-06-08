"""Tests for the classical momentum research runner script."""

from __future__ import annotations


from tests.unit.scripts.research_runner_test_helpers import (
    assert_standard_bundle_payload,
    fake_research_bundle,
    load_research_runner_module,
    read_bundle_payload,
)


def _load_module():
    return load_research_runner_module("run_classical_momentum_research.py")


def test_parse_args_accepts_classical_momentum_options() -> None:
    module = _load_module()

    args = module.parse_args(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2024-12-31",
            "--lookback-specs",
            "126:20,252:20",
            "--hold-sessions",
            "20,60",
            "--selection-fractions",
            "0.05,0.10",
            "--rebalance-interval-sessions",
            "20",
            "--output-root",
            "/tmp/research",
            "--run-id",
            "20260509_test",
        ]
    )

    assert args.db_path == "/tmp/market.duckdb"
    assert args.lookback_specs == ((126, 20), (252, 20))
    assert args.hold_sessions == (20, 60)
    assert args.selection_fractions == (0.05, 0.10)
    assert args.rebalance_interval_sessions == 20
    assert args.run_id == "20260509_test"


def test_main_runs_classical_momentum_research_and_prints_bundle_payload(
    monkeypatch,
    capsys,
) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = fake_research_bundle(
        experiment_id="market-behavior/classical-momentum-research",
        run_id="20260509_test",
    )

    monkeypatch.setattr(
        module,
        "run_classical_momentum_research",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_classical_momentum_research_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--run-id",
            "20260509_test",
        ]
    )

    payload = read_bundle_payload(capsys)
    assert exit_code == 0
    assert_standard_bundle_payload(payload, run_id="20260509_test")
