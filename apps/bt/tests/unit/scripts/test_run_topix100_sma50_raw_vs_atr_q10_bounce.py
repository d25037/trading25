"""Tests for the runner-first TOPIX100 SMA50 raw-vs-ATR Q10 bounce script."""

from __future__ import annotations


from tests.unit.scripts.research_runner_test_helpers import (
    assert_standard_bundle_payload,
    fake_research_bundle,
    load_research_runner_module,
    read_bundle_payload,
)


def _load_module():
    return load_research_runner_module("run_topix100_sma50_raw_vs_atr_q10_bounce.py")


def test_parse_args_accepts_runner_options() -> None:
    module = _load_module()

    args = module.parse_args(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2024-12-31",
            "--lookback-years",
            "7",
            "--min-constituents-per-day",
            "70",
            "--volume-feature",
            "volume_sma_5_20",
            "--output-root",
            "/tmp/research",
            "--run-id",
            "20260331_210000_testabcd",
            "--notes",
            "raw vs atr bundle",
        ]
    )

    assert args.db_path == "/tmp/market.duckdb"
    assert args.volume_feature == "volume_sma_5_20"
    assert args.run_id == "20260331_210000_testabcd"


def test_main_runs_research_and_prints_bundle_payload(monkeypatch, capsys) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = fake_research_bundle(
        experiment_id="market-behavior/topix100-sma50-raw-vs-atr-q10-bounce",
        run_id="20260331_210000_testabcd",
    )

    monkeypatch.setattr(
        module,
        "run_topix100_sma50_raw_vs_atr_q10_bounce_research",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_topix100_sma50_raw_vs_atr_q10_bounce_research_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--run-id",
            "20260331_210000_testabcd",
        ]
    )

    payload = read_bundle_payload(capsys)
    assert exit_code == 0
    assert_standard_bundle_payload(payload, run_id="20260331_210000_testabcd")
