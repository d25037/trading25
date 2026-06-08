"""Tests for the runner-first PRIME ex TOPIX500 SMA ratio research script."""

from __future__ import annotations


from tests.unit.scripts.research_runner_test_helpers import (
    assert_standard_bundle_payload,
    fake_research_bundle,
    load_research_runner_module,
    read_bundle_payload,
)


def _load_module():
    return load_research_runner_module(
        "run_prime_ex_topix500_sma_ratio_rank_future_close.py"
    )


def test_parse_args_accepts_prime_ex_runner_options() -> None:
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
            "400",
            "--output-root",
            "/tmp/research",
            "--run-id",
            "20260331_181500_testabcd",
        ]
    )

    assert args.db_path == "/tmp/market.duckdb"
    assert args.min_constituents_per_day == 400
    assert args.run_id == "20260331_181500_testabcd"


def test_main_runs_prime_ex_research_and_prints_bundle_payload(
    monkeypatch,
    capsys,
) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = fake_research_bundle(
        experiment_id="market-behavior/prime-ex-topix500-sma-ratio-rank-future-close",
        run_id="20260331_181500_testabcd",
    )

    monkeypatch.setattr(
        module,
        "run_prime_ex_topix500_sma_ratio_rank_future_close_research",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_prime_ex_topix500_sma_ratio_rank_future_close_research_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--run-id",
            "20260331_181500_testabcd",
        ]
    )

    payload = read_bundle_payload(capsys)
    assert exit_code == 0
    assert_standard_bundle_payload(payload, run_id="20260331_181500_testabcd")
