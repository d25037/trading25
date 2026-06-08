"""Tests for the runner-first TOPIX100 price/SMA50 decile partition script."""

from __future__ import annotations


from tests.unit.scripts.research_runner_test_helpers import (
    assert_standard_bundle_payload,
    fake_research_bundle,
    load_research_runner_module,
    read_bundle_payload,
)


def _load_module():
    return load_research_runner_module(
        "run_topix100_price_to_sma50_decile_partitions.py"
    )


def test_parse_args_accepts_partition_runner_options() -> None:
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
            "--output-root",
            "/tmp/research",
            "--run-id",
            "20260403_120000_testabcd",
            "--notes",
            "partition bundle",
        ]
    )

    assert args.db_path == "/tmp/market.duckdb"
    assert args.start_date == "2024-01-01"
    assert args.end_date == "2024-12-31"
    assert args.lookback_years == 7
    assert args.min_constituents_per_day == 70
    assert args.run_id == "20260403_120000_testabcd"


def test_main_runs_partition_research_and_prints_bundle_payload(
    monkeypatch,
    capsys,
) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = fake_research_bundle(
        experiment_id="market-behavior/topix100-price-to-sma50-decile-partitions",
        run_id="20260403_120000_testabcd",
    )

    monkeypatch.setattr(
        module,
        "run_topix100_price_to_sma50_decile_partitions_research",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_topix100_price_to_sma50_decile_partitions_research_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--run-id",
            "20260403_120000_testabcd",
        ]
    )

    payload = read_bundle_payload(capsys)
    assert exit_code == 0
    assert_standard_bundle_payload(payload, run_id="20260403_120000_testabcd")
