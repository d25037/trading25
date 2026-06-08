"""Tests for the NT ratio change / stock overnight runner script."""

from __future__ import annotations


from tests.unit.scripts.research_runner_test_helpers import (
    assert_standard_bundle_payload,
    fake_research_bundle,
    load_research_runner_module,
    read_bundle_payload,
)


def _load_module():
    return load_research_runner_module(
        "run_nt_ratio_change_stock_overnight_distribution.py"
    )


def test_parse_args_accepts_nt_ratio_runner_options() -> None:
    module = _load_module()

    args = module.parse_args(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2024-12-31",
            "--sigma-threshold-1",
            "1.5",
            "--sigma-threshold-2",
            "2.5",
            "--selected-groups",
            "PRIME,TOPIX100",
            "--sample-size",
            "500",
            "--clip-lower",
            "5",
            "--clip-upper",
            "95",
            "--output-root",
            "/tmp/research",
            "--run-id",
            "20260331_181000_testabcd",
            "--notes",
            "nt ratio bundle",
        ]
    )

    assert args.db_path == "/tmp/market.duckdb"
    assert args.sigma_threshold_1 == 1.5
    assert args.sigma_threshold_2 == 2.5
    assert args.selected_groups == "PRIME,TOPIX100"
    assert args.run_id == "20260331_181000_testabcd"


def test_main_runs_nt_ratio_research_and_prints_bundle_payload(
    monkeypatch, capsys
) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = fake_research_bundle(
        experiment_id="market-behavior/nt-ratio-change-stock-overnight-distribution",
        run_id="20260331_181000_testabcd",
    )

    monkeypatch.setattr(
        module,
        "run_nt_ratio_change_stock_overnight_distribution",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_nt_ratio_change_stock_overnight_distribution_research_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--run-id",
            "20260331_181000_testabcd",
        ]
    )

    payload = read_bundle_payload(capsys)
    assert exit_code == 0
    assert_standard_bundle_payload(payload, run_id="20260331_181000_testabcd")
