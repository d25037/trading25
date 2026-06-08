"""Tests for the NT ratio change x TOPIX close overnight runner script."""

from __future__ import annotations

from types import SimpleNamespace


from tests.unit.scripts.research_runner_test_helpers import (
    assert_standard_bundle_payload,
    fake_research_bundle,
    load_research_runner_module,
    read_bundle_payload,
)


def _load_module():
    return load_research_runner_module(
        "run_nt_ratio_change_topix_close_stock_overnight_distribution.py"
    )


def test_parse_args_accepts_joint_runner_options() -> None:
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
            "--topix-sigma-threshold-1",
            "1.2",
            "--topix-sigma-threshold-2",
            "2.2",
            "--selected-groups",
            "PRIME,TOPIX100",
            "--sample-size",
            "500",
            "--clip-lower",
            "5",
            "--clip-upper",
            "95",
            "--run-id",
            "20260331_181200_testabcd",
        ]
    )

    assert args.sigma_threshold_1 == 1.5
    assert args.sigma_threshold_2 == 2.5
    assert args.topix_sigma_threshold_1 == 1.2
    assert args.topix_sigma_threshold_2 == 2.2
    assert args.run_id == "20260331_181200_testabcd"


def test_main_runs_joint_research_and_prints_bundle_payload(
    monkeypatch, capsys
) -> None:
    module = _load_module()
    fake_stats = SimpleNamespace(threshold_1=0.01, threshold_2=0.02)
    fake_result = object()
    fake_bundle = fake_research_bundle(
        experiment_id="market-behavior/nt-ratio-change-topix-close-stock-overnight-distribution",
        run_id="20260331_181200_testabcd",
    )

    monkeypatch.setattr(
        module,
        "get_topix_close_return_stats",
        lambda *args, **kwargs: fake_stats,
    )
    monkeypatch.setattr(
        module,
        "run_nt_ratio_change_topix_close_stock_overnight_distribution",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_nt_ratio_change_topix_close_stock_overnight_distribution_research_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--run-id",
            "20260331_181200_testabcd",
        ]
    )

    payload = read_bundle_payload(capsys)
    assert exit_code == 0
    assert_standard_bundle_payload(payload, run_id="20260331_181200_testabcd")
