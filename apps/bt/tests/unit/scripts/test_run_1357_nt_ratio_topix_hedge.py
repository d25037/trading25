"""Tests for the 1357 x NT ratio / TOPIX hedge runner script."""

from __future__ import annotations


from tests.unit.scripts.research_runner_test_helpers import (
    assert_standard_bundle_payload,
    fake_research_bundle,
    load_research_runner_module,
    read_bundle_payload,
)


def _load_module():
    return load_research_runner_module("run_1357_nt_ratio_topix_hedge.py")


def test_parse_args_accepts_hedge_runner_options() -> None:
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
            "TOPIX100,TOPIX500",
            "--fixed-weights",
            "0.2,0.4",
            "--output-root",
            "/tmp/research",
            "--run-id",
            "20260401_122000_testabcd",
        ]
    )

    assert args.db_path == "/tmp/market.duckdb"
    assert args.sigma_threshold_1 == 1.5
    assert args.sigma_threshold_2 == 2.5
    assert args.selected_groups == "TOPIX100,TOPIX500"
    assert args.fixed_weights == "0.2,0.4"
    assert args.run_id == "20260401_122000_testabcd"


def test_main_runs_hedge_research_and_prints_bundle_payload(
    monkeypatch,
    capsys,
) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = fake_research_bundle(
        experiment_id="market-behavior/hedge-1357-nt-ratio-topix",
        run_id="20260401_122000_testabcd",
    )

    monkeypatch.setattr(
        module,
        "run_1357_nt_ratio_topix_hedge_research",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_1357_nt_ratio_topix_hedge_research_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--run-id",
            "20260401_122000_testabcd",
        ]
    )

    payload = read_bundle_payload(capsys)
    assert exit_code == 0
    assert_standard_bundle_payload(payload, run_id="20260401_122000_testabcd")
