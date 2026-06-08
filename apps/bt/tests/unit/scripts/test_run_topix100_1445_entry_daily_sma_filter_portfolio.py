"""Tests for the TOPIX100 14:45 daily-SMA filter portfolio runner."""

from __future__ import annotations


from tests.unit.scripts.research_runner_test_helpers import (
    assert_standard_bundle_payload,
    fake_research_bundle,
    load_research_runner_module,
    read_bundle_payload,
)


def _load_module():
    return load_research_runner_module(
        "run_topix100_1445_entry_daily_sma_filter_portfolio.py"
    )


def test_parse_args_accepts_runner_options() -> None:
    module = _load_module()

    args = module.parse_args(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--start-date",
            "2024-04-18",
            "--end-date",
            "2026-04-17",
            "--interval-minutes",
            "15",
            "--signal-family",
            "previous_open_vs_open",
            "--exit-label",
            "next_open",
            "--market-regime-bucket-key",
            "weak",
            "--subgroup-key",
            "losers",
            "--sma-window",
            "50",
            "--sma-filter-state",
            "at_or_below",
            "--bucket-count",
            "4",
            "--period-months",
            "6",
            "--entry-time",
            "14:45",
            "--next-session-exit-time",
            "10:30",
            "--tail-fraction",
            "0.20",
            "--output-root",
            "/tmp/research",
            "--run-id",
            "20260419_130000_testabcd",
            "--notes",
            "14:45 filtered portfolio bundle",
        ]
    )

    assert args.db_path == "/tmp/market.duckdb"
    assert args.market_regime_bucket_key == "weak"
    assert args.subgroup_key == "losers"
    assert args.sma_window == 50
    assert args.sma_filter_state == "at_or_below"
    assert args.tail_fraction == 0.20


def test_main_runs_research_and_prints_bundle_payload(monkeypatch, capsys) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = fake_research_bundle(
        experiment_id="market-behavior/topix100-1445-entry-daily-sma-filter-portfolio",
        run_id="20260419_130000_testabcd",
    )

    monkeypatch.setattr(
        module,
        "run_topix100_1445_entry_daily_sma_filter_portfolio_research",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_topix100_1445_entry_daily_sma_filter_portfolio_research_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--interval-minutes",
            "15",
            "--signal-family",
            "previous_open_vs_open",
            "--exit-label",
            "next_open",
            "--market-regime-bucket-key",
            "weak",
            "--subgroup-key",
            "losers",
            "--sma-window",
            "50",
            "--sma-filter-state",
            "at_or_below",
            "--tail-fraction",
            "0.20",
            "--run-id",
            "20260419_130000_testabcd",
        ]
    )

    payload = read_bundle_payload(capsys)
    assert exit_code == 0
    assert_standard_bundle_payload(payload, run_id="20260419_130000_testabcd")
