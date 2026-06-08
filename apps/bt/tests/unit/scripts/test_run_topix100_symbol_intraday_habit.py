"""Tests for the runner-first TOPIX100 symbol intraday habit script."""

from __future__ import annotations


from tests.unit.scripts.research_runner_test_helpers import (
    assert_standard_bundle_payload,
    fake_research_bundle,
    load_research_runner_module,
    read_bundle_payload,
)


def _load_module():
    return load_research_runner_module("run_topix100_symbol_intraday_habit.py")


def test_parse_args_accepts_symbol_intraday_habit_options() -> None:
    module = _load_module()

    args = module.parse_args(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--start-date",
            "2024-04-15",
            "--end-date",
            "2026-04-14",
            "--interval-minutes",
            "30",
            "--sample-seed",
            "42",
            "--random-sample-size",
            "4",
            "--anchor-code",
            "6857",
            "--analysis-period-months",
            "6",
            "--output-root",
            "/tmp/research",
            "--run-id",
            "20260415_150000_testabcd",
            "--notes",
            "symbol habit bundle",
        ]
    )

    assert args.db_path == "/tmp/market.duckdb"
    assert args.interval_minutes == 30
    assert args.sample_seed == 42
    assert args.run_id == "20260415_150000_testabcd"


def test_main_runs_research_and_prints_bundle_payload(monkeypatch, capsys) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = fake_research_bundle(
        experiment_id="market-behavior/topix100-symbol-intraday-habit",
        run_id="20260415_150000_testabcd",
    )

    monkeypatch.setattr(
        module,
        "run_topix100_symbol_intraday_habit_research",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_topix100_symbol_intraday_habit_research_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--interval-minutes",
            "30",
            "--sample-seed",
            "42",
            "--run-id",
            "20260415_150000_testabcd",
        ]
    )

    payload = read_bundle_payload(capsys)
    assert exit_code == 0
    assert_standard_bundle_payload(payload, run_id="20260415_150000_testabcd")
