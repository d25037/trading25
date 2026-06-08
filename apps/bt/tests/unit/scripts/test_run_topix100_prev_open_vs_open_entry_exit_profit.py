"""Tests for the TOPIX100 prev-open-vs-open entry/exit profit runner."""

from __future__ import annotations


from tests.unit.scripts.research_runner_test_helpers import (
    assert_standard_bundle_payload,
    fake_research_bundle,
    load_research_runner_module,
    read_bundle_payload,
)


def _load_module():
    return load_research_runner_module(
        "run_topix100_prev_open_vs_open_entry_exit_profit.py"
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
            "--intervals",
            "5,15,30",
            "--bucket-count",
            "4",
            "--period-months",
            "6",
            "--round-trip-cost-bps",
            "12.5",
            "--output-root",
            "/tmp/research",
            "--run-id",
            "20260418_150000_testabcd",
            "--notes",
            "prev open vs open entry exit bundle",
        ]
    )

    assert args.db_path == "/tmp/market.duckdb"
    assert args.intervals == "5,15,30"
    assert args.bucket_count == 4
    assert args.period_months == 6
    assert args.round_trip_cost_bps == 12.5
    assert args.run_id == "20260418_150000_testabcd"


def test_main_runs_research_and_prints_bundle_payload(monkeypatch, capsys) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = fake_research_bundle(
        experiment_id="market-behavior/topix100-prev-open-vs-open-entry-exit-profit",
        run_id="20260418_150000_testabcd",
    )

    monkeypatch.setattr(
        module,
        "run_topix100_prev_open_vs_open_entry_exit_profit_research",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_topix100_prev_open_vs_open_entry_exit_profit_research_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--intervals",
            "5,15,30",
            "--bucket-count",
            "4",
            "--period-months",
            "6",
            "--round-trip-cost-bps",
            "8",
            "--run-id",
            "20260418_150000_testabcd",
        ]
    )

    payload = read_bundle_payload(capsys)
    assert exit_code == 0
    assert_standard_bundle_payload(payload, run_id="20260418_150000_testabcd")
