"""Tests for the runner-first TOPIX100 13:30 entry to next 10:45 exit script."""

from __future__ import annotations

from tests.unit.scripts.research_runner_test_helpers import (
    assert_standard_bundle_payload,
    fake_research_bundle,
    load_research_runner_module,
    read_bundle_payload,
)


def _load_module():
    return load_research_runner_module("run_topix100_1330_entry_next_1045_exit.py")


def test_parse_args_accepts_overnight_reversal_options() -> None:
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
            "5",
            "--entry-time",
            "13:30",
            "--exit-time",
            "10:45",
            "--tail-fraction",
            "0.10",
            "--output-root",
            "/tmp/research",
            "--run-id",
            "20260416_010500_testabcd",
            "--notes",
            "overnight reversal bundle",
        ]
    )

    assert args.db_path == "/tmp/market.duckdb"
    assert args.interval_minutes == 5
    assert args.entry_time == "13:30"
    assert args.exit_time == "10:45"
    assert args.tail_fraction == 0.10
    assert args.run_id == "20260416_010500_testabcd"


def test_main_runs_research_and_prints_bundle_payload(monkeypatch, capsys) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = fake_research_bundle(
        experiment_id="market-behavior/topix100-1330-entry-next-1045-exit",
        run_id="20260416_010500_testabcd",
    )

    monkeypatch.setattr(
        module,
        "run_topix100_1330_entry_next_1045_exit_research",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_topix100_1330_entry_next_1045_exit_research_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--interval-minutes",
            "5",
            "--run-id",
            "20260416_010500_testabcd",
        ]
    )

    payload = read_bundle_payload(capsys)
    assert exit_code == 0
    assert_standard_bundle_payload(payload, run_id="20260416_010500_testabcd")
