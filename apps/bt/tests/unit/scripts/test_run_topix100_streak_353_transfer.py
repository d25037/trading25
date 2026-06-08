"""Tests for the TOPIX100 streak 3/53 transfer runner script."""

from __future__ import annotations


from tests.unit.scripts.research_runner_test_helpers import (
    assert_standard_bundle_payload,
    fake_research_bundle,
    load_research_runner_module,
    read_bundle_payload,
)


def _load_module():
    return load_research_runner_module("run_topix100_streak_353_transfer.py")


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
            "--future-horizons",
            "1",
            "5",
            "10",
            "--validation-ratio",
            "0.25",
            "--short-window-streaks",
            "3",
            "--long-window-streaks",
            "53",
            "--min-stock-events-per-state",
            "4",
            "--min-constituents-per-date-state",
            "9",
            "--output-root",
            "/tmp/research",
            "--run-id",
            "20260418_120000_testabcd",
            "--notes",
            "transfer bundle",
        ]
    )

    assert args.db_path == "/tmp/market.duckdb"
    assert args.future_horizons == [1, 5, 10]
    assert args.run_id == "20260418_120000_testabcd"


def test_main_runs_research_and_prints_bundle_payload(monkeypatch, capsys) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = fake_research_bundle(
        experiment_id="market-behavior/topix100-streak-3-53-transfer",
        run_id="20260418_120000_testabcd",
    )

    monkeypatch.setattr(
        module,
        "run_topix100_streak_353_transfer_research",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_topix100_streak_353_transfer_research_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--run-id",
            "20260418_120000_testabcd",
        ]
    )

    payload = read_bundle_payload(capsys)
    assert exit_code == 0
    assert_standard_bundle_payload(payload, run_id="20260418_120000_testabcd")
