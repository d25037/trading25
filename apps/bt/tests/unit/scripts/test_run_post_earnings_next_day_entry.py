"""Tests for the post-earnings next-day entry runner."""

from __future__ import annotations

from tests.unit.scripts.research_runner_test_helpers import (
    assert_standard_bundle_payload,
    fake_research_bundle,
    load_research_runner_module,
    read_bundle_payload,
)


def _load_module():
    return load_research_runner_module("run_post_earnings_next_day_entry.py")


def test_parse_args_accepts_post_earnings_options() -> None:
    module = _load_module()

    args = module.parse_args(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2024-12-31",
            "--pre-windows",
            "20,60",
            "--horizons",
            "1,5,20",
            "--liquidity-window",
            "60",
            "--run-id",
            "20260515_test",
        ]
    )

    assert args.db_path == "/tmp/market.duckdb"
    assert args.start_date == "2024-01-01"
    assert args.end_date == "2024-12-31"
    assert args.pre_windows == "20,60"
    assert args.horizons == "1,5,20"
    assert args.liquidity_window == 60
    assert args.run_id == "20260515_test"


def test_main_runs_post_earnings_and_prints_bundle_payload(
    monkeypatch,
    capsys,
) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = fake_research_bundle(
        experiment_id="market-behavior/post-earnings-next-day-entry",
        run_id="20260515_test",
    )

    monkeypatch.setattr(
        module,
        "run_post_earnings_next_day_entry_research",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_post_earnings_next_day_entry_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--run-id",
            "20260515_test",
        ]
    )

    payload = read_bundle_payload(capsys)
    assert exit_code == 0
    assert_standard_bundle_payload(payload, run_id="20260515_test")
