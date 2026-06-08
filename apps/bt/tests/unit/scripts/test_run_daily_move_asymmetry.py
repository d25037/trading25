"""Tests for the daily move asymmetry runner script."""

from __future__ import annotations

from tests.unit.scripts.research_runner_test_helpers import (
    assert_standard_bundle_payload,
    fake_research_bundle,
    load_research_runner_module,
    read_bundle_payload,
)


def _load_module():
    return load_research_runner_module("run_daily_move_asymmetry.py")


def test_parse_args_accepts_daily_move_asymmetry_options() -> None:
    module = _load_module()

    args = module.parse_args(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--start-date",
            "2020-01-01",
            "--end-date",
            "2025-12-31",
            "--horizons",
            "1,5,20",
            "--rolling-vol-window",
            "40",
            "--min-observations",
            "50",
            "--severe-loss-threshold-pct",
            "-7.5",
            "--observation-sample-limit",
            "500",
            "--output-root",
            "/tmp/research",
            "--run-id",
            "daily-asymmetry",
        ]
    )

    assert args.db_path == "/tmp/market.duckdb"
    assert args.start_date == "2020-01-01"
    assert args.end_date == "2025-12-31"
    assert args.horizons == (1, 5, 20)
    assert args.rolling_vol_window == 40
    assert args.min_observations == 50
    assert args.severe_loss_threshold_pct == -7.5
    assert args.observation_sample_limit == 500
    assert args.output_root == "/tmp/research"
    assert args.run_id == "daily-asymmetry"


def test_main_runs_daily_move_asymmetry_and_prints_bundle_payload(
    monkeypatch,
    capsys,
) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = fake_research_bundle(
        experiment_id="market-behavior/daily-move-asymmetry",
        run_id="daily-asymmetry",
    )

    monkeypatch.setattr(
        module,
        "run_daily_move_asymmetry_research",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_daily_move_asymmetry_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--run-id",
            "daily-asymmetry",
        ]
    )

    payload = read_bundle_payload(capsys)
    assert exit_code == 0
    assert_standard_bundle_payload(payload, run_id="daily-asymmetry")
    assert payload["resultsDbPath"] == "/tmp/research/run/results.duckdb"
