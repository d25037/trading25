"""Tests for the runner-first TOPIX100 SMA ratio LightGBM script."""

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
        "run_topix100_sma_ratio_rank_future_close_lightgbm.py"
    )


def test_parse_args_accepts_lightgbm_runner_options() -> None:
    module = _load_module()

    args = module.parse_args(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2024-12-31",
            "--lookback-years",
            "7",
            "--min-constituents-per-day",
            "70",
            "--train-window",
            "504",
            "--test-window",
            "84",
            "--step",
            "84",
            "--skip-diagnostic",
            "--output-root",
            "/tmp/research",
            "--run-id",
            "20260419_120000_testabcd",
            "--notes",
            "sma ratio lightgbm bundle",
        ]
    )

    assert args.db_path == "/tmp/market.duckdb"
    assert args.lookback_years == 7
    assert args.min_constituents_per_day == 70
    assert args.train_window == 504
    assert args.test_window == 84
    assert args.step == 84
    assert args.skip_diagnostic is True
    assert args.run_id == "20260419_120000_testabcd"


def test_main_runs_lightgbm_research_and_prints_bundle_payload(
    monkeypatch,
    capsys,
) -> None:
    module = _load_module()
    fake_base_result = SimpleNamespace(
        analysis_start_date="2024-01-01",
        analysis_end_date="2024-12-31",
        lookback_years=10,
        min_constituents_per_day=80,
    )
    fake_result = object()
    fake_bundle = fake_research_bundle(
        experiment_id="market-behavior/topix100-sma-ratio-lightgbm",
        run_id="20260419_120000_testabcd",
    )

    monkeypatch.setattr(
        module,
        "run_topix100_sma_ratio_rank_future_close_research",
        lambda *args, **kwargs: fake_base_result,
    )
    monkeypatch.setattr(
        module,
        "run_topix100_sma_ratio_rank_future_close_lightgbm_research",
        lambda *args, **kwargs: fake_result,
    )

    def _write_bundle(result, *, base_result, **kwargs):
        assert result is fake_result
        assert base_result is fake_base_result
        return fake_bundle

    monkeypatch.setattr(
        module,
        "write_topix100_sma_ratio_rank_future_close_lightgbm_research_bundle",
        _write_bundle,
    )

    exit_code = module.main(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--run-id",
            "20260419_120000_testabcd",
        ]
    )

    payload = read_bundle_payload(capsys)
    assert exit_code == 0
    assert_standard_bundle_payload(payload, run_id="20260419_120000_testabcd")
