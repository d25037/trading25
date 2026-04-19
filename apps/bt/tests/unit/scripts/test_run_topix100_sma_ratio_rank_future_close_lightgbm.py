"""Tests for the runner-first TOPIX100 SMA ratio LightGBM script."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import SimpleNamespace


def _load_module():
    repo_root = Path(__file__).resolve().parents[5]
    module_path = (
        repo_root
        / "apps"
        / "bt"
        / "scripts"
        / "research"
        / "run_topix100_sma_ratio_rank_future_close_lightgbm.py"
    )
    spec = importlib.util.spec_from_file_location(
        "run_topix100_sma_ratio_rank_future_close_lightgbm",
        module_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(
            "Failed to load run_topix100_sma_ratio_rank_future_close_lightgbm module"
        )
    module = importlib.util.module_from_spec(spec)
    sys.modules["run_topix100_sma_ratio_rank_future_close_lightgbm"] = module
    spec.loader.exec_module(module)
    return module


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
    fake_bundle = SimpleNamespace(
        experiment_id="market-behavior/topix100-sma-ratio-lightgbm",
        run_id="20260419_120000_testabcd",
        bundle_dir=Path("/tmp/research/run"),
        manifest_path=Path("/tmp/research/run/manifest.json"),
        results_db_path=Path("/tmp/research/run/results.duckdb"),
        summary_path=Path("/tmp/research/run/summary.md"),
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

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["runId"] == "20260419_120000_testabcd"
    assert payload["bundlePath"] == "/tmp/research/run"
