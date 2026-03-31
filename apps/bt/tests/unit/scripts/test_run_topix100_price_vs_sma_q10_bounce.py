"""Tests for the runner-first TOPIX100 price-vs-SMA Q10 bounce script."""

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
        / "run_topix100_price_vs_sma_q10_bounce.py"
    )
    spec = importlib.util.spec_from_file_location(
        "run_topix100_price_vs_sma_q10_bounce",
        module_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load run_topix100_price_vs_sma_q10_bounce module")
    module = importlib.util.module_from_spec(spec)
    sys.modules["run_topix100_price_vs_sma_q10_bounce"] = module
    spec.loader.exec_module(module)
    return module


def test_parse_args_accepts_q10_bounce_runner_options() -> None:
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
            "--price-features",
            "price_vs_sma_50_gap,price_vs_sma_100_gap",
            "--volume-features",
            "volume_sma_5_20",
            "--output-root",
            "/tmp/research",
            "--run-id",
            "20260331_173000_testabcd",
            "--notes",
            "q10 bounce bundle",
        ]
    )

    assert args.db_path == "/tmp/market.duckdb"
    assert args.price_features == "price_vs_sma_50_gap,price_vs_sma_100_gap"
    assert args.volume_features == "volume_sma_5_20"
    assert args.run_id == "20260331_173000_testabcd"


def test_main_runs_q10_bounce_research_and_prints_bundle_payload(
    monkeypatch,
    capsys,
) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = SimpleNamespace(
        experiment_id="market-behavior/topix100-price-vs-sma-q10-bounce",
        run_id="20260331_173000_testabcd",
        bundle_dir=Path("/tmp/research/run"),
        manifest_path=Path("/tmp/research/run/manifest.json"),
        results_db_path=Path("/tmp/research/run/results.duckdb"),
        summary_path=Path("/tmp/research/run/summary.md"),
    )

    monkeypatch.setattr(
        module,
        "run_topix100_price_vs_sma_q10_bounce_research",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_topix100_price_vs_sma_q10_bounce_research_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--run-id",
            "20260331_173000_testabcd",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["runId"] == "20260331_173000_testabcd"
    assert payload["bundlePath"] == "/tmp/research/run"
