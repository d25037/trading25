"""Tests for the runner-first TOPIX100 price/SMA50 decile partition script."""

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
        / "run_topix100_price_to_sma50_decile_partitions.py"
    )
    spec = importlib.util.spec_from_file_location(
        "run_topix100_price_to_sma50_decile_partitions",
        module_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(
            "Failed to load run_topix100_price_to_sma50_decile_partitions module"
        )
    module = importlib.util.module_from_spec(spec)
    sys.modules["run_topix100_price_to_sma50_decile_partitions"] = module
    spec.loader.exec_module(module)
    return module


def test_parse_args_accepts_partition_runner_options() -> None:
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
            "--output-root",
            "/tmp/research",
            "--run-id",
            "20260403_120000_testabcd",
            "--notes",
            "partition bundle",
        ]
    )

    assert args.db_path == "/tmp/market.duckdb"
    assert args.start_date == "2024-01-01"
    assert args.end_date == "2024-12-31"
    assert args.lookback_years == 7
    assert args.min_constituents_per_day == 70
    assert args.run_id == "20260403_120000_testabcd"


def test_main_runs_partition_research_and_prints_bundle_payload(
    monkeypatch,
    capsys,
) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = SimpleNamespace(
        experiment_id="market-behavior/topix100-price-to-sma50-decile-partitions",
        run_id="20260403_120000_testabcd",
        bundle_dir=Path("/tmp/research/run"),
        manifest_path=Path("/tmp/research/run/manifest.json"),
        results_db_path=Path("/tmp/research/run/results.duckdb"),
        summary_path=Path("/tmp/research/run/summary.md"),
    )

    monkeypatch.setattr(
        module,
        "run_topix100_price_to_sma50_decile_partitions_research",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_topix100_price_to_sma50_decile_partitions_research_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--run-id",
            "20260403_120000_testabcd",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["runId"] == "20260403_120000_testabcd"
    assert payload["bundlePath"] == "/tmp/research/run"
