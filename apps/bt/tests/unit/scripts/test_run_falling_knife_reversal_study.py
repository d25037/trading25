"""Tests for the falling-knife reversal study runner script."""

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
        / "run_falling_knife_reversal_study.py"
    )
    spec = importlib.util.spec_from_file_location(
        "run_falling_knife_reversal_study",
        module_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load falling-knife runner module")
    module = importlib.util.module_from_spec(spec)
    sys.modules["run_falling_knife_reversal_study"] = module
    spec.loader.exec_module(module)
    return module


def test_parse_args_accepts_falling_knife_runner_options() -> None:
    module = _load_module()

    args = module.parse_args(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--start-date",
            "2024-01-01",
            "--end-date",
            "2025-12-31",
            "--market-code",
            "0111",
            "--forward-horizon",
            "20",
            "--risk-adjusted-lookback",
            "40",
            "--condition-ratio-type",
            "sharpe",
            "--min-condition-count",
            "3",
            "--max-wait-days",
            "15",
            "--signal-cooldown-days",
            "30",
            "--output-root",
            "/tmp/research",
            "--run-id",
            "20260427_120000_testabcd",
        ]
    )

    assert args.db_path == "/tmp/market.duckdb"
    assert args.start_date == "2024-01-01"
    assert args.end_date == "2025-12-31"
    assert args.market_codes == ["0111"]
    assert args.forward_horizons == [20]
    assert args.risk_adjusted_lookback == 40
    assert args.condition_ratio_type == "sharpe"
    assert args.min_condition_count == 3
    assert args.max_wait_days == 15
    assert args.signal_cooldown_days == 30
    assert args.run_id == "20260427_120000_testabcd"


def test_main_runs_falling_knife_study_and_prints_bundle_payload(
    monkeypatch,
    capsys,
) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = SimpleNamespace(
        experiment_id="market-behavior/falling-knife-reversal-study",
        run_id="20260427_120000_testabcd",
        bundle_dir=Path("/tmp/research/run"),
        manifest_path=Path("/tmp/research/run/manifest.json"),
        results_db_path=Path("/tmp/research/run/results.duckdb"),
        summary_path=Path("/tmp/research/run/summary.md"),
    )

    monkeypatch.setattr(
        module,
        "run_falling_knife_reversal_study",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_falling_knife_reversal_study_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--run-id",
            "20260427_120000_testabcd",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["runId"] == "20260427_120000_testabcd"
    assert payload["bundlePath"] == "/tmp/research/run"
