from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import SimpleNamespace


def _load_module(script_name: str):
    repo_root = Path(__file__).resolve().parents[5]
    module_path = repo_root / "apps" / "bt" / "scripts" / "research" / script_name
    module_name = script_name.removesuffix(".py")
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load {script_name}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_market_bubble_footprint_runner_parses_options() -> None:
    module = _load_module("run_market_bubble_footprint.py")

    args = module.parse_args(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--start-date",
            "2020-01-31",
            "--end-date",
            "2026-05-29",
            "--return-horizons",
            "20,60,120",
            "--markets",
            "prime,standard",
            "--frequency",
            "weekly",
            "--run-id",
            "bubble",
        ]
    )

    assert args.db_path == "/tmp/market.duckdb"
    assert args.start_date == "2020-01-31"
    assert args.end_date == "2026-05-29"
    assert args.return_horizons == (20, 60, 120)
    assert args.markets == ("prime", "standard")
    assert args.frequency == "weekly"
    assert args.run_id == "bubble"


def test_rerating_bubble_regime_runner_prints_bundle_payload(monkeypatch, capsys) -> None:
    module = _load_module("run_rerating_bubble_regime_forward_response.py")
    fake_result = object()
    fake_bundle = SimpleNamespace(
        experiment_id="market-behavior/rerating-bubble-regime-forward-response",
        run_id="rerating-bubble",
        bundle_dir=Path("/tmp/research/rerating-bubble"),
        manifest_path=Path("/tmp/research/rerating-bubble/manifest.json"),
        results_db_path=Path("/tmp/research/rerating-bubble/results.duckdb"),
        summary_path=Path("/tmp/research/rerating-bubble/summary.md"),
    )
    calls: dict[str, object] = {}

    def fake_run(*args, **kwargs):
        calls["run_args"] = args
        calls["run_kwargs"] = kwargs
        return fake_result

    monkeypatch.setattr(
        module,
        "run_rerating_bubble_regime_forward_response_research",
        fake_run,
    )
    monkeypatch.setattr(
        module,
        "write_rerating_bubble_regime_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--signal-horizons",
            "20,60",
            "--footprint-horizons",
            "60,120",
            "--markets",
            "prime",
            "--run-id",
            "rerating-bubble",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["runId"] == "rerating-bubble"
    assert payload["resultsDbPath"] == "/tmp/research/rerating-bubble/results.duckdb"
    assert calls["run_kwargs"]["signal_horizons"] == (20, 60)
    assert calls["run_kwargs"]["footprint_horizons"] == (60, 120)
