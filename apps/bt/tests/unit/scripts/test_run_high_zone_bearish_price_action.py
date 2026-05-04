"""Tests for the high-zone bearish price-action runner script."""

from __future__ import annotations

import importlib.util
import json
import runpy
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


def _load_module():
    repo_root = Path(__file__).resolve().parents[5]
    module_path = (
        repo_root
        / "apps"
        / "bt"
        / "scripts"
        / "research"
        / "run_high_zone_bearish_price_action.py"
    )
    spec = importlib.util.spec_from_file_location(
        "run_high_zone_bearish_price_action",
        module_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load high-zone bearish price-action runner module")
    module = importlib.util.module_from_spec(spec)
    sys.modules["run_high_zone_bearish_price_action"] = module
    spec.loader.exec_module(module)
    return module


def test_parse_args_accepts_high_zone_bearish_price_action_options() -> None:
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
            "5",
            "--horizons",
            "1,5,10",
            "--sample-event-size",
            "4",
            "--min-events-for-selection",
            "7",
            "--output-root",
            "/tmp/research",
            "--run-id",
            "20260101_000000_test",
        ]
    )

    assert args.db_path == "/tmp/market.duckdb"
    assert args.lookback_years == 5
    assert args.horizons == (1, 5, 10)
    assert args.sample_event_size == 4
    assert args.min_events_for_selection == 7
    assert args.run_id == "20260101_000000_test"


def test_main_runs_high_zone_bearish_price_action_research_and_prints_bundle_payload(
    monkeypatch,
    capsys,
) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = SimpleNamespace(
        experiment_id="market-behavior/high-zone-bearish-price-action",
        run_id="20260101_000000_test",
        bundle_dir=Path("/tmp/research/run"),
        manifest_path=Path("/tmp/research/run/manifest.json"),
        results_db_path=Path("/tmp/research/run/results.duckdb"),
        summary_path=Path("/tmp/research/run/summary.md"),
    )

    monkeypatch.setattr(
        module,
        "run_high_zone_bearish_price_action_research",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_high_zone_bearish_price_action_research_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--run-id",
            "20260101_000000_test",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["runId"] == "20260101_000000_test"
    assert payload["bundlePath"] == "/tmp/research/run"


def test_main_module_executes_as_script(monkeypatch) -> None:
    repo_root = Path(__file__).resolve().parents[5]
    module_path = (
        repo_root
        / "apps"
        / "bt"
        / "scripts"
        / "research"
        / "run_high_zone_bearish_price_action.py"
    )
    bt_root = module_path.resolve().parents[2]
    bt_root_str = str(bt_root)
    if bt_root_str not in sys.path:
        sys.path.append(bt_root_str)

    import scripts.research.common as common
    import src.domains.analytics.high_zone_bearish_price_action as research_module

    fake_bundle = SimpleNamespace(
        bundle_dir=Path("/tmp/research/run"),
    )

    monkeypatch.setattr(
        research_module,
        "run_high_zone_bearish_price_action_research",
        lambda *args, **kwargs: object(),
    )
    monkeypatch.setattr(
        research_module,
        "write_high_zone_bearish_price_action_research_bundle",
        lambda *args, **kwargs: fake_bundle,
    )
    monkeypatch.setattr(common, "emit_bundle_payload", lambda bundle: None)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(module_path),
            "--db-path",
            "/tmp/market.duckdb",
            "--run-id",
            "20260101_000000_test",
        ],
    )

    with pytest.raises(SystemExit) as exc:
        runpy.run_path(str(module_path), run_name="__main__")

    assert exc.value.code == 0
