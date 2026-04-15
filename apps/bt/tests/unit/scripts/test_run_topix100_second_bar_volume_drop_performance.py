"""Tests for the TOPIX100 second-bar volume drop performance runner script."""

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
        / "run_topix100_second_bar_volume_drop_performance.py"
    )
    spec = importlib.util.spec_from_file_location(
        "run_topix100_second_bar_volume_drop_performance",
        module_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(
            "Failed to load run_topix100_second_bar_volume_drop_performance module"
        )
    module = importlib.util.module_from_spec(spec)
    sys.modules["run_topix100_second_bar_volume_drop_performance"] = module
    spec.loader.exec_module(module)
    return module


def test_parse_args_accepts_runner_options() -> None:
    module = _load_module()

    args = module.parse_args(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--start-date",
            "2024-04-15",
            "--end-date",
            "2026-04-14",
            "--intervals",
            "5,15,30",
            "--drop-percentile",
            "0.2",
            "--performance-start-time",
            "10:30",
            "--performance-end-time",
            "13:30",
            "--output-root",
            "/tmp/research",
            "--run-id",
            "20260415_140000_testabcd",
            "--notes",
            "second-bar volume drop bundle",
        ]
    )

    assert args.db_path == "/tmp/market.duckdb"
    assert args.intervals == "5,15,30"
    assert args.drop_percentile == 0.2
    assert args.performance_start_time == "10:30"
    assert args.performance_end_time == "13:30"
    assert args.run_id == "20260415_140000_testabcd"


def test_main_runs_research_and_prints_bundle_payload(monkeypatch, capsys) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = SimpleNamespace(
        experiment_id="market-behavior/topix100-second-bar-volume-drop-window-performance",
        run_id="20260415_140000_testabcd",
        bundle_dir=Path("/tmp/research/run"),
        manifest_path=Path("/tmp/research/run/manifest.json"),
        results_db_path=Path("/tmp/research/run/results.duckdb"),
        summary_path=Path("/tmp/research/run/summary.md"),
    )

    monkeypatch.setattr(
        module,
        "run_topix100_second_bar_volume_drop_performance_research",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_topix100_second_bar_volume_drop_performance_research_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--intervals",
            "5,15,30",
            "--drop-percentile",
            "0.2",
            "--performance-start-time",
            "10:30",
            "--performance-end-time",
            "13:30",
            "--run-id",
            "20260415_140000_testabcd",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["runId"] == "20260415_140000_testabcd"
    assert payload["bundlePath"] == "/tmp/research/run"
