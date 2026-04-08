"""Tests for the intraday portfolio-construction walk-forward runner."""

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
        / "run_topix100_streak_353_next_session_intraday_portfolio_construction_walkforward.py"
    )
    spec = importlib.util.spec_from_file_location(
        "run_topix100_streak_353_next_session_intraday_portfolio_construction_walkforward",
        module_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load intraday portfolio-construction runner")
    module = importlib.util.module_from_spec(spec)
    sys.modules[
        "run_topix100_streak_353_next_session_intraday_portfolio_construction_walkforward"
    ] = module
    spec.loader.exec_module(module)
    return module


def test_parse_args_accepts_portfolio_construction_options() -> None:
    module = _load_module()
    args = module.parse_args(
        [
            "--reference-top-k",
            "3",
            "--absolute-selection-count",
            "5",
            "--train-window",
            "756",
            "--test-window",
            "126",
            "--step",
            "126",
            "--run-id",
            "20260408_020000_testabcd",
        ]
    )

    assert args.reference_top_k == 3
    assert args.absolute_selection_count == 5
    assert args.train_window == 756
    assert args.test_window == 126
    assert args.step == 126
    assert args.run_id == "20260408_020000_testabcd"


def test_main_runs_research_and_prints_bundle_payload(monkeypatch, capsys) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = SimpleNamespace(
        experiment_id="market-behavior/topix100-streak-3-53-next-session-intraday-portfolio-construction-walkforward",
        run_id="20260408_020000_testabcd",
        bundle_dir=Path("/tmp/research/run"),
        manifest_path=Path("/tmp/research/run/manifest.json"),
        results_db_path=Path("/tmp/research/run/results.duckdb"),
        summary_path=Path("/tmp/research/run/summary.md"),
    )

    monkeypatch.setattr(
        module,
        "run_topix100_streak_353_next_session_intraday_portfolio_construction_walkforward_research",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_topix100_streak_353_next_session_intraday_portfolio_construction_walkforward_research_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(["--run-id", "20260408_020000_testabcd"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["runId"] == "20260408_020000_testabcd"
    assert payload["bundlePath"] == "/tmp/research/run"
