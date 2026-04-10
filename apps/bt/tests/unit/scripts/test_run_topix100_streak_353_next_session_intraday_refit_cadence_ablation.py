"""Tests for the intraday refit-cadence ablation runner."""

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
        / "run_topix100_streak_353_next_session_intraday_refit_cadence_ablation.py"
    )
    spec = importlib.util.spec_from_file_location(
        "run_topix100_streak_353_next_session_intraday_refit_cadence_ablation",
        module_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load intraday refit-cadence ablation runner")
    module = importlib.util.module_from_spec(spec)
    sys.modules["run_topix100_streak_353_next_session_intraday_refit_cadence_ablation"] = (
        module
    )
    spec.loader.exec_module(module)
    return module


def test_parse_args_accepts_refit_cadence_options() -> None:
    module = _load_module()
    args = module.parse_args(
        [
            "--refit-cadence-days",
            "1",
            "5",
            "20",
            "63",
            "126",
            "--reference-cadence-days",
            "126",
            "--train-window",
            "756",
            "--purge-signal-dates",
            "1",
            "--run-id",
            "20260409_210000_testabcd",
        ]
    )

    assert args.refit_cadence_days == [1, 5, 20, 63, 126]
    assert args.reference_cadence_days == 126
    assert args.train_window == 756
    assert args.purge_signal_dates == 1
    assert args.run_id == "20260409_210000_testabcd"


def test_main_runs_research_and_prints_bundle_payload(monkeypatch, capsys) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = SimpleNamespace(
        experiment_id="market-behavior/topix100-streak-3-53-next-session-intraday-refit-cadence-ablation",
        run_id="20260409_210000_testabcd",
        bundle_dir=Path("/tmp/research/run"),
        manifest_path=Path("/tmp/research/run/manifest.json"),
        results_db_path=Path("/tmp/research/run/results.duckdb"),
        summary_path=Path("/tmp/research/run/summary.md"),
    )

    monkeypatch.setattr(
        module,
        "run_topix100_streak_353_next_session_intraday_refit_cadence_ablation_research",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_topix100_streak_353_next_session_intraday_refit_cadence_ablation_research_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(["--run-id", "20260409_210000_testabcd"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["runId"] == "20260409_210000_testabcd"
    assert payload["bundlePath"] == "/tmp/research/run"
