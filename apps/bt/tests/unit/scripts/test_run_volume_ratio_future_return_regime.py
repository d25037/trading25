"""Tests for the volume-ratio future-return regime runner script."""

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
        / "run_volume_ratio_future_return_regime.py"
    )
    spec = importlib.util.spec_from_file_location(
        "run_volume_ratio_future_return_regime",
        module_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load volume-ratio future-return runner module")
    module = importlib.util.module_from_spec(spec)
    sys.modules["run_volume_ratio_future_return_regime"] = module
    spec.loader.exec_module(module)
    return module


def test_parse_args_accepts_volume_ratio_future_return_runner_options() -> None:
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
            "--validation-ratio",
            "0.2",
            "--short-windows",
            "5,20,50",
            "--long-windows",
            "20,50,150",
            "--thresholds",
            "1.2,1.7",
            "--horizons",
            "5,10",
            "--sample-seed",
            "7",
            "--sample-size-per-universe",
            "3",
            "--sample-event-size-per-universe",
            "2",
            "--min-signal-events",
            "4",
            "--min-unique-codes",
            "2",
            "--top-k",
            "2",
            "--output-root",
            "/tmp/research",
            "--run-id",
            "20260414_120000_testabcd",
        ]
    )

    assert args.db_path == "/tmp/market.duckdb"
    assert args.lookback_years == 5
    assert args.validation_ratio == 0.2
    assert args.short_windows == (5, 20, 50)
    assert args.long_windows == (20, 50, 150)
    assert args.thresholds == (1.2, 1.7)
    assert args.horizons == (5, 10)
    assert args.top_k == 2
    assert args.run_id == "20260414_120000_testabcd"


def test_ensure_bt_root_on_path_inserts_when_missing() -> None:
    module = _load_module()
    bt_root_str = str(Path(module.__file__).resolve().parents[2])
    sys.path = [entry for entry in sys.path if entry != bt_root_str]

    inserted_root = module._ensure_bt_root_on_path()

    assert str(inserted_root) == bt_root_str
    assert sys.path[0] == bt_root_str


def test_main_runs_volume_ratio_future_return_research_and_prints_bundle_payload(
    monkeypatch,
    capsys,
) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = SimpleNamespace(
        experiment_id="market-behavior/volume-ratio-future-return-regime",
        run_id="20260414_120000_testabcd",
        bundle_dir=Path("/tmp/research/run"),
        manifest_path=Path("/tmp/research/run/manifest.json"),
        results_db_path=Path("/tmp/research/run/results.duckdb"),
        summary_path=Path("/tmp/research/run/summary.md"),
    )

    monkeypatch.setattr(
        module,
        "run_volume_ratio_future_return_regime_research",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_volume_ratio_future_return_regime_research_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--run-id",
            "20260414_120000_testabcd",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["runId"] == "20260414_120000_testabcd"
    assert payload["bundlePath"] == "/tmp/research/run"


def test_main_module_executes_as_script(monkeypatch) -> None:
    repo_root = Path(__file__).resolve().parents[5]
    module_path = (
        repo_root
        / "apps"
        / "bt"
        / "scripts"
        / "research"
        / "run_volume_ratio_future_return_regime.py"
    )
    bt_root = module_path.resolve().parents[2]
    bt_root_str = str(bt_root)
    if bt_root_str not in sys.path:
        sys.path.append(bt_root_str)

    import scripts.research.common as common
    import src.domains.analytics.volume_ratio_future_return_regime as research_module

    fake_bundle = SimpleNamespace(
        bundle_dir=Path("/tmp/research/run"),
    )

    monkeypatch.setattr(
        research_module,
        "run_volume_ratio_future_return_regime_research",
        lambda *args, **kwargs: object(),
    )
    monkeypatch.setattr(
        research_module,
        "write_volume_ratio_future_return_regime_research_bundle",
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
            "20260414_120000_testabcd",
        ],
    )

    with pytest.raises(SystemExit) as exc:
        runpy.run_path(str(module_path), run_name="__main__")

    assert exc.value.code == 0
