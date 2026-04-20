"""Tests for the speculative volume-surge follow-on runner script."""

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
        / "run_speculative_volume_surge_follow_on.py"
    )
    spec = importlib.util.spec_from_file_location(
        "run_speculative_volume_surge_follow_on",
        module_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load speculative volume-surge follow-on runner")
    module = importlib.util.module_from_spec(spec)
    sys.modules["run_speculative_volume_surge_follow_on"] = module
    spec.loader.exec_module(module)
    return module


def test_parse_args_accepts_speculative_volume_surge_runner_options() -> None:
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
            "--price-jump-threshold",
            "0.12",
            "--volume-ratio-threshold",
            "8.0",
            "--volume-window",
            "30",
            "--adv-window",
            "15",
            "--extension-windows",
            "0,1,5",
            "--full-extension-windows",
            "20,60",
            "--follow-on-gaps",
            "5,10",
            "--follow-on-windows",
            "20,40",
            "--cooldown-sessions",
            "15",
            "--sample-size",
            "4",
            "--primary-extension-window",
            "5",
            "--primary-gap",
            "10",
            "--primary-follow-on-window",
            "40",
            "--output-root",
            "/tmp/research",
            "--run-id",
            "20260420_120000_testabcd",
        ]
    )

    assert args.db_path == "/tmp/market.duckdb"
    assert args.lookback_years == 5
    assert args.price_jump_threshold == 0.12
    assert args.volume_ratio_threshold == 8.0
    assert args.extension_windows == (0, 1, 5)
    assert args.follow_on_gaps == (5, 10)
    assert args.primary_gap == 10
    assert args.run_id == "20260420_120000_testabcd"


def test_ensure_bt_root_on_path_inserts_when_missing() -> None:
    module = _load_module()
    bt_root_str = str(Path(module.__file__).resolve().parents[2])
    sys.path = [entry for entry in sys.path if entry != bt_root_str]

    inserted_root = module._ensure_bt_root_on_path()

    assert str(inserted_root) == bt_root_str
    assert sys.path[0] == bt_root_str


def test_main_runs_speculative_volume_surge_follow_on_and_prints_payload(
    monkeypatch,
    capsys,
) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = SimpleNamespace(
        experiment_id="market-behavior/speculative-volume-surge-follow-on",
        run_id="20260420_120000_testabcd",
        bundle_dir=Path("/tmp/research/run"),
        manifest_path=Path("/tmp/research/run/manifest.json"),
        results_db_path=Path("/tmp/research/run/results.duckdb"),
        summary_path=Path("/tmp/research/run/summary.md"),
    )

    monkeypatch.setattr(
        module,
        "run_speculative_volume_surge_follow_on_research",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_speculative_volume_surge_follow_on_research_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--db-path",
            "/tmp/market.duckdb",
            "--run-id",
            "20260420_120000_testabcd",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["runId"] == "20260420_120000_testabcd"
    assert payload["bundlePath"] == "/tmp/research/run"


def test_main_module_executes_as_script(monkeypatch) -> None:
    repo_root = Path(__file__).resolve().parents[5]
    module_path = (
        repo_root
        / "apps"
        / "bt"
        / "scripts"
        / "research"
        / "run_speculative_volume_surge_follow_on.py"
    )
    bt_root = module_path.resolve().parents[2]
    bt_root_str = str(bt_root)
    if bt_root_str not in sys.path:
        sys.path.append(bt_root_str)

    import scripts.research.common as common
    import src.domains.analytics.speculative_volume_surge_follow_on as research_module

    fake_bundle = SimpleNamespace(bundle_dir=Path("/tmp/research/run"))

    monkeypatch.setattr(
        research_module,
        "run_speculative_volume_surge_follow_on_research",
        lambda *args, **kwargs: object(),
    )
    monkeypatch.setattr(
        research_module,
        "write_speculative_volume_surge_follow_on_research_bundle",
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
            "20260420_120000_testabcd",
        ],
    )

    with pytest.raises(SystemExit) as exc:
        runpy.run_path(str(module_path), run_name="__main__")

    assert exc.value.code == 0
