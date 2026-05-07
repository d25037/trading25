"""Tests for the falling-knife long-horizon technical profile runner script."""

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
        / "run_falling_knife_long_horizon_technical_profile.py"
    )
    spec = importlib.util.spec_from_file_location(
        "run_falling_knife_long_horizon_technical_profile",
        module_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load falling-knife long-horizon technical runner module")
    module = importlib.util.module_from_spec(spec)
    sys.modules["run_falling_knife_long_horizon_technical_profile"] = module
    spec.loader.exec_module(module)
    return module


def test_parse_args_accepts_long_horizon_technical_options() -> None:
    module = _load_module()

    args = module.parse_args(
        [
            "--input-bundle",
            "/tmp/input",
            "--horizon-days",
            "60",
            "--bucket-count",
            "4",
            "--severe-loss-threshold",
            "-0.15",
            "--output-root",
            "/tmp/research",
            "--run-id",
            "20260506_long_tech",
        ]
    )

    assert args.input_bundle == "/tmp/input"
    assert args.horizon_days == 60
    assert args.bucket_count == 4
    assert args.severe_loss_threshold == -0.15
    assert args.run_id == "20260506_long_tech"


def test_main_runs_long_horizon_technical_profile_and_prints_bundle_payload(
    monkeypatch,
    capsys,
) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = SimpleNamespace(
        experiment_id="market-behavior/falling-knife-long-horizon-technical-profile",
        run_id="20260506_long_tech",
        bundle_dir=Path("/tmp/research/run"),
        manifest_path=Path("/tmp/research/run/manifest.json"),
        results_db_path=Path("/tmp/research/run/results.duckdb"),
        summary_path=Path("/tmp/research/run/summary.md"),
    )

    monkeypatch.setattr(
        module,
        "run_falling_knife_long_horizon_technical_profile",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_falling_knife_long_horizon_technical_profile_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--input-bundle",
            "/tmp/input",
            "--run-id",
            "20260506_long_tech",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["runId"] == "20260506_long_tech"
    assert payload["bundlePath"] == "/tmp/research/run"
