"""Tests for the forward EPS component decomposition runner."""

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
        / "run_forward_eps_component_decomposition.py"
    )
    spec = importlib.util.spec_from_file_location(
        "run_forward_eps_component_decomposition",
        module_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load forward EPS component runner module")
    module = importlib.util.module_from_spec(spec)
    sys.modules["run_forward_eps_component_decomposition"] = module
    spec.loader.exec_module(module)
    return module


def test_parse_args_accepts_component_options() -> None:
    module = _load_module()

    args = module.parse_args(
        [
            "--input-bundle",
            "/tmp/forward-eps/run",
            "--quantile-buckets",
            "4",
            "--severe-loss-threshold-pct",
            "-12",
            "--output-root",
            "/tmp/research",
            "--run-id",
            "20260509_test",
        ]
    )

    assert args.input_bundle == "/tmp/forward-eps/run"
    assert args.quantile_buckets == 4
    assert args.severe_loss_threshold_pct == -12.0
    assert args.run_id == "20260509_test"


def test_main_runs_component_decomposition_and_prints_bundle_payload(
    monkeypatch,
    capsys,
) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = SimpleNamespace(
        experiment_id="strategy-audit/forward-eps-component-decomposition",
        run_id="20260509_test",
        bundle_dir=Path("/tmp/research/run"),
        manifest_path=Path("/tmp/research/run/manifest.json"),
        results_db_path=Path("/tmp/research/run/results.duckdb"),
        summary_path=Path("/tmp/research/run/summary.md"),
    )

    monkeypatch.setattr(
        module,
        "run_forward_eps_component_decomposition",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_forward_eps_component_decomposition_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(
        [
            "--input-bundle",
            "/tmp/forward-eps/run",
            "--run-id",
            "20260509_test",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["runId"] == "20260509_test"
    assert payload["bundlePath"] == "/tmp/research/run"
