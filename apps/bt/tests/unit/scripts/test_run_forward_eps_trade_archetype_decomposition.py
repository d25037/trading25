"""Tests for the forward EPS trade-archetype runner script."""

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
        / "run_forward_eps_trade_archetype_decomposition.py"
    )
    spec = importlib.util.spec_from_file_location(
        "run_forward_eps_trade_archetype_decomposition",
        module_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load forward EPS archetype runner module")
    module = importlib.util.module_from_spec(spec)
    sys.modules["run_forward_eps_trade_archetype_decomposition"] = module
    spec.loader.exec_module(module)
    return module


def test_parse_args_accepts_custom_options() -> None:
    module = _load_module()

    args = module.parse_args(
        [
            "--strategy",
            "production/forward_eps_driven",
            "--dataset",
            "primeExTopix500_20260325",
            "--holdout-months",
            "9",
            "--severe-loss-threshold-pct",
            "-12.5",
            "--quantile-buckets",
            "6",
        ]
    )

    assert args.strategy == "production/forward_eps_driven"
    assert args.dataset == "primeExTopix500_20260325"
    assert args.holdout_months == 9
    assert args.severe_loss_threshold_pct == -12.5
    assert args.quantile_buckets == 6


def test_main_runs_research_and_prints_bundle_payload(monkeypatch, capsys) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = SimpleNamespace(
        experiment_id="strategy-audit/forward-eps-trade-archetype-decomposition",
        run_id="20260418_120000_testabcd",
        bundle_dir=Path("/tmp/research/run"),
        manifest_path=Path("/tmp/research/run/manifest.json"),
        results_db_path=Path("/tmp/research/run/results.duckdb"),
        summary_path=Path("/tmp/research/run/summary.md"),
    )
    recorded: dict[str, object] = {}

    monkeypatch.setattr(
        module,
        "run_forward_eps_trade_archetype_decomposition",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_forward_eps_trade_archetype_decomposition_bundle",
        lambda result, **kwargs: fake_bundle,
    )
    monkeypatch.setattr(
        module,
        "ensure_bt_workdir",
        lambda bt_root: recorded.setdefault("bt_root", bt_root),
    )

    exit_code = module.main(["--run-id", "20260418_120000_testabcd"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert recorded["bt_root"] == module._BT_ROOT
    assert payload["runId"] == "20260418_120000_testabcd"
    assert payload["bundlePath"] == "/tmp/research/run"
