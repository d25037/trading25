"""Tests for the TOPIX100 duplicate-policy analysis runner."""

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
        / "run_topix100_top1_open_to_open_5d_duplicate_policy_analysis.py"
    )
    spec = importlib.util.spec_from_file_location(
        "run_topix100_top1_open_to_open_5d_duplicate_policy_analysis",
        module_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load duplicate-policy analysis runner")
    module = importlib.util.module_from_spec(spec)
    sys.modules["run_topix100_top1_open_to_open_5d_duplicate_policy_analysis"] = module
    spec.loader.exec_module(module)
    return module


def test_parse_args_accepts_duplicate_policy_options() -> None:
    module = _load_module()
    args = module.parse_args(
        [
            "--fallback-candidate-top-k",
            "5",
            "--duplicate-policies",
            "allow_stack",
            "skip_if_held",
            "next_unique_within_top5",
            "--run-id",
            "20260413_170000_testabcd",
        ]
    )

    assert args.fallback_candidate_top_k == 5
    assert args.duplicate_policies == [
        "allow_stack",
        "skip_if_held",
        "next_unique_within_top5",
    ]
    assert args.run_id == "20260413_170000_testabcd"


def test_main_runs_research_and_prints_bundle_payload(monkeypatch, capsys) -> None:
    module = _load_module()
    fake_result = object()
    fake_bundle = SimpleNamespace(
        experiment_id="market-behavior/topix100-top1-open-to-open-5d-duplicate-policy-analysis",
        run_id="20260413_170000_testabcd",
        bundle_dir=Path("/tmp/research/run"),
        manifest_path=Path("/tmp/research/run/manifest.json"),
        results_db_path=Path("/tmp/research/run/results.duckdb"),
        summary_path=Path("/tmp/research/run/summary.md"),
    )

    monkeypatch.setattr(
        module,
        "run_topix100_top1_open_to_open_5d_duplicate_policy_analysis",
        lambda *args, **kwargs: fake_result,
    )
    monkeypatch.setattr(
        module,
        "write_topix100_top1_open_to_open_5d_duplicate_policy_analysis_bundle",
        lambda result, **kwargs: fake_bundle,
    )

    exit_code = module.main(["--run-id", "20260413_170000_testabcd"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["runId"] == "20260413_170000_testabcd"
    assert payload["bundlePath"] == "/tmp/research/run"
