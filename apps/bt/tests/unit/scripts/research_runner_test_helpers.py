"""Shared helpers for research runner script tests."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any


def load_research_runner_module(script_name: str) -> ModuleType:
    repo_root = Path(__file__).resolve().parents[5]
    module_name = script_name.removesuffix(".py")
    module_path = repo_root / "apps" / "bt" / "scripts" / "research" / script_name
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load research runner module: {script_name}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def fake_research_bundle(
    *,
    experiment_id: str,
    run_id: str,
    bundle_dir: Path = Path("/tmp/research/run"),
) -> SimpleNamespace:
    return SimpleNamespace(
        experiment_id=experiment_id,
        run_id=run_id,
        bundle_dir=bundle_dir,
        manifest_path=bundle_dir / "manifest.json",
        results_db_path=bundle_dir / "results.duckdb",
        summary_path=bundle_dir / "summary.md",
    )


def read_bundle_payload(capsys: Any) -> dict[str, Any]:
    return json.loads(capsys.readouterr().out)


def assert_standard_bundle_payload(
    payload: dict[str, Any],
    *,
    run_id: str,
    bundle_path: str = "/tmp/research/run",
) -> None:
    assert payload["runId"] == run_id
    assert payload["bundlePath"] == bundle_path
