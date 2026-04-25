from __future__ import annotations

import json
from pathlib import Path


def test_backtest_run_manifest_versions_do_not_require_marimo() -> None:
    repo_root = Path(__file__).resolve().parents[5]
    schema_path = repo_root / "contracts" / "backtest-run-manifest-v1.schema.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))

    required_versions = schema["properties"]["versions"]["required"]
    assert "marimo" not in required_versions
