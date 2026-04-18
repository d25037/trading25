"""Tests for shared research runner helpers."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from types import SimpleNamespace

from scripts.research.common import (
    add_bundle_output_arguments,
    emit_bundle_payload,
    ensure_bt_workdir,
)


def test_add_bundle_output_arguments_and_emit_bundle_payload(capsys) -> None:
    parser = argparse.ArgumentParser()
    add_bundle_output_arguments(parser)

    args = parser.parse_args(["--run-id", "demo", "--notes", "memo"])
    emit_bundle_payload(
        SimpleNamespace(
            experiment_id="strategy-audit/demo",
            run_id="demo",
            bundle_dir=Path("/tmp/bundle"),
            manifest_path=Path("/tmp/bundle/manifest.json"),
            results_db_path=Path("/tmp/bundle/results.duckdb"),
            summary_path=Path("/tmp/bundle/summary.md"),
        )
    )

    payload = json.loads(capsys.readouterr().out)
    assert args.output_root is None
    assert args.run_id == "demo"
    assert args.notes == "memo"
    assert payload["experimentId"] == "strategy-audit/demo"
    assert payload["bundlePath"] == "/tmp/bundle"


def test_ensure_bt_workdir_switches_and_stabilizes_cwd(
    tmp_path,
    monkeypatch,
) -> None:
    start_dir = tmp_path / "start"
    bt_root = tmp_path / "bt"
    start_dir.mkdir()
    bt_root.mkdir()
    monkeypatch.chdir(start_dir)

    resolved = ensure_bt_workdir(bt_root)
    resolved_again = ensure_bt_workdir(bt_root)

    assert Path.cwd() == bt_root
    assert resolved == bt_root.resolve()
    assert resolved_again == bt_root.resolve()
