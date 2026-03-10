"""Tests for the backtest strategy analysis template helpers."""

from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_template_module():
    module_path = (
        Path(__file__).resolve().parents[3]
        / "notebooks"
        / "templates"
        / "strategy_analysis.py"
    )
    spec = importlib.util.spec_from_file_location("strategy_analysis_template", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_load_simulation_payload_returns_empty_values_for_corrupt_pickle(tmp_path: Path):
    module = _load_template_module()
    payload_path = tmp_path / "broken.simulation.pkl"
    payload_path.write_bytes(b"not-a-pickle")

    result = module._load_simulation_payload(str(payload_path))

    assert result == (None, None, None, None)
