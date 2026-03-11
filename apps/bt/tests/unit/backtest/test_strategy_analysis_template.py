"""Tests for the backtest strategy analysis template module."""

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


def test_strategy_analysis_template_exports_marimo_app():
    module = _load_template_module()

    assert getattr(module, "app", None) is not None
