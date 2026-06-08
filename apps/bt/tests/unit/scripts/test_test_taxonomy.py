"""Tests for scripts/ci/test_taxonomy.py."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _load_module():
    repo_root = Path(__file__).resolve().parents[5]
    module_path = repo_root / "scripts" / "ci" / "test_taxonomy.py"
    spec = importlib.util.spec_from_file_location("test_taxonomy", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load test_taxonomy module")
    module = importlib.util.module_from_spec(spec)
    sys.modules["test_taxonomy"] = module
    spec.loader.exec_module(module)
    return module


def test_production_analytics_modules_are_product_not_research() -> None:
    module = _load_module()

    assert module.is_product_path(
        "apps/bt/src/domains/analytics/screening_evaluator.py"
    )
    assert not module.is_research_path(
        "apps/bt/src/domains/analytics/screening_evaluator.py"
    )


def test_experiment_analytics_modules_are_research_not_product() -> None:
    module = _load_module()

    path = "apps/bt/src/domains/analytics/annual_value_composite_selection.py"

    assert module.is_research_path(path)
    assert not module.is_product_path(path)


def test_research_docs_are_research_not_docs_only() -> None:
    module = _load_module()

    path = "apps/bt/docs/experiments/market-behavior/foo/README.md"

    assert module.is_research_path(path)
    assert module.is_docs_path(path)


def test_plain_docs_are_docs_only_candidates() -> None:
    module = _load_module()

    path = "docs/architecture-sot-matrix.md"

    assert module.is_docs_path(path)
    assert not module.is_research_path(path)
    assert not module.is_product_path(path)
