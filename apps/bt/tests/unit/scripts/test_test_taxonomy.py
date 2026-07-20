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


def test_research_fixture_is_research_not_product() -> None:
    module = _load_module()
    path = (
        "apps/bt/tests/fixtures/research/"
        "ranking_technical_fit_score_shape_evidence_published_digest.json"
    )

    assert module.is_research_path(path)
    assert not module.is_product_path(path)


def test_research_analytics_test_is_research_not_product() -> None:
    module = _load_module()
    path = (
        "apps/bt/tests/unit/domains/analytics/"
        "test_ranking_research_selection_contract.py"
    )

    assert module.is_research_path(path)
    assert not module.is_product_path(path)


def test_production_analytics_test_retains_product_semantics() -> None:
    module = _load_module()
    path = "apps/bt/tests/unit/domains/analytics/test_screening_evaluator.py"

    assert module.is_product_path(path)
    assert not module.is_research_path(path)


def test_explicit_production_analytics_tests_do_not_inherit_source_taxonomy() -> None:
    module = _load_module()

    for module_name in (
        "annual_value_composite_selection",
        "readonly_duckdb_support",
    ):
        path = f"apps/bt/tests/unit/domains/analytics/test_{module_name}.py"

        assert module.is_product_path(path)
        assert not module.is_research_path(path)


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


def test_skill_markdown_is_governance_not_plain_docs() -> None:
    module = _load_module()
    path = ".codex/skills/ts-api-endpoints/SKILL.md"

    assert module.is_docs_path(path)
    assert module.is_governance_path(path)


def test_nested_agents_file_is_governance() -> None:
    module = _load_module()

    assert module.is_governance_path("apps/bt/AGENTS.md")
    assert module.is_governance_path("AGENTS.md")


def test_application_contract_is_product_and_contract_path() -> None:
    module = _load_module()
    path = "apps/bt/src/application/contracts/factor_regression.py"

    assert module.is_product_path(path)
    assert module.is_contract_path(path)


def test_monitor_private_research_dependencies_are_explicit_target_policy() -> None:
    module = _load_module()
    monitor_test = (
        "tests/unit/domains/analytics/test_market_bubble_footprint_monitor.py"
    )

    assert module.PRODUCTION_ANALYTICS_DEPENDENCIES == {
        "earnings_holdthrough_expectancy": (monitor_test,),
        "market_bubble_footprint": (monitor_test,),
        "readonly_duckdb_support": (monitor_test,),
    }


def test_each_monitor_research_dependency_resolves_monitor_test_target() -> None:
    module = _load_module()
    monitor_test = (
        "tests/unit/domains/analytics/test_market_bubble_footprint_monitor.py"
    )

    for source_module in module.PRODUCTION_ANALYTICS_DEPENDENCIES:
        source_path = f"apps/bt/src/domains/analytics/{source_module}.py"
        assert module.production_analytics_test_targets(source_path) == (monitor_test,)
