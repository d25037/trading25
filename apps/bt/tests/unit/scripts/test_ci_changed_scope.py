"""Tests for scripts/ci/changed-scope.py."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest


def _load_module():
    repo_root = Path(__file__).resolve().parents[5]
    module_path = repo_root / "scripts" / "ci" / "changed-scope.py"
    spec = importlib.util.spec_from_file_location("ci_changed_scope", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load ci_changed_scope module")
    module = importlib.util.module_from_spec(spec)
    sys.modules["ci_changed_scope"] = module
    spec.loader.exec_module(module)
    return module


def test_fastapi_route_change_is_product_only() -> None:
    module = _load_module()

    scope = module.classify_changed_paths(
        ["apps/bt/src/entrypoints/http/routes/analytics_complex.py"]
    )

    assert scope.product_ci is True
    assert scope.contracts_ci is True
    assert scope.research_ci is False
    assert scope.docs_only is False


def test_research_runner_change_is_research_only() -> None:
    module = _load_module()

    scope = module.classify_changed_paths(
        ["apps/bt/scripts/research/run_topix100_sma_ratio_rank_future_close.py"]
    )

    assert scope.product_ci is False
    assert scope.research_ci is True
    assert scope.docs_only is False


def test_research_experiment_doc_change_is_research_only() -> None:
    module = _load_module()

    scope = module.classify_changed_paths(
        ["apps/bt/docs/experiments/market-behavior/topix100-study.md"]
    )

    assert scope.product_ci is False
    assert scope.research_ci is True
    assert scope.docs_only is False


def test_research_analytics_module_change_is_research_only() -> None:
    module = _load_module()

    scope = module.classify_changed_paths(
        ["apps/bt/src/domains/analytics/annual_value_technical_feature_importance.py"]
    )

    assert scope.product_ci is False
    assert scope.research_ci is True
    assert scope.docs_only is False


def test_value_composite_research_module_change_is_research_only() -> None:
    module = _load_module()

    scope = module.classify_changed_paths(
        ["apps/bt/src/domains/analytics/annual_value_composite_selection.py"]
    )

    assert scope.product_ci is False
    assert scope.research_ci is True
    assert scope.docs_only is False


def test_api_backed_analytics_module_change_is_product_only() -> None:
    module = _load_module()

    scope = module.classify_changed_paths(
        ["apps/bt/src/domains/analytics/screening_evaluator.py"]
    )

    assert scope.product_ci is True
    assert scope.research_ci is False
    assert scope.docs_only is False


def test_value_composite_scoring_change_is_product_only() -> None:
    module = _load_module()

    scope = module.classify_changed_paths(
        ["apps/bt/src/domains/analytics/value_composite_scoring.py"]
    )

    assert scope.product_ci is True
    assert scope.research_ci is False
    assert scope.docs_only is False


def test_lab_agent_change_is_an_explicit_product_contract_surface() -> None:
    module = _load_module()

    scope = module.classify_changed_paths(
        ["apps/bt/src/domains/lab_agent/strategy_generator.py"]
    )

    assert scope.product_ci is True
    assert scope.contracts_ci is True
    assert scope.research_ci is False
    assert scope.docs_only is False


def test_http_backed_analytics_module_change_is_product_only() -> None:
    module = _load_module()

    scope = module.classify_changed_paths(
        ["apps/bt/src/domains/analytics/market_bubble_footprint_monitor.py"]
    )

    assert scope.product_ci is True
    assert scope.contracts_ci is True
    assert scope.research_ci is False
    assert scope.docs_only is False


def test_shared_pit_guard_change_runs_product_and_research() -> None:
    module = _load_module()

    scope = module.classify_changed_paths(["apps/bt/src/shared/utils/pit_guard.py"])

    assert scope.product_ci is True
    assert scope.research_ci is True
    assert scope.docs_only is False


def test_non_experiment_docs_are_docs_only() -> None:
    module = _load_module()

    scope = module.classify_changed_paths(["docs/architecture-sot-matrix.md"])

    assert scope.product_ci is False
    assert scope.research_ci is False
    assert scope.security_ci is False
    assert scope.docs_only is True


def test_empty_change_set_runs_full_ci() -> None:
    module = _load_module()

    scope = module.classify_changed_paths([])

    assert scope.product_ci is True
    assert scope.research_ci is True
    assert scope.contracts_ci is True
    assert scope.security_ci is True
    assert scope.docs_only is False


def test_dependency_manifest_change_runs_security_and_product_ci() -> None:
    module = _load_module()

    scope = module.classify_changed_paths(["apps/bt/pyproject.toml"])

    assert scope.product_ci is True
    assert scope.contracts_ci is True
    assert scope.research_ci is False
    assert scope.security_ci is True
    assert scope.docs_only is False


def test_dotfile_security_path_keeps_leading_dot() -> None:
    module = _load_module()

    scope = module.classify_changed_paths(["./.github/dependabot.yml"])

    assert scope.product_ci is True
    assert scope.research_ci is False
    assert scope.security_ci is True
    assert scope.docs_only is False


def test_unknown_non_docs_change_falls_back_to_product_ci() -> None:
    module = _load_module()

    scope = module.classify_changed_paths(["some/new/config.toml"])

    assert scope.product_ci is True
    assert scope.research_ci is False
    assert scope.docs_only is False


def test_skill_only_change_runs_guardrails_without_product_ci() -> None:
    module = _load_module()

    scope = module.classify_changed_paths(
        [".codex/skills/ts-api-endpoints/SKILL.md"]
    )

    assert scope.product_ci is False
    assert scope.research_ci is False
    assert scope.contracts_ci is False
    assert scope.security_ci is False
    assert scope.docs_only is False


def test_agents_only_change_runs_guardrails_without_product_ci() -> None:
    module = _load_module()

    scope = module.classify_changed_paths(["apps/ts/AGENTS.md"])

    assert scope.product_ci is False
    assert scope.research_ci is False
    assert scope.contracts_ci is False
    assert scope.security_ci is False
    assert scope.docs_only is False


def test_application_contract_change_runs_contract_and_product_ci() -> None:
    module = _load_module()
    scope = module.classify_changed_paths(
        ["apps/bt/src/application/contracts/factor_regression.py"]
    )

    assert scope.product_ci is True
    assert scope.contracts_ci is True
    assert scope.research_ci is False
    assert scope.security_ci is False
    assert scope.docs_only is False


@pytest.mark.parametrize(
    "path",
    [
        "apps/bt/src/entrypoints/http/app.py",
        "apps/bt/src/entrypoints/http/openapi_config.py",
        "apps/bt/pyproject.toml",
        "apps/bt/src/domains/analytics/screening_results.py",
        "apps/bt/src/shared/models/jobs.py",
    ],
)
def test_openapi_source_change_runs_contract_ci(path: str) -> None:
    module = _load_module()

    scope = module.classify_changed_paths([path])

    assert scope.product_ci is True
    assert scope.contracts_ci is True
