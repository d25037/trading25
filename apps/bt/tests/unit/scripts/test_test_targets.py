"""Tests for scripts/ci/test_targets.py."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest


def _load_module():
    repo_root = Path(__file__).resolve().parents[5]
    module_path = repo_root / "scripts" / "ci" / "test_targets.py"
    spec = importlib.util.spec_from_file_location("test_targets", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load test_targets module")
    module = importlib.util.module_from_spec(spec)
    sys.modules["test_targets"] = module
    spec.loader.exec_module(module)
    return module


def test_fast_research_targets_stay_on_infra_surface() -> None:
    module = _load_module()

    assert module.targets_for_group("bt-fast-research") == (
        "tests/unit/scripts/test_check_research_guardrails.py",
        "tests/unit/domains/analytics/test_research_bundle.py",
        "tests/unit/domains/analytics/test_research_core.py",
    )


def test_product_analytics_targets_exclude_experiment_suite_directories() -> None:
    module = _load_module()

    targets = module.targets_for_group("bt-product-analytics")

    assert "tests/unit/domains/analytics" not in targets
    assert "tests/unit/scripts" not in targets
    assert all(target.endswith(".py") for target in targets)


def test_product_analytics_targets_cover_http_backed_monitor() -> None:
    module = _load_module()

    assert (
        "tests/unit/domains/analytics/test_market_bubble_footprint_monitor.py"
        in module.targets_for_group("bt-product-analytics")
    )


def test_product_script_targets_cover_ci_target_helpers() -> None:
    module = _load_module()

    targets = module.targets_for_group("bt-product-scripts")

    assert "tests/unit/scripts/test_ci_changed_scope.py" in targets
    assert "tests/unit/scripts/test_research_test_targets.py" in targets
    assert "tests/unit/scripts/test_test_taxonomy.py" in targets
    assert "tests/unit/scripts/test_test_targets.py" in targets


def test_product_script_targets_cover_all_ci_self_tests() -> None:
    module = _load_module()

    targets = module.targets_for_group("bt-product-scripts")

    assert "tests/unit/scripts/test_ci_workflow.py" in targets
    assert "tests/unit/scripts/test_check_dep_direction.py" in targets
    assert "tests/unit/scripts/test_check_ts_wire_contracts.py" in targets
    assert "tests/unit/scripts/test_maintainability_snapshot.py" in targets
    assert "tests/unit/scripts/test_openapi_compat.py" in targets
    assert "tests/unit/scripts/test_removed_future_leak_surfaces.py" in targets


def test_core_unit_targets_cover_all_production_domain_directories() -> None:
    module = _load_module()

    targets = module.targets_for_group("bt-core-unit")

    assert "tests/unit/contracts" in targets
    assert "tests/unit/domains/fundamentals" in targets
    assert "tests/unit/domains/strategy/runtime" in targets


def test_contract_sync_script_test_is_in_product_script_targets() -> None:
    module = _load_module()

    assert (
        "tests/unit/scripts/test_check_contract_sync.py"
        in module.BT_PRODUCT_SCRIPT_TESTS
    )


def test_policy_defined_production_test_universe_is_selected_exactly_once() -> None:
    module = _load_module()
    bt_root = Path(__file__).resolve().parents[3]

    coverage = module.production_test_target_coverage(bt_root)

    assert set(coverage) == set(module.production_test_files(bt_root))
    assert coverage
    assert all(len(matches) == 1 for matches in coverage.values())


def test_production_test_policy_preserves_intentional_research_classifications() -> None:
    module = _load_module()

    assert module.is_production_unit_test(
        "tests/unit/domains/analytics/test_market_bubble_footprint_monitor.py"
    )
    assert not module.is_production_unit_test(
        "tests/unit/domains/analytics/test_market_bubble_footprint.py"
    )
    assert not module.is_production_unit_test(
        "tests/unit/scripts/test_run_market_bubble_research.py"
    )
    assert module.is_production_unit_test(
        "tests/unit/scripts/test_test_targets.py"
    )
    assert module.is_production_unit_test(
        "tests/unit/future_product_domain/test_new_surface.py"
    )


def test_production_test_validation_rejects_future_directory_omission(
    tmp_path: Path,
) -> None:
    module = _load_module()
    bt_root = tmp_path / "bt"
    future_test = (
        bt_root
        / "tests"
        / "unit"
        / "future_product_domain"
        / "test_new_surface.py"
    )
    future_test.parent.mkdir(parents=True)
    future_test.write_text("def test_new_surface(): pass\n", encoding="utf-8")

    with pytest.raises(ValueError, match="uncovered production tests.*test_new_surface"):
        module.validate_production_test_targets(bt_root, target_groups={})


def test_production_test_validation_rejects_duplicate_mapping(tmp_path: Path) -> None:
    module = _load_module()
    bt_root = tmp_path / "bt"
    test_file = bt_root / "tests" / "unit" / "example" / "test_example.py"
    test_file.parent.mkdir(parents=True)
    test_file.write_text("def test_example(): pass\n", encoding="utf-8")

    with pytest.raises(ValueError, match="multiply selected production tests.*test_example"):
        module.validate_production_test_targets(
            bt_root,
            target_groups={
                "directory": ("tests/unit/example",),
                "file": ("tests/unit/example/test_example.py",),
            },
        )
