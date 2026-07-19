#!/usr/bin/env python3
"""Canonical local/CI pytest target groups."""

from __future__ import annotations

import argparse
from collections.abc import Mapping
from pathlib import Path
import sys


PRODUCTION_ANALYTICS_TEST_MODULES = (
    "annual_value_composite_selection",
    "market_bubble_footprint_monitor",
    "readonly_duckdb_support",
    "screening_evaluator",
    "screening_requirements",
    "screening_results",
    "value_composite_scoring",
)
BT_PRODUCT_ANALYTICS_TESTS = tuple(
    f"tests/unit/domains/analytics/test_{module_name}.py"
    for module_name in PRODUCTION_ANALYTICS_TEST_MODULES
)
BT_PRODUCT_SCRIPT_TESTS = (
    "tests/unit/scripts/test_audit_skills.py",
    "tests/unit/scripts/test_check_contract_sync.py",
    "tests/unit/scripts/test_check_dep_direction.py",
    "tests/unit/scripts/test_check_privacy_leaks.py",
    "tests/unit/scripts/test_check_research_guardrails.py",
    "tests/unit/scripts/test_check_ts_wire_contracts.py",
    "tests/unit/scripts/test_ci_changed_scope.py",
    "tests/unit/scripts/test_ci_workflow.py",
    "tests/unit/scripts/test_export_openapi_unittest.py",
    "tests/unit/scripts/test_maintainability_snapshot.py",
    "tests/unit/scripts/test_openapi_compat.py",
    "tests/unit/scripts/test_prepush_ci_execution.py",
    "tests/unit/scripts/test_refresh_skill_references.py",
    "tests/unit/scripts/test_research_common.py",
    "tests/unit/scripts/test_research_test_targets.py",
    "tests/unit/scripts/test_removed_future_leak_surfaces.py",
    "tests/unit/scripts/test_test_taxonomy.py",
    "tests/unit/scripts/test_test_targets.py",
)
BT_SERVER_UNIT_TESTS = (
    "tests/unit/server",
    "tests/unit/backtest",
    "tests/unit/data",
    "tests/unit/optimization",
    "tests/unit/strategies",
    "tests/unit/strategy_config",
)
BT_CORE_UNIT_TESTS = (
    "tests/unit/agent",
    "tests/unit/api",
    "tests/unit/application",
    "tests/unit/architecture",
    "tests/unit/cli",
    "tests/unit/cli_bt",
    "tests/unit/config",
    "tests/unit/contracts",
    "tests/unit/domains/fundamentals",
    "tests/unit/domains/strategy/runtime",
    "tests/unit/filters",
    "tests/unit/models",
    "tests/unit/shared",
    "tests/unit/utils",
    "tests/unit/test_collect_production_smoke_baseline.py",
    "tests/unit/test_data.py",
    "tests/unit/test_type_safety.py",
    "tests/unit/test_validation.py",
)
BT_FAST_RESEARCH_TESTS = (
    "tests/unit/scripts/test_check_research_guardrails.py",
    "tests/unit/domains/analytics/test_research_bundle.py",
    "tests/unit/domains/analytics/test_research_core.py",
    "tests/unit/domains/analytics/test_ranking_technical_fit_price_projection_contract.py",
    "tests/unit/domains/analytics/test_ranking_research_selection_contract.py",
    "tests/unit/domains/analytics/test_ranking_publication_registry.py",
)


TARGET_GROUPS = {
    "bt-product-analytics": BT_PRODUCT_ANALYTICS_TESTS,
    "bt-product-scripts": BT_PRODUCT_SCRIPT_TESTS,
    "bt-server-unit": BT_SERVER_UNIT_TESTS,
    "bt-core-unit": BT_CORE_UNIT_TESTS,
    "bt-fast-research": BT_FAST_RESEARCH_TESTS,
}
PRODUCTION_TARGET_GROUPS = (
    "bt-product-analytics",
    "bt-product-scripts",
    "bt-server-unit",
    "bt-core-unit",
)


def is_production_unit_test(path: str) -> bool:
    normalized = path.strip().lstrip("./")
    if not normalized.startswith("tests/unit/"):
        return False
    filename = normalized.rsplit("/", 1)[-1]
    if not filename.startswith("test_") or not filename.endswith(".py"):
        return False
    if normalized.startswith("tests/unit/domains/analytics/"):
        return normalized in BT_PRODUCT_ANALYTICS_TESTS
    if normalized.startswith("tests/unit/scripts/test_run_"):
        return False
    return True


def production_test_files(bt_root: Path) -> tuple[str, ...]:
    unit_root = bt_root / "tests" / "unit"
    if not unit_root.is_dir():
        return ()
    return tuple(
        path.relative_to(bt_root).as_posix()
        for path in sorted(unit_root.rglob("test_*.py"))
        if is_production_unit_test(path.relative_to(bt_root).as_posix())
    )


def _production_target_groups() -> dict[str, tuple[str, ...]]:
    return {name: TARGET_GROUPS[name] for name in PRODUCTION_TARGET_GROUPS}


def production_test_target_coverage(
    bt_root: Path,
    *,
    target_groups: Mapping[str, tuple[str, ...]] | None = None,
) -> dict[str, tuple[str, ...]]:
    groups = _production_target_groups() if target_groups is None else target_groups
    coverage: dict[str, tuple[str, ...]] = {}
    for test_file in production_test_files(bt_root):
        matches = tuple(
            f"{group}:{target}"
            for group, targets in groups.items()
            for target in targets
            if test_file == target or test_file.startswith(f"{target.rstrip('/')}/")
        )
        coverage[test_file] = matches
    return coverage


def validate_production_test_targets(
    bt_root: Path,
    *,
    target_groups: Mapping[str, tuple[str, ...]] | None = None,
) -> None:
    coverage = production_test_target_coverage(
        bt_root,
        target_groups=target_groups,
    )
    uncovered = [path for path, matches in coverage.items() if not matches]
    duplicated = [path for path, matches in coverage.items() if len(matches) > 1]
    if uncovered:
        raise ValueError(f"uncovered production tests: {', '.join(uncovered)}")
    if duplicated:
        raise ValueError(f"multiply selected production tests: {', '.join(duplicated)}")


def targets_for_group(group: str) -> tuple[str, ...]:
    try:
        return TARGET_GROUPS[group]
    except KeyError as exc:
        raise ValueError(f"unknown test target group: {group}") from exc


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--group",
        choices=tuple(TARGET_GROUPS),
        required=True,
        help="Target group to print, one path per line.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    validate_production_test_targets(Path(__file__).resolve().parents[2] / "apps" / "bt")
    print("\n".join(targets_for_group(args.group)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
