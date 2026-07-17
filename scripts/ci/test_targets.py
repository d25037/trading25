#!/usr/bin/env python3
"""Canonical local/CI pytest target groups."""

from __future__ import annotations

import argparse
import sys


BT_PRODUCT_ANALYTICS_TESTS = (
    "tests/unit/domains/analytics/test_annual_value_composite_selection.py",
    "tests/unit/domains/analytics/test_market_bubble_footprint_monitor.py",
    "tests/unit/domains/analytics/test_readonly_duckdb_support.py",
    "tests/unit/domains/analytics/test_screening_evaluator.py",
    "tests/unit/domains/analytics/test_screening_requirements.py",
    "tests/unit/domains/analytics/test_screening_results.py",
    "tests/unit/domains/analytics/test_value_composite_scoring.py",
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
)


TARGET_GROUPS = {
    "bt-product-analytics": BT_PRODUCT_ANALYTICS_TESTS,
    "bt-product-scripts": BT_PRODUCT_SCRIPT_TESTS,
    "bt-server-unit": BT_SERVER_UNIT_TESTS,
    "bt-core-unit": BT_CORE_UNIT_TESTS,
    "bt-fast-research": BT_FAST_RESEARCH_TESTS,
}


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
    print("\n".join(targets_for_group(args.group)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
