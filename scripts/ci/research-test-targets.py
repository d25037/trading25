#!/usr/bin/env python3
"""Map changed research files to the smallest useful bt pytest targets."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from test_taxonomy import is_research_path  # noqa: E402
from test_targets import BT_FAST_RESEARCH_TESTS  # noqa: E402

RESEARCH_PY_PREFIXES = (
    "apps/bt/src/domains/analytics/",
    "apps/bt/scripts/research/",
)
RESEARCH_INFRA_TESTS = (
    "tests/unit/scripts/test_check_research_guardrails.py",
    "tests/unit/domains/analytics/test_research_bundle.py",
)
RESEARCH_ANALYTICS_TEST_PREFIX = "apps/bt/tests/unit/domains/analytics/test_"
RESEARCH_FIXTURE_CONSUMER_TESTS = {
    "apps/bt/tests/fixtures/research/ranking_publication_registry.json": (
        "tests/unit/domains/analytics/test_ranking_publication_registry.py",
    ),
    (
        "apps/bt/tests/fixtures/research/"
        "ranking_technical_fit_score_shape_evidence_published_digest.json"
    ): (
        "tests/unit/domains/analytics/"
        "test_ranking_technical_fit_score_shape_evidence.py",
    ),
}
DAILY_RANKING_SHARED_CONSUMER_TESTS = (
    "tests/unit/domains/analytics/test_atr_expansion_forward_response.py",
    "tests/unit/domains/analytics/test_daily_ranking_consumer_support.py",
    "tests/unit/domains/analytics/test_daily_ranking_feature_builders.py",
    "tests/unit/domains/analytics/test_daily_ranking_research_base.py",
    "tests/unit/domains/analytics/test_market_bubble_footprint_monitor.py",
    "tests/unit/domains/analytics/test_ranking_color_evidence.py",
    "tests/unit/domains/analytics/test_ranking_core_factor_regime_breakdown.py",
    (
        "tests/unit/domains/analytics/"
        "test_ranking_core_sector_neutral_value_regime_breakdown.py"
    ),
    (
        "tests/unit/domains/analytics/"
        "test_ranking_core_sector_relative_value_evidence.py"
    ),
    "tests/unit/domains/analytics/test_ranking_crowded_long_tail_evidence.py",
    "tests/unit/domains/analytics/test_ranking_daily_triage_lens.py",
    "tests/unit/domains/analytics/test_ranking_fixed_return_priority_evidence.py",
    (
        "tests/unit/domains/analytics/"
        "test_ranking_forecast_operating_profit_growth_evidence.py"
    ),
    (
        "tests/unit/domains/analytics/"
        "test_ranking_liquidity_price_action_recomposition.py"
    ),
    "tests/unit/domains/analytics/test_ranking_liquidity_z_long_evidence.py",
    (
        "tests/unit/domains/analytics/"
        "test_ranking_long_scaffold_factor_cross_evidence.py"
    ),
    (
        "tests/unit/domains/analytics/"
        "test_ranking_long_scaffold_value_composite_evidence.py"
    ),
    (
        "tests/unit/domains/analytics/"
        "test_ranking_long_sector_leadership_horizon_decomposition.py"
    ),
    (
        "tests/unit/domains/analytics/"
        "test_ranking_moving_average_replacement_evidence.py"
    ),
    "tests/unit/domains/analytics/test_ranking_psr_valuation_evidence.py",
    "tests/unit/domains/analytics/test_ranking_roe_quality_evidence.py",
    "tests/unit/domains/analytics/test_ranking_sector_strength_evidence.py",
    "tests/unit/domains/analytics/test_ranking_short_red_evidence.py",
    (
        "tests/unit/domains/analytics/"
        "test_ranking_short_sector_strength_evidence.py"
    ),
    (
        "tests/unit/domains/analytics/"
        "test_ranking_short_value_composite_evidence.py"
    ),
    "tests/unit/domains/analytics/test_ranking_sma5_atr_deviation_evidence.py",
    "tests/unit/domains/analytics/test_ranking_sma5_below_streak_evidence.py",
    "tests/unit/domains/analytics/test_ranking_sma5_count_long_evidence.py",
    "tests/unit/domains/analytics/test_ranking_sma5_count_short_evidence.py",
    "tests/unit/domains/analytics/test_ranking_sma5_deviation_evidence.py",
    "tests/unit/domains/analytics/test_ranking_sma5_position_state_evidence.py",
    (
        "tests/unit/domains/analytics/"
        "test_ranking_technical_fit_score_shape_evidence.py"
    ),
    (
        "tests/unit/domains/analytics/"
        "test_ranking_trend_acceleration_conditional_lift.py"
    ),
    "tests/unit/domains/analytics/test_ranking_trend_slope_evidence.py",
)
DOMAIN_CONSUMER_TESTS = {
    "daily_ranking_consumer_support": DAILY_RANKING_SHARED_CONSUMER_TESTS,
    "daily_ranking_research_base": DAILY_RANKING_SHARED_CONSUMER_TESTS,
    "market_bubble_footprint_support": (
        "tests/unit/domains/analytics/test_market_bubble_footprint.py",
        "tests/unit/domains/analytics/test_market_bubble_footprint_monitor.py",
    ),
    "ranking_n225_rerating_benchmark_support": (
        (
            "tests/unit/domains/analytics/"
            "test_ranking_n225_crowded_rerating_benchmark.py"
        ),
        (
            "tests/unit/domains/analytics/"
            "test_ranking_n225_neutral_rerating_benchmark.py"
        ),
    ),
}


def _normalize(path: str) -> str:
    return path.strip().lstrip("./")


def _exists(relative_path: str) -> bool:
    return (REPO_ROOT / "apps" / "bt" / relative_path).exists()


def _module_name(path: str, *, prefix: str) -> str | None:
    if not path.startswith(prefix) or not path.endswith(".py"):
        return None
    return Path(path).stem


def research_python_files(paths: list[str]) -> tuple[str, ...]:
    files: list[str] = []
    for path in paths:
        normalized = _normalize(path)
        if (
            normalized.endswith(".py")
            and normalized.startswith(RESEARCH_PY_PREFIXES)
            and (REPO_ROOT / normalized).is_file()
        ):
            files.append(normalized)
    return tuple(dict.fromkeys(files))


def pytest_targets_for_research_changes(paths: list[str]) -> tuple[str, ...]:
    targets: list[str] = []
    normalized_paths = [_normalize(path) for path in paths if _normalize(path)]
    if not normalized_paths:
        return (
            "tests/unit/domains/analytics",
            "tests/unit/scripts",
        )

    for path in normalized_paths:
        if path.startswith(RESEARCH_ANALYTICS_TEST_PREFIX) and is_research_path(path):
            target = path.removeprefix("apps/bt/")
            if _exists(target):
                targets.append(target)
            continue
        fixture_target = RESEARCH_FIXTURE_CONSUMER_TESTS.get(path)
        if fixture_target is not None:
            targets.extend(fixture_target)
            continue
        if path == "scripts/check-research-guardrails.py":
            targets.append("tests/unit/scripts/test_check_research_guardrails.py")
            continue
        if path.startswith("apps/bt/src/shared/"):
            targets.extend(
                [
                    "tests/unit/domains/analytics",
                    "tests/unit/scripts",
                ]
            )
            continue
        if path.startswith("apps/bt/scripts/research/"):
            module_name = _module_name(path, prefix="apps/bt/scripts/research/")
            if module_name in {None, "common"}:
                targets.append("tests/unit/scripts")
                continue
            target = f"tests/unit/scripts/test_{module_name}.py"
            if _exists(target):
                targets.append(target)
                continue
            domain_module = module_name.removeprefix("run_")
            domain_target = f"tests/unit/domains/analytics/test_{domain_module}.py"
            targets.append(
                domain_target if _exists(domain_target) else "tests/unit/scripts"
            )
            continue
        if path.startswith("apps/bt/src/domains/analytics/"):
            module_name = _module_name(path, prefix="apps/bt/src/domains/analytics/")
            if module_name is None:
                continue
            if module_name == "research_bundle":
                targets.extend(RESEARCH_INFRA_TESTS)
                continue
            if module_name in DOMAIN_CONSUMER_TESTS:
                targets.extend(DOMAIN_CONSUMER_TESTS[module_name])
                continue
            target = f"tests/unit/domains/analytics/test_{module_name}.py"
            targets.append(
                target if _exists(target) else "tests/unit/domains/analytics"
            )
            continue
        if path.startswith("apps/bt/docs/experiments/"):
            continue

    return tuple(dict.fromkeys(targets))


def fast_research_pytest_targets() -> tuple[str, ...]:
    return BT_FAST_RESEARCH_TESTS


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=("pytest", "py-files", "fast-pytest"),
        default="pytest",
        help="Output pytest targets or changed research Python files.",
    )
    parser.add_argument("paths", nargs="*", help="Changed paths. Reads stdin when empty.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    paths = args.paths or [line.strip() for line in sys.stdin if line.strip()]
    if args.mode == "fast-pytest":
        output = fast_research_pytest_targets()
    elif args.mode == "py-files":
        output = research_python_files(paths)
    else:
        output = pytest_targets_for_research_changes(paths)
    print("\n".join(output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
