#!/usr/bin/env python3
"""Detect research workflow regressions in runner / bundle / docs surfaces."""

from __future__ import annotations

import argparse
import ast
import re
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
PLAYGROUND_ROOT = Path("apps/bt/notebooks/playground")
EXPERIMENT_DOCS_ROOT = Path("apps/bt/docs/experiments")
RESEARCH_CODE_ROOTS = (
    Path("apps/bt/scripts/research"),
    Path("apps/bt/src/domains/analytics"),
)
FORBIDDEN_DOC_PATTERNS = {
    "legacy-notebook-reference": "apps/bt/notebooks/playground",
    "legacy-marimo-command": "marimo edit",
}
PUBLISHED_READOUT_HEADING = "published readout"
PUBLISHED_READOUT_REQUIRED_SECTIONS = {
    "decision",
    "main findings",
    "interpretation",
    "production implication",
    "caveats",
    "source artifacts",
}
PUBLISHED_SUMMARY_ASSIGNMENT = "published_summary="
CATALOG_PATH = EXPERIMENT_DOCS_ROOT / "research-catalog-metadata.toml"
PIT_INVALIDATION_REGISTER_PATH = Path("docs/research-pit-invalidation-register.md")
PIT_REGISTER_REQUIRED_MARKERS = (
    "`market.duckdb` schema v5",
    "`stock_price_adjustment_mode=provider_adjusted_v1`",
    "`providerVintage`",
    "current-basis",
    "`statement_metrics_adjusted`",
    "`daily_valuation`",
)
RESEARCH_DECISION_STATUSES = {
    "observed",
    "robust",
    "candidate",
    "ranking_surface",
    "strategy_draft",
    "production",
    "rejected",
}
STATUS_PROMOTED_SURFACE = {
    "ranking_surface": "Ranking",
    "strategy_draft": "Strategy",
}
REMOVED_DAILY_RANKING_CODE_TOKENS = (
    "DAILY_RANKING_RESEARCH_RANKED_TABLE",
    "DAILY_RANKING_RESEARCH_LIQUIDITY_RANKED_TABLE",
    "create_daily_ranking_research_panel",
    "daily_ranking_query_start_date",
    "daily_ranking_query_end_date",
    "event_time_basis_only=",
    "price_feature_relation=",
    "price_outcome_relation=",
    "ranking_technical_fit_price_projection",
    "_create_rerating_bubble_observation_table",
)


@dataclass(frozen=True)
class ResearchGuardrailFinding:
    relative_path: Path
    line_number: int
    rule_name: str
    message: str


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fail when research surfaces regress from the current "
            "runner-first / bundle-backed workflow."
        )
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=REPO_ROOT,
        help="Repository root to scan. Defaults to the current repository.",
    )
    parser.add_argument(
        "files",
        nargs="*",
        type=Path,
        help="Optional files to scan. Paths may be absolute or relative to --root.",
    )
    return parser.parse_args(argv)


def _normalize_relative_path(root: Path, path: Path) -> Path:
    candidate = path if path.is_absolute() else root / path
    return candidate.resolve().relative_to(root.resolve())


def list_playground_files(root: Path) -> list[Path]:
    playground_dir = root / PLAYGROUND_ROOT
    if not playground_dir.exists():
        return []
    return sorted(
        path.relative_to(root)
        for path in playground_dir.glob("*.py")
        if path.is_file()
    )


def list_experiment_readmes(root: Path) -> list[Path]:
    docs_dir = root / EXPERIMENT_DOCS_ROOT
    if not docs_dir.exists():
        return []
    return sorted(
        path.relative_to(root)
        for path in docs_dir.rglob("README.md")
        if path.is_file()
    )


def list_research_code_files(root: Path) -> list[Path]:
    files: list[Path] = []
    for code_root in RESEARCH_CODE_ROOTS:
        absolute_root = root / code_root
        if not absolute_root.exists():
            continue
        files.extend(
            path.relative_to(root)
            for path in absolute_root.glob("*.py")
            if path.is_file()
        )
    return sorted(files)


def _line_number_for_offset(text: str, start_offset: int) -> int:
    return text.count("\n", 0, start_offset) + 1


def _is_legacy_playground_path(relative_path: Path) -> bool:
    return (
        relative_path.suffix == ".py"
        and relative_path.as_posix().startswith(f"{PLAYGROUND_ROOT.as_posix()}/")
    )


def _is_research_code_path(relative_path: Path) -> bool:
    if relative_path.suffix != ".py":
        return False
    return any(relative_path.is_relative_to(root) for root in RESEARCH_CODE_ROOTS)


def _is_experiment_readme_path(relative_path: Path) -> bool:
    if relative_path.name != "README.md":
        return False
    if not relative_path.is_relative_to(EXPERIMENT_DOCS_ROOT):
        return False
    docs_relative = relative_path.relative_to(EXPERIMENT_DOCS_ROOT)
    return len(docs_relative.parts) >= 2 and "figures" not in docs_relative.parts


def find_docs_guardrail_findings_in_text(
    relative_path: Path,
    text: str,
) -> list[ResearchGuardrailFinding]:
    findings: list[ResearchGuardrailFinding] = []
    for rule_name, pattern in FORBIDDEN_DOC_PATTERNS.items():
        start = text.find(pattern)
        if start >= 0:
            findings.append(
                ResearchGuardrailFinding(
                    relative_path=relative_path,
                    line_number=_line_number_for_offset(text, start),
                    rule_name=rule_name,
                    message=(
                        "Experiment docs must use runner / bundle / canonical note "
                        "reproduction paths, not notebook runtime paths."
                    ),
                )
            )
    findings.extend(find_published_readout_findings(relative_path, text))
    return findings


def find_pit_register_contract_findings(
    relative_path: Path,
    text: str,
) -> list[ResearchGuardrailFinding]:
    findings: list[ResearchGuardrailFinding] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        normalized = line.lower()
        for schema_version in ("v3", "v4"):
            if f"schema {schema_version}" not in normalized:
                continue
            if "archived" in normalized and "historical" in normalized:
                continue
            findings.append(
                ResearchGuardrailFinding(
                    relative_path=relative_path,
                    line_number=line_number,
                    rule_name=f"pit-register-current-schema-{schema_version}",
                    message=(
                        "The living PIT register must not accept Market schema "
                        f"{schema_version} as current evidence. Preserve "
                        f"{schema_version} provenance only on lines marked both "
                        "archived and historical."
                    ),
                )
            )

    for marker in PIT_REGISTER_REQUIRED_MARKERS:
        if marker in text:
            continue
        findings.append(
            ResearchGuardrailFinding(
                relative_path=relative_path,
                line_number=1,
                rule_name="pit-register-market-v5-contract-missing",
                message=(
                    "The living PIT register must require the complete Market v5 "
                    f"provider-adjusted contract; missing marker: {marker}"
                ),
            )
        )
    return findings


def find_research_code_guardrail_findings_in_text(
    relative_path: Path,
    text: str,
) -> list[ResearchGuardrailFinding]:
    findings: list[ResearchGuardrailFinding] = []
    lines = text.splitlines()
    for index, line in enumerate(lines):
        if PUBLISHED_SUMMARY_ASSIGNMENT not in line:
            continue
        findings.append(
            ResearchGuardrailFinding(
                relative_path=relative_path,
                line_number=index + 1,
                rule_name="published-summary-generation",
                message=(
                    "Published Readout is the only publication SoT. Do not generate "
                    "bundle `summary.json` via `published_summary=`."
                ),
            )
        )
    removed = next(
        (token for token in REMOVED_DAILY_RANKING_CODE_TOKENS if token in text),
        None,
    )
    if removed is not None:
        findings.append(
            ResearchGuardrailFinding(
                relative_path=relative_path,
                line_number=_line_number_for_offset(text, text.index(removed)),
                rule_name="removed-daily-ranking-compatibility-surface",
                message=f"Removed Daily Ranking compatibility surface: {removed}",
            )
        )

    try:
        tree = ast.parse(text)
    except SyntaxError:
        return findings
    imports_typed_request = any(
        isinstance(node, ast.ImportFrom)
        and node.module == "src.domains.analytics.daily_ranking_research_base"
        and any(alias.name == "DailyRankingPanelRequest" for alias in node.names)
        for node in ast.walk(tree)
    )
    is_daily_ranking_consumer = (
        relative_path.stem.startswith("ranking_")
        or relative_path.stem == "atr_expansion_forward_response"
        or relative_path.stem == "market_bubble_footprint_support"
    )
    stock_scan_root: ast.AST = tree
    if relative_path.stem == "market_bubble_footprint_support":
        stock_scan_root = next(
            (
                node
                for node in tree.body
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                and node.name
                == "run_rerating_bubble_regime_forward_response_research"
            ),
            tree,
        )
    stock_data_literal = next(
        (
            node
            for node in ast.walk(stock_scan_root)
            if isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and re.search(
                r"\b(?:from|join)\s+stock_data\b",
                node.value,
                re.IGNORECASE,
            )
        ),
        None,
    )
    if imports_typed_request and is_daily_ranking_consumer and stock_data_literal:
        findings.append(
            ResearchGuardrailFinding(
                relative_path=relative_path,
                line_number=stock_data_literal.lineno,
                rule_name="daily-ranking-stock-data-read",
                message=(
                    "Cutoff-aware Daily Ranking consumers must read the Market v4 "
                    "event-time projection, not stock_data."
                ),
            )
        )
    if (
        relative_path.stem == "market_bubble_footprint_support"
        and not all(
            marker in text
            for marker in (
                "price_history_name=ranking_relations.price_history.name",
                "signal_basis_name=ranking_relations.signal_prices.name",
            )
        )
    ):
        findings.append(
            ResearchGuardrailFinding(
                relative_path=relative_path,
                line_number=1,
                rule_name="daily-ranking-stock-data-read",
                message=(
                    "Rerating footprint grouping must consume the issued event-time "
                    "price history and exact signal basis relations."
                ),
            )
        )
    return findings


def find_published_readout_findings(
    relative_path: Path,
    text: str,
) -> list[ResearchGuardrailFinding]:
    section_has_content: dict[str, bool] = {}
    in_readout = False
    readout_line_number: int | None = None
    current_section: str | None = None

    for line_number, raw_line in enumerate(text.splitlines(), start=1):
        stripped = raw_line.strip()
        if stripped.startswith("## "):
            heading = stripped[3:].strip().lower()
            in_readout = heading == PUBLISHED_READOUT_HEADING
            if in_readout:
                readout_line_number = line_number
            current_section = None
            continue
        if not in_readout:
            continue
        if stripped.startswith("### "):
            current_section = stripped[4:].strip().lower()
            section_has_content.setdefault(current_section, False)
            continue
        if current_section is not None and stripped:
            section_has_content[current_section] = True

    if readout_line_number is None:
        if _is_experiment_readme_path(relative_path):
            return [
                ResearchGuardrailFinding(
                    relative_path=relative_path,
                    line_number=1,
                    rule_name="missing-published-readout",
                    message=(
                        "Experiment README files must include a complete "
                        "`## Published Readout` section."
                    ),
                )
            ]
        return []

    missing_sections = sorted(
        section
        for section in PUBLISHED_READOUT_REQUIRED_SECTIONS
        if not section_has_content.get(section)
    )
    if not missing_sections:
        return []

    return [
        ResearchGuardrailFinding(
            relative_path=relative_path,
            line_number=readout_line_number,
            rule_name="incomplete-published-readout",
            message=(
                "Published Readout is present but missing required sections: "
                + ", ".join(missing_sections)
            ),
        )
    ]


def scan_research_files(root: Path, files: list[Path]) -> list[ResearchGuardrailFinding]:
    findings: list[ResearchGuardrailFinding] = []
    for relative_path in files:
        absolute_path = root / relative_path
        if not absolute_path.is_file():
            continue

        if _is_legacy_playground_path(relative_path):
            findings.append(
                ResearchGuardrailFinding(
                    relative_path=relative_path,
                    line_number=1,
                    rule_name="legacy-playground-file",
                    message=(
                        "Research playground notebooks were removed from the active "
                        "repo surface; use runner scripts and bundle outputs instead."
                    ),
                )
            )
            continue

        if relative_path == PIT_INVALIDATION_REGISTER_PATH:
            text = absolute_path.read_text(encoding="utf-8", errors="ignore")
            findings.extend(find_pit_register_contract_findings(relative_path, text))
            continue

        if relative_path.name == "README.md":
            text = absolute_path.read_text(encoding="utf-8", errors="ignore")
            findings.extend(find_docs_guardrail_findings_in_text(relative_path, text))
            continue

        if _is_research_code_path(relative_path):
            text = absolute_path.read_text(encoding="utf-8", errors="ignore")
            findings.extend(find_research_code_guardrail_findings_in_text(relative_path, text))
    return findings


def scan_research_publication_integrity(
    repo_root: Path,
) -> list[ResearchGuardrailFinding]:
    docs_root = repo_root / EXPERIMENT_DOCS_ROOT
    # A publication README is exactly `<family>/<experiment>/README.md`.
    # Root index and deeper subordinate notes such as `figures/README.md` are
    # documentation, but do not define publication IDs.
    readme_ids = {
        relative.parent.as_posix()
        for readme in docs_root.rglob("README.md")
        if len((relative := readme.relative_to(docs_root)).parts) == 3
    }
    findings: list[ResearchGuardrailFinding] = []
    catalog_path = repo_root / CATALOG_PATH
    if not catalog_path.is_file():
        return [
            ResearchGuardrailFinding(
                relative_path=CATALOG_PATH,
                line_number=1,
                rule_name="research-catalog-missing",
                message="Publication integrity requires the research catalog.",
            )
        ]

    try:
        catalog = tomllib.loads(catalog_path.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as error:
        return [
            ResearchGuardrailFinding(
                relative_path=CATALOG_PATH,
                line_number=1,
                rule_name="invalid-research-catalog-toml",
                message=f"Research catalog TOML is invalid: {error}",
            )
        ]

    if "experiments" not in catalog:
        return [
            ResearchGuardrailFinding(
                relative_path=CATALOG_PATH,
                line_number=1,
                rule_name="catalog-experiments-missing",
                message="Research catalog must define an `experiments` table.",
            )
        ]
    raw_experiments = catalog["experiments"]
    if not isinstance(raw_experiments, dict):
        return [
            ResearchGuardrailFinding(
                relative_path=CATALOG_PATH,
                line_number=1,
                rule_name="catalog-experiments-invalid",
                message="Research catalog `experiments` must be a table.",
            )
        ]
    catalog_experiments: dict[str, object] = raw_experiments

    for experiment_id in sorted(readme_ids - set(catalog_experiments)):
        findings.append(
            ResearchGuardrailFinding(
                relative_path=(
                    EXPERIMENT_DOCS_ROOT / experiment_id / "README.md"
                ),
                line_number=1,
                rule_name="readout-catalog-missing",
                message=f"Publication README has no catalog entry: {experiment_id}",
            )
        )

    for experiment_id, metadata in catalog_experiments.items():
        if experiment_id not in readme_ids:
            findings.append(
                ResearchGuardrailFinding(
                    relative_path=CATALOG_PATH,
                    line_number=1,
                    rule_name="catalog-readout-missing",
                    message=f"Catalog experiment has no README: {experiment_id}",
                )
            )
        if not isinstance(metadata, dict):
            findings.append(
                ResearchGuardrailFinding(
                    relative_path=CATALOG_PATH,
                    line_number=1,
                    rule_name="catalog-entry-invalid",
                    message=f"Catalog entry must be a table: {experiment_id}",
                )
            )
            continue

        status = metadata.get("status")
        if not isinstance(status, str) or status not in RESEARCH_DECISION_STATUSES:
            findings.append(
                ResearchGuardrailFinding(
                    relative_path=CATALOG_PATH,
                    line_number=1,
                    rule_name="catalog-status-invalid",
                    message=(
                        f"{experiment_id} status must be one of the supported "
                        "ResearchDecisionStatus values."
                    ),
                )
            )
        promoted_surface = metadata.get("promotedSurface")
        expected_surface = STATUS_PROMOTED_SURFACE.get(status)
        if expected_surface is not None and promoted_surface != expected_surface:
            findings.append(
                ResearchGuardrailFinding(
                    relative_path=CATALOG_PATH,
                    line_number=1,
                    rule_name="catalog-status-surface-mismatch",
                    message=(
                        f"{experiment_id} status {status!r} requires "
                        f"promotedSurface = {expected_surface!r}."
                    ),
                )
            )

        related = metadata.get("relatedExperiments")
        if not isinstance(related, list) or not all(
            isinstance(item, str) and item.strip() for item in related
        ):
            findings.append(
                ResearchGuardrailFinding(
                    relative_path=CATALOG_PATH,
                    line_number=1,
                    rule_name="catalog-related-experiments-invalid",
                    message=(
                        f"{experiment_id} relatedExperiments must be an array "
                        "of non-empty publication IDs."
                    ),
                )
            )
            continue
        for related_id in related:
            if isinstance(related_id, str) and related_id not in readme_ids:
                findings.append(
                    ResearchGuardrailFinding(
                        relative_path=CATALOG_PATH,
                        line_number=1,
                        rule_name="dangling-related-experiment",
                        message=(
                            f"{experiment_id} relates to missing README: {related_id}"
                        ),
                    )
                )

    return findings


def format_findings(findings: list[ResearchGuardrailFinding]) -> str:
    lines = [
        "[research-guardrails] Found research workflow regressions.",
        "Research surfaces must stay runner-first and bundle-backed.",
        "",
    ]
    for finding in findings:
        lines.append(
            f"{finding.relative_path.as_posix()}:{finding.line_number}: "
            f"{finding.rule_name}: {finding.message}"
        )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    root = args.root.resolve()
    files = (
        [_normalize_relative_path(root, path) for path in args.files]
        if args.files
        else [
            *list_playground_files(root),
            *list_experiment_readmes(root),
            PIT_INVALIDATION_REGISTER_PATH,
        ]
        + list_research_code_files(root)
    )
    findings = scan_research_files(root, files)
    if not args.files:
        findings.extend(scan_research_publication_integrity(root))
    if findings:
        print(format_findings(findings), file=sys.stderr)
        return 1
    print("[research-guardrails] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
