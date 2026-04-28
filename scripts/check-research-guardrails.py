#!/usr/bin/env python3
"""Detect research workflow regressions in runner / bundle / docs surfaces."""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
PLAYGROUND_ROOT = Path("apps/bt/notebooks/playground")
EXPERIMENT_DOCS_ROOT = Path("apps/bt/docs/experiments")
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


def _line_number_for_offset(text: str, start_offset: int) -> int:
    return text.count("\n", 0, start_offset) + 1


def _is_legacy_playground_path(relative_path: Path) -> bool:
    return (
        relative_path.suffix == ".py"
        and relative_path.as_posix().startswith(f"{PLAYGROUND_ROOT.as_posix()}/")
    )


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

        if relative_path.name == "README.md":
            text = absolute_path.read_text(encoding="utf-8", errors="ignore")
            findings.extend(find_docs_guardrail_findings_in_text(relative_path, text))
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
        else [*list_playground_files(root), *list_experiment_readmes(root)]
    )
    findings = scan_research_files(root, files)
    if findings:
        print(format_findings(findings), file=sys.stderr)
        return 1
    print("[research-guardrails] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
