#!/usr/bin/env python3
"""Detect research workflow regressions in playground notebooks.

This checker enforces the current runner-first / viewer-only workflow for
`apps/bt/notebooks/playground`:

- every playground notebook must use the shared viewer helper
- playground notebooks must not import or call `run_*_research(...)` directly
- every notebook-declared `runner_path` must point at a real runner script
"""

from __future__ import annotations

import argparse
import ast
import sys
from dataclasses import dataclass
from pathlib import Path
import re


REPO_ROOT = Path(__file__).resolve().parents[1]
PLAYGROUND_ROOT = Path("apps/bt/notebooks/playground")
SHARED_VIEWER_MODULE = "src.shared.research_notebook_viewer"
RUN_RESEARCH_NAME_RE = re.compile(r"^run_[a-z0-9_]+_research$")
RUNNER_PATH_RE = re.compile(
    r"runner_path\s*=\s*(?P<quote>['\"])(?P<path>[^'\"]+)(?P=quote)"
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
            "Fail when playground notebooks regress from the current "
            "runner-first / viewer-only research workflow."
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


def _line_number_for_offset(text: str, start_offset: int) -> int:
    return text.count("\n", 0, start_offset) + 1


def _is_shared_viewer_import(node: ast.AST) -> bool:
    if isinstance(node, ast.ImportFrom):
        return node.module == SHARED_VIEWER_MODULE
    if isinstance(node, ast.Import):
        return any(alias.name == SHARED_VIEWER_MODULE for alias in node.names)
    return False


def _extract_call_name(func: ast.expr) -> str | None:
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


def find_guardrail_findings_in_text(
    root: Path,
    relative_path: Path,
    text: str,
) -> list[ResearchGuardrailFinding]:
    findings: list[ResearchGuardrailFinding] = []

    if not relative_path.name.endswith("_playground.py"):
        findings.append(
            ResearchGuardrailFinding(
                relative_path=relative_path,
                line_number=1,
                rule_name="playground-name",
                message="Playground notebook filenames must end with `_playground.py`.",
            )
        )

    runner_matches = list(RUNNER_PATH_RE.finditer(text))
    if not runner_matches:
        findings.append(
            ResearchGuardrailFinding(
                relative_path=relative_path,
                line_number=1,
                rule_name="missing-runner-path",
                message="Playground notebooks must declare a canonical `runner_path`.",
            )
        )

    for match in runner_matches:
        runner_path = Path(match.group("path"))
        line_number = _line_number_for_offset(text, match.start())
        if not runner_path.as_posix().startswith("apps/bt/scripts/research/run_"):
            findings.append(
                ResearchGuardrailFinding(
                    relative_path=relative_path,
                    line_number=line_number,
                    rule_name="runner-path-prefix",
                    message=(
                        "Playground `runner_path` must point to "
                        "`apps/bt/scripts/research/run_*.py`."
                    ),
                )
            )
        if runner_path.suffix != ".py":
            findings.append(
                ResearchGuardrailFinding(
                    relative_path=relative_path,
                    line_number=line_number,
                    rule_name="runner-path-extension",
                    message="Playground `runner_path` must point to a Python file.",
                )
            )
        if not (root / runner_path).is_file():
            findings.append(
                ResearchGuardrailFinding(
                    relative_path=relative_path,
                    line_number=line_number,
                    rule_name="missing-runner-script",
                    message=f"Declared runner path was not found: {runner_path.as_posix()}",
                )
            )

    try:
        tree = ast.parse(text)
    except SyntaxError as exc:
        findings.append(
            ResearchGuardrailFinding(
                relative_path=relative_path,
                line_number=exc.lineno or 1,
                rule_name="syntax-error",
                message=str(exc),
            )
        )
        return findings

    if not any(_is_shared_viewer_import(node) for node in ast.walk(tree)):
        findings.append(
            ResearchGuardrailFinding(
                relative_path=relative_path,
                line_number=1,
                rule_name="missing-shared-viewer-import",
                message=(
                    "Playground notebooks must use "
                    "`src.shared.research_notebook_viewer`."
                ),
            )
        )

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            for alias in node.names:
                if RUN_RESEARCH_NAME_RE.match(alias.name):
                    findings.append(
                        ResearchGuardrailFinding(
                            relative_path=relative_path,
                            line_number=node.lineno,
                            rule_name="direct-research-import",
                            message=(
                                "Playground notebooks must not import "
                                f"`{alias.name}` directly; use bundles via the runner."
                            ),
                        )
                    )
        elif isinstance(node, ast.Call):
            call_name = _extract_call_name(node.func)
            if call_name and RUN_RESEARCH_NAME_RE.match(call_name):
                findings.append(
                    ResearchGuardrailFinding(
                        relative_path=relative_path,
                        line_number=node.lineno,
                        rule_name="direct-research-call",
                        message=(
                            "Playground notebooks must not execute "
                            f"`{call_name}(...)` directly."
                        ),
                    )
                )

    return findings


def scan_playground_files(root: Path, files: list[Path]) -> list[ResearchGuardrailFinding]:
    findings: list[ResearchGuardrailFinding] = []
    for relative_path in files:
        absolute_path = root / relative_path
        if not absolute_path.is_file():
            continue
        text = absolute_path.read_text(encoding="utf-8", errors="ignore")
        findings.extend(find_guardrail_findings_in_text(root, relative_path, text))
    return findings


def format_findings(findings: list[ResearchGuardrailFinding]) -> str:
    lines = [
        "[research-guardrails] Found research workflow regressions.",
        "Playground notebooks must stay viewer-only and point at a real runner.",
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
        else list_playground_files(root)
    )
    findings = scan_playground_files(root, files)
    if findings:
        print(format_findings(findings), file=sys.stderr)
        return 1
    print("[research-guardrails] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
