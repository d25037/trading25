#!/usr/bin/env python3
"""Detect user-specific local path leaks before push.

This checker is intentionally narrow. It blocks absolute local paths that can
leak a developer's home directory, Codex workspace, or local trading25 data
directory, while allowing generic sample paths such as `/Users/john/project`.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
import re


REPO_ROOT = Path(__file__).resolve().parents[1]
SKIPPED_PATH_PARTS = {".git", ".venv", "node_modules", "__pycache__"}
SKIPPED_RELATIVE_PATHS = {
    "scripts/check-privacy-leaks.py",
    "apps/bt/tests/unit/scripts/test_check_privacy_leaks.py",
}

PATH_TOKEN = r"(?:file://)?(?:/Users|/home)/[^/\s\"'`]+"
FINDING_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "repo-local-path",
        re.compile(rf"(?P<path>{PATH_TOKEN}/dev/trading25(?:/[^\s\"'`)]*)?)"),
    ),
    (
        "codex-or-agents-home-path",
        re.compile(rf"(?P<path>{PATH_TOKEN}/\.(?:codex|agents)(?:/[^\s\"'`)]*)?)"),
    ),
    (
        "trading25-data-home-path",
        re.compile(rf"(?P<path>{PATH_TOKEN}/\.local/share/trading25(?:/[^\s\"'`)]*)?)"),
    ),
)


@dataclass(frozen=True)
class PrivacyLeakFinding:
    relative_path: Path
    line_number: int
    pattern_name: str
    path_value: str
    line_text: str


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fail when tracked or untracked repo files contain user-specific "
            "absolute local paths for this repo, Codex worktrees, or the local "
            "trading25 data directory."
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


def list_repo_files(root: Path) -> list[Path]:
    result = subprocess.run(
        ["git", "-C", str(root), "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
        capture_output=True,
        check=True,
    )
    return sorted(
        Path(item.decode("utf-8"))
        for item in result.stdout.split(b"\0")
        if item
    )


def should_skip_path(relative_path: Path) -> bool:
    if relative_path.as_posix() in SKIPPED_RELATIVE_PATHS:
        return True
    return any(part in SKIPPED_PATH_PARTS for part in relative_path.parts)


def find_privacy_leaks_in_text(
    relative_path: Path,
    text: str,
) -> list[PrivacyLeakFinding]:
    findings: list[PrivacyLeakFinding] = []
    for line_number, line_text in enumerate(text.splitlines(), start=1):
        for pattern_name, pattern in FINDING_PATTERNS:
            for match in pattern.finditer(line_text):
                findings.append(
                    PrivacyLeakFinding(
                        relative_path=relative_path,
                        line_number=line_number,
                        pattern_name=pattern_name,
                        path_value=match.group("path"),
                        line_text=line_text.strip(),
                    )
                )
    return findings


def scan_files(root: Path, files: list[Path]) -> list[PrivacyLeakFinding]:
    findings: list[PrivacyLeakFinding] = []
    for relative_path in files:
        if should_skip_path(relative_path):
            continue
        absolute_path = root / relative_path
        if not absolute_path.is_file():
            continue
        text = absolute_path.read_text(encoding="utf-8", errors="ignore")
        findings.extend(find_privacy_leaks_in_text(relative_path, text))
    return findings


def format_findings(findings: list[PrivacyLeakFinding]) -> str:
    lines = [
        "[privacy-leak-check] Found user-specific local path leaks.",
        "Replace them with repo-relative paths or `~/.local/share/trading25/...`.",
        "",
    ]
    for finding in findings:
        lines.append(
            f"{finding.relative_path.as_posix()}:{finding.line_number}: "
            f"{finding.pattern_name}: {finding.path_value}"
        )
        lines.append(f"  {finding.line_text}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    root = args.root.resolve()
    files = (
        [_normalize_relative_path(root, path) for path in args.files]
        if args.files
        else list_repo_files(root)
    )
    findings = scan_files(root, files)
    if findings:
        print(format_findings(findings), file=sys.stderr)
        return 1
    print("[privacy-leak-check] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
