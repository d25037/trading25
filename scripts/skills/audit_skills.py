#!/usr/bin/env python3
"""Audit root Codex skills for governance rules."""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path

BANNED_PATTERNS = [
    re.compile(r"localhost:3001"),
    re.compile(r"\\b3001\\b"),
    re.compile(r"\\bHono\\b"),
    re.compile(r"apps/ts/packages/api"),
]

LEGACY_DIRS = (
    "apps/ts/.claude/skills",
    "apps/bt/.claude/skills",
)


def parse_frontmatter(content: str) -> dict[str, str] | None:
    if not content.startswith("---\n"):
        return None
    try:
        _, rest = content.split("---\n", 1)
        fm_raw, _ = rest.split("\n---\n", 1)
    except ValueError:
        return None

    data: dict[str, str] = {}
    for raw_line in fm_raw.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if ":" not in line:
            return None
        key, value = line.split(":", 1)
        data[key.strip()] = value.strip().strip('"').strip("'")
    return data


def run(cmd: list[str], cwd: Path, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, cwd=cwd, text=True, capture_output=True, check=check)


def changed_files(repo_root: Path) -> list[str]:
    base_ref = os.getenv("GITHUB_BASE_REF")

    if base_ref:
        candidate = ["git", "diff", "--name-only", f"origin/{base_ref}...HEAD"]
        result = run(candidate, cwd=repo_root, check=False)
        if result.returncode == 0:
            return [line for line in result.stdout.splitlines() if line.strip()]

    if os.getenv("GITHUB_EVENT_NAME") == "push":
        result = run(["git", "diff", "--name-only", "HEAD~1...HEAD"], cwd=repo_root, check=False)
        if result.returncode == 0:
            return [line for line in result.stdout.splitlines() if line.strip()]

    staged = run(["git", "diff", "--name-only", "--cached"], cwd=repo_root, check=False)
    unstaged = run(["git", "diff", "--name-only"], cwd=repo_root, check=False)
    files = set(staged.stdout.splitlines()) | set(unstaged.stdout.splitlines())
    return [line for line in sorted(files) if line.strip()]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--strict-legacy", action="store_true", help="Fail if legacy files (except LEGACY.md) changed")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[2]
    skills_root = repo_root / ".codex/skills"
    errors: list[str] = []

    skill_files = sorted(skills_root.glob("*/SKILL.md"))
    if not skill_files:
        errors.append("No SKILL.md found under .codex/skills")

    for skill_file in skill_files:
        content = skill_file.read_text()

        fm = parse_frontmatter(content)
        if fm is None:
            errors.append(f"Invalid or missing frontmatter: {skill_file}")
            continue

        required = {"name", "description"}
        keys = set(fm)
        if keys != required:
            errors.append(f"Frontmatter must contain only name/description: {skill_file} (got: {sorted(keys)})")

        for pattern in BANNED_PATTERNS:
            if pattern.search(content):
                errors.append(f"Banned pattern {pattern.pattern!r} found in {skill_file}")

    refresh_script = repo_root / "scripts/skills/refresh_skill_references.py"
    refresh = run([sys.executable, str(refresh_script), "--check"], cwd=repo_root, check=False)
    if refresh.returncode != 0:
        errors.append("Generated references are stale. Run refresh_skill_references.py.")
        if refresh.stdout.strip():
            errors.append(refresh.stdout.strip())
        if refresh.stderr.strip():
            errors.append(refresh.stderr.strip())

    if args.strict_legacy:
        changed = changed_files(repo_root)
        for path in changed:
            if any(path.startswith(prefix) for prefix in LEGACY_DIRS):
                if Path(path).name != "LEGACY.md":
                    errors.append(f"Legacy skill file modified (read-only violation): {path}")

    if errors:
        print("Skill audit failed:")
        for err in errors:
            print(f"- {err}")
        return 1

    print("Skill audit passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
