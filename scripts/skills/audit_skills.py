#!/usr/bin/env python3
"""Audit root Codex skills for governance rules."""

from __future__ import annotations

import argparse
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
    "apps/ts/.claude",
    "apps/bt/.claude",
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


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--strict-legacy", action="store_true", help="Fail if legacy skill directories exist")
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
        for legacy_dir in LEGACY_DIRS:
            if (repo_root / legacy_dir).exists():
                errors.append(f"Legacy skill directory must not exist: {legacy_dir}")

    if errors:
        print("Skill audit failed:")
        for err in errors:
            print(f"- {err}")
        return 1

    print("Skill audit passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
