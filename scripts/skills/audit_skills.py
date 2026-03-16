#!/usr/bin/env python3
"""Audit root Codex skills for governance rules."""

from __future__ import annotations

import argparse
import glob
import re
import subprocess
import sys
from pathlib import Path

BANNED_PATTERNS = [
    re.compile(r"localhost:3001"),
    re.compile(r"\b3001\b"),
    re.compile(r"\bHono\b"),
    re.compile(r"apps/ts/packages/api(?:/|$)"),
    re.compile(r"apps/bt/src/server/"),
    re.compile(r"apps/bt/src/lib/"),
    re.compile(r"Compatibility alias"),
]

LEGACY_DIRS = (
    "apps/ts/.claude",
    "apps/bt/.claude",
)

WORKFLOW_SKILLS = {
    "bt-agent-system",
    "bt-api-architecture",
    "bt-cli-commands",
    "bt-database-management",
    "bt-financial-analysis",
    "bt-jquants-proxy-optimization",
    "bt-marimo-playground",
    "bt-market-sync-strategies",
    "bt-optimization",
    "bt-signal-system",
    "bt-strategy-config",
    "ts-api-endpoints",
    "ts-dataset-management",
    "ts-financial-analysis",
    "ts-portfolio-management",
    "ts-use-gunshi-cli",
    "ts-web-design-guidelines",
}
RULE_CATALOG_SKILLS = {"ts-vercel-react-best-practices"}
SHORTHAND_SKILLS = {"api-endpoints"}
SUPPORTED_SKILLS = WORKFLOW_SKILLS | RULE_CATALOG_SKILLS | SHORTHAND_SKILLS

WORKFLOW_REQUIRED_HEADINGS = (
    "## When to use",
    "## Source of Truth",
    "## Workflow",
    "## Guardrails",
    "## Verification",
)
RULE_CATALOG_REQUIRED_HEADINGS = (
    "## References",
    "## Usage",
)
SHORTHAND_REQUIRED_HEADINGS = (
    "## Canonical skill",
    "## Usage",
)

ROOT_PATH_PREFIXES = (
    "apps/",
    "contracts/",
    "docs/",
    "issues/",
    ".codex/",
    "scripts/",
)
SKILL_LOCAL_PREFIXES = (
    "references/",
    "rules/",
    "scripts/",
    "assets/",
)
LOCAL_FILE_NAMES = {"AGENTS.md", "README.md", "CLAUDE.md", "SKILL.md"}
CODE_SPAN_PATTERN = re.compile(r"`([^`\n]+)`")


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


def _required_headings(skill_name: str) -> tuple[str, ...]:
    if skill_name in WORKFLOW_SKILLS:
        return WORKFLOW_REQUIRED_HEADINGS
    if skill_name in RULE_CATALOG_SKILLS:
        return RULE_CATALOG_REQUIRED_HEADINGS
    if skill_name in SHORTHAND_SKILLS:
        return SHORTHAND_REQUIRED_HEADINGS
    return ()


def _looks_like_repo_path(token: str) -> bool:
    if " " in token or token.startswith("$") or token.startswith("@"):
        return False
    if "://" in token:
        return False
    if token in LOCAL_FILE_NAMES:
        return True
    if token.startswith(("./", "../")):
        return True
    if token.startswith(SKILL_LOCAL_PREFIXES):
        return True
    return token.startswith(ROOT_PATH_PREFIXES)


def _resolve_path_candidates(token: str, skill_file: Path, repo_root: Path) -> list[Path]:
    if token.startswith("/"):
        return [Path(token)]
    if token in LOCAL_FILE_NAMES:
        return [skill_file.parent / token, repo_root / token]
    if token.startswith(("./", "../")) or token.startswith(SKILL_LOCAL_PREFIXES):
        return [skill_file.parent / token]
    return [repo_root / token]


def _candidate_exists(path: Path) -> bool:
    path_text = str(path)
    if any(ch in path_text for ch in "*?["):
        return bool(glob.glob(path_text, recursive=True))
    if "{" in path_text or "}" in path_text:
        return False
    return path.exists()


def validate_skill_file(skill_file: Path, repo_root: Path) -> list[str]:
    errors: list[str] = []
    content = skill_file.read_text()
    fm = parse_frontmatter(content)
    if fm is None:
        return [f"Invalid or missing frontmatter: {skill_file}"]

    required = {"name", "description"}
    keys = set(fm)
    if keys != required:
        errors.append(f"Frontmatter must contain only name/description: {skill_file} (got: {sorted(keys)})")
        return errors

    skill_name = fm["name"]
    if skill_name != skill_file.parent.name:
        errors.append(f"Frontmatter name must match skill directory: {skill_file}")

    if skill_name not in SUPPORTED_SKILLS:
        errors.append(f"Unsupported skill name: {skill_file}")

    if skill_name in SHORTHAND_SKILLS and "supported shorthand" not in fm["description"].lower():
        errors.append(f"Shorthand skill must describe itself as supported shorthand: {skill_file}")

    for heading in _required_headings(skill_name):
        if heading not in content:
            errors.append(f"Missing required heading {heading!r}: {skill_file}")

    for pattern in BANNED_PATTERNS:
        if pattern.search(content):
            errors.append(f"Banned pattern {pattern.pattern!r} found in {skill_file}")

    for token in CODE_SPAN_PATTERN.findall(content):
        if not _looks_like_repo_path(token):
            continue
        candidates = _resolve_path_candidates(token, skill_file, repo_root)
        if not any(_candidate_exists(path) for path in candidates):
            errors.append(f"Referenced path not found: {skill_file} -> {token}")

    return errors


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
        errors.extend(validate_skill_file(skill_file, repo_root))

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
