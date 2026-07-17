#!/usr/bin/env python3
"""Audit root Codex skills for governance rules."""

from __future__ import annotations

import argparse
import glob
import hashlib
import re
import subprocess
import sys
from pathlib import Path

DELETED_TASK16_HTTP_SCHEMA_PATHS = (
    "apps/bt/src/entrypoints/http/schemas/analytics_margin.py",
    "apps/bt/src/entrypoints/http/schemas/analytics_roe.py",
    "apps/bt/src/entrypoints/http/schemas/chart.py",
    "apps/bt/src/entrypoints/http/schemas/dataset_data.py",
    "apps/bt/src/entrypoints/http/schemas/jquants.py",
    "apps/bt/src/entrypoints/http/schemas/market_data.py",
)

BANNED_PATTERNS = [
    re.compile(r"localhost:3001"),
    re.compile(r"\b3001\b"),
    re.compile(r"\bHono\b"),
    re.compile(r"\bCLAUDE\.md\b"),
    re.compile(r"\.claude(?:/|$)"),
    re.compile(r"apps/ts/packages/api(?:/|$)"),
    re.compile(r"apps/bt/src/server/"),
    re.compile(r"apps/bt/src/lib/"),
    re.compile(r"Compatibility alias"),
    *(re.compile(re.escape(path)) for path in DELETED_TASK16_HTTP_SCHEMA_PATHS),
]

RETIRED_TS_DATA_PLANE_SURFACES = (
    "apps/ts/packages/utils/src/utils/dataset-paths.ts",
    "@trading25/utils/utils/dataset-paths",
    "getDatasetPath",
    "getMarketDbPath",
    "getPortfolioDbPath",
    "normalizeDatasetPath",
    "resolveDatasetPath",
)

LEGACY_PATHS = (
    ".claude",
    ".agents",
    "CLAUDE.md",
    "apps/ts/.claude",
    "apps/ts/.agents",
    "apps/ts/CLAUDE.md",
    "apps/bt/.claude",
    "apps/bt/.agents",
    "apps/bt/CLAUDE.md",
    "apps/ts/packages/web/.claude",
    "apps/ts/packages/web/.agents",
    "apps/ts/packages/web/CLAUDE.md",
)

WORKFLOW_SKILLS = {
    "bt-agent-system",
    "bt-api-architecture",
    "bt-cli-commands",
    "bt-database-management",
    "bt-financial-analysis",
    "bt-jquants-proxy-optimization",
    "bt-market-sync-strategies",
    "bt-optimization",
    "bt-research-workflow",
    "bt-signal-system",
    "bt-strategy-config",
    "trading25-dependabot-maintenance",
    "trading25-research-semantic-layer",
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
LOCAL_FILE_NAMES = {"AGENTS.md", "README.md", "SKILL.md"}
CODE_SPAN_PATTERN = re.compile(r"`([^`\n]+)`")
FRONTMATTER_PATTERN = re.compile(r"\A---\n([\s\S]*?)\n---\n")
SKILL_NAME_PATTERN = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
ROOT_SAFE_BUN_PREFIX = 'bun --cwd="$PWD/apps/ts" run '
VERIFICATION_SECTION_PATTERN = re.compile(
    r"^## Verification\s*$([\s\S]*?)(?=^## |\Z)", re.MULTILINE
)
PLACEHOLDER_COMMAND_PATTERN = re.compile(
    r"<[^>]+>|\b(?:TODO|TBD|YOUR_[A-Z0-9_]*)\b"
)
EXECUTABLE_COMMAND_PATTERN = re.compile(
    r"^(?:uv\s|bun\s|python3\s|rg\s|git\s|gh\s|!\s+rg\s)"
)

REACT_CATALOG_SKILL = "ts-vercel-react-best-practices"
REACT_CATALOG_PROVENANCE = (
    "build-web-apps@0.1.2",
    "skills/react-best-practices",
    "Catalog version: `1.0.0`",
)
REACT_AGENTS_SHA256 = "722aa11cb37a6fc3748414c095870e8547b95b370152371272ca2afb8db880f4"
REACT_RULES_SHA256 = "1dc5c38c674bc9dc42d2d3c7369cf0c0215a94d889faa34bb696000aac3eab7f"
REACT_RULE_FILES = frozenset(
    {
        "advanced-event-handler-refs.md",
        "advanced-init-once.md",
        "advanced-use-latest.md",
        "async-api-routes.md",
        "async-defer-await.md",
        "async-dependencies.md",
        "async-parallel.md",
        "async-suspense-boundaries.md",
        "bundle-barrel-imports.md",
        "bundle-conditional.md",
        "bundle-defer-third-party.md",
        "bundle-dynamic-imports.md",
        "bundle-preload.md",
        "client-event-listeners.md",
        "client-localstorage-schema.md",
        "client-passive-event-listeners.md",
        "client-swr-dedup.md",
        "js-batch-dom-css.md",
        "js-cache-function-results.md",
        "js-cache-property-access.md",
        "js-cache-storage.md",
        "js-combine-iterations.md",
        "js-early-exit.md",
        "js-flatmap-filter.md",
        "js-hoist-regexp.md",
        "js-index-maps.md",
        "js-length-check-first.md",
        "js-min-max-loop.md",
        "js-set-map-lookups.md",
        "js-tosorted-immutable.md",
        "rendering-activity.md",
        "rendering-animate-svg-wrapper.md",
        "rendering-conditional-render.md",
        "rendering-content-visibility.md",
        "rendering-hoist-jsx.md",
        "rendering-hydration-no-flicker.md",
        "rendering-hydration-suppress-warning.md",
        "rendering-resource-hints.md",
        "rendering-script-defer-async.md",
        "rendering-svg-precision.md",
        "rendering-usetransition-loading.md",
        "rerender-defer-reads.md",
        "rerender-dependencies.md",
        "rerender-derived-state-no-effect.md",
        "rerender-derived-state.md",
        "rerender-functional-setstate.md",
        "rerender-lazy-state-init.md",
        "rerender-memo-with-default-value.md",
        "rerender-memo.md",
        "rerender-move-effect-to-event.md",
        "rerender-no-inline-components.md",
        "rerender-simple-expression-in-memo.md",
        "rerender-split-combined-hooks.md",
        "rerender-transitions.md",
        "rerender-use-deferred-value.md",
        "rerender-use-ref-transient-values.md",
        "server-after-nonblocking.md",
        "server-auth-actions.md",
        "server-cache-lru.md",
        "server-cache-react.md",
        "server-dedup-props.md",
        "server-hoist-static-io.md",
        "server-parallel-fetching.md",
        "server-serialization.md",
    }
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


def frontmatter_text(content: str) -> str | None:
    match = FRONTMATTER_PATTERN.match(content)
    return match.group(1) if match is not None else None


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
    if token.startswith("scripts/"):
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


def verification_commands(content: str) -> tuple[str, ...]:
    match = VERIFICATION_SECTION_PATTERN.search(content)
    if match is None:
        return ()
    return tuple(CODE_SPAN_PATTERN.findall(match.group(1)))


def validate_verification_commands(content: str, skill_file: Path) -> list[str]:
    errors: list[str] = []
    commands = verification_commands(content)
    for command in commands:
        if PLACEHOLDER_COMMAND_PATTERN.search(command):
            errors.append(
                f"Verification command contains a placeholder: {skill_file} -> {command}"
            )
        if command.startswith("uv run ") and not command.startswith(
            "uv run --directory apps/bt "
        ):
            errors.append(
                f"Verification must use a root-safe uv command: {skill_file} -> {command}"
            )
        if command.startswith("bun "):
            bun_payload = command.removeprefix(ROOT_SAFE_BUN_PREFIX).strip()
            if (
                not command.startswith(ROOT_SAFE_BUN_PREFIX)
                or not bun_payload
                or bun_payload in {"--help", "-h"}
            ):
                errors.append(
                    f"Verification must use a root-safe bun command: {skill_file} -> {command}"
                )
        if command.startswith("python "):
            errors.append(f"Verification must use python3: {skill_file} -> {command}")
    if not any(EXECUTABLE_COMMAND_PATTERN.match(command) for command in commands):
        errors.append(f"Verification must include an executable command: {skill_file}")
    return errors


def _catalog_digest(files: list[Path]) -> str:
    digest = hashlib.sha256()
    for path in sorted(files, key=lambda candidate: candidate.name):
        digest.update(path.name.encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def validate_react_catalog(repo_root: Path) -> list[str]:
    errors: list[str] = []
    skill_root = repo_root / ".codex/skills" / REACT_CATALOG_SKILL
    skill_file = skill_root / "SKILL.md"
    agents_file = skill_root / "AGENTS.md"
    rules_root = skill_root / "rules"

    if not skill_file.exists():
        return [f"React catalog skill is missing: {skill_file}"]

    skill_content = skill_file.read_text()
    for marker in REACT_CATALOG_PROVENANCE:
        if marker not in skill_content:
            errors.append(f"React catalog provenance is missing {marker!r}: {skill_file}")

    if not agents_file.exists():
        errors.append(f"React catalog handbook is missing: {agents_file}")
    elif hashlib.sha256(agents_file.read_bytes()).hexdigest() != REACT_AGENTS_SHA256:
        errors.append(f"React catalog handbook drifted from pinned upstream: {agents_file}")

    rule_files = sorted(rules_root.glob("*.md"))
    actual_inventory = {path.name for path in rule_files}
    missing = sorted(REACT_RULE_FILES - actual_inventory)
    unexpected = sorted(actual_inventory - REACT_RULE_FILES)
    if missing:
        errors.append(f"React catalog rules are missing: {', '.join(missing)}")
    if unexpected:
        errors.append(f"React catalog rules are unexpected: {', '.join(unexpected)}")
    if not missing and not unexpected and _catalog_digest(rule_files) != REACT_RULES_SHA256:
        errors.append(f"React catalog rules drifted from pinned upstream: {rules_root}")

    return errors


def validate_skill_file(skill_file: Path, repo_root: Path) -> list[str]:
    errors: list[str] = []
    content = skill_file.read_text()
    fm = parse_frontmatter(content)
    if fm is None:
        return [f"Invalid or missing frontmatter: {skill_file}"]

    fm_text = frontmatter_text(content)
    if fm_text is None:
        return [f"Invalid or missing frontmatter: {skill_file}"]
    if len(fm_text) > 1024:
        errors.append(f"Frontmatter must not exceed 1024 characters: {skill_file}")

    required = {"name", "description"}
    keys = set(fm)
    if keys != required:
        errors.append(f"Frontmatter must contain only name/description: {skill_file} (got: {sorted(keys)})")
        return errors

    skill_name = fm["name"]
    if len(skill_name) > 64 or SKILL_NAME_PATTERN.fullmatch(skill_name) is None:
        errors.append(
            f"Skill name must be 1-64 lowercase letters, numbers, or single hyphens: {skill_file}"
        )
    if skill_name != skill_file.parent.name:
        errors.append(f"Frontmatter name must match skill directory: {skill_file}")

    if skill_name not in SUPPORTED_SKILLS:
        errors.append(f"Unsupported skill name: {skill_file}")

    description = fm["description"]
    if not description.startswith("Use when "):
        errors.append(f"Description must start with 'Use when ': {skill_file}")

    if skill_name in SHORTHAND_SKILLS and "supported shorthand" not in description.lower():
        errors.append(f"Shorthand skill must describe itself as supported shorthand: {skill_file}")

    for heading in _required_headings(skill_name):
        if heading not in content:
            errors.append(f"Missing required heading {heading!r}: {skill_file}")

    for pattern in BANNED_PATTERNS:
        if pattern.search(content):
            errors.append(f"Banned pattern {pattern.pattern!r} found in {skill_file}")

    for retired_surface in RETIRED_TS_DATA_PLANE_SURFACES:
        if retired_surface in content:
            errors.append(
                f"Retired TypeScript Data Plane surface found in {skill_file}: {retired_surface}"
            )

    if skill_name in WORKFLOW_SKILLS:
        errors.extend(validate_verification_commands(content, skill_file))

    for token in CODE_SPAN_PATTERN.findall(content):
        if not _looks_like_repo_path(token):
            continue
        candidates = _resolve_path_candidates(token, skill_file, repo_root)
        if not any(_candidate_exists(path) for path in candidates):
            errors.append(f"Referenced path not found: {skill_file} -> {token}")

    return errors


def find_legacy_paths(repo_root: Path) -> list[str]:
    return sorted(
        legacy_path
        for legacy_path in LEGACY_PATHS
        if (repo_root / legacy_path).exists()
    )


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

    errors.extend(validate_react_catalog(repo_root))

    refresh_script = repo_root / "scripts/skills/refresh_skill_references.py"
    refresh = run([sys.executable, str(refresh_script), "--check"], cwd=repo_root, check=False)
    if refresh.returncode != 0:
        errors.append("Generated references are stale. Run refresh_skill_references.py.")
        if refresh.stdout.strip():
            errors.append(refresh.stdout.strip())
        if refresh.stderr.strip():
            errors.append(refresh.stderr.strip())

    if args.strict_legacy:
        for legacy_path in find_legacy_paths(repo_root):
            errors.append(f"Legacy Claude/agents path must not exist: {legacy_path}")

    if errors:
        print("Skill audit failed:")
        for err in errors:
            print(f"- {err}")
        return 1

    print("Skill audit passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
