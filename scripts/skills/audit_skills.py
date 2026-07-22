#!/usr/bin/env python3
"""Audit root Codex skills for governance rules."""

from __future__ import annotations

import argparse
import ast
import glob
import re
import shlex
import subprocess
import sys
from pathlib import Path

from verify_react_catalog import validate_local_catalog

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
VERIFICATION_SECTION_PATTERN = re.compile(
    r"^## Verification\s*$([\s\S]*?)(?=^## |\Z)", re.MULTILINE
)
VERIFICATION_FENCE_PATTERN = re.compile(
    r"^```(?:bash|sh)\s*$([\s\S]*?)^```\s*$", re.MULTILINE
)
PLACEHOLDER_COMMAND_PATTERN = re.compile(
    r"<[^>]+>|\b(?:TODO|TBD|YOUR_[A-Z0-9_]*)\b"
)
HELP_VERSION_TOKENS = {"--help", "-h", "help", "--version", "-V", "-v", "version"}
ALLOWED_VERIFICATION_PROGRAMS = {"uv", "bun", "python3", "rg", "git", "gh"}
ROOT_SAFE_BUN_PREFIX = 'bun --cwd="$PWD/apps/ts" run '
VERIFICATION_PATH_PREFIXES = (
    "tests/",
    "src/",
    "scripts/",
    "config/",
    "apps/",
    "contracts/",
    "docs/",
    "issues/",
    ".codex/",
)
def parse_frontmatter(content: str) -> dict[str, str] | None:
    fm_raw = frontmatter_text(content)
    if fm_raw is None:
        return None

    data: dict[str, str] = {}
    for raw_line in fm_raw.splitlines():
        if not raw_line or raw_line != raw_line.strip() or ":" not in raw_line:
            return None
        key, raw_value = raw_line.split(":", 1)
        if key not in {"name", "description"} or key in data:
            return None
        value = raw_value.lstrip()
        if not value:
            return None
        if value[0] in {'"', "'"}:
            if len(value) < 2 or value[-1] != value[0]:
                return None
            try:
                parsed = ast.literal_eval(value)
            except (SyntaxError, ValueError):
                return None
            if not isinstance(parsed, str):
                return None
            value = parsed
        elif '"' in value or "'" in value:
            return None
        data[key] = value
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
    commands: list[str] = []
    for block in VERIFICATION_FENCE_PATTERN.findall(match.group(1)):
        commands.extend(
            line.strip()
            for line in block.splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        )
    return tuple(commands)


def _scan_shell_syntax(
    command: str,
) -> tuple[list[tuple[int, str]], list[tuple[int, str]]]:
    """Find effective expansions and unsupported syntax in the shell subset.

    The subset models single/double quotes and backslash escaping, and recognizes
    named/braced/positional/special parameters, command substitution, modern and
    legacy arithmetic expansion, and backticks. Shell operators are validated
    separately. ANSI-C ``$'...'`` and locale ``$"..."`` quoting, multiline
    commands, heredocs, and shell evaluation are unsupported.
    """
    expansions: list[tuple[int, str]] = []
    unsupported: list[tuple[int, str]] = []
    quote: str | None = None
    index = 0
    while index < len(command):
        char = command[index]
        if quote == "single":
            if char == "'":
                quote = None
            index += 1
            continue
        if char == "\\":
            index += 2
            continue
        if char == "'" and quote is None:
            quote = "single"
            index += 1
            continue
        if char == '"':
            quote = None if quote == "double" else "double"
            index += 1
            continue
        if char == "`":
            expansions.append((index, "`"))
            index += 1
            continue
        if char != "$" or index + 1 >= len(command):
            index += 1
            continue

        next_char = command[index + 1]
        if next_char in "'\"":
            unsupported.append((index, command[index : index + 2]))
            index += 1
            continue
        if next_char in "({[":
            expansions.append((index, command[index : index + 2]))
            index += 2
            continue
        if next_char.isdigit():
            end = index + 2
            while end < len(command) and command[end].isdigit():
                end += 1
            expansions.append((index, command[index:end]))
            index = end
            continue
        if next_char in "?@*#$!-":
            expansions.append((index, command[index : index + 2]))
            index += 2
            continue
        if next_char.isalpha() or next_char == "_":
            end = index + 2
            while end < len(command) and (command[end].isalnum() or command[end] == "_"):
                end += 1
            expansions.append((index, command[index:end]))
            index = end
            continue
        index += 1
    return expansions, unsupported


def _command_tokens(command: str) -> tuple[list[str] | None, str | None]:
    try:
        lexer = shlex.shlex(command, posix=True, punctuation_chars=";&|<>")
        lexer.whitespace_split = True
        lexer.commenters = ""
        tokens = list(lexer)
    except ValueError:
        return None, "Verification command has invalid shell quoting"
    if any(token and set(token) <= set(";&|<>") for token in tokens):
        return None, "Shell control operators are not allowed"
    return tokens, None


def _verification_path_arguments(tokens: list[str]) -> tuple[Path, list[str]] | None:
    if tokens[0] == "uv" and len(tokens) > 4:
        return Path("apps/bt"), tokens[5:]
    if tokens[0] == "python3" and len(tokens) > 1:
        return Path(), [tokens[1]]
    if tokens[0] == "rg":
        positional = [token for token in tokens[1:] if not token.startswith("-")]
        return Path(), positional[1:]
    return None


def _looks_like_verification_path(token: str) -> bool:
    path_part = token.split("::", 1)[0]
    return (
        path_part.startswith(("./", "../", "/", *VERIFICATION_PATH_PREFIXES))
        or Path(path_part).suffix in {".py", ".toml", ".yaml", ".yml", ".json"}
    )


def _validate_verification_paths(
    tokens: list[str],
    repo_root: Path,
    skill_file: Path,
    command: str,
) -> list[str]:
    path_arguments = _verification_path_arguments(tokens)
    if path_arguments is None:
        return []
    relative_root, arguments = path_arguments
    canonical_root = repo_root.resolve(strict=False)
    base = canonical_root / relative_root
    errors: list[str] = []
    for argument in arguments:
        if argument.startswith("-") or not _looks_like_verification_path(argument):
            continue
        path_text = argument.split("::", 1)[0]
        raw_candidate = Path(path_text)
        candidate = raw_candidate if raw_candidate.is_absolute() else base / raw_candidate
        canonical_candidate = candidate.resolve(strict=False)
        try:
            canonical_candidate.relative_to(canonical_root)
        except ValueError:
            errors.append(
                f"Verification path must stay within the repository: "
                f"{skill_file} -> {command} -> {path_text}"
            )
            continue

        candidate_text = str(canonical_candidate)
        if any(char in candidate_text for char in "*?["):
            matches = [Path(match).resolve(strict=False) for match in glob.glob(candidate_text)]
            if not matches:
                errors.append(
                    f"Verification path not found: {skill_file} -> {command} -> {path_text}"
                )
                continue
            escaped_match = next(
                (
                    match
                    for match in matches
                    if not match.is_relative_to(canonical_root)
                ),
                None,
            )
            if escaped_match is not None:
                errors.append(
                    f"Verification path must stay within the repository: "
                    f"{skill_file} -> {command} -> {path_text}"
                )
        elif not canonical_candidate.exists():
            errors.append(
                f"Verification path not found: {skill_file} -> {command} -> {path_text}"
            )
    return errors


def _validate_command(
    command: str,
    skill_file: Path,
    repo_root: Path,
) -> tuple[list[str], bool]:
    errors: list[str] = []
    tokens, token_error = _command_tokens(command)
    if token_error is not None:
        return [f"{token_error}: {skill_file} -> {command}"], False
    if not tokens:
        return [f"Verification command is empty: {skill_file}"], False

    expansions, unsupported = _scan_shell_syntax(command)
    if unsupported:
        return [f"Verification command uses unsupported Bash dollar-quote syntax: {skill_file} -> {command}"], False
    pwd_offset = ROOT_SAFE_BUN_PREFIX.index("$PWD")
    exact_pwd_bun = (
        expansions == [(pwd_offset, "$PWD")]
        and command.startswith(ROOT_SAFE_BUN_PREFIX)
    )
    if expansions and not exact_pwd_bun:
        return [f"Verification command has unresolved variable expansion: {skill_file} -> {command}"], False

    program = tokens[0]
    if program not in ALLOWED_VERIFICATION_PROGRAMS:
        return [f"Verification command uses an unsupported executable: {skill_file} -> {command}"], False

    if any(token in HELP_VERSION_TOKENS for token in tokens[1:]):
        errors.append(f"Verification command must not be help/version-only: {skill_file} -> {command}")

    root_safe = True
    if program == "uv":
        root_safe = tokens[:4] == ["uv", "run", "--directory", "apps/bt"] and len(tokens) > 4
        if not root_safe:
            errors.append(f"Verification must use a root-safe uv command: {skill_file} -> {command}")
    elif program == "bun":
        root_safe = (
            len(tokens) > 3
            and command.startswith(ROOT_SAFE_BUN_PREFIX)
            and tokens[1] == "--cwd=$PWD/apps/ts"
            and tokens[2] == "run"
        )
        if not root_safe:
            errors.append(f"Verification must use a root-safe bun command: {skill_file} -> {command}")
    elif program == "python3":
        root_safe = len(tokens) > 1 and tokens[1].endswith(".py")
        if not root_safe and not any(token in HELP_VERSION_TOKENS for token in tokens[1:]):
            errors.append(
                f"Verification python3 command must execute a repository script: {skill_file} -> {command}"
            )

    if root_safe:
        errors.extend(_validate_verification_paths(tokens, repo_root, skill_file, command))

    substantive = root_safe and not errors and not (
        program == "git" and len(tokens) >= 2 and tokens[1] == "status"
    )
    return errors, substantive


def validate_verification_commands(
    content: str,
    skill_file: Path,
    repo_root: Path,
) -> list[str]:
    errors: list[str] = []
    section_match = VERIFICATION_SECTION_PATTERN.search(content)
    if section_match is None or not VERIFICATION_FENCE_PATTERN.search(section_match.group(1)):
        errors.append(f"Verification must include a fenced bash/sh command block: {skill_file}")
    commands = verification_commands(content)
    substantive = False
    for command in commands:
        if PLACEHOLDER_COMMAND_PATTERN.search(command):
            errors.append(
                f"Verification command contains a placeholder: {skill_file} -> {command}"
            )
        command_errors, command_substantive = _validate_command(command, skill_file, repo_root)
        errors.extend(command_errors)
        substantive = substantive or command_substantive
    if not substantive:
        errors.append(f"Verification must include an executable command: {skill_file}")
    return errors


def validate_react_catalog(repo_root: Path) -> list[str]:
    return validate_local_catalog(repo_root)


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
        errors.extend(validate_verification_commands(content, skill_file, repo_root))
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
