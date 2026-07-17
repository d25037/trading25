"""Tests for scripts/skills/audit_skills.py."""

from __future__ import annotations

import importlib
import shutil
import sys
from pathlib import Path

import pytest


def _load_audit_module():
    repo_root = Path(__file__).resolve().parents[5]
    module_dir = repo_root / "scripts/skills"
    module_dir_str = str(module_dir)
    if module_dir_str not in sys.path:
        sys.path.insert(0, module_dir_str)

    module = importlib.import_module("audit_skills")
    module = importlib.reload(module)
    return module


def _write(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    return path


def _workflow_skill(
    tmp_path: Path,
    skill_name: str,
    verification_command: str,
) -> Path:
    return _write(
        tmp_path / f".codex/skills/{skill_name}/SKILL.md",
        "\n".join(
            [
                "---",
                f"name: {skill_name}",
                "description: Use when the matching repository workflow is requested.",
                "---",
                "",
                f"# {skill_name}",
                "",
                "## When to use",
                "",
                "- Use for the matching workflow.",
                "",
                "## Source of Truth",
                "",
                "- Follow this skill.",
                "",
                "## Workflow",
                "",
                "1. Perform the workflow.",
                "",
                "## Guardrails",
                "",
                "- Keep changes scoped.",
                "",
                "## Verification",
                "",
                "```bash",
                verification_command,
                "```",
                "",
            ]
        ),
    )


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[5]


def test_repository_skill_descriptions_are_discovery_compliant() -> None:
    module = _load_audit_module()
    repo_root = _repo_root()

    errors = [
        error
        for skill_file in sorted((repo_root / ".codex/skills").glob("*/SKILL.md"))
        for error in module.validate_skill_file(skill_file, repo_root)
        if "Description must start with 'Use when '" in error
    ]

    assert errors == []


def test_description_without_use_when_trigger_is_rejected(tmp_path: Path) -> None:
    module = _load_audit_module()
    skill_file = _workflow_skill(
        tmp_path,
        "bt-api-architecture",
        "uv run --directory apps/bt pytest tests/unit/server/routes",
    )
    content = skill_file.read_text().replace(
        "description: Use when the matching repository workflow is requested.",
        "description: Canonical API architecture workflow.",
    )
    skill_file.write_text(content)

    errors = module.validate_skill_file(skill_file, tmp_path)

    assert any("Description must start with 'Use when '" in error for error in errors)


@pytest.mark.parametrize(
    "skill_name",
    (
        "a" * 65,
        "Invalid-Uppercase-Name",
        "invalid_name",
        "leading--separator",
    ),
)
def test_invalid_skill_name_limits_are_rejected(
    tmp_path: Path,
    skill_name: str,
) -> None:
    module = _load_audit_module()
    skill_file = _write(
        tmp_path / f".codex/skills/{skill_name}/SKILL.md",
        "\n".join(
            [
                "---",
                f"name: {skill_name}",
                "description: Use when validating an invalid skill name.",
                "---",
                "",
                f"# {skill_name}",
                "",
            ]
        ),
    )

    errors = module.validate_skill_file(skill_file, tmp_path)

    assert any("Skill name must be 1-64 lowercase" in error for error in errors)


def test_frontmatter_over_1024_characters_is_rejected(tmp_path: Path) -> None:
    module = _load_audit_module()
    skill_file = _write(
        tmp_path / ".codex/skills/bt-api-architecture/SKILL.md",
        "\n".join(
            [
                "---",
                "name: bt-api-architecture",
                f"description: Use when {'x' * 1000}",
                "---",
                "",
                "# bt-api-architecture",
                "",
            ]
        ),
    )

    errors = module.validate_skill_file(skill_file, tmp_path)

    assert any("Frontmatter must not exceed 1024 characters" in error for error in errors)


@pytest.mark.parametrize(
    "frontmatter",
    (
        "name: bt-api-architecture\nname: bt-api-architecture\ndescription: Use when API work changes.",
        "name: bt-api-architecture\ndescription: Use when API work changes.\ndescription: Use when routes change.",
        'name: bt-api-architecture\ndescription: "Use when API work changes.',
        "name: bt-api-architecture\ndescription: 'Use when API work changes.",
        "name: bt-api-architecture\ndescription Use when API work changes.",
        "name: bt-api-architecture\ndescription: Use when API work changes.\nmetadata: forbidden",
    ),
)
def test_malformed_or_duplicate_frontmatter_is_rejected(
    tmp_path: Path,
    frontmatter: str,
) -> None:
    module = _load_audit_module()
    skill_file = _write(
        tmp_path / ".codex/skills/bt-api-architecture/SKILL.md",
        f"---\n{frontmatter}\n---\n\n# bt-api-architecture\n",
    )

    errors = module.validate_skill_file(skill_file, tmp_path)

    assert any("Invalid or missing frontmatter" in error for error in errors)


@pytest.mark.parametrize(
    "verification_command",
    (
        "uv run --directory apps/bt pytest <affected tests>",
        "uv run --directory apps/bt python scripts/research/<runner>.py --help",
        "python3 TODO",
        "bun --cwd=\"$PWD/apps/ts\" run YOUR_SCRIPT",
    ),
)
def test_placeholder_verification_command_is_rejected(
    tmp_path: Path,
    verification_command: str,
) -> None:
    module = _load_audit_module()
    skill_file = _workflow_skill(
        tmp_path,
        "bt-api-architecture",
        verification_command,
    )

    errors = module.validate_skill_file(skill_file, tmp_path)

    assert any("Verification command contains a placeholder" in error for error in errors)


def test_workflow_skill_without_executable_verification_is_rejected(
    tmp_path: Path,
) -> None:
    module = _load_audit_module()
    skill_file = _workflow_skill(
        tmp_path,
        "bt-api-architecture",
        "confirm the change manually",
    )

    errors = module.validate_skill_file(skill_file, tmp_path)

    assert any("Verification must include an executable command" in error for error in errors)


def test_inline_verification_command_is_not_an_explicit_command_block(
    tmp_path: Path,
) -> None:
    module = _load_audit_module()
    skill_file = _workflow_skill(
        tmp_path,
        "bt-api-architecture",
        "uv run --directory apps/bt pytest tests/unit/server/routes",
    )
    skill_file.write_text(
        skill_file.read_text().replace(
            "```bash\nuv run --directory apps/bt pytest tests/unit/server/routes\n```",
            "`uv run --directory apps/bt pytest tests/unit/server/routes`",
        )
    )

    errors = module.validate_skill_file(skill_file, tmp_path)

    assert any("fenced bash/sh command block" in error for error in errors)


@pytest.mark.parametrize(
    "verification_command",
    (
        "git status --short && uv run --directory apps/bt pytest tests/unit/server/routes",
        'python3 -V; bun --cwd="$PWD/apps/ts" run workspace:test',
    ),
)
def test_shell_control_operator_bypass_is_rejected(
    tmp_path: Path,
    verification_command: str,
) -> None:
    module = _load_audit_module()
    skill_file = _workflow_skill(
        tmp_path,
        "bt-api-architecture",
        verification_command,
    )

    errors = module.validate_skill_file(skill_file, tmp_path)

    assert any("Shell control operators are not allowed" in error for error in errors)


@pytest.mark.parametrize(
    "verification_command",
    (
        'uv run --directory apps/bt pytest "$TARGETS"',
        'uv run --directory apps/bt pytest "${TARGETS}"',
        'python3 "$RUNNER.py"',
        "python3 scripts/check.py $1",
        "python3 scripts/check.py $0",
        "python3 scripts/check.py $?",
        "python3 scripts/check.py $@",
        "python3 scripts/check.py $*",
        "python3 scripts/check.py $$",
        "python3 scripts/check.py $#",
        "python3 scripts/check.py $-",
        "python3 scripts/check.py $!",
        "python3 scripts/check.py $(runner)",
        "python3 scripts/check.py $((1 + 1))",
        "python3 scripts/check.py `runner`",
        'bun --cwd="$PWD/apps/ts" run "$TARGET"',
        'bun --cwd="$PWD/apps/ts" run workspace:test $1',
    ),
)
def test_unresolved_verification_expansion_is_rejected(
    tmp_path: Path,
    verification_command: str,
) -> None:
    module = _load_audit_module()
    (tmp_path / "apps/bt").mkdir(parents=True)
    skill_file = _workflow_skill(
        tmp_path,
        "bt-api-architecture",
        verification_command,
    )

    errors = module.validate_skill_file(skill_file, tmp_path)

    assert any("unresolved variable expansion" in error for error in errors)


def test_nonexistent_fenced_verification_target_is_rejected(tmp_path: Path) -> None:
    module = _load_audit_module()
    (tmp_path / "apps/bt/tests/unit").mkdir(parents=True)
    skill_file = _workflow_skill(
        tmp_path,
        "bt-api-architecture",
        "uv run --directory apps/bt pytest tests/unit/does-not-exist.py",
    )

    errors = module.validate_skill_file(skill_file, tmp_path)

    assert any("Verification path not found" in error for error in errors)


@pytest.mark.parametrize("absolute", (False, True))
def test_fenced_verification_target_traversal_outside_repository_is_rejected(
    tmp_path: Path,
    absolute: bool,
) -> None:
    module = _load_audit_module()
    (tmp_path / "apps/bt/tests/unit").mkdir(parents=True)
    outside = _write(tmp_path.parent / f"{tmp_path.name}-outside.py", "print('outside')\n")
    target = f"{tmp_path}/../{outside.name}" if absolute else f"../../../{outside.name}"
    skill_file = _workflow_skill(
        tmp_path,
        "bt-api-architecture",
        f"uv run --directory apps/bt pytest {target}",
    )

    errors = module.validate_skill_file(skill_file, tmp_path)

    assert any("must stay within the repository" in error for error in errors)


def test_fenced_verification_symlink_escape_is_rejected(tmp_path: Path) -> None:
    module = _load_audit_module()
    target_dir = tmp_path / "apps/bt/tests/unit"
    target_dir.mkdir(parents=True)
    outside = _write(tmp_path.parent / f"{tmp_path.name}-outside.py", "print('outside')\n")
    (target_dir / "test_escape.py").symlink_to(outside)
    skill_file = _workflow_skill(
        tmp_path,
        "bt-api-architecture",
        "uv run --directory apps/bt pytest tests/unit/test_escape.py",
    )

    errors = module.validate_skill_file(skill_file, tmp_path)

    assert any("must stay within the repository" in error for error in errors)


def test_fenced_verification_accepts_exact_pwd_bun_existing_paths_node_ids_and_globs(
    tmp_path: Path,
) -> None:
    module = _load_audit_module()
    _write(tmp_path / "apps/bt/tests/unit/test_example.py", "def test_example(): pass\n")
    _write(tmp_path / "scripts/check.py", "print('ok')\n")
    (tmp_path / "apps/ts").mkdir(parents=True)
    skill_file = _workflow_skill(
        tmp_path,
        "bt-api-architecture",
        "\n".join(
            (
                "uv run --directory apps/bt pytest tests/unit/test_example.py::test_example",
                "uv run --directory apps/bt pytest tests/unit/test_*.py -q",
                "python3 scripts/check.py",
                'bun --cwd="$PWD/apps/ts" run workspace:test',
            )
        ),
    )

    assert module.validate_skill_file(skill_file, tmp_path) == []


@pytest.mark.parametrize(
    "verification_command",
    (
        "python3 -V",
        "uv --help",
        'bun --cwd="$PWD/apps/ts" run --help',
    ),
)
def test_help_or_version_only_verification_is_rejected(
    tmp_path: Path,
    verification_command: str,
) -> None:
    module = _load_audit_module()
    skill_file = _workflow_skill(
        tmp_path,
        "bt-api-architecture",
        verification_command,
    )

    errors = module.validate_skill_file(skill_file, tmp_path)

    assert any("help/version-only" in error for error in errors)


def test_valid_fenced_verification_commands_are_accepted(tmp_path: Path) -> None:
    module = _load_audit_module()
    (tmp_path / "apps/bt/tests/unit/server/routes").mkdir(parents=True)
    _write(tmp_path / "scripts/skills/refresh_skill_references.py", "print('ok')\n")
    skill_file = _workflow_skill(
        tmp_path,
        "bt-api-architecture",
        "\n".join(
            (
                "uv run --directory apps/bt pytest tests/unit/server/routes",
                "python3 scripts/skills/refresh_skill_references.py --check",
            )
        ),
    )

    assert module.validate_skill_file(skill_file, tmp_path) == []


@pytest.mark.parametrize(
    "retired_surface",
    (
        "apps/ts/packages/utils/src/utils/dataset-paths.ts",
        "@trading25/utils/utils/dataset-paths",
        "getDatasetPath",
        "getMarketDbPath",
        "getPortfolioDbPath",
        "normalizeDatasetPath",
        "resolveDatasetPath",
    ),
)
def test_retired_ts_dataset_path_surface_is_rejected(
    tmp_path: Path,
    retired_surface: str,
) -> None:
    module = _load_audit_module()
    skill_file = _workflow_skill(
        tmp_path,
        "ts-dataset-management",
        'bun --cwd="$PWD/apps/ts" run quality:typecheck',
    )
    skill_file.write_text(
        skill_file.read_text().replace(
            "- Follow this skill.",
            f"- Use `{retired_surface}`.",
        )
    )

    errors = module.validate_skill_file(skill_file, tmp_path)

    assert any("Retired TypeScript Data Plane surface" in error for error in errors)


def test_react_catalog_matches_pinned_provenance_and_inventory() -> None:
    module = _load_audit_module()

    assert module.validate_react_catalog(_repo_root()) == []


def _load_react_catalog_module():
    module_dir = _repo_root() / "scripts/skills"
    module_dir_str = str(module_dir)
    if module_dir_str not in sys.path:
        sys.path.insert(0, module_dir_str)
    module = importlib.import_module("verify_react_catalog")
    return importlib.reload(module)


def _create_react_source(tmp_path: Path) -> Path:
    source = tmp_path / "react-best-practices"
    local = _repo_root() / ".codex/skills/ts-vercel-react-best-practices"
    (source / "rules").mkdir(parents=True)
    shutil.copy2(local / "AGENTS.md", source / "AGENTS.md")
    for rule in (local / "rules").glob("*.md"):
        shutil.copy2(rule, source / "rules" / rule.name)
    _write(source / "metadata.json", '{"version": "1.0.0"}\n')
    return source


def test_react_catalog_verifier_matches_installed_source(tmp_path: Path) -> None:
    module = _load_react_catalog_module()

    assert module.verify_catalog(_repo_root(), _create_react_source(tmp_path)) == []


def test_react_catalog_normalization_and_digest_are_reproducible(tmp_path: Path) -> None:
    module = _load_react_catalog_module()
    source = _create_react_source(tmp_path)
    rule_files = module.source_rule_files(source)

    assert len(rule_files) == 64
    assert module.normalized_file_digest(source / "AGENTS.md") == (
        "722aa11cb37a6fc3748414c095870e8547b95b370152371272ca2afb8db880f4"
    )
    assert module.normalized_catalog_digest(rule_files) == (
        "dbe900a7c2412eed7d4afe026743d5262b7de29b9970b8c452decd81cf8ae5f0"
    )


def test_react_catalog_verifier_detects_upstream_content_drift(tmp_path: Path) -> None:
    module = _load_react_catalog_module()
    source = _create_react_source(tmp_path)
    drifted_rule = source / "rules/async-parallel.md"
    drifted_rule.write_text(drifted_rule.read_text() + "\nupstream drift\n")

    errors = module.verify_catalog(_repo_root(), source)

    assert any("differs from installed source" in error for error in errors)


def test_react_catalog_refresh_is_deterministic(tmp_path: Path) -> None:
    module = _load_react_catalog_module()
    repo_root = tmp_path / "repo"
    source = _create_react_source(tmp_path)

    module.refresh_catalog(repo_root, source)

    assert module.verify_catalog(repo_root, source) == []
    assert module.validate_local_catalog(repo_root) == []


def test_web_design_guideline_source_is_commit_pinned() -> None:
    skill_file = _repo_root() / ".codex/skills/ts-web-design-guidelines/SKILL.md"
    content = skill_file.read_text()

    assert (
        "https://raw.githubusercontent.com/vercel-labs/web-interface-guidelines/"
        "d0a657bfe87e86dd3a4753d7ec28c7e7dd7a88fe/command.md"
    ) in content
    assert "/web-interface-guidelines/main/" not in content


@pytest.mark.parametrize(
    "skill_name",
    ("bt-database-management", "bt-market-sync-strategies"),
)
def test_market_cutover_skills_retrieve_retained_promotion_contract(
    skill_name: str,
) -> None:
    module = _load_audit_module()
    skill_file = _repo_root() / f".codex/skills/{skill_name}/SKILL.md"
    content = skill_file.read_text()

    assert "bt market-cutover promote-retained" in content
    assert "bt market-cutover cutover" in content
    assert "noSync" in content
    assert "noJQuants" in content
    assert "same-attempt recovery" in content

    assert module.validate_market_cutover_guidance(content, skill_file) == []


def test_market_cutover_guidance_rejects_retained_promotion_contradiction() -> None:
    module = _load_audit_module()
    skill_file = _repo_root() / ".codex/skills/bt-database-management/SKILL.md"
    contradictory = skill_file.read_text() + (
        "\nRetained promotion with `bt market-cutover promote-retained` may run sync "
        "and J-Quants rebuild when the deadline is close.\n"
    )

    errors = module.validate_market_cutover_guidance(contradictory, skill_file)

    assert any("contradictory retained-promotion guidance" in error for error in errors)


def test_root_unsafe_uv_verification_command_is_rejected(tmp_path: Path) -> None:
    module = _load_audit_module()
    skill_file = _workflow_skill(
        tmp_path,
        "bt-api-architecture",
        "uv run --project apps/bt pytest tests/unit/server/routes",
    )

    errors = module.validate_skill_file(skill_file, tmp_path)

    assert any("root-safe uv command" in error for error in errors)


def test_root_unsafe_bun_verification_command_is_rejected(tmp_path: Path) -> None:
    module = _load_audit_module()
    skill_file = _workflow_skill(
        tmp_path,
        "ts-api-endpoints",
        "bun run quality:typecheck",
    )

    errors = module.validate_skill_file(skill_file, tmp_path)

    assert any("root-safe bun command" in error for error in errors)


def test_relative_bun_cwd_verification_command_is_rejected(tmp_path: Path) -> None:
    module = _load_audit_module()
    skill_file = _workflow_skill(
        tmp_path,
        "ts-api-endpoints",
        "bun --cwd apps/ts run quality:typecheck",
    )

    errors = module.validate_skill_file(skill_file, tmp_path)

    assert any("root-safe bun command" in error for error in errors)


def test_bun_verification_command_without_script_is_rejected(tmp_path: Path) -> None:
    module = _load_audit_module()
    skill_file = _workflow_skill(
        tmp_path,
        "ts-api-endpoints",
        "bun --cwd apps/ts run",
    )

    errors = module.validate_skill_file(skill_file, tmp_path)

    assert any("root-safe bun command" in error for error in errors)


def test_bun_help_verification_command_is_rejected(tmp_path: Path) -> None:
    module = _load_audit_module()
    skill_file = _workflow_skill(
        tmp_path,
        "ts-api-endpoints",
        "bun --help",
    )

    errors = module.validate_skill_file(skill_file, tmp_path)

    assert any("root-safe bun command" in error for error in errors)


def test_portable_bun_long_help_payload_is_rejected(tmp_path: Path) -> None:
    module = _load_audit_module()
    skill_file = _workflow_skill(
        tmp_path,
        "ts-api-endpoints",
        'bun --cwd="$PWD/apps/ts" run --help',
    )

    errors = module.validate_skill_file(skill_file, tmp_path)

    assert any("help/version-only" in error for error in errors)


def test_portable_bun_short_help_payload_is_rejected(tmp_path: Path) -> None:
    module = _load_audit_module()
    skill_file = _workflow_skill(
        tmp_path,
        "ts-api-endpoints",
        'bun --cwd="$PWD/apps/ts" run -h',
    )

    errors = module.validate_skill_file(skill_file, tmp_path)

    assert any("help/version-only" in error for error in errors)


def test_root_safe_verification_commands_pass(tmp_path: Path) -> None:
    module = _load_audit_module()
    (tmp_path / "apps/bt/tests/unit/server/routes").mkdir(parents=True)
    (tmp_path / "apps/ts").mkdir(parents=True)
    bt_skill = _workflow_skill(
        tmp_path,
        "bt-api-architecture",
        "uv run --directory apps/bt pytest tests/unit/server/routes",
    )
    ts_skill = _workflow_skill(
        tmp_path,
        "ts-api-endpoints",
        'bun --cwd="$PWD/apps/ts" run quality:typecheck',
    )

    assert module.validate_skill_file(bt_skill, tmp_path) == []
    assert module.validate_skill_file(ts_skill, tmp_path) == []


def test_api_endpoints_shorthand_passes_with_relative_canonical_reference(
    tmp_path: Path,
) -> None:
    module = _load_audit_module()
    _write(
        tmp_path / ".codex/skills/ts-api-endpoints/SKILL.md",
        "---\nname: ts-api-endpoints\ndescription: Use when FastAPI endpoints are integrated from TypeScript.\n---\n",
    )
    skill_file = _write(
        tmp_path / ".codex/skills/api-endpoints/SKILL.md",
        "\n".join(
            [
                "---",
                "name: api-endpoints",
                "description: Use when the supported shorthand for ts-api-endpoints is requested.",
                "---",
                "",
                "# api-endpoints",
                "",
                "## Canonical skill",
                "",
                "- Use `../ts-api-endpoints/SKILL.md` as the source of truth.",
                "",
                "## Usage",
                "",
                "- Keep only one shorthand.",
                "",
            ]
        ),
    )

    assert module.validate_skill_file(skill_file, tmp_path) == []


def test_removed_alias_skill_is_rejected(tmp_path: Path) -> None:
    module = _load_audit_module()
    skill_file = _write(
        tmp_path / ".codex/skills/optimization/SKILL.md",
        "\n".join(
            [
                "---",
                "name: optimization",
                "description: Old alias.",
                "---",
                "",
                "# optimization",
                "",
            ]
        ),
    )

    errors = module.validate_skill_file(skill_file, tmp_path)

    assert any("Unsupported skill name" in error for error in errors)


def test_deprecated_bt_server_path_is_rejected(tmp_path: Path) -> None:
    module = _load_audit_module()
    skill_file = _write(
        tmp_path / ".codex/skills/bt-financial-analysis/SKILL.md",
        "\n".join(
            [
                "---",
                "name: bt-financial-analysis",
                "description: Use when bt financial analysis routes are changed.",
                "---",
                "",
                "# bt-financial-analysis",
                "",
                "## When to use",
                "",
                "- backend analytics",
                "",
                "## Source of Truth",
                "",
                "- `apps/bt/src/server/app.py`",
                "",
                "## Workflow",
                "",
                "1. inspect route",
                "",
                "## Guardrails",
                "",
                "- keep SoT in bt",
                "",
                "## Verification",
                "",
                "- `uv run --directory apps/bt pytest tests/unit/server/services`",
                "",
            ]
        ),
    )

    errors = module.validate_skill_file(skill_file, tmp_path)

    assert any("apps/bt/src/server/" in error for error in errors)


@pytest.mark.parametrize(
    "deleted_path",
    (
        "apps/bt/src/entrypoints/http/schemas/analytics_margin.py",
        "apps/bt/src/entrypoints/http/schemas/analytics_roe.py",
        "apps/bt/src/entrypoints/http/schemas/chart.py",
        "apps/bt/src/entrypoints/http/schemas/dataset_data.py",
        "apps/bt/src/entrypoints/http/schemas/jquants.py",
        "apps/bt/src/entrypoints/http/schemas/market_data.py",
    ),
)
def test_deleted_task16_http_schema_path_is_rejected(
    tmp_path: Path,
    deleted_path: str,
) -> None:
    module = _load_audit_module()
    skill_file = _write(
        tmp_path / ".codex/skills/bt-jquants-proxy-optimization/SKILL.md",
        "\n".join(
            [
                "---",
                "name: bt-jquants-proxy-optimization",
                "description: Canonical skill.",
                "---",
                "",
                "# bt-jquants-proxy-optimization",
                "",
                "## When to use",
                "",
                "- J-Quants proxy work",
                "",
                "## Source of Truth",
                "",
                f"- `{deleted_path}`",
                "",
                "## Workflow",
                "",
                "1. inspect route and service",
                "",
                "## Guardrails",
                "",
                "- keep contracts application-owned",
                "",
                "## Verification",
                "",
                "- `uv run --directory apps/bt pytest tests/unit/server/routes/test_jquants_proxy.py`",
                "",
            ]
        ),
    )

    errors = module.validate_skill_file(skill_file, tmp_path)

    assert any(
        Path(deleted_path).stem in error and "Banned pattern" in error
        for error in errors
    )


def test_missing_referenced_path_is_rejected(tmp_path: Path) -> None:
    module = _load_audit_module()
    skill_file = _write(
        tmp_path / ".codex/skills/bt-api-architecture/SKILL.md",
        "\n".join(
            [
                "---",
                "name: bt-api-architecture",
                "description: Canonical skill.",
                "---",
                "",
                "# bt-api-architecture",
                "",
                "## When to use",
                "",
                "- backend api work",
                "",
                "## Source of Truth",
                "",
                "- `apps/bt/src/entrypoints/http/missing.py`",
                "",
                "## Workflow",
                "",
                "1. inspect route",
                "",
                "## Guardrails",
                "",
                "- keep FastAPI as backend",
                "",
                "## Verification",
                "",
                "- `python3 scripts/skills/refresh_skill_references.py --check`",
                "",
            ]
        ),
    )

    errors = module.validate_skill_file(skill_file, tmp_path)

    assert any("Referenced path not found" in error for error in errors)


def test_repo_root_scripts_path_is_allowed_from_skill(tmp_path: Path) -> None:
    module = _load_audit_module()
    _write(tmp_path / "scripts/check-research-guardrails.py", "# guardrail script\n")
    skill_file = _write(
        tmp_path / ".codex/skills/bt-research-workflow/SKILL.md",
        "\n".join(
            [
                "---",
                "name: bt-research-workflow",
                "description: Use when the research workflow is changed.",
                "---",
                "",
                "# bt-research-workflow",
                "",
                "## When to use",
                "",
                "- research workflow",
                "",
                "## Source of Truth",
                "",
                "- `apps/bt/src/domains`",
                "",
                "## Workflow",
                "",
                "1. run the guardrail.",
                "",
                "## Guardrails",
                "",
                "- `scripts/check-research-guardrails.py` prevents stale surfaces.",
                "",
                "## Verification",
                "",
                "```bash",
                "python3 scripts/check-research-guardrails.py",
                "```",
                "",
            ]
        ),
    )
    _write(tmp_path / "apps/bt/src/domains/.gitkeep", "")

    assert module.validate_skill_file(skill_file, tmp_path) == []


def test_legacy_claude_paths_are_rejected_by_strict_audit(tmp_path: Path) -> None:
    module = _load_audit_module()
    _write(tmp_path / "CLAUDE.md", "legacy")
    _write(tmp_path / "apps/ts/.claude/settings.json", "{}")

    assert module.find_legacy_paths(tmp_path) == ["CLAUDE.md", "apps/ts/.claude"]
    assert "CLAUDE.md" not in module.LOCAL_FILE_NAMES


def test_claude_references_are_rejected_in_skill_content(tmp_path: Path) -> None:
    module = _load_audit_module()
    skill_file = _write(
        tmp_path / ".codex/skills/bt-api-architecture/SKILL.md",
        "\n".join(
            [
                "---",
                "name: bt-api-architecture",
                "description: Canonical skill.",
                "---",
                "",
                "# bt-api-architecture",
                "",
                "## When to use",
                "",
                "- backend api work",
                "",
                "## Source of Truth",
                "",
                "- Read `CLAUDE.md` first.",
                "",
                "## Workflow",
                "",
                "1. inspect route",
                "",
                "## Guardrails",
                "",
                "- keep FastAPI as backend",
                "",
                "## Verification",
                "",
                "- `python3 scripts/skills/refresh_skill_references.py --check`",
                "",
            ]
        ),
    )

    errors = module.validate_skill_file(skill_file, tmp_path)

    assert any("CLAUDE" in error for error in errors)
