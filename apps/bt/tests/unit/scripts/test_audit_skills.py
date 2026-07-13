"""Tests for scripts/skills/audit_skills.py."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path


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
                "description: Canonical skill.",
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
                f"- `{verification_command}`",
                "",
            ]
        ),
    )


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


def test_root_safe_verification_commands_pass(tmp_path: Path) -> None:
    module = _load_audit_module()
    bt_skill = _workflow_skill(
        tmp_path,
        "bt-api-architecture",
        "uv run --directory apps/bt pytest tests/unit/server/routes",
    )
    ts_skill = _workflow_skill(
        tmp_path,
        "ts-api-endpoints",
        "bun --cwd apps/ts run quality:typecheck",
    )

    assert module.validate_skill_file(bt_skill, tmp_path) == []
    assert module.validate_skill_file(ts_skill, tmp_path) == []


def test_api_endpoints_shorthand_passes_with_relative_canonical_reference(
    tmp_path: Path,
) -> None:
    module = _load_audit_module()
    _write(
        tmp_path / ".codex/skills/ts-api-endpoints/SKILL.md",
        "---\nname: ts-api-endpoints\ndescription: Canonical skill.\n---\n",
    )
    skill_file = _write(
        tmp_path / ".codex/skills/api-endpoints/SKILL.md",
        "\n".join(
            [
                "---",
                "name: api-endpoints",
                "description: Supported shorthand for ts-api-endpoints.",
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
                "description: Canonical skill.",
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
                "- `uv run --project apps/bt pytest tests/unit/server/services`",
                "",
            ]
        ),
    )

    errors = module.validate_skill_file(skill_file, tmp_path)

    assert any("apps/bt/src/server/" in error for error in errors)


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
                "description: Canonical skill.",
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
                "- `python3 scripts/check-research-guardrails.py`",
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
