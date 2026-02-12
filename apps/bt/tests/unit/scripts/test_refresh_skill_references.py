"""Tests for scripts/skills/refresh_skill_references.py."""

from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path


def _load_refresh_module():
    repo_root = Path(__file__).resolve().parents[5]
    module_dir = repo_root / "scripts/skills"
    module_dir_str = str(module_dir)
    if module_dir_str not in sys.path:
        sys.path.insert(0, module_dir_str)

    module = importlib.import_module("refresh_skill_references")
    module = importlib.reload(module)
    return module, repo_root


def test_write_or_check_detects_stale_content_in_check_mode(tmp_path: Path) -> None:
    module, _ = _load_refresh_module()
    target = tmp_path / "ref.md"
    target.write_text("old")

    changed = module._write_or_check(target, "new", check=True)

    assert changed == [str(target)]
    assert target.read_text() == "old"


def test_write_or_check_writes_when_not_check_mode(tmp_path: Path) -> None:
    module, _ = _load_refresh_module()
    target = tmp_path / "refs" / "ref.md"

    changed = module._write_or_check(target, "content", check=False)

    assert changed == []
    assert target.read_text() == "content"


def test_render_openapi_reference_groups_paths(tmp_path: Path) -> None:
    module, _ = _load_refresh_module()
    openapi_path = tmp_path / "openapi.json"
    openapi_path.write_text(
        json.dumps(
            {
                "paths": {
                    "/api/db/stats": {"get": {}},
                    "/api/db/sync": {"post": {}},
                    "/api/chart/stocks/{symbol}": {"get": {}},
                }
            }
        )
    )

    rendered = module._render_openapi_reference(openapi_path)

    assert "Total paths: **3**" in rendered
    assert "## /api/db" in rendered
    assert "`/api/db/stats`" in rendered
    assert "`GET`" in rendered
    assert "`/api/db/sync`" in rendered
    assert "`POST`" in rendered


def test_render_router_reference_extracts_include_order(tmp_path: Path) -> None:
    module, _ = _load_refresh_module()
    app_path = tmp_path / "app.py"
    app_path.write_text(
        "\n".join(
            [
                "app.include_router(health_router)",
                "app.include_router(analytics_router, prefix='/api')",
            ]
        )
    )

    rendered = module._render_router_reference(app_path)

    assert "Total include_router calls: **2**" in rendered
    assert "| 1 | `health_router` | `1` |" in rendered
    assert "| 2 | `analytics_router, prefix='/api'` | `2` |" in rendered


def test_extract_commands_reads_typer_declarations(tmp_path: Path) -> None:
    module, _ = _load_refresh_module()
    cli_path = tmp_path / "cli.py"
    cli_path.write_text(
        "\n".join(
            [
                '@app.command(name="alpha")',
                "def alpha(): pass",
                '@app.command(name="beta")',
                "def beta(): pass",
            ]
        )
    )

    commands = module._extract_commands(cli_path, "app", "bt")

    assert commands == ["bt alpha", "bt beta"]


def test_render_cli_reference_excludes_portfolio_commands() -> None:
    module, repo_root = _load_refresh_module()

    rendered: str = module._render_cli_reference(repo_root)

    assert "portfolio " not in rendered
    assert "apps/bt/src/cli_portfolio/__init__.py" not in rendered
    assert "Total commands: **9**" in rendered
    assert "| `bt lab optimize` |" in rendered
    assert "| `bt server` |" in rendered


def test_main_check_mode_returns_success(monkeypatch) -> None:
    module, _ = _load_refresh_module()
    monkeypatch.setattr(sys, "argv", ["refresh_skill_references.py", "--check"])

    assert module.main() == 0
