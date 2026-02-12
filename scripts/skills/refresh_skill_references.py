#!/usr/bin/env python3
"""Generate skill reference markdown from source-of-truth artifacts."""

from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from pathlib import Path

HTTP_METHODS = ("get", "post", "put", "patch", "delete", "options", "head")


def _write_or_check(path: Path, content: str, check: bool) -> list[str]:
    changes: list[str] = []
    current = path.read_text() if path.exists() else None
    if current == content:
        return changes

    if check:
        changes.append(str(path))
        return changes

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    return changes


def _render_openapi_reference(openapi_path: Path) -> str:
    schema = json.loads(openapi_path.read_text())
    paths = schema.get("paths", {})

    grouped: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for endpoint in sorted(paths):
        item = paths[endpoint]
        methods = [m.upper() for m in HTTP_METHODS if m in item]
        if not methods:
            continue
        parts = endpoint.strip("/").split("/")
        if len(parts) >= 2:
            group = "/" + "/".join(parts[:2])
        elif parts:
            group = "/" + parts[0]
        else:
            group = "/"
        grouped[group].append((endpoint, ", ".join(methods)))

    lines = [
        "# OpenAPI Paths",
        "",
        "Generated from `apps/ts/packages/shared/openapi/bt-openapi.json`. Do not edit manually.",
        "",
        f"Total paths: **{sum(len(v) for v in grouped.values())}**",
        "",
    ]

    for group in sorted(grouped):
        lines.extend([f"## {group}", "", "| Path | Methods |", "|---|---|"])
        for endpoint, methods in grouped[group]:
            lines.append(f"| `{endpoint}` | `{methods}` |")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def _render_router_reference(app_path: Path) -> str:
    text = app_path.read_text()
    includes: list[tuple[str, int]] = []
    for line_no, line in enumerate(text.splitlines(), 1):
        match = re.search(r"app\.include_router\(([^)]+)\)", line)
        if match:
            includes.append((match.group(1).strip(), line_no))

    lines = [
        "# FastAPI Router Wiring",
        "",
        "Generated from `apps/bt/src/server/app.py`. Do not edit manually.",
        "",
        f"Total include_router calls: **{len(includes)}**",
        "",
        "| Order | Router | Line |",
        "|---|---|---|",
    ]

    for idx, (router, line_no) in enumerate(includes, 1):
        lines.append(f"| {idx} | `{router}` | `{line_no}` |")

    return "\n".join(lines).rstrip() + "\n"


def _extract_commands(path: Path, decorator: str, prefix: str) -> list[str]:
    text = path.read_text()
    pattern = re.compile(rf'@{re.escape(decorator)}\.command\(name="([^"]+)"')
    commands = sorted(set(pattern.findall(text)))
    return [f"{prefix} {cmd}" for cmd in commands]


def _render_cli_reference(repo_root: Path) -> str:
    bt_main = repo_root / "apps/bt/src/cli_bt/__init__.py"
    bt_lab = repo_root / "apps/bt/src/cli_bt/lab.py"

    commands = []
    commands.extend(_extract_commands(bt_main, "app", "bt"))
    commands.extend(_extract_commands(bt_lab, "lab_app", "bt lab"))
    commands = sorted(set(commands))

    lines = [
        "# bt CLI Commands",
        "",
        "Generated from Typer command declarations. Do not edit manually.",
        "",
        f"Total commands: **{len(commands)}**",
        "",
        "| Command |",
        "|---|",
    ]
    lines.extend([f"| `{cmd}` |" for cmd in commands])
    lines.append("")
    lines.extend(
        [
            "## Source Files",
            "",
            "- `apps/bt/src/cli_bt/__init__.py`",
            "- `apps/bt/src/cli_bt/lab.py`",
            "",
        ]
    )

    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="Fail if generated files are out of date")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[2]

    openapi_json = repo_root / "apps/ts/packages/shared/openapi/bt-openapi.json"
    fastapi_app = repo_root / "apps/bt/src/server/app.py"

    targets = {
        repo_root / ".codex/skills/ts-api-endpoints/references/openapi-paths.md": _render_openapi_reference(openapi_json),
        repo_root / ".codex/skills/bt-api-architecture/references/fastapi-routers.md": _render_router_reference(fastapi_app),
        repo_root / ".codex/skills/bt-cli-commands/references/bt-cli-commands.md": _render_cli_reference(repo_root),
    }

    changed: list[str] = []
    for path, content in targets.items():
        changed.extend(_write_or_check(path, content, args.check))

    if args.check and changed:
        print("Generated skill references are stale:")
        for path in changed:
            print(f"- {path}")
        return 1

    if not args.check:
        print("Skill references refreshed:")
        for path in targets:
            print(f"- {path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
