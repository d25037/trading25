"""Guardrails for J-Quants usage boundaries.

Verification paths must consume local SoT data only. Direct J-Quants client imports
are reserved for proxy/sync/bootstrap modules.
"""

from __future__ import annotations

import ast
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_ROOT = PROJECT_ROOT / "src"

FORBIDDEN_MODULES = {
    "src.infrastructure.external_api.clients.jquants_client",
    "src.infrastructure.external_api.jquants_client",
}

FORBIDDEN_SYMBOLS = {
    "JQuantsAsyncClient",
    "JQuantsAPIClient",
}

ALLOWED_IMPORTERS = {
    "src.application.services.jquants_proxy_service",
    "src.entrypoints.http.routes.db",
    "src.entrypoints.http.app",
}

SCAN_ROOTS = (
    SRC_ROOT / "application" / "services",
    SRC_ROOT / "entrypoints" / "http" / "routes",
    SRC_ROOT / "entrypoints" / "http" / "app.py",
)


def _iter_python_files() -> list[Path]:
    files: list[Path] = []
    for root in SCAN_ROOTS:
        if root.is_file():
            files.append(root)
            continue
        files.extend(sorted(root.rglob("*.py")))
    return files


def _module_name(py_file: Path) -> str:
    relative = py_file.relative_to(SRC_ROOT).with_suffix("")
    return ".".join(("src", *relative.parts))


def _iter_imports(py_file: Path) -> list[tuple[str, str | None, int]]:
    tree = ast.parse(py_file.read_text(encoding="utf-8"))
    imports: list[tuple[str, str | None, int]] = []

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.append((alias.name, None, node.lineno))
        elif isinstance(node, ast.ImportFrom):
            if node.level != 0 or node.module is None:
                continue
            for alias in node.names:
                imports.append((node.module, alias.name, node.lineno))

    return imports


def test_only_proxy_sync_paths_import_jquants_clients() -> None:
    violations: list[str] = []

    for py_file in _iter_python_files():
        importer = _module_name(py_file)
        if importer in ALLOWED_IMPORTERS:
            continue

        for imported_module, imported_name, line_no in _iter_imports(py_file):
            if imported_name is None and imported_module in FORBIDDEN_MODULES:
                violations.append(
                    f"{py_file.relative_to(PROJECT_ROOT)}:{line_no} imports module {imported_module}"
                )
            elif (
                imported_module in FORBIDDEN_MODULES
                and imported_name in FORBIDDEN_SYMBOLS
            ):
                violations.append(
                    f"{py_file.relative_to(PROJECT_ROOT)}:{line_no} imports {imported_name} from {imported_module}"
                )

    assert not violations, (
        "Direct J-Quants client imports are only allowed in proxy/sync/bootstrap modules.\n"
        + "\n".join(violations)
    )
