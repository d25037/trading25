"""Architecture guards for the focused Market v4 cutover package."""

from __future__ import annotations

import ast
import importlib
from pathlib import Path


BT_ROOT = Path(__file__).resolve().parents[4]
SRC_ROOT = BT_ROOT / "src"
CUTOVER_FILE = SRC_ROOT / "application/services/market_v4_cutover.py"
CUTOVER_PACKAGE = SRC_ROOT / "application/services/market_v4_cutover"
TEST_ROOT = Path(__file__).parent

EXPECTED_RESPONSIBILITIES = {
    "backup.py",
    "contracts.py",
    "duckdb_identity.py",
    "errors.py",
    "filesystem.py",
    "journal.py",
    "leases.py",
    "promotion.py",
    "rebuild.py",
    "reports.py",
    "runtime.py",
    "service.py",
    "smoke.py",
}


def _python_sources(root: Path) -> list[Path]:
    roots = (root / "src", root / "tests") if root == BT_ROOT else (root,)
    return sorted(
        path
        for source_root in roots
        for path in source_root.rglob("*.py")
        if "__pycache__" not in path.parts
    )


def _module_name(path: Path) -> str:
    return "src." + ".".join(path.relative_to(SRC_ROOT).with_suffix("").parts)


def _cutover_imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(
                alias.name
                for alias in node.names
                if alias.name.startswith("src.application.services.market_v4_cutover.")
            )
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if module.startswith("src.application.services.market_v4_cutover."):
                imports.add(module)
    return imports


def _assert_acyclic(graph: dict[str, set[str]]) -> None:
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(node: str) -> None:
        if node in visiting:
            raise AssertionError(f"cutover import cycle detected at {node}")
        if node in visited:
            return
        visiting.add(node)
        for dependency in graph.get(node, set()):
            visit(dependency)
        visiting.remove(node)
        visited.add(node)

    for node in graph:
        visit(node)


def test_cutover_is_a_focused_package_without_compatibility_module() -> None:
    assert not CUTOVER_FILE.exists()
    assert CUTOVER_PACKAGE.is_dir()
    assert EXPECTED_RESPONSIBILITIES <= {
        path.name for path in CUTOVER_PACKAGE.glob("*.py")
    }
    init_source = (CUTOVER_PACKAGE / "__init__.py").read_text(encoding="utf-8")
    init_tree = ast.parse(init_source)
    assert not any(
        isinstance(node, (ast.Import, ast.ImportFrom, ast.Assign, ast.AnnAssign))
        for node in init_tree.body
    )


def test_no_python_caller_imports_cutover_package_root() -> None:
    forbidden = "src.application.services.market_v4_cutover"
    offenders: list[str] = []
    for path in _python_sources(BT_ROOT):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module == forbidden:
                offenders.append(str(path.relative_to(BT_ROOT)))
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == forbidden:
                        offenders.append(str(path.relative_to(BT_ROOT)))
    assert offenders == []


def test_cutover_modules_are_acyclic_and_bounded() -> None:
    modules = _python_sources(CUTOVER_PACKAGE)
    graph = {_module_name(path): _cutover_imports(path) for path in modules}
    _assert_acyclic(graph)

    oversized = {
        path.name: len(path.read_text(encoding="utf-8").splitlines())
        for path in modules
        if len(path.read_text(encoding="utf-8").splitlines())
        > (600 if path.name == "service.py" else 700)
    }
    assert oversized == {}

    long_methods: dict[str, int] = {}
    for path in modules:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                length = (node.end_lineno or node.lineno) - node.lineno + 1
                if length > 180:
                    long_methods[f"{path.name}:{node.name}"] = length
    assert long_methods == {}


def test_split_cutover_test_modules_are_bounded() -> None:
    oversized = {
        path.name: len(path.read_text(encoding="utf-8").splitlines())
        for path in TEST_ROOT.glob("test_market_v4_cutover*.py")
        if len(path.read_text(encoding="utf-8").splitlines()) > 1000
    }
    assert oversized == {}


def test_project_root_resolution_is_independent_of_module_depth() -> None:
    paths = importlib.import_module(
        "src.application.services.market_v4_cutover.project_paths"
    )
    assert paths.bt_project_root() == BT_ROOT
    assert paths.repository_default_config_path() == BT_ROOT / "config/default.yaml"
