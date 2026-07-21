"""Architecture guards for the focused Market v5 cutover package."""

from __future__ import annotations

import ast
import importlib
from pathlib import Path
import textwrap


BT_ROOT = Path(__file__).resolve().parents[4]
SRC_ROOT = BT_ROOT / "src"
CUTOVER_FILE = SRC_ROOT / "application/services/market_v4_cutover.py"
CUTOVER_PACKAGE = SRC_ROOT / "application/services/market_v4_cutover"
TEST_ROOT = Path(__file__).parent

EXPECTED_RESPONSIBILITIES = {
    "activation_journal.py",
    "activation_contract.py",
    "backup.py",
    "contracts.py",
    "duckdb_identity.py",
    "errors.py",
    "evidence.py",
    "filesystem.py",
    "full_rehearsal.py",
    "reports.py",
    "runtime.py",
    "service.py",
    "smoke.py",
    "workspace.py",
}

FORBIDDEN_RETAINED_PROMOTION_JOURNALS = {
    "journal.py",
    "journal_directories.py",
    "journal_storage.py",
    "journal_validation.py",
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


def _resolve_cutover_imports(
    path: Path,
    *,
    source_root: Path = SRC_ROOT,
) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    current = _module_name(path) if source_root == SRC_ROOT else ".".join(
        path.relative_to(source_root).with_suffix("").parts
    )
    package = current.rpartition(".")[0]
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                anchor = package.split(".")
                base = anchor[: len(anchor) - node.level + 1]
                module = ".".join((*base, *(node.module or "").split("."))).rstrip(".")
            else:
                module = node.module or ""
            if module:
                imports.add(module)
            for alias in node.names:
                if alias.name != "*":
                    imports.add(f"{module}.{alias.name}" if module else alias.name)
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
    assert FORBIDDEN_RETAINED_PROMOTION_JOURNALS.isdisjoint(
        path.name for path in CUTOVER_PACKAGE.glob("*.py")
    )
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
    names = {_module_name(path) for path in modules}
    graph = {
        _module_name(path): _resolve_cutover_imports(path) & names for path in modules
    }
    _assert_acyclic(graph)

    oversized = {
        path.name: len(path.read_text(encoding="utf-8").splitlines())
        for path in modules
        if len(path.read_text(encoding="utf-8").splitlines())
        > (
            725
            if path.name == "activation.py"
            else 600
            if path.name == "service.py"
            else 700
        )
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


def test_cycle_guard_resolves_relative_and_absolute_package_imports(
    tmp_path: Path,
) -> None:
    package = tmp_path / "pkg"
    package.mkdir()
    sources = {
        "__init__.py": "",
        "a.py": "from . import b\n",
        "b.py": "from .c import value\n",
        "c.py": "from pkg.a import value\n",
    }
    for name, source in sources.items():
        (package / name).write_text(textwrap.dedent(source), encoding="utf-8")
    modules = sorted(package.glob("*.py"))
    names = {".".join(path.relative_to(tmp_path).with_suffix("").parts) for path in modules}
    graph = {
        ".".join(path.relative_to(tmp_path).with_suffix("").parts):
        _resolve_cutover_imports(path, source_root=tmp_path) & names
        for path in modules
    }

    try:
        _assert_acyclic(graph)
    except AssertionError as error:
        assert "cycle" in str(error)
    else:
        raise AssertionError("relative three-node cycle was not detected")

    (package / "c.py").write_text("VALUE = 1\n", encoding="utf-8")
    graph = {
        ".".join(path.relative_to(tmp_path).with_suffix("").parts):
        _resolve_cutover_imports(path, source_root=tmp_path) & names
        for path in modules
    }
    _assert_acyclic(graph)


def test_cutover_service_is_an_explicit_facade_without_mixins() -> None:
    classes: list[tuple[Path, ast.ClassDef]] = []
    for path in _python_sources(CUTOVER_PACKAGE):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        classes.extend(
            (path, node) for node in tree.body if isinstance(node, ast.ClassDef)
        )

    mixin_classes = [
        f"{path.name}:{node.name}"
        for path, node in classes
        if node.name.endswith("Mixin")
    ]
    mixin_bases = [
        f"{path.name}:{node.name}"
        for path, node in classes
        if any(
            (
                isinstance(base, ast.Name)
                and base.id.endswith("Mixin")
                or isinstance(base, ast.Attribute)
                and base.attr.endswith("Mixin")
            )
            for base in node.bases
        )
    ]
    service = next(
        node
        for path, node in classes
        if path.name == "service.py" and node.name == "MarketV4CutoverService"
    )
    constructor = next(
        node
        for node in service.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == "__init__"
    )
    collaborator_fields = {
        target.attr
        for node in ast.walk(constructor)
        if isinstance(node, ast.Assign)
        for target in node.targets
        if (
            isinstance(target, ast.Attribute)
            and isinstance(target.value, ast.Name)
            and target.value.id == "self"
        )
    }

    assert service.bases == []
    assert mixin_classes == []
    assert mixin_bases == []
    assert 6 <= len(collaborator_fields) <= 10
    assert not any(
        isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == "__getattr__"
        for node in service.body
    )


def test_cutover_collaborators_do_not_graft_methods_from_other_classes() -> None:
    violations: list[str] = []
    for path in sorted(CUTOVER_PACKAGE.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for class_node in (
            node for node in tree.body if isinstance(node, ast.ClassDef)
        ):
            for node in ast.walk(class_node):
                if isinstance(node, ast.Attribute) and node.attr == "__dict__":
                    violations.append(
                        f"{path.name}:{class_node.name}:class __dict__ access"
                    )
                if (
                    isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Name)
                    and node.func.id == "setattr"
                ):
                    violations.append(
                        f"{path.name}:{class_node.name}:dynamic setattr"
                    )
    assert violations == []


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
