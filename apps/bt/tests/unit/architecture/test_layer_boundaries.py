"""Architecture boundary checks for the 5-layer src layout."""

from __future__ import annotations

import ast
from pathlib import Path
import sys

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_ROOT = PROJECT_ROOT / "src"
APPLICATION_HTTP_SCHEMA_BASELINE = Path(__file__).with_name(
    "application_http_schema_imports.txt"
)
APPLICATION_HTTP_SCHEMA_PREFIX = "src.entrypoints.http.schemas"
FORBIDDEN_HTTP_JOB_CONTRACT_NAMES = {
    "JobStatus",
    "JobProgress",
    "JobEvent",
    "SSEJobEvent",
}

LAYER_NAMES = ("entrypoints", "application", "domains", "infrastructure", "shared")

# Core dependency rules between top-level layers.
ALLOWED_TARGET_LAYERS = {
    "entrypoints": {"entrypoints", "application", "domains", "infrastructure", "shared"},
    "application": {"application", "domains", "infrastructure", "shared"},
    "domains": {"domains", "infrastructure", "shared"},
    "infrastructure": {"infrastructure", "shared"},
    "shared": {"shared"},
}

# Transitional allowance: application services currently reuse API schema models.
ALLOWED_EXTRA_PREFIXES = {
    "application": ("src.entrypoints.http.schemas",),
    "shared": ("src.infrastructure.data_access.loaders",),
}

IGNORED_TOP_LEVEL_DIRS = {
    "__pycache__",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    "backtrading.egg-info",
}


def _iter_layer_python_files() -> list[Path]:
    files: list[Path] = []
    for py_file in SRC_ROOT.rglob("*.py"):
        rel = py_file.relative_to(SRC_ROOT)
        if rel.parts and rel.parts[0] in LAYER_NAMES:
            files.append(py_file)
    return files


def _resolve_import_from_module(py_file: Path, node: ast.ImportFrom) -> str | None:
    if node.module is None:
        return None
    if node.level == 0:
        return node.module

    relative = py_file.relative_to(PROJECT_ROOT).with_suffix("")
    package_parts = list(relative.parts[:-1])
    parent_count = node.level - 1
    if parent_count >= len(package_parts):
        return None

    return ".".join(
        [*package_parts[: len(package_parts) - parent_count], *node.module.split(".")]
    )


def _iter_src_imports(py_file: Path) -> list[tuple[str, int]]:
    tree = ast.parse(py_file.read_text(encoding="utf-8"))
    imports: list[tuple[str, int]] = []

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith("src."):
                    imports.append((alias.name, node.lineno))
        elif isinstance(node, ast.ImportFrom):
            module_name = _resolve_import_from_module(py_file, node)
            if module_name is not None and module_name.startswith("src."):
                imports.append((module_name, node.lineno))

    return imports


def _application_http_schema_imports() -> set[str]:
    imports: set[str] = set()
    application_root = SRC_ROOT / "application"
    for py_file in application_root.rglob("*.py"):
        for module_name, _line_no in _iter_src_imports(py_file):
            if module_name == APPLICATION_HTTP_SCHEMA_PREFIX or module_name.startswith(
                f"{APPLICATION_HTTP_SCHEMA_PREFIX}."
            ):
                relative = py_file.relative_to(SRC_ROOT).as_posix()
                imports.add(f"{relative}|{module_name}")
    return imports


def _is_http_schema_module(module_name: str) -> bool:
    return module_name == APPLICATION_HTTP_SCHEMA_PREFIX or module_name.startswith(
        f"{APPLICATION_HTTP_SCHEMA_PREFIX}."
    )


def _attribute_path(node: ast.expr) -> str | None:
    parts: list[str] = []
    current = node
    while isinstance(current, ast.Attribute):
        parts.append(current.attr)
        current = current.value
    if not isinstance(current, ast.Name):
        return None
    parts.append(current.id)
    return ".".join(reversed(parts))


def _literal_dynamic_schema_module(
    node: ast.expr,
    importlib_module_aliases: set[str],
    import_module_aliases: set[str],
) -> str | None:
    if not isinstance(node, ast.Call) or not node.args:
        return None

    is_loader = isinstance(node.func, ast.Name) and node.func.id in {
        "__import__",
        *import_module_aliases,
    }
    if (
        isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id in importlib_module_aliases
        and node.func.attr == "import_module"
    ):
        is_loader = True
    if not is_loader:
        return None

    module_arg = node.args[0]
    if (
        isinstance(module_arg, ast.Constant)
        and isinstance(module_arg.value, str)
        and _is_http_schema_module(module_arg.value)
    ):
        return module_arg.value
    return None


def _schema_module_reference(
    node: ast.expr,
    module_aliases: dict[str, str],
    importlib_module_aliases: set[str],
    import_module_aliases: set[str],
) -> str | None:
    if isinstance(node, ast.Name):
        return module_aliases.get(node.id)

    path = _attribute_path(node)
    if path is not None and _is_http_schema_module(path):
        return path

    return _literal_dynamic_schema_module(
        node,
        importlib_module_aliases,
        import_module_aliases,
    )


def _module_scope_nodes(tree: ast.Module) -> list[ast.stmt]:
    nodes: list[ast.stmt] = []

    def visit(statement: ast.stmt) -> None:
        nodes.append(statement)
        if isinstance(statement, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            return
        for child in ast.iter_child_nodes(statement):
            if isinstance(child, ast.stmt):
                visit(child)

    for statement in tree.body:
        visit(statement)
    return nodes


def _bound_names(target: ast.expr) -> set[str]:
    if isinstance(target, ast.Name):
        return {target.id}
    if isinstance(target, ast.Starred):
        return _bound_names(target.value)
    if isinstance(target, (ast.Tuple, ast.List)):
        return {
            name
            for element in target.elts
            for name in _bound_names(element)
        }
    return set()


def _http_schema_top_level_contracts(py_file: Path, tree: ast.Module) -> list[str]:
    violations: list[str] = []
    relative = py_file.relative_to(PROJECT_ROOT)

    for node in _module_scope_nodes(tree):
        forbidden_bindings: set[str] = set()
        if isinstance(node, ast.ClassDef):
            forbidden_bindings = {node.name} & FORBIDDEN_HTTP_JOB_CONTRACT_NAMES
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            forbidden_bindings = (
                set().union(*(_bound_names(target) for target in targets))
                & FORBIDDEN_HTTP_JOB_CONTRACT_NAMES
            )
        elif isinstance(node, ast.Import):
            bound = {alias.asname or alias.name.split(".")[0] for alias in node.names}
            forbidden_bindings = bound & FORBIDDEN_HTTP_JOB_CONTRACT_NAMES
        elif isinstance(node, ast.ImportFrom):
            imported = {alias.name for alias in node.names}
            bound = {alias.asname or alias.name for alias in node.names}
            forbidden_bindings = (imported | bound) & FORBIDDEN_HTTP_JOB_CONTRACT_NAMES

        if forbidden_bindings:
            violations.append(
                f"{relative}:{node.lineno} binds forbidden HTTP job contracts "
                f"{sorted(forbidden_bindings)}"
            )

        if not isinstance(node, (ast.Assign, ast.AnnAssign, ast.AugAssign)):
            continue
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        if "__all__" not in set().union(*(_bound_names(target) for target in targets)):
            continue
        value = node.value
        if value is None:
            continue
        exported = {
            child.value
            for child in ast.walk(value)
            if isinstance(child, ast.Constant) and isinstance(child.value, str)
        }
        forbidden_exports = exported & FORBIDDEN_HTTP_JOB_CONTRACT_NAMES
        if forbidden_exports:
            violations.append(
                f"{relative}:{node.lineno} exports forbidden HTTP job contracts "
                f"{sorted(forbidden_exports)} via __all__"
            )

    return violations


def _http_schema_contract_accesses(py_file: Path, tree: ast.Module) -> list[str]:
    violations: list[str] = []
    relative = py_file.relative_to(PROJECT_ROOT)
    module_aliases: dict[str, str] = {}
    importlib_module_aliases: set[str] = set()
    import_module_aliases: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "importlib":
                    importlib_module_aliases.add(alias.asname or alias.name)
                if _is_http_schema_module(alias.name) and alias.asname:
                    module_aliases[alias.asname] = alias.name
            continue
        if not isinstance(node, ast.ImportFrom):
            continue

        module_name = _resolve_import_from_module(py_file, node)
        if module_name == "importlib":
            for alias in node.names:
                if alias.name == "import_module":
                    import_module_aliases.add(alias.asname or alias.name)
        if module_name is None or not _is_http_schema_module(module_name):
            continue

        imported = {
            name
            for alias in node.names
            for name in (alias.name, alias.asname)
            if name in FORBIDDEN_HTTP_JOB_CONTRACT_NAMES
        }
        if imported:
            violations.append(
                f"{relative}:{node.lineno} imports forbidden HTTP job contracts "
                f"{sorted(imported)} from {module_name}"
            )
        for alias in node.names:
            if alias.name != "*":
                module_aliases[alias.asname or alias.name] = (
                    f"{module_name}.{alias.name}"
                )

    changed = True
    while changed:
        changed = False
        for node in ast.walk(tree):
            if not isinstance(node, (ast.Assign, ast.AnnAssign)):
                continue
            value = node.value
            if value is None:
                continue
            module_name = _schema_module_reference(
                value,
                module_aliases,
                importlib_module_aliases,
                import_module_aliases,
            )
            if module_name is None:
                continue
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                for name in _bound_names(target):
                    if module_aliases.get(name) != module_name:
                        module_aliases[name] = module_name
                        changed = True

    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute) and node.attr in FORBIDDEN_HTTP_JOB_CONTRACT_NAMES:
            module_name = _schema_module_reference(
                node.value,
                module_aliases,
                importlib_module_aliases,
                import_module_aliases,
            )
            if module_name is not None:
                violations.append(
                    f"{relative}:{node.lineno} accesses forbidden HTTP job contract "
                    f"{node.attr} from {module_name}"
                )
        elif (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "getattr"
            and len(node.args) >= 2
            and isinstance(node.args[1], ast.Constant)
            and node.args[1].value in FORBIDDEN_HTTP_JOB_CONTRACT_NAMES
        ):
            module_name = _schema_module_reference(
                node.args[0],
                module_aliases,
                importlib_module_aliases,
                import_module_aliases,
            )
            if module_name is not None:
                violations.append(
                    f"{relative}:{node.lineno} dynamically accesses forbidden HTTP job "
                    f"contract {node.args[1].value} from {module_name}"
                )

    return violations


def _forbidden_http_job_contract_references(*roots: Path) -> list[str]:
    violations: list[str] = []
    schema_root = PROJECT_ROOT / "src" / "entrypoints" / "http" / "schemas"
    for root in roots:
        for py_file in root.rglob("*.py"):
            tree = ast.parse(py_file.read_text(encoding="utf-8"))
            violations.extend(_http_schema_contract_accesses(py_file, tree))
            if py_file.is_relative_to(schema_root):
                violations.extend(_http_schema_top_level_contracts(py_file, tree))
    return sorted(violations)


def _synthetic_legacy_job_schema_imports(
    tmp_path: Path,
    monkeypatch,
    source: str,
) -> list[str]:
    source_root = tmp_path / "src"
    source_root.mkdir()
    (source_root / "consumer.py").write_text(source, encoding="utf-8")
    monkeypatch.setattr(sys.modules[__name__], "PROJECT_ROOT", tmp_path)
    return _forbidden_http_job_contract_references(source_root)


def _synthetic_http_schema_contract_violations(
    tmp_path: Path,
    monkeypatch,
    source: str,
) -> list[str]:
    schemas_root = tmp_path / "src" / "entrypoints" / "http" / "schemas"
    schemas_root.mkdir(parents=True)
    (schemas_root / "synthetic.py").write_text(source, encoding="utf-8")
    monkeypatch.setattr(sys.modules[__name__], "PROJECT_ROOT", tmp_path)
    return _forbidden_http_job_contract_references(schemas_root)


def _application_http_schema_baseline() -> set[str]:
    return {
        line.strip()
        for line in APPLICATION_HTTP_SCHEMA_BASELINE.read_text().splitlines()
        if line.strip() and not line.startswith("#")
    }


def _is_allowed_import(source_layer: str, module_name: str) -> bool:
    parts = module_name.split(".")
    if len(parts) < 2 or parts[0] != "src":
        return True

    target_layer = parts[1]
    if target_layer not in LAYER_NAMES:
        return True

    if target_layer in ALLOWED_TARGET_LAYERS[source_layer]:
        return True

    for allowed_prefix in ALLOWED_EXTRA_PREFIXES.get(source_layer, ()):
        if module_name == allowed_prefix or module_name.startswith(f"{allowed_prefix}."):
            return True

    return False


def test_relative_imports_are_resolved_for_boundary_checks() -> None:
    node = ast.parse(
        "from ...entrypoints.http.schemas.ranking import RankingItem"
    ).body[0]
    assert isinstance(node, ast.ImportFrom)

    py_file = SRC_ROOT / "application" / "services" / "example.py"
    assert _resolve_import_from_module(py_file, node) == (
        "src.entrypoints.http.schemas.ranking"
    )


def test_src_top_level_layout_is_layered() -> None:
    actual = {
        path.name
        for path in SRC_ROOT.iterdir()
        if path.is_dir() and path.name not in IGNORED_TOP_LEVEL_DIRS
    }
    assert actual == set(LAYER_NAMES), (
        "src top-level directories must be the fixed 5-layer set.\n"
        f"expected={sorted(LAYER_NAMES)} actual={sorted(actual)}"
    )


def test_layer_dependency_boundaries() -> None:
    violations: list[str] = []

    for py_file in _iter_layer_python_files():
        source_layer = py_file.relative_to(SRC_ROOT).parts[0]
        for module_name, line_no in _iter_src_imports(py_file):
            if not _is_allowed_import(source_layer, module_name):
                relative = py_file.relative_to(PROJECT_ROOT)
                violations.append(
                    f"{relative}:{line_no} ({source_layer} -> {module_name})"
                )

    assert not violations, "Layer boundary violations found:\n" + "\n".join(sorted(violations))


def test_application_http_schema_dependency_baseline_is_exact() -> None:
    actual = _application_http_schema_imports()
    expected = _application_http_schema_baseline()

    added = sorted(actual - expected)
    stale = sorted(expected - actual)
    assert not added and not stale, (
        "Application HTTP schema dependency baseline changed.\n"
        f"added={added}\n"
        f"stale={stale}\n"
        "New entries are forbidden; remove stale entries in the same DTO migration."
    )


def test_application_job_contracts_do_not_import_http_schemas() -> None:
    violations = _forbidden_http_job_contract_references(SRC_ROOT / "application")
    assert not violations, (
        "Application job contracts must be application-owned:\n" + "\n".join(violations)
    )


def test_http_schemas_do_not_export_legacy_job_contracts() -> None:
    violations = _forbidden_http_job_contract_references(
        SRC_ROOT / "entrypoints" / "http" / "schemas"
    )
    assert not violations, "HTTP schemas own forbidden job contracts:\n" + "\n".join(
        violations
    )


def test_legacy_job_schema_scanner_allows_star_import_from_safe_schema(
    tmp_path: Path,
    monkeypatch,
) -> None:
    violations = _synthetic_legacy_job_schema_imports(
        tmp_path,
        monkeypatch,
        "from src.entrypoints.http.schemas.common import *\n",
    )

    assert not violations


def test_legacy_job_schema_scanner_rejects_explicit_aliases_from_any_schema_module(
    tmp_path: Path,
    monkeypatch,
) -> None:
    violations = _synthetic_legacy_job_schema_imports(
        tmp_path,
        monkeypatch,
        "\n".join(
            (
                "from src.entrypoints.http.schemas.synthetic_one import JobStatus as Status",
                "from src.entrypoints.http.schemas.synthetic_two import JobProgress as Progress",
                "from src.entrypoints.http.schemas.synthetic_three import JobEvent as Event",
                "from src.entrypoints.http.schemas.synthetic_four import SSEJobEvent as SSEEvent",
            )
        ),
    )

    assert len(violations) == 4
    joined = "\n".join(violations)
    assert all(
        name in joined
        for name in ("JobStatus", "JobProgress", "JobEvent", "SSEJobEvent")
    )


def test_legacy_job_schema_scanner_rejects_module_alias_attributes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    violations = _synthetic_legacy_job_schema_imports(
        tmp_path,
        monkeypatch,
        "import src.entrypoints.http.schemas.common as common\n"
        "Legacy = common.JobStatus\n",
    )

    assert len(violations) == 1
    assert "JobStatus" in violations[0]


def test_legacy_job_schema_scanner_rejects_imported_submodule_alias_attributes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    violations = _synthetic_legacy_job_schema_imports(
        tmp_path,
        monkeypatch,
        "from src.entrypoints.http.schemas import job as job_schema\n"
        "Legacy = job_schema.JobProgress\n",
    )

    assert len(violations) == 1
    assert "JobProgress" in violations[0]


def test_legacy_job_schema_scanner_rejects_any_submodule_alias_attributes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    violations = _synthetic_legacy_job_schema_imports(
        tmp_path,
        monkeypatch,
        "import src.entrypoints.http.schemas.synthetic_one as schema_one\n"
        "from src.entrypoints.http.schemas import synthetic_two as schema_two\n"
        "First = schema_one.JobEvent\n"
        "Second = schema_two.SSEJobEvent\n",
    )

    assert len(violations) == 2


def test_legacy_job_schema_scanner_rejects_unaliased_fully_qualified_access(
    tmp_path: Path,
    monkeypatch,
) -> None:
    violations = _synthetic_legacy_job_schema_imports(
        tmp_path,
        monkeypatch,
        "import src.entrypoints.http.schemas.synthetic\n"
        "Legacy = src.entrypoints.http.schemas.synthetic.JobStatus\n",
    )

    assert len(violations) == 1
    assert "JobStatus" in violations[0]


@pytest.mark.parametrize(
    "source",
    (
        "import importlib\n"
        "Legacy = importlib.import_module(\"src.entrypoints.http.schemas.synthetic\").JobStatus\n",
        "import importlib as loader\n"
        "Legacy = loader.import_module(\"src.entrypoints.http.schemas.synthetic\").JobProgress\n",
        "from importlib import import_module as load\n"
        "schema = load(\"src.entrypoints.http.schemas.synthetic\")\n"
        "Legacy = schema.JobEvent\n",
        "Legacy = __import__(\"src.entrypoints.http.schemas.synthetic\", fromlist=[\"SSEJobEvent\"]).SSEJobEvent\n",
        "import importlib\n"
        "Legacy = getattr(importlib.import_module(\"src.entrypoints.http.schemas.synthetic\"), \"JobEvent\")\n",
    ),
)
def test_legacy_job_schema_scanner_rejects_literal_dynamic_import_access(
    tmp_path: Path,
    monkeypatch,
    source: str,
) -> None:
    violations = _synthetic_legacy_job_schema_imports(tmp_path, monkeypatch, source)

    assert len(violations) == 1


@pytest.mark.parametrize(
    ("source", "forbidden_name"),
    (
        ("class JobStatus:\n    pass\n", "JobStatus"),
        ("JobProgress = object()\n", "JobProgress"),
        ("JobEvent: object\n", "JobEvent"),
        (
            "from src.application.contracts.jobs import SSEJobEvent as LegacyEvent\n",
            "SSEJobEvent",
        ),
        ("from somewhere import Value as JobStatus\n", "JobStatus"),
        ("import somewhere as JobProgress\n", "JobProgress"),
    ),
)
def test_http_schema_scanner_rejects_forbidden_top_level_bindings(
    tmp_path: Path,
    monkeypatch,
    source: str,
    forbidden_name: str,
) -> None:
    violations = _synthetic_http_schema_contract_violations(
        tmp_path,
        monkeypatch,
        source,
    )

    assert len(violations) == 1
    assert forbidden_name in violations[0]


def test_http_schema_scanner_rejects_forbidden_all_exports(
    tmp_path: Path,
    monkeypatch,
) -> None:
    violations = _synthetic_http_schema_contract_violations(
        tmp_path,
        monkeypatch,
        '__all__ = ["SafeResponse", "JobEvent"]\n',
    )

    assert len(violations) == 1
    assert "JobEvent" in violations[0]


def test_http_schema_scanner_allows_canonical_module_annotation(
    tmp_path: Path,
    monkeypatch,
) -> None:
    violations = _synthetic_http_schema_contract_violations(
        tmp_path,
        monkeypatch,
        "from src.application.contracts import jobs as job_contracts\n"
        "status: job_contracts.JobStatus\n",
    )

    assert not violations


def test_legacy_job_schema_scanner_allows_unrelated_dtos(
    tmp_path: Path,
    monkeypatch,
) -> None:
    violations = _synthetic_legacy_job_schema_imports(
        tmp_path,
        monkeypatch,
        "from src.entrypoints.http.schemas.backtest import BacktestRequest\n"
        "import src.entrypoints.http.schemas.common as common\n"
        "import importlib\n"
        "Response = common.BaseJobResponse\n"
        "DynamicResponse = importlib.import_module(\"src.entrypoints.http.schemas.synthetic\").OtherDto\n",
    )

    assert not violations


def test_repository_does_not_import_legacy_job_contract_paths() -> None:
    violations = _forbidden_http_job_contract_references(
        SRC_ROOT, PROJECT_ROOT / "tests"
    )
    assert not violations, "Legacy job contract imports found:\n" + "\n".join(
        violations
    )
