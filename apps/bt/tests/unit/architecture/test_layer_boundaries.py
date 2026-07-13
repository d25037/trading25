"""Architecture boundary checks for the 5-layer src layout."""

from __future__ import annotations

import ast
from pathlib import Path
import sys

import pytest

from tests.unit.architecture.application_contract_boundary_guard import (
    APPLICATION_HTTP_SCHEMA_PREFIX,
    forbidden_http_application_contract_references,
)


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_ROOT = PROJECT_ROOT / "src"
APPLICATION_HTTP_SCHEMA_BASELINE = Path(__file__).with_name(
    "application_http_schema_imports.txt"
)
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


def _forbidden_http_application_contract_references(*roots: Path) -> list[str]:
    return forbidden_http_application_contract_references(
        *roots,
        project_root=PROJECT_ROOT,
        resolve_import_from_module=_resolve_import_from_module,
    )


def _synthetic_legacy_application_schema_imports(
    tmp_path: Path,
    monkeypatch,
    source: str,
) -> list[str]:
    source_root = tmp_path / "src"
    source_root.mkdir()
    (source_root / "consumer.py").write_text(source, encoding="utf-8")
    monkeypatch.setattr(sys.modules[__name__], "PROJECT_ROOT", tmp_path)
    return _forbidden_http_application_contract_references(source_root)


def _synthetic_http_schema_contract_violations(
    tmp_path: Path,
    monkeypatch,
    source: str,
) -> list[str]:
    schemas_root = tmp_path / "src" / "entrypoints" / "http" / "schemas"
    schemas_root.mkdir(parents=True)
    (schemas_root / "synthetic.py").write_text(source, encoding="utf-8")
    monkeypatch.setattr(sys.modules[__name__], "PROJECT_ROOT", tmp_path)
    return _forbidden_http_application_contract_references(schemas_root)


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


def test_application_contracts_do_not_import_http_schemas() -> None:
    violations = _forbidden_http_application_contract_references(
        SRC_ROOT / "application"
    )
    assert not violations, (
        "Application contracts must be application-owned:\n" + "\n".join(violations)
    )


def test_http_schemas_do_not_export_legacy_application_contracts() -> None:
    violations = _forbidden_http_application_contract_references(
        SRC_ROOT / "entrypoints" / "http" / "schemas"
    )
    assert not violations, (
        "HTTP schemas own forbidden application contracts:\n" + "\n".join(violations)
    )


@pytest.mark.parametrize(
    ("source", "forbidden_name"),
    (
        (
            "from src.entrypoints.http.schemas import JobStatus\n",
            "JobStatus",
        ),
        (
            "from src.entrypoints.http.schemas.synthetic import JobProgress as Progress\n",
            "JobProgress",
        ),
        (
            "from .entrypoints.http.schemas.arbitrary import JobEvent\n",
            "JobEvent",
        ),
        (
            "def load():\n"
            "    from src.entrypoints.http.schemas.nested import SSEJobEvent as Event\n",
            "SSEJobEvent",
        ),
    ),
)
def test_application_contract_guard_rejects_direct_schema_imports(
    tmp_path: Path,
    monkeypatch,
    source: str,
    forbidden_name: str,
) -> None:
    violations = _synthetic_legacy_application_schema_imports(
        tmp_path, monkeypatch, source
    )

    assert len(violations) == 1
    assert forbidden_name in violations[0]


@pytest.mark.parametrize(
    "source",
    (
        "from src.entrypoints.http.schemas.common import *\n",
        "from src.entrypoints.http.schemas.backtest import BacktestRequest\n",
        "from src.application.contracts import jobs as job_contracts\n"
        "status: job_contracts.JobStatus\n",
    ),
)
def test_application_contract_guard_allows_non_direct_import_patterns(
    tmp_path: Path,
    monkeypatch,
    source: str,
) -> None:
    violations = _synthetic_legacy_application_schema_imports(
        tmp_path, monkeypatch, source
    )

    assert not violations


@pytest.mark.parametrize(
    ("source", "forbidden_name"),
    (
        ("class JobStatus:\n    pass\n", "JobStatus"),
        ("def JobStatus():\n    pass\n", "JobStatus"),
        ("JobProgress = object()\n", "JobProgress"),
        ("JobEvent: object\n", "JobEvent"),
        ("SSEJobEvent += 1\n", "SSEJobEvent"),
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


def test_http_schema_guard_rejects_forbidden_type_alias(
    tmp_path: Path,
    monkeypatch,
) -> None:
    violations = _synthetic_http_schema_contract_violations(
        tmp_path,
        monkeypatch,
        "type JobStatus = str\n",
    )

    assert len(violations) == 1
    assert "JobStatus" in violations[0]


@pytest.mark.parametrize(
    ("source", "forbidden_name"),
    (
        ("if enabled:\n    JobStatus = object()\n", "JobStatus"),
        (
            "try:\n    from somewhere import Value as JobProgress\n"
            "except ImportError:\n    pass\n",
            "JobProgress",
        ),
        ("for JobEvent in events:\n    pass\n", "JobEvent"),
        ("with resource() as SSEJobEvent:\n    pass\n", "SSEJobEvent"),
        (
            "match value:\n    case JobStatus:\n        pass\n",
            "JobStatus",
        ),
    ),
)
def test_http_schema_guard_rejects_bindings_in_module_control_flow(
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


@pytest.mark.parametrize(
    ("source", "forbidden_name"),
    (
        (
            "def build(value=(JobStatus := object())):\n    pass\n",
            "JobStatus",
        ),
        (
            "@(JobProgress := decorate)\ndef build():\n    pass\n",
            "JobProgress",
        ),
        (
            "class Container((JobEvent := Base)):\n    pass\n",
            "JobEvent",
        ),
        (
            "class Container(metaclass=(SSEJobEvent := type)):\n    pass\n",
            "SSEJobEvent",
        ),
        (
            "factory = lambda value=(JobProgress := object()): value\n",
            "JobProgress",
        ),
    ),
)
def test_http_schema_guard_inspects_definition_time_expressions(
    tmp_path: Path,
    monkeypatch,
    source: str,
    forbidden_name: str,
) -> None:
    violations = _synthetic_http_schema_contract_violations(tmp_path, monkeypatch, source)

    assert len(violations) == 1
    assert forbidden_name in violations[0]


@pytest.mark.parametrize(
    ("source", "forbidden_name"),
    (
        ('__all__ = ["SafeResponse", "JobEvent"]\n', "JobEvent"),
        ('__all__ = []\n__all__ += ["JobProgress"]\n', "JobProgress"),
        ('__all__ = []\n__all__.append("JobEvent")\n', "JobEvent"),
        (
            '__all__ = ["SafeResponse"]\n'
            '__all__.extend(["JobStatus", "OtherResponse"])\n',
            "JobStatus",
        ),
    ),
)
def test_http_schema_scanner_rejects_literal_all_mutations(
    tmp_path: Path,
    monkeypatch,
    source: str,
    forbidden_name: str,
) -> None:
    violations = _synthetic_http_schema_contract_violations(tmp_path, monkeypatch, source)

    assert len(violations) == 1
    assert forbidden_name in violations[0]


@pytest.mark.parametrize(
    "source",
    (
        "from src.application.contracts import jobs as job_contracts\n"
        "status: job_contracts.JobStatus\n",
        "def build():\n    JobStatus = object()\n",
        "class Container:\n    JobProgress = object()\n",
        "factory = lambda: (JobEvent := object())\n",
    ),
)
def test_http_schema_guard_allows_canonical_and_nested_bindings(
    tmp_path: Path,
    monkeypatch,
    source: str,
) -> None:
    violations = _synthetic_http_schema_contract_violations(
        tmp_path,
        monkeypatch,
        source,
    )

    assert not violations


def test_repository_does_not_import_legacy_application_contract_paths() -> None:
    violations = _forbidden_http_application_contract_references(PROJECT_ROOT)
    assert not violations, "Legacy application contract imports found:\n" + "\n".join(
        violations
    )
