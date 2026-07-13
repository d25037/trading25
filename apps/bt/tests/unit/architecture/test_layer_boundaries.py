"""Architecture boundary checks for the 5-layer src layout."""

from __future__ import annotations

import ast
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_ROOT = PROJECT_ROOT / "src"
APPLICATION_HTTP_SCHEMA_BASELINE = Path(__file__).with_name(
    "application_http_schema_imports.txt"
)
APPLICATION_HTTP_SCHEMA_PREFIX = "src.entrypoints.http.schemas"
LEGACY_JOB_SCHEMA_NAMES = {"JobStatus", "JobProgress", "SSEJobEvent"}
LEGACY_JOB_SCHEMA_MODULES = {
    APPLICATION_HTTP_SCHEMA_PREFIX,
    *(f"{APPLICATION_HTTP_SCHEMA_PREFIX}.{name}" for name in ("backtest", "common", "job")),
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


def _legacy_job_schema_imports(*roots: Path) -> list[str]:
    violations: list[str] = []
    for root in roots:
        for py_file in root.rglob("*.py"):
            tree = ast.parse(py_file.read_text(encoding="utf-8"))
            module_aliases: dict[str, str] = {}
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        if alias.name in LEGACY_JOB_SCHEMA_MODULES and alias.asname:
                            module_aliases[alias.asname] = alias.name
                    continue

                if not isinstance(node, ast.ImportFrom):
                    continue

                module_name = _resolve_import_from_module(py_file, node)
                if module_name is None:
                    continue

                for alias in node.names:
                    imported_module = f"{module_name}.{alias.name}"
                    if imported_module in LEGACY_JOB_SCHEMA_MODULES:
                        module_aliases[alias.asname or alias.name] = imported_module

                if module_name not in LEGACY_JOB_SCHEMA_MODULES:
                    continue

                if any(alias.name == "*" for alias in node.names):
                    relative = py_file.relative_to(PROJECT_ROOT)
                    violations.append(
                        f"{relative}:{node.lineno} star imports legacy job contracts from {module_name}"
                    )
                    continue

                imported = LEGACY_JOB_SCHEMA_NAMES.intersection(
                    alias.name for alias in node.names
                )
                if imported:
                    relative = py_file.relative_to(PROJECT_ROOT)
                    violations.append(
                        f"{relative}:{node.lineno} imports {sorted(imported)} from {module_name}"
                    )

            for node in ast.walk(tree):
                if not isinstance(node, ast.Attribute) or not isinstance(
                    node.value, ast.Name
                ):
                    continue
                module_name = module_aliases.get(node.value.id)
                if (
                    module_name in LEGACY_JOB_SCHEMA_MODULES
                    and node.attr in LEGACY_JOB_SCHEMA_NAMES
                ):
                    relative = py_file.relative_to(PROJECT_ROOT)
                    violations.append(
                        f"{relative}:{node.lineno} accesses {node.attr} from {module_name}"
                    )
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
    return _legacy_job_schema_imports(source_root)


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
    violations = _legacy_job_schema_imports(SRC_ROOT / "application")
    assert not violations, (
        "Application job contracts must be application-owned:\n" + "\n".join(violations)
    )


def test_http_schemas_do_not_export_legacy_job_contracts() -> None:
    from src.entrypoints.http import schemas
    from src.entrypoints.http.schemas import backtest, common, job

    schema_modules = (schemas, common, job, backtest)
    for schema_module in schema_modules:
        for legacy_name in LEGACY_JOB_SCHEMA_NAMES:
            assert not hasattr(schema_module, legacy_name), (
                f"{schema_module.__name__} exports {legacy_name}"
            )


def test_legacy_job_schema_scanner_rejects_star_imports(
    tmp_path: Path,
    monkeypatch,
) -> None:
    violations = _synthetic_legacy_job_schema_imports(
        tmp_path,
        monkeypatch,
        "\n".join(
            (
                "from src.entrypoints.http.schemas.common import *",
                "from src.entrypoints.http.schemas.job import *",
                "from src.entrypoints.http.schemas.backtest import *",
                "from src.entrypoints.http.schemas import *",
            )
        ),
    )

    assert len(violations) == 4


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


def test_legacy_job_schema_scanner_allows_unrelated_dtos(
    tmp_path: Path,
    monkeypatch,
) -> None:
    violations = _synthetic_legacy_job_schema_imports(
        tmp_path,
        monkeypatch,
        "from src.entrypoints.http.schemas.backtest import BacktestRequest\n"
        "import src.entrypoints.http.schemas.common as common\n"
        "Response = common.BaseJobResponse\n",
    )

    assert not violations


def test_repository_does_not_import_legacy_job_contract_paths() -> None:
    violations = _legacy_job_schema_imports(SRC_ROOT, PROJECT_ROOT / "tests")
    assert not violations, "Legacy job contract imports found:\n" + "\n".join(
        violations
    )
