"""Architecture boundary checks for the 5-layer src layout."""

from __future__ import annotations

import ast
from dataclasses import dataclass
import hashlib
import importlib.util
import json
from pathlib import Path
import sys

import pytest

from tests.unit.architecture.application_contract_boundary_guard import (
    APPLICATION_HTTP_SCHEMA_PREFIX,
    _binding_names,
    _module_scope_nodes,
    forbidden_http_application_contract_references,
)


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_ROOT = PROJECT_ROOT / "src"
HTTP_SCHEMA_ROOT = SRC_ROOT / "entrypoints" / "http" / "schemas"
LAYER_NAMES = ("entrypoints", "application", "domains", "infrastructure", "shared")
TASK16_APPLICATION_CONTRACT_MODULES = (
    "chart.py",
    "dataset.py",
    "dataset_data.py",
    "fundamentals.py",
    "jquants.py",
    "lab.py",
    "margin_analytics.py",
    "market_data.py",
    "market_data_plane.py",
    "roe.py",
    "strategy_authoring.py",
)
TASK16_APPLICATION_TYPE_ALIASES = {
    "AdjustedMetricsStatusLiteral",
    "AuthoringFieldSection",
    "AuthoringFieldSource",
    "AuthoringFieldType",
    "AuthoringWidgetType",
    "DatasetStorageBackend",
    "IntradayFreshnessStatusLiteral",
    "IntradaySyncModeLiteral",
    "LabResultData",
    "Options225CoverageStatusLiteral",
    "StrictIsoDate",
    "SyncModeLiteral",
    "ValidationHealthStatusLiteral",
}
TASK16_DELETED_HTTP_SCHEMA_MODULES = (
    "analytics_margin.py",
    "analytics_roe.py",
    "chart.py",
    "dataset_data.py",
    "jquants.py",
    "market_data.py",
)

# Core dependency rules between top-level layers.
ALLOWED_TARGET_LAYERS = {
    "entrypoints": {"entrypoints", "application", "domains", "infrastructure", "shared"},
    "application": {"application", "domains", "infrastructure", "shared"},
    "domains": {"domains", "infrastructure", "shared"},
    "infrastructure": {"infrastructure", "shared"},
    "shared": {"shared"},
}

ALLOWED_EXTRA_PREFIXES = {
    "shared": ("src.infrastructure.data_access.loaders",),
}

# Approved Data Plane edges: infrastructure implements the canonical PIT read
# contract and persists the domain adjustment-basis model. Keep these
# exceptions file/module exact so unrelated upward dependencies still fail.
ALLOWED_EXACT_IMPORT_EDGES = {
    (
        "infrastructure/data_access/clients.py",
        "src.application.contracts.fundamentals_pit",
    ),
    (
        "infrastructure/data_access/fundamentals_pit_reader.py",
        "src.application.contracts.fundamentals_pit",
    ),
    (
        "infrastructure/db/market/dataset_snapshot_reader.py",
        "src.domains.fundamentals.adjustment_basis",
    ),
    (
        "infrastructure/db/dataset_io/dataset_pit_lineage.py",
        "src.domains.fundamentals.adjustment_basis",
    ),
    (
        "infrastructure/db/market/adjustment_basis_validation.py",
        "src.domains.fundamentals.adjustment_basis",
    ),
    (
        "infrastructure/db/market/valuation_writers.py",
        "src.domains.fundamentals.adjustment_basis",
    ),
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


def _renamed_contract_binding_violations(
    py_file: Path,
    *,
    canonical_modules: dict[str, set[str]],
) -> list[str]:
    tree = ast.parse(py_file.read_text(encoding="utf-8"), filename=str(py_file))
    module_aliases: dict[str, set[str]] = {}
    for node in _module_scope_nodes(tree):
        if isinstance(node, ast.ImportFrom):
            resolved_module = _resolve_import_from_module(py_file, node)
            if resolved_module is None:
                continue
            for imported in node.names:
                module_name = f"{resolved_module}.{imported.name}"
                canonical_names = canonical_modules.get(module_name)
                if canonical_names is not None:
                    module_aliases[imported.asname or imported.name] = canonical_names
        elif isinstance(node, ast.Import):
            for imported in node.names:
                canonical_names = canonical_modules.get(imported.name)
                if canonical_names is not None and imported.asname is not None:
                    module_aliases[imported.asname] = canonical_names

    def canonical_reference(expression: ast.expr) -> str | None:
        def dotted_name(node: ast.expr) -> str | None:
            if isinstance(node, ast.Name):
                return node.id
            if isinstance(node, ast.Attribute):
                prefix = dotted_name(node.value)
                return f"{prefix}.{node.attr}" if prefix is not None else None
            return None

        dotted = dotted_name(expression)
        if dotted is None:
            return None
        for module_name, canonical_names in canonical_modules.items():
            if any(dotted == f"{module_name}.{name}" for name in canonical_names):
                return dotted
        alias, separator, member = dotted.partition(".")
        canonical_names = module_aliases.get(alias)
        if not separator or canonical_names is None or member not in canonical_names:
            return None
        return dotted

    violations: list[str] = []
    for node in _module_scope_nodes(tree):
        references: list[str] = []
        if isinstance(node, ast.Assign):
            reference = canonical_reference(node.value)
            if reference is not None:
                references.append(reference)
        elif isinstance(node, (ast.AnnAssign, ast.TypeAlias)) and node.value is not None:
            reference = canonical_reference(node.value)
            if reference is not None:
                references.append(reference)
        elif isinstance(node, ast.ClassDef):
            references.extend(
                reference
                for base in node.bases
                if (reference := canonical_reference(base)) is not None
            )
        if references:
            violations.append(
                f"{py_file.relative_to(PROJECT_ROOT)}:{node.lineno} creates renamed "
                f"contract binding from {sorted(references)}"
            )
    return violations


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
    *,
    module_name: str = "synthetic.py",
) -> list[str]:
    schemas_root = tmp_path / "src" / "entrypoints" / "http" / "schemas"
    schemas_root.mkdir(parents=True)
    (schemas_root / module_name).write_text(source, encoding="utf-8")
    monkeypatch.setattr(sys.modules[__name__], "PROJECT_ROOT", tmp_path)
    return _forbidden_http_application_contract_references(schemas_root)


def _synthetic_http_route_contract_violations(
    tmp_path: Path,
    monkeypatch,
    source: str,
) -> list[str]:
    routes_root = tmp_path / "src" / "entrypoints" / "http" / "routes"
    routes_root.mkdir(parents=True)
    (routes_root / "synthetic.py").write_text(source, encoding="utf-8")
    monkeypatch.setattr(sys.modules[__name__], "PROJECT_ROOT", tmp_path)
    return _forbidden_http_application_contract_references(routes_root)


def _is_allowed_import(
    source_layer: str,
    module_name: str,
    *,
    source_file: Path | None = None,
) -> bool:
    parts = module_name.split(".")
    if len(parts) < 2 or parts[0] != "src":
        return True

    target_layer = parts[1]
    if target_layer not in LAYER_NAMES:
        return True

    if target_layer in ALLOWED_TARGET_LAYERS[source_layer]:
        return True

    if source_file is not None:
        relative_source = source_file.relative_to(SRC_ROOT).as_posix()
        if (relative_source, module_name) in ALLOWED_EXACT_IMPORT_EDGES:
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
            if not _is_allowed_import(
                source_layer,
                module_name,
                source_file=py_file,
            ):
                relative = py_file.relative_to(PROJECT_ROOT)
                violations.append(
                    f"{relative}:{line_no} ({source_layer} -> {module_name})"
                )

    assert not violations, "Layer boundary violations found:\n" + "\n".join(sorted(violations))


def test_application_does_not_import_http_schemas() -> None:
    violations = sorted(_application_http_schema_imports())
    assert not violations, (
        "Application code must own its contracts and cannot import HTTP schemas:\n"
        + "\n".join(violations)
    )


def test_task16_http_modules_do_not_bind_application_owned_contracts() -> None:
    contract_root = SRC_ROOT / "application" / "contracts"
    forbidden_names = set(TASK16_APPLICATION_TYPE_ALIASES)
    canonical_modules: dict[str, set[str]] = {}
    for contract_module in TASK16_APPLICATION_CONTRACT_MODULES:
        tree = ast.parse(
            (contract_root / contract_module).read_text(encoding="utf-8"),
            filename=contract_module,
        )
        contract_names = {
            node.name for node in tree.body if isinstance(node, ast.ClassDef)
        }
        forbidden_names.update(contract_names)
        module_name = contract_module.removesuffix(".py")
        module_aliases = {
            alias
            for alias in TASK16_APPLICATION_TYPE_ALIASES
            if alias in (contract_root / contract_module).read_text(encoding="utf-8")
        }
        canonical_modules[f"src.application.contracts.{module_name}"] = (
            contract_names | module_aliases
        )

    violations: list[str] = []
    http_root = SRC_ROOT / "entrypoints" / "http"
    for py_file in http_root.rglob("*.py"):
        tree = ast.parse(py_file.read_text(encoding="utf-8"), filename=str(py_file))
        for node in _module_scope_nodes(tree):
            rebound = _binding_names(node) & forbidden_names
            if (
                py_file == HTTP_SCHEMA_ROOT / "indicators.py"
                and isinstance(node, ast.ClassDef)
                and node.name == "OHLCVRecord"
            ):
                rebound.discard("OHLCVRecord")
            if rebound:
                violations.append(
                    f"{py_file.relative_to(PROJECT_ROOT)}:{node.lineno} "
                    f"binds application-owned contracts {sorted(rebound)}"
                )
        violations.extend(
            _renamed_contract_binding_violations(
                py_file,
                canonical_modules=canonical_modules,
            )
        )

    assert not violations, (
        "HTTP schemas must compose application contracts without aliases, "
        "re-exports, subclasses, or duplicate canonical models:\n"
        + "\n".join(violations)
    )


def test_market_maintenance_contract_has_no_alternate_source_bindings() -> None:
    canonical_path = (
        SRC_ROOT / "shared" / "contracts" / "market_maintenance.py"
    )
    canonical_tree = ast.parse(
        canonical_path.read_text(encoding="utf-8"),
        filename=str(canonical_path),
    )
    forbidden_names = {
        node.name
        for node in canonical_tree.body
        if isinstance(node, ast.ClassDef)
    }
    canonical_modules = {
        "src.shared.contracts.market_maintenance": forbidden_names,
    }
    violations: list[str] = []
    for py_file in SRC_ROOT.rglob("*.py"):
        if py_file == canonical_path:
            continue
        tree = ast.parse(py_file.read_text(encoding="utf-8"), filename=str(py_file))
        for node in _module_scope_nodes(tree):
            rebound = _binding_names(node) & forbidden_names
            if rebound:
                violations.append(
                    f"{py_file.relative_to(PROJECT_ROOT)}:{node.lineno} "
                    f"binds shared maintenance contracts {sorted(rebound)}"
                )
        violations.extend(
            _renamed_contract_binding_violations(
                py_file,
                canonical_modules=canonical_modules,
            )
        )
    assert not violations, (
        "Market maintenance contract names must only be bound by their shared "
        "canonical module:\n" + "\n".join(violations)
    )


@pytest.mark.parametrize(
    "source",
    (
        "from src.application.contracts import chart as chart_contracts\n"
        "ChartResult = chart_contracts.IndexDataResponse\n",
        "from src.application.contracts import chart as chart_contracts\n"
        "class ChartResult(chart_contracts.IndexDataResponse):\n"
        "    pass\n",
        "import src.application.contracts.chart\n"
        "ChartResult = src.application.contracts.chart.IndexDataResponse\n",
        "from typing import TYPE_CHECKING\n"
        "if TYPE_CHECKING:\n"
        "    from src.application.contracts import chart as chart_contracts\n"
        "ChartResult = chart_contracts.IndexDataResponse\n",
        "from typing import TYPE_CHECKING\n"
        "if TYPE_CHECKING:\n"
        "    import src.application.contracts.chart as chart_contracts\n"
        "class ChartResult(chart_contracts.IndexDataResponse):\n"
        "    pass\n",
    ),
)
def test_task16_guard_rejects_renamed_http_contract_bindings(
    tmp_path: Path,
    monkeypatch,
    source: str,
) -> None:
    http_file = tmp_path / "src" / "entrypoints" / "http" / "synthetic.py"
    http_file.parent.mkdir(parents=True)
    http_file.write_text(source, encoding="utf-8")
    monkeypatch.setattr(sys.modules[__name__], "PROJECT_ROOT", tmp_path)
    violations = _renamed_contract_binding_violations(
        http_file,
        canonical_modules={
            "src.application.contracts.chart": {"IndexDataResponse"},
        },
    )
    assert len(violations) == 1


@pytest.mark.parametrize(
    "source",
    (
        "from ....application.contracts import chart as chart_contracts\n"
        "ChartResult = chart_contracts.IndexDataResponse\n",
        "if True:\n"
        "    from ....application.contracts import chart as chart_contracts\n"
        "class ChartResult(chart_contracts.IndexDataResponse):\n"
        "    pass\n",
        "from typing import TYPE_CHECKING\n"
        "if TYPE_CHECKING:\n"
        "    from ....application.contracts import chart as chart_contracts\n"
        "ChartResult = chart_contracts.IndexDataResponse\n",
    ),
)
def test_task16_guard_rejects_relative_module_contract_aliases(
    tmp_path: Path,
    monkeypatch,
    source: str,
) -> None:
    http_file = (
        tmp_path / "src" / "entrypoints" / "http" / "routes" / "synthetic.py"
    )
    http_file.parent.mkdir(parents=True)
    http_file.write_text(source, encoding="utf-8")
    monkeypatch.setattr(sys.modules[__name__], "PROJECT_ROOT", tmp_path)

    violations = _renamed_contract_binding_violations(
        http_file,
        canonical_modules={
            "src.application.contracts.chart": {"IndexDataResponse"},
        },
    )

    assert len(violations) == 1


@pytest.mark.parametrize(
    "source",
    (
        "def build_response():\n"
        "    from src.application.contracts import chart as chart_contracts\n"
        "    return chart_contracts.IndexDataResponse\n",
        "class ResponseFactory:\n"
        "    from src.application.contracts import chart as chart_contracts\n"
        "    response_type = chart_contracts.IndexDataResponse\n",
    ),
)
def test_task16_guard_ignores_function_and_class_local_contract_aliases(
    tmp_path: Path,
    monkeypatch,
    source: str,
) -> None:
    http_file = tmp_path / "src" / "entrypoints" / "http" / "synthetic.py"
    http_file.parent.mkdir(parents=True)
    http_file.write_text(source, encoding="utf-8")
    monkeypatch.setattr(sys.modules[__name__], "PROJECT_ROOT", tmp_path)

    violations = _renamed_contract_binding_violations(
        http_file,
        canonical_modules={
            "src.application.contracts.chart": {"IndexDataResponse"},
        },
    )

    assert violations == []


def test_task16_guard_ignores_unrelated_relative_import(
    tmp_path: Path,
    monkeypatch,
) -> None:
    http_file = (
        tmp_path / "src" / "entrypoints" / "http" / "routes" / "synthetic.py"
    )
    http_file.parent.mkdir(parents=True)
    http_file.write_text(
        "from ....application.services import chart as chart_contracts\n"
        "ChartResult = chart_contracts.IndexDataResponse\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(sys.modules[__name__], "PROJECT_ROOT", tmp_path)

    violations = _renamed_contract_binding_violations(
        http_file,
        canonical_modules={
            "src.application.contracts.chart": {"IndexDataResponse"},
        },
    )

    assert violations == []


@pytest.mark.parametrize(
    "source",
    (
        "from src.shared.contracts import market_maintenance as maintenance_contracts\n"
        "Evidence = maintenance_contracts.MarketMaintenanceRecord\n",
        "from src.shared.contracts import market_maintenance as maintenance_contracts\n"
        "class Evidence(maintenance_contracts.MarketMaintenanceRecord):\n"
        "    pass\n",
        "import src.shared.contracts.market_maintenance\n"
        "Evidence = src.shared.contracts.market_maintenance.MarketMaintenanceRecord\n",
        "from typing import TYPE_CHECKING\n"
        "if TYPE_CHECKING:\n"
        "    from src.shared.contracts import market_maintenance as maintenance_contracts\n"
        "Evidence = maintenance_contracts.MarketMaintenanceRecord\n",
    ),
)
def test_maintenance_guard_rejects_renamed_contract_bindings(
    tmp_path: Path,
    monkeypatch,
    source: str,
) -> None:
    source_file = tmp_path / "src" / "application" / "synthetic.py"
    source_file.parent.mkdir(parents=True)
    source_file.write_text(source, encoding="utf-8")
    monkeypatch.setattr(sys.modules[__name__], "PROJECT_ROOT", tmp_path)
    violations = _renamed_contract_binding_violations(
        source_file,
        canonical_modules={
            "src.shared.contracts.market_maintenance": {
                "MarketMaintenanceRecord"
            },
        },
    )
    assert len(violations) == 1


@pytest.mark.parametrize(
    "source",
    (
        "from ....shared.contracts import market_maintenance as maintenance_contracts\n"
        "Evidence = maintenance_contracts.MarketMaintenanceRecord\n",
        "from typing import TYPE_CHECKING\n"
        "if TYPE_CHECKING:\n"
        "    from ....shared.contracts import market_maintenance as maintenance_contracts\n"
        "Evidence = maintenance_contracts.MarketMaintenanceRecord\n",
    ),
)
def test_maintenance_guard_rejects_relative_module_contract_aliases(
    tmp_path: Path,
    monkeypatch,
    source: str,
) -> None:
    source_file = (
        tmp_path / "src" / "entrypoints" / "http" / "routes" / "synthetic.py"
    )
    source_file.parent.mkdir(parents=True)
    source_file.write_text(source, encoding="utf-8")
    monkeypatch.setattr(sys.modules[__name__], "PROJECT_ROOT", tmp_path)

    violations = _renamed_contract_binding_violations(
        source_file,
        canonical_modules={
            "src.shared.contracts.market_maintenance": {
                "MarketMaintenanceRecord"
            },
        },
    )

    assert len(violations) == 1


def test_task16_response_only_http_schema_modules_are_deleted() -> None:
    existing = [
        module_name
        for module_name in TASK16_DELETED_HTTP_SCHEMA_MODULES
        if (HTTP_SCHEMA_ROOT / module_name).exists()
    ]
    assert not existing


def test_market_maintenance_contract_is_shared_not_application_owned() -> None:
    assert not (
        SRC_ROOT / "application" / "contracts" / "market_maintenance.py"
    ).exists()
    assert (
        SRC_ROOT / "shared" / "contracts" / "market_maintenance.py"
    ).is_file()


def test_legacy_ranking_http_schema_is_deleted() -> None:
    assert not (HTTP_SCHEMA_ROOT / "ranking.py").exists()


def test_legacy_factor_regression_http_schemas_are_deleted() -> None:
    assert not (HTTP_SCHEMA_ROOT / "factor_regression.py").exists()
    assert not (HTTP_SCHEMA_ROOT / "portfolio_factor_regression.py").exists()


def test_production_source_does_not_import_legacy_ranking_http_schema() -> None:
    violations = [
        f"{py_file.relative_to(PROJECT_ROOT)}:{line_no}"
        for py_file in _iter_layer_python_files()
        for module_name, line_no in _iter_src_imports(py_file)
        if module_name == "src.entrypoints.http.schemas.ranking"
    ]
    assert not violations


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
        (
            "from src.entrypoints.http.schemas.backtest import SignalAttributionResult\n",
            "SignalAttributionResult",
        ),
        (
            "from src.entrypoints.http.schemas.analytics_common import DataProvenance\n",
            "DataProvenance",
        ),
        (
            "from src.entrypoints.http.schemas.screening import MatchedStrategyItem\n",
            "MatchedStrategyItem",
        ),
        (
            "from src.entrypoints.http.schemas.screening import ScreeningSortBy\n",
            "ScreeningSortBy",
        ),
        (
            "from src.entrypoints.http.schemas.signal_reference import "
            "SignalReferenceResponse\n",
            "SignalReferenceResponse",
        ),
        *(
            (
                f"from src.entrypoints.http.schemas.portfolio_performance import {name}\n",
                name,
            )
            for name in (
                "PerformanceSummary",
                "HoldingDetail",
                "TimeSeriesPoint",
                "BenchmarkResult",
                "BenchmarkTimeSeriesPoint",
                "PortfolioPerformanceResponse",
                "WatchlistStockPrice",
                "WatchlistPricesResponse",
            )
        ),
        (
            "from src.entrypoints.http.schemas.ranking import RankingItem\n",
            "RankingItem",
        ),
        (
            "from src.entrypoints.http.schemas.ranking import SafeName\n",
            "SafeName",
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
        "from src.application.contracts import signal_reference as "
        "signal_reference_contracts\n"
        "response: signal_reference_contracts.SignalReferenceResponse\n",
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
        ("class SignalAttributionResult:\n    pass\n", "SignalAttributionResult"),
        ("class ResponseDiagnostics:\n    pass\n", "ResponseDiagnostics"),
        (
            'AnalyticsSourceKind = Literal["market", "dataset"]\n',
            "AnalyticsSourceKind",
        ),
        ("class MarketScreeningResponse:\n    pass\n", "MarketScreeningResponse"),
        (
            'EntryDecidability = Literal["pre_open_decidable"]\n',
            "EntryDecidability",
        ),
        ("class SignalReferenceResponse:\n    pass\n", "SignalReferenceResponse"),
        (
            'SignalFieldTypeValue = Literal["boolean", "number"]\n',
            "SignalFieldTypeValue",
        ),
        *((f"class {name}:\n    pass\n", name) for name in (
            "PerformanceSummary",
            "HoldingDetail",
            "TimeSeriesPoint",
            "BenchmarkResult",
            "BenchmarkTimeSeriesPoint",
            "PortfolioPerformanceResponse",
            "WatchlistStockPrice",
            "WatchlistPricesResponse",
        )),
        *((f"class {name}:\n    pass\n", name) for name in (
            "RankingItem",
            "Rankings",
            "IndexPerformanceItem",
            "MarketRankingResponse",
            "MarketRankingSymbolResponse",
            "FundamentalRankingItem",
            "FundamentalRankings",
            "MarketFundamentalRankingResponse",
            "ValueCompositeTechnicalMetrics",
            "ValueCompositeRankingItem",
            "ValueCompositeRankingResponse",
            "ValueCompositeScoreResponse",
            "ValueCompositeScoreMethod",
            "ValueCompositeProfileId",
            "ValueCompositeForwardEpsMode",
            "ValueCompositeScoreUnavailableReason",
            "LiquidityRegime",
            "RankingRiskFlag",
            "RankingTechnicalFlag",
            "RankingRegimeStateFilter",
            "RankingRiskStateFilter",
            "RankingTechnicalStateFilter",
            "RankingFundamentalStateFilter",
            "SectorStrengthBucket",
            "SectorStrengthFamily",
            "normalize_sector_strength_family",
            "RankingStateFilter",
        )),
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


def test_http_schema_guard_rejects_recreated_ranking_module(
    tmp_path: Path,
    monkeypatch,
) -> None:
    violations = _synthetic_http_schema_contract_violations(
        tmp_path,
        monkeypatch,
        "SAFE_CONSTANT = 1\n",
        module_name="ranking.py",
    )

    assert len(violations) == 1
    assert "recreates deleted HTTP ranking schema module" in violations[0]


@pytest.mark.parametrize(
    ("module_name", "canonical_module", "contract_names"),
    (
        (
            "factor_regression.py",
            "src.application.contracts.factor_regression",
            ("DateRange", "IndexMatch", "FactorRegressionResponse"),
        ),
        (
            "portfolio_factor_regression.py",
            "src.application.contracts.portfolio_factor_regression",
            (
                "StockWeight",
                "ExcludedStock",
                "IndexMatch",
                "DateRange",
                "PortfolioFactorRegressionResponse",
            ),
        ),
    ),
)
def test_http_schema_guard_rejects_recreated_factor_regression_modules(
    tmp_path: Path,
    monkeypatch,
    module_name: str,
    canonical_module: str,
    contract_names: tuple[str, ...],
) -> None:
    source = "".join(
        f"from {canonical_module} import {contract_name}\n"
        for contract_name in contract_names
    )
    violations = _synthetic_http_schema_contract_violations(
        tmp_path,
        monkeypatch,
        source,
        module_name=module_name,
    )

    assert any("recreates deleted HTTP" in violation for violation in violations)
    for contract_name in contract_names:
        assert any(contract_name in violation for violation in violations)


@pytest.mark.parametrize(
    ("module_name", "contract_names"),
    (
        (
            "factor_regression.py",
            ("DateRange", "IndexMatch", "FactorRegressionResponse"),
        ),
        (
            "portfolio_factor_regression.py",
            (
                "StockWeight",
                "ExcludedStock",
                "IndexMatch",
                "DateRange",
                "PortfolioFactorRegressionResponse",
            ),
        ),
    ),
)
def test_deleted_factor_regression_http_modules_cannot_reexport_contract_names(
    tmp_path: Path,
    monkeypatch,
    module_name: str,
    contract_names: tuple[str, ...],
) -> None:
    violations = _synthetic_http_schema_contract_violations(
        tmp_path,
        monkeypatch,
        f"__all__ = {list(contract_names)!r}\n",
        module_name=module_name,
    )

    for contract_name in contract_names:
        assert any(
            "exports forbidden HTTP application contracts" in violation
            and contract_name in violation
            for violation in violations
        )


@pytest.mark.parametrize(
    "source",
    (
        "from src.application.contracts.factor_regression import "
        "FactorRegressionResponse as CompatResponse\n",
        "from src.application.contracts.portfolio_factor_regression import "
        "PortfolioFactorRegressionResponse\n",
        "from src.application.contracts import factor_regression as factor_contracts\n"
        "FactorRegressionResponse = factor_contracts.FactorRegressionResponse\n"
        "__all__ = ['FactorRegressionResponse']\n",
        "from src.application.contracts import portfolio_factor_regression as "
        "portfolio_factor_contracts\n"
        "DateRange = portfolio_factor_contracts.DateRange\n"
        "__all__ = ['DateRange']\n",
        "from src.application.contracts import factor_regression as factor_contracts\n"
        "class CompatResponse(factor_contracts.FactorRegressionResponse):\n"
        "    pass\n",
    ),
)
def test_alternate_http_schema_cannot_forward_canonical_factor_contracts(
    tmp_path: Path,
    monkeypatch,
    source: str,
) -> None:
    violations = _synthetic_http_schema_contract_violations(
        tmp_path,
        monkeypatch,
        source,
        module_name="compat.py",
    )

    assert violations


@pytest.mark.parametrize(
    "contract_name",
    (
        "FactorRegressionResponse",
        "PortfolioFactorRegressionResponse",
        "StockWeight",
        "ExcludedStock",
    ),
)
def test_alternate_http_schema_cannot_redefine_unique_factor_contracts(
    tmp_path: Path,
    monkeypatch,
    contract_name: str,
) -> None:
    violations = _synthetic_http_schema_contract_violations(
        tmp_path,
        monkeypatch,
        f"class {contract_name}:\n    pass\n",
        module_name="compat.py",
    )

    assert any(contract_name in violation for violation in violations)


@pytest.mark.parametrize(
    ("service_name", "canonical_module", "required_alias", "contract_names"),
    (
        (
            "factor_regression_service.py",
            "factor_regression",
            "factor_contracts",
            ("DateRange", "IndexMatch", "FactorRegressionResponse"),
        ),
        (
            "portfolio_factor_regression_service.py",
            "portfolio_factor_regression",
            "portfolio_factor_contracts",
            (
                "StockWeight",
                "ExcludedStock",
                "IndexMatch",
                "DateRange",
                "PortfolioFactorRegressionResponse",
            ),
        ),
    ),
)
def test_factor_services_use_module_qualified_contract_imports(
    service_name: str,
    canonical_module: str,
    required_alias: str,
    contract_names: tuple[str, ...],
) -> None:
    service_path = SRC_ROOT / "application" / "services" / service_name
    tree = ast.parse(service_path.read_text(encoding="utf-8"))
    matching_imports = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        and node.module == "src.application.contracts"
        and any(
            alias.name == canonical_module and alias.asname == required_alias
            for alias in node.names
        )
    ]
    direct_or_wildcard_imports = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        and node.module == f"src.application.contracts.{canonical_module}"
    ]
    direct_model_bindings = {
        binding
        for node in _module_scope_nodes(tree)
        for binding in _binding_names(node)
        if binding in contract_names
    }

    assert len(matching_imports) == 1
    assert not direct_or_wildcard_imports
    assert not direct_model_bindings


@pytest.mark.parametrize(
    "canonical_module",
    (
        "src.application.contracts.factor_regression",
        "src.application.contracts.portfolio_factor_regression",
    ),
)
def test_http_guard_rejects_canonical_factor_regression_wildcard_imports(
    tmp_path: Path,
    monkeypatch,
    canonical_module: str,
) -> None:
    violations = _synthetic_http_route_contract_violations(
        tmp_path,
        monkeypatch,
        f"from {canonical_module} import *\n",
    )

    assert len(violations) == 1
    assert canonical_module in violations[0]
    assert "wildcard" in violations[0]


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
        (
            '__all__ = ["SignalAttributionResult"]\n',
            "SignalAttributionResult",
        ),
        ('__all__ = ["DataProvenance"]\n', "DataProvenance"),
        ('__all__ = ["ScreeningJobPayload"]\n', "ScreeningJobPayload"),
        ('__all__ = ["ScreeningSupport"]\n', "ScreeningSupport"),
        ('__all__ = ["SignalCategorySchema"]\n', "SignalCategorySchema"),
        *(
            (f'__all__ = ["{name}"]\n', name)
            for name in (
                "RankingItem",
                "Rankings",
                "IndexPerformanceItem",
                "MarketRankingResponse",
                "MarketRankingSymbolResponse",
                "FundamentalRankingItem",
                "FundamentalRankings",
                "MarketFundamentalRankingResponse",
                "ValueCompositeTechnicalMetrics",
                "ValueCompositeRankingItem",
                "ValueCompositeRankingResponse",
                "ValueCompositeScoreResponse",
                "ValueCompositeScoreMethod",
                "ValueCompositeProfileId",
                "ValueCompositeForwardEpsMode",
                "ValueCompositeScoreUnavailableReason",
                "LiquidityRegime",
                "RankingRiskFlag",
                "RankingTechnicalFlag",
                "RankingRegimeStateFilter",
                "RankingRiskStateFilter",
                "RankingTechnicalStateFilter",
                "RankingFundamentalStateFilter",
                "SectorStrengthBucket",
                "SectorStrengthFamily",
                "normalize_sector_strength_family",
                "RankingStateFilter",
            )
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
        "from src.application.contracts import signal_reference as "
        "signal_reference_contracts\n"
        "response: signal_reference_contracts.SignalReferenceResponse\n",
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


@pytest.mark.parametrize(
    ("source", "forbidden_name"),
    (
        (
            "from src.application.contracts.signal_reference import "
            "SignalReferenceResponse as Response\n",
            "SignalReferenceResponse",
        ),
        ("class FieldConstraints:\n    pass\n", "FieldConstraints"),
        ("SignalCategorySchema = object()\n", "SignalCategorySchema"),
        ('__all__ = ["SignalFieldSchema"]\n', "SignalFieldSchema"),
        *(
            (f"from somewhere import Value as {name}\n", name)
            for name in (
                "PerformanceSummary",
                "HoldingDetail",
                "TimeSeriesPoint",
                "BenchmarkResult",
                "BenchmarkTimeSeriesPoint",
                "PortfolioPerformanceResponse",
                "WatchlistStockPrice",
                "WatchlistPricesResponse",
            )
        ),
        *(
            (f"from somewhere import Value as {name}\n", name)
            for name in (
                "RankingItem",
                "Rankings",
                "IndexPerformanceItem",
                "MarketRankingResponse",
                "MarketRankingSymbolResponse",
                "FundamentalRankingItem",
                "FundamentalRankings",
                "MarketFundamentalRankingResponse",
                "ValueCompositeTechnicalMetrics",
                "ValueCompositeRankingItem",
                "ValueCompositeRankingResponse",
                "ValueCompositeScoreResponse",
                "ValueCompositeScoreMethod",
                "ValueCompositeProfileId",
                "ValueCompositeForwardEpsMode",
                "ValueCompositeScoreUnavailableReason",
                "LiquidityRegime",
                "RankingRiskFlag",
                "RankingTechnicalFlag",
                "RankingRegimeStateFilter",
                "RankingRiskStateFilter",
                "RankingTechnicalStateFilter",
                "RankingFundamentalStateFilter",
                "SectorStrengthBucket",
                "SectorStrengthFamily",
                "normalize_sector_strength_family",
                "RankingStateFilter",
            )
        ),
    ),
)
def test_http_route_guard_rejects_canonical_contract_bindings(
    tmp_path: Path,
    monkeypatch,
    source: str,
    forbidden_name: str,
) -> None:
    violations = _synthetic_http_route_contract_violations(
        tmp_path,
        monkeypatch,
        source,
    )

    assert len(violations) == 1
    assert forbidden_name in violations[0]


def test_http_route_guard_allows_qualified_canonical_module_import(
    tmp_path: Path,
    monkeypatch,
) -> None:
    violations = _synthetic_http_route_contract_violations(
        tmp_path,
        monkeypatch,
        "from src.application.contracts import signal_reference as "
        "signal_reference_contracts\n"
        "response: signal_reference_contracts.SignalReferenceResponse\n",
    )

    assert not violations


def test_http_route_guard_allows_qualified_ranking_contract_module_import(
    tmp_path: Path,
    monkeypatch,
) -> None:
    violations = _synthetic_http_route_contract_violations(
        tmp_path,
        monkeypatch,
        "from src.application.contracts import ranking as ranking_contracts\n"
        "response: ranking_contracts.MarketRankingResponse\n",
    )

    assert not violations


def test_http_route_guard_rejects_canonical_ranking_wildcard_import(
    tmp_path: Path,
    monkeypatch,
) -> None:
    violations = _synthetic_http_route_contract_violations(
        tmp_path,
        monkeypatch,
        "from src.application.contracts.ranking import *\n",
    )

    assert len(violations) == 1
    assert "src/entrypoints/http/routes/synthetic.py:1" in violations[0]
    assert "src.application.contracts.ranking" in violations[0]
    assert "wildcard" in violations[0]


def test_http_schema_guard_rejects_canonical_ranking_wildcard_import(
    tmp_path: Path,
    monkeypatch,
) -> None:
    violations = _synthetic_http_schema_contract_violations(
        tmp_path,
        monkeypatch,
        "from src.application.contracts.ranking import *\n",
    )

    assert len(violations) == 1
    assert "src/entrypoints/http/schemas/synthetic.py:1" in violations[0]
    assert "src.application.contracts.ranking" in violations[0]
    assert "wildcard" in violations[0]


@pytest.mark.parametrize(
    "source",
    (
        "from src.application.contracts.jobs import *\n",
        "from unrelated.module import *\n",
    ),
)
def test_http_guard_allows_unrelated_wildcard_imports(
    tmp_path: Path,
    monkeypatch,
    source: str,
) -> None:
    violations = _synthetic_http_route_contract_violations(
        tmp_path,
        monkeypatch,
        source,
    )

    assert not violations


@pytest.mark.parametrize(
    "source",
    (
        "class DateRange:\n    pass\n",
        "from src.application.contracts.portfolio_performance import DateRange\n",
        "from somewhere import Value as DateRange\n",
    ),
)
def test_portfolio_performance_http_schema_cannot_bind_date_range(
    tmp_path: Path,
    monkeypatch,
    source: str,
) -> None:
    violations = _synthetic_http_schema_contract_violations(
        tmp_path,
        monkeypatch,
        source,
        module_name="portfolio_performance.py",
    )

    assert len(violations) == 1
    assert "DateRange" in violations[0]


def test_unrelated_http_schema_can_bind_date_range(
    tmp_path: Path,
    monkeypatch,
) -> None:
    violations = _synthetic_http_schema_contract_violations(
        tmp_path,
        monkeypatch,
        "class DateRange:\n    pass\n",
        module_name="dataset.py",
    )

    assert not violations


def test_repository_has_no_legacy_portfolio_performance_http_contract() -> None:
    legacy_module = SRC_ROOT / "entrypoints" / "http" / "schemas" / "portfolio_performance.py"
    assert not legacy_module.exists()

    legacy_path = "src.entrypoints.http.schemas.portfolio_performance"
    references = [
        py_file.relative_to(PROJECT_ROOT).as_posix()
        for py_file in _iter_layer_python_files()
        if legacy_path in py_file.read_text(encoding="utf-8")
    ]
    assert not references, f"Legacy portfolio performance path found in: {references}"


def test_repository_does_not_import_legacy_application_contract_paths() -> None:
    violations = _forbidden_http_application_contract_references(PROJECT_ROOT)
    assert not violations, "Legacy application contract imports found:\n" + "\n".join(
        violations
    )


_DAILY_RANKING_TASK8_CONSUMERS = frozenset(
    {
        "ranking_crowded_long_tail_evidence",
        "ranking_daily_triage_lens",
        "ranking_forecast_operating_profit_growth_evidence",
        "ranking_liquidity_z_long_evidence",
        "ranking_long_scaffold_value_composite_evidence",
        "ranking_psr_valuation_evidence",
        "ranking_roe_quality_evidence",
        "ranking_short_red_evidence",
        "ranking_short_value_composite_evidence",
    }
)
_DAILY_RANKING_TASK9_CONSUMERS = frozenset(
    {
        "ranking_core_factor_regime_breakdown",
        "ranking_core_sector_neutral_value_regime_breakdown",
        "ranking_core_sector_relative_value_evidence",
        "ranking_fixed_return_priority_evidence",
        "ranking_long_scaffold_factor_cross_evidence",
        "ranking_long_sector_leadership_horizon_decomposition",
        "ranking_n225_neutral_rerating_benchmark",
        "ranking_sector_strength_evidence",
        "ranking_short_sector_strength_evidence",
        "ranking_technical_fit_score_shape_evidence",
        "ranking_trend_acceleration_conditional_lift",
        "ranking_trend_slope_evidence",
    }
)
_DAILY_RANKING_TASK10_CONSUMERS = frozenset(
    {
        "atr_expansion_forward_response",
        "ranking_liquidity_price_action_recomposition",
        "ranking_moving_average_replacement_evidence",
        "ranking_sma5_atr_deviation_evidence",
        "ranking_sma5_below_streak_evidence",
        "ranking_sma5_count_long_evidence",
        "ranking_sma5_count_short_evidence",
        "ranking_sma5_deviation_evidence",
        "ranking_sma5_position_state_evidence",
    }
)
_DAILY_RANKING_PRIVATE_EDGE_COUNT = 292
_DAILY_RANKING_PRIVATE_EDGE_FILE_COUNT = 33
_DAILY_RANKING_PRIVATE_OWNER_SYMBOL_COUNT = 73
_DAILY_RANKING_PRIVATE_EDGE_TASK_COUNTS = {
    "task_8": 74,
    "task_9": 80,
    "task_10": 124,
    "expanded_30_consumer_plan": 14,
}
_DAILY_RANKING_PRIVATE_EDGE_SHA256 = (
    "6a44ebbd0d9df87fc0ceb69ca1c86a2345e3fccd30e015bc14af7791f7b2837e"
)


def _daily_ranking_private_edge_owner(importer: str) -> str:
    if importer in _DAILY_RANKING_TASK8_CONSUMERS:
        return "task_8"
    if importer in _DAILY_RANKING_TASK9_CONSUMERS:
        return "task_9"
    if importer in _DAILY_RANKING_TASK10_CONSUMERS:
        return "task_10"
    return "expanded_30_consumer_plan"


def _is_experiment_module(py_file: Path) -> bool:
    tree = ast.parse(py_file.read_text(encoding="utf-8"), filename=str(py_file))
    for node in tree.body:
        targets: list[ast.expr] = []
        if isinstance(node, ast.Assign):
            targets.extend(node.targets)
        elif isinstance(node, ast.AnnAssign):
            targets.append(node.target)
        if any(
            isinstance(target, ast.Name) and target.id.endswith("_EXPERIMENT_ID")
            for target in targets
        ):
            return True
    return False


@dataclass(frozen=True, slots=True)
class _PrivateEdgeBinding:
    kind: str
    module: str
    symbol: str
    origin: str
    imported_modules: frozenset[str] = frozenset()


def _scan_daily_ranking_private_edges(
    tree: ast.AST,
    *,
    importer_module: str,
    experiment_modules: set[str],
) -> tuple[tuple[str, str, str, tuple[str, ...]], ...]:
    """Resolve private experiment bindings, aliases, and dynamic module imports."""

    bindings: list[dict[str, _PrivateEdgeBinding]] = [{}]
    calls: dict[tuple[str, str, str], list[str]] = {}

    def resolve_import(node: ast.ImportFrom) -> str | None:
        if node.level == 0:
            return node.module
        package = importer_module.rpartition(".")[0]
        relative = "." * node.level + (node.module or "")
        try:
            return importlib.util.resolve_name(relative, package)
        except (ImportError, ValueError):
            return None

    class BindingVisitor(ast.NodeVisitor):
        def __init__(self) -> None:
            self.scope: list[str] = []

        @property
        def environment(self) -> dict[str, _PrivateEdgeBinding]:
            return bindings[-1]

        def record(
            self,
            binding: _PrivateEdgeBinding,
            *,
            called: bool = False,
        ) -> None:
            if binding.kind != "symbol" or binding.module not in experiment_modules:
                return
            key = (binding.module, binding.symbol, binding.origin)
            scopes = calls.setdefault(key, [])
            if called:
                scopes.append(".".join(self.scope) or "<module>")

        def resolve(self, node: ast.expr) -> _PrivateEdgeBinding | None:
            if isinstance(node, ast.Name):
                return self.environment.get(node.id)
            if isinstance(node, ast.Attribute):
                owner = self.resolve(node.value)
                if owner is None:
                    return None
                if owner.kind == "module_prefix":
                    if (
                        owner.module in owner.imported_modules
                        and owner.module in experiment_modules
                        and node.attr.startswith("_")
                    ):
                        return _PrivateEdgeBinding(
                            "symbol",
                            owner.module,
                            node.attr,
                            f"{owner.origin}.{node.attr}",
                        )
                    candidate = f"{owner.module}.{node.attr}"
                    matching_modules = frozenset(
                        imported_module
                        for imported_module in owner.imported_modules
                        if imported_module == candidate
                        or imported_module.startswith(f"{candidate}.")
                    )
                    if matching_modules:
                        return _PrivateEdgeBinding(
                            "module_prefix",
                            candidate,
                            "",
                            f"{owner.origin}.{node.attr}",
                            matching_modules,
                        )
                    return None
                if owner.kind != "module":
                    return None
                module = owner.module
                if module in experiment_modules and node.attr.startswith("_"):
                    return _PrivateEdgeBinding(
                        "symbol",
                        module,
                        node.attr,
                        f"{owner.origin}.{node.attr}",
                    )
                candidate = f"{module}.{node.attr}"
                if candidate in experiment_modules:
                    return _PrivateEdgeBinding(
                        "module", candidate, "", f"{owner.origin}.{node.attr}"
                    )
                if module == "importlib" and node.attr == "import_module":
                    return _PrivateEdgeBinding(
                        "importer", "importlib", "import_module", owner.origin
                    )
                return None
            if isinstance(node, ast.Call):
                function = self.resolve(node.func)
                is_importer = function is not None and function.kind == "importer"
                is_dunder_import = (
                    isinstance(node.func, ast.Name) and node.func.id == "__import__"
                )
                if (is_importer or is_dunder_import) and node.args:
                    argument = node.args[0]
                    if isinstance(argument, ast.Constant) and isinstance(argument.value, str):
                        return _PrivateEdgeBinding(
                            "module", argument.value, "", argument.value
                        )
                if (
                    isinstance(node.func, ast.Name)
                    and node.func.id == "getattr"
                    and len(node.args) >= 2
                    and isinstance(node.args[1], ast.Constant)
                    and isinstance(node.args[1].value, str)
                ):
                    owner = self.resolve(node.args[0])
                    symbol = node.args[1].value
                    if (
                        owner is not None
                        and owner.kind in {"module", "module_prefix"}
                        and owner.module in experiment_modules
                        and symbol.startswith("_")
                    ):
                        return _PrivateEdgeBinding(
                            "symbol",
                            owner.module,
                            symbol,
                            f"{owner.origin}.{symbol}",
                        )
            return None

        @staticmethod
        def dotted_target(node: ast.Attribute) -> tuple[str, str] | None:
            parts = [node.attr]
            owner = node.value
            while isinstance(owner, ast.Attribute):
                parts.append(owner.attr)
                owner = owner.value
            if not isinstance(owner, ast.Name):
                return None
            return owner.id, ".".join((owner.id, *reversed(parts)))

        def bind_target(
            self,
            target: ast.expr,
            binding: _PrivateEdgeBinding | None,
        ) -> None:
            if isinstance(target, ast.Attribute):
                dotted_target = self.dotted_target(target)
                if dotted_target is None:
                    return
                root, rebound_path = dotted_target
                root_binding = self.environment.get(root)
                if (
                    root_binding is not None
                    and root_binding.kind == "module_prefix"
                ):
                    retained_modules = frozenset(
                        imported_module
                        for imported_module in root_binding.imported_modules
                        if imported_module != rebound_path
                        and not imported_module.startswith(f"{rebound_path}.")
                    )
                    if retained_modules:
                        self.environment[root] = _PrivateEdgeBinding(
                            root_binding.kind,
                            root_binding.module,
                            root_binding.symbol,
                            root_binding.origin,
                            retained_modules,
                        )
                    else:
                        self.environment.pop(root, None)
                return
            if not isinstance(target, ast.Name):
                return
            if binding is None:
                self.environment.pop(target.id, None)
                return
            rebound = _PrivateEdgeBinding(
                binding.kind,
                binding.module,
                binding.symbol,
                target.id,
                binding.imported_modules,
            )
            self.environment[target.id] = rebound
            self.record(rebound)

        def visit_Import(self, node: ast.Import) -> None:
            for alias in node.names:
                local_name = alias.asname or alias.name.split(".")[0]
                existing = self.environment.get(local_name)
                if alias.asname:
                    self.environment[local_name] = _PrivateEdgeBinding(
                        "module", alias.name, "", local_name
                    )
                elif "." in alias.name or (
                    existing is not None and existing.kind == "module_prefix"
                ):
                    imported_modules = {alias.name}
                    if existing is not None:
                        if existing.kind == "module_prefix":
                            imported_modules.update(existing.imported_modules)
                        elif existing.kind == "module" and existing.module == local_name:
                            imported_modules.add(existing.module)
                    self.environment[local_name] = _PrivateEdgeBinding(
                        "module_prefix",
                        local_name,
                        "",
                        local_name,
                        frozenset(imported_modules),
                    )
                else:
                    self.environment[local_name] = _PrivateEdgeBinding(
                        "module", alias.name, "", local_name
                    )

        def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
            module = resolve_import(node)
            if module is None:
                return
            for alias in node.names:
                local_name = alias.asname or alias.name
                candidate = f"{module}.{alias.name}"
                if candidate in experiment_modules:
                    binding = _PrivateEdgeBinding(
                        "module", candidate, "", local_name
                    )
                elif module == "importlib" and alias.name == "import_module":
                    binding = _PrivateEdgeBinding(
                        "importer", module, alias.name, local_name
                    )
                else:
                    binding = _PrivateEdgeBinding(
                        "symbol", module, alias.name, local_name
                    )
                self.environment[local_name] = binding
                if alias.name.startswith("_"):
                    self.record(binding)

        def visit_function(
            self,
            node: ast.FunctionDef | ast.AsyncFunctionDef,
        ) -> None:
            for expression in (*node.decorator_list, *node.args.defaults, *node.args.kw_defaults):
                if expression is not None:
                    self.visit(expression)
            self.scope.append(node.name)
            child = dict(self.environment)
            child.pop(node.name, None)
            for argument in (
                *node.args.posonlyargs,
                *node.args.args,
                *node.args.kwonlyargs,
            ):
                child.pop(argument.arg, None)
            if node.args.vararg is not None:
                child.pop(node.args.vararg.arg, None)
            if node.args.kwarg is not None:
                child.pop(node.args.kwarg.arg, None)
            bindings.append(child)
            for statement in node.body:
                self.visit(statement)
            bindings.pop()
            self.scope.pop()
            self.environment.pop(node.name, None)

        def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
            self.visit_function(node)

        def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
            self.visit_function(node)

        def visit_ClassDef(self, node: ast.ClassDef) -> None:
            self.scope.append(node.name)
            bindings.append(dict(self.environment))
            for statement in node.body:
                self.visit(statement)
            bindings.pop()
            self.scope.pop()
            self.environment.pop(node.name, None)

        def visit_Assign(self, node: ast.Assign) -> None:
            self.visit(node.value)
            binding = self.resolve(node.value)
            for target in node.targets:
                self.bind_target(target, binding)

        def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
            if node.value is not None:
                self.visit(node.value)
                self.bind_target(node.target, self.resolve(node.value))

        def visit_Call(self, node: ast.Call) -> None:
            binding = self.resolve(node.func)
            if binding is not None:
                self.record(binding, called=True)
            for argument in (*node.args, *(keyword.value for keyword in node.keywords)):
                self.visit(argument)

        def visit_Attribute(self, node: ast.Attribute) -> None:
            binding = self.resolve(node)
            if binding is not None:
                self.record(binding)
            self.visit(node.value)

    BindingVisitor().visit(tree)
    return tuple(
        sorted(
            (module, symbol, origin, tuple(sorted(scopes)))
            for (module, symbol, origin), scopes in calls.items()
        )
    )


def _daily_ranking_private_edge_inventory() -> tuple[tuple[object, ...], ...]:
    analytics_root = SRC_ROOT / "domains" / "analytics"
    experiment_modules = {
        f"src.domains.analytics.{path.stem}"
        for path in analytics_root.glob("*.py")
        if _is_experiment_module(path)
    }
    importer_paths = {
        *analytics_root.glob("ranking_*.py"),
        analytics_root / "atr_expansion_forward_response.py",
        analytics_root / "market_bubble_footprint.py",
        analytics_root / "market_bubble_footprint_monitor.py",
    }
    inventory: list[tuple[object, ...]] = []
    for path in sorted(importer_paths):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        bindings = _scan_daily_ranking_private_edges(
            tree,
            importer_module=f"src.domains.analytics.{path.stem}",
            experiment_modules=experiment_modules,
        )
        inventory.extend(
            (
                path.name,
                module,
                symbol,
                local_name,
                _daily_ranking_private_edge_owner(path.stem),
                call_scopes,
            )
            for module, symbol, local_name, call_scopes in bindings
            if module != f"src.domains.analytics.{path.stem}"
        )
    return tuple(sorted(inventory))


_SYNTHETIC_PRIVATE_OWNER = "src.domains.analytics.synthetic_private_owner"
_SYNTHETIC_PRIVATE_OWNER_A = "src.domains.analytics.owner_a"
_SYNTHETIC_PRIVATE_OWNER_B = "src.domains.analytics.owner_b"


@pytest.mark.parametrize(
    ("source", "expected_origin", "expected_scope"),
    (
        (
            "from src.domains.analytics.synthetic_private_owner import _private\n"
            "def run():\n    _private()\n",
            "_private",
            "run",
        ),
        (
            "from .synthetic_private_owner import _private as relative_private\n"
            "relative_private()\n",
            "relative_private",
            "<module>",
        ),
        (
            "import src.domains.analytics.synthetic_private_owner as owner\n"
            "def run():\n    owner._private()\n",
            "owner._private",
            "run",
        ),
        (
            "import src.domains.analytics.synthetic_private_owner\n"
            "def run():\n"
            "    src.domains.analytics.synthetic_private_owner._private()\n",
            "src.domains.analytics.synthetic_private_owner._private",
            "run",
        ),
        (
            "import importlib as loader\n"
            "owner = loader.import_module("
            "'src.domains.analytics.synthetic_private_owner')\n"
            "owner._private()\n",
            "owner._private",
            "<module>",
        ),
        (
            "from importlib import import_module\n"
            "owner = import_module("
            "'src.domains.analytics.synthetic_private_owner')\n"
            "owner._private()\n",
            "owner._private",
            "<module>",
        ),
        (
            "owner = __import__("
            "'src.domains.analytics.synthetic_private_owner')\n"
            "owner._private()\n",
            "owner._private",
            "<module>",
        ),
        (
            "from src.domains.analytics.synthetic_private_owner import "
            "_private as imported_private\n"
            "reexported_private = imported_private\n"
            "def outer():\n"
            "    def inner():\n"
            "        reexported_private()\n"
            "    inner()\n",
            "reexported_private",
            "outer.inner",
        ),
    ),
)
def test_daily_ranking_private_edge_scanner_rejects_indirect_import_forms(
    source: str,
    expected_origin: str,
    expected_scope: str,
) -> None:
    edges = _scan_daily_ranking_private_edges(
        ast.parse(source),
        importer_module="src.domains.analytics.synthetic_consumer",
        experiment_modules={_SYNTHETIC_PRIVATE_OWNER},
    )

    assert any(
        module == _SYNTHETIC_PRIVATE_OWNER
        and symbol == "_private"
        and origin == expected_origin
        and expected_scope in scopes
        for module, symbol, origin, scopes in edges
    ), edges


def test_daily_ranking_private_edge_scanner_honors_rebinding_and_shadowing() -> None:
    edges = _scan_daily_ranking_private_edges(
        ast.parse(
            "from src.domains.analytics.synthetic_private_owner import _private\n"
            "_private = lambda: None\n"
            "_private()\n"
            "def run(_private):\n"
            "    _private()\n"
        ),
        importer_module="src.domains.analytics.synthetic_consumer",
        experiment_modules={_SYNTHETIC_PRIVATE_OWNER},
    )

    assert edges == ((_SYNTHETIC_PRIVATE_OWNER, "_private", "_private", ()),)


def test_daily_ranking_private_edge_scanner_rebinding_changes_call_scope_digest() -> None:
    before_rebind = _scan_daily_ranking_private_edges(
        ast.parse(
            "from src.domains.analytics.synthetic_private_owner import _private\n"
            "def before():\n"
            "    _private()\n"
        ),
        importer_module="src.domains.analytics.synthetic_consumer",
        experiment_modules={_SYNTHETIC_PRIVATE_OWNER},
    )
    after_rebind = _scan_daily_ranking_private_edges(
        ast.parse(
            "from src.domains.analytics.synthetic_private_owner import _private\n"
            "_private = lambda: None\n"
            "def after():\n"
            "    _private()\n"
        ),
        importer_module="src.domains.analytics.synthetic_consumer",
        experiment_modules={_SYNTHETIC_PRIVATE_OWNER},
    )

    assert before_rebind == (
        (_SYNTHETIC_PRIVATE_OWNER, "_private", "_private", ("before",)),
    )
    assert after_rebind == (
        (_SYNTHETIC_PRIVATE_OWNER, "_private", "_private", ()),
    )
    assert hashlib.sha256(repr(before_rebind).encode()).hexdigest() != hashlib.sha256(
        repr(after_rebind).encode()
    ).hexdigest()


def test_daily_ranking_private_edge_scanner_module_rebind_stops_later_scopes() -> None:
    edges = _scan_daily_ranking_private_edges(
        ast.parse(
            "import src.domains.analytics.synthetic_private_owner as owner\n"
            "def before():\n"
            "    owner._private()\n"
            "owner = object()\n"
            "def after():\n"
            "    owner._private()\n"
        ),
        importer_module="src.domains.analytics.synthetic_consumer",
        experiment_modules={_SYNTHETIC_PRIVATE_OWNER},
    )

    assert edges == (
        (_SYNTHETIC_PRIVATE_OWNER, "_private", "owner._private", ("before",)),
    )


@pytest.mark.parametrize(
    "source",
    (
        (
            "import src.domains.analytics.owner_a\n"
            "import src.domains.analytics.owner_b\n"
            "src.domains.analytics.owner_a._private()\n"
        ),
        (
            "import src.domains.analytics.owner_b\n"
            "import src.domains.analytics.owner_a\n"
            "src.domains.analytics.owner_a._private()\n"
        ),
    ),
)
def test_daily_ranking_private_edge_scanner_retains_shared_root_imports(
    source: str,
) -> None:
    edges = _scan_daily_ranking_private_edges(
        ast.parse(source),
        importer_module="src.domains.analytics.synthetic_consumer",
        experiment_modules={
            _SYNTHETIC_PRIVATE_OWNER_A,
            _SYNTHETIC_PRIVATE_OWNER_B,
        },
    )

    assert edges == (
        (
            _SYNTHETIC_PRIVATE_OWNER_A,
            "_private",
            "src.domains.analytics.owner_a._private",
            ("<module>",),
        ),
    )


def test_daily_ranking_private_edge_scanner_resolves_both_shared_root_imports() -> None:
    edges = _scan_daily_ranking_private_edges(
        ast.parse(
            "import src.domains.analytics.owner_a\n"
            "import src.domains.analytics.owner_b\n"
            "src.domains.analytics.owner_a._private()\n"
            "src.domains.analytics.owner_b._private()\n"
        ),
        importer_module="src.domains.analytics.synthetic_consumer",
        experiment_modules={
            _SYNTHETIC_PRIVATE_OWNER_A,
            _SYNTHETIC_PRIVATE_OWNER_B,
        },
    )

    assert edges == (
        (
            _SYNTHETIC_PRIVATE_OWNER_A,
            "_private",
            "src.domains.analytics.owner_a._private",
            ("<module>",),
        ),
        (
            _SYNTHETIC_PRIVATE_OWNER_B,
            "_private",
            "src.domains.analytics.owner_b._private",
            ("<module>",),
        ),
    )


@pytest.mark.parametrize(
    "rebind",
    (
        "src = object()",
        "src.domains = object()",
        "src.domains.analytics = object()",
        "src.domains.analytics.owner_b = object()",
    ),
)
def test_daily_ranking_private_edge_scanner_shared_root_rebind_discards_stale_edges(
    rebind: str,
) -> None:
    edges = _scan_daily_ranking_private_edges(
        ast.parse(
            "import src.domains.analytics.owner_a\n"
            "import src.domains.analytics.owner_b\n"
            f"{rebind}\n"
            "src.domains.analytics.owner_b._private()\n"
        ),
        importer_module="src.domains.analytics.synthetic_consumer",
        experiment_modules={
            _SYNTHETIC_PRIVATE_OWNER_A,
            _SYNTHETIC_PRIVATE_OWNER_B,
        },
    )

    assert edges == ()


def test_daily_ranking_private_edge_scanner_subpath_rebind_preserves_sibling() -> None:
    edges = _scan_daily_ranking_private_edges(
        ast.parse(
            "import src.domains.analytics.owner_a\n"
            "import src.domains.analytics.owner_b\n"
            "src.domains.analytics.owner_a = object()\n"
            "src.domains.analytics.owner_a._private()\n"
            "src.domains.analytics.owner_b._private()\n"
        ),
        importer_module="src.domains.analytics.synthetic_consumer",
        experiment_modules={
            _SYNTHETIC_PRIVATE_OWNER_A,
            _SYNTHETIC_PRIVATE_OWNER_B,
        },
    )

    assert edges == (
        (
            _SYNTHETIC_PRIVATE_OWNER_B,
            "_private",
            "src.domains.analytics.owner_b._private",
            ("<module>",),
        ),
    )


def test_daily_ranking_private_edge_inventory_cannot_grow_or_change() -> None:
    inventory = _daily_ranking_private_edge_inventory()
    payload = json.dumps(inventory, ensure_ascii=True, separators=(",", ":"))
    digest = hashlib.sha256(payload.encode()).hexdigest()
    task_counts = {
        owner: sum(1 for edge in inventory if edge[4] == owner)
        for owner in _DAILY_RANKING_PRIVATE_EDGE_TASK_COUNTS
    }

    assert len(inventory) == _DAILY_RANKING_PRIVATE_EDGE_COUNT, inventory
    assert len({edge[0] for edge in inventory}) == _DAILY_RANKING_PRIVATE_EDGE_FILE_COUNT
    assert (
        len({(edge[1], edge[2]) for edge in inventory})
        == _DAILY_RANKING_PRIVATE_OWNER_SYMBOL_COUNT
    )
    assert task_counts == _DAILY_RANKING_PRIVATE_EDGE_TASK_COUNTS
    assert digest == _DAILY_RANKING_PRIVATE_EDGE_SHA256, "\n".join(map(str, inventory))


@pytest.mark.parametrize(
    ("source_owner", "builder"),
    (
        ("atr_expansion_forward_response.py", "build_atr_features"),
        ("ranking_sector_strength_evidence.py", "build_sector_strength_features"),
        (
            "ranking_long_sector_leadership_horizon_decomposition.py",
            "build_long_leadership_features",
        ),
        ("ranking_psr_valuation_evidence.py", "build_psr_features"),
        ("ranking_roe_quality_evidence.py", "build_roe_features"),
        (
            "ranking_long_scaffold_value_composite_evidence.py",
            "build_long_scaffold_features",
        ),
        ("ranking_short_red_evidence.py", "build_short_scaffold_features"),
        ("ranking_moving_average_replacement_evidence.py", "build_sma_features"),
        ("ranking_sma5_atr_deviation_evidence.py", "build_sma_features"),
        ("ranking_sma5_below_streak_evidence.py", "build_sma_features"),
        ("ranking_sma5_count_long_evidence.py", "build_sma_features"),
        ("ranking_sma5_count_short_evidence.py", "build_sma_features"),
        ("ranking_sma5_deviation_evidence.py", "build_sma_features"),
        ("ranking_sma5_position_state_evidence.py", "build_sma_features"),
    ),
)
def test_daily_ranking_source_owners_publish_the_public_builder(
    source_owner: str,
    builder: str,
) -> None:
    path = SRC_ROOT / "domains" / "analytics" / source_owner
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    imports = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        and node.module
        == "src.domains.analytics.daily_ranking_feature_builders"
        for alias in node.names
    }
    assert builder in imports
