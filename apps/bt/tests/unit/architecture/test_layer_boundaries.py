"""Architecture boundary checks for the 5-layer src layout."""

from __future__ import annotations

import ast
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
        "infrastructure/db/market/adjustment_basis_writers.py",
        "src.domains.fundamentals.adjustment_basis",
    ),
    (
        "infrastructure/db/market/dataset_snapshot_reader.py",
        "src.domains.fundamentals.adjustment_basis",
    ),
    (
        "infrastructure/db/market/market_db.py",
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


def _application_http_schema_baseline() -> set[str]:
    return {
        line.strip()
        for line in APPLICATION_HTTP_SCHEMA_BASELINE.read_text().splitlines()
        if line.strip() and not line.startswith("#")
    }


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


def test_application_http_schema_dependency_baseline_has_19_entries() -> None:
    assert len(_application_http_schema_baseline()) == 19


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
