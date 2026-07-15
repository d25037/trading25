from __future__ import annotations

import ast
from pathlib import Path


SRC_ROOT = Path(__file__).parents[3] / "src"
MARKET_ROOT = SRC_ROOT / "infrastructure" / "db" / "market"
LEGACY_MARKET_DB_WRITERS = {
    "upsert_stock_data",
    "upsert_stock_minute_data",
    "upsert_topix_data",
    "upsert_indices_data",
    "upsert_options_225_data",
    "upsert_margin_data",
    "upsert_statements",
    "upsert_statement_metrics_adjusted",
    "upsert_daily_valuation",
    "upsert_daily_valuation_from_adjusted_metrics",
}
STANDALONE_ADJUSTED_WRITERS = {
    "upsert_statement_metrics_adjusted",
    "upsert_daily_valuation",
    "upsert_daily_valuation_from_adjusted_metrics",
}


def _annotation_names(annotation: ast.expr | None) -> set[str]:
    if annotation is None:
        return set()
    return {
        node.id
        for node in ast.walk(annotation)
        if isinstance(node, ast.Name)
    }


def _legacy_market_db_calls(source: str, *, path: Path) -> list[str]:
    tree = ast.parse(source, filename=str(path))
    market_db_aliases = {
        alias.asname or alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        and node.module == "src.infrastructure.db.market.market_db"
        for alias in node.names
        if alias.name == "MarketDb"
    }
    bound_names: set[str] = set()
    bound_self_attributes: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.arg) and "MarketDb" in _annotation_names(node.annotation):
            bound_names.add(node.arg)
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            if "MarketDb" in _annotation_names(node.annotation):
                bound_names.add(node.target.id)

    changed = True
    while changed:
        changed = False
        for node in ast.walk(tree):
            if not isinstance(node, ast.Assign):
                continue
            value_is_market_db = (
                isinstance(node.value, ast.Call)
                and isinstance(node.value.func, ast.Name)
                and node.value.func.id in market_db_aliases
            ) or (isinstance(node.value, ast.Name) and node.value.id in bound_names)
            if not value_is_market_db:
                continue
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id not in bound_names:
                    bound_names.add(target.id)
                    changed = True
                if (
                    isinstance(target, ast.Attribute)
                    and isinstance(target.value, ast.Name)
                    and target.value.id == "self"
                    and target.attr not in bound_self_attributes
                ):
                    bound_self_attributes.add(target.attr)
                    changed = True

    callers: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        if node.func.attr not in LEGACY_MARKET_DB_WRITERS:
            continue
        receiver = node.func.value
        receiver_is_market_db = (
            isinstance(receiver, ast.Name) and receiver.id in bound_names
        ) or (
            isinstance(receiver, ast.Attribute)
            and isinstance(receiver.value, ast.Name)
            and receiver.value.id == "self"
            and receiver.attr in bound_self_attributes
        )
        if receiver_is_market_db:
            callers.append(f"{path}:{node.lineno}:{node.func.attr}")
    return sorted(callers)


def _module_level_function_names(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    return {
        node.name
        for node in tree.body
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
    }


def _class_method_names(path: Path, class_name: str) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    class_node = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == class_name
    )
    return {
        node.name
        for node in class_node.body
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
    }


def _imported_names(path: Path, module: str) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    return {
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom) and node.module == module
        for alias in node.names
    }


def _called_names(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    return {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }


def test_market_db_call_guard_ignores_unrelated_same_named_methods() -> None:
    source = """
class DatasetWriter:
    def upsert_stock_data(self, rows):
        return len(rows)

writer = DatasetWriter()
writer.upsert_stock_data([])
"""

    assert _legacy_market_db_calls(source, path=Path("infrastructure/db/dataset_io/x.py")) == []


def test_market_db_call_guard_detects_bound_market_db_in_any_path() -> None:
    source = """
from src.infrastructure.db.market.market_db import MarketDb

db = MarketDb("market.duckdb")
db.upsert_stock_data([])
"""

    assert _legacy_market_db_calls(
        source,
        path=Path("infrastructure/db/dataset_io/x.py"),
    ) == ["infrastructure/db/dataset_io/x.py:5:upsert_stock_data"]


def test_market_db_exposes_no_legacy_time_series_or_standalone_adjusted_upserts() -> (
    None
):
    market_db_methods = _class_method_names(MARKET_ROOT / "market_db.py", "MarketDb")

    assert market_db_methods.isdisjoint(LEGACY_MARKET_DB_WRITERS)
    assert not (MARKET_ROOT / "time_series_writers.py").exists()
    assert _module_level_function_names(
        MARKET_ROOT / "valuation_writers.py"
    ).isdisjoint(STANDALONE_ADJUSTED_WRITERS)


def test_production_has_no_legacy_market_db_writer_callers() -> None:
    callers: list[str] = []
    for path in SRC_ROOT.rglob("*.py"):
        callers.extend(
            _legacy_market_db_calls(
                path.read_text(encoding="utf-8"),
                path=path.relative_to(SRC_ROOT),
            )
        )

    assert callers == []


def test_production_has_no_legacy_writer_module_or_symbol_imports() -> None:
    violations: list[str] = []
    for path in SRC_ROOT.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                if any(alias.name.endswith(".time_series_writers") for alias in node.names):
                    violations.append(f"{path.relative_to(SRC_ROOT)}:{node.lineno}")
            if isinstance(node, ast.ImportFrom):
                if (node.module or "").endswith(".time_series_writers"):
                    violations.append(f"{path.relative_to(SRC_ROOT)}:{node.lineno}")
                if node.module == "src.infrastructure.db.market.valuation_writers":
                    imported = {alias.name for alias in node.names}
                    if imported & STANDALONE_ADJUSTED_WRITERS:
                        violations.append(f"{path.relative_to(SRC_ROOT)}:{node.lineno}")

    assert violations == []


def test_canonical_market_writer_boundaries_are_explicit() -> None:
    store_methods = _class_method_names(
        MARKET_ROOT / "time_series_store.py",
        "DuckDbParquetTimeSeriesStore",
    )
    assert {
        "publish_stock_data",
        "publish_stock_minute_data",
        "publish_topix_data",
        "publish_indices_data",
        "publish_options_225_data",
        "publish_margin_data",
        "publish_statements",
    } <= store_methods

    market_db_methods = _class_method_names(MARKET_ROOT / "market_db.py", "MarketDb")
    assert {
        "upsert_stock_master_daily",
        "upsert_stock_master_daily_rows",
        "publish_stock_adjustment_lineages",
        "publish_adjusted_basis_materialization",
    } <= market_db_methods


def test_atomic_writers_import_and_use_focused_lineage_validation() -> None:
    validation_module = (
        "src.infrastructure.db.market.adjustment_basis_validation"
    )
    required = {"validate_lineages", "validate_final_catalog"}
    for filename in ("adjustment_basis_writers.py", "valuation_writers.py"):
        path = MARKET_ROOT / filename
        assert required <= _imported_names(path, validation_module)
        assert required <= _called_names(path)
