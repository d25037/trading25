from __future__ import annotations

import ast
from pathlib import Path


SRC_ROOT = Path(__file__).parents[3] / "src"
MARKET_ROOT = SRC_ROOT / "infrastructure" / "db" / "market"
FORBIDDEN_WRITER_NAMES = {
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
ALLOWED_DATASET_WRITER_CALLS = {
    Path("infrastructure/db/dataset_io/dataset_writer.py"): {
        "upsert_stock_data",
        "upsert_topix_data",
        "upsert_indices_data",
        "upsert_margin_data",
        "upsert_statements",
    }
}


def _tree(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _class_methods(path: Path, class_name: str) -> set[str]:
    class_node = next(
        node
        for node in _tree(path).body
        if isinstance(node, ast.ClassDef) and node.name == class_name
    )
    return {
        node.name
        for node in class_node.body
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
    }


def _module_functions(path: Path) -> set[str]:
    return {
        node.name
        for node in _tree(path).body
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
    }


def _imported_names(path: Path, module: str) -> set[str]:
    return {
        alias.name
        for node in _tree(path).body
        if isinstance(node, ast.ImportFrom) and node.module == module
        for alias in node.names
    }


def _called_names(path: Path) -> set[str]:
    return {
        node.func.id
        for node in ast.walk(_tree(path))
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }


def test_legacy_market_writer_apis_are_deleted() -> None:
    market_db_methods = _class_methods(MARKET_ROOT / "market_db.py", "MarketDb")
    assert market_db_methods.isdisjoint(FORBIDDEN_WRITER_NAMES)
    assert not (MARKET_ROOT / "time_series_writers.py").exists()
    assert _module_functions(MARKET_ROOT / "valuation_writers.py").isdisjoint(
        STANDALONE_ADJUSTED_WRITERS
    )


def test_production_imports_no_deleted_writer_module_or_symbols() -> None:
    violations: list[str] = []
    for path in SRC_ROOT.rglob("*.py"):
        relative = path.relative_to(SRC_ROOT)
        for node in ast.walk(_tree(path)):
            if isinstance(node, ast.Import) and any(
                alias.name.endswith(".time_series_writers") for alias in node.names
            ):
                violations.append(f"{relative}:{node.lineno}")
            if isinstance(node, ast.ImportFrom):
                imported = {alias.name for alias in node.names}
                if (node.module or "").endswith("time_series_writers") or (
                    "time_series_writers" in imported
                    and (node.module or "").endswith(".market")
                ):
                    violations.append(f"{relative}:{node.lineno}")
                if (node.module or "").endswith("valuation_writers") and (
                    imported & STANDALONE_ADJUSTED_WRITERS
                ):
                    violations.append(f"{relative}:{node.lineno}")

    assert violations == []


def test_forbidden_writer_calls_are_limited_to_explicit_dataset_whitelist() -> None:
    violations: list[str] = []
    observed_allowed: dict[Path, set[str]] = {}
    for path in SRC_ROOT.rglob("*.py"):
        relative = path.relative_to(SRC_ROOT)
        allowed = ALLOWED_DATASET_WRITER_CALLS.get(relative, set())
        for node in ast.walk(_tree(path)):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                continue
            if node.func.attr not in FORBIDDEN_WRITER_NAMES:
                continue
            if node.func.attr in allowed:
                observed_allowed.setdefault(relative, set()).add(node.func.attr)
            else:
                violations.append(f"{relative}:{node.lineno}:{node.func.attr}")

    assert violations == []
    assert observed_allowed == ALLOWED_DATASET_WRITER_CALLS


def test_dataset_io_never_imports_market_db() -> None:
    violations: list[str] = []
    dataset_root = SRC_ROOT / "infrastructure" / "db" / "dataset_io"
    for path in dataset_root.rglob("*.py"):
        for node in ast.walk(_tree(path)):
            if isinstance(node, ast.Import) and any(
                alias.name.endswith(".market.market_db") for alias in node.names
            ):
                violations.append(f"{path.relative_to(SRC_ROOT)}:{node.lineno}")
            if isinstance(node, ast.ImportFrom) and (
                node.module or ""
            ).endswith("market_db"):
                violations.append(f"{path.relative_to(SRC_ROOT)}:{node.lineno}")
            if (
                isinstance(node, ast.ImportFrom)
                and "market_db" in {alias.name for alias in node.names}
                and (node.module or "").endswith(".market")
            ):
                violations.append(f"{path.relative_to(SRC_ROOT)}:{node.lineno}")

    assert violations == []


def test_canonical_market_writer_paths_exist() -> None:
    store_methods = _class_methods(
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

    market_db_methods = _class_methods(MARKET_ROOT / "market_db.py", "MarketDb")
    assert {
        "upsert_stock_master_daily",
        "upsert_stock_master_daily_rows",
        "publish_stock_adjustment_lineages",
        "publish_adjusted_basis_materialization",
    } <= market_db_methods


def test_time_series_store_has_one_semantic_delta_writer_kernel() -> None:
    source = (MARKET_ROOT / "time_series_store.py").read_text()

    assert "def _apply_semantic_delta(" in source
    assert "_publish_rows_with_upsert_spec" not in source
    assert "_publish_rows_via_relation" not in source
    assert "_build_executemany_upsert_sql" not in source
    assert "dirty_all or not dirty_dates" not in source
    assert "_dirty_stock_minute_dates or" not in source
    assert "DELETE FROM stock_data WHERE code" not in source


def test_time_series_publish_contracts_do_not_return_legacy_int_counts() -> None:
    tree = _tree(MARKET_ROOT / "time_series_store.py")
    names = {
        "publish_topix_data",
        "publish_stock_data",
        "publish_stock_minute_data",
        "publish_indices_data",
        "publish_options_225_data",
        "publish_margin_data",
        "publish_statements",
        "stage_stock_data_rows",
        "flush_staged_stock_data",
    }
    violations = [
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name in names
        and isinstance(node.returns, ast.Name)
        and node.returns.id == "int"
    ]

    assert violations == []


def test_atomic_writers_import_and_use_focused_lineage_validation() -> None:
    module = "src.infrastructure.db.market.adjustment_basis_validation"
    required = {"validate_lineages", "validate_final_catalog"}
    for filename in ("adjustment_basis_writers.py", "valuation_writers.py"):
        path = MARKET_ROOT / filename
        assert required <= _imported_names(path, module)
        assert required <= _called_names(path)
