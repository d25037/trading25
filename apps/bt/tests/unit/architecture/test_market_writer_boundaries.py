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


class _BindingScope:
    def __init__(
        self,
        parent: _BindingScope | None,
        *,
        class_attributes: set[str] | None = None,
    ) -> None:
        self.parent = parent
        self.type_aliases: set[str] = set()
        self.bindings: dict[str, bool] = {}
        self.class_attributes = class_attributes


class _MarketDbLegacyCallVisitor(ast.NodeVisitor):
    def __init__(self, path: Path) -> None:
        self.path = path
        self.scope = _BindingScope(None)
        self.callers: list[str] = []

    def _push_scope(
        self, *, class_attributes: set[str] | None = None
    ) -> _BindingScope:
        previous = self.scope
        self.scope = _BindingScope(previous, class_attributes=class_attributes)
        return previous

    def _is_market_type_alias(self, name: str) -> bool:
        scope: _BindingScope | None = self.scope
        while scope is not None:
            if name in scope.type_aliases:
                return True
            scope = scope.parent
        return False

    def _annotation_is_market_db(self, annotation: ast.expr | None) -> bool:
        if annotation is None:
            return False
        return any(
            isinstance(node, ast.Name) and self._is_market_type_alias(node.id)
            for node in ast.walk(annotation)
        )

    def _name_binding_is_market_db(self, name: str) -> bool:
        scope: _BindingScope | None = self.scope
        while scope is not None:
            if name in scope.bindings:
                return scope.bindings[name]
            scope = scope.parent
        return False

    def _class_attributes(self) -> set[str] | None:
        scope: _BindingScope | None = self.scope
        while scope is not None:
            if scope.class_attributes is not None:
                return scope.class_attributes
            scope = scope.parent
        return None

    def _expression_is_market_db(self, expression: ast.expr | None) -> bool:
        if isinstance(expression, ast.Name):
            return self._name_binding_is_market_db(expression.id)
        if isinstance(expression, ast.Call) and isinstance(expression.func, ast.Name):
            return self._is_market_type_alias(expression.func.id)
        if (
            isinstance(expression, ast.Attribute)
            and isinstance(expression.value, ast.Name)
            and expression.value.id == "self"
        ):
            attributes = self._class_attributes()
            return attributes is not None and expression.attr in attributes
        return False

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if node.module == "src.infrastructure.db.market.market_db":
            self.scope.type_aliases.update(
                alias.asname or alias.name
                for alias in node.names
                if alias.name == "MarketDb"
            )

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        attributes: set[str] = set()
        for child in ast.walk(node):
            if (
                isinstance(child, ast.AnnAssign)
                and isinstance(child.target, ast.Attribute)
                and isinstance(child.target.value, ast.Name)
                and child.target.value.id == "self"
                and self._annotation_is_market_db(child.annotation)
            ):
                attributes.add(child.target.attr)

        previous = self._push_scope(class_attributes=attributes)
        try:
            for statement in node.body:
                self.visit(statement)
        finally:
            self.scope = previous

    def _visit_function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        previous = self._push_scope()
        try:
            for argument in (
                *node.args.posonlyargs,
                *node.args.args,
                *node.args.kwonlyargs,
            ):
                self.scope.bindings[argument.arg] = self._annotation_is_market_db(
                    argument.annotation
                )
            if node.args.vararg is not None:
                self.scope.bindings[node.args.vararg.arg] = False
            if node.args.kwarg is not None:
                self.scope.bindings[node.args.kwarg.arg] = False
            for statement in node.body:
                self.visit(statement)
        finally:
            self.scope = previous

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._visit_function(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._visit_function(node)

    def visit_Assign(self, node: ast.Assign) -> None:
        is_market_db = self._expression_is_market_db(node.value)
        for target in node.targets:
            if isinstance(target, ast.Name):
                self.scope.bindings[target.id] = is_market_db
            elif (
                isinstance(target, ast.Attribute)
                and isinstance(target.value, ast.Name)
                and target.value.id == "self"
            ):
                attributes = self._class_attributes()
                if attributes is not None:
                    if is_market_db:
                        attributes.add(target.attr)
                    else:
                        attributes.discard(target.attr)
        self.visit(node.value)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        annotated_market_db = self._annotation_is_market_db(node.annotation)
        if isinstance(node.target, ast.Name):
            self.scope.bindings[node.target.id] = annotated_market_db
        elif (
            isinstance(node.target, ast.Attribute)
            and isinstance(node.target.value, ast.Name)
            and node.target.value.id == "self"
        ):
            attributes = self._class_attributes()
            if attributes is not None:
                if annotated_market_db:
                    attributes.add(node.target.attr)
                else:
                    attributes.discard(node.target.attr)
        if node.value is not None:
            self.visit(node.value)

    def visit_Call(self, node: ast.Call) -> None:
        if (
            isinstance(node.func, ast.Attribute)
            and node.func.attr in LEGACY_MARKET_DB_WRITERS
            and self._expression_is_market_db(node.func.value)
        ):
            self.callers.append(f"{self.path}:{node.lineno}:{node.func.attr}")
        self.generic_visit(node)


def _legacy_market_db_calls(source: str, *, path: Path) -> list[str]:
    tree = ast.parse(source, filename=str(path))
    visitor = _MarketDbLegacyCallVisitor(path)
    visitor.visit(tree)
    return sorted(visitor.callers)
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


def test_market_db_call_guard_keeps_bindings_in_their_lexical_scope() -> None:
    source = """
from src.infrastructure.db.market.market_db import MarketDb

def market_write():
    db = MarketDb("market.duckdb")
    db.upsert_stock_data([])

def unrelated_write():
    db = DatasetWriter()
    db.upsert_stock_data([])
"""

    assert _legacy_market_db_calls(source, path=Path("mixed.py")) == [
        "mixed.py:6:upsert_stock_data"
    ]


def test_market_db_call_guard_resolves_import_and_annotation_aliases() -> None:
    source = """
from src.infrastructure.db.market.market_db import MarketDb as DB

def write(db: DB):
    db.upsert_statements([])
"""

    assert _legacy_market_db_calls(source, path=Path("alias.py")) == [
        "alias.py:5:upsert_statements"
    ]


def test_market_db_call_guard_detects_chained_constructor_call() -> None:
    source = """
from src.infrastructure.db.market.market_db import MarketDb as DB

DB("market.duckdb").upsert_topix_data([])
"""

    assert _legacy_market_db_calls(source, path=Path("chain.py")) == [
        "chain.py:4:upsert_topix_data"
    ]


def test_market_db_call_guard_detects_annotated_self_attribute() -> None:
    source = """
from src.infrastructure.db.market.market_db import MarketDb as DB

class Service:
    def __init__(self, db):
        self.db: DB = db

    def write(self):
        self.db.upsert_margin_data([])
"""

    assert _legacy_market_db_calls(source, path=Path("service.py")) == [
        "service.py:9:upsert_margin_data"
    ]


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
