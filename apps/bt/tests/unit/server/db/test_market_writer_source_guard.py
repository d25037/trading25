from __future__ import annotations

import ast
from pathlib import Path


SRC = Path(__file__).resolve().parents[4] / "src"
FACTORY = SRC / "infrastructure/db/market/market_writer_resources.py"
CONNECTION = SRC / "infrastructure/db/market/duckdb_connection.py"
TIME_STORE = SRC / "infrastructure/db/market/time_series_store.py"
MARKET_DB = SRC / "infrastructure/db/market/market_db.py"
CUTOVER = SRC / "application/services/market_v4_cutover.py"


def _calls(path: Path) -> list[ast.Call]:
    return [
        node
        for node in ast.walk(ast.parse(path.read_text()))
        if isinstance(node, ast.Call)
    ]


def _name(call: ast.Call) -> str | None:
    if isinstance(call.func, ast.Name):
        return call.func.id
    if isinstance(call.func, ast.Attribute):
        return call.func.attr
    return None


def _read_only_is_true(call: ast.Call) -> bool:
    for keyword in call.keywords:
        if keyword.arg == "read_only":
            return isinstance(keyword.value, ast.Constant) and keyword.value.value is True
    return False


def test_market_writable_constructors_are_factory_confined() -> None:
    violations: list[str] = []
    for path in SRC.rglob("*.py"):
        if path in {FACTORY, CONNECTION, TIME_STORE, MARKET_DB}:
            continue
        for call in _calls(path):
            if _name(call) in {
                "MarketDb",
                "create_time_series_store",
                "DuckDbParquetTimeSeriesStore",
                "connect_market_duckdb",
            } and not _read_only_is_true(call):
                violations.append(f"{path.relative_to(SRC)}:{call.lineno}")
    assert violations == []


def test_cutover_monolith_does_not_reexport_extracted_primitives() -> None:
    source = CUTOVER.read_text()
    assert "class MarketOperationLease" not in source
    assert "class ManagedRootFd" not in source
    assert "from src.infrastructure.db.market.market_operation_lease import" not in source
    assert "from src.infrastructure.db.market.managed_root import" not in source


def test_normal_fastapi_startup_does_not_acquire_shared_market_lease() -> None:
    app_source = (SRC / "entrypoints/http/app.py").read_text()
    assert "MarketOperationLease.acquire(data_root, exclusive=False)" not in app_source
    assert "operation_lease = None" in app_source
