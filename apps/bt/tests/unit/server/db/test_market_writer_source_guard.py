from __future__ import annotations

import ast
from pathlib import Path
from types import SimpleNamespace

import pytest


SRC = Path(__file__).resolve().parents[4] / "src"
FACTORY = SRC / "infrastructure/db/market/market_writer_resources.py"
CONNECTION = SRC / "infrastructure/db/market/duckdb_connection.py"
TIME_STORE = SRC / "infrastructure/db/market/time_series_store.py"
MARKET_DB = SRC / "infrastructure/db/market/market_db.py"
RAW_DUCKDB_WRITER_ALLOWLIST = {
    SRC / "application/services/dataset_builder_service.py",
    SRC / "domains/analytics/readonly_duckdb_support.py",
    SRC / "domains/analytics/research_bundle.py",
    SRC / "domains/analytics/turtle_like_momentum_research.py",
}


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


def test_market_writer_token_issuer_is_private_and_factory_confined() -> None:
    import src.infrastructure.db.market.duckdb_connection as connection_module

    assert not hasattr(connection_module, "issue_market_writer_token")
    assert not hasattr(connection_module, "_issue_market_writer_token")
    with pytest.raises(PermissionError, match="concrete Market lease"):
        connection_module.MarketWriterToken._from_writer_factory(
            SimpleNamespace(exclusive=True, fd=123),
            "/tmp/market-timeseries/market.duckdb",
        )
    namespace = {
        "__name__": "src.infrastructure.db.market.market_writer_resources",
        "MarketWriterToken": connection_module.MarketWriterToken,
        "fake": SimpleNamespace(exclusive=True, fd=123),
    }
    with pytest.raises(PermissionError, match="concrete Market lease"):
        exec(
            "MarketWriterToken._from_writer_factory("
            "fake, '/tmp/market-timeseries/market.duckdb')",
            namespace,
        )
    connection_source = CONNECTION.read_text()
    assert "def issue_market_writer_token" not in connection_source
    violations: list[str] = []
    for path in SRC.rglob("*.py"):
        if path == FACTORY:
            continue
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and any(
                alias.name == "_issue_market_writer_token" for alias in node.names
            ):
                violations.append(f"{path.relative_to(SRC)}:{node.lineno}")
        for call in (node for node in ast.walk(tree) if isinstance(node, ast.Call)):
            if _name(call) in {"_issue_market_writer_token", "_from_writer_factory"}:
                violations.append(f"{path.relative_to(SRC)}:{call.lineno}")
    assert violations == []


def test_raw_duckdb_writable_calls_have_an_exact_module_allowlist() -> None:
    actual: set[Path] = set()
    for path in SRC.rglob("*.py"):
        for call in _calls(path):
            if (
                isinstance(call.func, ast.Attribute)
                and isinstance(call.func.value, ast.Name)
                and call.func.value.id == "duckdb"
                and call.func.attr == "connect"
                and not _read_only_is_true(call)
            ):
                actual.add(path)
    assert actual == RAW_DUCKDB_WRITER_ALLOWLIST


def test_normal_fastapi_startup_does_not_acquire_shared_market_lease() -> None:
    app_source = (SRC / "entrypoints/http/app.py").read_text()
    assert "MarketOperationLease.acquire(data_root, exclusive=False)" not in app_source
    assert "prepare_market_managed_root(market_root.parent, market_root)" in app_source
    assert "TRADING25_RUNTIME_CAPABILITY" not in app_source
    assert "TRADING25_MARKET_OPERATION_LOCK_FD" not in app_source
