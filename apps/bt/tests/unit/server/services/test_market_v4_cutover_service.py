"""Market v4 cutover service tests."""

from __future__ import annotations

from pathlib import Path


import src.application.services.market_v4_cutover.service as cutover_module
from src.application.services.market_v4_cutover.filesystem import (
    DarwinAtomicExchange,
)
from src.application.services.market_v4_cutover.service import MarketV4CutoverService
from src.infrastructure.db.market import managed_root
from src.application.contracts.market_data_plane import MarketSchemaStats
from tests.unit.server.services.market_v4_cutover_test_support import (
    FakeDuckDb,
    FakeRuntime,
)


def test_market_schema_stats_requires_v4_by_default() -> None:
    assert MarketSchemaStats().requiredVersion == 4


def test_cutover_service_exposes_injected_boundaries(tmp_path: Path) -> None:
    assert hasattr(cutover_module, "MarketV4CutoverService")
    assert hasattr(cutover_module, "DuckDbAdapter")
    assert hasattr(cutover_module, "RuntimeAdapter")
    atomic_exchange = DarwinAtomicExchange()
    service = MarketV4CutoverService(
        tmp_path,
        duckdb=FakeDuckDb(),
        runtime=FakeRuntime(),
        disk_free_bytes=lambda _path: 1,
        now=lambda: "2026-07-16T00:00:00Z",
        code_version=lambda: "deadbeef",
        atomic_exchange=atomic_exchange,
    )

    assert service._workspace.atomic_exchange is atomic_exchange


def test_cutover_service_preserves_false_valued_atomic_exchange(
    tmp_path: Path,
) -> None:
    class FalseValuedAtomicExchange:
        def __bool__(self) -> bool:
            return False

        def exchange(
            self,
            managed_root: managed_root.ManagedRootFd,
            left: Path,
            right: Path,
        ) -> None:
            del managed_root, left, right

    atomic_exchange = FalseValuedAtomicExchange()
    service = MarketV4CutoverService(
        tmp_path,
        duckdb=FakeDuckDb(),
        runtime=FakeRuntime(),
        disk_free_bytes=lambda _path: 1,
        now=lambda: "2026-07-16T00:00:00Z",
        code_version=lambda: "deadbeef",
        atomic_exchange=atomic_exchange,
    )

    assert service._workspace.atomic_exchange is atomic_exchange
