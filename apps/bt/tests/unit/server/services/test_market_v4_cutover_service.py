"""Market v5 cutover service tests."""

from __future__ import annotations

from pathlib import Path
import importlib.util

import pytest

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
    _TestAtomicExchange,
    _service,
)


def test_market_schema_stats_requires_v5_by_default() -> None:
    assert MarketSchemaStats().requiredVersion == 5


def test_market_v5_cutover_has_no_retained_v4_public_path(tmp_path: Path) -> None:
    service = _service(tmp_path)

    assert not hasattr(service, "rehearse_retained")
    assert not hasattr(service, "promote_retained")


@pytest.mark.parametrize(
    "module",
    (
        "promotion",
        "promotion_artifacts",
        "promotion_cleanup",
        "promotion_contracts",
        "promotion_evidence",
        "promotion_eligibility",
        "promotion_recovery",
        "promotion_reports",
        "promotion_rollback",
        "promotion_transaction",
        "rehearsal",
        "journal",
        "journal_directories",
        "journal_storage",
        "journal_validation",
    ),
)
def test_internal_retained_v4_execution_boundary_is_deleted(module: str) -> None:
    assert (
        importlib.util.find_spec(
            f"src.application.services.market_v4_cutover.{module}"
        )
        is None
    )


def test_market_v5_cutover_uses_v5_operation_identity(tmp_path: Path) -> None:
    service = _service(tmp_path)

    assert service._workspace.operations_root == (
        tmp_path / "operations" / "market-v5-cutover"
    )


def test_cutover_test_service_uses_explicit_atomic_exchange_capability(
    tmp_path: Path,
) -> None:
    service = _service(tmp_path)

    assert isinstance(service._workspace.atomic_exchange, _TestAtomicExchange)


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

        def require_capability(self) -> None:
            return None

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
