from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
from pathlib import Path
import re
from typing import Any

import duckdb
import pytest

from src.shared.contracts.market_maintenance import (
    MaintenanceEvidenceStatus,
    MaintenanceOutcome,
    MarketOperationOutcome,
)
from src.application.services.adjusted_metrics_materializer import (
    AdjustedMetricsMaterializer,
)
from src.application.services.market_maintenance_finalizer import (
    MarketMaintenanceFinalizer,
)
from src.infrastructure.db.market.market_compaction import (
    CompactionTrigger,
    DuckDbSizeSnapshot,
    MarketCompactionPolicy,
    MarketCompactor,
)
from src.infrastructure.db.market.market_maintenance_evidence import (
    read_market_maintenance_evidence,
)
from src.infrastructure.db.market.market_schema import MARKET_SCHEMA_VERSION
from src.infrastructure.db.market.market_writer_resources import (
    MarketWriterResourceFactory,
    MarketWriterSession,
    ReadOnlyMarketResources,
)


_PROVIDER_PLAN = "premium"


class _SqlRecorder:
    def __init__(self, connection: Any) -> None:
        self._connection = connection
        self.statements: list[str] = []

    def __getattr__(self, name: str) -> Any:
        return getattr(self._connection, name)

    def execute(self, sql: str, parameters: Any = None) -> Any:
        self.statements.append(" ".join(sql.split()).upper())
        if parameters is None:
            return self._connection.execute(sql)
        return self._connection.execute(sql, parameters)

    def executemany(self, sql: str, parameters: Any) -> Any:
        self.statements.append(" ".join(sql.split()).upper())
        return self._connection.executemany(sql, parameters)


@dataclass(frozen=True)
class _CycleObservation:
    mutations: dict[str, int]
    row_counts: dict[str, int]
    statements: tuple[str, ...]
    parquet: dict[str, tuple[int, str, int]]
    db_bytes: int
    size: DuckDbSizeSnapshot


def _topix_rows() -> list[dict[str, object]]:
    return [
        {
            "date": f"2026-02-{day:02d}",
            "open": float(day),
            "high": float(day + 1),
            "low": float(day) - 0.5,
            "close": float(day) + 0.5,
        }
        for day in range(2, 12)
    ]


def _stock_rows() -> list[dict[str, object]]:
    return [
        {
            "code": "7203",
            "date": f"2026-02-{day:02d}",
            "open": float(100 + day),
            "high": float(101 + day),
            "low": float(99 + day),
            "close": float(100 + day),
            "volume": 1_000 + day,
            "turnover_value": float((100 + day) * (1_000 + day)),
            "adjustment_factor": 1.0,
            "adjusted_open": float(100 + day),
            "adjusted_high": float(101 + day),
            "adjusted_low": float(99 + day),
            "adjusted_close": float(100 + day),
            "adjusted_volume": 1_000 + day,
            "created_at": f"2026-02-{day:02d}T00:00:00+00:00",
        }
        for day in range(2, 12)
    ]


def _statement_rows() -> list[dict[str, object]]:
    return [
        {
            "code": "7203",
            "statement_id": "disclosure-1",
            "disclosure_number": "disclosure-1",
            "disclosed_date": "2026-02-02",
            "disclosed_at": "2026-02-02T15:30:00+09:00",
            "period_start": "2025-04-01",
            "period_end": "2026-03-31",
            "type_of_current_period": "FY",
            "type_of_document": "FYFinancialStatements",
            "earnings_per_share": 100.0,
            "diluted_earnings_per_share": 98.0,
            "bps": 1_000.0,
            "forecast_eps": 110.0,
            "dividend_fy": 30.0,
            "forecast_dividend_fy": 40.0,
            "shares_outstanding": 10_000_000.0,
            "treasury_shares": 1_000_000.0,
        }
    ]


def _stock_master_rows() -> list[dict[str, object]]:
    return [
        {
            "date": "2026-02-11",
            "code": "7203",
            "company_name": "Toyota",
            "company_name_english": "TOYOTA",
            "market_code": "0111",
            "market_name": "Prime",
            "sector_17_code": "6",
            "sector_17_name": "Automobiles",
            "sector_33_code": "3700",
            "sector_33_name": "Transportation Equipment",
            "scale_category": "TOPIX Core30",
            "listed_date": "1949-05-16",
            "created_at": "2026-02-11T00:00:00+00:00",
        }
    ]


def _size_from_cursor(cursor: Any, *, path: Path) -> DuckDbSizeSnapshot:
    row = cursor.fetchone()
    assert row is not None
    values = dict(zip((item[0] for item in cursor.description), row, strict=False))
    return DuckDbSizeSnapshot(
        block_size=int(values["block_size"]),
        total_blocks=int(values["total_blocks"]),
        used_blocks=int(values["used_blocks"]),
        free_blocks=int(values["free_blocks"]),
        wal_bytes=(
            Path(f"{path}.wal").stat().st_size if Path(f"{path}.wal").exists() else 0
        ),
    )


def _read_size(path: Path) -> DuckDbSizeSnapshot:
    conn = duckdb.connect(str(path), read_only=True)
    try:
        return _size_from_cursor(conn.execute("PRAGMA database_size"), path=path)
    finally:
        conn.close()


def _parquet_identity(root: Path) -> dict[str, tuple[int, str, int]]:
    conn = duckdb.connect()
    try:
        return {
            path.relative_to(root).as_posix(): (
                path.stat().st_ino,
                hashlib.sha256(path.read_bytes()).hexdigest(),
                int(
                    conn.execute(
                        "SELECT COUNT(*) FROM read_parquet(?)",
                        [str(path)],
                    ).fetchone()[0]
                ),
            )
            for path in sorted(root.rglob("*.parquet"))
        }
    finally:
        conn.close()


_REQUIRED_TABLES = (
    "topix_data",
    "stock_data_raw",
    "stock_data",
    "statements",
    "stock_master_daily",
    "index_membership_daily",
    "stock_master_intervals",
    "stocks_latest",
    "stocks",
    "stock_adjustment_events",
    "stock_provider_windows",
    "current_basis_fundamentals_state",
    "current_basis_recompute_pending",
    "statement_metrics_adjusted",
    "daily_valuation",
    "daily_technical_metrics",
)


def _table_counts(market_db: Any) -> dict[str, int]:
    return {table: market_db._count_rows(table) for table in _REQUIRED_TABLES}


def _persistent_writes(statements: tuple[str, ...]) -> list[str]:
    persistent_tables = (
        "TOPIX_DATA|STOCK_DATA_RAW|STOCK_DATA|STATEMENTS|STOCK_MASTER_DAILY|"
        "INDEX_MEMBERSHIP_DAILY|STOCK_MASTER_INTERVALS|STOCKS_LATEST|STOCKS|"
        "STOCK_ADJUSTMENT_EVENTS|STOCK_PROVIDER_WINDOWS|"
        "CURRENT_BASIS_FUNDAMENTALS_STATE|CURRENT_BASIS_RECOMPUTE_PENDING|"
        "STATEMENT_METRICS_ADJUSTED|DAILY_VALUATION|DAILY_TECHNICAL_METRICS"
    )
    persistent_dml = re.compile(
        rf"\b(?:INSERT(?:\s+OR\s+\w+)?\s+INTO|UPDATE|DELETE\s+FROM|MERGE\s+INTO)"
        rf'\s+"?(?:{persistent_tables})\b'
    )
    provider_event_rebind_probe = re.compile(
        r"^UPDATE STOCK_ADJUSTMENT_EVENTS SET SOURCE_FINGERPRINT = \? "
        r"WHERE CODE = \? AND SOURCE_FINGERPRINT IS DISTINCT FROM \? RETURNING CODE$"
    )
    return [
        sql
        for sql in statements
        if (
            persistent_dml.search(sql)
            and provider_event_rebind_probe.fullmatch(sql) is None
        )
        or ("COPY " in sql and " TO " in sql)
    ]


def _assert_steady_state_growth(
    values: tuple[int, int, int],
    *,
    tolerance: int,
) -> None:
    first, second, third = values
    assert max(values) - min(values) <= tolerance
    assert not (first < second < third)
    assert third <= min(first, second) + tolerance


def test_growth_guard_rejects_cumulative_one_block_monotonic_growth() -> None:
    block = 256 * 1024

    with pytest.raises(AssertionError):
        _assert_steady_state_growth(
            (10 * block, 11 * block, 12 * block),
            tolerance=block,
        )


def _cleanup_writer_session_for_test(session: MarketWriterSession) -> None:
    if session._process_lock is None:
        return
    if session.fenced:
        if not session._handles_closed:
            session.handles.close()
            session._handles_closed = True
        session.lease.release()
        session._process_lock.release()
        session._process_lock = None
        return
    token = session.close_writable_handles()
    read_only = session.reopen_read_only(token)
    try:
        session.release_after_read_only_reopen(token)
    finally:
        read_only.close()


@contextmanager
def _managed_writer_session(
    factory: MarketWriterResourceFactory,
    *,
    reset: bool,
) -> Iterator[MarketWriterSession]:
    session = factory.reset_and_open_v4() if reset else factory.open_existing()
    try:
        yield session
    finally:
        _cleanup_writer_session_for_test(session)


def test_managed_writer_session_releases_process_lock_after_body_failure(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    factory = MarketWriterResourceFactory(
        data_root=data_root,
        market_root=data_root / "market-timeseries",
    )

    with pytest.raises(RuntimeError, match="injected writer body failure"):
        with _managed_writer_session(factory, reset=True):
            raise RuntimeError("injected writer body failure")

    with _managed_writer_session(factory, reset=False) as probe:
        assert probe.handles.market_db.get_market_schema_version() == (
            MARKET_SCHEMA_VERSION
        )


def _run_cycle(
    factory: MarketWriterResourceFactory,
    *,
    reset: bool,
) -> _CycleObservation:
    with _managed_writer_session(factory, reset=reset) as session:
        market_db = session.handles.market_db
        store = session.handles.time_series_store
        db_recorder = _SqlRecorder(market_db._conn)
        store_recorder = _SqlRecorder(store._conn)
        market_db._conn = db_recorder
        store._conn = store_recorder
        before_counts = _table_counts(market_db)

        topix = store.publish_topix_data(_topix_rows())
        stocks = store.publish_stock_data(
            _stock_rows(), provider_plan=_PROVIDER_PLAN
        )
        statements = store.publish_statements(_statement_rows())
        store.index_topix_data()
        store.index_stock_data()
        store.index_statements()
        master = market_db.publish_stock_master_daily_rows(_stock_master_rows())
        adjusted = AdjustedMetricsMaterializer(market_db).rebuild_current_basis([])
        after_counts = _table_counts(market_db)

        attached: list[ReadOnlyMarketResources] = []
        terminals = []
        finalizer = MarketMaintenanceFinalizer(
            session=session,
            operation="synthetic_incremental_acceptance",
            attach=lambda resources, _record: attached.append(resources),
        )
        decision = finalizer.finalize(
            operation_outcome=MarketOperationOutcome.SUCCEEDED,
            publish_terminal=terminals.append,
        )
        assert terminals == [decision]
        assert decision.terminal_outcome is MarketOperationOutcome.SUCCEEDED
        assert decision.maintenance.outcome is MaintenanceOutcome.PASSED
        assert len(attached) == 1
        assert attached[0].market_db.get_market_schema_version() == (
            MARKET_SCHEMA_VERSION
        )
        attached[0].close()

        evidence = read_market_maintenance_evidence(factory.market_root)
        assert evidence.evidenceStatus is MaintenanceEvidenceStatus.VALID
        assert evidence.outcome is MaintenanceOutcome.PASSED
        db_path = factory.market_root / "market.duckdb"
        size = _read_size(db_path)
        return _CycleObservation(
            mutations={
                "topix": topix.mutated_rows,
                "stock_data_raw": stocks.mutated_rows,
                "stock_data": (
                    after_counts["stock_data"] - before_counts["stock_data"]
                ),
                "statements": statements.mutated_rows,
                "stock_master_daily": master.daily.mutated_rows,
                "index_membership_daily": master.membership.mutated_rows,
                "stock_master_intervals": master.intervals.mutated_rows,
                "stocks_latest": master.stocks_latest.mutated_rows,
                "stocks": master.stocks.mutated_rows,
                "stock_adjustment_events": (
                    after_counts["stock_adjustment_events"]
                    - before_counts["stock_adjustment_events"]
                ),
                "stock_provider_windows": (
                    after_counts["stock_provider_windows"]
                    - before_counts["stock_provider_windows"]
                ),
                "current_basis_fundamentals_state": (
                    after_counts["current_basis_fundamentals_state"]
                    - before_counts["current_basis_fundamentals_state"]
                ),
                "current_basis_recompute_pending": (
                    after_counts["current_basis_recompute_pending"]
                    - before_counts["current_basis_recompute_pending"]
                ),
                "statement_metrics_adjusted": adjusted.mutation_stats[
                    "statements"
                ].mutated_rows,
                "daily_valuation": (
                    after_counts["daily_valuation"]
                    - before_counts["daily_valuation"]
                ),
                "daily_technical_metrics": (
                    after_counts["daily_technical_metrics"]
                    - before_counts["daily_technical_metrics"]
                ),
            },
            row_counts=after_counts,
            statements=tuple(db_recorder.statements + store_recorder.statements),
            parquet=_parquet_identity(factory.market_root / "parquet"),
            db_bytes=db_path.stat().st_size,
            size=size,
        )


def test_identical_incremental_cycle_reaches_zero_mutation_steady_state(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    factory = MarketWriterResourceFactory(
        data_root=data_root,
        market_root=data_root / "market-timeseries",
    )

    first = _run_cycle(factory, reset=True)
    second = _run_cycle(factory, reset=False)
    third = _run_cycle(factory, reset=False)

    assert first.mutations["topix"] == 10
    assert first.mutations["stock_data_raw"] == 10
    assert first.mutations["stock_data"] == 10
    assert first.mutations["statements"] == 1
    assert first.mutations["stock_master_daily"] == 1
    assert first.mutations["index_membership_daily"] == 1
    assert first.mutations["stock_master_intervals"] == 1
    assert first.mutations["stocks_latest"] == 1
    assert first.mutations["stocks"] == 1
    assert first.mutations["stock_adjustment_events"] == 0
    assert first.mutations["stock_provider_windows"] == 1
    assert first.mutations["current_basis_fundamentals_state"] == 1
    assert first.mutations["current_basis_recompute_pending"] == 0
    assert first.mutations["statement_metrics_adjusted"] == 1
    assert first.mutations["daily_valuation"] == 10
    assert first.mutations["daily_technical_metrics"] == 0
    assert first.row_counts["topix_data"] == 10
    assert first.row_counts["stock_data_raw"] == 10
    assert first.row_counts["stock_data"] == 10
    assert first.row_counts["statements"] == 1
    assert first.row_counts["stock_master_daily"] == 1
    assert first.row_counts["index_membership_daily"] == 1
    assert first.row_counts["stock_master_intervals"] == 1
    assert first.row_counts["stocks_latest"] == 1
    assert first.row_counts["stocks"] == 1
    assert first.row_counts["stock_adjustment_events"] == 0
    assert first.row_counts["stock_provider_windows"] == 1
    assert first.row_counts["current_basis_fundamentals_state"] == 1
    assert first.row_counts["current_basis_recompute_pending"] == 0
    assert first.row_counts["statement_metrics_adjusted"] == 1
    assert first.row_counts["daily_valuation"] == 10
    assert first.row_counts["daily_technical_metrics"] == 0
    assert second.mutations == {name: 0 for name in second.mutations}
    assert third.mutations == {name: 0 for name in third.mutations}
    assert second.row_counts == first.row_counts
    assert third.row_counts == second.row_counts
    assert _persistent_writes(second.statements) == []
    assert _persistent_writes(third.statements) == []
    expected_parquet_counts = {
        "topix_data.parquet": 10,
        "statements.parquet": 1,
        **{
            f"stock_data_raw/date=2026-02-{day:02d}/data.parquet": 1
            for day in range(2, 12)
        },
        **{
            f"stock_data/date=2026-02-{day:02d}/data.parquet": 1
            for day in range(2, 12)
        },
    }
    assert {
        path: identity[2] for path, identity in first.parquet.items()
    } == expected_parquet_counts
    assert second.parquet == first.parquet
    assert third.parquet == second.parquet

    tolerance = max(
        observation.size.block_size + observation.size.free_bytes
        for observation in (first, second, third)
    )
    _assert_steady_state_growth(
        (first.db_bytes, second.db_bytes, third.db_bytes),
        tolerance=tolerance,
    )
    _assert_steady_state_growth(
        (
            first.size.free_bytes,
            second.size.free_bytes,
            third.size.free_bytes,
        ),
        tolerance=tolerance,
    )


def test_test_equivalent_hard_cap_compacts_and_persists_verified_evidence(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    factory = MarketWriterResourceFactory(
        data_root=data_root,
        market_root=data_root / "market-timeseries",
    )
    with _managed_writer_session(factory, reset=True) as session:
        source = factory.market_root / "market.duckdb"
        actual_before = _size_from_cursor(
            session.handles.market_db._conn.execute("PRAGMA database_size"),
            path=source,
        )
        hard_cap = max(actual_before.block_size * 2, 2)
        policy = MarketCompactionPolicy(
            soft_free_bytes=hard_cap * 2,
            soft_free_ratio=1.0,
            hard_free_bytes=hard_cap,
        )
        observations = 0

        def forced_size_reader(path: Path) -> DuckDbSizeSnapshot:
            nonlocal observations
            observations += 1
            actual = _read_size(path)
            if observations == 1:
                free_blocks = max(
                    2, (hard_cap + actual.block_size - 1) // actual.block_size
                )
                total_blocks = max(actual.total_blocks, free_blocks)
                return DuckDbSizeSnapshot(
                    block_size=actual.block_size,
                    total_blocks=total_blocks,
                    used_blocks=total_blocks - free_blocks,
                    free_blocks=free_blocks,
                    wal_bytes=actual.wal_bytes,
                )
            return actual

        attached: list[ReadOnlyMarketResources] = []
        decisions = []
        finalizer = MarketMaintenanceFinalizer(
            session=session,
            operation="forced_hard_cap_acceptance",
            compactor=MarketCompactor(
                size_reader=forced_size_reader,
                policy=policy,
            ),
            attach=lambda resources, _record: attached.append(resources),
        )
        decision = finalizer.finalize(
            operation_outcome=MarketOperationOutcome.SUCCEEDED,
            publish_terminal=decisions.append,
        )

        assert decisions == [decision]
        assert decision.terminal_outcome is MarketOperationOutcome.SUCCEEDED
        assert decision.maintenance.compacted is True
        assert decision.maintenance.trigger == CompactionTrigger.HARD.value
        assert decision.maintenance.validation == "passed"
        assert observations == 2
        assert _read_size(source).free_bytes < hard_cap
        assert len(attached) == 1
        assert attached[0].market_db.get_market_schema_version() == (
            MARKET_SCHEMA_VERSION
        )
        attached[0].close()

    sidecar = read_market_maintenance_evidence(factory.market_root)
    assert sidecar.evidenceStatus is MaintenanceEvidenceStatus.VALID
    assert sidecar.outcome is MaintenanceOutcome.PASSED
    assert sidecar.compacted is True
    assert sidecar.trigger == CompactionTrigger.HARD.value
    assert sidecar.validation == "passed"

    with _managed_writer_session(factory, reset=False) as probe:
        assert probe.handles.market_db.get_market_schema_version() == (
            MARKET_SCHEMA_VERSION
        )
