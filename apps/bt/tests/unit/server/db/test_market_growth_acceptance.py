from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path
import re
from typing import Any

import duckdb

from src.application.contracts.market_maintenance import (
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
from src.infrastructure.db.market.market_writer_resources import (
    MarketWriterResourceFactory,
    ReadOnlyMarketResources,
)


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
    statements: tuple[str, ...]
    parquet: dict[str, tuple[int, str]]
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
        for day in range(2, 8)
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
            "adjustment_factor": 1.0,
            "created_at": f"2026-02-{day:02d}T00:00:00+00:00",
        }
        for day in range(2, 8)
    ]


def _statement_rows() -> list[dict[str, object]]:
    return [
        {
            "code": "7203",
            "disclosed_date": "2026-02-02",
            "type_of_current_period": "FY",
            "earnings_per_share": 100.0,
            "bps": 1_000.0,
            "forecast_eps": 110.0,
            "shares_outstanding": 10_000_000.0,
            "treasury_shares": 1_000_000.0,
        }
    ]


def _stock_master_rows() -> list[dict[str, object]]:
    return [
        {
            "date": "2026-02-07",
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
            "created_at": "2026-02-07T00:00:00+00:00",
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


def _parquet_identity(root: Path) -> dict[str, tuple[int, str]]:
    return {
        path.relative_to(root).as_posix(): (
            path.stat().st_ino,
            hashlib.sha256(path.read_bytes()).hexdigest(),
        )
        for path in sorted(root.rglob("*.parquet"))
    }


def _persistent_writes(statements: tuple[str, ...]) -> list[str]:
    persistent_tables = (
        "TOPIX_DATA|STOCK_DATA_RAW|STOCK_DATA|STATEMENTS|STOCK_MASTER_DAILY|"
        "INDEX_MEMBERSHIP_DAILY|STOCK_MASTER_INTERVALS|STOCKS_LATEST|STOCKS|"
        "STOCK_ADJUSTMENT_BASES|STOCK_ADJUSTMENT_BASIS_SEGMENTS|"
        "STATEMENT_METRICS_ADJUSTED|DAILY_VALUATION|DAILY_TECHNICAL_METRICS"
    )
    persistent_dml = re.compile(
        rf"\b(?:INSERT(?:\s+OR\s+\w+)?\s+INTO|UPDATE|DELETE\s+FROM|MERGE\s+INTO)"
        rf'\s+"?(?:{persistent_tables})\b'
    )
    return [
        sql
        for sql in statements
        if sql.startswith(("BEGIN", "COMMIT", "ROLLBACK"))
        or persistent_dml.search(sql)
        or ("COPY " in sql and " TO " in sql)
    ]


def _run_cycle(
    factory: MarketWriterResourceFactory,
    *,
    reset: bool,
) -> _CycleObservation:
    session = factory.reset_and_open_v4() if reset else factory.open_existing()
    market_db = session.handles.market_db
    store = session.handles.time_series_store
    db_recorder = _SqlRecorder(market_db._conn)
    store_recorder = _SqlRecorder(store._conn)
    market_db._conn = db_recorder
    store._conn = store_recorder

    topix = store.publish_topix_data(_topix_rows())
    stocks = store.publish_stock_data(_stock_rows())
    statements = store.publish_statements(_statement_rows())
    store.index_topix_data()
    store.index_stock_data()
    store.index_statements()
    master = market_db.publish_stock_master_daily_rows(_stock_master_rows())
    adjusted = AdjustedMetricsMaterializer(market_db).rebuild_all()

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
    assert attached[0].market_db.get_market_schema_version() == 4
    attached[0].close()

    evidence = read_market_maintenance_evidence(factory.market_root)
    assert evidence.evidenceStatus is MaintenanceEvidenceStatus.VALID
    assert evidence.outcome is MaintenanceOutcome.PASSED
    db_path = factory.market_root / "market.duckdb"
    size = _read_size(db_path)
    return _CycleObservation(
        mutations={
            "topix": topix.mutated_rows,
            "stock_raw_and_projection": stocks.mutated_rows,
            "statements": statements.mutated_rows,
            "stock_master_dated_and_derived": master.mutated_rows,
            "adjusted": sum(
                stats.mutated_rows for stats in adjusted.mutation_stats.values()
            ),
            "technical": adjusted.mutation_stats["technical_metrics"].mutated_rows,
        },
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

    assert any(value > 0 for value in first.mutations.values())
    assert second.mutations == {name: 0 for name in second.mutations}
    assert third.mutations == {name: 0 for name in third.mutations}
    assert _persistent_writes(second.statements) == []
    assert _persistent_writes(third.statements) == []
    assert first.parquet
    assert second.parquet == first.parquet
    assert third.parquet == second.parquet

    tolerance = max(
        first.size.block_size, second.size.block_size, third.size.block_size
    )
    assert second.db_bytes <= first.db_bytes + tolerance
    assert third.db_bytes <= second.db_bytes + tolerance
    assert second.size.free_bytes <= first.size.free_bytes + tolerance
    assert third.size.free_bytes <= second.size.free_bytes + tolerance
    assert third.db_bytes <= max(first.db_bytes, second.db_bytes) + tolerance
    assert (
        third.size.free_bytes
        <= max(first.size.free_bytes, second.size.free_bytes) + tolerance
    )


def test_test_equivalent_hard_cap_compacts_and_persists_verified_evidence(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    factory = MarketWriterResourceFactory(
        data_root=data_root,
        market_root=data_root / "market-timeseries",
    )
    session = factory.reset_and_open_v4()
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
    assert attached[0].market_db.get_market_schema_version() == 4
    attached[0].close()

    sidecar = read_market_maintenance_evidence(factory.market_root)
    assert sidecar.evidenceStatus is MaintenanceEvidenceStatus.VALID
    assert sidecar.outcome is MaintenanceOutcome.PASSED
    assert sidecar.compacted is True
    assert sidecar.trigger == CompactionTrigger.HARD.value
    assert sidecar.validation == "passed"

    probe = factory.open_existing(blocking=False)
    token = probe.close_writable_handles()
    reopened = probe.reopen_read_only(token)
    probe.release_after_read_only_reopen(token)
    reopened.close()
