from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from threading import RLock
from typing import Any

import pytest

from src.infrastructure.db.market.market_db import MarketDb
from src.infrastructure.db.market.market_mutations import MarketMutationStats
from src.infrastructure.db.market.technical_metric_writers import (
    TechnicalMetricRebuildResult,
    rebuild_daily_technical_metrics_from_stock_data,
)


@pytest.fixture()
def market_db(tmp_path: Path) -> Iterator[MarketDb]:
    db = MarketDb(str(tmp_path / "market.duckdb"))
    yield db
    db.close()


def _seed_prices(
    market_db: MarketDb,
    code: str,
    closes: list[float],
    *,
    start_day: int = 1,
) -> None:
    for offset, close in enumerate(closes):
        date = f"2024-01-{start_day + offset:02d}"
        market_db._execute(
            """
            INSERT INTO stock_data (
                code, date, open, high, low, close, volume,
                adjustment_factor, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, 1000, 1.0, NULL)
            """,
            [code, date, close, close, close, close],
        )


def _rows(market_db: MarketDb) -> list[tuple[Any, ...]]:
    return market_db._execute(
        """
        SELECT code, date, close, sma5, sma5_sessions,
               close_above_sma5_flag, sma5_above_count_5d,
               sma5_above_count_sessions, sma5_above_count_group,
               sma5_below_streak, created_at
        FROM daily_technical_metrics
        ORDER BY code, date
        """
    ).fetchall()


class _RecordingConnection:
    def __init__(self, connection: Any, *, fail_on: str | None = None) -> None:
        self._connection = connection
        self.statements: list[str] = []
        self.fail_on = fail_on

    def execute(self, sql: str, parameters: Any = None) -> Any:
        normalized = " ".join(sql.split()).upper()
        self.statements.append(normalized)
        if self.fail_on is not None and normalized.startswith(self.fail_on):
            raise RuntimeError("injected technical metric DML failure")
        if parameters is None:
            return self._connection.execute(sql)
        return self._connection.execute(sql, parameters)


def _rebuild_with_spy(
    market_db: MarketDb, *, fail_on: str | None = None
) -> tuple[TechnicalMetricRebuildResult, _RecordingConnection]:
    spy = _RecordingConnection(market_db._conn, fail_on=fail_on)
    result = rebuild_daily_technical_metrics_from_stock_data(
        spy,
        RLock(),
        market_db._table_exists,
    )
    return result, spy


def test_first_rebuild_inserts_desired_rows_and_reports_final_count(
    market_db: MarketDb,
) -> None:
    _seed_prices(market_db, "7203", [10, 11, 12, 13, 14, 15, 16, 17, 18, 19])

    result = market_db.rebuild_daily_technical_metrics_from_stock_data()

    assert result == TechnicalMetricRebuildResult(
        stats=MarketMutationStats(2, 2, 0, 0, 0), final_count=2
    )
    assert len(_rows(market_db)) == 2


def test_exact_repeat_executes_no_persistent_transaction_or_dml_and_preserves_created_at(
    market_db: MarketDb,
) -> None:
    _seed_prices(market_db, "7203", [10, 11, 12, 13, 14, 15, 16, 17, 18, 19])
    market_db.rebuild_daily_technical_metrics_from_stock_data()
    before = _rows(market_db)

    result, spy = _rebuild_with_spy(market_db)

    assert result.stats == MarketMutationStats(2, 0, 0, 2, 0)
    assert _rows(market_db) == before
    forbidden = (
        "BEGIN",
        "DELETE FROM DAILY_TECHNICAL_METRICS",
        "UPDATE DAILY_TECHNICAL_METRICS",
        "INSERT INTO DAILY_TECHNICAL_METRICS",
    )
    assert not any(statement.startswith(forbidden) for statement in spy.statements)


def test_source_change_updates_only_distinct_rows(market_db: MarketDb) -> None:
    _seed_prices(market_db, "7203", [10, 11, 12, 13, 14, 15, 16, 17, 18, 19])
    market_db.rebuild_daily_technical_metrics_from_stock_data()
    created_at = {row[1]: row[-1] for row in _rows(market_db)}
    market_db._execute(
        "UPDATE stock_data SET close = 30 WHERE code = '7203' AND date = '2024-01-10'"
    )

    result = market_db.rebuild_daily_technical_metrics_from_stock_data()

    assert result.stats == MarketMutationStats(2, 0, 1, 1, 0)
    after = {row[1]: row for row in _rows(market_db)}
    assert after["2024-01-09"][-1] == created_at["2024-01-09"]
    assert after["2024-01-10"][2] == 30


def test_stale_target_key_is_deleted_without_replacing_survivors(
    market_db: MarketDb,
) -> None:
    _seed_prices(market_db, "7203", [10, 11, 12, 13, 14, 15, 16, 17, 18, 19])
    market_db.rebuild_daily_technical_metrics_from_stock_data()
    survivor_created_at = _rows(market_db)[0][-1]
    market_db._execute(
        "DELETE FROM stock_data WHERE code = '7203' AND date = '2024-01-10'"
    )

    result = market_db.rebuild_daily_technical_metrics_from_stock_data()

    assert result.stats == MarketMutationStats(1, 0, 0, 1, 1)
    assert _rows(market_db)[0][-1] == survivor_created_at


def test_new_code_inserts_only_new_desired_rows(market_db: MarketDb) -> None:
    _seed_prices(market_db, "7203", [10, 11, 12, 13, 14, 15, 16, 17, 18])
    market_db.rebuild_daily_technical_metrics_from_stock_data()
    _seed_prices(market_db, "6758", [20, 21, 22, 23, 24, 25, 26, 27, 28])

    result = market_db.rebuild_daily_technical_metrics_from_stock_data()

    assert result.stats == MarketMutationStats(2, 1, 0, 1, 0)
    assert [row[0] for row in _rows(market_db)] == ["6758", "7203"]


def test_null_corruption_is_detected_with_null_safe_comparison(
    market_db: MarketDb,
) -> None:
    _seed_prices(market_db, "7203", [10, 11, 12, 13, 14, 15, 16, 17, 18])
    market_db.rebuild_daily_technical_metrics_from_stock_data()
    market_db._execute(
        "UPDATE daily_technical_metrics SET sma5 = NULL WHERE code = '7203'"
    )

    result = market_db.rebuild_daily_technical_metrics_from_stock_data()

    assert result.stats == MarketMutationStats(1, 0, 1, 0, 0)
    assert _rows(market_db)[0][3] == 16


def test_four_and_five_digit_duplicates_keep_four_digit_precedence(
    market_db: MarketDb,
) -> None:
    _seed_prices(market_db, "72030", [100, 101, 102, 103, 104, 105, 106, 107, 108])
    _seed_prices(market_db, "7203", [10, 11, 12, 13, 14, 15, 16, 17, 18])

    result = market_db.rebuild_daily_technical_metrics_from_stock_data()

    assert result.stats == MarketMutationStats(1, 1, 0, 0, 0)
    assert _rows(market_db)[0][0:4] == ("7203", "2024-01-09", 18, 16)


def test_empty_source_deletes_all_stale_target_rows(market_db: MarketDb) -> None:
    _seed_prices(market_db, "7203", [10, 11, 12, 13, 14, 15, 16, 17, 18])
    market_db.rebuild_daily_technical_metrics_from_stock_data()
    market_db._execute("DELETE FROM stock_data")

    result = market_db.rebuild_daily_technical_metrics_from_stock_data()

    assert result.stats == MarketMutationStats(0, 0, 0, 0, 1)
    assert result.final_count == 0
    assert _rows(market_db) == []


def test_empty_target_is_populated_by_differential_insert(market_db: MarketDb) -> None:
    _seed_prices(market_db, "7203", [10, 11, 12, 13, 14, 15, 16, 17, 18])

    result, spy = _rebuild_with_spy(market_db)

    assert result.stats == MarketMutationStats(1, 1, 0, 0, 0)
    assert any(
        statement.startswith("INSERT INTO DAILY_TECHNICAL_METRICS")
        for statement in spy.statements
    )


def test_dml_failure_rolls_back_and_cleans_temporary_relation(
    market_db: MarketDb,
) -> None:
    _seed_prices(market_db, "7203", [10, 11, 12, 13, 14, 15, 16, 17, 18])
    market_db.rebuild_daily_technical_metrics_from_stock_data()
    before = _rows(market_db)
    market_db._execute(
        "UPDATE stock_data SET close = 30 WHERE code = '7203' AND date = '2024-01-09'"
    )
    spy = _RecordingConnection(
        market_db._conn, fail_on="UPDATE DAILY_TECHNICAL_METRICS"
    )

    with pytest.raises(RuntimeError, match="injected technical metric DML failure"):
        rebuild_daily_technical_metrics_from_stock_data(
            spy, RLock(), market_db._table_exists
        )

    assert _rows(market_db) == before
    assert any(statement.startswith("ROLLBACK") for statement in spy.statements)
    temp_tables = market_db._execute(
        "SELECT table_name FROM duckdb_tables() WHERE temporary"
    ).fetchall()
    assert temp_tables == []


def test_missing_stock_data_returns_zero_without_mutating_target(tmp_path: Path) -> None:
    class _MissingSourceConnection:
        def execute(self, _sql: str, _parameters: Any = None) -> Any:
            raise AssertionError("connection must not be used when source is absent")

    result = rebuild_daily_technical_metrics_from_stock_data(
        _MissingSourceConnection(), RLock(), lambda _table: False
    )

    assert result == TechnicalMetricRebuildResult(MarketMutationStats.empty(), 0)
