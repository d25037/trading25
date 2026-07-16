from __future__ import annotations

import ast
from pathlib import Path

import pytest

from src.infrastructure.db.market.market_db import MarketDb
from tests.unit.server.db.market_writer_test_support import open_market_db
from src.infrastructure.db.market.market_mutations import MarketMutationStats


def _row(
    date: str = "2026-02-10",
    code: str = "7203",
    *,
    name: str = "Toyota",
    scale: str = "TOPIX Core30",
    created_at: str = "created-first",
) -> dict[str, str]:
    return {
        "date": date,
        "code": code,
        "company_name": name,
        "company_name_english": name.upper(),
        "market_code": "0111",
        "market_name": "Prime",
        "sector_17_code": "6",
        "sector_17_name": "Automobiles",
        "sector_33_code": "3700",
        "sector_33_name": "Transportation Equipment",
        "scale_category": scale,
        "listed_date": "1949-05-16",
        "created_at": created_at,
    }


@pytest.fixture()
def db(tmp_path: Path):
    value = open_market_db(str(tmp_path / "market.duckdb"))
    try:
        yield value
    finally:
        value.close()


def test_stock_master_publication_is_last_wins_and_exact_noop(db: MarketDb) -> None:
    first = db.publish_stock_master_daily_rows(
        [_row(name="loser"), _row(name="winner", created_at="winner-created")]
    )
    assert first.daily.stats == MarketMutationStats(2, 1, 0, 1, 0)
    assert first.daily.affected_codes == frozenset({"7203"})
    assert first.daily.affected_dates == frozenset({"2026-02-10"})
    assert first.membership.stats.mutated_rows == 1
    assert first.intervals.stats.mutated_rows == 1
    assert first.stocks_latest.stats.mutated_rows == 1
    assert first.stocks.stats.mutated_rows == 1

    timestamps = db._fetchone(
        "SELECT created_at FROM stock_master_daily WHERE date='2026-02-10' AND code='7203'"
    )
    derived = db._fetchone(
        "SELECT created_at, updated_at FROM stocks_latest WHERE code='7203'"
    )
    second = db.publish_stock_master_daily_rows(
        [_row(name="winner", created_at="ignored-new-created")]
    )
    assert second.daily.stats == MarketMutationStats(1, 0, 0, 1, 0)
    assert second.mutated_rows == 0
    assert second.derived_evaluated is False
    assert (
        db._fetchone(
            "SELECT created_at FROM stock_master_daily WHERE date='2026-02-10' AND code='7203'"
        )
        == timestamps
    )
    assert (
        db._fetchone(
            "SELECT created_at, updated_at FROM stocks_latest WHERE code='7203'"
        )
        == derived
    )


def test_stock_master_true_change_is_scoped_and_membership_exact_delta(
    db: MarketDb,
) -> None:
    db.publish_stock_master_daily_rows(
        [_row(code="7203"), _row(code="6758", name="Sony")]
    )
    untouched = db._fetchone(
        "SELECT created_at, updated_at FROM stocks_latest WHERE code='6758'"
    )
    changed = db.publish_stock_master_daily_rows(
        [
            _row(
                code="7203",
                name="Toyota Motor",
                scale="TOPIX Small 1",
                created_at="new",
            )
        ]
    )
    assert changed.daily.stats == MarketMutationStats(1, 0, 1, 0, 0)
    assert changed.membership.stats.deleted == 1
    assert db.get_index_membership_codes("2026-02-10", "TOPIX500") == {"6758"}
    assert db._fetchone(
        "SELECT created_at FROM stock_master_daily WHERE date='2026-02-10' AND code='7203'"
    ) == ("created-first",)
    assert (
        db._fetchone(
            "SELECT created_at, updated_at FROM stocks_latest WHERE code='6758'"
        )
        == untouched
    )


def test_membership_delta_is_repaired_without_evaluating_other_derived_tables(
    db: MarketDb,
) -> None:
    row = _row()
    db.publish_stock_master_daily_rows([row])
    db._execute(
        "DELETE FROM index_membership_daily WHERE date='2026-02-10' AND code='7203'"
    )
    repaired = db.publish_stock_master_daily_rows([row])
    assert repaired.daily.mutated_rows == 0
    assert repaired.membership.stats.inserted == 1
    assert repaired.derived_evaluated is False
    assert db.get_index_membership_codes("2026-02-10", "TOPIX500") == {"7203"}

    non_member = _row("2026-02-11", scale="TOPIX Small 1")
    db.publish_stock_master_daily_rows([non_member])
    db._execute(
        "INSERT INTO index_membership_daily VALUES ('2026-02-11', 'TOPIX500', '7203', 'stale')"
    )
    deleted = db.publish_stock_master_daily_rows([non_member])
    assert deleted.daily.mutated_rows == 0
    assert deleted.membership.stats.deleted == 1
    assert deleted.derived_evaluated is False


def test_stock_master_intervals_only_replace_affected_code(db: MarketDb) -> None:
    db.publish_stock_master_daily_rows(
        [
            _row("2026-02-09", "7203"),
            _row("2026-02-10", "7203"),
            _row("2026-02-09", "6758", name="Sony"),
            _row("2026-02-10", "6758", name="Sony"),
        ]
    )
    sony_before = db._fetchall("SELECT * FROM stock_master_intervals WHERE code='6758'")
    result = db.publish_stock_master_daily_rows(
        [_row("2026-02-10", "7203", name="Toyota Motor")]
    )
    assert result.intervals.affected_codes == frozenset({"7203"})
    assert (
        db._fetchall("SELECT * FROM stock_master_intervals WHERE code='6758'")
        == sony_before
    )


def test_historical_change_does_not_churn_latest_and_frontier_only_updates_received_code(
    db: MarketDb,
) -> None:
    db.publish_stock_master_daily_rows(
        [
            _row("2026-02-09", "7203"),
            _row("2026-02-09", "6758", name="Sony"),
            _row("2026-02-10", "7203"),
            _row("2026-02-10", "6758", name="Sony"),
        ]
    )
    latest_before = db._fetchall(
        "SELECT code, source_date, created_at, updated_at FROM stocks_latest ORDER BY code"
    )
    historical = db.publish_stock_master_daily_rows(
        [_row("2026-02-09", "7203", name="Historical correction")]
    )
    assert historical.stocks_latest.mutated_rows == 0
    assert (
        db._fetchall(
            "SELECT code, source_date, created_at, updated_at FROM stocks_latest ORDER BY code"
        )
        == latest_before
    )

    frontier = db.publish_stock_master_daily_rows(
        [_row("2026-02-11", "7203", name="Frontier")]
    )
    assert frontier.stocks_latest.affected_codes == frozenset({"7203"})
    assert db._fetchone("SELECT source_date FROM stocks_latest WHERE code='7203'") == (
        "2026-02-11",
    )
    assert db._fetchone("SELECT source_date FROM stocks_latest WHERE code='6758'") == (
        "2026-02-10",
    )

    reconciled = db.reconcile_stock_master_frontier("2026-02-11")
    assert reconciled.stocks_latest.stats.deleted == 1
    assert reconciled.stocks.stats.deleted == 1
    assert db._fetchone("SELECT code FROM stocks_latest WHERE code='6758'") is None
    assert db._fetchone("SELECT code FROM stocks WHERE code='6758'") is None


def test_stock_master_empty_and_invalid_rows_are_zero_stats(db: MarketDb) -> None:
    assert db.publish_stock_master_daily_rows([]).mutated_rows == 0
    invalid = db.publish_stock_master_daily_rows(
        [{"date": "", "code": "7203"}, {"date": "2026-02-10", "code": ""}]
    )
    assert invalid.daily.stats == MarketMutationStats.empty()
    assert invalid.mutated_rows == 0


def test_index_master_merged_value_is_anti_diffed(db: MarketDb) -> None:
    first = db.upsert_index_master(
        [
            {
                "code": "0000",
                "name": "TOPIX",
                "category": "topix",
                "data_start_date": "2020-01-01",
                "created_at": "first",
            }
        ]
    )
    row = db._fetchone(
        "SELECT created_at, updated_at FROM index_master WHERE code='0000'"
    )
    same = db.upsert_index_master(
        [
            {
                "code": "0000",
                "name": "TOPIX",
                "category": "topix",
                "data_start_date": "2021-01-01",
                "created_at": "ignored",
            }
        ]
    )
    assert (
        db._fetchone("SELECT created_at, updated_at FROM index_master WHERE code='0000'")
        == row
    )
    earlier = db.upsert_index_master(
        [
            {
                "code": "0000",
                "name": "TOPIX",
                "category": "topix",
                "data_start_date": "2019-01-01",
                "created_at": "ignored",
            }
        ]
    )
    assert first.stats == MarketMutationStats(1, 1, 0, 0, 0)
    assert same.stats == MarketMutationStats(1, 0, 0, 1, 0)
    assert db._fetchone("SELECT created_at FROM index_master WHERE code='0000'") == (
        "first",
    )
    assert db._fetchone("SELECT updated_at FROM index_master WHERE code='0000'") != (
        row[1],
    )
    assert earlier.stats == MarketMutationStats(1, 0, 1, 0, 0)


def test_direct_stocks_exact_noop_preserves_timestamps(db: MarketDb) -> None:
    source = {key: value for key, value in _row().items() if key != "date"}
    first = db.upsert_stocks([source])
    timestamps = db._fetchone(
        "SELECT created_at, updated_at FROM stocks WHERE code='7203'"
    )
    second = db.upsert_stocks([dict(source, created_at="ignored")])
    assert first.stats.inserted == 1
    assert second.stats.unchanged == 1
    assert db._fetchone(
        "SELECT created_at, updated_at FROM stocks WHERE code='7203'"
    ) == timestamps


def test_stock_master_legacy_writers_are_absent() -> None:
    root = Path(__file__).parents[4] / "src" / "infrastructure" / "db" / "market"
    market_db_tree = ast.parse((root / "market_db.py").read_text())
    writer_tree = ast.parse((root / "stock_master_writers.py").read_text())
    market_methods = {
        node.name
        for node in ast.walk(market_db_tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    writer_functions = {
        node.name
        for node in ast.walk(writer_tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    assert "upsert_stock_master_daily" not in market_methods
    assert "upsert_stock_master_daily" not in writer_functions
    assert "rebuild_stock_master_intervals" not in market_methods
    assert "rebuild_stocks_latest" not in market_methods
