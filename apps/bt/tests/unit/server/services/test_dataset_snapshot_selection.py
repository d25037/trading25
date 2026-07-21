from __future__ import annotations

import duckdb
import pytest

from src.application.services.dataset_snapshot_selection import (
    DatasetSnapshotSelectionError,
    load_selected_price_range,
)


class _Source:
    def __init__(self) -> None:
        self.conn = duckdb.connect(":memory:")

    def query(self, sql: str, params: tuple[object, ...] = ()) -> list[dict[str, object]]:
        result = self.conn.execute(sql, params)
        columns = [str(item[0]) for item in result.description]
        return [dict(zip(columns, row, strict=True)) for row in result.fetchall()]


def test_selected_price_range_accepts_suspended_no_trade_sessions() -> None:
    source = _Source()
    source.conn.execute(
        """
        CREATE TABLE stock_provider_windows (
            code TEXT, coverage_start TEXT, coverage_end TEXT,
            provider_plan TEXT, provider_as_of TEXT, source_fingerprint TEXT
        );
        CREATE TABLE current_basis_fundamentals_state (
            code TEXT, fundamentals_adjustment_basis_date TEXT,
            source_fingerprint TEXT
        );
        CREATE TABLE stock_data_raw (
            code TEXT, date TEXT, open DOUBLE, high DOUBLE, low DOUBLE,
            close DOUBLE, volume BIGINT, adjusted_open DOUBLE,
            adjusted_high DOUBLE, adjusted_low DOUBLE, adjusted_close DOUBLE,
            adjusted_volume BIGINT
        );
        CREATE TABLE stock_data (code TEXT, date TEXT);
        CREATE TABLE stock_master_daily (code TEXT, date TEXT);
        CREATE TABLE daily_valuation (code TEXT, date TEXT);
        CREATE TABLE topix_data (date TEXT);
        INSERT INTO stock_provider_windows (
            code, coverage_start, coverage_end, provider_plan, provider_as_of,
            source_fingerprint
        ) VALUES
            ('7203', '2026-02-10', '2026-02-12', 'premium', '2026-02-12', 'aaaaaaaa');
        INSERT INTO current_basis_fundamentals_state VALUES
            ('7203', '2026-02-12', 'bbbbbbbb');
        INSERT INTO topix_data VALUES ('2026-02-10'), ('2026-02-11'), ('2026-02-12');
        INSERT INTO stock_master_daily VALUES
            ('7203', '2026-02-10'), ('7203', '2026-02-11'), ('7203', '2026-02-12');
        INSERT INTO stock_data_raw VALUES
            ('7203', '2026-02-10', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1),
            ('7203', '2026-02-12', 2, 2, 2, 2, 2, 2, 2, 2, 2, 2);
        INSERT INTO stock_data VALUES ('7203', '2026-02-10'), ('7203', '2026-02-12');
        INSERT INTO daily_valuation VALUES ('7203', '2026-02-10'), ('7203', '2026-02-12');
        """
    )

    assert load_selected_price_range(source, ["7203"], "2026-02-12") == (
        "2026-02-10",
        "2026-02-12",
    )


def _two_code_provider_source(*, second_plan: str = "premium") -> _Source:
    source = _Source()
    source.conn.execute(
        f"""
        CREATE TABLE stock_provider_windows (
            code TEXT, coverage_start TEXT, coverage_end TEXT,
            provider_plan TEXT, provider_as_of TEXT, source_fingerprint TEXT
        );
        CREATE TABLE current_basis_fundamentals_state (
            code TEXT, fundamentals_adjustment_basis_date TEXT,
            source_fingerprint TEXT
        );
        CREATE TABLE stock_data_raw (
            code TEXT, date TEXT, open DOUBLE, high DOUBLE, low DOUBLE,
            close DOUBLE, volume BIGINT, adjusted_open DOUBLE,
            adjusted_high DOUBLE, adjusted_low DOUBLE, adjusted_close DOUBLE,
            adjusted_volume BIGINT
        );
        CREATE TABLE stock_data (code TEXT, date TEXT);
        CREATE TABLE stock_master_daily (code TEXT, date TEXT);
        CREATE TABLE daily_valuation (code TEXT, date TEXT);
        CREATE TABLE topix_data (date TEXT);
        INSERT INTO stock_provider_windows (
            code, coverage_start, coverage_end, provider_plan, provider_as_of,
            source_fingerprint
        ) VALUES
            ('7203', '2026-02-10', '2026-02-12', 'premium', '2026-02-12', 'aaaaaaaa'),
            ('6758', '2026-02-10', '2026-02-10', '{second_plan}', '2026-02-12', 'bbbbbbbb');
        INSERT INTO current_basis_fundamentals_state VALUES
            ('7203', '2026-02-10', 'cccccccc'),
            ('6758', '2026-02-10', 'dddddddd');
        INSERT INTO topix_data VALUES ('2026-02-10'), ('2026-02-11'), ('2026-02-12');
        INSERT INTO stock_master_daily VALUES
            ('7203', '2026-02-10'), ('7203', '2026-02-11'), ('7203', '2026-02-12'),
            ('6758', '2026-02-10'), ('6758', '2026-02-11'), ('6758', '2026-02-12');
        INSERT INTO stock_data_raw VALUES
            ('7203', '2026-02-10', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1),
            ('7203', '2026-02-12', 2, 2, 2, 2, 2, 2, 2, 2, 2, 2),
            ('6758', '2026-02-10', 3, 3, 3, 3, 3, 3, 3, 3, 3, 3);
        INSERT INTO stock_data VALUES
            ('7203', '2026-02-10'), ('7203', '2026-02-12'),
            ('6758', '2026-02-10');
        INSERT INTO daily_valuation VALUES
            ('7203', '2026-02-10'), ('7203', '2026-02-12'),
            ('6758', '2026-02-10');
        """
    )
    return source


def test_selected_price_range_accepts_suspended_symbol_at_common_provider_frontier() -> (
    None
):
    source = _two_code_provider_source()

    assert load_selected_price_range(source, ["7203", "6758"], "2026-02-10") == (
        "2026-02-10",
        "2026-02-10",
    )


def test_selected_price_range_rejects_mixed_provider_plans() -> None:
    source = _two_code_provider_source(second_plan="free")

    with pytest.raises(DatasetSnapshotSelectionError, match="provider plan"):
        load_selected_price_range(source, ["7203", "6758"], "2026-02-10")
