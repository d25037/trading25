"""Upgrade legacy Daily Ranking fixtures to canonical Market v5 inputs."""

from __future__ import annotations

from typing import Any

from src.shared.provider_stock_window import provider_stock_source_fingerprint


_PROVIDER_RAW_COLUMNS = (
    "code",
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "turnover_value",
    "adjustment_factor",
    "adjusted_open",
    "adjusted_high",
    "adjusted_low",
    "adjusted_close",
    "adjusted_volume",
)


def upgrade_daily_ranking_fixture_to_market_v5(conn: Any) -> None:
    """Add one verified provider-adjusted window per fixture symbol."""

    conn.execute(
        """
        CREATE TABLE market_schema_version (
            version INTEGER,
            applied_at TEXT,
            notes TEXT
        )
        """
    )
    conn.execute("INSERT INTO market_schema_version VALUES (5, NULL, NULL)")
    conn.execute(
        """
        CREATE TABLE sync_metadata (
            key VARCHAR,
            value VARCHAR,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        "INSERT INTO sync_metadata VALUES "
        "('stock_price_adjustment_mode', 'provider_adjusted_v1', NULL)"
    )
    conn.execute(
        """
        CREATE TABLE stock_data_raw (
            code VARCHAR,
            date DATE,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume BIGINT,
            turnover_value DOUBLE,
            adjustment_factor DOUBLE,
            adjusted_open DOUBLE,
            adjusted_high DOUBLE,
            adjusted_low DOUBLE,
            adjusted_close DOUBLE,
            adjusted_volume BIGINT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO stock_data_raw
        SELECT CAST(code AS VARCHAR), CAST(date AS DATE),
               CAST(open AS DOUBLE), CAST(high AS DOUBLE), CAST(low AS DOUBLE),
               CAST(close AS DOUBLE), CAST(volume AS BIGINT),
               CAST(close AS DOUBLE) * CAST(volume AS BIGINT), 1.0,
               CAST(open AS DOUBLE), CAST(high AS DOUBLE), CAST(low AS DOUBLE),
               CAST(close AS DOUBLE), CAST(volume AS BIGINT)
        FROM stock_data
        """
    )
    conn.execute(
        """
        CREATE TABLE stock_provider_windows (
            code VARCHAR,
            coverage_start DATE,
            coverage_end DATE,
            provider_as_of DATE,
            source_fingerprint VARCHAR,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE stock_adjustment_events (
            code VARCHAR,
            date DATE,
            adjustment_factor DOUBLE,
            source_fingerprint VARCHAR
        )
        """
    )
    for (code,) in conn.execute(
        "SELECT DISTINCT code FROM stock_data_raw ORDER BY code"
    ).fetchall():
        refresh_daily_ranking_provider_window(conn, code=str(code))

    conn.execute(
        "ALTER TABLE daily_valuation "
        "ADD COLUMN IF NOT EXISTS fundamentals_adjustment_basis_date DATE"
    )
    conn.execute(
        "ALTER TABLE daily_valuation "
        "ADD COLUMN IF NOT EXISTS source_fingerprint VARCHAR"
    )
    valuation_columns = {
        str(row[1])
        for row in conn.execute("PRAGMA table_info('daily_valuation')").fetchall()
    }
    if "basis_version" in valuation_columns:
        conn.execute("ALTER TABLE daily_valuation DROP COLUMN basis_version")
    existing = {
        str(row[0])
        for row in conn.execute(
            "SELECT table_name FROM information_schema.tables"
        ).fetchall()
    }
    if "indices_data" not in existing:
        conn.execute(
            """
            CREATE TABLE indices_data (
                code VARCHAR,
                date DATE,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT
            )
            """
        )


def refresh_daily_ranking_provider_window(conn: Any, *, code: str) -> str:
    """Refresh one fixture window after an intentional provider-row mutation."""

    rows = [
        dict(zip(_PROVIDER_RAW_COLUMNS, row, strict=True))
        for row in conn.execute(
            f"SELECT {', '.join(_PROVIDER_RAW_COLUMNS)} "
            "FROM stock_data_raw WHERE code = ? ORDER BY date",
            [code],
        ).fetchall()
    ]
    if not rows:
        raise ValueError(f"Fixture provider window has no rows for code={code}")
    for row in rows:
        row["code"] = str(row["code"])
        row["date"] = str(row["date"])
    fingerprint = provider_stock_source_fingerprint(rows)
    conn.execute("DELETE FROM stock_provider_windows WHERE code = ?", [code])
    conn.execute(
        """
        INSERT INTO stock_provider_windows VALUES (?, ?, ?, ?, ?, 'unit-fixture')
        """,
        [code, rows[0]["date"], rows[-1]["date"], rows[-1]["date"], fingerprint],
    )
    conn.execute(
        "UPDATE stock_adjustment_events SET source_fingerprint = ? WHERE code = ?",
        [fingerprint, code],
    )
    return fingerprint
