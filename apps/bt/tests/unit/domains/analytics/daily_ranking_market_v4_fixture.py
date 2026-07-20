"""Upgrade legacy Daily Ranking test fixtures to the canonical Market v4 inputs."""

from __future__ import annotations

from typing import Any


def upgrade_daily_ranking_fixture_to_market_v4(conn: Any) -> None:
    """Add raw prices and one ready event-time basis per fixture symbol."""

    conn.execute(
        """
        CREATE TABLE market_schema_version (
            version INTEGER,
            migrated_at TIMESTAMP,
            migration_from INTEGER
        )
        """
    )
    conn.execute("INSERT INTO market_schema_version VALUES (4, NULL, NULL)")
    conn.execute("CREATE TABLE sync_metadata (key VARCHAR, value VARCHAR)")
    conn.execute(
        "INSERT INTO sync_metadata VALUES "
        "('stock_price_adjustment_mode', 'local_projection_v2_event_time')"
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
            adjustment_factor DOUBLE
        )
        """
    )
    conn.execute(
        """
        INSERT INTO stock_data_raw
        SELECT CAST(code AS VARCHAR), CAST(date AS DATE),
               CAST(open AS DOUBLE), CAST(high AS DOUBLE), CAST(low AS DOUBLE),
               CAST(close AS DOUBLE), CAST(volume AS BIGINT), 1.0
        FROM stock_data
        """
    )
    conn.execute(
        """
        CREATE TABLE stock_adjustment_bases (
            code VARCHAR,
            basis_id VARCHAR,
            valid_from DATE,
            valid_to_exclusive DATE,
            adjustment_through_date DATE,
            source_fingerprint VARCHAR,
            materialized_through_date DATE,
            status VARCHAR
        )
        """
    )
    conn.execute(
        """
        INSERT INTO stock_adjustment_bases
        SELECT code,
               'event-pit-v1:' || code || ':' || CAST(min(date) AS VARCHAR),
               min(date), NULL, min(date), 'unit-fixture', max(date), 'ready'
        FROM stock_data_raw
        GROUP BY code
        """
    )
    conn.execute(
        """
        CREATE TABLE stock_adjustment_basis_segments (
            code VARCHAR,
            basis_id VARCHAR,
            source_date_from DATE,
            source_date_to_exclusive DATE,
            cumulative_factor DOUBLE
        )
        """
    )
    conn.execute(
        """
        INSERT INTO stock_adjustment_basis_segments
        SELECT code, basis_id, valid_from, NULL, 1.0
        FROM stock_adjustment_bases
        """
    )
    conn.execute(
        """
        UPDATE daily_valuation AS valuation
        SET basis_version = basis.basis_id
        FROM stock_adjustment_bases basis
        WHERE basis.code = valuation.code
        """
    )
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
