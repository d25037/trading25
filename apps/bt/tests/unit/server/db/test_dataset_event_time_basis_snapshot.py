from __future__ import annotations

from pathlib import Path
import json

import duckdb
import pytest

from src.infrastructure.db.dataset_io.dataset_writer import (
    DatasetSnapshotError,
    DatasetWriter,
    ProviderSnapshotCopyResult,
)
from src.infrastructure.db.market.dataset_snapshot_reader import DatasetSnapshotReader
from src.infrastructure.db.market.market_reader import MarketDbReader
from src.application.services.dataset_snapshot_selection import load_global_cutoff
from src.shared.provider_stock_window import (
    provider_stock_source_fingerprint,
)
from tests.unit.server.db.market_writer_test_support import open_market_db
from tests.unit.server.test_dataset_snapshot_reader import _write_manifest


_DATES = ("2024-01-04", "2024-01-05")
_THREE_DATES = ("2024-01-04", "2024-01-05", "2024-01-08")
_FUNDAMENTALS_FINGERPRINT = "2" * 64


def test_dataset_v4_contract_has_no_retained_basis_graph() -> None:
    from src.infrastructure.db.dataset_io import snapshot_contract

    required = snapshot_contract.MARKET_V5_PROVIDER_REQUIRED_TABLES
    exports = snapshot_contract.DATASET_V4_PARQUET_ARTIFACT_NAMES

    assert "stock_adjustment_bases" not in required
    assert "stock_adjustment_basis_segments" not in required
    assert "stock_adjustment_bases.parquet" not in exports
    assert "stock_adjustment_basis_segments.parquet" not in exports
    assert {
        "stock_data_raw",
        "stock_provider_windows",
        "current_basis_fundamentals_state",
        "statement_metrics_adjusted",
        "daily_valuation",
    } <= required


def test_dataset_v4_json_contract_matches_exact_writer_schema() -> None:
    from src.infrastructure.db.dataset_io.snapshot_contract import (
        DATASET_V4_PHYSICAL_SCHEMAS,
    )

    contract_path = Path(__file__).parents[6] / "contracts/dataset-db-schema-v4.json"
    payload = json.loads(contract_path.read_text(encoding="utf-8"))
    tables = payload["properties"]["tables"]
    assert set(tables["required"]) == set(DATASET_V4_PHYSICAL_SCHEMAS)
    assert set(tables["properties"]) == set(DATASET_V4_PHYSICAL_SCHEMAS)
    type_prefix = {"VARCHAR": "text", "DOUBLE": "real", "BIGINT": "integer"}
    for table, (columns, primary_key) in DATASET_V4_PHYSICAL_SCHEMAS.items():
        properties = tables["properties"][table]["properties"]
        contract_columns = properties["columns"]
        assert contract_columns["required"] == [name for name, _, _ in columns]
        assert contract_columns["properties"] == {
            name: {
                "$ref": (
                    f"#/$defs/{type_prefix[data_type]}_"
                    f"{'not_null' if not_null else 'nullable'}"
                )
            }
            for name, data_type, not_null in columns
        }
        assert properties["primary_key"]["const"] == list(primary_key)
        assert properties["indexes"]["const"] == []


def _insert_provider_code(
    conn: duckdb.DuckDBPyConnection,
    *,
    code: str,
    name: str,
    dates: tuple[str, ...] = _DATES,
    coverage_start: str | None = None,
    coverage_end: str | None = None,
    provider_plan: str = "premium",
    provider_as_of: str | None = None,
    window_fingerprint: str | None = None,
    fundamentals_fingerprint: str = _FUNDAMENTALS_FINGERPRINT,
) -> None:
    coverage_start = coverage_start or dates[0]
    coverage_end = coverage_end or dates[-1]
    provider_as_of = provider_as_of or coverage_end
    provider_rows: list[dict[str, object]] = []
    for index, session in enumerate(dates):
        raw_close = 100.0 + index
        adjusted_close = raw_close * 2
        conn.execute(
            """
            INSERT INTO stock_data_raw VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                code,
                session,
                raw_close - 1,
                raw_close + 1,
                raw_close - 2,
                raw_close,
                1000 + index,
                100000.0,
                0.5,
                (raw_close - 1) * 2,
                (raw_close + 1) * 2,
                (raw_close - 2) * 2,
                adjusted_close,
                (1000 + index) // 2,
                None,
            ),
        )
        provider_rows.append(
            {
                "code": code,
                "date": session,
                "open": raw_close - 1,
                "high": raw_close + 1,
                "low": raw_close - 2,
                "close": raw_close,
                "volume": 1000 + index,
                "turnover_value": 100000.0,
                "adjustment_factor": 0.5,
                "adjusted_open": (raw_close - 1) * 2,
                "adjusted_high": (raw_close + 1) * 2,
                "adjusted_low": (raw_close - 2) * 2,
                "adjusted_close": adjusted_close,
                "adjusted_volume": (1000 + index) // 2,
            }
        )
        conn.execute(
            "INSERT INTO stock_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)",
            (
                code,
                session,
                (raw_close - 1) * 2,
                (raw_close + 1) * 2,
                (raw_close - 2) * 2,
                adjusted_close,
                (1000 + index) // 2,
                0.5,
            ),
        )
        conn.execute(
            """
            INSERT INTO stock_master_daily
            (date, code, company_name, market_code, market_name,
             sector_17_code, sector_17_name, sector_33_code, sector_33_name,
             listed_date)
            VALUES (?, ?, ?, '0111', 'Prime', '7', 'Transport', '3050', 'Auto',
                    '1949-05-16')
            """,
            (session, code, name),
        )
    conn.execute(
        """
        INSERT INTO stock_provider_windows (
            code, coverage_start, coverage_end, provider_plan, provider_as_of,
            source_fingerprint, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            code,
            coverage_start,
            coverage_end,
            provider_plan,
            provider_as_of,
            window_fingerprint or provider_stock_source_fingerprint(provider_rows),
            "2024-01-05T16:00:00+09:00",
        ),
    )
    statement_id = f"statement-{code}"
    conn.execute(
        """
        INSERT INTO statements
        (code, statement_id, disclosed_date, disclosed_at, period_start, period_end,
         earnings_per_share, profit, equity, type_of_current_period,
         type_of_document, bps, forecast_eps, shares_outstanding, treasury_shares)
        VALUES (?, ?, '2024-01-03', '2024-01-03T15:00:00+09:00',
                '2023-01-01', '2023-12-31', 10, 100, 1000, 'FY', 'FY', 50, 12,
                1000000, 10000)
        """,
        (code, statement_id),
    )
    conn.execute(
        """
        INSERT INTO statement_metrics_adjusted
        (code, statement_id, disclosed_date, disclosed_at, period_end, period_type,
         fundamentals_adjustment_basis_date, raw_eps, adjusted_eps, raw_bps,
         adjusted_bps, raw_forecast_eps, adjusted_forecast_eps,
         raw_shares_outstanding, adjusted_shares_outstanding,
         raw_treasury_shares, adjusted_treasury_shares,
         adjustment_factor_cumulative, source_fingerprint)
        VALUES (?, ?, '2024-01-03', '2024-01-03T15:00:00+09:00', '2023-12-31',
                'FY', ?, 10, 20, 50, 100, 12, 24, 1000000, 500000, 10000,
                5000, 2, ?)
        """,
        (code, statement_id, coverage_end, fundamentals_fingerprint),
    )
    conn.execute(
        "INSERT INTO current_basis_fundamentals_state VALUES (?, ?, ?, 1, ?)",
        (code, coverage_end, fundamentals_fingerprint, "2024-01-05T16:00:00+09:00"),
    )


def _build_v5_provider_market(
    tmp_path: Path,
    *,
    two_codes: bool = False,
    dates: tuple[str, ...] = _DATES,
) -> Path:
    source = tmp_path / "market-v5.duckdb"
    db = open_market_db(str(source))
    db.close()
    conn = duckdb.connect(str(source))
    try:
        conn.execute(
            """
            INSERT INTO sync_metadata (key, value) VALUES ('provider_plan', 'premium')
            ON CONFLICT (key) DO UPDATE SET value = excluded.value
            """
        )
        conn.executemany(
            "INSERT INTO topix_data VALUES (?, 1, 1, 1, 1, NULL)",
            [(session,) for session in dates],
        )
        _insert_provider_code(conn, code="7203", name="Toyota", dates=dates)
        if two_codes:
            _insert_provider_code(conn, code="6758", name="Sony", dates=dates)
    finally:
        conn.close()
    return source


def _build_readable_provider_snapshot(tmp_path: Path) -> Path:
    source = _build_v5_provider_market(tmp_path)
    snapshot, _ = _copy_snapshot(tmp_path, source)
    return snapshot


def test_provider_copy_uses_window_plan_when_global_metadata_is_stale(
    tmp_path: Path,
) -> None:
    source = _build_v5_provider_market(tmp_path)
    conn = duckdb.connect(str(source))
    try:
        conn.execute(
            "UPDATE sync_metadata SET value = 'free' WHERE key = 'provider_plan'"
        )
    finally:
        conn.close()

    snapshot, _result = _copy_snapshot(tmp_path, source)
    reader = DatasetSnapshotReader(str(snapshot))
    try:
        assert reader.manifest.source.providerPlan == "premium"
    finally:
        reader.close()


def test_provider_copy_rejects_mixed_plans_before_destination_mutation(
    tmp_path: Path,
) -> None:
    source = _build_v5_provider_market(tmp_path, two_codes=True)
    conn = duckdb.connect(str(source))
    try:
        conn.execute(
            "UPDATE stock_provider_windows SET provider_plan = 'free' "
            "WHERE code = '6758'"
        )
    finally:
        conn.close()
    writer = DatasetWriter(str(tmp_path / "dataset"))

    with pytest.raises(DatasetSnapshotError, match="provider plan"):
        writer.copy_provider_snapshot_from_source(
            source_duckdb_path=str(source),
            normalized_codes=["7203", "6758"],
            date_from=_DATES[0],
            date_to=_DATES[-1],
        )

    assert {
        table: writer._duckdb_store._conn.execute(  # noqa: SLF001
            f"SELECT COUNT(*) FROM {table}"
        ).fetchone()[0]
        for table in (
            "stock_data_raw",
            "stock_master_daily",
            "statement_metrics_adjusted",
            "daily_valuation",
        )
    } == {
        "stock_data_raw": 0,
        "stock_master_daily": 0,
        "statement_metrics_adjusted": 0,
        "daily_valuation": 0,
    }
    writer.close()


def _copy_snapshot(
    tmp_path: Path,
    source: Path,
    *,
    codes: tuple[str, ...] = ("7203",),
    date_from: str = _DATES[0],
    date_to: str = _DATES[-1],
) -> tuple[Path, ProviderSnapshotCopyResult]:
    snapshot = tmp_path / "dataset"
    writer = DatasetWriter(str(snapshot))
    writer.upsert_stocks(
        [
            {
                "code": code,
                "company_name": "Toyota" if code == "7203" else "Sony",
                "market_code": "0111",
                "market_name": "Prime",
                "sector_17_code": "7",
                "sector_17_name": "Transport",
                "sector_33_code": "3050",
                "sector_33_name": "Auto",
                "listed_date": "1949-05-16",
            }
            for code in codes
        ]
    )
    _seed_destination_prices(writer, source, codes)
    writer.copy_topix_data_from_source(
        source_duckdb_path=str(source),
        date_from=date_from,
        date_to=date_to,
    )
    result = writer.copy_provider_snapshot_from_source(
        source_duckdb_path=str(source),
        normalized_codes=list(codes),
        date_from=date_from,
        date_to=date_to,
    )
    writer.set_dataset_info("manifest_schema_version", "4")
    writer.set_dataset_info("source_market_schema_version", "5")
    writer.set_dataset_info("source_stock_price_adjustment_mode", "provider_adjusted_v1")
    writer.set_dataset_info("preset", "quickTesting")
    writer.close()
    _write_manifest(snapshot)
    return snapshot, result


def _seed_destination_prices(
    writer: DatasetWriter,
    source: Path,
    codes: tuple[str, ...] = ("7203",),
) -> None:
    conn = duckdb.connect(str(source), read_only=True)
    try:
        rows = conn.execute(
            "SELECT code, date, open, high, low, close, volume, adjustment_factor, created_at "
            "FROM stock_data WHERE code IN (" + ",".join("?" for _ in codes) + ")",
            codes,
        ).fetchall()
    finally:
        conn.close()
    writer.upsert_stock_data(
        [
            dict(
                zip(
                    (
                        "code", "date", "open", "high", "low", "close", "volume",
                        "adjustment_factor", "created_at",
                    ),
                    row,
                    strict=True,
                )
            )
            for row in rows
        ]
    )


def _prepare_provider_copy_writer(
    tmp_path: Path,
    source: Path,
    *,
    dates: tuple[str, ...] = _DATES,
) -> DatasetWriter:
    writer = DatasetWriter(str(tmp_path / "dataset"))
    writer.upsert_stocks(
        [{
            "code": "7203", "company_name": "Toyota", "market_code": "0111",
            "market_name": "Prime", "sector_17_code": "7",
            "sector_17_name": "Transport", "sector_33_code": "3050",
            "sector_33_name": "Auto", "listed_date": "1949-05-16",
        }]
    )
    _seed_destination_prices(writer, source)
    writer.copy_topix_data_from_source(
        source_duckdb_path=str(source),
        date_from=dates[0],
        date_to=dates[-1],
    )
    return writer


def _refresh_declared_provider_fingerprint(conn: duckdb.DuckDBPyConnection) -> None:
    columns = (
        "code", "date", "open", "high", "low", "close", "volume",
        "turnover_value", "adjustment_factor", "adjusted_open", "adjusted_high",
        "adjusted_low", "adjusted_close", "adjusted_volume",
    )
    rows = conn.execute(
        f"SELECT {', '.join(columns)} FROM stock_data_raw "
        "WHERE code = '7203' ORDER BY date"
    ).fetchall()
    fingerprint = provider_stock_source_fingerprint(
        [dict(zip(columns, row, strict=True)) for row in rows]
    )
    conn.execute(
        "UPDATE stock_provider_windows SET source_fingerprint = ? WHERE code = '7203'",
        (fingerprint,),
    )


def test_copy_provider_snapshot_is_bounded_and_drops_basis_graph(tmp_path: Path) -> None:
    source = _build_v5_provider_market(tmp_path)
    snapshot, result = _copy_snapshot(tmp_path, source)

    assert result == ProviderSnapshotCopyResult(2, 2, 1, 1, 2)
    conn = duckdb.connect(str(snapshot / "dataset.duckdb"), read_only=True)
    try:
        tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
        assert "stock_adjustment_bases" not in tables
        assert "stock_adjustment_basis_segments" not in tables
        assert conn.execute("SELECT min(date), max(date) FROM stock_data_raw").fetchone() == _DATES
        assert conn.execute("SELECT close FROM stock_data ORDER BY date").fetchall() == [
            (200.0,),
            (202.0,),
        ]
    finally:
        conn.close()


def test_manifest_pins_provider_vintage_and_reader_uses_current_metrics(
    tmp_path: Path,
) -> None:
    source = _build_v5_provider_market(tmp_path)
    snapshot, _ = _copy_snapshot(tmp_path, source)

    reader = DatasetSnapshotReader(str(snapshot))
    try:
        assert reader.get_snapshot_lineage() == (4, 5, "provider_adjusted_v1")
        assert reader.manifest.source.providerPlan == "premium"
        assert reader.manifest.source.providerAsOf == _DATES[-1]
        assert reader.manifest.source.providerCoverageStart == _DATES[0]
        assert reader.manifest.source.providerCoverageEnd == _DATES[-1]
        assert len(reader.manifest.source.providerSourceFingerprint) == 64
        assert reader.get_adjusted_statement_metrics("7203")[0]["statement_id"] == (
            "statement-7203"
        )
        assert len(reader.get_daily_valuation("7203")) == 2
    finally:
        reader.close()


def test_provider_copy_rejects_market_v4_before_mutation(tmp_path: Path) -> None:
    source = _build_v5_provider_market(tmp_path)
    conn = duckdb.connect(str(source))
    try:
        conn.execute("UPDATE market_schema_version SET version = 4")
    finally:
        conn.close()
    writer = DatasetWriter(str(tmp_path / "dataset"))
    with pytest.raises(DatasetSnapshotError, match="Market schema version 5"):
        writer.copy_provider_snapshot_from_source(
            source_duckdb_path=str(source),
            normalized_codes=["7203"],
            date_from=_DATES[0],
            date_to=_DATES[-1],
        )
    assert writer.get_stock_data_count() == 0
    writer.close()


def test_provider_copy_rejects_pending_current_basis_recompute(tmp_path: Path) -> None:
    source = _build_v5_provider_market(tmp_path)
    conn = duckdb.connect(str(source))
    try:
        conn.execute(
            "INSERT INTO current_basis_recompute_pending VALUES "
            "('7203', 'refresh', ?, '2024-01-05T16:00:00+09:00')",
            (_FUNDAMENTALS_FINGERPRINT,),
        )
    finally:
        conn.close()
    writer = DatasetWriter(str(tmp_path / "dataset"))
    with pytest.raises(DatasetSnapshotError, match="unresolved current-basis"):
        writer.copy_provider_snapshot_from_source(
            source_duckdb_path=str(source),
            normalized_codes=["7203"],
            date_from=_DATES[0],
            date_to=_DATES[-1],
        )
    writer.close()


@pytest.mark.parametrize(
    ("sql", "message"),
    [
        (
            "UPDATE stock_data_raw SET adjusted_close = adjusted_close + 1 "
            "WHERE code = '7203' AND date = '2024-01-04'",
            "(differs from provider-adjusted raw values|provider source fingerprint)",
        ),
        (
            "DELETE FROM stock_master_daily WHERE code = '7203' AND date = '2024-01-04'",
            "(missing exact daily master|empty, gap, or bound mismatch)",
        ),
        (
            "DELETE FROM statement_metrics_adjusted WHERE code = '7203'",
            "current-basis statement state is inconsistent",
        ),
    ],
)
def test_provider_copy_fail_closed_on_incomplete_source(
    tmp_path: Path,
    sql: str,
    message: str,
) -> None:
    source = _build_v5_provider_market(tmp_path)
    conn = duckdb.connect(str(source))
    try:
        conn.execute(sql)
    finally:
        conn.close()
    writer = DatasetWriter(str(tmp_path / "dataset"))
    writer.upsert_stocks(
        [{
            "code": "7203", "company_name": "Toyota", "market_code": "0111",
            "market_name": "Prime", "sector_17_code": "7",
            "sector_17_name": "Transport", "sector_33_code": "3050",
            "sector_33_name": "Auto", "listed_date": "1949-05-16",
        }]
    )
    _seed_destination_prices(writer, source)
    with pytest.raises(DatasetSnapshotError, match=message):
        writer.copy_provider_snapshot_from_source(
            source_duckdb_path=str(source),
            normalized_codes=["7203"],
            date_from=_DATES[0],
            date_to=_DATES[-1],
        )
    writer.close()


def test_provider_copy_rejects_incoherent_selected_vintage(tmp_path: Path) -> None:
    source = _build_v5_provider_market(tmp_path, two_codes=True)
    conn = duckdb.connect(str(source))
    try:
        conn.execute("UPDATE stock_provider_windows SET provider_as_of = '2024-01-06' WHERE code = '6758'")
    finally:
        conn.close()
    writer = DatasetWriter(str(tmp_path / "dataset"))
    with pytest.raises(DatasetSnapshotError, match="one provider vintage"):
        writer.copy_provider_snapshot_from_source(
            source_duckdb_path=str(source),
            normalized_codes=["7203", "6758"],
            date_from=_DATES[0],
            date_to=_DATES[-1],
        )
    writer.close()


def test_provider_snapshot_is_immutable_on_source_correction(tmp_path: Path) -> None:
    source = _build_v5_provider_market(tmp_path)
    snapshot, _ = _copy_snapshot(tmp_path, source)
    conn = duckdb.connect(str(source))
    try:
        conn.execute("UPDATE stock_data_raw SET adjusted_close = 999 WHERE date = '2024-01-04'")
        conn.execute("UPDATE stock_data SET close = 999 WHERE date = '2024-01-04'")
    finally:
        conn.close()
    writer = DatasetWriter(str(snapshot))
    before = _provider_destination_state(writer)
    with pytest.raises(
        DatasetSnapshotError,
        match="(immutable Dataset provider snapshot|differs from provider-adjusted raw values|provider source fingerprint)",
    ):
        writer.copy_provider_snapshot_from_source(
            source_duckdb_path=str(source),
            normalized_codes=["7203"],
            date_from=_DATES[0],
            date_to=_DATES[-1],
        )
    assert _provider_destination_state(writer) == before
    writer.close()


def test_provider_snapshot_stays_immutable_when_source_rows_are_deleted(
    tmp_path: Path,
) -> None:
    source = _build_v5_provider_market(tmp_path)
    snapshot, _ = _copy_snapshot(tmp_path, source)
    conn = duckdb.connect(str(source))
    try:
        conn.execute(
            "DELETE FROM stock_data_raw WHERE code = '7203' AND date = '2024-01-04'"
        )
    finally:
        conn.close()
    writer = DatasetWriter(str(snapshot))
    before = _provider_destination_state(writer)
    with pytest.raises(DatasetSnapshotError, match="provider source fingerprint"):
        writer.copy_provider_snapshot_from_source(
            source_duckdb_path=str(source),
            normalized_codes=["7203"],
            date_from=_DATES[0],
            date_to=_DATES[-1],
        )
    assert _provider_destination_state(writer) == before
    writer.close()


def _provider_destination_state(writer: DatasetWriter) -> dict[str, list[tuple[object, ...]]]:
    tables = (
        "stocks",
        "stock_data",
        "stock_data_raw",
        "stock_master_daily",
        "statements",
        "statement_metrics_adjusted",
        "daily_valuation",
        "dataset_info",
    )
    conn = writer._duckdb_store._conn  # noqa: SLF001 - immutability proof
    return {
        table: conn.execute(f"SELECT * FROM {table} ORDER BY 1, 2").fetchall()
        if table not in {"stocks", "dataset_info"}
        else conn.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall()
        for table in tables
    }


def test_recopy_rejects_stale_destination_row_without_any_mutation(
    tmp_path: Path,
) -> None:
    source = _build_v5_provider_market(tmp_path, dates=_THREE_DATES)
    snapshot, _ = _copy_snapshot(
        tmp_path,
        source,
        date_from=_THREE_DATES[0],
        date_to=_THREE_DATES[-1],
    )
    writer = DatasetWriter(str(snapshot))
    writer._duckdb_store._conn.execute(  # noqa: SLF001 - stale-row sentinel
        "INSERT INTO stock_data_raw VALUES "
        "('7203', '2024-01-06', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, NULL)"
    )
    before = _provider_destination_state(writer)
    with pytest.raises(DatasetSnapshotError, match="immutable Dataset provider snapshot"):
        writer.copy_provider_snapshot_from_source(
            source_duckdb_path=str(source),
            normalized_codes=["7203"],
            date_from=_THREE_DATES[0],
            date_to=_THREE_DATES[-1],
        )
    assert _provider_destination_state(writer) == before
    writer.close()


def test_provider_copy_rejects_extra_stale_destination_stock_without_mutation(
    tmp_path: Path,
) -> None:
    source = _build_v5_provider_market(tmp_path)
    writer = _prepare_provider_copy_writer(tmp_path, source)
    writer.upsert_stocks(
        [{
            "code": "9999", "company_name": "Stale", "market_code": "0111",
            "market_name": "Prime", "sector_17_code": "",
            "sector_17_name": "", "sector_33_code": "",
            "sector_33_name": "", "listed_date": "",
        }]
    )
    before = _provider_destination_state(writer)
    with pytest.raises(DatasetSnapshotError, match="exactly match coverage-end"):
        writer.copy_provider_snapshot_from_source(
            source_duckdb_path=str(source),
            normalized_codes=["7203"],
            date_from=_DATES[0],
            date_to=_DATES[-1],
        )
    assert _provider_destination_state(writer) == before
    writer.close()


def test_provider_copy_requires_exact_requested_bounds(tmp_path: Path) -> None:
    source = _build_v5_provider_market(tmp_path)
    writer = DatasetWriter(str(tmp_path / "dataset"))
    with pytest.raises(DatasetSnapshotError, match="requested bounds differ"):
        writer.copy_provider_snapshot_from_source(
            source_duckdb_path=str(source),
            normalized_codes=["7203"],
            date_from="2024-01-05",
            date_to="2024-01-05",
        )
    writer.close()


def test_selected_provider_cutoff_ignores_unselected_stale_symbol(tmp_path: Path) -> None:
    source = _build_v5_provider_market(tmp_path, two_codes=True)
    conn = duckdb.connect(str(source))
    try:
        conn.execute(
            "UPDATE stock_provider_windows SET coverage_end = '2024-01-04' "
            "WHERE code = '6758'"
        )
    finally:
        conn.close()
    reader = MarketDbReader(str(source))
    try:
        assert load_global_cutoff(reader, ["7203"]) == _DATES[-1]
    finally:
        reader.close()


@pytest.mark.parametrize(
    "family",
    [
        "raw_interior",
        "consumer_interior",
        "master_interior",
        "valuation_interior",
        "raw_empty",
        "consumer_lower",
        "consumer_upper",
    ],
)
def test_provider_copy_rejects_empty_gap_and_bound_mismatch_per_physical_family(
    tmp_path: Path,
    family: str,
) -> None:
    source = _build_v5_provider_market(tmp_path, dates=_THREE_DATES)
    conn = duckdb.connect(str(source))
    try:
        if family == "valuation_interior":
            conn.execute("CREATE TABLE valuation_materialized AS SELECT * FROM daily_valuation")
            conn.execute("DROP VIEW daily_valuation")
            conn.execute("ALTER TABLE valuation_materialized RENAME TO daily_valuation")
            conn.execute("DELETE FROM daily_valuation WHERE date = '2024-01-05'")
        elif family == "raw_empty":
            conn.execute("DELETE FROM stock_data_raw WHERE code = '7203'")
            _refresh_declared_provider_fingerprint(conn)
        else:
            table, target_date = {
                "raw_interior": ("stock_data_raw", "2024-01-05"),
                "consumer_interior": ("stock_data", "2024-01-05"),
                "master_interior": ("stock_master_daily", "2024-01-05"),
                "consumer_lower": ("stock_data", "2024-01-04"),
                "consumer_upper": ("stock_data", "2024-01-08"),
            }[family]
            conn.execute(
                f"DELETE FROM {table} WHERE code = '7203' AND date = ?",
                (target_date,),
            )
            if table == "stock_data_raw":
                _refresh_declared_provider_fingerprint(conn)
    finally:
        conn.close()
    writer = _prepare_provider_copy_writer(
        tmp_path,
        source,
        dates=_THREE_DATES,
    )
    with pytest.raises(
        DatasetSnapshotError,
        match="(empty, gap, or bound mismatch|coverage is missing)",
    ):
        writer.copy_provider_snapshot_from_source(
            source_duckdb_path=str(source),
            normalized_codes=["7203"],
            date_from=_THREE_DATES[0],
            date_to=_THREE_DATES[-1],
        )
    writer.close()


def test_provider_copy_accepts_shared_suspended_quote_session_subset(
    tmp_path: Path,
) -> None:
    source = _build_v5_provider_market(tmp_path, dates=_THREE_DATES)
    conn = duckdb.connect(str(source))
    try:
        conn.execute(
            "DELETE FROM stock_data_raw WHERE code = '7203' AND date = '2024-01-05'"
        )
        conn.execute(
            "DELETE FROM stock_data WHERE code = '7203' AND date = '2024-01-05'"
        )
        _refresh_declared_provider_fingerprint(conn)
    finally:
        conn.close()
    writer = _prepare_provider_copy_writer(tmp_path, source, dates=_THREE_DATES)

    result = writer.copy_provider_snapshot_from_source(
        source_duckdb_path=str(source),
        normalized_codes=["7203"],
        date_from=_THREE_DATES[0],
        date_to=_THREE_DATES[-1],
    )

    assert result.raw_price_rows == 2
    assert result.stock_master_rows == 3
    assert result.daily_valuation_rows == 2
    writer.set_dataset_info("manifest_schema_version", "4")
    writer.set_dataset_info("source_market_schema_version", "5")
    writer.set_dataset_info("source_stock_price_adjustment_mode", "provider_adjusted_v1")
    writer.set_dataset_info("preset", "quickTesting")
    writer.close()
    snapshot = tmp_path / "dataset"
    _write_manifest(snapshot)

    reader = DatasetSnapshotReader(str(snapshot))
    try:
        assert [row["date"] for row in reader.get_stock_ohlcv("7203")] == [
            "2024-01-04",
            "2024-01-08",
        ]
        assert len(reader.get_daily_valuation("7203")) == 2
    finally:
        reader.close()


def test_writer_snapshot_with_all_lag_valid_basis_dates_resolves_in_reader(
    tmp_path: Path,
) -> None:
    source = _build_v5_provider_market(tmp_path, two_codes=True)
    conn = duckdb.connect(str(source))
    try:
        conn.execute(
            "UPDATE current_basis_fundamentals_state "
            "SET fundamentals_adjustment_basis_date = ?",
            (_DATES[0],),
        )
        conn.execute(
            "UPDATE statement_metrics_adjusted "
            "SET fundamentals_adjustment_basis_date = ?",
            (_DATES[0],),
        )
    finally:
        conn.close()
    writer = DatasetWriter(str(tmp_path / "dataset"))
    writer.upsert_stocks(
        [
            {
                "code": code,
                "company_name": "Toyota" if code == "7203" else "Sony",
                "market_code": "0111",
                "market_name": "Prime",
                "sector_17_code": "7",
                "sector_17_name": "Transport",
                "sector_33_code": "3050",
                "sector_33_name": "Auto",
                "listed_date": "1949-05-16",
            }
            for code in ("7203", "6758")
        ]
    )
    _seed_destination_prices(writer, source, ("7203", "6758"))
    writer.copy_topix_data_from_source(
        source_duckdb_path=str(source),
        date_from=_DATES[0],
        date_to=_DATES[-1],
    )

    result = writer.copy_provider_snapshot_from_source(
        source_duckdb_path=str(source),
        normalized_codes=["7203", "6758"],
        date_from=_DATES[0],
        date_to=_DATES[-1],
    )

    assert result.statement_metric_rows == 2
    writer.set_dataset_info("manifest_schema_version", "4")
    writer.set_dataset_info("source_market_schema_version", "5")
    writer.set_dataset_info("source_stock_price_adjustment_mode", "provider_adjusted_v1")
    writer.set_dataset_info("preset", "quickTesting")
    writer.close()
    snapshot = tmp_path / "dataset"
    _write_manifest(snapshot)

    reader = DatasetSnapshotReader(str(snapshot))
    try:
        metrics = {
            row["code"]: row["fundamentals_adjustment_basis_date"]
            for code in ("7203", "6758")
            for row in reader.get_adjusted_statement_metrics(code)
        }
        assert metrics == {"7203": _DATES[0], "6758": _DATES[0]}
        assert reader.manifest.source.fundamentalsAdjustmentBasisDate == _DATES[0]
    finally:
        reader.close()


def test_provider_copy_rejects_stale_declared_source_fingerprint(tmp_path: Path) -> None:
    source = _build_v5_provider_market(tmp_path)
    conn = duckdb.connect(str(source))
    try:
        conn.execute(
            "UPDATE stock_provider_windows SET source_fingerprint = ? WHERE code = '7203'",
            ("0" * 64,),
        )
    finally:
        conn.close()
    writer = DatasetWriter(str(tmp_path / "dataset"))
    with pytest.raises(DatasetSnapshotError, match="provider source fingerprint"):
        writer.copy_provider_snapshot_from_source(
            source_duckdb_path=str(source),
            normalized_codes=["7203"],
            date_from=_DATES[0],
            date_to=_DATES[-1],
        )
    writer.close()


def test_provider_copy_rejects_conflicting_normalized_alias_atomically(
    tmp_path: Path,
) -> None:
    source = _build_v5_provider_market(tmp_path)
    conn = duckdb.connect(str(source))
    try:
        conn.execute(
            """
            INSERT INTO stock_data_raw
            SELECT '72030', date, open, high, low, close, volume, turnover_value,
                   adjustment_factor, adjusted_open, adjusted_high, adjusted_low,
                   adjusted_close + 1, adjusted_volume, created_at
            FROM stock_data_raw WHERE code = '7203' AND date = '2024-01-04'
            """
        )
    finally:
        conn.close()
    writer = DatasetWriter(str(tmp_path / "dataset"))
    with pytest.raises(DatasetSnapshotError, match="conflicting normalized stock-code aliases"):
        writer.copy_provider_snapshot_from_source(
            source_duckdb_path=str(source),
            normalized_codes=["7203"],
            date_from=_DATES[0],
            date_to=_DATES[-1],
        )
    assert writer.get_stock_data_count() == 0
    writer.close()


def test_provider_copy_deduplicates_identical_alias_and_prefers_four_digit_code(
    tmp_path: Path,
) -> None:
    source = _build_v5_provider_market(tmp_path)
    conn = duckdb.connect(str(source))
    try:
        conn.execute(
            """
            INSERT INTO stock_data_raw
            SELECT '72030', date, open, high, low, close, volume, turnover_value,
                   adjustment_factor, adjusted_open, adjusted_high, adjusted_low,
                   adjusted_close, adjusted_volume, created_at
            FROM stock_data_raw WHERE code = '7203'
            """
        )
    finally:
        conn.close()
    snapshot, result = _copy_snapshot(tmp_path, source)
    assert result.raw_price_rows == len(_DATES)
    conn = duckdb.connect(str(snapshot / "dataset.duckdb"), read_only=True)
    try:
        assert conn.execute(
            "SELECT DISTINCT code FROM stock_data_raw"
        ).fetchall() == [("7203",)]
    finally:
        conn.close()


@pytest.mark.parametrize(
    "changed_column",
    ["earnings_per_share", "forecast_sales", "next_year_forecast_sales"],
)
def test_provider_copy_rejects_conflicting_raw_statement_alias_atomically(
    tmp_path: Path,
    changed_column: str,
) -> None:
    source = _build_v5_provider_market(tmp_path)
    conn = duckdb.connect(str(source))
    try:
        conn.execute(
            f"""
            INSERT INTO statements
            SELECT * REPLACE (
                '72030' AS code,
                coalesce({changed_column}, 0) + 1 AS {changed_column}
            )
            FROM statements WHERE code = '7203'
            """
        )
    finally:
        conn.close()
    writer = DatasetWriter(str(tmp_path / "dataset"))
    with pytest.raises(
        DatasetSnapshotError,
        match="conflicting normalized stock-code aliases in statements",
    ):
        writer.copy_provider_snapshot_from_source(
            source_duckdb_path=str(source),
            normalized_codes=["7203"],
            date_from=_DATES[0],
            date_to=_DATES[-1],
        )
    assert writer.get_stock_data_count() == 0
    writer.close()


def test_provider_copy_deduplicates_only_identical_raw_statement_alias(
    tmp_path: Path,
) -> None:
    source = _build_v5_provider_market(tmp_path)
    conn = duckdb.connect(str(source))
    try:
        conn.execute(
            "INSERT INTO statements SELECT '72030', * EXCLUDE (code) "
            "FROM statements WHERE code = '7203'"
        )
    finally:
        conn.close()
    snapshot, result = _copy_snapshot(tmp_path, source)
    assert result.statement_rows == 1
    conn = duckdb.connect(str(snapshot / "dataset.duckdb"), read_only=True)
    try:
        assert conn.execute(
            "SELECT code, statement_id FROM statements"
        ).fetchall() == [("7203", "statement-7203")]
    finally:
        conn.close()


def test_provider_copy_rejects_metric_raw_period_type_mismatch(tmp_path: Path) -> None:
    source = _build_v5_provider_market(tmp_path)
    conn = duckdb.connect(str(source))
    try:
        conn.execute(
            "UPDATE statement_metrics_adjusted SET period_type = 'Q1' "
            "WHERE code = '7203'"
        )
    finally:
        conn.close()
    writer = DatasetWriter(str(tmp_path / "dataset"))
    writer.upsert_stocks(
        [{
            "code": "7203", "company_name": "Toyota", "market_code": "0111",
            "market_name": "Prime", "sector_17_code": "7",
            "sector_17_name": "Transport", "sector_33_code": "3050",
            "sector_33_name": "Auto", "listed_date": "1949-05-16",
        }]
    )
    _seed_destination_prices(writer, source)
    writer.copy_topix_data_from_source(
        source_duckdb_path=str(source),
        date_from=_DATES[0],
        date_to=_DATES[-1],
    )
    with pytest.raises(DatasetSnapshotError, match="exact raw statement identity"):
        writer.copy_provider_snapshot_from_source(
            source_duckdb_path=str(source),
            normalized_codes=["7203"],
            date_from=_DATES[0],
            date_to=_DATES[-1],
        )
    writer.close()


def test_provider_copy_accepts_statementless_current_provider_snapshot(
    tmp_path: Path,
) -> None:
    source = _build_v5_provider_market(tmp_path)
    conn = duckdb.connect(str(source))
    try:
        conn.execute("DELETE FROM statement_metrics_adjusted WHERE code = '7203'")
        conn.execute("DELETE FROM statements WHERE code = '7203'")
        conn.execute(
            "UPDATE current_basis_fundamentals_state SET statement_count = 0 "
            "WHERE code = '7203'"
        )
    finally:
        conn.close()
    writer = _prepare_provider_copy_writer(tmp_path, source)
    result = writer.copy_provider_snapshot_from_source(
        source_duckdb_path=str(source),
        normalized_codes=["7203"],
        date_from=_DATES[0],
        date_to=_DATES[-1],
    )
    assert result.statement_rows == 0
    assert result.statement_metric_rows == 0
    assert result.daily_valuation_rows == len(_DATES)
    writer.close()
