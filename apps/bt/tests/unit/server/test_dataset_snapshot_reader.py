from __future__ import annotations

import hashlib
import importlib
import json
from pathlib import Path
from typing import Any

import pytest

import src.infrastructure.db.market.dataset_snapshot_reader as snapshot_reader_module
from src.infrastructure.db.dataset_io.dataset_writer import DatasetWriter
from src.infrastructure.db.dataset_io.snapshot_contract import (
    DATASET_FUNDAMENTALS_BASIS_DATE_INFO_KEY,
    DATASET_PROVIDER_AS_OF_INFO_KEY,
    DATASET_PROVIDER_COVERAGE_END_INFO_KEY,
    DATASET_PROVIDER_COVERAGE_START_INFO_KEY,
    DATASET_PROVIDER_PLAN_INFO_KEY,
    DATASET_PROVIDER_SOURCE_FINGERPRINT_INFO_KEY,
)
from src.infrastructure.db.market.dataset_snapshot_reader import (
    DatasetManifestValidationError,
    DatasetSnapshotReader,
    UnsupportedDatasetSnapshotError,
    build_dataset_snapshot_logical_checksum,
    inspect_dataset_snapshot_duckdb,
    validate_dataset_snapshot,
)


def _set_v4_source_info(
    writer: DatasetWriter,
    *,
    coverage_start: str,
    coverage_end: str,
) -> None:
    writer.set_dataset_info("manifest_schema_version", "4")
    writer.set_dataset_info("source_market_schema_version", "5")
    writer.set_dataset_info("source_stock_price_adjustment_mode", "provider_adjusted_v1")
    writer.set_dataset_info(DATASET_PROVIDER_PLAN_INFO_KEY, "premium")
    writer.set_dataset_info(DATASET_PROVIDER_AS_OF_INFO_KEY, coverage_end)
    writer.set_dataset_info(DATASET_PROVIDER_COVERAGE_START_INFO_KEY, coverage_start)
    writer.set_dataset_info(DATASET_PROVIDER_COVERAGE_END_INFO_KEY, coverage_end)
    writer.set_dataset_info(DATASET_PROVIDER_SOURCE_FINGERPRINT_INFO_KEY, "a" * 64)
    writer.set_dataset_info(DATASET_FUNDAMENTALS_BASIS_DATE_INFO_KEY, coverage_end)


def _write_manifest(
    snapshot_dir: Path,
    *,
    schema_version: object = 4,
    source: dict[str, Any] | None = None,
) -> None:
    duckdb_path = snapshot_dir / "dataset.duckdb"
    parquet_dir = snapshot_dir / "parquet"
    inspection = inspect_dataset_snapshot_duckdb(duckdb_path)
    manifest: dict[str, Any] = {
        "schemaVersion": schema_version,
        "generatedAt": "2026-07-21T00:00:00+00:00",
        "dataset": {
            "name": "sample",
            "preset": "quickTesting",
            "duckdbFile": "dataset.duckdb",
            "parquetDir": "parquet",
        },
        "source": source or inspection.source.model_dump(),
        "logicalCounts": inspection.counts.model_dump(),
        "coverage": inspection.coverage.model_dump(),
        "checksums": {
            "duckdbSha256": hashlib.sha256(duckdb_path.read_bytes()).hexdigest(),
            "logicalSha256": build_dataset_snapshot_logical_checksum(
                source=inspection.source,
                counts=inspection.counts,
                coverage=inspection.coverage,
                date_range=inspection.date_range,
            ),
            "parquet": {
                path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                for path in sorted(parquet_dir.glob("*.parquet"))
            },
        },
    }
    if inspection.date_range is not None:
        manifest["dateRange"] = inspection.date_range.model_dump()
    (snapshot_dir / "manifest.v2.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )


def _create_snapshot(tmp_path: Path) -> Path:
    snapshot_dir = tmp_path / "sample"
    writer = DatasetWriter(str(snapshot_dir))
    writer.set_dataset_info("preset", "quickTesting")
    _set_v4_source_info(
        writer,
        coverage_start="2026-03-09",
        coverage_end="2026-03-09",
    )
    writer.close()
    _write_manifest(snapshot_dir)
    return snapshot_dir


def _populate_provider_payload_from_convenience(snapshot_dir: Path) -> None:
    """Complete legacy API fixtures with Dataset v4 provider/current tables."""
    duckdb = importlib.import_module("duckdb")
    conn = duckdb.connect(str(snapshot_dir / "dataset.duckdb"))
    try:
        conn.execute(
            """
            INSERT INTO stock_data_raw
            SELECT code, date, open, high, low, close, volume, NULL,
                   adjustment_factor, open, high, low, close, volume, created_at
            FROM stock_data
            """
        )
        conn.execute(
            """
            INSERT INTO stock_master_daily
            SELECT price.date, stock.code, stock.company_name,
                   stock.company_name_english, stock.market_code, stock.market_name,
                   stock.sector_17_code, stock.sector_17_name,
                   stock.sector_33_code, stock.sector_33_name,
                   stock.scale_category, stock.listed_date, price.created_at
            FROM stock_data AS price JOIN stocks AS stock USING (code)
            """
        )
        conn.execute(
            """
            INSERT INTO daily_valuation (code, date, price_basis_date, close)
            SELECT code, date, date, close FROM stock_data
            """
        )
        conn.execute(
            """
            INSERT INTO statement_metrics_adjusted
            (code, statement_id, disclosed_date, disclosed_at, period_end,
             period_type, fundamentals_adjustment_basis_date,
             adjustment_factor_cumulative, source_fingerprint)
            SELECT statement.code, statement.statement_id,
                   statement.disclosed_date, statement.disclosed_at,
                   statement.period_end,
                   coalesce(statement.type_of_current_period, 'UNKNOWN'),
                   info.value, 1,
                   'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'
            FROM statements AS statement
            CROSS JOIN dataset_info AS info
            WHERE info.key = 'fundamentals_adjustment_basis_date'
            """
        )
    finally:
        conn.close()
    DatasetWriter(str(snapshot_dir)).close()


def _create_rich_snapshot(tmp_path: Path) -> Path:
    snapshot_dir = tmp_path / "rich"
    writer = DatasetWriter(str(snapshot_dir))
    writer.upsert_stocks(
        [
            {
                "code": "7203",
                "company_name": "Toyota",
                "company_name_english": "Toyota Motor",
                "market_code": "0111",
                "market_name": "Prime",
                "sector_17_code": "7",
                "sector_17_name": "Transport",
                "sector_33_code": "3050",
                "sector_33_name": "Auto",
                "listed_date": "1949-05-16",
            }
        ]
    )
    writer.upsert_stock_data(
        [
            {
                "code": "7203",
                "date": "2024-01-04",
                "open": 200.0,
                "high": 202.0,
                "low": 198.0,
                "close": 201.0,
                "volume": 500,
                "adjustment_factor": 0.5,
            }
        ]
    )
    writer.upsert_topix_data(
        [{"date": "2024-01-04", "open": 1, "high": 1, "low": 1, "close": 1}]
    )
    writer.upsert_indices_data(
        [{
            "code": "0040", "date": "2024-01-04", "open": 1, "high": 1,
            "low": 1, "close": 1, "sector_name": "Auto",
        }]
    )
    writer.upsert_margin_data(
        [{
            "code": "7203", "date": "2024-01-04",
            "long_margin_volume": 10, "short_margin_volume": 5,
        }]
    )
    writer.upsert_statements(
        [{
            "code": "7203",
            "statement_id": "statement-7203",
            "disclosed_date": "2024-01-03",
            "disclosed_at": "2024-01-03T15:00:00+09:00",
            "period_start": "2023-01-01",
            "period_end": "2023-12-31",
            "earnings_per_share": 10,
            "profit": 100,
            "equity": 1000,
            "type_of_current_period": "FY",
            "type_of_document": "FY",
            "bps": 50,
        }]
    )
    _set_v4_source_info(
        writer,
        coverage_start="2024-01-04",
        coverage_end="2024-01-04",
    )
    writer.close()

    duckdb = importlib.import_module("duckdb")
    conn = duckdb.connect(str(snapshot_dir / "dataset.duckdb"))
    try:
        conn.execute(
            """
            INSERT INTO stock_data_raw
            (code, date, open, high, low, close, volume, turnover_value,
             adjustment_factor, adjusted_open, adjusted_high, adjusted_low,
             adjusted_close, adjusted_volume)
            VALUES ('7203', '2024-01-04', 100, 101, 99, 100.5, 1000, 100000,
                    0.5, 200, 202, 198, 201, 500)
            """
        )
        conn.execute(
            """
            INSERT INTO stock_master_daily
            (date, code, company_name, company_name_english, market_code, market_name,
             sector_17_code, sector_17_name, sector_33_code, sector_33_name,
             listed_date)
            VALUES ('2024-01-04', '7203', 'Toyota', 'Toyota Motor', '0111',
                    'Prime', '7', 'Transport', '3050', 'Auto', '1949-05-16')
            """
        )
        conn.execute(
            """
            INSERT INTO statement_metrics_adjusted
            (code, statement_id, disclosed_date, disclosed_at, period_end, period_type,
             fundamentals_adjustment_basis_date, raw_eps, adjusted_eps, raw_bps,
             adjusted_bps, adjustment_factor_cumulative, source_fingerprint)
            VALUES ('7203', 'statement-7203', '2024-01-03',
                    '2024-01-03T15:00:00+09:00', '2023-12-31', 'FY',
                    '2024-01-04', 10, 20, 50, 100, 2, ?)
            """,
            ["b" * 64],
        )
        conn.execute(
            """
            INSERT INTO daily_valuation
            (code, date, price_basis_date, close, eps, bps, per, pbr,
             statement_disclosed_date, statement_id, statement_disclosed_at,
             fundamentals_adjustment_basis_date, source_fingerprint)
            VALUES ('7203', '2024-01-04', '2024-01-04', 201, 20, 100, 10.05,
                    2.01, '2024-01-03', 'statement-7203',
                    '2024-01-03T15:00:00+09:00', '2024-01-04', ?)
            """,
            ["b" * 64],
        )
    finally:
        conn.close()
    DatasetWriter(str(snapshot_dir)).close()
    _write_manifest(snapshot_dir)
    return snapshot_dir


def _load_manifest(snapshot_dir: Path) -> dict[str, Any]:
    return json.loads((snapshot_dir / "manifest.v2.json").read_text(encoding="utf-8"))


def _save_manifest(snapshot_dir: Path, manifest: dict[str, Any]) -> None:
    (snapshot_dir / "manifest.v2.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )


def _refresh_duckdb_checksum(snapshot_dir: Path) -> None:
    manifest = _load_manifest(snapshot_dir)
    manifest["checksums"]["duckdbSha256"] = hashlib.sha256(
        (snapshot_dir / "dataset.duckdb").read_bytes()
    ).hexdigest()
    _save_manifest(snapshot_dir, manifest)


def test_validate_dataset_snapshot_accepts_valid_bundle(tmp_path: Path) -> None:
    manifest = validate_dataset_snapshot(_create_snapshot(tmp_path))
    assert manifest.schemaVersion == 4
    assert manifest.source.marketSchemaVersion == 5
    assert manifest.source.stockPriceAdjustmentMode == "provider_adjusted_v1"


@pytest.mark.parametrize("version", [1, 2, 3, "4"])
def test_legacy_or_coercible_manifest_schema_is_unsupported(
    tmp_path: Path,
    version: object,
) -> None:
    snapshot_dir = _create_snapshot(tmp_path)
    _write_manifest(snapshot_dir, schema_version=version)
    with pytest.raises(UnsupportedDatasetSnapshotError, match="schemaVersion"):
        validate_dataset_snapshot(snapshot_dir)


def test_dataset_v3_manifest_is_rejected_after_v4_cutover(tmp_path: Path) -> None:
    snapshot_dir = _create_snapshot(tmp_path)
    _write_manifest(snapshot_dir, schema_version=3)
    with pytest.raises(UnsupportedDatasetSnapshotError, match="schemaVersion: 3"):
        validate_dataset_snapshot(snapshot_dir)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("marketSchemaVersion", 4, "marketSchemaVersion"),
        ("marketSchemaVersion", "5", "marketSchemaVersion"),
        ("stockPriceAdjustmentMode", "local_projection_v2_event_time", "literal_error"),
    ],
)
def test_manifest_requires_exact_market_v5_provider_lineage(
    tmp_path: Path,
    field: str,
    value: object,
    message: str,
) -> None:
    snapshot_dir = _create_snapshot(tmp_path)
    manifest = _load_manifest(snapshot_dir)
    manifest["source"][field] = value
    _save_manifest(snapshot_dir, manifest)
    with pytest.raises(DatasetManifestValidationError, match=message):
        validate_dataset_snapshot(snapshot_dir)


def test_manifest_source_must_match_immutable_dataset_info(tmp_path: Path) -> None:
    snapshot_dir = _create_snapshot(tmp_path)
    manifest = _load_manifest(snapshot_dir)
    manifest["source"]["providerPlan"] = "free"
    _save_manifest(snapshot_dir, manifest)
    with pytest.raises(RuntimeError, match="source metadata mismatch"):
        validate_dataset_snapshot(snapshot_dir)


@pytest.mark.parametrize(
    ("key", "value"),
    [
        (DATASET_PROVIDER_AS_OF_INFO_KEY, "2024-1-04"),
        (DATASET_PROVIDER_COVERAGE_START_INFO_KEY, "2026-03-10"),
        (DATASET_FUNDAMENTALS_BASIS_DATE_INFO_KEY, "2026-03-10"),
    ],
)
def test_dataset_info_rejects_invalid_provider_vintage_dates(
    tmp_path: Path,
    key: str,
    value: str,
) -> None:
    snapshot_dir = _create_snapshot(tmp_path)
    duckdb = importlib.import_module("duckdb")
    conn = duckdb.connect(str(snapshot_dir / "dataset.duckdb"))
    try:
        conn.execute("UPDATE dataset_info SET value = ? WHERE key = ?", [value, key])
    finally:
        conn.close()
    _refresh_duckdb_checksum(snapshot_dir)
    with pytest.raises(DatasetManifestValidationError, match="provider vintage|canonical"):
        validate_dataset_snapshot(snapshot_dir)


def test_snapshot_rejects_missing_required_table(tmp_path: Path) -> None:
    snapshot_dir = _create_snapshot(tmp_path)
    duckdb = importlib.import_module("duckdb")
    conn = duckdb.connect(str(snapshot_dir / "dataset.duckdb"))
    try:
        conn.execute("DROP TABLE margin_data")
    finally:
        conn.close()
    _refresh_duckdb_checksum(snapshot_dir)
    with pytest.raises(DatasetManifestValidationError, match="missing=.*margin_data"):
        validate_dataset_snapshot(snapshot_dir)


@pytest.mark.parametrize("table", ["stock_adjustment_bases", "stock_adjustment_basis_segments"])
def test_snapshot_rejects_retained_basis_graph_table(tmp_path: Path, table: str) -> None:
    snapshot_dir = _create_snapshot(tmp_path)
    duckdb = importlib.import_module("duckdb")
    conn = duckdb.connect(str(snapshot_dir / "dataset.duckdb"))
    try:
        conn.execute(f"CREATE TABLE {table} (basis_id TEXT)")
    finally:
        conn.close()
    _refresh_duckdb_checksum(snapshot_dir)
    with pytest.raises(DatasetManifestValidationError, match="exact table set"):
        validate_dataset_snapshot(snapshot_dir)


def test_snapshot_rejects_arbitrary_extra_table(tmp_path: Path) -> None:
    snapshot_dir = _create_snapshot(tmp_path)
    duckdb = importlib.import_module("duckdb")
    conn = duckdb.connect(str(snapshot_dir / "dataset.duckdb"))
    try:
        conn.execute("CREATE TABLE unexpected_payload (value TEXT)")
    finally:
        conn.close()
    _refresh_duckdb_checksum(snapshot_dir)
    with pytest.raises(DatasetManifestValidationError, match="exact table set"):
        validate_dataset_snapshot(snapshot_dir)


def test_snapshot_rejects_arbitrary_extra_column(tmp_path: Path) -> None:
    snapshot_dir = _create_snapshot(tmp_path)
    duckdb = importlib.import_module("duckdb")
    conn = duckdb.connect(str(snapshot_dir / "dataset.duckdb"))
    try:
        conn.execute("ALTER TABLE stocks ADD COLUMN unexpected_payload TEXT")
    finally:
        conn.close()
    _refresh_duckdb_checksum(snapshot_dir)
    with pytest.raises(DatasetManifestValidationError, match="physical schema"):
        validate_dataset_snapshot(snapshot_dir)


def test_snapshot_rejects_pre_fractional_bigint_stock_volume_schema(tmp_path: Path) -> None:
    snapshot_dir = _create_snapshot(tmp_path)
    duckdb = importlib.import_module("duckdb")
    conn = duckdb.connect(str(snapshot_dir / "dataset.duckdb"))
    try:
        conn.execute("ALTER TABLE stock_data ALTER volume SET DATA TYPE BIGINT")
        conn.execute("ALTER TABLE stock_data_raw ALTER adjusted_volume SET DATA TYPE BIGINT")
    finally:
        conn.close()
    _refresh_duckdb_checksum(snapshot_dir)

    with pytest.raises(
        DatasetManifestValidationError,
        match="Dataset v4 physical schema mismatch for stock_data",
    ):
        validate_dataset_snapshot(snapshot_dir)


def test_snapshot_rejects_unlisted_physical_parquet_file(tmp_path: Path) -> None:
    snapshot_dir = _create_snapshot(tmp_path)
    (snapshot_dir / "parquet" / "unexpected.parquet").write_bytes(b"tamper")
    with pytest.raises(DatasetManifestValidationError, match="physical Parquet artifacts"):
        validate_dataset_snapshot(snapshot_dir)


def test_manifest_requires_all_v4_logical_counts(tmp_path: Path) -> None:
    snapshot_dir = _create_snapshot(tmp_path)
    manifest = _load_manifest(snapshot_dir)
    del manifest["logicalCounts"]["daily_valuation"]
    _save_manifest(snapshot_dir, manifest)
    with pytest.raises(DatasetManifestValidationError, match="daily_valuation"):
        validate_dataset_snapshot(snapshot_dir)


@pytest.mark.parametrize("artifact", ["stock_data.parquet", "unexpected.parquet"])
def test_manifest_requires_exact_parquet_checksum_keys(
    tmp_path: Path,
    artifact: str,
) -> None:
    snapshot_dir = _create_snapshot(tmp_path)
    manifest = _load_manifest(snapshot_dir)
    if artifact == "unexpected.parquet":
        manifest["checksums"]["parquet"][artifact] = "f" * 64
    else:
        del manifest["checksums"]["parquet"][artifact]
    _save_manifest(snapshot_dir, manifest)
    with pytest.raises(DatasetManifestValidationError, match="exactly match Dataset v4"):
        validate_dataset_snapshot(snapshot_dir)


def test_validate_dataset_snapshot_rejects_duckdb_checksum_mismatch(tmp_path: Path) -> None:
    snapshot_dir = _create_snapshot(tmp_path)
    manifest = _load_manifest(snapshot_dir)
    manifest["checksums"]["duckdbSha256"] = "0" * 64
    _save_manifest(snapshot_dir, manifest)
    with pytest.raises(RuntimeError, match="duckdb checksum mismatch"):
        validate_dataset_snapshot(snapshot_dir)


def test_validate_dataset_snapshot_rejects_logical_checksum_mismatch(tmp_path: Path) -> None:
    snapshot_dir = _create_snapshot(tmp_path)
    manifest = _load_manifest(snapshot_dir)
    manifest["checksums"]["logicalSha256"] = "0" * 64
    _save_manifest(snapshot_dir, manifest)
    with pytest.raises(RuntimeError, match="logical checksum mismatch"):
        validate_dataset_snapshot(snapshot_dir)


def test_validate_dataset_snapshot_rejects_parquet_checksum_mismatch(tmp_path: Path) -> None:
    snapshot_dir = _create_snapshot(tmp_path)
    manifest = _load_manifest(snapshot_dir)
    name = next(iter(manifest["checksums"]["parquet"]))
    manifest["checksums"]["parquet"][name] = "0" * 64
    _save_manifest(snapshot_dir, manifest)
    with pytest.raises(RuntimeError, match="Parquet checksum mismatch"):
        validate_dataset_snapshot(snapshot_dir)


@pytest.mark.parametrize(
    ("sql", "message"),
    [
        (
            "UPDATE stock_data_raw SET adjusted_close = 999",
            "differs from provider-adjusted raw values",
        ),
        (
            "UPDATE statement_metrics_adjusted SET fundamentals_adjustment_basis_date = '2024-01-03'",
            "no exact adjusted statement provenance",
        ),
        (
            "UPDATE statement_metrics_adjusted SET statement_id = 'orphan'",
            "no exact raw statement identity",
        ),
        (
            "UPDATE statement_metrics_adjusted SET period_type = 'Q1'",
            "no exact raw statement identity",
        ),
        (
            "UPDATE daily_valuation SET price_basis_date = '2024-01-03'",
            "current-basis provenance is inconsistent",
        ),
        (
            "UPDATE daily_valuation SET source_fingerprint = 'cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc'",
            "no exact adjusted statement provenance",
        ),
        (
            "UPDATE daily_valuation SET statement_disclosed_date = '2024-01-02'",
            "no exact adjusted statement provenance",
        ),
        (
            "DELETE FROM statement_metrics_adjusted",
            "raw statement has no exact adjusted metric identity",
        ),
        (
            "UPDATE stock_data SET date = '2024-01-05'",
            "(exceeds pinned provider coverage|session coverage has an empty, gap, or bound mismatch)",
        ),
        (
            "UPDATE stock_master_daily SET listed_date = '1949-5-16'",
            "stock_master_daily.listed_date",
        ),
    ],
)
def test_snapshot_integrity_fails_closed(
    tmp_path: Path,
    sql: str,
    message: str,
) -> None:
    snapshot_dir = _create_rich_snapshot(tmp_path)
    duckdb = importlib.import_module("duckdb")
    conn = duckdb.connect(str(snapshot_dir / "dataset.duckdb"))
    try:
        conn.execute(sql)
    finally:
        conn.close()
    _refresh_duckdb_checksum(snapshot_dir)
    with pytest.raises(DatasetManifestValidationError, match=message):
        validate_dataset_snapshot(snapshot_dir)


@pytest.mark.parametrize(
    ("sql", "field"),
    [
        ("UPDATE stocks SET listed_date = '1949-5-16'", "stocks.listed_date"),
        ("UPDATE stock_data SET date = '2024-1-04'", "stock_data.date"),
        ("UPDATE topix_data SET date = '2024-1-04'", "topix_data.date"),
        ("UPDATE indices_data SET date = '2024-1-04'", "indices_data.date"),
        ("UPDATE margin_data SET date = '2024-1-04'", "margin_data.date"),
        ("UPDATE stock_data_raw SET date = '2024-1-04'", "stock_data_raw.date"),
        ("UPDATE stock_master_daily SET date = '2024-1-04'", "stock_master_daily.date"),
        ("UPDATE stock_master_daily SET listed_date = '1949-5-16'", "stock_master_daily.listed_date"),
        ("UPDATE statements SET disclosed_date = '2024-1-03'", "statements.disclosed_date"),
        ("UPDATE statements SET period_start = '2023-1-01'", "statements.period_start"),
        ("UPDATE statements SET period_end = '2023-12-1'", "statements.period_end"),
        ("UPDATE statement_metrics_adjusted SET disclosed_date = '2024-1-03'", "statement_metrics_adjusted.disclosed_date"),
        ("UPDATE statement_metrics_adjusted SET period_end = '2023-12-1'", "statement_metrics_adjusted.period_end"),
        ("UPDATE statement_metrics_adjusted SET fundamentals_adjustment_basis_date = '2024-1-04'", "statement_metrics_adjusted.fundamentals_adjustment_basis_date"),
        ("UPDATE daily_valuation SET date = '2024-1-04'", "daily_valuation.date"),
        ("UPDATE daily_valuation SET price_basis_date = '2024-1-04'", "daily_valuation.price_basis_date"),
        ("UPDATE daily_valuation SET statement_disclosed_date = '2024-1-03'", "daily_valuation.statement_disclosed_date"),
        ("UPDATE daily_valuation SET forward_eps_disclosed_date = '2024-1-03'", "daily_valuation.forward_eps_disclosed_date"),
        ("UPDATE daily_valuation SET forward_sales_disclosed_date = '2024-1-03'", "daily_valuation.forward_sales_disclosed_date"),
        ("UPDATE daily_valuation SET fundamentals_adjustment_basis_date = '2024-1-04'", "daily_valuation.fundamentals_adjustment_basis_date"),
    ],
)
def test_snapshot_rejects_noncanonical_dates_across_every_physical_family(
    tmp_path: Path,
    sql: str,
    field: str,
) -> None:
    snapshot_dir = _create_rich_snapshot(tmp_path)
    duckdb = importlib.import_module("duckdb")
    conn = duckdb.connect(str(snapshot_dir / "dataset.duckdb"))
    try:
        conn.execute(sql)
    finally:
        conn.close()
    _refresh_duckdb_checksum(snapshot_dir)
    with pytest.raises(DatasetManifestValidationError, match=field):
        validate_dataset_snapshot(snapshot_dir)


def test_dataset_snapshot_reader_reads_provider_current_bundle(tmp_path: Path) -> None:
    snapshot_dir = _create_rich_snapshot(tmp_path)
    reader = DatasetSnapshotReader(str(snapshot_dir))
    try:
        assert reader.get_snapshot_lineage() == (4, 5, "provider_adjusted_v1")
        assert reader.get_stocks()[0].code == "7203"
        assert reader.get_stock_ohlcv("72030")[0].close == 201.0
        assert reader.get_topix()[0].date == "2024-01-04"
        assert reader.get_index_data("0040")[0].sector_name == "Auto"
        assert reader.get_margin("7203")[0].long_margin_volume == 10
        assert reader.get_statements("7203")[0].statement_id == "statement-7203"
        assert reader.get_adjusted_statement_metrics("7203")[0]["adjusted_eps"] == 20
        assert reader.get_daily_valuation("7203")[0]["close"] == 201
        assert reader.get_stock_count() == 1
        assert reader.get_stocks_with_quotes_count() == 1
        assert reader.get_date_range() == {"min": "2024-01-04", "max": "2024-01-04"}
    finally:
        reader.close()


def test_reader_rechecks_artifacts_before_first_connection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot_dir = _create_rich_snapshot(tmp_path)
    proof = snapshot_reader_module.validate_supported_dataset_snapshot_proof(snapshot_dir)
    reader = DatasetSnapshotReader._from_validation_proof(proof)
    monkeypatch.setattr(
        snapshot_reader_module,
        "build_dataset_artifact_fingerprint",
        lambda _path: snapshot_reader_module.DatasetArtifactFingerprint(
            manifest_sha256="f" * 64,
            artifacts=(),
        ),
    )
    with pytest.raises(DatasetManifestValidationError, match="changed after support validation"):
        reader.get_stocks()
    reader.close()


def test_reader_rechecks_proof_after_connection_is_open(tmp_path: Path) -> None:
    snapshot_dir = _create_rich_snapshot(tmp_path)
    reader = DatasetSnapshotReader(str(snapshot_dir))
    assert reader.get_stocks()[0].code == "7203"
    manifest = _load_manifest(snapshot_dir)
    manifest["generatedAt"] = "2026-07-21T00:00:01+00:00"
    _save_manifest(snapshot_dir, manifest)
    with pytest.raises(
        DatasetManifestValidationError,
        match="changed after support validation",
    ):
        reader.get_stocks()
    reader.close()


def test_snapshot_root_symlink_is_rejected(tmp_path: Path) -> None:
    snapshot_dir = _create_snapshot(tmp_path)
    alias = tmp_path / "alias"
    alias.symlink_to(snapshot_dir, target_is_directory=True)
    with pytest.raises(DatasetManifestValidationError, match="root must not be a symlink"):
        DatasetSnapshotReader(str(alias))
