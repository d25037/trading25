"""Dataset snapshot reader for `dataset.duckdb + parquet + manifest.v2.json` snapshots."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date
import hashlib
import importlib
import json
from pathlib import Path
import random
import stat as stat_module
import threading
from typing import Annotated, Any, Literal, cast

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from src.domains.fundamentals.adjustment_basis import (
    BasisStatus,
    StockAdjustmentBasis,
)
from src.infrastructure.db.dataset_io.snapshot_contract import (
    DATASET_V3_PARQUET_ARTIFACT_NAMES,
    EVENT_TIME_PIT_DATE_TO_INFO_KEY,
)
from src.infrastructure.db.market import adjustment_basis_queries
from src.infrastructure.db.market.query_helpers import normalize_stock_code
from src.infrastructure.db.market.valuation_queries import (
    get_adjusted_statement_metrics_for_basis,
    get_daily_valuation_for_basis,
)
from src.shared.models.types import normalize_period_type

_ACTUAL_ONLY_COLUMNS = (
    "earnings_per_share",
    "profit",
    "equity",
)
_REQUIRED_SNAPSHOT_TABLES = (
    "stocks",
    "stock_data",
    "topix_data",
    "indices_data",
    "margin_data",
    "statements",
    "stock_data_raw",
    "stock_master_daily",
    "stock_adjustment_bases",
    "stock_adjustment_basis_segments",
    "statement_metrics_adjusted",
    "daily_valuation",
    "dataset_info",
)


class UnsupportedDatasetSnapshotError(RuntimeError):
    """The bundle uses a snapshot generation that runtime no longer supports."""


class DatasetManifestValidationError(RuntimeError):
    """The v3 manifest or its DuckDB payload is incomplete or inconsistent."""


@dataclass(frozen=True)
class _DuckDbRow:
    _columns: tuple[str, ...]
    _values: tuple[Any, ...]
    _index_map: dict[str, int]

    def __getitem__(self, key: str | int) -> Any:
        if isinstance(key, int):
            return self._values[key]
        return self._values[self._index_map[str(key)]]

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def __iter__(self) -> Iterator[tuple[str, Any]]:
        return iter(zip(self._columns, self._values))

    def keys(self) -> tuple[str, ...]:
        return self._columns

    def items(self):
        return zip(self._columns, self._values)

    def values(self) -> tuple[Any, ...]:
        return self._values


@dataclass(frozen=True)
class DatasetSnapshotInspection:
    counts: "DatasetLogicalCountsV3"
    coverage: "DatasetCoverageV3"
    date_range: "DatasetDateRangeV3 | None"


class DatasetSourceV3(BaseModel):
    model_config = ConfigDict(extra="forbid")

    backend: Literal["duckdb-parquet"]
    marketSchemaVersion: Literal[4]
    stockPriceAdjustmentMode: Literal["local_projection_v2_event_time"]


class DatasetDescriptorV3(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    preset: str = Field(min_length=1)
    duckdbFile: Literal["dataset.duckdb"]
    parquetDir: Literal["parquet"]


Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class DatasetChecksumsV3(BaseModel):
    model_config = ConfigDict(extra="forbid")

    duckdbSha256: Sha256
    logicalSha256: Sha256
    parquet: dict[str, Sha256]

    @model_validator(mode="after")
    def require_exact_parquet_artifacts(self) -> "DatasetChecksumsV3":
        actual = set(self.parquet)
        if actual != DATASET_V3_PARQUET_ARTIFACT_NAMES:
            missing = sorted(DATASET_V3_PARQUET_ARTIFACT_NAMES - actual)
            extra = sorted(actual - DATASET_V3_PARQUET_ARTIFACT_NAMES)
            raise ValueError(
                "parquet checksum keys must exactly match Dataset v3 artifacts; "
                f"missing={missing}, extra={extra}"
            )
        return self


class DatasetLogicalCountsV3(BaseModel):
    model_config = ConfigDict(extra="forbid")

    stocks: int = Field(ge=0)
    stock_data: int = Field(ge=0)
    topix_data: int = Field(ge=0)
    indices_data: int = Field(ge=0)
    margin_data: int = Field(ge=0)
    statements: int = Field(ge=0)
    stock_data_raw: int = Field(ge=0)
    stock_master_daily: int = Field(ge=0)
    stock_adjustment_bases: int = Field(ge=0)
    stock_adjustment_basis_segments: int = Field(ge=0)
    statement_metrics_adjusted: int = Field(ge=0)
    daily_valuation: int = Field(ge=0)
    dataset_info: int = Field(ge=0)


class DatasetCoverageV3(BaseModel):
    model_config = ConfigDict(extra="forbid")

    totalStocks: int = Field(ge=0)
    stocksWithQuotes: int = Field(ge=0)
    stocksWithStatements: int = Field(ge=0)
    stocksWithMargin: int = Field(ge=0)


class DatasetDateRangeV3(BaseModel):
    model_config = ConfigDict(extra="forbid")

    min: str = Field(min_length=1)
    max: str = Field(min_length=1)


class DatasetManifestV3(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schemaVersion: Literal[3] = 3
    generatedAt: str = Field(min_length=1)
    dataset: DatasetDescriptorV3
    source: DatasetSourceV3
    logicalCounts: DatasetLogicalCountsV3
    coverage: DatasetCoverageV3
    checksums: DatasetChecksumsV3
    dateRange: DatasetDateRangeV3 | None = None


@dataclass(frozen=True)
class DatasetArtifactStat:
    path: str
    st_dev: int
    st_ino: int
    st_size: int
    st_mtime_ns: int


@dataclass(frozen=True)
class DatasetArtifactFingerprint:
    manifest_sha256: str
    artifacts: tuple[DatasetArtifactStat, ...]


@dataclass(frozen=True)
class DatasetValidationProof:
    snapshot_dir: Path
    manifest: DatasetManifestV3
    fingerprint: DatasetArtifactFingerprint


_LEGACY_PERIOD_TYPE_MAP = {
    "1Q": "Q1",
    "2Q": "Q2",
    "3Q": "Q3",
}


def _resolve_period_filter_values(period_type: str) -> list[str] | None:
    normalized_period = normalize_period_type(period_type)
    if normalized_period is None or normalized_period == "all":
        return None

    values = [normalized_period]
    legacy_value = _LEGACY_PERIOD_TYPE_MAP.get(normalized_period)
    if legacy_value is not None:
        values.append(legacy_value)
    return values


def _sha256_of_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def build_dataset_snapshot_logical_checksum(
    *,
    counts: DatasetLogicalCountsV3,
    coverage: DatasetCoverageV3,
    date_range: DatasetDateRangeV3 | None,
) -> str:
    payload = {
        "counts": counts.model_dump(),
        "coverage": coverage.model_dump(),
        "dateRange": date_range.model_dump() if date_range is not None else None,
    }
    normalized = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _connect_duckdb(duckdb_path: Path, *, read_only: bool) -> Any:
    duckdb = importlib.import_module("duckdb")
    return cast(Any, duckdb).connect(str(duckdb_path), read_only=read_only)


def _query_scalar_int(conn: Any, sql: str) -> int:
    row = conn.execute(sql).fetchone()
    if row is None or row[0] is None:
        return 0
    return int(row[0])


def _table_count(conn: Any, table_name: str) -> int:
    return _query_scalar_int(conn, f"SELECT COUNT(*) FROM {table_name}")


def _read_event_time_pit_date_to(conn: Any) -> str:
    try:
        cutoff_row = conn.execute(
            "SELECT value FROM dataset_info WHERE key = ?",
            [EVENT_TIME_PIT_DATE_TO_INFO_KEY],
        ).fetchone()
    except Exception as exc:
        raise DatasetManifestValidationError(
            "Event-time PIT snapshot cutoff metadata is missing"
        ) from exc
    if cutoff_row is None:
        raise DatasetManifestValidationError(
            "Event-time PIT snapshot cutoff metadata is missing"
        )
    snapshot_date_to = str(cutoff_row[0])
    try:
        parsed_cutoff = date.fromisoformat(snapshot_date_to)
    except ValueError as exc:
        raise DatasetManifestValidationError(
            "Event-time PIT snapshot cutoff metadata is invalid"
        ) from exc
    if parsed_cutoff.isoformat() != snapshot_date_to:
        raise DatasetManifestValidationError(
            "Event-time PIT snapshot cutoff metadata is invalid"
        )
    return snapshot_date_to


def _validate_event_time_pit_integrity(conn: Any, counts: DatasetLogicalCountsV3) -> None:
    _read_event_time_pit_date_to(conn)
    if _query_scalar_int(
        conn,
        f"""
        SELECT COUNT(*) FROM (
            SELECT date AS cutoff_date FROM stock_data
            UNION ALL SELECT date FROM topix_data
            UNION ALL SELECT date FROM indices_data
            UNION ALL SELECT date FROM margin_data
            UNION ALL SELECT disclosed_date FROM statements
            UNION ALL SELECT date FROM stock_data_raw
            UNION ALL SELECT date FROM stock_master_daily
            UNION ALL SELECT disclosed_date FROM statement_metrics_adjusted
            UNION ALL SELECT date FROM daily_valuation
        ) physical_dates
        WHERE cutoff_date > (
            SELECT value FROM dataset_info
            WHERE key = '{EVENT_TIME_PIT_DATE_TO_INFO_KEY}'
        )
        """,
    ):
        raise DatasetManifestValidationError(
            "Dataset physical data exceeds the snapshot cutoff"
        )
    pit_counts = (
        counts.stock_data_raw,
        counts.stock_master_daily,
        counts.stock_adjustment_bases,
        counts.stock_adjustment_basis_segments,
        counts.statement_metrics_adjusted,
        counts.daily_valuation,
    )
    if not any(pit_counts):
        return
    required_pit_counts = (
        counts.stock_data_raw,
        counts.stock_master_daily,
        counts.stock_adjustment_bases,
        counts.stock_adjustment_basis_segments,
        counts.daily_valuation,
    )
    if not all(required_pit_counts):
        raise DatasetManifestValidationError(
            "Event-time PIT snapshot is missing required lineage table data"
        )
    checks = (
        (
            "SELECT COUNT(*) FROM stock_adjustment_bases WHERE status <> 'ready'",
            "Event-time PIT catalog contains a non-ready basis",
        ),
        (
            """
            SELECT COUNT(*) FROM (
                SELECT code, valid_from, valid_to_exclusive,
                       lead(valid_from) OVER (PARTITION BY code ORDER BY valid_from) AS next_from,
                       count(*) OVER (PARTITION BY code, valid_from) AS same_start_count,
                       count(*) OVER (PARTITION BY code, basis_id) AS same_id_count
                FROM stock_adjustment_bases
            ) ordered
            WHERE same_start_count <> 1 OR same_id_count <> 1
               OR (valid_to_exclusive IS NOT NULL AND valid_from >= valid_to_exclusive)
               OR (next_from IS NOT NULL
                   AND (valid_to_exclusive IS NULL OR valid_to_exclusive <> next_from))
            """,
            "Event-time PIT basis intervals are overlapping or incomplete",
        ),
        (
            """
            SELECT COUNT(*) FROM stock_adjustment_basis_segments segment
            LEFT JOIN stock_adjustment_bases basis
              ON basis.code = segment.code AND basis.basis_id = segment.basis_id
            WHERE basis.basis_id IS NULL
            """,
            "Event-time PIT segment has a dangling basis FK",
        ),
        (
            """
            SELECT COUNT(*) FROM statement_metrics_adjusted metric
            LEFT JOIN stock_adjustment_bases basis
              ON basis.code = metric.code AND basis.basis_id = metric.basis_version
            WHERE basis.basis_id IS NULL
            """,
            "Event-time PIT adjusted metric has a dangling basis FK",
        ),
        (
            """
            SELECT COUNT(*) FROM daily_valuation valuation
            LEFT JOIN stock_adjustment_bases basis
              ON basis.code = valuation.code AND basis.basis_id = valuation.basis_version
            WHERE basis.basis_id IS NULL
            """,
            "Event-time PIT valuation has a dangling basis FK",
        ),
        (
            """
            SELECT COUNT(*) FROM stock_adjustment_bases basis
            LEFT JOIN stock_adjustment_basis_segments segment
              ON basis.code = segment.code AND basis.basis_id = segment.basis_id
            GROUP BY basis.code, basis.basis_id
            HAVING COUNT(segment.source_date_from) = 0
            """,
            "Event-time PIT basis is missing segments",
        ),
        (
            """
            SELECT COUNT(*) FROM (
                SELECT code, basis_id, source_date_from, source_date_to_exclusive,
                       lead(source_date_from) OVER (
                           PARTITION BY code, basis_id ORDER BY source_date_from
                       ) AS next_from,
                       count(*) OVER (
                           PARTITION BY code, basis_id, source_date_from
                       ) AS same_start_count,
                       cumulative_factor
                FROM stock_adjustment_basis_segments
            ) ordered
            WHERE same_start_count <> 1
               OR NOT isfinite(cumulative_factor) OR cumulative_factor <= 0
               OR (source_date_to_exclusive IS NOT NULL
                   AND source_date_from >= source_date_to_exclusive)
               OR (next_from IS NOT NULL
                   AND (source_date_to_exclusive IS NULL
                        OR source_date_to_exclusive <> next_from))
            """,
            "Event-time PIT segment intervals are overlapping or incomplete",
        ),
        (
            """
            SELECT COUNT(*) FROM (
                SELECT raw.code, raw.date
                FROM stock_data_raw raw
                LEFT JOIN stock_adjustment_bases basis
                  ON raw.code = basis.code AND raw.date >= basis.valid_from
                 AND (basis.valid_to_exclusive IS NULL OR raw.date < basis.valid_to_exclusive)
                GROUP BY raw.code, raw.date
                HAVING COUNT(basis.basis_id) <> 1
            ) uncovered
            """,
            "Event-time PIT raw price is not covered by exactly one basis",
        ),
        (
            """
            SELECT COUNT(*) FROM (
                SELECT code, date FROM stock_data_raw
                WHERE open IS NOT NULL AND high IS NOT NULL AND low IS NOT NULL
                  AND close IS NOT NULL AND volume IS NOT NULL
                EXCEPT ALL
                SELECT code, date FROM stock_master_daily
            ) raw_without_daily_master
            """,
            "Event-time PIT raw price is missing stock master coverage",
        ),
        (
            """
            SELECT COUNT(*) FROM (
                SELECT basis.code, basis.basis_id, basis.materialized_through_date,
                       max(raw.date) AS required_through
                FROM stock_adjustment_bases basis
                JOIN stock_data_raw raw ON basis.code = raw.code
                 AND raw.date >= basis.valid_from
                 AND (basis.valid_to_exclusive IS NULL OR raw.date < basis.valid_to_exclusive)
                GROUP BY basis.code, basis.basis_id, basis.materialized_through_date
            ) coverage
            WHERE materialized_through_date < required_through
            """,
            "Event-time PIT basis has insufficient materialized coverage",
        ),
        (
            """
            SELECT COUNT(*) FROM (
                SELECT basis.code, basis.basis_id, raw.date
                FROM stock_adjustment_bases basis
                JOIN stock_data_raw raw ON basis.code = raw.code
                 AND raw.date <= basis.materialized_through_date
                LEFT JOIN stock_adjustment_basis_segments segment
                  ON basis.code = segment.code AND basis.basis_id = segment.basis_id
                 AND raw.date >= segment.source_date_from
                 AND (segment.source_date_to_exclusive IS NULL
                      OR raw.date < segment.source_date_to_exclusive)
                GROUP BY basis.code, basis.basis_id, raw.date
                HAVING COUNT(segment.source_date_from) <> 1
            ) uncovered
            """,
            "Event-time PIT segment coverage is insufficient",
        ),
        (
            """
            SELECT COUNT(*) FROM (
                (SELECT basis.code, basis.basis_id, raw.date
                 FROM stock_adjustment_bases basis
                 JOIN stock_data_raw raw ON basis.code = raw.code
                  AND raw.date <= basis.materialized_through_date
                 EXCEPT ALL
                 SELECT code, basis_version, date FROM daily_valuation)
                UNION ALL
                (SELECT code, basis_version, date FROM daily_valuation
                 EXCEPT ALL
                 SELECT basis.code, basis.basis_id, raw.date
                 FROM stock_adjustment_bases basis
                 JOIN stock_data_raw raw ON basis.code = raw.code
                  AND raw.date <= basis.materialized_through_date)
            ) missing
            """,
            "Event-time PIT valuation coverage is insufficient",
        ),
        (
            """
            SELECT COUNT(*) FROM daily_valuation
            WHERE (statement_disclosed_date IS NOT NULL AND statement_disclosed_date > date)
               OR (forward_eps_disclosed_date IS NOT NULL AND forward_eps_disclosed_date > date)
               OR (forward_sales_disclosed_date IS NOT NULL
                   AND forward_sales_disclosed_date > date)
            """,
            "Event-time PIT valuation provenance is inconsistent",
        ),
        (
            f"""
            SELECT COUNT(*) FROM (
                SELECT basis.code, basis.basis_id, statement.disclosed_date,
                       statement.disclosed_date AS period_end,
                       coalesce(statement.type_of_current_period, '') AS period_type
                FROM stock_adjustment_bases basis
                JOIN statements statement ON basis.code = statement.code
                 AND statement.disclosed_date <= (
                     SELECT value FROM dataset_info
                     WHERE key = '{EVENT_TIME_PIT_DATE_TO_INFO_KEY}'
                 )
                 AND (basis.valid_to_exclusive IS NULL
                      OR statement.disclosed_date < basis.valid_to_exclusive)
                EXCEPT ALL
                SELECT code, basis_version, disclosed_date, period_end, period_type
                FROM statement_metrics_adjusted
            ) missing_expected_metric
            """,
            "Event-time PIT adjusted metric coverage is insufficient",
        ),
        (
            """
            SELECT COUNT(*) FROM (
                SELECT basis.code, basis.basis_id, identity.disclosed_date,
                       identity.period_end, identity.period_type
                FROM stock_adjustment_bases basis
                JOIN (
                    SELECT DISTINCT code, disclosed_date, period_end, period_type
                    FROM statement_metrics_adjusted
                ) identity ON basis.code = identity.code
                 AND (basis.valid_to_exclusive IS NULL
                      OR identity.disclosed_date < basis.valid_to_exclusive)
                EXCEPT ALL
                SELECT code, basis_version, disclosed_date, period_end, period_type
                FROM statement_metrics_adjusted
            ) missing_metric_basis
            """,
            "Event-time PIT adjusted metric coverage is incomplete or gapped",
        ),
    )
    for sql, message in checks:
        if _query_scalar_int(conn, sql):
            raise DatasetManifestValidationError(message)


def inspect_dataset_snapshot_duckdb(duckdb_path: str | Path) -> DatasetSnapshotInspection:
    conn = _connect_duckdb(Path(duckdb_path), read_only=True)
    try:
        existing_tables = {
            str(row[0])
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
        }
        missing_tables = sorted(set(_REQUIRED_SNAPSHOT_TABLES) - existing_tables)
        if missing_tables:
            raise DatasetManifestValidationError(
                "Dataset snapshot is missing required tables: " + ", ".join(missing_tables)
            )
        counts = DatasetLogicalCountsV3(
            stocks=_table_count(conn, "stocks"),
            stock_data=_table_count(conn, "stock_data"),
            topix_data=_table_count(conn, "topix_data"),
            indices_data=_table_count(conn, "indices_data"),
            margin_data=_table_count(conn, "margin_data"),
            statements=_table_count(conn, "statements"),
            stock_data_raw=_table_count(conn, "stock_data_raw"),
            stock_master_daily=_table_count(conn, "stock_master_daily"),
            stock_adjustment_bases=_table_count(conn, "stock_adjustment_bases"),
            stock_adjustment_basis_segments=_table_count(
                conn, "stock_adjustment_basis_segments"
            ),
            statement_metrics_adjusted=_table_count(
                conn,
                "statement_metrics_adjusted",
            ),
            daily_valuation=_table_count(conn, "daily_valuation"),
            dataset_info=_table_count(conn, "dataset_info"),
        )
        coverage = DatasetCoverageV3(
            totalStocks=counts.stocks,
            stocksWithQuotes=_query_scalar_int(conn, "SELECT COUNT(DISTINCT code) FROM stock_data"),
            stocksWithStatements=_query_scalar_int(conn, "SELECT COUNT(DISTINCT code) FROM statements"),
            stocksWithMargin=_query_scalar_int(conn, "SELECT COUNT(DISTINCT code) FROM margin_data"),
        )
        date_row = conn.execute("SELECT MIN(date), MAX(date) FROM stock_data").fetchone()
        _validate_event_time_pit_integrity(conn, counts)
    finally:
        conn.close()

    date_range = None
    if date_row is not None and date_row[0] is not None:
        date_range = DatasetDateRangeV3(min=str(date_row[0]), max=str(date_row[1]))
    return DatasetSnapshotInspection(counts=counts, coverage=coverage, date_range=date_range)


def _validate_raw_manifest_lineage(payload: object) -> None:
    if not isinstance(payload, dict):
        raise UnsupportedDatasetSnapshotError(
            "Unsupported dataset snapshot schemaVersion: None"
        )
    schema_version = payload.get("schemaVersion")
    if type(schema_version) is not int or schema_version != 3:
        raise UnsupportedDatasetSnapshotError(
            f"Unsupported dataset snapshot schemaVersion: {schema_version}"
        )

    source = payload.get("source")
    market_schema_version = (
        source.get("marketSchemaVersion") if isinstance(source, dict) else None
    )
    if type(market_schema_version) is not int or market_schema_version != 4:
        raise DatasetManifestValidationError(
            "Dataset snapshot source.marketSchemaVersion must be the integer 4"
        )


def read_dataset_snapshot_manifest(snapshot_dir: str | Path) -> DatasetManifestV3:
    manifest_path = Path(snapshot_dir) / "manifest.v2.json"
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise DatasetManifestValidationError(f"Invalid dataset manifest JSON: {exc}") from exc
    _validate_raw_manifest_lineage(payload)
    try:
        return DatasetManifestV3.model_validate(payload)
    except ValidationError as exc:
        raise DatasetManifestValidationError(str(exc)) from exc


def build_dataset_artifact_fingerprint(
    snapshot_dir: str | Path,
) -> DatasetArtifactFingerprint:
    requested_root = Path(snapshot_dir).absolute()
    root_lstat = requested_root.lstat()
    if stat_module.S_ISLNK(root_lstat.st_mode):
        raise DatasetManifestValidationError("Dataset snapshot root must not be a symlink")
    snapshot_root = requested_root.resolve()
    if snapshot_root != requested_root:
        raise DatasetManifestValidationError("Dataset snapshot root is not canonical")
    manifest_path = snapshot_root / "manifest.v2.json"
    manifest_lstat = manifest_path.lstat()
    if stat_module.S_ISLNK(manifest_lstat.st_mode):
        raise DatasetManifestValidationError(
            "Dataset artifact must not be a symlink: manifest.v2.json"
        )
    manifest_bytes = manifest_path.read_bytes()
    manifest = read_dataset_snapshot_manifest(snapshot_root)
    parquet_dir = snapshot_root / manifest.dataset.parquetDir
    parquet_dir_lstat = parquet_dir.lstat()
    if stat_module.S_ISLNK(parquet_dir_lstat.st_mode):
        raise DatasetManifestValidationError(
            "Dataset Parquet directory must not be a symlink"
        )
    paths = [
        manifest_path,
        snapshot_root / manifest.dataset.duckdbFile,
        *(
            parquet_dir / name
            for name in sorted(manifest.checksums.parquet)
        ),
    ]
    artifacts: list[DatasetArtifactStat] = []
    for path in paths:
        path_lstat = path.lstat()
        if stat_module.S_ISLNK(path_lstat.st_mode):
            raise DatasetManifestValidationError(
                f"Dataset artifact must not be a symlink: {path.name}"
            )
        resolved = path.resolve()
        if resolved.parent != snapshot_root and snapshot_root not in resolved.parents:
            raise DatasetManifestValidationError("Dataset artifact escapes snapshot root")
        stat = path.stat()
        artifacts.append(
            DatasetArtifactStat(
                path=str(path),
                st_dev=stat.st_dev,
                st_ino=stat.st_ino,
                st_size=stat.st_size,
                st_mtime_ns=stat.st_mtime_ns,
            )
        )
    return DatasetArtifactFingerprint(
        manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
        artifacts=tuple(artifacts),
    )


def dataset_snapshot_manifest_preflight(snapshot_dir: str | Path) -> bool:
    """Identify a fully validated runtime-compatible v3 bundle."""
    try:
        validate_supported_dataset_snapshot_proof(snapshot_dir)
    except Exception:
        return False
    return True


def validate_dataset_snapshot(snapshot_dir: str | Path) -> DatasetManifestV3:
    snapshot_root = Path(snapshot_dir)
    manifest = read_dataset_snapshot_manifest(snapshot_root)
    duckdb_path = snapshot_root / manifest.dataset.duckdbFile
    if not duckdb_path.exists():
        raise FileNotFoundError(f"dataset.duckdb not found: {duckdb_path}")
    if _sha256_of_file(duckdb_path) != manifest.checksums.duckdbSha256:
        raise RuntimeError(f"dataset.duckdb checksum mismatch: {duckdb_path}")

    parquet_dir = snapshot_root / manifest.dataset.parquetDir
    if not parquet_dir.exists():
        raise FileNotFoundError(f"Parquet directory not found: {parquet_dir}")
    for file_name, expected_sha in manifest.checksums.parquet.items():
        parquet_path = parquet_dir / file_name
        if not parquet_path.exists():
            raise FileNotFoundError(f"Parquet file not found: {parquet_path}")
        if _sha256_of_file(parquet_path) != expected_sha:
            raise RuntimeError(f"Parquet checksum mismatch: {parquet_path}")

    inspection = inspect_dataset_snapshot_duckdb(duckdb_path)
    if inspection.counts != manifest.logicalCounts:
        raise RuntimeError("Dataset snapshot manifest counts mismatch")
    if inspection.coverage != manifest.coverage:
        raise RuntimeError("Dataset snapshot manifest coverage mismatch")
    if inspection.date_range != manifest.dateRange:
        raise RuntimeError("Dataset snapshot manifest dateRange mismatch")
    logical_checksum = build_dataset_snapshot_logical_checksum(
        counts=inspection.counts,
        coverage=inspection.coverage,
        date_range=inspection.date_range,
    )
    if logical_checksum != manifest.checksums.logicalSha256:
        raise RuntimeError("Dataset snapshot logical checksum mismatch")

    return manifest


def validate_supported_dataset_snapshot(snapshot_dir: str | Path) -> DatasetManifestV3:
    """Validate all support invariants shared by discovery and runtime resolve."""
    snapshot_root = Path(snapshot_dir)
    manifest = validate_dataset_snapshot(snapshot_root)
    duckdb_path = snapshot_root / manifest.dataset.duckdbFile
    conn = _connect_duckdb(duckdb_path, read_only=True)
    try:
        _read_event_time_pit_date_to(conn)
    finally:
        conn.close()
    return manifest


def validate_supported_dataset_snapshot_proof(
    snapshot_dir: str | Path,
) -> DatasetValidationProof:
    snapshot_root = Path(snapshot_dir).absolute()
    before = build_dataset_artifact_fingerprint(snapshot_root)
    manifest = validate_supported_dataset_snapshot(snapshot_root)
    after = build_dataset_artifact_fingerprint(snapshot_root)
    if before != after:
        raise DatasetManifestValidationError(
            "Dataset artifacts changed during support validation"
        )
    return DatasetValidationProof(
        snapshot_dir=snapshot_root,
        manifest=manifest,
        fingerprint=after,
    )


class DatasetSnapshotReader:
    """Resolve and validate a dataset snapshot, then read directly from DuckDB."""

    def __init__(self, snapshot_dir: str) -> None:
        proof = validate_supported_dataset_snapshot_proof(snapshot_dir)
        self._initialize_from_validation_proof(proof)

    @classmethod
    def _from_validation_proof(
        cls, proof: DatasetValidationProof
    ) -> DatasetSnapshotReader:
        reader = cls.__new__(cls)
        reader._initialize_from_validation_proof(proof)
        return reader

    def _initialize_from_validation_proof(self, proof: DatasetValidationProof) -> None:
        self._snapshot_dir = proof.snapshot_dir
        self._manifest = proof.manifest
        self._proof_fingerprint = proof.fingerprint
        self._duckdb_path = self._snapshot_dir / self._manifest.dataset.duckdbFile
        self._conns: dict[int, Any] = {}
        self._conn_lock = threading.Lock()
        self._duckdb_statements_columns_cache: set[str] | None = None

    @property
    def snapshot_dir(self) -> Path:
        return self._snapshot_dir

    @property
    def manifest(self) -> DatasetManifestV3:
        return self._manifest

    def get_snapshot_lineage(
        self,
    ) -> tuple[Literal[3], Literal[4], Literal["local_projection_v2_event_time"]]:
        """Return lineage from the manifest validated during reader construction."""
        return (
            self._manifest.schemaVersion,
            self._manifest.source.marketSchemaVersion,
            self._manifest.source.stockPriceAdjustmentMode,
        )

    def _create_connection(self) -> Any:
        self._assert_artifacts_unchanged()
        conn = _connect_duckdb(self._duckdb_path, read_only=True)
        try:
            self._assert_artifacts_unchanged()
        except Exception:
            conn.close()
            raise
        return conn

    def _assert_artifacts_unchanged(self) -> None:
        if build_dataset_artifact_fingerprint(self._snapshot_dir) != self._proof_fingerprint:
            raise DatasetManifestValidationError(
                "Dataset artifacts changed after support validation"
            )

    def _get_thread_connection(self) -> Any:
        thread_id = threading.get_ident()
        conn = self._conns.get(thread_id)
        if conn is not None:
            return conn

        with self._conn_lock:
            conn = self._conns.get(thread_id)
            if conn is None:
                conn = self._create_connection()
                self._conns[thread_id] = conn
        return conn

    @property
    def conn(self) -> Any:
        return self._get_thread_connection()

    def close(self) -> None:
        with self._conn_lock:
            conns = list(self._conns.values())
            self._conns.clear()
        for conn in conns:
            conn.close()

    def _adapt_rows(self, cursor: Any, rows: list[tuple[Any, ...]]) -> list[_DuckDbRow]:
        description = getattr(cursor, "description", None) or []
        columns = tuple(str(column[0]) for column in description if column)
        if not columns:
            return []
        index_map = {column: idx for idx, column in enumerate(columns)}
        return [
            _DuckDbRow(_columns=columns, _values=tuple(row), _index_map=index_map)
            for row in rows
        ]

    def query(self, sql: str, params: tuple[Any, ...] = ()) -> list[_DuckDbRow]:
        cursor = self.conn.execute(sql, params)
        return self._adapt_rows(cursor, cursor.fetchall())

    def query_one(self, sql: str, params: tuple[Any, ...] = ()) -> _DuckDbRow | None:
        cursor = self.conn.execute(sql, params)
        row = cursor.fetchone()
        if row is None:
            return None
        adapted = self._adapt_rows(cursor, [tuple(row)])
        return adapted[0] if adapted else None

    def _fetchall_dicts(
        self,
        sql: str,
        params: list[Any] | tuple[Any, ...] | None = None,
    ) -> list[dict[str, Any]]:
        return [dict(row.items()) for row in self.query(sql, tuple(params or ()))]

    def _table_exists(self, table_name: str) -> bool:
        row = self.query_one(
            """
            SELECT COUNT(*) AS count
            FROM information_schema.tables
            WHERE table_schema = 'main' AND table_name = ?
            """,
            (table_name,),
        )
        return row is not None and int(row.count) == 1

    def _get_statements_columns(self) -> set[str]:
        if self._duckdb_statements_columns_cache is not None:
            return self._duckdb_statements_columns_cache
        rows = self.conn.execute("PRAGMA table_info('statements')").fetchall()
        columns = {
            str(row[1])
            for row in rows
            if row and len(row) > 1 and row[1]
        }
        self._duckdb_statements_columns_cache = columns
        return columns

    def get_stocks(
        self,
        sector: str | None = None,
        market: str | None = None,
    ) -> list[_DuckDbRow]:
        clauses: list[str] = []
        params: list[Any] = []
        if sector:
            clauses.append("sector_33_name = ?")
            params.append(sector)
        if market:
            clauses.append("market_code = ?")
            params.append(market)
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        return self.query(
            f"SELECT * FROM stocks {where_sql} ORDER BY code",
            tuple(params),
        )

    def get_stock_ohlcv(
        self,
        code: str,
        start: str | None = None,
        end: str | None = None,
    ) -> list[_DuckDbRow]:
        normalized = normalize_stock_code(code)
        clauses = ["code = ?"]
        params: list[Any] = [normalized]
        if start:
            clauses.append("date >= ?")
            params.append(start)
        if end:
            clauses.append("date <= ?")
            params.append(end)
        return self.query(
            f"SELECT * FROM stock_data WHERE {' AND '.join(clauses)} ORDER BY date",
            tuple(params),
        )

    def resolve_adjustment_basis(
        self,
        code: str,
        effective_market_date: str,
    ) -> StockAdjustmentBasis:
        """Resolve exactly one ready, containing, sufficiently covered basis."""
        row = adjustment_basis_queries.get_ready_adjustment_basis(
            self._fetchall_dicts,
            code,
            effective_market_date,
        )
        if row is None:
            raise RuntimeError(
                "Dataset snapshot has no unique complete ready adjustment basis "
                f"for {normalize_stock_code(code)} on {effective_market_date}"
            )
        return StockAdjustmentBasis(
            code=str(row["code"]),
            basis_id=str(row["basis_id"]),
            valid_from=str(row["valid_from"]),
            valid_to_exclusive=(
                str(row["valid_to_exclusive"])
                if row["valid_to_exclusive"] is not None
                else None
            ),
            adjustment_through_date=str(row["adjustment_through_date"]),
            source_fingerprint=str(row["source_fingerprint"]),
            materialized_through_date=str(row["materialized_through_date"]),
            status=cast(BasisStatus, row["status"]),
        )

    def get_basis_adjusted_stock_ohlcv(
        self,
        code: str,
        *,
        basis_id: str,
        start: str | None = None,
        end: str | None = None,
    ) -> pd.DataFrame:
        """Project OHLCV only from raw prices and the selected basis segments."""
        rows = adjustment_basis_queries.get_basis_adjusted_stock_data(
            self._fetchall_dicts,
            code,
            basis_id,
            start=start,
            end=end,
        )
        return pd.DataFrame(rows)

    def get_ohlcv_batch(
        self,
        codes: list[str],
        start: str | None = None,
        end: str | None = None,
    ) -> dict[str, list[_DuckDbRow]]:
        normalized = [normalize_stock_code(code) for code in codes]
        if not normalized:
            return {}
        code_placeholders = ",".join("?" for _ in normalized)
        clauses = [f"code IN ({code_placeholders})"]
        params: list[Any] = list(normalized)
        if start:
            clauses.append("date >= ?")
            params.append(start)
        if end:
            clauses.append("date <= ?")
            params.append(end)
        rows = self.query(
            f"""
            SELECT * FROM stock_data
            WHERE {' AND '.join(clauses)}
            ORDER BY code, date
            """,
            tuple(params),
        )
        result: dict[str, list[_DuckDbRow]] = {code: [] for code in normalized}
        for row in rows:
            result.setdefault(str(row.code), []).append(row)
        return result

    def get_topix(
        self,
        start: str | None = None,
        end: str | None = None,
    ) -> list[_DuckDbRow]:
        clauses: list[str] = []
        params: list[Any] = []
        if start:
            clauses.append("date >= ?")
            params.append(start)
        if end:
            clauses.append("date <= ?")
            params.append(end)
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        return self.query(
            f"SELECT * FROM topix_data {where_sql} ORDER BY date",
            tuple(params),
        )

    def get_indices(self) -> list[_DuckDbRow]:
        return self.query(
            """
            SELECT DISTINCT code, sector_name
            FROM indices_data
            ORDER BY code
            """
        )

    def get_index_data(
        self,
        code: str,
        start: str | None = None,
        end: str | None = None,
    ) -> list[_DuckDbRow]:
        clauses = ["code = ?"]
        params: list[Any] = [code]
        if start:
            clauses.append("date >= ?")
            params.append(start)
        if end:
            clauses.append("date <= ?")
            params.append(end)
        return self.query(
            f"SELECT * FROM indices_data WHERE {' AND '.join(clauses)} ORDER BY date",
            tuple(params),
        )

    def get_margin(
        self,
        code: str | None = None,
        start: str | None = None,
        end: str | None = None,
    ) -> list[_DuckDbRow]:
        clauses: list[str] = []
        params: list[Any] = []
        if code:
            clauses.append("code = ?")
            params.append(normalize_stock_code(code))
        if start:
            clauses.append("date >= ?")
            params.append(start)
        if end:
            clauses.append("date <= ?")
            params.append(end)
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        return self.query(
            f"SELECT * FROM margin_data {where_sql} ORDER BY date",
            tuple(params),
        )

    def get_margin_batch(
        self,
        codes: list[str],
        start: str | None = None,
        end: str | None = None,
    ) -> dict[str, list[_DuckDbRow]]:
        normalized = [normalize_stock_code(code) for code in codes]
        if not normalized:
            return {}
        code_placeholders = ",".join("?" for _ in normalized)
        clauses = [f"code IN ({code_placeholders})"]
        params: list[Any] = list(normalized)
        if start:
            clauses.append("date >= ?")
            params.append(start)
        if end:
            clauses.append("date <= ?")
            params.append(end)
        rows = self.query(
            f"""
            SELECT * FROM margin_data
            WHERE {' AND '.join(clauses)}
            ORDER BY code, date
            """,
            tuple(params),
        )
        result: dict[str, list[_DuckDbRow]] = {code: [] for code in normalized}
        for row in rows:
            result.setdefault(str(row.code), []).append(row)
        return result

    def get_statements(
        self,
        code: str,
        start: str | None = None,
        end: str | None = None,
        period_type: str = "all",
        actual_only: bool = True,
    ) -> list[_DuckDbRow]:
        normalized = normalize_stock_code(code)
        clauses = ["code = ?"]
        params: list[Any] = [normalized]
        if start:
            clauses.append("disclosed_date >= ?")
            params.append(start)
        if end:
            clauses.append("disclosed_date <= ?")
            params.append(end)

        period_values = _resolve_period_filter_values(period_type)
        if period_values:
            placeholders = ",".join("?" for _ in period_values)
            clauses.append(f"type_of_current_period IN ({placeholders})")
            params.extend(period_values)

        if actual_only:
            actual_clause = " OR ".join(f"{column} IS NOT NULL" for column in _ACTUAL_ONLY_COLUMNS)
            clauses.append(f"({actual_clause})")

        return self.query(
            f"SELECT * FROM statements WHERE {' AND '.join(clauses)} ORDER BY disclosed_date",
            tuple(params),
        )

    def get_adjusted_statement_metrics(
        self,
        code: str,
        *,
        basis_id: str,
        as_of_date: str | None = None,
    ) -> list[dict[str, Any]]:
        return get_adjusted_statement_metrics_for_basis(
            self._table_exists,
            self._fetchall_dicts,
            code,
            basis_id=basis_id,
            as_of_date=as_of_date,
        )

    def get_daily_valuation(
        self,
        code: str,
        *,
        basis_id: str,
        start: str | None = None,
        end: str | None = None,
    ) -> list[dict[str, Any]]:
        return get_daily_valuation_for_basis(
            self._table_exists,
            self._fetchall_dicts,
            code,
            basis_id=basis_id,
            start=start,
            end=end,
        )

    def get_statements_batch(
        self,
        codes: list[str],
        start: str | None = None,
        end: str | None = None,
        period_type: str = "all",
        actual_only: bool = True,
    ) -> dict[str, list[_DuckDbRow]]:
        normalized = [normalize_stock_code(code) for code in codes]
        if not normalized:
            return {}
        code_placeholders = ",".join("?" for _ in normalized)
        clauses = [f"code IN ({code_placeholders})"]
        params: list[Any] = list(normalized)
        if start:
            clauses.append("disclosed_date >= ?")
            params.append(start)
        if end:
            clauses.append("disclosed_date <= ?")
            params.append(end)
        period_values = _resolve_period_filter_values(period_type)
        if period_values:
            placeholders = ",".join("?" for _ in period_values)
            clauses.append(f"type_of_current_period IN ({placeholders})")
            params.extend(period_values)
        if actual_only:
            actual_clause = " OR ".join(f"{column} IS NOT NULL" for column in _ACTUAL_ONLY_COLUMNS)
            clauses.append(f"({actual_clause})")

        rows = self.query(
            f"""
            SELECT * FROM statements
            WHERE {' AND '.join(clauses)}
            ORDER BY code, disclosed_date
            """,
            tuple(params),
        )
        result: dict[str, list[_DuckDbRow]] = {code: [] for code in normalized}
        for row in rows:
            result.setdefault(str(row.code), []).append(row)
        return result

    def get_sectors(self) -> list[dict[str, str]]:
        rows = self.query(
            """
            SELECT DISTINCT sector_33_code, sector_33_name
            FROM stocks
            ORDER BY sector_33_code
            """
        )
        return [{"code": str(row[0]), "name": str(row[1])} for row in rows]

    def get_sector_mapping(self) -> dict[str, str]:
        return {sector["code"]: sector["name"] for sector in self.get_sectors()}

    def get_sector_stock_mapping(self) -> dict[str, list[str]]:
        rows = self.query(
            """
            SELECT sector_33_name, code
            FROM stocks
            ORDER BY sector_33_name, code
            """
        )
        result: dict[str, list[str]] = {}
        for row in rows:
            result.setdefault(str(row[0]), []).append(str(row[1]))
        return result

    def get_sector_stocks(self, sector_name: str) -> list[_DuckDbRow]:
        return self.query(
            """
            SELECT *
            FROM stocks
            WHERE sector_33_name = ?
            ORDER BY code
            """,
            (sector_name,),
        )

    def get_dataset_info(self) -> dict[str, str]:
        rows = self.query("SELECT key, value FROM dataset_info")
        return {str(row.key): str(row.value) for row in rows}

    def get_stock_count(self) -> int:
        row = self.query_one("SELECT COUNT(*) AS count FROM stocks")
        return int(row.count) if row is not None else 0

    def get_stock_list_with_counts(self, min_records: int = 100) -> list[_DuckDbRow]:
        return self.query(
            """
            SELECT
                s.code AS stockCode,
                COUNT(sd.date) AS record_count,
                MIN(sd.date) AS start_date,
                MAX(sd.date) AS end_date
            FROM stocks s
            LEFT JOIN stock_data sd ON s.code = sd.code
            GROUP BY s.code
            HAVING COUNT(sd.date) >= ?
            ORDER BY s.code
            """,
            (min_records,),
        )

    def get_index_list_with_counts(self, min_records: int = 100) -> list[_DuckDbRow]:
        return self.query(
            """
            SELECT
                code AS indexCode,
                MIN(sector_name) AS indexName,
                COUNT(date) AS record_count,
                MIN(date) AS start_date,
                MAX(date) AS end_date
            FROM indices_data
            GROUP BY code
            HAVING COUNT(date) >= ?
            ORDER BY code
            """,
            (min_records,),
        )

    def get_margin_list(self, min_records: int = 10) -> list[_DuckDbRow]:
        return self.query(
            """
            SELECT
                code AS stockCode,
                COUNT(date) AS record_count,
                MIN(date) AS start_date,
                MAX(date) AS end_date,
                AVG(long_margin_volume) AS avg_long_margin,
                AVG(short_margin_volume) AS avg_short_margin
            FROM margin_data
            GROUP BY code
            HAVING COUNT(date) >= ?
            ORDER BY code
            """,
            (min_records,),
        )

    def search_stocks(self, term: str, exact: bool = False, limit: int = 50) -> list[_DuckDbRow]:
        if exact:
            return self.query(
                """
                SELECT code, company_name, 'exact' AS match_type
                FROM stocks
                WHERE code = ? OR company_name = ?
                LIMIT ?
                """,
                (term, term, limit),
            )
        pattern = f"%{term}%"
        return self.query(
            """
            SELECT code, company_name, 'partial' AS match_type
            FROM stocks
            WHERE code LIKE ? OR company_name LIKE ?
            ORDER BY code
            LIMIT ?
            """,
            (pattern, pattern, limit),
        )

    def get_sample_codes(self, size: int = 10, seed: int | None = None) -> list[str]:
        rows = self.query("SELECT code FROM stocks ORDER BY code")
        codes = [str(row[0]) for row in rows]
        if not codes:
            return []
        rng = random.Random(seed)  # noqa: S311
        return rng.sample(codes, min(size, len(codes)))

    def get_table_counts(self) -> dict[str, int]:
        tables = (
            "stocks",
            "stock_data",
            "topix_data",
            "indices_data",
            "margin_data",
            "statements",
            "dataset_info",
        )
        result: dict[str, int] = {}
        for table_name in tables:
            row = self.query_one(f"SELECT COUNT(*) AS count FROM {table_name}")
            result[table_name] = int(row.count) if row is not None else 0
        return result

    def get_date_range(self) -> dict[str, str] | None:
        row = self.query_one(
            """
            SELECT MIN(date) AS min, MAX(date) AS max
            FROM stock_data
            """
        )
        if row is None or row.min is None:
            return None
        return {"min": str(row.min), "max": str(row.max)}

    def get_sectors_with_count(self) -> list[_DuckDbRow]:
        return self.query(
            """
            SELECT sector_33_name AS sectorName, COUNT(code) AS count
            FROM stocks
            GROUP BY sector_33_name
            ORDER BY sector_33_name
            """
        )

    def get_stocks_with_quotes_count(self) -> int:
        row = self.query_one("SELECT COUNT(DISTINCT code) AS count FROM stock_data")
        return int(row.count) if row is not None else 0

    def get_stocks_with_margin_count(self) -> int:
        row = self.query_one("SELECT COUNT(DISTINCT code) AS count FROM margin_data")
        return int(row.count) if row is not None else 0

    def get_stocks_with_statements_count(self) -> int:
        row = self.query_one("SELECT COUNT(DISTINCT code) AS count FROM statements")
        return int(row.count) if row is not None else 0

    def get_fk_orphan_counts(self) -> dict[str, int]:
        stock_data_orphans = self.query_one(
            """
            SELECT COUNT(*) AS count
            FROM stock_data sd
            LEFT JOIN stocks s ON sd.code = s.code
            WHERE s.code IS NULL
            """
        )
        margin_data_orphans = self.query_one(
            """
            SELECT COUNT(*) AS count
            FROM margin_data m
            LEFT JOIN stocks s ON m.code = s.code
            WHERE s.code IS NULL
            """
        )
        statements_orphans = self.query_one(
            """
            SELECT COUNT(*) AS count
            FROM statements st
            LEFT JOIN stocks s ON st.code = s.code
            WHERE s.code IS NULL
            """
        )
        return {
            "stockDataOrphans": int(stock_data_orphans.count) if stock_data_orphans is not None else 0,
            "marginDataOrphans": int(margin_data_orphans.count) if margin_data_orphans is not None else 0,
            "statementsOrphans": int(statements_orphans.count) if statements_orphans is not None else 0,
        }

    def get_stocks_without_quotes_count(self) -> int:
        row = self.query_one(
            """
            SELECT COUNT(DISTINCT s.code) AS count
            FROM stocks s
            LEFT JOIN stock_data sd ON s.code = sd.code
            WHERE sd.code IS NULL
            """
        )
        return int(row.count) if row is not None else 0
