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

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from src.infrastructure.db.dataset_io.snapshot_contract import (
    DATASET_FUNDAMENTALS_BASIS_DATE_INFO_KEY,
    DATASET_PROVIDER_AS_OF_INFO_KEY,
    DATASET_PROVIDER_COVERAGE_END_INFO_KEY,
    DATASET_PROVIDER_COVERAGE_START_INFO_KEY,
    DATASET_PROVIDER_PLAN_INFO_KEY,
    DATASET_PROVIDER_SOURCE_FINGERPRINT_INFO_KEY,
    DATASET_V4_PARQUET_ARTIFACT_NAMES,
    DATASET_V4_PHYSICAL_SCHEMAS,
    DATASET_V4_REQUIRED_TABLES,
)
from src.infrastructure.db.dataset_io.pit_validation import (
    find_dataset_snapshot_audit_error,
)
from src.infrastructure.db.market.query_helpers import normalize_stock_code
from src.shared.models.types import normalize_period_type

_ACTUAL_ONLY_COLUMNS = (
    "earnings_per_share",
    "profit",
    "equity",
)
_REQUIRED_SNAPSHOT_TABLES = tuple(sorted(DATASET_V4_REQUIRED_TABLES))


class UnsupportedDatasetSnapshotError(RuntimeError):
    """The bundle uses a snapshot generation that runtime no longer supports."""


class DatasetManifestValidationError(RuntimeError):
    """The v4 manifest or its DuckDB payload is incomplete or inconsistent."""


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
    counts: "DatasetLogicalCountsV4"
    coverage: "DatasetCoverageV4"
    date_range: "DatasetDateRangeV4 | None"
    source: "DatasetSourceV4"


Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class DatasetSourceV4(BaseModel):
    model_config = ConfigDict(extra="forbid")

    backend: Literal["duckdb-parquet"]
    marketSchemaVersion: Literal[5]
    stockPriceAdjustmentMode: Literal["provider_adjusted_v1"]
    providerPlan: str = Field(min_length=1)
    providerAsOf: str = Field(min_length=1)
    providerCoverageStart: str = Field(min_length=1)
    providerCoverageEnd: str = Field(min_length=1)
    providerSourceFingerprint: Sha256
    fundamentalsAdjustmentBasisDate: str = Field(min_length=1)


class DatasetDescriptorV4(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    preset: str = Field(min_length=1)
    duckdbFile: Literal["dataset.duckdb"]
    parquetDir: Literal["parquet"]


class DatasetChecksumsV4(BaseModel):
    model_config = ConfigDict(extra="forbid")

    duckdbSha256: Sha256
    logicalSha256: Sha256
    parquet: dict[str, Sha256]

    @model_validator(mode="after")
    def require_exact_parquet_artifacts(self) -> "DatasetChecksumsV4":
        actual = set(self.parquet)
        if actual != DATASET_V4_PARQUET_ARTIFACT_NAMES:
            missing = sorted(DATASET_V4_PARQUET_ARTIFACT_NAMES - actual)
            extra = sorted(actual - DATASET_V4_PARQUET_ARTIFACT_NAMES)
            raise ValueError(
                "parquet checksum keys must exactly match Dataset v4 artifacts; "
                f"missing={missing}, extra={extra}"
            )
        return self


class DatasetLogicalCountsV4(BaseModel):
    model_config = ConfigDict(extra="forbid")

    stocks: int = Field(ge=0)
    stock_data: int = Field(ge=0)
    topix_data: int = Field(ge=0)
    indices_data: int = Field(ge=0)
    margin_data: int = Field(ge=0)
    statements: int = Field(ge=0)
    stock_data_raw: int = Field(ge=0)
    stock_master_daily: int = Field(ge=0)
    statement_metrics_adjusted: int = Field(ge=0)
    daily_valuation: int = Field(ge=0)
    dataset_info: int = Field(ge=0)


class DatasetCoverageV4(BaseModel):
    model_config = ConfigDict(extra="forbid")

    totalStocks: int = Field(ge=0)
    stocksWithQuotes: int = Field(ge=0)
    stocksWithStatements: int = Field(ge=0)
    stocksWithMargin: int = Field(ge=0)


class DatasetDateRangeV4(BaseModel):
    model_config = ConfigDict(extra="forbid")

    min: str = Field(min_length=1)
    max: str = Field(min_length=1)


class DatasetManifestV4(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schemaVersion: Literal[4] = 4
    generatedAt: str = Field(min_length=1)
    dataset: DatasetDescriptorV4
    source: DatasetSourceV4
    logicalCounts: DatasetLogicalCountsV4
    coverage: DatasetCoverageV4
    checksums: DatasetChecksumsV4
    dateRange: DatasetDateRangeV4 | None = None


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
    manifest: DatasetManifestV4
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
    source: DatasetSourceV4,
    counts: DatasetLogicalCountsV4,
    coverage: DatasetCoverageV4,
    date_range: DatasetDateRangeV4 | None,
) -> str:
    payload = {
        "source": source.model_dump(),
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

def _canonical_date(value: str, *, field: str) -> str:
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise DatasetManifestValidationError(
            f"{field} must be a canonical ISO YYYY-MM-DD date"
        ) from exc
    if parsed.isoformat() != value:
        raise DatasetManifestValidationError(
            f"{field} must be a canonical ISO YYYY-MM-DD date"
        )
    return value


def _read_source_from_dataset_info(conn: Any) -> DatasetSourceV4:
    rows = conn.execute("SELECT key, value FROM dataset_info").fetchall()
    info = {str(key): str(value) for key, value in rows}
    required = {
        "manifest_schema_version": "4",
        "source_market_schema_version": "5",
        "source_stock_price_adjustment_mode": "provider_adjusted_v1",
        DATASET_PROVIDER_PLAN_INFO_KEY: None,
        DATASET_PROVIDER_AS_OF_INFO_KEY: None,
        DATASET_PROVIDER_COVERAGE_START_INFO_KEY: None,
        DATASET_PROVIDER_COVERAGE_END_INFO_KEY: None,
        DATASET_PROVIDER_SOURCE_FINGERPRINT_INFO_KEY: None,
        DATASET_FUNDAMENTALS_BASIS_DATE_INFO_KEY: None,
    }
    for key, exact in required.items():
        value = info.get(key)
        if value is None or not value.strip() or (exact is not None and value != exact):
            raise DatasetManifestValidationError(
                f"Dataset provider vintage metadata is missing or invalid: {key}"
            )
    source = DatasetSourceV4(
        backend="duckdb-parquet",
        marketSchemaVersion=5,
        stockPriceAdjustmentMode="provider_adjusted_v1",
        providerPlan=info[DATASET_PROVIDER_PLAN_INFO_KEY],
        providerAsOf=info[DATASET_PROVIDER_AS_OF_INFO_KEY],
        providerCoverageStart=info[DATASET_PROVIDER_COVERAGE_START_INFO_KEY],
        providerCoverageEnd=info[DATASET_PROVIDER_COVERAGE_END_INFO_KEY],
        providerSourceFingerprint=info[
            DATASET_PROVIDER_SOURCE_FINGERPRINT_INFO_KEY
        ],
        fundamentalsAdjustmentBasisDate=info[
            DATASET_FUNDAMENTALS_BASIS_DATE_INFO_KEY
        ],
    )
    start = _canonical_date(
        source.providerCoverageStart, field="providerCoverageStart"
    )
    end = _canonical_date(source.providerCoverageEnd, field="providerCoverageEnd")
    as_of = _canonical_date(source.providerAsOf, field="providerAsOf")
    basis = _canonical_date(
        source.fundamentalsAdjustmentBasisDate,
        field="fundamentalsAdjustmentBasisDate",
    )
    if start > end or end > as_of or basis != end:
        raise DatasetManifestValidationError(
            "Dataset provider vintage dates are incoherent"
        )
    return source


def _validate_provider_snapshot_integrity(
    conn: Any,
    *,
    source: DatasetSourceV4,
) -> None:
    audit_error = find_dataset_snapshot_audit_error(
        conn,
        coverage_start=source.providerCoverageStart,
        coverage_end=source.providerCoverageEnd,
        fundamentals_basis_date=source.fundamentalsAdjustmentBasisDate,
        tables={table: table for table in _REQUIRED_SNAPSHOT_TABLES},
    )
    if audit_error is not None:
        raise DatasetManifestValidationError(audit_error)
    stock_count = _table_count(conn, "stocks")
    if stock_count:
        session_bounds = conn.execute(
            "SELECT count(*), min(date), max(date) FROM topix_data"
        ).fetchone()
        if (
            session_bounds is None
            or int(session_bounds[0] or 0) == 0
            or str(session_bounds[1]) != source.providerCoverageStart
            or str(session_bounds[2]) != source.providerCoverageEnd
        ):
            raise DatasetManifestValidationError(
                "Dataset provider coverage lacks exact market sessions at both bounds"
            )
        expected_pairs = (
            "SELECT stocks.code, sessions.date FROM stocks "
            "CROSS JOIN topix_data AS sessions"
        )
        for table in (
            "stock_master_daily",
            "stock_data_raw",
            "stock_data",
            "daily_valuation",
        ):
            if _query_scalar_int(
                conn,
                f"""
                SELECT COUNT(*) FROM (
                    ({expected_pairs} EXCEPT ALL SELECT code, date FROM {table})
                    UNION ALL
                    (SELECT code, date FROM {table} EXCEPT ALL {expected_pairs})
                ) differences
                """,
            ):
                raise DatasetManifestValidationError(
                    "Dataset provider session coverage has an empty, gap, or bound "
                    f"mismatch: {table}"
                )
    if _query_scalar_int(
        conn,
        """
        SELECT COUNT(*) FROM (
            (SELECT code, date FROM stock_data
             EXCEPT ALL SELECT code, date FROM stock_data_raw)
            UNION ALL
            (SELECT code, date FROM stock_data_raw
             EXCEPT ALL SELECT code, date FROM stock_data)
        ) differences
        """,
    ):
        raise DatasetManifestValidationError(
            "Dataset provider-adjusted/raw price coverage differs"
        )
    if _query_scalar_int(
        conn,
        """
        SELECT COUNT(*) FROM (
            (SELECT code, date FROM stock_data
             EXCEPT ALL SELECT code, date FROM daily_valuation)
            UNION ALL
            (SELECT code, date FROM daily_valuation
             EXCEPT ALL SELECT code, date FROM stock_data)
        ) differences
        """,
    ):
        raise DatasetManifestValidationError(
            "Dataset daily valuation coverage differs from provider prices"
        )
    if _query_scalar_int(
        conn,
        """
        SELECT COUNT(*) FROM daily_valuation valuation
        JOIN stock_data price USING (code, date)
        WHERE valuation.close IS DISTINCT FROM price.close
        """,
    ):
        raise DatasetManifestValidationError(
            "Dataset daily valuation close differs from provider price"
        )
    for table in ("statement_metrics_adjusted", "daily_valuation"):
        columns = {
            str(row[1])
            for row in conn.execute(f"PRAGMA table_info('{table}')").fetchall()
        }
        forbidden = {"basis_id", "basis_version", "price_basis_version"} & columns
        if forbidden:
            raise DatasetManifestValidationError(
                f"Dataset v4 payload retains unsupported basis columns in {table}"
            )


def inspect_dataset_snapshot_duckdb(duckdb_path: str | Path) -> DatasetSnapshotInspection:
    conn = _connect_duckdb(Path(duckdb_path), read_only=True)
    try:
        existing_tables = {
            str(row[0])
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
        }
        expected_tables = set(_REQUIRED_SNAPSHOT_TABLES)
        if existing_tables != expected_tables:
            missing_tables = sorted(expected_tables - existing_tables)
            extra_tables = sorted(existing_tables - expected_tables)
            raise DatasetManifestValidationError(
                "Dataset v4 requires the exact table set; "
                f"missing={missing_tables}, extra={extra_tables}"
            )
        for table, (expected_columns, expected_pk) in DATASET_V4_PHYSICAL_SCHEMAS.items():
            actual_columns = tuple(
                (str(row[1]), str(row[2]), bool(row[3]))
                for row in conn.execute(f"PRAGMA table_info('{table}')").fetchall()
            )
            pk_row = conn.execute(
                "SELECT constraint_column_names FROM duckdb_constraints() "
                "WHERE table_name = ? AND constraint_type = 'PRIMARY KEY'",
                (table,),
            ).fetchone()
            actual_pk = tuple(str(value) for value in pk_row[0]) if pk_row else ()
            if actual_columns != expected_columns or actual_pk != expected_pk:
                raise DatasetManifestValidationError(
                    f"Dataset v4 physical schema mismatch for {table}"
                )
        named_indexes = conn.execute(
            "SELECT index_name FROM duckdb_indexes() WHERE schema_name = 'main'"
        ).fetchall()
        if named_indexes:
            raise DatasetManifestValidationError(
                "Dataset v4 physical schema has unsupported named indexes"
            )
        source = _read_source_from_dataset_info(conn)
        counts = DatasetLogicalCountsV4(
            stocks=_table_count(conn, "stocks"),
            stock_data=_table_count(conn, "stock_data"),
            topix_data=_table_count(conn, "topix_data"),
            indices_data=_table_count(conn, "indices_data"),
            margin_data=_table_count(conn, "margin_data"),
            statements=_table_count(conn, "statements"),
            stock_data_raw=_table_count(conn, "stock_data_raw"),
            stock_master_daily=_table_count(conn, "stock_master_daily"),
            statement_metrics_adjusted=_table_count(
                conn,
                "statement_metrics_adjusted",
            ),
            daily_valuation=_table_count(conn, "daily_valuation"),
            dataset_info=_table_count(conn, "dataset_info"),
        )
        coverage = DatasetCoverageV4(
            totalStocks=counts.stocks,
            stocksWithQuotes=_query_scalar_int(conn, "SELECT COUNT(DISTINCT code) FROM stock_data"),
            stocksWithStatements=_query_scalar_int(conn, "SELECT COUNT(DISTINCT code) FROM statements"),
            stocksWithMargin=_query_scalar_int(conn, "SELECT COUNT(DISTINCT code) FROM margin_data"),
        )
        date_row = conn.execute("SELECT MIN(date), MAX(date) FROM stock_data").fetchone()
        _validate_provider_snapshot_integrity(conn, source=source)
    finally:
        conn.close()

    date_range = None
    if date_row is not None and date_row[0] is not None:
        date_range = DatasetDateRangeV4(min=str(date_row[0]), max=str(date_row[1]))
    return DatasetSnapshotInspection(
        counts=counts,
        coverage=coverage,
        date_range=date_range,
        source=source,
    )


def _validate_raw_manifest_lineage(payload: object) -> None:
    if not isinstance(payload, dict):
        raise UnsupportedDatasetSnapshotError(
            "Unsupported dataset snapshot schemaVersion: None"
        )
    schema_version = payload.get("schemaVersion")
    if type(schema_version) is not int or schema_version != 4:
        raise UnsupportedDatasetSnapshotError(
            f"Unsupported dataset snapshot schemaVersion: {schema_version}"
        )

    source = payload.get("source")
    market_schema_version = (
        source.get("marketSchemaVersion") if isinstance(source, dict) else None
    )
    if type(market_schema_version) is not int or market_schema_version != 5:
        raise DatasetManifestValidationError(
            "Dataset snapshot source.marketSchemaVersion must be the integer 5"
        )


def read_dataset_snapshot_manifest(snapshot_dir: str | Path) -> DatasetManifestV4:
    manifest_path = Path(snapshot_dir) / "manifest.v2.json"
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise DatasetManifestValidationError(f"Invalid dataset manifest JSON: {exc}") from exc
    _validate_raw_manifest_lineage(payload)
    try:
        return DatasetManifestV4.model_validate(payload)
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
    physical_parquet = {path.name for path in parquet_dir.iterdir()}
    if physical_parquet != DATASET_V4_PARQUET_ARTIFACT_NAMES:
        raise DatasetManifestValidationError(
            "Dataset physical Parquet artifacts must exactly match Dataset v4"
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
    """Identify a fully validated runtime-compatible v4 bundle."""
    try:
        validate_supported_dataset_snapshot_proof(snapshot_dir)
    except Exception:
        return False
    return True


def validate_dataset_snapshot(snapshot_dir: str | Path) -> DatasetManifestV4:
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
    physical_parquet = {path.name for path in parquet_dir.iterdir()}
    if physical_parquet != DATASET_V4_PARQUET_ARTIFACT_NAMES:
        raise DatasetManifestValidationError(
            "Dataset physical Parquet artifacts must exactly match Dataset v4"
        )
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
    if inspection.source != manifest.source:
        raise RuntimeError("Dataset snapshot manifest source metadata mismatch")
    logical_checksum = build_dataset_snapshot_logical_checksum(
        source=inspection.source,
        counts=inspection.counts,
        coverage=inspection.coverage,
        date_range=inspection.date_range,
    )
    if logical_checksum != manifest.checksums.logicalSha256:
        raise RuntimeError("Dataset snapshot logical checksum mismatch")

    return manifest


def validate_supported_dataset_snapshot(snapshot_dir: str | Path) -> DatasetManifestV4:
    """Validate all support invariants shared by discovery and runtime resolve."""
    return validate_dataset_snapshot(Path(snapshot_dir))


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
    def manifest(self) -> DatasetManifestV4:
        return self._manifest

    def get_snapshot_lineage(
        self,
    ) -> tuple[Literal[4], Literal[5], Literal["provider_adjusted_v1"]]:
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
        self._assert_artifacts_unchanged()
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
        as_of_date: str | None = None,
    ) -> list[dict[str, Any]]:
        clauses = ["code = ?"]
        params: list[Any] = [normalize_stock_code(code)]
        if as_of_date is not None:
            clauses.append("disclosed_date <= ?")
            params.append(as_of_date)
        return self._fetchall_dicts(
            f"SELECT * FROM statement_metrics_adjusted "
            f"WHERE {' AND '.join(clauses)} "
            "ORDER BY disclosed_date, period_end, period_type",
            params,
        )

    def get_daily_valuation(
        self,
        code: str,
        *,
        start: str | None = None,
        end: str | None = None,
    ) -> list[dict[str, Any]]:
        clauses = ["code = ?"]
        params: list[Any] = [normalize_stock_code(code)]
        if start is not None:
            clauses.append("date >= ?")
            params.append(start)
        if end is not None:
            clauses.append("date <= ?")
            params.append(end)
        return self._fetchall_dicts(
            f"SELECT * FROM daily_valuation WHERE {' AND '.join(clauses)} "
            "ORDER BY date",
            params,
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
