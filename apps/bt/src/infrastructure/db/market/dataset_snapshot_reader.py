"""Dataset snapshot reader for `dataset.duckdb + parquet + manifest` snapshots."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
import hashlib
import importlib
import json
from pathlib import Path
import random
import threading
from typing import Any, Literal, cast

from pydantic import BaseModel, ConfigDict, Field

from src.infrastructure.db.market.dataset_db import _resolve_period_filter_values
from src.infrastructure.db.market.query_helpers import normalize_stock_code

_ACTUAL_ONLY_COLUMNS = (
    "earnings_per_share",
    "profit",
    "equity",
)


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
    counts: "DatasetSnapshotCounts"
    coverage: "DatasetSnapshotCoverage"
    date_range: "DatasetSnapshotDateRange | None"


class DatasetSnapshotSource(BaseModel):
    model_config = ConfigDict(extra="forbid")

    backend: Literal["duckdb-parquet"] = "duckdb-parquet"
    compatibilityArtifact: str | None = None


class DatasetSnapshotDescriptor(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    preset: str = Field(min_length=1)
    duckdbFile: str = "dataset.duckdb"
    compatibilityDbFile: str | None = "dataset.db"
    parquetDir: str = "parquet"


class DatasetSnapshotChecksums(BaseModel):
    model_config = ConfigDict(extra="forbid")

    duckdbSha256: str = Field(min_length=1)
    logicalSha256: str = Field(min_length=1)
    compatibilityDbSha256: str | None = None
    parquet: dict[str, str] = Field(default_factory=dict)


class DatasetSnapshotCounts(BaseModel):
    model_config = ConfigDict(extra="forbid")

    stocks: int = Field(ge=0)
    stock_data: int = Field(ge=0)
    topix_data: int = Field(ge=0)
    indices_data: int = Field(ge=0)
    margin_data: int = Field(ge=0)
    statements: int = Field(ge=0)
    dataset_info: int = Field(ge=0)


class DatasetSnapshotCoverage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    totalStocks: int = Field(ge=0)
    stocksWithQuotes: int = Field(ge=0)
    stocksWithStatements: int = Field(ge=0)
    stocksWithMargin: int = Field(ge=0)


class DatasetSnapshotDateRange(BaseModel):
    model_config = ConfigDict(extra="forbid")

    min: str = Field(min_length=1)
    max: str = Field(min_length=1)


class DatasetSnapshotManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schemaVersion: Literal[1] = 1
    generatedAt: str = Field(min_length=1)
    dataset: DatasetSnapshotDescriptor
    source: DatasetSnapshotSource
    counts: DatasetSnapshotCounts
    coverage: DatasetSnapshotCoverage
    checksums: DatasetSnapshotChecksums
    dateRange: DatasetSnapshotDateRange | None = None


def _sha256_of_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def build_dataset_snapshot_logical_checksum(
    *,
    counts: DatasetSnapshotCounts,
    coverage: DatasetSnapshotCoverage,
    date_range: DatasetSnapshotDateRange | None,
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


def inspect_dataset_snapshot_duckdb(duckdb_path: str | Path) -> DatasetSnapshotInspection:
    conn = _connect_duckdb(Path(duckdb_path), read_only=True)
    try:
        counts = DatasetSnapshotCounts(
            stocks=_query_scalar_int(conn, "SELECT COUNT(*) FROM stocks"),
            stock_data=_query_scalar_int(conn, "SELECT COUNT(*) FROM stock_data"),
            topix_data=_query_scalar_int(conn, "SELECT COUNT(*) FROM topix_data"),
            indices_data=_query_scalar_int(conn, "SELECT COUNT(*) FROM indices_data"),
            margin_data=_query_scalar_int(conn, "SELECT COUNT(*) FROM margin_data"),
            statements=_query_scalar_int(conn, "SELECT COUNT(*) FROM statements"),
            dataset_info=_query_scalar_int(conn, "SELECT COUNT(*) FROM dataset_info"),
        )
        coverage = DatasetSnapshotCoverage(
            totalStocks=counts.stocks,
            stocksWithQuotes=_query_scalar_int(conn, "SELECT COUNT(DISTINCT code) FROM stock_data"),
            stocksWithStatements=_query_scalar_int(conn, "SELECT COUNT(DISTINCT code) FROM statements"),
            stocksWithMargin=_query_scalar_int(conn, "SELECT COUNT(DISTINCT code) FROM margin_data"),
        )
        date_row = conn.execute("SELECT MIN(date), MAX(date) FROM stock_data").fetchone()
    finally:
        conn.close()

    date_range = None
    if date_row is not None and date_row[0] is not None:
        date_range = DatasetSnapshotDateRange(min=str(date_row[0]), max=str(date_row[1]))
    return DatasetSnapshotInspection(counts=counts, coverage=coverage, date_range=date_range)


def read_dataset_snapshot_manifest(snapshot_dir: str | Path) -> DatasetSnapshotManifest:
    manifest_path = Path(snapshot_dir) / "manifest.v1.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    return DatasetSnapshotManifest.model_validate(payload)


def validate_dataset_snapshot(snapshot_dir: str | Path) -> DatasetSnapshotManifest:
    snapshot_root = Path(snapshot_dir)
    manifest = read_dataset_snapshot_manifest(snapshot_root)
    if manifest.schemaVersion != 1:
        raise RuntimeError(f"Unsupported dataset snapshot schemaVersion: {manifest.schemaVersion}")

    duckdb_path = snapshot_root / manifest.dataset.duckdbFile
    if not duckdb_path.exists():
        raise FileNotFoundError(f"dataset.duckdb not found: {duckdb_path}")
    if _sha256_of_file(duckdb_path) != manifest.checksums.duckdbSha256:
        raise RuntimeError(f"dataset.duckdb checksum mismatch: {duckdb_path}")

    compatibility_db = manifest.dataset.compatibilityDbFile
    compatibility_sha = manifest.checksums.compatibilityDbSha256
    if compatibility_db and compatibility_sha:
        compatibility_path = snapshot_root / compatibility_db
        if not compatibility_path.exists():
            raise FileNotFoundError(f"dataset.db compatibility artifact not found: {compatibility_path}")
        if _sha256_of_file(compatibility_path) != compatibility_sha:
            raise RuntimeError(f"dataset.db checksum mismatch: {compatibility_path}")

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
    if inspection.counts != manifest.counts:
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


class DatasetSnapshotReader:
    """Resolve and validate a dataset snapshot, then read directly from DuckDB."""

    def __init__(self, snapshot_dir: str) -> None:
        self._snapshot_dir = Path(snapshot_dir)
        self._manifest = validate_dataset_snapshot(self._snapshot_dir)
        compatibility_name = self._manifest.dataset.compatibilityDbFile
        if not compatibility_name:
            raise RuntimeError("Dataset snapshot compatibility artifact is not configured")
        compatibility_path = self._snapshot_dir / compatibility_name
        if not compatibility_path.exists():
            raise FileNotFoundError(f"Dataset snapshot compatibility artifact not found: {compatibility_path}")

        self._duckdb_path = self._snapshot_dir / self._manifest.dataset.duckdbFile
        self._conns: dict[int, Any] = {}
        self._conn_lock = threading.Lock()
        self._duckdb_statements_columns_cache: set[str] | None = None

    @property
    def snapshot_dir(self) -> Path:
        return self._snapshot_dir

    @property
    def manifest(self) -> DatasetSnapshotManifest:
        return self._manifest

    def _create_connection(self) -> Any:
        return _connect_duckdb(self._duckdb_path, read_only=True)

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

    def get_ohlcv_batch(self, codes: list[str]) -> dict[str, list[_DuckDbRow]]:
        normalized = [normalize_stock_code(code) for code in codes]
        if not normalized:
            return {}
        placeholders = ",".join("?" for _ in normalized)
        rows = self.query(
            f"""
            SELECT * FROM stock_data
            WHERE code IN ({placeholders})
            ORDER BY code, date
            """,
            tuple(normalized),
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

    def get_margin_batch(self, codes: list[str]) -> dict[str, list[_DuckDbRow]]:
        normalized = [normalize_stock_code(code) for code in codes]
        if not normalized:
            return {}
        placeholders = ",".join("?" for _ in normalized)
        rows = self.query(
            f"""
            SELECT * FROM margin_data
            WHERE code IN ({placeholders})
            ORDER BY code, date
            """,
            tuple(normalized),
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
