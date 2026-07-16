"""Lease-bound, bounded-growth maintenance for the Market DuckDB."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import stat
from time import perf_counter
from typing import Any, Callable, Protocol, cast
from uuid import uuid4

from .atomic_exchange import PlatformAtomicExchange
from .managed_root import ManagedRootFd
from .market_source_identity import inspect_market_source_identity
from .market_writer_resources import MarketMaintenanceAuthority
from .valuation_queries import (
    get_adjusted_metrics_snapshot,
    get_adjusted_metrics_source_diagnostics,
)


MIB = 1024 * 1024
GIB = 1024 * MIB
SOFT_FREE_BYTES = 512 * MIB
SOFT_FREE_RATIO = 0.10
HARD_FREE_BYTES = 1 * GIB
MIN_CAPACITY_RESERVE_BYTES = 512 * MIB


class CompactionTrigger(StrEnum):
    NONE = "none"
    SOFT = "soft_threshold"
    HARD = "hard_cap"


@dataclass(frozen=True)
class DuckDbSizeSnapshot:
    block_size: int
    total_blocks: int
    used_blocks: int
    free_blocks: int
    wal_bytes: int

    @property
    def total_bytes(self) -> int:
        return self.total_blocks * self.block_size

    @property
    def free_bytes(self) -> int:
        return self.free_blocks * self.block_size

    @property
    def free_ratio(self) -> float:
        return self.free_bytes / self.total_bytes if self.total_bytes else 0.0


def evaluate_compaction_trigger(snapshot: DuckDbSizeSnapshot) -> CompactionTrigger:
    if snapshot.free_bytes >= HARD_FREE_BYTES:
        return CompactionTrigger.HARD
    if (
        snapshot.free_bytes >= SOFT_FREE_BYTES
        and snapshot.free_ratio >= SOFT_FREE_RATIO
    ):
        return CompactionTrigger.SOFT
    return CompactionTrigger.NONE


def required_compaction_capacity(source_bytes: int) -> int:
    reserve = max(MIN_CAPACITY_RESERVE_BYTES, math.ceil(source_bytes * 0.10))
    return source_bytes + reserve


class MarketCompactionError(RuntimeError):
    """Verified maintenance could not safely complete."""


class RegularFileExchange(Protocol):
    def exchange_regular_files(
        self,
        managed_root: ManagedRootFd,
        left: Path,
        right: Path,
        *,
        expected_right_parent_identity: tuple[int, int] | None = None,
    ) -> None: ...


@dataclass(frozen=True)
class MarketValidationSnapshot:
    schema_fingerprint: str
    table_counts: dict[str, int]
    semantic_digests: dict[str, str]


@dataclass(frozen=True)
class MarketMaintenanceEvidence:
    compacted: bool
    trigger: CompactionTrigger
    before: DuckDbSizeSnapshot
    after: DuckDbSizeSnapshot
    before_bytes: int
    after_bytes: int
    duration_ms: float
    validation: str
    schema_fingerprint: str
    table_counts: dict[str, int]
    semantic_digests: dict[str, str]


@dataclass(frozen=True)
class CandidateStaging:
    directory_relative: Path
    candidate_relative: Path
    candidate_path: Path
    parent_identity: tuple[int, int]


_CRITICAL_TABLES = (
    "stock_data_raw",
    "stock_data",
    "stock_master_daily",
    "stock_adjustment_bases",
    "stock_adjustment_basis_segments",
    "statements",
    "statement_metrics_adjusted",
    "daily_valuation",
    "margin_data",
    "topix_data",
    "indices_data",
)
_JOURNAL_NAME = ".market-maintenance.v1.jsonl"
_STAGING_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}")
_JOURNAL_STATES = (
    "VALIDATED",
    "EXCHANGE_INTENT",
    "EXCHANGED",
    "ACTIVE_VALIDATED",
    "COMMITTED",
    "CLEANED",
)


def _create_candidate_staging(
    authority: MarketMaintenanceAuthority,
    operation_id: str,
) -> CandidateStaging:
    authority.assert_valid()
    if _STAGING_ID.fullmatch(operation_id) is None:
        raise ValueError("Maintenance operation id is not safe")
    market_relative = authority.market_root.relative_to(authority.data_root)
    directory_relative = market_relative / f".market-maintenance-{operation_id}"
    with ManagedRootFd.open(authority.data_root) as root:
        directory_fd = root.open_dir(
            directory_relative,
            create=True,
            exclusive_leaf=True,
        )
        try:
            os.fchmod(directory_fd, 0o700)
            os.fsync(directory_fd)
            parent_stat = os.fstat(directory_fd)
        finally:
            os.close(directory_fd)
        root.fsync_dir(market_relative)
    candidate_relative = directory_relative / "candidate.duckdb"
    return CandidateStaging(
        directory_relative=directory_relative,
        candidate_relative=candidate_relative,
        candidate_path=authority.data_root / candidate_relative,
        parent_identity=(parent_stat.st_dev, parent_stat.st_ino),
    )


def _read_size_snapshot(path: Path) -> DuckDbSizeSnapshot:
    duckdb = __import__("duckdb")
    conn = cast(Any, duckdb).connect(str(path), read_only=True)
    try:
        row = conn.execute("PRAGMA database_size").fetchone()
        columns = [item[0] for item in conn.description]
    finally:
        conn.close()
    values = dict(zip(columns, row, strict=False))
    return DuckDbSizeSnapshot(
        block_size=int(values.get("block_size") or 0),
        total_blocks=int(values.get("total_blocks") or 0),
        used_blocks=int(values.get("used_blocks") or 0),
        free_blocks=int(values.get("free_blocks") or 0),
        wal_bytes=(
            Path(f"{path}.wal").stat().st_size if Path(f"{path}.wal").exists() else 0
        ),
    )


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _schema_objects(conn: Any) -> list[tuple[str, str]]:
    """Canonicalize every persistent catalog class exposed by DuckDB."""
    queries = (
        (
            "schema",
            "SELECT schema_name, comment, tags, sql FROM duckdb_schemas() "
            "WHERE NOT internal",
        ),
        (
            "table",
            "SELECT schema_name, table_name, comment, tags, has_primary_key, "
            "column_count, index_count, check_constraint_count, sql "
            "FROM duckdb_tables() WHERE NOT internal AND NOT temporary",
        ),
        (
            "column",
            "SELECT schema_name, table_name, column_name, column_index, comment, "
            "column_default, is_nullable, data_type, character_maximum_length, "
            "numeric_precision, numeric_precision_radix, numeric_scale "
            "FROM duckdb_columns() WHERE NOT internal",
        ),
        (
            "constraint",
            "SELECT schema_name, table_name, constraint_index, constraint_type, "
            "constraint_text, expression, constraint_column_indexes, "
            "constraint_column_names, constraint_name, referenced_table, "
            "referenced_column_names FROM duckdb_constraints()",
        ),
        (
            "view",
            "SELECT schema_name, view_name, comment, tags, column_count, sql, is_bound "
            "FROM duckdb_views() WHERE NOT internal AND NOT temporary",
        ),
        (
            "index",
            "SELECT schema_name, index_name, table_name, comment, tags, is_unique, "
            "is_primary, expressions, sql FROM duckdb_indexes()",
        ),
        (
            "sequence",
            "SELECT schema_name, sequence_name, comment, tags, start_value, min_value, "
            "max_value, increment_by, cycle, last_value, sql "
            "FROM duckdb_sequences() WHERE NOT temporary",
        ),
        (
            "type",
            "SELECT schema_name, type_name, type_size, logical_type, type_category, "
            "comment, tags, labels FROM duckdb_types() WHERE NOT internal",
        ),
        (
            "function",
            "SELECT schema_name, function_name, alias_of, function_type, description, "
            "comment, tags, return_type, parameters, parameter_types, varargs, "
            "macro_definition, has_side_effects, examples, stability, categories "
            "FROM duckdb_functions() WHERE NOT internal",
        ),
    )
    objects: list[tuple[str, str]] = []
    for kind, query in queries:
        rows = conn.execute(query).fetchall()
        objects.extend(
            (
                kind,
                json.dumps(
                    row,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=False,
                    default=str,
                ),
            )
            for row in rows
        )
    return sorted(objects)


def _semantic_digest(conn: Any, schema: str, table: str) -> str:
    qualified = f"{_quote_identifier(schema)}.{_quote_identifier(table)}"
    row = conn.execute(
        f"SELECT COUNT(*), COALESCE(bit_xor(hash(to_json(t))), 0), "
        f"COALESCE(sum(hash(to_json(t)))::VARCHAR, '0') FROM {qualified} AS t"
    ).fetchone()
    return hashlib.sha256(json.dumps(row, separators=(",", ":")).encode()).hexdigest()


def _validate_pit_lineage(conn: Any) -> None:
    invalid_queries = (
        """
        SELECT COUNT(*) FROM stock_adjustment_bases
        WHERE basis_id != 'event-pit-v1:' || code || ':' || CAST(valid_from AS VARCHAR)
           OR status != 'ready'
           OR (valid_to_exclusive IS NOT NULL AND valid_to_exclusive <= valid_from)
        """,
        """
        SELECT COUNT(*) FROM stock_adjustment_basis_segments s
        LEFT JOIN stock_adjustment_bases b USING (code, basis_id)
        WHERE b.basis_id IS NULL OR s.cumulative_factor <= 0
           OR (s.source_date_to_exclusive IS NOT NULL
               AND s.source_date_to_exclusive <= s.source_date_from)
        """,
        """
        SELECT COUNT(*) FROM statement_metrics_adjusted m
        LEFT JOIN stock_adjustment_bases b
          ON m.code = b.code AND m.basis_version = b.basis_id
        WHERE b.basis_id IS NULL
        """,
        """
        SELECT COUNT(*) FROM daily_valuation v
        LEFT JOIN stock_adjustment_bases b
          ON v.code = b.code AND v.basis_version = b.basis_id
        WHERE b.basis_id IS NULL
        """,
    )
    for query in invalid_queries:
        row = conn.execute(query).fetchone()
        if row and int(row[0]) != 0:
            raise MarketCompactionError("Market v4/PIT lineage validation failed")
    overlap = conn.execute(
        """
        SELECT COUNT(*) FROM stock_adjustment_bases current
        JOIN stock_adjustment_bases following
          ON current.code = following.code AND current.valid_from < following.valid_from
        WHERE current.valid_to_exclusive IS NULL
           OR current.valid_to_exclusive > following.valid_from
        """
    ).fetchone()
    if overlap and int(overlap[0]) != 0:
        raise MarketCompactionError("Market v4/PIT lineage validation failed")

    def table_exists(table: str) -> bool:
        return bool(
            conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
                [table],
            ).fetchone()[0]
        )

    def fetchone(sql: str, params: Any = None) -> Any:
        return conn.execute(sql, params or []).fetchone()

    snapshot = get_adjusted_metrics_snapshot(
        table_exists,
        lambda table: (
            int(
                conn.execute(
                    f"SELECT COUNT(*) FROM {_quote_identifier(table)}"
                ).fetchone()[0]
            )
            if table_exists(table)
            else 0
        ),
        fetchone,
    )
    invalid_snapshot = (
        "invalidBasisCount",
        "underCoveredActiveBasisCount",
        "overlappingBasisCount",
        "orphanAdjustedStatementRows",
        "orphanDailyValuationRows",
    )
    if any(int(snapshot.get(key, 0) or 0) != 0 for key in invalid_snapshot):
        raise MarketCompactionError("Market v4/PIT lineage validation failed")
    diagnostics = get_adjusted_metrics_source_diagnostics(
        table_exists,
        fetchone,
    )
    invalid_diagnostics = (
        "missingAdjustedStatementRows",
        "extraAdjustedStatementRows",
        "staleAdjustedStatementRows",
        "wrongBasisAdjustedStatementRows",
        "missingDailyValuationRows",
        "extraDailyValuationRows",
        "wrongBasisDailyValuationRows",
    )
    if any(int(diagnostics.get(key, 0)) != 0 for key in invalid_diagnostics):
        raise MarketCompactionError("Market v4/PIT lineage validation failed")


def _validation_snapshot(path: Path) -> MarketValidationSnapshot:
    inspect_market_source_identity(path)
    duckdb = __import__("duckdb")
    conn = cast(Any, duckdb).connect(str(path), read_only=True)
    try:
        objects = _schema_objects(conn)
        tables = sorted(
            (str(row[0]), str(row[1]))
            for row in conn.execute(
                "SELECT schema_name, table_name FROM duckdb_tables() "
                "WHERE NOT internal AND NOT temporary"
            ).fetchall()
        )
        counts = {
            f"{schema}.{table}": int(
                conn.execute(
                    f"SELECT COUNT(*) FROM {_quote_identifier(schema)}.{_quote_identifier(table)}"
                ).fetchone()[0]
            )
            for schema, table in tables
        }
        digests = {
            f"main.{table}": _semantic_digest(conn, "main", table)
            for table in _CRITICAL_TABLES
            if f"main.{table}" in counts
        }
        _validate_pit_lineage(conn)
    finally:
        conn.close()
    fingerprint = hashlib.sha256(
        json.dumps(objects, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()
    return MarketValidationSnapshot(fingerprint, counts, digests)


def _append_journal(path: Path, state: str, payload: dict[str, object]) -> None:
    if state not in _JOURNAL_STATES:
        raise ValueError("Unknown maintenance journal state")
    record = (
        json.dumps(
            {"schemaVersion": 1, "state": state, **payload},
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        + b"\n"
    )
    flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags, 0o600)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise MarketCompactionError("Maintenance journal must be a regular file")
        offset = 0
        while offset < len(record):
            written = os.write(fd, record[offset:])
            if written <= 0:
                raise OSError("Maintenance journal write made no progress")
            offset += written
        os.fsync(fd)
    finally:
        os.close(fd)
    parent = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(parent)
    finally:
        os.close(parent)


def _read_journal(path: Path) -> tuple[str, dict[str, object]]:
    try:
        path_stat = path.lstat()
        if not path_stat.st_mode or not path.is_file() or path.is_symlink():
            raise MarketCompactionError("Maintenance journal must be a regular file")
        raw_lines = path.read_bytes().splitlines(keepends=True)
    except OSError as exc:
        raise MarketCompactionError("Maintenance journal is unreadable") from exc
    if not raw_lines or any(not line.endswith(b"\n") for line in raw_lines):
        raise MarketCompactionError("Maintenance journal is torn")
    records: list[dict[str, object]] = []
    try:
        for line in raw_lines:
            value = json.loads(line)
            if not isinstance(value, dict) or value.get("schemaVersion") != 1:
                raise ValueError
            records.append(value)
    except (ValueError, json.JSONDecodeError) as exc:
        raise MarketCompactionError("Maintenance journal is invalid") from exc
    states = [record.get("state") for record in records]
    expected = list(_JOURNAL_STATES[: len(states)])
    if states != expected or len(states) > len(_JOURNAL_STATES):
        raise MarketCompactionError("Maintenance journal transition is invalid")
    payload = {
        key: value
        for key, value in records[0].items()
        if key not in {"schemaVersion", "state"}
    }
    for record in records[1:]:
        current = {
            key: value
            for key, value in record.items()
            if key not in {"schemaVersion", "state"}
        }
        if current != payload:
            raise MarketCompactionError("Maintenance journal identity changed")
    for name in ("source", "candidate"):
        identity = payload.get(name)
        if not isinstance(identity, dict) or any(
            type(identity.get(key)) is not int for key in ("device", "inode", "size")
        ):
            raise MarketCompactionError("Maintenance journal identity is invalid")
    candidate_relative = payload.get("candidateRelative")
    candidate_parent = payload.get("candidateParent")
    if (
        not isinstance(candidate_relative, str)
        or Path(candidate_relative).is_absolute()
        or ".." in Path(candidate_relative).parts
        or Path(candidate_relative).name != "candidate.duckdb"
        or not Path(candidate_relative).parent.name.startswith(".market-maintenance-")
        or not isinstance(candidate_parent, dict)
        or any(
            type(candidate_parent.get(key)) is not int for key in ("device", "inode")
        )
    ):
        raise MarketCompactionError("Maintenance journal candidate path is invalid")
    schema_fingerprint = payload.get("schemaFingerprint")
    table_counts = payload.get("tableCounts")
    semantic_digests = payload.get("semanticDigests")
    if (
        not isinstance(schema_fingerprint, str)
        or not isinstance(table_counts, dict)
        or any(
            not isinstance(key, str) or type(value) is not int
            for key, value in table_counts.items()
        )
        or not isinstance(semantic_digests, dict)
        or any(
            not isinstance(key, str) or not isinstance(value, str)
            for key, value in semantic_digests.items()
        )
    ):
        raise MarketCompactionError(
            "Maintenance journal validation snapshot is invalid"
        )
    return cast(str, states[-1]), payload


def _unlink_durable(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        return
    parent = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(parent)
    finally:
        os.close(parent)


def _remove_candidate_staging(
    authority: MarketMaintenanceAuthority,
    candidate: Path,
) -> None:
    relative = candidate.parent.relative_to(authority.data_root)
    with ManagedRootFd.open(authority.data_root) as root:
        root.remove_tree(relative, missing_ok=True)


class MarketCompactor:
    def __init__(
        self,
        *,
        size_reader: Callable[[Path], DuckDbSizeSnapshot] = _read_size_snapshot,
        available_bytes: Callable[[Path], int] = lambda path: (
            shutil.disk_usage(path).free
        ),
        copy_builder: Callable[[Path, Path, MarketMaintenanceAuthority], None]
        | None = None,
        exchange: RegularFileExchange | None = None,
    ) -> None:
        self._size_reader = size_reader
        self._available_bytes = available_bytes
        self._copy_builder = copy_builder or self._build_copy
        self._exchange = exchange or PlatformAtomicExchange()

    @staticmethod
    def _build_copy(
        source: Path, candidate: Path, authority: MarketMaintenanceAuthority
    ) -> None:
        conn = authority._open_writable_connection(candidate)
        try:
            escaped = str(source).replace("'", "''")
            conn.execute(f"ATTACH '{escaped}' AS source_db (READ_ONLY)")
            target_name = str(conn.execute("SELECT current_database()").fetchone()[0])
            conn.execute(
                f"COPY FROM DATABASE source_db TO {_quote_identifier(target_name)}"
            )
            conn.execute("CHECKPOINT")
        finally:
            conn.close()

    @staticmethod
    def _identity_payload(path: Path) -> dict[str, int]:
        item = path.stat()
        return {"device": item.st_dev, "inode": item.st_ino, "size": item.st_size}

    def _rollback(
        self,
        authority: MarketMaintenanceAuthority,
        source: Path,
        candidate: Path,
        original_inode: int,
        candidate_inode: int | None,
        candidate_parent_identity: tuple[int, int],
    ) -> None:
        try:
            parent_stat = candidate.parent.lstat()
            if (
                stat.S_ISLNK(parent_stat.st_mode)
                or not stat.S_ISDIR(parent_stat.st_mode)
                or (parent_stat.st_dev, parent_stat.st_ino) != candidate_parent_identity
            ):
                raise MarketCompactionError(
                    "Compaction candidate parent identity changed"
                )
            source_inode = source.stat().st_ino if source.exists() else None
            sibling_inode = candidate.stat().st_ino if candidate.exists() else None
            if (
                candidate_inode is not None
                and source_inode == candidate_inode
                and sibling_inode == original_inode
            ):
                with ManagedRootFd.open(authority.data_root) as root:
                    self._exchange.exchange_regular_files(
                        root,
                        source.relative_to(authority.data_root),
                        candidate.relative_to(authority.data_root),
                        expected_right_parent_identity=candidate_parent_identity,
                    )
            if source.stat().st_ino != original_inode:
                raise MarketCompactionError("Exact original could not be restored")
            _remove_candidate_staging(authority, candidate)
            _unlink_durable(source.parent / _JOURNAL_NAME)
        except BaseException as exc:
            authority.fence()
            raise MarketCompactionError(
                "Compaction rollback failed; writer ownership remains fenced"
            ) from exc

    def recover(self, authority: MarketMaintenanceAuthority) -> None:
        """Resolve one durable maintenance journal without guessing file identity."""
        authority.assert_ownership()
        source = authority.identity.path
        journal = source.parent / _JOURNAL_NAME
        if not journal.exists():
            authority.assert_valid()
            return
        try:
            state, payload = _read_journal(journal)
        except BaseException:
            authority.fence()
            raise

        def fail(message: str) -> None:
            authority.fence()
            raise MarketCompactionError(message)

        def matches(path: Path, identity: dict[str, int]) -> bool:
            try:
                value = path.lstat()
            except OSError:
                return False
            return stat.S_ISREG(value.st_mode) and (
                value.st_dev,
                value.st_ino,
                value.st_size,
            ) == (identity["device"], identity["inode"], identity["size"])

        candidate_relative = Path(cast(str, payload["candidateRelative"]))
        expected_market_relative = authority.market_root.relative_to(
            authority.data_root
        )
        if (
            candidate_relative.parent.parent != expected_market_relative
            or not candidate_relative.parent.name.startswith(".market-maintenance-")
        ):
            fail("Maintenance journal candidate root identity is invalid")
        candidate = authority.data_root / candidate_relative
        parent_record = cast(dict[str, int], payload["candidateParent"])
        candidate_parent_identity = (parent_record["device"], parent_record["inode"])
        source_record = cast(dict[str, int], payload["source"])
        candidate_record = cast(dict[str, int], payload["candidate"])
        original_inode = source_record["inode"]
        compact_inode = candidate_record["inode"]
        parent_present = candidate.parent.exists() or candidate.parent.is_symlink()
        if parent_present:
            parent_stat = candidate.parent.lstat()
            parent_matches = (
                not stat.S_ISLNK(parent_stat.st_mode)
                and stat.S_ISDIR(parent_stat.st_mode)
                and (parent_stat.st_dev, parent_stat.st_ino)
                == candidate_parent_identity
            )
        else:
            parent_matches = False
        original_layout = (
            parent_matches
            and matches(source, source_record)
            and matches(candidate, candidate_record)
        )
        exchanged_layout = (
            parent_matches
            and matches(source, candidate_record)
            and matches(candidate, source_record)
        )

        if state == "VALIDATED" or (state == "EXCHANGE_INTENT" and original_layout):
            if not original_layout:
                fail("Maintenance recovery identity mismatch")
            _remove_candidate_staging(authority, candidate)
            _unlink_durable(journal)
            authority.assert_valid()
            return
        if state in {"EXCHANGE_INTENT", "EXCHANGED", "ACTIVE_VALIDATED"}:
            if not exchanged_layout:
                fail("Maintenance recovery identity mismatch")
            self._rollback(
                authority,
                source,
                candidate,
                original_inode,
                compact_inode,
                candidate_parent_identity,
            )
            authority.assert_valid()
            return
        if state == "COMMITTED":
            committed_layout = matches(source, candidate_record) and (
                (matches(candidate, source_record) and parent_matches)
                or (not candidate.exists() and not parent_present)
            )
            if not committed_layout:
                fail("Maintenance recovery identity mismatch")
            try:
                active = _validation_snapshot(source)
            except BaseException:
                authority.fence()
                raise
            expected = MarketValidationSnapshot(
                schema_fingerprint=cast(str, payload["schemaFingerprint"]),
                table_counts=cast(dict[str, int], payload["tableCounts"]),
                semantic_digests=cast(dict[str, str], payload["semanticDigests"]),
            )
            if active != expected:
                fail("Committed maintenance validation identity mismatch")
            _remove_candidate_staging(authority, candidate)
            authority.replace_identity(inspect_market_source_identity(source))
            _append_journal(journal, "CLEANED", payload)
            _unlink_durable(journal)
            return
        if state == "CLEANED":
            if not matches(source, candidate_record) or parent_present:
                fail("Maintenance recovery identity mismatch")
            authority.replace_identity(inspect_market_source_identity(source))
            _unlink_durable(journal)
            return
        fail("Maintenance journal transition identity is invalid")

    def maintain(
        self, authority: MarketMaintenanceAuthority
    ) -> MarketMaintenanceEvidence:
        started = perf_counter()
        source = authority.identity.path
        journal = source.parent / _JOURNAL_NAME
        if journal.exists():
            self.recover(authority)
        authority.assert_valid()
        orphaned = [
            entry.name
            for entry in os.scandir(authority.market_root)
            if entry.name.startswith(".market-maintenance-")
        ]
        if orphaned:
            raise MarketCompactionError(
                "Unjournaled private maintenance staging requires operator review"
            )

        checkpoint = authority._open_writable_connection(source)
        try:
            checkpoint.execute("CHECKPOINT")
        finally:
            checkpoint.close()
        wal = Path(f"{source}.wal")
        if wal.exists() and wal.stat().st_size:
            raise MarketCompactionError("Market WAL is not empty after checkpoint")

        before = self._size_reader(source)
        trigger = evaluate_compaction_trigger(before)
        baseline = _validation_snapshot(source)
        before_bytes = source.stat().st_size
        if trigger is CompactionTrigger.NONE:
            return MarketMaintenanceEvidence(
                False,
                trigger,
                before,
                before,
                before_bytes,
                before_bytes,
                (perf_counter() - started) * 1000,
                "passed",
                baseline.schema_fingerprint,
                baseline.table_counts,
                baseline.semantic_digests,
            )
        required = required_compaction_capacity(before_bytes)
        if self._available_bytes(source.parent) < required:
            raise MarketCompactionError(
                f"Insufficient filesystem capacity for compaction; required={required}"
            )

        staging = _create_candidate_staging(authority, uuid4().hex)
        candidate = staging.candidate_path
        original_inode = source.stat().st_ino
        candidate_inode: int | None = None
        committed = False
        payload: dict[str, object] = {
            "source": self._identity_payload(source),
            "trigger": trigger.value,
            "candidateRelative": staging.candidate_relative.as_posix(),
            "candidateParent": {
                "device": staging.parent_identity[0],
                "inode": staging.parent_identity[1],
            },
        }
        try:
            self._copy_builder(source, candidate, authority)
            candidate_inode = candidate.stat().st_ino
            if (
                Path(f"{candidate}.wal").exists()
                and Path(f"{candidate}.wal").stat().st_size
            ):
                raise MarketCompactionError("Compaction candidate WAL is not empty")
            candidate_validation = _validation_snapshot(candidate)
            if candidate_validation != baseline:
                raise MarketCompactionError("Compaction candidate verification failed")
            payload["candidate"] = self._identity_payload(candidate)
            payload["schemaFingerprint"] = baseline.schema_fingerprint
            payload["tableCounts"] = baseline.table_counts
            payload["semanticDigests"] = baseline.semantic_digests
            _append_journal(journal, "VALIDATED", payload)
            _append_journal(journal, "EXCHANGE_INTENT", payload)
            with ManagedRootFd.open(authority.data_root) as root:
                self._exchange.exchange_regular_files(
                    root,
                    source.relative_to(authority.data_root),
                    candidate.relative_to(authority.data_root),
                    expected_right_parent_identity=staging.parent_identity,
                )
            _append_journal(journal, "EXCHANGED", payload)
            active_validation = _validation_snapshot(source)
            if active_validation != baseline:
                raise MarketCompactionError("Active compaction verification failed")
            after = self._size_reader(source)
            if after.free_bytes >= HARD_FREE_BYTES:
                raise MarketCompactionError(
                    "Compacted Market source remains above hard cap"
                )
            _append_journal(journal, "ACTIVE_VALIDATED", payload)
            _append_journal(journal, "COMMITTED", payload)
            committed = True
            _remove_candidate_staging(authority, candidate)
            _append_journal(journal, "CLEANED", payload)
            _unlink_durable(journal)
        except BaseException as exc:
            if committed:
                try:
                    self.recover(authority)
                except BaseException as recovery_error:
                    authority.fence()
                    raise MarketCompactionError(
                        "Post-commit cleanup failed and recovery remains fenced"
                    ) from recovery_error
                raise MarketCompactionError(
                    "Market compaction post-commit cleanup failed"
                ) from exc
            self._rollback(
                authority,
                source,
                candidate,
                original_inode,
                candidate_inode,
                staging.parent_identity,
            )
            if isinstance(exc, MarketCompactionError):
                raise
            raise MarketCompactionError("Market compaction exchange failed") from exc

        new_identity = inspect_market_source_identity(source)
        authority.replace_identity(new_identity)
        return MarketMaintenanceEvidence(
            True,
            trigger,
            before,
            after,
            before_bytes,
            source.stat().st_size,
            (perf_counter() - started) * 1000,
            "passed",
            baseline.schema_fingerprint,
            baseline.table_counts,
            baseline.semantic_digests,
        )
