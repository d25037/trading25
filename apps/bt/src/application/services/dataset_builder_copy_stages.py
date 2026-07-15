"""Dataset snapshot copy stages used by dataset_builder_service."""

from __future__ import annotations

import asyncio
import importlib
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from time import perf_counter
from typing import Any

from src.application.services.generic_job_manager import JobInfo
from src.application.contracts.jobs import JobProgress
from src.infrastructure.db.dataset_io.dataset_writer import (
    DatasetSnapshotError,
    DatasetWriter,
    StockDataCopyResult,
)
from src.infrastructure.db.dataset_io.snapshot_contract import (
    MARKET_V4_EVENT_TIME_REQUIRED_TABLES,
)
from src.infrastructure.db.market.query_helpers import normalize_stock_code

_TOTAL_STAGES = 8
_BATCH_COPY_SIZE = 200
_WARNING_SAMPLE_SIZE = 5
class DatasetBuildCancelled(Exception):
    def __init__(self, processed_stocks: int = 0) -> None:
        self.processed_stocks = processed_stocks


class DatasetWriterWorker:
    """Keep DuckDB-backed DatasetWriter calls on one dedicated thread."""

    def __init__(self, snapshot_path: str) -> None:
        self._snapshot_path = snapshot_path
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="dataset-writer")
        self._writer: DatasetWriter | None = None

    def _get_writer(self) -> DatasetWriter:
        if self._writer is None:
            self._writer = DatasetWriter(self._snapshot_path)
        return self._writer

    def _call(self, method_name: str, *args: Any, **kwargs: Any) -> Any:
        writer = self._get_writer()
        method = getattr(writer, method_name)
        return method(*args, **kwargs)

    def _close(self) -> None:
        if self._writer is None:
            return
        self._writer.close()
        self._writer = None

    async def call(self, method_name: str, *args: Any, **kwargs: Any) -> Any:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor,
            partial(self._call, method_name, *args, **kwargs),
        )

    async def close(self) -> None:
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(self._executor, self._close)
        finally:
            self._executor.shutdown(wait=True)


ProgressCallback = Callable[[str, int, int, str], None]
StageLogCallback = Callable[..., None]
def _raise_if_cancelled(
    job: JobInfo[Any, JobProgress, Any],
    processed: int,
) -> None:
    if job.cancelled.is_set():
        raise DatasetBuildCancelled(processed)


def preflight_event_time_pit_source(source_duckdb_path: str) -> None:
    """Reject non-v4 Market sources without creating Dataset artifacts."""
    duckdb = importlib.import_module("duckdb")
    conn = duckdb.connect(source_duckdb_path, read_only=True)
    try:
        tables = {
            str(row[0])
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main'"
            ).fetchall()
        }
        missing = sorted(MARKET_V4_EVENT_TIME_REQUIRED_TABLES - tables)
        if missing:
            raise DatasetSnapshotError(
                "Market v4 event-time source is missing required tables: "
                + ", ".join(missing)
            )
        version = conn.execute("SELECT MAX(version) FROM market_schema_version").fetchone()
        if version is None or version[0] != 4:
            raise DatasetSnapshotError(
                "Dataset v3 snapshots require Market schema version 4"
            )
        mode = conn.execute(
            "SELECT value FROM sync_metadata "
            "WHERE key = 'stock_price_adjustment_mode'"
        ).fetchone()
        if mode is None or mode[0] != "local_projection_v2_event_time":
            raise DatasetSnapshotError(
                "Dataset v3 snapshots require local_projection_v2_event_time"
            )
    finally:
        conn.close()


async def copy_event_time_pit_stage(
    *,
    job: JobInfo[Any, JobProgress, Any],
    processed: int,
    writer_worker: Any,
    source_duckdb_path: str,
    normalized_codes: Sequence[str],
    date_from: str | None,
    date_to: str | None,
    progress: ProgressCallback,
    log_stage_elapsed: StageLogCallback,
) -> None:
    pit_started = perf_counter()
    progress(
        "event_time_pit",
        3,
        _TOTAL_STAGES,
        "Copying complete event-time PIT data from market.duckdb...",
    )
    _raise_if_cancelled(job, processed)
    result = await writer_worker.call(
        "copy_event_time_pit_from_source",
        source_duckdb_path=source_duckdb_path,
        normalized_codes=list(normalized_codes),
        date_from=date_from,
        date_to=date_to,
    )
    _raise_if_cancelled(job, processed)
    log_stage_elapsed(
        "event_time_pit",
        pit_started,
        mode="duckdb-direct",
        target_count=len(normalized_codes),
        inserted_rows=sum(
            (
                result.raw_price_rows,
                result.stock_master_rows,
                result.basis_rows,
                result.segment_rows,
                result.statement_metric_rows,
                result.daily_valuation_rows,
            )
        ),
    )


async def copy_stock_data_stage(
    *,
    job: JobInfo[Any, JobProgress, Any],
    filtered: Sequence[dict[str, Any]],
    writer_worker: Any,
    source_duckdb_path: str,
    date_to: str,
    copy_mode: str,
    warnings: list[str],
    progress: ProgressCallback,
    log_stage_elapsed: StageLogCallback,
    batch_size: int = _BATCH_COPY_SIZE,
) -> int:
    stock_data_started = perf_counter()
    progress(
        "stock_data",
        2,
        _TOTAL_STAGES,
        f"Copying stock price data from market.duckdb in batches ({len(filtered)} targets)...",
    )
    processed = 0
    empty_ohlcv_codes: list[str] = []
    incomplete_ohlcv_codes: list[tuple[str, int, int]] = []
    for batch in _chunked(filtered, batch_size):
        _raise_if_cancelled(job, processed)
        batch_codes = [normalize_stock_code(stock.get("Code", "")) for stock in batch]
        copy_result = await writer_worker.call(
            "copy_stock_data_from_source",
            source_duckdb_path=source_duckdb_path,
            normalized_codes=batch_codes,
            date_to=date_to,
        )
        _collect_stock_copy_warnings(
            batch_codes=batch_codes,
            copy_result=copy_result,
            empty_ohlcv_codes=empty_ohlcv_codes,
            incomplete_ohlcv_codes=incomplete_ohlcv_codes,
        )
        processed += len(batch)
        progress(
            "stock_data",
            2,
            _TOTAL_STAGES,
            f"Stock data from market.duckdb: {processed}/{len(filtered)}",
        )

    _append_stock_copy_warnings(
        warnings=warnings,
        empty_ohlcv_codes=empty_ohlcv_codes,
        incomplete_ohlcv_codes=incomplete_ohlcv_codes,
    )
    log_stage_elapsed("stock_data", stock_data_started, mode=copy_mode, target_count=len(filtered))
    return processed


def _append_stock_copy_warnings(
    *,
    warnings: list[str],
    empty_ohlcv_codes: list[str],
    incomplete_ohlcv_codes: list[tuple[str, int, int]],
) -> None:
    if empty_ohlcv_codes:
        warnings.append(
            "No valid OHLCV rows for "
            f"{len(empty_ohlcv_codes)} stocks "
            f"(sample: {_sample_text(empty_ohlcv_codes)})"
        )
    if incomplete_ohlcv_codes:
        warning_samples = [
            f"{code}({skipped}/{total})"
            for code, skipped, total in incomplete_ohlcv_codes[:_WARNING_SAMPLE_SIZE]
        ]
        warnings.append(
            "Skipped incomplete OHLCV rows for "
            f"{len(incomplete_ohlcv_codes)} stocks "
            f"(sample: {', '.join(warning_samples)})"
        )


async def copy_topix_stage(
    *,
    job: JobInfo[Any, JobProgress, Any],
    include_topix: bool,
    processed: int,
    writer_worker: Any,
    source_duckdb_path: str,
    date_to: str,
    copy_mode: str,
    progress: ProgressCallback,
    log_stage_elapsed: StageLogCallback,
) -> None:
    if not include_topix:
        return
    topix_started = perf_counter()
    progress("topix", 4, _TOTAL_STAGES, "Copying TOPIX data from market.duckdb...")
    _raise_if_cancelled(job, processed)
    inserted_rows = 0
    inserted_rows = await writer_worker.call(
        "copy_topix_data_from_source",
        source_duckdb_path=source_duckdb_path,
        date_to=date_to,
    )
    log_stage_elapsed("topix", topix_started, mode=copy_mode, inserted_rows=inserted_rows)


async def copy_indices_stage(
    *,
    job: JobInfo[Any, JobProgress, Any],
    include_sector_indices: bool,
    processed: int,
    writer_worker: Any,
    source_duckdb_path: str,
    date_to: str,
    copy_mode: str,
    progress: ProgressCallback,
    log_stage_elapsed: StageLogCallback,
    index_catalog_codes: Callable[[], set[str]],
) -> None:
    if not include_sector_indices:
        return
    indices_started = perf_counter()
    target_index_codes = sorted(
        code for code in index_catalog_codes() if _is_sector_index_code(code)
    )
    progress(
        "indices",
        5,
        _TOTAL_STAGES,
        f"Copying sector index data from market.duckdb in one batch ({len(target_index_codes)} targets)...",
    )
    _raise_if_cancelled(job, processed)
    inserted_rows = 0
    inserted_rows = await writer_worker.call(
        "copy_indices_data_from_source",
        source_duckdb_path=source_duckdb_path,
        normalized_codes=target_index_codes,
        date_to=date_to,
    )
    log_stage_elapsed(
        "indices",
        indices_started,
        mode=copy_mode,
        target_count=len(target_index_codes),
        inserted_rows=inserted_rows,
    )


async def copy_statements_stage(
    *,
    job: JobInfo[Any, JobProgress, Any],
    include_statements: bool,
    filtered: Sequence[dict[str, Any]],
    processed: int,
    writer_worker: Any,
    source_duckdb_path: str,
    date_to: str,
    copy_mode: str,
    progress: ProgressCallback,
    log_stage_elapsed: StageLogCallback,
    batch_size: int = _BATCH_COPY_SIZE,
) -> None:
    if not include_statements:
        return
    statements_started = perf_counter()
    progress(
        "statements",
        6,
        _TOTAL_STAGES,
        f"Copying financial statements from market.duckdb in batches ({len(filtered)} targets)...",
    )
    statements_processed = 0
    for batch in _chunked(filtered, batch_size):
        _raise_if_cancelled(job, processed)
        batch_codes = [normalize_stock_code(stock.get("Code", "")) for stock in batch]
        await writer_worker.call(
            "copy_statements_from_source",
            source_duckdb_path=source_duckdb_path,
            normalized_codes=batch_codes,
            date_to=date_to,
        )
        statements_processed += len(batch)
        progress(
            "statements",
            6,
            _TOTAL_STAGES,
            f"Financial statements from market.duckdb: {statements_processed}/{len(filtered)}",
        )
    log_stage_elapsed("statements", statements_started, mode=copy_mode, target_count=len(filtered))


async def copy_margin_stage(
    *,
    job: JobInfo[Any, JobProgress, Any],
    include_margin: bool,
    filtered: Sequence[dict[str, Any]],
    processed: int,
    writer_worker: Any,
    source_duckdb_path: str,
    date_to: str,
    copy_mode: str,
    progress: ProgressCallback,
    log_stage_elapsed: StageLogCallback,
    batch_size: int = _BATCH_COPY_SIZE,
) -> None:
    if not include_margin:
        return
    margin_started = perf_counter()
    progress(
        "margin",
        7,
        _TOTAL_STAGES,
        f"Copying margin data from market.duckdb in batches ({len(filtered)} targets)...",
    )
    margin_processed = 0
    for batch in _chunked(filtered, batch_size):
        _raise_if_cancelled(job, processed)
        batch_codes = [normalize_stock_code(stock.get("Code", "")) for stock in batch]
        await writer_worker.call(
            "copy_margin_data_from_source",
            source_duckdb_path=source_duckdb_path,
            normalized_codes=batch_codes,
            date_to=date_to,
        )
        margin_processed += len(batch)
        progress(
            "margin",
            7,
            _TOTAL_STAGES,
            f"Margin data from market.duckdb: {margin_processed}/{len(filtered)}",
        )
    log_stage_elapsed("margin", margin_started, mode=copy_mode, target_count=len(filtered))


def _sample_text(values: list[str]) -> str:
    sample = values[:_WARNING_SAMPLE_SIZE]
    suffix = ", ..." if len(values) > _WARNING_SAMPLE_SIZE else ""
    return ", ".join(sample) + suffix


def _collect_stock_copy_warnings(
    *,
    batch_codes: Sequence[str],
    copy_result: StockDataCopyResult,
    empty_ohlcv_codes: list[str],
    incomplete_ohlcv_codes: list[tuple[str, int, int]],
) -> None:
    for code in batch_codes:
        stats = copy_result.code_stats.get(code)
        if stats is None:
            empty_ohlcv_codes.append(code)
            continue
        if stats.skipped_rows > 0:
            incomplete_ohlcv_codes.append((code, stats.skipped_rows, stats.total_rows))
        if stats.valid_rows == 0:
            empty_ohlcv_codes.append(code)


def _normalize_index_code(value: Any) -> str:
    text = str(value).strip() if value is not None else ""
    if not text:
        return ""
    if text.isdigit() and len(text) < 4:
        return text.zfill(4)
    return text.upper()


def _is_sector_index_code(code: str) -> bool:
    normalized = _normalize_index_code(code)
    try:
        value = int(normalized, 16)
    except ValueError:
        return False
    return (
        int("0040", 16) <= value <= int("0060", 16)
        or int("0080", 16) <= value <= int("0090", 16)
    )


def _chunked(values: Sequence[dict[str, Any]], size: int) -> list[list[dict[str, Any]]]:
    return [list(values[index : index + size]) for index in range(0, len(values), size)]
