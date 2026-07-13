"""Dataset snapshot copy stages used by dataset_builder_service."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from time import perf_counter
from typing import Any

from src.application.services.generic_job_manager import JobInfo
from src.application.services.stock_data_row_builder import build_stock_data_row
from src.application.contracts.jobs import JobProgress
from src.infrastructure.db.dataset_io.dataset_writer import (
    DatasetWriter,
    StockDataCopyResult,
)
from src.infrastructure.db.market.query_helpers import normalize_stock_code

_TOTAL_STAGES = 7
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
MarketLoader = Callable[..., Awaitable[Any]]


def _raise_if_cancelled(
    job: JobInfo[Any, JobProgress, Any],
    processed: int,
) -> None:
    if job.cancelled.is_set():
        raise DatasetBuildCancelled(processed)


async def copy_stock_data_stage(
    *,
    job: JobInfo[Any, JobProgress, Any],
    filtered: Sequence[dict[str, Any]],
    market_reader: Any,
    writer_worker: Any,
    source_duckdb_path: str | None,
    direct_copy_enabled: bool,
    copy_mode: str,
    warnings: list[str],
    progress: ProgressCallback,
    log_stage_elapsed: StageLogCallback,
    load_stock_data_batch: MarketLoader,
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
        if direct_copy_enabled and source_duckdb_path is not None:
            copy_result = await writer_worker.call(
                "copy_stock_data_from_source",
                source_duckdb_path=source_duckdb_path,
                normalized_codes=batch_codes,
            )
            _collect_stock_copy_warnings(
                batch_codes=batch_codes,
                copy_result=copy_result,
                empty_ohlcv_codes=empty_ohlcv_codes,
                incomplete_ohlcv_codes=incomplete_ohlcv_codes,
            )
        else:
            await _copy_legacy_stock_data_batch(
                batch=batch,
                batch_codes=batch_codes,
                market_reader=market_reader,
                writer_worker=writer_worker,
                empty_ohlcv_codes=empty_ohlcv_codes,
                incomplete_ohlcv_codes=incomplete_ohlcv_codes,
                load_stock_data_batch=load_stock_data_batch,
            )
            _raise_if_cancelled(job, processed)
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


async def _copy_legacy_stock_data_batch(
    *,
    batch: Sequence[dict[str, Any]],
    batch_codes: Sequence[str],
    market_reader: Any,
    writer_worker: Any,
    empty_ohlcv_codes: list[str],
    incomplete_ohlcv_codes: list[tuple[str, int, int]],
    load_stock_data_batch: MarketLoader,
) -> None:
    batch_data = await load_stock_data_batch(market_reader, batch_codes)
    rows_to_write: list[dict[str, Any]] = []
    for stock in batch:
        code4 = normalize_stock_code(stock.get("Code", ""))
        data = batch_data.get(code4, [])
        rows, skipped_rows = _build_stock_data_rows_for_code(code4, data)
        if skipped_rows > 0:
            incomplete_ohlcv_codes.append((code4, skipped_rows, len(data)))
        if rows:
            rows_to_write.extend(rows)
        else:
            empty_ohlcv_codes.append(code4)
    if rows_to_write:
        await writer_worker.call("upsert_stock_data", rows_to_write)


def _build_stock_data_rows_for_code(
    code: str,
    quotes: Sequence[Any],
) -> tuple[list[dict[str, Any]], int]:
    rows: list[dict[str, Any]] = []
    skipped_rows = 0
    for quote in quotes:
        if not isinstance(quote, dict):
            skipped_rows += 1
            continue
        created_at = quote.get("created_at")
        row = build_stock_data_row(
            quote,
            normalized_code=code,
            created_at=str(created_at) if created_at is not None else None,
        )
        if row is None:
            skipped_rows += 1
            continue
        rows.append(row)
    return rows, skipped_rows


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
    market_reader: Any,
    writer_worker: Any,
    source_duckdb_path: str | None,
    direct_copy_enabled: bool,
    copy_mode: str,
    progress: ProgressCallback,
    log_stage_elapsed: StageLogCallback,
    load_topix_data: MarketLoader,
) -> None:
    if not include_topix:
        return
    topix_started = perf_counter()
    progress("topix", 3, _TOTAL_STAGES, "Copying TOPIX data from market.duckdb...")
    _raise_if_cancelled(job, processed)
    inserted_rows = 0
    if direct_copy_enabled and source_duckdb_path is not None:
        inserted_rows = await writer_worker.call(
            "copy_topix_data_from_source",
            source_duckdb_path=source_duckdb_path,
        )
    else:
        topix_rows = await load_topix_data(market_reader)
        _raise_if_cancelled(job, processed)
        if topix_rows:
            inserted_rows = len(topix_rows)
            await writer_worker.call("upsert_topix_data", topix_rows)
    log_stage_elapsed("topix", topix_started, mode=copy_mode, inserted_rows=inserted_rows)


async def copy_indices_stage(
    *,
    job: JobInfo[Any, JobProgress, Any],
    include_sector_indices: bool,
    processed: int,
    market_reader: Any,
    writer_worker: Any,
    source_duckdb_path: str | None,
    direct_copy_enabled: bool,
    copy_mode: str,
    progress: ProgressCallback,
    log_stage_elapsed: StageLogCallback,
    load_index_data_batch: MarketLoader,
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
        4,
        _TOTAL_STAGES,
        f"Copying sector index data from market.duckdb in one batch ({len(target_index_codes)} targets)...",
    )
    _raise_if_cancelled(job, processed)
    inserted_rows = 0
    if direct_copy_enabled and source_duckdb_path is not None:
        inserted_rows = await writer_worker.call(
            "copy_indices_data_from_source",
            source_duckdb_path=source_duckdb_path,
            normalized_codes=target_index_codes,
        )
    else:
        index_rows = await load_index_data_batch(market_reader, target_index_codes)
        _raise_if_cancelled(job, processed)
        rows_to_write = [
            row
            for code in target_index_codes
            for row in index_rows.get(_normalize_index_code(code), [])
        ]
        if rows_to_write:
            inserted_rows = len(rows_to_write)
            await writer_worker.call("upsert_indices_data", rows_to_write)
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
    market_reader: Any,
    writer_worker: Any,
    source_duckdb_path: str | None,
    direct_copy_enabled: bool,
    copy_mode: str,
    progress: ProgressCallback,
    log_stage_elapsed: StageLogCallback,
    load_statements_batch: MarketLoader,
    batch_size: int = _BATCH_COPY_SIZE,
) -> None:
    if not include_statements:
        return
    statements_started = perf_counter()
    progress(
        "statements",
        5,
        _TOTAL_STAGES,
        f"Copying financial statements from market.duckdb in batches ({len(filtered)} targets)...",
    )
    statements_processed = 0
    for batch in _chunked(filtered, batch_size):
        _raise_if_cancelled(job, processed)
        batch_codes = [normalize_stock_code(stock.get("Code", "")) for stock in batch]
        if direct_copy_enabled and source_duckdb_path is not None:
            await writer_worker.call(
                "copy_statements_from_source",
                source_duckdb_path=source_duckdb_path,
                normalized_codes=batch_codes,
            )
            await writer_worker.call(
                "copy_adjusted_metrics_from_source",
                source_duckdb_path=source_duckdb_path,
                normalized_codes=batch_codes,
            )
        else:
            statement_rows = await load_statements_batch(market_reader, batch_codes)
            _raise_if_cancelled(job, processed)
            rows_to_write = [
                row
                for stock in batch
                for row in statement_rows.get(normalize_stock_code(stock.get("Code", "")), [])
            ]
            if rows_to_write:
                await writer_worker.call("upsert_statements", rows_to_write)
        statements_processed += len(batch)
        progress(
            "statements",
            5,
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
    market_reader: Any,
    writer_worker: Any,
    source_duckdb_path: str | None,
    direct_copy_enabled: bool,
    copy_mode: str,
    progress: ProgressCallback,
    log_stage_elapsed: StageLogCallback,
    load_margin_batch: MarketLoader,
    batch_size: int = _BATCH_COPY_SIZE,
) -> None:
    if not include_margin:
        return
    margin_started = perf_counter()
    progress(
        "margin",
        6,
        _TOTAL_STAGES,
        f"Copying margin data from market.duckdb in batches ({len(filtered)} targets)...",
    )
    margin_processed = 0
    for batch in _chunked(filtered, batch_size):
        _raise_if_cancelled(job, processed)
        batch_codes = [normalize_stock_code(stock.get("Code", "")) for stock in batch]
        if direct_copy_enabled and source_duckdb_path is not None:
            await writer_worker.call(
                "copy_margin_data_from_source",
                source_duckdb_path=source_duckdb_path,
                normalized_codes=batch_codes,
            )
        else:
            margin_rows = await load_margin_batch(market_reader, batch_codes)
            _raise_if_cancelled(job, processed)
            rows_to_write = [
                row
                for stock in batch
                for row in margin_rows.get(normalize_stock_code(stock.get("Code", "")), [])
            ]
            if rows_to_write:
                await writer_worker.call("upsert_margin_data", rows_to_write)
        margin_processed += len(batch)
        progress(
            "margin",
            6,
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
