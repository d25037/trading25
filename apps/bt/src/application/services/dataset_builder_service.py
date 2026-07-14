"""
Dataset Builder Service

データセット作成のオーケストレーション。
GenericJobManager を使用してバックグラウンドビルドを管理する。
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import shutil
from collections.abc import Iterable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from time import perf_counter
from typing import Any, Protocol

from loguru import logger

from src.shared.utils.market_code_alias import expand_market_codes
from src.application.services.dataset_builder_copy_stages import (
    DatasetBuildCancelled as _DatasetBuildCancelled,
    copy_event_time_pit_stage as _copy_event_time_pit_stage,
    copy_indices_stage as _copy_indices_stage,
    copy_margin_stage as _copy_margin_stage,
    copy_statements_stage as _copy_statements_stage,
    copy_stock_data_stage as _copy_stock_data_stage,
    copy_topix_stage as _copy_topix_stage,
    preflight_event_time_pit_source as _preflight_event_time_pit_source,
)
from src.application.services.dataset_presets import PresetConfig, get_preset
from src.application.services.dataset_resolver import DatasetResolver
from src.application.services.generic_job_manager import GenericJobManager, JobInfo
from src.application.services.index_master_catalog import get_index_catalog_codes
from src.application.contracts.jobs import JobProgress, JobStatus
from src.infrastructure.db.dataset_io.dataset_writer import (
    DatasetWriter,
    StockDataCopyResult as _StockDataCopyResult,
    duckdb_path_for_path,
    parquet_dir_for_path,
    snapshot_dir_for_path,
)
from src.infrastructure.db.market.dataset_snapshot_reader import (
    build_dataset_snapshot_logical_checksum,
    inspect_dataset_snapshot_duckdb,
)
from src.infrastructure.db.market.query_helpers import (
    expand_stock_code,
    normalize_stock_code,
)
from src.shared.config.reliability import DATASET_BUILD_TIMEOUT_MINUTES


@dataclass
class DatasetJobData:
    name: str
    preset: str
    overwrite: bool = False


@dataclass
class DatasetResult:
    success: bool
    totalStocks: int = 0
    processedStocks: int = 0
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    outputPath: str = ""


StockDataCopyResult = _StockDataCopyResult

__all__ = ["StockDataCopyResult"]


# Module-level manager instance
dataset_job_manager: GenericJobManager[DatasetJobData, JobProgress, DatasetResult] = GenericJobManager()
_TOTAL_STAGES = 8
_BATCH_COPY_SIZE = 200
class MarketDatasetSource(Protocol):
    def query(self, sql: str, params: tuple[Any, ...] = ()) -> list[Any]:
        """Read-only DuckDB query interface."""
        ...


class _DatasetWriterWorker:
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


def _manifest_path_for_snapshot(snapshot_path: str) -> Path:
    return snapshot_dir_for_path(snapshot_path) / "manifest.v2.json"


def _delete_dataset_artifacts(resolver: DatasetResolver, name: str) -> None:
    for path in resolver.get_artifact_paths(name):
        target = Path(path)
        if target.is_dir():
            shutil.rmtree(target)
        elif target.exists():
            target.unlink()


def _sha256_of_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _cancelled_build_result(
    success_result: DatasetResult,
    snapshot_dir: Path,
) -> DatasetResult:
    return DatasetResult(
        success=False,
        totalStocks=success_result.totalStocks,
        processedStocks=success_result.processedStocks,
        warnings=success_result.warnings,
        errors=["Cancelled"],
        outputPath=str(snapshot_dir),
    )


async def _write_staged_manifest_off_thread(
    *,
    snapshot_path: str,
    dataset_name: str,
    preset_name: str,
    staged_manifest_path: Path,
) -> str:
    task = asyncio.create_task(
        asyncio.to_thread(
            _write_dataset_manifest,
            snapshot_path=snapshot_path,
            dataset_name=dataset_name,
            preset_name=preset_name,
            manifest_path=staged_manifest_path,
        )
    )
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError as cancelled:
        try:
            await task
        except Exception:
            logger.exception("Manifest checksum worker failed during cancellation")
        staged_manifest_path.unlink(missing_ok=True)
        raise cancelled


def _write_dataset_manifest(
    *,
    snapshot_path: str,
    dataset_name: str,
    preset_name: str,
    manifest_path: Path | None = None,
) -> str:
    duckdb_path = duckdb_path_for_path(snapshot_path)
    parquet_dir = parquet_dir_for_path(snapshot_path)
    if not duckdb_path.exists():
        raise FileNotFoundError(f"dataset.duckdb not found: {duckdb_path}")

    inspection = inspect_dataset_snapshot_duckdb(duckdb_path)
    counts = inspection.counts.model_dump()
    coverage = inspection.coverage.model_dump()
    date_range = inspection.date_range.model_dump() if inspection.date_range is not None else None

    manifest = {
        "schemaVersion": 3,
        "generatedAt": datetime.now(UTC).isoformat(),
        "dataset": {
            "name": dataset_name,
            "preset": preset_name,
            "duckdbFile": duckdb_path.name,
            "parquetDir": parquet_dir.name,
        },
        "source": {
            "backend": "duckdb-parquet",
            "marketSchemaVersion": 4,
            "stockPriceAdjustmentMode": "local_projection_v2_event_time",
        },
        "logicalCounts": counts,
        "coverage": coverage,
        "checksums": {
            "duckdbSha256": _sha256_of_file(duckdb_path),
            "logicalSha256": build_dataset_snapshot_logical_checksum(
                counts=inspection.counts,
                coverage=inspection.coverage,
                date_range=inspection.date_range,
            ),
            "parquet": {
                parquet_file.name: _sha256_of_file(parquet_file)
                for parquet_file in sorted(parquet_dir.glob("*.parquet"))
            },
        },
    }
    if date_range is not None:
        manifest["dateRange"] = {
            "min": date_range.get("min"),
            "max": date_range.get("max"),
        }

    output_path = manifest_path or _manifest_path_for_snapshot(snapshot_path)
    output_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(output_path)


async def start_dataset_build(
    data: DatasetJobData,
    resolver: DatasetResolver,
    market_reader: MarketDatasetSource,
    source_duckdb_path: str,
) -> JobInfo[DatasetJobData, JobProgress, DatasetResult] | None:
    """データセットビルドジョブを作成して開始"""
    if not source_duckdb_path:
        raise ValueError("source_duckdb_path is required")
    job = await dataset_job_manager.create_job(data)
    if job is None:
        return None

    async def _run() -> None:
        try:
            result = await asyncio.wait_for(
                _build_dataset(
                    job,
                    resolver,
                    market_reader,
                    source_duckdb_path=source_duckdb_path,
                ),
                timeout=DATASET_BUILD_TIMEOUT_MINUTES * 60,
            )
            if job.status == JobStatus.COMPLETED:
                return
            if dataset_job_manager.is_cancelled(job.job_id):
                return
            dataset_job_manager.complete_job(job.job_id, result)
        except asyncio.TimeoutError:
            if dataset_job_manager.is_cancelled(job.job_id):
                return
            dataset_job_manager.fail_job(
                job.job_id,
                f"Dataset build timed out after {DATASET_BUILD_TIMEOUT_MINUTES} minutes",
            )
        except asyncio.CancelledError:
            pass
        except Exception as e:
            if dataset_job_manager.is_cancelled(job.job_id):
                return
            logger.exception(f"Dataset build {job.job_id} failed: {e}")
            dataset_job_manager.fail_job(job.job_id, str(e))

    task = asyncio.create_task(_run())
    job.task = task
    return job


async def _build_dataset(
    job: JobInfo[DatasetJobData, JobProgress, DatasetResult],
    resolver: DatasetResolver,
    market_reader: MarketDatasetSource,
    *,
    source_duckdb_path: str,
) -> DatasetResult:
    """データセットをビルドする実際のロジック"""
    name = job.data.name
    preset_name = job.data.preset
    warnings: list[str] = []
    errors: list[str] = []

    preset = get_preset(preset_name)
    if preset is None:
        return DatasetResult(success=False, errors=[f"Unknown preset: {preset_name}"])

    if not source_duckdb_path:
        raise ValueError("source_duckdb_path is required")
    if not Path(source_duckdb_path).exists():
        raise FileNotFoundError(f"market.duckdb not found: {source_duckdb_path}")
    await asyncio.to_thread(
        _preflight_event_time_pit_source,
        source_duckdb_path,
    )

    if job.data.overwrite:
        resolver.evict(name)
        _delete_dataset_artifacts(resolver, name)

    snapshot_path = resolver.get_dataset_path(name)
    snapshot_dir = snapshot_dir_for_path(snapshot_path)

    def progress(stage: str, current: int, total: int, message: str) -> None:
        pct = (current / total * 100) if total > 0 else 0
        dataset_job_manager.update_progress(
            job.job_id,
            JobProgress(stage=stage, current=current, total=total, percentage=pct, message=message),
        )

    def log_stage_elapsed(
        stage: str,
        started_at: float,
        *,
        mode: str,
        target_count: int | None = None,
        inserted_rows: int | None = None,
    ) -> None:
        logger.bind(
            event="dataset_build_stage_complete",
            stage=stage,
            mode=mode,
            elapsedSeconds=round(perf_counter() - started_at, 4),
            targetCount=target_count,
            insertedRows=inserted_rows,
            jobId=job.job_id,
            dataset=name,
        ).info("Dataset build stage complete")

    # Step 1: 銘柄マスタ取得
    master_started = perf_counter()
    progress("master", 0, _TOTAL_STAGES, "Loading stock master from market.duckdb...")
    if job.cancelled.is_set():
        return DatasetResult(success=False, errors=["Cancelled"])

    stocks_data = await _load_market_stock_master(market_reader)
    filtered = _filter_stocks(stocks_data, preset)
    log_stage_elapsed("master", master_started, mode="duckdb-direct", target_count=len(filtered))

    if not filtered:
        return DatasetResult(success=False, errors=["No stocks matched the preset filters"])

    normalized_codes = sorted(
        {
            normalize_stock_code(stock.get("Code", ""))
            for stock in filtered
            if normalize_stock_code(stock.get("Code", ""))
        }
    )
    stock_date_from: str | None = None
    stock_date_to: str | None = None
    stock_date_from, stock_date_to = await _load_market_stock_date_range(
        market_reader,
        normalized_codes,
    )
    if job.cancelled.is_set():
        return DatasetResult(success=False, errors=["Cancelled"])

    # Step 2: Writer 作成
    progress("init", 1, _TOTAL_STAGES, f"Creating dataset with {len(filtered)} stocks...")
    writer_worker = _DatasetWriterWorker(snapshot_path)
    copy_mode = "duckdb-direct"
    success_result: DatasetResult | None = None
    manifest_path = _manifest_path_for_snapshot(snapshot_path)

    try:
        # 銘柄データ書き込み
        stock_rows = _convert_stocks(filtered)
        await writer_worker.call("upsert_stocks", stock_rows)
        await writer_worker.call("set_dataset_info", "preset", preset_name)
        await writer_worker.call("set_dataset_info", "created_at", datetime.now(UTC).isoformat())
        await writer_worker.call("set_dataset_info", "stock_count", str(len(filtered)))
        # DuckDB can abort when stocks metadata writes and direct index copies
        # share one destination connection. Reopen before direct-copy stages.
        await writer_worker.close()
        writer_worker = _DatasetWriterWorker(snapshot_path)

        processed = await _copy_stock_data_stage(
            job=job,
            filtered=filtered,
            writer_worker=writer_worker,
            source_duckdb_path=source_duckdb_path,
            copy_mode=copy_mode,
            warnings=warnings,
            progress=progress,
            log_stage_elapsed=log_stage_elapsed,
            batch_size=_BATCH_COPY_SIZE,
        )
        await _copy_event_time_pit_stage(
            job=job,
            processed=processed,
            writer_worker=writer_worker,
            source_duckdb_path=source_duckdb_path,
            normalized_codes=normalized_codes,
            date_from=stock_date_from,
            date_to=stock_date_to,
            progress=progress,
            log_stage_elapsed=log_stage_elapsed,
        )
        await _copy_topix_stage(
            job=job,
            include_topix=preset.include_topix,
            processed=processed,
            writer_worker=writer_worker,
            source_duckdb_path=source_duckdb_path,
            copy_mode=copy_mode,
            progress=progress,
            log_stage_elapsed=log_stage_elapsed,
        )
        await _copy_indices_stage(
            job=job,
            include_sector_indices=preset.include_sector_indices,
            processed=processed,
            writer_worker=writer_worker,
            source_duckdb_path=source_duckdb_path,
            copy_mode=copy_mode,
            progress=progress,
            log_stage_elapsed=log_stage_elapsed,
            index_catalog_codes=get_index_catalog_codes,
        )
        await _copy_statements_stage(
            job=job,
            include_statements=preset.include_statements,
            filtered=filtered,
            processed=processed,
            writer_worker=writer_worker,
            source_duckdb_path=source_duckdb_path,
            copy_mode=copy_mode,
            progress=progress,
            log_stage_elapsed=log_stage_elapsed,
            batch_size=_BATCH_COPY_SIZE,
        )
        await _copy_margin_stage(
            job=job,
            include_margin=preset.include_margin,
            filtered=filtered,
            processed=processed,
            writer_worker=writer_worker,
            source_duckdb_path=source_duckdb_path,
            copy_mode=copy_mode,
            progress=progress,
            log_stage_elapsed=log_stage_elapsed,
            batch_size=_BATCH_COPY_SIZE,
        )

        await writer_worker.call("set_dataset_info", "manifest_path", str(manifest_path))
        await writer_worker.call("set_dataset_info", "manifest_schema_version", "3")
        await writer_worker.call("set_dataset_info", "source_market_schema_version", "4")
        await writer_worker.call(
            "set_dataset_info",
            "source_stock_price_adjustment_mode",
            "local_projection_v2_event_time",
        )
        success_result = DatasetResult(
            success=True,
            totalStocks=len(filtered),
            processedStocks=processed,
            warnings=warnings,
            errors=errors,
            outputPath=str(snapshot_dir),
        )
    except _DatasetBuildCancelled as exc:
        return DatasetResult(
            success=False,
            processedStocks=exc.processed_stocks,
            errors=["Cancelled"],
        )
    finally:
        await writer_worker.close()

    if success_result is None:
        raise RuntimeError("dataset build result was not prepared")
    if job.cancelled.is_set():
        return DatasetResult(
            success=False,
            totalStocks=success_result.totalStocks,
            processedStocks=success_result.processedStocks,
            warnings=success_result.warnings,
            errors=["Cancelled"],
            outputPath=str(snapshot_dir),
        )

    staged_manifest_path = manifest_path.with_name(
        f".{manifest_path.name}.{job.job_id}.tmp"
    )
    try:
        await _write_staged_manifest_off_thread(
            snapshot_path=snapshot_path,
            dataset_name=name,
            preset_name=preset_name,
            staged_manifest_path=staged_manifest_path,
        )

        def publish_manifest() -> None:
            staged_manifest_path.replace(manifest_path)

        completed = await dataset_job_manager.complete_job_with_publication(
            job.job_id,
            success_result,
            publish_manifest,
            final_progress=JobProgress(
                stage="complete",
                current=_TOTAL_STAGES,
                total=_TOTAL_STAGES,
                percentage=100.0,
                message="Dataset build complete!",
            ),
        )
        if not completed:
            staged_manifest_path.unlink(missing_ok=True)
            if job.status != JobStatus.COMPLETED:
                manifest_path.unlink(missing_ok=True)
            return _cancelled_build_result(success_result, snapshot_dir)
    finally:
        staged_manifest_path.unlink(missing_ok=True)
    return success_result


def _row_to_dict(row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        return dict(row)
    if isinstance(row, Mapping):
        return dict(row)
    keys = getattr(row, "keys", None)
    if callable(keys):
        row_keys = keys()
        if isinstance(row_keys, Iterable):
            return {str(key): row[key] for key in row_keys}
    raise TypeError(f"Unsupported row type: {type(row)!r}")


async def _query_market_rows(
    market_reader: MarketDatasetSource,
    sql: str,
    params: tuple[Any, ...] = (),
) -> list[dict[str, Any]]:
    rows = await asyncio.to_thread(market_reader.query, sql, params)
    if asyncio.iscoroutine(rows):
        rows = await rows
    return [_row_to_dict(row) for row in rows]


async def _load_market_stock_master(market_reader: MarketDatasetSource) -> list[dict[str, Any]]:
    rows = await _query_market_rows(
        market_reader,
        """
        SELECT
            code,
            company_name,
            company_name_english,
            market_code,
            market_name,
            sector_17_code,
            sector_17_name,
            sector_33_code,
            sector_33_name,
            scale_category,
            listed_date
        FROM stocks
        ORDER BY code
        """,
    )
    return [
        {
            "Code": expand_stock_code(str(row.get("code", "") or "")),
            "CoName": str(row.get("company_name", "") or ""),
            "CoNameEn": row.get("company_name_english"),
            "Mkt": str(row.get("market_code", "") or ""),
            "MktNm": str(row.get("market_name", "") or ""),
            "S17": str(row.get("sector_17_code", "") or ""),
            "S17Nm": str(row.get("sector_17_name", "") or ""),
            "S33": str(row.get("sector_33_code", "") or ""),
            "S33Nm": str(row.get("sector_33_name", "") or ""),
            "ScaleCat": row.get("scale_category"),
            "Date": str(row.get("listed_date", "") or ""),
        }
        for row in rows
    ]


async def _load_market_stock_date_range(
    market_reader: MarketDatasetSource,
    normalized_codes: Sequence[str],
) -> tuple[str | None, str | None]:
    codes = sorted({normalize_stock_code(code) for code in normalized_codes if code})
    if not codes:
        return None, None
    placeholders = ", ".join("?" for _ in codes)
    rows = await _query_market_rows(
        market_reader,
        f"""
        WITH source_rows AS (
            SELECT
                CASE
                    WHEN length(code) IN (5, 6) AND right(code, 1) = '0'
                    THEN left(code, length(code) - 1)
                    ELSE code
                END AS code,
                date,
                CASE
                    WHEN length(code) IN (5, 6) AND right(code, 1) = '0'
                    THEN 1 ELSE 0
                END AS source_priority,
                open, high, low, close, volume
            FROM stock_data
            WHERE CASE
                    WHEN length(code) IN (5, 6) AND right(code, 1) = '0'
                    THEN left(code, length(code) - 1)
                    ELSE code
                  END IN ({placeholders})
              AND date IS NOT NULL
              AND date <> ''
        ),
        merged_rows AS (
            SELECT
                code,
                date,
                COALESCE(MAX(CASE WHEN source_priority = 0 THEN open END),
                         MAX(CASE WHEN source_priority = 1 THEN open END)) AS open,
                COALESCE(MAX(CASE WHEN source_priority = 0 THEN high END),
                         MAX(CASE WHEN source_priority = 1 THEN high END)) AS high,
                COALESCE(MAX(CASE WHEN source_priority = 0 THEN low END),
                         MAX(CASE WHEN source_priority = 1 THEN low END)) AS low,
                COALESCE(MAX(CASE WHEN source_priority = 0 THEN close END),
                         MAX(CASE WHEN source_priority = 1 THEN close END)) AS close,
                COALESCE(MAX(CASE WHEN source_priority = 0 THEN volume END),
                         MAX(CASE WHEN source_priority = 1 THEN volume END)) AS volume
            FROM source_rows
            GROUP BY code, date
        )
        SELECT MIN(date) AS date_from, MAX(date) AS date_to
        FROM merged_rows
        WHERE open IS NOT NULL
          AND high IS NOT NULL
          AND low IS NOT NULL
          AND close IS NOT NULL
          AND volume IS NOT NULL
        """,
        tuple(codes),
    )
    if not rows:
        return None, None
    row = rows[0]
    date_from = row.get("date_from")
    date_to = row.get("date_to")
    return (
        str(date_from) if date_from is not None else None,
        str(date_to) if date_to is not None else None,
    )


def _filter_stocks(stocks: list[dict[str, Any]], preset: PresetConfig) -> list[dict[str, Any]]:
    """プリセットフィルタを適用"""
    # Market name mapping (JQuants uses MarketCodeName)
    market_name_map = {
        "prime": "プライム",
        "standard": "スタンダード",
        "growth": "グロース",
    }
    market_names = [market_name_map.get(m, m) for m in preset.markets]
    query_market_codes = {code.lower() for code in expand_market_codes(preset.markets)}

    filtered = [
        s
        for s in stocks
        if s.get("MktNm", "") in market_names or str(s.get("Mkt", "") or "").lower() in query_market_codes
    ]

    if preset.scale_categories:
        filtered = [s for s in filtered if s.get("ScaleCat", "") in preset.scale_categories]

    if preset.exclude_scale_categories:
        filtered = [s for s in filtered if s.get("ScaleCat", "") not in preset.exclude_scale_categories]

    if preset.max_stocks:
        filtered = filtered[:preset.max_stocks]

    return filtered


def _convert_stocks(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """JQuants 銘柄マスタ → stock row"""
    return [
        {
            "code": normalize_stock_code(d.get("Code", "")),
            "company_name": d.get("CoName", ""),
            "company_name_english": d.get("CoNameEn"),
            "market_code": d.get("Mkt", ""),
            "market_name": d.get("MktNm", ""),
            "sector_17_code": d.get("S17", ""),
            "sector_17_name": d.get("S17Nm", ""),
            "sector_33_code": d.get("S33", ""),
            "sector_33_name": d.get("S33Nm", ""),
            "scale_category": d.get("ScaleCat"),
            "listed_date": d.get("Date", ""),
            "created_at": datetime.now(UTC).isoformat(),
        }
        for d in data
    ]
