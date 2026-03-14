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
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from loguru import logger

from src.application.services.market_code_alias import expand_market_codes
from src.application.services.dataset_presets import PresetConfig, get_preset
from src.application.services.dataset_resolver import DatasetResolver
from src.application.services.generic_job_manager import GenericJobManager, JobInfo
from src.application.services.index_master_catalog import get_index_catalog_codes
from src.application.services.stock_data_row_builder import build_stock_data_row
from src.entrypoints.http.schemas.job import JobProgress
from src.infrastructure.db.dataset_io.dataset_writer import (
    DatasetWriter,
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
    stock_code_candidates,
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
    warnings: list[str] | None = None
    errors: list[str] | None = None
    outputPath: str = ""


# Module-level manager instance
dataset_job_manager: GenericJobManager[DatasetJobData, JobProgress, DatasetResult] = GenericJobManager()
_TOTAL_STAGES = 7
_BATCH_COPY_SIZE = 200
_WARNING_SAMPLE_SIZE = 5
_STATEMENT_COLUMNS: tuple[str, ...] = (
    "code",
    "disclosed_date",
    "earnings_per_share",
    "profit",
    "equity",
    "type_of_current_period",
    "type_of_document",
    "next_year_forecast_earnings_per_share",
    "bps",
    "sales",
    "operating_profit",
    "ordinary_profit",
    "operating_cash_flow",
    "dividend_fy",
    "forecast_dividend_fy",
    "next_year_forecast_dividend_fy",
    "payout_ratio",
    "forecast_payout_ratio",
    "next_year_forecast_payout_ratio",
    "forecast_eps",
    "investing_cash_flow",
    "financing_cash_flow",
    "cash_and_equivalents",
    "total_assets",
    "shares_outstanding",
    "treasury_shares",
)


class MarketDatasetSource(Protocol):
    def query(self, sql: str, params: tuple[Any, ...] = ()) -> list[Any]:
        """Read-only DuckDB query interface."""
        ...


def _is_missing_value(value: Any) -> bool:
    return value is None or (isinstance(value, str) and value == "")


def _merge_prefer_existing(target: dict[str, Any], incoming: Mapping[str, Any]) -> dict[str, Any]:
    for key, value in incoming.items():
        if _is_missing_value(target.get(key)) and not _is_missing_value(value):
            target[key] = value
    return target


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
        "schemaVersion": 2,
        "generatedAt": datetime.now(UTC).isoformat(),
        "dataset": {
            "name": dataset_name,
            "preset": preset_name,
            "duckdbFile": duckdb_path.name,
            "parquetDir": parquet_dir.name,
        },
        "source": {
            "backend": "duckdb-parquet",
        },
        "counts": counts,
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
) -> JobInfo[DatasetJobData, JobProgress, DatasetResult] | None:
    """データセットビルドジョブを作成して開始"""
    job = await dataset_job_manager.create_job(data)
    if job is None:
        return None

    async def _run() -> None:
        try:
            result = await asyncio.wait_for(
                _build_dataset(job, resolver, market_reader),
                timeout=DATASET_BUILD_TIMEOUT_MINUTES * 60,
            )
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
) -> DatasetResult:
    """データセットをビルドする実際のロジック"""
    name = job.data.name
    preset_name = job.data.preset
    snapshot_path = resolver.get_dataset_path(name)
    snapshot_dir = snapshot_dir_for_path(snapshot_path)
    warnings: list[str] = []
    errors: list[str] = []

    preset = get_preset(preset_name)
    if preset is None:
        return DatasetResult(success=False, errors=[f"Unknown preset: {preset_name}"])

    if job.data.overwrite:
        _delete_dataset_artifacts(resolver, name)

    def progress(stage: str, current: int, total: int, message: str) -> None:
        pct = (current / total * 100) if total > 0 else 0
        dataset_job_manager.update_progress(
            job.job_id,
            JobProgress(stage=stage, current=current, total=total, percentage=pct, message=message),
        )

    # Step 1: 銘柄マスタ取得
    progress("master", 0, _TOTAL_STAGES, "Loading stock master from market.duckdb...")
    if job.cancelled.is_set():
        return DatasetResult(success=False, errors=["Cancelled"])

    stocks_data = await _load_market_stock_master(market_reader)
    filtered = _filter_stocks(stocks_data, preset)

    if not filtered:
        return DatasetResult(success=False, errors=["No stocks matched the preset filters"])

    # Step 2: Writer 作成
    progress("init", 1, _TOTAL_STAGES, f"Creating dataset with {len(filtered)} stocks...")
    writer = DatasetWriter(snapshot_path)
    success_result: DatasetResult | None = None
    manifest_path = _manifest_path_for_snapshot(snapshot_path)

    try:
        # 銘柄データ書き込み
        stock_rows = _convert_stocks(filtered)
        await asyncio.to_thread(writer.upsert_stocks, stock_rows)
        writer.set_dataset_info("preset", preset_name)
        writer.set_dataset_info("created_at", datetime.now(UTC).isoformat())
        writer.set_dataset_info("stock_count", str(len(filtered)))

        # Step 3: 株価データ取得
        progress(
            "stock_data",
            2,
            _TOTAL_STAGES,
            f"Copying stock price data from market.duckdb in batches ({len(filtered)} targets)...",
        )
        processed = 0
        empty_ohlcv_codes: list[str] = []
        incomplete_ohlcv_codes: list[tuple[str, int, int]] = []
        for batch in _chunked(filtered, _BATCH_COPY_SIZE):
            if job.cancelled.is_set():
                return DatasetResult(success=False, processedStocks=processed, errors=["Cancelled"])

            batch_codes = [normalize_stock_code(stock.get("Code", "")) for stock in batch]
            batch_data = await _load_market_stock_data_batch(market_reader, batch_codes)
            if job.cancelled.is_set():
                return DatasetResult(success=False, processedStocks=processed, errors=["Cancelled"])

            rows_to_write: list[dict[str, Any]] = []
            for stock in batch:
                code4 = normalize_stock_code(stock.get("Code", ""))
                data = batch_data.get(code4, [])
                rows: list[dict[str, Any]] = []
                skipped_rows = 0
                for quote in data:
                    if not isinstance(quote, dict):
                        skipped_rows += 1
                        continue
                    created_at = quote.get("created_at")
                    row = build_stock_data_row(
                        quote,
                        normalized_code=code4,
                        created_at=str(created_at) if created_at is not None else None,
                    )
                    if row is None:
                        skipped_rows += 1
                        continue
                    rows.append(row)
                if skipped_rows > 0:
                    incomplete_ohlcv_codes.append((code4, skipped_rows, len(data)))
                if rows:
                    rows_to_write.extend(rows)
                else:
                    empty_ohlcv_codes.append(code4)
            if rows_to_write:
                await asyncio.to_thread(writer.upsert_stock_data, rows_to_write)
            processed += len(batch)
            progress(
                "stock_data",
                2,
                _TOTAL_STAGES,
                f"Stock data from market.duckdb: {processed}/{len(filtered)}",
            )

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

        # Step 4: TOPIX
        if preset.include_topix:
            progress(
                "topix",
                3,
                _TOTAL_STAGES,
                "Copying TOPIX data from market.duckdb...",
            )
            if job.cancelled.is_set():
                return DatasetResult(success=False, processedStocks=processed, errors=["Cancelled"])
            topix_rows = await _load_market_topix_data(market_reader)
            if job.cancelled.is_set():
                return DatasetResult(success=False, processedStocks=processed, errors=["Cancelled"])
            if topix_rows:
                await asyncio.to_thread(writer.upsert_topix_data, topix_rows)

        # Step 5: セクター指数
        if preset.include_sector_indices:
            target_index_codes = sorted(
                code for code in get_index_catalog_codes() if _is_sector_index_code(code)
            )
            progress(
                "indices",
                4,
                _TOTAL_STAGES,
                f"Copying sector index data from market.duckdb in one batch ({len(target_index_codes)} targets)...",
            )
            if job.cancelled.is_set():
                return DatasetResult(success=False, processedStocks=processed, errors=["Cancelled"])
            index_rows = await _load_market_index_data_batch(market_reader, target_index_codes)
            if job.cancelled.is_set():
                return DatasetResult(success=False, processedStocks=processed, errors=["Cancelled"])
            rows_to_write = [
                row
                for code in target_index_codes
                for row in index_rows.get(_normalize_index_code(code), [])
            ]
            if rows_to_write:
                await asyncio.to_thread(writer.upsert_indices_data, rows_to_write)

        # Step 6: 財務諸表
        if preset.include_statements:
            progress(
                "statements",
                5,
                _TOTAL_STAGES,
                f"Copying financial statements from market.duckdb in batches ({len(filtered)} targets)...",
            )
            statements_processed = 0
            for batch in _chunked(filtered, _BATCH_COPY_SIZE):
                if job.cancelled.is_set():
                    return DatasetResult(success=False, processedStocks=processed, errors=["Cancelled"])
                batch_codes = [normalize_stock_code(stock.get("Code", "")) for stock in batch]
                statement_rows = await _load_market_statements_batch(market_reader, batch_codes)
                if job.cancelled.is_set():
                    return DatasetResult(success=False, processedStocks=processed, errors=["Cancelled"])
                rows_to_write = [
                    row
                    for stock in batch
                    for row in statement_rows.get(normalize_stock_code(stock.get("Code", "")), [])
                ]
                if rows_to_write:
                    await asyncio.to_thread(writer.upsert_statements, rows_to_write)
                statements_processed += len(batch)
                progress(
                    "statements",
                    5,
                    _TOTAL_STAGES,
                    f"Financial statements from market.duckdb: {statements_processed}/{len(filtered)}",
                )

        # Step 7: 信用取引
        if preset.include_margin:
            progress(
                "margin",
                6,
                _TOTAL_STAGES,
                f"Copying margin data from market.duckdb in batches ({len(filtered)} targets)...",
            )
            margin_processed = 0
            for batch in _chunked(filtered, _BATCH_COPY_SIZE):
                if job.cancelled.is_set():
                    return DatasetResult(success=False, processedStocks=processed, errors=["Cancelled"])
                batch_codes = [normalize_stock_code(stock.get("Code", "")) for stock in batch]
                margin_rows = await _load_market_margin_data_batch(market_reader, batch_codes)
                if job.cancelled.is_set():
                    return DatasetResult(success=False, processedStocks=processed, errors=["Cancelled"])
                rows_to_write = [
                    row
                    for stock in batch
                    for row in margin_rows.get(normalize_stock_code(stock.get("Code", "")), [])
                ]
                if rows_to_write:
                    await asyncio.to_thread(writer.upsert_margin_data, rows_to_write)
                margin_processed += len(batch)
                progress(
                    "margin",
                    6,
                    _TOTAL_STAGES,
                    f"Margin data from market.duckdb: {margin_processed}/{len(filtered)}",
                )

        writer.set_dataset_info("manifest_path", str(manifest_path))
        writer.set_dataset_info("manifest_schema_version", "2")
        success_result = DatasetResult(
            success=True,
            totalStocks=len(filtered),
            processedStocks=processed,
            warnings=warnings if warnings else None,
            errors=errors if errors else None,
            outputPath=str(snapshot_dir),
        )
    finally:
        writer.close()

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

    _write_dataset_manifest(
        snapshot_path=snapshot_path,
        dataset_name=name,
        preset_name=preset_name,
        manifest_path=manifest_path,
    )
    progress("complete", _TOTAL_STAGES, _TOTAL_STAGES, "Dataset build complete!")
    return success_result


def _sample_text(values: list[str]) -> str:
    sample = values[:_WARNING_SAMPLE_SIZE]
    suffix = ", ..." if len(values) > _WARNING_SAMPLE_SIZE else ""
    return ", ".join(sample) + suffix


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


def _chunked(values: Sequence[dict[str, Any]], size: int) -> Iterator[list[dict[str, Any]]]:
    for index in range(0, len(values), size):
        yield list(values[index : index + size])


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


async def _load_market_topix_data(market_reader: MarketDatasetSource) -> list[dict[str, Any]]:
    rows = await _query_market_rows(
        market_reader,
        """
        SELECT date, open, high, low, close, created_at
        FROM topix_data
        ORDER BY date
        """,
    )
    return [
        {
            "date": str(row.get("date", "") or ""),
            "open": row.get("open"),
            "high": row.get("high"),
            "low": row.get("low"),
            "close": row.get("close"),
            "created_at": row.get("created_at"),
        }
        for row in rows
        if row.get("date") is not None
    ]


async def _load_market_index_data_batch(
    market_reader: MarketDatasetSource,
    codes: Sequence[str],
) -> dict[str, list[dict[str, Any]]]:
    normalized_codes = _normalize_index_codes(codes)
    if not normalized_codes:
        return {}

    placeholders = ", ".join("?" for _ in normalized_codes)
    rows = await _query_market_rows(
        market_reader,
        f"""
        SELECT code, date, open, high, low, close, sector_name, created_at
        FROM indices_data
        WHERE upper(code) IN ({placeholders})
        ORDER BY upper(code), date
        """,
        tuple(normalized_codes),
    )
    grouped: dict[str, list[dict[str, Any]]] = {code: [] for code in normalized_codes}
    for row in rows:
        date = str(row.get("date", "") or "")
        if not date:
            continue
        normalized_code = _normalize_index_code(row.get("code"))
        if not normalized_code:
            continue
        grouped.setdefault(normalized_code, []).append(
            {
                "code": normalized_code,
                "date": date,
                "open": row.get("open"),
                "high": row.get("high"),
                "low": row.get("low"),
                "close": row.get("close"),
                "sector_name": row.get("sector_name"),
                "created_at": row.get("created_at"),
            }
        )
    return {code: rows for code, rows in grouped.items() if rows}


async def _load_market_statements_batch(
    market_reader: MarketDatasetSource,
    normalized_codes: Sequence[str],
) -> dict[str, list[dict[str, Any]]]:
    requested_codes, candidate_map = _build_stock_candidate_map(normalized_codes)
    if not candidate_map:
        return {}

    placeholders = ", ".join("?" for _ in candidate_map)
    rows = await _query_market_rows(
        market_reader,
        f"""
        SELECT {", ".join(_STATEMENT_COLUMNS)}
        FROM statements
        WHERE code IN ({placeholders})
        ORDER BY code, disclosed_date, CASE WHEN length(code) = 4 THEN 0 ELSE 1 END
        """,
        tuple(candidate_map),
    )
    grouped: dict[str, dict[str, dict[str, Any]]] = {code: {} for code in requested_codes}
    for row in rows:
        disclosed_date = str(row.get("disclosed_date", "") or "")
        if not disclosed_date:
            continue
        normalized = _resolve_batch_stock_code(row.get("code"), candidate_map)
        if not normalized:
            continue
        mapped = dict(row)
        mapped["code"] = normalized
        per_code = grouped.setdefault(normalized, {})
        existing = per_code.get(disclosed_date)
        if existing is None:
            per_code[disclosed_date] = mapped
            continue
        _merge_prefer_existing(existing, mapped)
    return {code: list(by_date.values()) for code, by_date in grouped.items() if by_date}


async def _load_market_margin_data_batch(
    market_reader: MarketDatasetSource,
    normalized_codes: Sequence[str],
) -> dict[str, list[dict[str, Any]]]:
    requested_codes, candidate_map = _build_stock_candidate_map(normalized_codes)
    if not candidate_map:
        return {}

    placeholders = ", ".join("?" for _ in candidate_map)
    rows = await _query_market_rows(
        market_reader,
        f"""
        SELECT code, date, long_margin_volume, short_margin_volume
        FROM margin_data
        WHERE code IN ({placeholders})
        ORDER BY code, date, CASE WHEN length(code) = 4 THEN 0 ELSE 1 END
        """,
        tuple(candidate_map),
    )
    grouped: dict[str, dict[str, dict[str, Any]]] = {code: {} for code in requested_codes}
    for row in rows:
        date = str(row.get("date", "") or "")
        if not date:
            continue
        normalized = _resolve_batch_stock_code(row.get("code"), candidate_map)
        if not normalized:
            continue
        candidate = {
            "code": normalized,
            "date": date,
            "long_margin_volume": row.get("long_margin_volume"),
            "short_margin_volume": row.get("short_margin_volume"),
        }
        per_code = grouped.setdefault(normalized, {})
        existing = per_code.get(date)
        if existing is None:
            per_code[date] = candidate
            continue
        _merge_prefer_existing(existing, candidate)
    return {code: list(by_date.values()) for code, by_date in grouped.items() if by_date}


async def _load_market_stock_data_batch(
    market_reader: MarketDatasetSource,
    normalized_codes: Sequence[str],
) -> dict[str, list[dict[str, Any]]]:
    requested_codes, candidate_map = _build_stock_candidate_map(normalized_codes)
    if not candidate_map:
        return {}

    placeholders = ", ".join("?" for _ in candidate_map)
    rows = await _query_market_rows(
        market_reader,
        f"""
        SELECT code, date, open, high, low, close, volume, adjustment_factor, created_at
        FROM stock_data
        WHERE code IN ({placeholders})
        ORDER BY code, date, CASE WHEN length(code) = 4 THEN 0 ELSE 1 END
        """,
        tuple(candidate_map),
    )
    grouped: dict[str, dict[str, dict[str, Any]]] = {code: {} for code in requested_codes}
    for row in rows:
        date = str(row.get("date", "") or "")
        if not date:
            continue
        normalized = _resolve_batch_stock_code(row.get("code"), candidate_map)
        if not normalized:
            continue
        candidate = {
            "Date": date,
            "O": row.get("open"),
            "H": row.get("high"),
            "L": row.get("low"),
            "C": row.get("close"),
            "Vo": row.get("volume"),
            "AdjFactor": row.get("adjustment_factor"),
            "created_at": row.get("created_at"),
        }
        per_code = grouped.setdefault(normalized, {})
        existing = per_code.get(date)
        if existing is None:
            per_code[date] = candidate
            continue
        _merge_prefer_existing(existing, candidate)
    return {code: list(by_date.values()) for code, by_date in grouped.items() if by_date}


def _build_stock_candidate_map(normalized_codes: Sequence[str]) -> tuple[list[str], dict[str, str]]:
    requested_codes: list[str] = []
    candidate_map: dict[str, str] = {}
    seen_codes: set[str] = set()
    for value in normalized_codes:
        normalized = normalize_stock_code(value)
        if not normalized or normalized in seen_codes:
            continue
        seen_codes.add(normalized)
        requested_codes.append(normalized)
        for candidate in stock_code_candidates(normalized):
            candidate_map[str(candidate)] = normalized
    return requested_codes, candidate_map


def _resolve_batch_stock_code(value: Any, candidate_map: Mapping[str, str]) -> str:
    raw_code = str(value).strip() if value is not None else ""
    if not raw_code:
        return ""
    return candidate_map.get(raw_code, normalize_stock_code(raw_code))


def _normalize_index_codes(codes: Sequence[str]) -> list[str]:
    normalized_codes: list[str] = []
    seen: set[str] = set()
    for code in codes:
        normalized = _normalize_index_code(code)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        normalized_codes.append(normalized)
    return normalized_codes


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
    """JQuants 銘柄マスタ → ds_stocks 行"""
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
