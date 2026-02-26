"""
Dataset Builder Service

データセット作成/再開のオーケストレーション。
GenericJobManager を使用してバックグラウンドビルドを管理する。
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from loguru import logger

from src.application.services.dataset_presets import PresetConfig, get_preset
from src.application.services.dataset_resolver import DatasetResolver
from src.application.services.fins_summary_mapper import convert_fins_summary_rows
from src.application.services.generic_job_manager import GenericJobManager, JobInfo
from src.application.services.index_master_catalog import get_index_catalog_codes
from src.application.services.stock_data_row_builder import build_stock_data_row
from src.entrypoints.http.schemas.job import JobProgress
from src.infrastructure.external_api.clients.jquants_client import JQuantsAsyncClient
from src.infrastructure.db.dataset_io.dataset_writer import DatasetWriter
from src.infrastructure.db.market.query_helpers import normalize_stock_code


@dataclass
class DatasetJobData:
    name: str
    preset: str
    overwrite: bool = False
    resume: bool = False


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
_WARNING_SAMPLE_SIZE = 5


async def start_dataset_build(
    data: DatasetJobData,
    resolver: DatasetResolver,
    jquants_client: JQuantsAsyncClient,
) -> JobInfo[DatasetJobData, JobProgress, DatasetResult] | None:
    """データセットビルドジョブを作成して開始"""
    job = await dataset_job_manager.create_job(data)
    if job is None:
        return None

    async def _run() -> None:
        try:
            result = await asyncio.wait_for(
                _build_dataset(job, resolver, jquants_client),
                timeout=35 * 60,
            )
            if dataset_job_manager.is_cancelled(job.job_id):
                return
            dataset_job_manager.complete_job(job.job_id, result)
        except asyncio.TimeoutError:
            dataset_job_manager.fail_job(job.job_id, "Dataset build timed out after 35 minutes")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.exception(f"Dataset build {job.job_id} failed: {e}")
            dataset_job_manager.fail_job(job.job_id, str(e))

    task = asyncio.create_task(_run())
    job.task = task
    return job


async def _build_dataset(
    job: JobInfo[DatasetJobData, JobProgress, DatasetResult],
    resolver: DatasetResolver,
    jquants_client: JQuantsAsyncClient,
) -> DatasetResult:
    """データセットをビルドする実際のロジック"""
    name = job.data.name
    preset_name = job.data.preset
    db_path = resolver.get_db_path(name)
    warnings: list[str] = []
    errors: list[str] = []

    preset = get_preset(preset_name)
    if preset is None:
        return DatasetResult(success=False, errors=[f"Unknown preset: {preset_name}"])

    def progress(stage: str, current: int, total: int, message: str) -> None:
        pct = (current / total * 100) if total > 0 else 0
        dataset_job_manager.update_progress(
            job.job_id,
            JobProgress(stage=stage, current=current, total=total, percentage=pct, message=message),
        )

    # Step 1: 銘柄マスタ取得
    progress("master", 0, _TOTAL_STAGES, "Fetching stock master data...")
    if job.cancelled.is_set():
        return DatasetResult(success=False, errors=["Cancelled"])

    stocks_data = await jquants_client.get_paginated("/equities/master")
    filtered = _filter_stocks(stocks_data, preset)

    if not filtered:
        return DatasetResult(success=False, errors=["No stocks matched the preset filters"])

    # Step 2: Writer 作成
    progress("init", 1, _TOTAL_STAGES, f"Creating dataset with {len(filtered)} stocks...")
    writer = DatasetWriter(db_path)

    try:
        # 銘柄データ書き込み
        stock_rows = _convert_stocks(filtered)
        await asyncio.to_thread(writer.upsert_stocks, stock_rows)
        writer.set_dataset_info("preset", preset_name)
        writer.set_dataset_info("created_at", datetime.now(UTC).isoformat())
        writer.set_dataset_info("stock_count", str(len(filtered)))

        # Step 3: 株価データ取得
        progress("stock_data", 2, _TOTAL_STAGES, "Fetching stock price data...")
        processed = 0
        empty_ohlcv_codes: list[str] = []
        incomplete_ohlcv_codes: list[tuple[str, int, int]] = []
        for i, stock in enumerate(filtered):
            if job.cancelled.is_set():
                return DatasetResult(success=False, processedStocks=processed, errors=["Cancelled"])
            code5 = stock.get("Code", "")
            code4 = normalize_stock_code(code5)
            try:
                data = await jquants_client.get_paginated("/equities/bars/daily", params={"code": code5})
                rows: list[dict[str, Any]] = []
                skipped_rows = 0
                created_at = datetime.now(UTC).isoformat()
                for quote in data:
                    if not isinstance(quote, dict):
                        skipped_rows += 1
                        continue
                    row = build_stock_data_row(
                        quote,
                        normalized_code=code4,
                        created_at=created_at,
                    )
                    if row is None:
                        skipped_rows += 1
                        continue
                    rows.append(row)
                if skipped_rows > 0:
                    incomplete_ohlcv_codes.append((code4, skipped_rows, len(data)))
                if rows:
                    await asyncio.to_thread(writer.upsert_stock_data, rows)
                else:
                    empty_ohlcv_codes.append(code4)
                processed += 1
                if (i + 1) % 10 == 0:
                    progress("stock_data", 2, _TOTAL_STAGES, f"Stock data: {i + 1}/{len(filtered)}")
            except Exception as e:
                warnings.append(f"Stock {code4}: {e}")

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
            progress("topix", 3, _TOTAL_STAGES, "Fetching TOPIX data...")
            if not job.cancelled.is_set():
                try:
                    topix = await jquants_client.get_paginated("/indices/bars/daily/topix")
                    topix_rows = [
                        {
                            "date": d.get("Date", ""),
                            "open": d.get("O", 0),
                            "high": d.get("H", 0),
                            "low": d.get("L", 0),
                            "close": d.get("C", 0),
                            "created_at": datetime.now(UTC).isoformat(),
                        }
                        for d in topix
                    ]
                    if topix_rows:
                        await asyncio.to_thread(writer.upsert_topix_data, topix_rows)
                except Exception as e:
                    warnings.append(f"TOPIX: {e}")

        # Step 5: セクター指数
        if preset.include_sector_indices:
            progress("indices", 4, _TOTAL_STAGES, "Fetching sector index data...")
            target_index_codes = sorted(
                code for code in get_index_catalog_codes() if _is_sector_index_code(code)
            )
            for code in target_index_codes:
                if job.cancelled.is_set():
                    break
                try:
                    data = await jquants_client.get_paginated("/indices/bars/daily", params={"code": code})
                    rows = _convert_indices_rows(data, fallback_code=code)
                    if rows:
                        await asyncio.to_thread(writer.upsert_indices_data, rows)
                except Exception as e:
                    warnings.append(f"Indices {code}: {e}")

        # Step 6: 財務諸表
        if preset.include_statements:
            progress("statements", 5, _TOTAL_STAGES, "Fetching financial statements...")
            for stock in filtered:
                if job.cancelled.is_set():
                    break
                code5 = stock.get("Code", "")
                code4 = normalize_stock_code(code5)
                try:
                    data = await jquants_client.get_paginated("/fins/summary", params={"code": code5})
                    rows = convert_fins_summary_rows(data, default_code=code4)
                    if rows:
                        await asyncio.to_thread(writer.upsert_statements, rows)
                except Exception as e:
                    warnings.append(f"Statements {code4}: {e}")

        # Step 7: 信用取引
        if preset.include_margin:
            progress("margin", 6, _TOTAL_STAGES, "Fetching margin data...")
            for stock in filtered:
                if job.cancelled.is_set():
                    break
                code5 = stock.get("Code", "")
                code4 = normalize_stock_code(code5)
                try:
                    data = await jquants_client.get_paginated("/markets/margin-interest", params={"code": code5})
                    rows = [
                        {
                            "code": code4,
                            "date": d.get("Date", ""),
                            "long_margin_volume": d.get("LongVol"),
                            "short_margin_volume": d.get("ShrtVol"),
                        }
                        for d in data
                    ]
                    if rows:
                        await asyncio.to_thread(writer.upsert_margin_data, rows)
                except Exception as e:
                    warnings.append(f"Margin {code4}: {e}")

        progress("complete", _TOTAL_STAGES, _TOTAL_STAGES, "Dataset build complete!")
        return DatasetResult(
            success=True,
            totalStocks=len(filtered),
            processedStocks=processed,
            warnings=warnings if warnings else None,
            errors=errors if errors else None,
            outputPath=db_path,
        )
    finally:
        writer.close()


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


def _extract_index_code(row: dict[str, Any]) -> str:
    return _normalize_index_code(
        row.get("Code")
        or row.get("code")
        or row.get("indexCode")
        or row.get("index_code")
    )


def _convert_indices_rows(data: list[dict[str, Any]], *, fallback_code: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    created_at = datetime.now(UTC).isoformat()
    normalized_fallback = _normalize_index_code(fallback_code)

    for row in data:
        if not isinstance(row, dict):
            continue
        code = _extract_index_code(row) or normalized_fallback
        date = row.get("Date") or row.get("date")
        if not code or not date:
            continue
        rows.append(
            {
                "code": code,
                "date": str(date),
                "open": row.get("O", row.get("open")),
                "high": row.get("H", row.get("high")),
                "low": row.get("L", row.get("low")),
                "close": row.get("C", row.get("close")),
                "sector_name": row.get("SectorName", row.get("sector_name")),
                "created_at": created_at,
            }
        )

    return rows


def _filter_stocks(stocks: list[dict[str, Any]], preset: PresetConfig) -> list[dict[str, Any]]:
    """プリセットフィルタを適用"""
    # Market name mapping (JQuants uses MarketCodeName)
    market_name_map = {
        "prime": "プライム",
        "standard": "スタンダード",
        "growth": "グロース",
    }
    market_names = [market_name_map.get(m, m) for m in preset.markets]

    filtered = [s for s in stocks if s.get("MktNm", "") in market_names]

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
