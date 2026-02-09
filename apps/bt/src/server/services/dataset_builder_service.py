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

from src.server.clients.jquants_client import JQuantsAsyncClient
from src.server.db.dataset_writer import DatasetWriter
from src.server.db.query_helpers import normalize_stock_code
from src.server.schemas.job import JobProgress
from src.server.services.dataset_presets import PresetConfig, get_preset
from src.server.services.dataset_resolver import DatasetResolver
from src.server.services.generic_job_manager import GenericJobManager, JobInfo


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
    progress("master", 0, 6, "Fetching stock master data...")
    if job.cancelled.is_set():
        return DatasetResult(success=False, errors=["Cancelled"])

    stocks_data = await jquants_client.get_paginated("/equities/master")
    filtered = _filter_stocks(stocks_data, preset)

    if not filtered:
        return DatasetResult(success=False, errors=["No stocks matched the preset filters"])

    # Step 2: Writer 作成
    progress("init", 1, 6, f"Creating dataset with {len(filtered)} stocks...")
    writer = DatasetWriter(db_path)

    try:
        # 銘柄データ書き込み
        stock_rows = _convert_stocks(filtered)
        await asyncio.to_thread(writer.upsert_stocks, stock_rows)
        writer.set_dataset_info("preset", preset_name)
        writer.set_dataset_info("created_at", datetime.now(UTC).isoformat())
        writer.set_dataset_info("stock_count", str(len(filtered)))

        # Step 3: 株価データ取得
        progress("stock_data", 2, 6, "Fetching stock price data...")
        processed = 0
        for i, stock in enumerate(filtered):
            if job.cancelled.is_set():
                return DatasetResult(success=False, processedStocks=processed, errors=["Cancelled"])
            code5 = stock.get("Code", "")
            code4 = normalize_stock_code(code5)
            try:
                data = await jquants_client.get_paginated("/equities/bars/daily", params={"code": code5})
                rows = [
                    {
                        "code": code4,
                        "date": d.get("Date", ""),
                        "open": d.get("O", 0),
                        "high": d.get("H", 0),
                        "low": d.get("L", 0),
                        "close": d.get("C", 0),
                        "volume": d.get("Vo", 0),
                        "adjustment_factor": d.get("AdjFactor"),
                        "created_at": datetime.now(UTC).isoformat(),
                    }
                    for d in data
                ]
                if rows:
                    await asyncio.to_thread(writer.upsert_stock_data, rows)
                processed += 1
                if (i + 1) % 10 == 0:
                    progress("stock_data", 2, 6, f"Stock data: {i + 1}/{len(filtered)}")
            except Exception as e:
                warnings.append(f"Stock {code4}: {e}")

        # Step 4: TOPIX
        if preset.include_topix:
            progress("topix", 3, 6, "Fetching TOPIX data...")
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

        # Step 5: 財務諸表
        if preset.include_statements:
            progress("statements", 4, 6, "Fetching financial statements...")
            for i, stock in enumerate(filtered):
                if job.cancelled.is_set():
                    break
                code5 = stock.get("Code", "")
                code4 = normalize_stock_code(code5)
                try:
                    data = await jquants_client.get_paginated("/fins/summary", params={"code": code5})
                    rows = _convert_statements(data, code4)
                    if rows:
                        await asyncio.to_thread(writer.upsert_statements, rows)
                except Exception as e:
                    warnings.append(f"Statements {code4}: {e}")

        # Step 6: 信用取引
        if preset.include_margin:
            progress("margin", 5, 6, "Fetching margin data...")
            for i, stock in enumerate(filtered):
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

        progress("complete", 6, 6, "Dataset build complete!")
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


def _convert_statements(data: list[dict[str, Any]], code: str) -> list[dict[str, Any]]:
    """JQuants 財務諸表 → statements 行"""
    return [
        {
            "code": code,
            "disclosed_date": d.get("DiscDate", ""),
            "earnings_per_share": d.get("EPS"),
            "profit": d.get("NP"),
            "equity": d.get("Eq"),
            "type_of_current_period": d.get("CurPerType"),
            "type_of_document": d.get("DocType"),
            "next_year_forecast_earnings_per_share": d.get("NxFEPS"),
            "bps": d.get("BPS"),
            "sales": d.get("Sales"),
            "operating_profit": d.get("OP"),
            "ordinary_profit": d.get("OdP"),
            "operating_cash_flow": d.get("CFO"),
            "dividend_fy": d.get("DivAnn"),
            "forecast_eps": d.get("FEPS"),
            "investing_cash_flow": d.get("CFI"),
            "financing_cash_flow": d.get("CFF"),
            "cash_and_equivalents": d.get("CashEq"),
            "total_assets": d.get("TA"),
            "shares_outstanding": d.get("ShOutFY"),
            "treasury_shares": d.get("TrShFY"),
        }
        for d in data
    ]
