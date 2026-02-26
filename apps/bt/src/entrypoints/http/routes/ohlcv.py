"""
OHLCV API Endpoints

OHLCVデータのTimeframe変換およびRelative OHLC変換API。
apps/ts/からの呼び出しを想定し、apps/bt/をSingle Source of Truthとして機能させる。

仕様: docs/spec-timeframe-resample.md
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from loguru import logger

from src.infrastructure.external_api.exceptions import APIError, APINotFoundError
from src.infrastructure.db.market.market_reader import MarketDbReader

from src.entrypoints.http.schemas.indicators import (
    OHLCVResampleRequest,
    OHLCVResampleResponse,
)
from src.application.services.indicator_service import IndicatorService
from src.domains.strategy.indicators.indicator_registry import (
    _clean_value,
    _format_date,
)
from src.domains.strategy.indicators.relative_ohlcv import calculate_relative_ohlcv

router = APIRouter(tags=["OHLCV"])

# ThreadPoolExecutor（モジュールレベルで1つ生成）
_executor = ThreadPoolExecutor(max_workers=3)


def _get_executor() -> ThreadPoolExecutor:
    """shutdown 済みの場合は再作成して返す"""
    global _executor
    if getattr(_executor, "_shutdown", False):
        _executor = ThreadPoolExecutor(max_workers=3)
    return _executor


TIMEOUT_SECONDS = 10


def _resample_ohlcv(
    stock_code: str,
    source: str,
    timeframe: str,
    start_date: Any,
    end_date: Any,
    benchmark_code: str | None,
    relative_options: dict[str, Any] | None,
    market_reader: MarketDbReader | None,
) -> dict[str, Any]:
    """OHLCVリサンプル処理（同期処理）"""
    service = IndicatorService(market_reader=market_reader)
    try:
        # OHLCVデータをロード
        ohlcv = service.load_ohlcv(stock_code, source, start_date, end_date)
        source_bars = len(ohlcv)

        # 相対モード: ベンチマークでOHLCVを変換
        if benchmark_code:
            benchmark_df = service.load_benchmark_ohlcv(
                benchmark_code, start_date, end_date
            )
            opts = relative_options or {}
            handle_zero = opts.get("handle_zero_division", "skip")
            ohlcv = calculate_relative_ohlcv(ohlcv, benchmark_df, handle_zero)

        # Timeframe変換
        ohlcv = service.resample_timeframe(ohlcv, timeframe)
        resampled_bars = len(ohlcv)

        # レコード形式に変換（NaN/Infをクリーニング）
        records: list[dict[str, Any]] = []
        for idx, row in ohlcv.iterrows():
            records.append({
                "date": _format_date(idx),
                "open": _clean_value(row["Open"]),
                "high": _clean_value(row["High"]),
                "low": _clean_value(row["Low"]),
                "close": _clean_value(row["Close"]),
                "volume": _clean_value(row["Volume"]),
            })

        return {
            "stock_code": stock_code,
            "timeframe": timeframe,
            "benchmark_code": benchmark_code,
            "meta": {
                "source_bars": source_bars,
                "resampled_bars": resampled_bars,
            },
            "data": records,
        }
    finally:
        service.close()


@router.post(
    "/api/ohlcv/resample",
    response_model=OHLCVResampleResponse,
    summary="OHLCVデータのTimeframe変換",
    description="""
OHLCVデータを指定したTimeframe（週足/月足）に変換します。

**機能**:
- 日足→週足/月足のリサンプル
- 相対OHLC（ベンチマーク比較）変換（オプション）

**仕様**: docs/spec-timeframe-resample.md を参照

**計算順序**: Relative OHLC計算 → Timeframe Resample

**集約ルール**:
- Open: first（期間最初の始値）
- High: max（期間中の最高値）
- Low: min（期間中の最安値）
- Close: last（期間最後の終値）
- Volume: sum（期間の出来高合計）
""",
)
async def resample_ohlcv(
    request: OHLCVResampleRequest,
    http_request: Request,
) -> OHLCVResampleResponse:
    """OHLCVデータをリサンプル"""
    logger.info(
        f"OHLCVリサンプル: {request.stock_code} "
        f"({request.source} → {request.timeframe})"
        + (f", benchmark={request.benchmark_code}" if request.benchmark_code else "")
    )

    relative_opts = (
        request.relative_options.model_dump() if request.relative_options else None
    )
    market_reader = getattr(http_request.app.state, "market_reader", None)

    loop = asyncio.get_running_loop()
    try:
        result = await asyncio.wait_for(
            loop.run_in_executor(
                _get_executor(),
                _resample_ohlcv,
                request.stock_code,
                request.source,
                request.timeframe,
                request.start_date,
                request.end_date,
                request.benchmark_code,
                relative_opts,
                market_reader,
            ),
            timeout=TIMEOUT_SECONDS,
        )
    except TimeoutError:
        raise HTTPException(
            status_code=504,
            detail=f"OHLCVリサンプルがタイムアウトしました ({TIMEOUT_SECONDS}秒)",
        )
    except APINotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except APIError as e:
        logger.error(f"OHLCVリサンプル APIエラー: {e}")
        raise HTTPException(
            status_code=e.status_code or 500, detail=str(e)
        )
    except ValueError as e:
        status = 404 if "取得できません" in str(e) else 422
        raise HTTPException(status_code=status, detail=str(e))
    except Exception as e:
        logger.exception(f"OHLCVリサンプルエラー: {e}")
        raise HTTPException(status_code=500, detail=f"リサンプルエラー: {e}")

    return OHLCVResampleResponse(**result)
