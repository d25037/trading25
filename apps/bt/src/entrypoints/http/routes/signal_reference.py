"""
シグナルリファレンス ルート

GET  /api/signals/reference — シグナルリファレンスデータ返却
GET  /api/signals/schema — SignalParams JSON Schema返却
POST /api/signals/compute — chart/screening 検算用シグナル計算
"""

from typing import Any

from fastapi import APIRouter, HTTPException, Request
from loguru import logger

from src.application.services.market_data_errors import MarketDataError
from src.entrypoints.http.error_utils import (
    classify_market_data_http_exception,
    market_data_http_exception,
)
from src.shared.models.signals import SignalParams
from src.entrypoints.http.schemas.signal_reference import SignalReferenceResponse
from src.entrypoints.http.schemas.signals import SignalComputeRequest, SignalComputeResponse
from src.application.services.signal_reference_service import build_signal_reference
from src.application.services.signal_service import SignalService

router = APIRouter(tags=["Signals"])


def _get_signal_service(request: Request) -> SignalService:
    market_reader = getattr(request.app.state, "market_reader", None)
    return SignalService(market_reader=market_reader)


@router.get("/api/signals/reference", response_model=SignalReferenceResponse)
async def get_signal_reference() -> SignalReferenceResponse:
    """シグナルリファレンスデータを取得"""
    try:
        data = build_signal_reference()
        return SignalReferenceResponse(**data)
    except Exception as e:
        logger.exception("シグナルリファレンス取得エラー")
        raise HTTPException(
            status_code=500, detail="シグナルリファレンス取得に失敗しました"
        ) from e


@router.get("/api/signals/schema")
async def get_signal_schema() -> dict[str, Any]:
    """SignalParams の JSON Schema を返却

    Pydanticモデル変更が自動的にスキーマに反映される。
    """
    try:
        return SignalParams.model_json_schema()
    except Exception as e:
        logger.exception("シグナルスキーマ取得エラー")
        raise HTTPException(
            status_code=500, detail="シグナルスキーマ取得に失敗しました"
        ) from e


@router.post("/api/signals/compute", response_model=SignalComputeResponse)
async def compute_signals(
    request: Request,
    payload: SignalComputeRequest,
) -> SignalComputeResponse:
    """シグナル計算を実行し、発火日を返却
    """
    try:
        signals_dicts = [
            {"type": s.type, "params": s.params, "mode": s.mode}
            for s in payload.signals
        ]
        service = _get_signal_service(request)
        try:
            result = service.compute_signals(
                stock_code=payload.stock_code,
                source=payload.source,
                timeframe=payload.timeframe,
                signals=signals_dicts,
                start_date=payload.start_date,
                end_date=payload.end_date,
                strategy_name=payload.strategy_name,
            )
            return SignalComputeResponse(**result)
        finally:
            service.close()
    except MarketDataError as e:
        raise market_data_http_exception(e) from e
    except ValueError as e:
        classified = classify_market_data_http_exception(
            stock_code=payload.stock_code,
            source=payload.source,
            raw_message=str(e),
            market_reader=getattr(request.app.state, "market_reader", None),
        )
        if classified is not None:
            raise classified from e
        logger.warning(f"シグナル計算バリデーションエラー: {e}")
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.exception("シグナル計算エラー")
        raise HTTPException(status_code=500, detail="シグナル計算に失敗しました") from e
