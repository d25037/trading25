"""
Signal API Schemas

シグナル計算APIのリクエスト/レスポンスモデル
"""

from datetime import date
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator

# Phase 1対象シグナル（OHLCV系のみ）
PHASE1_SIGNAL_TYPES = Literal[
    # oscillator
    "rsi_threshold",
    "rsi_spread",
    # breakout
    "period_breakout",
    "ma_breakout",
    "atr_support_break",
    "retracement",
    "mean_reversion",
    "crossover",
    "buy_and_hold",
    # volatility
    "bollinger_bands",
    # volume
    "volume",
    "trading_value",
    "trading_value_range",
]

PHASE1_SIGNAL_LIST = [
    "rsi_threshold",
    "rsi_spread",
    "period_breakout",
    "ma_breakout",
    "atr_support_break",
    "retracement",
    "mean_reversion",
    "crossover",
    "buy_and_hold",
    "bollinger_bands",
    "volume",
    "trading_value",
    "trading_value_range",
]


class SignalSpec(BaseModel):
    """シグナル指定

    Example: {"type": "rsi_threshold", "params": {"threshold": 30}, "mode": "entry"}

    Note: type のバリデーションはサービスレイヤーで行う（エラー情報を結果に含めるため）
    """

    type: str = Field(description="シグナルタイプ")
    params: dict[str, Any] = Field(
        default_factory=dict, description="シグナルパラメータ"
    )
    mode: Literal["entry", "exit"] = Field(
        default="entry",
        description="シグナルモード (entry/exit)",
    )

    @field_validator("type")
    @classmethod
    def normalize_signal_type(cls, v: str) -> str:
        """シグナルタイプを正規化（小文字・トリム）"""
        return v.strip().lower()


class SignalComputeRequest(BaseModel):
    """シグナル計算リクエスト"""

    stock_code: str = Field(
        min_length=1, max_length=10, description="銘柄コード"
    )
    source: str = Field(
        default="market", description="データソース ('market' or dataset名)"
    )
    timeframe: Literal["daily", "weekly", "monthly"] = Field(
        default="daily", description="時間枠"
    )
    signals: list[SignalSpec] = Field(
        default_factory=list,
        max_length=5,
        description="計算するシグナル一覧（最大5個）",
    )
    start_date: date | None = Field(default=None, description="開始日")
    end_date: date | None = Field(default=None, description="終了日")


class SignalResult(BaseModel):
    """単一シグナルの計算結果"""

    trigger_dates: list[str] = Field(
        description="シグナル発火日リスト (YYYY-MM-DD)"
    )
    count: int = Field(description="発火回数")
    error: str | None = Field(
        default=None, description="エラーメッセージ（計算失敗時）"
    )


class SignalComputeResponse(BaseModel):
    """シグナル計算レスポンス"""

    stock_code: str = Field(description="銘柄コード")
    timeframe: str = Field(description="時間枠")
    signals: dict[str, SignalResult] = Field(
        description="シグナル結果 {signal_type: SignalResult}"
    )
