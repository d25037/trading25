"""
Signal API Schemas

シグナル計算APIのリクエスト/レスポンスモデル
"""

from datetime import date
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator

from src.entrypoints.http.schemas.analytics_common import (
    DataProvenance,
    ResponseDiagnostics,
)

# Phase 1対象シグナル（OHLCV系のみ）
PHASE1_SIGNAL_TYPES = Literal[
    # oscillator
    "rsi_threshold",
    "rsi_spread",
    # breakout
    "baseline_cross",
    "baseline_deviation",
    "baseline_position",
    "period_extrema_break",
    "period_extrema_position",
    "atr_support_position",
    "atr_support_cross",
    "retracement_position",
    "retracement_cross",
    "crossover",
    "buy_and_hold",
    # volatility
    "volatility_percentile",
    "bollinger_position",
    "bollinger_cross",
    # volume
    "volume_ratio_above",
    "volume_ratio_below",
    "trading_value",
    "trading_value_range",
    "cmf_threshold",
    "chaikin_oscillator",
    "obv_flow_score",
    "accumulation_pressure",
]

PHASE1_SIGNAL_LIST = [
    "rsi_threshold",
    "rsi_spread",
    "baseline_cross",
    "baseline_deviation",
    "baseline_position",
    "period_extrema_break",
    "period_extrema_position",
    "atr_support_position",
    "atr_support_cross",
    "retracement_position",
    "retracement_cross",
    "crossover",
    "buy_and_hold",
    "volatility_percentile",
    "bollinger_position",
    "bollinger_cross",
    "volume_ratio_above",
    "volume_ratio_below",
    "trading_value",
    "trading_value_range",
    "cmf_threshold",
    "chaikin_oscillator",
    "obv_flow_score",
    "accumulation_pressure",
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
        default="market", description="データソース ('market' only)"
    )
    timeframe: Literal["daily", "weekly", "monthly"] = Field(
        default="daily", description="時間枠"
    )
    strategy_name: str | None = Field(
        default=None,
        description="chart検算用の戦略名。指定時は signals を省略する",
    )
    signals: list[SignalSpec] = Field(
        default_factory=list,
        max_length=5,
        description="計算するシグナル一覧（strategy_name 未指定時のみ、最大5個）",
    )
    start_date: date | None = Field(default=None, description="開始日")
    end_date: date | None = Field(default=None, description="終了日")

    @field_validator("source")
    @classmethod
    def validate_source(cls, value: str) -> str:
        normalized = value.strip()
        if normalized != "market":
            raise ValueError("source='market' のみ対応しています")
        return normalized

    @field_validator("strategy_name")
    @classmethod
    def normalize_strategy_name(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        return normalized or None

    @field_validator("signals")
    @classmethod
    def validate_signals_when_present(cls, value: list[SignalSpec]) -> list[SignalSpec]:
        return value

    def model_post_init(self, __context: Any) -> None:
        if self.strategy_name is None and not self.signals:
            raise ValueError("strategy_name または signals のいずれかが必要です")
        if self.strategy_name is not None and self.signals:
            raise ValueError("strategy_name と signals は同時指定できません")


class SignalResult(BaseModel):
    """単一シグナルの計算結果"""

    label: str | None = Field(default=None, description="表示用ラベル")
    mode: Literal["entry", "exit"] | None = Field(default=None, description="シグナルモード")
    trigger_dates: list[str] = Field(
        description="シグナル発火日リスト (YYYY-MM-DD)"
    )
    count: int = Field(description="発火回数")
    error: str | None = Field(
        default=None, description="エラーメッセージ（計算失敗時）"
    )
    diagnostics: ResponseDiagnostics = Field(default_factory=ResponseDiagnostics)


class SignalComputeResponse(BaseModel):
    """シグナル計算レスポンス"""

    stock_code: str = Field(description="銘柄コード")
    timeframe: str = Field(description="時間枠")
    strategy_name: str | None = Field(default=None, description="検算対象戦略")
    signals: dict[str, SignalResult] = Field(
        description="シグナル結果 {signal_type: SignalResult}"
    )
    combined_entry: SignalResult | None = Field(
        default=None,
        description="strategy_name 指定時の合成 entry シグナル",
    )
    combined_exit: SignalResult | None = Field(
        default=None,
        description="strategy_name 指定時の合成 exit シグナル",
    )
    provenance: DataProvenance
    diagnostics: ResponseDiagnostics = Field(default_factory=ResponseDiagnostics)
