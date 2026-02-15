"""
Indicator API Schemas

インジケーター計算APIのリクエスト/レスポンスモデル
"""

from datetime import date
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator


# ===== Individual Indicator Params =====


class SMAParams(BaseModel):
    """SMAパラメータ"""

    period: int = Field(ge=1, le=500, description="移動平均期間")


class EMAParams(BaseModel):
    """EMAパラメータ"""

    period: int = Field(ge=1, le=500, description="移動平均期間")


class RSIParams(BaseModel):
    """RSIパラメータ"""

    period: int = Field(default=14, ge=1, le=500, description="RSI期間")


class MACDParams(BaseModel):
    """MACDパラメータ"""

    fast_period: int = Field(default=12, ge=1, le=500, description="短期EMA期間")
    slow_period: int = Field(default=26, ge=1, le=500, description="長期EMA期間")
    signal_period: int = Field(default=9, ge=1, le=500, description="シグナル期間")


class PPOParams(BaseModel):
    """PPOパラメータ"""

    fast_period: int = Field(default=12, ge=1, le=500, description="短期EMA期間")
    slow_period: int = Field(default=26, ge=1, le=500, description="長期EMA期間")
    signal_period: int = Field(default=9, ge=1, le=500, description="シグナル期間")


class BollingerParams(BaseModel):
    """ボリンジャーバンドパラメータ"""

    period: int = Field(default=20, ge=1, le=500, description="期間")
    std_dev: float = Field(default=2.0, gt=0, le=5.0, description="標準偏差倍率")


class ATRParams(BaseModel):
    """ATRパラメータ"""

    period: int = Field(default=14, ge=1, le=500, description="ATR期間")


class ATRSupportParams(BaseModel):
    """ATRサポートラインパラメータ"""

    lookback_period: int = Field(default=20, ge=1, le=500, description="計算期間")
    atr_multiplier: float = Field(default=2.0, gt=0, le=10.0, description="ATR倍率")


class NBarSupportParams(BaseModel):
    """N日安値サポートパラメータ"""

    period: int = Field(default=20, ge=1, le=500, description="N日期間")


class VolumeComparisonParams(BaseModel):
    """出来高比較パラメータ"""

    short_period: int = Field(default=20, ge=1, le=500, description="短期MA期間")
    long_period: int = Field(default=100, ge=1, le=500, description="長期MA期間")
    lower_multiplier: float = Field(default=1.0, gt=0, description="長期MA下限倍率")
    higher_multiplier: float = Field(default=1.5, gt=0, description="長期MA上限倍率")
    ma_type: Literal["sma", "ema"] = Field(default="sma", description="MA種別")


class TradingValueMAParams(BaseModel):
    """売買代金MAパラメータ"""

    period: int = Field(default=20, ge=1, le=500, description="MA期間")


class RiskAdjustedReturnParams(BaseModel):
    """リスク調整リターンパラメータ"""

    lookback_period: int = Field(default=60, ge=1, le=500, description="計算期間")
    ratio_type: Literal["sharpe", "sortino"] = Field(
        default="sortino",
        description="レシオ種別",
    )


# ===== Indicator Spec (Discriminated Union) =====

INDICATOR_PARAMS_MAP: dict[str, type[BaseModel]] = {
    "sma": SMAParams,
    "ema": EMAParams,
    "rsi": RSIParams,
    "macd": MACDParams,
    "ppo": PPOParams,
    "bollinger": BollingerParams,
    "atr": ATRParams,
    "atr_support": ATRSupportParams,
    "nbar_support": NBarSupportParams,
    "volume_comparison": VolumeComparisonParams,
    "trading_value_ma": TradingValueMAParams,
    "risk_adjusted_return": RiskAdjustedReturnParams,
}

INDICATOR_TYPES = Literal[
    "sma",
    "ema",
    "rsi",
    "macd",
    "ppo",
    "bollinger",
    "atr",
    "atr_support",
    "nbar_support",
    "volume_comparison",
    "trading_value_ma",
    "risk_adjusted_return",
]


class IndicatorSpec(BaseModel):
    """インジケーター指定

    Example: {"type": "sma", "params": {"period": 20}}
    """

    type: INDICATOR_TYPES = Field(description="インジケータータイプ")
    params: dict[str, Any] = Field(default_factory=dict, description="パラメータ")

    @model_validator(mode="after")
    def validate_params(self) -> "IndicatorSpec":
        """パラメータをインジケータータイプに応じてバリデーション"""
        params_cls = INDICATOR_PARAMS_MAP.get(self.type)
        if params_cls:
            # Pydanticモデルでバリデーション（エラーはそのまま伝播）
            params_cls.model_validate(self.params)
        return self


# ===== Relative OHLC Options =====


class RelativeOHLCOptions(BaseModel):
    """相対OHLCオプション"""

    handle_zero_division: Literal["skip", "zero", "null"] = Field(
        default="skip",
        description="ゼロ除算の処理方法 (skip: 除外, zero: 0を返す, null: nullを返す)",
    )


# ===== Request / Response =====


class IndicatorComputeRequest(BaseModel):
    """インジケーター計算リクエスト"""

    stock_code: str = Field(
        min_length=1, max_length=10, description="銘柄コード"
    )
    source: Literal["market", "dataset"] = Field(
        default="dataset", description="データソース"
    )
    timeframe: Literal["daily", "weekly", "monthly"] = Field(
        default="daily", description="時間枠"
    )
    indicators: list[IndicatorSpec] = Field(
        default_factory=list, description="計算するインジケーター一覧 (output='ohlcv'時は空でも可)"
    )
    start_date: date | None = Field(default=None, description="開始日")
    end_date: date | None = Field(default=None, description="終了日")
    nan_handling: Literal["include", "omit"] = Field(
        default="include", description="NaN処理方式"
    )
    benchmark_code: str | None = Field(
        default=None, description="ベンチマークコード (e.g., 'topix')"
    )
    relative_options: RelativeOHLCOptions | None = Field(
        default=None, description="相対OHLCオプション"
    )
    output: Literal["indicators", "ohlcv"] = Field(
        default="indicators",
        description="出力形式: indicators=インジケーター計算結果, ohlcv=変換後OHLCVのみ",
    )

    @model_validator(mode="after")
    def validate_indicators_for_output(self) -> "IndicatorComputeRequest":
        """output=indicatorsの場合はindicatorsが必須"""
        if self.output == "indicators" and len(self.indicators) == 0:
            raise ValueError("output='indicators'の場合、indicatorsを1つ以上指定してください")
        return self

    @field_validator("benchmark_code")
    @classmethod
    def validate_benchmark_code(cls, v: str | None) -> str | None:
        if v is not None:
            v = v.lower().strip()
            if v not in ("topix",):
                raise ValueError(f"未対応のベンチマーク: {v} (対応: topix)")
        return v


class IndicatorComputeResponse(BaseModel):
    """インジケーター計算レスポンス"""

    stock_code: str = Field(description="銘柄コード")
    timeframe: str = Field(description="時間枠")
    meta: dict[str, Any] = Field(
        default_factory=dict, description="メタ情報（データ件数等）"
    )
    indicators: dict[str, list[dict[str, Any]]] = Field(
        default_factory=dict,
        description="インジケーター結果 {key: [{date, value, ...}]} (output='ohlcv'時は空)",
    )
    ohlcv: list[dict[str, Any]] | None = Field(
        default=None,
        description="OHLCVデータ (output='ohlcv'時のみ)",
    )


# ===== Margin Indicator =====


class MarginIndicatorRequest(BaseModel):
    """信用指標リクエスト"""

    stock_code: str = Field(
        min_length=1, max_length=10, description="銘柄コード"
    )
    source: str = Field(
        default="topix500",
        description="データソース（データセット名: topix500, topix100等）",
    )
    indicators: list[
        Literal[
            "margin_long_pressure",
            "margin_flow_pressure",
            "margin_turnover_days",
            "margin_volume_ratio",
        ]
    ] = Field(min_length=1, max_length=4, description="信用指標")
    average_period: int = Field(
        default=15, ge=1, le=200, description="出来高平均期間"
    )
    start_date: date | None = Field(default=None, description="開始日")
    end_date: date | None = Field(default=None, description="終了日")


class MarginIndicatorResponse(BaseModel):
    """信用指標レスポンス"""

    stock_code: str = Field(description="銘柄コード")
    indicators: dict[str, list[dict[str, Any]]] = Field(
        description="信用指標結果"
    )


# ===== OHLCV Resample =====


class OHLCVRecord(BaseModel):
    """OHLCVレコード"""

    date: str = Field(description="日付 (YYYY-MM-DD)")
    open: float = Field(description="始値")
    high: float = Field(description="高値")
    low: float = Field(description="安値")
    close: float = Field(description="終値")
    volume: float = Field(description="出来高")


class OHLCVResampleRequest(BaseModel):
    """OHLCVリサンプルリクエスト

    Timeframe変換およびRelative OHLC変換を実行。
    仕様: docs/spec-timeframe-resample.md
    """

    stock_code: str = Field(
        min_length=1, max_length=10, description="銘柄コード"
    )
    source: Literal["market", "dataset"] = Field(
        default="market", description="データソース"
    )
    timeframe: Literal["daily", "weekly", "monthly"] = Field(
        default="weekly", description="出力時間枠"
    )
    start_date: date | None = Field(default=None, description="開始日")
    end_date: date | None = Field(default=None, description="終了日")
    benchmark_code: str | None = Field(
        default=None, description="ベンチマークコード (e.g., 'topix') - 指定時は相対OHLCを計算"
    )
    relative_options: RelativeOHLCOptions | None = Field(
        default=None, description="相対OHLCオプション"
    )

    @field_validator("benchmark_code")
    @classmethod
    def validate_benchmark_code(cls, v: str | None) -> str | None:
        if v is not None:
            v = v.lower().strip()
            if v not in ("topix",):
                raise ValueError(f"未対応のベンチマーク: {v} (対応: topix)")
        return v


class OHLCVResampleResponse(BaseModel):
    """OHLCVリサンプルレスポンス"""

    stock_code: str = Field(description="銘柄コード")
    timeframe: str = Field(description="時間枠")
    benchmark_code: str | None = Field(
        default=None, description="使用したベンチマーク（相対モード時）"
    )
    meta: dict[str, Any] = Field(
        default_factory=dict,
        description="メタ情報 (source_bars: 元データ件数, resampled_bars: 変換後件数)",
    )
    data: list[OHLCVRecord] = Field(description="OHLCVデータ")
