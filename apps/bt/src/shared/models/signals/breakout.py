"""
ブレイクアウト・クロスオーバー・基準線シグナルパラメータ
"""

from typing import Literal

from pydantic import Field, ValidationInfo, field_validator

from .base import BaseSignalParams, _validate_condition_above_below, _validate_period_order


class CrossoverSignalParams(BaseSignalParams):
    """クロスオーバーシグナルパラメータ（汎用・シンプル設計）"""
    type: str = Field(default="sma", description="指標タイプ（sma/rsi/macd/ema）")
    direction: str = Field(default="golden", description="クロス方向（golden/dead）")
    fast_period: int = Field(default=10, gt=0, le=500, description="高速線期間")
    slow_period: int = Field(default=30, gt=0, le=1000, description="低速線期間")
    signal_period: int = Field(
        default=9, gt=0, le=100, description="MACDシグナル期間（type=macd時のみ使用）"
    )
    lookback_days: int = Field(
        default=1,
        gt=0,
        le=100,
        description="クロス検出期間（1=その日のみ、>1=直近X日以内）",
    )

    @field_validator("slow_period")
    @classmethod
    def validate_period_order(cls, v: int, info: ValidationInfo) -> int:
        return _validate_period_order(
            v, info, "fast_period", "slow_periodはfast_periodより大きい必要があります"
        )


class PeriodExtremaBreakSignalParams(BaseSignalParams):
    """期間高値/安値ブレイクイベントシグナルパラメータ"""
    direction: Literal["high", "low"] = Field(
        default="high", description="方向（high=最高値比較、low=最安値比較）"
    )
    period: int = Field(
        default=20, gt=0, le=1000, description="比較対象期間（N日最高値/最安値）"
    )
    lookback_days: int = Field(
        default=1,
        gt=0,
        le=500,
        description="イベント検出期間（1=当日のみ、N=直近N日以内）",
    )


class PeriodExtremaPositionSignalParams(BaseSignalParams):
    """期間高値/安値に対する状態シグナルパラメータ"""
    direction: Literal["high", "low"] = Field(
        default="high", description="方向（high=最高値比較、low=最安値比較）"
    )
    state: Literal["at_extrema", "away_from_extrema"] = Field(
        default="at_extrema",
        description="状態（at_extrema=期間高値/安値圏、away_from_extrema=期間高値/安値圏外）",
    )
    period: int = Field(
        default=20, gt=0, le=1000, description="比較対象期間（N日最高値/最安値）"
    )
    lookback_days: int = Field(
        default=1,
        gt=0,
        le=500,
        description="状態判定期間（1=当日のみ、N=直近N日以内）",
    )


class BaselineCrossSignalParams(BaseSignalParams):
    """基準線クロスシグナルパラメータ"""
    baseline_type: str = Field(default="sma", description="基準線タイプ（sma/ema/vwema）")
    baseline_period: int = Field(default=200, gt=0, le=1000, description="基準線期間")
    direction: str = Field(
        default="above", description="方向（above=上抜け、below=下抜け）"
    )
    lookback_days: int = Field(
        default=1, gt=0, le=100, description="検出期間（1=その日のみ、N=直近N日以内）"
    )
    price_column: str = Field(
        default="close", description="判定価格カラム（close/high/low）"
    )


# ===== レガシー: 段階的削除予定 =====
class BreakoutSignalParams(BaseSignalParams):
    """【レガシー】ブレイクアウトシグナルパラメータ（統合版に移行予定）"""
    price_column: str = Field(
        default="high", description="価格カラム（high/low/close）"
    )
    threshold_type: str = Field(
        default="rolling_max",
        description="閾値タイプ（rolling_max/rolling_min/sma/ema）",
    )
    period: int = Field(default=20, gt=0, le=1000, description="期間")
    direction: str = Field(
        default="upward", description="ブレイク方向（upward/downward）"
    )


class BaselineDeviationSignalParams(BaseSignalParams):
    """基準線乖離シグナルパラメータ"""
    baseline_type: str = Field(default="sma", description="基準線タイプ（sma/ema/vwema）")
    baseline_period: int = Field(default=25, gt=0, le=500, description="基準線期間")
    deviation_threshold: float = Field(
        default=0.2,
        ge=0,
        le=1.0,
        description="乖離率閾値（0.2 = 20%乖離、0.0で無効化）",
    )
    direction: str = Field(
        default="below", description="乖離方向（below/above）"
    )


class BaselinePositionSignalParams(BaseSignalParams):
    """基準線位置シグナルパラメータ"""
    baseline_type: str = Field(default="sma", description="基準線タイプ（sma/ema/vwema）")
    baseline_period: int = Field(default=25, gt=0, le=500, description="基準線期間")
    price_column: str = Field(
        default="close", description="判定価格カラム（close/high/low）"
    )
    direction: str = Field(
        default="above", description="位置方向（above/below）"
    )


class BuyAndHoldSignalParams(BaseSignalParams):
    """Buy&Holdシグナルパラメータ（全日程エントリー可能）"""


class RiskAdjustedReturnSignalParams(BaseSignalParams):
    """リスク調整リターンシグナルパラメータ（シャープ/ソルティノレシオベース）"""
    lookback_period: int = Field(
        default=60, gt=0, le=500, description="計算期間（日数）"
    )
    threshold: float = Field(
        default=1.0, ge=-5.0, le=10.0, description="リスク調整リターン閾値"
    )
    ratio_type: str = Field(
        default="sortino",
        description="計算タイプ（sharpe=全体分散、sortino=ダウンサイド分散のみ）",
    )
    condition: str = Field(
        default="above",
        description="閾値条件（above=閾値以上、below=閾値未満）",
    )
    margin_min: float | None = Field(
        default=None,
        ge=-10.0,
        le=10.0,
        description=(
            "research用の最小マージン。ratio - threshold がこの値以上のときだけ通す"
        ),
    )
    margin_max: float | None = Field(
        default=None,
        ge=-10.0,
        le=10.0,
        description=(
            "research用の最大マージン。ratio - threshold がこの値以下のときだけ通す"
        ),
    )

    @field_validator("ratio_type")
    @classmethod
    def validate_ratio_type(cls, v: str) -> str:
        if v not in ["sharpe", "sortino"]:
            raise ValueError("ratio_typeは'sharpe'または'sortino'のみ指定可能です")
        return v

    @field_validator("condition")
    @classmethod
    def validate_condition(cls, v: str) -> str:
        return _validate_condition_above_below(v)

    @field_validator("margin_max")
    @classmethod
    def validate_margin_order(
        cls,
        v: float | None,
        info: ValidationInfo,
    ) -> float | None:
        margin_min = info.data.get("margin_min")
        if (
            v is not None
            and margin_min is not None
            and float(v) < float(margin_min)
        ):
            raise ValueError("margin_maxはmargin_min以上である必要があります")
        return v
