"""
ブレイクアウト・クロスオーバー・平均回帰シグナルパラメータ
"""

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


class PeriodBreakoutParams(BaseSignalParams):
    """期間ブレイクアウトパラメータ（統合版）"""
    direction: str = Field(
        default="high", description="方向（high=最高値比較、low=最安値比較）"
    )
    condition: str = Field(
        default="break", description="条件（break=ブレイク検出、maintained=維持検出）"
    )
    period: int = Field(
        default=20, gt=0, le=1000, description="比較対象期間（N日最高値/最安値）"
    )
    lookback_days: int = Field(
        default=1,
        gt=0,
        le=500,
        description="イベント検出期間（1=今日の価格、N=直近N日の最高値/最安値）",
    )


class MABreakoutParams(BaseSignalParams):
    """移動平均線ブレイクアウトパラメータ（統合版）"""
    period: int = Field(default=200, gt=0, le=1000, description="移動平均期間")
    ma_type: str = Field(default="sma", description="移動平均タイプ（sma/ema）")
    direction: str = Field(
        default="above", description="方向（above=上抜け、below=下抜け）"
    )
    lookback_days: int = Field(
        default=1, gt=0, le=100, description="検出期間（1=その日のみ、N=直近N日以内）"
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


class MeanReversionSignalParams(BaseSignalParams):
    """平均回帰シグナルパラメータ（汎用・シンプル設計）"""
    baseline_type: str = Field(default="sma", description="基準線タイプ（sma/ema）")
    baseline_period: int = Field(default=25, gt=0, le=500, description="基準線期間")
    deviation_threshold: float = Field(
        default=0.2,
        ge=0,
        le=1.0,
        description="乖離率閾値（0.2 = 20%乖離、0.0で無効化）",
    )
    deviation_direction: str = Field(
        default="below", description="乖離方向（below/above）"
    )
    recovery_price: str = Field(
        default="high", description="回復判定価格（high/low/close/none、none で無効化）"
    )
    recovery_direction: str = Field(
        default="above", description="回復方向（above/below）"
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
