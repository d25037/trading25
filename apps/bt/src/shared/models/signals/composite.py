"""
統合シグナルパラメータ（SignalParams）
"""

from pydantic import BaseModel, ConfigDict, Field

from .breakout import (
    BaselineCrossSignalParams,
    BaselineDeviationSignalParams,
    BaselinePositionSignalParams,
    BuyAndHoldSignalParams,
    CrossoverSignalParams,
    PeriodExtremaBreakSignalParams,
    PeriodExtremaPositionSignalParams,
    RiskAdjustedReturnSignalParams,
)
from .fundamental import FundamentalSignalParams
from .macro import (
    BetaSignalParams,
    IndexDailyChangeSignalParams,
    IndexOpenGapRegimeSignalParams,
    IndexMACDHistogramSignalParams,
    MarginSignalParams,
    UniverseRankBucketSignalParams,
)
from .oscillator import RSISpreadSignalParams, RSIThresholdSignalParams
from .trend import (
    RetracementCrossSignalParams,
    RetracementPositionSignalParams,
    TrendSignalParams,
)
from .volatility import (
    ATRSupportCrossParams,
    ATRSupportPositionParams,
    BollingerCrossSignalParams,
    BollingerPositionSignalParams,
    VolatilityPercentileSignalParams,
)
from .sector import (
    SectorRotationPhaseParams,
    SectorStrengthRankingParams,
    SectorVolatilityRegimeParams,
)
from .volume import (
    TradingValueEmaRatioAboveSignalParams,
    TradingValueEmaRatioBelowSignalParams,
    TradingValueRangeSignalParams,
    TradingValueSignalParams,
    VolumeRatioAboveSignalParams,
    VolumeRatioBelowSignalParams,
)


class SignalParams(BaseModel):
    """
    統合シグナルパラメータ（旧FilterParams + TriggerParams）

    エントリー・エグジット両方のシグナルパラメータを統一管理
    """

    model_config = ConfigDict(extra="forbid")

    volume_ratio_above: VolumeRatioAboveSignalParams = Field(
        default_factory=VolumeRatioAboveSignalParams,
        description="出来高比率上抜けシグナル",
    )
    volume_ratio_below: VolumeRatioBelowSignalParams = Field(
        default_factory=VolumeRatioBelowSignalParams,
        description="出来高比率下抜けシグナル",
    )
    trading_value: TradingValueSignalParams = Field(
        default_factory=TradingValueSignalParams, description="売買代金シグナル"
    )
    trading_value_ema_ratio_above: TradingValueEmaRatioAboveSignalParams = Field(
        default_factory=TradingValueEmaRatioAboveSignalParams,
        description="短期EMA売買代金 / ADV シグナル",
    )
    trading_value_ema_ratio_below: TradingValueEmaRatioBelowSignalParams = Field(
        default_factory=TradingValueEmaRatioBelowSignalParams,
        description="短期EMA売買代金 / ADV stale-volume シグナル",
    )
    trading_value_range: TradingValueRangeSignalParams = Field(
        default_factory=TradingValueRangeSignalParams, description="売買代金範囲シグナル"
    )
    trend: TrendSignalParams = Field(
        default_factory=TrendSignalParams, description="トレンドシグナル"
    )
    fundamental: FundamentalSignalParams = Field(
        default_factory=FundamentalSignalParams, description="財務指標シグナル"
    )
    volatility_percentile: VolatilityPercentileSignalParams = Field(
        default_factory=VolatilityPercentileSignalParams,
        description="ボラティリティパーセンタイルシグナル",
    )
    beta: BetaSignalParams = Field(
        default_factory=BetaSignalParams, description="β値シグナル"
    )
    margin: MarginSignalParams = Field(
        default_factory=MarginSignalParams, description="信用残高シグナル"
    )
    atr_support_position: ATRSupportPositionParams = Field(
        default_factory=ATRSupportPositionParams,
        description="ATRサポートライン位置シグナル",
    )
    atr_support_cross: ATRSupportCrossParams = Field(
        default_factory=ATRSupportCrossParams,
        description="ATRサポートラインクロスシグナル",
    )
    retracement_position: RetracementPositionSignalParams = Field(
        default_factory=RetracementPositionSignalParams,
        description="リトレースメント位置シグナル（フィボナッチ下落率ベース）",
    )
    retracement_cross: RetracementCrossSignalParams = Field(
        default_factory=RetracementCrossSignalParams,
        description="リトレースメントクロスシグナル（フィボナッチ下落率ベース）",
    )
    period_extrema_break: PeriodExtremaBreakSignalParams = Field(
        default_factory=PeriodExtremaBreakSignalParams,
        description="期間高値・安値ブレイクイベントシグナル",
    )
    period_extrema_position: PeriodExtremaPositionSignalParams = Field(
        default_factory=PeriodExtremaPositionSignalParams,
        description="期間高値・安値位置シグナル",
    )
    crossover: CrossoverSignalParams = Field(
        default_factory=CrossoverSignalParams,
        description="クロスオーバーシグナル（SMA/RSI/MACD/EMA）",
    )
    bollinger_position: BollingerPositionSignalParams = Field(
        default_factory=BollingerPositionSignalParams,
        description="ボリンジャーバンド位置シグナル",
    )
    bollinger_cross: BollingerCrossSignalParams = Field(
        default_factory=BollingerCrossSignalParams,
        description="ボリンジャーバンドクロスシグナル",
    )
    buy_and_hold: BuyAndHoldSignalParams = Field(
        default_factory=BuyAndHoldSignalParams,
        description="Buy&Holdシグナル（全日程エントリー可能）",
    )
    baseline_deviation: BaselineDeviationSignalParams = Field(
        default_factory=BaselineDeviationSignalParams,
        description="基準線からの乖離率シグナル",
    )
    baseline_position: BaselinePositionSignalParams = Field(
        default_factory=BaselinePositionSignalParams,
        description="価格と基準線の位置関係シグナル",
    )
    rsi_threshold: RSIThresholdSignalParams = Field(
        default_factory=RSIThresholdSignalParams,
        description="RSI閾値シグナル（買われすぎ・売られすぎ判定）",
    )
    rsi_spread: RSISpreadSignalParams = Field(
        default_factory=RSISpreadSignalParams,
        description="RSIスプレッドシグナル（短期RSIと長期RSIの差分判定）",
    )
    baseline_cross: BaselineCrossSignalParams = Field(
        default_factory=BaselineCrossSignalParams,
        description="価格と基準線のクロスシグナル",
    )
    index_daily_change: IndexDailyChangeSignalParams = Field(
        default_factory=IndexDailyChangeSignalParams,
        description="指数前日比シグナル（市場環境フィルター）",
    )
    index_macd_histogram: IndexMACDHistogramSignalParams = Field(
        default_factory=IndexMACDHistogramSignalParams,
        description="INDEXヒストグラムシグナル（市場モメンタム強弱判定）",
    )
    index_open_gap_regime: IndexOpenGapRegimeSignalParams = Field(
        default_factory=IndexOpenGapRegimeSignalParams,
        description="指数寄り付きギャップレジームシグナル",
    )
    universe_rank_bucket: UniverseRankBucketSignalParams = Field(
        default_factory=UniverseRankBucketSignalParams,
        description="指数/ユニバース内順位バケットシグナル",
    )
    risk_adjusted_return: RiskAdjustedReturnSignalParams = Field(
        default_factory=RiskAdjustedReturnSignalParams,
        description="リスク調整リターンシグナル（シャープ/ソルティノレシオベース）",
    )
    sector_strength_ranking: SectorStrengthRankingParams = Field(
        default_factory=SectorStrengthRankingParams,
        description="セクター強度ランキングシグナル（上位/下位Nセクター選択）",
    )
    sector_rotation_phase: SectorRotationPhaseParams = Field(
        default_factory=SectorRotationPhaseParams,
        description="セクターローテーション位相シグナル（RRG的4象限分類）",
    )
    sector_volatility_regime: SectorVolatilityRegimeParams = Field(
        default_factory=SectorVolatilityRegimeParams,
        description="セクターボラティリティレジームシグナル（低ボラ/高ボラ環境判定）",
    )

    def has_any_enabled(self) -> bool:
        """いずれかのシグナルが有効かチェック（Entry/Exit統合）"""
        for field_name in type(self).model_fields:
            field_value = getattr(self, field_name)

            # 直接 enabled 属性を持つ場合
            if hasattr(field_value, "enabled") and field_value.enabled:
                return True

            # ネストされた構造（fundamental等）の場合
            if hasattr(field_value, "model_fields"):
                for nested_name in type(field_value).model_fields:
                    nested_value = getattr(field_value, nested_name)
                    if hasattr(nested_value, "enabled") and nested_value.enabled:
                        return True

        return False

    def has_any_entry_enabled(self) -> bool:
        """エントリーシグナル有効チェック（has_any_enabled()のエイリアス）"""
        return self.has_any_enabled()

    def has_any_exit_enabled(self) -> bool:
        """エグジットシグナル有効チェック（has_any_enabled()のエイリアス）"""
        return self.has_any_enabled()
