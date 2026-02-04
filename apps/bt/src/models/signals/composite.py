"""
統合シグナルパラメータ（SignalParams）
"""

from pydantic import BaseModel, Field

from .breakout import (
    BuyAndHoldSignalParams,
    CrossoverSignalParams,
    MABreakoutParams,
    MeanReversionSignalParams,
    PeriodBreakoutParams,
    RiskAdjustedReturnSignalParams,
)
from .fundamental import FundamentalSignalParams
from .macro import (
    BetaSignalParams,
    IndexDailyChangeSignalParams,
    IndexMACDHistogramSignalParams,
    MarginSignalParams,
)
from .oscillator import RSISpreadSignalParams, RSIThresholdSignalParams
from .trend import RetracementSignalParams, TrendSignalParams
from .volatility import (
    ATRSupportBreakParams,
    BollingerBandsSignalParams,
    VolatilitySignalParams,
)
from .sector import (
    SectorRotationPhaseParams,
    SectorStrengthRankingParams,
    SectorVolatilityRegimeParams,
)
from .volume import (
    TradingValueRangeSignalParams,
    TradingValueSignalParams,
    VolumeSignalParams,
)


class SignalParams(BaseModel):
    """
    統合シグナルパラメータ（旧FilterParams + TriggerParams）

    エントリー・エグジット両方のシグナルパラメータを統一管理
    """

    volume: VolumeSignalParams = Field(
        default_factory=VolumeSignalParams, description="出来高シグナル"
    )
    trading_value: TradingValueSignalParams = Field(
        default_factory=TradingValueSignalParams, description="売買代金シグナル"
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
    volatility: VolatilitySignalParams = Field(
        default_factory=VolatilitySignalParams, description="ボラティリティシグナル"
    )
    beta: BetaSignalParams = Field(
        default_factory=BetaSignalParams, description="β値シグナル"
    )
    margin: MarginSignalParams = Field(
        default_factory=MarginSignalParams, description="信用残高シグナル"
    )
    atr_support_break: ATRSupportBreakParams = Field(
        default_factory=ATRSupportBreakParams,
        description="ATRサポートラインブレイクシグナル",
    )
    retracement: RetracementSignalParams = Field(
        default_factory=RetracementSignalParams,
        description="リトレースメントシグナル（フィボナッチ下落率ベース）",
    )
    period_breakout: PeriodBreakoutParams = Field(
        default_factory=PeriodBreakoutParams,
        description="期間ブレイクアウトシグナル（direction統一設計）",
    )
    crossover: CrossoverSignalParams = Field(
        default_factory=CrossoverSignalParams,
        description="クロスオーバーシグナル（SMA/RSI/MACD/EMA）",
    )
    bollinger_bands: BollingerBandsSignalParams = Field(
        default_factory=BollingerBandsSignalParams,
        description="ボリンジャーバンドシグナル（エントリー・エグジット両用）",
    )
    buy_and_hold: BuyAndHoldSignalParams = Field(
        default_factory=BuyAndHoldSignalParams,
        description="Buy&Holdシグナル（全日程エントリー可能）",
    )
    mean_reversion: MeanReversionSignalParams = Field(
        default_factory=MeanReversionSignalParams,
        description="平均回帰シグナル（SMA/EMA基準線・乖離・回復統合）",
    )
    rsi_threshold: RSIThresholdSignalParams = Field(
        default_factory=RSIThresholdSignalParams,
        description="RSI閾値シグナル（買われすぎ・売られすぎ判定）",
    )
    rsi_spread: RSISpreadSignalParams = Field(
        default_factory=RSISpreadSignalParams,
        description="RSIスプレッドシグナル（短期RSIと長期RSIの差分判定）",
    )
    ma_breakout: MABreakoutParams = Field(
        default_factory=MABreakoutParams,
        description="移動平均線ブレイクアウトシグナル（クロス検出）",
    )
    index_daily_change: IndexDailyChangeSignalParams = Field(
        default_factory=IndexDailyChangeSignalParams,
        description="指数前日比シグナル（市場環境フィルター）",
    )
    index_macd_histogram: IndexMACDHistogramSignalParams = Field(
        default_factory=IndexMACDHistogramSignalParams,
        description="INDEXヒストグラムシグナル（市場モメンタム強弱判定）",
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
