"""
財務指標シグナルパラメータ
"""

from typing import Literal

from pydantic import BaseModel, Field

from src.shared.models.types import StatementsPeriodType

from .base import BaseSignalParams


class FundamentalSignalParams(BaseSignalParams):
    """財務指標シグナルパラメータ"""

    use_adjusted: bool = Field(
        default=True,
        description="株式分割等を考慮したAdjusted指標を使用するか",
    )

    # =========================================================================
    # バリュエーション系パラメータ
    # =========================================================================

    class PERParams(BaseModel):
        """PER（株価収益率）シグナルパラメータ"""

        enabled: bool = Field(default=False, description="PERシグナル有効")
        threshold: float = Field(
            default=15.0, gt=0, le=100.0, description="PER閾値（この値以下で割安判定）"
        )
        condition: Literal["above", "below"] = Field(
            default="below",
            description="条件（below=閾値以下、above=閾値以上）",
        )
        exclude_negative: bool = Field(
            default=True,
            description="負のPER（損失企業）を除外するか",
        )

    class PBRParams(BaseModel):
        """PBR（株価純資産倍率）シグナルパラメータ"""

        enabled: bool = Field(default=False, description="PBRシグナル有効")
        threshold: float = Field(
            default=1.0, gt=0, le=10.0, description="PBR閾値（この値以下で割安判定）"
        )
        condition: Literal["above", "below"] = Field(
            default="below",
            description="条件（below=閾値以下、above=閾値以上）",
        )
        exclude_negative: bool = Field(
            default=True,
            description="負のPBR（債務超過企業）を除外するか",
        )

    class PEGRatioParams(BaseModel):
        """PEG Ratio シグナルパラメータ"""

        enabled: bool = Field(default=False, description="PEG Ratioシグナル有効")
        threshold: float = Field(
            default=1.0,
            gt=0,
            le=50.0,
            description="PEG Ratio閾値（この値以下で割安成長株判定）",
        )
        condition: Literal["above", "below"] = Field(
            default="below",
            description="条件（below=閾値以下、above=閾値以上）",
        )

    # =========================================================================
    # 成長率系パラメータ
    # =========================================================================

    class ForwardEPSParams(BaseModel):
        """Forward EPS成長率シグナルパラメータ（来期予想EPS vs 当期EPS）"""

        enabled: bool = Field(
            default=False, description="Forward EPS成長率シグナル有効"
        )
        threshold: float = Field(
            default=0.1, gt=0, le=2.0, description="Forward EPS成長率閾値（10%=0.1）"
        )
        condition: Literal["above", "below"] = Field(
            default="above",
            description="条件（above=閾値以上、below=閾値以下）",
        )

    class ForecastEPSAboveAllActualsParams(BaseModel):
        """最新予想EPSが過去すべての実績EPSより大きいシグナルパラメータ"""

        enabled: bool = Field(
            default=False,
            description="最新予想EPS > 過去実績EPSシグナル有効",
        )

    class ForwardDividendGrowthParams(BaseModel):
        """Forward 1株配当成長率シグナル（来期予想配当 vs 当期配当）"""

        enabled: bool = Field(
            default=False, description="Forward 1株配当成長率シグナル有効"
        )
        threshold: float = Field(
            default=0.05, gt=0, le=2.0, description="Forward 1株配当成長率閾値（5%=0.05）"
        )
        condition: Literal["above", "below"] = Field(
            default="above",
            description="条件（above=閾値以上、below=閾値以下）",
        )

    class EPSGrowthParams(BaseModel):
        """EPS成長率シグナルパラメータ（実績EPS同士の比較）"""

        enabled: bool = Field(default=False, description="EPS成長率シグナル有効")
        threshold: float = Field(
            default=0.1, gt=0, le=2.0, description="成長率閾値（10%=0.1）"
        )
        periods: int = Field(
            default=1, ge=1, le=20, description="比較期間（決算発表回数、1=前期比）"
        )
        condition: Literal["above", "below"] = Field(
            default="above",
            description="条件（above=閾値以上、below=閾値以下）",
        )

    class ProfitGrowthParams(BaseModel):
        """Profit成長率シグナルパラメータ"""

        enabled: bool = Field(default=False, description="Profit成長率シグナル有効")
        threshold: float = Field(
            default=0.1, gt=0, le=2.0, description="成長率閾値（10%=0.1）"
        )
        periods: int = Field(
            default=1, ge=1, le=20, description="比較期間（決算発表回数、1=前期比）"
        )
        condition: Literal["above", "below"] = Field(
            default="above",
            description="条件（above=閾値以上、below=閾値以下）",
        )

    class SalesGrowthParams(BaseModel):
        """Sales成長率シグナルパラメータ"""

        enabled: bool = Field(default=False, description="Sales成長率シグナル有効")
        threshold: float = Field(
            default=0.1, gt=0, le=2.0, description="成長率閾値（10%=0.1）"
        )
        periods: int = Field(
            default=1, ge=1, le=20, description="比較期間（決算発表回数、1=前期比）"
        )
        condition: Literal["above", "below"] = Field(
            default="above",
            description="条件（above=閾値以上、below=閾値以下）",
        )

    class DividendPerShareGrowthParams(BaseModel):
        """1株配当成長率シグナルパラメータ"""

        enabled: bool = Field(default=False, description="1株配当成長率シグナル有効")
        threshold: float = Field(
            default=0.1, gt=0, le=2.0, description="成長率閾値（10%=0.1）"
        )
        periods: int = Field(
            default=1, ge=1, le=20, description="比較期間（決算発表回数、1=前期比）"
        )
        condition: Literal["above", "below"] = Field(
            default="above",
            description="条件（above=閾値以上、below=閾値以下）",
        )

    # =========================================================================
    # 収益性・キャッシュフロー系パラメータ
    # =========================================================================

    class ROEParams(BaseModel):
        """ROE（自己資本利益率）シグナルパラメータ"""

        enabled: bool = Field(default=False, description="ROEシグナル有効")
        threshold: float = Field(
            default=10.0,
            gt=0,
            le=100.0,
            description="ROE閾値（この値を上回る高ROE判定、%単位）",
        )
        condition: Literal["above", "below"] = Field(
            default="above",
            description="条件（above=閾値以上、below=閾値以下）",
        )

    class ROAParams(BaseModel):
        """ROA（総資産利益率）シグナルパラメータ"""

        enabled: bool = Field(default=False, description="ROAシグナル有効")
        threshold: float = Field(
            default=5.0,
            gt=0,
            le=100.0,
            description="ROA閾値（この値を上回る高ROA判定、%単位）",
        )
        condition: Literal["above", "below"] = Field(
            default="above",
            description="条件（above=閾値以上、below=閾値以下）",
        )

    class OperatingMarginParams(BaseModel):
        """営業利益率シグナルパラメータ"""

        enabled: bool = Field(default=False, description="営業利益率シグナル有効")
        threshold: float = Field(
            default=10.0,
            gt=0,
            le=100.0,
            description="営業利益率閾値（この値以上で高収益判定、%単位）",
        )
        condition: Literal["above", "below"] = Field(
            default="above",
            description="条件（above=閾値以上、below=閾値以下）",
        )

    class OperatingCashFlowParams(BaseModel):
        """営業キャッシュフローシグナルパラメータ"""

        enabled: bool = Field(default=False, description="営業CFシグナル有効")
        threshold: float = Field(
            default=0.0, description="営業CF閾値（この値を超えると正のCF判定）"
        )
        condition: Literal["above", "below"] = Field(
            default="above",
            description="条件（above=閾値より大きい、below=閾値より小さい）",
        )
        consecutive_periods: int = Field(
            default=1,
            ge=1,
            le=10,
            description="連続期間数（直近N回分の決算発表で条件を満たす必要がある）",
        )

    class CFOToNetProfitRatioParams(BaseModel):
        """営業CF/純利益シグナルパラメータ"""

        enabled: bool = Field(
            default=False,
            description="営業CF/純利益シグナル有効",
        )
        threshold: float = Field(
            default=1.0,
            description="営業CF/純利益閾値（1.0以上は利益の質が高い目安）",
        )
        condition: Literal["above", "below"] = Field(
            default="above",
            description="条件（above=閾値以上、below=閾値未満）",
        )
        consecutive_periods: int = Field(
            default=1,
            ge=1,
            le=10,
            description="連続期間数（直近N回分の決算発表で条件を満たす必要がある）",
        )

    class DividendYieldParams(BaseModel):
        """配当利回りシグナルパラメータ"""

        enabled: bool = Field(default=False, description="配当利回りシグナル有効")
        threshold: float = Field(
            default=2.0,
            gt=0,
            le=30.0,
            description="配当利回り閾値（この値以上で高配当判定、%単位）",
        )
        condition: Literal["above", "below"] = Field(
            default="above",
            description="条件（above=閾値以上、below=閾値以下）",
        )

    class PayoutRatioParams(BaseModel):
        """配当性向シグナルパラメータ"""

        enabled: bool = Field(default=False, description="配当性向シグナル有効")
        threshold: float = Field(
            default=30.0,
            ge=0,
            le=300.0,
            description="配当性向閾値（%単位）",
        )
        condition: Literal["above", "below"] = Field(
            default="above",
            description="条件（above=閾値以上、below=閾値以下）",
        )

    class ForwardPayoutRatioParams(BaseModel):
        """予想配当性向シグナルパラメータ"""

        enabled: bool = Field(default=False, description="予想配当性向シグナル有効")
        threshold: float = Field(
            default=30.0,
            ge=0,
            le=300.0,
            description="予想配当性向閾値（%単位）",
        )
        condition: Literal["above", "below"] = Field(
            default="above",
            description="条件（above=閾値以上、below=閾値以下）",
        )

    class SimpleFCFParams(BaseModel):
        """簡易FCF（CFO + CFI）シグナルパラメータ"""

        enabled: bool = Field(default=False, description="簡易FCFシグナル有効")
        threshold: float = Field(
            default=0.0,
            description="簡易FCF閾値（この値以上で正のFCF判定）",
        )
        condition: Literal["above", "below"] = Field(
            default="above",
            description="条件（above=閾値以上、below=閾値以下）",
        )
        consecutive_periods: int = Field(
            default=1,
            ge=1,
            le=10,
            description="連続期間数（直近N回分の決算発表で条件を満たす必要がある）",
        )

    class CFOMarginParams(BaseModel):
        """CFOマージン（営業CF/売上高）シグナルパラメータ"""

        enabled: bool = Field(default=False, description="CFOマージンシグナル有効")
        threshold: float = Field(
            default=5.0,
            ge=-100.0,
            le=100.0,
            description="CFOマージン閾値（この値以上で高CFOマージン判定、%単位）",
        )
        condition: Literal["above", "below"] = Field(
            default="above",
            description="条件（above=閾値以上、below=閾値以下）",
        )

    class SimpleFCFMarginParams(BaseModel):
        """簡易FCFマージン（(CFO+CFI)/売上高）シグナルパラメータ"""

        enabled: bool = Field(default=False, description="簡易FCFマージンシグナル有効")
        threshold: float = Field(
            default=5.0,
            ge=-100.0,
            le=100.0,
            description="簡易FCFマージン閾値（この値以上で高FCFマージン判定、%単位）",
        )
        condition: Literal["above", "below"] = Field(
            default="above",
            description="条件（above=閾値以上、below=閾値以下）",
        )

    # =========================================================================
    # 時価総額系パラメータ
    # =========================================================================

    class MarketCapParams(BaseModel):
        """時価総額閾値シグナルパラメータ"""

        enabled: bool = Field(default=False, description="時価総額シグナル有効")
        threshold: float = Field(
            default=100.0,
            gt=0,
            le=100000.0,
            description="時価総額閾値（億円単位、100.0 = 100億円）",
        )
        condition: Literal["above", "below"] = Field(
            default="above",
            description="条件（above=閾値以上、below=閾値未満）",
        )
        use_floating_shares: bool = Field(
            default=True,
            description="流通株式を使用（発行済み-自己株式）、Falseなら発行済み全体",
        )

    # =========================================================================
    # 利回り系パラメータ
    # =========================================================================

    class CFOYieldParams(BaseModel):
        """CFO利回り（営業CF/時価総額）シグナルパラメータ"""

        enabled: bool = Field(default=False, description="CFO利回りシグナル有効")
        threshold: float = Field(
            default=5.0,
            ge=-50.0,
            le=50.0,
            description="CFO利回り閾値（この値以上で高CFO利回り判定、%単位）",
        )
        condition: Literal["above", "below"] = Field(
            default="above",
            description="条件（above=閾値以上、below=閾値以下）",
        )
        use_floating_shares: bool = Field(
            default=True,
            description="流通株式を使用（発行済み-自己株式）、Falseなら発行済み全体",
        )

    class SimpleFCFYieldParams(BaseModel):
        """簡易FCF利回り（(CFO+CFI)/時価総額）シグナルパラメータ"""

        enabled: bool = Field(default=False, description="簡易FCF利回りシグナル有効")
        threshold: float = Field(
            default=5.0,
            ge=-50.0,
            le=50.0,
            description="簡易FCF利回り閾値（この値以上で高FCF利回り判定、%単位）",
        )
        condition: Literal["above", "below"] = Field(
            default="above",
            description="条件（above=閾値以上、below=閾値以下）",
        )
        use_floating_shares: bool = Field(
            default=True,
            description="流通株式を使用（発行済み-自己株式）、Falseなら発行済み全体",
        )

    class CFOYieldGrowthParams(BaseModel):
        """CFO利回り成長率シグナルパラメータ"""

        enabled: bool = Field(default=False, description="CFO利回り成長率シグナル有効")
        threshold: float = Field(
            default=0.1, gt=0, le=2.0, description="成長率閾値（10%=0.1）"
        )
        periods: int = Field(
            default=1, ge=1, le=20, description="比較期間（決算発表回数、1=前期比）"
        )
        condition: Literal["above", "below"] = Field(
            default="above",
            description="条件（above=閾値以上、below=閾値以下）",
        )
        use_floating_shares: bool = Field(
            default=True,
            description="流通株式を使用（発行済み-自己株式）、Falseなら発行済み全体",
        )

    class SimpleFCFYieldGrowthParams(BaseModel):
        """簡易FCF利回り成長率シグナルパラメータ"""

        enabled: bool = Field(default=False, description="簡易FCF利回り成長率シグナル有効")
        threshold: float = Field(
            default=0.1, gt=0, le=2.0, description="成長率閾値（10%=0.1）"
        )
        periods: int = Field(
            default=1, ge=1, le=20, description="比較期間（決算発表回数、1=前期比）"
        )
        condition: Literal["above", "below"] = Field(
            default="above",
            description="条件（above=閾値以上、below=閾値以下）",
        )
        use_floating_shares: bool = Field(
            default=True,
            description="流通株式を使用（発行済み-自己株式）、Falseなら発行済み全体",
        )

    # =========================================================================
    # 親レベルパラメータ
    # =========================================================================

    period_type: StatementsPeriodType = Field(
        default="FY",
        description="決算期間タイプ（FY=本決算のみ、all=全四半期、1Q/2Q/3Q=特定四半期）",
    )

    # =========================================================================
    # フィールド定義
    # =========================================================================

    # バリュエーション系
    per: PERParams = Field(default_factory=PERParams, description="PERシグナル")
    pbr: PBRParams = Field(default_factory=PBRParams, description="PBRシグナル")
    peg_ratio: PEGRatioParams = Field(
        default_factory=PEGRatioParams, description="PEG Ratioシグナル"
    )

    # 成長率系
    forward_eps_growth: ForwardEPSParams = Field(
        default_factory=ForwardEPSParams, description="Forward EPS成長率シグナル"
    )
    forecast_eps_above_all_actuals: ForecastEPSAboveAllActualsParams = Field(
        default_factory=ForecastEPSAboveAllActualsParams,
        description="最新予想EPSが過去すべての実績EPSより大きいシグナル",
    )
    forward_dividend_growth: ForwardDividendGrowthParams = Field(
        default_factory=ForwardDividendGrowthParams,
        description="Forward 1株配当成長率シグナル",
    )
    eps_growth: EPSGrowthParams = Field(
        default_factory=EPSGrowthParams, description="EPS成長率シグナル（実績ベース）"
    )
    profit_growth: ProfitGrowthParams = Field(
        default_factory=ProfitGrowthParams, description="Profit成長率シグナル"
    )
    sales_growth: SalesGrowthParams = Field(
        default_factory=SalesGrowthParams, description="Sales成長率シグナル"
    )
    dividend_per_share_growth: DividendPerShareGrowthParams = Field(
        default_factory=DividendPerShareGrowthParams,
        description="1株配当成長率シグナル",
    )

    # 収益性・キャッシュフロー系
    roe: ROEParams = Field(default_factory=ROEParams, description="ROEシグナル")
    roa: ROAParams = Field(default_factory=ROAParams, description="ROAシグナル")
    operating_margin: OperatingMarginParams = Field(
        default_factory=OperatingMarginParams, description="営業利益率シグナル"
    )
    operating_cash_flow: OperatingCashFlowParams = Field(
        default_factory=OperatingCashFlowParams, description="営業CFシグナル"
    )
    cfo_to_net_profit_ratio: CFOToNetProfitRatioParams = Field(
        default_factory=CFOToNetProfitRatioParams, description="営業CF/純利益シグナル"
    )
    dividend_yield: DividendYieldParams = Field(
        default_factory=DividendYieldParams, description="配当利回りシグナル"
    )
    payout_ratio: PayoutRatioParams = Field(
        default_factory=PayoutRatioParams, description="配当性向シグナル"
    )
    forward_payout_ratio: ForwardPayoutRatioParams = Field(
        default_factory=ForwardPayoutRatioParams,
        description="予想配当性向シグナル",
    )
    simple_fcf: SimpleFCFParams = Field(
        default_factory=SimpleFCFParams, description="簡易FCF（CFO+CFI）シグナル"
    )
    cfo_margin: CFOMarginParams = Field(
        default_factory=CFOMarginParams, description="CFOマージンシグナル"
    )
    simple_fcf_margin: SimpleFCFMarginParams = Field(
        default_factory=SimpleFCFMarginParams, description="簡易FCFマージンシグナル"
    )

    # 利回り系
    cfo_yield: CFOYieldParams = Field(
        default_factory=CFOYieldParams, description="CFO利回りシグナル"
    )
    simple_fcf_yield: SimpleFCFYieldParams = Field(
        default_factory=SimpleFCFYieldParams, description="簡易FCF利回りシグナル"
    )
    cfo_yield_growth: CFOYieldGrowthParams = Field(
        default_factory=CFOYieldGrowthParams, description="CFO利回り成長率シグナル"
    )
    simple_fcf_yield_growth: SimpleFCFYieldGrowthParams = Field(
        default_factory=SimpleFCFYieldGrowthParams,
        description="簡易FCF利回り成長率シグナル",
    )

    # 時価総額系
    market_cap: MarketCapParams = Field(
        default_factory=MarketCapParams, description="時価総額シグナル"
    )
