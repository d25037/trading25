"""
セクターシグナルパラメータ

33業種セクターインデックスベースのシグナルパラメータモデル
"""

from typing import Literal

from pydantic import Field

from .base import BaseSignalParams


class SectorStrengthRankingParams(BaseSignalParams):
    """セクター強度ランキングシグナルパラメータ

    複合スコア（モメンタム + シャープレシオ + 相対強度）で
    全33セクターをランキングし、上位または下位Nセクターのみエントリー許可
    """

    momentum_period: int = Field(
        default=20, gt=0, le=300, description="モメンタム計算期間（日数）"
    )
    sharpe_period: int = Field(
        default=60, gt=0, le=500, description="シャープレシオ計算期間（日数）"
    )
    top_n: int = Field(
        default=10, gt=0, le=33, description="選択するセクター数（上位/下位N、1-33）"
    )
    selection_mode: Literal["top", "bottom"] = Field(
        default="top", description="選択モード（top=上位Nセクター、bottom=下位Nセクター）"
    )
    momentum_weight: float = Field(
        default=0.4, ge=0.0, le=1.0, description="モメンタムスコア重み"
    )
    sharpe_weight: float = Field(
        default=0.4, ge=0.0, le=1.0, description="シャープレシオスコア重み"
    )
    relative_weight: float = Field(
        default=0.2, ge=0.0, le=1.0, description="相対強度スコア重み"
    )


class SectorRotationPhaseParams(BaseSignalParams):
    """セクターローテーション位相シグナルパラメータ

    RRG（Relative Rotation Graph）的な4象限分類に基づき、
    セクターの位相を判定するシグナル
    """

    rs_period: int = Field(
        default=20, gt=0, le=300, description="相対強度移動平均期間（日数）"
    )
    direction: Literal["leading", "weakening"] = Field(
        default="leading",
        description="判定方向（leading=先行局面、weakening=衰退局面）",
    )


class SectorVolatilityRegimeParams(BaseSignalParams):
    """セクターボラティリティレジームシグナルパラメータ

    セクターの年率ボラティリティが平均以下（低ボラ環境）か
    平均以上（高ボラ環境）かを判定するシグナル
    """

    vol_period: int = Field(
        default=20, gt=0, le=300, description="ボラティリティ計算期間（日数）"
    )
    vol_ma_period: int = Field(
        default=60, gt=0, le=500, description="ボラティリティ移動平均期間（日数）"
    )
    direction: Literal["low_vol", "high_vol"] = Field(
        default="low_vol",
        description="判定方向（low_vol=低ボラ環境、high_vol=高ボラ環境）",
    )
    spike_multiplier: float = Field(
        default=1.5, gt=1.0, le=5.0, description="高ボラ判定倍率"
    )
