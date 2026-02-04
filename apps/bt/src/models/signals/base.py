"""
基底シグナルモデル

Signals と BaseSignalParams、共通バリデーション関数を提供
"""

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator

__all__ = [
    "Signals",
    "BaseSignalParams",
    "_validate_period_order",
    "_validate_condition_above_below",
]


def _validate_period_order(
    v: int, info: ValidationInfo, short_field: str, error_msg: str
) -> int:
    """短期・長期期間の順序検証（共通）

    Args:
        v: バリデーション対象の長期期間値
        info: Pydanticバリデーション情報
        short_field: 短期期間フィールド名
        error_msg: エラーメッセージ

    Returns:
        int: 検証済みの長期期間値

    Raises:
        ValueError: 長期期間が短期期間以下の場合
    """
    if (
        hasattr(info, "data")
        and short_field in info.data
        and v <= info.data[short_field]
    ):
        raise ValueError(error_msg)
    return v


class Signals(BaseModel):
    """
    売買シグナルを管理するPydanticモデル

    型安全性と実行時バリデーションを提供します。

    Attributes:
        entries: 買いシグナル（boolean Series）
        exits: 売りシグナル（boolean Series）
    """

    entries: pd.Series
    exits: pd.Series

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @field_validator("entries", "exits")
    @classmethod
    def validate_boolean_series(cls, v) -> pd.Series:
        """
        Series型とboolean型の厳密バリデーション

        Args:
            v: バリデーション対象のSeries

        Returns:
            pd.Series: 検証済みのboolean Series

        Raises:
            ValueError: Series型でない、またはboolean型でない場合
        """
        if not isinstance(v, pd.Series):
            raise ValueError("Must be pd.Series")

        if v.dtype != bool:
            raise ValueError(f"Must be boolean Series, got {v.dtype}")

        return v

    @field_validator("exits")
    @classmethod
    def validate_index_consistency(cls, v: pd.Series, info: ValidationInfo) -> pd.Series:
        """
        entriesとexitsのインデックス一致性を検証

        Args:
            v: exits Series
            info: バリデーション情報（entriesアクセス用）

        Returns:
            pd.Series: 検証済みのexits Series

        Raises:
            ValueError: インデックスが一致しない場合
        """
        if "entries" in info.data:
            entries = info.data["entries"]
            if not v.index.equals(entries.index):
                raise ValueError("entries and exits must have identical indices")

        return v

    def __len__(self) -> int:
        """シグナル数（Series長）を返す"""
        return len(self.entries)

    def any_entries(self) -> bool:
        """買いシグナルが存在するかチェック"""
        return bool(self.entries.any())

    def any_exits(self) -> bool:
        """売りシグナルが存在するかチェック"""
        return bool(self.exits.any())

    def summary(self) -> dict:
        """シグナル概要を辞書で返す"""
        return {
            "total_length": len(self),
            "entry_signals": self.entries.sum(),
            "exit_signals": self.exits.sum(),
            "has_entries": self.any_entries(),
            "has_exits": self.any_exits(),
        }


class BaseSignalParams(BaseModel):
    """シグナル基底クラス"""

    enabled: bool = Field(default=False, description="シグナル有効フラグ")


def _validate_condition_above_below(v: str) -> str:
    """condition フィールドの 'above'/'below' バリデーション（共通）"""
    if v not in ["above", "below"]:
        raise ValueError("conditionは'above'または'below'のみ指定可能です")
    return v
