"""
共通型定義
"""

from typing import Literal

# 決算期間タイプ
# - "all": 全期間
# - "FY": 本決算のみ
# - "1Q", "2Q", "3Q": 各四半期
StatementsPeriodType = Literal["all", "FY", "1Q", "2Q", "3Q"]

# レガシー期間タイプ変換マップ (Q1->1Q, Q2->2Q, Q3->3Q)
_LEGACY_PERIOD_MAP: dict[str, str] = {"Q1": "1Q", "Q2": "2Q", "Q3": "3Q"}


def normalize_period_type(period_type: str | None) -> str | None:
    """Normalize period type to 1Q/2Q/3Q when possible.

    Handles legacy Q1/Q2/Q3 format and passes through valid types unchanged.
    """
    if period_type is None:
        return None
    if period_type in ("1Q", "2Q", "3Q", "FY", "all"):
        return period_type
    return _LEGACY_PERIOD_MAP.get(period_type, period_type)
