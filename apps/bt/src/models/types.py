"""
共通型定義
"""

from typing import Literal

# 決算期間タイプ
# - "all": 全期間
# - "FY": 本決算のみ
# - "1Q", "2Q", "3Q": 各四半期
StatementsPeriodType = Literal["all", "FY", "1Q", "2Q", "3Q"]
