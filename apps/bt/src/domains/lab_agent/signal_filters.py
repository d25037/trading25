"""Lab用のシグナルフィルター判定ヘルパー。"""

from .models import SignalCategory
from .signal_catalog import SIGNAL_CATEGORY_MAP


def is_signal_allowed(
    signal_name: str,
    allowed_categories: set[SignalCategory],
) -> bool:
    """カテゴリ制約に基づいてシグナルを許可判定する。"""
    if not allowed_categories:
        return True
    category = SIGNAL_CATEGORY_MAP.get(signal_name)
    return category in allowed_categories
