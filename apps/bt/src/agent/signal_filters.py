"""Lab用のシグナルフィルター判定ヘルパー。"""

from .models import SignalCategory

SIGNAL_CATEGORY_MAP: dict[str, SignalCategory] = {
    "period_breakout": "breakout",
    "ma_breakout": "trend",
    "crossover": "trend",
    "mean_reversion": "oscillator",
    "bollinger_bands": "volatility",
    "atr_support_break": "volatility",
    "rsi_threshold": "oscillator",
    "rsi_spread": "oscillator",
    "volume": "volume",
    "trading_value": "volume",
    "trading_value_range": "volume",
    "beta": "macro",
    "margin": "macro",
    "index_daily_change": "macro",
    "index_macd_histogram": "macro",
    "fundamental": "fundamental",
}


def is_signal_allowed(
    signal_name: str,
    allowed_categories: set[SignalCategory],
) -> bool:
    """カテゴリ制約に基づいてシグナルを許可判定する。"""
    if not allowed_categories:
        return True
    category = SIGNAL_CATEGORY_MAP.get(signal_name)
    return category in allowed_categories
