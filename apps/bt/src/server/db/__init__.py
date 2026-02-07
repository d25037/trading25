"""
Database access layer

market.db への読み取り専用アクセスを提供する。
"""

from src.server.db.market_reader import MarketDbReader

__all__ = ["MarketDbReader"]
