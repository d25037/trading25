"""
Database access layer

market.db / dataset.db / portfolio.db へのアクセスを提供する。
- MarketDbReader: Phase 3B 用読み取り専用リーダー（生 SQL）
- BaseDbAccess: SQLAlchemy Core ベースの DB アクセス基底クラス
- MarketDb / DatasetDb / PortfolioDb: Phase 3C 以降の DB アクセスクラス
"""

from src.server.db.market_reader import MarketDbReader

__all__ = ["MarketDbReader"]
