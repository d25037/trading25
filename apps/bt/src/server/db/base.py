"""
Base Database Access

SQLAlchemy Core Engine 管理の基底クラス。
StaticPool + check_same_thread=False で FastAPI の非同期環境に対応。
PRAGMA は event.listens_for で接続ごとに確実に設定される。
"""

from __future__ import annotations

from sqlalchemy import Engine, create_engine, event
from sqlalchemy.pool import StaticPool


class BaseDbAccess:
    """SQLAlchemy Core ベースの DB アクセス基底クラス"""

    def __init__(self, db_path: str, *, read_only: bool = False) -> None:
        self._read_only = read_only

        if read_only:
            # SQLite URI mode: file:{path}?mode=ro
            # creator を使って sqlite3.connect に URI を直接渡す
            import sqlite3

            def _creator() -> sqlite3.Connection:
                uri = f"file:{db_path}?mode=ro"
                return sqlite3.connect(uri, uri=True, check_same_thread=False)

            self._engine = create_engine(
                "sqlite://",
                creator=_creator,
                poolclass=StaticPool,
            )
        else:
            self._engine = create_engine(
                f"sqlite:///{db_path}",
                connect_args={"check_same_thread": False},
                poolclass=StaticPool,
            )

        # 全接続で PRAGMA を確実に設定（プール再利用時も）
        @event.listens_for(self._engine, "connect")
        def _set_pragmas(dbapi_conn: object, _connection_record: object) -> None:  # pyright: ignore[reportUnusedFunction]
            cursor = dbapi_conn.cursor()  # type: ignore[union-attr]
            if not read_only:
                cursor.execute("PRAGMA journal_mode=WAL")
                cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

    @property
    def engine(self) -> Engine:
        return self._engine

    def close(self) -> None:
        self._engine.dispose()
