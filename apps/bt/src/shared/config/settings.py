"""
Centralized settings

環境変数とデフォルト値の単一ソースを提供する。
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from pydantic import BaseModel, Field

# Monorepo SoT: repository root の .env を読み込む
def _find_repo_root(start: Path) -> Path:
    for current in (start, *start.parents):
        if (current / ".git").exists():
            return current
    raise RuntimeError(f"Repository root not found from {start}")


_repo_root = _find_repo_root(Path(__file__).resolve())
_dotenv_path = _repo_root / ".env"
load_dotenv(_dotenv_path, override=False)


def _default_data_dir() -> str:
    """XDG準拠のデフォルトデータディレクトリ (TRADING25_DATA_DIR と同じロジック)"""
    env = os.environ.get("TRADING25_DATA_DIR")
    if env:
        return env
    return str(Path.home() / ".local" / "share" / "trading25")


class Settings(BaseModel):
    """アプリケーション設定"""

    api_base_url: str = Field(
        default="http://localhost:3002", alias="API_BASE_URL"
    )
    api_timeout: float = Field(default=30.0, alias="API_TIMEOUT")
    log_level: str = Field(default="WARNING", alias="LOG_LEVEL")

    # JQuants API
    jquants_api_key: str = Field(default="", alias="JQUANTS_API_KEY")
    jquants_plan: str = Field(default="free", alias="JQUANTS_PLAN")

    # market.db (Phase 3B-2a)
    market_db_path: str = Field(default="", alias="MARKET_DB_PATH")

    # market time-series data plane (Phase 2)
    market_timeseries_backend: str = Field(
        default="duckdb-parquet",
        alias="MARKET_TIMESERIES_BACKEND",
    )
    market_timeseries_dir: str = Field(default="", alias="MARKET_TIMESERIES_DIR")
    market_timeseries_sqlite_mirror: bool = Field(
        default=True,
        alias="MARKET_TIMESERIES_SQLITE_MIRROR",
    )

    # portfolio.db (Phase 3C)
    portfolio_db_path: str = Field(default="", alias="PORTFOLIO_DB_PATH")

    # dataset base path (Phase 3C)
    dataset_base_path: str = Field(default="", alias="DATASET_BASE_PATH")

    model_config = {"populate_by_name": True}

    def model_post_init(self, __context: Any) -> None:
        """環境変数未設定時にXDGデフォルトパスを自動設定"""
        data_dir = _default_data_dir()
        if not self.market_db_path:
            self.market_db_path = str(Path(data_dir) / "market.db")
        if not self.market_timeseries_dir:
            self.market_timeseries_dir = str(Path(data_dir) / "market-timeseries")
        if not self.portfolio_db_path:
            self.portfolio_db_path = str(Path(data_dir) / "portfolio.db")
        if not self.dataset_base_path:
            self.dataset_base_path = str(Path(data_dir) / "datasets")


@lru_cache
def get_settings() -> Settings:
    """キャッシュされた設定を取得"""
    return Settings.model_validate(dict(os.environ))


def reload_settings() -> Settings:
    """環境変数の再読み込み"""
    get_settings.cache_clear()
    return get_settings()
