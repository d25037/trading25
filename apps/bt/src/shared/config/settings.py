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

ENV_RUNTIME_ENV_FILE = "TRADING25_ENV_FILE"


def _runtime_env_file_path() -> Path | None:
    raw_path = os.environ.get(ENV_RUNTIME_ENV_FILE, "").strip()
    if not raw_path:
        return None
    return Path(raw_path).expanduser()


def _load_runtime_env_file() -> Path | None:
    env_file = _runtime_env_file_path()
    if env_file is None:
        return None
    if not env_file.exists():
        raise RuntimeError(f"{ENV_RUNTIME_ENV_FILE} points to a missing file: {env_file}")
    load_dotenv(env_file, override=False)
    return env_file


def _default_data_dir() -> str:
    """XDG準拠のデフォルトデータディレクトリ (TRADING25_DATA_DIR と同じロジック)"""
    env = os.environ.get("TRADING25_DATA_DIR")
    if env:
        return env
    return str(Path.home() / ".local" / "share" / "trading25")


class Settings(BaseModel):
    """アプリケーション設定"""

    bt_api_url: str = Field(default="http://localhost:3002", alias="BT_API_URL")
    api_timeout: float = Field(default=30.0, alias="API_TIMEOUT")
    log_level: str = Field(default="WARNING", alias="LOG_LEVEL")
    backtest_job_timeout_seconds: int = Field(
        default=3600,
        alias="BT_BACKTEST_JOB_TIMEOUT_SECONDS",
    )
    optimization_job_timeout_seconds: int = Field(
        default=3600,
        alias="BT_OPTIMIZATION_JOB_TIMEOUT_SECONDS",
    )
    lab_job_timeout_seconds: int = Field(
        default=3600,
        alias="BT_LAB_JOB_TIMEOUT_SECONDS",
    )

    # JQuants API
    jquants_api_key: str = Field(default="", alias="JQUANTS_API_KEY")
    jquants_plan: str = Field(default="free", alias="JQUANTS_PLAN")

    # moomoo OpenD read-only quote API
    moomoo_opend_enabled: bool = Field(default=True, alias="MOOMOO_OPEND_ENABLED")
    moomoo_opend_host: str = Field(default="127.0.0.1", alias="MOOMOO_OPEND_HOST")
    moomoo_opend_port: int = Field(default=11111, ge=1, le=65535, alias="MOOMOO_OPEND_PORT")
    moomoo_opend_is_encrypt: bool = Field(default=False, alias="MOOMOO_OPEND_IS_ENCRYPT")
    moomoo_opend_max_history_rows: int = Field(default=5000, ge=1, alias="MOOMOO_OPEND_MAX_HISTORY_ROWS")

    # Deprecated alias (legacy name): now points to DuckDB time-series file.
    market_db_path: str = Field(default="", alias="MARKET_DB_PATH")

    # market snapshot resolver root.
    # The mutable latest pointer is {MARKET_TIMESERIES_DIR}/market.duckdb.
    market_timeseries_dir: str = Field(default="", alias="MARKET_TIMESERIES_DIR")

    # portfolio.db (Phase 3C)
    portfolio_db_path: str = Field(default="", alias="PORTFOLIO_DB_PATH")

    # dataset snapshot resolver root.
    # Immutable snapshots live under {DATASET_BASE_PATH}/{snapshot}/.
    dataset_base_path: str = Field(default="", alias="DATASET_BASE_PATH")

    model_config = {"populate_by_name": True}

    def model_post_init(self, __context: Any) -> None:
        """環境変数未設定時にXDGデフォルトパスを自動設定"""
        data_dir = _default_data_dir()
        if not self.market_timeseries_dir:
            self.market_timeseries_dir = str(Path(data_dir) / "market-timeseries")
        if not self.market_db_path:
            self.market_db_path = str(Path(self.market_timeseries_dir) / "market.duckdb")
        if not self.portfolio_db_path:
            self.portfolio_db_path = str(Path(data_dir) / "portfolio.db")
        if not self.dataset_base_path:
            self.dataset_base_path = str(Path(data_dir) / "datasets")


@lru_cache
def get_settings() -> Settings:
    """キャッシュされた設定を取得"""
    _load_runtime_env_file()
    return Settings.model_validate(dict(os.environ))


def reload_settings() -> Settings:
    """環境変数の再読み込み"""
    get_settings.cache_clear()
    return get_settings()
