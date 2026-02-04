"""
Centralized settings

環境変数とデフォルト値の単一ソースを提供する。
"""

from __future__ import annotations

import os
from functools import lru_cache

from pydantic import BaseModel, Field


class Settings(BaseModel):
    """アプリケーション設定"""

    api_base_url: str = Field(
        default="http://localhost:3001", alias="API_BASE_URL"
    )
    api_timeout: float = Field(default=30.0, alias="API_TIMEOUT")
    log_level: str = Field(default="WARNING", alias="LOG_LEVEL")

    model_config = {"populate_by_name": True}


@lru_cache
def get_settings() -> Settings:
    """キャッシュされた設定を取得"""
    return Settings.model_validate(dict(os.environ))


def reload_settings() -> Settings:
    """環境変数の再読み込み"""
    get_settings.cache_clear()
    return get_settings()
