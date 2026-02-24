"""
データキャッシュモジュール

パラメータ最適化時のAPI呼び出し削減のためのインメモリキャッシュ
"""

from __future__ import annotations

import functools
import threading
from typing import Any, Callable, ClassVar

import pandas as pd
from loguru import logger


class DataCache:
    """
    インメモリデータキャッシュ（シングルトン）

    パラメータ最適化時に同一データの再取得を防ぐためのキャッシュ機構。
    明示的に有効化/無効化することで、通常のバックテストには影響しない。

    使用例:
        # 最適化開始時
        cache = DataCache.enable()

        # データ取得時（loaders内部で自動利用）
        if cache.is_enabled():
            cached = cache.get(key)
            if cached is not None:
                return cached

        # 最適化終了時
        DataCache.disable()
    """

    _instance: ClassVar[DataCache | None] = None
    _lock: ClassVar[threading.Lock] = threading.Lock()

    def __init__(self) -> None:
        """初期化（直接呼び出し禁止、get_instance()を使用）"""
        self._cache: dict[str, pd.DataFrame] = {}
        self._enabled: bool = False
        self._hit_count: int = 0
        self._miss_count: int = 0

    @classmethod
    def get_instance(cls) -> DataCache:
        """
        シングルトンインスタンスを取得

        Returns:
            DataCacheインスタンス
        """
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    @classmethod
    def enable(cls) -> DataCache:
        """
        キャッシュを有効化

        最適化セッション開始時に呼び出す。
        既にキャッシュが有効な場合は既存キャッシュをクリアして再初期化。

        Returns:
            DataCacheインスタンス
        """
        instance = cls.get_instance()
        with cls._lock:
            instance._cache.clear()
            instance._enabled = True
            instance._hit_count = 0
            instance._miss_count = 0
        logger.debug("DataCache enabled")
        return instance

    @classmethod
    def disable(cls) -> None:
        """
        キャッシュを無効化してクリア

        最適化セッション終了時に呼び出す。
        メモリ解放とキャッシュ統計のログ出力を行う。
        """
        instance = cls.get_instance()
        with cls._lock:
            cache_size = len(instance._cache)
            hit_count = instance._hit_count
            miss_count = instance._miss_count
            instance._cache.clear()
            instance._enabled = False
        logger.debug(
            f"DataCache disabled: {cache_size} entries cleared, "
            f"hits={hit_count}, misses={miss_count}"
        )

    def is_enabled(self) -> bool:
        """
        キャッシュが有効かどうかを確認

        Returns:
            True if キャッシュ有効
        """
        return self._enabled

    def get(self, key: str) -> pd.DataFrame | None:
        """
        キャッシュからデータを取得

        Args:
            key: キャッシュキー（例: "dataset:stock_code:start:end:timeframe"）

        Returns:
            キャッシュされたDataFrame、または None（キャッシュミス時）
        """
        if not self._enabled:
            return None

        with self._lock:
            if key in self._cache:
                self._hit_count += 1
                logger.trace(f"Cache hit: {key}")
                return self._cache[key].copy()
            else:
                self._miss_count += 1
                logger.trace(f"Cache miss: {key}")
                return None

    def set(self, key: str, data: pd.DataFrame) -> None:
        """
        データをキャッシュに保存

        Args:
            key: キャッシュキー
            data: 保存するDataFrame
        """
        if not self._enabled:
            return

        with self._lock:
            self._cache[key] = data.copy()
        logger.trace(f"Cache set: {key}")

    def get_stats(self) -> dict[str, int]:
        """
        キャッシュ統計を取得

        Returns:
            統計情報の辞書 {size, hits, misses}
        """
        return {
            "size": len(self._cache),
            "hits": self._hit_count,
            "misses": self._miss_count,
        }

    def clear(self) -> None:
        """キャッシュをクリア（有効状態は維持）"""
        with self._lock:
            self._cache.clear()
            self._hit_count = 0
            self._miss_count = 0
        logger.debug("DataCache cleared")


def cached_loader(
    key_template: str,
) -> Callable[..., Callable[..., pd.DataFrame]]:
    """DataFrameを返すloader関数にキャッシュを適用するデコレータ

    キャッシュキーは key_template に関数の引数値を format() で埋め込んで生成する。
    DataCache が無効な場合はキャッシュなしで素通しする。

    Args:
        key_template: キャッシュキーテンプレート。
            関数の引数名をプレースホルダーとして使用する。
            例: "stock:{dataset}:{stock_code}:{start_date}:{end_date}:{timeframe}"

    Usage:
        @cached_loader("topix:{dataset}:{start_date}:{end_date}")
        def load_topix_data(dataset, start_date=None, end_date=None):
            ...
    """

    def decorator(func: Callable[..., pd.DataFrame]) -> Callable[..., pd.DataFrame]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> pd.DataFrame:
            import inspect

            sig = inspect.signature(func)
            bound = sig.bind(*args, **kwargs)
            bound.apply_defaults()

            cache = DataCache.get_instance()
            cache_key = key_template.format(**bound.arguments)

            if cache.is_enabled():
                cached = cache.get(cache_key)
                if cached is not None:
                    return cached

            result = func(*args, **kwargs)

            if cache.is_enabled():
                cache.set(cache_key, result)

            return result

        return wrapper

    return decorator
