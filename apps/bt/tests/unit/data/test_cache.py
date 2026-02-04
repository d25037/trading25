"""
DataCache のユニットテスト
"""

import pandas as pd

from src.data.loaders.cache import DataCache


class TestDataCache:
    """DataCache のテスト"""

    def setup_method(self) -> None:
        """各テスト前にキャッシュをリセット"""
        DataCache.disable()

    def teardown_method(self) -> None:
        """各テスト後にキャッシュをクリーンアップ"""
        DataCache.disable()

    def test_singleton_instance(self) -> None:
        """シングルトンインスタンスが同一であることを確認"""
        instance1 = DataCache.get_instance()
        instance2 = DataCache.get_instance()
        assert instance1 is instance2

    def test_disabled_by_default(self) -> None:
        """デフォルトで無効であることを確認"""
        cache = DataCache.get_instance()
        assert cache.is_enabled() is False

    def test_enable_disable(self) -> None:
        """有効化・無効化が正しく動作することを確認"""
        cache = DataCache.enable()
        assert cache.is_enabled() is True

        DataCache.disable()
        assert cache.is_enabled() is False

    def test_set_get_when_enabled(self) -> None:
        """有効時にset/getが正しく動作することを確認"""
        cache = DataCache.enable()

        # テストデータ
        df = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
        key = "test:key:1"

        # 保存
        cache.set(key, df)

        # 取得
        result = cache.get(key)
        assert result is not None
        pd.testing.assert_frame_equal(result, df)

    def test_get_returns_copy(self) -> None:
        """getが元データのコピーを返すことを確認"""
        cache = DataCache.enable()

        df = pd.DataFrame({"A": [1, 2, 3]})
        key = "test:key:copy"

        cache.set(key, df)
        result = cache.get(key)

        # 取得したデータを変更
        assert result is not None
        result["A"] = [10, 20, 30]

        # 元データは変更されていない
        original = cache.get(key)
        assert original is not None
        assert original["A"].tolist() == [1, 2, 3]

    def test_set_stores_copy(self) -> None:
        """setが元データのコピーを保存することを確認"""
        cache = DataCache.enable()

        df = pd.DataFrame({"A": [1, 2, 3]})
        key = "test:key:store"

        cache.set(key, df)

        # 元データを変更
        df["A"] = [10, 20, 30]

        # キャッシュは変更されていない
        result = cache.get(key)
        assert result is not None
        assert result["A"].tolist() == [1, 2, 3]

    def test_get_returns_none_when_disabled(self) -> None:
        """無効時にgetがNoneを返すことを確認"""
        cache = DataCache.get_instance()

        # 有効化してデータを保存
        DataCache.enable()
        df = pd.DataFrame({"A": [1]})
        cache.set("test:key", df)

        # 無効化後はNone
        DataCache.disable()
        result = cache.get("test:key")
        assert result is None

    def test_set_does_nothing_when_disabled(self) -> None:
        """無効時にsetが何もしないことを確認"""
        cache = DataCache.get_instance()

        # 無効状態でset
        df = pd.DataFrame({"A": [1]})
        cache.set("test:key", df)

        # 有効化してもデータは存在しない
        DataCache.enable()
        result = cache.get("test:key")
        assert result is None

    def test_cache_miss(self) -> None:
        """キャッシュミスでNoneが返ることを確認"""
        cache = DataCache.enable()
        result = cache.get("nonexistent:key")
        assert result is None

    def test_get_stats(self) -> None:
        """統計情報が正しく取得できることを確認"""
        cache = DataCache.enable()

        # 初期状態
        stats = cache.get_stats()
        assert stats["size"] == 0
        assert stats["hits"] == 0
        assert stats["misses"] == 0

        # データ追加
        df = pd.DataFrame({"A": [1]})
        cache.set("test:key", df)

        # ヒット
        cache.get("test:key")
        cache.get("test:key")

        # ミス
        cache.get("nonexistent:key")

        stats = cache.get_stats()
        assert stats["size"] == 1
        assert stats["hits"] == 2
        assert stats["misses"] == 1

    def test_clear(self) -> None:
        """clearがキャッシュをクリアすることを確認"""
        cache = DataCache.enable()

        # データ追加
        df = pd.DataFrame({"A": [1]})
        cache.set("test:key", df)

        # ヒットカウント増加
        cache.get("test:key")

        # クリア
        cache.clear()

        stats = cache.get_stats()
        assert stats["size"] == 0
        assert stats["hits"] == 0
        assert stats["misses"] == 0

        # データも消えている
        result = cache.get("test:key")
        assert result is None

    def test_enable_clears_previous_cache(self) -> None:
        """enable()が前回のキャッシュをクリアすることを確認"""
        cache = DataCache.enable()

        # データ追加
        df = pd.DataFrame({"A": [1]})
        cache.set("test:key", df)

        # 再有効化
        cache = DataCache.enable()

        # 前回のデータは消えている
        result = cache.get("test:key")
        assert result is None

        stats = cache.get_stats()
        assert stats["size"] == 0

    def test_disable_logs_stats(self) -> None:
        """disable()が統計をログ出力することを確認（動作確認のみ）"""
        cache = DataCache.enable()

        df = pd.DataFrame({"A": [1]})
        cache.set("test:key", df)
        cache.get("test:key")
        cache.get("nonexistent")

        # エラーなく実行できることを確認
        DataCache.disable()

    def test_multiple_keys(self) -> None:
        """複数キーが正しく管理されることを確認"""
        cache = DataCache.enable()

        df1 = pd.DataFrame({"A": [1]})
        df2 = pd.DataFrame({"B": [2]})
        df3 = pd.DataFrame({"C": [3]})

        cache.set("key1", df1)
        cache.set("key2", df2)
        cache.set("key3", df3)

        result1 = cache.get("key1")
        result2 = cache.get("key2")
        result3 = cache.get("key3")

        assert result1 is not None
        assert result2 is not None
        assert result3 is not None

        pd.testing.assert_frame_equal(result1, df1)
        pd.testing.assert_frame_equal(result2, df2)
        pd.testing.assert_frame_equal(result3, df3)

        stats = cache.get_stats()
        assert stats["size"] == 3

    def test_overwrite_key(self) -> None:
        """同一キーへの上書きが正しく動作することを確認"""
        cache = DataCache.enable()

        df1 = pd.DataFrame({"A": [1]})
        df2 = pd.DataFrame({"A": [2]})

        cache.set("key", df1)
        cache.set("key", df2)

        result = cache.get("key")
        assert result is not None
        pd.testing.assert_frame_equal(result, df2)

        stats = cache.get_stats()
        assert stats["size"] == 1
