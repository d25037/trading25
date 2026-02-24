"""
Tests for src/paths/resolver.py
"""

import os
from pathlib import Path
from unittest.mock import patch


from src.shared.paths import (
    get_data_dir,
    get_strategies_dir,
    get_backtest_results_dir,
    get_backtest_attribution_dir,
    get_optimization_results_dir,
    get_optimization_grid_dir,
    get_cache_dir,
    find_strategy_path,
    ensure_data_dirs,
    DEFAULT_DATA_DIR,
    EXTERNAL_CATEGORIES,
    PROJECT_CATEGORIES,
)


class TestGetDataDir:
    """get_data_dir関数のテスト"""

    def test_default_data_dir(self):
        """デフォルトデータディレクトリのテスト"""
        with patch.dict(os.environ, {}, clear=True):
            result = get_data_dir()
            assert result == DEFAULT_DATA_DIR

    def test_custom_data_dir_from_env(self, tmp_path: Path):
        """環境変数からカスタムデータディレクトリを取得"""
        custom_dir = str(tmp_path / "custom_data")
        with patch.dict(os.environ, {"TRADING25_DATA_DIR": custom_dir}):
            result = get_data_dir()
            assert result == Path(custom_dir)


class TestGetStrategiesDir:
    """get_strategies_dir関数のテスト"""

    def test_experimental_category_returns_external(self):
        """experimentalカテゴリは外部ディレクトリを返す"""
        result = get_strategies_dir("experimental")
        assert "experimental" in str(result)
        # デフォルトでは外部ディレクトリ
        assert ".local/share/trading25" in str(result) or "TRADING25" in str(result)

    def test_production_category_returns_project(self):
        """productionカテゴリはプロジェクト内を返す"""
        result = get_strategies_dir("production")
        assert result == Path("config/strategies/production")

    def test_reference_category_returns_project(self):
        """referenceカテゴリはプロジェクト内を返す"""
        result = get_strategies_dir("reference")
        assert result == Path("config/strategies/reference")

    def test_legacy_category_returns_project(self):
        """legacyカテゴリはプロジェクト内を返す"""
        result = get_strategies_dir("legacy")
        assert result == Path("config/strategies/legacy")

    def test_none_category_returns_base(self):
        """カテゴリなしはベースディレクトリを返す"""
        result = get_strategies_dir(None)
        assert result == Path("config/strategies")


class TestGetBacktestResultsDir:
    """get_backtest_results_dir関数のテスト"""

    def test_default_backtest_dir(self):
        """デフォルトバックテスト結果ディレクトリのテスト"""
        result = get_backtest_results_dir()
        assert "backtest" in str(result)
        assert "results" in str(result)

    def test_backtest_dir_with_strategy(self):
        """戦略名付きバックテスト結果ディレクトリのテスト"""
        result = get_backtest_results_dir("test_strategy")
        assert "test_strategy" in str(result)


class TestGetBacktestAttributionDir:
    """get_backtest_attribution_dir関数のテスト"""

    def test_default_attribution_dir(self):
        """デフォルト寄与分析ディレクトリのテスト"""
        result = get_backtest_attribution_dir()
        assert "backtest" in str(result)
        assert "attribution" in str(result)

    def test_attribution_dir_with_strategy(self):
        """戦略名付き寄与分析ディレクトリのテスト"""
        result = get_backtest_attribution_dir("test_strategy")
        assert "test_strategy" in str(result)


class TestGetOptimizationResultsDir:
    """get_optimization_results_dir関数のテスト"""

    def test_default_optimization_dir(self):
        """デフォルト最適化結果ディレクトリのテスト"""
        result = get_optimization_results_dir()
        assert "optimization" in str(result)

    def test_optimization_dir_with_strategy(self):
        """戦略名付き最適化結果ディレクトリのテスト"""
        result = get_optimization_results_dir("test_strategy")
        assert "test_strategy" in str(result)


class TestGetOptimizationGridDir:
    """get_optimization_grid_dir関数のテスト"""

    def test_optimization_grid_dir(self):
        """最適化グリッドディレクトリのテスト"""
        result = get_optimization_grid_dir()
        assert "optimization" in str(result)


class TestGetCacheDir:
    """get_cache_dir関数のテスト"""

    def test_cache_dir(self):
        """キャッシュディレクトリのテスト"""
        result = get_cache_dir()
        assert "cache" in str(result)


class TestFindStrategyPath:
    """find_strategy_path関数のテスト"""

    def test_find_existing_reference_strategy(self):
        """既存のreference戦略を検索"""
        # reference/sma_crossが存在する前提
        result = find_strategy_path("reference/sma_cross")
        if result:
            assert result.exists()
            assert "sma_cross" in str(result)

    def test_find_nonexistent_strategy(self):
        """存在しない戦略の検索"""
        result = find_strategy_path("nonexistent/strategy_xyz_123")
        assert result is None

    def test_find_strategy_without_category(self):
        """カテゴリなしで戦略を検索"""
        # sma_crossが存在する前提（reference配下）
        result = find_strategy_path("sma_cross")
        if result:
            assert result.exists()


class TestEnsureDataDirs:
    """ensure_data_dirs関数のテスト"""

    def test_ensure_data_dirs_creates_directories(self, tmp_path: Path):
        """ディレクトリが作成されることを確認"""
        custom_dir = str(tmp_path / "test_trading25")
        with patch.dict(os.environ, {"TRADING25_DATA_DIR": custom_dir}):
            ensure_data_dirs()

            # 作成されたディレクトリを確認
            data_dir = Path(custom_dir)
            assert data_dir.exists()
            assert (data_dir / "strategies" / "experimental").exists()
            assert (data_dir / "backtest" / "results").exists()
            assert (data_dir / "backtest" / "attribution").exists()
            assert (data_dir / "backtest" / "optimization").exists()
            assert (data_dir / "optimization").exists()
            assert (data_dir / "cache").exists()


class TestResolveStrategyName:
    """_resolve_strategy_name関数のテスト"""

    def test_simple_name(self, tmp_path: Path):
        """単純な戦略名の解決"""
        from src.shared.paths.resolver import _resolve_strategy_name

        base = tmp_path / "strategies" / "production"
        yaml_file = base / "my_strategy.yaml"
        assert _resolve_strategy_name(yaml_file, base, "production") == "production/my_strategy"

    def test_nested_name(self, tmp_path: Path):
        """ネストされた戦略名の解決"""
        from src.shared.paths.resolver import _resolve_strategy_name

        base = tmp_path / "strategies" / "experimental"
        yaml_file = base / "auto" / "evolved_v1.yaml"
        assert _resolve_strategy_name(yaml_file, base, "experimental") == "experimental/auto/evolved_v1"

    def test_unrelated_path_fallback(self, tmp_path: Path):
        """relative_to失敗時のフォールバック"""
        from src.shared.paths.resolver import _resolve_strategy_name

        base = tmp_path / "strategies" / "production"
        unrelated = Path("/completely/different/path/strat.yaml")
        assert _resolve_strategy_name(unrelated, base, "production") == "production/strat"


class TestCollectStrategyNames:
    """_collect_strategy_names関数のテスト"""

    def test_collect_from_directory(self, tmp_path: Path):
        """ディレクトリからの戦略名収集"""
        from src.shared.paths.resolver import _collect_strategy_names

        cat_dir = tmp_path / "production"
        cat_dir.mkdir()
        (cat_dir / "alpha.yaml").write_text("test: true")
        (cat_dir / "beta.yaml").write_text("test: true")

        result = _collect_strategy_names(cat_dir, "production")
        assert sorted(result) == ["production/alpha", "production/beta"]

    def test_collect_from_nonexistent_directory(self, tmp_path: Path):
        """存在しないディレクトリからの収集は空リスト"""
        from src.shared.paths.resolver import _collect_strategy_names

        result = _collect_strategy_names(tmp_path / "nonexistent", "production")
        assert result == []

    def test_collect_nested_subdirectories(self, tmp_path: Path):
        """サブディレクトリ内のYAMLも収集"""
        from src.shared.paths.resolver import _collect_strategy_names

        cat_dir = tmp_path / "experimental"
        (cat_dir / "auto").mkdir(parents=True)
        (cat_dir / "top.yaml").write_text("test: true")
        (cat_dir / "auto" / "nested.yaml").write_text("test: true")

        result = _collect_strategy_names(cat_dir, "experimental")
        assert "experimental/top" in result
        assert "experimental/auto/nested" in result


class TestMergeInto:
    """_merge_into関数のテスト"""

    def test_merge_new_category(self):
        """新規カテゴリのマージ"""
        from src.shared.paths.resolver import _merge_into

        cat: dict[str, list[str]] = {}
        _merge_into(cat, "prod", ["prod/a", "prod/b"])
        assert cat == {"prod": ["prod/a", "prod/b"]}

    def test_merge_existing_category_deduplicates(self):
        """既存カテゴリへのマージで重複除去"""
        from src.shared.paths.resolver import _merge_into

        cat: dict[str, list[str]] = {"prod": ["prod/a", "prod/b"]}
        _merge_into(cat, "prod", ["prod/b", "prod/c"])
        assert cat == {"prod": ["prod/a", "prod/b", "prod/c"]}

    def test_merge_empty_list_noop(self):
        """空リストのマージは何も変更しない"""
        from src.shared.paths.resolver import _merge_into

        cat: dict[str, list[str]] = {"prod": ["prod/a"]}
        _merge_into(cat, "prod", [])
        assert cat == {"prod": ["prod/a"]}


class TestGetCategorizedStrategies:
    """get_categorized_strategies関数のテスト"""

    def test_with_tmp_project_dir(self, tmp_path: Path):
        """一時ディレクトリでのカテゴリ別取得"""
        from src.shared.paths.resolver import get_categorized_strategies

        strategies_dir = tmp_path / "strategies"
        prod_dir = strategies_dir / "production"
        prod_dir.mkdir(parents=True)
        (prod_dir / "strat_a.yaml").write_text("test: true")
        (prod_dir / "strat_b.yaml").write_text("test: true")

        result = get_categorized_strategies(project_strategies_dir=strategies_dir)
        assert "production" in result
        assert "production/strat_a" in result["production"]
        assert "production/strat_b" in result["production"]

    def test_empty_project_dir(self, tmp_path: Path):
        """空ディレクトリでは外部カテゴリのみ返す可能性"""
        from src.shared.paths.resolver import get_categorized_strategies

        strategies_dir = tmp_path / "strategies"
        strategies_dir.mkdir()
        result = get_categorized_strategies(project_strategies_dir=strategies_dir)
        # プロジェクト内に戦略がなくても外部ディレクトリのものは返る可能性あり
        assert isinstance(result, dict)

    def test_nonexistent_project_dir(self, tmp_path: Path):
        """存在しないディレクトリでもエラーにならない"""
        from src.shared.paths.resolver import get_categorized_strategies

        result = get_categorized_strategies(
            project_strategies_dir=tmp_path / "nonexistent"
        )
        assert isinstance(result, dict)

    def test_root_level_strategies(self, tmp_path: Path):
        """ルート直下のYAMLファイルがrootカテゴリとして取得される"""
        from src.shared.paths.resolver import get_categorized_strategies

        strategies_dir = tmp_path / "strategies"
        strategies_dir.mkdir()
        (strategies_dir / "template.yaml").write_text("test: true")

        result = get_categorized_strategies(project_strategies_dir=strategies_dir)
        assert "root" in result
        assert "template" in result["root"]


class TestGetStrategyMetadataList:
    """get_strategy_metadata_list関数のテスト"""

    def test_metadata_from_project_dir(self, tmp_path: Path):
        """プロジェクトディレクトリからのメタデータ取得"""
        from src.shared.paths.resolver import StrategyMetadata, get_strategy_metadata_list

        strategies_dir = tmp_path / "strategies"
        prod_dir = strategies_dir / "production"
        prod_dir.mkdir(parents=True)
        (prod_dir / "my_strat.yaml").write_text("test: true")

        result = get_strategy_metadata_list(
            project_strategies_dir=strategies_dir, include_external=False
        )
        assert len(result) == 1
        assert isinstance(result[0], StrategyMetadata)
        assert result[0].name == "production/my_strat"
        assert result[0].category == "production"
        assert result[0].path == prod_dir / "my_strat.yaml"

    def test_metadata_empty_dir(self, tmp_path: Path):
        """空ディレクトリからのメタデータ取得"""
        from src.shared.paths.resolver import get_strategy_metadata_list

        strategies_dir = tmp_path / "strategies"
        strategies_dir.mkdir()
        result = get_strategy_metadata_list(
            project_strategies_dir=strategies_dir, include_external=False
        )
        assert result == []

    def test_metadata_nonexistent_dir(self, tmp_path: Path):
        """存在しないディレクトリからのメタデータ取得"""
        from src.shared.paths.resolver import get_strategy_metadata_list

        result = get_strategy_metadata_list(
            project_strategies_dir=tmp_path / "nonexistent", include_external=False
        )
        assert result == []

    def test_metadata_deduplicates(self, tmp_path: Path):
        """重複パスが除外される"""
        from src.shared.paths.resolver import get_strategy_metadata_list

        strategies_dir = tmp_path / "strategies"
        prod_dir = strategies_dir / "production"
        prod_dir.mkdir(parents=True)
        (prod_dir / "strat.yaml").write_text("test: true")

        result = get_strategy_metadata_list(
            project_strategies_dir=strategies_dir, include_external=False
        )
        paths = [m.path for m in result]
        assert len(paths) == len(set(paths))


class TestConstants:
    """定数のテスト"""

    def test_external_categories(self):
        """外部カテゴリの定義"""
        assert "experimental" in EXTERNAL_CATEGORIES

    def test_project_categories(self):
        """プロジェクト内カテゴリの定義"""
        assert "production" in PROJECT_CATEGORIES
        assert "reference" in PROJECT_CATEGORIES
        assert "legacy" in PROJECT_CATEGORIES

    def test_categories_are_mutually_exclusive(self):
        """カテゴリが重複しないことを確認"""
        external_set = set(EXTERNAL_CATEGORIES)
        project_set = set(PROJECT_CATEGORIES)
        assert external_set.isdisjoint(project_set)
