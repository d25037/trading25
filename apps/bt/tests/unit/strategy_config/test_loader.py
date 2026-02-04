"""
ConfigLoader のテスト

shared_config マージ機能のテスト
"""

import pytest
from src.strategy_config.loader import ConfigLoader, StrategyMetadata


@pytest.fixture
def config_loader():
    """ConfigLoader フィクスチャ"""
    loader = ConfigLoader()
    # デフォルト設定をモックする（実際の default.yaml に依存しない）
    loader.default_config = {
        "parameters": {
            "shared_config": {
                "initial_cash": 10000000,
                "fees": 0.001,
                "stock_codes": ["all"],
                "start_date": "",
                "end_date": "",
                "dataset": "primeExTopix500",
                "kelly_fraction": 2.0,
                "min_allocation": 0.01,
                "max_allocation": 0.5,
            }
        }
    }
    return loader


def test_merge_shared_config_no_override(config_loader):
    """
    戦略YAMLに shared_config がない場合、デフォルト設定をそのまま返すことを確認
    """
    strategy_config = {
        "entry_filter_params": {"period_breakout": {"enabled": True}},
        "exit_trigger_params": {"atr_support_break": {"enabled": True}},
    }

    merged = config_loader.merge_shared_config(strategy_config)

    # デフォルト設定がそのまま返される
    assert merged["initial_cash"] == 10000000
    assert merged["fees"] == 0.001
    assert merged["dataset"] == "primeExTopix500"
    assert merged["kelly_fraction"] == 2.0


def test_merge_shared_config_partial_override(config_loader):
    """
    戦略YAMLで一部のパラメータのみ override する場合を確認
    """
    strategy_config = {
        "shared_config": {
            "initial_cash": 20000000,  # override
            "dataset": "topix100-A",  # override
        },
        "entry_filter_params": {"period_breakout": {"enabled": True}},
    }

    merged = config_loader.merge_shared_config(strategy_config)

    # override されたパラメータ
    assert merged["initial_cash"] == 20000000
    assert merged["dataset"] == "topix100-A"

    # override されていないパラメータはデフォルト値を維持
    assert merged["fees"] == 0.001
    assert merged["kelly_fraction"] == 2.0
    assert merged["min_allocation"] == 0.01
    assert merged["max_allocation"] == 0.5


def test_merge_shared_config_deep_merge_nested(config_loader):
    """
    shared_configのネスト辞書がディープマージされることを確認
    """
    config_loader.default_config["parameters"]["shared_config"]["parameter_optimization"] = {
        "enabled": False,
        "method": "grid_search",
        "n_trials": 100,
        "n_jobs": -1,
        "scoring_weights": {
            "sharpe_ratio": 0.5,
            "calmar_ratio": 0.3,
            "total_return": 0.2,
        },
    }

    strategy_config = {
        "shared_config": {
            "parameter_optimization": {
                "scoring_weights": {"total_return": 0.4},
            }
        }
    }

    merged = config_loader.merge_shared_config(strategy_config)

    # 既存キーは維持される
    assert merged["parameter_optimization"]["enabled"] is False
    assert merged["parameter_optimization"]["method"] == "grid_search"
    assert merged["parameter_optimization"]["n_trials"] == 100
    assert merged["parameter_optimization"]["n_jobs"] == -1

    # scoring_weightsは部分上書きされる
    assert merged["parameter_optimization"]["scoring_weights"]["sharpe_ratio"] == 0.5
    assert merged["parameter_optimization"]["scoring_weights"]["calmar_ratio"] == 0.3
    assert merged["parameter_optimization"]["scoring_weights"]["total_return"] == 0.4


def test_merge_shared_config_full_override(config_loader):
    """
    戦略YAMLで複数のパラメータを override する場合を確認
    """
    strategy_config = {
        "shared_config": {
            "initial_cash": 50000000,
            "fees": 0.0005,
            "dataset": "custom",
            "kelly_fraction": 1.0,
            "min_allocation": 0.02,
            "max_allocation": 0.3,
        }
    }

    merged = config_loader.merge_shared_config(strategy_config)

    # 全てのパラメータが override されている
    assert merged["initial_cash"] == 50000000
    assert merged["fees"] == 0.0005
    assert merged["dataset"] == "custom"
    assert merged["kelly_fraction"] == 1.0
    assert merged["min_allocation"] == 0.02
    assert merged["max_allocation"] == 0.3

    # override されていないパラメータ
    assert merged["stock_codes"] == ["all"]
    assert merged["start_date"] == ""
    assert merged["end_date"] == ""


def test_merge_shared_config_empty_strategy_config(config_loader):
    """
    戦略設定が空の辞書の場合を確認
    """
    strategy_config = {}

    merged = config_loader.merge_shared_config(strategy_config)

    # デフォルト設定がそのまま返される
    assert merged["initial_cash"] == 10000000
    assert merged["fees"] == 0.001
    assert merged["dataset"] == "primeExTopix500"


def test_merge_shared_config_invalid_type(config_loader):
    """
    戦略の shared_config が辞書でない場合を確認（エラーハンドリング）
    """
    strategy_config = {
        "shared_config": "invalid_type",  # 辞書ではない
    }

    merged = config_loader.merge_shared_config(strategy_config)

    # 無効な shared_config は無視され、デフォルト設定が返される
    assert merged["initial_cash"] == 10000000
    assert merged["fees"] == 0.001


def test_merge_shared_config_default_invalid_type():
    """
    デフォルト設定の shared_config が辞書でない場合を確認
    """
    loader = ConfigLoader()
    loader.default_config = {
        "parameters": {
            "shared_config": "invalid_type"  # 辞書ではない
        }
    }

    strategy_config = {
        "shared_config": {
            "initial_cash": 20000000,
        }
    }

    merged = loader.merge_shared_config(strategy_config)

    # デフォルトが無効な場合は空の辞書から開始し、戦略設定が追加される
    assert merged["initial_cash"] == 20000000
    assert "fees" not in merged  # デフォルトが無効なので存在しない


# ========== is_editable_category テスト ==========


def test_is_editable_category_experimental():
    """experimental カテゴリは編集可能"""
    loader = ConfigLoader()
    assert loader.is_editable_category("experimental/my_strategy") is True


def test_is_editable_category_production():
    """production カテゴリは編集不可"""
    loader = ConfigLoader()
    assert loader.is_editable_category("production/range_break_v5") is False


def test_is_editable_category_reference():
    """reference カテゴリは編集不可"""
    loader = ConfigLoader()
    assert loader.is_editable_category("reference/strategy_template") is False


def test_is_editable_category_legacy():
    """legacy カテゴリは編集不可"""
    loader = ConfigLoader()
    assert loader.is_editable_category("legacy/old_strategy") is False


# ========== save_strategy_config テスト ==========


def test_save_strategy_config_experimental(tmp_path):
    """experimental カテゴリへの保存が成功する"""
    # テスト用の config ディレクトリを作成
    config_dir = tmp_path / "config"
    strategies_dir = config_dir / "strategies" / "experimental"
    strategies_dir.mkdir(parents=True)

    loader = ConfigLoader(config_dir=str(config_dir))

    config = {
        "entry_filter_params": {"volume": {"enabled": True, "threshold": 1.5}},
        "exit_trigger_params": {"volume": {"enabled": False}},
    }

    path = loader.save_strategy_config("test_strategy", config, force=True)

    assert path.exists()
    assert path.name == "test_strategy.yaml"
    assert "experimental" in str(path)


def test_save_strategy_config_production_raises():
    """production カテゴリへの保存は PermissionError"""
    loader = ConfigLoader()

    config = {"entry_filter_params": {}}

    with pytest.raises(PermissionError, match="experimental のみ保存可能"):
        loader.save_strategy_config("production/my_strategy", config)


def test_save_strategy_config_reference_raises():
    """reference カテゴリへの保存は PermissionError"""
    loader = ConfigLoader()

    config = {"entry_filter_params": {}}

    with pytest.raises(PermissionError, match="experimental のみ保存可能"):
        loader.save_strategy_config("reference/my_strategy", config)


# ========== duplicate_strategy テスト ==========


def test_duplicate_strategy_success(tmp_path):
    """戦略の複製が成功する"""
    # テスト用の config ディレクトリを作成
    config_dir = tmp_path / "config"
    experimental_dir = config_dir / "strategies" / "experimental"
    experimental_dir.mkdir(parents=True)

    # 複製元ファイルを作成
    source_file = experimental_dir / "source_strategy.yaml"
    source_file.write_text(
        """
entry_filter_params:
  volume:
    enabled: true
    threshold: 1.5
exit_trigger_params:
  volume:
    enabled: false
""",
        encoding="utf-8",
    )

    loader = ConfigLoader(config_dir=str(config_dir))

    path = loader.duplicate_strategy("experimental/source_strategy", "new_strategy")

    assert path.exists()
    assert path.name == "new_strategy.yaml"

    # 複製されたファイルの内容を確認
    import yaml

    with open(path, encoding="utf-8") as f:
        duplicated_config = yaml.safe_load(f)

    assert duplicated_config["entry_filter_params"]["volume"]["enabled"] is True
    assert duplicated_config["entry_filter_params"]["volume"]["threshold"] == 1.5


def test_duplicate_strategy_target_exists_raises(tmp_path):
    """複製先が既に存在する場合は FileExistsError"""
    # テスト用の config ディレクトリを作成
    config_dir = tmp_path / "config"
    experimental_dir = config_dir / "strategies" / "experimental"
    experimental_dir.mkdir(parents=True)

    # 複製元と複製先の両方を作成
    (experimental_dir / "source.yaml").write_text("entry_filter_params: {}", encoding="utf-8")
    (experimental_dir / "existing.yaml").write_text("entry_filter_params: {}", encoding="utf-8")

    loader = ConfigLoader(config_dir=str(config_dir))

    with pytest.raises(FileExistsError, match="既に存在します"):
        loader.duplicate_strategy("experimental/source", "existing")


def test_duplicate_strategy_with_category_raises():
    """複製先にカテゴリを含めると ValueError"""
    loader = ConfigLoader()

    with pytest.raises(ValueError, match="カテゴリを含めないでください"):
        loader.duplicate_strategy("reference/strategy_template", "experimental/new_strategy")


# ========== delete_strategy テスト ==========


def test_delete_strategy_experimental(tmp_path):
    """experimental カテゴリの戦略を削除できる"""
    # テスト用の config ディレクトリを作成
    config_dir = tmp_path / "config"
    experimental_dir = config_dir / "strategies" / "experimental"
    experimental_dir.mkdir(parents=True)

    # 削除対象ファイルを作成
    target_file = experimental_dir / "to_delete.yaml"
    target_file.write_text("entry_filter_params: {}", encoding="utf-8")

    loader = ConfigLoader(config_dir=str(config_dir))

    result = loader.delete_strategy("experimental/to_delete")

    assert result is True
    assert not target_file.exists()


def test_delete_strategy_production_raises(tmp_path):
    """production カテゴリの戦略は削除できない"""
    # テスト用の config ディレクトリを作成
    config_dir = tmp_path / "config"
    production_dir = config_dir / "strategies" / "production"
    production_dir.mkdir(parents=True)

    # 削除対象ファイルを作成
    target_file = production_dir / "protected.yaml"
    target_file.write_text("entry_filter_params: {}", encoding="utf-8")

    loader = ConfigLoader(config_dir=str(config_dir))

    with pytest.raises(PermissionError, match="experimental のみ削除可能"):
        loader.delete_strategy("production/protected")


# ========== get_strategy_metadata テスト ==========


def test_get_strategy_metadata_returns_list(tmp_path):
    """get_strategy_metadata が StrategyMetadata のリストを返す"""
    config_dir = tmp_path / "config"
    experimental_dir = config_dir / "strategies" / "experimental"
    experimental_dir.mkdir(parents=True)

    # テスト用ファイルを作成
    (experimental_dir / "strategy_a.yaml").write_text("entry_filter_params: {}", encoding="utf-8")
    (experimental_dir / "strategy_b.yaml").write_text("entry_filter_params: {}", encoding="utf-8")

    loader = ConfigLoader(config_dir=str(config_dir))
    metadata_list = loader.get_strategy_metadata()

    assert len(metadata_list) == 2
    assert all(isinstance(m, StrategyMetadata) for m in metadata_list)


def test_get_strategy_metadata_contains_correct_info(tmp_path):
    """get_strategy_metadata が正しい情報を含む"""
    config_dir = tmp_path / "config"
    experimental_dir = config_dir / "strategies" / "experimental"
    production_dir = config_dir / "strategies" / "production"
    experimental_dir.mkdir(parents=True)
    production_dir.mkdir(parents=True)

    (experimental_dir / "my_strategy.yaml").write_text("entry_filter_params: {}", encoding="utf-8")
    (production_dir / "prod_strategy.yaml").write_text("entry_filter_params: {}", encoding="utf-8")

    loader = ConfigLoader(config_dir=str(config_dir))
    metadata_list = loader.get_strategy_metadata()

    # 名前とカテゴリの確認
    names = [m.name for m in metadata_list]
    categories = [m.category for m in metadata_list]

    assert "experimental/my_strategy" in names
    assert "production/prod_strategy" in names
    assert "experimental" in categories
    assert "production" in categories

    # mtimeがdatetimeであることを確認
    from datetime import datetime
    for m in metadata_list:
        assert isinstance(m.mtime, datetime)


def test_get_strategy_metadata_empty_directory(tmp_path):
    """戦略ディレクトリが空の場合は空リストを返す"""
    config_dir = tmp_path / "config"
    strategies_dir = config_dir / "strategies"
    strategies_dir.mkdir(parents=True)

    loader = ConfigLoader(config_dir=str(config_dir))
    metadata_list = loader.get_strategy_metadata()

    assert metadata_list == []


def test_get_strategy_metadata_no_strategies_directory(tmp_path):
    """strategies ディレクトリが存在しない場合は空リストを返す"""
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True)

    loader = ConfigLoader(config_dir=str(config_dir))
    metadata_list = loader.get_strategy_metadata()

    assert metadata_list == []
