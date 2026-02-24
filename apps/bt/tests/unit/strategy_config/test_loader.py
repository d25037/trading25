"""
ConfigLoader のテスト

shared_config マージ機能のテスト
"""

from pathlib import Path

import pytest
from src.domains.strategy.runtime.loader import ConfigLoader, StrategyMetadata


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


# ========== is_updatable_category テスト ==========


def test_is_updatable_category_experimental():
    """experimental カテゴリは更新可能"""
    loader = ConfigLoader()
    assert loader.is_updatable_category("experimental/my_strategy") is True


def test_is_updatable_category_production():
    """production カテゴリは更新可能"""
    loader = ConfigLoader()
    assert loader.is_updatable_category("production/range_break_v5") is True


def test_is_updatable_category_reference():
    """reference カテゴリは更新不可"""
    loader = ConfigLoader()
    assert loader.is_updatable_category("reference/strategy_template") is False


def test_is_updatable_category_legacy():
    """legacy カテゴリは更新不可"""
    loader = ConfigLoader()
    assert loader.is_updatable_category("legacy/old_strategy") is False


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


def test_save_strategy_config_production_allowed(tmp_path):
    """allow_production=True の場合は production カテゴリ更新を許可する"""
    config_dir = tmp_path / "config"
    production_dir = config_dir / "strategies" / "production"
    production_dir.mkdir(parents=True)

    loader = ConfigLoader(config_dir=str(config_dir))

    config = {
        "entry_filter_params": {"volume": {"enabled": True, "threshold": 1.5}},
        "exit_trigger_params": {"volume": {"enabled": False}},
    }

    path = loader.save_strategy_config(
        "production/my_strategy",
        config,
        force=True,
        allow_production=True,
    )

    assert path.exists()
    assert path.name == "my_strategy.yaml"
    assert "production" in str(path)


def test_save_strategy_config_reference_raises():
    """reference カテゴリへの保存は PermissionError"""
    loader = ConfigLoader()

    config = {"entry_filter_params": {}}

    with pytest.raises(PermissionError, match="experimental のみ保存可能"):
        loader.save_strategy_config("reference/my_strategy", config)


def test_save_strategy_config_strict_validation_raises(tmp_path):
    """ネストされた未知キーを含む設定の保存は ValueError"""
    config_dir = tmp_path / "config"
    strategies_dir = config_dir / "strategies" / "experimental"
    strategies_dir.mkdir(parents=True)

    loader = ConfigLoader(config_dir=str(config_dir))

    config = {
        "entry_filter_params": {
            "fundamental": {
                "foward_eps_growth": {  # typo
                    "enabled": True,
                    "threshold": 0.2,
                    "condition": "above",
                }
            }
        }
    }

    with pytest.raises(ValueError, match="foward_eps_growth"):
        loader.save_strategy_config("strict_invalid", config, force=True)


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


def test_duplicate_strategy_with_category_raises(tmp_path):
    """複製先にカテゴリを含めると ValueError"""
    config_dir = tmp_path / "config"
    reference_dir = config_dir / "strategies" / "reference"
    reference_dir.mkdir(parents=True)
    (reference_dir / "strategy_template.yaml").write_text(
        "entry_filter_params: {}",
        encoding="utf-8",
    )

    loader = ConfigLoader(config_dir=str(config_dir))

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


def test_delete_strategy_experimental_subdirectory(tmp_path):
    """experimental 配下サブディレクトリの戦略も削除できる"""
    config_dir = tmp_path / "config"
    nested_dir = config_dir / "strategies" / "experimental" / "optuna"
    nested_dir.mkdir(parents=True)

    target_file = nested_dir / "to_delete.yaml"
    target_file.write_text("entry_filter_params: {}", encoding="utf-8")

    loader = ConfigLoader(config_dir=str(config_dir))
    result = loader.delete_strategy("experimental/optuna/to_delete")

    assert result is True
    assert not target_file.exists()
    assert not nested_dir.exists()


def test_delete_strategy_experimental_subdirectory_with_sibling_keeps_dir(tmp_path):
    """同階層に別ファイルがある場合は親ディレクトリを削除しない"""
    config_dir = tmp_path / "config"
    nested_dir = config_dir / "strategies" / "experimental" / "optuna"
    nested_dir.mkdir(parents=True)

    target_file = nested_dir / "to_delete.yaml"
    target_file.write_text("entry_filter_params: {}", encoding="utf-8")
    sibling_file = nested_dir / "keep.yaml"
    sibling_file.write_text("entry_filter_params: {}", encoding="utf-8")

    loader = ConfigLoader(config_dir=str(config_dir))
    result = loader.delete_strategy("experimental/optuna/to_delete")

    assert result is True
    assert not target_file.exists()
    assert sibling_file.exists()
    assert nested_dir.exists()


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


# ========== move_strategy テスト ==========


def test_move_strategy_success(tmp_path):
    """experimental から production への移動が成功する"""
    config_dir = tmp_path / "config"
    source_dir = config_dir / "strategies" / "experimental" / "auto"
    source_dir.mkdir(parents=True)
    source_file = source_dir / "sample.yaml"
    source_file.write_text("entry_filter_params: {}", encoding="utf-8")

    loader = ConfigLoader(config_dir=str(config_dir))
    new_name, new_path = loader.move_strategy("experimental/auto/sample", "production")

    assert new_name == "production/auto/sample"
    assert new_path == config_dir / "strategies" / "production" / "auto" / "sample.yaml"
    assert new_path.exists()
    assert not source_file.exists()


def test_move_strategy_same_category_noop(tmp_path):
    """同一カテゴリ指定時は no-op として同一パスを返す"""
    config_dir = tmp_path / "config"
    source_dir = config_dir / "strategies" / "legacy"
    source_dir.mkdir(parents=True)
    source_file = source_dir / "sample.yaml"
    source_file.write_text("entry_filter_params: {}", encoding="utf-8")

    loader = ConfigLoader(config_dir=str(config_dir))
    new_name, new_path = loader.move_strategy("legacy/sample", "legacy")

    assert new_name == "legacy/sample"
    assert new_path == source_file
    assert source_file.exists()


def test_move_strategy_conflict_raises(tmp_path):
    """移動先に同名が存在する場合は FileExistsError"""
    config_dir = tmp_path / "config"
    experimental_dir = config_dir / "strategies" / "experimental"
    production_dir = config_dir / "strategies" / "production"
    experimental_dir.mkdir(parents=True)
    production_dir.mkdir(parents=True)

    (experimental_dir / "sample.yaml").write_text("entry_filter_params: {}", encoding="utf-8")
    (production_dir / "sample.yaml").write_text("entry_filter_params: {}", encoding="utf-8")

    loader = ConfigLoader(config_dir=str(config_dir))

    with pytest.raises(FileExistsError, match="既に存在します"):
        loader.move_strategy("experimental/sample", "production")


def test_move_strategy_from_reference_raises(tmp_path):
    """reference からの移動はサポート外"""
    config_dir = tmp_path / "config"
    reference_dir = config_dir / "strategies" / "reference"
    reference_dir.mkdir(parents=True)
    (reference_dir / "template.yaml").write_text("entry_filter_params: {}", encoding="utf-8")

    loader = ConfigLoader(config_dir=str(config_dir))

    with pytest.raises(ValueError, match="サポートされていません"):
        loader.move_strategy("reference/template", "production")


def test_move_strategy_invalid_target_category_raises(tmp_path):
    """移動先カテゴリが不正な場合は ValueError"""
    config_dir = tmp_path / "config"
    experimental_dir = config_dir / "strategies" / "experimental"
    experimental_dir.mkdir(parents=True)
    (experimental_dir / "sample.yaml").write_text("entry_filter_params: {}", encoding="utf-8")

    loader = ConfigLoader(config_dir=str(config_dir))

    with pytest.raises(ValueError, match="移動先カテゴリ"):
        loader.move_strategy("experimental/sample", "reference")


def test_move_strategy_rename_oserror_raises(tmp_path, monkeypatch):
    """ファイル移動時の OS エラーを伝播する"""
    config_dir = tmp_path / "config"
    experimental_dir = config_dir / "strategies" / "experimental"
    production_dir = config_dir / "strategies" / "production"
    experimental_dir.mkdir(parents=True)
    production_dir.mkdir(parents=True)

    source_path = experimental_dir / "sample.yaml"
    source_path.write_text("entry_filter_params: {}", encoding="utf-8")

    loader = ConfigLoader(config_dir=str(config_dir))

    original_rename = Path.rename

    def _raise_oserror(self: Path, target: Path):  # type: ignore[override]
        if self == source_path:
            raise OSError("disk full")
        return original_rename(self, target)

    monkeypatch.setattr(Path, "rename", _raise_oserror)

    with pytest.raises(OSError, match="disk full"):
        loader.move_strategy("experimental/sample", "production")


def test_get_category_roots_default_experimental_includes_fallback(monkeypatch):
    """デフォルト設定時の experimental は外部 + プロジェクト内フォールバック"""
    loader = ConfigLoader()
    monkeypatch.setattr(loader, "_is_default_config", lambda: True)
    monkeypatch.setattr(
        "src.shared.paths.get_strategies_dir",
        lambda category: Path(f"/tmp/external/{category}"),
    )

    roots = loader._get_category_roots("experimental")

    assert roots[0] == Path("/tmp/external/experimental")
    assert roots[1] == Path("config/strategies/experimental")


def test_get_category_roots_default_non_experimental(monkeypatch):
    """デフォルト設定時の non-experimental は外部ルートのみ"""
    loader = ConfigLoader()
    monkeypatch.setattr(loader, "_is_default_config", lambda: True)
    monkeypatch.setattr(
        "src.shared.paths.get_strategies_dir",
        lambda category: Path(f"/tmp/external/{category}"),
    )

    roots = loader._get_category_roots("production")

    assert roots == [Path("/tmp/external/production")]


def test_resolve_category_root_and_relative_path_raises_for_outside_path(tmp_path):
    """許可ディレクトリ外のパスは ValueError"""
    config_dir = tmp_path / "config"
    (config_dir / "strategies").mkdir(parents=True)
    outside = tmp_path / "outside.yaml"
    outside.write_text("entry_filter_params: {}", encoding="utf-8")

    loader = ConfigLoader(config_dir=str(config_dir))

    with pytest.raises(ValueError, match="許可された戦略ディレクトリ外"):
        loader._resolve_category_root_and_relative_path(outside)


def test_cleanup_empty_strategy_dirs_stops_on_non_empty(tmp_path):
    """掃除処理は非空ディレクトリで停止する"""
    root = tmp_path / "root"
    start = root / "level1"
    start.mkdir(parents=True)
    keep = start / "keep.txt"
    keep.write_text("x", encoding="utf-8")

    loader = ConfigLoader(config_dir=str(tmp_path / "config"))
    loader._cleanup_empty_strategy_dirs(start, root)

    assert start.exists()
    assert keep.exists()


def test_cleanup_empty_strategy_dirs_root_immediate_noop(tmp_path):
    """開始ディレクトリがルートと同じ場合は no-op"""
    root = tmp_path / "root"
    root.mkdir(parents=True)

    loader = ConfigLoader(config_dir=str(tmp_path / "config"))
    loader._cleanup_empty_strategy_dirs(root, root)

    assert root.exists()


# ========== カバレッジ補強テスト ==========


def test_load_strategy_config_file_not_found_branch(monkeypatch):
    """load_strategy_config の FileNotFoundError 分岐を通す"""
    loader = ConfigLoader(config_dir="/tmp/config")

    monkeypatch.setattr(loader, "_infer_strategy_path", lambda _name: Path("/tmp/sample.yaml"))
    monkeypatch.setattr("src.domains.strategy.runtime.loader.validate_path_within_strategies", lambda *_args: None)
    monkeypatch.setattr(
        "src.domains.strategy.runtime.loader.load_yaml_file",
        lambda _path: (_ for _ in ()).throw(FileNotFoundError("missing")),
    )

    with pytest.raises(FileNotFoundError, match="missing"):
        loader.load_strategy_config("experimental/sample")


def test_load_strategy_config_generic_error_branch(monkeypatch):
    """load_strategy_config の汎用例外分岐を通す"""
    loader = ConfigLoader(config_dir="/tmp/config")

    monkeypatch.setattr(loader, "_infer_strategy_path", lambda _name: Path("/tmp/sample.yaml"))
    monkeypatch.setattr("src.domains.strategy.runtime.loader.validate_path_within_strategies", lambda *_args: None)
    monkeypatch.setattr(
        "src.domains.strategy.runtime.loader.load_yaml_file",
        lambda _path: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    with pytest.raises(RuntimeError, match="boom"):
        loader.load_strategy_config("experimental/sample")


def test_runtime_wrapper_methods_delegate(monkeypatch):
    """ラッパーメソッドが依存関数へ委譲することを確認"""
    from datetime import datetime

    loader = ConfigLoader(config_dir="/tmp/config")
    loader.default_config = {"k": "v"}

    monkeypatch.setattr(
        "src.domains.strategy.runtime.loader.get_execution_config",
        lambda cfg, default: {"cfg": cfg, "default": default},
    )
    monkeypatch.setattr(
        "src.domains.strategy.runtime.loader.get_available_strategies",
        lambda _config_dir: {"experimental": ["demo"]},
    )
    monkeypatch.setattr(
        "src.domains.strategy.runtime.loader.get_strategy_metadata",
        lambda _config_dir: [
            StrategyMetadata(
                name="experimental/demo",
                category="experimental",
                path=Path("/tmp/demo.yaml"),
                mtime=datetime.now(),
            )
        ],
    )
    monkeypatch.setattr(
        "src.domains.strategy.runtime.loader.validate_strategy_config",
        lambda _cfg: True,
    )
    monkeypatch.setattr(
        "src.domains.strategy.runtime.loader.get_template_notebook_path",
        lambda _execution: Path("/tmp/template.ipynb"),
    )
    monkeypatch.setattr(
        "src.domains.strategy.runtime.loader.get_output_directory",
        lambda _execution: Path("/tmp/output"),
    )
    monkeypatch.setattr(
        "src.domains.strategy.runtime.loader.extract_entry_filter_params",
        lambda _cfg: {"entry": True},
    )
    monkeypatch.setattr(
        "src.domains.strategy.runtime.loader.extract_exit_trigger_params",
        lambda _cfg: {"exit": True},
    )

    strategy_config = {"entry_filter_params": {}}
    assert loader.get_execution_config(strategy_config) == {"cfg": strategy_config, "default": {"k": "v"}}
    assert loader.get_available_strategies() == {"experimental": ["demo"]}
    assert loader.get_strategy_metadata()[0].name == "experimental/demo"
    assert loader.validate_strategy_config(strategy_config) is True
    assert loader.get_template_notebook_path(strategy_config) == Path("/tmp/template.ipynb")
    assert loader.get_output_directory(strategy_config) == Path("/tmp/output")
    assert loader.extract_entry_filter_params(strategy_config) == {"entry": True}
    assert loader.extract_exit_trigger_params(strategy_config) == {"exit": True}


def test_is_editable_category_without_prefix_uses_inferred_path(monkeypatch):
    """カテゴリ省略時は推測パスのカテゴリルートで判定する"""
    loader = ConfigLoader(config_dir="/tmp/config")
    monkeypatch.setattr(
        loader,
        "_infer_strategy_path",
        lambda _name: Path("/tmp/config/strategies/production/sample.yaml"),
    )
    assert loader.is_editable_category("sample") is False


def test_is_editable_category_without_prefix_nested_experimental(monkeypatch):
    """カテゴリ省略 + experimental サブディレクトリでも編集可能と判定する"""
    loader = ConfigLoader(config_dir="/tmp/config")
    monkeypatch.setattr(
        loader,
        "_infer_strategy_path",
        lambda _name: Path("/tmp/config/strategies/experimental/optuna/sample.yaml"),
    )
    assert loader.is_editable_category("sample") is True


def test_is_editable_category_without_prefix_not_found_returns_true(monkeypatch):
    """カテゴリ省略 + 未存在の場合は True を返す（互換仕様）"""
    loader = ConfigLoader(config_dir="/tmp/config")
    monkeypatch.setattr(
        loader,
        "_infer_strategy_path",
        lambda _name: (_ for _ in ()).throw(FileNotFoundError("missing")),
    )
    assert loader.is_editable_category("sample") is True


def test_is_updatable_category_without_prefix_uses_inferred_path(monkeypatch):
    """更新可否もカテゴリ省略時に推測パスで判定する"""
    loader = ConfigLoader(config_dir="/tmp/config")
    monkeypatch.setattr(
        loader,
        "_infer_strategy_path",
        lambda _name: Path("/tmp/config/strategies/production/sample.yaml"),
    )
    assert loader.is_updatable_category("sample") is True


def test_is_updatable_category_without_prefix_nested_experimental(monkeypatch):
    """更新可否判定もカテゴリルート基準で nested experimental を許可する"""
    loader = ConfigLoader(config_dir="/tmp/config")
    monkeypatch.setattr(
        loader,
        "_infer_strategy_path",
        lambda _name: Path("/tmp/config/strategies/experimental/optuna/sample.yaml"),
    )
    assert loader.is_updatable_category("sample") is True


def test_save_strategy_config_logs_warning_when_force_false(tmp_path):
    """既存ファイル + force=False では警告分岐を通って保存する"""
    config_dir = tmp_path / "config"
    strategies_dir = config_dir / "strategies" / "experimental"
    strategies_dir.mkdir(parents=True)
    existing = strategies_dir / "test_strategy.yaml"
    existing.write_text("entry_filter_params: {}", encoding="utf-8")

    loader = ConfigLoader(config_dir=str(config_dir))
    config = {"entry_filter_params": {"volume": {"enabled": True, "threshold": 1.5}}}

    path = loader.save_strategy_config("test_strategy", config, force=False)
    assert path == existing


def test_duplicate_strategy_default_config_conflict_branch(tmp_path, monkeypatch):
    """default config モードの duplicate 競合分岐を通す"""
    external_experimental = tmp_path / "external" / "experimental"
    external_experimental.mkdir(parents=True)

    loader = ConfigLoader(config_dir="config")
    monkeypatch.setattr(loader, "load_strategy_config", lambda _name: {"entry_filter_params": {}})
    monkeypatch.setattr("src.shared.paths.get_strategies_dir", lambda _category: external_experimental)
    monkeypatch.setattr(
        "src.shared.paths.find_strategy_path",
        lambda _name: external_experimental / "already_exists.yaml",
    )

    with pytest.raises(FileExistsError, match="既に存在します"):
        loader.duplicate_strategy("experimental/source", "already_exists")


def test_rename_strategy_default_config_success(tmp_path, monkeypatch):
    """default config モードの rename 成功分岐を通す"""
    external_experimental = tmp_path / "external" / "experimental"
    external_experimental.mkdir(parents=True)
    current_path = external_experimental / "old_name.yaml"
    current_path.write_text("entry_filter_params: {}", encoding="utf-8")

    loader = ConfigLoader(config_dir="config")
    monkeypatch.setattr(loader, "_infer_strategy_path", lambda _name: current_path)
    monkeypatch.setattr("src.shared.paths.get_strategies_dir", lambda _category: external_experimental)
    monkeypatch.setattr("src.shared.paths.find_strategy_path", lambda _name: None)

    new_path = loader.rename_strategy("experimental/old_name", "new_name")
    assert new_path == external_experimental / "new_name.yaml"
    assert new_path.exists()
    assert not current_path.exists()


def test_rename_strategy_subdirectory_success(tmp_path):
    """experimental 配下サブディレクトリの戦略もリネームできる"""
    config_dir = tmp_path / "config"
    nested_dir = config_dir / "strategies" / "experimental" / "optuna"
    nested_dir.mkdir(parents=True)
    current_path = nested_dir / "old_name.yaml"
    current_path.write_text("entry_filter_params: {}", encoding="utf-8")

    loader = ConfigLoader(config_dir=str(config_dir))
    new_path = loader.rename_strategy("experimental/optuna/old_name", "new_name")

    assert new_path == config_dir / "strategies" / "experimental" / "new_name.yaml"
    assert new_path.exists()
    assert not current_path.exists()
    assert not nested_dir.exists()


def test_rename_strategy_default_config_conflict(tmp_path, monkeypatch):
    """default config モードの rename 競合分岐を通す"""
    external_experimental = tmp_path / "external" / "experimental"
    external_experimental.mkdir(parents=True)
    current_path = external_experimental / "old_name.yaml"
    current_path.write_text("entry_filter_params: {}", encoding="utf-8")

    loader = ConfigLoader(config_dir="config")
    monkeypatch.setattr(loader, "_infer_strategy_path", lambda _name: current_path)
    monkeypatch.setattr("src.shared.paths.get_strategies_dir", lambda _category: external_experimental)
    monkeypatch.setattr(
        "src.shared.paths.find_strategy_path",
        lambda _name: external_experimental / "new_name.yaml",
    )

    with pytest.raises(FileExistsError, match="既に存在します"):
        loader.rename_strategy("experimental/old_name", "new_name")
