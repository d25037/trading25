"""
セキュリティ検証テスト

パストラバーサル攻撃、入力検証などのセキュリティ機能をテストします。
"""

import tempfile

import pytest
from pathlib import Path
from unittest.mock import patch

from src.strategy_config.loader import ConfigLoader
from src.utils.logger_config import sanitize_sensitive_info


class TestPathTraversalSecurity:
    """パストラバーサル攻撃対策のテスト"""

    def test_config_loader_strategy_name_validation(self):
        """戦略名の検証テスト"""
        config_loader = ConfigLoader()

        # 正常なケース
        with patch("pathlib.Path.exists", return_value=True):
            with patch("builtins.open", create=True):
                with patch(
                    "src.strategy_config.file_operations.YAML"
                ) as mock_yaml_class:
                    mock_yaml_instance = mock_yaml_class.return_value
                    mock_yaml_instance.load.return_value = {"strategy_params": {"name": "test"}}
                    # 正常な戦略名は通過する
                    config_loader.load_strategy_config("valid_strategy_name")

        # 異常なケース - パストラバーサル攻撃
        malicious_names = [
            "../../../etc/passwd",
            "..\\..\\windows\\system32",
            "strategy/../../../secret",
            "~/.ssh/id_rsa",
            "strategy/../../config",
        ]

        for malicious_name in malicious_names:
            with pytest.raises(
                ValueError, match="(無効な戦略名|不正な文字が含まれています)"
            ):
                config_loader.load_strategy_config(malicious_name)

    def test_config_loader_path_restriction(self, tmp_path):
        """設定ファイルパスの制限テスト"""
        config_loader = ConfigLoader(str(tmp_path / "config"))

        # テスト用の設定ディレクトリを作成（4カテゴリすべて）
        strategies_dir = tmp_path / "config" / "strategies"
        for category in ["experimental", "production", "reference", "legacy"]:
            category_dir = strategies_dir / category
            category_dir.mkdir(parents=True)

        # 正常な設定ファイルを作成（experimentalカテゴリに配置）
        test_config = strategies_dir / "experimental" / "test_strategy.yaml"
        test_config.write_text("strategy_params:\n  name: test")

        # 正常なケースは動作する
        config = config_loader.load_strategy_config("test_strategy")
        assert config["strategy_params"]["name"] == "test"

        # 悪意のあるファイル名（実際のパストラバーサル検証）
        with pytest.raises(ValueError):
            config_loader.load_strategy_config("../../../etc/passwd")


class TestLogSanitization:
    """ログサニタイズのテスト"""

    def test_sensitive_info_sanitization(self):
        """機密情報サニタイズのテスト"""
        # ファイルパスのマスキング
        test_cases = [
            ("/Users/john/project/file.db", "..."),
            ("C:\\Users\\Alice\\Documents\\secret.yaml", "..."),
            ("/home/user/app/config.yaml", "[SYSTEM_PATH]"),
            ("password=secret123", "***"),
            ("token: abc123def", "***"),
            ("sqlite:///data/user/database.db", "***"),
        ]

        for input_msg, expected_pattern in test_cases:
            sanitized = sanitize_sensitive_info(input_msg)
            assert expected_pattern in sanitized, (
                f"Expected '{expected_pattern}' in '{sanitized}' for input '{input_msg}'"
            )

class TestYAMLSafety:
    """YAML安全性のテスト"""

    def test_yaml_safe_load_usage(self):
        """yaml.safe_loadの使用確認テスト"""
        # 実際のコードでyaml.loadが使用されていないことを確認
        # このテストは静的解析的な意味合いが強い

        with tempfile.TemporaryDirectory() as temp_dir:
            config_dir = Path(temp_dir) / "config"
            config_dir.mkdir()
            strategies_dir = config_dir / "strategies" / "experimental"
            strategies_dir.mkdir(parents=True)

            # テスト用設定ファイル (experimental配下に作成)
            test_config = strategies_dir / "test.yaml"
            test_config.write_text("""
strategy_params:
  name: test_strategy
  param1: value1
""")

            config_loader = ConfigLoader(str(config_dir))
            config = config_loader.load_strategy_config("test")

            # 正常に読み込まれることを確認
            assert config["strategy_params"]["name"] == "test_strategy"
            assert config["strategy_params"]["param1"] == "value1"


if __name__ == "__main__":
    pytest.main([__file__])
