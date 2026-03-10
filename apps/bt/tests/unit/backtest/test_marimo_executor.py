"""
MarimoExecutor unit tests

Marimo Notebook実行ラッパーのテスト
"""

import json
import subprocess
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.domains.backtest.core.marimo_executor import MarimoExecutor


class TestMarimoExecutorInitialization:
    """初期化テスト"""

    def test_default_output_dir(self):
        """デフォルト出力ディレクトリのテスト（外部ディレクトリを使用）"""
        from src.shared.paths import get_backtest_results_dir

        executor = MarimoExecutor()
        # 外部ディレクトリ（~/.local/share/trading25/backtest/results）を使用
        assert executor.output_dir == get_backtest_results_dir()

    def test_custom_output_dir(self):
        """カスタム出力ディレクトリのテスト"""
        with tempfile.TemporaryDirectory() as tmpdir:
            executor = MarimoExecutor(output_dir=tmpdir)
            assert executor.output_dir == Path(tmpdir)

    def test_invalid_output_dir_path_traversal(self):
        """パストラバーサル攻撃対策のテスト"""
        with pytest.raises(ValueError, match="不正な出力ディレクトリパス"):
            MarimoExecutor(output_dir="../etc/passwd")

    def test_invalid_output_dir_home(self):
        """ホームディレクトリ攻撃対策のテスト"""
        with pytest.raises(ValueError, match="出力ディレクトリはプロジェクト内"):
            MarimoExecutor(output_dir="~/malicious")


class TestParameterSerialization:
    """JSONパラメータシリアライズテスト"""

    def test_serialize_simple_params(self):
        """シンプルなパラメータのシリアライズ"""
        with tempfile.TemporaryDirectory() as tmpdir:
            executor = MarimoExecutor(output_dir=tmpdir)

            params = {
                "shared_config": {"initial_cash": 10000000, "dataset": "topix100"},
            }

            json_path = executor._serialize_params_to_json(params)

            assert Path(json_path).exists()
            with open(json_path) as f:
                loaded = json.load(f)

            assert loaded == params

    def test_serialize_complex_nested_params(self):
        """複雑なネスト辞書のシリアライズ"""
        with tempfile.TemporaryDirectory() as tmpdir:
            executor = MarimoExecutor(output_dir=tmpdir)

            params = {
                "shared_config": {
                    "initial_cash": 10000000,
                    "stock_codes": ["1234", "5678"],
                    "nested": {"deep": {"value": 123}},
                },
                "entry_filter_params": {"volume_ratio_above": {"ratio_threshold": 2.0}},
                "exit_trigger_params": {"atr": {"multiplier": 1.5}},
            }

            json_path = executor._serialize_params_to_json(params)

            assert Path(json_path).exists()
            with open(json_path) as f:
                loaded = json.load(f)

            assert loaded == params

    def test_serialize_params_with_japanese(self):
        """日本語を含むパラメータのシリアライズ"""
        with tempfile.TemporaryDirectory() as tmpdir:
            executor = MarimoExecutor(output_dir=tmpdir)

            params = {
                "shared_config": {"description": "テスト戦略"},
            }

            json_path = executor._serialize_params_to_json(params)

            with open(json_path, encoding="utf-8") as f:
                loaded = json.load(f)

            assert loaded["shared_config"]["description"] == "テスト戦略"


class TestFilenameValidation:
    """ファイル名検証テスト"""

    def test_valid_html_filename(self):
        """正常なHTMLファイル名"""
        with tempfile.TemporaryDirectory() as tmpdir:
            executor = MarimoExecutor(output_dir=tmpdir)
            # Should not raise
            executor._validate_filename("test_output.html", ".html")

    def test_invalid_ipynb_filename_for_html(self):
        """ipynb拡張子はHTML出力として無効"""
        with tempfile.TemporaryDirectory() as tmpdir:
            executor = MarimoExecutor(output_dir=tmpdir)
            with pytest.raises(ValueError, match="不正な"):
                executor._validate_filename("test_output.ipynb", ".html")

    def test_invalid_filename_path_traversal(self):
        """パストラバーサル文字を含むファイル名"""
        with tempfile.TemporaryDirectory() as tmpdir:
            executor = MarimoExecutor(output_dir=tmpdir)
            with pytest.raises(ValueError, match="不正な"):
                executor._validate_filename("../malicious.html", ".html")

    def test_invalid_filename_too_long(self):
        """長すぎるファイル名"""
        with tempfile.TemporaryDirectory() as tmpdir:
            executor = MarimoExecutor(output_dir=tmpdir)
            long_name = "a" * 100 + ".html"
            with pytest.raises(ValueError, match="長すぎます"):
                executor._validate_filename(long_name, ".html")

    def test_invalid_filename_double_dot(self):
        """危険な .. を含むファイル名"""
        with tempfile.TemporaryDirectory() as tmpdir:
            executor = MarimoExecutor(output_dir=tmpdir)
            with pytest.raises(ValueError, match="不正な文字"):
                executor._validate_filename("bad..html", ".html")


class TestOutputFilenameGeneration:
    """出力ファイル名生成テスト"""

    def test_generate_filename_with_dataset(self):
        """データセット付きファイル名生成"""
        with tempfile.TemporaryDirectory() as tmpdir:
            executor = MarimoExecutor(output_dir=tmpdir)

            params = {"shared_config": {"dataset": "topix100"}}
            strategy_dir, filename = executor._generate_output_filename(
                params, "test_strategy"
            )

            assert strategy_dir == "backtest/test_strategy"
            assert "topix100" in filename

    def test_generate_filename_without_dataset(self):
        """データセットなしファイル名生成"""
        with tempfile.TemporaryDirectory() as tmpdir:
            executor = MarimoExecutor(output_dir=tmpdir)

            params = {"shared_config": {}}
            strategy_dir, filename = executor._generate_output_filename(params, None)

            assert strategy_dir == "backtest/unknown"
            assert "unknown" in filename

    def test_generate_filename_with_path_dataset(self):
        """パス形式データセット名の処理"""
        with tempfile.TemporaryDirectory() as tmpdir:
            executor = MarimoExecutor(output_dir=tmpdir)

            params = {"shared_config": {"dataset": "dataset/primeExTopix500.db"}}
            _, filename = executor._generate_output_filename(params, "range_break")

            assert "primeExTopix500" in filename

    def test_plan_report_paths_with_explicit_output_filename(self):
        """明示ファイル名指定時も sibling artifact を計画できる"""
        with tempfile.TemporaryDirectory() as tmpdir:
            executor = MarimoExecutor(output_dir=tmpdir)

            paths = executor.plan_report_paths(
                {"shared_config": {"dataset": "sample"}},
                strategy_name="demo",
                output_filename="custom-report.html",
            )

            assert paths.html_path.name == "custom-report.html"
            assert paths.metrics_path == paths.html_path.with_suffix(".metrics.json")
            assert paths.manifest_path == paths.html_path.with_suffix(".manifest.json")
            assert paths.simulation_payload_path == paths.html_path.with_suffix(".simulation.pkl")

    def test_validate_output_dir_tmp_fallback_when_data_dir_lookup_fails(self, monkeypatch):
        """get_data_dir が失敗しても /tmp は許可される"""
        monkeypatch.setattr(
            "src.shared.paths.get_data_dir",
            lambda: (_ for _ in ()).throw(RuntimeError("boom")),
        )

        executor = MarimoExecutor(output_dir="/tmp/trading25-marimo-test")
        assert str(executor.output_dir) == "/tmp/trading25-marimo-test"


class TestExecuteNotebook:
    """Notebook実行テスト（モック使用）"""

    @patch("src.domains.backtest.core.marimo_executor.subprocess.run")
    def test_execute_generates_html(self, mock_run):
        """HTML生成の確認"""
        with tempfile.TemporaryDirectory() as tmpdir:
            executor = MarimoExecutor(output_dir=tmpdir)

            # モックが呼ばれたときにHTMLファイルを作成するside_effect
            def create_html_file(*args, **kwargs):
                # コマンドから出力パスを取得（-o フラグの次の引数）
                cmd = args[0]
                try:
                    output_idx = cmd.index("-o") + 1
                    html_path = Path(cmd[output_idx])
                    html_path.parent.mkdir(parents=True, exist_ok=True)
                    html_path.write_text("<html>test</html>")
                except (ValueError, IndexError):
                    pass
                return MagicMock(returncode=0, stderr="", stdout="")

            mock_run.side_effect = create_html_file

            # テンプレートファイルを作成
            template_path = Path(tmpdir) / "test_template.py"
            template_path.write_text("# test template")

            html_path = executor.execute_notebook(
                template_path=str(template_path),
                parameters={"shared_config": {"dataset": "test"}},
                strategy_name="test_strategy",
            )

            assert str(html_path).endswith(".html")
            assert mock_run.called

    @patch("src.domains.backtest.core.marimo_executor.subprocess.run")
    def test_execute_with_timeout(self, mock_run):
        """タイムアウト設定の確認"""
        with tempfile.TemporaryDirectory() as tmpdir:
            executor = MarimoExecutor(output_dir=tmpdir)

            # モックが呼ばれたときにHTMLファイルを作成するside_effect
            def create_html_file(*args, **kwargs):
                cmd = args[0]
                try:
                    output_idx = cmd.index("-o") + 1
                    html_path = Path(cmd[output_idx])
                    html_path.parent.mkdir(parents=True, exist_ok=True)
                    html_path.write_text("<html>test</html>")
                except (ValueError, IndexError):
                    pass
                return MagicMock(returncode=0, stderr="", stdout="")

            mock_run.side_effect = create_html_file

            template_path = Path(tmpdir) / "test_template.py"
            template_path.write_text("# test template")

            executor.execute_notebook(
                template_path=str(template_path),
                parameters={"shared_config": {}},
                strategy_name="test",
                timeout=300,
            )

            # 最初の呼び出し（HTML export）のタイムアウトを確認
            # mock_run.call_args_list[0] = HTML export, [1] = ipynb export
            first_call_kwargs = mock_run.call_args_list[0][1]
            assert first_call_kwargs["timeout"] == 300

    @patch("src.domains.backtest.core.marimo_executor.subprocess.run")
    def test_execute_error_handling(self, mock_run):
        """エラーハンドリングの確認"""
        with tempfile.TemporaryDirectory() as tmpdir:
            executor = MarimoExecutor(output_dir=tmpdir)

            mock_run.return_value = MagicMock(returncode=1, stderr="Error occurred")

            template_path = Path(tmpdir) / "test_template.py"
            template_path.write_text("# test template")

            with pytest.raises(RuntimeError, match="Marimo execution failed"):
                executor.execute_notebook(
                    template_path=str(template_path),
                    parameters={"shared_config": {}},
                    strategy_name="test",
                )

    @patch("src.domains.backtest.core.marimo_executor.subprocess.run")
    def test_execute_error_when_html_not_created(self, mock_run):
        """subprocess 成功でも HTML 未作成ならエラー"""
        with tempfile.TemporaryDirectory() as tmpdir:
            executor = MarimoExecutor(output_dir=tmpdir)
            mock_run.return_value = MagicMock(returncode=0, stderr="", stdout="")

            template_path = Path(tmpdir) / "test_template.py"
            template_path.write_text("# test template")

            with pytest.raises(RuntimeError, match="HTML file was not created"):
                executor.execute_notebook(
                    template_path=str(template_path),
                    parameters={"shared_config": {}},
                    strategy_name="test",
                )

    @patch("src.domains.backtest.core.marimo_executor.subprocess.run")
    def test_execute_timeout_raises_runtime_error(self, mock_run):
        """タイムアウトを RuntimeError へ変換する"""
        with tempfile.TemporaryDirectory() as tmpdir:
            executor = MarimoExecutor(output_dir=tmpdir)
            mock_run.side_effect = subprocess.TimeoutExpired(cmd="marimo", timeout=3)

            template_path = Path(tmpdir) / "test_template.py"
            template_path.write_text("# test template")

            with pytest.raises(RuntimeError, match="timed out after 3 seconds"):
                executor.execute_notebook(
                    template_path=str(template_path),
                    parameters={"shared_config": {}},
                    strategy_name="test",
                    timeout=3,
                )

    def test_execute_template_not_found(self):
        """テンプレート存在確認"""
        with tempfile.TemporaryDirectory() as tmpdir:
            executor = MarimoExecutor(output_dir=tmpdir)

            with pytest.raises(FileNotFoundError, match="テンプレートが見つかりません"):
                executor.execute_notebook(
                    template_path="/nonexistent/template.py",
                    parameters={"shared_config": {}},
                    strategy_name="test",
                )


class TestExecutionSummary:
    """実行サマリーテスト"""

    def test_get_execution_summary(self):
        """実行サマリー取得"""
        with tempfile.TemporaryDirectory() as tmpdir:
            executor = MarimoExecutor(output_dir=tmpdir)

            # テストファイル作成
            test_file = Path(tmpdir) / "test.html"
            test_file.write_text("<html>test</html>")

            summary = executor.get_execution_summary(test_file)

            assert "html_path" in summary
            assert "file_size" in summary
            assert summary["file_size"] > 0
            assert "generated_at" in summary

    def test_get_execution_summary_missing_file(self):
        """存在しないファイルのサマリー"""
        with tempfile.TemporaryDirectory() as tmpdir:
            executor = MarimoExecutor(output_dir=tmpdir)

            summary = executor.get_execution_summary(Path(tmpdir) / "nonexistent.html")

            assert summary["file_size"] == 0


class TestCleanup:
    """クリーンアップテスト"""

    @patch("src.domains.backtest.core.marimo_executor.subprocess.run")
    def test_json_cleanup_on_success(self, mock_run):
        """成功時のJSON削除確認"""
        with tempfile.TemporaryDirectory() as tmpdir:
            executor = MarimoExecutor(output_dir=tmpdir)

            # モックが呼ばれたときにHTMLファイルを作成するside_effect
            def create_html_file(*args, **kwargs):
                cmd = args[0]
                try:
                    output_idx = cmd.index("-o") + 1
                    html_path = Path(cmd[output_idx])
                    html_path.parent.mkdir(parents=True, exist_ok=True)
                    html_path.write_text("<html>test</html>")
                except (ValueError, IndexError):
                    pass
                return MagicMock(returncode=0, stderr="", stdout="")

            mock_run.side_effect = create_html_file

            template_path = Path(tmpdir) / "test_template.py"
            template_path.write_text("# test template")

            executor.execute_notebook(
                template_path=str(template_path),
                parameters={"shared_config": {}},
                strategy_name="test",
            )

            # params_*.json ファイルが削除されているか確認
            json_files = list(Path(tmpdir).glob("params_*.json"))
            assert len(json_files) == 0

    @patch("src.domains.backtest.core.marimo_executor.subprocess.run")
    def test_json_cleanup_on_error(self, mock_run):
        """エラー時のJSON削除確認"""
        with tempfile.TemporaryDirectory() as tmpdir:
            executor = MarimoExecutor(output_dir=tmpdir)

            mock_run.return_value = MagicMock(returncode=1, stderr="Error")

            template_path = Path(tmpdir) / "test_template.py"
            template_path.write_text("# test template")

            with pytest.raises(RuntimeError):
                executor.execute_notebook(
                    template_path=str(template_path),
                    parameters={"shared_config": {}},
                    strategy_name="test",
                )

            # エラー時もJSONファイルが削除されているか確認
            json_files = list(Path(tmpdir).glob("params_*.json"))
            assert len(json_files) == 0
