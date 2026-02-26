"""
パラメータ最適化Notebook生成機能のテスト（Marimo実行方式）
"""

import json
import os
from unittest.mock import MagicMock, patch

import pytest

from src.domains.optimization.notebook_generator import (
    _save_results_as_json,
    generate_optimization_notebook,
)


@pytest.fixture
def sample_optimization_results():
    """サンプル最適化結果"""
    return [
        {
            "params": {
                "entry_filter_params.period_breakout.period": 50,
                "entry_filter_params.volume.threshold": 2.0,
            },
            "score": 0.85,
            "metric_values": {
                "sharpe_ratio": 1.5,
                "calmar_ratio": 1.2,
                "total_return": 0.45,
                "trade_count": 42,
            },
            "normalized_metrics": {
                "sharpe_ratio": 0.9,
                "calmar_ratio": 0.85,
                "total_return": 0.8,
            },
        },
        {
            "params": {
                "entry_filter_params.period_breakout.period": 100,
                "entry_filter_params.volume.threshold": 1.5,
            },
            "score": 0.75,
            "metric_values": {
                "sharpe_ratio": 1.3,
                "calmar_ratio": 1.0,
                "total_return": 0.38,
                "trade_count": 35,
            },
            "normalized_metrics": {
                "sharpe_ratio": 0.8,
                "calmar_ratio": 0.7,
                "total_return": 0.75,
            },
        },
        {
            "params": {
                "entry_filter_params.period_breakout.period": 200,
                "entry_filter_params.volume.threshold": 2.5,
            },
            "score": 0.65,
            "metric_values": {
                "sharpe_ratio": 1.1,
                "calmar_ratio": 0.9,
                "total_return": 0.32,
                "trade_count": 28,
            },
            "normalized_metrics": {
                "sharpe_ratio": 0.7,
                "calmar_ratio": 0.6,
                "total_return": 0.65,
            },
        },
    ]


@pytest.fixture
def sample_parameter_ranges():
    """サンプルパラメータ範囲"""
    return {
        "entry_filter_params": {
            "period_breakout": {"period": [50, 100, 200]},
            "volume": {"threshold": [1.5, 2.0, 2.5]},
        }
    }


@pytest.fixture
def sample_scoring_weights():
    """サンプルスコアリング重み"""
    return {"sharpe_ratio": 0.6, "calmar_ratio": 0.3, "total_return": 0.1}


class TestSaveResultsAsJson:
    """結果データJSON保存機能のテスト"""

    def test_save_results_basic(self, sample_optimization_results, tmp_path):
        """基本的なJSON保存テスト"""
        json_path = _save_results_as_json(sample_optimization_results, str(tmp_path))

        # ファイル存在確認
        assert os.path.exists(json_path)
        assert json_path.endswith(".json")

        # JSON読み込み
        with open(json_path, encoding="utf-8") as f:
            loaded_data = json.load(f)

        # データ内容確認
        assert isinstance(loaded_data, list)
        assert len(loaded_data) == 3
        assert loaded_data[0]["score"] == 0.85
        assert (
            loaded_data[0]["params"]["entry_filter_params.period_breakout.period"] == 50
        )
        assert loaded_data[0]["metric_values"]["trade_count"] == 42

    def test_json_serializable_format(self, sample_optimization_results, tmp_path):
        """JSONシリアライズ可能形式の確認"""
        json_path = _save_results_as_json(sample_optimization_results, str(tmp_path))

        with open(json_path, encoding="utf-8") as f:
            loaded_data = json.load(f)

        # 必須キーの存在確認
        for result in loaded_data:
            assert "params" in result
            assert "score" in result
            assert "metric_values" in result
            assert "normalized_metrics" in result

    def test_empty_results(self, tmp_path):
        """空の結果リストのJSON保存"""
        json_path = _save_results_as_json([], str(tmp_path))

        assert os.path.exists(json_path)

        with open(json_path, encoding="utf-8") as f:
            loaded_data = json.load(f)

        assert loaded_data == []

    def test_save_results_without_trade_count_keeps_compatibility(self, tmp_path):
        """trade_countがない旧データ形式も保存できることを確認"""
        old_format_results = [
            {
                "params": {"entry_filter_params.period_breakout.period": 50},
                "score": 0.5,
                "metric_values": {
                    "sharpe_ratio": 1.0,
                    "calmar_ratio": 0.8,
                    "total_return": 0.2,
                },
                "normalized_metrics": {
                    "sharpe_ratio": 0.5,
                    "calmar_ratio": 0.5,
                    "total_return": 0.5,
                },
            }
        ]

        json_path = _save_results_as_json(old_format_results, str(tmp_path))
        with open(json_path, encoding="utf-8") as f:
            loaded_data = json.load(f)

        assert "trade_count" not in loaded_data[0]["metric_values"]


class TestNotebookGeneratorMarimo:
    """Notebook生成機能のテスト（Marimo実行方式）"""

    @patch("src.domains.backtest.core.marimo_executor.MarimoExecutor")
    def test_generate_notebook_with_marimo_mock(
        self,
        mock_executor_class,
        sample_optimization_results,
        sample_parameter_ranges,
        sample_scoring_weights,
        tmp_path,
    ):
        """モックを使用したNotebook生成テスト（Marimo）"""
        # モックMarimoExecutorを設定
        mock_executor = MagicMock()
        mock_executor_class.return_value = mock_executor

        # execute_notebookのモック戻り値を設定
        output_path = tmp_path / "notebooks/generated/optimization/test_strategy/test_output.html"
        mock_executor.execute_notebook.return_value = output_path

        # Notebook生成実行
        result_path = generate_optimization_notebook(
            results=sample_optimization_results,
            output_path=str(tmp_path / "test_output.ipynb"),
            strategy_name="test_strategy",
            parameter_ranges=sample_parameter_ranges,
            scoring_weights=sample_scoring_weights,
            n_combinations=9,
            _skip_path_validation=True,
        )

        # MarimoExecutorが正しく呼び出されたか確認
        mock_executor_class.assert_called_once_with(output_dir=str(tmp_path))
        mock_executor.execute_notebook.assert_called_once()

        # 呼び出し引数確認
        call_args = mock_executor.execute_notebook.call_args
        assert (
            call_args.kwargs["template_path"]
            == "notebooks/templates/optimization_analysis.py"
        )
        assert call_args.kwargs["strategy_name"] is None

        # パラメータ内容確認
        params = call_args.kwargs["parameters"]
        assert "results_json_path" in params
        assert params["strategy_name"] == "test_strategy"
        assert params["n_combinations"] == 9

        # 戻り値確認
        assert result_path == str(output_path)

    @patch("src.domains.backtest.core.marimo_executor.MarimoExecutor")
    @patch("src.domains.optimization.notebook_generator.os.remove")
    def test_json_file_creation_marimo(
        self,
        mock_remove,
        mock_executor_class,
        sample_optimization_results,
        sample_parameter_ranges,
        sample_scoring_weights,
        tmp_path,
    ):
        """JSONファイルが正しく作成されるか確認（Marimo）"""
        mock_executor = MagicMock()
        mock_executor_class.return_value = mock_executor

        output_dir = tmp_path / "notebooks/generated/optimization/test_strategy"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "test_output.ipynb"
        mock_executor.execute_notebook.return_value = output_path

        # Notebook生成実行
        generate_optimization_notebook(
            results=sample_optimization_results,
            output_path=str(output_path),
            strategy_name="test_strategy",
            parameter_ranges=sample_parameter_ranges,
            scoring_weights=sample_scoring_weights,
            n_combinations=9,
            _skip_path_validation=True,
        )

        # JSONファイルが作成されたか確認
        json_files = list(output_dir.glob("data_*.json"))
        assert len(json_files) == 1

        # JSON内容確認
        with open(json_files[0], encoding="utf-8") as f:
            loaded_data = json.load(f)

        assert len(loaded_data) == 3
        assert loaded_data[0]["score"] == 0.85

    @patch("src.domains.backtest.core.marimo_executor.MarimoExecutor")
    def test_parameter_passing_marimo(
        self,
        mock_executor_class,
        sample_optimization_results,
        sample_parameter_ranges,
        sample_scoring_weights,
        tmp_path,
    ):
        """パラメータが正しく渡されるか確認（Marimo）"""
        mock_executor = MagicMock()
        mock_executor_class.return_value = mock_executor

        output_path = tmp_path / "test_output.ipynb"
        mock_executor.execute_notebook.return_value = output_path

        generate_optimization_notebook(
            results=sample_optimization_results,
            output_path=str(output_path),
            strategy_name="range_break_v6",
            parameter_ranges=sample_parameter_ranges,
            scoring_weights=sample_scoring_weights,
            n_combinations=27,
            _skip_path_validation=True,
        )

        # パラメータ確認
        call_args = mock_executor.execute_notebook.call_args
        params = call_args.kwargs["parameters"]

        assert params["strategy_name"] == "range_break_v6"
        assert params["n_combinations"] == 27
        assert params["parameter_ranges"] == sample_parameter_ranges
        assert params["scoring_weights"] == sample_scoring_weights
        assert "results_json_path" in params

    @patch("src.domains.backtest.core.marimo_executor.MarimoExecutor")
    def test_error_handling_marimo(
        self,
        mock_executor_class,
        sample_optimization_results,
        sample_parameter_ranges,
        sample_scoring_weights,
        tmp_path,
    ):
        """エラーハンドリングのテスト（Marimo）"""
        mock_executor = MagicMock()
        mock_executor_class.return_value = mock_executor

        # execute_notebookがエラーを発生させる
        mock_executor.execute_notebook.side_effect = Exception(
            "Marimo execution failed"
        )

        output_path = tmp_path / "test_output.ipynb"

        # エラーが伝播することを確認
        with pytest.raises(Exception, match="Marimo execution failed"):
            generate_optimization_notebook(
                results=sample_optimization_results,
                output_path=str(output_path),
                strategy_name="test_strategy",
                parameter_ranges=sample_parameter_ranges,
                scoring_weights=sample_scoring_weights,
                n_combinations=9,
                _skip_path_validation=True,
            )
