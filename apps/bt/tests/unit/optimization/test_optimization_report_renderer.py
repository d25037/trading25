"""パラメータ最適化HTML生成機能のテスト。"""

import json
import os
from pathlib import Path

import pytest

from src.domains.optimization.optimization_report_renderer import (
    _save_results_as_json,
    generate_optimization_report,
)


@pytest.fixture
def sample_optimization_results():
    """サンプル最適化結果"""
    return [
        {
            "params": {
                "entry_filter_params.period_extrema_break.period": 50,
                "entry_filter_params.volume_ratio_above.ratio_threshold": 2.0,
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
                "entry_filter_params.period_extrema_break.period": 100,
                "entry_filter_params.volume_ratio_above.ratio_threshold": 1.5,
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
                "entry_filter_params.period_extrema_break.period": 200,
                "entry_filter_params.volume_ratio_above.ratio_threshold": 2.5,
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
            "period_extrema_break": {"period": [50, 100, 200]},
            "volume_ratio_above": {"ratio_threshold": [1.5, 2.0, 2.5]},
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
            loaded_data[0]["params"]["entry_filter_params.period_extrema_break.period"]
            == 50
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
                "params": {"entry_filter_params.period_extrema_break.period": 50},
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


class TestOptimizationReportRendererStaticHtml:
    """静的HTML生成機能のテスト"""

    def test_generate_report_writes_static_html(
        self,
        sample_optimization_results,
        sample_parameter_ranges,
        sample_scoring_weights,
        tmp_path,
    ):
        """最適化結果を静的HTMLとして保存する"""
        output_path = tmp_path / "test_output.html"

        result_path = generate_optimization_report(
            results=sample_optimization_results,
            output_path=str(output_path),
            strategy_name="test_strategy",
            parameter_ranges=sample_parameter_ranges,
            scoring_weights=sample_scoring_weights,
            n_combinations=9,
            _skip_path_validation=True,
        )

        html = output_path.read_text(encoding="utf-8")
        assert result_path == str(output_path)
        assert "Optimization Analysis" in html
        assert "test_strategy" in html
        assert "entry_filter_params.period_extrema_break.period" in html
        assert "trade_count" in html
        assert "0.8500" in html

    def test_json_file_is_removed_after_generation(
        self,
        sample_optimization_results,
        sample_parameter_ranges,
        sample_scoring_weights,
        tmp_path,
    ):
        """HTML生成後に一時JSONを削除する"""
        output_dir = tmp_path / "optimization/test_strategy"
        output_path = output_dir / "test_output.html"

        generate_optimization_report(
            results=sample_optimization_results,
            output_path=str(output_path),
            strategy_name="test_strategy",
            parameter_ranges=sample_parameter_ranges,
            scoring_weights=sample_scoring_weights,
            n_combinations=9,
            _skip_path_validation=True,
        )

        assert output_path.exists()
        assert list(output_dir.glob("data_*.json")) == []

    def test_parameter_metadata_is_rendered(
        self,
        sample_optimization_results,
        sample_parameter_ranges,
        sample_scoring_weights,
        tmp_path,
    ):
        """strategy / combination / scoring metadataをHTMLへ埋め込む"""
        output_path = tmp_path / "test_output.html"

        generate_optimization_report(
            results=sample_optimization_results,
            output_path=str(output_path),
            strategy_name="range_break_v6",
            parameter_ranges=sample_parameter_ranges,
            scoring_weights=sample_scoring_weights,
            n_combinations=27,
            _skip_path_validation=True,
        )

        html = output_path.read_text(encoding="utf-8")
        assert "range_break_v6" in html
        assert "27" in html
        assert "sharpe_ratio" in html
        assert "ratio_threshold" in html

    def test_write_error_propagates_and_cleans_temp_json(
        self,
        monkeypatch,
        sample_optimization_results,
        sample_parameter_ranges,
        sample_scoring_weights,
        tmp_path,
    ):
        """HTML書き込みエラーを伝播し、一時JSONを削除する"""
        output_path = tmp_path / "test_output.html"

        def fail_write_text(self, *args, **kwargs):  # noqa: ANN001, ANN002
            _ = (self, args, kwargs)
            raise OSError("write failed")

        monkeypatch.setattr(Path, "write_text", fail_write_text)

        with pytest.raises(OSError, match="write failed"):
            generate_optimization_report(
                results=sample_optimization_results,
                output_path=str(output_path),
                strategy_name="test_strategy",
                parameter_ranges=sample_parameter_ranges,
                scoring_weights=sample_scoring_weights,
                n_combinations=9,
                _skip_path_validation=True,
            )

        assert list(tmp_path.glob("data_*.json")) == []
