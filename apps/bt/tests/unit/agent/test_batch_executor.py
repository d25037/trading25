"""batch_executor.py のテスト"""

from unittest.mock import MagicMock, patch

from src.domains.lab_agent.evaluator.batch_executor import (
    fetch_benchmark_data,
    fetch_ohlcv_data,
    fetch_stock_codes,
    get_max_workers,
    prepare_batch_data,
)
from src.domains.lab_agent.models import StrategyCandidate


class TestGetMaxWorkers:
    def test_positive_value(self):
        assert get_max_workers(4) == 4

    def test_one(self):
        assert get_max_workers(1) == 1

    def test_negative_one_returns_none(self):
        result = get_max_workers(-1)
        assert result is None


class TestFetchStockCodes:
    def test_no_dataset_returns_none(self):
        result = fetch_stock_codes({})
        assert result is None

    def test_success(self):
        with patch("src.domains.lab_agent.evaluator.batch_executor.get_stock_list") as mock_get:
            mock_get.return_value = ["1234", "5678"]
            result = fetch_stock_codes({"dataset": "test_dataset"})
        assert result == ["1234", "5678"]

    def test_exception_returns_none(self):
        with patch("src.domains.lab_agent.evaluator.batch_executor.get_stock_list") as mock_get:
            mock_get.side_effect = Exception("API error")
            result = fetch_stock_codes({"dataset": "test_dataset"})
        assert result is None


class TestFetchOhlcvData:
    def test_no_stock_codes_returns_none(self):
        result = fetch_ohlcv_data({"dataset": "test"}, None)
        assert result is None

    def test_no_dataset_returns_none(self):
        result = fetch_ohlcv_data({}, ["1234"])
        assert result is None

    def test_success(self):
        with (
            patch("src.domains.lab_agent.evaluator.batch_executor.prepare_multi_data") as mock_prep,
            patch("src.domains.lab_agent.evaluator.batch_executor.convert_dataframes_to_dict") as mock_conv,
        ):
            mock_prep.return_value = {"1234": MagicMock()}
            mock_conv.return_value = {"1234": {"data": "test"}}
            result = fetch_ohlcv_data(
                {"dataset": "test", "start_date": "2025-01-01", "end_date": "2025-12-31"},
                ["1234"],
            )
        assert result is not None
        assert "1234" in result

    def test_exception_returns_none(self):
        with patch("src.domains.lab_agent.evaluator.batch_executor.prepare_multi_data") as mock_prep:
            mock_prep.side_effect = Exception("data error")
            result = fetch_ohlcv_data({"dataset": "test"}, ["1234"])
        assert result is None


class TestFetchBenchmarkData:
    def test_no_dataset_returns_none(self):
        result = fetch_benchmark_data({})
        assert result is None

    def test_success(self):
        import pandas as pd

        mock_df = pd.DataFrame(
            {"Close": [100.0, 101.0]},
            index=pd.date_range("2025-01-01", periods=2),
        )
        with patch("src.domains.lab_agent.evaluator.batch_executor.load_topix_data") as mock_load:
            mock_load.return_value = mock_df
            result = fetch_benchmark_data({"dataset": "test"})
        assert result is not None
        assert "index" in result
        assert "columns" in result
        assert "data" in result

    def test_exception_returns_none(self):
        with patch("src.domains.lab_agent.evaluator.batch_executor.load_topix_data") as mock_load:
            mock_load.side_effect = Exception("load error")
            result = fetch_benchmark_data({"dataset": "test"})
        assert result is None


class TestPrepareBatchData:
    def test_integration(self):
        with (
            patch("src.domains.lab_agent.evaluator.batch_executor.fetch_stock_codes") as mock_codes,
            patch("src.domains.lab_agent.evaluator.batch_executor.fetch_ohlcv_data") as mock_ohlcv,
            patch("src.domains.lab_agent.evaluator.batch_executor.fetch_benchmark_data") as mock_bench,
        ):
            mock_codes.return_value = ["1234"]
            mock_ohlcv.return_value = {"1234": {"data": "test"}}
            mock_bench.return_value = {"index": [], "columns": [], "data": []}
            result = prepare_batch_data({"dataset": "test"})
        assert result.stock_codes == ["1234"]
        assert result.ohlcv_data is not None
        assert result.benchmark_data is not None

    def test_integration_enables_forecast_revision_for_forecast_signals(self):
        with (
            patch("src.domains.lab_agent.evaluator.batch_executor.fetch_stock_codes") as mock_codes,
            patch("src.domains.lab_agent.evaluator.batch_executor.fetch_ohlcv_data") as mock_ohlcv,
            patch("src.domains.lab_agent.evaluator.batch_executor.fetch_benchmark_data") as mock_bench,
        ):
            mock_codes.return_value = ["1234"]
            mock_ohlcv.return_value = {"1234": {"data": "test"}}
            mock_bench.return_value = {"index": [], "columns": [], "data": []}

            candidate = StrategyCandidate(
                strategy_id="c1",
                entry_filter_params={
                    "fundamental": {
                        "enabled": True,
                        "forward_eps_growth": {"enabled": True},
                    }
                },
                exit_trigger_params={},
            )

            prepare_batch_data({"dataset": "test"}, [candidate])

        assert mock_ohlcv.call_args.kwargs["include_forecast_revision"] is True

    def test_integration_enables_forecast_revision_for_forward_dividend_signal(self):
        with (
            patch("src.domains.lab_agent.evaluator.batch_executor.fetch_stock_codes") as mock_codes,
            patch("src.domains.lab_agent.evaluator.batch_executor.fetch_ohlcv_data") as mock_ohlcv,
            patch("src.domains.lab_agent.evaluator.batch_executor.fetch_benchmark_data") as mock_bench,
        ):
            mock_codes.return_value = ["1234"]
            mock_ohlcv.return_value = {"1234": {"data": "test"}}
            mock_bench.return_value = {"index": [], "columns": [], "data": []}

            candidate = StrategyCandidate(
                strategy_id="c2",
                entry_filter_params={
                    "fundamental": {
                        "enabled": True,
                        "forward_dividend_growth": {"enabled": True},
                    }
                },
                exit_trigger_params={},
            )

            prepare_batch_data({"dataset": "test"}, [candidate])

        assert mock_ohlcv.call_args.kwargs["include_forecast_revision"] is True
