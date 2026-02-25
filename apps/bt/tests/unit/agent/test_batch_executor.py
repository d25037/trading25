"""batch_executor.py のテスト"""

from unittest.mock import MagicMock, patch

from src.domains.lab_agent.evaluator.batch_executor import (
    _is_forecast_signal_enabled,
    _should_include_forecast_revision,
    execute_batch_evaluation,
    execute_parallel,
    execute_single_process,
    fetch_benchmark_data,
    fetch_ohlcv_data,
    fetch_stock_codes,
    get_max_workers,
    handle_future_result,
    prepare_batch_data,
)
from src.domains.lab_agent.models import EvaluationResult, StrategyCandidate


def _candidate(
    strategy_id: str,
    entry_filter_params: dict | None = None,
    exit_trigger_params: dict | None = None,
) -> StrategyCandidate:
    return StrategyCandidate(
        strategy_id=strategy_id,
        entry_filter_params=entry_filter_params or {},
        exit_trigger_params=exit_trigger_params or {},
    )


def _result(candidate: StrategyCandidate, success: bool = True, score: float = 1.0) -> EvaluationResult:
    return EvaluationResult(
        candidate=candidate,
        score=score,
        success=success,
        sharpe_ratio=0.5,
    )


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


class TestForecastRevisionHelpers:
    def test_is_forecast_signal_enabled_handles_invalid_and_disabled_payloads(self):
        assert _is_forecast_signal_enabled({}) is False
        assert _is_forecast_signal_enabled({"fundamental": {}}) is False
        assert _is_forecast_signal_enabled(
            {
                "fundamental": {
                    "enabled": True,
                }
            }
        ) is False

    def test_is_forecast_signal_enabled_detects_supported_signals(self):
        assert _is_forecast_signal_enabled(
            {
                "fundamental": {
                    "enabled": True,
                    "forecast_eps_above_all_actuals": {"enabled": True},
                }
            }
        ) is True
        assert _is_forecast_signal_enabled(
            {
                "fundamental": {
                    "enabled": True,
                    "forward_payout_ratio": {"enabled": True},
                }
            }
        ) is True

    def test_should_include_forecast_revision_checks_entry_and_exit(self):
        assert _should_include_forecast_revision(None) is False
        assert _should_include_forecast_revision([]) is False

        candidate = _candidate(
            "exit-only",
            entry_filter_params={"fundamental": {"enabled": False}},
            exit_trigger_params={
                "fundamental": {
                    "enabled": True,
                    "forecast_eps_above_all_actuals": {"enabled": True},
                }
            },
        )
        assert _should_include_forecast_revision([candidate]) is True


class TestBatchExecutionPaths:
    def test_execute_batch_evaluation_uses_single_process_for_one_worker(self):
        candidates = [_candidate("s1"), _candidate("s2"), _candidate("s3")]
        prepared_data = MagicMock(stock_codes=[], ohlcv_data={}, benchmark_data={})
        with (
            patch(
                "src.domains.lab_agent.evaluator.batch_executor.execute_single_process"
            ) as mock_single,
            patch(
                "src.domains.lab_agent.evaluator.batch_executor.execute_parallel"
            ) as mock_parallel,
        ):
            mock_single.return_value = [_result(candidates[0])]
            result = execute_batch_evaluation(
                candidates,
                max_workers=1,
                prepared_data=prepared_data,
                shared_config_dict={},
                scoring_weights={},
                timeout_seconds=60,
            )
        assert len(result) == 1
        mock_single.assert_called_once()
        mock_parallel.assert_not_called()

    def test_execute_batch_evaluation_uses_single_process_for_small_batch(self):
        candidates = [_candidate("s1"), _candidate("s2")]
        prepared_data = MagicMock(stock_codes=[], ohlcv_data={}, benchmark_data={})
        with (
            patch(
                "src.domains.lab_agent.evaluator.batch_executor.execute_single_process"
            ) as mock_single,
            patch(
                "src.domains.lab_agent.evaluator.batch_executor.execute_parallel"
            ) as mock_parallel,
        ):
            mock_single.return_value = [_result(candidates[0]), _result(candidates[1])]
            execute_batch_evaluation(
                candidates,
                max_workers=4,
                prepared_data=prepared_data,
                shared_config_dict={},
                scoring_weights={},
                timeout_seconds=60,
            )
        mock_single.assert_called_once()
        mock_parallel.assert_not_called()

    def test_execute_batch_evaluation_uses_parallel_for_large_batch(self):
        candidates = [_candidate("s1"), _candidate("s2"), _candidate("s3")]
        prepared_data = MagicMock(stock_codes=[], ohlcv_data={}, benchmark_data={})
        with (
            patch(
                "src.domains.lab_agent.evaluator.batch_executor.execute_single_process"
            ) as mock_single,
            patch(
                "src.domains.lab_agent.evaluator.batch_executor.execute_parallel"
            ) as mock_parallel,
        ):
            mock_parallel.return_value = [_result(candidates[0])]
            execute_batch_evaluation(
                candidates,
                max_workers=2,
                prepared_data=prepared_data,
                shared_config_dict={},
                scoring_weights={},
                timeout_seconds=60,
            )
        mock_parallel.assert_called_once()
        mock_single.assert_not_called()

    def test_execute_single_process_collects_results(self):
        candidates = [_candidate("s1"), _candidate("s2")]
        prepared_data = MagicMock(stock_codes=["1234"], ohlcv_data={"1234": {}}, benchmark_data={})
        with patch(
            "src.domains.lab_agent.evaluator.batch_executor.evaluate_single_candidate"
        ) as mock_eval:
            mock_eval.side_effect = [
                _result(candidates[0], success=True, score=1.2),
                _result(candidates[1], success=False, score=-1.0),
            ]
            result = execute_single_process(
                candidates,
                prepared_data=prepared_data,
                shared_config_dict={"dataset": "test"},
                scoring_weights={"sharpe_ratio": 1.0},
            )
        assert len(result) == 2
        assert result[0].success is True
        assert result[1].success is False

    def test_execute_parallel_collects_future_results(self):
        candidates = [_candidate("s1"), _candidate("s2")]
        prepared_data = MagicMock(stock_codes=["1234"], ohlcv_data={"1234": {}}, benchmark_data={})

        class _Future:
            def __init__(self, key: str):
                self.key = key

            def __hash__(self) -> int:
                return hash(self.key)

        class _Executor:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def submit(self, *_args, **_kwargs):
                candidate = _args[1]
                return _Future(candidate.strategy_id)

        with (
            patch(
                "src.domains.lab_agent.evaluator.batch_executor.ProcessPoolExecutor",
                return_value=_Executor(),
            ),
            patch(
                "src.domains.lab_agent.evaluator.batch_executor.as_completed",
                side_effect=lambda futures: list(futures),
            ),
            patch(
                "src.domains.lab_agent.evaluator.batch_executor.handle_future_result"
            ) as mock_handle,
        ):
            mock_handle.side_effect = [
                _result(candidates[0], score=1.1),
                _result(candidates[1], score=1.0),
            ]
            results = execute_parallel(
                candidates,
                max_workers=2,
                prepared_data=prepared_data,
                shared_config_dict={"dataset": "test"},
                scoring_weights={"sharpe_ratio": 1.0},
                timeout_seconds=60,
            )
        assert len(results) == 2
        assert mock_handle.call_count == 2

    def test_handle_future_result_covers_success_timeout_and_error(self):
        candidate = _candidate("s1")

        class _FutureSuccess:
            def result(self, timeout):
                assert timeout == 30
                return _result(candidate, success=True, score=2.0)

        class _FutureFailure:
            def result(self, timeout):
                assert timeout == 30
                return _result(candidate, success=False, score=-1.0)

        class _FutureTimeout:
            def result(self, timeout):
                assert timeout == 30
                raise TimeoutError

        class _FutureError:
            def result(self, timeout):
                assert timeout == 30
                raise RuntimeError("boom")

        success = handle_future_result(_FutureSuccess(), candidate, 1, 4, 30)
        failure = handle_future_result(_FutureFailure(), candidate, 2, 4, 30)
        timeout = handle_future_result(_FutureTimeout(), candidate, 3, 4, 30)
        error = handle_future_result(_FutureError(), candidate, 4, 4, 30)

        assert success.success is True
        assert failure.success is False
        assert timeout.success is False
        assert timeout.error_message == "Timeout"
        assert error.success is False
        assert error.error_message == "boom"

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

    def test_integration_enables_forecast_revision_for_forecast_vs_actual_signal(self):
        with (
            patch("src.domains.lab_agent.evaluator.batch_executor.fetch_stock_codes") as mock_codes,
            patch("src.domains.lab_agent.evaluator.batch_executor.fetch_ohlcv_data") as mock_ohlcv,
            patch("src.domains.lab_agent.evaluator.batch_executor.fetch_benchmark_data") as mock_bench,
        ):
            mock_codes.return_value = ["1234"]
            mock_ohlcv.return_value = {"1234": {"data": "test"}}
            mock_bench.return_value = {"index": [], "columns": [], "data": []}

            candidate = StrategyCandidate(
                strategy_id="c3",
                entry_filter_params={
                    "fundamental": {
                        "enabled": True,
                        "forecast_eps_above_all_actuals": {"enabled": True},
                    }
                },
                exit_trigger_params={},
            )

            prepare_batch_data({"dataset": "test"}, [candidate])

        assert mock_ohlcv.call_args.kwargs["include_forecast_revision"] is True
