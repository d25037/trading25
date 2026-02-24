"""BacktestExecutorMixin の実行経路テスト."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import vectorbt as vbt

from src.shared.models.allocation import AllocationInfo
from src.shared.models.signals import SignalParams, Signals
from src.shared.models.signals.fundamental import FundamentalSignalParams
from src.shared.models.signals.macro import MarginSignalParams
from src.shared.models.signals.sector import SectorStrengthRankingParams
from src.domains.strategy.core.mixins.backtest_executor_mixin import (
    BacktestExecutorMixin,
    _any_signal_enabled,
    _is_signal_enabled,
)


def _ohlcv_df(start: str = "2020-01-01", periods: int = 4) -> pd.DataFrame:
    idx = pd.date_range(start, periods=periods, freq="D")
    return pd.DataFrame(
        {
            "Open": [10.0, 11.0, 12.0, 13.0],
            "High": [11.0, 12.0, 13.0, 14.0],
            "Low": [9.0, 10.0, 11.0, 12.0],
            "Close": [10.5, 11.5, 12.5, 13.5],
            "Volume": [100.0, 200.0, 300.0, 400.0],
        },
        index=idx,
    )


def _margin_df(index: pd.DatetimeIndex) -> pd.DataFrame:
    return pd.DataFrame({"margin_balance": [1.0, 2.0, 3.0, 4.0]}, index=index)


def _statements_df(index: pd.DatetimeIndex) -> pd.DataFrame:
    return pd.DataFrame({"EPS": [1.0, 1.1, 1.2, 1.3]}, index=index)


def _signals(index: pd.DatetimeIndex) -> Signals:
    entries = pd.Series([True, True, False, False], index=index, dtype=bool)
    exits = pd.Series([False, False, False, True], index=index, dtype=bool)
    return Signals(entries=entries, exits=exits)


class _RuntimeStrategy(BacktestExecutorMixin):
    def __init__(self) -> None:
        self.stock_codes = ["1111", "2222"]
        self.stock_code = "1111"
        self.initial_cash = 1_000_000.0
        self.fees = 0.001
        self.spread = 0.0
        self.borrow_fee = 0.0
        self.slippage = 0.0
        self.max_concurrent_positions = None
        self.max_exposure = None
        self.cash_sharing = True
        self.group_by = True
        self.direction = "longonly"
        self.printlog = False
        self.relative_mode = False
        self.dataset = "primeExTopix500"
        self.start_date = None
        self.end_date = None
        self.timeframe = "daily"
        self.include_margin_data = False
        self.include_statements_data = False
        self.entry_filter_params = None
        self.exit_trigger_params = None
        self.benchmark_table = "topix"
        self.benchmark_data = None
        self.relative_data_dict = None
        self.execution_data_dict = None
        self.multi_data_dict = None
        self.combined_portfolio = None
        self.portfolio = None
        self._grouped_portfolio_inputs_cache = None
        self.kelly_fraction = 0.5
        self.min_allocation = 0.01
        self.max_allocation = 0.5
        self.logs: list[tuple[str, str]] = []
        self._mock_multi_data: dict[str, dict[str, pd.DataFrame]] = {}
        self._mock_relative_data: tuple[
            dict[str, dict[str, pd.DataFrame]],
            dict[str, dict[str, pd.DataFrame]],
        ] = ({}, {})
        self._next_signals = {}

    def _log(self, message: str, level: str = "info") -> None:
        self.logs.append((level, message))

    def load_multi_data(self) -> dict[str, dict[str, pd.DataFrame]]:
        return self._mock_multi_data

    def load_relative_data(
        self,
    ) -> tuple[dict[str, dict[str, pd.DataFrame]], dict[str, dict[str, pd.DataFrame]]]:
        return self._mock_relative_data

    def load_benchmark_data(self) -> pd.DataFrame:
        self.benchmark_data = _ohlcv_df()[["Open", "High", "Low", "Close"]]
        return self.benchmark_data

    def generate_multi_signals(self, stock_code: str, data: pd.DataFrame, **kwargs) -> Signals:
        if stock_code in self._next_signals:
            return self._next_signals[stock_code]
        return _signals(data.index)


class TestBacktestExecutorMixinPaths:
    def test_signal_helpers(self) -> None:
        params = SignalParams(margin=MarginSignalParams(enabled=True))
        assert _is_signal_enabled(params, "margin") is True
        assert _is_signal_enabled(params, "fundamental") is False
        assert _is_signal_enabled(None, "margin") is False
        assert (
            _any_signal_enabled(params, None, ("margin",))
            == "entry_filter_params.margin"
        )
        assert _any_signal_enabled(None, None, ("margin",)) is None

    def test_find_data_requirement(self) -> None:
        strategy = _RuntimeStrategy()
        strategy.entry_filter_params = SignalParams(
            fundamental=FundamentalSignalParams(
                enabled=True,
                per={"enabled": True, "threshold": 15.0, "condition": "below"},
            )
        )
        strategy.exit_trigger_params = SignalParams(
            margin=MarginSignalParams(enabled=True)
        )
        assert strategy._find_signal_for_data_requirement("statements")
        assert strategy._find_signal_for_data_requirement("margin")
        assert strategy._find_signal_for_data_requirement("not_exists") is None

    def test_calculate_cost_params_for_short(self) -> None:
        strategy = _RuntimeStrategy()
        strategy.spread = 0.0005
        strategy.borrow_fee = 0.0007
        strategy.direction = "both"
        fees, slippage = strategy._calculate_cost_params()
        assert fees == pytest.approx(0.0022)
        assert slippage == 0.0

    def test_cache_helpers(self) -> None:
        strategy = _RuntimeStrategy()
        close = pd.DataFrame({"1111": [1.0]})
        entries = pd.DataFrame({"1111": [True]})
        exits = pd.DataFrame({"1111": [False]})
        strategy._set_grouped_portfolio_inputs_cache(close, entries, exits)
        cached = strategy._get_grouped_portfolio_inputs_cache()
        assert cached is not None
        assert cached[0].equals(close)
        strategy._grouped_portfolio_inputs_cache = ("bad", "bad", "bad")
        assert strategy._get_grouped_portfolio_inputs_cache() is None
        strategy._clear_grouped_portfolio_inputs_cache()
        assert strategy._get_grouped_portfolio_inputs_cache() is None

    def test_create_grouped_portfolio_multi_and_single(self) -> None:
        strategy = _RuntimeStrategy()
        close = pd.DataFrame({"1111": [1.0, 2.0], "2222": [3.0, 4.0]})
        entries = pd.DataFrame({"1111": [True, False], "2222": [True, False]})
        exits = pd.DataFrame({"1111": [False, True], "2222": [False, True]})
        strategy.max_exposure = 0.2

        with patch.object(vbt.Portfolio, "from_signals", return_value="pf-multi") as mocked:
            out = strategy._create_grouped_portfolio(close, entries, exits, allocation_pct=0.3)
            assert out == "pf-multi"
            kwargs = mocked.call_args.kwargs
            assert kwargs["size"] == pytest.approx(0.3)
            assert kwargs["max_size"] == pytest.approx(0.2)

        strategy.stock_codes = ["1111"]
        strategy.cash_sharing = False
        with patch.object(vbt.Portfolio, "from_signals", return_value="pf-single") as mocked:
            out = strategy._create_grouped_portfolio(close[["1111"]], entries[["1111"]], exits[["1111"]])
            assert out == "pf-single"
            kwargs = mocked.call_args.kwargs
            assert kwargs["group_by"] is None

    def test_run_multi_backtest_from_cached_signals(self) -> None:
        strategy = _RuntimeStrategy()
        with pytest.raises(ValueError):
            strategy.run_multi_backtest_from_cached_signals(0.2)

        close = pd.DataFrame({"1111": [1.0], "2222": [2.0]})
        entries = pd.DataFrame({"1111": [True], "2222": [False]})
        exits = pd.DataFrame({"1111": [False], "2222": [True]})
        strategy._set_grouped_portfolio_inputs_cache(close, entries, exits)
        with patch.object(strategy, "_create_grouped_portfolio", return_value="cached-pf") as mocked:
            out = strategy.run_multi_backtest_from_cached_signals(0.15)
            assert out == "cached-pf"
            assert strategy.combined_portfolio == "cached-pf"
            mocked.assert_called_once()

    def test_run_multi_backtest_grouped_standard_mode(self) -> None:
        strategy = _RuntimeStrategy()
        strategy.max_concurrent_positions = 1
        strategy._mock_multi_data = {
            "1111": {"daily": _ohlcv_df()},
            "2222": {"daily": _ohlcv_df()},
        }

        with patch.object(vbt.Portfolio, "from_signals", return_value="pf"):
            portfolio, all_entries = strategy.run_multi_backtest()

        assert portfolio == "pf"
        assert all_entries is not None
        assert isinstance(all_entries, pd.DataFrame)
        assert strategy._grouped_portfolio_inputs_cache is not None

    def test_run_multi_backtest_grouped_with_sector_and_benchmark(self, monkeypatch) -> None:
        strategy = _RuntimeStrategy()
        strategy.entry_filter_params = SignalParams(
            sector_strength_ranking=SectorStrengthRankingParams(enabled=True)
        )
        strategy._mock_multi_data = {
            "1111": {"daily": _ohlcv_df()},
            "2222": {"daily": _ohlcv_df()},
        }

        monkeypatch.setattr(
            "src.infrastructure.data_access.loaders.sector_loaders.load_all_sector_indices",
            lambda *_args, **_kwargs: {"Tech": _ohlcv_df()},
        )
        monkeypatch.setattr(
            "src.infrastructure.data_access.loaders.sector_loaders.get_stock_sector_mapping",
            lambda *_args, **_kwargs: {"1111": "Tech", "2222": "Tech"},
        )

        with patch.object(vbt.Portfolio, "from_signals", return_value="pf"):
            portfolio, _ = strategy.run_multi_backtest()
        assert portfolio == "pf"
        assert strategy.benchmark_data is not None

    def test_run_multi_backtest_relative_mode(self) -> None:
        strategy = _RuntimeStrategy()
        strategy.stock_codes = ["1111"]
        strategy.relative_mode = True
        strategy.include_margin_data = True
        strategy.include_statements_data = True
        relative_df = _ohlcv_df()
        execution_df = _ohlcv_df()
        strategy._mock_relative_data = (
            {"1111": {"daily": relative_df}},
            {
                "1111": {
                    "daily": execution_df,
                    "margin_daily": _margin_df(execution_df.index),
                    "statements_daily": _statements_df(execution_df.index),
                }
            },
        )
        strategy._next_signals = {"1111": _signals(relative_df.index)}

        with patch.object(vbt.Portfolio, "from_signals", return_value="pf-relative"):
            portfolio, all_entries = strategy.run_multi_backtest()

        assert portfolio == "pf-relative"
        assert all_entries is not None

    def test_run_multi_backtest_missing_codes_and_empty_data(self) -> None:
        strategy = _RuntimeStrategy()
        strategy.stock_codes = ["1111", "9999"]
        strategy._mock_multi_data = {"1111": {"daily": _ohlcv_df()}}

        with patch.object(vbt.Portfolio, "from_signals", return_value="pf"):
            portfolio, _ = strategy.run_multi_backtest()
        assert portfolio == "pf"
        assert strategy.stock_codes == ["1111"]

        strategy2 = _RuntimeStrategy()
        strategy2.stock_codes = ["9999"]
        strategy2._mock_multi_data = {}
        with pytest.raises(ValueError):
            strategy2.run_multi_backtest()

    def test_run_multi_backtest_individual_mode(self) -> None:
        strategy = _RuntimeStrategy()
        strategy.group_by = False
        strategy.max_exposure = 0.4
        strategy._mock_multi_data = {
            "1111": {"daily": _ohlcv_df()},
            "2222": {"daily": _ohlcv_df()},
        }
        with patch.object(vbt.Portfolio, "from_signals", return_value="pf-individual"):
            portfolio, all_entries = strategy.run_multi_backtest()
        assert portfolio == "pf-individual"
        assert all_entries is None

    def test_limit_entries_per_day(self) -> None:
        entries = pd.DataFrame(
            {"1111": [True, True], "2222": [True, False], "3333": [True, True]}
        )
        limited = BacktestExecutorMixin._limit_entries_per_day(entries, 2)
        assert limited.iloc[0].sum() == 2
        assert limited.iloc[1].sum() <= 2
        assert BacktestExecutorMixin._limit_entries_per_day(entries, 0).equals(entries)

    def test_run_optimized_backtest_success_and_failure(self) -> None:
        strategy = _RuntimeStrategy()
        initial_pf = MagicMock()
        final_pf = MagicMock()
        entries = pd.DataFrame({"1111": [True]})
        with patch.object(
            strategy,
            "run_optimized_backtest_kelly",
            return_value=(
                initial_pf,
                final_pf,
                0.2,
                {
                    "win_rate": 0.5,
                    "avg_win": 1.0,
                    "avg_loss": 0.5,
                    "total_trades": 10,
                    "kelly": 0.4,
                },
                entries,
            ),
            create=True,
        ):
            got_initial, got_final, alloc = strategy.run_optimized_backtest()
            assert got_initial is initial_pf
            assert got_final is final_pf
            assert isinstance(alloc, AllocationInfo)
            assert strategy.all_entries is entries

        with patch.object(
            strategy,
            "run_optimized_backtest_kelly",
            side_effect=Exception("boom"),
            create=True,
        ):
            with pytest.raises(RuntimeError):
                strategy.run_optimized_backtest()
