"""registry.py データチェックヘルパー関数のテスト"""

import pandas as pd

from src.models.signals import SignalParams

from src.strategies.signals.registry import (
    SIGNAL_REGISTRY,
    _has_benchmark_data,
    _has_margin_data,
    _has_sector_data,
    _has_sector_data_and_benchmark,
    _has_statements_column,
    _has_statements_columns,
    _has_stock_sector_close,
    _has_stock_sector_close_and_benchmark,
)


class TestHasStatementsColumn:
    def test_valid(self) -> None:
        df = pd.DataFrame({"EPS": [1.0, 2.0]})
        assert _has_statements_column({"statements_data": df}, "EPS")

    def test_missing_key(self) -> None:
        assert not _has_statements_column({}, "EPS")

    def test_none_data(self) -> None:
        assert not _has_statements_column({"statements_data": None}, "EPS")

    def test_empty_df(self) -> None:
        df = pd.DataFrame()
        assert not _has_statements_column({"statements_data": df}, "EPS")

    def test_column_not_present(self) -> None:
        df = pd.DataFrame({"ROE": [0.1]})
        assert not _has_statements_column({"statements_data": df}, "EPS")

    def test_all_nan(self) -> None:
        df = pd.DataFrame({"EPS": [float("nan")]})
        assert not _has_statements_column({"statements_data": df}, "EPS")


class TestHasStatementsColumns:
    def test_all_present(self) -> None:
        df = pd.DataFrame({"A": [1.0], "B": [2.0]})
        assert _has_statements_columns({"statements_data": df}, "A", "B")

    def test_one_missing(self) -> None:
        df = pd.DataFrame({"A": [1.0]})
        assert not _has_statements_columns({"statements_data": df}, "A", "B")

    def test_none_data(self) -> None:
        assert not _has_statements_columns({"statements_data": None}, "A")

    def test_missing_key(self) -> None:
        assert not _has_statements_columns({}, "A")


class TestHasBenchmarkData:
    def test_valid(self) -> None:
        df = pd.DataFrame({"Close": [100.0, 101.0]})
        assert _has_benchmark_data({"benchmark_data": df})

    def test_missing(self) -> None:
        assert not _has_benchmark_data({})

    def test_none(self) -> None:
        assert not _has_benchmark_data({"benchmark_data": None})

    def test_empty(self) -> None:
        assert not _has_benchmark_data({"benchmark_data": pd.DataFrame()})

    def test_no_close(self) -> None:
        df = pd.DataFrame({"Open": [100.0]})
        assert not _has_benchmark_data({"benchmark_data": df})


class TestHasMarginData:
    def test_valid(self) -> None:
        df = pd.DataFrame({"margin_balance": [1000.0]})
        assert _has_margin_data({"margin_data": df})

    def test_missing(self) -> None:
        assert not _has_margin_data({})

    def test_none(self) -> None:
        assert not _has_margin_data({"margin_data": None})


class TestHasSectorData:
    def test_valid(self) -> None:
        d = {"sector_data": {"電気機器": pd.DataFrame()}, "stock_sector_name": "電気機器"}
        assert _has_sector_data(d)

    def test_missing_sector_name(self) -> None:
        d = {"sector_data": {"電気機器": pd.DataFrame()}}
        assert not _has_sector_data(d)

    def test_sector_not_in_data(self) -> None:
        d = {"sector_data": {"電気機器": pd.DataFrame()}, "stock_sector_name": "食品"}
        assert not _has_sector_data(d)

    def test_empty_sector_data(self) -> None:
        d = {"sector_data": {}, "stock_sector_name": "電気機器"}
        assert not _has_sector_data(d)


class TestHasStockSectorClose:
    def test_valid(self) -> None:
        df = pd.DataFrame({"Close": [100.0]})
        d = {"sector_data": {"電気機器": df}, "stock_sector_name": "電気機器"}
        assert _has_stock_sector_close(d)

    def test_no_close_column(self) -> None:
        df = pd.DataFrame({"Open": [100.0]})
        d = {"sector_data": {"電気機器": df}, "stock_sector_name": "電気機器"}
        assert not _has_stock_sector_close(d)

    def test_no_sector_data(self) -> None:
        assert not _has_stock_sector_close({})


class TestHasSectorDataAndBenchmark:
    def test_both_present(self) -> None:
        d = {
            "sector_data": {"電気機器": pd.DataFrame()},
            "stock_sector_name": "電気機器",
            "benchmark_data": pd.DataFrame({"Close": [100.0]}),
        }
        assert _has_sector_data_and_benchmark(d)

    def test_missing_benchmark(self) -> None:
        d = {"sector_data": {"電気機器": pd.DataFrame()}, "stock_sector_name": "電気機器"}
        assert not _has_sector_data_and_benchmark(d)


class TestHasStockSectorCloseAndBenchmark:
    def test_both_present(self) -> None:
        df = pd.DataFrame({"Close": [100.0]})
        d = {
            "sector_data": {"電気機器": df},
            "stock_sector_name": "電気機器",
            "benchmark_data": pd.DataFrame({"Close": [100.0]}),
        }
        assert _has_stock_sector_close_and_benchmark(d)

    def test_missing_close(self) -> None:
        df = pd.DataFrame({"Open": [100.0]})
        d = {
            "sector_data": {"電気機器": df},
            "stock_sector_name": "電気機器",
            "benchmark_data": pd.DataFrame({"Close": [100.0]}),
        }
        assert not _has_stock_sector_close_and_benchmark(d)


class TestSignalRegistry:
    def test_registry_not_empty(self) -> None:
        assert len(SIGNAL_REGISTRY) > 0

    def test_all_have_required_fields(self) -> None:
        for sig in SIGNAL_REGISTRY:
            assert sig.name
            assert sig.category
            assert sig.param_key

    def test_forward_eps_growth_registered(self) -> None:
        """forward_eps_growthがレジストリに登録されていること"""
        matches = [s for s in SIGNAL_REGISTRY if s.param_key == "fundamental.forward_eps_growth"]
        assert len(matches) == 1
        sig = matches[0]
        assert sig.name == "Forward EPS成長率"
        assert "statements:EPS" in sig.data_requirements
        assert "statements:NextYearForecastEPS" in sig.data_requirements

    def test_eps_growth_registered(self) -> None:
        """eps_growth（実績ベース）がレジストリに登録されていること"""
        matches = [s for s in SIGNAL_REGISTRY if s.param_key == "fundamental.eps_growth"]
        assert len(matches) == 1
        sig = matches[0]
        assert sig.name == "EPS成長率"
        assert "statements:EPS" in sig.data_requirements
        assert "statements:NextYearForecastEPS" not in sig.data_requirements

    def test_dividend_per_share_growth_registered(self) -> None:
        matches = [s for s in SIGNAL_REGISTRY if s.param_key == "fundamental.dividend_per_share_growth"]
        assert len(matches) == 1
        sig = matches[0]
        assert sig.name == "1株配当成長率"
        assert sig.category == "fundamental"
        assert sig.data_requirements == ["statements:DividendFY"]

    def test_cfo_yield_growth_registered(self) -> None:
        matches = [s for s in SIGNAL_REGISTRY if s.param_key == "fundamental.cfo_yield_growth"]
        assert len(matches) == 1
        sig = matches[0]
        assert sig.name == "CFO利回り成長率"
        assert sig.category == "fundamental"
        assert "statements:OperatingCashFlow" in sig.data_requirements
        assert "statements:SharesOutstanding" in sig.data_requirements

    def test_simple_fcf_yield_growth_registered(self) -> None:
        matches = [s for s in SIGNAL_REGISTRY if s.param_key == "fundamental.simple_fcf_yield_growth"]
        assert len(matches) == 1
        sig = matches[0]
        assert sig.name == "簡易FCF利回り成長率"
        assert sig.category == "fundamental"
        assert "statements:OperatingCashFlow" in sig.data_requirements
        assert "statements:InvestingCashFlow" in sig.data_requirements
        assert "statements:SharesOutstanding" in sig.data_requirements

    def test_roa_registered(self) -> None:
        """roa（総資産利益率）がレジストリに登録されていること"""
        matches = [s for s in SIGNAL_REGISTRY if s.param_key == "fundamental.roa"]
        assert len(matches) == 1
        sig = matches[0]
        assert sig.name == "ROA"
        assert sig.category == "fundamental"
        assert "statements:ROA" in sig.data_requirements

    def test_market_cap_registered(self) -> None:
        """market_cap（時価総額）がレジストリに登録されていること"""
        matches = [s for s in SIGNAL_REGISTRY if s.param_key == "fundamental.market_cap"]
        assert len(matches) == 1
        sig = matches[0]
        assert sig.name == "時価総額"
        assert sig.category == "fundamental"
        assert "statements:SharesOutstanding" in sig.data_requirements

    def test_market_cap_enabled_checker(self) -> None:
        """market_capのenabled_checkerが正しく動作すること"""
        sig = next(s for s in SIGNAL_REGISTRY if s.param_key == "fundamental.market_cap")

        # fundamental.enabled=False → disabled
        params = SignalParams()
        params.fundamental.enabled = False
        params.fundamental.market_cap.enabled = True
        assert not sig.enabled_checker(params)

        # fundamental.enabled=True, market_cap.enabled=True → enabled
        params.fundamental.enabled = True
        assert sig.enabled_checker(params)

        # fundamental.enabled=True, market_cap.enabled=False → disabled
        params.fundamental.market_cap.enabled = False
        assert not sig.enabled_checker(params)

    def test_cfo_yield_growth_enabled_checker(self) -> None:
        sig = next(s for s in SIGNAL_REGISTRY if s.param_key == "fundamental.cfo_yield_growth")

        params = SignalParams()
        params.fundamental.enabled = False
        params.fundamental.cfo_yield_growth.enabled = True
        assert not sig.enabled_checker(params)

        params.fundamental.enabled = True
        assert sig.enabled_checker(params)

        params.fundamental.cfo_yield_growth.enabled = False
        assert not sig.enabled_checker(params)

    def test_risk_adjusted_return_registered_as_volatility(self) -> None:
        """risk_adjusted_return は独立シグナルとして volatility に分類されること"""
        matches = [s for s in SIGNAL_REGISTRY if s.param_key == "risk_adjusted_return"]
        assert len(matches) == 1
        sig = matches[0]
        assert sig.name == "リスク調整リターン"
        assert sig.category == "volatility"
        assert sig.data_requirements == ["ohlc"]


class TestFundamentalAdjustedSelection:
    def _get_signal(self, param_key: str):
        return next(s for s in SIGNAL_REGISTRY if s.param_key == param_key)

    def test_per_uses_adjusted_by_default(self) -> None:
        params = SignalParams()
        params.fundamental.per.enabled = True

        df = pd.DataFrame(
            {
                "EPS": [1.0],
                "AdjustedEPS": [0.5],
            }
        )
        close = pd.Series([100.0])

        sig = self._get_signal("fundamental.per")
        built = sig.param_builder(params, {"statements_data": df, "execution_close": close})
        assert built["eps"].equals(df["AdjustedEPS"])

    def test_forward_eps_growth_uses_adjusted_when_enabled(self) -> None:
        params = SignalParams()
        params.fundamental.forward_eps_growth.enabled = True

        df = pd.DataFrame(
            {
                "EPS": [1.0],
                "AdjustedEPS": [0.8],
                "NextYearForecastEPS": [2.0],
                "AdjustedNextYearForecastEPS": [1.6],
            }
        )

        sig = self._get_signal("fundamental.forward_eps_growth")
        built = sig.param_builder(params, {"statements_data": df})
        assert built["eps"].equals(df["AdjustedEPS"])
        assert built["next_year_forecast_eps"].equals(df["AdjustedNextYearForecastEPS"])

    def test_adjusted_can_be_disabled(self) -> None:
        params = SignalParams()
        params.fundamental.use_adjusted = False
        params.fundamental.per.enabled = True

        df = pd.DataFrame(
            {
                "EPS": [1.0],
                "AdjustedEPS": [0.5],
            }
        )
        close = pd.Series([100.0])

        sig = self._get_signal("fundamental.per")
        built = sig.param_builder(params, {"statements_data": df, "execution_close": close})
        assert built["eps"].equals(df["EPS"])
