"""registry.py データチェックヘルパー関数のテスト"""

import pandas as pd
import pytest

from src.models.signals import SignalParams

from src.strategies.signals.registry import (
    SIGNAL_REGISTRY,
    _has_any_statements_column,
    _has_benchmark_data,
    _has_margin_data,
    _has_sector_data,
    _has_sector_data_and_benchmark,
    _has_statements_column,
    _has_statements_columns,
    _select_existing_fundamental_column,
    _has_stock_sector_close,
    _has_stock_sector_close_and_benchmark,
    _validate_registry,
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


class TestHasAnyStatementsColumn:
    def test_any_present(self) -> None:
        df = pd.DataFrame({"A": [float("nan")], "B": [1.0]})
        assert _has_any_statements_column({"statements_data": df}, "A", "B")

    def test_none_data(self) -> None:
        assert not _has_any_statements_column({"statements_data": None}, "A", "B")


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
        assert "statements:ForwardForecastEPS" in sig.data_requirements

    def test_eps_growth_registered(self) -> None:
        """eps_growth（実績ベース）がレジストリに登録されていること"""
        matches = [s for s in SIGNAL_REGISTRY if s.param_key == "fundamental.eps_growth"]
        assert len(matches) == 1
        sig = matches[0]
        assert sig.name == "EPS成長率"
        assert "statements:EPS" in sig.data_requirements
        assert "statements:NextYearForecastEPS" not in sig.data_requirements

    def test_forward_dividend_growth_registered(self) -> None:
        matches = [s for s in SIGNAL_REGISTRY if s.param_key == "fundamental.forward_dividend_growth"]
        assert len(matches) == 1
        sig = matches[0]
        assert sig.name == "Forward 1株配当成長率"
        assert "statements:DividendFY" in sig.data_requirements
        assert "statements:ForwardForecastDividendFY" in sig.data_requirements

    def test_dividend_per_share_growth_registered(self) -> None:
        matches = [s for s in SIGNAL_REGISTRY if s.param_key == "fundamental.dividend_per_share_growth"]
        assert len(matches) == 1
        sig = matches[0]
        assert sig.name == "1株配当成長率"
        assert sig.category == "fundamental"
        assert sig.data_requirements == ["statements:DividendFY"]

    def test_payout_ratio_registered(self) -> None:
        matches = [s for s in SIGNAL_REGISTRY if s.param_key == "fundamental.payout_ratio"]
        assert len(matches) == 1
        sig = matches[0]
        assert sig.name == "配当性向"
        assert sig.data_requirements == ["statements:PayoutRatio"]

    def test_forward_payout_ratio_registered(self) -> None:
        matches = [s for s in SIGNAL_REGISTRY if s.param_key == "fundamental.forward_payout_ratio"]
        assert len(matches) == 1
        sig = matches[0]
        assert sig.name == "予想配当性向"
        assert sig.data_requirements == ["statements:ForwardForecastPayoutRatio"]

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

    def test_cfo_margin_registered(self) -> None:
        matches = [s for s in SIGNAL_REGISTRY if s.param_key == "fundamental.cfo_margin"]
        assert len(matches) == 1
        sig = matches[0]
        assert sig.name == "CFOマージン"
        assert sig.category == "fundamental"
        assert "statements:OperatingCashFlow" in sig.data_requirements
        assert "statements:Sales" in sig.data_requirements

    def test_simple_fcf_margin_registered(self) -> None:
        matches = [s for s in SIGNAL_REGISTRY if s.param_key == "fundamental.simple_fcf_margin"]
        assert len(matches) == 1
        sig = matches[0]
        assert sig.name == "簡易FCFマージン"
        assert sig.category == "fundamental"
        assert "statements:OperatingCashFlow" in sig.data_requirements
        assert "statements:InvestingCashFlow" in sig.data_requirements
        assert "statements:Sales" in sig.data_requirements

    def test_cfo_to_net_profit_ratio_registered(self) -> None:
        matches = [s for s in SIGNAL_REGISTRY if s.param_key == "fundamental.cfo_to_net_profit_ratio"]
        assert len(matches) == 1
        sig = matches[0]
        assert sig.name == "営業CF/純利益"
        assert sig.category == "fundamental"
        assert "statements:OperatingCashFlow" in sig.data_requirements
        assert "statements:Profit" in sig.data_requirements

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

    def test_cfo_margin_enabled_checker(self) -> None:
        sig = next(s for s in SIGNAL_REGISTRY if s.param_key == "fundamental.cfo_margin")

        params = SignalParams()
        params.fundamental.enabled = False
        params.fundamental.cfo_margin.enabled = True
        assert not sig.enabled_checker(params)

        params.fundamental.enabled = True
        assert sig.enabled_checker(params)

        params.fundamental.cfo_margin.enabled = False
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

    def test_forward_eps_growth_prefers_forward_columns_when_available(self) -> None:
        params = SignalParams()
        params.fundamental.forward_eps_growth.enabled = True

        df = pd.DataFrame(
            {
                "EPS": [1.0],
                "AdjustedEPS": [0.8],
                "ForwardBaseEPS": [1.1],
                "AdjustedForwardBaseEPS": [0.9],
                "NextYearForecastEPS": [2.0],
                "AdjustedNextYearForecastEPS": [1.6],
                "ForwardForecastEPS": [1.7],
                "AdjustedForwardForecastEPS": [1.4],
            }
        )

        sig = self._get_signal("fundamental.forward_eps_growth")
        built = sig.param_builder(params, {"statements_data": df})
        assert built["eps"].equals(df["AdjustedForwardBaseEPS"])
        assert built["next_year_forecast_eps"].equals(df["AdjustedForwardForecastEPS"])

    def test_peg_ratio_prefers_forward_columns_when_available(self) -> None:
        params = SignalParams()
        params.fundamental.peg_ratio.enabled = True

        df = pd.DataFrame(
            {
                "EPS": [1.0],
                "AdjustedEPS": [0.8],
                "ForwardBaseEPS": [1.1],
                "AdjustedForwardBaseEPS": [0.9],
                "NextYearForecastEPS": [2.0],
                "AdjustedNextYearForecastEPS": [1.6],
                "ForwardForecastEPS": [1.7],
                "AdjustedForwardForecastEPS": [1.4],
            }
        )
        close = pd.Series([100.0])

        sig = self._get_signal("fundamental.peg_ratio")
        built = sig.param_builder(params, {"statements_data": df, "execution_close": close})
        assert built["eps"].equals(df["AdjustedForwardBaseEPS"])
        assert built["next_year_forecast_eps"].equals(df["AdjustedForwardForecastEPS"])

    def test_forward_eps_growth_fallbacks_when_forward_columns_are_all_nan(self) -> None:
        params = SignalParams()
        params.fundamental.forward_eps_growth.enabled = True

        df = pd.DataFrame(
            {
                "EPS": [1.0, 1.2],
                "AdjustedEPS": [0.8, 1.0],
                "NextYearForecastEPS": [2.0, 2.4],
                "AdjustedNextYearForecastEPS": [1.6, 2.0],
                "ForwardBaseEPS": [float("nan"), float("nan")],
                "AdjustedForwardBaseEPS": [float("nan"), float("nan")],
                "ForwardForecastEPS": [float("nan"), float("nan")],
                "AdjustedForwardForecastEPS": [float("nan"), float("nan")],
            }
        )

        sig = self._get_signal("fundamental.forward_eps_growth")
        built = sig.param_builder(params, {"statements_data": df})
        assert built["eps"].equals(df["AdjustedEPS"])
        assert built["next_year_forecast_eps"].equals(df["AdjustedNextYearForecastEPS"])

    def test_peg_ratio_fallbacks_when_forward_columns_are_all_nan(self) -> None:
        params = SignalParams()
        params.fundamental.peg_ratio.enabled = True

        df = pd.DataFrame(
            {
                "EPS": [1.0, 1.2],
                "AdjustedEPS": [0.8, 1.0],
                "NextYearForecastEPS": [2.0, 2.4],
                "AdjustedNextYearForecastEPS": [1.6, 2.0],
                "ForwardBaseEPS": [float("nan"), float("nan")],
                "AdjustedForwardBaseEPS": [float("nan"), float("nan")],
                "ForwardForecastEPS": [float("nan"), float("nan")],
                "AdjustedForwardForecastEPS": [float("nan"), float("nan")],
            }
        )
        close = pd.Series([100.0, 101.0])

        sig = self._get_signal("fundamental.peg_ratio")
        built = sig.param_builder(params, {"statements_data": df, "execution_close": close})
        assert built["eps"].equals(df["AdjustedEPS"])
        assert built["next_year_forecast_eps"].equals(df["AdjustedNextYearForecastEPS"])

    def test_forward_dividend_growth_prefers_forward_columns_when_available(self) -> None:
        params = SignalParams()
        params.fundamental.forward_dividend_growth.enabled = True

        df = pd.DataFrame(
            {
                "DividendFY": [10.0],
                "AdjustedDividendFY": [9.0],
                "ForwardBaseDividendFY": [11.0],
                "AdjustedForwardBaseDividendFY": [10.0],
                "NextYearForecastDividendFY": [12.0],
                "AdjustedNextYearForecastDividendFY": [11.0],
                "ForwardForecastDividendFY": [12.5],
                "AdjustedForwardForecastDividendFY": [11.5],
            }
        )

        sig = self._get_signal("fundamental.forward_dividend_growth")
        built = sig.param_builder(params, {"statements_data": df})
        assert built["dividend_fy"].equals(df["AdjustedForwardBaseDividendFY"])
        assert built["next_year_forecast_dividend_fy"].equals(
            df["AdjustedForwardForecastDividendFY"]
        )

    def test_forward_dividend_growth_fallbacks_when_forward_columns_are_all_nan(self) -> None:
        params = SignalParams()
        params.fundamental.forward_dividend_growth.enabled = True

        df = pd.DataFrame(
            {
                "DividendFY": [10.0, 10.5],
                "AdjustedDividendFY": [9.0, 9.5],
                "NextYearForecastDividendFY": [12.0, 12.5],
                "AdjustedNextYearForecastDividendFY": [11.0, 11.5],
                "ForwardBaseDividendFY": [float("nan"), float("nan")],
                "AdjustedForwardBaseDividendFY": [float("nan"), float("nan")],
                "ForwardForecastDividendFY": [float("nan"), float("nan")],
                "AdjustedForwardForecastDividendFY": [float("nan"), float("nan")],
            }
        )

        sig = self._get_signal("fundamental.forward_dividend_growth")
        built = sig.param_builder(params, {"statements_data": df})
        assert built["dividend_fy"].equals(df["AdjustedDividendFY"])
        assert built["next_year_forecast_dividend_fy"].equals(
            df["AdjustedNextYearForecastDividendFY"]
        )

    def test_forward_payout_ratio_prefers_forward_column(self) -> None:
        params = SignalParams()
        params.fundamental.forward_payout_ratio.enabled = True

        df = pd.DataFrame(
            {
                "PayoutRatio": [30.0],
                "ForwardForecastPayoutRatio": [35.0],
                "NextYearForecastPayoutRatio": [40.0],
            }
        )

        sig = self._get_signal("fundamental.forward_payout_ratio")
        built = sig.param_builder(params, {"statements_data": df})
        assert built["payout_ratio"].equals(df["ForwardForecastPayoutRatio"])

    def test_forward_payout_ratio_fallbacks_when_forward_column_missing(self) -> None:
        params = SignalParams()
        params.fundamental.forward_payout_ratio.enabled = True

        df = pd.DataFrame({"NextYearForecastPayoutRatio": [40.0]})

        sig = self._get_signal("fundamental.forward_payout_ratio")
        built = sig.param_builder(params, {"statements_data": df})
        assert built["payout_ratio"].equals(df["NextYearForecastPayoutRatio"])

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

    def test_select_existing_prefers_present_column_even_when_empty(self) -> None:
        params = SignalParams()
        params.fundamental.use_adjusted = True
        statements = pd.DataFrame(
            {
                "AdjustedForwardBaseEPS": [float("nan")],
                "AdjustedEPS": [float("nan")],
            }
        )

        selected = _select_existing_fundamental_column(
            params=params,
            statements_data=statements,
            preferred_adjusted="AdjustedForwardBaseEPS",
            preferred_raw="ForwardBaseEPS",
            fallback_adjusted="AdjustedEPS",
            fallback_raw="EPS",
        )
        assert selected == "AdjustedForwardBaseEPS"

    def test_select_existing_falls_back_when_preferred_missing(self) -> None:
        params = SignalParams()
        params.fundamental.use_adjusted = True
        statements = pd.DataFrame({"AdjustedEPS": [float("nan")]})

        selected = _select_existing_fundamental_column(
            params=params,
            statements_data=statements,
            preferred_adjusted="AdjustedForwardBaseEPS",
            preferred_raw="ForwardBaseEPS",
            fallback_adjusted="AdjustedEPS",
            fallback_raw="EPS",
        )
        assert selected == "AdjustedEPS"


class TestValidateRegistry:
    def test_validate_registry_detects_duplicate_param_key(self) -> None:
        duplicate = SIGNAL_REGISTRY[0]
        SIGNAL_REGISTRY.append(duplicate)
        try:
            with pytest.raises(ValueError, match="Duplicate param_key"):
                _validate_registry()
        finally:
            SIGNAL_REGISTRY.pop()
