"""SignalService tests for chart/screening parity paths."""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
import pytest

from src.application.services.market_data_errors import MarketDataError
from src.application.services.signal_service import (
    SignalService,
    _LoadedSignalData,
    _StrategyContext,
    _SIGNAL_DEFINITION_MAP,
    _build_signal_definition_map,
    _extract_trigger_dates,
    _get_signal_definition,
)
from src.domains.strategy.runtime.compiler import compile_runtime_strategy
from src.shared.models.config import SharedConfig
from src.shared.models.signals import SignalParams


def _make_ohlcv_df(n: int = 5) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Open": [100.0 + i for i in range(n)],
            "High": [105.0 + i for i in range(n)],
            "Low": [95.0 + i for i in range(n)],
            "Close": [102.0 + i for i in range(n)],
            "Volume": [1000 + i * 10 for i in range(n)],
        },
        index=pd.date_range("2025-01-01", periods=n),
    )


def _make_loaded_signal_data() -> _LoadedSignalData:
    daily = _make_ohlcv_df()
    return _LoadedSignalData(
        stock_code="7203",
        daily=daily,
        margin_data=None,
        statements_data=None,
        benchmark_data=None,
        sector_data=None,
        stock_sector_name=None,
        universe_multi_data={"7203": {"daily": daily}},
        universe_member_codes=["7203"],
        loaded_domains=["stock_data"],
        warnings=[],
    )


def _make_shared_config(*, stock_codes: list[str] | None = None) -> SharedConfig:
    payload: dict[str, object] = {"timeframe": "daily"}
    if stock_codes is not None:
        payload["stock_codes"] = stock_codes
    return SharedConfig.model_validate(
        payload,
        context={"resolve_stock_codes": False},
    )


def _make_compiled_strategy(
    *,
    entry_params: SignalParams | None = None,
    exit_params: SignalParams | None = None,
) -> object:
    return compile_runtime_strategy(
        strategy_name="test_signal_service",
        shared_config=_make_shared_config(),
        entry_signal_params=entry_params or SignalParams(),
        exit_signal_params=exit_params or SignalParams(),
    )


class TestSignalDefinitionMap:
    def test_map_is_populated(self) -> None:
        assert len(_SIGNAL_DEFINITION_MAP) > 0

    def test_nested_param_key_extraction(self) -> None:
        mapping = _build_signal_definition_map()
        assert "per" in mapping
        assert mapping["per"].param_key == "fundamental.per"

    def test_duplicate_signal_type_logs_warning(self) -> None:
        sig_def = _get_signal_definition("volume_ratio_above")
        assert sig_def is not None

        with patch("src.application.services.signal_service.SIGNAL_REGISTRY", [sig_def, sig_def]):
            with patch("src.application.services.signal_service.logger") as mock_logger:
                mapping = _build_signal_definition_map()

        assert mapping["volume_ratio_above"] is sig_def
        mock_logger.warning.assert_called_once()


class TestSignalDefinitionLookup:
    def test_volume_ratio_signal(self) -> None:
        sig_def = _get_signal_definition("volume_ratio_above")
        assert sig_def is not None
        assert sig_def.name == "出来高比率上抜け"
        assert sig_def.category == "volume"

    def test_unknown_signal(self) -> None:
        assert _get_signal_definition("unknown_signal") is None


class TestExtractTriggerDates:
    def test_extracts_true_values_only(self) -> None:
        series = pd.Series(
            [True, False, True, None],
            index=pd.date_range("2025-01-01", periods=4),
        )
        assert _extract_trigger_dates(series) == ["2025-01-01", "2025-01-03"]

    def test_non_datetime_index_is_stringified(self) -> None:
        series = pd.Series([True, False], index=[1, 2])
        assert _extract_trigger_dates(series) == ["1"]


class TestSignalService:
    @pytest.fixture
    def service(self) -> SignalService:
        return SignalService()

    def test_empty_manual_request_returns_empty_response(self, service: SignalService) -> None:
        result = service.compute_signals(
            stock_code="7203",
            source="market",
            timeframe="daily",
            signals=[],
        )

        assert result["stock_code"] == "7203"
        assert result["signals"] == {}
        assert result["provenance"]["source_kind"] == "market"

    def test_invalid_date_range_raises(self, service: SignalService) -> None:
        with pytest.raises(ValueError, match="無効な日付範囲"):
            service.compute_signals(
                stock_code="7203",
                source="market",
                timeframe="daily",
                signals=[{"type": "buy_and_hold", "params": {}, "mode": "entry"}],
                start_date=date(2025, 12, 31),
                end_date=date(2025, 1, 1),
            )

    def test_non_market_source_is_rejected(self, service: SignalService) -> None:
        with pytest.raises(ValueError, match="source='market'"):
            service.compute_signals(
                stock_code="7203",
                source="dataset",
                timeframe="daily",
                signals=[{"type": "buy_and_hold", "params": {}, "mode": "entry"}],
            )

    def test_unknown_signal_returns_error_result(self, service: SignalService) -> None:
        data = {
            "ohlc_data": _make_ohlcv_df(3),
            "close": pd.Series([102.0, 103.0, 104.0], index=pd.date_range("2025-01-01", periods=3)),
            "execution_close": pd.Series([102.0, 103.0, 104.0], index=pd.date_range("2025-01-01", periods=3)),
            "volume": pd.Series([1000, 1100, 1200], index=pd.date_range("2025-01-01", periods=3)),
            "margin_data": None,
            "statements_data": None,
            "benchmark_data": None,
            "sector_data": None,
            "stock_sector_name": None,
            "execution_data": None,
            "is_relative_mode": False,
        }

        result = service._compute_signal_result_from_params(
            signal_type="unknown_signal",
            mode="entry",
            signal_params=SignalParams(),
            data=data,
            compiled_strategy=_make_compiled_strategy(),
        )

        assert result["count"] == 0
        assert "未対応のシグナル" in result["error"]
        assert result["diagnostics"]["warnings"]

    def test_fundamental_signal_reports_missing_required_data(self, service: SignalService) -> None:
        data = {
            "ohlc_data": _make_ohlcv_df(3),
            "close": pd.Series([102.0, 103.0, 104.0], index=pd.date_range("2025-01-01", periods=3)),
            "execution_close": pd.Series([102.0, 103.0, 104.0], index=pd.date_range("2025-01-01", periods=3)),
            "volume": pd.Series([1000, 1100, 1200], index=pd.date_range("2025-01-01", periods=3)),
            "margin_data": None,
            "statements_data": None,
            "benchmark_data": None,
            "sector_data": None,
            "stock_sector_name": None,
            "execution_data": None,
            "is_relative_mode": False,
        }
        signal_params = service._build_signal_params(
            "per",
            {"threshold": 10.0, "condition": "below"},
            "entry",
        )

        result = service._compute_signal_result_from_params(
            signal_type="per",
            mode="entry",
            signal_params=signal_params,
            data=data,
            compiled_strategy=_make_compiled_strategy(entry_params=signal_params),
        )

        assert result["count"] == 0
        assert result["diagnostics"]["missing_required_data"]
        assert result["diagnostics"]["effective_period_type"] == signal_params.fundamental.period_type

    def test_load_signal_data_without_market_reader_raises_structured_error(self, service: SignalService) -> None:
        with pytest.raises(MarketDataError, match="ローカル市場データが初期化されていません") as exc_info:
            service._load_signal_data(
                "7203",
                shared_config=_make_shared_config(),
                entry_params=SignalParams(),
                exit_params=SignalParams(),
                start_date=None,
                end_date=None,
            )

        assert exc_info.value.reason == "market_db_missing"
        assert exc_info.value.recovery == "market_db_sync"

    def test_manual_overlay_returns_signal_results_and_provenance(self, service: SignalService) -> None:
        with patch.object(service, "_load_signal_data", return_value=_make_loaded_signal_data()):
            result = service.compute_signals(
                stock_code="7203",
                source="market",
                timeframe="daily",
                signals=[{"type": "buy_and_hold", "params": {}, "mode": "entry"}],
            )

        assert result["signals"]["buy_and_hold"]["count"] == 5
        assert result["signals"]["buy_and_hold"]["mode"] == "entry"
        assert result["provenance"]["source_kind"] == "market"
        assert "stock_data" in result["provenance"]["loaded_domains"]

    def test_strategy_overlay_returns_combined_entry_and_strategy_provenance(self, service: SignalService) -> None:
        entry_params = service._build_signal_params("buy_and_hold", {}, "entry")
        exit_params = SignalParams()
        strategy_context = _StrategyContext(
            strategy_name="production/test_strategy",
            strategy_fingerprint="fingerprint-123",
            shared_config=_make_shared_config(),
            entry_params=entry_params,
            exit_params=exit_params,
            compiled_strategy=None,
        )

        with patch.object(service, "_resolve_strategy_context", return_value=strategy_context):
            with patch.object(service, "_load_signal_data", return_value=_make_loaded_signal_data()):
                result = service.compute_signals(
                    stock_code="7203",
                    source="market",
                    timeframe="daily",
                    strategy_name="production/test_strategy",
                )

        assert result["strategy_name"] == "production/test_strategy"
        assert result["combined_entry"]["count"] == 5
        assert result["combined_exit"]["count"] == 0
        assert "entry:buy_and_hold" in result["signals"]
        assert result["provenance"]["strategy_name"] == "production/test_strategy"
        assert result["provenance"]["strategy_fingerprint"] == "fingerprint-123"

    def test_resample_helpers_cover_non_daily_branches(self, service: SignalService) -> None:
        ohlcv = _make_ohlcv_df(10)
        weekly = service._resample_ohlcv(ohlcv, "weekly")
        monthly_ohlc = service._resample_ohlc(ohlcv[["Open", "High", "Low", "Close"]], "monthly")
        aligned = service._resample_aligned_frame(
            ohlcv[["Close"]],
            pd.DatetimeIndex(pd.date_range("2025-01-01", periods=12)),
        )

        assert len(weekly) < len(ohlcv)
        assert monthly_ohlc is not None
        assert aligned is not None
        assert aligned.index[-1] == pd.Timestamp("2025-01-12")

    def test_resolve_strategy_context_hashes_loaded_config(self, service: SignalService) -> None:
        loaded = SimpleNamespace(
            config={"name": "demo", "signals": ["buy_and_hold"]},
            shared_config=_make_shared_config(),
            entry_params=service._build_signal_params("buy_and_hold", {}, "entry"),
            exit_params=SignalParams(),
            compiled_strategy={"kind": "compiled"},
        )

        with patch(
            "src.application.services.signal_service.load_strategy_screening_config",
            return_value=loaded,
        ):
            context = service._resolve_strategy_context("production/demo")

        assert context.strategy_name == "production/demo"
        assert len(context.strategy_fingerprint) == 64
        assert context.compiled_strategy == {"kind": "compiled"}

    def test_build_signal_params_updates_top_level_and_nested_fields(self, service: SignalService) -> None:
        top_level = service._build_signal_params(
            "rsi_threshold",
            {"threshold": 70.0, "condition": "above"},
            "entry",
        )
        nested = service._build_signal_params(
            "per",
            {"threshold": 12.0, "condition": "below"},
            "entry",
        )
        untouched = SignalParams()

        service._update_top_level_field(untouched, "missing_field", {"threshold": 10.0})
        service._update_nested_field(untouched, ["missing_parent", "child"], {"threshold": 10.0})

        assert top_level.rsi_threshold.enabled is True
        assert top_level.rsi_threshold.threshold == 70.0
        assert top_level.rsi_threshold.condition == "above"
        assert nested.fundamental.enabled is True
        assert nested.fundamental.per.enabled is True
        assert nested.fundamental.per.threshold == 12.0
        assert untouched.model_dump() == SignalParams().model_dump()

    def test_build_signal_params_unknown_signal_raises(self, service: SignalService) -> None:
        with pytest.raises(ValueError, match="未対応のシグナル"):
            service._build_signal_params("unknown_signal", {}, "entry")

    def test_load_signal_data_success_loads_optional_domains(self, service: SignalService) -> None:
        service._market_reader = object()  # type: ignore[assignment]
        daily = _make_ohlcv_df()
        margin = pd.DataFrame({"Long": [1.0]}, index=[pd.Timestamp("2025-01-01")])
        statements = pd.DataFrame({"EPS": [10.0]}, index=[pd.Timestamp("2025-01-01")])
        benchmark = pd.DataFrame(
            {"Open": [1.0], "High": [1.0], "Low": [1.0], "Close": [1.0]},
            index=[pd.Timestamp("2025-01-01")],
        )
        sector_frame = pd.DataFrame(
            {"Open": [2.0], "High": [2.0], "Low": [2.0], "Close": [2.0]},
            index=[pd.Timestamp("2025-01-01")],
        )
        requirements = SimpleNamespace(
            multi_data_key=SimpleNamespace(
                start_date="2025-01-01",
                end_date="2025-01-05",
                include_margin_data=True,
                include_statements_data=True,
                period_type="FY",
                include_forecast_revision=True,
            ),
            needs_benchmark=True,
            benchmark_data_key=SimpleNamespace(start_date="2025-01-01", end_date="2025-01-05"),
            needs_sector=True,
            sector_data_key=SimpleNamespace(start_date="2025-01-01", end_date="2025-01-05"),
        )

        with (
            patch(
                "src.application.services.signal_service.build_strategy_data_requirements",
                return_value=requirements,
            ),
            patch(
                "src.application.services.signal_service.load_market_multi_data",
                return_value=(
                    {
                        "7203": {
                            "daily": daily,
                            "margin_daily": margin,
                            "statements_daily": statements,
                        }
                    },
                    ["statements lagging"],
                ),
            ),
            patch(
                "src.application.services.signal_service.load_market_topix_data",
                return_value=benchmark,
            ),
            patch(
                "src.application.services.signal_service.load_market_sector_indices",
                return_value={"transport": sector_frame},
            ),
            patch(
                "src.application.services.signal_service.load_market_stock_sector_mapping",
                return_value={"7203": "transport"},
            ),
        ):
            loaded = service._load_signal_data(
                "72030",
                shared_config=_make_shared_config(),
                entry_params=SignalParams(),
                exit_params=SignalParams(),
                start_date=date(2025, 1, 1),
                end_date=date(2025, 1, 5),
            )

        assert loaded.stock_code == "7203"
        assert loaded.stock_sector_name == "transport"
        assert loaded.warnings == ["statements lagging"]
        assert loaded.margin_data is not None
        assert loaded.statements_data is not None
        assert loaded.benchmark_data is not None
        assert loaded.sector_data is not None
        assert loaded.universe_member_codes == ["7203"]
        assert set(loaded.universe_multi_data) == {"7203"}
        assert loaded.loaded_domains == [
            "stock_data",
            "margin_data",
            "statements",
            "topix_data",
            "indices_data",
        ]

    def test_load_signal_data_expands_load_scope_for_universe_signal(
        self,
        service: SignalService,
    ) -> None:
        service._market_reader = object()  # type: ignore[assignment]
        daily = _make_ohlcv_df()
        requirements = SimpleNamespace(
            multi_data_key=SimpleNamespace(
                start_date="2025-01-01",
                end_date="2025-01-05",
                include_margin_data=False,
                include_statements_data=False,
                period_type="FY",
                include_forecast_revision=False,
            ),
            needs_benchmark=False,
            benchmark_data_key=None,
            needs_sector=False,
            sector_data_key=None,
        )
        entry_params = SignalParams(universe_rank_bucket={"enabled": True})

        with (
            patch(
                "src.application.services.signal_service.build_strategy_data_requirements",
                return_value=requirements,
            ) as build_requirements,
            patch(
                "src.application.services.signal_service.load_market_multi_data",
                return_value=(
                    {
                        "7203": {"daily": daily},
                        "6758": {"daily": daily},
                    },
                    [],
                ),
            ) as load_multi_data,
        ):
            loaded = service._load_signal_data(
                "7203",
                shared_config=_make_shared_config(stock_codes=["7203", "6758"]),
                entry_params=entry_params,
                exit_params=SignalParams(),
                start_date=date(2025, 1, 1),
                end_date=date(2025, 1, 5),
            )

        assert build_requirements.call_args.kwargs["stock_codes"] == ("7203", "6758")
        assert load_multi_data.call_args.args[1] == ["7203", "6758"]
        assert loaded.universe_member_codes == ["7203", "6758"]
        assert set(loaded.universe_multi_data) == {"7203", "6758"}

    def test_load_signal_data_missing_daily_when_stock_exists_raises_refresh_error(
        self,
        service: SignalService,
    ) -> None:
        service._market_reader = object()  # type: ignore[assignment]
        requirements = SimpleNamespace(
            multi_data_key=SimpleNamespace(
                start_date="2025-01-01",
                end_date="2025-01-05",
                include_margin_data=False,
                include_statements_data=False,
                period_type="FY",
                include_forecast_revision=False,
            ),
            needs_benchmark=False,
            benchmark_data_key=None,
            needs_sector=False,
            sector_data_key=None,
        )

        with (
            patch(
                "src.application.services.signal_service.build_strategy_data_requirements",
                return_value=requirements,
            ),
            patch(
                "src.application.services.signal_service.load_market_multi_data",
                return_value=({"7203": {}}, []),
            ),
            patch(
                "src.application.services.signal_service.stock_exists_in_reader",
                return_value=True,
            ),
        ):
            with pytest.raises(MarketDataError, match="ローカルOHLCVデータがありません") as exc_info:
                service._load_signal_data(
                    "7203",
                    shared_config=_make_shared_config(),
                    entry_params=SignalParams(),
                    exit_params=SignalParams(),
                    start_date=None,
                    end_date=None,
                )

        assert exc_info.value.reason == "local_stock_data_missing"
        assert exc_info.value.recovery == "stock_refresh"

    def test_load_signal_data_missing_daily_when_stock_missing_raises_not_found(
        self,
        service: SignalService,
    ) -> None:
        service._market_reader = object()  # type: ignore[assignment]
        requirements = SimpleNamespace(
            multi_data_key=SimpleNamespace(
                start_date="2025-01-01",
                end_date="2025-01-05",
                include_margin_data=False,
                include_statements_data=False,
                period_type="FY",
                include_forecast_revision=False,
            ),
            needs_benchmark=False,
            benchmark_data_key=None,
            needs_sector=False,
            sector_data_key=None,
        )

        with (
            patch(
                "src.application.services.signal_service.build_strategy_data_requirements",
                return_value=requirements,
            ),
            patch(
                "src.application.services.signal_service.load_market_multi_data",
                return_value=({}, []),
            ),
            patch(
                "src.application.services.signal_service.stock_exists_in_reader",
                return_value=False,
            ),
        ):
            with pytest.raises(MarketDataError, match="ローカル市場データに存在しません") as exc_info:
                service._load_signal_data(
                    "7203",
                    shared_config=_make_shared_config(),
                    entry_params=SignalParams(),
                    exit_params=SignalParams(),
                    start_date=None,
                    end_date=None,
                )

        assert exc_info.value.reason == "stock_not_found"

    def test_build_signal_data_raises_when_resampled_close_is_missing(self, service: SignalService) -> None:
        bad_daily = _make_ohlcv_df().assign(Close=float("nan"))
        loaded = _LoadedSignalData(
            stock_code="7203",
            daily=bad_daily,
            margin_data=None,
            statements_data=None,
            benchmark_data=None,
            sector_data=None,
            stock_sector_name=None,
            universe_multi_data={"7203": {"daily": bad_daily}},
            universe_member_codes=["7203"],
            loaded_domains=["stock_data"],
            warnings=[],
        )

        with pytest.raises(ValueError, match="リサンプル後データが不足しています"):
            service._build_signal_data(loaded, "daily")

    def test_exit_disabled_signal_returns_error_without_processor_call(self, service: SignalService) -> None:
        data = {
            "ohlc_data": _make_ohlcv_df(3),
            "close": pd.Series([102.0, 103.0, 104.0], index=pd.date_range("2025-01-01", periods=3)),
            "execution_close": pd.Series([102.0, 103.0, 104.0], index=pd.date_range("2025-01-01", periods=3)),
            "volume": pd.Series([1000, 1100, 1200], index=pd.date_range("2025-01-01", periods=3)),
            "margin_data": None,
            "statements_data": None,
            "benchmark_data": None,
            "sector_data": None,
            "stock_sector_name": None,
            "stock_code": "7203",
            "universe_multi_data": {"7203": {"daily": _make_ohlcv_df(3)}},
            "universe_member_codes": ["7203"],
            "execution_data": None,
            "is_relative_mode": False,
        }
        signal_params = service._build_signal_params("buy_and_hold", {}, "entry")

        result = service._compute_signal_result_from_params(
            signal_type="buy_and_hold",
            mode="exit",
            signal_params=signal_params,
            data=data,
            compiled_strategy=_make_compiled_strategy(entry_params=signal_params),
        )

        assert result["count"] == 0
        assert "Exitモードでは使用できません" in result["error"]
        assert result["diagnostics"]["warnings"]

    def test_exit_mode_uses_exit_processor(self, service: SignalService) -> None:
        data = {
            "ohlc_data": _make_ohlcv_df(3),
            "close": pd.Series([102.0, 103.0, 104.0], index=pd.date_range("2025-01-01", periods=3)),
            "execution_close": pd.Series([102.0, 103.0, 104.0], index=pd.date_range("2025-01-01", periods=3)),
            "volume": pd.Series([1000, 1100, 1200], index=pd.date_range("2025-01-01", periods=3)),
            "margin_data": None,
            "statements_data": None,
            "benchmark_data": None,
            "sector_data": None,
            "stock_sector_name": None,
            "stock_code": "7203",
            "universe_multi_data": {"7203": {"daily": _make_ohlcv_df(3)}},
            "universe_member_codes": ["7203"],
            "execution_data": None,
            "is_relative_mode": False,
        }
        signal_params = service._build_signal_params("rsi_threshold", {"threshold": 70.0}, "exit")
        exit_series = pd.Series([False, True, False], index=data["ohlc_data"].index)

        with patch.object(service._signal_processor, "apply_exit_signals", return_value=exit_series) as mock_exit:
            result = service._compute_signal_result_from_params(
                signal_type="rsi_threshold",
                mode="exit",
                signal_params=signal_params,
                data=data,
                compiled_strategy=_make_compiled_strategy(exit_params=signal_params),
            )

        mock_exit.assert_called_once()
        assert result["trigger_dates"] == ["2025-01-02"]

    def test_signal_processor_exception_is_captured_in_result(self, service: SignalService) -> None:
        data = {
            "ohlc_data": _make_ohlcv_df(3),
            "close": pd.Series([102.0, 103.0, 104.0], index=pd.date_range("2025-01-01", periods=3)),
            "execution_close": pd.Series([102.0, 103.0, 104.0], index=pd.date_range("2025-01-01", periods=3)),
            "volume": pd.Series([1000, 1100, 1200], index=pd.date_range("2025-01-01", periods=3)),
            "margin_data": None,
            "statements_data": None,
            "benchmark_data": None,
            "sector_data": None,
            "stock_sector_name": None,
            "execution_data": None,
            "is_relative_mode": False,
        }
        signal_params = service._build_signal_params("buy_and_hold", {}, "entry")

        with patch.object(service._signal_processor, "apply_entry_signals", side_effect=RuntimeError("boom")):
            result = service._compute_signal_result_from_params(
                signal_type="buy_and_hold",
                mode="entry",
                signal_params=signal_params,
                data=data,
                compiled_strategy=_make_compiled_strategy(entry_params=signal_params),
            )

        assert result["count"] == 0
        assert result["error"] == "boom"
        assert "boom" in result["diagnostics"]["warnings"]

    def test_extract_strategy_signal_specs_includes_exit_entries(self, service: SignalService) -> None:
        entry_params = service._build_signal_params("buy_and_hold", {}, "entry")
        exit_params = service._build_signal_params(
            "rsi_threshold",
            {"threshold": 70.0, "condition": "above"},
            "exit",
        )

        specs = service._extract_strategy_signal_specs(entry_params, exit_params)

        assert ("buy_and_hold", "entry", entry_params) in specs
        assert ("rsi_threshold", "exit", exit_params) in specs
