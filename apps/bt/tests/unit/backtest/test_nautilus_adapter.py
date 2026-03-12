"""Tests for Nautilus verification adapter."""

from __future__ import annotations

from types import SimpleNamespace
from pathlib import Path

import pandas as pd
import pytest

from src.domains.backtest.contracts import EngineFamily, RunType
from src.domains.backtest import nautilus_adapter as adapter
from src.domains.backtest.nautilus_adapter import (
    MissingNautilusDependencyError,
    NautilusVerificationRunner,
    _PreparedPortfolioInputs,
    _TradePlan,
    _build_bars_dataframe,
    _build_verification_plan,
)
from src.domains.backtest.contracts import RunSpec
from src.shared.models.config import SharedConfig
from src.shared.models.signals import SignalParams


def test_build_verification_plan_computes_metrics_for_daily_round_trip() -> None:
    prepared = _PreparedPortfolioInputs(
        strategy_name="demo",
        dataset_name="sample",
        shared_config=SharedConfig(
            dataset="sample",
            stock_codes=["1301"],
            initial_cash=1_000_000,
            timeframe="daily",
            direction="longonly",
            group_by=True,
            cash_sharing=True,
        ),
        compiled_strategy=None,
        open_data=pd.DataFrame(
            {"1301": [100.0, 110.0]},
            index=pd.to_datetime(["2024-01-04", "2024-01-05"]),
        ),
        close_data=pd.DataFrame(
            {"1301": [110.0, 105.0]},
            index=pd.to_datetime(["2024-01-04", "2024-01-05"]),
        ),
        entries_data=pd.DataFrame(
            {"1301": [True, False]},
            index=pd.to_datetime(["2024-01-04", "2024-01-05"]),
        ),
        execution_mode="next_session_round_trip",
        effective_fees=0.001,
        effective_slippage=0.0,
        allocation_per_asset=1.0,
    )

    plan = _build_verification_plan(prepared)

    assert plan.summary_metrics.trade_count == 1
    assert plan.metrics_payload["trade_count"] == 1
    assert plan.metrics_payload["total_return"] > 0.0
    assert plan.diagnostics["tradePlanCount"] == 1
    assert plan.trade_records[0]["code"] == "1301"


def test_build_verification_plan_skips_unaffordable_trade_after_fees_and_slippage() -> None:
    prepared = _PreparedPortfolioInputs(
        strategy_name="demo",
        dataset_name="sample",
        shared_config=SharedConfig(
            dataset="sample",
            stock_codes=["1301"],
            initial_cash=100,
            timeframe="daily",
            direction="longonly",
            group_by=True,
            cash_sharing=True,
        ),
        compiled_strategy=None,
        open_data=pd.DataFrame(
            {"1301": [100.0]},
            index=pd.to_datetime(["2024-01-04"]),
        ),
        close_data=pd.DataFrame(
            {"1301": [101.0]},
            index=pd.to_datetime(["2024-01-04"]),
        ),
        entries_data=pd.DataFrame(
            {"1301": [True]},
            index=pd.to_datetime(["2024-01-04"]),
        ),
        execution_mode="next_session_round_trip",
        effective_fees=0.002,
        effective_slippage=0.01,
        allocation_per_asset=1.0,
    )

    plan = _build_verification_plan(prepared)

    assert plan.summary_metrics.trade_count == 0
    assert plan.metrics_payload["trade_count"] == 0
    assert plan.diagnostics["skippedZeroQuantityCount"] == 1
    assert plan.diagnostics["tradePlanCount"] == 0
    assert plan.trade_records == []


def test_build_verification_plan_matches_vectorbt_cash_sharing_order_sizing() -> None:
    prepared = _PreparedPortfolioInputs(
        strategy_name="demo",
        dataset_name="sample",
        shared_config=SharedConfig(
            dataset="sample",
            stock_codes=["1301", "1332"],
            initial_cash=100_000,
            timeframe="daily",
            direction="longonly",
            group_by=True,
            cash_sharing=True,
        ),
        compiled_strategy=None,
        open_data=pd.DataFrame(
            {"1301": [100.0], "1332": [100.0]},
            index=pd.to_datetime(["2024-01-04"]),
        ),
        close_data=pd.DataFrame(
            {"1301": [110.0], "1332": [110.0]},
            index=pd.to_datetime(["2024-01-04"]),
        ),
        entries_data=pd.DataFrame(
            {"1301": [True], "1332": [True]},
            index=pd.to_datetime(["2024-01-04"]),
        ),
        execution_mode="next_session_round_trip",
        effective_fees=0.0,
        effective_slippage=0.0,
        allocation_per_asset=0.5,
    )

    plan = _build_verification_plan(prepared)

    assert [trade["quantity"] for trade in plan.trade_records] == [500, 250]
    assert plan.metrics_payload["trade_count"] == 2
    assert plan.metrics_payload["total_return"] == pytest.approx(7.5)


def test_nautilus_verification_runner_writes_core_artifacts(monkeypatch, tmp_path: Path) -> None:
    runner = NautilusVerificationRunner()
    output_dir = Path.cwd() / ".tmp" / "nautilus-runner-test"
    output_dir.mkdir(parents=True, exist_ok=True)
    prepared = _PreparedPortfolioInputs(
        strategy_name="demo",
        dataset_name="sample",
        shared_config=SharedConfig(
            dataset="sample",
            stock_codes=["1301"],
            initial_cash=1_000_000,
            timeframe="daily",
            direction="longonly",
            group_by=True,
            cash_sharing=True,
        ),
        compiled_strategy=None,
        open_data=pd.DataFrame(
            {"1301": [100.0]},
            index=pd.to_datetime(["2024-01-04"]),
        ),
        close_data=pd.DataFrame(
            {"1301": [105.0]},
            index=pd.to_datetime(["2024-01-04"]),
        ),
        entries_data=pd.DataFrame(
            {"1301": [True]},
            index=pd.to_datetime(["2024-01-04"]),
        ),
        execution_mode="next_session_round_trip",
        effective_fees=0.0,
        effective_slippage=0.0,
        allocation_per_asset=1.0,
    )
    verification_plan = _build_verification_plan(prepared)

    monkeypatch.setattr(
        runner._vectorbt_runner,
        "build_parameters_for_strategy",
        lambda strategy, config_override=None: {
            "shared_config": {"dataset": "sample", "timeframe": "daily"},
            "entry_filter_params": {},
        },
    )
    monkeypatch.setattr(
        runner._vectorbt_runner.config_loader,
        "load_strategy_config",
        lambda strategy_name: {"strategy_params": {"name": strategy_name}},
    )
    monkeypatch.setattr(
        runner._vectorbt_runner.config_loader,
        "get_output_directory",
        lambda strategy_config: output_dir,
    )
    monkeypatch.setattr(
        "src.domains.backtest.nautilus_adapter._prepare_portfolio_inputs",
        lambda strategy_name, parameters: prepared,
    )
    monkeypatch.setattr(
        "src.domains.backtest.nautilus_adapter._load_nautilus_runtime",
        lambda: object(),
    )
    monkeypatch.setattr(
        "src.domains.backtest.nautilus_adapter._run_nautilus_engine",
        lambda runtime, *, prepared, verification_plan: {
            "engine": "nautilus",
            "engineVersion": "test",
            "strategyCount": 1,
            "totalSyntheticBars": 2,
            "symbols": ["1301"],
            "executionMode": "next_session_round_trip",
        },
    )
    monkeypatch.setattr(
        "src.domains.backtest.nautilus_adapter._build_verification_plan",
        lambda prepared_inputs: verification_plan,
    )

    result = runner.execute(
        "demo-strategy",
        run_spec=RunSpec(
            run_type=RunType.BACKTEST,
            strategy_name="demo-strategy",
            dataset_snapshot_id="sample",
            market_snapshot_id="market:latest",
            engine_family=EngineFamily.NAUTILUS,
            execution_policy_version="nautilus-daily-verification-v1",
        ),
        run_id="job-12345678",
    )

    assert result.html_path is None
    assert result.metrics_path is not None and result.metrics_path.exists()
    manifest_path = Path(str(result.summary["_manifest_path"]))
    engine_path = Path(str(result.summary["_engine_path"]))
    diagnostics_path = Path(str(result.summary["_diagnostics_path"]))
    assert manifest_path.exists()
    assert engine_path.exists()
    assert diagnostics_path.exists()
    assert result.summary["engine_family"] == "nautilus"


def test_prepare_portfolio_inputs_captures_round_trip_inputs(monkeypatch) -> None:
    class _FakeStrategy:
        def __init__(
            self,
            *,
            shared_config,
            entry_filter_params,
            exit_trigger_params,
            execution_adapter,
        ) -> None:
            _ = (entry_filter_params, exit_trigger_params)
            self.shared_config = shared_config
            self.execution_adapter = execution_adapter
            self.compiled_strategy = {"compiled": True}
            self.stock_codes = ["1301", "1332"]
            self.group_by = True
            self.cash_sharing = True

        def run_multi_backtest(self) -> None:
            index = pd.to_datetime(["2024-01-04"])
            self.execution_adapter.create_round_trip_portfolio(
                open_data=pd.DataFrame({"1301": [100.0], "1332": [50.0]}, index=index),
                close_data=pd.DataFrame({"1301": [102.0], "1332": [52.0]}, index=index),
                entries_data=pd.DataFrame({"1301": [True], "1332": [0]}, index=index),
                entry_size=0.5,
                entry_size_type=1,
                direction=1,
                fees=0.001,
                slippage=0.002,
                init_cash=1_000_000,
                max_size=1.0,
                cash_sharing=self.cash_sharing,
                group_by=self.group_by,
            )

    monkeypatch.setattr(adapter, "YamlConfigurableStrategy", _FakeStrategy)
    monkeypatch.setattr(
        adapter,
        "resolve_round_trip_execution_mode_name",
        lambda compiled_strategy: "next_session_round_trip",
    )

    prepared = adapter._prepare_portfolio_inputs(  # noqa: SLF001
        "demo",
        {
            "shared_config": {
                "dataset": "sample",
                "stock_codes": ["1301", "1332"],
                "initial_cash": 1_000_000,
                "timeframe": "daily",
                "direction": "longonly",
            }
        },
    )

    assert prepared.dataset_name == "sample"
    assert prepared.execution_mode == "next_session_round_trip"
    assert prepared.allocation_per_asset == 0.5
    assert prepared.entries_data.dtypes.tolist() == [bool, bool]
    assert prepared.effective_fees == 0.001
    assert prepared.effective_slippage == 0.002


@pytest.mark.parametrize(
    ("execution_mode", "timeframe", "direction", "group_by", "cash_sharing", "expected_error"),
    [
        ("unsupported_mode", "daily", "longonly", True, True, "supports only"),
        (
            "next_session_round_trip",
            "weekly",
            "longonly",
            True,
            True,
            "supports only daily timeframe",
        ),
        ("next_session_round_trip", "daily", "both", True, True, "supports only longonly direction"),
        (
            "next_session_round_trip",
            "daily",
            "longonly",
            False,
            True,
            "requires grouped cash-sharing round-trip execution",
        ),
        (
            "next_session_round_trip",
            "daily",
            "longonly",
            True,
            False,
            "requires grouped cash-sharing round-trip execution",
        ),
    ],
)
def test_prepare_portfolio_inputs_rejects_unsupported_config(
    monkeypatch,
    execution_mode: str,
    timeframe: str,
    direction: str,
    group_by: bool,
    cash_sharing: bool,
    expected_error: str,
) -> None:
    class _FakeStrategy:
        def __init__(self, *, shared_config, execution_adapter, **_: object) -> None:
            self.shared_config = shared_config
            self.execution_adapter = execution_adapter
            self.compiled_strategy = {"compiled": True}
            self.stock_codes = ["1301"]
            self.group_by = False
            self.cash_sharing = False

        def run_multi_backtest(self) -> None:
            index = pd.to_datetime(["2024-01-04"])
            self.execution_adapter.create_round_trip_portfolio(
                open_data=pd.DataFrame({"1301": [100.0]}, index=index),
                close_data=pd.DataFrame({"1301": [102.0]}, index=index),
                entries_data=pd.DataFrame({"1301": [True]}, index=index),
                entry_size=1.0,
                entry_size_type=1,
                direction=1,
                fees=0.0,
                slippage=0.0,
                init_cash=1_000_000,
                max_size=1.0,
                cash_sharing=cash_sharing,
                group_by=group_by,
            )

    monkeypatch.setattr(adapter, "YamlConfigurableStrategy", _FakeStrategy)
    monkeypatch.setattr(
        adapter,
        "resolve_round_trip_execution_mode_name",
        lambda compiled_strategy: execution_mode,
    )

    with pytest.raises(ValueError, match=expected_error):
        adapter._prepare_portfolio_inputs(  # noqa: SLF001
            "demo",
            {
                "shared_config": {
                    "dataset": "sample",
                    "stock_codes": ["1301"],
                    "initial_cash": 1_000_000,
                    "timeframe": timeframe,
                    "direction": direction,
                }
            },
        )


def test_prepare_portfolio_inputs_rejects_signal_portfolio_mode(monkeypatch) -> None:
    class _FakeStrategy:
        def __init__(self, *, shared_config, execution_adapter, **_: object) -> None:
            self.shared_config = shared_config
            self.execution_adapter = execution_adapter
            self.compiled_strategy = {"compiled": True}
            self.stock_codes = ["1301"]
            self.group_by = False
            self.cash_sharing = False

        def run_multi_backtest(self) -> None:
            index = pd.to_datetime(["2024-01-04"])
            self.execution_adapter.create_signal_portfolio(
                close=pd.DataFrame({"1301": [101.0]}, index=index),
                entries=pd.DataFrame({"1301": [True]}, index=index),
                exits=pd.DataFrame({"1301": [False]}, index=index),
                direction="longonly",
                init_cash=1_000_000,
                fees=0.0,
                slippage=0.0,
            )

    monkeypatch.setattr(adapter, "YamlConfigurableStrategy", _FakeStrategy)
    monkeypatch.setattr(
        adapter,
        "resolve_round_trip_execution_mode_name",
        lambda compiled_strategy: "next_session_round_trip",
    )

    with pytest.raises(ValueError, match="does not support signal-portfolio mode"):
        adapter._prepare_portfolio_inputs(  # noqa: SLF001
            "demo",
            {
                "shared_config": {
                    "dataset": "sample",
                    "stock_codes": ["1301"],
                    "initial_cash": 1_000_000,
                    "timeframe": "daily",
                    "direction": "longonly",
                }
            },
        )


def test_prepare_portfolio_inputs_requires_round_trip_capture(monkeypatch) -> None:
    class _FakeStrategy:
        def __init__(self, *, shared_config, execution_adapter, **_: object) -> None:
            self.shared_config = shared_config
            self.execution_adapter = execution_adapter
            self.compiled_strategy = {"compiled": True}
            self.stock_codes = ["1301"]
            self.group_by = False
            self.cash_sharing = False

        def run_multi_backtest(self) -> None:
            return None

    monkeypatch.setattr(adapter, "YamlConfigurableStrategy", _FakeStrategy)
    monkeypatch.setattr(
        adapter,
        "resolve_round_trip_execution_mode_name",
        lambda compiled_strategy: "next_session_round_trip",
    )

    with pytest.raises(RuntimeError, match="Failed to capture round-trip execution inputs"):
        adapter._prepare_portfolio_inputs(  # noqa: SLF001
            "demo",
            {
                "shared_config": {
                    "dataset": "sample",
                    "stock_codes": ["1301"],
                    "initial_cash": 1_000_000,
                    "timeframe": "daily",
                    "direction": "longonly",
                }
            },
        )


def test_nautilus_helper_functions_cover_edge_cases(monkeypatch, tmp_path: Path) -> None:
    payload = adapter._sanitize_json_payload(  # noqa: SLF001
        {"items": (1, float("nan")), "nested": [float("inf"), {"value": 3.0}]}
    )
    assert payload == {"items": [1, None], "nested": [None, {"value": 3.0}]}
    assert adapter._coerce_signal_params(None) is None  # noqa: SLF001
    assert isinstance(adapter._coerce_signal_params({"enabled": True}), object)  # noqa: SLF001
    marker = object()
    assert adapter._coerce_signal_params(marker) is marker  # noqa: SLF001
    assert adapter._CapturedExecutionAdapter().build_summary_metrics(object()) is None  # noqa: SLF001

    report_paths = SimpleNamespace(metrics_path=tmp_path / "demo.metrics.json")
    metrics_path, engine_path, diagnostics_path = adapter._metrics_artifact_paths(  # noqa: SLF001
        report_paths
    )
    assert metrics_path.name == "demo.metrics.json"
    assert engine_path.name == "demo.engine.json"
    assert diagnostics_path.name == "demo.diagnostics.json"

    event_ts = adapter._daily_event_timestamp(  # noqa: SLF001
        pd.Timestamp("2024-01-04"),
        adapter._OPEN_EVENT_TIME,  # noqa: SLF001
    )
    assert event_ts.tzinfo is not None
    assert event_ts.hour == 9

    bars = _build_bars_dataframe(
        pd.Series([100.0], index=pd.to_datetime(["2024-01-04"])),
        pd.Series([105.0], index=pd.to_datetime(["2024-01-04"])),
    )
    assert list(bars["close"]) == [100.0, 105.0]
    with pytest.raises(ValueError, match="No executable bars"):
        _build_bars_dataframe(
            pd.Series([float("nan")], index=pd.to_datetime(["2024-01-04"])),
            pd.Series([float("nan")], index=pd.to_datetime(["2024-01-04"])),
        )

    assert adapter._annualized_sharpe_ratio(pd.Series(dtype=float)) == 0.0  # noqa: SLF001
    assert adapter._annualized_sortino_ratio(pd.Series([0.01, 0.02])) is None  # noqa: SLF001
    assert adapter._profit_factor([]) is None  # noqa: SLF001

    class _Currency:
        @staticmethod
        def from_str(value: str) -> str:
            return f"currency:{value}"

    runtime = SimpleNamespace(JPY="JPY", Currency=_Currency, Quantity=int)
    assert adapter._resolve_base_currency(runtime) == "JPY"  # noqa: SLF001
    runtime.JPY = None
    assert adapter._resolve_base_currency(runtime) == "currency:JPY"  # noqa: SLF001
    runtime.Currency = SimpleNamespace(from_str="not-callable")
    assert adapter._resolve_base_currency(runtime) == "JPY"  # noqa: SLF001
    runtime.Currency = None
    assert adapter._resolve_base_currency(runtime) == "JPY"  # noqa: SLF001

    class _Quantity:
        @staticmethod
        def from_int(value: int) -> tuple[str, int]:
            return ("int", value)

    assert adapter._build_quantity(SimpleNamespace(Quantity=_Quantity), 3) == (  # noqa: SLF001
        "int",
        3,
    )

    class _FallbackQuantity:
        @staticmethod
        def from_str(value: str) -> tuple[str, str]:
            return ("str", value)

    assert adapter._build_quantity(  # noqa: SLF001
        SimpleNamespace(Quantity=_FallbackQuantity),
        4,
    ) == ("str", "4")

    def _import_module(name: str):
        raise ModuleNotFoundError(name)

    monkeypatch.setattr(adapter.importlib, "import_module", _import_module)
    with pytest.raises(MissingNautilusDependencyError):
        adapter._load_nautilus_runtime()  # noqa: SLF001


def test_nautilus_low_level_helper_fallbacks(monkeypatch) -> None:
    signal_params = SignalParams(enabled=True)
    assert adapter._module_attr(  # noqa: SLF001
        primary=SimpleNamespace(name="primary"),
        fallback=SimpleNamespace(name="fallback"),
        name="name",
    ) == "primary"
    assert adapter._module_attr(  # noqa: SLF001
        primary=SimpleNamespace(),
        fallback=SimpleNamespace(name="fallback"),
        name="name",
    ) == "fallback"
    assert adapter._coerce_signal_params(None) is None  # noqa: SLF001
    assert adapter._coerce_signal_params(signal_params) is signal_params  # noqa: SLF001
    assert isinstance(adapter._coerce_signal_params({"enabled": True}), SignalParams)  # noqa: SLF001
    passthrough = object()
    assert adapter._coerce_signal_params(passthrough) is passthrough  # noqa: SLF001

    assert adapter._max_affordable_quantity(  # noqa: SLF001
        budget=0.0,
        open_price=100.0,
        effective_slippage=0.0,
        effective_fees=0.0,
    ) == 0
    assert adapter._max_affordable_quantity(  # noqa: SLF001
        budget=1000.0,
        open_price=-1.0,
        effective_slippage=0.0,
        effective_fees=0.0,
    ) == 0
    assert adapter._max_affordable_quantity(  # noqa: SLF001
        budget=1000.0,
        open_price=100.0,
        effective_slippage=-1.0,
        effective_fees=0.0,
    ) == 0
    assert adapter._max_affordable_quantity(  # noqa: SLF001
        budget=1000.0,
        open_price=100.0,
        effective_slippage=0.0,
        effective_fees=0.0,
    ) == 10

    class _Provider:
        kwargs_attempts = 0
        args_attempts = 0

        @classmethod
        def equity(cls, *args: object, **kwargs: object) -> object:
            if kwargs:
                cls.kwargs_attempts += 1
                raise RuntimeError("kwargs unsupported")
            cls.args_attempts += 1
            if args:
                return SimpleNamespace(id=f"SIM-{args[0]}")
            raise RuntimeError("missing symbol")

    instrument = adapter._build_instrument(  # noqa: SLF001
        SimpleNamespace(TestInstrumentProvider=_Provider),
        "1301",
        "SIM",
    )
    assert instrument.id == "SIM-1301"
    assert _Provider.kwargs_attempts == 2
    assert _Provider.args_attempts == 2

    class _AlwaysFailProvider:
        @staticmethod
        def equity(*args: object, **kwargs: object) -> object:
            _ = (args, kwargs)
            raise RuntimeError("fail")

    with pytest.raises(RuntimeError, match="Unable to construct"):
        adapter._build_instrument(  # noqa: SLF001
            SimpleNamespace(TestInstrumentProvider=_AlwaysFailProvider),
            "1301",
            "SIM",
        )

    class _FallbackBarType:
        def __call__(self, *args: object, **kwargs: object) -> object:
            _ = (args, kwargs)
            raise RuntimeError("boom")

        @staticmethod
        def from_str(value: str) -> str:
            return f"fallback:{value}"

    assert adapter._build_bar_type(  # noqa: SLF001
        SimpleNamespace(
            BarSpecification=lambda **kwargs: kwargs,
            BarAggregation=SimpleNamespace(MINUTE="minute"),
            PriceType=SimpleNamespace(LAST="last"),
            AggregationSource=SimpleNamespace(EXTERNAL="external"),
            BarType=_FallbackBarType(),
        ),
        SimpleNamespace(id="SIM-1301"),
    ) == "fallback:SIM-1301-1-MINUTE-LAST-EXTERNAL"

    class _ExplodingBarType:
        def __call__(self, *args: object, **kwargs: object) -> object:
            _ = (args, kwargs)
            raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        adapter._build_bar_type(  # noqa: SLF001
            SimpleNamespace(
                BarSpecification=lambda **kwargs: kwargs,
                BarAggregation=SimpleNamespace(MINUTE="minute"),
                PriceType=SimpleNamespace(LAST="last"),
                AggregationSource=SimpleNamespace(EXTERNAL="external"),
                BarType=_ExplodingBarType(),
            ),
            SimpleNamespace(id="SIM-1301"),
        )

    def _metadata_version(name: str) -> str:
        if name == "nautilus_trader":
            raise adapter.metadata.PackageNotFoundError
        if name == "nautilus-trader":
            return "fallback-version"
        raise AssertionError(name)

    monkeypatch.setattr(adapter.metadata, "version", _metadata_version)
    assert adapter._resolve_nautilus_version() == "fallback-version"  # noqa: SLF001

    monkeypatch.setattr(
        adapter.metadata,
        "version",
        lambda name: (_ for _ in ()).throw(adapter.metadata.PackageNotFoundError()),
    )
    assert adapter._resolve_nautilus_version() is None  # noqa: SLF001


def test_build_verification_plan_tracks_missing_price_skips() -> None:
    prepared = _PreparedPortfolioInputs(
        strategy_name="demo",
        dataset_name="sample",
        shared_config=SharedConfig(
            dataset="sample",
            stock_codes=["1301"],
            initial_cash=1_000_000,
            timeframe="daily",
            direction="longonly",
            group_by=True,
            cash_sharing=True,
        ),
        compiled_strategy=None,
        open_data=pd.DataFrame(
            {"1301": [float("nan")]},
            index=pd.to_datetime(["2024-01-04"]),
        ),
        close_data=pd.DataFrame(
            {"1301": [105.0]},
            index=pd.to_datetime(["2024-01-04"]),
        ),
        entries_data=pd.DataFrame(
            {"1301": [True]},
            index=pd.to_datetime(["2024-01-04"]),
        ),
        execution_mode="next_session_round_trip",
        effective_fees=0.0,
        effective_slippage=0.0,
        allocation_per_asset=1.0,
    )

    plan = _build_verification_plan(prepared)

    assert plan.summary_metrics.trade_count == 0
    assert plan.diagnostics["skippedMissingPricesCount"] == 1


def test_build_strategy_and_engine_helpers_submit_expected_orders() -> None:
    class _FakeQuantity:
        @staticmethod
        def from_int(value: int) -> int:
            return value

    class _FakeOrderFactory:
        def market(self, **kwargs: object) -> dict[str, object]:
            return dict(kwargs)

    class _FakeStrategyBase:
        def __init__(self) -> None:
            self.subscriptions: list[object] = []
            self.orders: list[dict[str, object]] = []
            self.order_factory = _FakeOrderFactory()

        def subscribe_bars(self, bar_type: object) -> None:
            self.subscriptions.append(bar_type)

        def submit_order(self, order: dict[str, object]) -> None:
            self.orders.append(order)

    runtime = SimpleNamespace(
        Strategy=_FakeStrategyBase,
        OrderSide=SimpleNamespace(BUY="BUY", SELL="SELL"),
        Quantity=_FakeQuantity,
    )
    trade_plan = _TradePlan(
        code="1301",
        trade_date=pd.Timestamp("2024-01-04"),
        open_ts=adapter._daily_event_timestamp(pd.Timestamp("2024-01-04"), adapter._OPEN_EVENT_TIME),  # noqa: SLF001
        close_ts=adapter._daily_event_timestamp(pd.Timestamp("2024-01-04"), adapter._CLOSE_EVENT_TIME),  # noqa: SLF001
        quantity=10,
        open_price=100.0,
        close_price=105.0,
        pnl=50.0,
        gross_return=5.0,
        net_return=4.8,
    )
    strategy = adapter._build_strategy(  # noqa: SLF001
        runtime,
        instrument_id="SIM-1301",
        bar_type="bar-type",
        trade_plans=[trade_plan],
    )
    strategy.on_start()
    strategy.on_bar(SimpleNamespace(ts_event=int(trade_plan.open_ts.tz_convert("UTC").value)))
    strategy.on_bar(SimpleNamespace(ts_event=int(trade_plan.close_ts.tz_convert("UTC").value)))
    strategy.on_bar(SimpleNamespace(ts_event=0))

    zero_quantity_plan = _TradePlan(
        code="1301",
        trade_date=pd.Timestamp("2024-01-04"),
        open_ts=trade_plan.open_ts,
        close_ts=trade_plan.close_ts,
        quantity=0,
        open_price=100.0,
        close_price=105.0,
        pnl=0.0,
        gross_return=0.0,
        net_return=0.0,
    )
    zero_quantity_strategy = adapter._build_strategy(  # noqa: SLF001
        runtime,
        instrument_id="SIM-1301",
        bar_type="bar-type",
        trade_plans=[zero_quantity_plan],
    )
    zero_quantity_strategy.on_bar(
        SimpleNamespace(ts_event=int(trade_plan.open_ts.tz_convert("UTC").value))
    )

    assert strategy.subscriptions == ["bar-type"]
    assert [order["order_side"] for order in strategy.orders] == ["BUY", "SELL"]


def test_build_strategy_ignores_unmatched_and_zero_quantity_actions() -> None:
    class _FakeStrategyBase:
        def __init__(self) -> None:
            self.subscriptions: list[object] = []
            self.orders: list[dict[str, object]] = []
            self.order_factory = SimpleNamespace(market=lambda **kwargs: dict(kwargs))

        def subscribe_bars(self, bar_type: object) -> None:
            self.subscriptions.append(bar_type)

        def submit_order(self, order: dict[str, object]) -> None:
            self.orders.append(order)

    runtime = SimpleNamespace(
        Strategy=_FakeStrategyBase,
        OrderSide=SimpleNamespace(BUY="BUY", SELL="SELL"),
        Quantity=SimpleNamespace(from_int=lambda value: value),
    )
    zero_trade = _TradePlan(
        code="1301",
        trade_date=pd.Timestamp("2024-01-04"),
        open_ts=adapter._daily_event_timestamp(pd.Timestamp("2024-01-04"), adapter._OPEN_EVENT_TIME),  # noqa: SLF001
        close_ts=adapter._daily_event_timestamp(pd.Timestamp("2024-01-04"), adapter._CLOSE_EVENT_TIME),  # noqa: SLF001
        quantity=0,
        open_price=100.0,
        close_price=100.0,
        pnl=0.0,
        gross_return=0.0,
        net_return=0.0,
    )
    strategy = adapter._build_strategy(  # noqa: SLF001
        runtime,
        instrument_id="SIM-1301",
        bar_type="bar-type",
        trade_plans=[zero_trade],
    )
    strategy.on_bar(SimpleNamespace(ts_event=0))
    strategy.on_bar(SimpleNamespace(ts_event=int(zero_trade.open_ts.tz_convert("UTC").value)))
    assert strategy.orders == []

    class _BarTypeWithoutFallback:
        def __init__(self, *args: object, **kwargs: object) -> None:
            raise RuntimeError("force failure")

    runtime_without_bar_fallback = SimpleNamespace(
        BarSpecification=lambda **kwargs: kwargs,
        BarAggregation=SimpleNamespace(MINUTE="minute"),
        PriceType=SimpleNamespace(LAST="last"),
        AggregationSource=SimpleNamespace(EXTERNAL="external"),
        BarType=_BarTypeWithoutFallback,
    )
    with pytest.raises(RuntimeError, match="force failure"):
        adapter._build_bar_type(runtime_without_bar_fallback, "SIM-1301")  # noqa: SLF001


def test_run_nautilus_engine_uses_fake_runtime_and_records_metadata(monkeypatch) -> None:
    disposed = {"value": False}

    class _FakeEngine:
        def __init__(self, *, config) -> None:
            self.config = config
            self.venues: list[object] = []
            self.instruments: list[object] = []
            self.data: list[object] = []
            self.strategies: list[object] = []

        def add_venue(self, **kwargs: object) -> None:
            self.venues.append(kwargs)

        def add_instrument(self, instrument: object) -> None:
            self.instruments.append(instrument)

        def add_data(self, bars: object) -> None:
            self.data.append(bars)

        def add_strategy(self, strategy: object) -> None:
            self.strategies.append(strategy)

        def run(self) -> None:
            return None

        def dispose(self) -> None:
            disposed["value"] = True

    class _FakeProvider:
        @staticmethod
        def equity(*args: object, **kwargs: object) -> object:
            symbol = kwargs.get("symbol") or (args[0] if args else "1301")
            return SimpleNamespace(id=f"SIM-{symbol}")

    class _FakeWrangler:
        def __init__(self, bar_type: object, instrument: object) -> None:
            _ = (bar_type, instrument)

        def process(self, bars_df: pd.DataFrame) -> list[dict[str, object]]:
            return bars_df.reset_index().to_dict("records")

    runtime = SimpleNamespace(
        BacktestEngine=_FakeEngine,
        BacktestEngineConfig=lambda **kwargs: kwargs,
        LoggingConfig=lambda **kwargs: kwargs,
        Venue=lambda value: value,
        Money=lambda amount, currency: (amount, currency),
        AccountType=SimpleNamespace(CASH="cash"),
        OmsType=SimpleNamespace(NETTING="netting"),
        TestInstrumentProvider=_FakeProvider,
        BarDataWrangler=_FakeWrangler,
        BarType=lambda instrument_id, bar_spec, source: (instrument_id, bar_spec, source),
        BarSpecification=lambda **kwargs: kwargs,
        AggregationSource=SimpleNamespace(EXTERNAL="external"),
        BarAggregation=SimpleNamespace(MINUTE="minute"),
        PriceType=SimpleNamespace(LAST="last"),
        OrderSide=SimpleNamespace(BUY="BUY", SELL="SELL"),
        Quantity=SimpleNamespace(from_int=lambda value: value),
        Strategy=type("FakeStrategyBase", (), {"__init__": lambda self: None}),
        JPY="JPY",
    )
    monkeypatch.setattr(adapter, "_resolve_nautilus_version", lambda: "test-version")

    prepared = _PreparedPortfolioInputs(
        strategy_name="demo",
        dataset_name="sample",
        shared_config=SharedConfig(
            dataset="sample",
            stock_codes=["1301"],
            initial_cash=1_000_000,
            timeframe="daily",
            direction="longonly",
            group_by=True,
            cash_sharing=True,
        ),
        compiled_strategy=None,
        open_data=pd.DataFrame({"1301": [100.0]}, index=pd.to_datetime(["2024-01-04"])),
        close_data=pd.DataFrame({"1301": [105.0]}, index=pd.to_datetime(["2024-01-04"])),
        entries_data=pd.DataFrame({"1301": [True]}, index=pd.to_datetime(["2024-01-04"])),
        execution_mode="next_session_round_trip",
        effective_fees=0.0,
        effective_slippage=0.0,
        allocation_per_asset=1.0,
    )
    verification_plan = _build_verification_plan(prepared)

    payload = adapter._run_nautilus_engine(  # noqa: SLF001
        runtime,
        prepared=prepared,
        verification_plan=verification_plan,
    )

    assert payload["engine"] == "nautilus"
    assert payload["engineVersion"] == "test-version"
    assert payload["strategyCount"] == 1
    assert payload["totalSyntheticBars"] == 2
    assert disposed["value"] is True


def test_build_verification_plan_handles_missing_prices_without_trades() -> None:
    prepared = _PreparedPortfolioInputs(
        strategy_name="demo",
        dataset_name="sample",
        shared_config=SharedConfig(
            dataset="sample",
            stock_codes=["1301"],
            initial_cash=1_000_000,
            timeframe="daily",
            direction="longonly",
            group_by=True,
            cash_sharing=True,
        ),
        compiled_strategy=None,
        open_data=pd.DataFrame({"1301": [float("nan")]}, index=pd.to_datetime(["2024-01-04"])),
        close_data=pd.DataFrame({"1301": [105.0]}, index=pd.to_datetime(["2024-01-04"])),
        entries_data=pd.DataFrame({"1301": [True]}, index=pd.to_datetime(["2024-01-04"])),
        execution_mode="next_session_round_trip",
        effective_fees=0.0,
        effective_slippage=0.0,
        allocation_per_asset=1.0,
    )

    plan = _build_verification_plan(prepared)

    assert plan.summary_metrics.trade_count == 0
    assert plan.diagnostics["skippedMissingPricesCount"] == 1


def test_nautilus_verification_runner_reports_progress(monkeypatch, tmp_path: Path) -> None:
    runner = NautilusVerificationRunner()
    output_dir = Path.cwd() / ".tmp" / "nautilus-progress"
    output_dir.mkdir(parents=True, exist_ok=True)
    prepared = _PreparedPortfolioInputs(
        strategy_name="demo",
        dataset_name="sample",
        shared_config=SharedConfig(
            dataset="sample",
            stock_codes=["1301"],
            initial_cash=1_000_000,
            timeframe="daily",
            direction="longonly",
            group_by=True,
            cash_sharing=True,
        ),
        compiled_strategy=None,
        open_data=pd.DataFrame({"1301": [100.0]}, index=pd.to_datetime(["2024-01-04"])),
        close_data=pd.DataFrame({"1301": [105.0]}, index=pd.to_datetime(["2024-01-04"])),
        entries_data=pd.DataFrame({"1301": [True]}, index=pd.to_datetime(["2024-01-04"])),
        execution_mode="next_session_round_trip",
        effective_fees=0.0,
        effective_slippage=0.0,
        allocation_per_asset=1.0,
    )
    verification_plan = _build_verification_plan(prepared)
    progress_updates: list[str] = []

    monkeypatch.setattr(
        runner._vectorbt_runner,
        "build_parameters_for_strategy",
        lambda strategy, config_override=None: {"shared_config": {"dataset": "sample", "timeframe": "daily"}},
    )
    monkeypatch.setattr(
        runner._vectorbt_runner.config_loader,
        "load_strategy_config",
        lambda strategy_name: {"strategy_params": {"name": strategy_name}},
    )
    monkeypatch.setattr(
        runner._vectorbt_runner.config_loader,
        "get_output_directory",
        lambda strategy_config: output_dir,
    )
    monkeypatch.setattr(adapter, "_prepare_portfolio_inputs", lambda strategy_name, parameters: prepared)
    monkeypatch.setattr(adapter, "_load_nautilus_runtime", lambda: object())
    monkeypatch.setattr(
        adapter,
        "_run_nautilus_engine",
        lambda runtime, *, prepared, verification_plan: {
            "engine": "nautilus",
            "engineVersion": "test",
            "strategyCount": 1,
            "totalSyntheticBars": 2,
            "symbols": ["1301"],
            "executionMode": "next_session_round_trip",
        },
    )
    monkeypatch.setattr(adapter, "_build_verification_plan", lambda prepared_inputs: verification_plan)

    runner.execute(
        "demo-strategy",
        run_spec=RunSpec(
            run_type=RunType.BACKTEST,
            strategy_name="demo-strategy",
            dataset_snapshot_id="sample",
            market_snapshot_id="market:latest",
            engine_family=EngineFamily.NAUTILUS,
            execution_policy_version="nautilus-daily-verification-v1",
        ),
        run_id="job-progress",
        progress_callback=lambda status, elapsed: progress_updates.append(status),
    )

    assert progress_updates == [
        "Nautilus verification の入力を準備中...",
        "Nautilus verification を実行中...",
    ]


def test_run_nautilus_engine_skips_strategy_for_symbols_without_trade_plan(monkeypatch) -> None:
    class _EngineWithoutDispose:
        def __init__(self, *, config) -> None:
            self.config = config
            self.strategies: list[object] = []

        def add_venue(self, **kwargs: object) -> None:
            return None

        def add_instrument(self, instrument: object) -> None:
            return None

        def add_data(self, bars: object) -> None:
            return None

        def add_strategy(self, strategy: object) -> None:
            self.strategies.append(strategy)

        def run(self) -> None:
            return None

    class _FakeProvider:
        @staticmethod
        def equity(*args: object, **kwargs: object) -> object:
            symbol = kwargs.get("symbol") or (args[0] if args else "1301")
            return SimpleNamespace(id=f"SIM-{symbol}")

    class _FakeWrangler:
        def __init__(self, bar_type: object, instrument: object) -> None:
            _ = (bar_type, instrument)

        def process(self, bars_df: pd.DataFrame) -> list[dict[str, object]]:
            return bars_df.reset_index().to_dict("records")

    runtime = SimpleNamespace(
        BacktestEngine=_EngineWithoutDispose,
        BacktestEngineConfig=lambda **kwargs: kwargs,
        LoggingConfig=lambda **kwargs: kwargs,
        Venue=lambda value: value,
        Money=lambda amount, currency: (amount, currency),
        AccountType=SimpleNamespace(CASH="cash"),
        OmsType=SimpleNamespace(NETTING="netting"),
        TestInstrumentProvider=_FakeProvider,
        BarDataWrangler=_FakeWrangler,
        BarType=lambda instrument_id, bar_spec, source: (instrument_id, bar_spec, source),
        BarSpecification=lambda **kwargs: kwargs,
        AggregationSource=SimpleNamespace(EXTERNAL="external"),
        BarAggregation=SimpleNamespace(MINUTE="minute"),
        PriceType=SimpleNamespace(LAST="last"),
        OrderSide=SimpleNamespace(BUY="BUY", SELL="SELL"),
        Quantity=SimpleNamespace(from_int=lambda value: value),
        Strategy=type("FakeStrategyBase", (), {"__init__": lambda self: None}),
        JPY="JPY",
    )
    monkeypatch.setattr(adapter, "_resolve_nautilus_version", lambda: "test-version")

    prepared = _PreparedPortfolioInputs(
        strategy_name="demo",
        dataset_name="sample",
        shared_config=SharedConfig(
            dataset="sample",
            stock_codes=["1301"],
            initial_cash=1_000_000,
            timeframe="daily",
            direction="longonly",
            group_by=True,
            cash_sharing=True,
        ),
        compiled_strategy=None,
        open_data=pd.DataFrame({"1301": [100.0]}, index=pd.to_datetime(["2024-01-04"])),
        close_data=pd.DataFrame({"1301": [105.0]}, index=pd.to_datetime(["2024-01-04"])),
        entries_data=pd.DataFrame({"1301": [False]}, index=pd.to_datetime(["2024-01-04"])),
        execution_mode="next_session_round_trip",
        effective_fees=0.0,
        effective_slippage=0.0,
        allocation_per_asset=1.0,
    )
    verification_plan = _build_verification_plan(prepared)

    payload = adapter._run_nautilus_engine(  # noqa: SLF001
        runtime,
        prepared=prepared,
        verification_plan=verification_plan,
    )

    assert payload["strategyCount"] == 0


def test_run_nautilus_engine_skips_symbols_without_executable_bars(monkeypatch) -> None:
    class _EngineWithoutDispose:
        def __init__(self, *, config) -> None:
            self.config = config
            self.data: list[object] = []

        def add_venue(self, **kwargs: object) -> None:
            return None

        def add_instrument(self, instrument: object) -> None:
            return None

        def add_data(self, bars: object) -> None:
            self.data.append(bars)

        def add_strategy(self, strategy: object) -> None:
            return None

        def run(self) -> None:
            return None

    class _FakeProvider:
        @staticmethod
        def equity(*args: object, **kwargs: object) -> object:
            symbol = kwargs.get("symbol") or (args[0] if args else "1301")
            return SimpleNamespace(id=f"SIM-{symbol}")

    class _FakeWrangler:
        def __init__(self, bar_type: object, instrument: object) -> None:
            _ = (bar_type, instrument)

        def process(self, bars_df: pd.DataFrame) -> list[dict[str, object]]:
            return bars_df.reset_index().to_dict("records")

    runtime = SimpleNamespace(
        BacktestEngine=_EngineWithoutDispose,
        BacktestEngineConfig=lambda **kwargs: kwargs,
        LoggingConfig=lambda **kwargs: kwargs,
        Venue=lambda value: value,
        Money=lambda amount, currency: (amount, currency),
        AccountType=SimpleNamespace(CASH="cash"),
        OmsType=SimpleNamespace(NETTING="netting"),
        TestInstrumentProvider=_FakeProvider,
        BarDataWrangler=_FakeWrangler,
        BarType=lambda instrument_id, bar_spec, source: (instrument_id, bar_spec, source),
        BarSpecification=lambda **kwargs: kwargs,
        AggregationSource=SimpleNamespace(EXTERNAL="external"),
        BarAggregation=SimpleNamespace(MINUTE="minute"),
        PriceType=SimpleNamespace(LAST="last"),
        OrderSide=SimpleNamespace(BUY="BUY", SELL="SELL"),
        Quantity=SimpleNamespace(from_int=lambda value: value),
        Strategy=type("FakeStrategyBase", (), {"__init__": lambda self: None}),
        JPY="JPY",
    )
    monkeypatch.setattr(adapter, "_resolve_nautilus_version", lambda: "test-version")

    prepared = _PreparedPortfolioInputs(
        strategy_name="demo",
        dataset_name="sample",
        shared_config=SharedConfig(
            dataset="sample",
            stock_codes=["1301", "1332"],
            initial_cash=1_000_000,
            timeframe="daily",
            direction="longonly",
            group_by=True,
            cash_sharing=True,
        ),
        compiled_strategy=None,
        open_data=pd.DataFrame(
            {"1301": [100.0], "1332": [float("nan")]},
            index=pd.to_datetime(["2024-01-04"]),
        ),
        close_data=pd.DataFrame(
            {"1301": [105.0], "1332": [float("nan")]},
            index=pd.to_datetime(["2024-01-04"]),
        ),
        entries_data=pd.DataFrame(
            {"1301": [False], "1332": [False]},
            index=pd.to_datetime(["2024-01-04"]),
        ),
        execution_mode="next_session_round_trip",
        effective_fees=0.0,
        effective_slippage=0.0,
        allocation_per_asset=0.5,
    )
    verification_plan = _build_verification_plan(prepared)

    payload = adapter._run_nautilus_engine(  # noqa: SLF001
        runtime,
        prepared=prepared,
        verification_plan=verification_plan,
    )

    assert payload["strategyCount"] == 0
    assert payload["totalSyntheticBars"] == 2


def test_resolve_nautilus_version_handles_success_and_total_miss(monkeypatch) -> None:
    monkeypatch.setattr(adapter.metadata, "version", lambda package_name: "1.2.3")
    assert adapter._resolve_nautilus_version() == "1.2.3"  # noqa: SLF001

    def _missing_version(_package_name: str) -> str:
        raise adapter.metadata.PackageNotFoundError("missing")  # type: ignore[arg-type]

    monkeypatch.setattr(adapter.metadata, "version", _missing_version)
    assert adapter._resolve_nautilus_version() is None  # noqa: SLF001
