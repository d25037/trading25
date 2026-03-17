"""Nautilus verification engine adapter for daily round-trip backtests."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import time
import importlib
from importlib import metadata
import json
import math
from pathlib import Path
from time import perf_counter
from typing import Any, Callable, cast
from zoneinfo import ZoneInfo

import pandas as pd
from loguru import logger

from src.domains.backtest.contracts import CanonicalExecutionMetrics, EngineFamily, RunSpec
from src.domains.backtest.core.artifacts import BacktestArtifactWriter
from src.domains.backtest.core.marimo_executor import MarimoExecutor
from src.domains.backtest.core.runner import BacktestResult, BacktestRunner
from src.domains.strategy.core.yaml_configurable_strategy import YamlConfigurableStrategy
from src.domains.strategy.runtime.compiler import resolve_round_trip_execution_mode_name
from src.shared.models.config import SharedConfig
from src.shared.models.signals import SignalParams

_JST = ZoneInfo("Asia/Tokyo")
_OPEN_EVENT_TIME = time(9, 0)
_CLOSE_EVENT_TIME = time(15, 0)
_SUPPORTED_EXECUTION_MODES = {
    "next_session_round_trip",
    "current_session_round_trip",
}


class MissingNautilusDependencyError(RuntimeError):
    """Raised when the optional Nautilus dependency is unavailable."""


@dataclass(slots=True)
class _RuntimeModule:
    BacktestEngine: Any
    BacktestEngineConfig: Any
    LoggingConfig: Any
    Venue: Any
    Money: Any
    AccountType: Any
    OmsType: Any
    TestInstrumentProvider: Any
    BarDataWrangler: Any
    BarType: Any
    BarSpecification: Any
    AggregationSource: Any
    BarAggregation: Any
    PriceType: Any
    OrderSide: Any
    Quantity: Any
    Strategy: Any
    JPY: Any = None
    Currency: Any = None


@dataclass(slots=True)
class _PreparedPortfolioInputs:
    strategy_name: str
    dataset_name: str
    shared_config: SharedConfig
    compiled_strategy: Any
    open_data: pd.DataFrame
    close_data: pd.DataFrame
    entries_data: pd.DataFrame
    execution_mode: str
    effective_fees: float
    effective_slippage: float
    allocation_per_asset: float


@dataclass(slots=True)
class _TradePlan:
    code: str
    trade_date: pd.Timestamp
    open_ts: pd.Timestamp
    close_ts: pd.Timestamp
    quantity: int
    open_price: float
    close_price: float
    pnl: float
    gross_return: float
    net_return: float


@dataclass(slots=True)
class _VerificationPlan:
    trades_by_code: dict[str, list[_TradePlan]]
    trade_records: list[dict[str, Any]]
    metrics_payload: dict[str, Any]
    summary_metrics: CanonicalExecutionMetrics
    diagnostics: dict[str, Any]


@dataclass(slots=True)
class _CapturedExecutionAdapter:
    round_trip_inputs: tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame] | None = None
    effective_fees: float | None = None
    effective_slippage: float | None = None
    entry_size: float | None = None
    max_size: float | None = None
    cash_sharing: bool | None = None
    group_by: bool | None = None
    signal_portfolio_requested: bool = False

    def create_signal_portfolio(
        self,
        *,
        close: pd.DataFrame,
        entries: pd.DataFrame,
        exits: pd.DataFrame,
        direction: str,
        init_cash: float,
        fees: float,
        slippage: float,
        cash_sharing: bool = False,
        group_by: bool | None = None,
        accumulate: bool = False,
        size: float | None = None,
        size_type: str | None = None,
        call_seq: str | None = None,
        max_size: float | None = None,
        freq: str = "D",
    ) -> Any:
        _ = (
            close,
            entries,
            exits,
            direction,
            init_cash,
            fees,
            slippage,
            cash_sharing,
            group_by,
            accumulate,
            size,
            size_type,
            call_seq,
            max_size,
            freq,
        )
        self.signal_portfolio_requested = True
        self.effective_fees = float(fees)
        self.effective_slippage = float(slippage)
        return object()

    def create_round_trip_portfolio(
        self,
        *,
        open_data: pd.DataFrame,
        close_data: pd.DataFrame,
        entries_data: pd.DataFrame,
        entry_size: float,
        entry_size_type: int,
        direction: int,
        fees: float,
        slippage: float,
        init_cash: float,
        max_size: float,
        cash_sharing: bool,
        group_by: bool | None,
        freq: str = "D",
    ) -> Any:
        _ = (
            entry_size_type,
            direction,
            init_cash,
            freq,
        )
        self.round_trip_inputs = (
            open_data.copy(),
            close_data.copy(),
            entries_data.copy(),
        )
        self.effective_fees = float(fees)
        self.effective_slippage = float(slippage)
        self.entry_size = float(entry_size)
        self.max_size = float(max_size)
        self.cash_sharing = bool(cash_sharing)
        self.group_by = group_by
        return object()

    def build_summary_metrics(self, portfolio: Any) -> None:
        _ = portfolio
        return None


def _load_nautilus_runtime() -> _RuntimeModule:
    try:
        model_data = importlib.import_module("nautilus_trader.model.data")
        model_enums = importlib.import_module("nautilus_trader.model.enums")
        model_objects = importlib.import_module("nautilus_trader.model.objects")
        return _RuntimeModule(
            BacktestEngine=importlib.import_module(
                "nautilus_trader.backtest.engine"
            ).BacktestEngine,
            BacktestEngineConfig=importlib.import_module(
                "nautilus_trader.backtest.config"
            ).BacktestEngineConfig,
            LoggingConfig=importlib.import_module(
                "nautilus_trader.common.config"
            ).LoggingConfig,
            Venue=importlib.import_module("nautilus_trader.model.identifiers").Venue,
            Money=importlib.import_module("nautilus_trader.model.objects").Money,
            AccountType=model_enums.AccountType,
            OmsType=model_enums.OmsType,
            TestInstrumentProvider=importlib.import_module(
                "nautilus_trader.test_kit.providers"
            ).TestInstrumentProvider,
            BarDataWrangler=importlib.import_module(
                "nautilus_trader.persistence.wranglers"
            ).BarDataWrangler,
            BarType=model_data.BarType,
            BarSpecification=model_data.BarSpecification,
            AggregationSource=_module_attr(
                primary=model_enums,
                fallback=model_data,
                name="AggregationSource",
            ),
            BarAggregation=_module_attr(
                primary=model_enums,
                fallback=model_data,
                name="BarAggregation",
            ),
            PriceType=model_enums.PriceType,
            OrderSide=model_enums.OrderSide,
            Quantity=model_objects.Quantity,
            Strategy=importlib.import_module("nautilus_trader.trading.strategy").Strategy,
            JPY=getattr(model_objects, "JPY", None),
            Currency=getattr(model_objects, "Currency", None),
        )
    except ModuleNotFoundError as exc:
        raise MissingNautilusDependencyError(
            "nautilus_trader is not installed. Install the optional `nautilus` "
            "dependency group before running verification backtests."
        ) from exc


def _normalize_bool_frame(frame: pd.DataFrame) -> pd.DataFrame:
    return frame.fillna(False).infer_objects(copy=False).astype(bool)


def _to_float(value: Any) -> float:
    return float(cast(Any, value))


def _to_timestamp(value: Any) -> pd.Timestamp:
    return pd.Timestamp(cast(Any, value))


def _module_attr(*, primary: Any, fallback: Any, name: str) -> Any:
    value = getattr(primary, name, None)
    if value is not None:
        return value
    return getattr(fallback, name)


def _coerce_signal_params(payload: Any) -> SignalParams | None:
    if payload is None:
        return None
    if isinstance(payload, SignalParams):
        return payload
    if isinstance(payload, dict):
        return SignalParams(**payload)
    return payload


def _sanitize_json_payload(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _sanitize_json_payload(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_sanitize_json_payload(item) for item in value]
    if isinstance(value, tuple):
        return [_sanitize_json_payload(item) for item in value]
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            _sanitize_json_payload(payload),
            ensure_ascii=False,
            indent=2,
            allow_nan=False,
        ),
        encoding="utf-8",
    )
    return path


def _max_affordable_quantity(
    *,
    budget: float,
    open_price: float,
    effective_slippage: float,
    effective_fees: float,
) -> int:
    if budget <= 0.0 or open_price <= 0.0:
        return 0

    entry_price = open_price * (1.0 + effective_slippage)
    entry_unit_cost = entry_price * (1.0 + effective_fees)
    if entry_unit_cost <= 0.0:
        return 0

    return int(math.floor(budget / entry_unit_cost))


def _metrics_artifact_paths(report_paths: Any) -> tuple[Path, Path, Path]:
    metrics_path = Path(report_paths.metrics_path)
    engine_path = metrics_path.with_name(metrics_path.name.replace(".metrics.json", ".engine.json"))
    diagnostics_path = metrics_path.with_name(
        metrics_path.name.replace(".metrics.json", ".diagnostics.json")
    )
    return metrics_path, engine_path, diagnostics_path


def _daily_event_timestamp(value: pd.Timestamp, event_time: time) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize(_JST)
    else:
        ts = ts.tz_convert(_JST)
    return ts.normalize() + pd.Timedelta(
        hours=event_time.hour,
        minutes=event_time.minute,
        seconds=event_time.second,
    )


def _resolve_base_currency(runtime: _RuntimeModule) -> Any:
    if runtime.JPY is not None:
        return runtime.JPY
    if runtime.Currency is not None:
        from_str = getattr(runtime.Currency, "from_str", None)
        if callable(from_str):
            return from_str("JPY")
    return "JPY"


def _build_instrument(runtime: _RuntimeModule, code: str, venue: Any) -> Any:
    provider = runtime.TestInstrumentProvider
    attempts: tuple[tuple[Any, ...], ...] = (
        tuple(),
        (code,),
    )
    kwargs_attempts = (
        {"symbol": code, "venue": venue},
        {"symbol": code, "venue": "SIM"},
    )
    for kwargs in kwargs_attempts:
        try:
            return provider.equity(**kwargs)
        except Exception:
            continue
    for args in attempts:
        try:
            return provider.equity(*args)
        except Exception:
            continue
    raise RuntimeError(f"Unable to construct Nautilus equity instrument for code={code}")


def _build_bar_type(runtime: _RuntimeModule, instrument: Any) -> Any:
    instrument_id = getattr(instrument, "id", instrument)
    try:
        bar_spec = runtime.BarSpecification(
            step=1,
            aggregation=runtime.BarAggregation.MINUTE,
            price_type=runtime.PriceType.LAST,
        )
        return runtime.BarType(instrument_id, bar_spec, runtime.AggregationSource.EXTERNAL)
    except Exception:
        from_str = getattr(runtime.BarType, "from_str", None)
        if callable(from_str):
            return from_str(f"{instrument_id}-1-MINUTE-LAST-EXTERNAL")
        raise


def _build_quantity(runtime: _RuntimeModule, quantity: int) -> Any:
    from_int = getattr(runtime.Quantity, "from_int", None)
    if callable(from_int):
        return from_int(quantity)
    from_str = getattr(runtime.Quantity, "from_str", None)
    if callable(from_str):
        return from_str(str(quantity))
    return runtime.Quantity(quantity)


def _prepare_portfolio_inputs(
    strategy_name: str,
    parameters: dict[str, Any],
) -> _PreparedPortfolioInputs:
    shared_config = SharedConfig(**parameters.get("shared_config", {}))
    entry_filter_params = _coerce_signal_params(parameters.get("entry_filter_params"))
    exit_trigger_params = _coerce_signal_params(parameters.get("exit_trigger_params"))
    capture_adapter = _CapturedExecutionAdapter()
    strategy = YamlConfigurableStrategy(
        shared_config=shared_config,
        entry_filter_params=entry_filter_params,
        exit_trigger_params=exit_trigger_params,
        execution_adapter=cast(Any, capture_adapter),
    )
    strategy.run_multi_backtest()

    execution_mode = resolve_round_trip_execution_mode_name(strategy.compiled_strategy)
    if execution_mode not in _SUPPORTED_EXECUTION_MODES:
        raise ValueError(
            "Nautilus verification currently supports only "
            f"{sorted(_SUPPORTED_EXECUTION_MODES)}. got={execution_mode!r}"
        )
    if shared_config.timeframe != "daily":
        raise ValueError(
            "Nautilus verification currently supports only daily timeframe. "
            f"got={shared_config.timeframe!r}"
        )
    if getattr(shared_config, "direction", "longonly") != "longonly":
        raise ValueError(
            "Nautilus verification currently supports only longonly direction. "
            f"got={shared_config.direction!r}"
        )
    if capture_adapter.round_trip_inputs is None:
        if capture_adapter.signal_portfolio_requested:
            raise ValueError(
                "Nautilus verification requires round-trip execution semantics and "
                "does not support signal-portfolio mode yet."
            )
        raise RuntimeError("Failed to capture round-trip execution inputs for Nautilus.")
    if capture_adapter.cash_sharing is not True or capture_adapter.group_by is not True:
        raise ValueError(
            "Nautilus verification currently requires grouped cash-sharing round-trip "
            "execution."
        )
    if capture_adapter.entry_size is None:
        raise RuntimeError("Failed to capture round-trip position sizing for Nautilus.")
    if (
        capture_adapter.max_size is not None
        and capture_adapter.max_size + 1e-12 < capture_adapter.entry_size
    ):
        raise ValueError(
            "Nautilus verification currently does not support capped round-trip position "
            "sizing (`max_exposure < entry_size`)."
        )

    open_data, close_data, entries_data = capture_adapter.round_trip_inputs
    dataset_name = str(shared_config.dataset or "unknown")
    return _PreparedPortfolioInputs(
        strategy_name=strategy_name,
        dataset_name=dataset_name,
        shared_config=shared_config,
        compiled_strategy=strategy.compiled_strategy,
        open_data=open_data.astype(float),
        close_data=close_data.astype(float),
        entries_data=_normalize_bool_frame(entries_data),
        execution_mode=execution_mode,
        effective_fees=float(capture_adapter.effective_fees or 0.0),
        effective_slippage=float(capture_adapter.effective_slippage or 0.0),
        allocation_per_asset=float(capture_adapter.entry_size),
    )


def _build_verification_plan(prepared: _PreparedPortfolioInputs) -> _VerificationPlan:
    current_equity = float(prepared.shared_config.initial_cash)
    trade_records: list[dict[str, Any]] = []
    trades_by_code: dict[str, list[_TradePlan]] = defaultdict(list)
    equity_points: list[dict[str, Any]] = []
    skipped_missing_prices: list[dict[str, Any]] = []
    skipped_zero_quantity: list[dict[str, Any]] = []

    for date_value in prepared.entries_data.index:
        trade_date = _to_timestamp(date_value)
        starting_equity = current_equity
        available_cash = starting_equity
        day_pnl = 0.0
        for code in prepared.entries_data.columns:
            if not bool(prepared.entries_data.at[date_value, code]):
                continue

            open_price = prepared.open_data.at[date_value, code]
            close_price = prepared.close_data.at[date_value, code]
            if pd.isna(open_price) or pd.isna(close_price):
                skipped_missing_prices.append(
                    {"code": code, "date": str(trade_date.date())}
                )
                continue

            budget = available_cash * prepared.allocation_per_asset
            open_price_value = _to_float(open_price)
            close_price_value = _to_float(close_price)
            quantity = _max_affordable_quantity(
                budget=float(budget),
                open_price=open_price_value,
                effective_slippage=prepared.effective_slippage,
                effective_fees=prepared.effective_fees,
            )
            if quantity <= 0:
                skipped_zero_quantity.append(
                    {
                        "code": code,
                        "date": str(trade_date.date()),
                        "budget": round(float(budget), 6),
                        "open_price": open_price_value,
                        "effective_slippage": prepared.effective_slippage,
                        "effective_fees": prepared.effective_fees,
                    }
                )
                continue

            entry_price = open_price_value * (1.0 + prepared.effective_slippage)
            exit_price = close_price_value * max(0.0, 1.0 - prepared.effective_slippage)
            notional_in = entry_price * quantity
            notional_out = exit_price * quantity
            available_cash = max(
                available_cash - (notional_in * (1.0 + prepared.effective_fees)),
                0.0,
            )
            fees_cost = (notional_in + notional_out) * prepared.effective_fees
            pnl = notional_out - notional_in - fees_cost
            gross_return = ((close_price_value / open_price_value) - 1.0) * 100.0
            net_return = (pnl / max(notional_in, 1e-12)) * 100.0
            open_ts = _daily_event_timestamp(trade_date, _OPEN_EVENT_TIME)
            close_ts = _daily_event_timestamp(trade_date, _CLOSE_EVENT_TIME)
            trade_plan = _TradePlan(
                code=code,
                trade_date=trade_date,
                open_ts=open_ts,
                close_ts=close_ts,
                quantity=quantity,
                open_price=open_price_value,
                close_price=close_price_value,
                pnl=pnl,
                gross_return=gross_return,
                net_return=net_return,
            )
            trades_by_code[code].append(trade_plan)
            trade_records.append(
                {
                    "code": code,
                    "trade_date": str(trade_date.date()),
                    "quantity": quantity,
                    "open_price": open_price_value,
                    "close_price": close_price_value,
                    "gross_return": gross_return,
                    "net_return": net_return,
                    "pnl": pnl,
                }
            )
            day_pnl += pnl

        current_equity += day_pnl
        day_return = 0.0 if starting_equity <= 0 else day_pnl / starting_equity
        equity_points.append(
            {
                "date": trade_date,
                "equity": current_equity,
                "return": day_return,
            }
        )

    if not equity_points:
        for date_value in prepared.entries_data.index:
            equity_points.append(
                {"date": _to_timestamp(date_value), "equity": current_equity, "return": 0.0}
            )

    returns = pd.Series(
        [float(point["return"]) for point in equity_points],
        index=pd.DatetimeIndex([point["date"] for point in equity_points]),
        dtype=float,
    )
    equity_curve = pd.Series(
        [float(point["equity"]) for point in equity_points],
        index=returns.index,
        dtype=float,
    )
    cumulative_max = equity_curve.cummax()
    drawdowns = ((equity_curve / cumulative_max) - 1.0) * 100.0
    trade_count = len(trade_records)
    winners = sum(1 for trade in trade_records if float(trade["pnl"]) > 0.0)
    total_return_pct = (
        ((current_equity / float(prepared.shared_config.initial_cash)) - 1.0) * 100.0
        if prepared.shared_config.initial_cash
        else 0.0
    )
    sharpe_ratio = _annualized_sharpe_ratio(returns)
    sortino_ratio = _annualized_sortino_ratio(returns)
    max_drawdown = float(drawdowns.min()) if not drawdowns.empty else 0.0
    calmar_ratio = (
        total_return_pct / abs(max_drawdown)
        if max_drawdown < 0.0
        else (0.0 if total_return_pct == 0.0 else None)
    )
    win_rate = (winners / trade_count) * 100.0 if trade_count else 0.0
    profit_factor = _profit_factor(trade_records)
    summary_metrics = CanonicalExecutionMetrics(
        total_return=total_return_pct,
        sharpe_ratio=sharpe_ratio,
        sortino_ratio=sortino_ratio,
        calmar_ratio=calmar_ratio,
        max_drawdown=max_drawdown,
        win_rate=win_rate,
        trade_count=trade_count,
    )
    metrics_payload = {
        "total_return": total_return_pct,
        "max_drawdown": max_drawdown,
        "sharpe_ratio": sharpe_ratio,
        "sortino_ratio": sortino_ratio,
        "calmar_ratio": calmar_ratio,
        "win_rate": win_rate,
        "trade_count": trade_count,
        "total_trades": trade_count,
        "profit_factor": profit_factor,
        "generated_at": pd.Timestamp.utcnow().isoformat(),
    }
    diagnostics = {
        "engine": EngineFamily.NAUTILUS.value,
        "executionMode": prepared.execution_mode,
        "tradePlanCount": trade_count,
        "symbols": sorted(prepared.entries_data.columns.tolist()),
        "skippedMissingPricesCount": len(skipped_missing_prices),
        "skippedZeroQuantityCount": len(skipped_zero_quantity),
        "skippedMissingPrices": skipped_missing_prices[:25],
        "skippedZeroQuantity": skipped_zero_quantity[:25],
        "tradeSamples": trade_records[:25],
    }
    return _VerificationPlan(
        trades_by_code=dict(trades_by_code),
        trade_records=trade_records,
        metrics_payload=metrics_payload,
        summary_metrics=summary_metrics,
        diagnostics=diagnostics,
    )


def _annualized_sharpe_ratio(returns: pd.Series) -> float | None:
    non_null = returns.dropna()
    if non_null.empty:
        return 0.0
    std = float(non_null.std(ddof=0))
    if std <= 0.0:
        return 0.0
    return float((non_null.mean() / std) * math.sqrt(252.0))


def _annualized_sortino_ratio(returns: pd.Series) -> float | None:
    non_null = returns.dropna()
    if non_null.empty:
        return None
    downside = non_null[non_null < 0.0]
    downside_std = float(downside.std(ddof=0)) if not downside.empty else 0.0
    if downside_std <= 0.0:
        return None
    return float((non_null.mean() / downside_std) * math.sqrt(252.0))


def _profit_factor(trade_records: list[dict[str, Any]]) -> float | None:
    gross_profit = sum(float(record["pnl"]) for record in trade_records if float(record["pnl"]) > 0.0)
    gross_loss = abs(
        sum(float(record["pnl"]) for record in trade_records if float(record["pnl"]) < 0.0)
    )
    if gross_profit <= 0.0 and gross_loss <= 0.0:
        return None
    if gross_loss <= 0.0:
        return None
    return gross_profit / gross_loss


def _build_bars_dataframe(
    open_data: pd.Series,
    close_data: pd.Series,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for date_value, open_price in open_data.items():
        close_price = close_data.get(date_value)
        if pd.isna(open_price) or pd.isna(close_price):
            continue
        open_price_value = _to_float(open_price)
        close_price_value = _to_float(close_price)
        open_ts = _daily_event_timestamp(_to_timestamp(date_value), _OPEN_EVENT_TIME).tz_convert(
            "UTC"
        )
        close_ts = _daily_event_timestamp(
            _to_timestamp(date_value),
            _CLOSE_EVENT_TIME,
        ).tz_convert("UTC")
        rows.append(
            {
                "timestamp": open_ts,
                "open": open_price_value,
                "high": open_price_value,
                "low": open_price_value,
                "close": open_price_value,
                "volume": 0,
            }
        )
        rows.append(
            {
                "timestamp": close_ts,
                "open": close_price_value,
                "high": close_price_value,
                "low": close_price_value,
                "close": close_price_value,
                "volume": 0,
            }
        )
    if not rows:
        raise ValueError("No executable bars were produced for Nautilus verification.")
    frame = pd.DataFrame(rows).set_index("timestamp").sort_index()
    frame.index.name = "timestamp"
    return frame


def _build_strategy(
    runtime: _RuntimeModule,
    *,
    instrument_id: Any,
    bar_type: Any,
    trade_plans: list[_TradePlan],
) -> Any:
    schedule: dict[int, list[tuple[str, int]]] = defaultdict(list)
    for trade_plan in trade_plans:
        schedule[int(trade_plan.open_ts.tz_convert("UTC").value)].append(
            ("buy", trade_plan.quantity)
        )
        schedule[int(trade_plan.close_ts.tz_convert("UTC").value)].append(
            ("sell", trade_plan.quantity)
        )

    class VerificationStrategy(runtime.Strategy):  # type: ignore[misc, valid-type]
        def __init__(self) -> None:
            super().__init__()

        def on_start(self) -> None:
            self.subscribe_bars(bar_type)

        def on_bar(self, bar: Any) -> None:
            ts_event = int(getattr(bar, "ts_event", getattr(bar, "ts_init", 0)))
            actions = schedule.get(ts_event, [])
            if not actions:
                return
            for action, quantity in actions:
                if quantity <= 0:
                    continue
                order = self.order_factory.market(
                    instrument_id=instrument_id,
                    order_side=(
                        runtime.OrderSide.BUY
                        if action == "buy"
                        else runtime.OrderSide.SELL
                    ),
                    quantity=_build_quantity(runtime, quantity),
                )
                self.submit_order(order)

    return VerificationStrategy()


def _run_nautilus_engine(
    runtime: _RuntimeModule,
    *,
    prepared: _PreparedPortfolioInputs,
    verification_plan: _VerificationPlan,
) -> dict[str, Any]:
    engine = runtime.BacktestEngine(
        config=runtime.BacktestEngineConfig(
            logging=runtime.LoggingConfig(log_level="ERROR"),
        )
    )
    venue = runtime.Venue("SIM")
    base_currency = _resolve_base_currency(runtime)
    engine.add_venue(
        venue=venue,
        oms_type=runtime.OmsType.NETTING,
        account_type=runtime.AccountType.CASH,
        base_currency=base_currency,
        starting_balances=[
            runtime.Money(float(prepared.shared_config.initial_cash), base_currency)
        ],
    )

    strategy_count = 0
    total_bars = 0
    try:
        for code in prepared.entries_data.columns:
            trade_plans = verification_plan.trades_by_code.get(code, [])
            instrument = _build_instrument(runtime, code, venue)
            bar_type = _build_bar_type(runtime, instrument)
            try:
                bars_df = _build_bars_dataframe(
                    prepared.open_data[code],
                    prepared.close_data[code],
                )
            except ValueError:
                if trade_plans:
                    raise
                continue
            wrangler = runtime.BarDataWrangler(bar_type, instrument)
            bars = wrangler.process(bars_df)
            engine.add_instrument(instrument)
            engine.add_data(bars)
            if trade_plans:
                engine.add_strategy(
                    _build_strategy(
                        runtime,
                        instrument_id=getattr(instrument, "id", instrument),
                        bar_type=bar_type,
                        trade_plans=trade_plans,
                    )
                )
                strategy_count += 1
            total_bars += len(bars_df)
        engine.run()
    finally:
        dispose = getattr(engine, "dispose", None)
        if callable(dispose):
            dispose()
    return {
        "engine": EngineFamily.NAUTILUS.value,
        "engineVersion": _resolve_nautilus_version(),
        "strategyCount": strategy_count,
        "totalSyntheticBars": total_bars,
        "symbols": sorted(prepared.entries_data.columns.tolist()),
        "executionMode": prepared.execution_mode,
    }


def _resolve_nautilus_version() -> str | None:
    try:
        return metadata.version("nautilus_trader")
    except metadata.PackageNotFoundError:
        try:
            return metadata.version("nautilus-trader")
        except metadata.PackageNotFoundError:
            return None


class NautilusVerificationRunner:
    """Run a daily round-trip verification pass through Nautilus."""

    def __init__(self) -> None:
        self._vectorbt_runner = BacktestRunner()
        self._artifact_writer = BacktestArtifactWriter()

    def execute(
        self,
        strategy: str,
        *,
        run_spec: RunSpec,
        run_id: str,
        progress_callback: Callable[[str, float], None] | None = None,
        config_override: dict[str, Any] | None = None,
    ) -> BacktestResult:
        started_at = perf_counter()

        def notify(status: str) -> None:
            if progress_callback is not None:
                progress_callback(status, perf_counter() - started_at)

        strategy_name_only = strategy.split("/")[-1]
        notify("Nautilus verification の入力を準備中...")
        parameters = self._vectorbt_runner.build_parameters_for_strategy(
            strategy,
            config_override=config_override,
        )
        prepared = _prepare_portfolio_inputs(strategy_name_only, parameters)
        verification_plan = _build_verification_plan(prepared)

        strategy_config = self._vectorbt_runner.config_loader.load_strategy_config(strategy)
        executor_output_dir = self._vectorbt_runner.config_loader.get_output_directory(
            strategy_config
        )
        report_paths = MarimoExecutor(str(executor_output_dir)).plan_report_paths(
            parameters,
            strategy_name_only,
            output_filename=f"{strategy_name_only}_{run_id[:8]}_nautilus",
        )
        metrics_path, engine_path, diagnostics_path = _metrics_artifact_paths(report_paths)

        notify("Nautilus verification を実行中...")
        runtime = _load_nautilus_runtime()
        engine_payload = _run_nautilus_engine(
            runtime,
            prepared=prepared,
            verification_plan=verification_plan,
        )
        self._artifact_writer.write_metrics(
            metrics_path=metrics_path,
            metrics_payload=verification_plan.metrics_payload,
        )
        _write_json(engine_path, engine_payload)
        _write_json(diagnostics_path, verification_plan.diagnostics)
        manifest_path = _write_json(
            Path(report_paths.manifest_path),
            {
                "generated_at": pd.Timestamp.utcnow().isoformat(),
                "strategy_name": strategy_name_only,
                "dataset_name": prepared.dataset_name,
                "html_path": None,
                "metrics_path": str(metrics_path),
                "manifest_path": str(report_paths.manifest_path),
                "engine_path": str(engine_path),
                "diagnostics_path": str(diagnostics_path),
                "execution_time": perf_counter() - started_at,
                "simulation_elapsed_time": perf_counter() - started_at,
                "total_elapsed_time": perf_counter() - started_at,
                "parameters": parameters,
                "simulation": {
                    "status": "completed",
                    "engine": EngineFamily.NAUTILUS.value,
                    "metrics_path": str(metrics_path),
                    "engine_path": str(engine_path),
                    "diagnostics_path": str(diagnostics_path),
                },
                "report": {
                    "renderer": None,
                    "status": "not_requested",
                    "html_path": None,
                    "render_time": None,
                    "error": None,
                },
                "run_spec": run_spec.model_dump(mode="json"),
                "engine": engine_payload,
                "diagnostics": {
                    "path": str(diagnostics_path),
                    "summary": {
                        "skippedMissingPricesCount": verification_plan.diagnostics[
                            "skippedMissingPricesCount"
                        ],
                        "skippedZeroQuantityCount": verification_plan.diagnostics[
                            "skippedZeroQuantityCount"
                        ],
                    },
                },
            },
        )

        elapsed_time = perf_counter() - started_at
        logger.info(
            "Nautilus verification completed",
            event="nautilus_verification",
            strategy=strategy_name_only,
            dataset=prepared.dataset_name,
            runId=run_id,
            tradeCount=verification_plan.summary_metrics.trade_count,
            durationMs=round(elapsed_time * 1000, 2),
        )
        return BacktestResult(
            html_path=None,
            metrics_path=metrics_path,
            manifest_path=manifest_path,
            simulation_payload_path=None,
            report_payload_path=None,
            elapsed_time=elapsed_time,
            simulation_elapsed_time=elapsed_time,
            summary={
                "html_path": None,
                "execution_time": elapsed_time,
                "simulation_elapsed_time": elapsed_time,
                "total_elapsed_time": elapsed_time,
                "report_status": "not_requested",
                "engine_family": EngineFamily.NAUTILUS.value,
                "engine_summary": engine_payload,
                "diagnostics_summary": {
                    "skippedMissingPricesCount": verification_plan.diagnostics[
                        "skippedMissingPricesCount"
                    ],
                    "skippedZeroQuantityCount": verification_plan.diagnostics[
                        "skippedZeroQuantityCount"
                    ],
                },
                "_metrics_path": str(metrics_path),
                "_manifest_path": str(manifest_path),
                "_engine_path": str(engine_path),
                "_diagnostics_path": str(diagnostics_path),
            },
            strategy_name=strategy_name_only,
            dataset_name=prepared.dataset_name,
            render_error=None,
        )
