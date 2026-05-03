"""Engine-neutral portfolio surface with the current VectorBT adapter."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Literal, Protocol, cast, runtime_checkable

import numpy as np
import pandas as pd
import vectorbt as vbt
from numba import njit
from vectorbt.portfolio import nb as portfolio_nb
from vectorbt.portfolio.enums import Direction, SizeType

from src.domains.backtest.contracts import CanonicalExecutionMetrics

VectorbtEngine = Literal["auto", "numba", "rust"]
VECTORBT_ENGINE_ENV = "BT_VECTORBT_ENGINE"
_VALID_VECTORBT_ENGINES: set[str] = {"auto", "numba", "rust"}

ROUND_TRIP_DIRECTION_MAP = {
    "longonly": int(Direction.LongOnly),
    "shortonly": int(Direction.ShortOnly),
    "both": int(Direction.Both),
}
PERCENT_SIZE_TYPE = int(SizeType.Percent)


def resolve_vectorbt_engine(engine: str | None = None) -> VectorbtEngine:
    """Resolve the VectorBT portfolio dispatch engine."""

    raw_engine = engine if engine is not None else os.getenv(VECTORBT_ENGINE_ENV, "auto")
    resolved = raw_engine.strip().lower()
    if resolved not in _VALID_VECTORBT_ENGINES:
        raise ValueError(
            f"Invalid VectorBT engine {raw_engine!r}. "
            "Expected one of: auto, numba, rust."
        )
    return cast(VectorbtEngine, resolved)


def _is_mock_value(value: Any) -> bool:
    module_name = type(value).__module__
    return module_name.startswith("unittest.mock")


def _coerce_metric(value: Any) -> float | None:
    if _is_mock_value(value):
        return None
    try:
        if hasattr(value, "mean"):
            value = value.mean()
        if _is_mock_value(value):
            return None
        coerced = float(value)
    except Exception:
        return None
    return coerced if np.isfinite(coerced) else None


def _read_metric(obj: Any, method_name: str) -> float | None:
    try:
        method = getattr(obj, method_name)
    except Exception:
        return None

    if not callable(method):
        return _coerce_metric(method)

    try:
        return _coerce_metric(method())
    except Exception:
        return None


def _coerce_trade_count(trades: "ExecutionTradeLedgerProtocol") -> int | None:
    try:
        count = trades.count()
        if hasattr(count, "sum"):
            count = count.sum()
        count_value = _coerce_metric(count)
        if count_value is not None:
            return int(count_value)
    except Exception:
        pass

    try:
        records = trades.records_readable
        return len(records)
    except Exception:
        return None


@runtime_checkable
class ExecutionTradeLedgerProtocol(Protocol):
    """Engine-neutral trade ledger surface used by analytics code."""

    @property
    def records_readable(self) -> Any:
        ...

    def count(self) -> Any:
        ...

    def win_rate(self) -> Any:
        ...


@runtime_checkable
class ExecutionPortfolioProtocol(Protocol):
    """Engine-neutral execution portfolio surface used across bt runtime."""

    @property
    def trades(self) -> ExecutionTradeLedgerProtocol:
        ...

    def total_return(self) -> Any:
        ...

    def sharpe_ratio(self) -> Any:
        ...

    def sortino_ratio(self) -> Any:
        ...

    def calmar_ratio(self) -> Any:
        ...

    def max_drawdown(self) -> Any:
        ...

    def drawdown(self) -> Any:
        ...

    def returns(self) -> Any:
        ...

    def unwrap(self) -> Any:
        ...


@runtime_checkable
class ExecutionAdapterProtocol(Protocol):
    """Concrete engine adapter surface used by strategy runtime."""

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
    ) -> ExecutionPortfolioProtocol:
        ...

    def create_round_trip_portfolio(
        self,
        *,
        open_data: pd.DataFrame,
        close_data: pd.DataFrame,
        entries_data: pd.DataFrame,
        execution_mode: str,
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
    ) -> ExecutionPortfolioProtocol:
        ...

    def build_summary_metrics(
        self,
        portfolio: Any,
    ) -> CanonicalExecutionMetrics | None:
        ...


@njit
def _round_trip_order_func_nb(
    c,
    entry_mask: np.ndarray,
    open_prices: np.ndarray,
    close_prices: np.ndarray,
    entry_size: float,
    entry_size_type: int,
    entry_direction: int,
    fees: float,
    slippage: float,
    max_size: float,
):
    group_len = c.to_col - c.from_col

    if c.call_idx < group_len:
        col = c.from_col + c.call_idx
        if not entry_mask[c.i, col]:
            return col, portfolio_nb.order_nothing_nb()
        return col, portfolio_nb.order_nb(
            size=entry_size,
            price=float(open_prices[c.i, col]),
            size_type=entry_size_type,
            direction=entry_direction,
            fees=fees,
            slippage=slippage,
            max_size=max_size,
        )

    if c.call_idx < group_len * 2:
        col = c.from_col + (c.call_idx - group_len)
        if not entry_mask[c.i, col]:
            return col, portfolio_nb.order_nothing_nb()
        position_now = c.last_position[col]
        if position_now == 0:
            return col, portfolio_nb.order_nothing_nb()
        exit_size = -position_now
        exit_direction = entry_direction
        if position_now < 0:
            exit_size = abs(position_now)
            exit_direction = Direction.Both
        return col, portfolio_nb.order_nb(
            size=exit_size,
            price=float(close_prices[c.i, col]),
            size_type=SizeType.Amount,
            direction=exit_direction,
            fees=fees,
            slippage=slippage,
        )

    return -1, portfolio_nb.order_nothing_nb()


@njit
def _overnight_round_trip_order_func_nb(
    c,
    entry_mask: np.ndarray,
    open_prices: np.ndarray,
    close_prices: np.ndarray,
    entry_size: float,
    entry_size_type: int,
    entry_direction: int,
    fees: float,
    slippage: float,
    max_size: float,
):
    group_len = c.to_col - c.from_col

    if c.call_idx < group_len:
        col = c.from_col + c.call_idx
        if c.i == 0 or not entry_mask[c.i - 1, col]:
            return col, portfolio_nb.order_nothing_nb()
        position_now = c.last_position[col]
        if position_now == 0:
            return col, portfolio_nb.order_nothing_nb()
        exit_size = -position_now
        exit_direction = entry_direction
        if position_now < 0:
            exit_size = abs(position_now)
            exit_direction = Direction.Both
        return col, portfolio_nb.order_nb(
            size=exit_size,
            price=float(open_prices[c.i, col]),
            size_type=SizeType.Amount,
            direction=exit_direction,
            fees=fees,
            slippage=slippage,
        )

    if c.call_idx < group_len * 2:
        col = c.from_col + (c.call_idx - group_len)
        if c.i >= len(entry_mask) - 1 or not entry_mask[c.i, col]:
            return col, portfolio_nb.order_nothing_nb()
        return col, portfolio_nb.order_nb(
            size=entry_size,
            price=float(close_prices[c.i, col]),
            size_type=entry_size_type,
            direction=entry_direction,
            fees=fees,
            slippage=slippage,
            max_size=max_size,
        )

    return -1, portfolio_nb.order_nothing_nb()


@dataclass(slots=True)
class VectorbtTradeLedgerAdapter:
    """Compatibility wrapper around `vectorbt` trades."""

    _trades: Any

    @property
    def records_readable(self) -> Any:
        return getattr(self._trades, "records_readable", None)

    def count(self) -> Any:
        return self._trades.count()

    def win_rate(self) -> Any:
        return self._trades.win_rate()

    def unwrap(self) -> Any:
        return self._trades

    def __getattr__(self, name: str) -> Any:
        return getattr(self._trades, name)


@dataclass(slots=True)
class VectorbtPortfolioAdapter:
    """Thin adapter that keeps `vectorbt` behind an engine-neutral surface."""

    _portfolio: vbt.Portfolio

    @property
    def trades(self) -> VectorbtTradeLedgerAdapter:
        return VectorbtTradeLedgerAdapter(self._portfolio.trades)

    def total_return(self) -> Any:
        return self._portfolio.total_return()

    def sharpe_ratio(self) -> Any:
        return self._portfolio.sharpe_ratio()

    def sortino_ratio(self) -> Any:
        return self._portfolio.sortino_ratio()

    def calmar_ratio(self) -> Any:
        return self._portfolio.calmar_ratio()

    def max_drawdown(self) -> Any:
        return self._portfolio.max_drawdown()

    def drawdown(self) -> Any:
        return self._portfolio.drawdown()

    def returns(self) -> Any:
        return self._portfolio.returns()

    def to_canonical_metrics(self) -> CanonicalExecutionMetrics:
        return CanonicalExecutionMetrics(
            total_return=_read_metric(self, "total_return"),
            sharpe_ratio=_read_metric(self, "sharpe_ratio"),
            sortino_ratio=_read_metric(self, "sortino_ratio"),
            calmar_ratio=_read_metric(self, "calmar_ratio"),
            max_drawdown=_read_metric(self, "max_drawdown"),
            win_rate=_read_metric(self.trades, "win_rate"),
            trade_count=_coerce_trade_count(self.trades),
        )

    def unwrap(self) -> vbt.Portfolio:
        return self._portfolio

    def __getattr__(self, name: str) -> Any:
        return getattr(self._portfolio, name)


class VectorbtAdapter:
    """Execute and normalize portfolios through vectorbt."""

    def __init__(self, engine: str | None = None) -> None:
        self.engine = resolve_vectorbt_engine(engine)

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
    ) -> ExecutionPortfolioProtocol:
        portfolio_kwargs: dict[str, Any] = {
            "close": close,
            "entries": entries,
            "exits": exits,
            "direction": direction,
            "init_cash": init_cash,
            "fees": fees,
            "slippage": slippage,
            "cash_sharing": cash_sharing,
            "group_by": group_by,
            "accumulate": accumulate,
            "freq": freq,
            "engine": self.engine,
        }
        if size is not None:
            portfolio_kwargs["size"] = size
        if size_type is not None:
            portfolio_kwargs["size_type"] = size_type
        if call_seq is not None:
            portfolio_kwargs["call_seq"] = call_seq
        if max_size is not None:
            portfolio_kwargs["max_size"] = max_size

        return ensure_execution_portfolio(vbt.Portfolio.from_signals(**portfolio_kwargs))

    def create_round_trip_portfolio(
        self,
        *,
        open_data: pd.DataFrame,
        close_data: pd.DataFrame,
        entries_data: pd.DataFrame,
        execution_mode: str,
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
    ) -> ExecutionPortfolioProtocol:
        normalized_entries = entries_data.fillna(False).infer_objects(copy=False).astype(bool)
        order_func = _round_trip_order_func_nb
        if execution_mode == "overnight_round_trip":
            order_func = _overnight_round_trip_order_func_nb
        max_orders = max(
            1,
            int(normalized_entries.to_numpy(dtype=np.bool_).sum()) * 2
            + normalized_entries.shape[1],
        )

        return ensure_execution_portfolio(
            vbt.Portfolio.from_order_func(
                close_data.astype(float),
                cast(Any, order_func),
                normalized_entries.to_numpy(dtype=np.bool_),
                open_data.astype(float).to_numpy(dtype=np.float64),
                close_data.astype(float).to_numpy(dtype=np.float64),
                float(entry_size),
                int(entry_size_type),
                int(direction),
                float(fees),
                float(slippage),
                float(max_size),
                flexible=True,
                init_cash=init_cash,
                cash_sharing=cash_sharing,
                group_by=group_by,
                freq=freq,
                max_orders=max_orders,
            )
        )

    def build_summary_metrics(
        self,
        portfolio: Any,
    ) -> CanonicalExecutionMetrics | None:
        return canonical_metrics_from_portfolio(portfolio)


def ensure_execution_portfolio(portfolio: Any) -> Any:
    """Wrap raw VectorBT portfolios while leaving test doubles untouched."""

    if isinstance(portfolio, VectorbtPortfolioAdapter):
        return portfolio
    if isinstance(portfolio, vbt.Portfolio):
        return VectorbtPortfolioAdapter(portfolio)
    return portfolio


def canonical_metrics_from_portfolio(
    portfolio: Any,
) -> CanonicalExecutionMetrics | None:
    """Normalize a portfolio object into canonical metrics when possible."""

    if portfolio is None:
        return None

    adapted = ensure_execution_portfolio(portfolio)
    to_canonical_metrics = getattr(adapted, "to_canonical_metrics", None)
    if callable(to_canonical_metrics):
        try:
            metrics = to_canonical_metrics()
        except Exception:
            metrics = None
        if isinstance(metrics, CanonicalExecutionMetrics):
            return metrics

    try:
        trades = adapted.trades
    except Exception:
        trades = None

    metrics = CanonicalExecutionMetrics(
        total_return=_read_metric(adapted, "total_return"),
        sharpe_ratio=_read_metric(adapted, "sharpe_ratio"),
        sortino_ratio=_read_metric(adapted, "sortino_ratio"),
        calmar_ratio=_read_metric(adapted, "calmar_ratio"),
        max_drawdown=_read_metric(adapted, "max_drawdown"),
        win_rate=_read_metric(trades, "win_rate") if trades is not None else None,
        trade_count=_coerce_trade_count(trades) if trades is not None else None,
    )
    if (
        metrics.total_return is None
        and metrics.sharpe_ratio is None
        and metrics.sortino_ratio is None
        and metrics.calmar_ratio is None
        and metrics.max_drawdown is None
        and metrics.win_rate is None
        and metrics.trade_count is None
    ):
        return None
    return metrics
