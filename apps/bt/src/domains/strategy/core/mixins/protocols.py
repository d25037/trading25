"""
Type protocols for mixin classes.

Defines the expected interfaces that mixin classes assume from their host classes.
"""

from typing import TYPE_CHECKING, Any, Literal, Protocol

import pandas as pd
import vectorbt as vbt

if TYPE_CHECKING:
    from src.shared.models.signals import SignalParams, Signals


class StrategyProtocol(Protocol):
    """Protocol defining the interface expected by strategy mixins."""

    # Basic attributes
    stock_codes: list[str]
    stock_code: str
    initial_cash: float
    fees: float
    cash_sharing: bool
    group_by: bool
    printlog: bool

    # Portfolio attributes
    combined_portfolio: vbt.Portfolio | None
    portfolio: vbt.Portfolio | None

    # Data management attributes
    include_margin_data: bool
    include_statements_data: bool
    dataset: str
    start_date: str | None
    end_date: str | None
    timeframe: Literal["daily", "weekly"]

    # Strategy name
    strategy_name: str

    # Signal parameters
    filter_params: "SignalParams | None"  # entry_filter_params compatibility
    entry_filter_params: "SignalParams | None"
    exit_trigger_params: "SignalParams | None"

    # Relative mode attributes
    relative_mode: bool
    benchmark_table: str
    benchmark_data: pd.DataFrame | None
    relative_data_dict: dict[str, dict[str, pd.DataFrame]] | None
    execution_data_dict: dict[str, dict[str, pd.DataFrame]] | None
    multi_data_dict: dict[str, dict[str, pd.DataFrame]] | None
    _grouped_portfolio_inputs_cache: (
        tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame] | None
    )

    # Logger method
    def _log(self, message: str, level: str = "info") -> None:
        """Log a message."""
        ...

    # Strategy methods

    # Data loading methods
    def load_multi_data(self) -> dict[str, dict[str, pd.DataFrame]]:
        """Load multi-stock data."""
        ...

    def load_relative_data(self) -> dict[str, dict[str, pd.DataFrame]]:
        """Load relative mode data."""
        ...

    def load_benchmark_data(self) -> pd.DataFrame:
        """Load benchmark data."""
        ...

    def _should_load_margin_data(self) -> bool:
        """Return whether margin data is required."""
        ...

    def _should_load_statements_data(self) -> bool:
        """Return whether statements data is required."""
        ...

    def generate_multi_signals(
        self,
        stock_code: str,
        stock_data: pd.DataFrame,
        margin_data: pd.DataFrame | None = None,
        statements_data: pd.DataFrame | None = None,
    ) -> "Signals":
        """Generate trading signals for a single stock."""
        ...

    def _create_individual_portfolios(self, **kwargs: Any) -> Any:
        """Create individual portfolios."""
        ...

    # Backtest execution methods
    def run_multi_backtest(
        self,
        optimize: bool | None = None,
        allocation_pct: float | None = None,
        **kwargs: Any,
    ) -> Any:
        """Run multi-stock backtest."""
        ...

    def run_multi_backtest_from_cached_signals(
        self,
        allocation_pct: float,
    ) -> vbt.Portfolio:
        """Run grouped backtest by reusing cached close/entry/exit matrices."""
        ...


class RiskManagementProtocol(Protocol):
    """Protocol for risk management mixin."""

    initial_cash: float

    def _log(self, message: str, level: str = "info") -> None:
        """Log a message."""
        ...
