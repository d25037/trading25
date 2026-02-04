# VectorBT Portfolio Module

## Overview
The portfolio module provides comprehensive portfolio management, backtesting, and performance analysis capabilities.

## Core Classes

### Portfolio
Main portfolio class for backtesting and analysis.
```python
import vectorbt as vbt

# Create portfolio from signals
portfolio = vbt.Portfolio.from_signals(
    data=close_prices,
    entries=buy_signals,
    exits=sell_signals,
    init_cash=10000,
    fees=0.001
)
```

### Order Management
```python
# Create portfolio from orders
portfolio = vbt.Portfolio.from_orders(
    data=close_prices,
    size=order_sizes,
    price=order_prices,
    fees=0.001
)
```

## Key Features

### Portfolio Statistics
```python
# Performance metrics
portfolio.total_return()
portfolio.sharpe_ratio()
portfolio.max_drawdown()
portfolio.win_rate()

# Detailed stats
portfolio.stats()  # Complete statistics summary
```

### Trade Analysis
```python
# Trade records
trades = portfolio.trades
trades.count()          # Number of trades
trades.pnl.mean()      # Average P&L
trades.duration.mean() # Average trade duration

# Position analysis  
positions = portfolio.positions
positions.count()
positions.pnl.sum()
```

### Cash Flow Management
```python
# Cash and position tracking
portfolio.cash()        # Cash over time
portfolio.shares()      # Share holdings
portfolio.value()       # Total portfolio value
```

## Advanced Features

### Parameter Optimization
```python
# Grid search optimization
results = vbt.Portfolio.from_signals(
    data,
    entries,
    exits,
    init_cash=[10000, 50000, 100000],  # Multiple values
    fees=[0.001, 0.01, 0.1]
).optimize_wrapper()
```

### Risk Management
```python
# Position sizing with Kelly criterion
kelly_size = portfolio.kelly_criterion()

# Stop loss implementation
portfolio_with_stops = vbt.Portfolio.from_signals(
    data,
    entries,
    exits,
    sl_stop=0.05,  # 5% stop loss
    tp_stop=0.10   # 10% take profit
)
```

### Multi-Asset Portfolios
```python
# Multiple assets
multi_portfolio = vbt.Portfolio.from_signals(
    data=multi_asset_data,  # DataFrame with multiple columns
    entries=multi_entries,
    exits=multi_exits,
    group_by=True  # Treat as single portfolio
)
```

## Performance Analysis

### Returns Analysis
```python
# Returns-based metrics
returns = portfolio.returns()
returns.vbt.returns.sharpe_ratio()
returns.vbt.returns.sortino_ratio()
returns.vbt.returns.calmar_ratio()
```

### Drawdown Analysis
```python
# Drawdown periods
drawdowns = portfolio.drawdowns
drawdowns.max_drawdown()
drawdowns.avg_drawdown()
drawdowns.recovery_time.mean()
```

### Benchmark Comparison
```python
# Compare against benchmark
benchmark_portfolio = vbt.Portfolio.from_holding(benchmark_data)
portfolio.compare(benchmark_portfolio)
```

## Common Patterns

### Strategy Backtesting
```python
def backtest_strategy(data, fast_ma, slow_ma):
    fast_sma = vbt.MA.run(data, fast_ma)
    slow_sma = vbt.MA.run(data, slow_ma)
    
    entries = fast_sma.ma_above(slow_sma)
    exits = fast_sma.ma_below(slow_sma)
    
    return vbt.Portfolio.from_signals(data, entries, exits)

# Batch backtesting
results = backtest_strategy(
    data, 
    fast_ma=[10, 20, 30], 
    slow_ma=[50, 100, 200]
)
```

### Walk-Forward Analysis
```python
# Time-based splitting
splitter = vbt.RangeSplitter(
    every=252,  # Annual rebalancing
    length=252 * 2  # 2-year training window
)

wf_results = portfolio.from_signals(
    data, entries, exits, 
    splitter=splitter
)
```

This module is the core of VectorBT's backtesting capabilities, providing institutional-grade portfolio analysis tools.