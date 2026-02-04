# VectorBT Portfolio Management Guide

## Overview

The Portfolio module is the heart of VectorBT's backtesting capabilities. It simulates trading activity, tracks positions, calculates performance metrics, and provides comprehensive analysis tools.

## Portfolio Construction Methods

### 1. From Signals

The most common approach - define entry and exit signals:

```python
import vectorbt as vbt
import pandas as pd
import numpy as np

# Load data
data = vbt.YFData.download('AAPL', start='2020-01-01', end='2023-01-01')
close = data.get('Close')

# Generate signals
sma = vbt.indicators.SMA.run(close, 20)
entries = close > sma.sma
exits = close < sma.sma

# Create portfolio
portfolio = vbt.Portfolio.from_signals(
    close=close,
    entries=entries,
    exits=exits,
    init_cash=10000,
    fees=0.001,
    freq='1D'
)
```

### 2. From Orders

More granular control over trade execution:

```python
# Define order sizes (positive = buy, negative = sell, 0 = no action)
order_size = pd.Series(0, index=close.index)
order_size.iloc[50] = 100   # Buy 100 shares
order_size.iloc[100] = -50  # Sell 50 shares
order_size.iloc[150] = -50  # Sell remaining 50 shares

portfolio = vbt.Portfolio.from_orders(
    close=close,
    size=order_size,
    init_cash=10000,
    fees=0.001
)
```

### 3. From Order Function

Dynamic order generation:

```python
def order_func(c, size, price, value_now, value_before, order_value_out):
    """Custom order function"""
    # Only buy if we don't have a position and price is rising
    if size == 0 and price > price.shift(1):
        return 100  # Buy 100 shares
    # Sell if we have position and price is falling
    elif size > 0 and price < price.shift(1):
        return -size  # Sell all shares
    return 0  # No action

portfolio = vbt.Portfolio.from_order_func(
    close=close,
    order_func=order_func,
    init_cash=10000
)
```

### 4. From Holding Value

Directly specify portfolio values:

```python
# Define target portfolio values
target_values = close * 100  # Always hold $100 worth
target_values.iloc[:50] = 0   # No position initially

portfolio = vbt.Portfolio.from_holding_value(
    close=close,
    holding_value=target_values,
    init_cash=10000,
    fees=0.001
)
```

## Position Sizing Strategies

### Fixed Size

```python
# Fixed number of shares
portfolio_fixed = vbt.Portfolio.from_signals(
    close, entries, exits,
    size=100,  # Always buy/sell 100 shares
    size_type='shares'
)

# Fixed dollar amount
portfolio_cash = vbt.Portfolio.from_signals(
    close, entries, exits,
    size=1000,  # Always invest $1000
    size_type='cash'
)
```

### Percentage-Based Sizing

```python
# Percentage of current portfolio value
portfolio_percent = vbt.Portfolio.from_signals(
    close, entries, exits,
    size=0.1,  # Invest 10% of portfolio
    size_type='percent'
)
```

### Dynamic Sizing

```python
# Size based on volatility (inverse volatility sizing)
returns = close.pct_change()
volatility = returns.rolling(20).std()
inv_vol_size = (0.02 / volatility).fillna(0)  # Target 2% volatility

portfolio_vol_sized = vbt.Portfolio.from_signals(
    close, entries, exits,
    size=inv_vol_size,
    size_type='percent'
)
```

### Kelly Criterion Sizing

```python
def kelly_size(returns, lookback=252):
    """Calculate Kelly fraction for position sizing"""
    mean_return = returns.rolling(lookback).mean()
    variance = returns.rolling(lookback).var()
    
    # Kelly fraction = mean / variance
    kelly_f = mean_return / variance
    
    # Clip to reasonable range (0-25% of portfolio)
    return np.clip(kelly_f, 0, 0.25)

returns = close.pct_change()
kelly_sizes = kelly_size(returns)

portfolio_kelly = vbt.Portfolio.from_signals(
    close, entries, exits,
    size=kelly_sizes,
    size_type='percent'
)
```

## Risk Management

### Stop Loss and Take Profit

```python
portfolio_stops = vbt.Portfolio.from_signals(
    close=close,
    entries=entries,
    exits=exits,
    sl_stop=0.05,    # 5% stop loss
    tp_stop=0.15,    # 15% take profit
    init_cash=10000,
    fees=0.001
)
```

### Trailing Stop Loss

```python
portfolio_trailing = vbt.Portfolio.from_signals(
    close=close,
    entries=entries,
    exits=exits,
    tsl_stop=0.05,   # 5% trailing stop
    tsl_th=0.02,     # Activate after 2% profit
    init_cash=10000,
    fees=0.001
)
```

### Position Limits

```python
# Limit maximum position size
portfolio_limited = vbt.Portfolio.from_signals(
    close, entries, exits,
    size=1000,
    size_type='cash',
    max_size=5000,    # Maximum $5000 position (size_typeに従う)
)
```

## Multi-Asset Portfolios

### Independent Assets

```python
# Multiple assets, independent portfolios
symbols = ['AAPL', 'GOOGL', 'MSFT']
data_multi = vbt.YFData.download(symbols, start='2020-01-01', end='2023-01-01')
close_multi = data_multi.get('Close')

# Generate signals for all assets
sma_multi = vbt.indicators.SMA.run(close_multi, 20)
entries_multi = close_multi > sma_multi.sma
exits_multi = close_multi < sma_multi.sma

portfolio_multi = vbt.Portfolio.from_signals(
    close=close_multi,
    entries=entries_multi,
    exits=exits_multi,
    init_cash=10000,
    fees=0.001,
    group_by=None  # Independent portfolios
)
```

### Grouped Assets (Shared Cash)

```python
# Group assets by sector
groups = [0, 0, 1]  # AAPL & GOOGL in group 0, MSFT in group 1

portfolio_grouped = vbt.Portfolio.from_signals(
    close=close_multi,
    entries=entries_multi,
    exits=exits_multi,
    init_cash=10000,
    fees=0.001,
    group_by=groups,
    cash_sharing=True
)
```

### Portfolio Allocation

```python
# Allocate different amounts to different assets
allocation = [0.4, 0.4, 0.2]  # 40%, 40%, 20%

portfolio_allocated = vbt.Portfolio.from_signals(
    close=close_multi,
    entries=entries_multi,
    exits=exits_multi,
    init_cash=10000,
    fees=0.001,
    call_seq='auto',  # Optimize order execution
    size=allocation,
    size_type='percent'
)
```

## Performance Analysis

### Basic Statistics

```python
# Get comprehensive statistics
stats = portfolio.stats()
print(stats)

# Key metrics
print(f"Total Return: {portfolio.total_return():.2%}")
print(f"Sharpe Ratio: {portfolio.sharpe_ratio():.2f}")
print(f"Max Drawdown: {portfolio.max_drawdown():.2%}")
print(f"Win Rate: {portfolio.trades.win_rate():.2%}")
```

### Advanced Metrics

```python
# Sortino ratio (downside deviation)
print(f"Sortino Ratio: {portfolio.sortino_ratio():.2f}")

# Calmar ratio (return/max drawdown)
print(f"Calmar Ratio: {portfolio.calmar_ratio():.2f}")

# Information ratio
benchmark_returns = close.pct_change()  # Use market as benchmark
print(f"Information Ratio: {portfolio.information_ratio(benchmark_returns):.2f}")

# Value at Risk (VaR)
returns = portfolio.returns()
var_95 = returns.quantile(0.05)
print(f"VaR (5%): {var_95:.2%}")

# Expected Shortfall (CVaR)
cvar_95 = returns[returns <= var_95].mean()
print(f"CVaR (5%): {cvar_95:.2%}")
```

### Custom Metrics

```python
def custom_metrics(portfolio):
    """Calculate custom performance metrics"""
    returns = portfolio.returns()
    
    metrics = {
        'Skewness': returns.skew(),
        'Kurtosis': returns.kurtosis(),
        'Tail Ratio': returns.quantile(0.95) / abs(returns.quantile(0.05)),
        'Up Capture': returns[returns > 0].mean(),
        'Down Capture': returns[returns < 0].mean(),
        'Profit Factor': portfolio.trades.profit_factor(),
        'Recovery Factor': portfolio.total_return() / abs(portfolio.max_drawdown()),
        'Expectancy': portfolio.trades.expectancy()
    }
    
    return pd.Series(metrics)

custom_stats = custom_metrics(portfolio)
print(custom_stats)
```

## Trade Analysis

### Trade Statistics

```python
# Access individual trades
trades = portfolio.trades

print(f"Total Trades: {trades.count()}")
print(f"Win Rate: {trades.win_rate():.2%}")
print(f"Average PnL: ${trades.pnl.mean():.2f}")
print(f"Best Trade: ${trades.pnl.max():.2f}")
print(f"Worst Trade: ${trades.pnl.min():.2f}")
print(f"Average Duration: {trades.duration.mean():.1f} days")
```

### Trade Distribution Analysis

```python
import matplotlib.pyplot as plt

# Plot trade PnL distribution
plt.figure(figsize=(12, 4))

plt.subplot(1, 2, 1)
trades.pnl.hist(bins=30)
plt.title('Trade PnL Distribution')
plt.xlabel('PnL ($)')
plt.ylabel('Frequency')

plt.subplot(1, 2, 2)
trades.duration.hist(bins=30)
plt.title('Trade Duration Distribution')
plt.xlabel('Duration (days)')
plt.ylabel('Frequency')

plt.tight_layout()
plt.show()
```

### Rolling Performance Analysis

```python
# Rolling Sharpe ratio
rolling_sharpe = portfolio.returns().rolling(252).apply(
    lambda x: x.mean() / x.std() * np.sqrt(252)
)

# Rolling maximum drawdown
portfolio_value = portfolio.value()
rolling_max = portfolio_value.expanding().max()
rolling_dd = (portfolio_value / rolling_max - 1)

# Plot rolling metrics
plt.figure(figsize=(12, 8))

plt.subplot(3, 1, 1)
portfolio_value.plot(title='Portfolio Value')

plt.subplot(3, 1, 2)
rolling_dd.plot(title='Running Drawdown')
plt.fill_between(rolling_dd.index, rolling_dd, alpha=0.3)

plt.subplot(3, 1, 3)
rolling_sharpe.plot(title='Rolling Sharpe Ratio (252 days)')
plt.axhline(y=0, color='k', linestyle='--', alpha=0.5)

plt.tight_layout()
plt.show()
```

## Portfolio Optimization

### Parameter Optimization

```python
def optimize_strategy_parameters(close):
    """Optimize SMA crossover parameters"""
    fast_windows = range(5, 25, 5)
    slow_windows = range(20, 100, 10)
    
    results = []
    
    for fast in fast_windows:
        for slow in slow_windows:
            if fast >= slow:
                continue
                
            # Test parameters
            fast_sma = vbt.indicators.SMA.run(close, fast)
            slow_sma = vbt.indicators.SMA.run(close, slow)
            
            entries = fast_sma.sma > slow_sma.sma
            exits = fast_sma.sma < slow_sma.sma
            
            portfolio = vbt.Portfolio.from_signals(
                close, entries, exits,
                init_cash=10000, fees=0.001
            )
            
            results.append({
                'fast_window': fast,
                'slow_window': slow,
                'total_return': portfolio.total_return(),
                'sharpe_ratio': portfolio.sharpe_ratio(),
                'max_drawdown': portfolio.max_drawdown(),
                'num_trades': portfolio.trades.count()
            })
    
    results_df = pd.DataFrame(results)
    
    # Find best parameters by Sharpe ratio
    best_idx = results_df['sharpe_ratio'].idxmax()
    best_params = results_df.loc[best_idx]
    
    return results_df, best_params

# Run optimization
results_df, best_params = optimize_strategy_parameters(close)
print("Best Parameters:")
print(best_params)
```

### Walk-Forward Analysis

```python
def walk_forward_analysis(close, strategy_func, train_window=252, test_window=63):
    """
    Perform walk-forward analysis
    """
    results = []
    
    for start in range(0, len(close) - train_window - test_window, test_window):
        # Training period
        train_start = start
        train_end = start + train_window
        train_data = close.iloc[train_start:train_end]
        
        # Testing period
        test_start = train_end
        test_end = train_end + test_window
        test_data = close.iloc[test_start:test_end]
        
        # Optimize on training data
        _, best_params = optimize_strategy_parameters(train_data)
        
        # Test on out-of-sample data
        fast_sma = vbt.indicators.SMA.run(test_data, int(best_params['fast_window']))
        slow_sma = vbt.indicators.SMA.run(test_data, int(best_params['slow_window']))
        
        entries = fast_sma.sma > slow_sma.sma
        exits = fast_sma.sma < slow_sma.sma
        
        portfolio = vbt.Portfolio.from_signals(
            test_data, entries, exits,
            init_cash=10000, fees=0.001
        )
        
        results.append({
            'period_start': test_data.index[0],
            'period_end': test_data.index[-1],
            'return': portfolio.total_return(),
            'fast_window': best_params['fast_window'],
            'slow_window': best_params['slow_window']
        })
    
    return pd.DataFrame(results)

# Perform walk-forward analysis
wf_results = walk_forward_analysis(close, None)
print(f"Average out-of-sample return: {wf_results['return'].mean():.2%}")
print(f"Std of returns: {wf_results['return'].std():.2%}")
```

## Risk-Adjusted Portfolio Construction

### Equal Risk Contribution (ERC)

```python
def equal_risk_contribution(returns, window=60):
    """
    Construct Equal Risk Contribution portfolio
    """
    from scipy.optimize import minimize
    
    def risk_budget_objective(weights, cov_matrix):
        """Objective function for ERC optimization"""
        portfolio_vol = np.sqrt(weights.T @ cov_matrix @ weights)
        marginal_contrib = cov_matrix @ weights / portfolio_vol
        contrib = weights * marginal_contrib
        
        # Target equal risk contribution
        target_contrib = np.ones(len(weights)) / len(weights)
        
        return np.sum((contrib / contrib.sum() - target_contrib) ** 2)
    
    n_assets = len(returns.columns)
    equal_weights = np.ones(n_assets) / n_assets
    
    # Rolling optimization
    weights_list = []
    
    for i in range(window, len(returns)):
        # Historical covariance matrix
        hist_returns = returns.iloc[i-window:i]
        cov_matrix = hist_returns.cov().values * 252  # Annualized
        
        # Optimize weights
        constraints = ({'type': 'eq', 'fun': lambda x: x.sum() - 1})
        bounds = tuple((0, 1) for _ in range(n_assets))
        
        result = minimize(
            risk_budget_objective,
            equal_weights,
            args=(cov_matrix,),
            method='SLSQP',
            bounds=bounds,
            constraints=constraints
        )
        
        weights_list.append(result.x)
    
    # Convert to DataFrame
    weights_df = pd.DataFrame(
        weights_list,
        index=returns.index[window:],
        columns=returns.columns
    )
    
    return weights_df

# Example with multiple assets
returns_multi = close_multi.pct_change().dropna()
erc_weights = equal_risk_contribution(returns_multi)

# Calculate portfolio returns
portfolio_returns = (returns_multi * erc_weights.shift(1)).sum(axis=1).dropna()

# Performance metrics
total_return = (1 + portfolio_returns).cumprod().iloc[-1] - 1
sharpe_ratio = portfolio_returns.mean() / portfolio_returns.std() * np.sqrt(252)

print(f"ERC Portfolio Return: {total_return:.2%}")
print(f"ERC Sharpe Ratio: {sharpe_ratio:.2f}")
```

This comprehensive guide covers the major aspects of portfolio management in VectorBT, from basic construction to advanced optimization techniques. The Portfolio module provides the flexibility to implement sophisticated trading strategies while maintaining high performance through vectorized operations.