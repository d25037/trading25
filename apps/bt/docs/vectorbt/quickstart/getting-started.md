# Getting Started with VectorBT

## Installation

```bash
# Basic installation
pip install vectorbt

# Full installation with all features
pip install vectorbt[full]

# Development installation
pip install vectorbt[dev]
```

## Core Concepts

### 1. Vectorized Operations

VectorBT processes entire datasets at once using vectorized operations:

```python
import vectorbt as vbt
import pandas as pd
import numpy as np

# Load sample data
data = vbt.YFData.download('AAPL', start='2020-01-01', end='2023-01-01')
close = data.get('Close')

# Traditional approach (slow)
sma_values = []
for i in range(20, len(close)):
    sma_values.append(close.iloc[i-20:i].mean())

# VectorBT approach (fast)
sma = vbt.indicators.SMA.run(close, 20)
```

### 2. Indicators

VectorBT provides a comprehensive library of technical indicators:

```python
# Simple Moving Average
sma = vbt.indicators.SMA.run(close, window=20)

# Relative Strength Index
rsi = vbt.indicators.RSI.run(close, window=14)

# Bollinger Bands
bb = vbt.indicators.BBANDS.run(close, window=20, alpha=2)

# MACD
macd = vbt.indicators.MACD.run(close)
```

### 3. Signal Generation

Generate trading signals using logical operations:

```python
# Buy when price crosses above SMA
entries = close > sma.ma

# Sell when RSI is overbought
exits = rsi.rsi > 70

# Combined conditions
entries = (close > sma.ma) & (rsi.rsi < 70) & (close > bb.lower)
exits = (close < sma.ma) | (rsi.rsi > 80) | (close < bb.upper)
```

### 4. Portfolio Construction

Create and analyze portfolios from signals:

```python
# Basic portfolio from signals
portfolio = vbt.Portfolio.from_signals(
    close=close,
    entries=entries,
    exits=exits,
    init_cash=10000,  # Starting capital
    fees=0.001,       # 0.1% commission
)

# Portfolio statistics
stats = portfolio.stats()
print(stats)

# Plot portfolio performance
portfolio.plot().show()
```

## Your First Strategy

Let's create a simple SMA crossover strategy:

```python
import vectorbt as vbt

# Download data
data = vbt.YFData.download('AAPL', start='2020-01-01', end='2023-01-01')
close = data.get('Close')

# Calculate moving averages
fast_sma = vbt.indicators.SMA.run(close, 10)
slow_sma = vbt.indicators.SMA.run(close, 30)

# Generate signals
entries = fast_sma.ma > slow_sma.ma  # Golden cross
exits = fast_sma.ma < slow_sma.ma    # Death cross

# Create portfolio
portfolio = vbt.Portfolio.from_signals(
    close=close,
    entries=entries,
    exits=exits,
    init_cash=10000,
    fees=0.001
)

# Analyze results
print("Portfolio Statistics:")
print(portfolio.stats())

# Plot results
portfolio.plot().show()
```

## Multiple Assets

VectorBT excels at analyzing multiple assets simultaneously:

```python
# Download multiple assets
symbols = ['AAPL', 'GOOGL', 'MSFT', 'TSLA']
data = vbt.YFData.download(symbols, start='2020-01-01', end='2023-01-01')
close = data.get('Close')

# Apply strategy to all assets
sma_fast = vbt.indicators.SMA.run(close, 10)
sma_slow = vbt.indicators.SMA.run(close, 30)

entries = sma_fast.ma > sma_slow.ma
exits = sma_fast.ma < sma_slow.ma

# Portfolio for all assets
portfolio = vbt.Portfolio.from_signals(
    close=close,
    entries=entries,
    exits=exits,
    init_cash=10000,
    fees=0.001,
    group_by=None  # Separate portfolio for each asset
)

# Compare performance
returns = portfolio.total_return()
print("Returns by asset:")
print(returns.sort_values(ascending=False))
```

## Parameter Optimization

Optimize strategy parameters efficiently:

```python
# Test multiple SMA periods
fast_periods = [5, 10, 15, 20]
slow_periods = [20, 30, 40, 50]

# Create parameter combinations
param_combinations = [(f, s) for f in fast_periods for s in slow_periods if f < s]

best_return = -float('inf')
best_params = None

for fast, slow in param_combinations:
    # Calculate SMAs
    sma_fast = vbt.indicators.SMA.run(close, fast)
    sma_slow = vbt.indicators.SMA.run(close, slow)
    
    # Generate signals
    entries = sma_fast.ma > sma_slow.ma
    exits = sma_fast.ma < sma_slow.ma
    
    # Create portfolio
    portfolio = vbt.Portfolio.from_signals(close, entries, exits)
    total_return = portfolio.total_return()
    
    if total_return > best_return:
        best_return = total_return
        best_params = (fast, slow)

print(f"Best parameters: Fast={best_params[0]}, Slow={best_params[1]}")
print(f"Best return: {best_return:.2%}")
```

## Advanced Features

### Custom Indicators

Create custom indicators using the IndicatorFactory:

```python
# Custom momentum indicator
def momentum_func(close, window=10):
    return close / close.shift(window) - 1

Momentum = vbt.IndicatorFactory(
    class_name='Momentum',
    short_name='mom',
    input_names=['close'],
    param_names=['window'],
    output_names=['momentum']
).from_apply_func(momentum_func)

# Use the custom indicator
mom = Momentum.run(close, window=10)
entries = mom.momentum > 0.02  # 2% momentum threshold
```

### Position Sizing

Different position sizing methods:

```python
# Fixed dollar amount
portfolio_fixed = vbt.Portfolio.from_signals(
    close, entries, exits,
    size=1000,  # $1000 per trade
    size_type='cash'
)

# Percentage of portfolio
portfolio_percent = vbt.Portfolio.from_signals(
    close, entries, exits,
    size=0.1,  # 10% of portfolio
    size_type='percent'
)

# Kelly criterion sizing
def kelly_size(returns, window=252):
    mean_return = returns.rolling(window).mean()
    variance = returns.rolling(window).var()
    kelly_f = mean_return / variance
    return np.clip(kelly_f, 0, 1)  # Limit to 0-100%

returns = close.pct_change()
kelly_sizes = kelly_size(returns)

portfolio_kelly = vbt.Portfolio.from_signals(
    close, entries, exits,
    size=kelly_sizes,
    size_type='percent'
)
```

### Risk Management

Implement stop losses and take profits:

```python
# Portfolio with stop loss and take profit
portfolio_risk = vbt.Portfolio.from_signals(
    close=close,
    entries=entries,
    exits=exits,
    sl_stop=0.05,  # 5% stop loss
    tp_stop=0.15,  # 15% take profit
    init_cash=10000,
    fees=0.001
)

# Analyze risk metrics
print(f"Max Drawdown: {portfolio_risk.max_drawdown():.2%}")
print(f"Sharpe Ratio: {portfolio_risk.sharpe_ratio():.2f}")
print(f"Calmar Ratio: {portfolio_risk.calmar_ratio():.2f}")
```

### Walk-Forward Analysis

Implement walk-forward optimization:

```python
def walk_forward_analysis(close, strategy_func, window=252, step=63):
    """
    Perform walk-forward analysis
    """
    results = []
    
    for start in range(0, len(close) - window, step):
        end = start + window
        train_data = close.iloc[start:end]
        test_data = close.iloc[end:end+step]
        
        # Optimize on training data
        best_params = optimize_strategy(train_data, strategy_func)
        
        # Test on out-of-sample data
        portfolio = strategy_func(test_data, **best_params)
        results.append(portfolio.total_return())
    
    return pd.Series(results)

# Use walk-forward analysis
wf_results = walk_forward_analysis(close, sma_strategy)
print(f"Average out-of-sample return: {wf_results.mean():.2%}")
```

## Performance Tips

1. **Use vectorized operations**: Avoid loops when possible
2. **Cache calculations**: Store expensive calculations
3. **Use appropriate data types**: Consider float32 for large datasets
4. **Parallel processing**: Use `n_jobs` parameter where available
5. **Memory management**: Use chunking for very large datasets

```python
# Example of efficient vectorized strategy
def efficient_strategy(close, volume, fast=10, slow=30):
    # Vectorized calculations
    sma_fast = close.rolling(fast).mean()
    sma_slow = close.rolling(slow).mean()
    volume_sma = volume.rolling(20).mean()
    
    # Vectorized conditions
    entries = (
        (sma_fast > sma_slow) &
        (volume > volume_sma * 1.2) &
        (close > close.shift(1))
    )
    
    exits = (
        (sma_fast < sma_slow) |
        (volume < volume_sma * 0.8)
    )
    
    return vbt.Portfolio.from_signals(close, entries, exits)
```

## Next Steps

1. **Explore Examples**: Check out more examples in the `/examples/` directory
2. **Read Module Documentation**: Study specific modules like `indicators`, `portfolio`, `signals`
3. **Join Community**: Connect with other users on GitHub and forums
4. **Contribute**: Help improve VectorBT by contributing code or documentation

## Common Issues and Solutions

### Memory Issues
```python
# For large datasets, use chunking
def process_in_chunks(data, chunk_size=10000):
    results = []
    for i in range(0, len(data), chunk_size):
        chunk = data.iloc[i:i+chunk_size]
        result = process_chunk(chunk)
        results.append(result)
    return pd.concat(results)
```

### Index Alignment
```python
# Ensure all series have the same index
close = data['Close']
volume = data['Volume']

# Check alignment
assert close.index.equals(volume.index)

# Force alignment if needed
close, volume = close.align(volume, join='inner')
```

### Performance Optimization
```python
# Use numba for custom functions
from numba import jit

@jit
def fast_custom_indicator(prices):
    # Your optimized calculation here
    return result

# Apply to pandas series
result = close.apply(fast_custom_indicator)
```

This getting started guide should provide a solid foundation for using VectorBT effectively in your trading strategy development.