# VectorBT Key Concepts

## Overview

Understanding VectorBT's core concepts is essential for effective strategy development. This document explains the fundamental principles and design patterns that make VectorBT powerful and efficient.

## 1. Vectorization

### Traditional Approach (Row-by-Row)
```python
# Slow: Processing one data point at a time
def calculate_sma_slow(prices, window):
    sma_values = []
    for i in range(window, len(prices)):
        window_data = prices[i-window:i]
        sma_values.append(sum(window_data) / window)
    return sma_values
```

### Vectorized Approach
```python
# Fast: Processing entire arrays at once
def calculate_sma_fast(prices, window):
    return prices.rolling(window).mean()

# Even faster with VectorBT
sma = vbt.indicators.SMA.run(prices, window)
```

### Key Benefits of Vectorization
- **Speed**: 10-100x faster than iterative approaches
- **Memory Efficiency**: Optimized memory usage patterns
- **NumPy/Pandas Integration**: Leverages highly optimized C code
- **Broadcasting**: Automatic handling of different array shapes

## 2. Broadcasting

VectorBT automatically handles parameter broadcasting across multiple dimensions:

```python
# Single asset, multiple parameters
close = data['Close']  # 1D series
windows = [10, 20, 30]  # Multiple SMA periods

# Automatically creates 2D output (time × parameters)
sma = vbt.indicators.SMA.run(close, windows)
print(sma.ma.shape)  # (n_timesteps, 3)

# Multiple assets, multiple parameters
close_multi = data[['AAPL', 'GOOGL', 'MSFT']]['Close']  # 2D DataFrame
sma_multi = vbt.indicators.SMA.run(close_multi, windows)
print(sma_multi.ma.shape)  # (n_timesteps, 3_assets × 3_windows)
```

## 3. Indicator Architecture

### Indicator Factory Pattern

VectorBT uses a factory pattern to create indicators:

```python
# Built-in indicators are created using IndicatorFactory
SMA = vbt.IndicatorFactory(
    class_name='SMA',
    short_name='sma',
    input_names=['close'],
    param_names=['window'],
    output_names=['sma']
).from_apply_func(
    apply_func=talib.SMA,  # or custom function
    window=20
)

# Usage creates a vectorized indicator
result = SMA.run(close, window=20)
print(result.sma)  # Access the SMA values
```

### Custom Indicator Creation

```python
def custom_momentum(close, window=10):
    """Calculate price momentum"""
    return (close / close.shift(window) - 1) * 100

# Create indicator class
Momentum = vbt.IndicatorFactory(
    class_name='Momentum',
    short_name='mom',
    input_names=['close'],
    param_names=['window'],
    output_names=['momentum']
).from_apply_func(custom_momentum)

# Use like any other indicator
mom = Momentum.run(close, window=10)
entries = mom.momentum > 5  # Buy when momentum > 5%
```

## 4. Signal Generation

### Boolean Arrays for Signals

Signals in VectorBT are represented as boolean arrays:

```python
# Price-based signals
entries = close > close.shift(1)  # Buy on up days
exits = close < close.shift(1)   # Sell on down days

# Indicator-based signals
sma = vbt.indicators.SMA.run(close, 20)
rsi = vbt.indicators.RSI.run(close, 14)

entries = (close > sma.sma) & (rsi.rsi < 70)
exits = (close < sma.sma) | (rsi.rsi > 80)

# Complex multi-condition signals
bb = vbt.indicators.BBANDS.run(close, window=20)
volume_sma = vbt.indicators.SMA.run(volume, 20)

entries = (
    (close > bb.upperband.shift(1)) &  # Price breaks upper band
    (volume > volume_sma.sma * 1.5) &  # High volume
    (rsi.rsi > 60) &                   # Strong momentum
    (~entries.shift(5).rolling(5).any())  # Not recently entered
)
```

### Signal Processing Functions

```python
# Clean signals to avoid conflicts
entries_clean, exits_clean = vbt.signals.clean_enex(entries, exits)

# Generate random signals for testing
random_entries = vbt.signals.generate_random_entries(
    close.shape, 
    n=100,  # 100 entry signals
    seed=42
)

# Generate exit signals after N bars
exits_after_n = vbt.signals.generate_exits_after_n(
    entries, 
    n=10  # Exit after 10 bars
)
```

## 5. Portfolio Construction

### From Signals
```python
portfolio = vbt.Portfolio.from_signals(
    close=close,
    entries=entries,
    exits=exits,
    init_cash=10000,
    fees=0.001,          # 0.1% commission
    freq='1D'            # Daily frequency
)
```

### From Orders
```python
# More granular control with orders
size_signals = np.where(entries, 100, np.where(exits, -100, 0))

portfolio = vbt.Portfolio.from_orders(
    close=close,
    size=size_signals,
    init_cash=10000,
    fees=0.001
)
```

### From Holding Arrays
```python
# Define position sizes directly
holdings = np.zeros_like(close)
holdings[entries] = 1    # Long position
holdings[exits] = 0      # No position
holdings = holdings.fillna(method='ffill')  # Forward fill

portfolio = vbt.Portfolio.from_holding_value(
    close=close,
    holding_value=holdings * 10000  # $10k position size
)
```

## 6. Performance Analysis

### Built-in Statistics
```python
stats = portfolio.stats()
print(stats['Sharpe Ratio'])
print(stats['Max Drawdown [%]'])
print(stats['Total Return [%]'])
```

### Custom Metrics
```python
# Define custom performance metrics
def custom_metrics(returns):
    return {
        'Custom Sharpe': returns.mean() / returns.std() * np.sqrt(252),
        'Skewness': returns.skew(),
        'Kurtosis': returns.kurtosis()
    }

# Apply to portfolio returns
returns = portfolio.returns()
metrics = custom_metrics(returns)
```

## 7. Data Structures

### ArrayWrapper
The foundation of VectorBT's data handling:

```python
# Wraps numpy arrays with pandas-like functionality
wrapper = vbt.ArrayWrapper(
    index=pd.date_range('2020-01-01', periods=100, freq='D'),
    columns=['Asset1', 'Asset2'],
    ndim=2
)

# Use wrapper to create structured arrays
data_array = wrapper.wrap(np.random.randn(100, 2))
```

### Records
Efficient storage for trade records:

```python
# Portfolio orders as records
orders = portfolio.orders
print(orders.size)       # Order sizes
print(orders.price)      # Execution prices
print(orders.fees)       # Commission fees

# Filter records
profitable_orders = orders.filter_by(
    orders.pnl > 0
)
```

## 8. Memory and Performance Optimization

### Chunked Processing
```python
def process_large_dataset(data, chunk_size=10000):
    """Process large datasets in chunks"""
    results = []
    
    for i in range(0, len(data), chunk_size):
        chunk = data.iloc[i:i+chunk_size]
        
        # Process chunk
        sma = vbt.indicators.SMA.run(chunk, 20)
        entries = chunk > sma.sma
        
        portfolio_chunk = vbt.Portfolio.from_signals(
            chunk, entries, ~entries
        )
        
        results.append(portfolio_chunk.total_return())
    
    return pd.Series(results)
```

### Caching
```python
# VectorBT automatically caches expensive calculations
@vbt.utils.decorators.cached_method
def expensive_calculation(self, param):
    # Expensive computation
    return result

# Use caching for custom functions
from functools import lru_cache

@lru_cache(maxsize=128)
def cached_indicator(close_hash, window):
    return vbt.indicators.SMA.run(close, window)
```

## 9. Multi-Asset and Multi-Strategy

### Portfolio Grouping
```python
# Group assets into sectors
symbols = ['AAPL', 'GOOGL', 'MSFT', 'JPM', 'BAC']
groups = [0, 0, 0, 1, 1]  # Tech, Tech, Tech, Finance, Finance

portfolio = vbt.Portfolio.from_signals(
    close, entries, exits,
    group_by=groups,
    cash_sharing=True  # Share cash within groups
)
```

### Strategy Comparison
```python
# Compare multiple strategies
strategies = {
    'SMA_10_30': lambda c: create_sma_strategy(c, 10, 30),
    'RSI_14': lambda c: create_rsi_strategy(c, 14),
    'MACD': lambda c: create_macd_strategy(c)
}

results = {}
for name, strategy_func in strategies.items():
    portfolio = strategy_func(close)
    results[name] = {
        'Return': portfolio.total_return(),
        'Sharpe': portfolio.sharpe_ratio(),
        'MaxDD': portfolio.max_drawdown()
    }

comparison_df = pd.DataFrame(results).T
print(comparison_df.sort_values('Sharpe', ascending=False))
```

## 10. Integration Patterns

### With TA-Lib
```python
# Use TA-Lib functions directly
import talib

# Create VectorBT indicator from TA-Lib
TALIB_RSI = vbt.indicators.talib('RSI')
rsi = TALIB_RSI.run(close, timeperiod=14)
```

### With pandas_ta
```python
# Use pandas_ta indicators
PANDAS_TA_MACD = vbt.indicators.pandas_ta('MACD')
macd = PANDAS_TA_MACD.run(close)
```

### Custom Integration
```python
# Integrate with external libraries
def integrate_external_indicator(close, **kwargs):
    # Use external library
    result = external_lib.calculate(close.values, **kwargs)
    
    # Return as pandas Series
    return pd.Series(result, index=close.index)

# Wrap in VectorBT indicator
ExternalInd = vbt.IndicatorFactory(
    class_name='ExternalIndicator',
    input_names=['close'],
    output_names=['value']
).from_apply_func(integrate_external_indicator)
```

## Best Practices

1. **Think in Arrays**: Always consider entire datasets, not individual points
2. **Use Broadcasting**: Leverage automatic parameter expansion
3. **Cache Expensive Operations**: Store results of complex calculations
4. **Validate Inputs**: Ensure data alignment and consistency
5. **Profile Performance**: Use timing tools to identify bottlenecks
6. **Memory Management**: Consider chunking for large datasets
7. **Test Thoroughly**: Validate strategies with out-of-sample data

These concepts form the foundation of effective VectorBT usage and will help you build robust, high-performance trading strategies.