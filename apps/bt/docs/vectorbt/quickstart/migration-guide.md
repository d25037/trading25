# Migration Guide: From Backtrader to VectorBT

## Overview

This guide helps you migrate your backtesting project from Backtrader to VectorBT, highlighting key differences and providing practical examples.

## Key Differences

### Performance Philosophy

**Backtrader**: Event-driven, processes data one row at a time
**VectorBT**: Vectorized operations, processes entire datasets at once

### Architecture

**Backtrader**:
```python
class MyStrategy(bt.Strategy):
    def __init__(self):
        self.sma = bt.indicators.SMA(self.data.close, period=20)
    
    def next(self):
        if not self.position:
            if self.data.close > self.sma:
                self.buy()
        else:
            if self.data.close < self.sma:
                self.sell()
```

**VectorBT**:
```python
import vectorbt as vbt

# Calculate indicator for entire dataset
sma = vbt.indicators.SMA.run(close, 20)

# Generate signals for entire dataset
entries = close > sma.ma
exits = close < sma.ma

# Run backtest on entire dataset
portfolio = vbt.Portfolio.from_signals(close, entries, exits)
```

## Common Migration Patterns

### 1. Simple Moving Average Strategy

**Backtrader Version**:
```python
import backtrader as bt

class SMAStrategy(bt.Strategy):
    params = (('period', 20),)
    
    def __init__(self):
        self.sma = bt.indicators.SimpleMovingAverage(
            self.datas[0], period=self.params.period)
    
    def next(self):
        if not self.position:
            if self.data.close[0] > self.sma[0]:
                self.buy()
        elif self.data.close[0] < self.sma[0]:
            self.sell()
```

**VectorBT Version**:
```python
import vectorbt as vbt

def sma_strategy(close, period=20):
    sma = vbt.indicators.SMA.run(close, period)
    
    entries = close > sma.ma
    exits = close < sma.ma
    
    return vbt.Portfolio.from_signals(close, entries, exits)

# Usage
portfolio = sma_strategy(data['Close'], period=20)
```

### 2. RSI Strategy

**Backtrader Version**:
```python
class RSIStrategy(bt.Strategy):
    params = (('rsi_period', 14), ('rsi_upper', 70), ('rsi_lower', 30))
    
    def __init__(self):
        self.rsi = bt.indicators.RSI(
            self.datas[0], period=self.params.rsi_period)
    
    def next(self):
        if not self.position:
            if self.rsi[0] < self.params.rsi_lower:
                self.buy()
        elif self.rsi[0] > self.params.rsi_upper:
            self.sell()
```

**VectorBT Version**:
```python
def rsi_strategy(close, rsi_period=14, rsi_upper=70, rsi_lower=30):
    rsi = vbt.indicators.RSI.run(close, rsi_period)
    
    entries = rsi.rsi < rsi_lower
    exits = rsi.rsi > rsi_upper
    
    return vbt.Portfolio.from_signals(close, entries, exits)
```

### 3. Multiple Indicators Strategy

**Backtrader Version**:
```python
class MultiStrategy(bt.Strategy):
    def __init__(self):
        self.sma_fast = bt.indicators.SMA(self.data.close, period=10)
        self.sma_slow = bt.indicators.SMA(self.data.close, period=20)
        self.rsi = bt.indicators.RSI(self.data.close, period=14)
    
    def next(self):
        if not self.position:
            if (self.sma_fast[0] > self.sma_slow[0] and 
                self.rsi[0] < 70):
                self.buy()
        elif (self.sma_fast[0] < self.sma_slow[0] or 
              self.rsi[0] > 80):
            self.sell()
```

**VectorBT Version**:
```python
def multi_strategy(close):
    sma_fast = vbt.indicators.SMA.run(close, 10)
    sma_slow = vbt.indicators.SMA.run(close, 20)
    rsi = vbt.indicators.RSI.run(close, 14)
    
    entries = (sma_fast.ma > sma_slow.ma) & (rsi.rsi < 70)
    exits = (sma_fast.ma < sma_slow.ma) | (rsi.rsi > 80)
    
    return vbt.Portfolio.from_signals(close, entries, exits)
```

## Performance Comparison

### Speed

VectorBT is typically 10-100x faster than Backtrader for equivalent strategies:

```python
import time

# Backtrader approach (slow)
start_time = time.time()
# ... backtrader backtesting code ...
bt_time = time.time() - start_time

# VectorBT approach (fast)
start_time = time.time()
portfolio = vbt.Portfolio.from_signals(close, entries, exits)
vbt_time = time.time() - start_time

print(f"Backtrader time: {bt_time:.2f}s")
print(f"VectorBT time: {vbt_time:.2f}s")
print(f"Speedup: {bt_time/vbt_time:.1f}x")
```

### Memory Usage

VectorBT uses memory more efficiently:

```python
import psutil
import os

# Monitor memory usage
process = psutil.Process(os.getpid())
memory_before = process.memory_info().rss / 1024 / 1024  # MB

# Run backtest
portfolio = vbt.Portfolio.from_signals(close, entries, exits)

memory_after = process.memory_info().rss / 1024 / 1024  # MB
print(f"Memory used: {memory_after - memory_before:.1f} MB")
```

## Key Concepts Mapping

### Data Handling

**Backtrader**:
```python
# Data feeds
data = bt.feeds.YahooFinanceData(dataname='AAPL')
cerebro.adddata(data)
```

**VectorBT**:
```python
# Direct pandas integration
data = vbt.YFData.download('AAPL', start='2020-01-01', end='2023-01-01')
close = data.get('Close')
```

### Position Sizing

**Backtrader**:
```python
self.buy(size=100)  # Buy 100 shares
```

**VectorBT**:
```python
portfolio = vbt.Portfolio.from_signals(
    close, entries, exits,
    size=100  # Fixed size
)

# Or percentage-based
portfolio = vbt.Portfolio.from_signals(
    close, entries, exits,
    size=0.1  # 10% of portfolio
)
```

### Commission and Slippage

**Backtrader**:
```python
cerebro.broker.setcommission(commission=0.001)
```

**VectorBT**:
```python
portfolio = vbt.Portfolio.from_signals(
    close, entries, exits,
    fees=0.001  # 0.1% commission
)
```

### Analysis and Statistics

**Backtrader**:
```python
# Add analyzers
cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')

results = cerebro.run()
print(f"Sharpe: {results[0].analyzers.sharpe.get_analysis()}")
```

**VectorBT**:
```python
# Built-in comprehensive stats
stats = portfolio.stats()
print(f"Sharpe: {stats['Sharpe Ratio']}")
print(f"Max Drawdown: {stats['Max Drawdown [%]']}")
print(f"Total Return: {stats['Total Return [%]']}")
```

## Advanced Features

### Custom Indicators

**Backtrader**:
```python
class MyIndicator(bt.Indicator):
    lines = ('custom',)
    params = (('period', 14),)
    
    def __init__(self):
        # Complex indicator logic
        pass
    
    def next(self):
        # Calculate next value
        pass
```

**VectorBT**:
```python
# Using IndicatorFactory for custom indicators
MyIndicator = vbt.IndicatorFactory(
    class_name='MyIndicator',
    short_name='my_ind',
    input_names=['close'],
    param_names=['period'],
    output_names=['value']
).from_apply_func(
    my_custom_function,
    period=14
)

# Usage
result = MyIndicator.run(close, period=20)
```

### Parameter Optimization

**Backtrader**:
```python
# Optimization requires multiple runs
strats = cerebro.optstrategy(
    MyStrategy,
    period=range(10, 31, 5)
)
```

**VectorBT**:
```python
# Vectorized optimization
periods = np.arange(10, 31, 5)
sma = vbt.indicators.SMA.run(close, periods, broadcast_named_args=dict(period=periods))

entries = close > sma.ma
exits = close < sma.ma

portfolio = vbt.Portfolio.from_signals(close, entries, exits)
best_period = periods[portfolio.total_return().argmax()]
```

## Common Pitfalls and Solutions

### 1. Look-ahead Bias

**Problem**: Using future data in calculations
**Solution**: VectorBT indicators are designed to avoid look-ahead bias

```python
# Safe: Uses only past data
sma = vbt.indicators.SMA.run(close, 20)
entries = close > sma.ma.shift(1)  # Use previous SMA value
```

### 2. Index Alignment

**Problem**: Misaligned time series
**Solution**: Ensure all series have the same index

```python
# Align all series to the same index
close = data['Close']
volume = data['Volume']

# Ensure alignment
assert close.index.equals(volume.index), "Index mismatch!"
```

### 3. Vectorization Understanding

**Problem**: Thinking in loops instead of vectors
**Solution**: Use pandas/numpy vectorized operations

```python
# Instead of loops, use vectorized conditions
entries = (condition1) & (condition2) & (condition3)
exits = (exit_condition1) | (exit_condition2)
```

## Best Practices

1. **Start Simple**: Begin with basic strategies and gradually add complexity
2. **Use Built-in Indicators**: Leverage VectorBT's optimized indicators
3. **Vectorize Everything**: Think in terms of entire arrays, not individual values
4. **Memory Management**: For large datasets, consider chunking or sampling
5. **Validation**: Always validate results against known benchmarks

## Testing Your Migration

```python
def validate_migration(bt_results, vbt_portfolio):
    """Compare Backtrader and VectorBT results"""
    
    # Compare key metrics
    bt_return = bt_results['Total Return']
    vbt_return = vbt_portfolio.total_return()
    
    print(f"Backtrader Return: {bt_return:.4f}")
    print(f"VectorBT Return: {vbt_return:.4f}")
    print(f"Difference: {abs(bt_return - vbt_return):.4f}")
    
    # Compare number of trades
    bt_trades = len(bt_results['Trades'])
    vbt_trades = vbt_portfolio.orders.count()
    
    print(f"Backtrader Trades: {bt_trades}")
    print(f"VectorBT Trades: {vbt_trades}")
```

This migration guide should help you transition from Backtrader to VectorBT effectively while maintaining the integrity of your trading strategies.