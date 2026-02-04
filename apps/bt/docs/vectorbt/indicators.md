# VectorBT Indicators Module

## Overview
Technical indicators for financial analysis with vectorized implementations for high performance.

## Core Indicators

### Moving Averages
```python
import vectorbt as vbt

# Simple Moving Average
sma = vbt.MA.run(close_prices, window=20)
sma_values = sma.ma

# Exponential Moving Average  
ema = vbt.MA.run(close_prices, window=20, ewm=True)
```

### RSI (Relative Strength Index)
```python
# RSI calculation
rsi = vbt.RSI.run(close_prices, window=14)
rsi_values = rsi.rsi

# RSI signals
oversold = rsi_values < 30
overbought = rsi_values > 70
```

### MACD (Moving Average Convergence Divergence)
```python
macd = vbt.MACD.run(close_prices, fast_window=12, slow_window=26, signal_window=9)
macd_line = macd.macd
signal_line = macd.signal
histogram = macd.histogram
```

### Bollinger Bands
```python
bb = vbt.BBANDS.run(close_prices, window=20, alpha=2)
upper_band = bb.upper
middle_band = bb.middle  # SMA
lower_band = bb.lower
```

## Advanced Indicators

### Stochastic Oscillator
```python
stoch = vbt.STOCH.run(high_prices, low_prices, close_prices, k_window=14, d_window=3)
k_percent = stoch.percent_k
d_percent = stoch.percent_d
```

### Average True Range (ATR)
```python
atr = vbt.ATR.run(high_prices, low_prices, close_prices, window=14)
atr_values = atr.atr
```

### Parabolic SAR
```python
psar = vbt.PSAR.run(high_prices, low_prices, af_start=0.02, af_inc=0.02, af_max=0.2)
psar_values = psar.psar
```

## Custom Indicators

### Creating Custom Indicators
```python
# Define custom indicator function
def my_indicator(close, window=10):
    return close.rolling(window).std()

# Create indicator factory
MyIndicator = vbt.IndicatorFactory(
    class_name='MyIndicator',
    apply_func=my_indicator,
    param_names=['close', 'window'],
    output_names=['std']
).from_apply_func()

# Use custom indicator
my_ind = MyIndicator.run(close_prices, window=20)
```

## Batch Processing

### Multiple Parameters
```python
# Run indicator with multiple parameters
rsi_batch = vbt.RSI.run(
    close_prices, 
    window=[14, 21, 28]  # Multiple windows
)

# Access results
rsi_14 = rsi_batch.rsi[0]  # First parameter set
rsi_21 = rsi_batch.rsi[1]  # Second parameter set
```

### Multiple Assets
```python
# Multi-asset DataFrame
multi_rsi = vbt.RSI.run(multi_asset_data, window=14)
# Results for each column/asset automatically calculated
```

## Signal Generation

### Crossover Signals
```python
# SMA crossover
fast_sma = vbt.MA.run(close_prices, window=10)
slow_sma = vbt.MA.run(close_prices, window=30)

golden_cross = fast_sma.ma_above(slow_sma)
death_cross = fast_sma.ma_below(slow_sma)
```

### Threshold Signals
```python
# RSI threshold signals
rsi = vbt.RSI.run(close_prices, window=14)

buy_signals = rsi.rsi < 30
sell_signals = rsi.rsi > 70
```

## Performance Optimization

### Caching Results
```python
# Cache expensive calculations
@vbt.cached_method
def expensive_indicator(self, data):
    return complex_calculation(data)
```

### Memory Efficiency
```python
# Use only required outputs
rsi = vbt.RSI.run(close_prices, window=14, cache_outputs=['rsi'])
# Don't cache intermediate calculations
```

## Common Patterns

### Multiple Daily Window Analysis
```python
# Different daily windows for comprehensive analysis
short_rsi = vbt.RSI.run(daily_data, window=7)   # Short-term daily RSI
standard_rsi = vbt.RSI.run(daily_data, window=14) # Standard daily RSI
long_rsi = vbt.RSI.run(daily_data, window=21)   # Longer-term daily RSI
```

### Indicator Combinations
```python
# Multiple indicator strategy
rsi = vbt.RSI.run(close_prices, window=14)
macd = vbt.MACD.run(close_prices)

# Combined signals
buy_signal = (rsi.rsi < 30) & (macd.histogram > 0)
sell_signal = (rsi.rsi > 70) & (macd.histogram < 0)
```

This module provides the foundation for technical analysis in your trading strategies with high-performance vectorized implementations.