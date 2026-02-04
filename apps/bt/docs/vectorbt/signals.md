# VectorBT Signals Module

## Overview
The signals module provides tools for generating, analyzing, and processing trading signals in a vectorized manner.

## Key Classes

### SignalAccessor
Main accessor for pandas objects to work with signals.
```python
import vectorbt as vbt
signals = df.vbt.signals
```

### Generators
Signal generation utilities:
- `vbt.signals.random()` - Random signals
- `vbt.signals.from_choice()` - Signals from choices
- `vbt.signals.from_entry_and_exit()` - Entry/exit based signals

### Factory
Create custom signal indicators:
```python
MySignal = vbt.IndicatorFactory(
    class_name='MySignal',
    entry_func=my_entry_func,
    exit_func=my_exit_func
).from_apply_func()
```

## Common Operations

### Signal Analysis
```python
# Basic signal properties
signals.count()           # Count signals
signals.first()           # First signal per column
signals.last()            # Last signal per column
signals.duration.mean()   # Average signal duration
```

### Signal Filtering
```python
# Filter by conditions
signals.map_reduce_between(entry_idx, exit_idx, reduce_func, *args)
signals.between_two_signals(other_signals)
signals.first_n(n=5)      # First N signals
```

### Signal Generation Patterns
```python
# Entry/Exit pairs
entries = price > sma_fast
exits = price < sma_slow
signals = vbt.signals.from_entry_and_exit(entries, exits)

# Random signals for testing
random_signals = vbt.signals.random(shape=(100, 10), prob=0.1)
```

## Integration with Strategies

### With Portfolio
```python
portfolio = vbt.Portfolio.from_signals(
    close_price, 
    entries=buy_signals, 
    exits=sell_signals
)
```

### With Indicators
```python
# RSI signals
rsi = vbt.RSI.run(close_price)
buy_signals = rsi.rsi < 30
sell_signals = rsi.rsi > 70
```

## Performance Optimization

### Vectorized Operations
- Use `vbt.signals.*` functions instead of loops
- Leverage NumPy broadcasting for multi-column operations
- Cache frequently used signal calculations

### Memory Efficiency
```python
# Efficient signal storage
signals_sparse = signals.vbt.signals.to_sparse()
signals_compressed = signals.vbt.signals.compress()
```

## Common Patterns

### Signal Confirmation
```python
# Combine multiple signals
confirmed_signals = signal1 & signal2
delayed_signals = signal1.shift(1) & signal2
```

### Signal Timing
```python
# Add delays and filters
delayed_entry = entries.shift(1)
filtered_exit = exits & (price < stop_loss)
```

This module is essential for creating robust trading strategies with proper signal generation and validation.