# Generic Package

## Overview
The generic package provides foundational tools for working with pandas objects, including broadcasting, transformations, and analysis utilities. It's the backbone for most vectorbt operations.

## Key Components

### GenericAccessor (.vbt accessor)
Main accessor class for pandas Series and DataFrames. Provides broadcasting and transformation capabilities.

```python
import pandas as pd
import vectorbt as vbt

# Basic operations
df = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
result = df.vbt.apply(lambda x: x * 2)  # Apply function element-wise
broadcasted = df.vbt.broadcast_to((3, 5))  # Broadcast to shape
```

#### Key Methods:
- `broadcast()`: Broadcast arrays to common shape
- `apply()`: Apply functions with broadcasting support
- `transform()`: Transform data with custom functions
- `stack()`: Stack along new dimension
- `tile()`: Tile arrays multiple times

### Ranges Class
Manages range data for efficient analysis of sequential events.

```python
# Example: Find consecutive True values
mask = pd.Series([True, True, False, True, False])
ranges = mask.vbt.ranges.get_ranges()
print(ranges.duration)  # Duration of each range
```

#### Key Properties:
- `start_idx`: Starting indices of ranges
- `end_idx`: Ending indices of ranges  
- `duration`: Length of each range
- `status`: Range status information

### Stats Methods
Statistical analysis functions accessible via .vbt accessor.

```python
# Common statistical operations
series = pd.Series([1, 2, 3, 4, 5])
print(series.vbt.stats['mean'])     # Mean value
print(series.vbt.stats['std'])      # Standard deviation
print(series.vbt.stats['count'])    # Count of values
```

Available stats:
- Basic: mean, std, min, max, count
- Advanced: skew, kurtosis, quantile
- Custom: User-defined statistical functions

## Common Usage Patterns

### Broadcasting for Strategy Backtesting
```python
# Broadcast parameters for multiple scenarios
close_prices = pd.Series([100, 101, 99, 102])
short_windows = [5, 10, 20]
long_windows = [20, 50, 100]

# Create parameter combinations
short_br, long_br = close_prices.vbt.broadcast(short_windows, long_windows)
```

### Element-wise Transformations
```python
# Apply custom functions with broadcasting
def custom_transform(arr, param1, param2):
    return arr * param1 + param2

result = df.vbt.apply(custom_transform, param1=2, param2=1)
```

### Working with Time Series
```python
# Rolling operations with custom functions
def rolling_custom(arr, window):
    return pd.Series(arr).rolling(window).apply(lambda x: x.std() / x.mean())

ts = pd.Series([1, 2, 3, 4, 5])
result = ts.vbt.transform(rolling_custom, window=3)
```

## Integration Points

### With Indicators
```python
# Broadcasting parameters for indicator calculations
close = pd.Series([100, 101, 99, 102, 105])
windows = [5, 10, 20]
sma = close.vbt.rolling_mean(windows)  # Multiple SMAs at once
```

### With Portfolio Analysis
```python
# Prepare data for portfolio backtesting
returns = pd.Series([0.01, -0.02, 0.015, -0.01])
weights = pd.DataFrame({'stock1': [0.6, 0.5], 'stock2': [0.4, 0.5]})
portfolio_returns = returns.vbt.broadcast_to(weights.shape[0])
```

## Performance Notes
- Use broadcasting instead of loops for parameter sweeps
- Numba-compiled functions (nb methods) are faster for large datasets
- Consider memory usage when broadcasting large parameter spaces
- Cache results of expensive transformations

## Error Handling
```python
try:
    result = df.vbt.apply(risky_function)
except Exception as e:
    # Handle broadcasting or transformation errors
    print(f"Error in generic operation: {e}")
```

The generic package is essential for efficient vectorized operations in trading strategy development, providing the foundation for parameter optimization and bulk analysis.