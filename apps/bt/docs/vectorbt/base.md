# Base Package

## Overview
The base package provides core foundational classes and utilities for pandas objects, including broadcasting, array wrapping, and data manipulation. It forms the infrastructure layer for all vectorbt operations.

## Key Components

### ArrayWrapper
Core class for wrapping NumPy arrays with pandas metadata (index, columns).

```python
import pandas as pd
import vectorbt as vbt
from vectorbt.base.array_wrapper import ArrayWrapper

# Create wrapper with metadata
index = pd.date_range('2020-01-01', periods=100, freq='D')
columns = ['AAPL', 'GOOGL', 'MSFT']

wrapper = ArrayWrapper(
    index=index,
    columns=columns,
    freq='D'
)

# Wrap numpy arrays as pandas objects
import numpy as np
data = np.random.randn(100, 3)
df = wrapper.wrap(data)  # Returns DataFrame with proper index/columns
```

### Broadcasting Functions
Utilities for broadcasting arrays to compatible shapes for vectorized operations.

```python
from vectorbt.base.reshape_fns import broadcast_arrays, broadcast_to

# Broadcast multiple arrays to common shape
arr1 = np.array([1, 2, 3])
arr2 = np.array([[1], [2]])

broadcasted = broadcast_arrays(arr1, arr2)
# Results in compatible shapes for element-wise operations
```

### Combine Functions
Functions for combining and applying operations across arrays efficiently.

```python
from vectorbt.base.combine_fns import apply_and_concat_one

# Apply function to multiple parameter combinations
def calculate_sma(data, window):
    return pd.Series(data).rolling(window).mean()

# Test multiple windows
windows = [5, 10, 20, 50]
price_data = pd.Series([100, 102, 99, 105, 103])

# Efficiently compute all SMAs
results = apply_and_concat_one(
    len(windows),
    lambda i: calculate_sma(price_data, windows[i]),
    show_progress=True
)
```

## Core Classes and Functions

### ArrayWrapper Methods
```python
# Essential wrapper operations
wrapper = ArrayWrapper(index=index, columns=columns, freq='D')

# Wrap arrays as pandas objects
df = wrapper.wrap(numpy_array)                    # Create DataFrame
series = wrapper.wrap(numpy_array, squeeze=True)  # Create Series

# Array manipulation
reshaped = wrapper.reshape(data, new_shape)       # Reshape with metadata
grouped = wrapper.groupby(group_by)               # Group wrapper
```

### Broadcasting Utilities
```python
# Common broadcasting patterns
from vectorbt.base.reshape_fns import *

# Broadcast to specific shape
broadcasted = broadcast_to(array, target_shape)

# Tile array multiple times
tiled = tile_axis(array, n_tiles, axis=0)

# Reshape operations
reshaped = soft_broadcast(arrays, target_shape)
```

### Index Functions
```python
from vectorbt.base.index_fns import *

# Index manipulation
new_index = repeat_index(original_index, n_times)
stacked_index = stack_indexes([index1, index2])
aligned_indexes = align_indexes([df1.index, df2.index])
```

## Common Usage Patterns

### Parameter Sweep Infrastructure
```python
def parameter_sweep_backtest(price_data, param_combinations):
    """Efficiently backtest multiple parameter combinations"""
    
    def single_backtest(i, price, combinations):
        params = combinations[i]
        # Run strategy with specific parameters
        return strategy_logic(price, **params)
    
    # Use vectorbt's combine functions
    from vectorbt.base.combine_fns import apply_and_concat_one
    
    results = apply_and_concat_one(
        len(param_combinations),
        single_backtest,
        price_data,
        param_combinations,
        show_progress=True
    )
    
    return results
```

### Memory-Efficient Operations
```python
def chunked_processing(large_dataset, chunk_size=1000):
    """Process large datasets in chunks to manage memory"""
    
    from vectorbt.base.combine_fns import apply_and_concat_multiple
    
    n_chunks = len(large_dataset) // chunk_size
    
    def process_chunk(i, dataset, chunk_size):
        start_idx = i * chunk_size
        end_idx = min((i + 1) * chunk_size, len(dataset))
        chunk = dataset.iloc[start_idx:end_idx]
        
        # Process chunk
        return expensive_calculation(chunk)
    
    results = apply_and_concat_multiple(
        n_chunks,
        process_chunk,
        large_dataset,
        chunk_size
    )
    
    return results
```

### Custom Array Wrappers
```python
class TradingArrayWrapper(ArrayWrapper):
    """Custom wrapper for trading data with additional functionality"""
    
    def __init__(self, *args, asset_names=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.asset_names = asset_names or self.columns
    
    def to_returns(self, data):
        """Convert price data to returns"""
        wrapped = self.wrap(data)
        return wrapped.pct_change().iloc[1:]
    
    def normalize_prices(self, data, base_date=None):
        """Normalize prices to base date = 100"""
        wrapped = self.wrap(data)
        if base_date is None:
            base_date = wrapped.index[0]
        
        base_values = wrapped.loc[base_date]
        return wrapped / base_values * 100
```

## Integration with Trading Workflows

### Signal Broadcasting
```python
def broadcast_signals_to_assets(signal_series, asset_columns):
    """Broadcast trading signals to multiple assets"""
    
    from vectorbt.base.reshape_fns import broadcast_to
    
    # Convert signal to 2D for multiple assets
    signal_array = signal_series.values.reshape(-1, 1)
    broadcasted = broadcast_to(signal_array, (len(signal_series), len(asset_columns)))
    
    # Wrap with proper metadata
    wrapper = ArrayWrapper(
        index=signal_series.index,
        columns=asset_columns
    )
    
    return wrapper.wrap(broadcasted)
```

### Multi-Asset Analysis
```python
def prepare_multi_asset_analysis(price_data, analysis_params):
    """Prepare data structure for multi-asset analysis"""
    
    # Create wrapper for results
    wrapper = ArrayWrapper(
        index=price_data.index,
        columns=pd.MultiIndex.from_product([
            price_data.columns,  # Assets
            list(analysis_params.keys())  # Parameters
        ], names=['Asset', 'Parameter']),
        freq=price_data.index.freq
    )
    
    # Broadcast price data for all parameter combinations
    n_params = len(analysis_params)
    broadcasted_prices = np.tile(price_data.values, (1, n_params))
    
    return wrapper.wrap(broadcasted_prices)
```

## Performance Optimization

### Efficient Array Operations
```python
# Use numba-compiled versions for speed
from vectorbt.base.combine_fns import apply_and_concat_one_nb

@numba.jit
def fast_calculation_nb(i, data, params):
    """Numba-compiled calculation for speed"""
    return np.mean(data) * params[i]

# Much faster than pure Python version
results = apply_and_concat_one_nb(
    len(param_list),
    fast_calculation_nb,
    price_data.values,
    np.array(param_list)
)
```

### Memory Management
```python
def memory_efficient_broadcast(small_array, target_shape):
    """Broadcast without creating full arrays in memory"""
    
    from vectorbt.base.reshape_fns import broadcast_to
    
    # Use view-based broadcasting when possible
    if small_array.shape[0] == 1:
        # Use repeat instead of full broadcast
        return np.repeat(small_array, target_shape[0], axis=0)
    else:
        return broadcast_to(small_array, target_shape)
```

### Parallel Processing Setup
```python
# Setup for distributed computing with Ray
def setup_distributed_backtest():
    """Configure vectorbt for distributed processing"""
    
    import ray
    from vectorbt.base.combine_fns import apply_and_concat_one_ray
    
    # Initialize Ray
    ray.init()
    
    def distributed_strategy(param_combinations, price_data):
        return apply_and_concat_one_ray(
            len(param_combinations),
            strategy_function,
            price_data,
            param_combinations
        )
    
    return distributed_strategy
```

## Error Handling and Validation
```python
def safe_array_operations(data, wrapper):
    """Safely perform array operations with validation"""
    
    try:
        # Validate input data
        if not isinstance(data, np.ndarray):
            data = np.asarray(data)
        
        # Check compatibility with wrapper
        if hasattr(wrapper, 'shape'):
            expected_shape = wrapper.shape
            if data.shape != expected_shape:
                print(f"Warning: Shape mismatch. Expected {expected_shape}, got {data.shape}")
                data = broadcast_to(data, expected_shape)
        
        # Wrap and return
        return wrapper.wrap(data)
        
    except Exception as e:
        print(f"Error in array operations: {e}")
        return None
```

The base package provides essential infrastructure for efficient vectorized operations in trading applications, enabling high-performance analysis across multiple assets and parameter combinations.