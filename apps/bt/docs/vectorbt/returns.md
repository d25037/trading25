# Returns Package

## Overview
The returns package provides comprehensive financial risk and performance metrics for analyzing trading strategies. Compatible with empyrical and quantstats, it offers essential tools for portfolio performance evaluation.

## Key Components

### ReturnsAccessor (.vbt.returns)
Main accessor for analyzing return series, providing comprehensive performance metrics.

```python
import pandas as pd
import vectorbt as vbt

# Convert prices to returns
price = pd.Series([100, 102, 99, 105, 103])
returns = price.vbt.returns.from_value(freq='D')

# Basic metrics
print(returns.total())          # Total return
print(returns.annualized())     # Annualized return
print(returns.sharpe_ratio())   # Sharpe ratio
print(returns.max_drawdown())   # Maximum drawdown
```

### Core Performance Metrics

#### Risk Metrics
```python
returns = pd.Series([0.01, -0.02, 0.015, -0.01, 0.005])
ret_acc = returns.vbt.returns(freq='D')

# Risk measures
volatility = ret_acc.volatility()           # Annualized volatility
max_dd = ret_acc.max_drawdown()             # Maximum drawdown
var = ret_acc.value_at_risk(cutoff=0.05)    # Value at Risk (5%)
cvar = ret_acc.cond_value_at_risk(cutoff=0.05)  # Conditional VaR
```

#### Return Metrics
```python
# Return measures
total_return = ret_acc.total()              # Cumulative return
annualized = ret_acc.annualized()          # Annualized return
geometric_mean = ret_acc.geometric_mean()   # Geometric mean
```

#### Risk-Adjusted Metrics
```python
# Risk-adjusted ratios
sharpe = ret_acc.sharpe_ratio()             # Sharpe ratio
sortino = ret_acc.sortino_ratio()           # Sortino ratio
calmar = ret_acc.calmar_ratio()             # Calmar ratio
omega = ret_acc.omega_ratio()               # Omega ratio
```

### Drawdown Analysis
```python
# Detailed drawdown analysis
drawdowns = ret_acc.drawdowns
dd_duration = drawdowns.duration.max()      # Max drawdown duration
dd_stats = ret_acc.drawdown_stats()         # Comprehensive DD stats
```

### Benchmark Comparison
```python
# Compare against benchmark
strategy_returns = pd.Series([0.02, -0.01, 0.015])
benchmark_returns = pd.Series([0.01, -0.005, 0.008])

strategy_acc = strategy_returns.vbt.returns(freq='D')
benchmark_acc = benchmark_returns.vbt.returns(freq='D')

# Relative metrics
alpha = strategy_acc.alpha(benchmark_acc)
beta = strategy_acc.beta(benchmark_acc)
tracking_error = strategy_acc.tracking_error(benchmark_acc)
information_ratio = strategy_acc.information_ratio(benchmark_acc)
```

## Common Usage Patterns

### Strategy Performance Analysis
```python
def analyze_strategy_performance(returns_series, benchmark=None, freq='D'):
    """Comprehensive strategy performance analysis"""
    ret_acc = returns_series.vbt.returns(freq=freq)
    
    metrics = {
        'Total Return': ret_acc.total(),
        'Annualized Return': ret_acc.annualized(),
        'Volatility': ret_acc.volatility(),
        'Sharpe Ratio': ret_acc.sharpe_ratio(),
        'Sortino Ratio': ret_acc.sortino_ratio(),
        'Max Drawdown': ret_acc.max_drawdown(),
        'Calmar Ratio': ret_acc.calmar_ratio()
    }
    
    if benchmark is not None:
        bench_acc = benchmark.vbt.returns(freq=freq)
        metrics.update({
            'Alpha': ret_acc.alpha(bench_acc),
            'Beta': ret_acc.beta(bench_acc),
            'Information Ratio': ret_acc.information_ratio(bench_acc)
        })
    
    return metrics
```

### Rolling Performance Metrics
```python
# Rolling Sharpe ratio for time-varying performance
rolling_sharpe = returns.vbt.returns.rolling_sharpe(window=252)  # 1-year rolling
rolling_max_dd = returns.vbt.returns.rolling_max_drawdown(window=252)
```

### Portfolio Attribution
```python
# Multi-asset portfolio analysis
portfolio_returns = pd.DataFrame({
    'Strategy_A': [0.02, -0.01, 0.015],
    'Strategy_B': [0.01, 0.005, -0.008],
    'Strategy_C': [-0.005, 0.02, 0.012]
})

# Analyze each strategy
for col in portfolio_returns.columns:
    ret_acc = portfolio_returns[col].vbt.returns(freq='D')
    print(f"{col}: Sharpe = {ret_acc.sharpe_ratio():.3f}")
```

### Risk Budgeting
```python
# Risk contribution analysis
def risk_contribution(returns_df):
    """Calculate risk contribution for each asset"""
    portfolio_vol = returns_df.vbt.returns.volatility()
    individual_vols = {}
    
    for col in returns_df.columns:
        ret_acc = returns_df[col].vbt.returns(freq='D')
        individual_vols[col] = ret_acc.volatility()
    
    return individual_vols, portfolio_vol
```

## Integration with Portfolio Analysis

### From Signals to Performance
```python
# Complete workflow: signals -> trades -> returns -> performance
close_prices = pd.Series([100, 102, 99, 105, 103, 107, 104])

# Generate signals (example)
entries = pd.Series([True, False, True, False, False, True, False])
exits = pd.Series([False, True, False, False, True, False, True])

# Calculate returns from trades
from vectorbt.portfolio import Portfolio
pf = Portfolio.from_signals(close_prices, entries, exits)
returns = pf.returns()

# Analyze performance
performance = returns.vbt.returns(freq='D')
print(f"Strategy Sharpe: {performance.sharpe_ratio():.3f}")
```

### Advanced Metrics
```python
# Advanced performance metrics
def advanced_metrics(returns_series):
    ret_acc = returns_series.vbt.returns(freq='D')
    
    return {
        'Tail Ratio': ret_acc.tail_ratio(),
        'Skewness': ret_acc.skew(),
        'Kurtosis': ret_acc.kurtosis(),
        'VaR (5%)': ret_acc.value_at_risk(cutoff=0.05),
        'CVaR (5%)': ret_acc.cond_value_at_risk(cutoff=0.05),
        'Gain-to-Pain Ratio': ret_acc.gain_to_pain_ratio()
    }
```

## Performance Optimization

### Efficient Batch Analysis
```python
# Analyze multiple strategies efficiently
strategies = {
    'SMA_Cross': returns_sma,
    'RSI_Mean_Reversion': returns_rsi,
    'Momentum': returns_mom
}

results = {}
for name, rets in strategies.items():
    results[name] = rets.vbt.returns(freq='D').sharpe_ratio()

# Compare strategies
best_strategy = max(results, key=results.get)
```

### Memory-Efficient Rolling Calculations
```python
# Use chunked processing for large datasets
def rolling_metrics_chunked(returns, window, chunk_size=1000):
    ret_acc = returns.vbt.returns(freq='D')
    chunks = []
    
    for i in range(0, len(returns), chunk_size):
        chunk = returns.iloc[i:i+chunk_size]
        chunk_acc = chunk.vbt.returns(freq='D')
        rolling_sharpe = chunk_acc.rolling_sharpe(window=window)
        chunks.append(rolling_sharpe)
    
    return pd.concat(chunks)
```

## Error Handling and Validation
```python
def safe_performance_analysis(returns_series):
    try:
        ret_acc = returns_series.vbt.returns(freq='D')
        
        # Validate data quality
        if ret_acc.count() < 30:
            print("Warning: Less than 30 observations")
        
        # Handle infinite/NaN values
        metrics = {}
        for metric_name, metric_func in [
            ('Sharpe', ret_acc.sharpe_ratio),
            ('Sortino', ret_acc.sortino_ratio),
            ('Max DD', ret_acc.max_drawdown)
        ]:
            try:
                value = metric_func()
                metrics[metric_name] = value if pd.notnull(value) else 0.0
            except Exception as e:
                print(f"Error calculating {metric_name}: {e}")
                metrics[metric_name] = None
        
        return metrics
        
    except Exception as e:
        print(f"Error in performance analysis: {e}")
        return None
```

The returns package is essential for quantifying trading strategy performance with industry-standard metrics and comprehensive risk analysis tools.