# Records Package

## Overview
The records package provides tools for working with sparse event data such as trades, positions, orders, and drawdowns. Records allow efficient storage and analysis of trading events without converting them back to matrix form.

## Key Components

### Records Base Class
Foundation for all record types with efficient storage and querying capabilities.

```python
import vectorbt as vbt
import pandas as pd

# Access trade records from portfolio
portfolio = vbt.Portfolio.from_signals(close, entries, exits)
trades = portfolio.trades

# Basic record operations
print(trades.count())           # Number of trades
print(trades.records_readable)  # Human-readable format
print(trades.apply_mask(trades.pnl > 0))  # Filter profitable trades
```

### MappedArray
Efficient storage structure for record data with fast indexing and querying.

```python
# Access underlying mapped array
mapped_trades = trades.values
print(mapped_trades.shape)      # Shape of record data
print(mapped_trades.dtype)      # Data type structure
```

### Field Access
Dynamic properties for accessing record fields.

```python
# Access trade fields
entry_prices = trades.entry_price    # Entry prices
exit_prices = trades.exit_price      # Exit prices  
pnl = trades.pnl                     # Profit/loss
durations = trades.duration          # Trade durations
sizes = trades.size                  # Position sizes
```

## Common Usage Patterns

### Trade Analysis
```python
def analyze_trades(portfolio):
    """Comprehensive trade analysis"""
    
    trades = portfolio.trades
    
    if trades.count() == 0:
        return "No trades found"
    
    analysis = {
        'total_trades': trades.count(),
        'winning_trades': trades.apply_mask(trades.pnl > 0).count(),
        'losing_trades': trades.apply_mask(trades.pnl < 0).count(),
        'avg_profit': trades.pnl[trades.pnl > 0].mean(),
        'avg_loss': trades.pnl[trades.pnl < 0].mean(),
        'max_profit': trades.pnl.max(),
        'max_loss': trades.pnl.min(),
        'avg_duration': trades.duration.mean(),
        'win_rate': (trades.pnl > 0).sum() / len(trades.pnl)
    }
    
    return analysis
```

### Position Tracking
```python
def track_positions(portfolio):
    """Track position changes over time"""
    
    positions = portfolio.positions
    
    # Position statistics
    position_stats = {
        'avg_position_size': positions.size.mean(),
        'max_position': positions.size.max(),
        'position_count': positions.count(),
        'avg_holding_period': positions.duration.mean()
    }
    
    # Long vs short positions
    long_positions = positions.apply_mask(positions.size > 0)
    short_positions = positions.apply_mask(positions.size < 0)
    
    position_stats.update({
        'long_positions': long_positions.count(),
        'short_positions': short_positions.count(),
        'long_avg_return': long_positions.pnl.mean() if long_positions.count() > 0 else 0,
        'short_avg_return': short_positions.pnl.mean() if short_positions.count() > 0 else 0
    })
    
    return position_stats
```

### Drawdown Analysis
```python
def analyze_drawdowns(portfolio):
    """Analyze drawdown periods"""
    
    drawdowns = portfolio.drawdowns
    
    if drawdowns.count() == 0:
        return "No drawdowns found"
    
    dd_analysis = {
        'total_drawdowns': drawdowns.count(),
        'max_drawdown': drawdowns.drawdown.max(),
        'avg_drawdown': drawdowns.drawdown.mean(),
        'max_duration': drawdowns.duration.max(),
        'avg_duration': drawdowns.duration.mean(),
        'current_drawdown': drawdowns.drawdown.iloc[-1] if drawdowns.count() > 0 else 0
    }
    
    # Recovery analysis
    recovered_dd = drawdowns.apply_mask(drawdowns.status == 'Recovered')
    if recovered_dd.count() > 0:
        dd_analysis['avg_recovery_time'] = recovered_dd.duration.mean()
        dd_analysis['recovery_rate'] = recovered_dd.count() / drawdowns.count()
    
    return dd_analysis
```

### Order Analysis
```python
def analyze_orders(portfolio):
    """Analyze order execution patterns"""
    
    orders = portfolio.orders
    
    order_analysis = {
        'total_orders': orders.count(),
        'buy_orders': orders.apply_mask(orders.side == 'Buy').count(),
        'sell_orders': orders.apply_mask(orders.side == 'Sell').count(),
        'avg_order_size': orders.size.mean(),
        'avg_fill_price': orders.price.mean()
    }
    
    # Order timing analysis
    if hasattr(orders, 'timestamp'):
        order_times = pd.to_datetime(orders.timestamp)
        order_analysis.update({
            'orders_per_day': orders.count() / (order_times.max() - order_times.min()).days,
            'first_order': order_times.min(),
            'last_order': order_times.max()
        })
    
    return order_analysis
```

## Advanced Record Operations

### Custom Record Filtering
```python
def custom_trade_filter(trades, min_duration=5, min_profit=100):
    """Filter trades based on custom criteria"""
    
    # Create compound mask
    duration_mask = trades.duration >= min_duration
    profit_mask = trades.pnl >= min_profit
    
    # Combine masks
    combined_mask = duration_mask & profit_mask
    
    # Apply filter
    filtered_trades = trades.apply_mask(combined_mask)
    
    return filtered_trades
```

### Record Aggregation
```python
def aggregate_trades_by_period(trades, freq='D'):
    """Aggregate trades by daily periods for detailed analysis"""
    
    if trades.count() == 0:
        return pd.DataFrame()
    
    # Convert to DataFrame with timestamps
    trades_df = trades.records_readable
    trades_df['exit_timestamp'] = pd.to_datetime(trades_df['Exit Timestamp'])
    trades_df = trades_df.set_index('exit_timestamp')
    
    # Group by daily periods
    daily_stats = trades_df.groupby(pd.Grouper(freq=freq)).agg({
        'PnL': ['count', 'sum', 'mean'],
        'Duration': 'mean',
        'Return': 'mean'
    }).round(4)
    
    daily_stats.columns = ['Trade_Count', 'Total_PnL', 'Avg_PnL', 'Avg_Duration', 'Avg_Return']
    
    return daily_stats
```

### Performance Attribution
```python
def performance_attribution(trades):
    """Attribute performance to different trade characteristics"""
    
    if trades.count() == 0:
        return {}
    
    # Convert to DataFrame for analysis
    trades_df = pd.DataFrame({
        'pnl': trades.pnl,
        'duration': trades.duration,
        'size': trades.size.abs(),  # Absolute position size
        'return_pct': trades.pnl / (trades.entry_price * trades.size.abs()) * 100
    })
    
    attribution = {}
    
    # Duration-based attribution
    short_term = trades_df[trades_df.duration <= 5]
    medium_term = trades_df[(trades_df.duration > 5) & (trades_df.duration <= 20)]
    long_term = trades_df[trades_df.duration > 20]
    
    attribution['by_duration'] = {
        'short_term': {
            'count': len(short_term),
            'total_pnl': short_term.pnl.sum(),
            'avg_return': short_term.return_pct.mean()
        },
        'medium_term': {
            'count': len(medium_term),
            'total_pnl': medium_term.pnl.sum(),
            'avg_return': medium_term.return_pct.mean()
        },
        'long_term': {
            'count': len(long_term),
            'total_pnl': long_term.pnl.sum(),
            'avg_return': long_term.return_pct.mean()
        }
    }
    
    # Size-based attribution
    size_quantiles = trades_df.size.quantile([0.33, 0.66])
    small_trades = trades_df[trades_df.size <= size_quantiles.iloc[0]]
    medium_trades = trades_df[(trades_df.size > size_quantiles.iloc[0]) & 
                             (trades_df.size <= size_quantiles.iloc[1])]
    large_trades = trades_df[trades_df.size > size_quantiles.iloc[1]]
    
    attribution['by_size'] = {
        'small': {'count': len(small_trades), 'total_pnl': small_trades.pnl.sum()},
        'medium': {'count': len(medium_trades), 'total_pnl': medium_trades.pnl.sum()},
        'large': {'count': len(large_trades), 'total_pnl': large_trades.pnl.sum()}
    }
    
    return attribution
```

## Integration with Portfolio Analysis

### Complete Trading Analysis Pipeline
```python
def comprehensive_trading_analysis(portfolio):
    """Complete analysis of trading performance using records"""
    
    results = {}
    
    # Trade analysis
    if portfolio.trades.count() > 0:
        results['trades'] = analyze_trades(portfolio)
        results['trade_attribution'] = performance_attribution(portfolio.trades)
        results['daily_trades'] = aggregate_trades_by_period(portfolio.trades)
    
    # Position analysis
    if portfolio.positions.count() > 0:
        results['positions'] = track_positions(portfolio)
    
    # Drawdown analysis
    if portfolio.drawdowns.count() > 0:
        results['drawdowns'] = analyze_drawdowns(portfolio)
    
    # Order analysis
    if portfolio.orders.count() > 0:
        results['orders'] = analyze_orders(portfolio)
    
    return results
```

### Risk Metrics from Records
```python
def calculate_risk_metrics_from_trades(trades):
    """Calculate risk metrics from trade records"""
    
    if trades.count() == 0:
        return {}
    
    returns = trades.pnl / (trades.entry_price * trades.size.abs())
    
    risk_metrics = {
        'sharpe_ratio': returns.mean() / returns.std() * np.sqrt(252) if returns.std() > 0 else 0,
        'sortino_ratio': returns.mean() / returns[returns < 0].std() * np.sqrt(252) if len(returns[returns < 0]) > 0 else 0,
        'max_consecutive_losses': max_consecutive_losses(trades.pnl < 0),
        'profit_factor': trades.pnl[trades.pnl > 0].sum() / abs(trades.pnl[trades.pnl < 0].sum()) if (trades.pnl < 0).any() else float('inf'),
        'hit_rate': (trades.pnl > 0).sum() / len(trades.pnl),
        'avg_win_loss_ratio': trades.pnl[trades.pnl > 0].mean() / abs(trades.pnl[trades.pnl < 0].mean()) if (trades.pnl < 0).any() else float('inf')
    }
    
    return risk_metrics

def max_consecutive_losses(loss_mask):
    """Calculate maximum consecutive losses"""
    consecutive = 0
    max_consecutive = 0
    
    for is_loss in loss_mask:
        if is_loss:
            consecutive += 1
            max_consecutive = max(max_consecutive, consecutive)
        else:
            consecutive = 0
    
    return max_consecutive
```

## Memory Efficiency and Performance

### Efficient Record Processing
```python
def process_large_record_set(records, batch_size=10000):
    """Process large record sets efficiently"""
    
    total_records = records.count()
    results = []
    
    for start_idx in range(0, total_records, batch_size):
        end_idx = min(start_idx + batch_size, total_records)
        
        # Process batch
        batch = records.iloc[start_idx:end_idx]
        batch_result = analyze_trades_batch(batch)
        results.append(batch_result)
        
        print(f"Processed {end_idx}/{total_records} records")
    
    # Combine results
    return combine_batch_results(results)

def analyze_trades_batch(trades_batch):
    """Analyze a batch of trades"""
    return {
        'count': trades_batch.count(),
        'total_pnl': trades_batch.pnl.sum(),
        'avg_duration': trades_batch.duration.mean()
    }

def combine_batch_results(batch_results):
    """Combine results from multiple batches"""
    combined = {
        'total_count': sum(r['count'] for r in batch_results),
        'total_pnl': sum(r['total_pnl'] for r in batch_results),
        'weighted_avg_duration': sum(r['avg_duration'] * r['count'] for r in batch_results) / sum(r['count'] for r in batch_results)
    }
    return combined
```

The records package enables efficient analysis of sparse trading events, providing powerful tools for understanding trade patterns, risk characteristics, and performance attribution without the memory overhead of full time series data.