# Labels Package

## Overview
The labels package provides tools for creating look-ahead indicators and generating labels for machine learning applications in trading. It's particularly useful for creating target variables and trend indicators.

## Key Components

### Label Generators
Create labels for supervised learning models based on future price movements.

```python
import pandas as pd
import vectorbt as vbt
from vectorbt.labels import FLFB, TPSL, TSLB

# Sample price data
close = pd.Series([100, 102, 99, 105, 103, 107, 104, 106])

# Fixed lookforward/lookback labels
flfb = FLFB.from_params(close, lookforward=3, lookback=2)
labels = flfb.generate()

# Take profit / Stop loss labels  
tpsl = TPSL.from_params(close, tp_thresh=0.05, sl_thresh=0.03)
tp_sl_labels = tpsl.generate()
```

### Trend Mode Labeling
Different approaches to labeling trends and price movements.

```python
from vectorbt.labels.enums import TrendMode

# Binary trend labels (up/down)
binary_labels = close.vbt.labels.trend(
    mode=TrendMode.Binary,
    forward_window=5
)

# Percentage change labels
pct_labels = close.vbt.labels.trend(
    mode=TrendMode.PctChange, 
    forward_window=3,
    threshold=0.02  # 2% threshold
)
```

## Core Label Types

### Fixed Lookforward/Lookback (FLFB)
```python
# Generate labels based on future returns
def create_return_labels(price_series, lookforward=5, threshold=0.02):
    """Create labels: 1 if return > threshold, -1 if < -threshold, 0 otherwise"""
    
    flfb = FLFB.from_params(
        price_series,
        lookforward=lookforward,
        lookback=1
    )
    
    labels = flfb.generate()
    returns = labels.pct_change()
    
    # Convert to classification labels
    conditions = [
        returns > threshold,
        returns < -threshold
    ]
    choices = [1, -1]  # Buy, Sell
    
    return pd.Series(
        np.select(conditions, choices, default=0),
        index=price_series.index
    )
```

### Take Profit/Stop Loss (TPSL)
```python
# Labels based on hitting TP/SL levels first
def create_tpsl_labels(price_series, tp_thresh=0.05, sl_thresh=0.03):
    """Create labels based on which level is hit first"""
    
    tpsl = TPSL.from_params(
        price_series,
        tp_thresh=tp_thresh,    # Take profit at +5%
        sl_thresh=sl_thresh,    # Stop loss at -3%
        max_window=20           # Maximum holding period
    )
    
    labels = tpsl.generate()
    return labels
```

### Trailing Stop/Limit/Barrier (TSLB)
```python
# Advanced labeling with trailing stops
def create_advanced_labels(price_series, target_thresh=0.04):
    """Create labels with trailing stop mechanism"""
    
    tslb = TSLB.from_params(
        price_series,
        target_thresh=target_thresh,
        trailing_thresh=0.02,
        max_window=15
    )
    
    return tslb.generate()
```

## Common Usage Patterns

### Machine Learning Feature Preparation
```python
def prepare_ml_dataset(price_data, features, label_params):
    """Prepare dataset for ML model training"""
    
    # Create features (indicators, technical analysis)
    feature_df = pd.DataFrame(index=price_data.index)
    
    # Add technical indicators as features
    feature_df['sma_5'] = price_data.rolling(5).mean()
    feature_df['sma_20'] = price_data.rolling(20).mean()
    feature_df['rsi'] = vbt.RSI.run(price_data, window=14).rsi
    feature_df['bb_position'] = vbt.BBANDS.run(price_data).bb_percent
    
    # Create labels
    labels = create_return_labels(
        price_data, 
        lookforward=label_params['lookforward'],
        threshold=label_params['threshold']
    )
    
    # Combine and clean
    ml_data = pd.concat([feature_df, labels.rename('target')], axis=1)
    return ml_data.dropna()
```

### Multi-Horizon Daily Labeling
```python
def create_multi_horizon_daily_labels(price_series):
    """Create labels for different daily time horizons"""
    
    labels = {}
    daily_horizons = {
        'short_term': {'lookforward': 3, 'threshold': 0.01},
        'medium_term': {'lookforward': 10, 'threshold': 0.03}, 
        'long_term': {'lookforward': 20, 'threshold': 0.05}
    }
    
    for name, params in daily_horizons.items():
        labels[f'{name}_daily'] = create_return_labels(
            price_series,
            lookforward=params['lookforward'],
            threshold=params['threshold']
        )
    
    return pd.DataFrame(labels)
```

### Event-Based Labeling
```python
def create_event_labels(price_series, events):
    """Create labels based on specific market events"""
    
    labels = pd.Series(0, index=price_series.index)
    
    for event_date in events:
        if event_date in price_series.index:
            # Look at price movement after event
            event_idx = price_series.index.get_loc(event_date)
            
            if event_idx + 5 < len(price_series):
                future_return = (
                    price_series.iloc[event_idx + 5] / 
                    price_series.iloc[event_idx] - 1
                )
                
                # Label based on magnitude of movement
                if future_return > 0.02:
                    labels.iloc[event_idx] = 1
                elif future_return < -0.02:
                    labels.iloc[event_idx] = -1
    
    return labels
```

## Integration with Strategy Development

### Label-Based Signal Generation
```python
def labels_to_signals(labels, confidence_threshold=0.7):
    """Convert ML model predictions to trading signals"""
    
    # Assuming labels contain probability scores
    buy_signals = labels > confidence_threshold
    sell_signals = labels < -confidence_threshold
    
    return buy_signals, sell_signals
```

### Validation and Backtesting
```python
def validate_labels(price_series, labels, forward_window=5):
    """Validate label quality by checking future returns"""
    
    future_returns = price_series.pct_change(forward_window).shift(-forward_window)
    
    # Analyze label accuracy
    results = {}
    for label_value in [-1, 0, 1]:
        mask = labels == label_value
        if mask.sum() > 0:
            avg_return = future_returns[mask].mean()
            accuracy = (
                (future_returns[mask] > 0).sum() / mask.sum() 
                if label_value == 1 else
                (future_returns[mask] < 0).sum() / mask.sum()
                if label_value == -1 else
                abs(future_returns[mask]).mean()
            )
            results[f'label_{label_value}'] = {
                'count': mask.sum(),
                'avg_return': avg_return,
                'accuracy': accuracy
            }
    
    return results
```

### Advanced Labeling Strategies
```python
def create_regime_aware_labels(price_series, vol_window=20):
    """Create labels that adapt to daily market volatility regime"""
    
    # Calculate rolling volatility based on daily returns
    returns = price_series.pct_change()
    volatility = returns.rolling(vol_window).std()
    vol_percentile = volatility.rolling(60).rank(pct=True)  # 60-day rolling percentile for daily analysis
    
    # Adjust thresholds based on volatility regime
    low_vol_thresh = 0.01
    high_vol_thresh = 0.03
    
    dynamic_threshold = np.where(
        vol_percentile > 0.7, 
        high_vol_thresh,
        low_vol_thresh
    )
    
    # Generate labels with dynamic thresholds
    labels = pd.Series(0, index=price_series.index)
    
    for i in range(5, len(price_series) - 5):
        future_return = (
            price_series.iloc[i + 5] / price_series.iloc[i] - 1
        )
        threshold = dynamic_threshold[i]
        
        if future_return > threshold:
            labels.iloc[i] = 1
        elif future_return < -threshold:
            labels.iloc[i] = -1
    
    return labels
```

## Performance and Best Practices

### Efficient Label Generation
```python
# Use vectorized operations when possible
def fast_binary_labels(price_series, lookforward=5, threshold=0.02):
    """Efficient binary label generation"""
    
    # Calculate future returns vectorized
    future_prices = price_series.shift(-lookforward)
    future_returns = future_prices / price_series - 1
    
    # Generate labels
    labels = pd.Series(0, index=price_series.index)
    labels[future_returns > threshold] = 1
    labels[future_returns < -threshold] = -1
    
    return labels
```

### Data Leakage Prevention
```python
def safe_label_split(features, labels, test_size=0.2, lookforward=5):
    """Split data preventing look-ahead bias"""
    
    # Ensure no overlap between train and test
    split_idx = int(len(features) * (1 - test_size))
    
    # Account for lookforward period
    train_end = split_idx - lookforward
    
    X_train = features.iloc[:train_end]
    y_train = labels.iloc[:train_end]
    
    X_test = features.iloc[split_idx:]
    y_test = labels.iloc[split_idx:]
    
    return X_train, X_test, y_train, y_test
```

The labels package provides essential tools for creating high-quality training data for machine learning models in quantitative trading, with built-in protections against common pitfalls like look-ahead bias.