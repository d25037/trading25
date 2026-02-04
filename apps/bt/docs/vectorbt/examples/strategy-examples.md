# VectorBT Strategy Examples

This document provides practical examples of trading strategies implemented in VectorBT, progressing from simple to complex implementations.

## Basic Strategies

### 1. Simple Moving Average Crossover

```python
import vectorbt as vbt
import pandas as pd
import numpy as np

def sma_crossover_strategy(close, fast_window=10, slow_window=30):
    """
    Simple moving average crossover strategy
    Buy when fast SMA crosses above slow SMA
    Sell when fast SMA crosses below slow SMA
    """
    # Calculate moving averages
    fast_sma = vbt.indicators.SMA.run(close, fast_window)
    slow_sma = vbt.indicators.SMA.run(close, slow_window)
    
    # Generate signals
    entries = fast_sma.sma_crossed_above(slow_sma.sma)
    exits = fast_sma.sma_crossed_below(slow_sma.sma)
    
    return vbt.Portfolio.from_signals(
        close=close,
        entries=entries,
        exits=exits,
        init_cash=10000,
        fees=0.001
    )

# Example usage
data = vbt.YFData.download('AAPL', start='2020-01-01', end='2023-01-01')
close = data.get('Close')

portfolio = sma_crossover_strategy(close)
print(f"Total Return: {portfolio.total_return():.2%}")
print(f"Sharpe Ratio: {portfolio.sharpe_ratio():.2f}")
```

### 2. RSI Mean Reversion

```python
def rsi_mean_reversion(close, rsi_window=14, oversold=30, overbought=70):
    """
    RSI mean reversion strategy
    Buy when RSI is oversold
    Sell when RSI is overbought
    """
    rsi = vbt.indicators.RSI.run(close, rsi_window)
    
    entries = rsi.rsi < oversold
    exits = rsi.rsi > overbought
    
    return vbt.Portfolio.from_signals(
        close=close,
        entries=entries,
        exits=exits,
        init_cash=10000,
        fees=0.001
    )

# Example usage
portfolio_rsi = rsi_mean_reversion(close)
print(f"RSI Strategy Return: {portfolio_rsi.total_return():.2%}")
```

### 3. Bollinger Bands Breakout

```python
def bollinger_breakout(close, window=20, alpha=2):
    """
    Bollinger Bands breakout strategy
    Buy when price breaks above upper band
    Sell when price breaks below lower band
    """
    bb = vbt.indicators.BBANDS.run(close, window=window, alpha=alpha)
    
    entries = close > bb.upperband
    exits = close < bb.lowerband
    
    return vbt.Portfolio.from_signals(
        close=close,
        entries=entries,
        exits=exits,
        init_cash=10000,
        fees=0.001
    )

portfolio_bb = bollinger_breakout(close)
print(f"Bollinger Breakout Return: {portfolio_bb.total_return():.2%}")
```

## Intermediate Strategies

### 4. Multi-Condition Strategy

```python
def multi_condition_strategy(close, volume=None):
    """
    Strategy combining multiple technical indicators
    """
    # Technical indicators
    sma_fast = vbt.indicators.SMA.run(close, 10)
    sma_slow = vbt.indicators.SMA.run(close, 30)
    rsi = vbt.indicators.RSI.run(close, 14)
    bb = vbt.indicators.BBANDS.run(close, 20)
    
    # Volume condition (if available)
    if volume is not None:
        volume_sma = vbt.indicators.SMA.run(volume, 20)
        volume_condition = volume > volume_sma.sma * 1.2
    else:
        volume_condition = True
    
    # Entry conditions
    trend_condition = sma_fast.sma > sma_slow.sma
    momentum_condition = rsi.rsi < 70  # Not overbought
    price_condition = close > bb.middleband  # Above middle band
    
    entries = trend_condition & momentum_condition & price_condition & volume_condition
    
    # Exit conditions
    exit_trend = sma_fast.sma < sma_slow.sma
    exit_momentum = rsi.rsi > 80
    
    exits = exit_trend | exit_momentum
    
    return vbt.Portfolio.from_signals(
        close=close,
        entries=entries,
        exits=exits,
        init_cash=10000,
        fees=0.001
    )

# Example with volume data
data_full = vbt.YFData.download('AAPL', start='2020-01-01', end='2023-01-01')
close = data_full.get('Close')
volume = data_full.get('Volume')

portfolio_multi = multi_condition_strategy(close, volume)
print(f"Multi-condition Strategy Return: {portfolio_multi.total_return():.2%}")
```

### 5. Momentum Strategy with Stop Loss

```python
def momentum_with_stops(close, lookback=20, momentum_threshold=0.05, 
                       stop_loss=0.05, take_profit=0.15):
    """
    Momentum strategy with risk management
    """
    # Calculate momentum
    momentum = (close / close.shift(lookback) - 1)
    
    # Entry signals
    entries = momentum > momentum_threshold
    
    # Exit signals (basic)
    exits = momentum < -momentum_threshold
    
    # Create portfolio with stops
    return vbt.Portfolio.from_signals(
        close=close,
        entries=entries,
        exits=exits,
        sl_stop=stop_loss,      # Stop loss
        tp_stop=take_profit,    # Take profit
        init_cash=10000,
        fees=0.001
    )

portfolio_momentum = momentum_with_stops(close)
print(f"Momentum with Stops Return: {portfolio_momentum.total_return():.2%}")
print(f"Max Drawdown: {portfolio_momentum.max_drawdown():.2%}")
```

## Advanced Strategies

### 6. Multi-Asset Pairs Trading

```python
def pairs_trading_strategy(close1, close2, lookback=20, entry_threshold=2, exit_threshold=0):
    """
    Statistical arbitrage strategy for pair trading
    """
    # Calculate spread
    spread = close1 - close2
    spread_sma = vbt.indicators.SMA.run(spread, lookback)
    spread_std = spread.rolling(lookback).std()
    
    # Z-score
    z_score = (spread - spread_sma.sma) / spread_std
    
    # Long spread (buy asset1, sell asset2) when spread is low
    long_entries = z_score < -entry_threshold
    long_exits = z_score > -exit_threshold
    
    # Short spread (sell asset1, buy asset2) when spread is high  
    short_entries = z_score > entry_threshold
    short_exits = z_score < exit_threshold
    
    # Combine long and short signals
    entries = long_entries | short_entries
    exits = long_exits | short_exits
    
    # Create portfolio for the spread
    return vbt.Portfolio.from_signals(
        close=spread,
        entries=entries,
        exits=exits,
        init_cash=10000,
        fees=0.001
    )

# Example with two correlated stocks
data_pairs = vbt.YFData.download(['AAPL', 'MSFT'], start='2020-01-01', end='2023-01-01')
close_aapl = data_pairs.get('Close')['AAPL']
close_msft = data_pairs.get('Close')['MSFT']

portfolio_pairs = pairs_trading_strategy(close_aapl, close_msft)
print(f"Pairs Trading Return: {portfolio_pairs.total_return():.2%}")
```

### 7. Mean Reversion with Regime Detection

```python
def regime_aware_strategy(close, regime_window=50, fast_window=10, slow_window=30):
    """
    Strategy that adapts to market regimes (trending vs mean-reverting)
    """
    # Detect regime using price volatility
    returns = close.pct_change()
    volatility = returns.rolling(regime_window).std()
    vol_threshold = volatility.quantile(0.7)  # High volatility threshold
    
    # High volatility = trending market, Low volatility = mean-reverting market
    trending_regime = volatility > vol_threshold
    mean_reverting_regime = ~trending_regime
    
    # Trending strategy (momentum)
    sma_fast = vbt.indicators.SMA.run(close, fast_window)
    sma_slow = vbt.indicators.SMA.run(close, slow_window)
    
    trend_entries = (close > sma_fast.sma) & (sma_fast.sma > sma_slow.sma)
    trend_exits = (close < sma_fast.sma) | (sma_fast.sma < sma_slow.sma)
    
    # Mean reversion strategy
    bb = vbt.indicators.BBANDS.run(close, 20, 2)
    rsi = vbt.indicators.RSI.run(close, 14)
    
    mr_entries = (close < bb.lowerband) & (rsi.rsi < 30)
    mr_exits = (close > bb.upperband) | (rsi.rsi > 70)
    
    # Combine based on regime
    entries = (trending_regime & trend_entries) | (mean_reverting_regime & mr_entries)
    exits = (trending_regime & trend_exits) | (mean_reverting_regime & mr_exits)
    
    return vbt.Portfolio.from_signals(
        close=close,
        entries=entries,
        exits=exits,
        init_cash=10000,
        fees=0.001
    )

portfolio_regime = regime_aware_strategy(close)
print(f"Regime-Aware Strategy Return: {portfolio_regime.total_return():.2%}")
```

### 8. Machine Learning Integration

```python
def ml_enhanced_strategy(close, features_window=20):
    """
    Strategy enhanced with machine learning predictions
    Requires scikit-learn: pip install scikit-learn
    """
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.preprocessing import StandardScaler
    
    # Create features
    returns = close.pct_change()
    
    # Technical features
    sma = vbt.indicators.SMA.run(close, 20)
    rsi = vbt.indicators.RSI.run(close, 14)
    bb = vbt.indicators.BBANDS.run(close, 20)
    
    # Feature matrix
    features = pd.DataFrame({
        'return_1': returns.shift(1),
        'return_5': returns.rolling(5).mean().shift(1),
        'return_20': returns.rolling(20).mean().shift(1),
        'rsi': rsi.rsi.shift(1),
        'bb_position': ((close - bb.lowerband) / (bb.upperband - bb.lowerband)).shift(1),
        'price_vs_sma': (close / sma.sma - 1).shift(1),
        'volatility': returns.rolling(20).std().shift(1)
    }).dropna()
    
    # Target: next day return > 0
    target = (returns.shift(-1) > 0).astype(int)
    target = target.loc[features.index]
    
    # Split data for training (first 70%)
    split_idx = int(len(features) * 0.7)
    
    X_train = features.iloc[:split_idx]
    y_train = target.iloc[:split_idx]
    X_test = features.iloc[split_idx:]
    
    # Train model
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train_scaled, y_train)
    
    # Generate predictions
    predictions = model.predict_proba(X_test_scaled)[:, 1]  # Probability of positive return
    
    # Create signals based on ML predictions
    prediction_series = pd.Series(predictions, index=X_test.index)
    
    # Entry when ML predicts positive return with high confidence
    entries = prediction_series > 0.6
    exits = prediction_series < 0.4
    
    # Align with close prices
    entries = entries.reindex(close.index, fill_value=False)
    exits = exits.reindex(close.index, fill_value=False)
    
    return vbt.Portfolio.from_signals(
        close=close,
        entries=entries,
        exits=exits,
        init_cash=10000,
        fees=0.001
    )

# Example usage (requires sufficient data)
if len(close) > 500:  # Need enough data for ML
    portfolio_ml = ml_enhanced_strategy(close)
    print(f"ML-Enhanced Strategy Return: {portfolio_ml.total_return():.2%}")
```

## Portfolio-Level Strategies

### 9. Daily Momentum Asset Selection Strategy

```python
def daily_momentum_strategy(closes, lookback=20, top_n=2):
    """
    Daily momentum strategy: select top performing assets based on daily returns
    """
    # Calculate daily momentum for all assets
    daily_returns = closes.pct_change()
    momentum = daily_returns.rolling(lookback).mean()  # Average daily return over lookback period
    
    # Rank assets by daily momentum
    rankings = momentum.rank(axis=1, ascending=False)
    
    # Select top N assets based on daily performance
    selected = rankings <= top_n
    
    # Create equal-weight portfolio of selected assets
    weights = selected.div(selected.sum(axis=1), axis=0).fillna(0)
    
    # Generate daily rebalancing signals for each asset
    entries = (weights > 0) & (weights.shift(1) == 0)
    exits = (weights == 0) & (weights.shift(1) > 0)
    
    # Create portfolio for each asset
    portfolios = {}
    for asset in closes.columns:
        portfolios[asset] = vbt.Portfolio.from_signals(
            close=closes[asset],
            entries=entries[asset],
            exits=exits[asset],
            init_cash=10000 / len(closes.columns),  # Equal allocation
            fees=0.001
        )
    
    return portfolios

# Example with multiple assets - daily momentum approach
symbols = ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN']
data_multi = vbt.YFData.download(symbols, start='2020-01-01', end='2023-01-01')
closes_multi = data_multi.get('Close')

portfolios_momentum = daily_momentum_strategy(closes_multi)

# Calculate combined performance
total_returns = pd.Series({asset: pf.total_return() for asset, pf in portfolios_momentum.items()})
print("Daily Momentum Strategy Returns by Asset:")
print(total_returns.sort_values(ascending=False))
```

### 10. Risk Parity Strategy

```python
def risk_parity_strategy(closes, rebalance_freq='D'):
    """
    Risk parity strategy: weight assets by inverse volatility with daily rebalancing
    """
    # Calculate rolling volatilities based on daily returns
    returns = closes.pct_change()
    volatilities = returns.rolling(20).std()  # 20-day volatility for daily analysis
    
    # Inverse volatility weights
    inv_vol = 1 / volatilities
    weights = inv_vol.div(inv_vol.sum(axis=1), axis=0)
    
    # Daily rebalancing for more responsive allocation
    if rebalance_freq == 'D':
        # Use daily weights with forward-fill for missing values
        weights = weights.fillna(method='ffill')
    
    # Calculate portfolio value
    portfolio_returns = (returns * weights.shift(1)).sum(axis=1)
    portfolio_value = (1 + portfolio_returns).cumprod() * 10000
    
    return portfolio_value, weights

# Example usage with daily rebalancing
portfolio_value, weights = risk_parity_strategy(closes_multi)

# Calculate performance metrics for daily-based strategy
returns = portfolio_value.pct_change().dropna()
total_return = (portfolio_value.iloc[-1] / portfolio_value.iloc[0] - 1)
sharpe_ratio = returns.mean() / returns.std() * np.sqrt(252)
max_drawdown = (portfolio_value / portfolio_value.expanding().max() - 1).min()

print(f"Daily Risk Parity Total Return: {total_return:.2%}")
print(f"Daily Risk Parity Sharpe Ratio: {sharpe_ratio:.2f}")
print(f"Daily Risk Parity Max Drawdown: {max_drawdown:.2%}")
```

## Strategy Comparison and Analysis

### Performance Comparison Framework

```python
def compare_strategies(strategies, close, **kwargs):
    """
    Compare multiple strategies on the same dataset
    """
    results = {}
    
    for name, strategy_func in strategies.items():
        try:
            portfolio = strategy_func(close, **kwargs)
            
            results[name] = {
                'Total Return': portfolio.total_return(),
                'Sharpe Ratio': portfolio.sharpe_ratio(),
                'Max Drawdown': portfolio.max_drawdown(),
                'Win Rate': portfolio.trades.win_rate(),
                'Profit Factor': portfolio.trades.profit_factor(),
                'Avg Trade': portfolio.trades.pnl.mean()
            }
        except Exception as e:
            print(f"Error in {name}: {e}")
            continue
    
    return pd.DataFrame(results).T

# Define daily-focused strategies to compare
strategies_to_compare = {
    'SMA Crossover': sma_crossover_strategy,
    'RSI Mean Reversion': rsi_mean_reversion,
    'Bollinger Breakout': bollinger_breakout,
    'Multi-Condition': lambda close: multi_condition_strategy(close, None),
    'Daily Momentum with Stops': momentum_with_stops
}

# Compare strategies
comparison = compare_strategies(strategies_to_compare, close)
print("\nStrategy Comparison:")
print(comparison.round(4))

# Rank by Sharpe ratio
print("\nRanked by Sharpe Ratio:")
print(comparison.sort_values('Sharpe Ratio', ascending=False)['Sharpe Ratio'])
```

These examples demonstrate the flexibility and power of VectorBT for implementing various trading strategies. Each strategy can be further customized and optimized based on specific requirements and market conditions.