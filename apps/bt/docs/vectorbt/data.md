# Data Package

## Overview
The data package provides tools for fetching, managing, and updating financial data from various sources. It supports both static data loading and real-time data streaming for trading applications.

## Key Components

### Data Sources
Built-in support for multiple data providers and formats.

```python
import vectorbt as vbt

# Yahoo Finance data
yf_data = vbt.YFData.download('AAPL', start='2020-01-01', end='2023-01-01')
close_prices = yf_data.get('Close')

# Custom data source
custom_data = vbt.Data.from_data(pandas_dataframe, symbol='CUSTOM')

# CSV data loading
csv_data = vbt.CSVData.download('data.csv', parse_dates=['Date'])
```

### Data Updater
Real-time data updating and streaming capabilities.

```python
from vectorbt.data.updater import DataUpdater

# Setup data updater for real-time feeds
class TradingDataUpdater(DataUpdater):
    def update(self):
        # Custom update logic
        new_data = fetch_latest_data()
        self.data.update(new_data)
        print(f"Updated with {len(new_data)} new data points")

# Initialize and start updates
updater = TradingDataUpdater(initial_data)
updater.update_every(interval='5min', count_limit=100)
```

### Data Wrapping and Management
```python
# Wrap raw data with metadata
data = vbt.Data.from_data(
    raw_dataframe,
    symbol=['AAPL', 'GOOGL'],
    freq='D',
    start_date='2020-01-01'
)

# Access wrapped data
prices = data.get()                    # Get all data
closes = data.get('Close')             # Get specific column
recent = data.get(tail=30)             # Get recent data
```

## Common Usage Patterns

### Multi-Asset Data Loading
```python
def load_portfolio_data(symbols, start_date, end_date):
    """Load data for multiple assets efficiently"""
    
    portfolio_data = {}
    
    for symbol in symbols:
        try:
            data = vbt.YFData.download(
                symbol, 
                start=start_date, 
                end=end_date
            )
            portfolio_data[symbol] = data.get('Close')
            print(f"Loaded {len(data.get())} data points for {symbol}")
            
        except Exception as e:
            print(f"Failed to load {symbol}: {e}")
            continue
    
    # Combine into single DataFrame
    combined = pd.DataFrame(portfolio_data)
    return combined.dropna()  # Remove dates with missing data
```

### Data Validation and Cleaning
```python
def validate_and_clean_data(data, symbol):
    """Comprehensive data validation and cleaning"""
    
    # Check for missing values
    if data.isnull().any().any():
        print(f"Warning: {symbol} has missing values")
        data = data.fillna(method='forward').fillna(method='backward')
    
    # Check for negative prices
    if (data <= 0).any().any():
        print(f"Warning: {symbol} has non-positive prices")
        data = data[data > 0]
    
    # Check for extreme outliers (more than 10x daily movement)
    daily_returns = data.pct_change().abs()
    outliers = daily_returns > 0.1
    
    if outliers.any().any():
        print(f"Warning: {symbol} has extreme price movements")
        # Handle outliers as needed
    
    return data
```

### Custom Data Source Integration
```python
class AlphaVantageData(vbt.Data):
    """Custom data source for Alpha Vantage API"""
    
    @classmethod
    def download(cls, symbol, api_key, function='TIME_SERIES_DAILY', **kwargs):
        """Download data from Alpha Vantage"""
        
        import requests
        
        url = 'https://www.alphavantage.co/query'
        params = {
            'function': function,
            'symbol': symbol,
            'apikey': api_key,
            'outputsize': 'full',
            'datatype': 'json'
        }
        
        response = requests.get(url, params=params)
        data = response.json()
        
        # Parse JSON response to DataFrame
        time_series = data['Time Series (Daily)']
        df = pd.DataFrame.from_dict(time_series, orient='index')
        df.index = pd.to_datetime(df.index)
        df = df.astype(float)
        
        # Rename columns to standard format
        df.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        df = df.sort_index()
        
        return cls.from_data(df, symbol=symbol, **kwargs)

# Usage
av_data = AlphaVantageData.download('AAPL', api_key='YOUR_KEY')
close_prices = av_data.get('Close')
```

### Real-Time Data Streaming
```python
class LiveTradingUpdater(DataUpdater):
    """Real-time data updater for live trading"""
    
    def __init__(self, data, api_connection, **kwargs):
        super().__init__(data, **kwargs)
        self.api = api_connection
        self.trade_signals = []
    
    def update(self):
        """Fetch and process new data"""
        try:
            # Get latest data from API
            latest_data = self.api.get_latest_bars(self.data.symbol)
            
            if latest_data is not None and not latest_data.empty:
                # Update the data
                self.data.update(latest_data)
                
                # Check for trading signals
                self.check_trading_signals(latest_data)
                
                print(f"Updated {self.data.symbol} at {latest_data.index[-1]}")
            
        except Exception as e:
            print(f"Error updating data: {e}")
    
    def check_trading_signals(self, new_data):
        """Check for trading signals in new data"""
        current_price = new_data['Close'].iloc[-1]
        
        # Simple example: check if price crosses moving average
        if len(self.data.get()) >= 20:
            ma_20 = self.data.get('Close').tail(20).mean()
            
            if current_price > ma_20 * 1.02:  # 2% above MA
                self.trade_signals.append({
                    'timestamp': new_data.index[-1],
                    'signal': 'BUY',
                    'price': current_price,
                    'reason': 'Price above 20-day MA'
                })

# Setup live trading
live_updater = LiveTradingUpdater(initial_data, api_connection)
live_updater.update_every(interval='1min')
```

### Data Caching and Persistence
```python
def setup_data_cache(cache_dir='./data_cache'):
    """Setup persistent data caching"""
    
    import os
    import pickle
    from pathlib import Path
    
    cache_path = Path(cache_dir)
    cache_path.mkdir(exist_ok=True)
    
    def cached_data_download(symbol, start_date, end_date, force_refresh=False):
        """Download data with caching"""
        
        cache_file = cache_path / f"{symbol}_{start_date}_{end_date}.pkl"
        
        # Check if cached data exists and is recent
        if cache_file.exists() and not force_refresh:
            cache_age = time.time() - cache_file.stat().st_mtime
            if cache_age < 24 * 3600:  # Less than 24 hours old
                print(f"Loading {symbol} from cache")
                with open(cache_file, 'rb') as f:
                    return pickle.load(f)
        
        # Download fresh data
        print(f"Downloading fresh data for {symbol}")
        data = vbt.YFData.download(symbol, start=start_date, end=end_date)
        
        # Cache the data
        with open(cache_file, 'wb') as f:
            pickle.dump(data, f)
        
        return data
    
    return cached_data_download

# Usage
download_cached = setup_data_cache()
data = download_cached('AAPL', '2020-01-01', '2023-01-01')
```

## Advanced Data Management

### Daily Data Analysis Enhancement
```python
def enhance_daily_dataset(symbol, start_date, end_date):
    """Enhance daily dataset with derived metrics and analysis"""
    
    # Download daily data
    daily_data = vbt.YFData.download(symbol, start=start_date, end=end_date)
    daily_prices = daily_data.get('Close')
    
    # Calculate daily-based metrics
    enhanced_data = {
        'Close': daily_prices,
        'Daily_Return': daily_prices.pct_change(),
        'Rolling_5d': daily_prices.rolling(5).mean(),
        'Rolling_20d': daily_prices.rolling(20).mean(),
        'Volatility_5d': daily_prices.pct_change().rolling(5).std(),
        'Volatility_20d': daily_prices.pct_change().rolling(20).std()
    }
    
    # Create comprehensive daily analysis DataFrame
    return pd.DataFrame(enhanced_data).dropna()
```

### Data Quality Monitoring
```python
class DataQualityMonitor:
    """Monitor data quality and detect issues"""
    
    def __init__(self, data_source):
        self.data_source = data_source
        self.quality_metrics = {}
    
    def check_data_quality(self, data):
        """Comprehensive data quality check"""
        
        metrics = {}
        
        # Check completeness
        total_points = len(data)
        missing_points = data.isnull().sum().sum()
        metrics['completeness'] = 1 - (missing_points / total_points)
        
        # Check for stale data
        if hasattr(data.index, 'freq'):
            expected_freq = pd.infer_freq(data.index)
            actual_gaps = data.index.to_series().diff().mode()[0]
            metrics['freshness'] = expected_freq == actual_gaps
        
        # Check for outliers
        if 'Close' in data.columns:
            returns = data['Close'].pct_change()
            outlier_threshold = returns.std() * 3
            outliers = (returns.abs() > outlier_threshold).sum()
            metrics['outlier_ratio'] = outliers / len(returns)
        
        # Check price consistency (OHLC relationships)
        if all(col in data.columns for col in ['Open', 'High', 'Low', 'Close']):
            consistency_issues = 0
            consistency_issues += (data['High'] < data['Low']).sum()
            consistency_issues += (data['High'] < data['Open']).sum()
            consistency_issues += (data['High'] < data['Close']).sum()
            consistency_issues += (data['Low'] > data['Open']).sum()
            consistency_issues += (data['Low'] > data['Close']).sum()
            
            metrics['price_consistency'] = 1 - (consistency_issues / len(data))
        
        self.quality_metrics = metrics
        return metrics
    
    def generate_quality_report(self):
        """Generate data quality report"""
        
        report = "Data Quality Report\n" + "="*20 + "\n"
        
        for metric, value in self.quality_metrics.items():
            status = "GOOD" if value > 0.95 else "WARNING" if value > 0.8 else "POOR"
            report += f"{metric.title()}: {value:.2%} ({status})\n"
        
        return report

# Usage
quality_monitor = DataQualityMonitor(data_source)
metrics = quality_monitor.check_data_quality(price_data)
print(quality_monitor.generate_quality_report())
```

### Memory-Efficient Data Processing
```python
def process_large_dataset(data_source, chunk_size=10000, processing_func=None):
    """Process large datasets in memory-efficient chunks"""
    
    total_rows = len(data_source.get())
    results = []
    
    for start_idx in range(0, total_rows, chunk_size):
        end_idx = min(start_idx + chunk_size, total_rows)
        
        # Load chunk
        chunk = data_source.get().iloc[start_idx:end_idx]
        
        # Process chunk
        if processing_func:
            processed_chunk = processing_func(chunk)
            results.append(processed_chunk)
        
        # Memory cleanup
        del chunk
        if start_idx > 0 and start_idx % (chunk_size * 10) == 0:
            import gc
            gc.collect()
        
        print(f"Processed {end_idx}/{total_rows} rows ({end_idx/total_rows:.1%})")
    
    # Combine results if any processing was done
    if results:
        return pd.concat(results, ignore_index=True)
    
    return None
```

The data package provides robust infrastructure for handling financial data from acquisition through real-time updates, with built-in quality monitoring and memory-efficient processing capabilities.