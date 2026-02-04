# VectorBT Documentation

VectorBT is a high-performance Python library for backtesting and analyzing trading strategies. It leverages vectorized operations and NumPy/Pandas for fast computation.

## Overview

VectorBT is designed to handle large-scale backtesting and analysis tasks efficiently. It provides a comprehensive toolkit for:

- **Fast backtesting**: Vectorized operations for high-performance strategy testing
- **Technical indicators**: Comprehensive library of technical analysis indicators
- **Portfolio management**: Advanced portfolio construction and analysis tools
- **Signal generation**: Flexible signal generation and processing
- **Data handling**: Efficient data management and processing
- **Visualization**: Rich plotting and visualization capabilities

## Key Features

- **Vectorized Operations**: All operations are vectorized using NumPy and Pandas for maximum performance
- **Flexible Architecture**: Modular design allows for easy extension and customization
- **Comprehensive Indicators**: Large library of technical indicators with TA-Lib, pandas_ta, and ta integration
- **Portfolio Analytics**: Advanced portfolio metrics and analysis tools
- **Daily Data Focus**: Optimized for daily trading data analysis
- **Memory Efficient**: Optimized memory usage for large datasets

## Core Modules

### [Base](./base.md)

Core foundational classes and utilities

### [Data](./data.md)

Data handling, loading, and management utilities

### [Generic](./generic.md)

Generic analysis tools and statistical functions

### [Indicators](./indicators.md)

Technical indicators and signal processing

### [Portfolio](./portfolio.md)

Portfolio construction, management, and analytics

### [Signals](./signals.md)

Trading signal generation and processing

### [Records](./records.md)

Record keeping and data structures

### [Returns](./returns.md)

Return analysis and performance metrics

### [Utils](./utils.md)

Utility functions and helper tools

### [Labels](./labels.md)

Labeling and categorization tools

### [Messaging](./messaging.md)

Messaging and notification systems


## Quick Start

```python
import vectorbt as vbt
import pandas as pd
import numpy as np

# Load data
data = vbt.YFData.download(['AAPL', 'GOOGL'], start='2020-01-01', end='2023-01-01')

# Create a simple SMA crossover strategy
fast_sma = vbt.indicators.SMA.run(data.get('Close'), 10)
slow_sma = vbt.indicators.SMA.run(data.get('Close'), 30)

# Generate signals
entries = fast_sma.sma_crossed_above(slow_sma)
exits = fast_sma.sma_crossed_below(slow_sma)

# Backtest the strategy
portfolio = vbt.Portfolio.from_signals(data.get('Close'), entries, exits)

# Analyze results
print(portfolio.stats())
portfolio.plot().show()
```

## Migration from Backtrader

VectorBT offers significant advantages over traditional backtesting libraries like Backtrader:

- **Performance**: Vectorized operations are orders of magnitude faster
- **Memory Efficiency**: Better memory management for large datasets
- **Flexibility**: More flexible architecture for complex strategies
- **Analysis Tools**: Rich set of analysis and visualization tools
- **Modern API**: Clean, intuitive API design

For projects migrating from Backtrader, VectorBT provides equivalent functionality with improved performance and additional features.

## Documentation Structure

### Quick Start Guides
- **[Getting Started](./quickstart/getting-started.md)**: Introduction to VectorBT basics
- **[Migration Guide](./quickstart/migration-guide.md)**: Moving from Backtrader to VectorBT

### Core Documentation
- **[Key Concepts](./core/key-concepts.md)**: Fundamental VectorBT concepts and patterns
- **[Strategy Examples](./examples/strategy-examples.md)**: Complete strategy implementations

### API Reference
- **[Indicators](./indicators.md)**: Technical indicators and signal processing
- **[Portfolio](./portfolio.md)**: Portfolio construction and management
- **[Portfolio Guide](./portfolio/portfolio-guide.md)**: Comprehensive portfolio management guide
- **[Signals](./signals.md)**: Trading signal generation and processing
- **[Returns](./returns.md)**: Return analysis and performance metrics
- **[Records](./records.md)**: Record keeping and data structures
- **[Base](./base.md)**: Core foundational classes and utilities
- **[Data](./data.md)**: Data handling, loading, and management utilities
- **[Generic](./generic.md)**: Generic analysis tools and statistical functions
- **[Utils](./utils.md)**: Utility functions and helper tools

### Specialized Modules
- **[Labels](./labels.md)**: Labeling and categorization tools
- **[Messaging](./messaging.md)**: Messaging and notification systems
- **[OHLCV Accessors](./ohlcv_accessors.md)**: OHLCV data access utilities
- **[Price Accessors](./px_accessors.md)**: Price data access utilities
- **[Root Accessors](./root_accessors.md)**: Root-level data access utilities

## Installation

```bash
pip install vectorbt
```

For additional features and performance optimizations:

```bash
pip install vectorbt[full]
```

## Resources

- [Official Website](https://vectorbt.dev/)
- [GitHub Repository](https://github.com/polakowo/vectorbt)
- [Documentation](https://vectorbt.dev/documentation/)
- [Examples](https://github.com/polakowo/vectorbt/tree/master/examples)

